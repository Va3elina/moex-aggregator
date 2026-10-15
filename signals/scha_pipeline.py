"""Конвейер справок о СЧА с сайтов УК: найти → скачать → вынуть текст → агент читает → залить → сообщить.

    python -m signals.scha_pipeline scan   # cron раз в день: новые справки → архив → задания
    python -m signals.scha_pipeline work   # cron раз в 5 мин: двигает задания по статусам

Статусы задания (scha_jobs.status):
    new → text_ready → agent_wait → agent_done → imported | report_only | doubt | failed
Читает справку рутина frame-scha-reader (агент): она берёт задание с сервера
(api/routers/scha_jobs.py), читает бриф research/scha_pipeline/BRIEF.md, проверяет себя и отвечает
«сошлось» или «сомневаюсь». Скрипты здесь — только «трубы»: решение о чтении принимает агент
(решение Вадима 15.10.2026).

Где что:
  - сайты УК и скачивание — Funds/scha_collector.py;
  - текст из файла и запись в базу — Funds/scha_job_tools.py ВНУТРИ api-контейнера (архив виден ему
    как /data/manual_scha/auto, только чтение);
  - запуск рутины — общая очередь signals/routine_queue.py + диспетчер signals/routine_dispatch.py;
  - сообщения — @framesignalbot через research/robots_tg/robots_tg.py;
  - таблицы — db/migrations/108_routine_queue_scha_jobs.sql; план — research/scha_pipeline/PLAN.md.

SCHA_AUTO_IMPORT=1 — «сошлось» заливается. Без него (режим отчёта) — сообщение «залил бы», база не меняется.
"""
from __future__ import annotations

import hashlib
import html
import json
import os
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# .env + DB_URL на хост — ДО импорта api.database (как в content_ai.py)
from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")
os.environ["DB_URL"] = os.environ.get("DB_URL", "").replace("@db:", "@127.0.0.1:")

from sqlalchemy import text  # noqa: E402

from api.database import get_engine  # noqa: E402
from Funds import scha_collector as col  # noqa: E402
from signals.routine_queue import enqueue  # noqa: E402

ARCHIVE = ROOT / "data" / "manual_scha" / "auto"        # хост: /opt/frame/data/manual_scha/auto
ARCHIVE_IN_API = "/data/manual_scha/auto"                # то же в api-контейнере (только чтение)
AUTO_IMPORT = os.environ.get("SCHA_AUTO_IMPORT", "0") == "1"
ROUTINE = "SCHA_READER"   # → TRIGGER_ID_SCHA_READER / CLAUDE_ROUTINE_FIRE_TOKEN_SCHA_READER в .env
API_HOST = "https://framedata.ru"   # ⚠️ только ASCII-домен: кириллица ломает curl внутри рутины
MONTHLY_SOURCES = ("interfax_manual", "vim_sdr")
MAX_ATTEMPTS = 3          # сбои окружения (контейнер, сеть) — потом 'failed' и сообщение
LEASE_MIN = 15            # задание взято в работу — другие его не трогают; упал воркер → вернётся само
WORK_BUDGET_SEC = 240     # cron раз в 5 мин — прогоны не наслаиваются (плюс flock в .sh)
LOCK_SCAN = 0x53434841    # pg advisory lock «SCHA»

MONTHS = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль", "август",
          "сентябрь", "октябрь", "ноябрь", "декабрь"]


class InfraError(RuntimeError):
    """Сбой не справки, а окружения (контейнер, docker) — задание повторяется позже."""


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def month_ru(period: str) -> str:
    y, m = period.split("-")
    return f"{MONTHS[int(m) - 1]} {y}"


def esc(s) -> str:
    return html.escape(str(s), quote=False)


def link(url: str, label: str = "справка") -> str:
    """Ссылка в Telegram-HTML: адрес экранируется ВМЕСТЕ с кавычками — он взят со страницы УК,
    кавычка в нём иначе закрыла бы атрибут href и вставила свою разметку."""
    return f'<a href="{html.escape(url, quote=True)}">{esc(label)}</a>'


def num_ru(x: float, nd: int = 0) -> str:
    return f"{x:,.{nd}f}".replace(",", " ").replace(".", ",")


# ── Telegram ──────────────────────────────────────────────────────────────

def notify(text_: str, silent: bool = False) -> None:
    sys.path.insert(0, str(ROOT / "research" / "robots_tg"))
    import robots_tg
    robots_tg.send(text_, silent=silent)


def notify_once(conn, key: str, text_: str) -> None:
    """Сбой — не чаще раза в сутки на ключ (сайт, контейнер)."""
    fresh = conn.execute(text("""
        INSERT INTO scha_notify_log (key, sent_at) VALUES (:k, now())
        ON CONFLICT (key) DO UPDATE SET sent_at = now()
            WHERE scha_notify_log.sent_at < now() - interval '1 day'
        RETURNING key
    """), {"k": key}).first()
    if fresh:
        notify(text_)


def event(conn, job_id, ev: str, detail: str = "") -> None:
    conn.execute(text("INSERT INTO scha_job_events (job_id, event, detail) VALUES (:j, :e, :d)"),
                 {"j": job_id, "e": ev, "d": detail[:2000]})


# ── api-контейнер ─────────────────────────────────────────────────────────

def run_tool(*args: str) -> dict:
    cid = subprocess.run(["docker", "ps", "-q", "-f", "label=com.docker.compose.service=api"],
                         capture_output=True, text=True, timeout=30).stdout.split()
    if not cid:
        raise InfraError("api-контейнер не запущен")
    r = subprocess.run(["docker", "exec", cid[0], "python3", "-m", "Funds.scha_job_tools", *args],
                       capture_output=True, text=True, timeout=600)
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    if r.returncode != 0 or not lines:
        raise InfraError(f"scha_job_tools {args[0]}: код {r.returncode}: {r.stderr[-400:]}")
    try:
        return json.loads(lines[-1])
    except ValueError as e:
        raise InfraError(f"scha_job_tools {args[0]}: не JSON: {lines[-1][:200]}") from e


def in_api(host_path: str | Path) -> str:
    return ARCHIVE_IN_API + "/" + str(Path(host_path).relative_to(ARCHIVE))


# ── scan ──────────────────────────────────────────────────────────────────

def _latest_periods(conn) -> dict[str, str]:
    rows = conn.execute(text("""
        SELECT f.ticker, to_char(max(h.snapshot_date), 'YYYY-MM')
        FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id
        WHERE h.source = ANY(:s) GROUP BY 1
    """), {"s": list(MONTHLY_SOURCES)}).all()
    return dict(rows)


def scan() -> int:
    eng = get_engine()
    with eng.connect() as lock:
        if not lock.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_SCAN}).scalar():
            log("сканер уже идёт — выходим")
            return 0
        try:
            return _scan(eng)
        finally:
            lock.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_SCAN})


def _scan(eng) -> int:
    with eng.begin() as conn:
        latest = _latest_periods(conn)
        names = dict(conn.execute(text("SELECT ticker, name FROM funds")).all())
    found, failed = [], 0
    for tk, (kind, arg) in col.SOURCES.items():
        try:
            docs = col.one_per_month(col.ADAPTERS[kind](tk, arg))
            if not docs:
                raise RuntimeError("на странице не нашлось ни одной справки (сменили вёрстку?)")
        except Exception as e:  # noqa: BLE001 — один сайт не роняет остальные
            failed += 1
            log(f"{tk}: страница раскрытия — {e}")
            site = urllib.parse.urlsplit(arg).hostname if "://" in arg else kind
            with eng.begin() as conn:
                notify_once(conn, f"scha:site:{site}",
                            f"⚠️ Справки СЧА: не открылась страница раскрытия {esc(site)} ({esc(tk)})\n"
                            f"{esc(str(e)[:200])}")
            continue
        # Только месяцы новее последнего залитого: старые дыры (TMOS 2020, 04.2023) — решение
        # человека, а не автомата. Новый фонд (в базе пусто) — только последняя справка.
        last = latest.get(tk)
        new = sorted(p for p in docs if p > last) if last else [max(docs)]
        for p in new:
            d = docs[p]
            with eng.begin() as conn:
                seen = conn.execute(text(
                    "SELECT 1 FROM scha_jobs WHERE ticker = :t AND period = :p AND url = :u"),
                    {"t": tk, "p": p, "u": d.url}).first()
            if seen:
                continue
            try:
                (ARCHIVE / tk).mkdir(parents=True, exist_ok=True)
                tmp = col.download(d, ARCHIVE / tk)
                sha = hashlib.sha256(tmp.read_bytes()).hexdigest()
                path = tmp.with_name(f"{tmp.stem}_{sha[:8]}{tmp.suffix}")   # перепубликация не затрёт прошлую
                tmp.replace(path)
            except Exception as e:  # noqa: BLE001
                failed += 1
                log(f"{tk} {p}: скачивание — {e}")
                with eng.begin() as conn:
                    notify_once(conn, f"scha:download:{tk}:{p}",
                                f"⚠️ Справки СЧА: не скачалась {esc(tk)} за {month_ru(p)}\n{esc(str(e)[:200])}")
                continue
            with eng.begin() as conn:
                job = conn.execute(text("""
                    INSERT INTO scha_jobs (ticker, period, url, sha256, file_path)
                    VALUES (:t, :p, :u, :s, :f) ON CONFLICT (sha256) DO NOTHING RETURNING id
                """), {"t": tk, "p": p, "u": d.url, "s": sha, "f": str(path)}).first()
                if job:
                    event(conn, job.id, "found", d.url)
                    found.append((tk, p, last is None))
                    log(f"{tk} {p}: новая справка → задание #{job.id}")
            time.sleep(col.PAUSE)
        time.sleep(col.PAUSE)
    if found:
        lines = [f"{'🆕 ' if new_fund else ''}{esc(names.get(tk, tk))} ({esc(tk)}) · {month_ru(p)}"
                 for tk, p, new_fund in found]
        notify("📄 Новые справки СЧА\n" + "\n".join(lines), silent=True)
    log(f"сканер: новых {len(found)}, сбоев {failed}")
    return 0


# ── work ──────────────────────────────────────────────────────────────────

def _set(conn, job_id: int, **cols) -> None:
    sets = ", ".join(f"{k} = :{k}" for k in cols)
    conn.execute(text(f"UPDATE scha_jobs SET {sets}, next_try_at = now(), updated_at = now() WHERE id = :id"),
                 {"id": job_id, **cols})


def _claim(eng):
    """Взять одно готовое к шагу задание на LEASE_MIN минут (упадёт воркер — вернётся само)."""
    with eng.begin() as conn:
        job = conn.execute(text("""
            SELECT j.*, f.name AS fund_name FROM scha_jobs j LEFT JOIN funds f ON f.ticker = j.ticker
            WHERE j.status IN ('new', 'text_ready', 'agent_done') AND j.next_try_at <= now()
            ORDER BY j.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED
        """)).first()
        if job:
            conn.execute(text("""
                UPDATE scha_jobs SET next_try_at = now() + make_interval(mins => :m), updated_at = now()
                WHERE id = :id
            """), {"id": job.id, "m": LEASE_MIN})
        return job


def _head(job) -> str:
    return f"{esc(job.fund_name or job.ticker)} ({esc(job.ticker)}) · {month_ru(job.period)}"


def _fail(eng, job, why: str) -> None:
    with eng.begin() as conn:
        _set(conn, job.id, status="failed", last_error=why[:1000])
        event(conn, job.id, "failed", why)
    notify(f"⚠️ {_head(job)}\nсправка не обработана: {esc(why[:300])}\n{link(job.url)}")
    log(f"#{job.id} {job.ticker} {job.period}: failed — {why}")


def _infra_fail(eng, job, e: Exception) -> None:
    attempts = job.attempts + 1
    if attempts >= MAX_ATTEMPTS:
        with eng.begin() as conn:
            conn.execute(text("UPDATE scha_jobs SET attempts = :a WHERE id = :id"), {"id": job.id, "a": attempts})
        _fail(eng, job, f"после {attempts} попыток: {e}")
        return
    with eng.begin() as conn:
        conn.execute(text("""
            UPDATE scha_jobs SET attempts = :a, last_error = :e, updated_at = now(),
                   next_try_at = now() + make_interval(mins => 10 * :a)
            WHERE id = :id
        """), {"id": job.id, "a": attempts, "e": str(e)[:1000]})
        event(conn, job.id, "retry", str(e))
    log(f"#{job.id} {job.ticker} {job.period}: сбой ({attempts}/{MAX_ATTEMPTS}): {e}")


def _step_text(eng, job) -> None:
    res = run_tool("text", in_api(job.file_path))
    if res.get("error"):
        _fail(eng, job, f"не удалось вынуть текст: {res['error']}")
        return
    txt = Path(job.file_path).with_suffix(".txt")
    txt.write_text(res["text"])
    with eng.begin() as conn:
        _set(conn, job.id, status="text_ready", text_path=str(txt))
        event(conn, job.id, "text", f"{len(res['text'])} символов")
    log(f"#{job.id} {job.ticker} {job.period}: текст {len(res['text'])} символов")


def _step_enqueue(eng, job) -> None:
    # СВОЙ токен, не CONTENT_AI_INTERNAL_TOKEN: агент читает внешний документ, токен в его руках
    # должен открывать только ручки справок (api/routers/scha_jobs.py).
    token = os.environ.get("SCHA_INTERNAL_TOKEN", "")
    if not token:
        raise InfraError("нет SCHA_INTERNAL_TOKEN в .env — рутине нечем постучаться за заданием")
    with eng.begin() as conn:
        qid = enqueue(conn, ROUTINE, "", source="scha", priority=100,
                      dedup_key=f"scha:{job.id}", timeout_sec=1800)
        # payload знает id задания очереди — дописываем после вставки
        payload = (f"job_id: {job.id}\nqueue_id: {qid}\napi_host: {API_HOST}\n"
                   f"internal_token: {token}\n"
                   f"Фонд: {job.fund_name or job.ticker} ({job.ticker}), справка за {month_ru(job.period)}")
        conn.execute(text("UPDATE routine_queue SET payload = :p WHERE id = :id"), {"p": payload, "id": qid})
        _set(conn, job.id, status="agent_wait", queue_id=qid)
        event(conn, job.id, "queued", f"routine_queue #{qid}")
    log(f"#{job.id} {job.ticker} {job.period}: в очередь рутин #{qid}")


_ISIN = __import__("re").compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
MAX_ROWS = 2000


def _format_problems(a: dict, period: str) -> list[str]:
    """Проверка ФОРМАТА ответа перед записью — не правильности чтения (её делает агент по брифу):
    ответ модели, прочитавшей внешний документ, в базу идёт только в ожидаемом виде."""
    import math
    out = []
    rows = a.get("rows")
    if not isinstance(rows, list) or not rows:
        return ["в ответе нет списка бумаг"]
    if len(rows) > MAX_ROWS:
        out.append(f"слишком много строк: {len(rows)}")
    if not str(a.get("snapshot_date") or "").startswith(period):
        out.append(f"дата справки {a.get('snapshot_date')!r} не за {period}")
    for r in rows[:MAX_ROWS]:
        if not isinstance(r, dict):
            out.append("строка ответа не объект")
            continue
        isin = str(r.get("isin") or "").strip().upper()
        if not _ISIN.match(isin):
            out.append(f"не ISIN: {isin[:20]!r}")
            continue
        for k in ("quantity", "value_rub"):
            v = r.get(k)
            if v in (None, ""):
                continue
            try:
                x = float(str(v).replace("\u00a0", "").replace(" ", "").replace(",", "."))
            except ValueError:
                out.append(f"{isin}: {k} не число ({str(v)[:20]!r})")
                continue
            if not math.isfinite(x) or x < 0:
                out.append(f"{isin}: {k} = {x}")
    return out[:5]


def _step_result(eng, job) -> None:
    a = job.agent or {}
    rows = [r for r in a.get("rows") or [] if isinstance(r, dict) and r.get("isin")]
    def _f(v):
        try:
            return float(str(v).replace("\u00a0", "").replace(" ", "").replace(",", ".")) if v not in (None, "") else 0.0
        except ValueError:
            return 0.0
    total = sum(_f(r.get("value_rub")) for r in rows)
    gross = _f(a.get("gross_assets_rub"))
    cov = f"{num_ru(total / gross * 100, 1)} % активов" if gross else "без итога активов"
    stats = f"{len(rows)} бумаг на {num_ru(total / 1e6, 1)} млн ₽, {cov}"
    doubts = [str(d) for d in (a.get("doubts") or []) if d]
    bad_format = _format_problems(a, job.period) if job.agent_ok else []
    if bad_format:
        doubts = ["ответ агента не в ожидаемом виде: " + "; ".join(bad_format)] + doubts
    if not job.agent_ok or bad_format:
        status = "doubt"
        msg = (f"🟡 {_head(job)}\nне залито — агент сомневается ({stats}):\n"
               + "\n".join(f"— {esc(d[:300])}" for d in doubts[:5]) + f"\n{link(job.url)}")
    elif AUTO_IMPORT:
        agent_file = Path(job.file_path).with_suffix(".agent.json")
        agent_file.write_text(json.dumps(a, ensure_ascii=False))
        res = run_tool("import", in_api(agent_file), job.ticker, job.period)
        if res.get("error"):
            status, msg = "doubt", f"🟡 {_head(job)}\nне залито: {esc(res['error'])}\n{link(job.url)}"
        else:
            status, msg = "imported", f"✅ {_head(job)}\nзалито: {stats}"
        with eng.begin() as conn:
            _set(conn, job.id, import_result=json.dumps(res, ensure_ascii=False))
    else:
        status, msg = "report_only", f"🟢 {_head(job)}\nзалил бы: {stats}"
    with eng.begin() as conn:
        first = not conn.execute(text("""
            SELECT 1 FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id
            WHERE f.ticker = :t AND h.source = ANY(:s) AND h.snapshot_date < CAST(:d AS date) LIMIT 1
        """), {"t": job.ticker, "s": list(MONTHLY_SOURCES), "d": job.period + "-01"}).first()
        _set(conn, job.id, status=status)
        event(conn, job.id, status, "; ".join(doubts)[:2000])
    if first:
        msg = "🆕 первые данные фонда\n" + msg
    notify(msg, silent=status in ("imported", "report_only"))
    with eng.begin() as conn:
        conn.execute(text("UPDATE scha_jobs SET notified_at = now() WHERE id = :id"), {"id": job.id})
    log(f"#{job.id} {job.ticker} {job.period}: {status}")


def _sweep_lost(eng) -> None:
    """Рутина так и не ответила — очередь сдалась ('failed') → задание тоже failed + сообщение."""
    with eng.begin() as conn:
        lost = conn.execute(text("""
            SELECT j.*, f.name AS fund_name, q.last_error AS q_error
            FROM scha_jobs j JOIN routine_queue q ON q.id = j.queue_id
            LEFT JOIN funds f ON f.ticker = j.ticker
            WHERE j.status = 'agent_wait' AND q.status IN ('failed', 'cancelled')
        """)).all()
    for job in lost:
        _fail(eng, job, f"агент не ответил: {job.q_error or 'очередь сдалась'}")


STEPS = {"new": _step_text, "text_ready": _step_enqueue, "agent_done": _step_result}


def work() -> int:
    eng = get_engine()
    _sweep_lost(eng)
    started = time.monotonic()
    while time.monotonic() - started < WORK_BUDGET_SEC:
        job = _claim(eng)
        if not job:
            break
        try:
            STEPS[job.status](eng, job)
        except Exception as e:  # noqa: BLE001 — InfraError и неизвестное: повтор с паузой и пределом
            _infra_fail(eng, job, e)
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd not in ("scan", "work"):
        print("usage: python -m signals.scha_pipeline scan|work")
        sys.exit(2)
    sys.exit(scan() if cmd == "scan" else work())
