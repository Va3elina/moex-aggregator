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
from datetime import date
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

def _tg():
    """Модуль сообщений бота. Рабочая копия — /opt/robot-notify (там state/emoji_ids.json пака эмодзи,
    без него вместо логотипов будут обычные эмодзи); в репозитории — тот же код без состояния."""
    for d in (Path("/opt/robot-notify"), ROOT / "research" / "robots_tg"):
        if (d / "robots_tg.py").exists():
            sys.path.insert(0, str(d))
            break
    import robots_tg
    return robots_tg


def notify(text_: str, silent: bool = False) -> None:
    _tg().send(text_, silent=silent)


# Значок фонда — логотип УК или фото автора из пака бота (research/robots_tg/emoji/make_fund_icons.py),
# те же картинки, что на сайте (fundConfig.ts: UK_LOGOS / AUTHOR_LOGOS).
UK_ICON = {"3597": "ук т-капитал", "5": "ук альфа", "34": "ук первая", "7": "ук вим",
           "aton": "ук атон", "record": "ук рекорд", "geroi": "ук герои"}
AUTHOR_ICON = {"RU000A10BZ69": "автор коган", "RU000A10EBY8": "автор воробьёв", "RU000A10B8Z2": "автор щетинин",
               "RU000A10B917": "автор кудрицкий", "RU000A10B909": "автор крейнин", "RU000A10D1E0": "автор шимко",
               "RU000A10D5D3": "автор линецкий"}
ICON_FALLBACK = {"ук т-капитал": "🟨",
                 "ук альфа": "🟥",
                 "ук первая": "🟩",
                 "ук вим": "🟦",
                 "ук атон": "🔷",
                 "ук рекорд": "🪆",
                 "ук герои": "🦸",
                 "автор коган": "🧔",
                 "автор воробьёв": "🧑",
                 "автор щетинин": "👨",
                 "автор кудрицкий": "👱",
                 "автор крейнин": "🤵",
                 "автор шимко": "🎩",
                 "автор линецкий": "👓"}


def fund_icon(ticker: str, uk_id: str | None) -> str:
    name = AUTHOR_ICON.get(ticker) or UK_ICON.get(str(uk_id or ""))
    if not name:
        return ""
    fb = ICON_FALLBACK.get(name, "▫️")
    try:
        cid = _tg()._emoji_ids().get(name)
    except Exception:  # noqa: BLE001
        cid = None
    return f'<tg-emoji emoji-id="{cid}">{fb}</tg-emoji>' if cid else fb


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


def notify_first(conn, key: str, text_: str, silent: bool = False) -> None:
    """Событие, о котором говорим один раз навсегда («все фонды за месяц загружены»)."""
    fresh = conn.execute(text("""
        INSERT INTO scha_notify_log (key, sent_at) VALUES (:k, now())
        ON CONFLICT (key) DO NOTHING RETURNING key
    """), {"k": key}).first()
    if fresh:
        notify(text_, silent=silent)


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


SITE_RETRY_SEC = 30   # 18.10: t-capital-funds.ru разово отдал страницу без списка (TMOS), через минуту — норма


class EmptyPage(RuntimeError):
    """Страница открылась, но справок на ней нет. У t-capital-funds.ru так бывает минутами (TMOS 22.10,
    TBRU 27.10 — обе попытки подряд), поэтому тревога только если пусто и на следующий день."""


def _list_docs(tk: str, kind: str, arg: str) -> dict:
    """Справки на странице раскрытия; пустой ответ или сбой — ещё одна попытка, потом ошибка.
    Разовый сбой сайта УК не должен будить Вадима (тревога шлётся только после второй неудачи)."""
    for attempt in (1, 2):
        try:
            docs = col.one_per_month(col.ADAPTERS[kind](tk, arg))
            if not docs:
                raise EmptyPage("на странице не нашлось ни одной справки (сменили вёрстку?)")
            return docs
        except Exception as e:  # noqa: BLE001
            if attempt == 2:
                raise
            log(f"{tk}: страница раскрытия — {e}; повтор через {SITE_RETRY_SEC} с")
            time.sleep(SITE_RETRY_SEC)
    raise AssertionError("unreachable")


def _empty_yesterday(conn, tk: str) -> bool:
    """Отметить «сегодня пусто»; True — пусто было и в прошлый скан (отметка 12–48 ч назад)."""
    prev = conn.execute(text("SELECT sent_at FROM scha_notify_log WHERE key = :k"),
                        {"k": f"scha:empty:{tk}"}).scalar()
    conn.execute(text("""
        INSERT INTO scha_notify_log (key, sent_at) VALUES (:k, now())
        ON CONFLICT (key) DO UPDATE SET sent_at = now()
    """), {"k": f"scha:empty:{tk}"})
    if prev is None:
        return False
    age_h = conn.execute(text("SELECT extract(epoch FROM now() - :t) / 3600"), {"t": prev}).scalar()
    return 12 <= age_h <= 48


def _scan(eng) -> int:
    with eng.begin() as conn:
        latest = _latest_periods(conn)
        names = dict(conn.execute(text("SELECT ticker, name FROM funds")).all())
    found, failed = [], 0
    for tk, (kind, arg) in col.SOURCES.items():
        try:
            docs = _list_docs(tk, kind, arg)
        except Exception as e:  # noqa: BLE001 — один сайт не роняет остальные
            failed += 1
            log(f"{tk}: страница раскрытия — {e}")
            site = urllib.parse.urlsplit(arg).hostname if "://" in arg else kind
            with eng.begin() as conn:
                if isinstance(e, EmptyPage) and not _empty_yesterday(conn, tk):
                    log(f"{tk}: пусто впервые — без тревоги, завтрашний сканер проверит снова")
                    continue
                notify_once(conn, f"scha:site:{site}",
                            f"⚠️ Справки СЧА: не открылась страница раскрытия {esc(site)} ({esc(tk)})\n"
                            f"{esc(str(e)[:200])}")
            continue
        with eng.begin() as conn:
            conn.execute(text("DELETE FROM scha_notify_log WHERE key = :k"), {"k": f"scha:empty:{tk}"})
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
    # «Найдены» в Telegram не шлём (Вадим 15.10: нужен только итог) — итог придёт сводкой из work().
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
            SELECT j.*, f.name AS fund_name, f.uk_id FROM scha_jobs j LEFT JOIN funds f ON f.ticker = j.ticker
            WHERE j.status IN ('new', 'text_ready', 'agent_done') AND j.next_try_at <= now()
            ORDER BY j.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED
        """)).first()
        if job:
            conn.execute(text("""
                UPDATE scha_jobs SET next_try_at = now() + make_interval(mins => :m), updated_at = now()
                WHERE id = :id
            """), {"id": job.id, "m": LEASE_MIN})
        return job


def _fail(eng, job, why: str) -> None:
    with eng.begin() as conn:
        _set(conn, job.id, status="failed", last_error=why[:1000])
        event(conn, job.id, "failed", why)
    log(f"#{job.id} {job.ticker} {job.period}: failed — {why}")   # в Telegram — сводкой (digest)


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


def _num(v) -> float:
    try:
        return float(str(v).replace("\u00a0", "").replace(" ", "").replace(",", ".")) if v not in (None, "") else 0.0
    except ValueError:
        return 0.0


BACKUP_DIR = Path("/opt/frame/backups/scha_replaced")


def _backup_month(job) -> None:
    """Перед заливкой (она заменяет месяц целиком) — копия уже лежащих строк фонда за этот месяц в CSV.
    Для нового месяца строк нет — файл не создаётся. Сбой копии = сбой заливки (повтор позже)."""
    import re
    if not re.fullmatch(r"[A-Z0-9-]{2,20}", job.ticker) or not re.fullmatch(r"\d{4}-\d{2}", job.period):
        raise InfraError(f"странный тикер/месяц для копии: {job.ticker} {job.period}")
    sql = (f"\\copy (SELECT h.* FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id "
           f"WHERE f.ticker = '{job.ticker}' AND to_char(h.snapshot_date, 'YYYY-MM') = '{job.period}' "
           f"AND h.source IN ('interfax_manual', 'vim_sdr')) TO STDOUT CSV HEADER")
    r = subprocess.run(["docker", "exec", "frame-db-1", "psql", "-U", "postgres", "-d", "moex_db", "-c", sql],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise InfraError(f"копия месяца не снялась: {r.stderr[-300:]}")
    if r.stdout.count("\n") > 1:   # шапка + хотя бы одна строка
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        (BACKUP_DIR / f"{job.ticker}_{job.period}_job{job.id}.csv").write_text(r.stdout)
        log(f"#{job.id} {job.ticker} {job.period}: копия {r.stdout.count(chr(10)) - 1} старых строк перед заменой")


def _step_result(eng, job) -> None:
    """Ответ агента → залить (или «залил бы» в режиме отчёта) либо отложить с причиной.
    В Telegram — не здесь, а одной сводкой на пачку (digest)."""
    a = job.agent or {}
    doubts = [str(d) for d in (a.get("doubts") or []) if d]
    bad_format = _format_problems(a, job.period) if job.agent_ok else []
    reason, import_result = None, None
    if job.agent_ok and not a.get("rows") and _num(a.get("gross_assets_rub")) > 0:
        # Бумаг у фонда нет (только деньги/РЕПО/дебиторка) — агент так и прочитал. Заливать нечего,
        # но это и не сомнение (16.10: WFLO в первый месяц, SAFE в годы «одного РЕПО»).
        status, reason = "no_securities", None
    elif bad_format:
        status, reason = "doubt", "ответ агента не в ожидаемом виде: " + "; ".join(bad_format)
    elif not job.agent_ok:
        status, reason = "doubt", "агент сомневается: " + (doubts[0] if doubts else "без пояснения")
    elif AUTO_IMPORT:
        _backup_month(job)
        agent_file = Path(job.file_path).with_suffix(".agent.json")
        agent_file.write_text(json.dumps(a, ensure_ascii=False))
        res = run_tool("import", in_api(agent_file), job.ticker, job.period)
        import_result = json.dumps(res, ensure_ascii=False)
        status, reason = ("doubt", f"не записалось: {res['error']}") if res.get("error") else ("imported", None)
    else:
        status = "report_only"
    with eng.begin() as conn:
        cols = dict(status=status, last_error=reason[:1000] if reason else None)
        if import_result:
            cols["import_result"] = import_result
        _set(conn, job.id, **cols)
        event(conn, job.id, status, (reason or "; ".join(doubts))[:2000])
    log(f"#{job.id} {job.ticker} {job.period}: {status}" + (f" — {reason}" if reason else ""))


DIGEST_MAX_WAIT_H = 3   # если одна справка застряла, остальным не ждать сводку дольше
DIGEST_COMPACT_FROM = 20  # больше справок в пачке (заливка истории) — строка на фонд, а не на месяц


def digest(eng) -> None:
    """Одно сообщение на пачку справок (Вадим 15.10: «очень шумно»): месяц один раз в заголовке,
    фонды списком с логотипом, без кодов; только итог — залито / не залито и почему.
    «Залил бы» (режим отчёта) в сводку не идёт, если не задан SCHA_NOTIFY_REPORT_ONLY=1.
    Шлём, когда в работе не осталось справок, или когда самый старый итог ждёт дольше DIGEST_MAX_WAIT_H."""
    with eng.begin() as conn:
        busy = conn.execute(text(
            "SELECT count(*) FROM scha_jobs WHERE status IN ('new','text_ready','agent_wait','agent_done')")).scalar()
        rows = conn.execute(text("""
            SELECT j.id, j.ticker, j.period, j.url, j.status, j.agent, j.last_error, j.updated_at,
                   f.name AS fund_name, f.uk_id,
                   NOT EXISTS (SELECT 1 FROM fund_holdings_history h JOIN funds f2 ON f2.fund_id = h.fund_id
                               WHERE f2.ticker = j.ticker AND h.source = ANY(:s)
                                 AND h.snapshot_date < CAST(j.period || '-01' AS date)) AS first_data
            FROM scha_jobs j LEFT JOIN funds f ON f.ticker = j.ticker
            WHERE j.notified_at IS NULL AND j.status IN ('imported', 'report_only', 'doubt', 'failed', 'no_securities')
            ORDER BY j.period, f.name
        """), {"s": list(MONTHLY_SOURCES)}).all()
        oldest_h = conn.execute(text("""
            SELECT extract(epoch FROM now() - min(updated_at)) / 3600 FROM scha_jobs
            WHERE notified_at IS NULL AND status IN ('imported', 'report_only', 'doubt', 'failed', 'no_securities')
        """)).scalar()
    if not rows or (busy and (oldest_h or 0) < DIGEST_MAX_WAIT_H):
        return
    show_report = os.environ.get("SCHA_NOTIFY_REPORT_ONLY", "0") == "1"
    by_period: dict[str, list[str]] = {}
    for r in rows:
        name = f"{fund_icon(r.ticker, r.uk_id)} {esc(r.fund_name or r.ticker)}".strip()
        if r.first_data:
            name += " 🆕"
        a = r.agent or {}
        n = len([x for x in a.get("rows") or [] if isinstance(x, dict) and x.get("isin")])
        tot = sum(_num(x.get("value_rub")) for x in a.get("rows") or [] if isinstance(x, dict))
        g = _num(a.get("gross_assets_rub"))
        stats = f"{n} бумаг, {num_ru(tot / g * 100, 1)} %" if g else f"{n} бумаг"
        if r.status == "imported":
            line = f"✅ {name} — {stats}"
        elif r.status == "no_securities":
            line = f"▫️ {name} — бумаг нет (только деньги и РЕПО), заливать нечего"
        elif r.status == "report_only":
            if not show_report:
                continue
            line = f"🟢 {name} — прочитано, {stats} (режим отчёта, в базу не писали)"
        else:
            why = esc((r.last_error or "без пояснения")[:220])
            line = f"❌ {name} — не залито: {why} · {link(r.url)}"
        by_period.setdefault(r.period, []).append(line)
    parts = [f"<b>Справки СЧА · {month_ru(per)}</b>\n" + "\n".join(lines) for per, lines in by_period.items()]
    if len(rows) > DIGEST_COMPACT_FROM:
        parts = [_compact_digest(rows)]
    if os.environ.get("SCHA_DIGEST_DRY") == "1":   # посмотреть сводку, не отправляя и не отмечая
        print("\n\n".join(parts) or "(сообщения не будет)")
        return
    if parts:
        bad = any(r.status in ("doubt", "failed") for r in rows)
        notify("\n\n".join(parts), silent=not bad)
    with eng.begin() as conn:
        conn.execute(text("UPDATE scha_jobs SET notified_at = now() WHERE id = ANY(:ids)"),
                     {"ids": [r.id for r in rows]})
    log(f"сводка: {len(rows)} справок" + ("" if parts else " (без сообщения: только режим отчёта)"))


def _compact_digest(rows) -> str:
    """Пачка истории (сотни справок): по фонду — сколько месяцев залито и за какой период;
    незалитые — отдельной строкой каждая, с причиной и ссылкой."""
    def mm(per: str) -> str:
        return f"{per[5:7]}.{per[:4]}"
    ok: dict[str, list[str]] = {}
    empty: dict[str, list[str]] = {}
    names: dict[str, str] = {}
    bad: list[str] = []
    for r in rows:
        names[r.ticker] = f"{fund_icon(r.ticker, r.uk_id)} {esc(r.fund_name or r.ticker)}".strip()
        if r.status == "imported":
            ok.setdefault(r.ticker, []).append(r.period)
        elif r.status == "no_securities":
            empty.setdefault(r.ticker, []).append(r.period)
        elif r.status in ("doubt", "failed"):
            why = esc((r.last_error or "без пояснения")[:160])
            bad.append(f"❌ {names[r.ticker]} · {mm(r.period)} — {why} · {link(r.url)}")
    lines = [f"✅ {names[tk]} — {len(ps)} мес. ({mm(min(ps))}–{mm(max(ps))})" if len(ps) > 1
             else f"✅ {names[tk]} — {mm(ps[0])}" for tk, ps in sorted(ok.items(), key=lambda x: names[x[0]])]
    lines += [f"▫️ {names[tk]} — бумаг нет (деньги и РЕПО): {len(ps)} мес. ({mm(min(ps))}–{mm(max(ps))})" if len(ps) > 1
              else f"▫️ {names[tk]} — бумаг нет (деньги и РЕПО): {mm(ps[0])}" for tk, ps in sorted(empty.items())]
    head = f"<b>Справки СЧА · залито {sum(len(v) for v in ok.values())}" + (f", не залито {len(bad)}" if bad else "") + "</b>"
    return "\n".join([head, *lines, *bad])


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


# ── итог месяца по типу фондов ────────────────────────────────────────────
# Вадим 18.10: «пусть приходит, что все фонды такого-то типа за такой-то месяц загружены;
# и если что-то отвалилось — что именно и почему». Сводка пачки (digest) говорит о каждой
# справке, а здесь — о месяце целиком: ✅ когда собраны все фонды типа, ⏳ если к LAG_DAYS
# кого-то нет (молчание сканера тоже видно: фонд просто окажется в списке «нет»).

CATEGORY_TITLE = {"stocks": "Фонды акций", "bonds": "Облигационные фонды"}
LAG_DAYS = (20, 28)        # дни месяца, когда напоминаем про недостающих за прошлый месяц
LAG_DRY = os.environ.get("SCHA_STATUS_DRY") == "1"   # посмотреть тексты, не отправляя и не отмечая


def _prev_period(today: date, back: int = 1) -> str:
    y, m = today.year, today.month - back
    while m < 1:
        y, m = y - 1, m + 12
    return f"{y}-{m:02d}"


def _site_down(conn, url: str) -> bool:
    """Страница раскрытия УК не открывалась последние двое суток (ключ из _scan)."""
    site = urllib.parse.urlsplit(url).hostname if "://" in url else None
    if not site:
        return False
    return bool(conn.execute(text(
        "SELECT 1 FROM scha_notify_log WHERE key = :k AND sent_at > now() - interval '2 days'"),
        {"k": f"scha:site:{site}"}).first())


def _month_status(conn, period: str) -> dict[str, list[dict]]:
    """Фонды конвейера (col.SOURCES) по типу: загружен ли месяц и если нет — почему."""
    start = f"{period}-01"
    funds = conn.execute(text("""
        SELECT f.ticker, f.name, f.uk_id, coalesce(f.category, 'other') AS category,
               EXISTS (SELECT 1 FROM fund_holdings_history h
                       WHERE h.fund_id = f.fund_id AND h.source = ANY(:s)
                         AND h.snapshot_date >= CAST(:d AS date)
                         AND h.snapshot_date < CAST(:d AS date) + interval '1 month') AS loaded,
               (SELECT to_char(max(h.snapshot_date), 'YYYY-MM') FROM fund_holdings_history h
                       WHERE h.fund_id = f.fund_id AND h.source = ANY(:s)) AS last_period
        FROM funds f WHERE f.ticker = ANY(:t)
    """), {"s": list(MONTHLY_SOURCES), "d": start, "t": list(col.SOURCES)}).mappings().all()
    jobs = {r.ticker: r for r in conn.execute(text("""
        SELECT DISTINCT ON (ticker) ticker, status, last_error, url, notified_at
        FROM scha_jobs WHERE period = :p ORDER BY ticker, updated_at DESC
    """), {"p": period}).all()}
    out: dict[str, list[dict]] = {}
    for f in funds:
        j = jobs.get(f["ticker"])
        st = dict(f, job=j, done=f["loaded"] or (j is not None and j.status == "no_securities"))
        if not st["done"]:
            if j is not None and j.status in ("new", "text_ready", "agent_wait", "agent_done"):
                st["why"] = "⏳ справка найдена, агент читает"
            elif j is not None and j.status in ("doubt", "failed"):
                st["why"] = f"❌ не залито: {esc((j.last_error or 'без пояснения')[:160])} · {link(j.url)}"
            elif j is not None and j.status == "report_only":
                st["why"] = "🟢 прочитано, но не залито (режим отчёта)"
            elif _site_down(conn, col.SOURCES[f["ticker"]][1]):
                st["why"] = "⚠️ страница раскрытия УК не открывается"
            else:
                last = month_ru(f["last_period"]) if f["last_period"] else "нет"
                st["why"] = f"на сайте УК справки ещё нет (в базе последняя: {last})"
        out.setdefault(f["category"], []).append(st)
    return out


def month_status(eng, today: date | None = None) -> None:
    """✅ «все фонды типа за месяц загружены» — один раз; ⏳ напоминание LAG_DAYS — один раз в каждый
    такой день. Месяц — прошлый календарный; в первые 10 дней ещё и позапрошлый (поздние УК)."""
    today = today or date.today()
    periods = [_prev_period(today)] + ([_prev_period(today, 2)] if today.day <= 10 else [])
    lag_day = max((d for d in LAG_DAYS if today.day >= d), default=None)
    with eng.begin() as conn:
        for period in periods:
            for cat, funds in sorted(_month_status(conn, period).items()):
                title = f"{CATEGORY_TITLE.get(cat, 'Фонды · ' + cat)} · {month_ru(period)}"
                missing = [f for f in funds if not f["done"]]
                if not missing:
                    # итог — после сводки пачки, а не раньше неё
                    if any(f["job"] is not None and f["job"].notified_at is None for f in funds):
                        continue
                    msg, key = (f"✅ <b>{title}</b>\nЗагружены все {len(funds)} фондов",
                                f"scha:complete:{cat}:{period}")
                elif lag_day and period == periods[0]:
                    lines = [f"{fund_icon(f['ticker'], f['uk_id'])} {esc(f['name'] or f['ticker'])} — {f['why']}".strip()
                             for f in sorted(missing, key=lambda x: x["name"] or x["ticker"])]
                    msg = (f"⏳ <b>{title}</b> — загружено {len(funds) - len(missing)} из {len(funds)}\n"
                           + "\n".join(lines))
                    key = f"scha:lag:{cat}:{period}:{lag_day}"
                else:
                    continue
                if LAG_DRY:
                    print(f"[{key}]\n{msg}\n")
                    continue
                notify_first(conn, key, msg)


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
    digest(eng)
    try:
        month_status(eng)
    except Exception as e:  # noqa: BLE001 — итог месяца не должен ронять конвейер
        log(f"итог месяца: {e}")
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd not in ("scan", "work"):
        print("usage: python -m signals.scha_pipeline scan|work")
        sys.exit(2)
    sys.exit(scan() if cmd == "scan" else work())
