"""
Лента раскрытия FinanceMarker → disclosure_events → кандидаты в посты (source='fm_disclosure').

⚠️ ПОЧЕМУ САЙТ, А НЕ API. API /fm/v2/disclosure отдаёт «кто, что, когда» без текста
сообщения; сам текст («2. Содержание сообщения…») есть только на странице
financemarker.ru/disclosure/ — в серверном состоянии Nuxt (window.__NUXT__), 30
записей на страницу, ?offset=N для истории. Страница публичная: ни токена, ни
суточной квоты 400 не тратит. Состояние — JS-выражение (функция с аргументами, не
JSON), поэтому его исполняет quickjs (pip: quickjs, стоит в signals/.venv с 06.09.2026).

⚠️ ХАЙП-ФИЛЬТР (ШАГ Н) ЗДЕСЬ НЕ НУЖЕН: он меряет разгон репостов в Telegram, а
раскрытие — не пост. Кандидат вставляется с hype_filter_result = TRUE, и Шаг А
(судья релевантности) берёт его как обычно по status='candidate'.

Кандидатом становится не всё: только компании из нашего справочника (issuers с
тикером smart-lab, 118 штук) и категории REPORT / DIVIDEND / INSIDER_TRANSACTION /
OPERATION; EVENT — лишь если в заголовке есть дивиденды, выкуп, реорганизация,
листинг, допэмиссия. «Проведение заседания совета директоров» без этого — шум
(семь из тридцати записей ленты), оно остаётся в disclosure_events без кандидата.

Крон (хост):  */15 * * * * /opt/frame/signals/fm_disclosure_scan.sh >> /opt/frame/logs/fm_disclosure_scan.log 2>&1
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
load_dotenv(os.path.join(_ROOT, ".env"))
_db = os.environ.get("DB_URL", "")
if "@db:" in _db:
    os.environ["DB_URL"] = _db.replace("@db:", "@127.0.0.1:")

import pipeline_heartbeat                  # noqa: E402
from api.database import SessionLocal      # noqa: E402

URL = "https://financemarker.ru/disclosure/"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
ТАЙМАУТ = (10, 40)
ПОВТОРОВ = 3
МАКС_ОПИСАНИЕ = 3500          # знаков в raw_text кандидата: у длинных сообщений сотни строк регуляторики

КАТЕГОРИЯ_RU = {
    "REPORT": "отчётность", "DIVIDEND": "дивиденды", "INSIDER_TRANSACTION": "сделка инсайдера",
    "OPERATION": "операционные результаты", "EVENT": "событие",
}
КАНДИДАТ_ВСЕГДА = {"REPORT", "DIVIDEND", "INSIDER_TRANSACTION", "OPERATION"}
СОБЫТИЕ_ВАЖНОЕ = re.compile(
    r"дивиденд|выкуп|buyback|реорганиз|листинг|делистинг|конверта|дополнительн\w* (выпуск|эмисс)|"
    r"размещени|оферт|поглощен|присоединен|разделен|выделен|крупн\w* сделк|банкрот|санац",
    re.I,
)

_UPSERT_EVENT = text("""
    INSERT INTO disclosure_events
        (fm_id, code, secid, event_date, category, type, period, year, month, title, link, source,
         name, description, dir_link, dividend_status, transaction_type, updated_at)
    VALUES
        (:fm_id, :code, :secid, CAST(:event_date AS date), :category, :type, :period, :year, :month, :title, :link,
         'financemarker', :name, :description, :dir_link, :dividend_status, :transaction_type, now())
    ON CONFLICT (fm_id) WHERE fm_id IS NOT NULL DO UPDATE SET
        description = COALESCE(EXCLUDED.description, disclosure_events.description),
        dir_link = COALESCE(NULLIF(EXCLUDED.dir_link, ''), disclosure_events.dir_link),
        dividend_status = COALESCE(EXCLUDED.dividend_status, disclosure_events.dividend_status),
        updated_at = now()
    RETURNING id, candidate_id, (xmax = 0) AS inserted
""")
_INSERT_CANDIDATE = text("""
    INSERT INTO content_candidates
        (status, source, headline, raw_text, tickers, source_url, hype_filter_result, created_at, updated_at)
    VALUES
        ('candidate', 'fm_disclosure', :headline, :raw_text, ARRAY[:ticker]::text[], :source_url, TRUE, now(), now())
    RETURNING id
""")
_LINK_CANDIDATE = text("UPDATE disclosure_events SET candidate_id = :cid WHERE id = :eid")
_SELECT_UNIVERSE = text("SELECT smartlab_ticker FROM issuers WHERE smartlab_ticker IS NOT NULL")


def fetch(offset: int = 0) -> str:
    последняя = None
    for попытка in range(ПОВТОРОВ):
        try:
            r = requests.get(URL, params={"offset": offset} if offset else None,
                             headers={"User-Agent": UA}, timeout=ТАЙМАУТ)
            r.raise_for_status()
            return r.text
        except (requests.ConnectionError, requests.Timeout) as e:
            последняя = e
            time.sleep(2 * (попытка + 1))
    raise последняя


def parse(page_html: str) -> list[dict]:
    """window.__NUXT__ — JS-выражение; исполняем в quickjs и ищем массив записей ленты."""
    import quickjs
    i = page_html.find("window.__NUXT__=")
    if i < 0:
        raise ValueError("на странице нет window.__NUXT__ — вёрстка FM изменилась")
    j = page_html.find("</script>", i)
    js = page_html[i + len("window.__NUXT__="):j].strip().rstrip(";")
    state = json.loads(quickjs.Context().eval("JSON.stringify(" + js + ")"))

    def найти(o):
        if isinstance(o, list):
            if o and isinstance(o[0], dict) and "description" in o[0] and "category" in o[0]:
                return o
            for x in o:
                r = найти(x)
                if r:
                    return r
        elif isinstance(o, dict):
            for v in o.values():
                r = найти(v)
                if r:
                    return r
        return None
    items = найти(state)
    if items is None:
        raise ValueError("в состоянии страницы нет массива записей с description/category")
    return items


def _кандидат_нужен(r: dict) -> bool:
    if r["category"] in КАНДИДАТ_ВСЕГДА:
        return True
    return bool(r["category"] == "EVENT" and СОБЫТИЕ_ВАЖНОЕ.search(r.get("title") or ""))


def _текст(r: dict) -> str:
    описание = (r.get("description") or "").strip()
    # «2. Содержание сообщения2.1. …» — пункты склеены без пробелов; ставим переносы.
    описание = re.sub(r"(?<=[^\s])(?=\d+\.\d+\. )", "\n", описание)
    if len(описание) > МАКС_ОПИСАНИЕ:
        описание = описание[:МАКС_ОПИСАНИЕ].rsplit(" ", 1)[0] + "…"
    return f"{r['title']}\n\n{описание}" if описание else r["title"]


def run_once(offsets, since: date, dry_run: bool = False) -> dict:
    итог = {"страниц": 0, "записей": 0, "новых": 0, "кандидатов": 0, "ошибок": 0, "пропущено_чужие": 0, "карточки": []}
    db = SessionLocal()
    try:
        вселенная = {r[0] for r in db.execute(_SELECT_UNIVERSE).all()}
        for offset in offsets:
            try:
                записи = parse(fetch(offset))
            except Exception as e:  # noqa: BLE001
                итог["ошибок"] += 1
                print(f"[fm_disclosure_scan] offset={offset}: {type(e).__name__}: {e}")
                continue
            итог["страниц"] += 1
            for r in записи:
                итог["записей"] += 1
                код = (r.get("code") or "").strip()
                наш = код in вселенная
                if dry_run:
                    print(f"  {r['date']} {r['category']:<20} {код:<6} {'✓' if наш else ' '} {r['title'][:70]}"
                          f" | описание {len(r.get('description') or '')} зн.")
                    continue
                row = db.execute(_UPSERT_EVENT, {
                    "fm_id": int(r["id"]), "code": код, "secid": код if наш else None,
                    "event_date": r["date"], "category": r["category"],
                    "type": (r.get("type") if r.get("type") not in (None, "NA") else None),
                    "period": None, "year": None, "month": None,
                    "title": r["title"], "link": r.get("link") or None, "name": r.get("name"),
                    "description": r.get("description") or None, "dir_link": r.get("dir_link") or None,
                    "dividend_status": r.get("dividend_status"), "transaction_type": r.get("transaction_type"),
                }).mappings().first()
                if row["inserted"]:
                    итог["новых"] += 1
                    if наш and r["category"] in ("REPORT", "OPERATION") and код not in итог["карточки"]:
                        итог["карточки"].append(код)
                if not наш:
                    итог["пропущено_чужие"] += 1
                    continue
                if row["candidate_id"] or not _кандидат_нужен(r):
                    continue
                if date.fromisoformat(r["date"]) < since:
                    continue           # первый прогон: не заваливать Шаг А прошлой неделей
                cid = db.execute(_INSERT_CANDIDATE, {
                    "headline": f"{r.get('name') or код} ({код}) · {КАТЕГОРИЯ_RU.get(r['category'], r['category'])}: {r['title']}",
                    "raw_text": _текст(r),
                    "ticker": код,
                    "source_url": r.get("dir_link") or r.get("link") or f"{URL}#fm{r['id']}",
                }).scalar()
                db.execute(_LINK_CANDIDATE, {"cid": cid, "eid": row["id"]})
                итог["кандидатов"] += 1
                print(f"[fm_disclosure_scan] кандидат #{cid}: {код} {r['category']} — {r['title'][:80]}")
            db.commit()
    except Exception as e:  # noqa: BLE001
        db.rollback()
        итог["ошибок"] += 1
        print(f"[fm_disclosure_scan] сбой: {type(e).__name__}: {e}")
    finally:
        db.close()
    if итог["карточки"] and not dry_run:
        обновить_карточки(итог["карточки"])
    return итог


def обновить_карточки(коды: list[str]) -> None:
    """Вышел отчёт — карточка этой компании нужна сейчас, а не в вечернем обходе.
    Фетчер живёт в контейнере оркестратора (там токен FM и учёт квоты); зовём его
    через docker exec с явным списком компаний. Бюджет он проверяет сам."""
    коды = коды[:10]
    try:
        контейнер = subprocess.run(["docker", "ps", "-q", "-f", "label=com.docker.compose.service=orchestrator"],
                                   capture_output=True, text=True, timeout=20).stdout.split()
        if not контейнер:
            print("[fm_disclosure_scan] карточки: контейнер оркестратора не найден")
            return
        r = subprocess.run(["docker", "exec", "-w", "/app/Company", контейнер[0], "python", "fetch_fm_cards.py",
                            "--once", "--secid", *коды], capture_output=True, text=True, timeout=600)
        хвост = (r.stdout.strip().splitlines() or [""])[-1][:300]
        print(f"[fm_disclosure_scan] карточки по отчёту {', '.join(коды)}: код {r.returncode} · {хвост}")
        if r.returncode != 0:
            print((r.stderr or "")[-600:])
        # Карточка обновлена — в company_documents появились ссылки на новые PDF: качаем и
        # раскладываем их сразу (хост, signals/.venv), чтобы бриф по отчёту читал сам документ.
        for код in коды:
            d = subprocess.run([os.path.join(_ROOT, "signals", "fetch_documents.sh"), "--secid", код, "--since-days", "3"],
                               capture_output=True, text=True, timeout=900)
            print(f"[fm_disclosure_scan] документы {код}: код {d.returncode} · {(d.stdout.strip().splitlines() or [''])[-1][:200]}")
    except Exception as e:  # noqa: BLE001 — карточка подождёт вечернего обхода, лента важнее
        print(f"[fm_disclosure_scan] карточки: {type(e).__name__}: {e}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=1, help="сколько страниц по 30 записей (обычно 1)")
    ap.add_argument("--since-days", type=int, default=2, help="кандидаты только по событиям не старше N дней")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    offsets = [i * 30 for i in range(a.pages)]
    since = date.today() - timedelta(days=a.since_days)
    итог = run_once(offsets, since, a.dry_run)
    print(f"[fm_disclosure_scan] итог: {итог}")
    if not a.dry_run:
        pipeline_heartbeat.record_pipeline_run(
            "fm_disclosure_scan", success=итог["страниц"] > 0,
            note=f"записей {итог['записей']}, новых {итог['новых']}, кандидатов {итог['кандидатов']}",
            degraded=итог["ошибок"] > 0 and итог["страниц"] > 0)


if __name__ == "__main__":
    main()
