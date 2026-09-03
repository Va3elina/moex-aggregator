#!/usr/bin/env python3
"""
Загрузка карточки компании из JSON парсера в БД (миграция 069).

Ключевое — SCD-2: значение за 2022 год не меняется никогда, поэтому ежедневный
снимок писать нельзя (145 тыс. строк в день). Новая версия строки появляется только
когда значение ИЗМЕНИЛОСЬ, иначе двигается last_seen. Побочный эффект полезный:
по first_seen видно, когда smart-lab переписал цифру задним числом.

Использование:
    python parse_smartlab_company.py SBER --out sber.json
    python load_company_card.py sber.json --issuer SBER
    python load_company_card.py sber.json --issuer SBER --dry-run
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

_root = Path(__file__).resolve()
for _p in _root.parents:
    if (_p / ".env").exists():
        load_dotenv(_p / ".env")
        break
DB_URL = os.getenv("DB_URL")  # в контейнере уже в окружении

QUARTER_END = {"1": (3, 31), "2": (6, 30), "3": (9, 30), "4": (12, 31)}

LOG_DIR = Path(__file__).resolve().parent / "logs"
log = logging.getLogger("company_card_loader")


def setup_logging():
    """Полный лог + отдельный лог ошибок — как у фетчеров проекта."""
    LOG_DIR.mkdir(exist_ok=True)
    day = date.today().strftime("%Y%m")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    for path, level in ((LOG_DIR / f"loader_{day}.log", logging.DEBUG),
                        (LOG_DIR / f"loader_errors_{day}.log", logging.WARNING)):
        h = logging.FileHandler(path, encoding="utf-8")
        h.setLevel(level); h.setFormatter(fmt); log.addHandler(h)
    c = logging.StreamHandler(sys.stderr)
    c.setLevel(logging.INFO); c.setFormatter(fmt); log.addHandler(c)


def period_end(period_type: str, label: str):
    if period_type == "year" and re.fullmatch(r"\d{4}", label):
        return date(int(label), 12, 31)
    if period_type == "quarter":
        m = re.fullmatch(r"(\d{4})Q([1-4])", label)
        if m:
            mm, dd = QUARTER_END[m.group(2)]
            return date(int(m.group(1)), mm, dd)
    return None  # LTM — скользящее окно, календарного конца нет


def as_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


def resolve(conn, issuer_key: str):
    row = conn.execute(text(
        "SELECT issuer_id FROM issuers WHERE issuer_key = :k"), {"k": issuer_key}).fetchone()
    if not row:
        sys.exit(f"эмитент {issuer_key} не заведён — сначала сид в миграции 069")
    secs = conn.execute(text(
        "SELECT share_class, secid FROM issuer_securities WHERE issuer_id = :i"),
        {"i": row[0]}).fetchall()
    return row[0], {c: s for c, s in secs}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("json_file", type=Path)
    ap.add_argument("--issuer", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    setup_logging()
    card = json.loads(a.json_file.read_text(encoding="utf-8"))
    log.info("загрузка %s из %s (снято %s)", a.issuer, a.json_file.name,
             card.get("captured_at"))
    standard = card.get("standard", "MSFO")
    today = date.today()
    engine = create_engine(DB_URL)
    stats = {"metrics_new": 0, "metrics_touched": 0, "metrics_changed": 0,
             "metrics_skipped_no_secid": 0, "metrics_ref": 0,
             "dividends": 0, "shareholders": 0, "theses": 0, "documents": 0}

    with engine.begin() as conn:
        issuer_id, by_class = resolve(conn, a.issuer)

        # --- справочник показателей: наполняется тем, что реально пришло
        seen = {}
        for m in card["metrics"]:
            seen.setdefault(m["metric_code"], (m["label"].split(",")[0].strip(), m["unit"]))
        for code, (label, unit) in seen.items():
            conn.execute(text("""
                INSERT INTO metrics_ref (metric_code, label_ru, unit)
                VALUES (:c, :l, :u)
                ON CONFLICT (metric_code) DO UPDATE
                   SET label_ru = EXCLUDED.label_ru, unit = COALESCE(EXCLUDED.unit, metrics_ref.unit),
                       last_seen = CURRENT_DATE
            """), {"c": code, "l": label, "u": unit})
            stats["metrics_ref"] += 1

        rdates = {("year", k): as_date(v) for k, v in card.get("report_dates_year", {}).items()}
        rdates.update({("quarter", k): as_date(v)
                       for k, v in card.get("report_dates_quarter", {}).items()})

        # --- метрики, SCD-2
        # ⚠️ LTM приходит ДВАЖДЫ: и с годовой страницы, и с квартальной. Значения там
        # совпадают, но если разойдутся — без дедупа каждая загрузка плодила бы новую
        # версию строки вечно. Побеждает годовая страница (идёт первой).
        deduped, seen_keys = [], set()
        for m in card["metrics"]:
            k = (m["share_class"], m["metric_code"], m["period_type"],
                 "LTM" if m["period"] is None else m["period"])
            if k in seen_keys:
                continue
            seen_keys.add(k)
            deduped.append(m)

        for m in deduped:
            secid = by_class.get(m["share_class"])
            if not secid:
                # у эмитента нет бумаги этого класса — например преф-строки у компании
                # без префа. Молча терять нельзя, поэтому считаем.
                stats["metrics_skipped_no_secid"] += 1
                continue
            label = "LTM" if m["period"] is None else m["period"]
            pend = period_end(m["period_type"], label)
            rd = rdates.get((m["period_type"], label)) or rdates.get(("year", label))

            cur = conn.execute(text("""
                SELECT id, value, note FROM company_metrics
                WHERE secid=:s AND metric_code=:c AND standard=:st
                  AND period_type=:pt AND period_label=:pl
                ORDER BY first_seen DESC LIMIT 1
            """), {"s": secid, "c": m["metric_code"], "st": standard,
                   "pt": m["period_type"], "pl": label}).fetchone()

            same = (cur is not None
                    and (cur[1] is None) == (m["value"] is None)
                    and (cur[1] is None or abs(float(cur[1]) - m["value"]) < 1e-9)
                    and cur[2] == m["note"])
            if same:
                conn.execute(text("UPDATE company_metrics SET last_seen=:d WHERE id=:i"),
                             {"d": today, "i": cur[0]})
                stats["metrics_touched"] += 1
                continue

            conn.execute(text("""
                INSERT INTO company_metrics
                    (secid, metric_code, standard, period_type, period_label, period_end,
                     value, note, raw_text, report_date, source, first_seen, last_seen)
                VALUES (:s,:c,:st,:pt,:pl,:pe,:v,:n,:raw,:rd,'smartlab',:d,:d)
                ON CONFLICT (secid, metric_code, standard, period_type, period_label, first_seen)
                DO UPDATE SET value=EXCLUDED.value, note=EXCLUDED.note,
                              raw_text=EXCLUDED.raw_text, last_seen=EXCLUDED.last_seen
            """), {"s": secid, "c": m["metric_code"], "st": standard,
                   "pt": m["period_type"], "pl": label, "pe": pend,
                   "v": m["value"], "n": m["note"], "raw": (m["raw"] or "")[:40],
                   "rd": rd, "d": today})
            if cur:
                # Значение переписали задним числом — это событие, а не рутина.
                log.info("ревизия: %s %s %s %s: %s → %s",
                         secid, m["metric_code"], m["period_type"], label, cur[1], m["value"])
                stats["metrics_changed"] += 1
            else:
                stats["metrics_new"] += 1

        # --- дивиденды
        for d in card.get("dividend_payments", []):
            rd = as_date(d["record_date"])
            if not rd or d["secid"] not in by_class.values():
                continue
            conn.execute(text("""
                INSERT INTO company_dividends (secid, period, record_date, dividend, price, div_yield)
                VALUES (:s,:p,:rd,:d,:px,:y)
                ON CONFLICT (secid, record_date, period) DO UPDATE
                   SET dividend=EXCLUDED.dividend, price=EXCLUDED.price,
                       div_yield=EXCLUDED.div_yield, updated_at=now()
            """), {"s": d["secid"], "p": d["period"], "rd": rd,
                   "d": d["dividend"], "px": d["price"], "y": d["div_yield"]})
            stats["dividends"] += 1

        # --- акционеры (структура целиком привязана к своей дате)
        as_of = as_date(card.get("shareholders_as_of"))
        for h in card.get("shareholders", []):
            conn.execute(text("""
                INSERT INTO company_shareholders (issuer_id, holder, share_pct, structure_as_of)
                VALUES (:i,:h,:p,:d)
                ON CONFLICT (issuer_id, holder, structure_as_of) DO UPDATE
                   SET share_pct=EXCLUDED.share_pct, updated_at=now()
            """), {"i": issuer_id, "h": h["holder"], "p": h["share_pct"], "d": as_of})
            stats["shareholders"] += 1

        # --- тезисы
        vis = f"{len(card.get('factors', []))}/{card.get('factors_total')}"
        for f in card.get("factors", []):
            conn.execute(text("""
                INSERT INTO company_theses (issuer_id, direction, statement, stated_date, visible_of_total)
                VALUES (:i,:d,:s,:sd,:v)
                ON CONFLICT (issuer_id, direction, statement) DO UPDATE
                   SET stated_date=EXCLUDED.stated_date,
                       visible_of_total=EXCLUDED.visible_of_total, updated_at=now()
            """), {"i": issuer_id, "d": f["direction"], "s": f["statement"],
                   "sd": as_date(f["stated_date"]), "v": vis})
            stats["theses"] += 1

        # --- документы: только ссылки
        for doc in card.get("documents", []):
            url = doc["url"]
            if not url.startswith("http"):
                continue  # внутренние ссылки smart-lab (/q/SBER/f/l/) — не документы
            conn.execute(text("""
                INSERT INTO company_documents (issuer_id, doc_type, period, url)
                VALUES (:i,:t,:p,:u) ON CONFLICT DO NOTHING
            """), {"i": issuer_id, "t": doc["doc_type"], "p": doc["period"], "u": url})
            stats["documents"] += 1

        if a.dry_run:
            conn.rollback()
            log.info("DRY-RUN, откат")

    if stats["metrics_skipped_no_secid"]:
        # Строки префа у эмитента без префа — норма. Но если счётчик вдруг большой,
        # значит бумага не заведена в issuer_securities, и данные молча теряются.
        log.warning("пропущено строк без бумаги нужного класса: %d",
                    stats["metrics_skipped_no_secid"])
    if stats["metrics_new"] == 0 and stats["metrics_touched"] == 0:
        log.error("не загружено ни одной метрики — проверьте JSON и сид эмитента")
    log.info("итог: %s", json.dumps(stats, ensure_ascii=False))
    print(json.dumps(stats, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
