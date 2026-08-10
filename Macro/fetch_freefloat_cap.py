#!/usr/bin/env python3
"""
Free-float капитализация акций по месяцам — знаменатель режима «От капитализации»
в «Потоках по компании» (/fund-trades, ручка /company-weights).

Источник — состав индекса МосБиржи широкого рынка (MOEXBMI) через ISS:
  - /statistics/.../index/analytics/MOEXBMI?date=D — СПИСОК бумаг корзины на дату D;
  - /statistics/.../index/analytics/MOEXBMI/tickers/{T}?date=D — детали бумаги:
    cap_total (полная капа на дату, руб) и ff_factor (официальный коэффициент
    free-float МосБиржи).

FF-капа бумаги = cap_total × ff_factor.

НЕЛЬЗЯ считать «вес% × CAPITALIZATION корзины»: в весах сидят ограничивающие
коэффициенты w_factor (у Сбера 0.28 — вес порезан вчетверо), т.е. произведение
даёт FF-капу × w_factor, и у каждой бумаги искажение своё. Проверено на живых
данных 2026-08-07: у SBER выходило 12.7% free-float против официальных 48%.

cap_total биржа считает по ИСТОРИЧЕСКОМУ размеру выпуска — допэмиссии/байбэки
учтены, в отличие от stock_market_cap с текущим ISSUESIZE на всю глубину.
Точность ff_factor — 2 знака (0.48) → ошибка FF-капы ~±1%.

Хранение: freefloat_cap(sec_id, month=1-е число, as_of=реальная дата среза,
ffcap руб). Точка месяца — ПОСЛЕДНЯЯ дата месяца, на которую ISS отдаёт
analytics (совпадает с последним торговым днём).

Запуск:
  python Macro/fetch_freefloat_cap.py --once           # догнать все месяцы с 2021-09
  python Macro/fetch_freefloat_cap.py --once --force   # + пересчитать текущий месяц
  python Macro/fetch_freefloat_cap.py --check          # покрытие по месяцам

--once идемпотентен и self-healing: заполняет только отсутствующие месяцы
(последние даты уже записанных месяцев не меняются задним числом), плюс всегда
освежает текущий месяц. Оркестратор гоняет его в дневном цикле 19:10.
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).parent
PROJECT_DIR = BASE_DIR.parent
load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL")

# Дальше истории составов фондов не смотрим (FT_HISTORY_FLOOR в fund_trades.py).
BACKFILL_FROM = date(2021, 9, 1)

INDEX_ID = "MOEXBMI"
ISS_ANALYTICS = ("https://iss.moex.com/iss/statistics/engines/stock/markets/index/"
                 "analytics/{idx}.json?iss.meta=off&date={d}&limit=100&start={start}")
ISS_TICKER = ("https://iss.moex.com/iss/statistics/engines/stock/markets/index/"
              "analytics/{idx}/tickers/{t}.json?iss.meta=off&date={d}")
_UA = {"User-Agent": "Mozilla/5.0 (compatible; FrameBot/1.0)"}
ISS_PAUSE = 0.15         # сек между запросами — не душить ISS
DATE_PROBE_BACK = 15     # сколько дней назад от конца месяца ищем дату среза

# Санити: FF-капа корзины широкого рынка (Σ cap_total×ff) — единицы-десятки трлн.
BASKET_MIN_RUB = 2e12
BASKET_MAX_RUB = 300e12
# Минимум бумаг с валидными cap_total/ff_factor, чтобы месяц считался полным.
MIN_TICKERS = 60

log = logging.getLogger("freefloat_cap")


def setup_logging():
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def ensure_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS freefloat_cap (
                id SERIAL PRIMARY KEY,
                sec_id VARCHAR(40) NOT NULL,
                month DATE NOT NULL,           -- 1-е число месяца
                as_of DATE NOT NULL,           -- реальная дата среза analytics
                ffcap NUMERIC(24, 2) NOT NULL, -- free-float капитализация, руб
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sec_id, month)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_freefloat_cap_month
            ON freefloat_cap(month, sec_id)
        """))


def _iss_json(url: str) -> dict:
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def fetch_tickers(d: date) -> list:
    """Список бумаг корзины MOEXBMI на дату d. Пусто = не торговый день."""
    out: list = []
    start = 0
    while True:
        data = _iss_json(ISS_ANALYTICS.format(idx=INDEX_ID, d=d.isoformat(), start=start))
        cols = data["analytics"]["columns"]
        rows = data["analytics"]["data"]
        if not rows:
            break
        ti = cols.index("ticker")
        out.extend(row[ti] for row in rows if row[ti])
        cur = data.get("analytics.cursor", {}).get("data")
        if cur:
            idx_pos, total, pagesize = cur[0][0], cur[0][1], cur[0][2]
            if idx_pos + pagesize >= total:
                break
            start = idx_pos + pagesize
        else:
            break
        time.sleep(ISS_PAUSE)
    return out


def fetch_ticker_ffcap(t: str, d: date):
    """FF-капа бумаги t на дату d: cap_total × ff_factor, руб. None = нет данных."""
    data = _iss_json(ISS_TICKER.format(idx=INDEX_ID, t=t, d=d.isoformat()))
    cols = data["ticker"]["columns"]
    rows = data["ticker"]["data"]
    if not rows:
        return None
    ci, fi = cols.index("cap_total"), cols.index("ff_factor")
    cap, ff = rows[0][ci], rows[0][fi]
    if not cap or not ff or float(cap) <= 0 or float(ff) <= 0:
        return None
    return float(cap) * float(ff)


def month_end(m: date) -> date:
    nxt = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
    return nxt - timedelta(days=1)


def month_iter(lo: date, hi: date):
    m = date(lo.year, lo.month, 1)
    while m <= hi:
        yield m
        m = date(m.year + (m.month == 12), m.month % 12 + 1, 1)


def load_month(engine, m: date) -> int:
    """Найти последний срез месяца m, посчитать FF-капы, перезаписать месяц.

    Возвращает число записанных бумаг (0 = данных нет / не прошли санити)."""
    probe = min(month_end(m), date.today())
    tickers: list = []
    as_of = None
    for _ in range(DATE_PROBE_BACK):
        if probe < m:
            break
        tickers = fetch_tickers(probe)
        time.sleep(ISS_PAUSE)
        if tickers:
            as_of = probe
            break
        probe -= timedelta(days=1)
    if not tickers or as_of is None:
        log.warning(f"  {m:%Y-%m}: нет analytics в пределах {DATE_PROBE_BACK} дней от конца месяца")
        return 0

    # Детали по каждой бумаге корзины: cap_total × ff_factor.
    ffcaps: dict = {}
    for t in tickers:
        try:
            v = fetch_ticker_ffcap(t, as_of)
        except Exception as e:
            log.warning(f"  {m:%Y-%m}: {t} — ошибка ISS ({e}), пропуск бумаги")
            v = None
        if v is not None:
            ffcaps[t] = v
        time.sleep(ISS_PAUSE)

    if len(ffcaps) < MIN_TICKERS:
        log.error(f"  {m:%Y-%m}: валидных бумаг {len(ffcaps)} < {MIN_TICKERS} — пропуск месяца")
        return 0
    basket = sum(ffcaps.values())
    if not (BASKET_MIN_RUB <= basket <= BASKET_MAX_RUB):
        log.error(f"  {m:%Y-%m}: Σ FF-кап {basket:.3e} вне санити-диапазона — пропуск")
        return 0

    # Перезапись месяца целиком: состав корзины меняется, и upsert без DELETE
    # оставлял бы бумаги, выбывшие из индекса между прогонами текущего месяца.
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM freefloat_cap WHERE month = :m"), {"m": m})
        for secid, v in ffcaps.items():
            conn.execute(text("""
                INSERT INTO freefloat_cap (sec_id, month, as_of, ffcap)
                VALUES (:s, :m, :d, :c)
            """), {"s": secid, "m": m, "d": as_of, "c": round(v, 2)})
    log.info(f"  {m:%Y-%m}: {len(ffcaps)}/{len(tickers)} бумаг, срез {as_of}, "
             f"Σ FF-кап {basket / 1e12:.2f} трлн ₽")
    return len(ffcaps)


def run_once(engine, force: bool = False) -> int:
    """Заполнить отсутствующие месяцы с BACKFILL_FROM + освежить текущий."""
    today = date.today()
    cur_month = date(today.year, today.month, 1)
    with engine.connect() as conn:
        have = {r[0] for r in conn.execute(text(
            "SELECT DISTINCT month FROM freefloat_cap"
        )).fetchall()}

    done = 0
    for m in month_iter(BACKFILL_FROM, cur_month):
        if m in have and m != cur_month and not force:
            continue
        if m == cur_month and m in have and not force:
            # Текущий месяц освежаем, только если появился более свежий срез.
            with engine.connect() as conn:
                last = conn.execute(text(
                    "SELECT MAX(as_of) FROM freefloat_cap WHERE month = :m"
                ), {"m": m}).scalar()
            probe = min(month_end(m), today)
            if last is not None and last >= probe:
                continue
        if load_month(engine, m) > 0:
            done += 1
    return done


def print_check(engine):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT month, COUNT(*), MAX(as_of), SUM(ffcap)
            FROM freefloat_cap GROUP BY month ORDER BY month
        """)).fetchall()
    if not rows:
        log.warning("Таблица freefloat_cap пуста")
        return
    log.info(f"Месяцев: {len(rows)} ({rows[0][0]:%Y-%m} — {rows[-1][0]:%Y-%m})")
    for m, n, as_of, total in rows:
        log.info(f"  {m:%Y-%m}: {n:3d} бумаг, срез {as_of}, Σ {float(total) / 1e12:.2f} трлн ₽")


def main():
    parser = argparse.ArgumentParser(description="Free-float cap monthly (MOEXBMI analytics)")
    parser.add_argument("--once", action="store_true", help="Догнать пропущенные месяцы + текущий")
    parser.add_argument("--force", action="store_true", help="Пересчитать ВСЕ месяцы заново")
    parser.add_argument("--check", action="store_true", help="Покрытие по месяцам")
    args = parser.parse_args()

    setup_logging()
    engine = get_engine()
    ensure_table(engine)

    if args.check:
        print_check(engine)
    elif args.once or args.force:
        n = run_once(engine, force=args.force)
        log.info(f"Готово: обновлено месяцев — {n}")
    else:
        parser.print_help()
    engine.dispose()


if __name__ == "__main__":
    main()
