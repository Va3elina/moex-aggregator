#!/usr/bin/env python3
"""
Верификация сезонности: ручной расчёт vs API.

Для SBER:
1. Скачивает candles + dividends из БД
2. Считает weekday/monthly сезонность вручную (raw + adjusted)
3. Сравнивает с API /api/seasonality
4. Печатает расхождения

Запуск (внутри Docker):
    python tests/verify_seasonality.py
"""

import os
import json
import subprocess
from datetime import date, timedelta
from collections import defaultdict

from sqlalchemy import create_engine, text

SECID = "SBER"
DB_URL = os.getenv("DB_URL")
API_BASE = "http://localhost:8000"


def load_data(engine, secid):
    with engine.connect() as conn:
        candles = conn.execute(text("""
            SELECT begin_time::date as trade_date, close
            FROM candles
            WHERE secid = :secid AND interval = 24 AND type = 'stock'
              AND close > 0
              AND EXTRACT(ISODOW FROM begin_time) BETWEEN 1 AND 5
            ORDER BY begin_time
        """), {"secid": secid}).fetchall()

        divs = conn.execute(text("""
            SELECT ex_date, value
            FROM dividends
            WHERE secid = :secid AND ex_date IS NOT NULL
        """), {"secid": secid}).fetchall()

    dates = [r[0] for r in candles]
    closes = [float(r[1]) for r in candles]
    ex_dates = {r[0]: float(r[1]) for r in divs}
    return dates, closes, ex_dates


def compute_adjusted(dates, closes, ex_dates):
    """Мультипликативная коррекция (Yahoo/CRSP)."""
    adjusted = closes.copy()
    relevant = [(d, v) for d, v in ex_dates.items() if dates[0] <= d <= dates[-1]]
    relevant.sort()

    for ex_d, div_val in relevant:
        ex_idx = None
        for i, d in enumerate(dates):
            if d >= ex_d:
                ex_idx = i
                break
        if ex_idx is not None and ex_idx > 0:
            raw_prev = closes[ex_idx - 1]
            if raw_prev > 0 and div_val < raw_prev:
                factor = (raw_prev - div_val) / raw_prev
                for j in range(ex_idx):
                    adjusted[j] *= factor
    return adjusted


def compute_seasonality(dates, prices, mode, iterations):
    """Ручной расчёт сезонности."""
    if mode == "weekday":
        get_key = lambda d: d.isoweekday()  # 1=Mon, 5=Fri
        get_iter = lambda d: d.isocalendar()[1]  # week number approximation
        # Iterations = last N weeks
        iter_keys = sorted(set((d.year, d.isocalendar()[1]) for d in dates), reverse=True)[:iterations]
        iter_set = set(iter_keys)
        in_range = lambda d: (d.year, d.isocalendar()[1]) in iter_set
    elif mode == "monthly":
        get_key = lambda d: d.month
        # Iterations = last N years
        iter_keys = sorted(set(d.year for d in dates), reverse=True)[:iterations]
        iter_set = set(iter_keys)
        in_range = lambda d: d.year in iter_set
    elif mode == "monthday":
        get_key = lambda d: d.day
        # Iterations = last N months
        iter_keys = sorted(set((d.year, d.month) for d in dates), reverse=True)[:iterations]
        iter_set = set(iter_keys)
        in_range = lambda d: (d.year, d.month) in iter_set
    else:
        raise ValueError(f"Unknown mode: {mode}")

    groups = defaultdict(list)
    for i in range(1, len(dates)):
        if not in_range(dates[i]):
            continue
        prev = prices[i - 1]
        if prev <= 0:
            continue
        ret = (prices[i] - prev) / prev * 100
        groups[get_key(dates[i])].append(ret)

    bars = []
    for key in sorted(groups.keys()):
        vals = groups[key]
        bars.append({
            "key": key,
            "avg_change": round(sum(vals) / len(vals), 4) if vals else 0,
            "count": len(vals),
        })
    return bars


def fetch_api(secid, mode, iterations, exclude_dividends):
    url = f"{API_BASE}/api/seasonality?secid={secid}&mode={mode}&iterations={iterations}&exclude_dividends={str(exclude_dividends).lower()}"
    r = subprocess.run(["curl", "-s", url], capture_output=True, text=True)
    return json.loads(r.stdout)


def compare(label, manual_bars, api_bars, tolerance=0.05):
    """Сравнивает ручной расчёт с API. Tolerance в % (абс)."""
    print(f"\n  {'Key':>5} {'Manual':>10} {'API':>10} {'Diff':>8} {'Cnt M':>6} {'Cnt A':>6} {'Status'}")
    print(f"  {'─'*5} {'─'*10} {'─'*10} {'─'*8} {'─'*6} {'─'*6} {'─'*6}")

    api_map = {b["key"]: b for b in api_bars}
    ok = 0
    fail = 0

    for mb in manual_bars:
        ab = api_map.get(mb["key"])
        if not ab:
            print(f"  {mb['key']:>5} {mb['avg_change']:>10.4f} {'MISSING':>10} {'':>8} {mb['count']:>6} {'':>6} ✗")
            fail += 1
            continue

        diff = abs(mb["avg_change"] - ab["avg_change"])
        status = "✓" if diff < tolerance else "✗"
        if diff >= tolerance:
            fail += 1
        else:
            ok += 1

        print(f"  {mb['key']:>5} {mb['avg_change']:>10.4f} {ab['avg_change']:>10.4f} {diff:>8.4f} {mb['count']:>6} {ab['count']:>6} {status}")

    return ok, fail


def main():
    print("=" * 70)
    print(f"  ВЕРИФИКАЦИЯ СЕЗОННОСТИ: {SECID}")
    print("=" * 70)

    engine = create_engine(DB_URL)
    dates, closes, ex_dates = load_data(engine, SECID)
    engine.dispose()

    print(f"  Candles: {len(dates)} дней ({dates[0]} — {dates[-1]})")
    print(f"  Dividends: {len(ex_dates)} экс-дат")

    adjusted = compute_adjusted(dates, closes, ex_dates)

    total_ok = 0
    total_fail = 0

    # ═══════════ WEEKDAY ═══════════
    for mode, iterations in [("weekday", 90), ("monthly", 30), ("monthday", 30)]:
        print(f"\n{'═' * 70}")
        print(f"  {mode.upper()} (iterations={iterations})")
        print(f"{'═' * 70}")

        # Raw returns (exclude_dividends=False)
        print(f"\n  [RAW — exclude_dividends=False]")
        manual_raw = compute_seasonality(dates, closes, mode, iterations)
        api_raw = fetch_api(SECID, mode, iterations, False)
        ok, fail = compare("raw", manual_raw, api_raw["bars"])
        print(f"  Результат: {ok} ✓ / {fail} ✗")
        total_ok += ok
        total_fail += fail

        # Adjusted returns (exclude_dividends=True)
        print(f"\n  [ADJUSTED — exclude_dividends=True]")
        manual_adj = compute_seasonality(dates, adjusted, mode, iterations)
        api_adj = fetch_api(SECID, mode, iterations, True)
        ok, fail = compare("adj", manual_adj, api_adj["bars"])
        print(f"  Результат: {ok} ✓ / {fail} ✗")
        total_ok += ok
        total_fail += fail

    # ═══════════ ИТОГ ═══════════
    print(f"\n{'═' * 70}")
    print(f"  ИТОГ: {total_ok} ✓ / {total_fail} ✗")
    if total_fail == 0:
        print(f"  ✓ ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
    else:
        print(f"  ✗ ЕСТЬ РАСХОЖДЕНИЯ")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
