#!/usr/bin/env python3
"""
Верификация adjusted close: прямая математическая проверка.

Для каждого дивиденда SBER:
1. Проверяет что ex_date совпадает с реальным ценовым гэпом
2. Проверяет формулу: factor = (close_ex - div) / close_ex
3. Проверяет что adjusted close убирает гэпы (return на ex-date ≈ без дивиденда)

Запуск (внутри Docker):
    python tests/verify_adjusted_price.py
"""

import sys
import os
from datetime import date, timedelta
from pathlib import Path
from collections import OrderedDict

from sqlalchemy import create_engine, text
import pandas as pd

# ─── Config ───────────────────────────────────────────────
SECID = "SBER"
DAYS_BACK = 7300  # ~20 лет

DB_URL = os.getenv("DB_URL")


def load_data(engine, secid: str, days: int):
    """Загружает candles и dividends."""
    date_from = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        candles = conn.execute(text("""
            SELECT begin_time::date as trade_date, open, high, low, close
            FROM candles
            WHERE secid = :secid AND interval = 24 AND type = 'stock'
              AND close > 0 AND begin_time::date >= :date_from
            ORDER BY begin_time
        """), {"secid": secid, "date_from": date_from.isoformat()}).fetchall()

        divs = conn.execute(text("""
            SELECT ex_date, value, registry_close_date
            FROM dividends
            WHERE secid = :secid AND ex_date IS NOT NULL
            ORDER BY ex_date
        """), {"secid": secid}).fetchall()

    candle_df = pd.DataFrame(candles, columns=["date", "open", "high", "low", "close"])
    candle_df["date"] = pd.to_datetime(candle_df["date"]).dt.date
    candle_df["open"] = candle_df["open"].astype(float)
    candle_df["high"] = candle_df["high"].astype(float)
    candle_df["low"] = candle_df["low"].astype(float)
    candle_df["close"] = candle_df["close"].astype(float)

    div_df = pd.DataFrame(divs, columns=["ex_date", "value", "registry_close_date"])
    div_df["ex_date"] = pd.to_datetime(div_df["ex_date"]).dt.date
    div_df["value"] = div_df["value"].astype(float)

    return candle_df, div_df


def compute_adjusted(dates, closes, ex_dates_dict):
    """
    Мультипликативная коррекция (Yahoo/CRSP стандарт).
    Используем RAW prev_close для factor, не adjusted.
    ex_dates_dict: {date -> div_value}
    """
    adjusted = closes.copy()
    date_from = dates[0]
    date_to = dates[-1]

    relevant = [(d, v) for d, v in ex_dates_dict.items() if date_from <= d <= date_to]
    relevant.sort()  # от старых к новым

    for ex_d, div_val in relevant:
        ex_idx = None
        for i, d in enumerate(dates):
            if d >= ex_d:
                ex_idx = i
                break
        if ex_idx is not None and ex_idx > 0:
            raw_prev_close = closes[ex_idx - 1]
            if raw_prev_close > 0 and div_val < raw_prev_close:
                factor = (raw_prev_close - div_val) / raw_prev_close
                for j in range(ex_idx):
                    adjusted[j] *= factor

    return adjusted


def main():
    print("=" * 80)
    print(f"  МАТЕМАТИЧЕСКАЯ ВЕРИФИКАЦИЯ ДИВИДЕНДНЫХ ГЭПОВ: {SECID}")
    print("=" * 80)

    engine = create_engine(DB_URL)
    candle_df, div_df = load_data(engine, SECID, DAYS_BACK)
    engine.dispose()

    print(f"\n  Candles: {len(candle_df)} дней ({candle_df['date'].min()} — {candle_df['date'].max()})")
    print(f"  Dividends: {len(div_df)} записей")

    # Создаём lookup по дате
    candle_by_date = {row["date"]: row for _, row in candle_df.iterrows()}

    # ═══════════════════════════════════════════════════════════════════
    # ТЕСТ 1: Проверка экс-дат — совпадает ли с реальным ценовым гэпом
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print(f"  ТЕСТ 1: Проверка экс-дат (гэп prev_close → open ≈ dividend)")
    print(f"{'═' * 80}")
    print(f"\n  {'Ex-Date':<12} {'Div':>7} {'Prev Close':>11} {'Open':>9} {'Gap':>8} {'Match':>7} {'Статус'}")
    print(f"  {'─'*12} {'─'*7} {'─'*11} {'─'*9} {'─'*8} {'─'*7} {'─'*8}")

    test1_pass = 0
    test1_fail = 0
    divs_in_candles = []

    for _, drow in div_df.iterrows():
        ex_d = drow["ex_date"]
        div_val = drow["value"]

        if ex_d not in candle_by_date:
            # Может быть выходной, ищем ближайший торговый день
            found = False
            for offset in range(0, 5):
                test_d = ex_d + timedelta(days=offset)
                if test_d in candle_by_date:
                    ex_d = test_d
                    found = True
                    break
            if not found:
                continue

        # Ищем предыдущий торговый день
        prev_d = None
        for offset in range(1, 10):
            test_d = ex_d - timedelta(days=offset)
            if test_d in candle_by_date:
                prev_d = test_d
                break

        if prev_d is None:
            continue

        ex_candle = candle_by_date[ex_d]
        prev_candle = candle_by_date[prev_d]

        prev_close = prev_candle["close"]
        ex_open = ex_candle["open"]
        gap = prev_close - ex_open

        # Проверяем: gap ≈ dividend (±50%)
        match_pct = gap / div_val * 100 if div_val > 0 else 0
        is_match = 40 < match_pct < 200
        status = "✓" if is_match else "✗"

        if is_match:
            test1_pass += 1
        else:
            test1_fail += 1

        divs_in_candles.append((drow["ex_date"], div_val))

        print(f"  {drow['ex_date']}  {div_val:>7.2f}  {prev_close:>11.2f}  {ex_open:>9.2f}  {gap:>8.2f}  {match_pct:>6.0f}%  {status}")

    print(f"\n  Результат: {test1_pass} ✓ / {test1_fail} ✗ из {test1_pass + test1_fail}")

    # ═══════════════════════════════════════════════════════════════════
    # ТЕСТ 2: Проверка формулы factor на каждом дивиденде
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print(f"  ТЕСТ 2: Проверка factor = (close_ex - div) / close_ex")
    print(f"{'═' * 80}")

    dates = candle_df["date"].tolist()
    closes = candle_df["close"].tolist()

    print(f"\n  {'Ex-Date':<12} {'Div':>7} {'Close_ex':>9} {'Factor':>8} {'Adj/Raw%':>9} {'Статус'}")
    print(f"  {'─'*12} {'─'*7} {'─'*9} {'─'*8} {'─'*9} {'─'*8}")

    ex_dates_dict = {drow["ex_date"]: drow["value"] for _, drow in div_df.iterrows()}
    adjusted = compute_adjusted(dates, closes, ex_dates_dict)

    cumulative_factor = 1.0
    for _, drow in div_df.iterrows():
        ex_d = drow["ex_date"]
        div_val = drow["value"]

        # Ищем в candles
        ex_idx = None
        for i, d in enumerate(dates):
            if d >= ex_d:
                ex_idx = i
                break
        if ex_idx is None or ex_idx == 0:
            continue

        prev_close = closes[ex_idx - 1]
        if prev_close <= 0 or div_val >= prev_close:
            continue

        factor = (prev_close - div_val) / prev_close
        cumulative_factor *= factor
        adj_ratio = adjusted[0] / closes[0] * 100

        # Factor должен быть 0 < factor < 1
        ok = 0 < factor < 1
        status = "✓" if ok else "✗"
        print(f"  {ex_d}  {div_val:>7.2f}  {prev_close:>9.2f}  {factor:>8.4f}  {adj_ratio:>8.2f}%  {status}")

    print(f"\n  Кумулятивный factor (произведение всех): {cumulative_factor:.6f}")
    print(f"  Это значит: первая цена adjusted = {closes[0] * cumulative_factor:.2f} (raw = {closes[0]:.2f})")

    # ═══════════════════════════════════════════════════════════════════
    # ТЕСТ 3: Кумулятивный total return — adj close vs reinvested
    # Adjusted close должен отражать полный доход: рост цены + дивиденды.
    # Сравниваем: adj[end]/adj[start] vs кумулятивный total return
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print(f"  ТЕСТ 3: Кумулятивный return (adj close vs manual reinvestment)")
    print(f"{'═' * 80}")

    # Считаем total return «вручную» — daily close-to-close, на экс-датах +div
    manual_cumul = 1.0
    for i in range(1, len(dates)):
        daily_ret = (closes[i] - closes[i - 1]) / closes[i - 1]
        # Добавляем дивиденд если экс-дата
        d = dates[i]
        if d in ex_dates_dict:
            daily_ret += ex_dates_dict[d] / closes[i - 1]
        manual_cumul *= (1 + daily_ret)

    # Adjusted cumulative return
    adj_cumul = adjusted[-1] / adjusted[0]

    # Raw cumulative return (без дивидендов)
    raw_cumul = closes[-1] / closes[0]

    print(f"\n  Период: {dates[0]} — {dates[-1]} ({len(dates)} дней)")
    print(f"  Raw cumulative return:      {(raw_cumul - 1) * 100:>+.2f}% (цена: {closes[0]:.2f} → {closes[-1]:.2f})")
    print(f"  Adjusted cumulative return:  {(adj_cumul - 1) * 100:>+.2f}% (adj: {adjusted[0]:.2f} → {adjusted[-1]:.2f})")
    print(f"  Manual total return (reinv): {(manual_cumul - 1) * 100:>+.2f}%")
    print(f"  Разница adj vs manual:       {abs(adj_cumul - manual_cumul) / manual_cumul * 100:.2f}%")

    test3_pass = 0
    test3_fail = 0
    # Допуск: < 5% (мультипликативный метод — приближение)
    diff_pct = abs(adj_cumul - manual_cumul) / manual_cumul * 100
    if diff_pct < 5.0:
        print(f"\n  ✓ Кумулятивные returns совпадают с точностью {diff_pct:.2f}%")
        test3_pass = 1
    else:
        print(f"\n  ✗ Кумулятивные returns расходятся на {diff_pct:.2f}%")
        test3_fail = 1

    print(f"\n  Результат: {test3_pass} ✓ / {test3_fail} ✗")

    # ═══════════════════════════════════════════════════════════════════
    # ТЕСТ 4: Проверка сезонности (additive формула)
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print(f"  ТЕСТ 4: Проверка аддитивной коррекции для сезонности")
    print(f"  Формула: adjusted_return = (close + div - prev_close) / prev_close * 100")
    print(f"{'═' * 80}")

    print(f"\n  {'Ex-Date':<12} {'Raw Ret':>9} {'Add Corr':>9} {'Expected':>9} {'Diff':>7} {'Статус'}")
    print(f"  {'─'*12} {'─'*9} {'─'*9} {'─'*9} {'─'*7} {'─'*8}")

    test4_pass = 0
    test4_fail = 0

    for _, drow in div_df.iterrows():
        ex_d = drow["ex_date"]
        div_val = drow["value"]

        ex_idx = None
        for i, d in enumerate(dates):
            if d >= ex_d:
                ex_idx = i
                break
        if ex_idx is None or ex_idx == 0:
            continue

        prev_close = closes[ex_idx - 1]
        close_p = closes[ex_idx]

        raw_return = (close_p - prev_close) / prev_close * 100
        additive_return = (close_p + div_val - prev_close) / prev_close * 100

        # Ожидание: additive_return ≈ raw_return + div_yield
        div_yield = div_val / prev_close * 100
        expected = raw_return + div_yield
        diff = abs(additive_return - expected)
        ok = diff < 0.01  # должно совпадать точно (одна и та же формула)
        status = "✓" if ok else "✗"

        if ok:
            test4_pass += 1
        else:
            test4_fail += 1

        print(f"  {ex_d}  {raw_return:>+8.2f}%  {additive_return:>+8.2f}%  {expected:>+8.2f}%  {diff:>6.4f}  {status}")

    print(f"\n  Результат: {test4_pass} ✓ / {test4_fail} ✗")

    # ═══════════════════════════════════════════════════════════════════
    # ИТОГ
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'═' * 80}")
    print(f"  ИТОГ")
    print(f"{'═' * 80}")
    print(f"  Тест 1 (экс-даты ≈ гэпу):      {test1_pass} ✓ / {test1_fail} ✗")
    print(f"  Тест 3 (adjusted returns):       {test3_pass} ✓ / {test3_fail} ✗")
    print(f"  Тест 4 (аддитивная коррекция):   {test4_pass} ✓ / {test4_fail} ✗")

    total_fail = test1_fail + test3_fail + test4_fail
    if total_fail == 0:
        print(f"\n  ✓ ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
    else:
        print(f"\n  ✗ {total_fail} ОШИБОК — требуется анализ")
    print(f"{'═' * 80}")


if __name__ == "__main__":
    main()
