"""
Feature engineering — все индикаторы для одного актива на данную дату.

Port из research/explore_v2.py — но прибранный, типизированный, production-ready.
Используется ensemble-детектором.

Главная функция: `compute_features(sectype, chart_fiz, chart_yur, as_of_date)`.
Возвращает dict ~25 features которые потом скармливаются в scoring.compute_score().
"""
from __future__ import annotations
import math
from datetime import date, datetime
from statistics import fmean, stdev
from typing import Optional

# ───────────────────────────────────────────────────────────────
# Constants (синхронизированы с research/explore_v2.py)
# ───────────────────────────────────────────────────────────────
WARMUP_DAYS = 60       # минимум для EMA50 + 20-day correlations
EMA_SHORT = 20
EMA_LONG = 50
CHANNEL_LOOKBACK = 20
HIGH_LOW_30 = 30
HIGH_LOW_90 = 90
ATR_PERIOD = 14
OI_CORR_WINDOW = 20
NET_CHANGE_LOOKBACK = 5


# ───────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────
def _ema(values: list[float], period: int) -> list[float]:
    """Стандартный EMA. Первое значение = SMA(period); дальше α = 2/(period+1)."""
    if len(values) < period:
        return []
    alpha = 2.0 / (period + 1)
    out = [fmean(values[:period])]
    for v in values[period:]:
        out.append(out[-1] + alpha * (v - out[-1]))
    return out


def _correlation(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation. 0 если degenerate."""
    n = len(xs)
    if n < 3 or n != len(ys):
        return 0.0
    mx, my = fmean(xs), fmean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return 0.0
    return cov / (sx * sy)


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or b is None:
        return default
    return a / b


def _pct_change(prev: Optional[float], cur: Optional[float]) -> float:
    if prev is None or cur is None or prev == 0:
        return 0.0
    return ((cur - prev) / abs(prev)) * 100


def _parse_date(iso: str) -> date:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).date()


def _index_by_date(items: list[dict]) -> dict[date, dict]:
    return {_parse_date(it["time"]): it for it in items if "time" in it}


# ───────────────────────────────────────────────────────────────
# Main entry
# ───────────────────────────────────────────────────────────────
def compute_features(
    sectype: str,
    chart_fiz: dict,
    chart_yur: dict,
    as_of_date: Optional[date] = None,
) -> Optional[dict]:
    """Расчёт всех features на дату `as_of_date` (или последнюю в данных).

    Args:
        sectype: тикер
        chart_fiz: response API /api/chart/{sectype}?clgroup=FIZ (с candles + open_interest)
        chart_yur: то же для clgroup=YUR
        as_of_date: дата для расчёта (None = последняя доступная)

    Returns:
        dict со всеми features, или None если данных мало (warm-up < 60 дней).
    """
    candles_by_date = _index_by_date(chart_fiz.get("candles", []))
    fiz_by_date = _index_by_date(chart_fiz.get("open_interest", []))
    yur_by_date = _index_by_date(chart_yur.get("open_interest", []))

    all_dates = sorted(candles_by_date.keys())
    if not all_dates:
        return None

    # Find index for as_of_date
    if as_of_date is None:
        i = len(all_dates) - 1
    else:
        # Find last date <= as_of_date
        i = None
        for idx, d in enumerate(all_dates):
            if d <= as_of_date:
                i = idx
            else:
                break
        if i is None:
            return None

    if i < WARMUP_DAYS:
        return None

    # Arrays
    opens = [float(candles_by_date[d].get("open") or 0) for d in all_dates]
    highs = [float(candles_by_date[d].get("high") or 0) for d in all_dates]
    lows = [float(candles_by_date[d].get("low") or 0) for d in all_dates]
    closes = [float(candles_by_date[d]["close"]) for d in all_dates]
    volumes = [float(candles_by_date[d].get("volume") or 0) for d in all_dates]
    fiz_data = [fiz_by_date.get(d) for d in all_dates]
    yur_data = [yur_by_date.get(d) for d in all_dates]

    close = closes[i]
    if close <= 0:
        return None

    closes_sofar = closes[: i + 1]

    # ── Trend (EMAs) ──
    ema20_list = _ema(closes_sofar, EMA_SHORT)
    ema50_list = _ema(closes_sofar, EMA_LONG)
    if not ema20_list or not ema50_list:
        return None
    ema20, ema50 = ema20_list[-1], ema50_list[-1]

    # ── Extrema ──
    win30 = closes_sofar[-HIGH_LOW_30:]
    win90 = closes_sofar[-HIGH_LOW_90:] if len(closes_sofar) >= HIGH_LOW_90 else closes_sofar
    is_new_high_30 = close >= max(win30)
    is_new_low_30 = close <= min(win30)
    is_new_high_90 = close >= max(win90)
    is_new_low_90 = close <= min(win90)

    # ── Channel position (Donchian 20d) ──
    high20 = max(closes_sofar[-CHANNEL_LOOKBACK:])
    low20 = min(closes_sofar[-CHANNEL_LOOKBACK:])
    channel_pos = _safe_div(close - low20, high20 - low20, 0.5)

    # ── Price-EMA distance ──
    price_vs_ema20 = _safe_div(close - ema20, ema20, 0.0) * 100
    price_vs_ema50 = _safe_div(close - ema50, ema50, 0.0) * 100

    # ── Daily range / ATR ──
    high_today = highs[i] or close
    low_today = lows[i] or close
    daily_range_pct = _safe_div(high_today - low_today, close, 0.0) * 100

    tr_values = []
    for j in range(max(0, i - ATR_PERIOD), i + 1):
        h = highs[j] or closes[j]
        l = lows[j] or closes[j]
        pc = closes[j - 1] if j > 0 else closes[j]
        tr_values.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr_14 = fmean(tr_values) if tr_values else 0
    atr_pct = _safe_div(atr_14, close, 0.0) * 100

    # ── OI helpers ──
    def _get_oi(arr: list, idx: int, key: str) -> Optional[float]:
        if idx < 0 or idx >= len(arr) or arr[idx] is None:
            return None
        v = arr[idx].get(key)
        return float(v) if v is not None else None

    # FIZ
    fiz_net = _get_oi(fiz_data, i, "net_position")
    fiz_net_5d = _get_oi(fiz_data, i - NET_CHANGE_LOOKBACK, "net_position")
    fiz_net_change_5d = _pct_change(fiz_net_5d, fiz_net)

    fiz_long = _get_oi(fiz_data, i, "pos_long")
    fiz_short = _get_oi(fiz_data, i, "pos_short")
    fiz_long_num = _get_oi(fiz_data, i, "pos_long_num")
    fiz_short_num = _get_oi(fiz_data, i, "pos_short_num")

    fiz_avg_long_size = _safe_div(fiz_long or 0, fiz_long_num or 1, 0.0)
    fiz_avg_short_size = _safe_div(abs(fiz_short or 0), fiz_short_num or 1, 0.0)
    fiz_total = (fiz_long or 0) + abs(fiz_short or 0)
    fiz_long_share = _safe_div(fiz_long or 0, fiz_total, 0.5)

    # YUR
    yur_net = _get_oi(yur_data, i, "net_position")
    yur_net_5d = _get_oi(yur_data, i - NET_CHANGE_LOOKBACK, "net_position")
    yur_net_change_5d = _pct_change(yur_net_5d, yur_net)

    yur_long_num = _get_oi(yur_data, i, "pos_long_num")
    yur_short_num = _get_oi(yur_data, i, "pos_short_num")
    yur_part_balance = _safe_div(yur_long_num or 0, yur_short_num or 1, 1.0)

    # ── OI-price correlation 20d ──
    closes_20 = closes_sofar[-OI_CORR_WINDOW:]
    fiz_20 = [_get_oi(fiz_data, j, "net_position") for j in range(i - OI_CORR_WINDOW + 1, i + 1)]
    fiz_20_clean = [v for v in fiz_20 if v is not None]
    if len(fiz_20_clean) == OI_CORR_WINDOW:
        oi_price_corr_20d = _correlation(closes_20, fiz_20_clean)
    else:
        oi_price_corr_20d = 0.0

    # ── Volume z-score ──
    vols_window = [v for v in volumes[i - 20 : i] if v > 0]
    if len(vols_window) >= 10:
        v_mean = fmean(vols_window)
        v_std = stdev(vols_window) if len(vols_window) >= 2 else 0
        v_now = volumes[i]
        volume_z = _safe_div(v_now - v_mean, v_std, 0.0)
    else:
        volume_z = 0.0

    return {
        # Core
        "tradedate": all_dates[i],
        "close": close,
        # Trend
        "ema20": ema20,
        "ema50": ema50,
        "ema20_above_ema50": ema20 > ema50,
        "price_vs_ema20_pct": price_vs_ema20,
        "price_vs_ema50_pct": price_vs_ema50,
        # Extrema (bools)
        "is_new_high_30": is_new_high_30,
        "is_new_low_30": is_new_low_30,
        "is_new_high_90": is_new_high_90,
        "is_new_low_90": is_new_low_90,
        # Channel
        "channel_pos": channel_pos,
        # Volatility
        "daily_range_pct": daily_range_pct,
        "atr_pct": atr_pct,
        # OI flows
        "fiz_net": fiz_net,
        "fiz_net_change_5d": fiz_net_change_5d,
        "yur_net": yur_net,
        "yur_net_change_5d": yur_net_change_5d,
        # OI structure
        "fiz_avg_long_size": fiz_avg_long_size,
        "fiz_avg_short_size": fiz_avg_short_size,
        "fiz_long_share": fiz_long_share,
        "yur_part_balance": yur_part_balance,
        # OI dynamics
        "oi_price_corr_20d": oi_price_corr_20d,
        # Volume
        "volume_z": volume_z,
    }
