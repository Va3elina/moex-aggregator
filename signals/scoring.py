"""
Type-aware scoring v3 — две модели по результатам R&D phase 3.

Универсальная часть (`universal_signals`) применяется ко всем активам.
Plus специфическая часть:
  - meanrev (Акции, Индексы, Сырьё/энерго, Сырьё/soft):
    extremes → откат; oversold → отскок; high vol → bearish
  - momentum (Валюта, Сырьё/metal):
    extremes → продолжение тренда; low vol → trend persists

Логика и веса основаны на per-class diagnostics (см. research/diagnostics_by_class.py
и research/explore_v3.py). Все правила проверены train/test split.

Используется ensemble-детектором — main entry compute_score(features, mode).
"""
from __future__ import annotations
from typing import Optional


# ───────────────────────────────────────────────────────────────
# Universal signals — features которые работают одинаково во всех классах
# ───────────────────────────────────────────────────────────────
def universal_signals(f: dict) -> float:
    """Универсальный вклад в score — features робастные по всем группам.

    Основано на diagnostics_v2 + bool features check на 65 активах.
    `is_new_low_90` — самый сильный single feature (universal +2.59% diff).
    `oi_price_corr_20d`, `fiz_avg_long_size` — устойчивы по группам.
    """
    s = 0.0

    # Extrema (validated universal positive direction)
    if f.get("is_new_low_90"):    s += 2.0   # bottom-fishing alpha (+2.59% on raw)
    if f.get("is_new_high_90"):   s += 1.0
    if f.get("is_new_high_30") and not f.get("is_new_high_90"):
        s += 0.5  # weaker breakout (not yet 90d high)
    if f.get("is_new_low_30") and not f.get("is_new_low_90"):
        s += 0.3  # mild bounce setup

    # Position sizing — крупные лонги ассоциированы с rising prices (t=+3.23)
    avg_size = f.get("fiz_avg_long_size") or 0
    if avg_size > 100:  s += 0.3
    if avg_size > 200:  s += 0.3

    # OI flows — contrarian retail / smart institutional
    fiz_5d = f.get("fiz_net_change_5d") or 0
    yur_5d = f.get("yur_net_change_5d") or 0
    if fiz_5d > 30:   s -= 0.3   # retail piled long → mean revert bearish
    if fiz_5d < -30:  s += 0.3
    if yur_5d > 20:   s += 0.3   # smart money in → bullish
    if yur_5d < -20:  s -= 0.3

    return s


# ───────────────────────────────────────────────────────────────
# Mean-reversion mode (equity / index / soft / energy)
# ───────────────────────────────────────────────────────────────
def meanrev_score(f: dict) -> float:
    """Для активов где цена откатывается к норме.

    Per diagnostics:
      price_vs_ema50_pct: t=-5.4 (equity), -5.5 (soft), -4.0 (index), -4.7 (energy)
      → выше EMA → bearish, ниже EMA → bullish
      channel_pos: t≈-0.12 (equity) — слабый сигнал
      oi_price_corr_20d: t=+3.46 (equity), +2.24 (metal), -7.56 (energy outlier)
    """
    s = universal_signals(f)

    # Price vs EMA50 — главное: extremes mean-revert
    p_ema50 = f.get("price_vs_ema50_pct") or 0.0
    if p_ema50 > 15:    s -= 2.0       # сильно overstretched → ожидаем откат
    elif p_ema50 > 10:  s -= 1.5
    elif p_ema50 > 5:   s -= 0.5
    if p_ema50 < -15:   s += 2.5       # сильно oversold → bounce
    elif p_ema50 < -10: s += 2.0
    elif p_ema50 < -5:  s += 0.5

    # Channel position
    cp = f.get("channel_pos")
    if cp is not None:
        if cp > 0.90:  s -= 0.5        # at top → revert
        if cp < 0.10:  s += 1.0        # at bottom → bounce

    # OI-price correlation
    corr = f.get("oi_price_corr_20d") or 0
    if corr > 0.3:   s += 0.5
    elif corr < -0.3: s -= 0.3

    return round(s, 2)


# ───────────────────────────────────────────────────────────────
# Momentum mode (fx_rub / fx_cross / metal)
# ───────────────────────────────────────────────────────────────
def momentum_score(f: dict) -> float:
    """Для активов где тренд продолжается.

    Per diagnostics:
      price_vs_ema50_pct: t=+4.12 (fx_rub), +1.67 (metal)
      → выше EMA → bullish (тренд)
      channel_pos: t=+6.90 (fx_rub), +3.54 (metal) — сильный
      atr_pct: t=-13.73 (fx_rub) — STRONGEST для FX (low vol → bullish)
      oi_price_corr_20d: t=+4.34 (fx_rub), +2.24 (metal)
    """
    s = universal_signals(f)

    # Price vs EMA — выше тренда → momentum continues
    p_ema50 = f.get("price_vs_ema50_pct") or 0.0
    if p_ema50 > 15:    s += 2.0
    elif p_ema50 > 10:  s += 1.5
    elif p_ema50 > 5:   s += 0.5
    if p_ema50 < -15:   s -= 1.5
    elif p_ema50 < -10: s -= 1.0
    elif p_ema50 < -5:  s -= 0.3

    # Channel position — at top → continues up
    cp = f.get("channel_pos")
    if cp is not None:
        if cp > 0.90:  s += 1.0
        if cp < 0.10:  s -= 0.5

    # ATR — critical для FX: low vol = strong clean trend
    atr = f.get("atr_pct") or 0
    if atr < 0.8:   s += 2.0    # очень спокойный → strong trend
    elif atr < 1.2: s += 1.0
    if atr > 3.5:   s -= 1.5    # волатильно → trend breakdown

    # OI confirmation
    corr = f.get("oi_price_corr_20d") or 0
    if corr > 0.3:   s += 1.0
    elif corr < -0.3: s -= 0.3

    return round(s, 2)


# ───────────────────────────────────────────────────────────────
# Dispatch
# ───────────────────────────────────────────────────────────────
def compute_score(features: dict, mode: str) -> float:
    """Main entry — выбирает scoring функцию по mode.

    Mode из AssetClass.mode: 'meanrev' / 'momentum' / 'neutral'.
    Для 'neutral' возвращаем только universal часть (consensus features).
    """
    if mode == "meanrev":
        return meanrev_score(features)
    if mode == "momentum":
        return momentum_score(features)
    return round(universal_signals(features), 2)


# ───────────────────────────────────────────────────────────────
# Direction interpretation
# ───────────────────────────────────────────────────────────────
def signal_direction(score: float, mode: str) -> Optional[str]:
    """Какой direction предсказывает данный score в заданной модели.

    Returns 'bull' / 'bear' / None (neutral).
    """
    if mode == "meanrev":
        if score >= 3.0:
            return "bull"
        if score <= -2.0:
            return "bear"
    elif mode == "momentum":
        if 2.0 <= score < 4.5:   # 4.5+ overfit на test, не доверяем
            return "bull"
    return None
