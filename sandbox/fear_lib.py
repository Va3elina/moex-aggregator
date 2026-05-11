"""Pure-python fear-and-greed index math.

Convention: score is GREED-positive in [0, 100].
    0   = extreme fear
    50  = neutral
    100 = extreme greed

Each factor has its own Transform config (kind, window/EMA, thresholds).
Composite is a weighted, NaN-safe mean across enabled factors.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

CACHE_DIR = Path(__file__).parent / "cache"

COMPONENT_KEYS = [
    "rvi",
    "breadth",
    "market_momentum",
]

COMPONENT_LABELS = {
    "rvi":              "1. RVI volatility",
    "breadth":          "2. Breadth (% > 200EMA)",
    "market_momentum":  "3. Market Momentum (IMOEX vs EMA-125)",
}


# ---------- Transform config ------------------------------------------------

TransformKind = Literal["pct_rank", "thresholds", "level_momentum"]
SignalMode    = Literal["both", "fear_only", "greed_only"]


@dataclass
class Transform:
    """How to turn a raw factor series into a 0..100 GREED score."""
    kind: TransformKind = "pct_rank"
    # Asymmetric shaping:
    #   "both"        -> score range [0, 100]
    #   "fear_only"   -> fear half full, greed half dampened by greed_residual
    #                    (greed_residual=0 -> hard clamp at 50, =1 -> symmetric)
    #   "greed_only"  -> mirror: greed full, fear dampened by fear_residual
    signal_mode: SignalMode = "both"
    greed_residual: float = 0.0   # for fear_only: amplitude of greed half [0..1]
    fear_residual:  float = 0.0   # for greed_only: amplitude of fear half [0..1]
    # pct_rank params
    window: int = 252
    higher_is_greed: bool = True
    # thresholds params: value mapped to 100 (full greed) and to 0 (full fear)
    greed_at: float | None = None
    fear_at:  float | None = None
    # extreme-emphasis exponent for thresholds: 1 = linear (default),
    # >1 = flatten middle, accelerate at edges (3 ≈ strong emphasis on extremes)
    tail_emphasis: float = 1.0
    # level_momentum params (sigmoid level + z-scored delta)
    lm_center: float = 50.0      # used when lm_center_kind == "fixed"
    lm_center_kind: Literal["fixed", "ema"] = "fixed"
    lm_center_ema_span: int = 200  # EMA span when lm_center_kind == "ema"
    lm_steepness: float = 0.07   # sigmoid k; higher = sharper transition at center
    lm_delta_n: int = 5          # bars between RVI(t) and RVI(t-n) for momentum
    lm_z_window: int = 252       # window for mean/std of delta
    lm_momentum_scale: float = 15.0  # one σ of delta moves score by this many points
    lm_level_weight: float = 0.6     # α — weight of absolute level vs momentum
    lm_higher_is_fear: bool = True   # if False, raw=high means greed
    # post-smoothing
    ema_span: int = 1


# Defaults — chosen so RVI 80+ punches hard into fear, etc.
DEFAULT_TRANSFORMS: dict[str, Transform] = {
    "rvi":             Transform(kind="level_momentum",
                                 signal_mode="fear_only",
                                 lm_center=40.0,
                                 lm_center_kind="ema",
                                 lm_center_ema_span=200,
                                 lm_steepness=0.08,
                                 lm_delta_n=5, lm_z_window=252,
                                 lm_momentum_scale=15.0, lm_level_weight=0.6,
                                 lm_higher_is_fear=True, ema_span=3),
    "breadth":         Transform(kind="thresholds", greed_at=100.0, fear_at=0.0,
                                 tail_emphasis=1.0, ema_span=3),
    "market_momentum": Transform(kind="level_momentum",
                                 signal_mode="both",
                                 lm_center_kind="ema",
                                 lm_center_ema_span=125,
                                 lm_steepness=0.05,
                                 lm_delta_n=5, lm_z_window=252,
                                 lm_momentum_scale=15.0, lm_level_weight=0.7,
                                 lm_higher_is_fear=False, ema_span=3),
}

DEFAULT_WEIGHTS = {
    "rvi":             1/3,
    "breadth":         1/3,
    "market_momentum": 1/3,
}


# ---------- helpers ---------------------------------------------------------

def rolling_percentile_rank(s: pd.Series, window: int) -> pd.Series:
    """Rank of last value among the last `window` values, in [0, 1]."""
    def _rank(arr: np.ndarray) -> float:
        last = arr[-1]
        if np.isnan(last):
            return np.nan
        valid = arr[~np.isnan(arr)]
        if len(valid) < 2:
            return np.nan
        return (valid <= last).sum() / len(valid)
    return s.rolling(window, min_periods=max(20, window // 4)).apply(_rank, raw=True)


def ema(s: pd.Series, span: int) -> pd.Series:
    if span <= 1:
        return s
    return s.ewm(span=span, adjust=False, min_periods=1).mean()


def apply_transform(raw: pd.Series, t: Transform) -> pd.Series:
    """Raw value -> GREED score in [0, 100]."""
    if t.kind == "pct_rank":
        rank = rolling_percentile_rank(raw, t.window)
        score = (rank if t.higher_is_greed else (1 - rank)) * 100.0
    elif t.kind == "thresholds":
        if t.greed_at is None or t.fear_at is None:
            raise ValueError("thresholds transform needs greed_at and fear_at")
        if t.greed_at == t.fear_at:
            score = pd.Series(50.0, index=raw.index)
        else:
            x = ((raw - t.fear_at) / (t.greed_at - t.fear_at)).clip(0, 1)
            if t.tail_emphasis == 1.0:
                score = (x * 100.0)
            else:
                c = (2.0 * x - 1.0)  # centred to [-1, +1]
                score = 50.0 + 50.0 * np.sign(c) * np.abs(c) ** t.tail_emphasis
    elif t.kind == "level_momentum":
        # Level: sigmoid mapping centred at lm_center (fixed) or rolling EMA.
        if t.lm_center_kind == "ema":
            center = raw.ewm(span=t.lm_center_ema_span,
                             adjust=False, min_periods=1).mean()
        else:
            center = t.lm_center
        # If higher raw == fear (e.g. RVI), exponent sign is +; otherwise flip.
        sign = 1.0 if t.lm_higher_is_fear else -1.0
        # protect from overflow
        z_arg = np.clip(sign * t.lm_steepness * (raw - center), -50, 50)
        level = 100.0 / (1.0 + np.exp(z_arg))
        # Momentum: z-score of n-bar delta, sign-flipped if higher_is_fear
        delta = raw - raw.shift(t.lm_delta_n)
        mu  = delta.rolling(t.lm_z_window, min_periods=max(20, t.lm_z_window//4)).mean()
        sd  = delta.rolling(t.lm_z_window, min_periods=max(20, t.lm_z_window//4)).std()
        z   = (delta - mu) / sd.replace(0, np.nan)
        momentum = (50.0 - sign * t.lm_momentum_scale * z).clip(0, 100)
        score = (t.lm_level_weight * level
                 + (1 - t.lm_level_weight) * momentum).clip(0, 100)
    else:
        raise ValueError(f"unknown transform kind: {t.kind}")
    # Asymmetric shaping
    if t.signal_mode == "fear_only":
        if t.greed_residual <= 0:
            score = score.clip(upper=50)
        else:
            # Сжимаем жадную половину: 50..100 -> 50..(50 + 50 * greed_residual)
            above = score > 50
            score = score.where(~above,
                                50.0 + (score - 50.0) * t.greed_residual)
    elif t.signal_mode == "greed_only":
        if t.fear_residual <= 0:
            score = score.clip(lower=50)
        else:
            below = score < 50
            score = score.where(~below,
                                50.0 - (50.0 - score) * t.fear_residual)
    return ema(score, t.ema_span).rename(raw.name)


# ---------- raw extraction --------------------------------------------------

def load_raw_from_cache() -> dict[str, pd.DataFrame]:
    files = ["rvi", "indices", "breadth"]
    out = {}
    for name in files:
        path = CACHE_DIR / f"{name}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"missing {path}. Run: python sandbox/fetch_data.py")
        out[name] = pd.read_parquet(path)
    return out


def build_raw_components(raw: dict[str, pd.DataFrame],
                         flow_window: int = 5) -> pd.DataFrame:
    """Pre-rank raw values per factor (orientation-agnostic).

    Columns:
      rvi              - RVI close (high = volatility = fear)
      breadth          - % above 200EMA  (high = broad rally = greed)
      market_momentum  - IMOEX price level (compared to EMA-125 in transform)
    """
    rvi = raw["rvi"]["rvi"].rename("rvi")
    breadth_col = ("pct_above_200" if "pct_above_200" in raw["breadth"].columns
                   else raw["breadth"].columns[0])
    breadth = raw["breadth"][breadth_col].rename("breadth")

    market_momentum = raw["indices"]["IMOEX"].rename("market_momentum")

    return pd.concat([rvi, breadth, market_momentum],
                     axis=1).sort_index()


# ---------- composition -----------------------------------------------------

@dataclass
class FearParams:
    transforms: dict[str, Transform] = field(
        default_factory=lambda: {k: Transform(**DEFAULT_TRANSFORMS[k].__dict__)
                                 for k in DEFAULT_TRANSFORMS})
    weights:  dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_WEIGHTS))
    enabled:  dict[str, bool] = field(
        default_factory=lambda: {k: True for k in COMPONENT_KEYS})
    final_ema_span: int = 5
    # Дополнительный фильтр: RVI как контроль над «страховой» половиной композита.
    # 0   — выключен.
    # 0.5 — мягко: спокойный RVI наполовину сжимает страховое плечо к нейтрали.
    # 1.0 — жёстко: при полном штиле RVI композит не может уходить ниже 50.
    rvi_fear_gate: float = 0.5


def compose(raw_df: pd.DataFrame, params: FearParams) -> pd.DataFrame:
    """Return DataFrame: <comp>_score, <comp>_strength + 'fng' composite.

    Voting weights are scaled by signal strength based on signal_mode:
      both        -> strength = 1 always
      fear_only   -> strength = max(0, (50 - score) / 50)
      greed_only  -> strength = max(0, (score - 50) / 50)
    A muted factor (strength=0) drops out of the weighted average.
    """
    out = pd.DataFrame(index=raw_df.index)

    # 1) per-factor score
    for comp in COMPONENT_KEYS:
        out[f"{comp}_score"] = apply_transform(raw_df[comp], params.transforms[comp])

    # 2) per-factor signal strength
    #    Унифицированная формула: |score - 50| / 50.
    #    Для signal_mode="both" оставляем strength=1, чтобы не менять
    #    исторически сложившееся поведение симметричных факторов.
    for comp in COMPONENT_KEYS:
        s = out[f"{comp}_score"]
        mode = params.transforms[comp].signal_mode
        if mode == "both":
            strength = pd.Series(1.0, index=s.index)
        else:
            strength = (s - 50.0).abs() / 50.0
            strength = strength.clip(0.0, 1.0)
        out[f"{comp}_strength"] = strength

    # 3) weighted average with effective weights
    enabled_keys = [c for c in COMPONENT_KEYS if params.enabled.get(c, True)]
    if not enabled_keys:
        out["fng"] = np.nan
        return out

    base_w  = np.array([params.weights[c] for c in enabled_keys], dtype=float)
    arr     = out[[f"{c}_score"    for c in enabled_keys]].to_numpy(dtype=float)
    str_arr = out[[f"{c}_strength" for c in enabled_keys]].to_numpy(dtype=float)

    mask = ~np.isnan(arr)
    # effective weight per (day, factor) — zero out where score is NaN
    eff_w = np.where(mask, base_w * np.nan_to_num(str_arr, nan=0.0), 0.0)
    weighted_score = np.where(mask, arr, 0.0) * eff_w
    eff_w_sum = eff_w.sum(axis=1)
    composite = np.where(eff_w_sum > 0,
                         weighted_score.sum(axis=1) / eff_w_sum,
                         50.0)  # all silent -> neutral

    # 4) RVI fear gate: спокойный RVI сжимает страховое плечо композита
    if params.rvi_fear_gate > 0 and "rvi" in enabled_keys:
        rvi_s = out["rvi_score"].to_numpy(dtype=float)
        # «спокойствие» = rvi_score / 100, в [0..1]
        calmness = np.clip(np.nan_to_num(rvi_s, nan=0.0) / 100.0, 0.0, 1.0)
        suppression = calmness * params.rvi_fear_gate
        fear_side = composite < 50.0
        composite = np.where(
            fear_side,
            composite + (50.0 - composite) * suppression,
            composite,
        )

    out["fng"] = ema(pd.Series(composite, index=out.index),
                     params.final_ema_span)
    return out


# ---------- classification (greed-positive) --------------------------------

CLASS_BANDS = [
    (0,   25,  "Extreme Fear",  "#dc2626"),
    (25,  45,  "Fear",          "#f97316"),
    (45,  55,  "Neutral",       "#9ca3af"),
    (55,  75,  "Greed",         "#84cc16"),
    (75,  101, "Extreme Greed", "#10b981"),
]


def classify(value: float) -> str:
    if value is None or np.isnan(value):
        return "n/a"
    for lo, hi, label, _ in CLASS_BANDS:
        if lo <= value < hi:
            return label
    return "n/a"


# ---------- correlation helpers --------------------------------------------

def imoex_returns(raw: dict[str, pd.DataFrame], horizon: int = 1) -> pd.Series:
    """N-day forward log-returns of IMOEX (kept for backwards compat)."""
    return market_returns(raw, "IMOEX", horizon)


def market_returns(raw: dict[str, pd.DataFrame], secid: str = "IMOEX",
                   horizon: int = 1) -> pd.Series:
    """N-day forward log-returns of any indexed market (IMOEX / RTSI / ...)."""
    px = raw["indices"][secid]
    fwd = np.log(px.shift(-horizon) / px)
    return fwd.rename(f"{secid.lower()}_fwd_{horizon}d")


def rolling_correlation(score: pd.Series, target: pd.Series,
                        window: int = 60) -> pd.Series:
    """Rolling Pearson corr between a factor SCORE and a returns series."""
    df = pd.concat([score, target], axis=1).dropna()
    return df.iloc[:, 0].rolling(window).corr(df.iloc[:, 1])
