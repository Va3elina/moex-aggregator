"""
Ensemble detector — главный детектор сигналов v3.

Для каждого актива:
  1. Классифицирует (classification.classify)
  2. Если tier > MIN_PUBLISH_TIER → skip (шум)
  3. Загружает чарт + OI (FIZ, YUR)
  4. Computes features (features.compute_features)
  5. Computes score (scoring.compute_score по mode)
  6. Determines direction (scoring.signal_direction)
  7. Lookups probability (calibration.lookup)
  8. Checks is_publishable
  9. Emits EnsembleSignalCandidate если passes

Замещает старый `detectors/oi.py` (z-score детектор только для физлиц).
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import requests
import urllib3
import warnings

from signals import config, classification, features, scoring, calibration

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter("ignore")

log = logging.getLogger("signals.detectors.ensemble")


# ───────────────────────────────────────────────────────────────
# Config (синхронизировано с production cron)
# ───────────────────────────────────────────────────────────────
API_BASE = "https://localhost"
API_HOST_HEADER = "framedata.ru"
CHART_PERIOD = "2y"  # данных хватает на warmup + текущий день


@dataclass(frozen=True)
class EnsembleSignalCandidate:
    asset: str
    asset_name: Optional[str]
    classification: classification.AssetClass
    direction: str       # 'bull' / 'bear'
    score: float
    probability: float   # %
    avg_expected_ret: float  # %
    n_historical: int    # сколько случаев в bucket
    features: dict       # все features для caption
    tradedate: date


# ───────────────────────────────────────────────────────────────
def _fetch_chart(sectype: str, clgroup: str) -> dict:
    """GET /api/chart/{sectype} с clgroup. timeout 60s."""
    r = requests.get(
        f"{API_BASE}/api/chart/{sectype}",
        params={
            "sectype": sectype,
            "inst_type": "futures",
            "interval": 24,
            "clgroup": clgroup,
            "show_oi": True,
            "period": CHART_PERIOD,
        },
        headers={"Host": API_HOST_HEADER},
        verify=False,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def detect_one(
    sectype: str,
    *,
    name: Optional[str] = None,
    group: Optional[str] = None,
    as_of_date: Optional[date] = None,
) -> Optional[EnsembleSignalCandidate]:
    """Анализ одного актива. Возвращает signal candidate или None.

    None возвращаем если:
      - tier > MIN_PUBLISH_TIER (мелкий актив, шум)
      - mode == 'neutral' (не классифицирован)
      - данных мало (warmup)
      - score не прошёл threshold
      - probability не прошла gate (по calibration)
    """
    cls = classification.classify(sectype, name=name, group=group)

    # Filter 1: tier
    if not cls.publishable:
        return None

    # Filter 2: mode
    if cls.mode == "neutral":
        return None

    # Fetch data
    try:
        chart_fiz = _fetch_chart(sectype, "FIZ")
        chart_yur = _fetch_chart(sectype, "YUR")
    except Exception as e:
        log.warning("fetch failed for %s: %s", sectype, e)
        return None

    # Compute features
    feat = features.compute_features(sectype, chart_fiz, chart_yur, as_of_date=as_of_date)
    if feat is None:
        return None

    # Compute score
    score = scoring.compute_score(feat, cls.mode)
    direction = scoring.signal_direction(score, cls.mode)
    if direction is None:
        return None  # noise zone

    # Lookup probability in calibration
    stats = calibration.lookup(score, cls.mode)
    if stats is None:
        return None  # bucket not in calibration

    # Filter 3: is publishable?
    if not calibration.is_publishable(stats, direction, min_probability=30, min_n=30):
        return None

    return EnsembleSignalCandidate(
        asset=sectype,
        asset_name=name,
        classification=cls,
        direction=direction,
        score=score,
        probability=stats.p_rise if direction == "bull" else stats.p_fall,
        avg_expected_ret=stats.avg_ret,
        n_historical=stats.n,
        features=feat,
        tradedate=feat["tradedate"],
    )


def detect_all(
    sectypes: list[str],
    *,
    name_resolver=None,
    group_resolver=None,
    as_of_date: Optional[date] = None,
) -> list[EnsembleSignalCandidate]:
    """Прогон по списку активов. Возвращает signal candidates прошедшие пороги.

    Args:
        sectypes: список тикеров
        name_resolver: callable(sectype) → name (optional, для caption)
        group_resolver: callable(sectype) → group из БД (optional, для classification)
    """
    out = []
    for sectype in sectypes:
        try:
            name = name_resolver(sectype) if name_resolver else None
            group = group_resolver(sectype) if group_resolver else None
            sig = detect_one(sectype, name=name, group=group, as_of_date=as_of_date)
            if sig is not None:
                out.append(sig)
                log.info("signal: %s score=%+.2f %s prob=%.1f%% n=%d",
                         sectype, sig.score, sig.direction,
                         sig.probability, sig.n_historical)
        except Exception:
            log.exception("error analysing %s", sectype)
    return out
