"""OI (Open Interest) anomaly detector — поддерживает FIZ и YUR группы.

Алгоритм (один и тот же для физлиц / юрлиц):
  1. Берём daily snapshots чистой позиции за последние LOOKBACK_DAYS + 1 дней.
  2. Считаем daily diffs: Δ[i] = net[i] - net[i-1].
  3. На rolling-окне предыдущих Δ (все кроме последнего) считаем mean и std.
  4. z = (Δ_последний - mean) / std.
  5. Если |z| > Z_THRESHOLD → возвращаем SignalCandidate.

Почему diff, а не уровень: OI накопительный (кумулятивный).
У растущего OI z-score уровня всегда положительный — тренд маскирует аномалию.
Z-score дневного изменения ловит именно «резкие» дни.

Backtest: параметр `as_of_date` пробрасывается в get_oi_daily — детектор работает
«как если бы сегодня была эта дата». Для прогона исторического диапазона.
"""
from __future__ import annotations
import statistics
from dataclasses import dataclass
from datetime import date
from typing import Optional, List

from signals import config
from signals.db import get_oi_daily, get_asset_name


@dataclass(frozen=True)
class OISignalCandidate:
    asset: str                # sectype (SR, GZ, ...)
    asset_name: Optional[str] # human-readable из instruments
    clgroup: str              # 'FIZ' | 'YUR'
    signal_type: str          # 'phys_net_long_anomaly' | 'legal_net_long_anomaly'
    direction: str            # 'up' (нарастили) | 'down' (сократили)
    z_score: float
    daily_change: int         # Δ net за последний день (контракты)
    current_net: int          # net на дату аномалии
    previous_net: int         # net за день до
    tradedate: date           # дата аномалии


_SIGNAL_TYPE_BY_GROUP = {
    "FIZ": "phys_net_long_anomaly",
    "YUR": "legal_net_long_anomaly",
}


def _detect_oi_anomaly(
    sectype: str,
    clgroup: str,
    as_of_date: Optional[date] = None,
) -> Optional[OISignalCandidate]:
    """Z-score детектор для одной (asset, clgroup), опционально на дату."""
    points = get_oi_daily(
        sectype, clgroup,
        days=config.LOOKBACK_DAYS + 1,
        as_of_date=as_of_date,
    )
    if len(points) < config.MIN_HISTORY_DAYS:
        return None

    nets = [p.net for p in points]
    diffs = [nets[i] - nets[i - 1] for i in range(1, len(nets))]
    if len(diffs) < 2:
        return None

    last_diff = diffs[-1]
    historical = diffs[:-1]
    if len(historical) < 2:
        return None

    mean_d = statistics.fmean(historical)
    stdev_d = statistics.stdev(historical)
    if stdev_d == 0:
        return None

    z = (last_diff - mean_d) / stdev_d
    if abs(z) < config.Z_THRESHOLD:
        return None

    return OISignalCandidate(
        asset=sectype,
        asset_name=get_asset_name(sectype),
        clgroup=clgroup,
        signal_type=_SIGNAL_TYPE_BY_GROUP.get(clgroup, f"{clgroup.lower()}_net_anomaly"),
        direction="up" if last_diff > 0 else "down",
        z_score=round(z, 2),
        daily_change=last_diff,
        current_net=points[-1].net,
        previous_net=points[-2].net,
        tradedate=points[-1].tradedate,
    )


def detect_all_oi(as_of_date: Optional[date] = None) -> List[OISignalCandidate]:
    """Прогон по всем тикерам × обе группы (FIZ, YUR).

    `as_of_date` — для backtest. Если None → детектит на сегодня.
    """
    candidates: List[OISignalCandidate] = []
    for sectype in config.OI_ASSETS:
        for clgroup in ("FIZ", "YUR"):
            try:
                sig = _detect_oi_anomaly(sectype, clgroup, as_of_date=as_of_date)
                if sig is not None:
                    candidates.append(sig)
            except Exception as e:
                # Один failed asset/group не ронит весь scan.
                print(f"[oi-detector] {sectype}/{clgroup} skipped: {type(e).__name__}: {e}")
    return candidates
