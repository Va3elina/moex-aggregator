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
from signals.db import get_oi_daily, get_asset_name, get_position_series


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


def compute_oi_z(
    sectype: str,
    clgroup: str,
    as_of_date: Optional[date] = None,
) -> Optional[tuple[float, int, int]]:
    """Сырой z-score дневного Δ чистой позиции (БЕЗ порога) — для пользовательских
    алертов «OI z > X». Возвращает (z, last_diff, current_net) или None если мало
    истории / нулевой std. Та же математика что в _detect_oi_anomaly, но без гейта."""
    points = get_oi_daily(sectype, clgroup, days=config.LOOKBACK_DAYS + 1, as_of_date=as_of_date)
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
    return (round(z, 2), last_diff, points[-1].net)


# Параметры детектора «резкое движение позиции» (ATR). Подобраны по бэктесту:
# окно 14 (≈ z 30д, но устойчивее и узнаваемее «ATR14»); guard'ы против шума на
# мёртвой базе / неликвиде. См. signals/research/oi_atr*.py.
ATR_WINDOW = 14
ATR_MIN_PART = 50            # ликвидность: «толпа», иначе шум
ATR_MIN_REL = 0.02          # материальность: |Δ|/|net| ≥ 2%
ATR_FLOOR_REL = 0.001       # ATR ≥ 0.1%·|net|, иначе позиция «заморожена»


def compute_position_atr(
    sectype: str,
    clgroup: str,
    as_of_date: Optional[date] = None,
) -> Optional[tuple]:
    """ATR-резкость последнего дневного изменения позиции — для алертов «резкое
    движение». ratio = |Δ_последний| / ATR(14), где ATR = среднее |дневных Δ| за 14
    дней ДО последнего. «Во сколько раз движение больше обычного».

    Возвращает (ratio, last_diff, current_net, direction) или None (мало истории /
    неликвид / immaterial / замороженная база). direction: 'up'(нарастили)/'down'."""
    pts = get_position_series(sectype, clgroup, days=ATR_WINDOW + 30, as_of_date=as_of_date)
    if len(pts) < ATR_WINDOW + 3:
        return None
    nets = [p[1] for p in pts]
    npart_now = pts[-1][2]
    diffs = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
    if len(diffs) < ATR_WINDOW + 1:
        return None
    last_signed = nets[-1] - nets[-2]
    last = abs(last_signed)
    net = nets[-1]
    # guard'ы: ликвидность, материальность, ATR-floor (как в бэктесте)
    if npart_now < ATR_MIN_PART:
        return None
    if last / max(abs(net), 1) < ATR_MIN_REL:
        return None
    atr = statistics.fmean(diffs[-(ATR_WINDOW + 1):-1])   # ATR за 14 дней ДО последнего
    if atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1):
        return None
    ratio = last / atr
    return (round(ratio, 2), last_signed, net, "up" if last_signed > 0 else "down")


def compute_participants_atr(
    sectype: str,
    clgroup: str,
    as_of_date: Optional[date] = None,
) -> Optional[tuple]:
    """ATR-резкость последнего дневного изменения ЧИСЛА УЧАСТНИКОВ — для алертов
    «резко изменилось число участников». Полная калька compute_position_atr, но ряд
    берётся по npart (число участников = 3-й элемент get_position_series), а не по net.

    ratio = |Δnpart_последний| / ATR(14), где ATR = среднее |дневных Δnpart| за 14 дней
    ДО последнего. «Во сколько раз изменение числа участников больше обычного».

    Guard'ы те же по смыслу (база — само npart, а не net): ликвидность
    (npart_now ≥ ATR_MIN_PART), материальность (|Δnpart|/max(npart,1) ≥ ATR_MIN_REL),
    ATR-floor (ATR ≥ ATR_FLOOR_REL·npart) — ловушка «мёртвой базы» сохранена.

    npart — НЕ зеркальное число (FIZ и YUR — независимые положительные счётчики),
    поэтому part_fiz и part_yur — самостоятельные сигналы (в отличие от net).

    Возвращает (ratio, last_signed_diff, current_npart, direction) или None.
    direction: 'up' (участников прибавилось) / 'down' (убыло)."""
    pts = get_position_series(sectype, clgroup, days=ATR_WINDOW + 30, as_of_date=as_of_date)
    if len(pts) < ATR_WINDOW + 3:
        return None
    nparts = [p[2] for p in pts]
    npart_now = nparts[-1]
    diffs = [abs(nparts[i] - nparts[i - 1]) for i in range(1, len(nparts))]
    if len(diffs) < ATR_WINDOW + 1:
        return None
    last_signed = nparts[-1] - nparts[-2]
    last = abs(last_signed)
    # guard'ы: ликвидность, материальность, ATR-floor — база = само npart
    if npart_now < ATR_MIN_PART:
        return None
    if last / max(npart_now, 1) < ATR_MIN_REL:
        return None
    atr = statistics.fmean(diffs[-(ATR_WINDOW + 1):-1])   # ATR за 14 дней ДО последнего
    if atr <= 0 or atr < ATR_FLOOR_REL * max(npart_now, 1):
        return None
    ratio = last / atr
    return (round(ratio, 2), last_signed, npart_now, "up" if last_signed > 0 else "down")


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
