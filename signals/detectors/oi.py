"""OI (Open Interest) anomaly detector — поддерживает FIZ и YUR группы.

Backtest: параметр `as_of_date` пробрасывается в get_oi_daily/get_position_series —
детектор работает «как если бы сегодня была эта дата». Для прогона исторического
диапазона.
"""
from __future__ import annotations
import statistics
from datetime import date
from typing import Optional

from signals import config
from signals.db import get_oi_daily, get_position_series


def compute_oi_z(
    sectype: str,
    clgroup: str,
    as_of_date: Optional[date] = None,
) -> Optional[tuple[float, int, int]]:
    """Сырой z-score дневного Δ чистой позиции (БЕЗ порога) — для пользовательских
    алертов «OI z > X». Возвращает (z, last_diff, current_net) или None если мало
    истории / нулевой std.

    Почему diff, а не уровень: OI накопительный (кумулятивный). У растущего OI
    z-score уровня всегда положительный — тренд маскирует аномалию. Z-score
    дневного изменения ловит именно «резкие» дни."""
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
ATR_MIN_PART = 50            # ликвидность ФИЗ (розница): «толпа», иначе шум
# ЮР — институты: участников структурно на 1-2 порядка меньше, чем розницы. Порог
# 50 глушил даже голубые фишки (Газпром/Сбер на юр ~60), а мид-кэпы с 15-40
# институтами — зря. Свой порог 15. ⚠️ КОПИЯ в api/services/oi_screener.py — синхронно.
ATR_MIN_PART_YUR = 15
ATR_MIN_REL = 0.02          # материальность: |Δ|/|net| ≥ 2%
ATR_FLOOR_REL = 0.001       # ATR ≥ 0.1%·|net|, иначе позиция «заморожена»


def min_part(clgroup: str) -> int:
    """Порог ликвидности (мин. участников) с учётом группы: у юрлиц институтов
    структурно меньше, чем розницы у физлиц → свой, более низкий порог."""
    return ATR_MIN_PART_YUR if clgroup == "YUR" else ATR_MIN_PART


def compute_position_atr(
    sectype: str,
    clgroup: str,
    as_of_date: Optional[date] = None,
    interval: int = 24,
) -> Optional[tuple]:
    """ATR-резкость последнего дневного изменения позиции — для алертов «резкое
    движение». ratio = |Δ_последний| / ATR(14), где ATR = среднее |дневных Δ| за 14
    дней ДО последнего. «Во сколько раз движение больше обычного».

    `interval` — таймфрейм источника «net сейчас» (24=дневная публикация; 5/60=
    последний внутридневной бар). Прокидывается в get_position_series: математика
    ATR (по day-over-day diffs закрытий дней), guard'ы и signal_date=pts[-1][0]
    идентичны — для интрадей pts[-1] = сегодняшний бегущий день, поэтому
    last_signed = net_сейчас − вчерашнее_закрытие выходит автоматически.

    Возвращает (ratio, last_diff, current_net, direction, signal_date, legs) или None
    (мало истории / неликвид / immaterial / замороженная база). direction:
    'up'(нарастили чистый лонг)/'down'. legs — dict с дневными Δ по каждой ноге
    {'long': Δpos_long, 'short': Δpos_short} (длинная +, короткая знаковая) — чтобы
    текст алерта мог сказать, какая нога двинулась; пустой dict если истории по
    ногам нет."""
    pts = get_position_series(sectype, clgroup, days=ATR_WINDOW + 30,
                              as_of_date=as_of_date, interval=interval)
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
    # guard'ы: ликвидность (по группе), материальность, ATR-floor (как в бэктесте)
    if npart_now < min_part(clgroup):
        return None
    if last / max(abs(net), 1) < ATR_MIN_REL:
        return None
    atr = statistics.fmean(diffs[-(ATR_WINDOW + 1):-1])   # ATR за 14 дней ДО последнего
    if atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1):
        return None
    ratio = last / atr
    # Дневные Δ по каждой ноге (длинная p[3] +, короткая p[4] знаковая хранится −).
    # Нужны тексту: «выросла длинная нога» vs «нарастили короткую». Берём по той же
    # последней паре дней, что и net-сдвиг.
    legs = {"long": pts[-1][3] - pts[-2][3], "short": pts[-1][4] - pts[-2][4]}
    # 5-й элемент — дата последнего дневного значения (для гейта «новый день»
    # в alerts_run: не пере-выстреливать тот же торговый день).
    return (round(ratio, 2), last_signed, net,
            "up" if last_signed > 0 else "down", pts[-1][0], legs)


def compute_participants_atr(
    sectype: str,
    clgroup: str,
    as_of_date: Optional[date] = None,
    interval: int = 24,
) -> Optional[tuple]:
    """ATR-резкость последнего дневного изменения ЧИСЛА УЧАСТНИКОВ — для алертов
    «резко изменилось число участников». Полная калька compute_position_atr, но ряд
    берётся по npart (число участников = 3-й элемент get_position_series), а не по net.

    ratio = |Δnpart_последний| / ATR(14), где ATR = среднее |дневных Δnpart| за 14 дней
    ДО последнего. «Во сколько раз изменение числа участников больше обычного».

    Guard'ы те же по смыслу (база — само npart, а не net): ликвидность
    (npart_now ≥ min_part(clgroup)), материальность (|Δnpart|/max(npart,1) ≥ ATR_MIN_REL),
    ATR-floor (ATR ≥ ATR_FLOOR_REL·npart) — ловушка «мёртвой базы» сохранена.

    npart — НЕ зеркальное число (FIZ и YUR — независимые положительные счётчики),
    поэтому part_fiz и part_yur — самостоятельные сигналы (в отличие от net).

    `interval` — таймфрейм источника «npart сейчас» (24=дневная публикация; 5/60=
    последний внутридневной бар), прокидывается в get_position_series. Математика
    и signal_date=pts[-1][0] без изменений.

    Возвращает (ratio, last_signed_diff, current_npart, direction) или None.
    direction: 'up' (участников прибавилось) / 'down' (убыло)."""
    pts = get_position_series(sectype, clgroup, days=ATR_WINDOW + 30,
                              as_of_date=as_of_date, interval=interval)
    if len(pts) < ATR_WINDOW + 3:
        return None
    nparts = [p[2] for p in pts]
    npart_now = nparts[-1]
    diffs = [abs(nparts[i] - nparts[i - 1]) for i in range(1, len(nparts))]
    if len(diffs) < ATR_WINDOW + 1:
        return None
    last_signed = nparts[-1] - nparts[-2]
    last = abs(last_signed)
    # guard'ы: ликвидность (по группе), материальность, ATR-floor — база = само npart
    if npart_now < min_part(clgroup):
        return None
    if last / max(npart_now, 1) < ATR_MIN_REL:
        return None
    atr = statistics.fmean(diffs[-(ATR_WINDOW + 1):-1])   # ATR за 14 дней ДО последнего
    if atr <= 0 or atr < ATR_FLOOR_REL * max(npart_now, 1):
        return None
    ratio = last / atr
    # 5-й элемент — дата последнего дневного значения (гейт «новый день»).
    return (round(ratio, 2), last_signed, npart_now, "up" if last_signed > 0 else "down", pts[-1][0])
