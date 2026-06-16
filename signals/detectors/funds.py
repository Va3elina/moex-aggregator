"""Fund-flow anomaly detector — «аномальный поток» по категориям фондов.

Зеркало OI-детектора (signals/detectors/oi.py: compute_position_atr), но по
дневному net_flow категории вместо чистой позиции по контрактам.

Метрика (решение Вадима — та же формула, что oi_move, БЕЗ серий/рекордов):
  ratio = |net_flow_сегодня| / ATR(14),
где ATR = среднее |дневных net_flow| за 14 дней ДО последнего. «Во сколько раз
сегодняшний поток (приток/отток денег в фонды категории) больше обычного дневного».

net_flow считается в signals/db.get_fund_flow_series — суммарный по фондам
категории ΔСЧА − рыночная переоценка пая (зеркало api/routers/funds.get_funds_flows
ветка timeframe='1d'). direction: 'up' = приток денег (net_flow>0), 'down' = отток.

Категории: 'money_market' | 'stocks' | 'bonds' | 'gold' (юань — «Скоро», не включён).
"""
from __future__ import annotations
import statistics
from typing import Optional

from signals.db import get_fund_flow_series

# Параметры ATR — по образцу oi.py (ATR_WINDOW=14). Guard'ов «ликвидность/
# материальность/floor» из OI здесь нет: net_flow — уже знаковая дельта в рублях
# (а не накопленный уровень), нулевой/мёртвой «базы» не бывает, поэтому единственная
# защита — минимум истории и ненулевой ATR (как stdev==0 в z-детекторе).
ATR_WINDOW = 14
MIN_HISTORY_DAYS = 30   # минимум дней ряда (как config.MIN_HISTORY_DAYS у OI)


def compute_fund_flow_atr(category: str) -> Optional[tuple]:
    """ATR-резкость последнего дневного net_flow категории — для fund-алертов
    «аномальный поток». Зеркало compute_position_atr.

    ratio = |net_flow_последний| / ATR(14), ATR = среднее |дневных net_flow| за 14
    дней ДО последнего. direction: 'up' (приток) если net_flow>0 иначе 'down'.

    Guard'ы: минимум истории (>= MIN_HISTORY_DAYS точек И >= ATR_WINDOW+1 дельт),
    защита от нулевого ATR (мёртвый ряд — деления не будет).

    Возвращает (ratio, direction, last_flow, signal_date) или None
    (мало истории / нулевой ATR). signal_date — дата последнего net_flow
    (для гейта «новый торговый день» в alerts_run, как у OI)."""
    series = get_fund_flow_series(category, days=ATR_WINDOW + 45)
    if len(series) < MIN_HISTORY_DAYS:
        return None
    flows = [s[1] for s in series]
    # Дневные net_flow — уже сами по себе дельты (в отличие от OI, где брался diff
    # накопленного net). ATR = среднее |net_flow| за 14 дней ДО последнего.
    abs_flows = [abs(f) for f in flows]
    if len(abs_flows) < ATR_WINDOW + 1:
        return None
    last_flow = flows[-1]
    last = abs(last_flow)
    atr = statistics.fmean(abs_flows[-(ATR_WINDOW + 1):-1])   # 14 дней ДО последнего
    if atr <= 0:
        return None
    ratio = last / atr
    return (round(ratio, 2),
            "up" if last_flow > 0 else "down",
            last_flow,
            series[-1][0])
