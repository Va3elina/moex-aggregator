"""Издержки. Комиссия брокера — % от стоимости контракта ЗА СТОРОНУ, с датой начала действия тарифа.
Источник: официальные PDF тарифов T-Bank (09.2026). Спред — круг по 3 уровням стакана (Algopack obstats, 09.2026),
одно число на бумагу: data/costs.json, поле c3, в % за круг. История спреда по дням — этап 4."""
import json, numpy as np
from . import store

TARIFFS = {            # тариф → [(действует с, % за сторону)]
    'none':     [('2000-01-01', 0.0)],
    'backtest': [('2000-01-01', 0.005)],      # допущение замороженных спецификаций: 0.01 % за круг
    'premium':  [('2000-01-01', 0.025)],
    'trader':   [('2000-01-01', 0.04)],
    'investor': [('2000-01-01', 0.10)],
    'sandbox':  [('2000-01-01', 0.05)],       # песочница T-Invest
}


def commission_side(tariff, d):
    """Доля (не %) за сторону на дату."""
    rate = 0.0
    for since, pct in TARIFFS[tariff]:
        if str(d)[:10] >= since: rate = pct
    return rate / 100


def spread_round(st, mode='c3'):
    """Доля (не %) за круг."""
    if mode == 'none': return 0.0
    c = json.loads((store.REF / 'costs.json').read_text())
    return c[st][mode] / 100


def apply(T, tariff='trader', spread='c3'):
    T = T.copy()
    T['comm'] = [2 * commission_side(tariff, d) for d in T.d]
    T['spread'] = [spread_round(s, spread) for s in T.st]
    T['net'] = T.gross - T.comm - T.spread
    return T
