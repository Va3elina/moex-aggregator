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


_S = {}


def spread_round(st, mode='c3'):
    """Доля (не %) за круг — одно число на бумагу (медиана 09.2025–09.2026)."""
    if mode == 'none': return 0.0
    if 'const' not in _S: _S['const'] = json.loads((store.REF / 'costs.json').read_text())
    return _S['const'][st]['c5' if mode == 'c5' else 'c3'] / 100


def spread_daily():
    """{тип: Series[дата → доля за круг по 3 уровням стакана]} — Algopack obstats, с 03.2023 (data/spread_daily.csv.gz,
    собирает research/backtest_terminal/build_spread_daily.py). Файла нет — пустой словарь."""
    if 'daily' not in _S:
        f = store.REF / 'spread_daily.csv.gz'; _S['daily'] = {}
        if f.exists():
            import pandas as pd
            df = pd.read_csv(f, parse_dates=['d'])
            _S['daily'] = {st: g.set_index('d').c3.sort_index() / 100 for st, g in df.groupby('st')}
            _S['cap'] = {st: g.set_index('d').cap3_rub.sort_index() for st, g in df.groupby('st')}
    return _S['daily']


def spread_on(st, d_in, d_out, mode='daily'):
    """Спред круга для сделки: половина — в день входа, половина — в день выхода. До 03.2023 и в дни без замера —
    медиана по бумаге из тех же замеров; нет замеров вовсе — постоянное число."""
    if mode != 'daily': return spread_round(st, mode)
    s = spread_daily().get(st)
    if s is None or not len(s): return spread_round(st, 'c3')
    med = _S.setdefault(('med', st), float(s.median()))
    import pandas as pd
    a, b = s.get(pd.Timestamp(d_in)), s.get(pd.Timestamp(d_out))
    return ((a if a is not None and a == a else med) + (b if b is not None and b == b else med)) / 2


def depth_penalty(st, d, notional):
    """Во сколько раз дороже спред, если заявка больше трёх уровней стакана: до 5/3 глубины — как 5 уровней (×1.5),
    дальше — как 10 уровней (×2.5). Соотношения — из замеров (ORDERBOOK.md: c5/c3 ≈ 1.5, c10/c3 ≈ 2.4–2.8)."""
    spread_daily(); cap = _S.get('cap', {}).get(st)
    if cap is None or not len(cap): return 1.0
    import pandas as pd
    c = cap.get(pd.Timestamp(d)); c = float(c) if c is not None and c == c else float(cap.median())
    if c <= 0 or notional <= c: return 1.0
    return 1.5 if notional <= c * 5 / 3 else 2.5


def apply(T, tariff='trader', spread='c3'):
    T = T.copy()
    T['comm'] = [2 * commission_side(tariff, d) for d in T.d]
    T['spread'] = [spread_on(s, di, do, spread) for s, di, do in zip(T.st, T.d, T.d_out)]
    T['net'] = T.gross - T.comm - T.spread
    return T
