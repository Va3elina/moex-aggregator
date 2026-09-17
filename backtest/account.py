"""Счёт в рублях: капитал, слоты, целые контракты, ГО, комиссия и спред в рублях. Последовательная модель —
как торгует робот: утром закрываем вчерашнее, вечером входим по сигналам в порядке бумаг правила.

ГО: 'mr1'  — историческое: стоимость контракта × ставка рыночного риска MR1 МосБиржи на дату (data/risk_rates.csv.gz);
    'snapshot' — константа из снимка T-Invest 15.09.2026 (так считал OsEngine);  'none' — не проверять."""
import json, math, gzip, csv
import numpy as np, pandas as pd
from . import store, costs

ASSET = {'AF': 'AFLT', 'AK': 'AFKS', 'BR': 'BR', 'CC': 'COCOA', 'CR': 'CNY', 'Eu': 'Eu', 'GK': 'GMKN', 'GZ': 'GAZR',
         'LK': 'LKOH', 'MN': 'MGNT', 'MX': 'MIX', 'NM': 'NLMK', 'PI': 'PIKK', 'PT': 'PLT', 'RI': 'RTS', 'SN': 'SNGR',
         'SR': 'SBRF', 'SS': 'SMLT', 'SZ': 'SGZH', 'Si': 'Si', 'TT': 'TATN', 'VB': 'VTBR'}
_C = {}


def specs():
    if 'specs' not in _C:
        _C['specs'] = json.loads((store.REF / 'tbank_contract_specs.json').read_text())
    return _C['specs']


def risk_rates():
    """DataFrame[дата × базовый актив] ставок MR1, протянутых вперёд."""
    if 'mr1' not in _C:
        f = store.REF / 'risk_rates.csv.gz'
        df = pd.read_csv(f, usecols=['tradedate', 'assetcode', 'mr1'], parse_dates=['tradedate'])
        _C['mr1'] = df.pivot_table(index='tradedate', columns='assetcode', values='mr1', aggfunc='last').sort_index().ffill()
    return _C['mr1']


def contract_value(st, price):
    s = specs()[st]
    return price / s['price_step'] * s['step_cost_rub'] * s['lot']


def go_per_contract(st, d, price, side, mode):
    if mode == 'none': return 0.0
    if mode == 'snapshot':
        s = specs()[st]; return s['go_buy'] if side > 0 else s['go_sell']
    rr = risk_rates(); a = ASSET[st]
    if a not in rr.columns: return float('nan')
    i = rr.index.searchsorted(pd.Timestamp(d), side='right') - 1
    return contract_value(st, price) * float(rr[a].iloc[i]) if i >= 0 else float('nan')


def simulate(T, capital=1_000_000, slots=6, go_limit=1.0, go_mode='mr1', tariff='trader', spread='c3',
             order=None):
    """T — сделки engine.trades(). Возвращает (сделки со счётом, журнал пропусков, дневная кривая капитала)."""
    rank = {s: i for i, s in enumerate(order or sorted(T.st.unique()))}
    T = T.assign(_r=T.st.map(rank)).sort_values(['d', '_r'])
    equity, open_pos, done, skipped, curve = float(capital), [], [], [], []
    days = sorted(set(T.d) | set(T.d_out.dropna()))
    byday = {d: g for d, g in T.groupby('d')}
    for d in days:
        # утро: закрыть всё, у чего выход сегодня
        still = []
        for p in open_pos:
            if p['d_out'] <= d:
                cv_out = contract_value(p['st'], p['px_out']) * p['qty']
                gross = p['side'] * (cv_out - p['notional'])
                comm = costs.commission_side(tariff, p['d']) * p['notional'] + costs.commission_side(tariff, d) * cv_out
                spr = costs.spread_round(p['st'], spread) * p['notional']
                p.update(gross_rub=gross, comm_rub=comm, spread_rub=spr, pnl_rub=gross - comm - spr)
                equity += p['pnl_rub']; done.append(p)
            else:
                still.append(p)
        open_pos = still
        # вечер: входы
        for _, t in (byday[d].iterrows() if d in byday else []):
            reason = ''
            cv = contract_value(t.st, t.px_in)
            go1 = go_per_contract(t.st, d, t.px_in, t.side, go_mode)
            if len(open_pos) >= slots:
                reason = 'нет свободного слота'
            else:
                qty = math.floor(equity / slots / cv) if cv > 0 else 0
                if qty < 1:
                    reason = 'на слот меньше 1 контракта'
                elif go1 and go1 > 0:
                    free = equity * go_limit - sum(p['go'] for p in open_pos)
                    if qty * go1 > free:
                        qty = math.floor(free / go1)
                        if qty < 1: reason = 'не хватает ГО'
            if reason:
                skipped.append({'d': d, 'st': t.st, 'side': t.side, 'reason': reason}); continue
            open_pos.append({'d': d, 'st': t.st, 'secid': t.secid, 'side': t.side, 'qty': qty, 'px_in': t.px_in,
                             'd_out': t.d_out, 'px_out': t.px_out, 'notional': cv * qty,
                             'go': (go1 or 0) * qty, 'equity_in': equity, 'move': t.move, 'ret': t.ret})
        curve.append({'d': d, 'equity': equity, 'positions': len(open_pos),
                      'notional': sum(p['notional'] for p in open_pos), 'go_used': sum(p['go'] for p in open_pos)})
    A = pd.DataFrame(done); K = pd.DataFrame(skipped, columns=['d', 'st', 'side', 'reason'])
    E = pd.DataFrame(curve).set_index('d')
    return A, K, E
