"""Счёт в рублях: капитал, слоты, целые контракты, ГО, комиссия и спред в рублях. Последовательная модель —
как торгует робот: утром закрываем вчерашнее, вечером входим по сигналам в порядке бумаг правила.

ГО: 'mr1'  — историческое: стоимость контракта × ставка рыночного риска MR1 МосБиржи на дату (data/risk_rates.csv.gz)
             × надбавка брокера (ГО T-Bank / ГО биржи по снимку 15.09.2026: у ликвидных ≈ 1.0–1.05, у Самолёта ≈ 1.2);
    'snapshot' — константа из снимка T-Invest 15.09.2026 (так считал OsEngine);  'none' — не проверять.
Стоимость пункта в рублях у контрактов в валюте (BR, PT, RI) — по дням, из данных самой биржи
(data/step_cost.csv.gz = OPENPOSITIONVALUE / OPENPOSITION / SETTLEPRICE из бесплатной истории ISS): это тот курс,
по которому биржа считала вариационную маржу. У остальных пункт стоит постоянно (шаг × лот в рублях).
Плечо: leverage > 1 — на сделку идёт капитал × плечо / слоты; тогда ГО начинает ограничивать объём, а go_mult
(стресс «ГО выросло в k раз», как в феврале 2022) и маржин-коллы становятся содержательными."""
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


def step_costs():
    """{тип: Series[дата → ₽ за 1 пункт цены]} для контрактов в валюте."""
    if 'rpp' not in _C:
        f = store.REF / 'step_cost.csv.gz'; _C['rpp'] = {}
        if f.exists():
            df = pd.read_csv(f, parse_dates=['d'])
            _C['rpp'] = {st: g.set_index('d').rub_per_point.sort_index() for st, g in df.groupby('st')}
    return _C['rpp']


def rub_per_point(st, d=None):
    s = specs()[st]; const = s['step_cost_rub'] / s['price_step'] * s['lot']
    r = step_costs().get(st)
    if r is None or d is None: return const
    i = r.index.searchsorted(pd.Timestamp(d), side='right') - 1
    return float(r.iloc[i]) if i >= 0 else const


def contract_value(st, price, d=None):
    return price * rub_per_point(st, d)


def broker_coef(st):
    """Надбавка брокера к биржевому ГО: ГО T-Bank из снимка / (цена × ставка MR1 на дату снимка)."""
    k = ('coef', st)
    if k not in _C:
        try:
            s = specs()[st]; d = pd.Timestamp('2026-09-15'); rr = risk_rates()
            i = rr.index.searchsorted(d, side='right') - 1
            base = s['value_rub'] * float(rr[ASSET[st]].iloc[i])
            _C[k] = min(2.0, max(1.0, (s['go_buy'] + s['go_sell']) / 2 / base)) if base > 0 else 1.0
        except Exception:
            _C[k] = 1.0
    return _C[k]


def go_per_contract(st, d, price, side, mode, step_d='same'):
    if mode == 'none': return 0.0
    if mode == 'snapshot':
        s = specs()[st]; return s['go_buy'] if side > 0 else s['go_sell']
    rr = risk_rates(); a = ASSET[st]
    if a not in rr.columns: return float('nan')
    i = rr.index.searchsorted(pd.Timestamp(d), side='right') - 1
    return contract_value(st, price, d if step_d == 'same' else step_d) * float(rr[a].iloc[i]) * broker_coef(st) if i >= 0 else float('nan')


def simulate(T, capital=1_000_000, slots=6, go_limit=1.0, go_mode='mr1', tariff='trader', spread='c3',
             order=None, leverage=1.0, go_mult=1.0, step_cost='history'):
    """T — сделки engine.trades(). Возвращает (сделки со счётом, журнал пропусков, дневная кривая капитала).
    Маржин-колл: утром перед выходом капитал с учётом ночного результата меньше требуемого ГО — событие в кривой."""
    hist = step_cost == 'history'                       # 'snapshot' — постоянная стоимость пункта, как считал OsEngine
    rank = {s: i for i, s in enumerate(order or sorted(T.st.unique()))}
    T = T.copy()
    if 'm_in' not in T: T['m_in'] = 1020
    if 'm_out' not in T: T['m_out'] = 660
    if 'n' not in T: T['n'] = range(len(T))
    T = T.assign(_r=T.st.map(rank).fillna(999)).sort_values(['d', 'm_in', '_r'])
    equity, open_pos, done, skipped, curve = float(capital), [], [], [], []
    days = sorted(set(T.d) | set(T.d_out.dropna()))
    byday = {d: g for d, g in T.groupby('d')}

    def close_until(d, minute):
        """Закрыть всё, у чего выход наступил к этому моменту. События идут по времени — поэтому внутридневные
        сделки освобождают слот и ГО сразу, а не на следующий день."""
        nonlocal equity, open_pos
        still = []
        for p in sorted(open_pos, key=lambda x: (x['d_out'], x['m_out'])):
            if (p['d_out'], p['m_out']) <= (d, minute):
                rpp = rub_per_point(p['st'], p['d_out'] if hist else None)   # вариационная маржа — по курсу дня выхода
                cv_out = p['px_out'] * rpp * p['qty']
                gross = p['side'] * (p['px_out'] - p['px_in']) * rpp * p['qty']
                comm = costs.commission_side(tariff, p['d'], p['st'], p['notional'] / p['qty']) * p['notional'] \
                    + costs.commission_side(tariff, p['d_out'], p['st'], cv_out / p['qty']) * cv_out
                spr = costs.spread_on(p['st'], p['d'], p['d_out'], spread) * p['notional']
                if spread == 'daily':                           # заявка больше глубины стакана — круг дороже
                    k = costs.depth_penalty(p['st'], p['d'], p['notional']); spr *= k; p['depth_k'] = k
                p.update(gross_rub=gross, comm_rub=comm, spread_rub=spr, pnl_rub=gross - comm - spr)
                equity += p['pnl_rub']; done.append(p)
            else:
                still.append(p)
        open_pos = still

    for d in days:
        mcall = 0
        if open_pos and go_mode != 'none':                  # утро: хватает ли капитала с учётом ночного результата на ГО
            mtm = equity + sum(p['side'] * (p['px_out'] - p['px_in']) * rub_per_point(p['st'], p['d_out'] if hist else None) * p['qty']
                               for p in open_pos if p['d_out'] <= d)
            mcall = int(mtm < sum(p['go'] for p in open_pos))
        for t in (byday[d].itertuples() if d in byday else []):
            close_until(d, t.m_in)
            reason = ''; cut = False
            cv = contract_value(t.st, t.px_in, d if hist else None)
            go1 = go_per_contract(t.st, d, t.px_in, t.side, go_mode, 'same' if hist else None) * go_mult
            if len(open_pos) >= slots:
                reason = 'нет свободного слота'
            else:
                qty = math.floor(equity * leverage / slots / cv) if cv > 0 else 0
                if qty < 1:
                    reason = 'на слот меньше 1 контракта'
                elif go1 and go1 > 0:
                    free = equity * go_limit - sum(p['go'] for p in open_pos)
                    if qty * go1 > free:
                        qty = math.floor(free / go1); cut = True
                        if qty < 1: reason = 'не хватает ГО'
            if reason:
                skipped.append({'n': t.n, 'd': d, 'st': t.st, 'side': t.side, 'reason': reason}); continue
            open_pos.append({'n': t.n, 'd': d, 'm_in': t.m_in, 'st': t.st, 'secid': t.secid, 'side': t.side, 'qty': qty, 'px_in': t.px_in,
                             'd_out': t.d_out, 'm_out': t.m_out, 'px_out': t.px_out, 'notional': cv * qty,
                             'go': (go1 or 0) * qty, 'equity_in': equity, 'go_cut': cut})
        close_until(d, 1440)
        curve.append({'d': d, 'equity': equity, 'positions': len(open_pos), 'margin_call': mcall,
                      'notional': sum(p['notional'] for p in open_pos), 'go_used': sum(p['go'] for p in open_pos)})
    A = pd.DataFrame(done, columns=None if done else ['n', 'd', 'st', 'qty', 'notional', 'go', 'equity_in', 'comm_rub', 'spread_rub', 'pnl_rub', 'go_cut'])
    K = pd.DataFrame(skipped, columns=['n', 'd', 'st', 'side', 'reason'])
    E = pd.DataFrame(curve).set_index('d')
    return A, K, E
