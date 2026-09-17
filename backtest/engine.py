"""Ряд V3 → сигнал → сделки. Перенос research/lab/lab/engine.py на 5-минутные свечи и декларативные правила.

Ряд V3: на каждый будний день — ближний неистёкший контракт с ценами в обеих точках окна, монотонно (без возврата к
более раннему контракту), контракт «живой» (≥ 60 свечей за день). Ночь держим, только если завтра тот же контракт и
разрыв ≤ 4 дней — иначе сигнал записывается, но сделки нет (причина — в журнале решений)."""
import numpy as np, pandas as pd
from . import store, rules as R

MIN_BARS_DAY, MAX_GAP_DAYS, MIN_ROWS = 60, 4, 300


def chain(st, a, b):
    """Строки ряда: d, secid, lsttrade, pa, pb."""
    pa, pb = store.price(st, 'close', a), store.price(st, 'close', b)
    g = pd.concat({'pa': pa, 'pb': pb, 'bars': store.day_counts(st)}, axis=1).reset_index()
    g['lsttrade'] = g.secid.map(store.contracts(st))
    g = g[(g.d.dt.dayofweek < 5) & (g.bars >= MIN_BARS_DAY)]
    g = g[(g.lsttrade >= g.d) & g.pa.notna() & g.pb.notna() & (g.pa > 0)].sort_values(['d', 'lsttrade'])
    p = g.drop_duplicates('d', keep='first').reset_index(drop=True)
    keep, cur = [], pd.Timestamp('1900-01-01')
    for lt in p.lsttrade:
        if lt >= cur: keep.append(True); cur = lt
        else: keep.append(False)
    return p[keep].reset_index(drop=True)


def _val(v, st):
    return v.get(st) if isinstance(v, dict) else v


def _side_thr(spec, mv_side, st):
    """Порог стороны по каждому дню (mv_side — ход «в сторону»: рост для лонга, падение для шорта; иначе NaN)."""
    n = len(mv_side)
    if spec is None:
        return np.full(n, np.inf), False
    if spec['type'] == 'fixed':
        v = _val(spec['value'], st)
        return np.full(n, np.inf if v is None else v), bool(spec.get('strict'))
    s = pd.Series(mv_side)
    thr = s.shift(1).rolling(spec['window'], min_periods=spec['min_obs']).quantile(spec['q']).values
    return thr, False                                   # NaN-порог = сторона молчит (истории мало)


def _straightness(st, p, pts):
    cols = [store.price(st, 'close', store.hm(t)).reindex(pd.MultiIndex.from_arrays([p.secid, p.d])).values for t in pts]
    P = np.column_stack(cols).astype(float)
    seg = P[:, 1:] / P[:, :-1] - 1
    path = np.abs(seg).sum(1)
    return np.abs(P[:, -1] / P[:, 0] - 1) / np.where(path > 0, path, np.nan)


def _px(st, p_secid, p_d, kind, minute):
    s = store.price(st, 'close', minute) if kind == 'close' else store.price(st, 'first_open', minute + 5)
    return s.reindex(pd.MultiIndex.from_arrays([p_secid, p_d])).values.astype(float)


def signals(rule, universe=None, since=None, until=None):
    """Журнал решений: каждая строка — день бумаги в ряду. Сделка = side != 0 и tradable."""
    r = R.load(rule)
    a, b = store.hm(r['signal']['from']), store.hm(r['signal']['to'])
    t_in = b + r['entry']['delay_min']; t_out = store.hm(r['exit']['at']) + r['exit']['delay_min']
    out = []
    for st in (universe or r['universe']):
        p = chain(st, a, b)
        if len(p) < MIN_ROWS: continue
        mv = (p.pb / p.pa - 1).values
        up = np.where(mv > 0, mv, np.nan); dn = np.where(mv < 0, -mv, np.nan)
        qu, su = _side_thr(r.get('long'), up, st); qd, sd = _side_thr(r.get('short'), dn, st)
        with np.errstate(invalid='ignore'):
            lg = (mv > qu) if su else (mv >= qu)
            sh = (-mv > qd) if sd else (-mv >= qd)
        side = np.where(lg, 1., np.where(sh, -1., 0.))
        straight = np.full(len(p), np.nan)
        for f in r['filters']:
            if f['type'] != 'straightness': raise ValueError(f['type'])
            straight = _straightness(st, p, f['points'])
            lo = _val(f.get('min'), st); hi = f.get('max')
            ok = np.isfinite(straight) & (straight >= (lo if lo is not None else -np.inf))
            if hi is not None: ok &= straight <= hi
            side = np.where(ok, side, 0.)
        # ночь
        nxt_sec = np.roll(p.secid.values, -1); nxt_d = pd.Series(p.d).shift(-1)
        gap = (nxt_d - p.d).dt.days.values
        px_in = _px(st, p.secid, p.d, r['entry']['price'], t_in)
        px_out = _px(st, p.secid, nxt_d, r['exit']['price'], t_out)
        skip = np.full(len(p), '', dtype=object)
        skip[~np.isfinite(px_out)] = 'нет цены выхода'
        skip[~np.isfinite(px_in)] = 'нет цены входа'
        skip[gap > MAX_GAP_DAYS] = f'разрыв > {MAX_GAP_DAYS} дней'
        skip[nxt_sec != p.secid.values] = 'завтра другой контракт'
        skip[-1] = 'последний день ряда'
        df = pd.DataFrame({'st': st, 'd': p.d.values, 'secid': p.secid.values, 'pa': p.pa.values, 'pb': p.pb.values,
                           'move': mv, 'thr_up': np.where(np.isfinite(qu), qu, np.nan),
                           'thr_dn': np.where(np.isfinite(qd), qd, np.nan), 'straight': straight, 'side': side,
                           'tradable': skip == '', 'skip': skip, 'px_in': px_in, 'd_out': nxt_d.values,
                           'px_out': px_out})
        df['ret'] = df.px_out / df.px_in - 1
        out.append(df)
    S = pd.concat(out, ignore_index=True)
    if since: S = S[S.d >= since]
    if until: S = S[S.d <= until]
    return S.reset_index(drop=True)


def trades(S):
    T = S[(S.side != 0) & S.tradable].copy()
    T['gross'] = T.side * T.ret
    return T.sort_values(['d', 'st']).reset_index(drop=True)
