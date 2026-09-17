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


def _risk_walk(st, df, ex, first_min, last_min, inclusive):
    """Досрочный выход по 5-минутным свечам: стоп, тейк, трейлинг. Условие проверяется по ЗАКРЫТИЮ свечи (так видит рынок
    робот, который просыпается раз в 5 минут), выход — по открытию следующей. Свечи выходных пропускаются: робот в
    выходные не работает. Меняет px_out / d_out / m_out / exit_reason у сработавших строк."""
    stop, take, trail = ex.get('stop'), ex.get('take'), ex.get('trail')
    B = store.bars(st)
    B = B[B.d.dt.dayofweek < 5]
    sec_arr = B.secid.values
    order = np.argsort(sec_arr, kind='stable')
    sec_sorted = sec_arr[order]
    uniq, start = np.unique(sec_sorted, return_index=True)
    bounds = dict(zip(uniq, zip(start, list(start[1:]) + [len(sec_sorted)])))
    T = B.t.values.astype('datetime64[m]').astype('int64')[order]          # минуты от эпохи
    O, C = B.open.values[order], B.close.values[order]
    day0 = np.datetime64('1970-01-01')
    rows = df.index[(df.side != 0) & df.tradable]
    for i in rows:
        sec, side, pin = df.at[i, 'secid'], df.at[i, 'side'], df.at[i, 'px_in']
        if sec not in bounds or not np.isfinite(pin): continue
        a, b = bounds[sec]
        t0 = (np.datetime64(df.at[i, 'd'], 'D') - day0).astype('int64') * 1440 + first_min
        t1 = (np.datetime64(df.at[i, 'd_out'], 'D') - day0).astype('int64') * 1440 + last_min
        lo = a + np.searchsorted(T[a:b], t0, 'left'); hi = a + np.searchsorted(T[a:b], t1, 'right' if inclusive else 'left')
        if hi <= lo: continue
        r = side * (C[lo:hi] / pin - 1)
        hit = np.zeros(len(r), bool); why = np.full(len(r), '', dtype=object)
        if trail:
            best = np.maximum.accumulate(np.maximum(r, 0.0)); m = (best - r) >= trail; why[m] = 'трейлинг'; hit |= m
        if take: m = r >= take; why[m] = 'тейк'; hit |= m
        if stop: m = r <= -stop; why[m] = 'стоп'; hit |= m
        if not hit.any(): continue
        j = lo + int(np.argmax(hit))
        if j + 1 >= b or T[j + 1] > t1: continue                            # следующей свечи до планового выхода нет — выходим по плану
        tx = T[j + 1]
        df.at[i, 'px_out'] = O[j + 1]; df.at[i, 'exit_reason'] = why[j - lo]
        df.at[i, 'd_out'] = pd.Timestamp(day0 + np.timedelta64(int(tx // 1440), 'D')); df.at[i, 'm_out'] = int(tx % 1440)


def _excursions(st, df, first_min):
    """Благоприятное и неблагоприятное отклонение сделки (MFE / MAE), доли от цены входа: насколько цена уходила в нашу
    сторону и против нас, пока позиция была открыта (по максимумам и минимумам 5-минутных свечей, будни)."""
    df['mfe'] = np.nan; df['mae'] = np.nan
    rows = df.index[(df.side != 0) & df.tradable]
    if not len(rows): return
    B = store.bars(st); B = B[B.d.dt.dayofweek < 5]
    sec_arr = B.secid.values; order = np.argsort(sec_arr, kind='stable'); sec_sorted = sec_arr[order]
    uniq, start = np.unique(sec_sorted, return_index=True)
    bounds = dict(zip(uniq, zip(start, list(start[1:]) + [len(sec_sorted)])))
    T = B.t.values.astype('datetime64[m]').astype('int64')[order]
    H, L = B.high.values.astype(float)[order], B.low.values.astype(float)[order]
    day0 = np.datetime64('1970-01-01')
    for i in rows:
        sec, side, pin = df.at[i, 'secid'], df.at[i, 'side'], df.at[i, 'px_in']
        if sec not in bounds or not np.isfinite(pin) or pd.isna(df.at[i, 'd_out']): continue
        a, b = bounds[sec]
        t0 = (np.datetime64(df.at[i, 'd'], 'D') - day0).astype('int64') * 1440 + first_min
        t1 = (np.datetime64(df.at[i, 'd_out'], 'D') - day0).astype('int64') * 1440 + int(df.at[i, 'm_out'])
        lo = a + np.searchsorted(T[a:b], t0, 'left'); hi = a + np.searchsorted(T[a:b], t1, 'left')
        if hi <= lo: df.at[i, 'mfe'] = 0.0; df.at[i, 'mae'] = 0.0; continue
        up, dn = np.nanmax(H[lo:hi]) / pin - 1, np.nanmin(L[lo:hi]) / pin - 1
        df.at[i, 'mfe'] = max(0.0, up if side > 0 else -dn); df.at[i, 'mae'] = min(0.0, dn if side > 0 else -up)


def signals(rule, universe=None, since=None, until=None):
    """Журнал решений: каждая строка — день бумаги в ряду. Сделка = side != 0 и tradable."""
    r = R.load(rule)
    a, b = store.hm(r['signal']['from']), store.hm(r['signal']['to'])
    t_in = b + r['entry']['delay_min']; t_out = store.hm(r['exit']['at']) + r['exit']['delay_min']
    hold = int(r['exit'].get('hold_days') or 1)
    risky = any(r['exit'].get(k) for k in ('stop', 'take', 'trail'))
    m_in = t_in + 5 if r['entry']['price'] == 'next_open' else t_in            # минута свечи, по которой исполнен вход
    m_out = t_out + 5 if r['exit']['price'] == 'next_open' else t_out
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
        # выход — на hold-й следующий день ряда; всё это время контракт тот же, и ни одного разрыва > 4 дней
        nxt_d = pd.Series(p.d).shift(-hold); sec = pd.Series(p.secid)
        same = np.ones(len(p), bool); gap = np.zeros(len(p))
        for k in range(1, hold + 1):
            same &= (sec.shift(-k) == sec).values
            gap = np.fmax(gap, (pd.Series(p.d).shift(-k) - pd.Series(p.d).shift(-k + 1)).dt.days.values)
        nxt_sec = np.where(same, p.secid.values, '')
        px_in = _px(st, p.secid, p.d, r['entry']['price'], t_in)
        px_out = _px(st, p.secid, nxt_d, r['exit']['price'], t_out)
        skip = np.full(len(p), '', dtype=object)
        skip[~np.isfinite(px_out)] = 'нет цены выхода'
        skip[~np.isfinite(px_in)] = 'нет цены входа'
        skip[gap > MAX_GAP_DAYS] = f'разрыв > {MAX_GAP_DAYS} дней'
        skip[nxt_sec != p.secid.values] = 'завтра другой контракт'
        skip[-hold:] = 'последний день ряда'
        df = pd.DataFrame({'st': st, 'd': p.d.values, 'secid': p.secid.values, 'pa': p.pa.values, 'pb': p.pb.values,
                           'move': mv, 'thr_up': np.where(np.isfinite(qu), qu, np.nan),
                           'thr_dn': np.where(np.isfinite(qd), qd, np.nan), 'straight': straight, 'side': side,
                           'tradable': skip == '', 'skip': skip, 'px_in': px_in, 'd_out': nxt_d.values,
                           'px_out': px_out, 'm_in': m_in, 'm_out': m_out, 'exit_reason': 'время'})
        if risky:
            _risk_walk(st, df, r['exit'], t_in + 5, m_out if r['exit']['price'] == 'next_open' else t_out - 5, True)
        _excursions(st, df, t_in + 5)
        df['ret'] = df.px_out / df.px_in - 1
        out.append(df)
    S = pd.concat(out, ignore_index=True)
    if since: S = S[S.d >= since]
    if until: S = S[S.d <= until]
    return S.reset_index(drop=True)


def trades(S):
    T = S[(S.side != 0) & S.tradable].sort_values(['st', 'd'])
    # по одной позиции на бумагу: пока прошлая сделка не закрыта (удержание несколько дней), новый сигнал пропускается
    keep, last_st, busy_until = [], None, None
    for st, d, d_out in zip(T.st.values, T.d.values, T.d_out.values):
        if st != last_st: last_st, busy_until = st, None
        ok = busy_until is None or d >= busy_until
        keep.append(ok)
        if ok: busy_until = d_out
    T = T[np.array(keep, bool)].copy() if len(T) else T.copy()
    T['gross'] = T.side * T.ret
    return T.sort_values(['d', 'st']).reset_index(drop=True)
