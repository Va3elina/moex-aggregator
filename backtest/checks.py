"""Батарея проверок стратегии — отвечает на вопрос «это закономерность или подгонка?». Перенос research/lab/checks.py
(без scipy: нормальное распределение — из стандартной библиотеки).

  halves     две половины истории — не усох ли результат
  by_year    плюс в каждом году или держится на одном
  placebo    стороны сделок перемешаны между днями внутри бумаги — связь «ход дня → ночь» разорвана, остальное то же
  deciles    продолжение ночью по децилям силы дневного хода — есть ли лестница
  tails      без 5 % лучших (и худших) дней — держится ли результат на редких ночах
  delay      вход позже на 5…30 минут — насколько результат зависит от скорости исполнения
  neighbors  соседние параметры — острый пик или плато; по ним DSR и PBO (Bailey & López de Prado)
"""
import copy, itertools
from math import comb
from statistics import NormalDist
import numpy as np, pandas as pd
from . import engine, costs, rules as R
from .metrics import tstat

N01 = NormalDist()


def _daily(T):
    """Дневная доходность «портфеля сигналов»: капитал делится поровну между сделками дня; дни без сделок — 0."""
    P = T.groupby('d').net.mean()
    return P.reindex(pd.bdate_range(T.d.min(), T.d.max()), fill_value=0.0)


def _sharpe(r):
    return float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0.0


def halves(T):
    split = T.d.sort_values().iloc[len(T) // 2]
    a, b = T[T.d < split], T[T.d >= split]
    return {'граница': str(split.date()), 'до_%': round(100 * a.net.mean(), 4), 'после_%': round(100 * b.net.mean(), 4),
            'n_до': len(a), 'n_после': len(b), 't_до': tstat(a.net), 't_после': tstat(b.net)}


def by_year(T):
    g = T.groupby(T.d.dt.year).net
    return [{'год': int(y), 'сделок': int(n), 'на_сделку_%': round(100 * m, 4), 'винрейт_%': round(100 * w, 1)}
            for y, n, m, w in zip(g.size().index, g.size(), g.mean(), g.apply(lambda x: (x > 0).mean()))]


def placebo(S, tariff, spread, n=300, seed=0):
    P = S[S.tradable & np.isfinite(S.ret)].reset_index(drop=True)
    T = costs.apply(engine.trades(P), tariff, spread)
    actual = T.net.mean()
    cost = pd.Series(costs.apply(P.assign(gross=0.0), tariff, spread).eval('comm + spread').values)
    rng = np.random.default_rng(seed)
    groups = [np.where(P.st.values == st)[0] for st in P.st.unique()]
    side, ret, c = P.side.values, P.ret.values, cost.values
    sims = np.empty(n)
    for i in range(n):
        s = side.copy()
        for g in groups: s[g] = rng.permutation(s[g])
        m = s != 0
        sims[i] = (s[m] * ret[m] - c[m]).mean()
    return {'факт_%': round(100 * actual, 4), 'плацебо_среднее_%': round(100 * sims.mean(), 4),
            'плацебо_95_%': round(100 * float(np.quantile(sims, 0.95)), 4), 'p': float((sims >= actual).mean()), 'n': n}


def deciles(S, win=250):
    P = S[S.tradable & np.isfinite(S.ret)].sort_values(['st', 'd'])
    rk = P.groupby('st').move.transform(lambda s: s.abs().rolling(win + 1, min_periods=100).rank(pct=True))
    P = P.assign(rank=rk).dropna(subset=['rank'])
    P['dec'] = np.minimum((P['rank'] * 10).astype(int), 9) + 1
    P['cont'] = np.sign(P.move) * P.ret
    g = P.groupby('dec').cont
    return [{'дециль': int(k), 'дней': int(n), 'продолжение_bp': round(1e4 * m, 2)} for k, n, m in zip(g.size().index, g.size(), g.mean())]


def tails(T, q=0.05):
    r = _daily(T); cut = r[r != 0]
    hi, lo = cut.quantile(1 - q), cut.quantile(q)
    return {'Шарп': round(_sharpe(r), 2), 'Шарп_без_лучших': round(_sharpe(r.where(r < hi, 0.0)), 2),
            'Шарп_без_обоих_хвостов': round(_sharpe(r.where((r < hi) & (r > lo), 0.0)), 2),
            'доля_дохода_в_лучших_%': round(100 * float(r[r >= hi].sum() / r.sum()), 1) if r.sum() else None}


def delay(rule, universe, since, until, tariff, spread, steps=(0, 5, 10, 15, 30, 60)):
    out = []
    for m in steps:
        r = R.with_exec(rule, {'entry': {'price': 'next_open', 'delay_min': m}, 'exit': {'price': 'next_open', 'delay_min': 0}})
        T = costs.apply(engine.trades(engine.signals(r, universe, since, until)), tariff, spread)
        out.append({'задержка_мин': m, 'сделок': len(T), 'на_сделку_%': round(100 * T.net.mean(), 4) if len(T) else None})
    return out


def _variants(rule):
    """Соседи по параметрам: квантили и окно — для порогов-квантилей, величина — для постоянных порогов."""
    grid = {}
    for side in ('long', 'short'):
        s = rule.get(side)
        if not s: continue
        if s['type'] == 'quantile':
            grid[(side, 'q')] = sorted({round(min(0.97, max(0.5, s['q'] + d)), 2) for d in (-0.08, 0, 0.08)})
        elif not isinstance(s['value'], dict):
            grid[(side, 'value')] = sorted({round(max(0.0, s['value'] * k), 5) for k in (0.5, 1, 1.5)} | ({0.005} if s['value'] == 0 else set()))
    if any((rule.get(x) or {}).get('type') == 'quantile' for x in ('long', 'short')):
        w = next((rule[x]['window'] for x in ('long', 'short') if (rule.get(x) or {}).get('type') == 'quantile'))
        grid[('*', 'window')] = sorted({max(60, w // 2), w, w * 2})
    return grid


def neighbors(rule, universe, since, until, tariff, spread):
    grid = _variants(rule); keys = list(grid)
    if not keys: return None
    rows, cols = [], {}
    for vals in itertools.product(*grid.values()):
        rv = copy.deepcopy(rule); name = []
        for (side, k), v in zip(keys, vals):
            for sd in (('long', 'short') if side == '*' else (side,)):
                if rv.get(sd) and (k != 'window' or rv[sd]['type'] == 'quantile'):
                    rv[sd][k] = v
                    if k == 'window': rv[sd]['min_obs'] = min(rv[sd].get('min_obs', 100), max(40, v // 3))
            name.append(f"{'' if side == '*' else ('лонг ' if side == 'long' else 'шорт ')}{'окно' if k == 'window' else 'порог'} {v}")
        T = costs.apply(engine.trades(engine.signals(rv, universe, since, until)), tariff, spread)
        if len(T) < 30: continue
        r = _daily(T); nm = ' · '.join(name)
        base = all(rv.get(sd) == rule.get(sd) for sd in ('long', 'short'))
        rows.append({'вариант': nm, 'базовый': base, 'сделок': len(T), 'на_сделку_%': round(100 * T.net.mean(), 4),
                     't': round(tstat(T.net) or 0, 2), 'Шарп': round(_sharpe(r), 2)}); cols[nm] = r
    if len(cols) < 3: return {'таблица': rows}
    M = pd.DataFrame(cols).fillna(0.0)
    base = next((x['вариант'] for x in rows if x['базовый']), rows[0]['вариант'])
    return {'таблица': rows, 'DSR': _dsr(M, base), 'PBO': _pbo(M)}


def _dsr(M, col):
    X = M.values; sd = X.std(0); srs = np.where(sd > 0, X.mean(0) / np.where(sd > 0, sd, 1), 0.0)
    N, T = X.shape[1], X.shape[0]; v = M[col].values
    z = (v - v.mean()) / v.std(); g3 = float((z ** 3).mean()); g4 = float((z ** 4).mean()); sr = v.mean() / v.std()
    gam = 0.5772156649
    sr0 = np.sqrt(np.var(srs)) * ((1 - gam) * N01.inv_cdf(1 - 1 / N) + gam * N01.inv_cdf(1 - 1 / (N * np.e)))
    den = np.sqrt(max(1e-12, 1 - g3 * sr + (g4 - 1) / 4 * sr ** 2))
    return {'DSR': round(N01.cdf(float((sr - sr0) * np.sqrt(T - 1) / den)), 3), 'вариантов': int(N),
            'Шарп_базового': round(float(sr * np.sqrt(252)), 2), 'Шарп_шума': round(float(sr0 * np.sqrt(252)), 2)}


def _pbo(M, S=16):
    n = len(M) // S * S; X = M.values[:n]; B = np.array_split(np.arange(n), S)
    s1 = np.array([X[b].sum(0) for b in B]); s2 = np.array([(X[b] ** 2).sum(0) for b in B]); cnt = np.array([len(b) for b in B], float)
    C = np.array([[i in c for i in range(S)] for c in itertools.combinations(range(S), S // 2)], float)

    def sh(W):
        k = W @ cnt; m = (W @ s1) / k[:, None]; var = (W @ s2) / k[:, None] - m ** 2
        return m / np.sqrt(np.maximum(var, 1e-18))
    tr, te = sh(C), sh(1 - C); best = tr.argmax(1)
    ranks = (te < te[np.arange(len(te)), best][:, None]).sum(1) / (X.shape[1] - 1)
    return {'PBO': round(float((ranks < 0.5).mean()), 3), 'разбиений': comb(S, S // 2)}


def run_all(s, S, T):
    """s — нормализованная спецификация прогона, S — журнал решений, T — сделки с издержками."""
    rule, uni, tariff, spread = s['rule'], s['universe'], s['tariff'], s['spread']
    out = {'halves': halves(T), 'by_year': by_year(T), 'placebo': placebo(S, tariff, spread), 'deciles': deciles(S), 'tails': tails(T)}
    out['delay'] = delay(rule, uni, s['since'], s['until'], tariff, spread)
    out['neighbors'] = neighbors(rule, uni, s['since'], s['until'], tariff, spread)
    return out
