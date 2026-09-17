"""Перебор параметров правила — с защитой от подгонки.

spec["sweep"] = {"grid": {"long.q": [0.6, 0.67, 0.75], "exit.take": [null, 0.03, 0.05]}, "oos_from": "2025-01-01"}
Путь — через точку внутри правила («filters.0.min»). null в списке — «параметр выключен».

Что защищает от подгонки:
  • история делится на обучение (до oos_from) и контроль (после): варианты ранжируются ТОЛЬКО по обучению,
    контрольный период показывается рядом и в выборе лучшего не участвует;
  • считается, сколько вариантов перебрано, и DSR лучшего (Шарп против лучшего из N случайных);
  • ранговая корреляция «обучение ↔ контроль» по всем вариантам: высокая — параметр действительно влияет,
    около нуля — «лучший» вариант выбран шумом.
Счёт (слоты, ГО) здесь не считается — только сделки и издержки: вариант, который понравился, запускается обычным прогоном."""
import copy, itertools
import numpy as np, pandas as pd
from . import engine, costs, checks, rules as R
from .metrics import tstat

MAX_VARIANTS = 150


def set_path(rule, path, value):
    cur = rule; keys = path.split('.')
    for k in keys[:-1]:
        cur = cur[int(k)] if isinstance(cur, list) else cur.setdefault(k, {})
    last = keys[-1]
    if isinstance(cur, list): cur[int(last)] = value
    elif value is None: cur.pop(last, None)
    else: cur[last] = value


def _part(T):
    if len(T) < 5: return {'сделок': int(len(T)), 'на_сделку_%': None, 't': None, 'Шарп': None, 'винрейт_%': None}
    return {'сделок': int(len(T)), 'на_сделку_%': round(100 * T.net.mean(), 4), 't': round(tstat(T.net) or 0, 2),
            'Шарп': round(checks._sharpe(checks._daily(T)), 2), 'винрейт_%': round(100 * (T.net > 0).mean(), 1)}


def _spearman(a, b):
    a, b = pd.Series(a), pd.Series(b); m = a.notna() & b.notna()
    return round(float(a[m].rank().corr(b[m].rank())), 2) if m.sum() >= 4 else None


def run(s, progress=None):
    """s — нормализованная спецификация (runner.normalize) с ключом sweep."""
    sw = s['sweep']; grid = {k: v for k, v in sw['grid'].items() if v}
    keys = list(grid); combos = list(itertools.product(*grid.values()))
    if not combos or len(combos) > MAX_VARIANTS:
        raise ValueError(f'вариантов {len(combos)}: нужно от 1 до {MAX_VARIANTS}')
    oos = pd.Timestamp(sw.get('oos_from') or '2025-01-01')
    rows, cols = [], {}
    for n, vals in enumerate(combos):
        rule = copy.deepcopy(s['rule'])
        for k, v in zip(keys, vals): set_path(rule, k, v)
        rule = R.load(rule)
        T = costs.apply(engine.trades(engine.signals(rule, s['universe'], s['since'], s['until'])), s['tariff'], s['spread'])
        a, b = T[T.d < oos], T[T.d >= oos]
        rows.append({'params': dict(zip(keys, vals)), 'обучение': _part(a), 'контроль': _part(b)})
        if len(a) >= 30: cols[n] = checks._daily(a)
        if progress: progress(n + 1, len(combos))
    rows_ok = [r for r in rows if r['обучение']['Шарп'] is not None]
    rows_ok.sort(key=lambda r: -r['обучение']['Шарп'])
    for i, r in enumerate(rows_ok): r['место_обучение'] = i + 1
    for i, r in enumerate(sorted([r for r in rows_ok if r['контроль']['Шарп'] is not None], key=lambda r: -r['контроль']['Шарп'])): r['место_контроль'] = i + 1
    out = {'grid': grid, 'oos_from': str(oos.date()), 'вариантов': len(combos), 'rows': rows_ok,
           'ранговая_корреляция': _spearman([r['обучение']['Шарп'] for r in rows_ok], [r['контроль']['Шарп'] for r in rows_ok])}
    if len(cols) >= 3 and rows_ok:
        M = pd.DataFrame(cols).fillna(0.0)
        best = next(n for n, vals in enumerate(combos) if dict(zip(keys, vals)) == rows_ok[0]['params'])
        if best in M.columns: out['DSR_лучшего'] = checks._dsr(M, best)
    return out
