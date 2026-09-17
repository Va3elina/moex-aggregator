"""Один прогон по описанию (spec) — общий код для команд, воркера и тестов.

spec = {"rule": "hybrid7c" | {...правило...}, "exec": "close"|"next_open"|"robot", "universe": [...]|null,
        "since": "2024-01-01"|null, "until": null, "tariff": "trader", "spread": "c3"|"none",
        "capital": 1000000|null, "slots": 6, "go": "mr1"|"snapshot"|"none", "go_limit": 1.0, "name": "...",
        "leverage": 1.0 (капитал × плечо / слоты на сделку), "go_mult": 1.0 (стресс: ГО выросло в k раз)}"""
import pandas as pd
from . import store, rules as R, engine, costs, account, metrics

DEFAULTS = {'exec': 'close', 'universe': None, 'since': None, 'until': None, 'tariff': 'trader', 'spread': 'c3',
            'capital': 1_000_000, 'slots': 6, 'go': 'mr1', 'go_limit': 1.0, 'leverage': 1.0, 'go_mult': 1.0, 'checks': False}
EXEC = {'close': None, 'next_open': R.EXEC_NEXT_OPEN, 'robot': R.EXEC_ROBOT}


def normalize(spec):
    s = {**DEFAULTS, **{k: v for k, v in spec.items() if v is not None}}
    if spec.get('code'):                                    # стратегия кодом: правила нет, бумаги — из запроса
        s['rule'] = {'id': 'python', 'name': spec.get('name') or 'Стратегия на Python', 'universe': R.UNIVERSE_21, 'python': True}
        rule = s['rule']
    else:
        rule = R.load(s['rule'])
        if EXEC[s['exec']]: rule = R.with_exec(rule, EXEC[s['exec']])
        s['rule'] = rule
    s['universe'] = [u for u in (s['universe'] or rule['universe']) if u in store.UNIVERSE_ALL]
    if not s['universe']: raise ValueError('пустой список бумаг')
    if s['tariff'] not in costs.TARIFFS: raise ValueError(f"тариф {s['tariff']}")
    return s


def execute(spec, progress=None, py_trades=None):
    """py_trades — (сделки, сведения) стратегии на Python, уже посчитанные песочницей (см. pybridge / pyengine)."""
    s = normalize(spec)
    if spec.get('code'):
        T0, info = py_trades
        T = costs.apply(T0, s['tariff'], s['spread']) if len(T0) else T0.assign(comm=[], spread=[], net=[])
        T['n'] = range(len(T))
        res = {'kind': 'python', 'period': [str(T.d.min().date()), str(T.d_out.max().date())] if len(T) else None, 'signals': int(len(T)),
               'per_trade': metrics.per_trade(T) if len(T) else {'сделок': 0}, 'python': info,
               'by_instrument': metrics.by_instrument(T).reset_index().to_dict('records') if len(T) else [],
               'data_until': store.data_until(s['universe'])}
        A = K = E = None
        if s['capital'] and len(T):
            A, K, E = account.simulate(T, s['capital'], s['slots'], s['go_limit'], s['go'], s['tariff'], s['spread'],
                                       order=s['universe'], leverage=float(s['leverage']), go_mult=float(s['go_mult']))
            res['account'] = _account_block(A, K, E, s)
        return s, res, None, T, A, K, E
    if spec.get('sweep'):                                   # перебор параметров: без счёта и без строк сделок
        from . import sweep
        s['sweep'] = spec['sweep']
        res = {'kind': 'sweep', 'sweep': sweep.run(s, progress), 'data_until': store.data_until(s['universe']), 'per_trade': {'сделок': 0}}
        return s, res, None, None, None, None, None
    S = engine.signals(s['rule'], s['universe'], s['since'], s['until'])
    T = costs.apply(engine.trades(S), s['tariff'], s['spread'])
    T['n'] = range(len(T))
    res = {'period': [str(S.d.min().date()), str(S.d.max().date())], 'signals': int((S.side != 0).sum()),
           'per_trade': metrics.per_trade(T) if len(T) else {'сделок': 0},
           'by_instrument': metrics.by_instrument(T).reset_index().to_dict('records') if len(T) else [],
           'data_until': store.data_until(s['universe'])}
    if spec.get('checks') and len(T) >= 60:
        from . import checks
        res['checks'] = checks.run_all(s, S, T)
    A = K = E = None
    if s['capital'] and len(T):
        A, K, E = account.simulate(T, s['capital'], s['slots'], s['go_limit'], s['go'], s['tariff'], s['spread'],
                                   order=s['universe'], leverage=float(s['leverage']), go_mult=float(s['go_mult']))
        res['account'] = _account_block(A, K, E, s)
    return s, res, S, T, A, K, E


def _account_block(A, K, E, s):
    return {**metrics.curve(E, s['capital']), 'сделок_исполнено': int(len(A)), 'пропущено': K.reason.value_counts().to_dict(),
            'маржин_коллов': int(E.margin_call.sum()), 'урезано_по_ГО': int(A.go_cut.sum()) if len(A) else 0,
            'по_годам_%': metrics.by_year(A, E, s['capital']),
            'прибыль_руб': round(float(A.pnl_rub.sum())) if len(A) else 0, 'комиссии_руб': round(float(A.comm_rub.sum())) if len(A) else 0,
            'спред_руб': round(float(A.spread_rub.sum())) if len(A) else 0}
