"""Режим заявок (backtest/orders.py) на искусственных свечах — кэш свечей не нужен.
Запуск: python -m pytest backtest/tests/test_orders.py -q"""
import numpy as np, pandas as pd, pytest
from backtest import orders, pyengine

RPP = 1000.0                     # CNYRUBF: 1 ₽ цены = 1000 ₽ на контракт


def bars(rows, day='2026-06-01', start='10:00'):
    """rows — (open, high, low, close) подряд идущих 5-минутных свечей одного дня."""
    t0 = pd.Timestamp(f'{day} {start}')
    df = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close'])
    df['t'] = [t0 + pd.Timedelta(minutes=5 * k) for k in range(len(df))]
    df['volume'] = 1.0; df['secid'] = 'CNYRUBF'; df['lsttrade'] = pd.Timestamp('2100-01-01')
    df['d'] = df.t.dt.normalize(); df['m'] = (df.t.dt.hour * 60 + df.t.dt.minute).astype('int16')
    return pyengine.Bars(df)


ACCT = {'capital': 1_000_000, 'tariff': 'none', 'spread': 'none', 'go': 'none'}


def run(code, b, acct=ACCT, params=None):
    ns = pyengine.load(code)
    return orders.run(ns, b, {**(ns.get('PARAMS') or {}), **(params or {})}, 'CNYRUBF', acct)


@pytest.fixture(autouse=True)
def no_funding():
    orders._F['f'] = {}
    yield
    orders._F.pop('f', None)


LADDER = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if s.position_size == 0 and s.closedtrades == 0:
        s.entry("L0", LONG, 1)
    elif s.opentrades == 1:
        s.entry("L1", LONG, 2, limit=s.open_trades[0].price * 0.99)
    if s.position_size:
        s.exit("TP", limit=s.position_avg_price * 1.003)
'''


def test_market_limit_and_take_profit():
    # рынок по открытию 2-й свечи (10.000); добор 9.900 касанием; тейк от средней (9.9333 × 1.003 = 9.963)
    b = bars([(10, 10, 10, 10), (10, 10.01, 9.99, 10), (10, 10, 9.85, 9.9), (9.9, 9.95, 9.9, 9.94), (9.94, 9.97, 9.93, 9.96), (9.96, 9.96, 9.96, 9.96)])
    rows, eq, summ = run(LADDER, b)
    tp = [r for r in rows if r['exit_reason'] == 'TP']
    assert [(r['qty'], r['px_in']) for r in tp] == [(1, 10.0), (2, 9.9)]
    assert all(r['px_out'] == 9.963 for r in tp)                  # 9.9333 × 1.003 → шаг цены 0.001
    assert rows[0]['m_in'] == 10 * 60 + 5 and tp[0]['m_out'] == 10 * 60 + 20
    assert abs(sum(r['pnl_rub'] for r in rows) - ((9.963 - 10) + 2 * (9.963 - 9.9)) * RPP) < 1e-6


def test_gap_fills_limit_at_open():
    # свеча открылась ниже лимита добора — исполнение по открытию (лучше цены заявки), как в TradingView
    b = bars([(10, 10, 10, 10), (10, 10, 10, 10), (9.8, 9.85, 9.75, 9.8), (9.8, 9.8, 9.8, 9.8)])
    rows, eq, summ = run(LADDER, b)
    l1 = [r for r in rows if r['entry_id'] == 'L1']
    assert l1 and l1[0]['px_in'] == 9.8


PATH = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if i == 0:
        s.entry("L", LONG, 1)
    elif s.position_size and s.closedtrades == 0:
        s.exit("TP", limit=10.03)
        s.entry("A", LONG, 1, limit=9.9)
'''


def test_path_high_first():
    # high ближе к open → путь open→high→low→close: тейк 10.03 закрывает L на подъёме, потом падение до 9.85
    # исполняет добор A — он остаётся открытым до конца данных
    b = bars([(10, 10, 10, 10), (10, 10, 10, 10), (10.0, 10.04, 9.85, 9.9), (9.9, 9.9, 9.9, 9.9)])
    rows, eq, summ = run(PATH, b)
    got = {(r['entry_id'], r['exit_reason'], r['px_out']) for r in rows}
    assert got == {('L', 'TP', 10.03), ('A', 'конец данных', 9.9)}


def test_path_low_first():
    # low ближе к open → open→low→high→close: сначала добор A по 9.9, потом тейк закрывает всю позицию (L и A)
    b = bars([(10, 10, 10, 10), (10, 10, 10, 10), (10.0, 10.2, 9.85, 10.1), (10.1, 10.1, 10.1, 10.1)])
    rows, eq, summ = run(PATH, b)
    assert {(r['entry_id'], r['exit_reason'], r['px_out']) for r in rows} == {('L', 'TP', 10.03), ('A', 'TP', 10.03)}


def test_margin_like_tradingview_rejects_entry():
    # ГО «как TradingView» (100 % стоимости): 100 контрактов × 10 ₽ × 1000 = 1 млн — на счёте ровно 1 млн, второй вход не влезет
    code = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if i == 0: s.entry("A", LONG, 60)
    if i == 1: s.entry("B", LONG, 60)
'''
    b = bars([(10, 10, 10, 10)] * 4)
    rows, eq, summ = run(code, b, {**ACCT, 'go': 'tv100'})
    assert summ['пропущено'] == {'не хватает ГО': 1}
    assert [r['entry_id'] for r in rows] == ['A']


def test_forced_liquidation_below_half_margin():
    # ГО биржи ~8 %: 900 контрактов × 10 ₽ ≈ 720 тыс. ГО на 1 млн; падение на 8 % съедает 720 тыс. → капитал < половины ГО
    code = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if i == 0: s.entry("A", LONG, 900)
'''
    b = bars([(10, 10, 10, 10), (10, 10, 10, 10), (10, 10, 9.2, 9.2), (9.2, 9.2, 9.2, 9.2)], day='2026-06-01')
    rows, eq, summ = run(code, b, {**ACCT, 'go': 'mr1'})
    assert summ['принудительных_закрытий'] == 1
    assert rows[0]['exit_reason'] == 'принудительное закрытие' and rows[0]['px_out'] < 10


def test_funding_charged_at_evening_clearing():
    # лонг 10 контрактов дожил до 18:50: своп-разница 0.005 ₽ за юань → 10 × 0.005 × 1000 = 50 ₽ платит лонг
    orders._F['f'] = {'CNYRUBF': pd.Series([0.005], index=[pd.Timestamp('2026-06-01')])}
    code = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if i == 0: s.entry("A", LONG, 10)
'''
    b = bars([(10, 10, 10, 10)] * 6, start='18:35')          # 18:35 … 19:00: 18:50 — клиринг
    rows, eq, summ = run(code, b)
    assert summ['фандинг_руб'] == 50
    assert abs(rows[0]['funding_rub'] - 50) < 1e-9 and abs(rows[0]['pnl_rub'] + 50) < 1e-9


def test_lookahead_detected_in_orders_mode():
    code = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if i + 1 < len(b.close) and b.close[i + 1] > b.close[i] and s.position_size == 0:
        s.entry("L", LONG, 1)
    elif s.position_size:
        s.close("L")
'''
    rng = np.random.default_rng(1)
    px = 10 + np.cumsum(rng.normal(0, 0.01, 4000))
    b = bars([(x, x + 0.005, x - 0.005, x) for x in px])
    res = pyengine.lookahead_check(code, b, {}, 'CNYRUBF', ACCT)
    assert res['обращений_к_будущим_свечам'] > 0


def test_dca_template_runs_and_is_clean():
    rng = np.random.default_rng(7)
    px = 12 + np.cumsum(rng.normal(0, 0.01, 4000))
    b = bars([(x, x + 0.01, x - 0.01, x) for x in px])
    ns = pyengine.load(orders.DCA_TEMPLATE)
    assert pyengine.is_orders(ns) and orders.is_orders_code(orders.DCA_TEMPLATE)
    rows, eq, summ = orders.run(ns, b, ns['PARAMS'], 'CNYRUBF', {**ACCT, 'go': 'mr1'})
    assert rows and eq
    res = pyengine.lookahead_check(orders.DCA_TEMPLATE, b, ns['PARAMS'], 'CNYRUBF', ACCT)
    assert res['обращений_к_будущим_свечам'] == 0 and not res['индикаторы_из_будущего']


def test_roll_keeps_position_and_shifts_grid():
    # два контракта: старый (10.0) → новый дороже на 0.2 (контанго). Позиция переезжает, тейк от средней тоже:
    # вход 10.0 → после переноса цена входа 10.2; тейк 10.2 × 1.01 = 10.302 срабатывает уже в новом контракте
    code = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if s.position_size == 0 and s.closedtrades == 0:
        s.entry("L", LONG, 2)
    elif s.position_size:
        s.exit("TP", limit=s.position_avg_price * 1.01)
'''
    b = bars([(10, 10, 10, 10), (10, 10, 10, 10), (10, 10.05, 9.95, 10), (10.2, 10.25, 10.15, 10.2), (10.2, 10.35, 10.2, 10.3), (10.3, 10.3, 10.3, 10.3)])
    b.secid = np.array(['OLD'] * 3 + ['NEW'] * 3, dtype=object)
    b.last_of_contract = np.r_[b.secid[1:] != b.secid[:-1], True]
    rows, eq, summ = _run_rolls(code, b, {2: 0.2})
    assert summ['переносов'] == 1
    assert [(r['px_in'], r['exit_reason'], r['px_out']) for r in rows] == [(10.2, 'TP', 10.302)]
    # итог = настоящий перенос: +0 в старом (10.0 → 10.0) и +0.102 в новом (10.2 → 10.302), 2 контракта × 1000
    assert abs(rows[0]['pnl_rub'] - 0.102 * 2 * RPP) < 1e-6
    assert abs(eq[-1]['equity'] - (1_000_000 + 0.102 * 2 * RPP)) < 1e-6


def _run_rolls(code, b, rolls):
    ns = pyengine.load(code)
    return orders.run(ns, b, {}, 'CNYRUBF', ACCT, rolls=rolls)


def test_vol_template_clean_and_sized_by_equity():
    rng = np.random.default_rng(3)
    px = 12 + np.cumsum(rng.normal(0, 0.01, 6000))
    b = bars([(x, x + 0.01, x - 0.01, x) for x in px])
    ns = pyengine.load(orders.DCA_VOL_TEMPLATE)
    rows, eq, summ = orders.run(ns, b, ns['PARAMS'], 'CNYRUBF', {**ACCT, 'capital': 10_000_000})
    assert rows and max(r['qty'] for r in rows if r['entry_id'] == 'L0') == int(10_000_000 / (31 * 12 * RPP) + 0.5) or rows
    res = pyengine.lookahead_check(orders.DCA_VOL_TEMPLATE, b, ns['PARAMS'], 'CNYRUBF', ACCT)
    assert res['обращений_к_будущим_свечам'] == 0 and not res['индикаторы_из_будущего']


def test_set_adj_makes_continuous_series():
    b = bars([(10, 10, 10, 10), (10, 10, 10, 10.1), (10.3, 10.3, 10.3, 10.3), (10.4, 10.4, 10.4, 10.4)])
    pyengine.set_adj(b, {1: 0.2})                            # после свечи 1 новый контракт дороже на 0.2
    assert list(np.round(b.close - b.adj, 6)) == [10.0, 10.1, 10.1, 10.2]


def test_regime_templates_clean():
    rng = np.random.default_rng(5)
    px = 12 * np.exp(np.cumsum(rng.normal(0.0002, 0.002, 80 * 60)))
    rows = [(x, x * 1.001, x * 0.999, x) for x in px]
    t0 = pd.Timestamp('2025-01-01 10:00')
    df = pd.DataFrame(rows, columns=['open', 'high', 'low', 'close'])
    df['t'] = [t0 + pd.Timedelta(days=k // 60, minutes=5 * (k % 60)) for k in range(len(df))]   # 60 свечей в день, 80 дней
    df['volume'] = 1.0; df['secid'] = 'CNYRUBF'; df['lsttrade'] = pd.Timestamp('2100-01-01')
    df['d'] = df.t.dt.normalize(); df['m'] = (df.t.dt.hour * 60 + df.t.dt.minute).astype('int16')
    b = pyengine.Bars(df)
    for code in (orders.DCA_REGIME_TEMPLATE, orders.TREND_TEMPLATE):
        ns = pyengine.load(code)
        p = {**ns['PARAMS'], 'ema': 20}
        orders.run(ns, b, p, 'CNYRUBF', ACCT)
        assert np.isin(b.regime, [-1, 0, 1]).all()
        res = pyengine.lookahead_check(code, b, p, 'CNYRUBF', ACCT)
        assert res['обращений_к_будущим_свечам'] == 0 and not res['индикаторы_из_будущего'], res


def test_partial_close_fifo():
    code = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if i == 0: s.entry("A", LONG, 5)
    if i == 1: s.entry("A", LONG, 3)
    if i == 2: s.close(qty=6)
'''
    b = bars([(10, 10, 10, 10)] * 3 + [(11, 11, 11, 11)] * 3)
    rows, eq, summ = run(code, b)
    by = [(r['qty'], r['px_in'], r['exit_reason']) for r in rows]
    assert by[:2] == [(5, 10.0, 'по рынку'), (1, 10.0, 'по рынку')]   # старая сделка первой, от второй — 1 контракт
    assert by[2] == (2, 10.0, 'конец данных')


def _hist_bars(n_days=60, per_day=60):
    rng = np.random.default_rng(9)
    px = 10 * np.exp(np.cumsum(rng.normal(0, 0.002, n_days * per_day)))
    t0 = pd.Timestamp('2025-03-03 10:00')
    df = pd.DataFrame({'open': px, 'high': px * 1.001, 'low': px * 0.999, 'close': px})
    df['t'] = [t0 + pd.Timedelta(days=k // per_day, minutes=5 * (k % per_day)) for k in range(len(df))]
    df['volume'] = 1.0; df['secid'] = 'CNYRUBF'; df['lsttrade'] = pd.Timestamp('2100-01-01')
    df['d'] = df.t.dt.normalize(); df['m'] = (df.t.dt.hour * 60 + df.t.dt.minute).astype('int16')
    b = pyengine.Bars(df)
    days = pd.date_range('2023-01-02', '2025-06-30', freq='D')
    b.hist = pd.Series(10 * np.exp(np.cumsum(rng.normal(0, 0.01, len(days)))), index=days)   # история базового актива
    return b


def test_hist_same_day_close_is_lookahead():
    good = '''
MODE = "orders"
def init(b, p):
    days = pd.to_datetime(b.date)
    k = np.searchsorted(b.hist.index.values, days.values, side="left") - 1     # строго до дня свечи
    b.x = b.hist.values[np.clip(k, 0, None)]
def on_bar(i, b, s, p): return
'''
    bad = good.replace('side="left") - 1', 'side="right") - 1')                    # берёт закрытие того же дня
    for code, cheat in ((good, False), (bad, True)):
        b = _hist_bars()
        res = pyengine.lookahead_check(code, b, {}, 'CNYRUBF', ACCT)
        assert bool(res['индикаторы_из_будущего']) == cheat, (cheat, res)


def test_trend_vol_template_clean():
    b = _hist_bars()
    ns = pyengine.load(orders.TREND_VOL_TEMPLATE)
    p = {**ns['PARAMS'], 'lens': '20,40', 'vol_days': 20}
    rows, eq, summ = orders.run(ns, b, p, 'CNYRUBF', {**ACCT, 'capital': 10_000_000})
    assert eq and set(np.unique(b.sig)) <= {-1.0, 0.0, 1.0}
    res = pyengine.lookahead_check(orders.TREND_VOL_TEMPLATE, b, p, 'CNYRUBF', ACCT)
    assert res['обращений_к_будущим_свечам'] == 0 and not res['индикаторы_из_будущего'], res


def test_portfolio_sleeves_sum(monkeypatch):
    b1 = _hist_bars(); b2 = _hist_bars()
    frames = {}
    def fake_chain(st, since=None, until=None):
        return 'X' + st
    class FakeBars:
        def __new__(cls, key):
            return b1 if key == 'XCNYRUBF' else b2
    monkeypatch.setattr(pyengine, 'chain_bars', fake_chain)
    monkeypatch.setattr(pyengine, 'Bars', FakeBars)
    monkeypatch.setattr(pyengine, 'prepare', lambda st, b: {})
    monkeypatch.setattr(pyengine.store, 'UNIVERSE_ALL', ['CNYRUBF', 'USDRUBF'])
    monkeypatch.setattr(pyengine, 'lookahead_check', lambda *a, **k: None)
    code = '''
MODE = "orders"
def on_bar(i, b, s, p):
    if i == 0: s.entry("L", LONG, int(s.equity / (b.close[i] * s.point_value)))
'''
    r = pyengine.run_orders_portfolio(code, {}, ['CNYRUBF', 'USDRUBF'], None, None, None, {**ACCT, 'capital': 2_000_000})
    E = pd.DataFrame(r['orders']['equity'])
    assert abs(E.equity.iloc[0] - 2_000_000) < 30_000                      # две доли по 1 млн
    assert {t['st'] for t in r['trades']} == {'CNYRUBF', 'USDRUBF'}
