"""Приёмочные числа этапа 1 (PLAN.md, раздел 0). Запуск: ../lab/.venv/bin/python -m pytest tests -q"""
import pathlib, numpy as np, pandas as pd, pytest
from backtest import engine, rules, costs, account, metrics, store

FIX = pathlib.Path(__file__).resolve().parent / 'fixtures'
pytestmark = pytest.mark.skipif(not store.status(), reason='нет кэша свечей (BT_DATA)')
FROZEN_UNTIL = '2026-09-08'          # на этих данных заморожен spec7c


@pytest.fixture(scope='module')
def hyb():
    return engine.signals('hybrid7c')


def test_baseline_spec7c(hyb):
    """Все сделки замороженной базы (без юаня) воспроизводятся: тот же день, сторона и доходность."""
    B = pd.read_csv(FIX / 'hybrid_baseline.csv.gz', parse_dates=['d']); B = B[B.st != 'CR']
    T = engine.trades(hyb); T = T[T.d < FROZEN_UNTIL]
    m = B.merge(T[['st', 'd', 'side', 'gross']], on=['st', 'd'], how='outer', suffixes=('_b', ''), indicator=True)
    both = m[m._merge == 'both']
    assert len(B) == 3801
    # Единственная сделка базы, которой у нас нет, — ошибка самой базы: PT 15.09.2023, вход PTU3 931.8, а цена выхода
    # 975.9 взята у ДРУГОГО контракта (PTZ3): у PTU3 18.09 нет свечи 11:00, а pandas groupby().first() в lab/freeze
    # подставил первое непустое значение из соседней строки. Фиктивные +4.7 %. Новый движок такую сделку не делает.
    lost = m[m._merge == 'left_only']
    assert [(r.st, str(r.d.date())) for r in lost.itertuples()] == [('PT', '2023-09-15')], lost.to_string()
    assert (both.side_b == both.side).all()
    assert np.allclose(both.gross_b, both.gross, atol=1e-9)
    assert (m._merge == 'right_only').sum() <= 5, m[m._merge == 'right_only'].to_string()


def test_h03_net(hyb):
    T = costs.apply(engine.trades(hyb), 'trader', 'c3'); T = T[T.d < FROZEN_UNTIL]
    assert abs(100 * T.net.mean() - 0.223) < 0.005, T.net.mean()


def test_samolet_september_as_osengine_and_tradingview():
    S = engine.signals(rules.with_exec('hybrid7c', rules.EXEC_NEXT_OPEN), ['SS'])
    T = engine.trades(S).set_index('d')
    for d, pin, pout in [('2026-09-10', 331, 326), ('2026-09-14', 297, 290), ('2026-09-15', 279, 267)]:
        assert (T.loc[d, 'px_in'], T.loc[d, 'px_out'], T.loc[d, 'side']) == (pin, pout, -1)
    net = 279 - 267 - 0.0004 * (279 + 267)
    assert abs(net - 11.78) < 0.01


def test_no_lookahead(hyb):
    """Порог дня не зависит от хода этого дня и будущих: пересчёт на усечённой истории даёт тот же порог."""
    g = hyb[hyb.st == 'Si'].reset_index(drop=True); i = len(g) - 40
    mv = g.move.values[:i]; ups = mv[-250:][mv[-250:] > 0]
    assert abs(np.quantile(ups, 0.67) - g.thr_up[i]) < 1e-12


def test_account_reproduces_osengine():
    """OsEngine считал размер позиции от капитала, из которого спред НЕ вычитался (спред добавлялся в отчёте поверх).
    В том же режиме мы обязаны попасть в его цифры: 3949 сигналов, ~3335 сделок, итог ~3.47 млн ₽ (15.09.2026)."""
    S = engine.signals(rules.with_exec('hybrid7c', rules.EXEC_NEXT_OPEN), until=FROZEN_UNTIL)
    assert int((S.side != 0).sum()) == 3949
    T = costs.apply(engine.trades(S), 'trader', 'c3')
    A, K, E = account.simulate(T, 1_000_000, 6, go_mode='snapshot', spread='none', order=rules.UNIVERSE_21, step_cost='snapshot')
    spread = sum(costs.spread_round(s, 'c3') * n for s, n in zip(A.st, A.notional))
    final = 1_000_000 + A.pnl_rub.sum() - spread
    assert abs(len(A) - 3335) <= 15 and abs(final / 3_473_348 - 1) < 0.03, (len(A), final)


def test_account_honest():
    """Честный счёт: спред уменьшает капитал сразу, позиции считаются от него. Это ниже цифры OsEngine (+19.7 %)."""
    S = engine.signals(rules.with_exec('hybrid7c', rules.EXEC_NEXT_OPEN), until=FROZEN_UNTIL)
    T = costs.apply(engine.trades(S), 'trader', 'c3')
    A, K, E = account.simulate(T, 1_000_000, 6, go_mode='snapshot', order=rules.UNIVERSE_21, step_cost='snapshot')
    c = metrics.curve(E, 1_000_000)
    assert 15.5 < c['годовых_%'] < 17.0 and -11 < c['просадка_%'] < -8.5, c


def test_step_cost_from_exchange():
    """Стоимость пункта Brent — из данных биржи по дням: в 2021 году ≈ 73 ₽/$ × 10 баррелей, в 2022-м пик > 1000 ₽."""
    assert 700 < account.rub_per_point('BR', '2021-08-12') < 760
    assert account.rub_per_point('BR', '2022-03-10') > 1000
    assert account.rub_per_point('SR', '2022-03-10') == account.rub_per_point('SR')


def test_leverage_and_go_stress():
    """Без плеча ГО не мешает; с плечом ×4 и ГО ×2 — объём режется по ГО, и это видно в результате."""
    S = engine.signals('hybrid7c', until=FROZEN_UNTIL); T = costs.apply(engine.trades(S), 'trader', 'c3')
    A1, _, E1 = account.simulate(T, 1_000_000, 6, order=rules.UNIVERSE_21)
    A4, K4, E4 = account.simulate(T, 1_000_000, 6, order=rules.UNIVERSE_21, leverage=4, go_mult=2)
    assert int(A1.go_cut.sum()) == 0 and int(E1.margin_call.sum()) == 0
    assert int(A4.go_cut.sum()) + int((K4.reason == 'не хватает ГО').sum()) > 50
