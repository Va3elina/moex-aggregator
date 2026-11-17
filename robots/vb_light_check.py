"""Сверка лёгкого прогона робота VB с полным на истории: решения обязаны совпасть 1:1.

Для каждого из N последних торговых дней D: «утро» — сегодняшний бар D почти пустой (доля f1 движения и объёма),
у части бумаг его ещё нет → полный прогон пишет кэш дня. «День» — бар D больше (доля f2, у всех бумаг) →
лёгкий прогон по кэшу против полного. Сравниваются входы (сделка → стоимость), принудительные закрытия, свежесть.
Данные из БД берутся ОДИН раз (все дневки), дальше режутся в памяти — прод не нагружается.

    cd /opt/frame && BT_DATA=/var/lib/docker/volumes/frame_bt_data/_data /opt/hybrid-bot/.venv/bin/python \
        -m robots.vb_light_check --days 25
"""
import argparse, datetime as dt, json, pathlib, random, re, sys, tempfile, time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backtest import screener_vb as S  # noqa: E402
from robots import vb_bot as B  # noqa: E402

_orig_copy = S._copy
_raw = {}          # sql-ключ → полный DataFrame из БД
_frame = {}        # текущий срез: 'candles' / 'imoex'


def _kind(sql):
    return 'candles' if 'FROM candles' in sql else 'imoex' if 'index_data' in sql else 'other'


def fake_copy(sql):
    k = _kind(sql)
    if k == 'other':
        if sql not in _raw: _raw[sql] = _orig_copy(sql)
        return _raw[sql].copy()
    df = _frame[k]
    w = re.search(r"begin_time >= '(\d{4}-\d\d-\d\d)'", sql)
    if w and w.group(1) > '2011-01-01':
        df = df[df.d >= w.group(1)]
    m = re.search(r"secid IN \(([^)]*)\)", sql)
    if m:
        df = df[df.secid.isin([x.strip("' ") for x in m.group(1).split(',')])]
    elif 'AND false' in sql:
        df = df.iloc[0:0]
    return df.copy()


def slice_day(cand, imoex, D, f, drop):
    """Данные «на момент внутри дня D»: бары после D убраны, бар D — доля f движения от открытия и объёма."""
    c = cand[cand.d <= D].copy()
    t = c.d == D
    o = c.loc[t, 'open']
    c.loc[t, 'high'] = o + f * (c.loc[t, 'high'] - o)
    c.loc[t, 'low'] = o - f * (o - c.loc[t, 'low'])
    c.loc[t, 'close'] = o + f * (c.loc[t, 'close'] - o)
    c.loc[t, 'volume'] = (c.loc[t, 'volume'] * f).round()
    if drop:
        c = c[~(t & c.secid.isin(drop))]
    return c, imoex[imoex.d < D]               # индекс за день D ещё не закрыт


def wild(c, D, frozen, rnd):
    """Злой бар: у замороженных бумаг сегодняшние h/l/c/v — крайние (h×10, l×0.1, v×100, c на границе)."""
    c = c.copy()
    t = (c.d == D) & c.secid.isin(frozen)
    o = c.loc[t, 'open']
    c.loc[t, 'high'] = o * 10
    c.loc[t, 'low'] = o * 0.1
    c.loc[t, 'close'] = o * (10 if rnd.random() < 0.5 else 0.1)
    c.loc[t, 'volume'] = c.loc[t, 'volume'] * 100 + 1
    return c


def decisions(now, st, inst, mode):
    exe, forced, fresh, last10 = B.model(now, st, inst, cache=mode)
    return (sorted((B.key(s), round(x, 4), s.res['reason'], s.res['exitDate'], s.entryDate, s.entryAtOpen,
                    round(s.ep, 6)) for s, x in exe), sorted(forced), fresh)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=25)
    ap.add_argument('--seed', type=int, default=1)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    t0 = time.time()
    cand = _orig_copy("""SELECT secid, begin_time::date AS d, open, high, low, close, volume FROM candles
                  WHERE interval = 24 AND type = 'stock' AND begin_time >= '2011-01-01' ORDER BY secid, begin_time""")
    cand['d'] = cand.d.astype(str).str[:10]
    imoex = _orig_copy("SELECT trade_date AS d, close FROM index_data WHERE secid = 'IMOEX' AND close IS NOT NULL ORDER BY trade_date")
    imoex['d'] = imoex.d.astype(str).str[:10]
    S._copy = fake_copy
    LEV0 = B.LEVERAGE
    inst = json.loads((pathlib.Path('/opt/vb-bot/state/instruments.json')).read_text())['map']
    days = sorted(d for d in cand[cand.secid == 'SBER'].d.unique() if not S.wk(d))[-a.days:]
    print(f'данные: {len(cand)} строк, {time.time() - t0:.1f} с; дней сверки {len(days)}', flush=True)
    bad = 0
    with tempfile.TemporaryDirectory() as tmp:
        B.STATE = pathlib.Path(tmp)
        for n, D in enumerate(days):
            secs = sorted(cand[cand.d == D].secid.unique())
            drop = set(rnd.sample([s for s in secs if s != 'SBER'], max(1, len(secs) // 10)))
            # главный риск: у бумаги со входом сегодня утром ещё нет свечи — их прячем всегда
            _frame['candles'], _frame['imoex'] = slice_day(cand, imoex, D, 1.0, set())
            tk0, mk0, dv0, vv0 = S.load_db()
            entry_today = {s.ticker for s in S.run(tk0, mk0, dv0, vv0, lambda d: d < D) if s.entryDate == D} - {'SBER'}
            drop |= entry_today
            del tk0
            all_d = sorted(cand.d.unique())
            start = all_d[max(0, all_d.index(D) - rnd.choice([0, 3, 8, 15]))]
            now = dt.datetime.fromisoformat(D + 'T12:00').replace(tzinfo=B.MSK)
            prev = all_d[all_d.index(D) - 1]
            late = dt.datetime.fromisoformat(D + 'T23:45').replace(tzinfo=B.MSK)
            for name, f1, f2, expect_light in (('обычный', 0.1, 0.6, True), ('весь бар', 0.3, 1.0, True),
                                               ('злой бар', 0.1, 1.0, True), ('с ногами', 0.2, 0.8, True),
                                               ('без плеча', 0.1, 0.7, True), ('правка вчера', 0.2, 0.5, False),
                                               ('конец дня', 0.2, 1.0, False)):
                st = {'start': start, 'legs': {}}
                B.LEVERAGE = 1.0 if name == 'без плеча' else LEV0
                for p in B.STATE.glob('model_*.pkl'): p.unlink()
                _frame['candles'], _frame['imoex'] = slice_day(cand, imoex, D, f1, drop)
                morning = B.model(now, st, inst, cache='auto')            # утро: полный прогон пишет кэш
                if not B._cache_path(D).exists():
                    print(f'{D}: кэш не записан'); bad += 1; continue
                c = B.load_cache(D, st)
                if c is None:
                    print(f'{D} {name}: кэш сразу недействителен'); bad += 1; continue
                live = set(c['live'])
                frozen = sorted({s.ticker for s in c['frozen']})
                if name == 'с ногами':                                     # ноги робота = утренние сделки модели
                    st['legs'] = {B.key(s): {'ticker': s.ticker} for s, x in morning[0] if s.res['reason'] == 'open'}
                _frame['candles'], _frame['imoex'] = slice_day(cand, imoex, D, f2, set())
                if name == 'злой бар':                                     # всё, что не пересчитывается, — в крайности
                    _frame['candles'] = wild(_frame['candles'], D, set(cand.secid.unique()) - live, rnd)
                if name == 'правка вчера' and frozen:
                    x = _frame['candles']
                    had_prev = sorted(set(x[(x.d == prev)].secid) & set(frozen))   # правим ту, у кого свеча вчера есть
                    t = (x.d == prev) & (x.secid == rnd.choice(had_prev))
                    assert t.sum() == 1
                    x.loc[t, 'volume'] = x.loc[t, 'volume'] * 1.5 + 1
                when = late if name == 'конец дня' else now
                t1 = time.time(); light = decisions(when, st, inst, 'auto'); tl = time.time() - t1
                used = B.model.light
                t1 = time.time(); full = decisions(when, st, inst, 'off'); tf = time.time() - t1
                ok = light == full and used == expect_light
                bad += not ok
                print(f'{D} старт {start} без свечи со входом {len(entry_today)} {name:12s}: живых {len(live)}, '
                      f'замор. {len(c["frozen"])}, ног {len(st["legs"])}, исполн. {len(full[0])}, закрытий {len(full[1])}, '
                      f'лёгкий={used} — {"OK" if ok else "РАСХОЖДЕНИЕ"} ({tl:.1f} с против {tf:.1f} с)', flush=True)
                if not ok:
                    for what, x, y in (('входы', light[0], full[0]), ('закрытия', light[1], full[1])):
                        print('  ', what, 'только лёгкий:', sorted(set(x) - set(y))[:5], 'только полный:', sorted(set(y) - set(x))[:5])
            B.LEVERAGE = LEV0
    print(f'итог: расхождений {bad}', flush=True)
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
