"""Воркер Стенда: берёт прогоны из bt_runs (queued), считает, пишет результат. Один прогон за раз.
Живёт в отдельном контейнере с лимитом памяти — тяжёлый pandas не делит память с API сайта.

    python -m backtest.worker            # вечный цикл
    python -m backtest.worker --once     # обработать очередь и выйти
"""
import json, math, os, sys, time, traceback, datetime as dt
import pandas as pd
from sqlalchemy import create_engine, text
from . import store, runner

POLL_SEC = 0.5      # автопересчёт при смене бумаги: прогон по одной бумаге ~0.3 с, ждать очередь дольше расчёта нельзя
REFRESH_EVERY_SEC = 6 * 3600          # свечи в кэше не старше 6 часов; spec.refresh=true — докачать сейчас
_url = os.environ['DB_URL']
ENG = create_engine(_url, pool_pre_ping=True, connect_args={'ssl_context': False} if 'pg8000' in _url else {})


def log(msg):
    print(f'{dt.datetime.now():%Y-%m-%d %H:%M:%S} bt-worker  {msg}', flush=True)


def _clean(v):
    if v is None: return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    if isinstance(v, pd.Timestamp): return None if pd.isna(v) else v.date()
    if hasattr(v, 'item'): return _clean(v.item())
    return v


INT_COLS = {'n', 'side', 'qty', 'positions', 'run_id', 'margin_call', 'm_in', 'm_out'}
BOOL_COLS = {'go_cut'}


def _rows(df, cols):
    out = []
    for r in df[cols].to_dict('records'):
        row = {c: _clean(r[c]) for c in cols}
        for c in INT_COLS & row.keys():                  # pandas отдаёт 1.0 / -1.0 — в smallint/integer так нельзя
            if row[c] is not None: row[c] = int(row[c])
        for c in BOOL_COLS & row.keys():
            row[c] = bool(row[c]) if row[c] is not None else None
        out.append(row)
    return out


def _insert(con, table, rows):
    if not rows: return
    cols = list(rows[0])
    q = text(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})")
    for i in range(0, len(rows), 2000):
        con.execute(q, rows[i:i + 2000])


def ensure_data(force=False):
    st = store.status()
    stamp = store.DATA / 'refreshed_at'
    fresh = stamp.exists() and time.time() - stamp.stat().st_mtime < REFRESH_EVERY_SEC
    if st and st.get('_format') == store.FORMAT and fresh and not force: return
    log('докачиваю свечи…' if st else 'первая загрузка свечей (несколько минут)…')
    t0 = time.time(); store.refresh()
    stamp.write_text(dt.datetime.now().isoformat())
    log(f'свечи готовы за {time.time() - t0:.0f} с')


def process(run_id, spec):
    ensure_data(force=bool(spec.get('refresh')))
    def progress(i, n):
        if i % 3 == 0 or i == n:
            with ENG.begin() as con:
                con.execute(text("UPDATE bt_runs SET progress=:p WHERE id=:r"), {'r': run_id, 'p': int(100 * i / n)})
    py = None
    if spec.get('code'):                                    # код стратегии исполняет ТОЛЬКО песочница (без сети и секретов)
        from . import pybridge
        py = pybridge.run(run_id, spec, runner.normalize(spec), progress)
    s, res, S, T, A, K, E = runner.execute(spec, progress, py_trades=py)
    if res.get('kind') == 'sweep':                          # перебор: только итоговая таблица
        with ENG.begin() as con:
            for t in ('bt_signals', 'bt_trades', 'bt_equity'):
                con.execute(text(f'DELETE FROM {t} WHERE run_id = :r'), {'r': run_id})
            con.execute(text("""UPDATE bt_runs SET status='done', finished_at=now(), result=CAST(:res AS JSONB),
                                spec_full=CAST(:sf AS JSONB), error=NULL, progress=100 WHERE id=:r"""),
                        {'r': run_id, 'res': json.dumps(res, ensure_ascii=False, default=str),
                         'sf': json.dumps(s, ensure_ascii=False, default=str)})
        return res
    S = S.assign(run_id=run_id) if S is not None else None
    T = T.assign(run_id=run_id, thr=[u if sd > 0 else d for u, d, sd in zip(T.thr_up, T.thr_dn, T.side)])
    acc_cols = ['qty', 'notional', 'go', 'equity_in', 'comm_rub', 'spread_rub', 'pnl_rub', 'go_cut']
    if A is not None and len(A):
        T = T.merge(A[['n'] + acc_cols], on='n', how='left')
        T = T.merge(K.rename(columns={'reason': 'account_skip'})[['n', 'account_skip']], on='n', how='left')
    else:
        for c in acc_cols + ['account_skip']: T[c] = None
    with ENG.begin() as con:
        for t in ('bt_signals', 'bt_trades', 'bt_equity'):
            con.execute(text(f'DELETE FROM {t} WHERE run_id = :r'), {'r': run_id})
        if S is not None: _insert(con, 'bt_signals', _rows(S, ['run_id', 'st', 'd', 'secid', 'pa', 'pb', 'move', 'thr_up', 'thr_dn',
                                             'straight', 'side', 'tradable', 'skip']))
        _insert(con, 'bt_trades', _rows(T, ['run_id', 'n', 'st', 'd', 'secid', 'side', 'move', 'thr', 'px_in', 'd_out',
                                            'px_out', 'gross', 'comm', 'spread', 'net', 'm_in', 'm_out', 'exit_reason', 'mfe', 'mae'] + acc_cols + ['account_skip']))
        if E is not None:
            _insert(con, 'bt_equity', _rows(E.reset_index().assign(run_id=run_id),
                                            ['run_id', 'd', 'equity', 'positions', 'notional', 'go_used', 'margin_call']))
        con.execute(text("""UPDATE bt_runs SET status='done', finished_at=now(), result=CAST(:res AS JSONB),
                            spec_full=CAST(:sf AS JSONB), error=NULL WHERE id=:r"""),
                    {'r': run_id, 'res': json.dumps(res, ensure_ascii=False, default=str),
                     'sf': json.dumps(s, ensure_ascii=False, default=str)})
    return res


def take():
    with ENG.begin() as con:
        row = con.execute(text("""UPDATE bt_runs SET status='running', started_at=now()
            WHERE id = (SELECT id FROM bt_runs WHERE status='queued' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
            RETURNING id, spec""")).fetchone()
    return row


def main():
    once = '--once' in sys.argv
    log('старт')
    with ENG.begin() as con:      # прогоны, оборванные перезапуском контейнера, — обратно в очередь
        con.execute(text("UPDATE bt_runs SET status='queued' WHERE status='running'"))
    while True:
        row = take()
        if row is None:
            if once: return
            time.sleep(POLL_SEC); continue
        run_id, spec = row[0], row[1] if isinstance(row[1], dict) else json.loads(row[1])
        t0 = time.time()
        try:
            res = process(run_id, spec)
            log(f"прогон {run_id} готов за {time.time() - t0:.1f} с: сделок {res['per_trade'].get('сделок')}")
        except Exception as e:
            log(f'прогон {run_id} упал: {str(e)[:300]}\n{traceback.format_exc(limit=3)[-1500:]}')
            with ENG.begin() as con:
                con.execute(text("UPDATE bt_runs SET status='error', finished_at=now(), error=:e WHERE id=:r"),
                            {'r': run_id, 'e': f'{type(e).__name__}: {e}'[:2000]})


if __name__ == '__main__':
    main()
