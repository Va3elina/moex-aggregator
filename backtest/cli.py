"""Команды Стенда — всё, что умеет страница /admin/backtest, доступно отсюда (так терминалом управляет агент).

На сервере (прогон попадёт в общий список и будет виден на странице):
    docker compose exec bt-worker python -m backtest.cli submit --rule hybrid7c --exec robot --name "гибрид как робот"
    docker compose exec bt-worker python -m backtest.cli runs
    docker compose exec bt-worker python -m backtest.cli show 12 [--trades SS] [--last 10]
    docker compose exec bt-worker python -m backtest.cli data status | pull
Локально, без БД (результат в файлы <BT_DATA>/runs/<имя>/):
    python -m backtest.cli run --rule algozavr --universe SS,Si --capital 1000000
    python -m backtest.cli rules
"""
import argparse, json, os, sys, time, datetime as dt
import pandas as pd
from . import store, rules as R, runner, costs

pd.set_option('display.width', 220); pd.set_option('display.max_columns', 40)


def _spec(a):
    rule = a.rule if a.rule in R.PRESETS else json.load(open(a.rule, encoding='utf-8'))
    return {'rule': rule, 'name': a.name, 'exec': a.exec, 'universe': a.universe.split(',') if a.universe else None,
            'since': a.since, 'until': a.until, 'tariff': a.tariff, 'spread': a.spread, 'capital': a.capital,
            'slots': a.slots, 'go': a.go, 'go_limit': a.go_limit}


def _db():
    from sqlalchemy import create_engine
    url = os.environ['DB_URL']
    return create_engine(url, connect_args={'ssl_context': False} if 'pg8000' in url else {})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = ap.add_subparsers(dest='cmd', required=True)
    p = sp.add_parser('data'); p.add_argument('what', choices=['status', 'pull'])
    sp.add_parser('rules'); sp.add_parser('runs')
    for c in ('run', 'submit'):
        p = sp.add_parser(c); p.add_argument('--rule', required=True, help='пресет или путь к JSON правила')
        p.add_argument('--exec', default='close', choices=list(runner.EXEC))
        p.add_argument('--universe'); p.add_argument('--from', dest='since'); p.add_argument('--to', dest='until')
        p.add_argument('--tariff', default='trader', choices=list(costs.TARIFFS)); p.add_argument('--spread', default='c3')
        p.add_argument('--capital', type=float, default=1_000_000); p.add_argument('--slots', type=int, default=6)
        p.add_argument('--go', default='mr1', choices=['mr1', 'snapshot', 'none'])
        p.add_argument('--go-limit', type=float, default=1.0); p.add_argument('--name')
        if c == 'submit': p.add_argument('--no-wait', action='store_true')
    p = sp.add_parser('show'); p.add_argument('id', type=int); p.add_argument('--trades'); p.add_argument('--last', type=int, default=15)
    a = ap.parse_args()

    if a.cmd == 'data':
        print(json.dumps(store.refresh() if a.what == 'pull' else store.status(), ensure_ascii=False, indent=1))
    elif a.cmd == 'rules':
        for k, r in R.PRESETS.items(): print(f"{k:14} {r['name']}")
    elif a.cmd == 'run':
        s, res, S, T, A, K, E = runner.execute(_spec(a))
        name = a.name or f"{s['rule']['id']}_{a.exec}_{dt.datetime.now():%m%d_%H%M%S}"
        out = store.DATA / 'runs' / name; out.mkdir(parents=True, exist_ok=True)
        S.to_csv(out / 'signals.csv', index=False); T.to_csv(out / 'trades.csv', index=False)
        if A is not None:
            A.to_csv(out / 'account_trades.csv', index=False); K.to_csv(out / 'skipped.csv', index=False); E.to_csv(out / 'equity.csv')
        (out / 'run.json').write_text(json.dumps({'spec': s, 'result': res}, ensure_ascii=False, indent=1, default=str))
        print(json.dumps(res, ensure_ascii=False, indent=1, default=str)); print('прогон:', out)
    elif a.cmd == 'submit':
        from sqlalchemy import text
        eng = _db()
        with eng.begin() as con:
            rid = con.execute(text("INSERT INTO bt_runs (name, spec) VALUES (:n, CAST(:s AS JSONB)) RETURNING id"),
                              {'n': a.name, 's': json.dumps(_spec(a), ensure_ascii=False)}).scalar()
        print('прогон', rid, 'в очереди')
        while not a.no_wait:
            time.sleep(1)
            with eng.connect() as con:
                r = con.execute(text("SELECT status, result, error FROM bt_runs WHERE id=:r"), {'r': rid}).fetchone()
            if r.status in ('done', 'error'):
                print(r.status, json.dumps(r.result, ensure_ascii=False, indent=1, default=str) if r.result else r.error); break
    elif a.cmd == 'runs':
        from sqlalchemy import text
        with _db().connect() as con:
            rows = con.execute(text("""SELECT id, status, name, spec->>'exec' AS exec, result->'per_trade'->>'сделок' AS n,
                result->'per_trade'->>'чистыми_%' AS net, result->'account'->>'годовых_%' AS cagr,
                result->'account'->>'просадка_%' AS dd, created_at FROM bt_runs ORDER BY id DESC LIMIT 30""")).fetchall()
        print(pd.DataFrame(rows, columns=['id', 'статус', 'имя', 'исполнение', 'сделок', 'чистыми_%', 'годовых_%', 'просадка_%', 'создан']).to_string(index=False))
    elif a.cmd == 'show':
        from sqlalchemy import text
        with _db().connect() as con:
            r = con.execute(text("SELECT status, spec, result, error FROM bt_runs WHERE id=:r"), {'r': a.id}).fetchone()
            print(r.status, r.error or ''); print(json.dumps(r.result, ensure_ascii=False, indent=1, default=str))
            if a.trades:
                t = con.execute(text("""SELECT st, d, side, move, px_in, d_out, px_out, net, qty, pnl_rub, account_skip
                    FROM bt_trades WHERE run_id=:r AND st=:st ORDER BY d DESC LIMIT :l"""),
                                {'r': a.id, 'st': a.trades, 'l': a.last}).fetchall()
                print(pd.DataFrame(t, columns=['st', 'd', 'side', 'move', 'px_in', 'd_out', 'px_out', 'net', 'qty', 'pnl_rub', 'skip']).iloc[::-1].to_string(index=False))


if __name__ == '__main__':
    main()
