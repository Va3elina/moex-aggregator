"""Мост воркер → песочница: положить задание, дождаться результата. Сам код стратегии воркер НЕ исполняет."""
import json, os, pathlib, time
import pandas as pd

JOBS = pathlib.Path(os.environ.get('BT_JOBS', '/app/bt_jobs'))
WAIT_SEC = 960


def run(run_id, spec, s, progress=None):
    JOBS.mkdir(parents=True, exist_ok=True)
    for f in JOBS.glob(f'{run_id}.*'): f.unlink(missing_ok=True)
    job = {'id': run_id, 'code': spec['code'], 'params': spec.get('params') or {}, 'universe': s['universe'],
           'since': s['since'], 'until': s['until'],
           'account': {k: s.get(k) for k in ('capital', 'tariff', 'spread', 'go', 'go_mult')}}   # счёт режима заявок
    tmp = JOBS / f'{run_id}.job.tmp'; tmp.write_text(json.dumps(job, ensure_ascii=False)); tmp.rename(JOBS / f'{run_id}.job.json')
    res_f, prog_f, t0 = JOBS / f'{run_id}.result.json', JOBS / f'{run_id}.progress.json', time.time()
    while not res_f.exists():
        if time.time() - t0 > WAIT_SEC:
            raise RuntimeError('песочница не ответила — контейнер bt-sandbox запущен?')
        if progress and prog_f.exists():
            try: progress(json.loads(prog_f.read_text())['p'], 100)
            except Exception: pass
        time.sleep(0.5)
    res = json.loads(res_f.read_text())
    for f in JOBS.glob(f'{run_id}.*'): f.unlink(missing_ok=True)
    if res.get('error'): raise RuntimeError('ошибка в коде стратегии:\n' + res['error'])
    info = {'params': res.get('params'), 'lookahead': res.get('lookahead')}
    if res.get('orders'): info['orders'] = res['orders']                 # режим заявок: кривая и сводка счёта из песочницы
    return to_frame(res['trades']), info


BASE = ['st', 'secid', 'side', 'd', 'm_in', 'px_in', 'd_out', 'm_out', 'px_out', 'exit_reason', 'mfe', 'mae']


def to_frame(rows):
    T = pd.DataFrame(rows)                                              # у режима заявок — ещё поля счёта (qty, pnl_rub, …)
    for c in BASE:
        if c not in T: T[c] = pd.Series(dtype=float)
    T['d'] = pd.to_datetime(T.d); T['d_out'] = pd.to_datetime(T.d_out)
    T['move'] = float('nan'); T['thr_up'] = float('nan'); T['thr_dn'] = float('nan')
    T['ret'] = T.px_out / T.px_in - 1; T['gross'] = T.side * T.ret
    return T.sort_values(['d', 'm_in', 'st']).reset_index(drop=True)
