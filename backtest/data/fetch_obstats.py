"""Этап 4: спред стакана ПО ДНЯМ. Algopack obstats (5-мин статистика стакана фьючерсов, с 03.2023) по контрактам
цепочки всех бумаг, только времена входа/выхода наших правил. Разовая выгрузка (~2–3 часа, с мака: IP сервера у MOEX
в тихом бане). Ключ — в scratchpad/.k (не печатать). Пишет по мере скачивания: data/obstats_points.csv.gz.

    BT_DATA=../../../MOEX-bt/bt_data ../lab/.venv/bin/python fetch_obstats.py
"""
import json, subprocess, sys, time, pathlib, gzip, csv, os
import pandas as pd
sys.path.insert(0, str(pathlib.Path.home() / 'PyCharmMiscProject/MOEX-bt'))
from backtest import engine, store                                        # noqa: E402

KEY = open('/private/tmp/claude-501/-Users-vadim-PyCharmMiscProject-MOEX/187107c2-f70b-442d-b0ff-cefe3999c639/scratchpad/.k').read().strip()
OUT = pathlib.Path(__file__).parent / 'data' / 'obstats_points.csv.gz'
DONE = pathlib.Path(__file__).parent / 'data' / 'obstats_done.txt'
TIMES = {'10:00:00', '10:05:00', '10:30:00', '11:00:00', '11:05:00', '11:10:00', '17:00:00', '17:05:00', '17:10:00', '17:15:00', '18:30:00', '18:35:00'}
KEEP = ['tradedate', 'tradetime', 'secid', 'mid_price', 'spread_l1', 'vol_b_l3', 'vol_s_l3', 'vwap_b_l3', 'vwap_s_l3', 'vwap_b_l5', 'vwap_s_l5', 'vwap_b_l10', 'vwap_s_l10']
SINCE = pd.Timestamp('2023-03-01')


def get(url):
    for k in range(4):
        p = subprocess.run(['curl', '-s', '--max-time', '90', '-H', f'Authorization: Bearer {KEY}', url], capture_output=True, text=True)
        try: return json.loads(p.stdout)
        except Exception: time.sleep(5 + 10 * k)
    raise RuntimeError('нет ответа')


def main():
    done = set(DONE.read_text().split()) if DONE.exists() else set()
    jobs = []
    for st in store.UNIVERSE_ALL:
        p = engine.chain(st, 630, 1020); p = p[p.d >= SINCE]
        for sec, g in p.groupby('secid'):
            jobs.append((g.d.max(), st, sec, g.d.min().date(), g.d.max().date()))
    jobs.sort(reverse=True)                                           # свежие контракты — первыми: частичный результат уже полезен
    print('контрактов', len(jobs), 'сделано', len(done), flush=True)
    new = not OUT.exists()
    with gzip.open(OUT, 'at', newline='') as f:
        w = csv.writer(f)
        if new: w.writerow(['st'] + KEEP)
        for n, (_, st, sec, a, b) in enumerate(jobs):
            if sec in done: continue
            start = got = 0
            while True:
                j = get(f'https://apim.moex.com/iss/datashop/algopack/fo/obstats/{sec}.json?from={a}&till={b}&start={start}&iss.meta=off')
                d = j.get('data', {}); rows = d.get('data', [])
                if not rows: break
                cols = d['columns']; ix = [cols.index(c) for c in KEEP]; it = cols.index('tradetime')
                for r in rows:
                    if r[it] in TIMES: w.writerow([st] + [r[i] for i in ix]); got += 1
                start += len(rows)
                if len(rows) < 1000: break
                time.sleep(0.4)
            f.flush(); done.add(sec); DONE.write_text('\n'.join(sorted(done)))
            print(f'{n + 1}/{len(jobs)} {st} {sec} {a}..{b} строк {got}', flush=True)
            time.sleep(0.4)


if __name__ == '__main__':
    main()
