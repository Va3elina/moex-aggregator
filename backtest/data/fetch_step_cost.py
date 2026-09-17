"""Этап 4: рублёвая стоимость пункта по дням — ИЗ ДАННЫХ САМОЙ БИРЖИ, без допущений о курсе.
Бесплатная история ISS по контракту отдаёт открытые позиции в контрактах (OPENPOSITION) и в рублях (OPENPOSITIONVALUE)
и расчётную цену в пунктах → ₽ за 1 пункт = OPENPOSITIONVALUE / OPENPOSITION / SETTLEPRICE. Это ровно тот курс, по
которому биржа считала вариационную маржу в этот день (сверено 17.09.2026: BR 07.2026 — 766 ₽, 08.2021 — 737 ₽).
Нужно только контрактам в валюте: BR, PT, RI (у остальных пункт = рубль × лот, постоянно).

    BT_DATA=../../../MOEX-bt/bt_data python fetch_step_cost.py     → data/step_cost.csv.gz (st, d, rub_per_point)
"""
import json, sys, time, pathlib, urllib.request
import pandas as pd
sys.path.insert(0, str(pathlib.Path.home() / 'PyCharmMiscProject/MOEX-bt'))
from backtest import engine                                               # noqa: E402

OUT = pathlib.Path(__file__).parent / 'data' / 'step_cost.csv.gz'
URL = ('https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/{sec}.json?iss.meta=off&iss.only=history'
       '&history.columns=TRADEDATE,SETTLEPRICE,OPENPOSITION,OPENPOSITIONVALUE&from={a}&till={b}&start={start}')
rows = []
for st in ('BR', 'PT', 'RI'):
    p = engine.chain(st, 630, 1020)
    for sec, g in p.groupby('secid'):
        a, b, start = g.d.min().date(), g.d.max().date(), 0
        while True:
            with urllib.request.urlopen(URL.format(sec=sec, a=a, b=b, start=start), timeout=40) as r:
                data = json.load(r)['history']['data']
            for d, sp, oi, oiv in data:
                if sp and oi and oiv: rows.append((st, d, oiv / oi / sp))
            start += len(data)
            if len(data) < 100: break
        time.sleep(0.2)
    print(st, len(rows), flush=True)
df = pd.DataFrame(rows, columns=['st', 'd', 'rub_per_point']).drop_duplicates(['st', 'd']).sort_values(['st', 'd'])
df['rub_per_point'] = df.rub_per_point.round(4)
df.to_csv(OUT, index=False)
print(df.groupby('st').rub_per_point.describe().round(2).to_string())
