"""Фандинг вечных фьючерсов по дням — поле SWAPRATE бесплатной истории ISS (своп-разница в пунктах цены за 1 контракт
на единицу базового актива; у CNYRUBF — рубли за 1 юань, × лот 1000 = рубли на контракт). Положительная — лонг платит
шорту в вечерний клиринг; внутри — процентная разница рубля и валюты плюс отклонение цены фьючерса от спота.
Разовое обновление вручную (как ГО и стоимость пункта), крона нет:

    python backtest/data/fetch_funding.py          → backtest/data/funding.csv.gz (st, d, swaprate)
"""
import json, pathlib, time, urllib.request
import pandas as pd

OUT = pathlib.Path(__file__).parent / 'funding.csv.gz'
PERPETUAL = ['CNYRUBF', 'USDRUBF', 'EURRUBF', 'GLDRUBF', 'IMOEXF', 'SBERF', 'GAZPF']
URL = ('https://iss.moex.com/iss/history/engines/futures/markets/forts/securities/{sec}.json?iss.meta=off&iss.only=history'
       '&history.columns=TRADEDATE,SWAPRATE&start={start}')
rows = []
for sec in PERPETUAL:
    start = 0
    while True:
        with urllib.request.urlopen(URL.format(sec=sec, start=start), timeout=40) as r:
            data = json.load(r)['history']['data']
        rows += [(sec, d, x) for d, x in data if x is not None]
        start += len(data)
        if len(data) < 100: break
        time.sleep(0.2)
    print(sec, sum(1 for r in rows if r[0] == sec), flush=True)
df = pd.DataFrame(rows, columns=['st', 'd', 'swaprate']).drop_duplicates(['st', 'd']).sort_values(['st', 'd'])
df.to_csv(OUT, index=False)
print(df.groupby('st').swaprate.describe().round(5).to_string())
