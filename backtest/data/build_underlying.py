"""Длинная история базовых активов для индикаторов Стенда (b.hist) → backtest/data/underlying.csv.gz (st, d, close).
Акции — дневные свечи из БД (с 2013), индексы IMOEX/RTSI, валюты и золото — ISS; у доллара и евро биржевой спот
прерывался (12.06.2024–02.2026) — после 11.06.2024 продолжаем вечными фьючерсами (стык по отношению цен).
Сырья (нефть, платина, какао) нет — Стенд берёт непрерывный ряд самого фьючерса. Разовое обновление вручную:

    BT_DATA=<кэш свечей> python backtest/data/build_underlying.py
"""
import io, json, pathlib, subprocess, sys, time, urllib.request
import pandas as pd
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from backtest import store  # noqa: E402

OUT = pathlib.Path(__file__).parent / 'underlying.csv.gz'
ISS = {'IMOEX': 'stock/markets/index/securities/IMOEX', 'RTSI': 'stock/markets/index/securities/RTSI',
       'USD000UTSTOM': 'currency/markets/selt/boards/CETS/securities/USD000UTSTOM',
       'CNYRUB_TOM': 'currency/markets/selt/boards/CETS/securities/CNYRUB_TOM',
       'EUR_RUB__TOM': 'currency/markets/selt/boards/CETS/securities/EUR_RUB__TOM',
       'GLDRUB_TOM': 'currency/markets/selt/boards/CETS/securities/GLDRUB_TOM'}
STOCKS = ['AFLT', 'AFKS', 'GMKN', 'GAZP', 'LKOH', 'MGNT', 'NLMK', 'PIKK', 'SNGS', 'SBER', 'SMLT', 'SGZH', 'TATN', 'VTBR']
MAP = {'AF': 'AFLT', 'AK': 'AFKS', 'GK': 'GMKN', 'GZ': 'GAZP', 'LK': 'LKOH', 'MN': 'MGNT', 'NM': 'NLMK', 'PI': 'PIKK',
       'SN': 'SNGS', 'SR': 'SBER', 'SS': 'SMLT', 'SZ': 'SGZH', 'TT': 'TATN', 'VB': 'VTBR', 'SBERF': 'SBER', 'GAZPF': 'GAZP',
       'MX': 'IMOEX', 'IMOEXF': 'IMOEX', 'RI': 'RTSI', 'Si': 'USD', 'USDRUBF': 'USD', 'Eu': 'EUR', 'EURRUBF': 'EUR',
       'CR': 'CNYRUB_TOM', 'CNYRUBF': 'CNYRUB_TOM', 'GLDRUBF': 'GLDRUB_TOM'}

S = {}
for a, path in ISS.items():
    rows, start = [], 0
    while True:
        u = f'https://iss.moex.com/iss/history/engines/{path}.json?iss.meta=off&iss.only=history&from=2013-01-01&start={start}'
        h = json.load(urllib.request.urlopen(u, timeout=40))['history']
        if not h['data']: break
        ci, cd = h['columns'].index('CLOSE'), h['columns'].index('TRADEDATE')
        rows += [(r[cd], r[ci]) for r in h['data'] if r[ci]]
        start += len(h['data']); time.sleep(0.25)
    S[a] = pd.Series({pd.Timestamp(d): float(c) for d, c in rows}).sort_index()
sql = ("COPY (SELECT secid, begin_time::date, close FROM candles WHERE interval=24 AND type='stock' AND begin_time >= '2013-01-01' "
       f"AND secid IN ({','.join(repr(s) for s in STOCKS)}) ORDER BY 1,2) TO STDOUT WITH CSV")
inner = f'docker exec frame-db-1 psql -U postgres -d moex_db -c "{sql}"'
cmd = ['bash', '-c', inner] if pathlib.Path('/opt/frame').exists() else store.SSH + [inner]
St = pd.read_csv(io.StringIO(subprocess.run(cmd, capture_output=True, text=True, timeout=600).stdout), header=None, names=['a', 'd', 'c'], parse_dates=['d'])
for a, g in St.groupby('a'): S[a] = g.set_index('d').c.sort_index()
for a, spot, perp in (('USD', 'USD000UTSTOM', 'USDRUBF'), ('EUR', 'EUR_RUB__TOM', 'EURRUBF')):
    s = S[spot][:'2024-06-11']; p = store.bars(perp).groupby('d').close.last()
    S[a] = pd.concat([s, s.iloc[-1] / p[:'2024-06-11'].iloc[-1] * p['2024-06-12':]])
out = pd.concat({st: S[a] for st, a in MAP.items()}, names=['st', 'd']).rename('close').round(6).reset_index()
out.to_csv(OUT, index=False)
print(out.groupby('st').d.agg(['min', 'max', 'count']).to_string())
