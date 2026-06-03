"""Fetch RTS index (RTSI) daily close from MOEX ISS history (index market).

RTSI is the dollar-denominated RTS index in index points (e.g. ~1100).
Output: rts_daily.csv (date, close) — used as the spot for backtests.
"""
import requests, csv, time
from pathlib import Path
requests.packages.urllib3.disable_warnings()
H = {'User-Agent': 'Mozilla/5.0'}
OUT = Path(__file__).parent / 'rts_daily.csv'
URL = 'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/RTSI.json'


def main():
    rows = []
    start = 0
    while True:
        for _ in range(4):
            try:
                r = requests.get(URL, params={'from': '2018-01-01', 'iss.meta': 'off', 'start': start},
                                 timeout=30, headers=H, verify=False)
                d = r.json()['history']; cols = d['columns']; data = d['data']
                break
            except Exception as e:
                print('retry', e); time.sleep(2)
        else:
            data = []
        if not data:
            break
        ci = {c: cols.index(c) for c in cols}
        for row in data:
            o = row[ci['OPEN']] if 'OPEN' in ci else None
            h = row[ci['HIGH']] if 'HIGH' in ci else None
            l = row[ci['LOW']] if 'LOW' in ci else None
            c = row[ci['CLOSE']] if 'CLOSE' in ci else None
            v = row[ci['VOLUME']] if 'VOLUME' in ci else None
            rows.append([row[ci['TRADEDATE']], o, h, l, c, v])
        if len(data) < 100:
            break
        start += 100
        time.sleep(0.05)
    rows = [r for r in rows if r[4] not in (None, '')]
    with open(OUT, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'open', 'high', 'low', 'close', 'volume'])
        w.writerows(rows)
    print(f"Saved {len(rows)} daily rows -> {OUT.name}")
    if rows:
        print(f"  {rows[0][0]} -> {rows[-1][0]}, last close={rows[-1][4]}")


if __name__ == "__main__":
    main()
