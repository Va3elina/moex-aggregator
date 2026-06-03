"""Fetch GAZP daily OHLCV from MOEX ISS history (TQBR board)."""
import requests, csv, time
from pathlib import Path
from datetime import date, timedelta
requests.packages.urllib3.disable_warnings()
H = {'User-Agent': 'Mozilla/5.0'}
OUT = Path(__file__).parent / 'gazp_daily.csv'
URL = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/GAZP.json'


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
        ci = {c: cols.index(c) for c in ['TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']}
        for row in data:
            rows.append([row[ci['TRADEDATE']], row[ci['OPEN']], row[ci['HIGH']],
                         row[ci['LOW']], row[ci['CLOSE']], row[ci['VOLUME']]])
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
