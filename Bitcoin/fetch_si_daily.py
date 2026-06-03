"""Build a continuous Si (USD/RUB futures) daily price series 2020-2026.

Si futures are quarterly (codes SiH/SiM/SiU/SiZ + year digit). We construct a
continuous front-month series by selecting, for each trading day, the contract
with the highest open interest (the liquid front month), using the full FORTS
history endpoint with assetcode=Si.

Units: MOEX quotes Si in points where 1000 points = 1 RUB/USD
  (e.g. 89579 points = 89.579 RUB/USD). We store both raw points and rub_per_usd.

Output: si_daily.csv  (date, secid, close, settle, volume, oi, rub_per_usd)
        si_futures_continuous.csv (same, the chosen front contract per day)
"""
import requests, csv, time
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
requests.packages.urllib3.disable_warnings()
H = {'User-Agent': 'Mozilla/5.0'}
ROOT = Path(__file__).parent
URL = 'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json'
OUT = ROOT / 'si_daily.csv'


def fetch_day(d):
    start = 0
    rows = []
    cols = None
    while True:
        for _ in range(4):
            try:
                r = requests.get(URL, params={'date': d, 'assetcode': 'Si',
                                              'iss.meta': 'off', 'start': start},
                                 timeout=30, headers=H, verify=False)
                j = r.json()['history']
                cols = j['columns']; data = j['data']
                break
            except Exception as e:
                time.sleep(2)
        else:
            return cols, rows
        if not data:
            break
        rows.extend(data)
        if len(data) < 100:
            break
        start += 100
        time.sleep(0.05)
    return cols, rows


def main():
    cur = date(2020, 1, 1)
    end = date(2026, 5, 19)
    out_rows = []
    n = 0
    while cur <= end:
        if cur.weekday() >= 5:
            cur += timedelta(days=1)
            continue
        d = cur.strftime('%Y-%m-%d')
        cols, rows = fetch_day(d)
        if rows and cols:
            ci = {c: cols.index(c) for c in cols}
            best = None
            for row in rows:
                secid = row[ci['SECID']]
                # only outright quarterly contracts: SiX# (4 chars), skip spreads (contain 2nd Si)
                if not secid.startswith('Si') or 'Si' in secid[2:]:
                    continue
                oi = row[ci.get('OPENPOSITION', -1)] if 'OPENPOSITION' in ci else None
                settle = row[ci.get('SETTLEPRICE', -1)] if 'SETTLEPRICE' in ci else None
                close = row[ci.get('CLOSE', -1)] if 'CLOSE' in ci else None
                oi = oi or 0
                if best is None or (oi or 0) > best[1]:
                    best = (secid, oi or 0, close, settle,
                            row[ci.get('VOLUME', -1)] if 'VOLUME' in ci else None)
            if best:
                secid, oi, close, settle, vol = best
                px = close if close not in (None, '') else settle
                if px not in (None, ''):
                    out_rows.append([d, secid, close, settle, vol, oi,
                                     round(float(px) / 1000.0, 4)])
        n += 1
        if n % 60 == 0:
            print(f"  {d}: {len(out_rows)} days collected", flush=True)
        cur += timedelta(days=1)

    with open(OUT, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'secid', 'close', 'settle', 'volume', 'oi', 'rub_per_usd'])
        w.writerows(out_rows)
    print(f"\nSaved {len(out_rows)} days -> {OUT.name}")
    if out_rows:
        print(f"  {out_rows[0][0]} ({out_rows[0][6]}) -> {out_rows[-1][0]} ({out_rows[-1][6]})")


if __name__ == "__main__":
    main()
