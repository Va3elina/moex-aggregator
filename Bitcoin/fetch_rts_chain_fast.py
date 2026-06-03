"""Fast RTS options chain history fetcher (part 1: 2020-01-01 -> 2023-12-31).

Mirror of fetch_gazp_chain_fast.py but assetcode=RTS, RI-prefixed SECIDs,
SSL verify disabled with retries. Pagination limit 100.

Output: rts_options_history.csv, rts_futures_history.csv
"""
import requests
import csv
import time
import json
from pathlib import Path
from datetime import date, timedelta
requests.packages.urllib3.disable_warnings()

ROOT = Path(__file__).parent
OUT_OPT = ROOT / "rts_options_history.csv"
OUT_FUT = ROOT / "rts_futures_history.csv"
PROG = ROOT / ".rts_chain_progress.json"

URL_OPT = "https://iss.moex.com/iss/history/engines/futures/markets/options/securities.json"
URL_FUT = "https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json"

START = date(2020, 1, 1)
END = date(2023, 12, 31)
ASSETCODE = 'RTS'
TAG = 'P1'


def fetch_page(url, params, retries=5):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30,
                             headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  retry {attempt+1}: {e}", flush=True)
            time.sleep(min(2 ** attempt, 8))
    return None


def fetch_day(d, url):
    start = 0
    all_rows = []
    cols = None
    while True:
        params = {'date': d, 'assetcode': ASSETCODE, 'iss.meta': 'off', 'start': start}
        data = fetch_page(url, params)
        if data is None:
            return None, None
        hist = data.get('history', {})
        if not cols:
            cols = hist.get('columns', [])
        rows = hist.get('data', [])
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < 100:
            break
        start += 100
        time.sleep(0.05)
    return cols, all_rows


def load_progress():
    if PROG.exists():
        return json.loads(PROG.read_text())
    return {'last_done': None, 'total_opt': 0, 'total_fut': 0}


def save_progress(p):
    PROG.write_text(json.dumps(p))


def main():
    progress = load_progress()
    resume_from = progress.get('last_done')

    if not OUT_OPT.exists():
        with open(OUT_OPT, 'w', newline='') as f:
            csv.writer(f).writerow([
                'TRADEDATE', 'SECID', 'STRIKE', 'OPTIONTYPE', 'LASTDELDATE',
                'OPEN', 'HIGH', 'LOW', 'CLOSE', 'SETTLEPRICE', 'WAPRICE',
                'VOLUME', 'OPENPOSITION', 'NUMTRADES', 'BOARDID'
            ])
    if not OUT_FUT.exists():
        with open(OUT_FUT, 'w', newline='') as f:
            csv.writer(f).writerow([
                'TRADEDATE', 'SECID', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                'SETTLEPRICE', 'WAPRICE', 'VOLUME', 'OPENPOSITION'
            ])

    cur = START
    if resume_from:
        cur = date.fromisoformat(resume_from) + timedelta(days=1)

    n_days = n_opt = n_fut = 0
    t_start = time.time()

    while cur <= END:
        if cur.weekday() >= 5:
            cur += timedelta(days=1)
            continue
        d_str = cur.strftime("%Y-%m-%d")

        cols_o, rows_o = fetch_day(d_str, URL_OPT)
        if rows_o:
            with open(OUT_OPT, 'a', newline='') as f:
                w = csv.writer(f)
                for row in rows_o:
                    rd = dict(zip(cols_o, row))
                    w.writerow([
                        rd.get('TRADEDATE'), rd.get('SECID'), '', '', '',
                        rd.get('OPEN'), rd.get('HIGH'), rd.get('LOW'), rd.get('CLOSE'),
                        rd.get('SETTLEPRICE'), rd.get('WAPRICE'),
                        rd.get('VOLUME'), rd.get('OPENPOSITION'), rd.get('NUMTRADES'),
                        rd.get('BOARDID')
                    ])
            n_opt += len(rows_o)

        cols_f, rows_f = fetch_day(d_str, URL_FUT)
        if rows_f:
            with open(OUT_FUT, 'a', newline='') as f:
                w = csv.writer(f)
                for row in rows_f:
                    rd = dict(zip(cols_f, row))
                    w.writerow([
                        rd.get('TRADEDATE'), rd.get('SECID'),
                        rd.get('OPEN'), rd.get('HIGH'), rd.get('LOW'), rd.get('CLOSE'),
                        rd.get('SETTLEPRICE'), rd.get('WAPRICE'),
                        rd.get('VOLUME'), rd.get('OPENPOSITION')
                    ])
            n_fut += len(rows_f)

        n_days += 1
        progress['last_done'] = d_str
        progress['total_opt'] = progress.get('total_opt', 0) + len(rows_o or [])
        progress['total_fut'] = progress.get('total_fut', 0) + len(rows_f or [])

        if n_days % 20 == 0:
            elapsed = time.time() - t_start
            rate = n_days / elapsed if elapsed else 0
            eta_min = ((END - cur).days / rate / 60) if rate else 0
            print(f"[{TAG}] {d_str}: +{n_opt} opt, +{n_fut} fut, "
                  f"{n_days}d at {rate:.1f}d/s, ETA {eta_min:.0f}min", flush=True)
            save_progress(progress)

        cur += timedelta(days=1)

    save_progress(progress)
    print(f"\n[{TAG}] DONE. +{n_opt} option rows, +{n_fut} future rows over {n_days} days", flush=True)


if __name__ == "__main__":
    main()
