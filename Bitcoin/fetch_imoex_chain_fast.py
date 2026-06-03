"""Fast IMOEX (MIX) options chain history fetcher.

Downloads daily snapshots of all MIX options (ASSETCODE=MIX, prefix MX, options on
the MX index futures MXM6/MXU6/...) plus the underlying MX index futures, 2020-2026.

Usage:
    python3 fetch_imoex_chain_fast.py [START_YYYY-MM-DD] [END_YYYY-MM-DD] [SUFFIX]

SUFFIX lets two processes write to separate files (e.g. '' and '_part2') so we can
run 2020-> and 2024-> in parallel and merge afterwards.

Output: imoex_options_history{SUFFIX}.csv  (settle, OI, volume per option per day)
        imoex_futures_history{SUFFIX}.csv   (MX index futures settles per day)
"""
import requests
import urllib3
import csv
import time
import json
import sys
from pathlib import Path
from datetime import date, timedelta

urllib3.disable_warnings()

ROOT = Path(__file__).parent

SUFFIX = sys.argv[3] if len(sys.argv) > 3 else ''
OUT_OPT = ROOT / f"imoex_options_history{SUFFIX}.csv"
OUT_FUT = ROOT / f"imoex_futures_history{SUFFIX}.csv"
PROG = ROOT / f".imoex_chain_progress{SUFFIX}.json"

URL_OPT = "https://iss.moex.com/iss/history/engines/futures/markets/options/securities.json"
URL_FUT = "https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json"

ASSETCODE = 'MIX'
START = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2020, 1, 1)
END = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2026, 5, 19)

RATE = 0.05


def fetch_page(url, params, retries=6):
    for attempt in range(retries):
        try:
            # Fall back to verify=False on SSL errors (sandbox cert race)
            verify = attempt < 2
            r = requests.get(url, params=params, timeout=30, verify=verify,
                             headers={'User-Agent': 'Mozilla/5.0'})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  retry {attempt+1}: {str(e)[:90]}", flush=True)
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
        time.sleep(RATE)
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
                'TRADEDATE', 'SECID', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                'SETTLEPRICE', 'WAPRICE', 'VOLUME', 'OPENPOSITION', 'NUMTRADES', 'BOARDID'
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
            cio = {c: i for i, c in enumerate(cols_o)}
            with open(OUT_OPT, 'a', newline='') as f:
                w = csv.writer(f)
                for row in rows_o:
                    g = lambda k: row[cio[k]] if k in cio else None
                    w.writerow([
                        g('TRADEDATE'), g('SECID'),
                        g('OPEN'), g('HIGH'), g('LOW'), g('CLOSE'),
                        g('SETTLEPRICE'), g('WAPRICE'),
                        g('VOLUME'), g('OPENPOSITION'), g('NUMTRADES'), g('BOARDID')
                    ])
            n_opt += len(rows_o)

        cols_f, rows_f = fetch_day(d_str, URL_FUT)
        if rows_f:
            cif = {c: i for i, c in enumerate(cols_f)}
            with open(OUT_FUT, 'a', newline='') as f:
                w = csv.writer(f)
                for row in rows_f:
                    g = lambda k: row[cif[k]] if k in cif else None
                    w.writerow([
                        g('TRADEDATE'), g('SECID'),
                        g('OPEN'), g('HIGH'), g('LOW'), g('CLOSE'),
                        g('SETTLEPRICE'), g('WAPRICE'),
                        g('VOLUME'), g('OPENPOSITION')
                    ])
            n_fut += len(rows_f)

        n_days += 1
        progress['last_done'] = d_str
        progress['total_opt'] = progress.get('total_opt', 0) + len(rows_o or [])
        progress['total_fut'] = progress.get('total_fut', 0) + len(rows_f or [])

        if n_days % 20 == 0:
            elapsed = time.time() - t_start
            rate = n_days / elapsed if elapsed else 0
            remaining = (END - cur).days
            eta = (remaining / rate / 60) if rate else 0
            print(f"  {d_str}: +{n_opt} opt, +{n_fut} fut, {n_days}d at "
                  f"{rate:.1f}d/s, ETA {eta:.0f}min", flush=True)
            save_progress(progress)

        cur += timedelta(days=1)

    save_progress(progress)
    print(f"\nDONE {SUFFIX or '(main)'}: +{n_opt} opt rows, +{n_fut} fut rows over {n_days} days",
          flush=True)


if __name__ == "__main__":
    main()
