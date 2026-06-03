"""Resolve exact expiry (LSTDELDATE) for each unique SBER option expiry_code.

Far fewer unique expiry codes (~600-800) than SECIDs (~49k), so this is fast.
For each code, pick one representative SECID and fetch its LSTDELDATE.

Output: sber_expiry_map.csv  (expiry_code → expiry_date)
"""
import pandas as pd
import requests
import time
import csv
from pathlib import Path
from parse_sber_secid import parse_secid

ROOT = Path(__file__).parent
OUT = ROOT / 'sber_expiry_map.csv'
INPUT_CSVS = ['sber_options_history.csv', 'sber_options_history_part2.csv',
              'sber_options_history_merged.csv']


def fetch_lstdel(secid, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(f'https://iss.moex.com/iss/securities/{secid}.json?iss.meta=off',
                             timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
            r.raise_for_status()
            d = r.json()
            desc = d.get('description', {})
            cols = desc.get('columns', [])
            rows = desc.get('data', [])
            kv = {row[cols.index('name')]: row[cols.index('value')] for row in rows if 'name' in cols}
            return kv.get('LSTDELDATE')
        except Exception:
            time.sleep(2 ** attempt)
    return None


def main():
    # Gather unique expiry_code → representative SECID
    code_to_secid = {}
    for fname in INPUT_CSVS:
        p = ROOT / fname
        if not p.exists():
            continue
        df = pd.read_csv(p, usecols=['SECID'])
        for s in df['SECID'].dropna().unique():
            parsed = parse_secid(s)
            if parsed:
                code = parsed['expiry_code']
                if code not in code_to_secid:
                    code_to_secid[code] = s
    print(f"Unique expiry codes: {len(code_to_secid)}")

    # Load already-resolved
    known = {}
    if OUT.exists():
        kdf = pd.read_csv(OUT)
        known = dict(zip(kdf['expiry_code'], kdf['expiry_date']))
        print(f"Already resolved: {len(known)}")

    to_do = {c: s for c, s in code_to_secid.items() if c not in known}
    print(f"To resolve: {len(to_do)}")

    write_header = not OUT.exists()
    with open(OUT, 'a', newline='') as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(['expiry_code', 'expiry_date', 'rep_secid'])
        for i, (code, secid) in enumerate(sorted(to_do.items())):
            ld = fetch_lstdel(secid)
            if ld:
                w.writerow([code, ld, secid])
            if (i + 1) % 50 == 0:
                f.flush()
                print(f"  {i+1}/{len(to_do)}", flush=True)
            time.sleep(0.03)

    print(f"Done. {OUT.name}")


if __name__ == "__main__":
    main()
