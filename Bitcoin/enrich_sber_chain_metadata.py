"""Enrich SBER options chain CSV with STRIKE, OPTIONTYPE, LSTDELDATE.

The history endpoint doesn't return these. We fetch them per-SECID from
/iss/securities/{SECID}.json (bulk fetch ~5-10k unique SECIDs over 6 years).

Output: sber_option_metadata.csv (SECID → STRIKE, OPTIONTYPE, LSTDELDATE, UNDERLYINGASSET, UNIT)
        Plus enriched sber_options_history_enriched.csv combined.

Run AFTER both fetchers complete (or any time — incremental, skips already known).
"""
import requests
import pandas as pd
import csv
import time
from pathlib import Path

ROOT = Path(__file__).parent
INPUT_CSVS = [ROOT/'sber_options_history.csv', ROOT/'sber_options_history_part2.csv']
META_OUT = ROOT/'sber_option_metadata.csv'
URL = "https://iss.moex.com/iss/securities/{}.json?iss.meta=off"

FIELDS_TO_KEEP = ['STRIKE', 'OPTIONTYPE', 'LSTDELDATE', 'FRSTTRADE',
                   'ASSETCODE', 'UNDERLYINGASSET', 'UNIT', 'LOTSIZE', 'EXECTYPE']


def fetch_meta(secid, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(URL.format(secid), timeout=20,
                             headers={'User-Agent': 'Mozilla/5.0'})
            r.raise_for_status()
            d = r.json()
            desc = d.get('description', {})
            cols = desc.get('columns', [])
            rows = desc.get('data', [])
            if not rows:
                return None
            kv = {row[cols.index('name')]: row[cols.index('value')] for row in rows
                  if 'name' in cols and 'value' in cols}
            return {f: kv.get(f) for f in FIELDS_TO_KEEP}
        except Exception as e:
            time.sleep(2 ** attempt)
    return None


def main():
    # Load all unique SECIDs from history CSVs
    all_secids = set()
    for path in INPUT_CSVS:
        if path.exists():
            df = pd.read_csv(path, usecols=['SECID'])
            all_secids.update(df['SECID'].dropna().unique())
    print(f"Total unique SECIDs across history files: {len(all_secids)}")

    # Skip filter-out composite SECIDs (calendar spreads etc) — usually have >9 chars or 'SR..SR..'
    options_secids = {s for s in all_secids
                       if isinstance(s, str)
                       and s.startswith('SR')
                       and 'SR' not in s[2:]  # no second SR
                       and len(s) <= 11}
    skipped = all_secids - options_secids
    print(f"Filtered to {len(options_secids)} likely option SECIDs (skipped {len(skipped)} composites/non-options)")

    # Load already-known metadata
    known = {}
    if META_OUT.exists():
        with open(META_OUT) as f:
            reader = csv.DictReader(f)
            for row in reader:
                known[row['SECID']] = row
        print(f"Already have metadata for {len(known)} SECIDs")

    to_fetch = sorted(options_secids - set(known.keys()))
    print(f"Need to fetch: {len(to_fetch)}")

    if not to_fetch:
        print("All up to date!")
        return

    # Open in append mode (write header if first time)
    write_header = not META_OUT.exists()
    with open(META_OUT, 'a', newline='') as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(['SECID'] + FIELDS_TO_KEEP)

        for i, secid in enumerate(to_fetch):
            meta = fetch_meta(secid)
            if meta:
                w.writerow([secid] + [meta.get(field, '') for field in FIELDS_TO_KEEP])
            if (i + 1) % 100 == 0:
                f.flush()
                print(f"  {i+1}/{len(to_fetch)} fetched", flush=True)
            time.sleep(0.05)  # 20 req/s

    print(f"\nDONE. Metadata file: {META_OUT.name} ({META_OUT.stat().st_size//1024}KB)")


if __name__ == "__main__":
    main()
