"""Merge the two parallel-fetched Si chain files, dedup by (TRADEDATE, SECID).

P1 covers 2020-01 → ~2024, P2 covers 2024-01 → 2026-05.
Overlap region 2024-01 → 2024-05 will have duplicates — keep first.

Output:
  si_options_history_merged.csv
  si_futures_history_merged.csv
"""
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).parent


def merge(files, key_cols, out_name):
    frames = []
    for f in files:
        p = ROOT / f
        if p.exists():
            df = pd.read_csv(p)
            frames.append(df)
            print(f"  {f}: {len(df)} rows")
    if not frames:
        print(f"  No input files for {out_name}!")
        return None
    combined = pd.concat(frames, ignore_index=True)
    before = len(combined)
    combined = combined.drop_duplicates(subset=key_cols, keep='first')
    after = len(combined)
    combined = combined.sort_values(['TRADEDATE', 'SECID']).reset_index(drop=True)
    out = ROOT / out_name
    combined.to_csv(out, index=False)
    print(f"  Merged → {out_name}: {before} → {after} rows after dedup "
          f"({before-after} dups removed)")
    print(f"  Date range: {combined['TRADEDATE'].min()} → {combined['TRADEDATE'].max()}")
    return combined


def main():
    print("Merging options chain:")
    opt = merge(['si_options_history.csv', 'si_options_history_part2.csv'],
                ['TRADEDATE', 'SECID'], 'si_options_history_merged.csv')

    print("\nMerging futures:")
    fut = merge(['si_futures_history.csv', 'si_futures_history_part2.csv'],
                ['TRADEDATE', 'SECID'], 'si_futures_history_merged.csv')

    if opt is not None:
        print(f"\nOptions coverage: {opt['TRADEDATE'].nunique()} unique trading days")
        print(f"Unique option SECIDs: {opt['SECID'].nunique()}")
    if fut is not None:
        print(f"Futures coverage: {fut['TRADEDATE'].nunique()} unique trading days")


if __name__ == "__main__":
    main()
