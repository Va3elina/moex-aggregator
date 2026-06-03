"""Fast SBER futoi EOD fetcher — only last snapshot per day via latest=1."""
import requests
import csv
import time
from pathlib import Path

OUT = Path(__file__).parent / "sber_futoi_history.csv"
URL = "https://iss.moex.com/iss/analyticalproducts/futoi/securities/SR.json"


def fetch_chunk(d_from, d_till):
    for attempt in range(4):
        try:
            r = requests.get(URL, params={'from': d_from, 'till': d_till, 'latest': 1},
                             timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  retry {attempt+1}: {e}")
            time.sleep(2 ** attempt)
    return None


def main():
    # Chunk by 90 days
    from datetime import date, timedelta
    cur = date(2020, 1, 1)
    end = date(2026, 5, 19)  # free API blocks last 14 days
    rows_all = []
    while cur <= end:
        chunk_end = min(cur + timedelta(days=90), end)
        d = fetch_chunk(cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"))
        if d:
            block = d['futoi']
            cols = block['columns']
            rows = block['data']
            print(f"  {cur} → {chunk_end}: {len(rows)} rows")
            rows_all.extend(rows)
        cur = chunk_end + timedelta(days=1)
        time.sleep(0.2)

    if not rows_all:
        print("No data!")
        return
    cols = ['sess_id', 'seqnum', 'tradedate', 'tradetime', 'ticker', 'clgroup',
            'pos', 'pos_long', 'pos_short', 'pos_long_num', 'pos_short_num',
            'systime', 'trade_session_date']
    print(f"\nTotal rows: {len(rows_all)}")

    # Pivot: date → {YUR_long, YUR_short, FIZ_long, FIZ_short}
    by_date = {}
    for row in rows_all:
        d = dict(zip(cols, row))
        date_key = d['tradedate']
        if date_key not in by_date:
            by_date[date_key] = {}
        clg = d['clgroup']
        by_date[date_key][f'{clg}_long'] = d['pos_long']
        by_date[date_key][f'{clg}_short'] = d['pos_short']
        by_date[date_key][f'{clg}_long_num'] = d['pos_long_num']
        by_date[date_key][f'{clg}_short_num'] = d['pos_short_num']

    # Write pivoted CSV
    with open(OUT, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'yur_long', 'yur_short', 'fiz_long', 'fiz_short',
                    'yur_net', 'fiz_net', 'yur_total_n', 'fiz_total_n', 'smart_money_ratio'])
        for date_key in sorted(by_date.keys()):
            r = by_date[date_key]
            yur_long = r.get('YUR_long', 0)
            yur_short = abs(r.get('YUR_short', 0))
            fiz_long = r.get('FIZ_long', 0)
            fiz_short = abs(r.get('FIZ_short', 0))
            yur_net = yur_long - yur_short
            fiz_net = fiz_long - fiz_short
            yur_n = r.get('YUR_long_num', 0) + r.get('YUR_short_num', 0)
            fiz_n = r.get('FIZ_long_num', 0) + r.get('FIZ_short_num', 0)
            smart_ratio = yur_net / (abs(fiz_net) + 1)
            w.writerow([date_key, yur_long, yur_short, fiz_long, fiz_short,
                        yur_net, fiz_net, yur_n, fiz_n, round(smart_ratio, 4)])
    print(f"Saved {len(by_date)} days to {OUT.name}")


if __name__ == "__main__":
    main()
