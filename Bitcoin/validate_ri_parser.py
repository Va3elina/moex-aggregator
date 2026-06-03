"""Validate RTS (RI) SECID parser against MOEX securities API on N samples.

Pulls chain snapshots, parses each SECID locally, then fetches the official
STRIKE + OPTIONTYPE from /iss/securities/{SECID}.json and compares.
Target: 100% match (as achieved for SBER/GAZP).
"""
import requests, time, random
from parse_ri_secid import parse_secid
requests.packages.urllib3.disable_warnings()
H = {'User-Agent': 'Mozilla/5.0'}
N = 200


def get_chain(date):
    rows = []
    start = 0
    while True:
        r = requests.get('https://iss.moex.com/iss/history/engines/futures/markets/options/securities.json',
                         params={'date': date, 'assetcode': 'RTS', 'iss.meta': 'off', 'start': start},
                         timeout=30, headers=H, verify=False)
        d = r.json()['history']; cols = d['columns']; data = d['data']
        if not data:
            break
        for row in data:
            rows.append(dict(zip(cols, row))['SECID'])
        if len(data) < 100:
            break
        start += 100
        time.sleep(0.05)
    return rows


def official(secid):
    for _ in range(4):
        try:
            r = requests.get(f'https://iss.moex.com/iss/securities/{secid}.json',
                             params={'iss.meta': 'off'}, timeout=20, headers=H, verify=False)
            d = r.json()['description']; cols = d['columns']
            kv = {row[cols.index('name')]: row[cols.index('value')] for row in d['data']}
            return kv.get('STRIKE'), kv.get('OPTIONTYPE')
        except Exception:
            time.sleep(1)
    return None, None


def main():
    secids = set()
    for date in ['2026-05-15', '2024-06-03', '2022-03-10', '2021-09-15', '2020-06-15']:
        try:
            secids.update(get_chain(date))
        except Exception as e:
            print('chain err', date, e)
    secids = [s for s in secids if parse_secid(s)]
    random.seed(42)
    random.shuffle(secids)
    sample = secids[:N]
    print(f"Validating {len(sample)} RTS SECIDs against API...")

    ok_strike = ok_type = total = 0
    fails = []
    for i, s in enumerate(sample):
        p = parse_secid(s)
        off_strike, off_type = official(s)
        if off_strike is None:
            continue
        total += 1
        if int(off_strike) == p['strike']:
            ok_strike += 1
        else:
            fails.append((s, 'strike', p['strike'], off_strike))
        if off_type == p['option_type']:
            ok_type += 1
        else:
            fails.append((s, 'type', p['option_type'], off_type))
        if (i + 1) % 50 == 0:
            print(f"  {i+1}/{len(sample)} checked", flush=True)
        time.sleep(0.03)

    print(f"\nTotal checked: {total}")
    print(f"  STRIKE match:     {ok_strike}/{total} = {ok_strike/total*100:.1f}%")
    print(f"  OPTIONTYPE match: {ok_type}/{total} = {ok_type/total*100:.1f}%")
    if fails:
        print("Failures:")
        for f in fails[:20]:
            print("  ", f)
    else:
        print("100% MATCH")


if __name__ == "__main__":
    main()
