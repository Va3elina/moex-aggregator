"""Этап 0: история ставок рыночного риска срочного рынка МосБиржи (бесплатно, без авторизации).
ISS rms/engines/futures/objects/limits — MR1/MR2/MR3 и лимиты концентрации по базовому активу, с 22.05.2018.
Базовое ГО ≈ расчётная цена × лот × MR1 (проверка на 17.09.2026: GAZR MR1 0.17 ↔ ГО GZU6 17.5 %; Si 0.15 ↔ 14.9 %).
История самого поля IM — платный продукт «Итоги торгов» (3750 ₽ за месяц данных, только с 01.2023; письмо МосБиржи 08.06.2026).

    python3 fetch_risk_rates.py            # докачать недостающие дни в data/risk_rates.csv.gz
"""
import csv, gzip, json, pathlib, time, datetime as dt, urllib.request

OUT = pathlib.Path(__file__).parent / 'data' / 'risk_rates.csv.gz'
URL = 'https://iss.moex.com/iss/rms/engines/futures/objects/limits.json?iss.meta=off&iss.only=limits&limit=1000&date={d}'
COLS = ['tradedate', 'assetcode', 'mr1', 'mr2', 'mr3', 'lk1', 'lk2', 'updatetime']


def get(d):
    for attempt in range(4):
        try:
            with urllib.request.urlopen(URL.format(d=d), timeout=30) as r:
                js = json.load(r)['limits']
            c = js['columns']
            return [dict(zip(c, row)) for row in js['data']]
        except Exception as e:
            time.sleep(2 + 3 * attempt)
    raise RuntimeError(f'{d}: не удалось')


def main():
    rows, have = [], set()
    if OUT.exists():
        with gzip.open(OUT, 'rt') as f:
            rows = list(csv.DictReader(f)); have = {r['tradedate'] for r in rows}
    d, today, n = dt.date(2018, 5, 22), dt.date.today(), 0
    while d <= today:
        ds = d.isoformat()
        if d.weekday() < 5 and ds not in have:
            got = [r for r in get(ds) if r['tradedate'] == ds]     # на неторговый день ISS отдаёт ближайший — отбрасываем
            rows += [{k: r[k] for k in COLS} for r in got]
            n += 1
            if n % 100 == 0:
                print(ds, len(rows), flush=True); save(rows)
            time.sleep(0.25)
        d += dt.timedelta(days=1)
    save(rows); print('готово: строк', len(rows), 'новых дней', n)


def save(rows):
    with gzip.open(OUT, 'wt', newline='') as f:
        w = csv.DictWriter(f, COLS); w.writeheader(); w.writerows(rows)


if __name__ == '__main__':
    main()
