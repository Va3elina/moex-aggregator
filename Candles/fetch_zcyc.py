#!/usr/bin/env python3
"""Бескупонная кривая доходности ОФЗ (ZCYC) ЦБ РФ → macro_data.

ЦБ публикует G-кривую (доходность по срокам до погашения) дневным рядом. Берём
ключевые сроки 1/3/5/10/15Y → macro_data (indicator ZCYC_<N>Y). Длинный конец
(10/15Y) — прокси «навеса ОФЗ» (паттерн #1) и общей стоимости денег.

Запуск: python Candles/fetch_zcyc.py [--from 2020-01-01]
"""
import os
import re
import sys
import argparse
import urllib.request
from datetime import date, datetime
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")
URL = ("https://www.cbr.ru/hd_base/zcyc_params/?UniDbQuery.Posted=True"
       "&UniDbQuery.From={frm}&UniDbQuery.To={to}")
TENORS = ["1", "3", "5", "10", "15"]   # лет
UA = "Mozilla/5.0 (compatible; FrameBot/1.0)"


def parse(html: str):
    """Возвращает список dict {date, tenor_label: yield}. Ручной парс (без lxml)."""
    # порядок сроков из заголовка (числовые th)
    th = [t.strip().replace(",", ".") for t in re.findall(r"<th[^>]*>([^<]+)</th>", html)]
    tenors = [t for t in th if re.fullmatch(r"\d+(\.\d+)?", t)]
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL):
        tds = [re.sub(r"<[^>]+>", "", c).strip() for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.DOTALL)]
        if not tds:
            continue
        m = re.match(r"\d{2}\.\d{2}\.\d{4}", tds[0])
        if not m:
            continue
        d = datetime.strptime(tds[0], "%d.%m.%Y").date()
        vals = [v.replace(",", ".").replace(" ", "") for v in tds[1:]]
        rec = {"date": d}
        for ten, v in zip(tenors, vals):
            try:
                rec[ten] = float(v)
            except ValueError:
                pass
        out.append(rec)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", default="2020-01-01")
    args = ap.parse_args()
    frm = date.fromisoformat(args.frm); to = date.today()

    url = URL.format(frm=frm.strftime("%d.%m.%Y"), to=to.strftime("%d.%m.%Y"))
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=45) as r:
        html = r.read().decode("utf-8", "ignore")
    recs = parse(html)
    if not recs:
        print("FATAL: ZCYC не распознан", file=sys.stderr); sys.exit(1)
    print(f"ZCYC: {len(recs)} дней [{recs[-1]['date']}..{recs[0]['date']}]")

    eng = create_engine(DB_URL)
    with eng.begin() as conn:
        mind = min(r["date"] for r in recs)
        for ten in TENORS:
            ind = f"ZCYC_{ten}Y"
            conn.execute(text("""
                INSERT INTO macro (indicator, name, frequency, source, start_date)
                VALUES (:i, :n, 'daily', 'CBR', :sd)
                ON CONFLICT (indicator) DO UPDATE SET start_date = LEAST(macro.start_date, EXCLUDED.start_date)
            """), {"i": ind, "n": f"Доходность ОФЗ {ten}Y (ZCYC ЦБ)", "sd": mind})
            n = 0
            for rec in recs:
                if ten not in rec:
                    continue
                conn.execute(text("""
                    INSERT INTO macro_data (indicator, period_date, value, source)
                    VALUES (:i, :d, :v, 'CBR')
                    ON CONFLICT (indicator, period_date) DO UPDATE SET value = EXCLUDED.value
                """), {"i": ind, "d": rec["date"], "v": rec[ten]})
                n += 1
            print(f"  {ind}: {n} строк")


if __name__ == "__main__":
    main()
