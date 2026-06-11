#!/usr/bin/env python3
"""Ключевая ставка ЦБ РФ → macro_data (indicator='KEY_RATE', дневной ряд).

Источник: HTML-таблица ЦБ https://www.cbr.ru/hd_base/KeyRate/ (РФ, не блокируется).
ЦБ отдаёт ставку как дневной ряд (ступенчатая функция между заседаниями). Нужен для
паттернов «ЦБ-заседания» и «навес ОФЗ» (контекст стоимости денег).

Запуск:
    python Candles/fetch_key_rate.py            # с 2013 по сегодня, upsert
    python Candles/fetch_key_rate.py --from 2024-01-01
Идемпотентно (ON CONFLICT). После — апдейтить раз в неделю/после заседаний.
"""
import os
import re
import sys
import argparse
import urllib.request
from datetime import date, datetime

from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")
URL = ("https://www.cbr.ru/hd_base/KeyRate/?UniDbQuery.Posted=True"
       "&UniDbQuery.From={frm}&UniDbQuery.To={to}")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def fetch(frm: date, to: date) -> list[tuple[date, float]]:
    url = URL.format(frm=frm.strftime("%d.%m.%Y"), to=to.strftime("%d.%m.%Y"))
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        html = r.read().decode("utf-8", "ignore")
    # таблица: <td>DD.MM.YYYY</td><td>NN,NN</td>
    pairs = re.findall(
        r"<td[^>]*>\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})\s*</td>\s*"
        r"<td[^>]*>\s*([0-9]+,[0-9]+)\s*</td>", html)
    out = []
    for d, v in pairs:
        out.append((datetime.strptime(d, "%d.%m.%Y").date(),
                    float(v.replace(",", "."))))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", default="2013-01-01")
    args = ap.parse_args()
    frm = date.fromisoformat(args.frm)
    to = date.today()

    rows = fetch(frm, to)
    if not rows:
        print("FATAL: ЦБ вернул пустую таблицу ставки", file=sys.stderr)
        sys.exit(1)
    rows.sort()
    print(f"ЦБ ключевая ставка: {len(rows)} дней [{rows[0][0]}..{rows[-1][0]}], "
          f"текущая {rows[-1][1]}%")

    eng = create_engine(DB_URL)
    with eng.begin() as conn:
        # метаданные индикатора (как у GDP/M2)
        conn.execute(text("""
            INSERT INTO macro (indicator, name, frequency, source, start_date)
            VALUES ('KEY_RATE', 'Ключевая ставка ЦБ РФ', 'daily', 'CBR', :sd)
            ON CONFLICT (indicator) DO UPDATE SET start_date = LEAST(macro.start_date, EXCLUDED.start_date)
        """), {"sd": rows[0][0]})
        n = 0
        for d, v in rows:
            conn.execute(text("""
                INSERT INTO macro_data (indicator, period_date, value, source)
                VALUES ('KEY_RATE', :d, :v, 'CBR')
                ON CONFLICT (indicator, period_date) DO UPDATE SET
                    value = EXCLUDED.value, source = EXCLUDED.source
            """), {"d": d, "v": v})
            n += 1
    print(f"upsert: {n} строк в macro_data (indicator=KEY_RATE)")


if __name__ == "__main__":
    main()
