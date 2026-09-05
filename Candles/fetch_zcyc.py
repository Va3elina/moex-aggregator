#!/usr/bin/env python3
"""Бескупонная кривая доходности ОФЗ (ZCYC) → macro_data (ZCYC_1Y … ZCYC_15Y).

⚠️ ЭТОТ РЯД ТРИ МЕСЯЦА СТОЯЛ. Скрипт был одноразовым: запущен 11.06.2026 и нигде
не расписан, монитор исключил его как «мёртвый», а панель показывала ряд ЦБ
на 11.06 при зелёном macro_daily. Теперь он в оркестраторе (zcyc_daily, 19:10)
и gap-safe: берёт с последней точки в базе минус неделя.

Два источника одной кривой:
- ЦБ, hd_base/zcyc_params — HTML-таблица, ДИАПАЗОН дат одним запросом. Основной.
- МосБиржа, ISS /iss/engines/stock/zcyc.json?date= — официальный JSON-API, но по
  одному дню на запрос. Фоллбэк: если ЦБ не ответил или не распознан, добираем
  недостающие дни (не больше 10) отсюда. Кривую для ЦБ считает сама биржа,
  значения совпадают до сотых (проверено 04.09.2026: 13,28 / 13,38 / 13,57).

Запуск: python Candles/fetch_zcyc.py [--from 2020-01-01]
Итог — последней строкой JSON для оркестратора.
"""
import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import date, datetime, timedelta

from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")
CBR_URL = ("https://www.cbr.ru/hd_base/zcyc_params/?UniDbQuery.Posted=True"
           "&UniDbQuery.From={frm}&UniDbQuery.To={to}")
ISS_URL = "https://iss.moex.com/iss/engines/stock/zcyc.json?date={d}"
TENORS = ["1", "3", "5", "10", "15"]   # лет
UA = "Mozilla/5.0 (compatible; FrameBot/1.0)"
ЗАПАС_ДНЕЙ = 7          # перечитываем хвост: ЦБ иногда правит последние точки
ФОЛЛБЭК_ДНЕЙ = 10       # больше дней по одному через ISS не добираем — это уже бэкфилл


def parse_cbr(html: str) -> list[dict]:
    """[{date, '1': 13.8, '3': 15.16, …}] из HTML-таблицы ЦБ. Без lxml."""
    th = [t.strip().replace(",", ".") for t in re.findall(r"<th[^>]*>([^<]+)</th>", html)]
    tenors = [t for t in th if re.fullmatch(r"\d+(\.\d+)?", t)]
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL):
        tds = [re.sub(r"<[^>]+>", "", c).strip() for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.DOTALL)]
        if not tds or not re.match(r"\d{2}\.\d{2}\.\d{4}", tds[0]):
            continue
        rec = {"date": datetime.strptime(tds[0], "%d.%m.%Y").date()}
        for ten, v in zip(tenors, [v.replace(",", ".").replace(" ", "") for v in tds[1:]]):
            try:
                rec[ten] = float(v)
            except ValueError:
                pass
        out.append(rec)
    return out


def fetch_cbr(frm: date, to: date) -> list[dict]:
    url = CBR_URL.format(frm=frm.strftime("%d.%m.%Y"), to=to.strftime("%d.%m.%Y"))
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=45) as r:
        return parse_cbr(r.read().decode("utf-8", "ignore"))


def fetch_iss(d: date) -> dict | None:
    """Кривая за один день из ISS: {date, '1': …}. None — торгов не было."""
    req = urllib.request.Request(ISS_URL.format(d=d.isoformat()), headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=20) as r:
        j = json.load(r)
    блок = j.get("yearyields") or {}
    cols, rows = блок.get("columns") or [], блок.get("data") or []
    if not rows:
        return None
    ip, iv = cols.index("period"), cols.index("value")
    rec = {"date": d}
    for row in rows:
        срок = row[ip]
        if float(срок).is_integer() and str(int(срок)) in TENORS:
            rec[str(int(срок))] = float(row[iv])
    return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", default=None, help="начало; по умолчанию — последняя точка в базе минус неделя")
    args = ap.parse_args()

    eng = create_engine(DB_URL)
    with eng.connect() as conn:
        была = conn.execute(text("SELECT MAX(period_date) FROM macro_data WHERE indicator = 'ZCYC_10Y'")).scalar()
    to = date.today()
    frm = (date.fromisoformat(args.frm) if args.frm
           else (была - timedelta(days=ЗАПАС_ДНЕЙ)) if была else date(2020, 1, 1))

    источник, recs, ошибки = "cbr", [], []
    try:
        recs = fetch_cbr(frm, to)
        if not recs:
            ошибки.append("ЦБ: таблица не распознана")
    except Exception as e:  # noqa: BLE001
        ошибки.append(f"ЦБ: {type(e).__name__}: {str(e)[:120]}")

    if not recs:
        # Фоллбэк — официальный API биржи, день за днём, только свежий хвост.
        источник = "iss"
        d = max(frm, to - timedelta(days=ФОЛЛБЭК_ДНЕЙ))
        while d <= to:
            if d.weekday() < 5:
                try:
                    rec = fetch_iss(d)
                    if rec:
                        recs.append(rec)
                except Exception as e:  # noqa: BLE001
                    ошибки.append(f"ISS {d}: {type(e).__name__}")
            d += timedelta(days=1)

    if not recs:
        print(json.dumps({"ошибка": "кривая не получена ни от ЦБ, ни от ISS", "детали": ошибки}, ensure_ascii=False))
        return 1

    строк = 0
    with eng.begin() as conn:
        mind = min(r["date"] for r in recs)
        for ten in TENORS:
            ind = f"ZCYC_{ten}Y"
            conn.execute(text("""
                INSERT INTO macro (indicator, name, frequency, source, start_date)
                VALUES (:i, :n, 'daily', 'CBR', :sd)
                ON CONFLICT (indicator) DO UPDATE SET start_date = LEAST(macro.start_date, EXCLUDED.start_date)
            """), {"i": ind, "n": f"Доходность ОФЗ {ten}Y (ZCYC ЦБ)", "sd": mind})
            for rec in recs:
                if ten in rec:
                    conn.execute(text("""
                        INSERT INTO macro_data (indicator, period_date, value, source)
                        VALUES (:i, :d, :v, :s)
                        ON CONFLICT (indicator, period_date) DO UPDATE SET value = EXCLUDED.value
                    """), {"i": ind, "d": rec["date"], "v": rec[ten], "s": "CBR" if источник == "cbr" else "MOEX_ISS"})
                    строк += 1
        стала = conn.execute(text("SELECT MAX(period_date) FROM macro_data WHERE indicator = 'ZCYC_10Y'")).scalar()

    новых = sum(1 for r in recs if была is None or r["date"] > была)
    итог = {"дней": len(recs), "новых_дней": новых, "последняя": стала.isoformat() if стала else None,
            "источник": источник, "записей": строк}
    if ошибки:
        итог["предупреждения"] = ошибки
    print(json.dumps(итог, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
