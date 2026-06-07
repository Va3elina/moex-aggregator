#!/usr/bin/env python3
"""
Сборщик дневных риск-параметров НКЦ/MOEX для «индекса страха».

Источники (ISS MOEX, бесплатные, без ключа):
  1. Срочный рынок — ставки рыночного риска (mr1/mr2/mr3):
     GET https://iss.moex.com/iss/rms/engines/futures/objects/limits.json?date=YYYY-MM-DD
     Глубина: с 2018-05-22. Покрывает индекс, нефть, газ, акции-фьючерсы, валюту-фьючерсы.
  2. Валютный рынок — ставки риска/дисконты по валютам:
     GET https://iss.moex.com/iss/rms/engines/currency/objects/marketrates.json?date=YYYY-MM-DD
     Глубина: с 2014-12-26. Самый глубокий ряд по USD/CNY/EUR.

Смысл сигнала: НКЦ поднимает ставку риска при росте волатильности/стресса.
Высокая ставка = страх, низкая = покой/жадность. Ряд ступенчатый (меняется редко).

Вывод: long-format CSV в RiskRates/data/:
  - limits_daily.csv     : date, assetcode, group, title, mr1, mr2, mr3
  - currency_daily.csv   : date, asset, base_discount, discount_levels

Запуск:
  python3 RiskRates/fetch_risk_rates.py                 # максимальная глубина
  python3 RiskRates/fetch_risk_rates.py --from 2018-01-01
  python3 RiskRates/fetch_risk_rates.py --only limits   # только срочный рынок
"""

import asyncio
import aiohttp
import argparse
import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path

# ---- настройки источников -----------------------------------------------

LIMITS_URL = "https://iss.moex.com/iss/rms/engines/futures/objects/limits.json"
CURRENCY_URL = "https://iss.moex.com/iss/rms/engines/currency/objects/marketrates.json"

LIMITS_START = date(2018, 5, 22)     # физический предел источника
CURRENCY_START = date(2014, 12, 26)  # физический предел источника

# Какие коды срочного рынка сохраняем (None = все 184 кода).
# Оставляем все — файл небольшой, зато потом можно выбрать любой инструмент.
LIMITS_CODES = None

# Валютные пары: assetid1 из currency-источника
CURRENCY_ASSETS = {"USD", "CNY", "EUR", "HKD", "GBP", "TRY", "KZT", "AED"}

MAX_CONCURRENT = 16
RETRIES = 4
DATA_DIR = Path(__file__).parent / "data"


# ---- HTTP с ретраями -----------------------------------------------------

async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict):
    delay = 1.0
    for attempt in range(RETRIES):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=40)) as r:
                if r.status == 200:
                    txt = await r.text()
                    return json.loads(txt)
                # 4xx/5xx — пробуем ещё
        except Exception:
            pass
        await asyncio.sleep(delay)
        delay *= 2
    return None


# ---- парсеры -------------------------------------------------------------

def parse_limits(doc):
    """-> list[dict] по одной строке на assetcode."""
    block = doc.get("limits", {})
    cols = block.get("columns", [])
    if not cols:
        return []
    idx = {c: i for i, c in enumerate(cols)}
    out = []
    for r in block.get("data", []):
        code = r[idx["assetcode"]]
        if LIMITS_CODES is not None and code not in LIMITS_CODES:
            continue
        out.append({
            "date": r[idx["tradedate"]],
            "assetcode": code,
            "group": r[idx.get("group_title", -1)] if "group_title" in idx else "",
            "title": r[idx.get("title", -1)] if "title" in idx else "",
            "mr1": r[idx["mr1"]],
            "mr2": r[idx["mr2"]],
            "mr3": r[idx["mr3"]],
        })
    return out


def parse_currency(doc):
    """-> list[dict]: базовый (минимальный) дисконт + все уровни по паре X/RUB."""
    block = doc.get("marketrates", {})
    cols = block.get("columns", [])
    if not cols:
        return []
    idx = {c: i for i, c in enumerate(cols)}
    a1, a2, dsc, td = idx["assetid1"], idx["assetid2"], idx["discount"], idx["tradedate"]
    # собираем все уровни дисконта по каждой паре
    levels = {}
    tradedate = None
    for r in block.get("data", []):
        base_asset = str(r[a1])
        if r[a2] != "RUB":
            continue
        if base_asset not in CURRENCY_ASSETS:
            continue
        tradedate = r[td]
        levels.setdefault(base_asset, []).append(r[dsc])
    out = []
    for asset, lv in levels.items():
        lv_sorted = sorted(set(lv))
        out.append({
            "date": tradedate,
            "asset": asset,
            "base_discount": min(lv_sorted),
            "discount_levels": "|".join(str(x) for x in lv_sorted),
        })
    return out


# ---- сбор по диапазону дат ----------------------------------------------

def daterange(start: date, end: date):
    d = start
    while d <= end:
        # пропускаем выходные сразу (в ISS их всё равно нет, экономим запросы)
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


async def collect(session, url, start, end, parser, label):
    dates = list(daterange(start, end))
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    results = {}
    done = 0

    async def one(d):
        nonlocal done
        async with sem:
            doc = await fetch_json(session, url, {
                "iss.meta": "off", "date": d.isoformat(),
            })
        rows = parser(doc) if doc else []
        if rows:
            results[d] = rows
        done += 1
        if done % 250 == 0:
            print(f"  [{label}] {done}/{len(dates)} дат обработано…", flush=True)

    await asyncio.gather(*(one(d) for d in dates))
    # собираем в плоский список, отсортированный по дате
    flat = []
    for d in sorted(results):
        flat.extend(results[d])
    print(f"  [{label}] готово: {len(results)} торговых дат, {len(flat)} строк", flush=True)
    return flat


def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  → {path}  ({len(rows)} строк)", flush=True)


# ---- main ----------------------------------------------------------------

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="frm", default=None, help="дата начала YYYY-MM-DD (по умолчанию — максимум источника)")
    ap.add_argument("--only", choices=["limits", "currency"], default=None)
    args = ap.parse_args()

    today = date.today()
    frm = date.fromisoformat(args.frm) if args.frm else None

    async with aiohttp.ClientSession() as session:
        if args.only in (None, "limits"):
            start = max(LIMITS_START, frm) if frm else LIMITS_START
            print(f"== Срочный рынок (limits, ставки риска) с {start} по {today} ==", flush=True)
            rows = await collect(session, LIMITS_URL, start, today, parse_limits, "limits")
            write_csv(DATA_DIR / "limits_daily.csv", rows,
                      ["date", "assetcode", "group", "title", "mr1", "mr2", "mr3"])

        if args.only in (None, "currency"):
            start = max(CURRENCY_START, frm) if frm else CURRENCY_START
            print(f"== Валютный рынок (currency, дисконты) с {start} по {today} ==", flush=True)
            rows = await collect(session, CURRENCY_URL, start, today, parse_currency, "currency")
            write_csv(DATA_DIR / "currency_daily.csv", rows,
                      ["date", "asset", "base_discount", "discount_levels"])

    print("Готово.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
