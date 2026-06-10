#!/usr/bin/env python3
"""Точечный backfill дневных/часовых/5-мин свечей фьючерсных контрактов из ISS MOEX.

Зачем: штатный fetch_candles_futures_realtime.py качает только АКТИВНЫЙ контракт
серии. Историю по уже истёкшим контрактам (нужна для непрерывного графика цены
и всех ТФ, как у любого старого фьючерса вроде BR) он не докачивает. Этот скрипт
точечно заливает полную историю заданных контрактов.

Источник — публичный ISS forts candles (без ключа), отдаёт interval=24/60/1
с пагинацией по 500. 5-минутки (наш interval=5) ISS/Algopack напрямую НЕ отдают,
поэтому строим агрегацией 1-минуток — 1:1 с aggregate_to_5min в realtime-сборщике.

Идемпотентно (ON CONFLICT DO UPDATE). sec_id (prefix серии) = secid без года.

Usage:
  python Candles/backfill_futures_candles.py FROM TILL SECID [SECID ...]
  напр.  python Candles/backfill_futures_candles.py 2025-11-01 2026-06-09 BTZ5 BTF6 BTG6

  --intervals 24,60  — ограничить интервалы (без 5: пропускает тяжёлую догрузку
  1-минуток). Для массового ремонта дневных/часовых свечей по сотням контрактов
  (инцидент DO NOTHING-заморозки, июнь 2026) 1-минутки за месяцы — это сотни
  запросов на контракт к ISS, риск бана IP. По умолчанию — как раньше: 24,60,5.
"""
import sys
import os
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

ISS = ("https://iss.moex.com/iss/engines/futures/markets/forts/securities/"
       "%s/candles.json?interval=%s&from=%s&till=%s&start=%s&iss.meta=off")

UP = text("""
    INSERT INTO candles (secid, sec_id, open, close, high, low, value, volume,
                         begin_time, end_time, interval, type)
    VALUES (:secid, :sec_id, :o, :c, :h, :l, :v, :vol, :b, :e, :iv, 'futures')
    ON CONFLICT (secid, begin_time, interval, type) DO UPDATE SET
      open = EXCLUDED.open, close = EXCLUDED.close,
      high = EXCLUDED.high, low = EXCLUDED.low,
      value = EXCLUDED.value, volume = EXCLUDED.volume,
      end_time = EXCLUDED.end_time
""")


def fetch(secid, interval, frm, till):
    """Свечи контракта за период с пагинацией. Ретрай на 429."""
    out, start = [], 0
    while True:
        url = ISS % (secid, interval, frm, till, start)
        data = None
        for _ in range(4):
            try:
                with urllib.request.urlopen(url, timeout=45) as r:
                    data = json.load(r).get("candles", {}).get("data", [])
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(5)
                    continue
                raise
        if not data:
            break
        out.extend(data)
        if len(data) < 500:
            break
        start += len(data)
        time.sleep(0.08)
    return out


def _floor5(dt):
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second,
                          microseconds=dt.microsecond)


def agg5(minrows):
    """Агрегация 1-минуток в 5-минутки. ISS cols: open,close,high,low,value,volume,begin,end."""
    buckets, order = {}, []
    for r in minrows:
        if r[0] is None or r[1] is None or r[2] is None or r[3] is None:
            continue
        k = _floor5(datetime.strptime(r[6], "%Y-%m-%d %H:%M:%S"))
        if k not in buckets:
            buckets[k] = [r[0], r[1], r[2], r[3], r[4] or 0, r[5] or 0]  # o,c,h,l,val,vol
            order.append(k)
        else:
            x = buckets[k]
            x[1] = r[1]                # close = last
            x[2] = max(x[2], r[2])     # high
            x[3] = min(x[3], r[3])     # low
            x[4] += r[4] or 0          # value
            x[5] += r[5] or 0          # volume
    res = []
    for k in order:
        o, c, h, l, val, vol = buckets[k]
        e = k + timedelta(minutes=4, seconds=59)
        res.append((o, c, h, l, val, vol,
                    k.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S")))
    return res


def main():
    args = sys.argv[1:]
    intervals = (24, 60, 5)
    if "--intervals" in args:
        i = args.index("--intervals")
        intervals = tuple(int(x) for x in args[i + 1].split(","))
        args = args[:i] + args[i + 2:]
    if len(args) < 3:
        print("usage: backfill_futures_candles.py [--intervals 24,60] FROM TILL SECID [SECID ...]")
        sys.exit(1)
    frm, till, contracts = args[0], args[1], args[2:]
    eng = create_engine(os.environ["DB_URL"], connect_args={"ssl_context": False})
    grand = 0
    for secid in contracts:
        sec_id = secid[:-1]
        with eng.begin() as conn:
            for iv in (i for i in (24, 60) if i in intervals):
                rows = fetch(secid, iv, frm, till)
                n = 0
                for r in rows:
                    if r[0] is None or r[1] is None or r[2] is None or r[3] is None:
                        continue
                    conn.execute(UP, {"secid": secid, "sec_id": sec_id, "o": r[0], "c": r[1],
                                      "h": r[2], "l": r[3], "v": r[4], "vol": r[5],
                                      "b": r[6], "e": r[7], "iv": iv})
                    n += 1
                grand += n
                print("%s iv=%-2d: %d" % (secid, iv, n), flush=True)
            if 5 in intervals:
                five = agg5(fetch(secid, 1, frm, till))
                for o, c, h, l, val, vol, b, e in five:
                    conn.execute(UP, {"secid": secid, "sec_id": sec_id, "o": o, "c": c, "h": h,
                                      "l": l, "v": val, "vol": vol, "b": b, "e": e, "iv": 5})
                grand += len(five)
                print("%s iv=5 : %d" % (secid, len(five)), flush=True)
    print("TOTAL upserted:", grand)


if __name__ == "__main__":
    main()
