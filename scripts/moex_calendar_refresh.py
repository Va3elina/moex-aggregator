#!/usr/bin/env python3
"""Обновить блок календаря ISS в moex_calendar.py (дни, когда закрыты фондовый и срочный рынки).

Источник — календарь ISS через AlgoPack (`apim.moex.com/iss/calendars.json`, только с токеном;
бесплатный iss.moex.com отдаёт HTML). Запуск на сервере в контейнере с токеном — 1 запрос на год:

    docker exec -i -w /app frame-orchestrator-1 python3 - --years 2024-2027 < scripts/moex_calendar_refresh.py > /tmp/block.py
    # затем вставить /tmp/block.py между маркерами в moex_calendar.py (или --json FILE локально) и отправить PR

Или из уже скачанного JSON {год: [строки off_days]}:  python3 scripts/moex_calendar_refresh.py --json cal.json --write

Когда: в декабре (биржа публикует календарь следующего года) и при новостях о переносах.
Почему не на лету: moex_calendar импортируют десятки загрузчиков; сеть или токен при импорте —
лишняя точка отказа. Блок в коде проходит ревью и CI, как любая правка.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

START = "# >>> ISS CALENDAR (генерирует scripts/moex_calendar_refresh.py — руками не править)"
END = "# <<< ISS CALENDAR"


def fetch(years: list[int]) -> dict:
    import urllib.request
    sys.path.insert(0, "/app")
    from moex_tls import moex_ssl_context
    key, ctx, out = os.environ["ALGOPACK_API_KEY"], moex_ssl_context(), {}
    for y in years:
        url = (f"https://apim.moex.com/iss/calendars.json?iss.meta=off&iss.only=off_days"
               f"&from={y}-01-01&till={y}-12-31")
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=40, context=ctx) as r:
            b = json.load(r)["off_days"]
        out[str(y)] = [dict(zip(b["columns"], row)) for row in b["data"]]
        time.sleep(1)
    return out


def block(cal: dict) -> str:
    lines = [START, f"# Обновлено {date.today().isoformat()} из apim.moex.com/iss/calendars.json (off_days).",
             "ISS_CALENDAR_YEARS = {" + ", ".join(sorted(cal)) + "}",
             "ISS_STOCK_CLOSED = {"]
    for kind in ("stock", "futures"):
        if kind == "futures":
            lines += ["}", "ISS_FUTURES_CLOSED = {"]
        for y in sorted(cal):
            days = sorted(r["tradedate"] for r in cal[y] if r.get(f"{kind}_workday") == 0)
            for i in range(0, len(days), 6):
                lines.append("    " + " ".join(f"date({d[:4]}, {int(d[5:7])}, {int(d[8:10])})," for d in days[i:i + 6]))
    lines += ["}", END]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default=f"{date.today().year - 1}-{date.today().year + 1}")
    ap.add_argument("--json", help="готовый JSON {год: [строки off_days]} вместо запроса")
    ap.add_argument("--write", action="store_true", help="переписать блок в moex_calendar.py рядом")
    a = ap.parse_args()
    if a.json:
        cal = json.loads(Path(a.json).read_text())
    else:
        y0, y1 = map(int, a.years.split("-"))
        cal = fetch(list(range(y0, y1 + 1)))
    text = block(cal)
    if not a.write:
        print(text)
        return 0
    p = Path(__file__).resolve().parent.parent / "moex_calendar.py"
    src = p.read_text()
    new = re.sub(re.escape(START) + r".*?" + re.escape(END), lambda _: text, src, flags=re.S)
    if new == src and START not in src:
        sys.exit("маркеры блока не найдены в moex_calendar.py")
    p.write_text(new)
    print(f"блок обновлён: годы {', '.join(sorted(cal))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
