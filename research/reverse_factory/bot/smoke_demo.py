"""Проверка работы с заявками на ДЕМО-счёте (виртуальные деньги): все вызовы, которыми пользуется исполнитель.
Монета SOL — бот её не торгует (минимальная заявка больше $5.5), поэтому проверка ему не мешает.
Запуск на сервере: sudo -u kamaz /opt/kamaz-bot/venv/bin/python /opt/kamaz-bot/bot/smoke_demo.py
На реальном ключе не запускается.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from bybit import Rest, Stream, WS_PRIVATE  # noqa: E402

SYM = "SOLUSDT"


def load_env():
    for line in (HERE / ".env").read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1); os.environ[k.strip()] = v.strip()


async def main():
    load_env()
    if os.environ.get("BYBIT_KEY_MODE") != "demo":
        raise SystemExit("ключ не демо — проверку не запускаю")
    r = Rest("demo", os.environ["BYBIT_API_KEY"], os.environ["BYBIT_API_SECRET"])
    seen = []

    async def on_msg(m):
        if m.get("topic", "").startswith("execution"):
            seen.extend((e["orderLinkId"], e["side"], e["execQty"], e["execPrice"]) for e in m.get("data", []) if e.get("symbol") == SYM)

    ws = Stream(WS_PRIVATE["demo"], ["execution.linear"], on_msg, auth=(r.key, r.secret), name="исполнения (проверка)")
    task = asyncio.create_task(ws.run())
    await asyncio.wait_for(ws.connected.wait(), 15)
    tag = str(int(time.time()))[-6:]
    ok = []

    def step(name, fn):
        try:
            out = fn(); ok.append((name, "ок")); print(f"  ✓ {name}"); return out
        except Exception as e:
            ok.append((name, f"ОШИБКА {e}")); print(f"  ✗ {name}: {e}"); return None

    px = float(r.public("/v5/market/tickers", dict(category="linear", symbol=SYM))["list"][0]["lastPrice"])
    print(f"SOL {px}")
    step("плечо x3", lambda: r.set_leverage(SYM, "3"))
    step("лимитная покупка далеко от цены", lambda: r.place(symbol=SYM, side="Buy", orderType="Limit", qty="0.1", price=f"{px * 0.8:.2f}",
                                                             timeInForce="GTC", orderLinkId=f"smk{tag}L"))
    step("перенос цены заявки", lambda: r.amend(symbol=SYM, orderLinkId=f"smk{tag}L", price=f"{px * 0.79:.2f}"))
    oo = step("список открытых заявок", lambda: r.open_orders(SYM))
    print("    заявка видна в списке:", any(o.get("orderLinkId") == f"smk{tag}L" for o in (oo or [])))
    step("снятие заявки", lambda: r.cancel(SYM, f"smk{tag}L"))
    step("покупка по рынку 0.1 SOL", lambda: r.place(symbol=SYM, side="Buy", orderType="Market", qty="0.1", orderLinkId=f"smk{tag}E"))
    await asyncio.sleep(3)
    ex = step("исполнения по REST", lambda: r.executions(SYM, int((time.time() - 300) * 1000)))
    print("    покупка в исполнениях REST:", any(e.get("orderLinkId") == f"smk{tag}E" for e in (ex or [])))
    pos = step("позиция", lambda: [p for p in r.positions() if p["symbol"] == SYM])
    print("    позиция:", [(p["symbol"], p["size"], p["avgPrice"]) for p in (pos or [])])
    step("цель: лимитная продажа только на закрытие", lambda: r.place(symbol=SYM, side="Sell", orderType="Limit", qty="0.1", price=f"{px * 1.2:.2f}",
                                                                        timeInForce="GTC", reduceOnly=True, orderLinkId=f"smk{tag}T"))
    step("перенос цели (цена и количество)", lambda: r.amend(symbol=SYM, orderLinkId=f"smk{tag}T", qty="0.1", price=f"{px * 1.21:.2f}"))
    step("снятие всех заявок по монете", lambda: r.cancel_all(SYM))
    step("выход по рынку только на закрытие", lambda: r.place(symbol=SYM, side="Sell", orderType="Market", qty="0.1", reduceOnly=True,
                                                               orderLinkId=f"smk{tag}X"))
    await asyncio.sleep(3)
    pos = step("позиция после выхода", lambda: [p for p in r.positions() if p["symbol"] == SYM and float(p["size"]) > 0])
    print("    позиция закрыта:", not pos)
    print("  поток исполнений получил:", seen)
    ws.stop(); task.cancel()
    bad = [n for n, s in ok if s != "ок"]
    print("ИТОГ:", "все вызовы работают" if not bad else f"проблемы: {bad}")


if __name__ == "__main__":
    asyncio.run(main())
