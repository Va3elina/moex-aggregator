"""Раз в день: календарь торгов МосБиржи из ISS → moex_trading_calendar; изменилась дата — сообщение в бот.

    15 4 * * *  /bin/bash /opt/frame/signals/moex_calendar_sync.sh >> /opt/frame/logs/moex_calendar_sync.log 2>&1

Источник — apim.moex.com/iss/calendars.json (off_days, только с токеном AlgoPack ALGOPACK_API_KEY; бесплатный
iss.moex.com календарь не отдаёт). 2 запроса в день: текущий и следующий год. Год перезаписывается целиком.
Читатель — moex_calendar.py (праздники для всех загрузчиков); при сбое здесь он берёт запасной блок в коде.

Сообщение в @framesignalbot:
  - биржа поменяла нерабочие будни (добавила/сняла праздник) по акциям или фьючерсам;
  - даты в базе разошлись с запасным блоком в коде (moex_calendar.ISS_*_CLOSED) — блок пора обновить
    scripts/moex_calendar_refresh.py (один раз на каждое новое расхождение);
  - календарь не скачался 2 дня подряд.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")
os.environ["DB_URL"] = os.environ.get("DB_URL", "").replace("@db:", "@127.0.0.1:")

from sqlalchemy import create_engine, text  # noqa: E402

import moex_calendar as MC  # noqa: E402
from moex_tls import moex_ssl_context  # noqa: E402

URL = "https://apim.moex.com/iss/calendars.json?iss.meta=off&iss.only=off_days&from={y}-01-01&till={y}-12-31"
COLS = ["stock_workday", "futures_workday", "currency_workday", "stock_reason", "futures_reason",
        "currency_reason", "stock_trade_session_date", "futures_trade_session_date", "currency_trade_session_date"]
STATE = ROOT / "logs" / "moex_calendar_sync.state.json"


def log(msg: str) -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def notify(text_: str) -> None:
    for d in (Path("/opt/robot-notify"), ROOT / "research" / "robots_tg"):
        if (d / "robots_tg.py").exists():
            sys.path.insert(0, str(d))
            break
    import robots_tg
    robots_tg.send(text_)


def fetch(year: int) -> list[dict]:
    req = urllib.request.Request(URL.format(y=year), headers={"Authorization": f"Bearer {os.environ['ALGOPACK_API_KEY']}"})
    with urllib.request.urlopen(req, timeout=40, context=moex_ssl_context()) as r:
        b = json.load(r)["off_days"]
    return [dict(zip(b["columns"], row)) for row in b["data"]]


def closed_weekdays(rows, market: str) -> set[str]:
    return {r["tradedate"] for r in rows
            if r.get(f"{market}_workday") in (0, "0") and date.fromisoformat(str(r["tradedate"])[:10]).weekday() < 5}


def _d(v):
    s = str(v or "")[:10]
    return None if not s or s.startswith("0000") else s


def main() -> int:
    eng = create_engine(os.environ["DB_URL"])
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    today = date.today()
    years = [today.year, today.year + 1]
    messages: list[str] = []
    try:
        fetched = {}
        for y in years:
            fetched[y] = fetch(y)
            time.sleep(1)
    except Exception as e:  # noqa: BLE001
        fails = state.get("fails", 0) + 1
        state["fails"] = fails
        STATE.write_text(json.dumps(state))
        log(f"календарь не скачался ({fails} дн. подряд): {type(e).__name__}: {e}")
        if fails == 2:
            notify(f"⚠️ Календарь МосБиржи не обновляется 2 дня: {type(e).__name__}. Загрузчики работают по "
                   f"запасному блоку в коде.")
        return 1
    state["fails"] = 0

    with eng.begin() as conn:
        for y, rows in fetched.items():
            if not rows:
                log(f"{y}: ISS вернул пусто — год не трогаем")
                continue
            old = [dict(r._mapping) for r in conn.execute(text(
                "SELECT tradedate::text AS tradedate, stock_workday, futures_workday FROM moex_trading_calendar "
                "WHERE extract(year FROM tradedate) = :y"), {"y": y})]
            changes = {}
            for m in ("stock", "futures"):
                before, after = closed_weekdays(old, m), closed_weekdays(rows, m)
                if old and before != after:
                    changes[m] = (sorted(after - before), sorted(before - after))
            if changes:
                same = len(changes) == 2 and changes["stock"] == changes["futures"]
                for m, (add, rem) in ([("both", changes["stock"])] if same else list(changes.items())):
                    label = {"both": "акции и фьючерсы", "stock": "акции", "futures": "фьючерсы"}[m]
                    messages.append(f"📅 МосБиржа поменяла календарь {y} ({label}): "
                                    + (f"новые нерабочие дни {', '.join(add)}" if add else "")
                                    + ("; " if add and rem else "")
                                    + (f"теперь торговые {', '.join(rem)}" if rem else ""))
            conn.execute(text("DELETE FROM moex_trading_calendar WHERE extract(year FROM tradedate) = :y"), {"y": y})
            conn.execute(text(f"""
                INSERT INTO moex_trading_calendar (tradedate, {', '.join(COLS)}, raw, updated_at)
                VALUES (:tradedate, {', '.join(':' + c for c in COLS)}, CAST(:raw AS jsonb), now())
            """), [{"tradedate": r["tradedate"], **{c: (_d(r.get(c)) if c.endswith("_date") else r.get(c)) for c in COLS},
                    "raw": json.dumps(r, ensure_ascii=False)} for r in rows])
            log(f"{y}: {len(rows)} особых дней, нерабочих будней: акции {len(closed_weekdays(rows, 'stock'))}, "
                f"фьючерсы {len(closed_weekdays(rows, 'futures'))}")

    # Запасной блок в коде отстал от биржи — напомнить раз в сутки (блок нужен, когда база недоступна)
    stale = []
    for y, rows in fetched.items():
        if rows and y in {int(x) for x in MC.ISS_CALENDAR_YEARS}:
            for m, block in (("stock", MC.ISS_STOCK_CLOSED), ("futures", MC.ISS_FUTURES_CLOSED)):
                code = {d.isoformat() for d in block if d.year == y and d.weekday() < 5}
                if code != closed_weekdays(rows, m):
                    stale.append(f"{y} {m}")
        elif rows:
            stale.append(f"{y} (нет в блоке)")
    sig = ", ".join(stale)
    if stale and state.get("stale_sig") != sig:        # один раз на каждое новое расхождение, не каждый день
        messages.append("ℹ️ Запасной блок календаря в moex_calendar.py отстал от МосБиржи: " + sig
                        + ". Загрузчики берут даты из базы; блок обновить scripts/moex_calendar_refresh.py.")
    state["stale_sig"] = sig
    STATE.write_text(json.dumps(state))

    for m in messages:
        log(m)
        notify(m)
    log(f"готово, сообщений: {len(messages)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
