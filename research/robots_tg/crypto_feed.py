"""Сообщения крипто-робота Kamaz в Telegram — по его журналу (запуск от root по cron раз в минуту).

Бот торговли сам в Telegram не пишет: у пользователя kamaz нет доступа к ключам сайта. Этот скрипт читает новые строки
журнала /opt/kamaz-bot/bot/state/journal/*.jsonl и шлёт:
  вход     — отдельное сообщение: что купили, цена, цель, лесенка;
  докупка  — ПРАВИТ сообщение о входе (без нового звонка): сколько докупок, средняя, новая цель;
  выход    — ответ на сообщение о входе: итог в $, причина закрытия (цель / таймер / стоп), итог с начала;
  ошибки   — ⚠️ любые (и торговли, и самого бота) не чаще раза в 10 минут на одну и ту же;
  внимание — ⚠️ без звука: сверка с биржей что-то поправила (частичный вход, остаток, позиция без кампании).
Состояние — state/crypto_feed.json (сколько строк каждого файла уже прочитано, открытые сделки, итог с начала).
"""
from __future__ import annotations

import datetime as dt
import json
import pathlib
import re
import sys
from zoneinfo import ZoneInfo

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import robots_tg as TG  # noqa: E402

JOURNAL = pathlib.Path("/opt/kamaz-bot/bot/state/journal")
STATUS = pathlib.Path("/opt/kamaz-bot/bot/state/status.txt")
STATE = HERE / "state" / "crypto_feed.json"
MSK = ZoneInfo("Europe/Moscow")
NAME = "Крипта"


def msk(t: str | None, fmt: str = "%d.%m %H:%M") -> str:
    if not t or t in ("None", "NaT"):
        return ""
    d = dt.datetime.fromisoformat(str(t)[:19]).replace(tzinfo=dt.timezone.utc)
    return d.astimezone(MSK).strftime(fmt)


def minutes(a: str, b: str) -> int:
    return int((dt.datetime.fromisoformat(str(b)[:19]) - dt.datetime.fromisoformat(str(a)[:19])).total_seconds() // 60)


def dur(m: int) -> str:
    return f"{m} мин" if m < 90 else f"{m / 60:.1f} ч".replace(".", ",")


def status_cfg() -> tuple[float | None, float | None]:
    """Бюджет и ячейка из первой строки статуса бота («бюджет $1500: 5 ячеек по $300»)."""
    try:
        head = STATUS.read_text().splitlines()[0]
        b = re.search(r"бюджет \$(\d+)", head); c = re.search(r"ячеек по \$(\d+)", head)
        return (float(b.group(1)) if b else None, float(c.group(1)) if c else None)
    except Exception:
        return None, None


def budget() -> str:
    b, _ = status_cfg()
    return f"бюджет {TG.usd(b, sign=False, nd=0)}" if b else ""


def coin(sym: str) -> str:
    return sym.replace("USDT", "")


def cost(c: dict) -> float:
    """Сколько денег в позиции: сумма всех исполненных покупок кампании."""
    return sum(p * q for _, p, q in c.get("fills") or [])


def pct_of(x: float, base: float | None) -> str:
    return f" {TG.pct(100 * x / base)}" if base else ""


def total_line(st: dict) -> str:
    """всего +$0.60 +0,04% от $1 500 — итог с начала и доля от бюджета."""
    b, _ = status_cfg()
    return f"всего {TG.usd(st['total'])}{pct_of(st['total'], b)}" + (f" от {TG.usd(b, sign=False, nd=0)}" if b else "")


# ---------- тексты (вариант A: только цифры)
def text_entry(c: dict) -> str:
    fills = c["fills"]; p0 = fills[0][1]
    L = [f"{TG.icon('вход')} <b>{NAME} {coin(c['sym'])}</b> вход {TG.price(p0)} на {TG.usd(fills[0][1] * fills[0][2], sign=False)}"]
    adds = fills[1:]
    if adds:                                   # значок лесенки: сколько ступеней из шести исполнено
        qty = sum(q for _, _, q in fills); cost = sum(p * q for _, p, q in fills)
        L.append(f"{TG.icon(f'лесенка {min(len(adds), 6)}')} средняя {TG.price_like(cost / qty, p0)} на {TG.usd(cost, sign=False)}")
    if c.get("tp"):
        L.append(f"{TG.icon('цель')} цель {TG.price_like(c['tp'], p0)}")
    return "\n".join(L)


def text_exit(c: dict, how: str, result: float, st: dict) -> str:
    px = c.get("exit_px")
    mark = "" if how == "цель" else f" {TG.icon('таймер')}" if how == "таймер" else f" {TG.icon('стоп')}"
    if result is None:                         # позицию закрыли мимо бота — цены продажи не знаем
        return f"{TG.icon('нет сделок')} <b>{NAME} {coin(c['sym'])}</b> позиция закрыта не ботом, итог неизвестен\n<i>{total_line(st)}</i>"
    L = [f"{TG.dot(result)} <b>{NAME} {coin(c['sym'])}</b> выход {TG.price_like(px, c['fills'][0][1]) if px else '—'}{mark} <b>{TG.usd(result)}{pct_of(result, cost(c))}</b>"]
    t = f"за {dur(minutes(c['t_entry'], c['t_exit']))}, " if c.get("t_exit") and c.get("t_entry") else ""
    L.append(f"<i>{t}{total_line(st)}</i>")
    return "\n".join(L)


# ---------- разбор журнала
def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return dict(offsets={}, signals={}, camps={}, total=0.0, n=0, start=None, errors={})


def save_state(st: dict):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp"); tmp.write_text(json.dumps(st, ensure_ascii=False)); tmp.replace(STATE)


def alert(ev: dict, st: dict, send, silent: bool = False):
    """⚠️ сбой или поправка — не чаще раза в 10 минут на одно и то же."""
    k = ev.get("kind"); sym = ev.get("sym")
    key = f"{sym}|{ev.get('what')}"
    last = st["errors"].get(key)
    now = ev.get("at") or ""
    if k == "стоп" or not last or minutes(last, now) >= 10:
        st["errors"][key] = now
        if k == "стоп":
            what = "стоп: заявки сняты, позиции закрыты по рынку"
        else:
            what = str(ev.get("what")) + (f": {ev.get('err')}" if ev.get("err") else "")
        head = "сбой" if k in ("ошибка", "стоп") else "внимание"
        send(f"{TG.icon(head)} <b>{NAME}</b> {head}\n{TG.esc((coin(sym) + ': ') if sym else '')}{TG.esc(what)[:400]}", silent=silent)


def handle(ev: dict, st: dict, send=TG.send, edit=TG.edit):  # send(text, reply_to=None, silent=False)
    k = ev.get("kind"); sym = ev.get("sym")
    if k in ("ошибка", "стоп"):                   # сбои и самого бота (без src), и торговли
        alert(ev, st, send)
        return
    if k == "сигнал":
        st["signals"][f"{sym}|{ev.get('t')}"] = {x: ev.get(x) for x in ("r3", "from_hi", "ch120", "price")}
        if len(st["signals"]) > 500:
            for key in list(st["signals"])[:-300]:
                del st["signals"][key]
        return
    if ev.get("src") != "реал":
        return
    c = st["camps"].get(sym)
    if k == "заявка" and ev.get("what") == "вход по рынку":
        st["camps"][sym] = dict(sym=sym, t_sig=ev.get("t"), fills=[], part={}, sig=st["signals"].get(f"{sym}|{ev.get('t')}"), slot=status_cfg()[1])
        st["start"] = st["start"] or msk(ev.get("at"), "%d.%m")
    elif k == "исполнение" and c is not None:
        what = str(ev.get("what"))
        q_sum, cost = c.setdefault("part", {}).get(what, [0.0, 0.0])      # заявка может исполниться частями
        q_sum += float(ev["qty"]); cost += float(ev["qty"]) * float(ev["price"])
        c["part"][what] = [q_sum, cost]
        if not ev.get("full"):
            return
        px = cost / q_sum; del c["part"][what]
        if what == "вход":
            c["fills"].append([0, px, q_sum]); c["t_entry"] = ev.get("t") or ev.get("at")
        elif what.startswith("ступень"):
            c["fills"].append([int(what.split()[1]), px, q_sum])
        elif what in ("цель", "таймер", "стоп"):     # выход бывает в несколько заявок (остаток) — средняя по всем
            c["exit_q"] = c.get("exit_q", 0.0) + q_sum; c["exit_cost"] = c.get("exit_cost", 0.0) + cost
            c["exit_px"] = c["exit_cost"] / c["exit_q"]; c["t_exit"] = ev.get("t") or ev.get("at")
    elif k == "лесенка" and c is not None:
        c["levels"] = ev.get("levels") or []
    elif k in ("цель", "цель перенесена") and c is not None and c.get("fills"):
        c["tp"] = float(ev["price"])
        if not c.get("msg_id"):
            c["msg_id"] = send(text_entry(c), silent=True)            # вход — без звука, выход — со звуком
        else:
            edit(c["msg_id"], text_entry(c))
    elif k == "внимание":
        alert(ev, st, send, silent=True)
    elif k == "кампания закрыта" and c is not None:
        res = None if ev.get("result") is None else float(ev["result"])
        st["total"] = float(st.get("total", 0.0)) + (res or 0.0); st["n"] = int(st.get("n", 0)) + 1
        st["start"] = st["start"] or msk(ev.get("at"), "%d.%m")
        c["t_exit"] = c.get("t_exit") or ev.get("t") or ev.get("at")
        send(text_exit(c, str(ev.get("how")), res, st), reply_to=c.get("msg_id"))
        del st["camps"][sym]


def run():
    st = load_state()
    files = sorted(JOURNAL.glob("*.jsonl"))
    if not files:
        return
    if not st["offsets"]:                      # первый запуск — начинаем с текущего конца, старое не рассылаем
        st["offsets"] = {f.name: sum(1 for _ in f.open()) for f in files}
        st["start"] = st["start"] or dt.datetime.now(MSK).strftime("%d.%m")
        save_state(st); return
    for f in files:
        lines = f.read_text().splitlines()
        done = st["offsets"].get(f.name, 0)
        for line in lines[done:]:
            try:
                handle(json.loads(line), st)
            except Exception as e:
                print(f"строка журнала не разобрана: {e}: {line[:200]}", flush=True)
        st["offsets"][f.name] = len(lines)
    save_state(st)


if __name__ == "__main__":
    run()
