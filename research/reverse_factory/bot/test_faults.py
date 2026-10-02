"""Аварийные случаи исполнителя (по итогам независимой проверки 27.09):
1) рыночный вход исполнился частично → через 15 с работаем с исполненным (лесенка и цель на факт);
2) цель исполнилась раньше, чем её передвинули после докупки → остаток получает новую цель;
3) «стоп»: заявка на закрытие не ушла → повтор, пока позиция не закрыта; кампания закрывается по исполнению;
4) сверка: позиция без кампании закрывается, кампания без позиции снимается (после двух сверок подряд);
5) таймер считается от времени исполнения на бирже, а не от времени, когда бот узнал о нём;
6) чужая позиция; 7) своя позиция после перезапуска без файла состояния — узнаётся по меткам заявок.
Запуск: .venv/bin/python bot/test_faults.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import live  # noqa: E402
from core import ge  # noqa: E402


class FakeEx:
    def __init__(self):
        self.calls = []; self.fail_next = 0

    def _rec(self, name, kw):
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError("сеть недоступна")
        self.calls.append((name, kw))
        return {}

    def place(self, **kw): return self._rec("place", kw)
    def amend(self, **kw): return self._rec("amend", kw)
    def cancel(self, symbol, orderLinkId): return self._rec("cancel", dict(symbol=symbol, orderLinkId=orderLinkId))
    def cancel_all(self, symbol=None): return self._rec("cancel_all", dict(symbol=symbol))


SPEC = live.Spec("DOGEUSDT", qty_step=1.0, min_qty=1.0, tick=0.00001, min_notional=5.0)
T0 = pd.Timestamp("2026-09-28 10:00:00")
ok = True


def check(name, cond):
    global ok
    ok &= bool(cond)
    print(("  ✓ " if cond else "  ✗ ") + name)


def make_executor(clock):
    J = []
    fx = FakeEx()
    L = live.LiveExec(fx, {"DOGEUSDT": SPEC}, ge.P(), J.append,
                      max_camps=5, capital=1500, sizing="slot")
    L.clock = lambda: clock[0]
    return L, fx, J


def fill(L, link, qty, px, t, fee=0.0):
    L.on_execution(dict(symbol="DOGEUSDT", orderLinkId=link, execQty=str(qty), execPrice=str(px), execFee=str(fee),
                        execTime=str(int(t.timestamp() * 1000))), t + pd.Timedelta(minutes=30))


def link_of(camp, kind):
    return [k for k, o in camp.orders.items() if o.kind == kind][-1]


print("1) частичный вход")
clock = [T0]; L, fx, J = make_executor(clock)
L.on_signal("DOGEUSDT", T0, 0.1, 1.0)
camp = L.camps["DOGEUSDT"]; e = link_of(camp, "вход"); q = camp.orders[e].qty
fill(L, e, q // 2, 0.1, T0 + pd.Timedelta(seconds=1))
check("после половины исполнения кампания ещё ждёт", camp.state == "вход")
clock[0] = T0 + pd.Timedelta(seconds=20); L.check_pending()
check("через 15 с вход принят по факту и стоят лесенка и цель", camp.state == "в позиции" and any(o.kind == "цель" for o in camp.orders.values())
      and camp.orders[camp.tp_link].qty == q // 2)

print("2) цель исполнилась раньше переноса после докупки")
tp_old = camp.tp_link; tp_qty = camp.orders[tp_old].qty
fill(L, tp_old, tp_qty, 0.1019, T0 + pd.Timedelta(minutes=5))          # цель по старому объёму, докупок не было
check("кампания закрыта, раз позиция пуста", "DOGEUSDT" not in L.camps)
clock[0] = T0 + pd.Timedelta(hours=1)
L.on_signal("DOGEUSDT", T0 + pd.Timedelta(hours=1), 0.1, 1.0)
camp = L.camps["DOGEUSDT"]; e = link_of(camp, "вход"); fill(L, e, camp.orders[e].qty, 0.1, T0 + pd.Timedelta(hours=1, seconds=1))
tp_old = camp.tp_link; tp_qty = camp.orders[tp_old].qty
fx.fail_next = 1                                                          # перенос цели после докупки сорвётся (цель уже исполнена)
st1 = link_of(camp, "ступень 1"); fill(L, st1, camp.orders[st1].qty, 0.0986, T0 + pd.Timedelta(hours=1, minutes=3))
fill(L, tp_old, tp_qty, 0.1019, T0 + pd.Timedelta(hours=1, minutes=3, seconds=1))
check("после выхода остался объём докупки — кампания жива, на остаток стоит новая цель",
      "DOGEUSDT" in L.camps and camp.orders[camp.tp_link].active and abs(camp.orders[camp.tp_link].qty - camp.qty) < 1)
check("в журнале есть «внимание: остаток»", any(j.get("kind") == "внимание" and "остаток" in str(j.get("what")) for j in J))

print("3) стоп с сорвавшейся заявкой")
fx.fail_next = 2                                                          # снятие и закрытие не проходят
L.halt(T0 + pd.Timedelta(hours=2))
check("после неудачи кампания в состоянии «выход»", camp.state == "выход")
clock[0] = T0 + pd.Timedelta(hours=2, seconds=30); L.check_pending()
stop = link_of(camp, "стоп")
check("повторная заявка на закрытие ушла", camp.orders[stop].active and any(c[0] == "place" and c[1].get("reduceOnly") for c in fx.calls[-2:]))
fill(L, stop, camp.orders[stop].qty, 0.098, T0 + pd.Timedelta(hours=2, seconds=31))
check("после исполнения стопа кампания закрыта", "DOGEUSDT" not in L.camps and L.done[-1].state == "закрыта")

print("4) сверка с позициями биржи")
clock = [T0]; L, fx, J = make_executor(clock)
L.reconcile([dict(symbol="DOGEUSDT", side="Buy", size="120")])
check("первая сверка: позицию без кампании ещё не трогаем", not any(c[0] == "place" for c in fx.calls))
L.reconcile([dict(symbol="DOGEUSDT", side="Buy", size="120")])
check("вторая сверка подряд: закрываем по рынку", any(c[0] == "place" and c[1].get("reduceOnly") and c[1]["qty"] == "120" for c in fx.calls))
L.on_signal("DOGEUSDT", T0, 0.1, 1.0); camp = L.camps["DOGEUSDT"]; e = link_of(camp, "вход")
fill(L, e, camp.orders[e].qty, 0.1, T0 + pd.Timedelta(seconds=1))
L.reconcile([]); L.reconcile([])
check("кампания без позиции на бирже снята после двух сверок", "DOGEUSDT" not in L.camps)
check("итог такой кампании — неизвестен, а не минус вся покупка", L.done[-1].result_usd is None)

print("5) таймер от времени исполнения на бирже")
clock = [T0]; L, fx, J = make_executor(clock)
L.on_signal("DOGEUSDT", T0, 0.1, 1.0); camp = L.camps["DOGEUSDT"]; e = link_of(camp, "вход")
fill(L, e, camp.orders[e].qty, 0.1, T0 + pd.Timedelta(seconds=2))       # бот узнал через 30 мин (сверка)
check("последняя покупка записана временем биржи", camp.last_fill == T0 + pd.Timedelta(seconds=2))

print("6) чужая позиция на счёте")
clock = [T0]; L, fx, J = make_executor(clock)
L.foreign = {"DOGEUSDT"}
L.reconcile([dict(symbol="DOGEUSDT", side="Buy", size="50")]); L.reconcile([dict(symbol="DOGEUSDT", side="Buy", size="50")])
check("чужую позицию сверка не закрывает", not any(c[0] == "place" for c in fx.calls))
check("в монету с чужой позицией бот не входит", L.on_signal("DOGEUSDT", T0, 0.1, 1.0) is False)
L.reconcile([])
check("чужую закрыли — монета снова свободна", "DOGEUSDT" not in L.foreign and L.on_signal("DOGEUSDT", T0, 0.1, 1.0) is not False)

print("7) своя позиция после перезапуска без файла состояния")
clock = [T0]; L, fx, J = make_executor(clock)
L.on_signal("DOGEUSDT", T0, 0.1, 1.0); camp = L.camps["DOGEUSDT"]; e = link_of(camp, "вход")
t1 = T0 + pd.Timedelta(seconds=1); fill(L, e, camp.orders[e].qty, 0.1, t1, fee=0.01)
st1 = next(o for o in camp.orders.values() if o.kind == "ступень 1")
st2 = next(o for o in camp.orders.values() if o.kind == "ступень 2")
t2 = T0 + pd.Timedelta(hours=2); fill(L, st1.link, st1.qty, st1.price, t2)
t3 = T0 + pd.Timedelta(hours=3); fill(L, st2.link, 5, st2.price, t3)              # ступень 2 исполнилась частично
old = dict(qty=camp.qty, fills=list(camp.fills), target=camp.target(L.p), last=camp.last_fill)
ms = lambda t: str(int(t.timestamp() * 1000))  # noqa: E731
execs = [dict(symbol="DOGEUSDT", orderLinkId="kz1234560001E", side="Buy", execQty="1", execPrice="0.2", execTime=ms(T0 - pd.Timedelta(days=2)), execId="old1"),
         dict(symbol="DOGEUSDT", orderLinkId="kz1234560002T", side="Sell", execQty="1", execPrice="0.21", execTime=ms(T0 - pd.Timedelta(days=1)), execId="old2"),
         dict(symbol="DOGEUSDT", orderLinkId=e, side="Buy", execQty=str(camp.orders[e].qty), execPrice="0.1", execFee="0.01", execTime=ms(t1), execId="a1"),
         dict(symbol="DOGEUSDT", orderLinkId=st1.link, side="Buy", execQty=str(st1.qty), execPrice=str(st1.price), execTime=ms(t2), execId="a2"),
         dict(symbol="DOGEUSDT", orderLinkId=st2.link, side="Buy", execQty="5", execPrice=str(st2.price), execTime=ms(t3), execId="a3"),
         dict(symbol="DOGEUSDT", orderLinkId="руками", side="Buy", execQty="7", execPrice="0.09", execTime=ms(t3), execId="m1")]
opens = [dict(symbol="DOGEUSDT", orderLinkId=o.link, side=o.side, price=str(o.price), qty=str(o.qty), createdTime=ms(o.placed))
         for o in camp.orders.values() if o.active]
size = old["qty"] + 5                                                           # на бирже и частичная ступень 2
clock = [T0 + pd.Timedelta(hours=4)]; L2, fx2, J2 = make_executor(clock)       # перезапуск: памяти нет
check("по меткам позиция признана своей", L2.adopt("DOGEUSDT", size, execs, opens) is True)
c2 = L2.camps["DOGEUSDT"]
check("покупки восстановлены (вход + ступень 1), частичная ступень 2 ждёт остатка", c2.fills == old["fills"] and c2.qty == old["qty"])
check("цель та же, заявки не переставлялись", abs(c2.target(L2.p) - old["target"]) < 1e-12 and not any(c[0] in ("place", "amend") for c in fx2.calls))
check("таймер от последней покупки на бирже", c2.last_fill == old["last"])
check("исполнения помечены учтёнными — сверка не задвоит", {"a1", "a2", "a3"} <= set(L2.seen) and "m1" not in L2.seen)
fill(L2, st2.link, st2.qty - 5, st2.price, T0 + pd.Timedelta(hours=5))
check("остаток ступени 2 дошёл — докупка засчитана, цель передвинута", any(f[0] == 2 for f in c2.fills) and any(c[0] == "amend" for c in fx2.calls))
clock = [T0]; L3, fx3, J3 = make_executor(clock)
check("размер не сходится с метками — позиция чужая", L3.adopt("DOGEUSDT", size + 100, execs, opens) is False and "DOGEUSDT" not in L3.camps)
check("позиция без меток бота — чужая", L3.adopt("DOGEUSDT", 7, [execs[-1]], []) is False)
c_old = execs[:2]
check("прошлая закрытая кампания не путается с позицией", L3.adopt("DOGEUSDT", 1, c_old, []) is False)

print("ИТОГ:", "все аварийные случаи обработаны" if ok else "ЕСТЬ ОШИБКИ")
