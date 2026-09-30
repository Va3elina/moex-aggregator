"""Исполнитель настоящих заявок для проверки исполнения ($50 на отдельном субаккаунте).

Те же сигналы, что у бумажной книги, но каждая ступень лесенки — минимальная заявка (≈ $5.5), не больше
MAX_CAMPS монет одновременно, только монеты с минимальной заявкой ≤ $5.5. Цель выхода считается по правилу стратегии
от РЕАЛЬНЫХ исполнений с весами лесенки из движка стратегии, чтобы цель стояла ровно там, где у стратегии.
Каждая заявка журналируется: поставлена, коснулась ли цена (и на сколько прошла сквозь), исполнилась ли.

Биржа за интерфейсом: place/amend/cancel/cancel_all (как bybit.Rest) — в проверке её подменяет sim_exchange.
"""
from __future__ import annotations

import math
import re
import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from core import ge

PIECE_USD = 5.5          # режим проверки исполнения: минимальная заявка на каждую ступень
MAX_CAMPS = 2
CAPITAL = 100.0          # режим «равные покупки»: счёт делится на MAX_CAMPS ячеек, ячейка — на 7 равных покупок


# Метка заявки бота: kz<время><номер><вид>, вид — E вход, L<j> ступень j, T цель, X таймер, S стоп/остаток, O закрытие по сверке.
LINK_RE = re.compile(r"^kz\d+([A-Z]\d*)$")
LINK_KIND = {"E": "вход", "T": "цель", "X": "таймер", "S": "стоп", "O": "сверка"}


def link_kind(link: str | None) -> str | None:
    """Вид заявки по метке бота; None — заявка не бота."""
    m = LINK_RE.match(link or "")
    if not m:
        return None
    k = m.group(1)
    return f"ступень {k[1:]}" if k[0] == "L" and k[1:] else LINK_KIND.get(k)


def floor_step(x: float, step: float) -> float:
    return math.floor(x / step + 1e-9) * step


def ceil_step(x: float, step: float) -> float:
    return math.ceil(x / step - 1e-9) * step


def fmt(x: float, step: float) -> str:
    d = max(0, -int(math.floor(math.log10(step)))) if step < 1 else 0
    return f"{x:.{d}f}"


@dataclass
class Spec:
    sym: str
    qty_step: float
    min_qty: float
    tick: float
    min_notional: float

    @classmethod
    def from_instrument(cls, i: dict) -> "Spec":
        f = i["lotSizeFilter"]; p = i["priceFilter"]
        return cls(i["symbol"], float(f["qtyStep"]), float(f["minOrderQty"]), float(p["tickSize"]), float(f.get("minNotionalValue") or 5))

    def qty_for(self, usd: float, price: float) -> float:
        q = max(ceil_step(usd / price, self.qty_step), self.min_qty)
        while q * price < self.min_notional:
            q += self.qty_step
        return float(fmt(q, self.qty_step))                 # ровно то число, что уйдёт на биржу

    def min_order_usd(self, price: float) -> float:
        return self.qty_for(self.min_notional, price) * price


@dataclass
class LiveOrder:
    link: str
    kind: str            # вход / ступень N / цель / таймер
    side: str
    price: float | None
    qty: float
    placed: pd.Timestamp
    filled_qty: float = 0.0
    fill_px: float = 0.0
    filled_at: pd.Timestamp | None = None
    touched_at: pd.Timestamp | None = None
    max_through: float = 0.0          # насколько цена прошла сквозь уровень, % (для лимиток)
    active: bool = True
    fee: float = 0.0                  # комиссия биржи по исполнениям, $
    sent_at: pd.Timestamp | None = None   # когда ушла на биржу (настенное время) — для «зависших» рыночных
    attempts: int = 0


@dataclass
class LiveCamp:
    sym: str
    t_sig: pd.Timestamp
    scale: float
    orders: dict = field(default_factory=dict)     # link -> LiveOrder
    fills: list = field(default_factory=list)      # (step, price, qty) — step 0 = вход
    last_fill: pd.Timestamp | None = None
    state: str = "вход"                             # вход → в позиции → выход → закрыта
    tp_link: str | None = None
    closed_at: pd.Timestamp | None = None
    result_usd: float | None = 0.0                   # None — позицию закрыли мимо бота, итог неизвестен
    q_unit: float = 0.0                              # равные покупки / ячейка: базовое число монет (ступень k = q_unit × объём k)
    equal: bool = False                              # цель от настоящей средней цены (режимы «равные» и «ячейка»)
    slot: bool = False                               # режим «ячейка»: лесенка с весами стратегии от суммы ячейки
    fees: float = 0.0                                # комиссии по кампании, $

    @property
    def qty(self) -> float:
        return sum(q for _, _, q in self.fills) - sum(o.filled_qty for o in self.orders.values() if o.side == "Sell")

    def target(self, p: ge.P) -> float:
        steps = sorted({s for s, _, _ in self.fills})
        p0 = next(px for s, px, _ in self.fills if s == 0)
        if steps == [0]:
            return p0 * (1 + p.tp1 / 100)
        if self.equal:                                  # цель от настоящей средней цены позиции
            return sum(px * q for _, px, q in self.fills) / sum(q for _, _, q in self.fills) * (1 + p.tpn / 100)
        w = {s: ge.SIZES[s] for s in steps}
        avg_px = {s: np.average([px for s2, px, _ in self.fills if s2 == s]) for s in steps}
        avg_w = sum(w[s] * avg_px[s] for s in steps) / sum(w.values())
        return avg_w * (1 + p.tpn / 100)


class LiveExec:
    def __init__(self, ex, specs: dict[str, Spec], p: ge.P, journal, max_camps: int = MAX_CAMPS, piece_usd: float | None = None,
                 capital: float = CAPITAL, equal: bool = False, sizing: str | None = None):
        """sizing: min — минимальные заявки по $5.5 (проверка исполнения); equal — 7 равных покупок; slot — настоящая лесенка
        на ячейку = capital / max_camps (первая покупка = ячейка / 32.33)."""
        self.ex = ex; self.specs = specs; self.p = p; self.journal = journal
        self.max_camps = max_camps
        self.sizing = sizing or ("equal" if equal else "min")
        self.equal = self.sizing in ("equal", "slot")
        self.slot_usd = capital / max_camps
        if piece_usd is not None:
            self.piece = piece_usd
        elif self.sizing == "slot":
            self.piece = self.slot_usd / float(ge.SIZES.sum())      # первая покупка
        elif self.sizing == "equal":
            self.piece = self.slot_usd / 7
        else:
            self.piece = PIECE_USD
        self.camps: dict[str, LiveCamp] = {}
        self.done: list[LiveCamp] = []
        self.halted = False
        self._n = 0
        self.clock = lambda: pd.Timestamp.now(tz="UTC").tz_localize(None)   # в проверке на имитации — время минутки
        self.seen: dict[str, None] = {}         # номера исполнений, уже учтённых (поток и сверка по REST приносят одни и те же); порядок = порядок прихода
        self.orphans: dict[str, int] = {}       # позиция на бирже без кампании: сколько сверок подряд
        self.gone: dict[str, int] = {}          # кампания есть, а позиции на бирже нет: сколько сверок подряд
        self.mismatch: dict[str, int] = {}
        self.foreign: set[str] = set()          # позиции, открытые на счёте не ботом (были до запуска): не трогаем и в эти монеты не входим

    # --- служебное
    def _link(self, sym: str, kind: str) -> str:
        self._n += 1
        return f"kz{int(time.time()) % 10_000_000}{self._n:04d}{kind}"[:36]

    def eligible(self, sym: str, price: float) -> bool:
        s = self.specs.get(sym)
        return s is not None and s.min_order_usd(price) <= self.piece

    def _log(self, kind: str, **kw):
        self.journal(dict(src="реал", kind=kind, **kw))

    # --- сигнал
    def on_signal(self, sym: str, t: pd.Timestamp, price: float, scale: float) -> bool:
        if self.halted or sym in self.camps or sym in self.foreign or len(self.camps) >= self.max_camps or not self.eligible(sym, price):
            return False
        s = self.specs[sym]
        q = s.qty_for(self.piece, price)
        camp = LiveCamp(sym, t, scale, q_unit=q, equal=self.equal, slot=self.sizing == "slot")
        link = self._link(sym, "E")
        camp.orders[link] = LiveOrder(link, "вход", "Buy", None, q, t, sent_at=self.clock())
        self.camps[sym] = camp
        try:
            self.ex.place(symbol=sym, side="Buy", orderType="Market", qty=fmt(q, s.qty_step), orderLinkId=link)
            self._log("заявка", sym=sym, what="вход по рынку", qty=q, t=t)
        except Exception as e:
            self._log("ошибка", sym=sym, what="вход", err=str(e), t=t)
            del self.camps[sym]
            return False
        return True

    # --- исполнения (из личного потока биржи или из имитации)
    def on_execution(self, e: dict, now: pd.Timestamp):
        eid = e.get("execId")
        if eid:
            if eid in self.seen:
                return
            self.seen[eid] = None
        sym = e["symbol"]; link = e.get("orderLinkId", "")
        et = str(e.get("execTime") or "")
        t_ex = pd.Timestamp(int(et), unit="ms") if et.isdigit() else now      # время исполнения — с биржи (таймер считается от него)
        camp = self.camps.get(sym)
        if camp is None or link not in camp.orders:
            self._log("внимание", sym=sym, what="исполнение без кампании", link=link, qty=e.get("execQty"), price=e.get("execPrice"), t=t_ex)
            return
        o = camp.orders[link]
        qty = float(e["execQty"]); px = float(e["execPrice"]); fee = float(e.get("execFee") or 0.0)
        o.fill_px = (o.fill_px * o.filled_qty + px * qty) / (o.filled_qty + qty); o.filled_qty += qty
        o.fee += fee; camp.fees += fee
        full = o.filled_qty >= o.qty * (1 - 1e-9) - 1e-12
        if full:
            o.filled_at = t_ex; o.active = False
        self._log("исполнение", sym=sym, what=o.kind, price=px, qty=qty, t=t_ex, full=full, link=link)
        if o.kind == "вход":
            if full and camp.state == "вход":
                self._entry_done(camp, o, t_ex)
        elif o.kind.startswith("ступень"):
            if full:
                camp.fills.append((int(o.kind.split()[1]), o.fill_px, o.filled_qty)); camp.last_fill = t_ex
                if camp.state == "в позиции":
                    self._place_or_move_target(camp, t_ex)
        elif o.kind in ("цель", "таймер", "стоп") and full:
            self._after_exit_fill(camp, o, t_ex)

    def _entry_done(self, camp: LiveCamp, o: LiveOrder, t: pd.Timestamp):
        camp.fills.append((0, o.fill_px, o.filled_qty)); camp.last_fill = t; camp.state = "в позиции"
        if camp.slot:                                   # как в движке: ступень k = (первая покупка / цена входа) × объём k
            camp.q_unit = self.piece / o.fill_px
        self._place_ladder(camp, t)
        self._place_or_move_target(camp, t)

    def _after_exit_fill(self, camp: LiveCamp, o: LiveOrder, t: pd.Timestamp):
        """Выход исполнился. Позиция может быть не пустой: докупка успела исполниться раньше, чем передвинули цель."""
        s = self.specs[camp.sym]
        rest = camp.qty
        if rest <= s.qty_step / 2:
            self._close(camp, t, o.kind)
            return
        self._log("внимание", sym=camp.sym, what=f"после выхода ({o.kind}) остаток", qty=rest, t=t)
        if o.kind == "цель" and camp.state == "в позиции":
            self._place_or_move_target(camp, t)          # цель на остаток
        else:
            self._send_market(camp, o.kind, rest, t)     # добиваем остаток по рынку

    def _send_market(self, camp: LiveCamp, kind: str, qty: float, t: pd.Timestamp):
        """Рыночная продажа только на закрытие (таймер, стоп, остаток). Не ушла — повторит check_pending."""
        s = self.specs[camp.sym]
        qty = float(fmt(round(qty / s.qty_step) * s.qty_step, s.qty_step))
        link = self._link(camp.sym, "X" if kind == "таймер" else "S")
        prev = [x for x in camp.orders.values() if x.kind == kind]
        camp.orders[link] = LiveOrder(link, kind, "Sell", None, qty, t, sent_at=self.clock(), attempts=len(prev) + 1)
        camp.state = "выход"
        try:
            self.ex.place(symbol=camp.sym, side="Sell", orderType="Market", qty=fmt(qty, s.qty_step), reduceOnly=True, orderLinkId=link)
            self._log("заявка", sym=camp.sym, what=f"выход по рынку ({kind})", qty=qty, t=t)
        except Exception as e:
            camp.orders[link].active = False
            self._log("ошибка", sym=camp.sym, what=f"выход по рынку ({kind})", err=str(e), t=t)

    def _place_ladder(self, camp: LiveCamp, now: pd.Timestamp):
        s = self.specs[camp.sym]; p0 = camp.fills[0][1]
        lv = self.p.levels * camp.scale / 100
        for j, x in enumerate(lv, start=1):
            price = float(fmt(floor_step(p0 * (1 - x), s.tick), s.tick))
            if camp.slot:
                q = max(round(camp.q_unit * float(ge.SIZES[j]) / s.qty_step) * s.qty_step, s.qty_for(s.min_notional, price))
                q = float(fmt(q, s.qty_step))
            elif camp.equal:
                q = max(camp.q_unit, s.qty_for(s.min_notional, price))
            else:
                q = s.qty_for(self.piece, price)
            link = self._link(camp.sym, f"L{j}")
            camp.orders[link] = LiveOrder(link, f"ступень {j}", "Buy", price, q, now)
            try:
                self.ex.place(symbol=camp.sym, side="Buy", orderType="Limit", qty=fmt(q, s.qty_step), price=fmt(price, s.tick),
                              timeInForce="GTC", orderLinkId=link)
            except Exception as e:
                camp.orders[link].active = False
                self._log("ошибка", sym=camp.sym, what=f"ступень {j}", err=str(e), t=now)
        self._log("лесенка", sym=camp.sym, p0=p0, levels=[o.price for o in camp.orders.values() if o.kind.startswith("ступень")], t=now)

    def _place_or_move_target(self, camp: LiveCamp, now: pd.Timestamp):
        s = self.specs[camp.sym]
        tp = float(fmt(ceil_step(camp.target(self.p), s.tick), s.tick))
        qty = float(fmt(round(camp.qty / s.qty_step) * s.qty_step, s.qty_step))
        if camp.tp_link and camp.orders[camp.tp_link].active:
            o = camp.orders[camp.tp_link]
            try:
                self.ex.amend(symbol=camp.sym, orderLinkId=o.link, qty=fmt(qty, s.qty_step), price=fmt(tp, s.tick))
                o.price = tp; o.qty = qty; o.placed = now; o.touched_at = None; o.max_through = 0.0
                self._log("цель перенесена", sym=camp.sym, price=tp, qty=qty, t=now)
                return
            except Exception as e:                          # не вышло поправить — ставим заново
                self._log("ошибка", sym=camp.sym, what="перенос цели", err=str(e), t=now)
                try:
                    self.ex.cancel(camp.sym, o.link)
                except Exception:
                    pass
                o.active = False
        link = self._link(camp.sym, "T")
        camp.orders[link] = LiveOrder(link, "цель", "Sell", tp, qty, now); camp.tp_link = link
        try:
            self.ex.place(symbol=camp.sym, side="Sell", orderType="Limit", qty=fmt(qty, s.qty_step), price=fmt(tp, s.tick),
                          timeInForce="GTC", reduceOnly=True, orderLinkId=link)
            self._log("цель", sym=camp.sym, price=tp, qty=qty, t=now)
        except Exception as e:
            camp.orders[link].active = False
            self._log("ошибка", sym=camp.sym, what="цель", err=str(e), t=now)

    def _close(self, camp: LiveCamp, now: pd.Timestamp, how: str):
        try:
            self.ex.cancel_all(camp.sym)
        except Exception as e:
            self._log("ошибка", sym=camp.sym, what="снятие заявок", err=str(e), t=now)
        for o in camp.orders.values():
            o.active = False
        buy = sum(q * px for _, px, q in camp.fills)
        sell = sum(o.filled_qty * o.fill_px for o in camp.orders.values() if o.side == "Sell")
        camp.result_usd = sell - buy - camp.fees; camp.state = "закрыта"; camp.closed_at = now
        if how == "нет позиции" and camp.qty > self.specs[camp.sym].qty_step / 2:
            camp.result_usd = None                       # позицию закрыли мимо бота (руками на бирже) — цены продажи не знаем
        self._log("кампания закрыта", sym=camp.sym, how=how, result=camp.result_usd, adds=len(camp.fills) - 1, t=now)
        self.done.append(camp); del self.camps[camp.sym]

    # --- после перезапуска без файла состояния
    def adopt(self, sym: str, size: float, execs: list[dict], open_orders: list[dict], scale: float = 1.0,
              now: pd.Timestamp | None = None) -> bool:
        """Позиция на бирже есть, а кампании нет (файл состояния потерян или не успел записаться). Если позицию собрали
        заявки бота — метки kz… от последнего входа дают ровно этот размер, — восстанавливаем кампанию по исполнениям и
        открытым заявкам: покупки, докупки, частичные выходы, цель, таймер от последней покупки. Иначе позиция чужая."""
        now = now or self.clock()
        s = self.specs.get(sym)
        if s is None:
            return False
        ex = sorted((e for e in execs if e.get("symbol", sym) == sym and link_kind(e.get("orderLinkId"))
                     and e.get("execType", "Trade") == "Trade"), key=lambda e: int(e["execTime"]))
        entries = [e["orderLinkId"] for e in ex if link_kind(e["orderLinkId"]) == "вход"]
        if not entries:
            return False
        start = next(i for i, e in enumerate(ex) if e["orderLinkId"] == entries[-1])   # первое исполнение последнего входа
        ex = ex[start:]
        net = sum(float(e["execQty"]) * (1 if e["side"] == "Buy" else -1) for e in ex)
        if abs(net - size) > s.qty_step / 2:           # размер на бирже = сумма исполнений по меткам, точно
            return False
        at = lambda e: pd.Timestamp(int(e["execTime"]), unit="ms")  # noqa: E731
        camp = LiveCamp(sym, at(ex[0]), scale, equal=self.equal, slot=self.sizing == "slot")
        for e in ex:
            link = e["orderLinkId"]; q = float(e["execQty"]); px = float(e["execPrice"]); fee = float(e.get("execFee") or 0.0)
            o = camp.orders.get(link) or LiveOrder(link, link_kind(link), e["side"], None, 0.0, at(e))
            o.fill_px = (o.fill_px * o.filled_qty + px * q) / (o.filled_qty + q); o.filled_qty += q; o.qty = o.filled_qty
            o.filled_at = at(e); o.active = False; o.fee += fee; camp.fees += fee
            camp.orders[link] = o
            if e.get("execId"):
                self.seen[e["execId"]] = None
        for r in open_orders:                          # стоящие лесенка и цель этой кампании
            link = r.get("orderLinkId"); kind = link_kind(link)
            if not kind or r.get("symbol", sym) != sym or (link not in camp.orders and not kind.startswith("ступень") and kind != "цель"):
                continue
            ct = str(r.get("createdTime") or "")
            o = camp.orders.get(link) or LiveOrder(link, kind, r["side"], None, 0.0, pd.Timestamp(int(ct), unit="ms") if ct.isdigit() else now)
            o.price = float(r["price"]) if r.get("price") else None; o.qty = float(r["qty"]); o.active = True; o.filled_at = None
            camp.orders[link] = o
            if kind == "цель":
                camp.tp_link = link
        for o in camp.orders.values():                 # докупка считается, когда исполнилась целиком (как в on_execution)
            if o.side == "Buy" and not o.active and o.filled_qty > 0:
                camp.fills.append((0 if o.kind == "вход" else int(o.kind.split()[1]), o.fill_px, o.filled_qty))
                camp.last_fill = max(camp.last_fill or o.filled_at, o.filled_at)
        if not any(f[0] == 0 for f in camp.fills):
            return False
        camp.fills.sort(key=lambda f: f[0])
        p0 = next(px for st, px, _ in camp.fills if st == 0)
        camp.q_unit = self.piece / p0 if camp.slot else next(q for st, _, q in camp.fills if st == 0)
        camp.state = "в позиции"
        self.camps[sym] = camp
        self._log("внимание", sym=sym, what="после перезапуска позиция узнана по меткам заявок — кампания восстановлена",
                  qty=size, fills=len(camp.fills), t=now)
        tp = camp.orders.get(camp.tp_link) if camp.tp_link else None
        want = float(fmt(ceil_step(camp.target(self.p), s.tick), s.tick))
        if tp is None or abs(tp.qty - camp.qty) > s.qty_step / 2 or abs((tp.price or 0) - want) > s.tick / 2:
            self._place_or_move_target(camp, now)     # цели нет или она не на весь размер — ставим/двигаем
        return True

    # --- каждая закрытая минутка: касания и таймер
    def on_bar(self, sym: str, t: pd.Timestamp, o_: float, h: float, l: float, c: float):
        camp = self.camps.get(sym)
        if camp is None or camp.state != "в позиции":
            return
        for o in camp.orders.values():
            if not o.active or o.price is None or o.placed > t:
                continue
            if o.side == "Buy" and l <= o.price:
                o.max_through = max(o.max_through, (o.price - l) / o.price * 100)
                o.touched_at = o.touched_at or t
            if o.side == "Sell" and h >= o.price:
                o.max_through = max(o.max_through, (h - o.price) / o.price * 100)
                o.touched_at = o.touched_at or t
        if camp.last_fill is not None and (t - camp.last_fill) >= pd.Timedelta(hours=self.p.timer_h):
            self._timer_exit(camp, t)

    def _timer_exit(self, camp: LiveCamp, t: pd.Timestamp):
        try:
            self.ex.cancel_all(camp.sym)
        except Exception as e:
            self._log("ошибка", sym=camp.sym, what="снятие заявок (таймер)", err=str(e), t=t)
        for o in camp.orders.values():
            if o.kind != "вход":
                o.active = False
        self._send_market(camp, "таймер", camp.qty, t)

    def halt(self, now: pd.Timestamp, close_positions: bool = True):
        """Кнопка «стоп»: снять все заявки, закрыть позиции по рынку, новых входов не делать. Сорвалось — повторит check_pending."""
        self.halted = True
        for camp in list(self.camps.values()):
            try:
                self.ex.cancel_all(camp.sym)
            except Exception as e:
                self._log("ошибка", sym=camp.sym, what="стоп: снятие", err=str(e), t=now)
            for o in camp.orders.values():
                if o.kind not in ("таймер", "стоп"):
                    o.active = False
            if close_positions and camp.qty > self.specs[camp.sym].qty_step / 2:
                self._send_market(camp, "стоп", camp.qty, now)
            elif camp.state == "вход":
                self._log("внимание", sym=camp.sym, what="стоп во время входа — позицию закроет сверка", t=now)
                del self.camps[camp.sym]
        self._log("стоп", t=now)

    def check_pending(self, now: pd.Timestamp | None = None):
        """Раз в минуту: рыночная заявка исполнилась частично или не ушла — довести дело до конца."""
        now = now or self.clock()
        for camp in list(self.camps.values()):
            s = self.specs[camp.sym]
            if camp.state == "вход":
                e = next(o for o in camp.orders.values() if o.kind == "вход")
                age = (now - e.sent_at) if e.sent_at is not None else pd.Timedelta(0)
                if e.filled_qty > 0 and age >= pd.Timedelta(seconds=15):
                    e.active = False; e.filled_at = e.filled_at or now
                    self._log("внимание", sym=camp.sym, what="вход исполнен частично — работаю с исполненным", qty=e.filled_qty, of=e.qty, t=now)
                    self._entry_done(camp, e, now)
                elif e.filled_qty == 0 and age >= pd.Timedelta(seconds=90):
                    self._log("внимание", sym=camp.sym, what="вход не исполнился за 90 с — кампания снята", t=now)
                    del self.camps[camp.sym]
            elif camp.state == "выход":
                exits = [o for o in camp.orders.values() if o.kind in ("таймер", "стоп")]
                last = exits[-1] if exits else None
                stale = last is None or (last.sent_at is not None and now - last.sent_at >= pd.Timedelta(seconds=15) and last.filled_qty < last.qty)
                if stale:
                    rest = camp.qty
                    if rest <= s.qty_step / 2:
                        self._close(camp, now, last.kind if last else "стоп")
                    elif last is not None and last.attempts >= 10:
                        if last.attempts == 10:
                            self._log("ошибка", sym=camp.sym, what="выход не удаётся 10 раз — нужна ручная проверка", qty=rest, t=now)
                            last.attempts += 1
                    else:
                        if last is not None:
                            last.active = False
                        self._send_market(camp, last.kind if last else "стоп", rest, now)

    def reconcile(self, positions: list[dict], now: pd.Timestamp | None = None, close_orphans: bool = True):
        """Сверка с позициями биржи (раз в 30 с): позиция без кампании, кампания без позиции, разный размер.
        Решение — только если расхождение держится две сверки подряд (исполнения могут идти с задержкой)."""
        now = now or self.clock()
        have = {p["symbol"]: float(p.get("size") or 0) for p in positions if float(p.get("size") or 0) > 0 and p.get("side") == "Buy"}
        self.foreign &= {p["symbol"] for p in positions if float(p.get("size") or 0) > 0}   # чужую позицию закрыли — монета снова свободна
        for sym, size in have.items():
            camp = self.camps.get(sym)
            spec = self.specs.get(sym)
            if camp is None and sym in self.foreign:
                continue
            if camp is None:
                self.orphans[sym] = self.orphans.get(sym, 0) + 1
                if self.orphans[sym] >= 2 and close_orphans and spec is not None:
                    self._log("внимание", sym=sym, what="позиция на бирже без кампании — закрываю по рынку", qty=size, t=now)
                    try:
                        self.ex.place(symbol=sym, side="Sell", orderType="Market", qty=fmt(size, spec.qty_step), reduceOnly=True,
                                      orderLinkId=self._link(sym, "O"))
                    except Exception as e:
                        self._log("ошибка", sym=sym, what="закрытие позиции без кампании", err=str(e), t=now)
                    self.orphans[sym] = 0
                continue
            self.orphans.pop(sym, None)
            if camp.state == "в позиции" and spec is not None and abs(size - camp.qty) > spec.qty_step * 1.5:
                self.mismatch[sym] = self.mismatch.get(sym, 0) + 1
                if self.mismatch[sym] == 2:
                    self._log("внимание", sym=sym, what="размер позиции на бирже не совпадает с журналом", exchange=size, bot=camp.qty, t=now)
            else:
                self.mismatch.pop(sym, None)
        for sym, camp in list(self.camps.items()):
            if camp.state in ("в позиции", "выход") and sym not in have:
                self.gone[sym] = self.gone.get(sym, 0) + 1
                if self.gone[sym] >= 2:
                    self._log("внимание", sym=sym, what="позиции на бирже нет — кампания закрыта по сверке", t=now)
                    self._close(camp, now, "нет позиции")
                    self.gone.pop(sym, None)
            else:
                self.gone.pop(sym, None)

    # --- статистика исполнения лимиток
    def fill_stats(self) -> pd.DataFrame:
        rows = []
        for camp in self.done + list(self.camps.values()):
            for o in camp.orders.values():
                if o.price is None:
                    continue
                rows.append(dict(sym=camp.sym, kind="ступень" if o.kind.startswith("ступень") else o.kind,
                                 touched=o.touched_at is not None or o.filled_at is not None, filled=o.filled_at is not None, through=o.max_through))
        return pd.DataFrame(rows)
