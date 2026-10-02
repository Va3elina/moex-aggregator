"""Бот «спокойный Kamaz» — запуск.

  python bot/run.py run --mode paper          бумажная книга на $100 000 по живым ценам (заявок нет, ключи не нужны)
  python bot/run.py run --mode live           бумага + настоящие минимальные заявки на субаккаунте (ключи в bot/.env)
  python bot/run.py run --mode demo           то же, но заявки на Demo Trading Bybit
  python bot/run.py stop                      «стоп»: снять все заявки, закрыть позиции по рынку, новых входов не делать
  python bot/run.py status                    что сейчас происходит
  python bot/run.py report                    итоги: сделки бумажной книги и доля исполненных лимиток

Правила — рабочий вариант после проверок 27.09 (см. experiments/grid_engine.py): лонг, x1, каждый день десятка по обороту,
пропуск монеты после −25% за сутки, лесенка не больше счёта ячейки, без стопа; фильтр по биткоину — флаг --filter.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import core  # noqa: E402
import live as live_mod  # noqa: E402
from bybit import Rest, Stream, WS_PRIVATE, WS_PUBLIC  # noqa: E402
from core import ge  # noqa: E402

sys.path.insert(0, str(core.ROOT / "experiments"))
import universe  # noqa: E402
from factory import market  # noqa: E402

STATE = HERE / "state"
STOP_FLAG = STATE / "STOP"
BTC = "BTCUSDT"
log = logging.getLogger("bot")


def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").tz_localize(None)


def load_env():
    f = HERE / ".env"
    if f.exists():
        for line in f.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


class Journal:
    def __init__(self):
        (STATE / "journal").mkdir(parents=True, exist_ok=True)

    def __call__(self, ev: dict):
        ev = {k: (str(v) if isinstance(v, pd.Timestamp) else (float(v) if isinstance(v, (np.floating,)) else v)) for k, v in ev.items()}
        ev.setdefault("at", str(now_utc().floor("s")))
        day = now_utc().strftime("%Y-%m-%d")
        with open(STATE / "journal" / f"{day}.jsonl", "a") as f:
            f.write(json.dumps(ev, ensure_ascii=False, default=str) + "\n")
        if ev.get("kind") not in ("сигнал",):
            log.info("%s", " ".join(f"{k}={v}" for k, v in ev.items() if k != "at"))


class Bot:
    def __init__(self, mode: str, btc_filter: bool, max_camps: int, capital: float, sizing: str, entry: str = "close", target: str | None = None):
        self.mode = mode
        self.p = ge.P(side=1, L=1.0, btc_filter=btc_filter, crash_skip=25.0, cap_to_equity=True)
        self.entry = entry                               # close — сигнал на закрытии минутки (рабочий); intrabar — вариант B (только бумага)
        if target:                                       # другая цель, например «3.9/3.0» = +3.9% с одной покупки / +3.0% от средней
            tp1, tpn = (float(x) for x in target.split("/"))
            self.p = ge.P(side=1, L=1.0, btc_filter=btc_filter, crash_skip=25.0, cap_to_equity=True, tp1=tp1, tpn=tpn)
        self.journal = Journal()
        if mode != "paper":                              # бумаге ключи не нужны — и не читаем их (вторая книга B работает без ключей)
            load_env()
        env = "demo" if mode == "demo" else "main"
        self.rest = Rest(env, os.environ.get("BYBIT_API_KEY"), os.environ.get("BYBIT_API_SECRET"))
        self.env = env
        self.windows: dict[str, core.Window] = {}
        self.books: dict[str, core.PaperBook] = {}
        self.day: pd.Timestamp | None = None
        self.top: list[str] = []
        self.scales: dict[str, float] = {}
        self.inst: dict[str, dict] = {}
        self.pending: dict[pd.Timestamp, dict[str, dict]] = {}
        self.first_seen: dict[pd.Timestamp, float] = {}
        self.last_t: pd.Timestamp | None = None
        self.live = None
        self.max_camps = max_camps; self.capital = capital; self.sizing = sizing
        self.public: Stream | None = None
        self.private: Stream | None = None
        # все действия с настоящими заявками — в одном отдельном потоке по очереди: сетевые вызовы не тормозят минутки и пинги
        self.live_q: queue.Queue = queue.Queue()
        self.live_lock = threading.Lock()
        self.live_snap: dict = {}                    # снимок состояния исполнителя для сохранения и статуса (пишет только поток)
        self.halt_sent = False
        self._reconcile_queued = False               # сверка уже ждёт в очереди — вторую не ставим (медленный REST не раздует очередь)
        self._next_refresh_try = 0.0

    # ---------- отбор монет и масштаб на день
    def refresh_day(self, day: pd.Timestamp):
        t0 = time.time()
        self.inst = {i["symbol"]: i for i in self.rest.instruments()
                     if i.get("quoteCoin") == "USDT" and i.get("contractType") == "LinearPerpetual" and i.get("status") == "Trading"}
        perps = sorted(self.inst)
        with ThreadPoolExecutor(8) as ex:              # дневные свечи всех контрактов (последний день перекачивается)
            list(ex.map(lambda s: market.klines(s, "D", day - pd.Timedelta(days=200), day + pd.Timedelta(days=3)), perps))
        U = universe.select(10, day.strftime("%Y-%m-%d"), day.strftime("%Y-%m-%d"), perps=perps)
        self.top = [s for s in U.columns if U.loc[day, s] == 1]
        doge = ge.range_series("DOGEUSDT", day - pd.Timedelta(days=5), day + pd.Timedelta(days=1))
        self.scales = {s: float(ge.daily_scale(s, pd.DatetimeIndex([day]), doge)[0]) for s in self.top}
        self.day = day
        self.journal(dict(kind="десятка дня", day=str(day.date()), top=[s.replace("USDT", "") for s in self.top],
                          scales={s.replace("USDT", ""): round(v, 3) for s, v in self.scales.items()}, sec=round(time.time() - t0)))

    def symbols_needed(self) -> list[str]:
        busy = [s for s, b in self.books.items() if b.x is not None]
        if self.live:
            busy += list(self.live_snap.get("camps", {}))
        return sorted(set(self.top) | set(busy) | {BTC})

    # ---------- минутки
    def load_window(self, sym: str, until: pd.Timestamp):
        start = until - pd.Timedelta(minutes=core.KEEP + 5)
        rows = self.rest.klines(sym, int(start.timestamp() * 1000), int((until + pd.Timedelta(minutes=1)).timestamp() * 1000) - 1)
        k = pd.DataFrame([[pd.Timestamp(int(r[0]), unit="ms")] + [float(x) for x in r[1:6]] for r in rows], columns=["t", "o", "h", "l", "c", "v"]).set_index("t")
        self.windows[sym] = core.Window(k[k.index <= until])

    def ensure_window(self, sym: str, t: pd.Timestamp):
        w = self.windows.get(sym)
        if w is None or w.last_t is None or w.last_t < t - pd.Timedelta(minutes=1):
            self.load_window(sym, t - pd.Timedelta(minutes=1))

    def process_minute(self, t: pd.Timestamp, bars: dict[str, dict], replay: bool = False):
        if (self.day is None or t.floor("D") != self.day) and time.time() >= self._next_refresh_try:
            try:
                self.refresh_day(t.floor("D"))
            except Exception as e:
                self._next_refresh_try = time.time() + 300
                self.journal(dict(kind="ошибка", what="десятка дня не обновилась — работаю со вчерашней, повтор через 5 мин", err=repr(e)))
        syms = self.symbols_needed()
        for s in syms:                                   # сначала все минутки — чтобы биткоин был на месте для фильтра
            if s not in bars:
                continue
            self.ensure_window(s, t)
            b = bars[s]
            self.windows[s].append(t, b["o"], b["h"], b["l"], b["c"], b.get("v", 0.0))
        for s in syms:
            if s == BTC and s not in self.top and s not in self.books:
                continue
            if s not in bars:
                self.journal(dict(kind="нет минутки", sym=s, t=t))
                continue
            b = bars[s]
            if self.entry == "intrabar":                 # вариант B: вход внутри этой же минутки по цене срабатывания
                book = self.books.setdefault(s, core.PaperBookB(s, self.p))
                fund = self.funding_at(s, t) if book.x is not None else 0.0
                trig = core.trigger_now(self.windows[s], self.p, self.scales.get(s, 1.0)) if s in self.top else float("nan")
                for ev in book.step(t, b["o"], b["h"], b["l"], b["c"], fund, trig=trig):
                    self.journal(dict(src="бумага", sym=s, **ev, **({"trig": trig} if ev["kind"] == "вход" else {})))
            else:
                book = self.books.setdefault(s, core.PaperBook(s, self.p))
                fund = self.funding_at(s, t) if book.x is not None else 0.0
                for ev in book.step(t, b["o"], b["h"], b["l"], b["c"], fund):
                    self.journal(dict(src="бумага", sym=s, **ev))
            allowed = s in self.top
            scale = self.scales.get(s, 1.0)
            sg = core.signal_now(self.windows[s], self.windows.get(BTC), self.p, scale, allowed)
            book.set_signal(sg["sig"], scale)
            if sg["sig"]:
                self.journal(dict(kind="сигнал", sym=s, t=t, price=b["c"], r3=round(sg["r3"], 1), from_hi=round(sg["from_hi"], 2), ch120=round(sg["ch120"], 2)))
            if self.live and not replay:
                self.live_call(self.live.on_bar, s, t, b["o"], b["h"], b["l"], b["c"])
                if sg["sig"] and not STOP_FLAG.exists():
                    self.live_call(self.live.on_signal, s, t, b["c"], scale)
        if self.live and not replay:
            self.live_call(self.live.check_pending)
        self.last_t = t
        self.save_state()

    def funding_at(self, sym: str, t: pd.Timestamp) -> float:
        iv = int(self.inst.get(sym, {}).get("fundingInterval", 480) or 480)
        mins = t.hour * 60 + t.minute
        if mins % iv:
            return 0.0
        try:
            for r in self.rest.funding_last(sym, 3):
                if pd.Timestamp(int(r["fundingRateTimestamp"]), unit="ms") == t:
                    return float(r["fundingRate"])
        except Exception as e:
            self.journal(dict(kind="ошибка", what="ставка финансирования", sym=sym, err=str(e)))
        return 0.0

    # ---------- потоки
    async def on_public(self, msg: dict):
        topic = msg.get("topic", "")
        if not topic.startswith("kline.1."):
            return
        sym = topic.split(".")[2]
        for d in msg.get("data", []):
            if not d.get("confirm"):
                continue
            t = pd.Timestamp(int(d["start"]), unit="ms")
            self.pending.setdefault(t, {})[sym] = dict(o=float(d["open"]), h=float(d["high"]), l=float(d["low"]), c=float(d["close"]), v=float(d["volume"]))
            self.first_seen.setdefault(t, time.time())

    async def on_private(self, msg: dict):
        if not self.live or not msg.get("topic", "").startswith("execution"):
            return
        for e in msg.get("data", []):
            if e.get("execType", "Trade") == "Trade":
                self.live_call(self.live.on_execution, e, now_utc())      # повторы отсеет сам исполнитель по execId

    async def minute_loop(self):
        """Обрабатываем минутку, когда пришли все монеты или прошло 3 с; пропуски добираем по REST."""
        while True:
            await asyncio.sleep(0.25)
            if STOP_FLAG.exists() and self.live and not self.halt_sent:
                self.halt_sent = True
                self.live_call(self.live.halt, now_utc())
            try:
                await self._minute_step()
            except Exception as e:                       # одна минутка не должна уронить весь цикл
                logging.exception("цикл минуток")
                self.journal(dict(kind="ошибка", what="цикл минуток", err=repr(e)))

    async def _minute_step(self):
        ready = sorted(self.pending)
        for t in ready:
            need = set(self.symbols_needed())
            got = set(self.pending[t])
            if not need <= got and time.time() - self.first_seen[t] < 3:
                break
            bars = self.pending.pop(t); self.first_seen.pop(t, None)
            if self.last_t is not None and t <= self.last_t:
                continue
            if self.last_t is not None and t > self.last_t + pd.Timedelta(minutes=1):
                try:
                    await asyncio.to_thread(self.catch_up, self.last_t + pd.Timedelta(minutes=1), t - pd.Timedelta(minutes=1))
                except Exception as e:               # не догнали пропуск — идём дальше с текущей минутки
                    self.journal(dict(kind="ошибка", what="догонялка пропуска", err=repr(e)))
            if (self.day is None or t.floor("D") != self.day) and time.time() >= self._next_refresh_try:   # новый день: десятка и масштабы
                try:
                    await asyncio.to_thread(self.refresh_day, t.floor("D"))
                except Exception as e:
                    self._next_refresh_try = time.time() + 300
                    self.journal(dict(kind="ошибка", what="десятка дня не обновилась — работаю со вчерашней, повтор через 5 мин", err=repr(e)))
                need = set(self.symbols_needed())
            for s in need - set(bars):             # не пришла по потоку — добираем
                b = await asyncio.to_thread(self.rest_bar, s, t)
                if b:
                    bars[s] = b
            try:
                self.process_minute(t, bars)
            except Exception as e:
                logging.exception("ошибка минутки %s", t)
                self.journal(dict(kind="ошибка", what="минутка", t=t, err=repr(e)))
            await self.sync_subscriptions()

    async def sync_subscriptions(self):
        if self.public is None:
            return
        want = {f"kline.1.{s}" for s in self.symbols_needed()}
        have = set(self.public.topics)
        if want - have:
            await self.public.subscribe(sorted(want - have))
            for s in sorted(want - have):
                await asyncio.to_thread(self.ensure_window, s.split(".")[2], self.last_t + pd.Timedelta(minutes=1))
        if have - want:
            await self.public.unsubscribe(sorted(have - want))

    def rest_bar(self, sym: str, t: pd.Timestamp) -> dict | None:
        ms = int(t.timestamp() * 1000)
        try:
            r = [x for x in self.rest.klines(sym, ms, ms + 59_999) if int(x[0]) == ms]
            return dict(o=float(r[0][1]), h=float(r[0][2]), l=float(r[0][3]), c=float(r[0][4]), v=float(r[0][5])) if r else None
        except Exception:
            return None

    def catch_up(self, a: pd.Timestamp, b: pd.Timestamp):
        """Пропущенные минутки (перезапуск, обрыв): прогоняем бумагу по порядку, заявок по прошлым сигналам не ставим."""
        syms = self.symbols_needed()
        data = {}
        for s in syms:
            rows = self.rest.klines(s, int(a.timestamp() * 1000), int(b.timestamp() * 1000) + 59_999)
            data[s] = {pd.Timestamp(int(r[0]), unit="ms"): dict(o=float(r[1]), h=float(r[2]), l=float(r[3]), c=float(r[4]), v=float(r[5])) for r in rows}
        n = 0
        for t in pd.date_range(a, b, freq="min"):
            bars = {s: d[t] for s, d in data.items() if t in d}
            if bars:
                self.process_minute(t, bars, replay=True); n += 1
        self.journal(dict(kind="догнали пропуск", a=a, b=b, minutes=n))

    async def housekeeping(self):
        while True:
            await asyncio.sleep(30)
            try:
                self.write_status()
            except Exception as e:
                self.journal(dict(kind="ошибка", what="статус", err=repr(e)))
            if self.live and not self._reconcile_queued:
                self._reconcile_queued = True
                self.live_call(self._reconcile_job)
            for st in (self.public, self.private):       # сторож: связь формально жива, а сообщений нет — переподключаем
                if st is not None and st.ws is not None and time.time() - st.last_msg > 120:
                    self.journal(dict(kind="внимание", what=f"поток «{st.name}» молчит {int(time.time() - st.last_msg)} с — переподключаю"))
                    try:
                        await st.ws.close()
                    except Exception:
                        pass

    # ---------- поток исполнителя
    def live_call(self, fn, *args):
        if self.live:
            self.live_q.put((fn, args))

    def _live_worker(self):
        while True:
            fn, args = self.live_q.get()
            name = getattr(fn, "__name__", str(fn))
            try:
                with self.live_lock:
                    fn(*args)
            except Exception as e:
                logging.exception("исполнитель: %s", name)
                self.journal(dict(kind="ошибка", what=f"исполнитель: {name}", err=repr(e)))
            try:
                with self.live_lock:
                    self.live_snap = self._live_state()
            except Exception as e:
                self.journal(dict(kind="ошибка", what="снимок исполнителя", err=repr(e)))

    def _reconcile_job(self):
        """В потоке исполнителя: исполнения по REST (на случай пропуска в потоке) и сверка позиций с биржей."""
        self._reconcile_queued = False
        since = int((now_utc() - pd.Timedelta(hours=24)).timestamp() * 1000)
        for s in list(self.live.camps):
            for e in self.rest.executions(s, since):
                if e.get("execType", "Trade") == "Trade":
                    self.live.on_execution(e, now_utc())
        self.live.reconcile(self.rest.positions(), now_utc())

    def _live_state(self) -> dict:
        L = self.live
        return dict(halted=L.halted, seen=list(L.seen)[-3000:], done=len(L.done),
                    camps={s: dict(t_sig=str(c.t_sig), scale=c.scale, fills=list(c.fills), state=c.state, tp_link=c.tp_link,
                                   q_unit=c.q_unit, equal=c.equal, slot=c.slot, fees=c.fees, last_fill=str(c.last_fill),
                                   tp=(c.orders[c.tp_link].price if c.tp_link and c.tp_link in c.orders else None),
                                   orders={k: dict(o.__dict__, placed=str(o.placed), filled_at=str(o.filled_at), touched_at=str(o.touched_at),
                                                   sent_at=str(o.sent_at)) for k, o in c.orders.items()})
                           for s, c in L.camps.items()})

    async def guard(self, name: str, factory):
        """Задача упала — пишем в журнал и поднимаем её снова, а не роняем весь бот."""
        while True:
            try:
                await factory()
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.exception("задача %s", name)
                self.journal(dict(kind="ошибка", what=f"задача «{name}» упала — перезапускаю", err=repr(e)))
                await asyncio.sleep(5)

    # ---------- состояние
    def save_state(self):
        st = dict(last_t=str(self.last_t) if self.last_t is not None else None, books={})
        for s, b in self.books.items():
            x = b.x
            st["books"][s] = dict(cash=b.cash, prev_sig=b.prev_sig, prev_scale=b.prev_scale, prev_c=b.prev_c, camps=[list(map(str, c)) for c in b.camps[-50:]],
                                  x=None if x is None else dict(t0=str(x.t0), p0=x.p0, Q=x.Q, cost=x.cost, bu=x.bu, sc=x.sc, lv=list(x.lv), c0=x.c0, k=x.k, last=str(x.last)))
        if self.live and self.live_snap:
            st["exec_seen"] = self.live_snap.get("seen", [])
            st["live"] = dict(halted=self.live_snap.get("halted"), camps=self.live_snap.get("camps", {}))
        tmp = STATE / "state.json.tmp"
        tmp.write_text(json.dumps(st, ensure_ascii=False, default=str)); tmp.replace(STATE / "state.json")

    def load_state(self):
        f = STATE / "state.json"
        if not f.exists():
            return
        st = json.loads(f.read_text())
        self.last_t = pd.Timestamp(st["last_t"]) if st.get("last_t") else None
        for s, d in st.get("books", {}).items():
            b = (core.PaperBookB if self.entry == "intrabar" else core.PaperBook)(s, self.p, cash=d["cash"])
            b.prev_sig = d["prev_sig"]; b.prev_scale = d["prev_scale"]; b.prev_c = d["prev_c"]
            if d.get("x"):
                x = d["x"]
                b.x = core.Campaign(t0=pd.Timestamp(x["t0"]), p0=x["p0"], Q=x["Q"], cost=x["cost"], bu=x["bu"], sc=x["sc"], lv=np.array(x["lv"]), c0=x["c0"],
                                    k=x["k"], last=pd.Timestamp(x["last"]))
            self.books[s] = b
        if self.live and st.get("live"):
            ts = lambda v: None if v in (None, "None", "NaT") else pd.Timestamp(v)  # noqa: E731
            self.live.seen = dict.fromkeys(st.get("exec_seen", []))
            self.live.halted = bool(st["live"].get("halted")); self.halt_sent = self.live.halted
            for s, d in st["live"].get("camps", {}).items():
                c = live_mod.LiveCamp(s, pd.Timestamp(d["t_sig"]), d["scale"], q_unit=d.get("q_unit", 0.0), equal=d.get("equal", False),
                                      slot=d.get("slot", False))
                c.fills = [tuple(x) for x in d["fills"]]; c.state = d["state"]; c.tp_link = d["tp_link"]; c.last_fill = ts(d["last_fill"])
                c.fees = float(d.get("fees") or 0.0)
                for k, o in d["orders"].items():
                    c.orders[k] = live_mod.LiveOrder(**{**o, "placed": ts(o["placed"]), "filled_at": ts(o["filled_at"]), "touched_at": ts(o["touched_at"]),
                                                        "sent_at": ts(o.get("sent_at"))})
                self.live.camps[s] = c
            if self.live.camps:
                self.journal(dict(kind="после перезапуска", what="восстановлены настоящие кампании, пропущенные исполнения доберёт сверка",
                                  camps=list(self.live.camps)))

    def write_status(self):
        lines = [f"режим {self.mode}{'' if self.mode == 'paper' else {'slot': f', бюджет ${self.capital:.0f}: {self.max_camps} ячеек по ${self.capital / self.max_camps:.0f}, лесенка 1:3.08:…',
                                       'equal': f', бюджет ${self.capital:.0f}: 7 равных покупок, до {self.max_camps} монет',
                                       'min': f', минимальные заявки по $5.5, до {self.max_camps} монет'}[self.sizing]}, фильтр по биткоину {'вкл' if self.p.btc_filter else 'выкл'}, последняя минутка {self.last_t} UTC, сейчас {now_utc().floor('s')} UTC",
                 f"десятка дня: {', '.join(s.replace('USDT', '') for s in self.top)}"]
        if self.entry != "close" or (self.p.tp1, self.p.tpn) != (ge.P().tp1, ge.P().tpn):
            lines.insert(1, f"вариант: вход {'внутри минутки (B)' if self.entry == 'intrabar' else 'на закрытии'}, цель {self.p.tp1}/{self.p.tpn}, таймер {self.p.timer_h:g} ч")
        eq = 0.0; n = 0
        for s, b in sorted(self.books.items()):
            w = self.windows.get(s)
            px = w.k.c.iloc[-1] if w is not None and len(w.k) else (b.prev_c or 0)
            e = b.equity(px); eq += e - b.E0; n += 1
            if b.x is not None:
                lines.append(f"  бумага {s.replace('USDT', ''):6s}: с {b.x.t0:%d.%m %H:%M}, докупок {b.x.k}, средняя {b.x.avg:.6g}, цена {px:.6g}, цель {b.x.target(self.p):.6g}, "
                             f"сейчас {(px - b.x.avg) * b.x.Q:+.0f}$")
        lines.append(f"бумажная книга: ячеек {n}, итог {eq:+,.0f}$ от стартовых по $10 000")
        if self.live:
            snap = self.live_snap
            lines.append(f"настоящие заявки: {'СТОП' if snap.get('halted') else 'работают'}, открыто кампаний {len(snap.get('camps', {}))}, "
                         f"закрыто {snap.get('done', 0)}, в очереди {self.live_q.qsize()}")
            for s, c in snap.get("camps", {}).items():
                lines.append(f"  реал {s.replace('USDT', '')}: {c['state']}, исполнений {len(c['fills'])}, цель {c.get('tp') or '—'}")
        (STATE / "status.txt").write_text("\n".join(lines) + "\n")

    # ---------- запуск
    async def main(self):
        STATE.mkdir(exist_ok=True)
        if self.mode in ("live", "demo"):
            if not (self.rest.key and self.rest.secret):
                raise SystemExit("нет ключей: запусти bot/setup_keys.sh")
            inst = {i["symbol"]: i for i in self.rest.instruments()}
            specs = {s: live_mod.Spec.from_instrument(i) for s, i in inst.items() if s.endswith("USDT")}
            self.live = live_mod.LiveExec(self.rest, specs, self.p, self.journal, self.max_camps, capital=self.capital, sizing=self.sizing)
            threading.Thread(target=self._live_worker, name="исполнитель", daemon=True).start()
            w = self.rest.wallet()
            self.journal(dict(kind="счёт", env=self.env, equity=w.get("totalEquity"), available=w.get("totalAvailableBalance")))
        self.load_state()
        if self.live:
            pos = [p for p in self.rest.positions() if float(p.get("size", 0)) > 0 and p["symbol"] not in self.live.camps]
            for p in pos:                                # своя позиция без файла состояния — узнаём по меткам заявок kz…
                try:
                    if p.get("side") == "Buy":
                        self.live.adopt(p["symbol"], float(p["size"]), self.rest.executions(p["symbol"]), self.rest.open_orders(p["symbol"]),
                                        scale=getattr(self, "scales", {}).get(p["symbol"], 1.0))
                except Exception as e:
                    self.journal(dict(kind="ошибка", sym=p["symbol"], what="восстановление позиции по меткам заявок", err=repr(e)))
            pos = [p for p in pos if p["symbol"] not in self.live.camps]
            if pos:                                      # не наши (кампаний по ним нет): сверка их не закрывает, входов в эти монеты нет
                self.live.foreign = {p["symbol"] for p in pos}
                self.journal(dict(kind="внимание", what="на счёте есть чужие позиции — бот их не трогает, новые входы только по свободным монетам",
                                  pos=[(p["symbol"], p["size"]) for p in pos]))
            with self.live_lock:
                self.live_snap = self._live_state()
        t_now = now_utc().floor("min") - pd.Timedelta(minutes=1)
        if self.last_t is not None and t_now - self.last_t > pd.Timedelta(days=2):
            self.journal(dict(kind="внимание", what="бот стоял больше двух суток — бумага начинает заново", last_t=self.last_t))
            self.books.clear(); self.last_t = None
        start = self.last_t if self.last_t is not None and self.last_t < t_now else t_now
        await asyncio.to_thread(self.refresh_day, start.floor("D"))
        for s in self.symbols_needed():
            await asyncio.to_thread(self.load_window, s, start)
        if start < t_now:
            try:
                await asyncio.to_thread(self.catch_up, start + pd.Timedelta(minutes=1), t_now)
            except Exception as e:                       # не догнали — продолжаем с текущей минутки, а не падаем
                self.journal(dict(kind="ошибка", what="догонялка при запуске", err=repr(e)))
                for s in self.symbols_needed():
                    await asyncio.to_thread(self.load_window, s, t_now)
        self.last_t = t_now
        if self.live:
            for s in self.top:
                if self.live.eligible(s, self.windows[s].k.c.iloc[-1]):
                    try:
                        await asyncio.to_thread(self.rest.set_leverage, s, "3")
                    except Exception as e:
                        self.journal(dict(kind="ошибка", what="плечо", sym=s, err=repr(e)))
        on_err = lambda name, e: self.journal(dict(kind="ошибка", what=f"обработка сообщения «{name}»", err=repr(e)))  # noqa: E731
        self.public = Stream(WS_PUBLIC, [f"kline.1.{s}" for s in self.symbols_needed()], self.on_public, name="рынок", on_error=on_err)
        tasks = [self.guard("рынок", self.public.run), self.guard("минутки", self.minute_loop), self.guard("обслуживание", self.housekeeping)]
        if self.live:
            self.private = Stream(WS_PRIVATE[self.env], ["execution.linear"], self.on_private,
                                  auth=(self.rest.key, self.rest.secret), name="исполнения", on_error=on_err)
            tasks.append(self.guard("исполнения", self.private.run))
        self.journal(dict(kind="старт", mode=self.mode, filter=self.p.btc_filter, top=[s.replace("USDT", "") for s in self.top],
                          **({"entry": self.entry, "tp": f"{self.p.tp1}/{self.p.tpn}"} if self.entry != "close" or self.p.tp1 != ge.P().tp1 else {})))
        await asyncio.gather(*tasks)


def check(mode: str):
    """Проверка ключа: права, привязка к IP, срок, баланс — без вывода самого ключа."""
    load_env()
    env = "demo" if mode == "demo" else "main"
    r = Rest(env, os.environ.get("BYBIT_API_KEY"), os.environ.get("BYBIT_API_SECRET"))
    if os.environ.get("BYBIT_KEY_MODE") and os.environ["BYBIT_KEY_MODE"] != mode:
        print(f"внимание: ключ сохранён как {os.environ['BYBIT_KEY_MODE']}, а проверяем {mode}")
    try:
        info = r.get("/v5/user/query-api")
    except Exception as e:
        print("ключ не работает:", e); return
    perms = info.get("permissions", {})
    print("счёт:", "ДЕМО" if env == "demo" else "РЕАЛЬНЫЙ", "| только чтение:", "да" if str(info.get("readOnly")) == "1" else "нет")
    print("права:", {k: v for k, v in perms.items() if v})
    print("привязка к IP:", info.get("ips") or "нет", "| истекает:", info.get("expiredAt") or "—")
    w = r.wallet()
    print("на счёте:", w.get("totalEquity"), "USD, свободно:", w.get("totalAvailableBalance"))
    ok = str(info.get("readOnly")) != "1" and any("Order" in x or "Position" in x for x in perms.get("ContractTrade", []))
    print("ИТОГ:", "ключ подходит для бота" if ok else "НЕ подходит: нужны «Чтение и запись» и Контракт → Ордера + Позиции")


def report():
    rows = []
    for f in sorted((STATE / "journal").glob("*.jsonl")):
        rows += [json.loads(x) for x in f.read_text().splitlines() if x.strip()]
    J = pd.DataFrame(rows)
    if J.empty:
        print("журнал пуст"); return
    J["src"] = J["src"] if "src" in J else None
    paper = J[(J.src == "бумага") & J.kind.isin(["цель", "таймер"])]
    print(f"бумага: закрыто кампаний {len(paper)}, итог {paper.pnl.sum():+,.2f}$" if len(paper) else "бумага: закрытых кампаний нет")
    lv = J[(J.src == "реал") & (J.kind == "кампания закрыта")]
    print(f"реал: закрыто кампаний {len(lv)}, итог {lv.result.sum():+,.4f}$" if len(lv) else "реал: закрытых кампаний нет")


def compare(a_dir: Path, b_dir: Path, since: str | None = None):
    """Бумажные книги двух состояний (например рабочая A и вариант B) за общий период: закрытые кампании и открытые позиции."""
    def load(d: Path):
        rows = []
        for f in sorted((d / "journal").glob("*.jsonl")):
            rows += [json.loads(x) for x in f.read_text().splitlines() if x.strip()]
        J = pd.DataFrame(rows)
        st = json.loads((d / "state.json").read_text()) if (d / "state.json").exists() else {}
        return J, st
    JA, SA = load(a_dir); JB, SB = load(b_dir)
    if JB.empty:
        print("журнал B пуст"); return
    t0 = pd.Timestamp(since) if since else pd.Timestamp(JB.loc[JB.kind == "старт", "at"].min())
    print(f"сравнение с {t0} UTC: A = {a_dir.name}, B = {b_dir.name}")
    out = []
    for name, J, st in (("A", JA, SA), ("B", JB, SB)):
        J = J[J["src"] == "бумага"].copy() if "src" in J else pd.DataFrame(columns=["sym", "kind", "t", "price", "pnl"])
        J["t"] = pd.to_datetime(J.t)
        ent = J[(J.kind == "вход") & (J.t >= t0)]
        cl = J[J.kind.isin(["цель", "таймер"]) & (J.t >= t0)]
        opened = [(s, b["x"]) for s, b in st.get("books", {}).items() if b.get("x")]
        out.append(dict(книга=name, входов=len(ent), закрыто=len(cl), по_цели=int((cl.kind == "цель").sum()), по_таймеру=int((cl.kind == "таймер").sum()),
                        в_плюс=f"{(cl.pnl > 0).mean() * 100:.0f}%" if len(cl) else "—", итог=f"{cl.pnl.sum():+,.2f}$" if len(cl) else "0",
                        открыто=", ".join(f"{s.replace('USDT', '')} с {pd.Timestamp(x['t0']):%d.%m %H:%M}" for s, x in opened) or "—"))
        if len(cl):
            print(f"\n{name}: закрытые кампании")
            print(cl[["sym", "kind", "t", "price", "pnl"]].assign(sym=cl.sym.str.replace("USDT", ""), t=cl.t.dt.strftime("%d.%m %H:%M")).to_string(index=False))
    print()
    print(pd.DataFrame(out).to_string(index=False))
    print("(закрытые кампании, начатые до начала сравнения, тоже учтены, если закрылись после него; итог — по ячейкам $10 000 без открытых)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "stop", "status", "report", "check", "compare"])
    ap.add_argument("--mode", choices=["paper", "live", "demo"], default="paper")
    ap.add_argument("--filter", action="store_true", help="фильтр по биткоину")
    ap.add_argument("--max-camps", type=int, default=live_mod.MAX_CAMPS)
    ap.add_argument("--sizing", choices=["min", "equal", "slot"], default="min",
                    help="min — минимальные заявки по $5.5; equal — 7 равных покупок; slot — настоящая лесенка на ячейку бюджет/ячейки")
    ap.add_argument("--capital", type=float, default=live_mod.CAPITAL, help="бюджет для режимов equal и slot, $")
    ap.add_argument("--entry", choices=["close", "intrabar"], default="close",
                    help="close — сигнал на закрытии минутки (рабочий); intrabar — вариант B: RSI по формирующейся 3-минутке, вход внутри минутки (только бумага)")
    ap.add_argument("--target", default=None, help="цель «tp1/tpn» в %%, например 3.9/3.0 (по умолчанию 1.94/1.6)")
    ap.add_argument("--state", default=None, help="каталог состояния (по умолчанию bot/state) — для второй книги, например bot/state_b")
    ap.add_argument("--b-state", default=None, help="compare: каталог состояния книги B (по умолчанию bot/state_b)")
    ap.add_argument("--since", default=None, help="compare: начало периода (по умолчанию — первый старт книги B)")
    a = ap.parse_args()
    if a.state:
        STATE = Path(a.state).resolve(); STOP_FLAG = STATE / "STOP"
    if a.cmd == "run" and (a.entry != "close" or a.target) and a.mode != "paper":
        raise SystemExit("вариант B и другая цель — только для бумажной книги (--mode paper)")
    STATE.mkdir(exist_ok=True)
    if a.cmd == "stop":
        STOP_FLAG.write_text(str(now_utc())); print("стоп отправлен: бот снимет заявки и закроет позиции в течение секунды")
    elif a.cmd == "status":
        print((STATE / "status.txt").read_text() if (STATE / "status.txt").exists() else "статуса нет — бот не запущен?")
    elif a.cmd == "report":
        report()
    elif a.cmd == "check":
        check(a.mode)
    elif a.cmd == "compare":
        compare(STATE, Path(a.b_state).resolve() if a.b_state else HERE / "state_b", a.since)
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s",
                            handlers=[logging.StreamHandler(), logging.FileHandler(STATE / "bot.log")])
        asyncio.run(Bot(a.mode, a.filter, a.max_camps, a.capital, a.sizing, entry=a.entry, target=a.target).main())
