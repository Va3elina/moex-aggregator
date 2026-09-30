"""Минимальный клиент Bybit V5 для бота: REST (публичный и с подписью) и WebSocket (рынок и личный поток).

Ключи берутся из окружения BYBIT_API_KEY / BYBIT_API_SECRET (файл .env рядом с ботом, его заполняет setup_keys.sh).
Режимы: main — настоящая биржа, demo — Demo Trading (заявки на api-demo, цены с основной биржи).
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request

import websockets

log = logging.getLogger("bybit")

REST = {"main": "https://api.bybit.com", "demo": "https://api-demo.bybit.com"}
WS_PUBLIC = "wss://stream.bybit.com/v5/public/linear"
WS_PRIVATE = {"main": "wss://stream.bybit.com/v5/private", "demo": "wss://stream-demo.bybit.com/v5/private"}
RECV_WINDOW = "10000"
RETRY_CODES = {10000, 10002, 10006, 10016, 10018, 10429}   # таймаут, время, частота запросов, внутренняя ошибка сервиса


class BybitError(RuntimeError):
    def __init__(self, code, msg, path):
        super().__init__(f"{path}: retCode={code} {msg}")
        self.code = code


class Rest:
    def __init__(self, env: str = "main", key: str | None = None, secret: str | None = None):
        self.base = REST[env]; self.key = key; self.secret = secret

    def _sign(self, ts: str, payload: str) -> str:
        return hmac.new(self.secret.encode(), (ts + self.key + RECV_WINDOW + payload).encode(), hashlib.sha256).hexdigest()

    def _request(self, method: str, path: str, params: dict | None = None, signed: bool = False, base: str | None = None):
        params = {k: v for k, v in (params or {}).items() if v is not None}
        url = (base or self.base) + path
        headers = {"User-Agent": "kamaz-bot/1.0", "Content-Type": "application/json"}
        body = None
        if method == "GET":
            qs = urllib.parse.urlencode(params)
            if qs:
                url += "?" + qs
            payload = qs
        else:
            body = json.dumps(params, separators=(",", ":"))
            payload = body
        if signed:
            if not (self.key and self.secret):
                raise RuntimeError("нет ключей Bybit (запусти setup_keys.sh)")
            ts = str(int(time.time() * 1000))
            headers.update({"X-BAPI-API-KEY": self.key, "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": RECV_WINDOW,
                            "X-BAPI-SIGN": self._sign(ts, payload)})
        last = None
        for attempt in range(5):
            try:
                req = urllib.request.Request(url, data=body.encode() if body else None, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=15) as r:
                    out = json.load(r)
                code = out.get("retCode")
                if code == 0:
                    return out.get("result", {})
                last = BybitError(code, out.get("retMsg"), path)
                if code not in RETRY_CODES:                  # ошибка в запросе — повтор не поможет
                    raise last
            except BybitError:
                raise
            except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
                last = e
            time.sleep(1 + 2 * attempt)                      # временная ошибка биржи или сети — ждём и повторяем
            if signed:                                       # новая метка времени для повтора
                ts = str(int(time.time() * 1000))
                headers.update({"X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": self._sign(ts, payload)})
        if isinstance(last, BybitError):
            raise last
        raise RuntimeError(f"{path}: сеть недоступна ({last})")

    # --- публичное (всегда с основной биржи: у демо те же цены)
    def public(self, path: str, params: dict | None = None):
        return self._request("GET", path, params, base=REST["main"])

    def instruments(self) -> list[dict]:
        out, cursor = [], None
        while True:
            r = self.public("/v5/market/instruments-info", dict(category="linear", limit=1000, cursor=cursor))
            out += r.get("list", [])
            cursor = r.get("nextPageCursor")
            if not cursor:
                return out

    def klines(self, sym: str, start_ms: int, end_ms: int, interval: str = "1") -> list[list]:
        rows, end = [], end_ms
        while True:
            L = self.public("/v5/market/kline", dict(category="linear", symbol=sym, interval=interval, start=start_ms, end=end, limit=1000)).get("list", [])
            if not L:
                break
            rows += L
            oldest = int(L[-1][0])
            if oldest <= start_ms or len(L) < 1000:
                break
            end = oldest - 1
        return sorted({int(r[0]): r for r in rows}.values(), key=lambda r: int(r[0]))

    def funding_last(self, sym: str, limit: int = 3) -> list[dict]:
        return self.public("/v5/market/funding/history", dict(category="linear", symbol=sym, limit=limit)).get("list", [])

    # --- с подписью
    def get(self, path: str, params: dict | None = None):
        return self._request("GET", path, params, signed=True)

    def post(self, path: str, params: dict | None = None):
        return self._request("POST", path, params, signed=True)

    def place(self, **kw) -> dict:
        return self.post("/v5/order/create", dict(category="linear", **kw))

    def amend(self, **kw) -> dict:
        return self.post("/v5/order/amend", dict(category="linear", **kw))

    def cancel(self, symbol: str, orderLinkId: str) -> dict:
        return self.post("/v5/order/cancel", dict(category="linear", symbol=symbol, orderLinkId=orderLinkId))

    def cancel_all(self, symbol: str | None = None) -> dict:
        return self.post("/v5/order/cancel-all", dict(category="linear", symbol=symbol, settleCoin=None if symbol else "USDT"))

    def open_orders(self, symbol: str | None = None) -> list[dict]:
        return self.get("/v5/order/realtime", dict(category="linear", symbol=symbol, settleCoin=None if symbol else "USDT", limit=50)).get("list", [])

    def positions(self) -> list[dict]:
        return self.get("/v5/position/list", dict(category="linear", settleCoin="USDT", limit=200)).get("list", [])

    def executions(self, symbol: str | None = None, start_ms: int | None = None) -> list[dict]:
        return self.get("/v5/execution/list", dict(category="linear", symbol=symbol, startTime=start_ms, limit=100)).get("list", [])

    def wallet(self) -> dict:
        r = self.get("/v5/account/wallet-balance", dict(accountType="UNIFIED")).get("list", [])
        return r[0] if r else {}

    def set_leverage(self, symbol: str, lev: str = "1"):
        try:
            return self.post("/v5/position/set-leverage", dict(category="linear", symbol=symbol, buyLeverage=lev, sellLeverage=lev))
        except BybitError as e:
            if e.code == 110043:                               # плечо уже такое
                return {}
            raise


class Stream:
    """WebSocket с переподключением и пингом раз в 20 с. on_message(dict) — корутина."""

    def __init__(self, url: str, topics: list[str], on_message, auth: tuple[str, str] | None = None, name: str = "ws", on_error=None):
        self.url = url; self.topics = list(topics); self.on_message = on_message; self.auth = auth; self.name = name
        self.on_error = on_error                                # ошибка в обработчике сообщения — в журнал бота, связь не рвём
        self.ws = None; self.connected = asyncio.Event(); self._stop = False
        self.last_msg = time.time()

    async def subscribe(self, topics: list[str]):
        new = [t for t in topics if t not in self.topics]
        self.topics += new
        if self.ws is not None and new:
            for i in range(0, len(new), 10):
                await self.ws.send(json.dumps({"op": "subscribe", "args": new[i:i + 10]}))

    async def unsubscribe(self, topics: list[str]):
        old = [t for t in topics if t in self.topics]
        self.topics = [t for t in self.topics if t not in old]
        if self.ws is not None and old:
            await self.ws.send(json.dumps({"op": "unsubscribe", "args": old}))

    async def _ping(self, ws):
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"op": "ping"}))

    async def run(self):
        delay = 1
        while not self._stop:
            try:
                async with websockets.connect(self.url, ping_interval=None, max_size=2 ** 22) as ws:
                    self.ws = ws
                    if self.auth:
                        key, secret = self.auth
                        expires = int((time.time() + 10) * 1000)
                        sig = hmac.new(secret.encode(), f"GET/realtime{expires}".encode(), hashlib.sha256).hexdigest()
                        await ws.send(json.dumps({"op": "auth", "args": [key, expires, sig]}))
                    for i in range(0, len(self.topics), 10):
                        await ws.send(json.dumps({"op": "subscribe", "args": self.topics[i:i + 10]}))
                    self.connected.set(); delay = 1
                    log.info("%s: подключено, тем %d", self.name, len(self.topics))
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        async for raw in ws:
                            self.last_msg = time.time()
                            msg = json.loads(raw)
                            if msg.get("op") in ("pong", "ping") or msg.get("ret_msg") == "pong":
                                continue
                            if msg.get("op") == "auth" and not msg.get("success", False):
                                log.error("%s: ключ не принят: %s", self.name, msg)
                            if msg.get("op") == "subscribe" and not msg.get("success", True):
                                log.error("%s: подписка не прошла: %s", self.name, msg)
                            try:
                                await self.on_message(msg)
                            except Exception as e:
                                log.exception("%s: ошибка обработки сообщения", self.name)
                                if self.on_error:
                                    self.on_error(self.name, e)
                    finally:
                        pinger.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:                             # обрыв — переподключаемся
                log.warning("%s: обрыв (%s), переподключение через %d с", self.name, e, delay)
            self.ws = None; self.connected.clear()
            await asyncio.sleep(delay); delay = min(delay * 2, 60)

    def stop(self):
        self._stop = True
