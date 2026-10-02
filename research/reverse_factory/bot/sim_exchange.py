"""Имитация биржи для проверки исполнителя (bot/live.py) на исторических минутках.
Правила — как в движке: рыночная заявка исполняется по открытию следующей минутки; лимитная покупка — при касании
уровня, по цене уровня; цель — при касании, но не в той же минутке, где была докупка, и только если стояла до начала минутки.
"""
from __future__ import annotations

import pandas as pd


class SimExchange:
    def __init__(self):
        self.orders: dict[str, dict] = {}
        self.bar_t: pd.Timestamp | None = None          # минутка, внутри которой сейчас «живём» (заявки после её начала)
        self.log: list = []

    # --- интерфейс как у bybit.Rest
    def place(self, symbol, side, orderType, qty, orderLinkId, price=None, timeInForce=None, reduceOnly=False):
        self.orders[orderLinkId] = dict(sym=symbol, side=side, type=orderType, qty=float(qty), price=float(price) if price else None,
                                        reduce=reduceOnly, placed=self.bar_t, link=orderLinkId)
        self.log.append(("place", self.bar_t, orderLinkId, side, orderType, price, qty))
        return {"orderLinkId": orderLinkId}

    def amend(self, symbol, orderLinkId, qty=None, price=None):
        o = self.orders.get(orderLinkId)
        if o is None:
            raise RuntimeError("нет такой заявки")
        if qty is not None:
            o["qty"] = float(qty)
        if price is not None:
            o["price"] = float(price)
        o["placed"] = self.bar_t
        return {}

    def cancel(self, symbol, orderLinkId):
        self.orders.pop(orderLinkId, None)
        return {}

    def cancel_all(self, symbol=None):
        for k in [k for k, o in self.orders.items() if symbol is None or o["sym"] == symbol]:
            del self.orders[k]
        return {}

    # --- ход минутки: вернуть исполнения
    def run_bar(self, sym: str, t: pd.Timestamp, o: float, h: float, l: float, c: float) -> list[dict]:
        ex = []
        mine = [x for x in self.orders.values() if x["sym"] == sym and x["placed"] is not None and x["placed"] < t]
        for x in [x for x in mine if x["type"] == "Market"]:
            ex.append(dict(symbol=sym, orderLinkId=x["link"], execQty=x["qty"], execPrice=o, side=x["side"]))
            del self.orders[x["link"]]
        bought = False
        for x in sorted([x for x in mine if x["type"] == "Limit" and x["side"] == "Buy"], key=lambda x: -x["price"]):
            if l <= x["price"]:
                ex.append(dict(symbol=sym, orderLinkId=x["link"], execQty=x["qty"], execPrice=x["price"], side="Buy"))
                del self.orders[x["link"]]; bought = True
        if not bought:
            for x in [x for x in mine if x["type"] == "Limit" and x["side"] == "Sell" and x["link"] in self.orders]:
                if h >= x["price"]:
                    ex.append(dict(symbol=sym, orderLinkId=x["link"], execQty=x["qty"], execPrice=x["price"], side="Sell"))
                    del self.orders[x["link"]]
        return ex
