"""Картинка к рекорду позиции (Вадим 18.11.2026): график сайта с разметкой сигнала — движок frame-chart-render.

Данные — те же, что видит пользователь на /oi (GET /api/chart, физлица, день, вся история); точка рекорда из бота
дописывается последней, если на сайте её ещё нет (у гостя позиции с задержкой на день). Разметка по правилам канала
(research/channel_charts/ANNOTATION_RULES.md): крупная подпись цветом линии, короткая стрелка к точке, кружки на прежних
пиках, пунктир главного прежнего пика.

Период — от того, с чем сравниваем:
  • исторический рекорд → период «Всё», отмечены 1–2 главных прежних пика;
  • рекорд за N лет → самый короткий период страницы /oi (6м, 1г, 5л, Всё), в который попадает прошлый раз, когда
    было выше; он — главный прежний пик, внутри ещё один крупный, если есть.

Рисует docker-образ frame-chart-render (frontend/chart-render). Любой сбой → None, бот шлёт текст без картинки.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import tempfile

import requests

SITE_API = "https://framedata.ru/api/chart"
IMAGE = "frame-chart-render"
MS = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]
PRESETS = (("6м", 182), ("1г", 365), ("5л", 1825))          # периоды страницы открытых позиций сайта
MONTH_CODE = "FGHJKMNQUVXZ"                                   # буква месяца экспирации фьючерса
# показатель бота → режим графика сайта и сторона рекорда
LEG = {"netl": ("positions", 1), "nets": ("positions", -1), "ntl": ("participants", 1), "nts": ("participants", -1)}


def _series(sec: str, mode: str):
    url = f"{SITE_API}/{sec}"
    p = {"sectype": sec, "inst_type": "futures", "interval": 24, "clgroup": "FIZ", "show_oi": "true", "period": "all"}
    d = requests.get(url, params=p, timeout=30, headers={"User-Agent": "frame-signal-card"}).json()

    def val(o):
        if mode == "positions":
            return o["net_position"] if o.get("net_position") is not None else (o["pos_long"] or 0) + (o["pos_short"] or 0)
        return (o["pos_long_num"] or 0) - (o["pos_short_num"] or 0)

    oi = {o["time"][:10]: val(o) for o in d.get("open_interest") or []}
    price, line, last = [], [], None
    for c in d.get("candles") or []:                          # как у страницы /oi: позиция подтягивается к свечам
        v = oi.get(c["time"][:10])
        last = v if v is not None else last
        if last is not None:
            price.append({"time": c["time"], "value": c["close"]})
            line.append({"time": c["time"], "value": last})
    return price, line, d.get("contract_switches") or []


def front_ticker(sec: str, switches: list, day: dt.date) -> str:
    """Тикер ближнего контракта, как в шапке экспорта сайта (DRZ6); нет данных — код инструмента."""
    to = (switches[-1] or {}).get("to") if switches else None
    if not to or to[-1] not in MONTH_CODE:
        return sec
    m = MONTH_CODE.index(to[-1]) + 1
    year = day.year + (1 if m < day.month else 0)
    return f"{to}{year % 10}"


def _local_peaks(vals: list, lo: int, hi: int, k: int) -> list:
    """До k локальных пиков (максимум в окне ±40 баров) на [lo, hi), не ближе 60 баров друг к другу, сильные первыми."""
    c = [i for i in range(lo, max(lo, hi)) if vals[i] > 0 and vals[i] == max(vals[max(0, i - 40):i + 41])]
    c.sort(key=lambda i: -vals[i])
    out: list = []
    for i in c:
        if all(abs(i - j) >= 60 for j in out):
            out.append(i)
        if len(out) == k:
            break
    return out


def build(x: dict, value: float, day) -> dict | None:
    """Карточка для движка: x — рекорд из record_notify (sec, name, leg, window), value — значение показателя в точке
    рекорда (со знаком показателя: у шорта — величина шорта), day — день рекорда."""
    mode, sg = LEG[x["leg"]]
    day = dt.date.fromisoformat(str(day)[:10])
    price, line, switches = _series(x["sec"], mode)
    price = [p for p in price if p["time"][:10] <= day.isoformat()]
    line = [p for p in line if p["time"][:10] <= day.isoformat()]
    if len(line) < 60:
        return None
    last = line[-1]["value"]
    same = abs(last - sg * value) <= max(1.0, abs(value) * 0.005)
    # точки рекорда на сайте ещё нет — дописываем; если последняя точка сайта уже и есть рекорд (дата у сайта —
    # торговый день позиции, у бота — день среза), второй раз не дописываем, иначе «вчера было столько же»
    if line[-1]["time"][:10] < day.isoformat() and not same:
        line.append({"time": f"{day.isoformat()}T00:00:00", "value": sg * value})
        price.append({"time": f"{day.isoformat()}T00:00:00", "value": price[-1]["value"]})
    vals = [sg * p["value"] for p in line]
    v = vals[-1]
    higher = [i for i, y in enumerate(vals[:-1]) if y >= v]
    side = "лонг" if sg > 0 else "шорт"
    if not higher or x["window"] == "за всё время":
        period, start = "Всё", None
        peaks = _local_peaks(vals, 0, len(vals) - 20, 2)
        head = "исторический максимум" if sg > 0 else "исторический шорт"
    else:
        ref = higher[-1]                                      # прошлый раз, когда было выше
        need = (day - dt.date.fromisoformat(line[ref]["time"][:10])).days * 1.15 + 20
        period, days = next(((p, n) for p, n in PRESETS if n >= need), ("Всё", None))
        start = (day - dt.timedelta(days=days)).isoformat() if days else None
        peaks = [ref] + [i for i in _local_peaks(vals, ref + 1, len(vals) - 20, 1) if vals[i] >= 0.6 * v and i - ref >= 60]
        head = f"рекордный {side} {x['window']}"
    if start:
        off = next((i for i, p in enumerate(line) if p["time"][:10] >= start), 0)
        price, line, vals = price[off:], line[off:], vals[off:]
        peaks = [i - off for i in peaks if i >= off]
    lab = lambda i: f"{MS[int(line[i]['time'][5:7]) - 1]} {line[i]['time'][:4]}"  # noqa: E731
    title = "Число трейдеров" if mode == "participants" else "Объём позиций"
    return {
        "asset": x["name"], "lineLabel": "Чистая позиция", "price": price, "line": line,
        "meta": {"title": f"Открытые позиции — {title}", "asset": x["name"],
                 "ticker": front_ticker(x["sec"], switches, day), "details": ["1д", period, "Физлица"]},
        "signal": {"headline": head, "colorVar": "--oi-cyan", "dir": "high" if sg > 0 else "low",
                   "peaks": [{"date": line[i]["time"][:10], "value": sg * vals[i], "label": lab(i)} for i in peaks]},
    }


def render(card: dict, timeout: int = 120) -> bytes | None:
    """PNG картинки или None. Один короткоживущий контейнер с потолком памяти — прод не задевает."""
    with tempfile.TemporaryDirectory(prefix="sigcard-") as d:
        os.chmod(d, 0o777)
        with open(os.path.join(d, "card.json"), "w", encoding="utf-8") as fh:
            json.dump(card, fh, ensure_ascii=False)
        try:
            r = subprocess.run(["docker", "run", "--rm", "--memory", "700m", "--cpus", "1", "--network", "none",
                                "-v", f"{d}:/work", IMAGE, "/work/card.json", "/work/out.png"],
                               capture_output=True, text=True, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired) as e:
            print(f"[record_card] движок не ответил: {type(e).__name__}")
            return None
        out = os.path.join(d, "out.png")
        if r.returncode != 0 or not os.path.exists(out):
            print(f"[record_card] движок упал ({r.returncode}): {(r.stderr or '')[-400:]}")
            return None
        with open(out, "rb") as fh:
            return fh.read()


def picture(x: dict, value: float, day) -> bytes | None:
    try:
        card = build(x, value, day)
    except Exception as e:  # noqa: BLE001 — без картинки лучше, чем без сигнала
        print(f"[record_card] {x.get('sec')}: данные графика: {type(e).__name__}: {e}")
        return None
    return render(card) if card else None


if __name__ == "__main__":
    # проверка руками: python3 -m signals.record_card SEC LEG "за 2 года" VALUE YYYY-MM-DD out.png [название]
    import sys
    sec, leg, win, value, day, out = sys.argv[1:7]
    png = picture({"sec": sec, "name": sys.argv[7] if len(sys.argv) > 7 else sec, "leg": leg, "window": win},
                  float(value), day)
    if png:
        open(out, "wb").write(png)
    print("ok" if png else "нет картинки", out)
