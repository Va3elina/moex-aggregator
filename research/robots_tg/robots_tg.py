"""Единые сообщения роботов в Telegram (бот сайта @framesignalbot → личный чат Вадима).

Формат у всех роботов одинаковый:
  первая строка  — рынок · робот · событие [· итог]
  дальше         — сделки: цветной значок, бумага, цена; строка «причина»; цель, если есть
  последняя      — «с начала»: сколько робот заработал всего
Отправка — как у алертов сайта: токен ALERT_BOT_TOKEN, чат ADMIN_CHAT_ID и релей TELEGRAM_API_ROOT из /opt/frame/.env
(с сервера Telegram напрямую закрыт). Только стандартная библиотека — модуль подключают роботы с разными окружениями.
"""
from __future__ import annotations

import html
import json
import pathlib
import time
import urllib.error
import urllib.request

ENV = pathlib.Path("/opt/frame/.env")
MARKET = {"рф": "🇷🇺", "крипта": "🪙"}

# Значки — свой пак эмодзи бота (рисунки: emoji/make_icons.py, загрузка: emoji_pack.py). Здесь — запасной обычный эмодзи:
# он же виден в пушах и списке чатов, и он же идёт в сообщение, пока пака нет или у владельца бота кончился Premium.
ICONS = {
    "вход": "🔵", "прибыль": "🟢", "убыток": "🔴", "нет сделок": "⚪️", "итоги": "📊", "покупка": "↗️", "продажа": "↘️",
    "цель": "🎯", "таймер": "⏱️", "стоп": "⏹️", "сбой": "⚠️", "внимание": "ℹ️",
    **{f"лесенка {k}": f"{k}\ufe0f\u20e3" for k in range(7)},       # 0️⃣ … 6️⃣
}
EMOJI_IDS = pathlib.Path(__file__).resolve().parent / "state" / "emoji_ids.json"   # имя → номер своего эмодзи (emoji_pack.py sync)
_ids: dict = {"mtime": None, "map": {}}


def _cfg() -> dict | None:
    if not ENV.exists():
        return None
    kv = {}
    for line in ENV.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1); kv[k.strip()] = v.strip().strip('"').strip("'")
    if not kv.get("ALERT_BOT_TOKEN") or not kv.get("ADMIN_CHAT_ID"):
        return None
    return dict(tok=kv["ALERT_BOT_TOKEN"], chat=kv["ADMIN_CHAT_ID"], root=kv.get("TELEGRAM_API_ROOT", "https://api.telegram.org").rstrip("/"))


def _call(method: str, payload: dict) -> dict | None:
    c = _cfg()
    if c is None:
        print("[tg] нет /opt/frame/.env — сообщение не отправлено:\n" + payload.get("text", ""), flush=True)
        return None
    data = json.dumps({"chat_id": c["chat"], **payload}).encode()
    headers = {"Content-Type": "application/json",
               # Cloudflare-релей режет стандартный «Python-urllib» (ошибка 1010) — представляемся явно
               "User-Agent": "Mozilla/5.0 (compatible; frame-robots/1.0)"}
    for attempt in range(4):                                  # связь сервер → релей иногда рвётся на рукопожатии
        try:
            req = urllib.request.Request(f"{c['root']}/bot{c['tok']}/{method}", data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                out = json.load(r)
            if not out.get("ok"):
                print(f"[tg] {method} не принят: {out}", flush=True)
            return out
        except urllib.error.HTTPError as e:
            if e.code < 500 and e.code != 429:                # ошибка в самом запросе — повтор не поможет
                print(f"[tg] {method} отклонён: {e.code} {e.read()[:200]!r}", flush=True)
                return None
            err = e
        except Exception as e:                                # сеть: таймаут, обрыв рукопожатия
            err = e
        time.sleep((2, 5, 10, 0)[attempt])
    print(f"[tg] {method} не ушёл после повторов: {err}", flush=True)   # сообщение — не повод ронять робота
    return None


def send(text: str, reply_to: int | None = None, silent: bool = False) -> int | None:
    """Отправить сообщение (HTML). silent — без звука. Вернёт номер сообщения — чтобы поправить его или ответить на него."""
    p = dict(text=text[:4000], parse_mode="HTML", disable_web_page_preview=True, disable_notification=silent)
    if reply_to:
        p["reply_parameters"] = {"message_id": reply_to, "allow_sending_without_reply": True}
    out = _call("sendMessage", p)
    return (out or {}).get("result", {}).get("message_id")


def edit(message_id: int, text: str) -> bool:
    if not message_id:
        return False
    out = _call("editMessageText", dict(message_id=message_id, text=text[:4000], parse_mode="HTML", disable_web_page_preview=True))
    return bool(out and out.get("ok"))


# ---------- оформление
def esc(s) -> str:
    return html.escape(str(s), quote=False)


def num(x: float, nd: int = 0) -> str:
    """12 345,6 — пробел между тысячами, запятая в дробях."""
    s = f"{abs(x):,.{nd}f}".replace(",", " ").replace(".", ",")
    return ("−" if x < 0 else "") + s


def signed(x: float, nd: int = 0) -> str:
    return ("+" if x > 0 else "") + num(x, nd) if x >= 0 else num(x, nd)


def rub(x: float, sign: bool = True) -> str:
    return (signed(x) if sign else num(x)) + " ₽"


def usd(x: float, sign: bool = True, nd: int = 2) -> str:
    v = signed(x, nd) if sign else num(x, nd)
    return (v[0] + "$" + v[1:]) if v[:1] in "+−" else "$" + v


def pct(x: float, nd: int = 2) -> str:
    return signed(x, nd) + "%"


def price(x: float) -> str:
    """Цена без лишних нулей: 85 545 · 12,807 · 0,09624."""
    if abs(x) >= 1000:
        return num(x, 0) if float(x).is_integer() else num(x, 2)
    s = f"{x:.6g}"
    return s.replace(".", ",")


def price_like(x: float, ref: float) -> str:
    """Цена с тем же числом знаков после запятой, что у образца (средняя — как цена входа)."""
    r = price(ref)
    d = len(r.split(",")[1]) if "," in r else 0
    return num(x, d)


def plural(n: int, one: str, few: str, many: str) -> str:
    return f"{n} " + (one if n % 10 == 1 and n % 100 != 11 else few if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14 else many)


def _emoji_ids() -> dict:
    try:
        m = EMOJI_IDS.stat().st_mtime
    except OSError:
        return {}
    if m != _ids["mtime"]:                                    # перечитываем, только если пак обновили
        try:
            _ids["map"] = {k: str(v["id"]) for k, v in json.loads(EMOJI_IDS.read_text()).items() if v.get("id")}
        except Exception:
            _ids["map"] = {}
        _ids["mtime"] = m
    return _ids["map"]


def icon(name: str) -> str:
    """Значок: свой эмодзи из пака бота, а если его нет — обычный эмодзи (для HTML-сообщений)."""
    fb = ICONS[name]
    cid = _emoji_ids().get(name)
    return f'<tg-emoji emoji-id="{cid}">{fb}</tg-emoji>' if cid else fb


def dot(x: float) -> str:
    return icon("прибыль") if x > 0 else icon("убыток") if x < 0 else icon("нет сделок")


def head(market: str, robot: str, event: str, result: str = "") -> str:
    return f"{MARKET.get(market, '')} <b>{esc(robot)}</b> · {esc(event)}" + (f" · {result}" if result else "")


def since(start: str, total: str, n: int, extra: str = "") -> str:
    word = "сделка" if n % 10 == 1 and n % 100 != 11 else "сделки" if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14 else "сделок"
    return f"💰 с начала {start}: <b>{total}</b> · {n} {word}" + (f" · {extra}" if extra else "")


REASON = "      <i>причина: {}</i>"
