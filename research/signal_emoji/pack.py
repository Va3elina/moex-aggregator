"""Пак своих эмодзи шкалы силы сигналов: t.me/addemoji/frame_signals_by_framesignalbot (запуск на сервере, от root).

    python3 pack.py create   создать пак из out/ (make_emoji.py), владелец — Вадим (ADMIN_CHAT_ID); потом sync
    python3 pack.py add      дописать в пак значки, которых в нём ещё нет (по порядку ITEMS)
    python3 pack.py sync     номера значков → ids.json (имя → custom_emoji_id), сопоставление по порядку в паке
    python3 pack.py test     одно сообщение Вадиму: все 7 уровней шкалы и пример рекорда

Токен и чат — из /opt/frame/.env (ALERT_BOT_TOKEN, ADMIN_CHAT_ID, TELEGRAM_API_ROOT — релей).
"""
from __future__ import annotations

import json
import pathlib
import sys
import time
import urllib.error
import urllib.request
import uuid

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out"
IDS = HERE / "ids.json"
SLUG, TITLE = "frame_signals", "FRAME сигналы"
UA = "Mozilla/5.0 (compatible; frame-robots/1.0)"
# порядок в паке = порядок здесь; запасной эмодзи — то, что видно в пуш-уведомлении и без Premium
# Вадим 18.11: три ступени — «вау» (за всё время) горящий огонь, «сильно» (2–5 лет) переливающийся кружок,
# «для понимания» (3 мес–год) без значка
ITEMS = [("pulse", "wave0.webm", "video", "🟠"), ("fire", "fire.webm", "video", "🔥"),
         # значки кнопок под сигналом (Вадим 18.11): make_buttons.py → out_buttons/
         ("gear", "../out_buttons/gear.webm", "video", "⚙️"), ("chart", "../out_buttons/chart.webm", "video", "📈")]


def env() -> dict:
    out = {}
    for line in pathlib.Path("/opt/frame/.env").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip("\r").strip('"')
    return {"tok": out["ALERT_BOT_TOKEN"], "chat": int(out["ADMIN_CHAT_ID"].split(",")[0]),   # первый — Вадим
            "root": out.get("TELEGRAM_API_ROOT") or "https://api.telegram.org"}


def api(method: str, fields: dict | None = None, files: dict | None = None) -> dict:
    c = env()
    fields = fields or {}
    if files:
        b = uuid.uuid4().hex
        parts = []
        for k, v in fields.items():
            v = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else str(v)
            parts += [f'--{b}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n'.encode(), v.encode(), b"\r\n"]
        for k, (fn, data, ctype) in files.items():
            parts += [f'--{b}\r\nContent-Disposition: form-data; name="{k}"; filename="{fn}"\r\nContent-Type: {ctype}\r\n\r\n'.encode(),
                      data, b"\r\n"]
        parts.append(f"--{b}--\r\n".encode())
        body, ctype = b"".join(parts), f"multipart/form-data; boundary={b}"
    else:
        body, ctype = json.dumps(fields, ensure_ascii=False).encode(), "application/json"
    err = None
    for attempt in range(5):
        req = urllib.request.Request(f"{c['root']}/bot{c['tok']}/{method}", data=body,
                                     headers={"Content-Type": ctype, "User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                out = json.load(r)
            break
        except urllib.error.HTTPError as e:
            if e.code >= 500 or e.code == 429:
                err = e
            else:
                out = json.loads(e.read() or b"{}")
                break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        time.sleep((3, 6, 10, 20, 0)[attempt])
    else:
        raise SystemExit(f"{method}: не дошло: {err}")
    if not out.get("ok"):
        raise SystemExit(f"{method}: {out.get('description') or out}")
    return out["result"]


def set_name() -> str:
    return f"{SLUG}_by_{api('getMe')['username']}"


def create() -> None:
    stickers, files = [], {}
    for i, (name, fn, fmt, emoji) in enumerate(ITEMS):
        key = f"f{i:02d}"
        stickers.append(dict(sticker=f"attach://{key}", format=fmt, emoji_list=[emoji], keywords=[name]))
        files[key] = (fn, (OUT / fn).read_bytes(), "video/webm" if fmt == "video" else "image/png")
    name = set_name()
    try:
        api("createNewStickerSet", dict(user_id=env()["chat"], name=name, title=TITLE, sticker_type="custom_emoji",
                                        stickers=stickers), files)
        print(f"пак создан: t.me/addemoji/{name}")
    except SystemExit as e:
        if "occupied" not in str(e).lower():
            raise
        print(f"пак уже есть: t.me/addemoji/{name}")
    sync()


def add() -> None:
    have = len(api("getStickerSet", dict(name=set_name()))["stickers"])
    for name, fn, fmt, emoji in ITEMS[have:]:
        sticker = dict(sticker="attach://f", format=fmt, emoji_list=[emoji], keywords=[name])
        api("addStickerToSet", dict(user_id=env()["chat"], name=set_name(), sticker=sticker),
            {"f": (pathlib.Path(fn).name, (OUT / fn).read_bytes(), "video/webm" if fmt == "video" else "image/png")})
        print("добавлен", name)
        time.sleep(1)
    sync()


def sync() -> None:
    got = api("getStickerSet", dict(name=set_name()))["stickers"]
    if len(got) != len(ITEMS):
        raise SystemExit(f"в паке {len(got)} значков, ждали {len(ITEMS)}")
    ids = {name: {"id": s["custom_emoji_id"], "emoji": emoji} for (name, *_, emoji), s in zip(ITEMS, got)}
    IDS.write_text(json.dumps(ids, ensure_ascii=False, indent=1))
    print(json.dumps(ids, ensure_ascii=False))


def mark(window: str) -> str:
    """Значок ступени по окну рекорда."""
    ids = json.loads(IDS.read_text())
    e = lambda n: f'<tg-emoji emoji-id="{ids[n]["id"]}">{ids[n]["emoji"]}</tg-emoji>'
    if window == "за всё время":
        return e("fire") + " "
    if window in ("за 2 года", "за 3 года", "за 5 лет"):
        return e("pulse") + " "
    return ""


def test() -> None:
    chat = env()["chat"]
    send = lambda t: api("sendMessage", dict(chat_id=chat, text=t, parse_mode="HTML", disable_notification=True,
                                             link_preview_options={"is_disabled": True}))
    send(f"Пробное: три ступени рекордов\n\n{mark('за всё время')}<b>Вау</b> — за всё время\n"
         f"{mark('за 3 года')}<b>Сильно</b> — 2, 3, 5 лет\n<b>Для понимания</b> — 3 месяца, полгода, год, без значка и без звука")
    for name, leg, w in (("Роснефть", "Чистая позиция по объёму · лонг", "за всё время"),
                         ("Серебро", "Чистая позиция по объёму · лонг", "за 3 года"),
                         ("CNY/RUB", "Чистая позиция по объёму · лонг", "за 3 месяца")):
        send(f"{mark(w)}<b>{name}</b>\n{leg}\nРекорд {w}")
        time.sleep(1.1)
    print("отправлено 4")


if __name__ == "__main__":
    {"create": create, "add": add, "sync": sync, "test": test}[sys.argv[1]]()
