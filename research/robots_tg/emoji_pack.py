"""Свой пак эмодзи для уведомлений роботов (запуск на сервере из /opt/robot-notify, от root).

  python3 emoji_pack.py create   создать пак: владелец — Вадим (ADMIN_CHAT_ID), имя <frame_robots>_by_<бот>; потом sync
  python3 emoji_pack.py sync     номера значков из пака → state/emoji_ids.json (robots_tg.icon подхватит сам)
  python3 emoji_pack.py add      дорисованные значки (есть в emoji/icons.json, нет в паке) — дописать в пак; потом sync
  python3 emoji_pack.py test     одно тихое сообщение со всеми значками — проверить, что бот может их слать

Рисунки — emoji/make_icons.py (PNG 100×100, прозрачный фон). Слать свои эмодзи бот может, пока у владельца бота есть
Telegram Premium; нет — в сообщениях остаются запасные обычные эмодзи, ничего не ломается.
Пак нельзя найти поиском, но добавить его может любой, у кого есть ссылка t.me/addemoji/<имя>.
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
sys.path.insert(0, str(HERE))
import robots_tg as TG  # noqa: E402

META = HERE / "emoji" / "icons.json"
SLUG = "frame_robots"
TITLE = "Frame роботы"
UA = "Mozilla/5.0 (compatible; frame-robots/1.0)"


def api(method: str, fields: dict | None = None, files: dict | None = None) -> dict:
    """Запрос к Bot API через релей. files — {поле: (имя файла, байты)} → multipart."""
    c = TG._cfg()
    if c is None:
        raise SystemExit("нет /opt/frame/.env с ALERT_BOT_TOKEN и ADMIN_CHAT_ID")
    fields = fields or {}
    if files:
        b = uuid.uuid4().hex
        parts = []
        for k, v in fields.items():
            v = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else str(v)
            parts += [f'--{b}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n'.encode(), v.encode(), b"\r\n"]
        for k, (fn, data) in files.items():
            parts += [f'--{b}\r\nContent-Disposition: form-data; name="{k}"; filename="{fn}"\r\nContent-Type: image/png\r\n\r\n'.encode(),
                      data, b"\r\n"]
        parts.append(f"--{b}--\r\n".encode())
        body, ctype = b"".join(parts), f"multipart/form-data; boundary={b}"
    else:
        body, ctype = json.dumps(fields, ensure_ascii=False).encode(), "application/json"
    for attempt in range(5):                              # связь сервер → релей иногда рвётся на рукопожатии
        req = urllib.request.Request(f"{c['root']}/bot{c['tok']}/{method}", data=body, headers={"Content-Type": ctype, "User-Agent": UA})
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
        print(f"  {method}: связь ({err}), повтор", flush=True)
        time.sleep((3, 6, 10, 20, 0)[attempt])
    else:
        raise SystemExit(f"{method}: не дошло после повторов: {err}")
    if not out.get("ok"):
        raise SystemExit(f"{method}: {out.get('description') or out}")
    return out["result"]


def set_name() -> str:
    return f"{SLUG}_by_{api('getMe')['username']}"


def owner() -> int:
    return int(TG._cfg()["chat"])


def input_sticker(i: int, name: str, m: dict) -> tuple[dict, tuple[str, bytes]]:
    key = f"f{i:02d}"
    st = dict(sticker=f"attach://{key}", format="static", emoji_list=[m["emoji"]], keywords=name.split()[:20])
    return st, (f"{key}.png", (HERE / "emoji" / m["file"]).read_bytes())


def create():
    meta = json.loads(META.read_text())
    stickers, files = [], {}
    for i, (name, m) in enumerate(meta.items()):
        st, f = input_sticker(i, name, m)
        stickers.append(st); files[st["sticker"].split("//")[1]] = f
    name = set_name()
    try:
        api("createNewStickerSet", dict(user_id=owner(), name=name, title=TITLE, sticker_type="custom_emoji", stickers=stickers), files)
        print(f"пак создан: t.me/addemoji/{name}, значков {len(stickers)}")
    except SystemExit as e:                                # ответ потерялся, а пак создался — повтор скажет «имя занято»
        if "occupied" not in str(e).lower():
            raise
        print(f"пак уже есть: t.me/addemoji/{name}")
    sync()


def _norm(e: str) -> str:
    return e.replace("️", "")


def sync():
    """Сопоставляем значки пака с именами по запасному эмодзи (у каждого свой), а не по порядку."""
    meta = json.loads(META.read_text())
    by_emoji = {_norm(m["emoji"]): name for name, m in meta.items()}
    got = api("getStickerSet", dict(name=set_name()))["stickers"]
    ids = {}
    for s in got:
        name = by_emoji.get(_norm(s.get("emoji", "")))
        if name and s.get("custom_emoji_id"):
            ids[name] = dict(id=s["custom_emoji_id"], emoji=meta[name]["emoji"])
    TG.EMOJI_IDS.parent.mkdir(parents=True, exist_ok=True)
    tmp = TG.EMOJI_IDS.with_suffix(".tmp"); tmp.write_text(json.dumps(ids, ensure_ascii=False, indent=1)); tmp.replace(TG.EMOJI_IDS)
    miss = [n for n in meta if n not in ids]
    print(f"в паке {len(got)} значков, сопоставлено {len(ids)}" + (f"; нет в паке: {', '.join(miss)} (python3 emoji_pack.py add)" if miss else ""))


def add():
    meta = json.loads(META.read_text())
    have = {_norm(s.get("emoji", "")) for s in api("getStickerSet", dict(name=set_name()))["stickers"]}
    new = [(i, n, m) for i, (n, m) in enumerate(meta.items()) if _norm(m["emoji"]) not in have]
    for i, n, m in new:
        st, f = input_sticker(i, n, m)
        api("addStickerToSet", dict(user_id=owner(), name=set_name(), sticker=st), {st["sticker"].split("//")[1]: f})
        print(f"добавлен: {n}")
    if not new:
        print("всё уже в паке")
    sync()


def test():
    meta = json.loads(META.read_text())
    ids = json.loads(TG.EMOJI_IDS.read_text()) if TG.EMOJI_IDS.exists() else {}
    if not ids:
        raise SystemExit("нет state/emoji_ids.json — сначала create или sync")
    line = " ".join(TG.icon(n) for n in meta)
    out = TG._call("sendMessage", dict(text=f"{TG.icon('итоги')} <b>Значки роботов</b>\n{line}", parse_mode="HTML", disable_notification=True))
    ents = [e for e in ((out or {}).get("result", {}).get("entities") or []) if e.get("type") == "custom_emoji"]
    print(f"сообщение ушло, своих эмодзи в нём {len(ents)} из {len(meta) + 1}"
          + ("" if ents else " — Telegram их снял: у владельца бота нет Premium, останутся обычные эмодзи"))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"create": create, "sync": sync, "add": add, "test": test}.get(cmd, lambda: print(__doc__))()
