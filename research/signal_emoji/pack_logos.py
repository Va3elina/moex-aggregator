"""Пак логотипов активов: t.me/addemoji/frame_logos_by_framesignalbot (запуск на сервере из /opt/signal-emoji, от root).

    python3 pack_logos.py upload   создать пак из out_logos/ (make_logos.py) и дописать недостающие; потом ids
    python3 pack_logos.py ids      out_logos/map.json + номера значков пака → logo_ids.json (ключ актива → custom_emoji_id)

Значки сопоставляются с картинками по порядку загрузки (order.json): пак отдаёт значки в порядке добавления.
Пак — до 200 значков; createNewStickerSet принимает до 50, остальные — addStickerToSet по одному.
"""
from __future__ import annotations

import json
import pathlib
import sys
import time

from pack import api, env

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out_logos"
IDS = HERE / "logo_ids.json"
SLUG, TITLE = "frame_logos", "FRAME логотипы"
FALLBACK = "▫️"                     # запасной эмодзи: виден в пуше и без Premium


def set_name() -> str:
    return f"{SLUG}_by_{api('getMe')['username']}"


def sticker(h: str) -> tuple[dict, dict]:
    key = f"f{h}"
    return (dict(sticker=f"attach://{key}", format="static", emoji_list=[FALLBACK], keywords=[h]),
            {key: (f"{h}.png", (OUT / f"{h}.png").read_bytes(), "image/png")})


def upload() -> None:
    hashes = sorted(p.stem for p in OUT.glob("*.png") if p.stem not in ("sheet",))
    name = set_name()
    owner = env()["chat"]
    first, rest = hashes[:50], hashes[50:]
    try:
        api("getStickerSet", dict(name=name))
        exists = True
    except SystemExit:
        exists = False
    if not exists:
        sts, files = [], {}
        for h in first:
            st, f = sticker(h)
            sts.append(st); files.update(f)
        api("createNewStickerSet", dict(user_id=owner, name=name, title=TITLE, sticker_type="custom_emoji", stickers=sts), files)
        print(f"пак создан: t.me/addemoji/{name}, первые {len(first)}")
        todo = rest
    else:
        n = len(api("getStickerSet", dict(name=name))["stickers"])
        todo = hashes[n:]                                  # дописываем в том же порядке, что и создавали
        print(f"пак уже есть, в нём {n}, дописать {len(todo)}")
    for i, h in enumerate(todo, 1):
        st, f = sticker(h)
        api("addStickerToSet", dict(user_id=owner, name=name, sticker=st), f)
        if i % 20 == 0:
            print(f"  +{i}", flush=True)
        time.sleep(0.4)
    (HERE / "logo_order.json").write_text(json.dumps(hashes))
    ids()


def ids() -> None:
    order = json.loads((HERE / "logo_order.json").read_text())
    got = api("getStickerSet", dict(name=set_name()))["stickers"]
    if len(got) != len(order):
        raise SystemExit(f"в паке {len(got)}, картинок {len(order)} — порядок не сходится")
    by_hash = {h: s["custom_emoji_id"] for h, s in zip(order, got)}
    mapping = json.loads((OUT / "map.json").read_text())
    out = {k: by_hash[h] for k, h in mapping.items()}
    IDS.write_text(json.dumps(out, ensure_ascii=False, indent=0, sort_keys=True))
    print(f"logo_ids.json: {len(out)} активов, {len(by_hash)} значков")


if __name__ == "__main__":
    {"upload": upload, "ids": ids}[sys.argv[1]]()
