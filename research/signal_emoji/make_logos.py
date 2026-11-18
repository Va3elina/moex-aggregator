"""Логотипы активов как свои эмодзи для сигналов @framesignalbot (Вадим 18.11.2026).

Логотип выбирается как на сайте (frontend/src/components/InstrumentIcon.tsx через research/content_cards/lib/logos.py):
акция — по тикеру, фьючерс — через базу контракта, у валют/индексов/сырья без картинки — цветная плашка с подписью.
Рисуется как TickerLogo: картинка целиком (contain) в круге; плашка — круг цвета bg с жирной подписью.

Вход — assets.json (списки с прода: фьючерсы открытого интереса за год и активы сезонности, см. pack_logos.py).
Выход — out_logos/<hash>.png (100×100, одинаковые картинки — один файл), out_logos/map.json: ключ → hash,
ключи «oi:<sectype>» и «se:<код сезонности>», и out_logos/sheet.png — лист для проверки глазами.

    python research/signal_emoji/make_logos.py assets.json
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import shutil
import sys

from PIL import Image, ImageDraw, ImageFont

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "content_cards" / "lib"))
from logos import fut_logo, logo_path  # noqa: E402

OUT = HERE / "out_logos"
SIZE, SS = 100, 4
FONT = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
FONT_SYM = "/System/Library/Fonts/Supplemental/Arial Unicode.ttf"
# у кода сезонности нет своей картинки на сайте — берём логотип фьючерса на тот же актив
SEASON_ALIAS = {"CNYRUB_TOM": "CR", "EUR_RUB__TOM": "Eu", "GLDRUB_TOM": "GL"}


def circle_mask() -> Image.Image:
    m = Image.new("L", (SIZE * SS, SIZE * SS), 0)
    ImageDraw.Draw(m).ellipse((0, 0, SIZE * SS - 1, SIZE * SS - 1), fill=255)
    return m.resize((SIZE, SIZE), Image.LANCZOS)


def from_image(p: pathlib.Path) -> Image.Image:
    src = Image.open(p).convert("RGBA")
    k = SIZE * SS / max(src.size)                          # вписать целиком, в том числе увеличить мелкие (80 px)
    src = src.resize((max(1, round(src.width * k)), max(1, round(src.height * k))), Image.LANCZOS)
    big = Image.new("RGBA", (SIZE * SS, SIZE * SS), (0, 0, 0, 0))
    big.alpha_composite(src, ((SIZE * SS - src.width) // 2, (SIZE * SS - src.height) // 2))
    im = big.resize((SIZE, SIZE), Image.LANCZOS)
    out = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    out.paste(im, (0, 0), Image.composite(im, Image.new("RGBA", im.size), circle_mask()))
    return out


def badge(text: str, bg: str) -> Image.Image:
    text = {"₹": "INR"}.get(text, text)                 # знака рупии нет ни в одном системном шрифте
    big = Image.new("RGBA", (SIZE * SS, SIZE * SS), (0, 0, 0, 0))
    d = ImageDraw.Draw(big)
    d.ellipse((0, 0, SIZE * SS - 1, SIZE * SS - 1), fill=bg)
    size = {1: 200, 2: 160, 3: 124}.get(len(text), 100)
    # в Arial Bold нет ₹ ₿ Ξ σ — такие подписи берут Arial Unicode
    f = ImageFont.truetype(FONT_SYM if any(c in "₹₿Ξσ" for c in text) else FONT, size)
    d.text((SIZE * SS / 2, SIZE * SS / 2), text, font=f, fill="white", anchor="mm")
    return big.resize((SIZE, SIZE), Image.LANCZOS)


def resolve(kind: str, code: str):
    if kind == "se":
        if code in SEASON_ALIAS:
            return fut_logo(SEASON_ALIAS[code])
        p = logo_path(code)
        return ("img", p) if p.exists() else fut_logo(code)
    return fut_logo(code)


def main(assets: str) -> None:
    a = json.loads(pathlib.Path(assets).read_text())
    items = [("oi", x["sec"], x["name"]) for x in a["oi"]] + [("se", x["code"], x["name"]) for x in a["season"]]
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir()
    mapping, names, missing = {}, {}, []
    for kind, code, name in items:
        r = resolve(kind, code)
        if r is None or (r[0] == "img" and not r[1].exists()):
            missing.append(f"{kind}:{code} {name}")
            continue
        im = from_image(r[1]) if r[0] == "img" else badge(*r[1])
        b = im.tobytes()
        h = hashlib.md5(b).hexdigest()[:12]
        if not (OUT / f"{h}.png").exists():
            im.save(OUT / f"{h}.png")
        mapping[f"{kind}:{code}"] = h
        names.setdefault(h, []).append(name)
    (OUT / "map.json").write_text(json.dumps(mapping, ensure_ascii=False, indent=1))
    (OUT / "names.json").write_text(json.dumps(names, ensure_ascii=False, indent=1))
    # лист проверки: значок + первое имя, на фоне тёмной темы Telegram
    hs = sorted(names, key=lambda h: names[h][0])
    cols, cw, ch = 8, 150, 92
    sheet = Image.new("RGBA", (cols * cw, (len(hs) + cols - 1) // cols * ch + 10), (23, 33, 43, 255))
    d = ImageDraw.Draw(sheet)
    f = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 13)
    for i, h in enumerate(hs):
        x, y = i % cols * cw, i // cols * ch + 6
        sheet.alpha_composite(Image.open(OUT / f"{h}.png").resize((56, 56), Image.LANCZOS), (x + (cw - 56) // 2, y))
        d.text((x + cw / 2, y + 70), names[h][0][:20], font=f, fill=(210, 220, 230), anchor="mm")
    sheet.save(OUT / "sheet.png")
    print(f"активов {len(mapping)}, значков {len(names)}, без логотипа {len(missing)}: {missing}")


if __name__ == "__main__":
    main(sys.argv[1])
