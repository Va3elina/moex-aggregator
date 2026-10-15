"""Значки фондов для сообщений о справках СЧА: логотипы УК и фото авторов с сайта → PNG 100×100
для пака эмодзи бота (как значки роботов, emoji_pack.py add). Источник — frontend/public/uk-logos и
frontend/public/authors (те же картинки, что на сайте, fundConfig.ts: UK_LOGOS / AUTHOR_LOGOS).

    python3 research/robots_tg/emoji/make_fund_icons.py   # → png/fund_*.png + строки в icons.json
"""
import json
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw

HERE = Path(__file__).resolve().parent
PUB = HERE.parents[2] / "frontend" / "public"
SIZE = 100

# имя значка → (картинка сайта, запасной обычный эмодзи, круглая обрезка)
ICONS = {
    "ук т-капитал": ("uk-logos/tcap.png", "🟡", False),
    "ук альфа": ("uk-logos/alfa.png", "🔴", False),
    "ук первая": ("uk-logos/pervaya.png", "🟢", False),
    "ук вим": ("uk-logos/vim.png", "🔵", False),
    "ук атон": ("uk-logos/aton.png", "🔷", False),
    "ук рекорд": ("uk-logos/alenka.svg", "🟦", False),
    "ук герои": ("uk-logos/geroi.png", "🟣", False),
    "автор коган": ("authors/kogan.png", "👤", True),
    "автор воробьёв": ("authors/vorobiev.png", "👤", True),
    "автор щетинин": ("authors/shchetinin.png", "👤", True),
    "автор кудрицкий": ("authors/vasilich.png", "👤", True),
    "автор крейнин": ("authors/kreinin.png", "👤", True),
    "автор шимко": ("authors/shimko.png", "👤", True),
    "автор линецкий": ("authors/linecki.png", "👤", True),
}


def load(src: Path) -> Image.Image:
    if src.suffix == ".svg":
        tmp = Path(tempfile.mkdtemp()) / "logo.png"
        subprocess.run(["rsvg-convert", "-w", "400", "-o", str(tmp), str(src)], check=True)
        src = tmp
    return Image.open(src).convert("RGBA")


def make(src: Path, circle: bool) -> Image.Image:
    im = load(src)
    if circle:   # фото: квадрат по центру → круг
        s = min(im.size)
        im = im.crop(((im.width - s) // 2, (im.height - s) // 2, (im.width + s) // 2, (im.height + s) // 2))
        im = im.resize((SIZE, SIZE), Image.LANCZOS)
        mask = Image.new("L", (SIZE, SIZE), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, SIZE - 1, SIZE - 1), fill=255)
        im.putalpha(mask)
        return im
    im.thumbnail((SIZE - 8, SIZE - 8), Image.LANCZOS)   # логотип: вписать с полями, фон прозрачный
    out = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    out.paste(im, ((SIZE - im.width) // 2, (SIZE - im.height) // 2), im)
    return out


def main() -> None:
    icons = json.loads((HERE / "icons.json").read_text())
    for name, (rel, emoji, circle) in ICONS.items():
        fname = "fund_" + rel.split("/")[-1].rsplit(".", 1)[0] + ".png"
        make(PUB / rel, circle).save(HERE / "png" / fname, optimize=True)
        icons[name] = {"file": f"png/{fname}", "emoji": emoji}
        print(name, "→", fname, (HERE / "png" / fname).stat().st_size, "байт")
    (HERE / "icons.json").write_text(json.dumps(icons, ensure_ascii=False, indent=1) + "\n")


if __name__ == "__main__":
    main()
