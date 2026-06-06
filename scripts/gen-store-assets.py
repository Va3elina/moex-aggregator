#!/usr/bin/env python3
"""Промо-графика для Chrome Web Store (и каталога Яндекса).

Генерит на editorial-палитре Фрейма:
  - tile-440x280.png    — малый промо-тайл (Small promo tile)
  - marquee-1400x560.png — широкий баннер (Marquee promo tile)
  - hero-1280x800.png   — пустой кадр-подложка под скриншот (рамка + лого),
                          поверх можно положить настоящий скрин панели.

Перегенерить: python3 scripts/gen-store-assets.py
"""
import os
from PIL import Image, ImageDraw, ImageFont

# Editorial dark
BG = (14, 14, 16, 255)        # #0E0E10
PANEL = (23, 22, 26, 255)     # #17161A
PUMPKIN = (255, 92, 43, 255)  # #FF5C2B
CREAM = (245, 241, 232, 255)  # #F5F1E8
DIM = (154, 149, 140, 255)    # #9A958C

SERIF = [
    '/System/Library/Fonts/Supplemental/Georgia Bold.ttf',
    '/System/Library/Fonts/Supplemental/Times New Roman Bold.ttf',
]
SANS = [
    '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
    '/System/Library/Fonts/Helvetica.ttc',
]
SANS_REG = [
    '/System/Library/Fonts/Supplemental/Arial.ttf',
    '/System/Library/Fonts/Helvetica.ttc',
]


def font(paths, sz):
    for p in paths:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, sz)
            except Exception:
                pass
    return ImageFont.load_default()


def logo(d, x, y, s):
    """Рыжий скруглённый квадрат с засечной «F» (как иконка расширения)."""
    d.rounded_rectangle([x, y, x + s, y + s], radius=int(s * 0.22), fill=PUMPKIN)
    f = font(SERIF, int(s * 0.66))
    bbox = d.textbbox((0, 0), 'F', font=f)
    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    d.text((x + (s - w) / 2 - bbox[0], y + (s - h) / 2 - bbox[1]), 'F', font=f, fill=CREAM)


def decorative_line(d, x0, y0, x1, baseline, amp, color, width, seed):
    """Лёгкая «биржевая» ломаная для фона баннера."""
    import math
    pts = []
    n = 60
    for i in range(n + 1):
        t = i / n
        x = x0 + (x1 - x0) * t
        y = baseline - amp * (math.sin(seed + t * 9) * 0.5 + math.sin(seed * 1.7 + t * 21) * 0.3)
        pts.append((x, y))
    d.line(pts, fill=color, width=width, joint='curve')


def text_center_block(img, lines):
    return img


def make_tile():
    SS = 2
    W, H = 440 * SS, 280 * SS
    img = Image.new('RGBA', (W, H), BG)
    d = ImageDraw.Draw(img)
    # тонкая рыжая кромка снизу
    d.rectangle([0, H - 6 * SS, W, H], fill=PUMPKIN)
    s = int(96 * SS)
    logo(d, int(40 * SS), int(44 * SS), s)
    tf = font(SERIF, int(40 * SS))
    d.text((int(40 * SS), int(160 * SS)), 'Фрейм', font=tf, fill=CREAM)
    sf = font(SANS_REG, int(17 * SS))
    d.text((int(42 * SS), int(212 * SS)), 'Аналитика в терминале', font=sf, fill=DIM)
    d.text((int(42 * SS), int(236 * SS)), 'Т-Инвестиций', font=sf, fill=DIM)
    return img.resize((440, 280), Image.LANCZOS)


def make_marquee():
    SS = 2
    W, H = 1400 * SS, 560 * SS
    img = Image.new('RGBA', (W, H), BG)
    d = ImageDraw.Draw(img)
    # декоративные линии справа
    decorative_line(d, int(840 * SS), 0, W, int(300 * SS), int(120 * SS), (47, 79, 120, 90), int(3 * SS), 1.0)
    decorative_line(d, int(840 * SS), 0, W, int(340 * SS), int(90 * SS), (255, 92, 43, 70), int(3 * SS), 2.4)
    # левый блок
    s = int(150 * SS)
    logo(d, int(90 * SS), int(120 * SS), s)
    tf = font(SERIF, int(86 * SS))
    d.text((int(270 * SS), int(120 * SS)), 'Фрейм', font=tf, fill=CREAM)
    sf = font(SANS, int(34 * SS))
    d.text((int(274 * SS), int(238 * SS)), 'Индикаторы прямо в терминале', font=sf, fill=CREAM)
    sf2 = font(SANS_REG, int(28 * SS))
    d.text((int(274 * SS), int(290 * SS)), 'Т-Инвестиций — открытый интерес, сезонность,', font=sf2, fill=DIM)
    d.text((int(274 * SS), int(326 * SS)), 'потоки фондов и ещё 4 индикатора', font=sf2, fill=DIM)
    # кромка снизу
    d.rectangle([0, H - 10 * SS, W, H], fill=PUMPKIN)
    return img.resize((1400, 560), Image.LANCZOS)


def make_hero():
    """Пустой кадр 1280×800 — рамка панели + лого, под наложение скриншота."""
    SS = 2
    W, H = 1280 * SS, 800 * SS
    img = Image.new('RGBA', (W, H), BG)
    d = ImageDraw.Draw(img)
    pad = int(70 * SS)
    d.rounded_rectangle([pad, pad, W - pad, H - pad], radius=int(16 * SS), fill=PANEL, outline=PUMPKIN, width=int(2 * SS))
    s = int(64 * SS)
    logo(d, pad + int(28 * SS), pad + int(24 * SS), s)
    tf = font(SERIF, int(34 * SS))
    d.text((pad + int(110 * SS), pad + int(38 * SS)), 'Фрейм · Открытый интерес', font=tf, fill=CREAM)
    sf = font(SANS_REG, int(22 * SS))
    d.text((pad + int(30 * SS), H - pad - int(60 * SS)), 'таймфрейм.рф', font=sf, fill=DIM)
    return img.resize((1280, 800), Image.LANCZOS)


def main():
    out = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'extension', 'store', 'assets'))
    os.makedirs(out, exist_ok=True)
    jobs = {
        'tile-440x280.png': make_tile,
        'marquee-1400x560.png': make_marquee,
        'hero-1280x800.png': make_hero,
    }
    for name, fn in jobs.items():
        p = os.path.join(out, name)
        fn().convert('RGB').save(p, quality=95)
        print('wrote', p)


if __name__ == '__main__':
    main()
