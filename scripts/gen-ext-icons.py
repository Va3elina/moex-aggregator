#!/usr/bin/env python3
"""Генератор иконок расширения Фрейм: рыжий скруглённый квадрат с засечной «F».
Один источник правды для 16/48/128. Перегенерить: python3 scripts/gen-ext-icons.py
"""
from PIL import Image, ImageDraw, ImageFont
import os

PUMPKIN = (255, 92, 43, 255)
CREAM = (245, 241, 232, 255)

FONT_PATHS = [
    '/System/Library/Fonts/Supplemental/Georgia Bold.ttf',
    '/System/Library/Fonts/Supplemental/Georgia.ttf',
    '/System/Library/Fonts/Supplemental/Times New Roman Bold.ttf',
    '/System/Library/Fonts/Supplemental/Times New Roman.ttf',
    '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
    '/System/Library/Fonts/Helvetica.ttc',
]


def get_font(sz):
    for p in FONT_PATHS:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, sz)
            except Exception:
                pass
    return ImageFont.load_default()


def make(size):
    # Супер-сэмплинг ×4 → гладкие края при даунскейле.
    s = size * 4
    img = Image.new('RGBA', (s, s), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, s - 1, s - 1], radius=int(s * 0.22), fill=PUMPKIN)
    f = get_font(int(s * 0.66))
    txt = 'F'
    bbox = d.textbbox((0, 0), txt, font=f)
    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    x = (s - w) / 2 - bbox[0]
    y = (s - h) / 2 - bbox[1]
    d.text((x, y), txt, font=f, fill=CREAM)
    return img.resize((size, size), Image.LANCZOS)


def main():
    out = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'extension', 'icons'))
    os.makedirs(out, exist_ok=True)
    for size in (16, 48, 128):
        path = os.path.join(out, f'icon{size}.png')
        make(size).save(path)
        print('wrote', path)


if __name__ == '__main__':
    main()
