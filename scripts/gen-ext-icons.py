#!/usr/bin/env python3
"""Генератор иконок расширения FRAME: фирменный знак «рамка» из четырёх уголков.
Источник правды — frontend/public/icon-512.svg (тот же знак, те же цвета).
Перегенерить: python3 scripts/gen-ext-icons.py
"""
from PIL import Image, ImageDraw
import os

PUMPKIN = (255, 92, 43, 255)
CREAM = (244, 241, 234, 255)

# Уголки в системе координат 0..32 (как в icon-512.svg), знак вписан с отступом 56/512.
CORNERS = [
    [(3, 3), (13, 3), (13, 8), (8, 8), (8, 13), (3, 13)],
    [(29, 3), (19, 3), (19, 8), (24, 8), (24, 13), (29, 13)],
    [(3, 29), (13, 29), (13, 24), (8, 24), (8, 19), (3, 19)],
    [(29, 29), (19, 29), (19, 24), (24, 24), (24, 19), (29, 19)],
]


def make(size):
    # Супер-сэмплинг ×8 → гладкие края при даунскейле.
    s = size * 8
    img = Image.new('RGBA', (s, s), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, s - 1, s - 1], radius=int(s * 80 / 512), fill=CREAM)
    pad = s * 56 / 512
    unit = (s - 2 * pad) / 32
    for poly in CORNERS:
        d.polygon([(pad + x * unit, pad + y * unit) for x, y in poly], fill=PUMPKIN)
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
