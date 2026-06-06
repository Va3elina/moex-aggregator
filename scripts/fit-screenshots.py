#!/usr/bin/env python3
"""Подгоняет произвольные скриншоты под формат Chrome Web Store: ровно 1280×800,
RGB (БЕЗ альфа-канала), PNG. Вписывает с сохранением пропорций (contain) и
паддит фоном editorial-dark — без обрезки контента.

Использование:
  python3 scripts/fit-screenshots.py <файл1> [<файл2> ...]
  python3 scripts/fit-screenshots.py --desktop N      # взять N свежих кадров с Рабочего стола

Результат: extension/store/screenshots/01.png, 02.png, ...
"""
import os
import sys
import glob
from PIL import Image

TARGET = (1280, 800)
BG = (14, 14, 16)  # #0E0E10
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT = os.path.join(ROOT, 'extension', 'store', 'screenshots')


def newest_desktop(n):
    desk = os.path.expanduser('~/Desktop')
    shots = []
    for ext in ('png', 'jpg', 'jpeg', 'PNG', 'JPG'):
        shots += glob.glob(os.path.join(desk, f'*.{ext}'))
    shots.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return shots[:n]


def fit(src, idx):
    im = Image.open(src).convert('RGB')
    w, h = im.size
    scale = min(TARGET[0] / w, TARGET[1] / h)
    nw, nh = max(1, round(w * scale)), max(1, round(h * scale))
    im2 = im.resize((nw, nh), Image.LANCZOS)
    canvas = Image.new('RGB', TARGET, BG)
    canvas.paste(im2, ((TARGET[0] - nw) // 2, (TARGET[1] - nh) // 2))
    dst = os.path.join(OUT, f'{idx:02d}.png')
    canvas.save(dst)  # RGB → 24-bit PNG, без альфы
    print(f'✓ {os.path.basename(src)} ({w}×{h}) → {dst} (1280×800, no alpha)')


def main():
    os.makedirs(OUT, exist_ok=True)
    args = sys.argv[1:]
    if not args:
        print('Укажи файлы или --desktop N'); return
    if args[0] == '--desktop':
        n = int(args[1]) if len(args) > 1 else 3
        files = newest_desktop(n)
        if not files:
            print('На Рабочем столе не нашёл изображений'); return
        print('Беру свежие кадры с Рабочего стола:')
        for f in files:
            print('  ', f)
    else:
        files = args
    for i, f in enumerate(files, 1):
        fit(f, i)


if __name__ == '__main__':
    main()
