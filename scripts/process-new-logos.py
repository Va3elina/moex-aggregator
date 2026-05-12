#!/usr/bin/env python3
"""Обрабатывает скачанные лого: конвертирует в квадратные PNG 256×256
с прозрачным фоном (или белым для JPG → удаляем фон).

Логика:
  1. SVG → PNG через sips (subprocess)
  2. PNG → центрируем в квадрате 256×256 с прозрачным фоном
  3. JPG → конвертируем в PNG, белый фон делаем прозрачным (chroma key)
"""
import shutil
import subprocess
from pathlib import Path
from PIL import Image

SRC = Path('/tmp/new-logos')
DST = Path('/Users/vadim/PyCharmMiscProject/MOEX/frontend/public/logos')
TARGET = 256
PADDING = 14  # px вокруг лого внутри квадрата


def svg_to_png(svg_path: Path, png_path: Path):
    """SVG → PNG через sips. Высоту/ширину масштабирует до TARGET по большей стороне."""
    subprocess.run(
        ['sips', '-s', 'format', 'png', '-Z', str(TARGET), str(svg_path), '--out', str(png_path)],
        check=True,
        capture_output=True,
    )


def jpg_to_png_transparent(jpg_path: Path, png_path: Path):
    """JPG → PNG. Белый фон (#FFFFFF) делаем прозрачным."""
    img = Image.open(jpg_path).convert('RGBA')
    pixels = img.load()
    if pixels is None:
        img.save(png_path, 'PNG')
        return
    w, h = img.size
    for y in range(h):
        for x in range(w):
            r, g, b, _ = pixels[x, y]
            # Белый/около-белый → прозрачный
            if r > 240 and g > 240 and b > 240:
                pixels[x, y] = (255, 255, 255, 0)
    img.save(png_path, 'PNG')


def center_in_square(input_png: Path, output: Path):
    """Берём картинку, центрируем в квадрате TARGET×TARGET с прозрачным фоном."""
    img = Image.open(input_png).convert('RGBA')
    # Resize чтобы поместиться в TARGET-2*PADDING
    inner = TARGET - 2 * PADDING
    img.thumbnail((inner, inner), Image.LANCZOS)
    # Создаём прозрачный квадрат
    canvas = Image.new('RGBA', (TARGET, TARGET), (0, 0, 0, 0))
    px = (TARGET - img.width) // 2
    py = (TARGET - img.height) // 2
    canvas.paste(img, (px, py), img)
    canvas.save(output, 'PNG', optimize=True, compress_level=9)


def process_one(src: Path, dst: Path):
    suffix = src.suffix.lower()
    if suffix == '.svg':
        tmp = src.with_suffix('.tmp.png')
        svg_to_png(src, tmp)
        center_in_square(tmp, dst)
        tmp.unlink()
    elif suffix == '.jpg':
        tmp = src.with_suffix('.tmp.png')
        jpg_to_png_transparent(src, tmp)
        center_in_square(tmp, dst)
        tmp.unlink()
    elif suffix == '.png':
        center_in_square(src, dst)
    else:
        print(f'  Skip {src.name}: unknown format')
        return
    out_size = dst.stat().st_size
    print(f'  {src.name} → {dst.name} ({out_size/1024:.1f} KB)')


for src in sorted(SRC.iterdir()):
    if src.suffix.lower() not in ('.svg', '.png', '.jpg'):
        continue
    ticker = src.stem.upper()
    if '.' in ticker:
        ticker = ticker.split('.')[0]
    dst = DST / f'{ticker}.png'
    print(f'\n{ticker}:')
    try:
        process_one(src, dst)
    except Exception as e:
        print(f'  ERROR: {e}')
