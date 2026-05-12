#!/usr/bin/env python3
"""Генерирует PNG-иконки в стиле стикерпака Russian Stocks (256×256).

Стили:
  currency  — сплошной зелёный круг, белый символ внутри (как USD/EUR из стикерпака)
  metal     — тёмный круг, белая карточка элемента с чёрной полосой сверху
              + чёрный символ снизу (как Pt/Pd/Ag из стикерпака)
  commodity — коричневый круг + белый текст (для пищевого сырья)

Запуск:
  python3 scripts/gen-icon.py <style> <symbol> <output.png>
"""
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

SIZE = 256

# Палитра
GREEN = (33, 160, 56)            # #21A038 — зелёный фон валют
WHITE = (255, 255, 255)
DARK_NAVY = (15, 23, 34)         # #0F1722 — фон карточек элементов
BROWN = (139, 90, 43)            # коричневый для пищевого сырья

FONT_PATHS = [
    '/System/Library/Fonts/Helvetica.ttc',
    '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
    '/System/Library/Fonts/Avenir Next.ttc',
]


def get_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for fp in FONT_PATHS:
        if Path(fp).exists():
            try:
                return ImageFont.truetype(fp, size, index=1)  # bold
            except Exception:
                try:
                    return ImageFont.truetype(fp, size)
                except Exception:
                    pass
    return ImageFont.load_default()


def fit_font_size(text: str, max_width: int, max_height: int, start: int = 200) -> int:
    for size in range(start, 20, -2):
        font = get_font(size)
        bbox = font.getbbox(text)
        w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
        if w <= max_width and h <= max_height:
            return size
    return 24


def draw_centered_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    cx: int,
    cy: int,
    color: tuple,
    max_w: int,
    max_h: int,
    start_size: int = 200,
):
    fsize = fit_font_size(text, max_w, max_h, start=start_size)
    font = get_font(fsize)
    bbox = font.getbbox(text)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    tx = cx - tw // 2 - bbox[0]
    ty = cy - th // 2 - bbox[1]
    draw.text((tx, ty), text, fill=color, font=font)


def draw_currency(symbol: str, out_path: Path):
    """Зелёный круг с тонким белым ободком по периметру + крупный белый символ.

    Структура (как в стикерах USD/EUR/GBP):
      1. Внешний белый круг (256×256)
      2. Зелёный круг чуть меньше (с отступом по 6px) → создаёт тонкий белый ободок
      3. Крупный белый символ внутри
    """
    img = Image.new('RGBA', (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    # Внешний белый круг — задаёт белый ободок.
    draw.ellipse((0, 0, SIZE, SIZE), fill=WHITE)
    # Зелёный круг внутри. Замеры оригинальных стикеров (USD/EUR/GBP/CNY)
    # показали белый ободок 7% от ширины → 18px при SIZE=256.
    border = int(SIZE * 0.07)  # 18px
    draw.ellipse(
        (border, border, SIZE - border, SIZE - border),
        fill=GREEN,
    )
    # Крупный белый символ — занимает ~70% диаметра (учли увеличенный ободок).
    draw_centered_text(
        draw, symbol, SIZE // 2, SIZE // 2,
        WHITE, int(SIZE * 0.66), int(SIZE * 0.66), start_size=220,
    )
    img.save(out_path, 'PNG', optimize=True, compress_level=9)


def draw_metal(symbol: str, out_path: Path):
    """Тёмно-синий круг + белая карточка элемента (с полосой сверху) + чёрный символ."""
    img = Image.new('RGBA', (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    cx, cy = SIZE // 2, SIZE // 2

    # Внешний тёмно-синий круг
    draw.ellipse((0, 0, SIZE, SIZE), fill=DARK_NAVY)

    # Белая карточка элемента с rounded corners
    card_w = int(SIZE * 0.50)
    card_h = int(SIZE * 0.62)
    card_x = cx - card_w // 2
    card_y = cy - card_h // 2 - 4
    draw.rounded_rectangle(
        (card_x, card_y, card_x + card_w, card_y + card_h),
        radius=10,
        fill=WHITE,
    )

    # Чёрная горизонтальная полоса сверху карточки (электронные орбитали)
    bar_pad = 14
    bar_h = 10
    bar_y = card_y + 18
    draw.rectangle(
        (card_x + bar_pad, bar_y, card_x + card_w - bar_pad, bar_y + bar_h),
        fill=DARK_NAVY,
    )

    # Символ — крупный чёрный текст в нижней части карточки
    text_cx = card_x + card_w // 2
    text_cy = card_y + int(card_h * 0.62)
    draw_centered_text(
        draw, symbol, text_cx, text_cy,
        DARK_NAVY,
        int(card_w * 0.78), int(card_h * 0.45),
        start_size=120,
    )

    img.save(out_path, 'PNG', optimize=True, compress_level=9)


def draw_commodity(symbol: str, out_path: Path):
    """Сплошной коричневый круг + белый текст."""
    img = Image.new('RGBA', (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse((0, 0, SIZE, SIZE), fill=BROWN)
    draw_centered_text(
        draw, symbol, SIZE // 2, SIZE // 2,
        WHITE, int(SIZE * 0.65), int(SIZE * 0.65), start_size=200,
    )
    img.save(out_path, 'PNG', optimize=True, compress_level=9)


def draw_brand(symbol: str, bg_hex: str, fg_hex: str, out_path: Path):
    """Brand-style: сплошной круг фирменного цвета + одна буква (как Сбер/Магнит).

    Используется для компаний которым нужна минималистичная иконка
    (только символ, без wordmark / без декоративного ободка).
    """
    bg = tuple(int(bg_hex[i:i+2], 16) for i in (0, 2, 4))
    fg = tuple(int(fg_hex[i:i+2], 16) for i in (0, 2, 4))
    img = Image.new('RGBA', (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse((0, 0, SIZE, SIZE), fill=bg)
    draw_centered_text(
        draw, symbol, SIZE // 2, SIZE // 2,
        fg, int(SIZE * 0.62), int(SIZE * 0.62), start_size=220,
    )
    img.save(out_path, 'PNG', optimize=True, compress_level=9)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage:')
        print('  gen-icon.py <currency|metal|commodity> <symbol> <output.png>')
        print('  gen-icon.py brand <symbol> <bg_hex> <fg_hex> <output.png>')
        sys.exit(1)
    style = sys.argv[1]
    if style == 'brand':
        symbol, bg, fg, out = sys.argv[2], sys.argv[3], sys.argv[4], Path(sys.argv[5])
        draw_brand(symbol, bg, fg, out)
    else:
        symbol, out = sys.argv[2], Path(sys.argv[3])
        if style == 'currency':
            draw_currency(symbol, out)
        elif style == 'metal':
            draw_metal(symbol, out)
        elif style == 'commodity':
            draw_commodity(symbol, out)
        else:
            print(f'Unknown style: {style}')
            sys.exit(1)
    print(f'Сгенерировано: {out}')
