#!/usr/bin/env python3
"""Создаёт спрайт всех лого + manifest с координатами + уменьшенные PNG для UI.

Output:
  frontend/public/logos/sprite.png  — 10 cols × N rows × 80px (легаси, кодом не грузится)
  frontend/public/logos/sprite-manifest.json — {ticker: [x, y]}
  frontend/public/logos/sm/<ticker>.png — 128px, источник для TickerLogo <img>

Размер ячейки 80px достаточен для retina-render до 40px UI:
  - модалка selector: 36px → retina 72px (с запасом)
  - header страниц: 28px → retina 56px (с запасом)

SM_SIZE=128 — по факту нигде в приложении лого не показывается крупнее 36px
(макс. usage InstrumentIcon/TickerLogo — see grep), т.е. 128px = запас на 3.5×
retina. Раньше TickerLogo грузил исходный 256px PNG (~20-90KB) под 28px иконку —
~1MB трафика на открытие модалки выбора актива (ОИ/Сезонность, ~30 видимых лого).
128px даёт то же качество отображения при кратно меньшем весе файла.
"""
import json
from pathlib import Path
from PIL import Image

LOGOS_DIR = Path(__file__).parent.parent / 'frontend' / 'public' / 'logos'
CELL_SIZE = 80
COLS = 10
SM_SIZE = 128
SM_DIR = LOGOS_DIR / 'sm'

# Собираем все доступные тикеры (по PNG-файлам, исключая sprite/manifest)
files = sorted(
    p for p in LOGOS_DIR.glob('*.png')
    if p.name != 'sprite.png'
)

# Уменьшенные PNG для TickerLogo (см. SM_SIZE выше) — источник для <img>
# в UI, полноразмерные 256px остаются как master-исходники.
SM_DIR.mkdir(exist_ok=True)
for png_path in files:
    img = Image.open(png_path).convert('RGBA')
    img.thumbnail((SM_SIZE, SM_SIZE), Image.LANCZOS)
    img.save(SM_DIR / png_path.name, 'PNG', optimize=True)
print(f'SM-лого: {len(files)} файлов, {SM_SIZE}px → {SM_DIR}')
n = len(files)
rows = (n + COLS - 1) // COLS

print(f'Лого: {n}, grid: {COLS}×{rows}, cell: {CELL_SIZE}px')

# Создаём прозрачный sprite RGBA
sprite_w = COLS * CELL_SIZE
sprite_h = rows * CELL_SIZE
sprite = Image.new('RGBA', (sprite_w, sprite_h), (0, 0, 0, 0))

manifest = {}
for i, png_path in enumerate(files):
    ticker = png_path.stem
    col = i % COLS
    row = i // COLS
    x = col * CELL_SIZE
    y = row * CELL_SIZE

    img = Image.open(png_path).convert('RGBA')
    # Resize с сохранением пропорций до CELL_SIZE
    img.thumbnail((CELL_SIZE, CELL_SIZE), Image.LANCZOS)
    # Центрируем в ячейке
    paste_x = x + (CELL_SIZE - img.width) // 2
    paste_y = y + (CELL_SIZE - img.height) // 2
    sprite.paste(img, (paste_x, paste_y), img)

    manifest[ticker] = [x, y]

# Сохраняем sprite с оптимизацией
sprite.save(LOGOS_DIR / 'sprite.png', 'PNG', optimize=True, compress_level=9)
sprite_size = (LOGOS_DIR / 'sprite.png').stat().st_size

# Manifest (компактный JSON одной строкой)
manifest_data = {
    'cell': CELL_SIZE,
    'sprite': '/logos/sprite.png',
    'logos': manifest,
}
manifest_path = LOGOS_DIR / 'sprite-manifest.json'
manifest_path.write_text(json.dumps(manifest_data, separators=(',', ':')))

print(f'Sprite: {sprite_w}×{sprite_h}, {sprite_size/1024:.0f} KB')
print(f'Manifest: {len(manifest)} entries, {manifest_path.stat().st_size} B')
