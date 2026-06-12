#!/usr/bin/env python3
"""Скриншоты для Chrome Web Store — отрисовка реальных панелей Фрейма 1-в-1
(значения/формы взяты с живого терминала). Editorial-палитра, ровно 1280×800,
RGB без альфа-канала. Приватно: без баланса счёта.

  01-oi.png        — панель «Открытые позиции» (Нефть Brent): цена + чистая позиция
  02-settings.png  — drawer настроек ОИ (таймфрейм/период/группа/режим/показатель)
  03-strength.png  — панель «Сила рынка»: индекс IMOEX + breadth-гистограмма

Перегенерить: python3 scripts/gen-store-screenshots.py
"""
import os
import math
import random
from PIL import Image, ImageDraw, ImageFont

W, H = 1280, 800
SS = 2  # супер-сэмплинг

# editorial-dark
BG = (14, 14, 16)
PANEL = (23, 22, 26)
CARD = (16, 16, 19)
TEXT = (245, 241, 232)
DIM = (154, 149, 140)
MUTE = (110, 106, 99)
ACCENT = (255, 92, 43)
BLUE = (106, 169, 233)        # цена (chart-line-1)
ORANGE = (242, 104, 60)       # чистая позиция / net
GREEN = (74, 146, 104)        # breadth +
RED = (192, 80, 77)           # breadth -
GRID = (245, 241, 232, 16)
BORDER = (245, 241, 232)
SOFT = (245, 241, 232, 36)

SERIF = ['/System/Library/Fonts/Supplemental/Georgia Bold.ttf',
         '/System/Library/Fonts/Supplemental/Times New Roman Bold.ttf']
SANS = ['/System/Library/Fonts/Supplemental/Arial Bold.ttf', '/System/Library/Fonts/Helvetica.ttc']
SANS_R = ['/System/Library/Fonts/Supplemental/Arial.ttf', '/System/Library/Fonts/Helvetica.ttc']


def font(paths, sz):
    for p in paths:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, sz)
            except Exception:
                pass
    return ImageFont.load_default()


def new():
    img = Image.new('RGB', (W * SS, H * SS), BG)
    return img, ImageDraw.Draw(img, 'RGBA')


def s(x):
    return int(x * SS)


def text(d, xy, t, f, fill, anchor=None):
    d.text((s(xy[0]), s(xy[1])), t, font=f, fill=fill, anchor=anchor)


def gear_icon(d, cx, cy, r, col, wd=None):
    wd = wd or max(1, r * 0.28)
    d.ellipse([s(cx - r), s(cy - r), s(cx + r), s(cy + r)], outline=col, width=s(wd))
    d.ellipse([s(cx - r * 0.34), s(cy - r * 0.34), s(cx + r * 0.34), s(cy + r * 0.34)], fill=col)
    for ang in range(0, 360, 45):
        a = math.radians(ang)
        d.line([s(cx + math.cos(a) * r), s(cy + math.sin(a) * r),
                s(cx + math.cos(a) * (r + r * 0.45)), s(cy + math.sin(a) * (r + r * 0.45))], fill=col, width=s(wd))


def ctrl(d, kind, gx, gy):
    """Мини-иконка кнопки заголовка панели (gear/pop/theme/close)."""
    d.rounded_rectangle([s(gx), s(gy), s(gx + 20), s(gy + 20)], radius=s(3), outline=SOFT, width=s(1))
    cx, cy, r = gx + 10, gy + 10, 4.0
    c = DIM
    if kind == 'close':
        d.line([s(cx - r), s(cy - r), s(cx + r), s(cy + r)], fill=c, width=s(1))
        d.line([s(cx - r), s(cy + r), s(cx + r), s(cy - r)], fill=c, width=s(1))
    elif kind == 'theme':
        d.ellipse([s(cx - r), s(cy - r), s(cx + r), s(cy + r)], outline=c, width=s(1))
        d.pieslice([s(cx - r), s(cy - r), s(cx + r), s(cy + r)], 90, 270, fill=c)
    elif kind == 'pop':
        d.rectangle([s(cx - r), s(cy - r + 2), s(cx + r - 2), s(cy + r)], outline=c, width=s(1))
        d.line([s(cx), s(cy), s(cx + r + 1), s(cy - r - 1)], fill=c, width=s(1))
        d.line([s(cx + r + 1), s(cy - r - 1), s(cx + r - 2), s(cy - r - 1)], fill=c, width=s(1))
        d.line([s(cx + r + 1), s(cy - r - 1), s(cx + r + 1), s(cy - r + 2)], fill=c, width=s(1))
    elif kind == 'gear':
        d.ellipse([s(cx - r), s(cy - r), s(cx + r), s(cy + r)], outline=c, width=s(1))
        d.ellipse([s(cx - 1.3), s(cy - 1.3), s(cx + 1.3), s(cy + 1.3)], fill=c)


def panel_frame(d, x, y, w, h, title):
    """Рамка панели Фрейма: шапка с заголовком + кнопки управления."""
    d.rounded_rectangle([s(x), s(y), s(x + w), s(y + h)], radius=s(6), fill=BG, outline=BORDER, width=s(2))
    # шапка
    head_h = 42
    d.rectangle([s(x) + s(2), s(y) + s(2), s(x + w) - s(2), s(y + head_h)], fill=PANEL)
    d.line([s(x), s(y + head_h), s(x + w), s(y + head_h)], fill=BORDER, width=s(2))
    d.ellipse([s(x + 14), s(y + 16), s(x + 24), s(y + 26)], fill=ACCENT)  # точка
    text(d, (x + 34, y + 12), title, font(SANS, s(15)), TEXT)
    # кнопки ⚙ ⤢ ◐ ×
    bx = x + w - 32
    for i, kind in enumerate(['close', 'theme', 'pop', 'gear']):
        ctrl(d, kind, bx - i * 28, y + 11)
    return head_h


def seeded_walk(n, start, vol, trend, seed, lo, hi):
    random.seed(seed)
    v = start
    out = [v]
    for i in range(1, n):
        v += trend + random.uniform(-vol, vol)
        v = max(lo, min(hi, v))
        out.append(v)
    return out


def line_chart(d, x, y, w, h, series_pts, color, width=2):
    pts = []
    n = len(series_pts)
    for i, val in enumerate(series_pts):
        px = x + (i / (n - 1)) * w
        pts.append((s(px), s(val)))
    d.line(pts, fill=color, width=s(width), joint='curve')


def watermark(d, x, y):
    # «⦚ FRAME»
    d.rounded_rectangle([s(x), s(y + 2), s(x + 14), s(y + 16)], radius=s(2), outline=(245, 241, 232, 40), width=s(2))
    text(d, (x + 22, y), 'FRAME', font(SANS, s(15)), (245, 241, 232, 40))


# ─────────────────────────── 01 — Открытые позиции ───────────────────────────
def shot_oi():
    img, d = new()
    px, py, pw, ph = 120, 70, 1040, 660
    # лёгкий намёк на терминал позади (тусклые свечи слева сверху)
    random.seed(7)
    cx = 30
    base = 150
    for i in range(46):
        op = base + random.uniform(-40, 40)
        cl = op + random.uniform(-26, 26)
        hi = max(op, cl) + random.uniform(4, 18)
        lo = min(op, cl) - random.uniform(4, 18)
        col = (74, 146, 104, 60) if cl >= op else (192, 80, 77, 60)
        d.line([s(cx), s(hi), s(cx), s(lo)], fill=col, width=s(1))
        d.rectangle([s(cx - 4), s(min(op, cl)), s(cx + 4), s(max(op, cl))], fill=col)
        cx += 12
        base += random.uniform(-6, 6)

    head_h = panel_frame(d, px, py, pw, ph, 'Фрейм · Открытые позиции')
    # подзаголовок
    text(d, (px + 22, py + head_h + 14), 'Нефть Brent', font(SANS, s(15)), TEXT)
    tw = d.textlength('Нефть Brent', font=font(SANS, s(15))) / SS
    text(d, (px + 30 + tw, py + head_h + 16), 'Открытые позиции', font(SANS_R, s(12)), DIM)

    # область графика
    gx, gy = px + 70, py + head_h + 78
    gw, gh = pw - 160, ph - head_h - 150
    # легенда
    lx = px + pw / 2 - 130
    ly = py + head_h + 44
    d.ellipse([s(lx), s(ly + 2), s(lx + 9), s(ly + 11)], fill=BLUE)
    text(d, (lx + 15, ly), 'Нефть Brent', font(SANS, s(12)), TEXT)
    lx2 = lx + 130
    d.ellipse([s(lx2), s(ly + 2), s(lx2 + 9), s(ly + 11)], fill=ORANGE)
    text(d, (lx2 + 15, ly), 'Чистая позиция', font(SANS, s(12)), TEXT)

    # сетка
    rows = 6
    for r in range(rows):
        yy = gy + (r / (rows - 1)) * gh
        d.line([s(gx), s(yy), s(gx + gw), s(yy)], fill=GRID, width=s(1))
    cols = 5
    for c in range(cols):
        xx = gx + (c / (cols - 1)) * gw
        d.line([s(xx), s(gy), s(xx), s(gy + gh)], fill=GRID, width=s(1))

    # оси
    left_lbls = ['120', '103', '86.53', '70.03', '53.54']
    for i, lbl in enumerate([left_lbls[0], left_lbls[1], None, left_lbls[2], left_lbls[3], left_lbls[4]][:rows]):
        if lbl:
            yy = gy + (i / (rows - 1)) * gh
            text(d, (px + 24, yy - 7), lbl, font(SANS_R, s(12)), BLUE)
    right_lbls = ['75 256', '-46 160', '-167 577', '-288 993', '-410 409']
    for i, lbl in enumerate([right_lbls[0], right_lbls[1], None, right_lbls[2], right_lbls[3], right_lbls[4]][:rows]):
        if lbl:
            yy = gy + (i / (rows - 1)) * gh
            text(d, (px + pw - 28, yy - 7), lbl, font(SANS_R, s(12)), ORANGE, anchor='ra')

    # серии (форма ~ как на реальном графике)
    price = seeded_walk(70, gy + gh * 0.85, 9, -3.0, 11, gy + 6, gy + gh)   # растёт вверх (значение y падает)
    net = seeded_walk(70, gy + gh * 0.28, 14, 2.4, 23, gy + 6, gy + gh)      # падает вниз потом восстановление
    # перегиб net: вниз к середине, вверх к концу
    net = [v + math.sin(i / 70 * math.pi) * gh * 0.30 for i, v in enumerate(net)]
    line_chart(d, gx, 0, gw, 0, price, BLUE, 2)
    line_chart(d, gx, 0, gw, 0, net, ORANGE, 2)

    # текущие значения-пилюли
    d.rounded_rectangle([s(px + 18), s(gy + 4), s(px + 64), s(gy + 22)], radius=s(3), fill=BLUE)
    text(d, (px + 41, gy + 5), '98.01', font(SANS, s(11)), (10, 10, 12), anchor='ma')
    d.rounded_rectangle([s(px + pw - 78), s(gy + gh * 0.34)], radius=s(3), fill=ORANGE) if False else None
    d.rounded_rectangle([s(px + pw - 86), s(gy + gh * 0.33 - 9), s(px + pw - 22), s(gy + gh * 0.33 + 9)], radius=s(3), fill=ORANGE)
    text(d, (px + pw - 54, gy + gh * 0.33 - 8), '-126 109', font(SANS, s(11)), (10, 10, 12), anchor='ma')

    # даты
    dates = ['05.12.25', '23.01.26', '11.03.26', '21.04.26', '03.06.26']
    for c, dt in enumerate(dates):
        xx = gx + (c / (len(dates) - 1)) * gw
        text(d, (xx, gy + gh + 12), dt, font(SANS_R, s(11)), DIM, anchor='ma')

    watermark(d, gx + 6, gy + gh - 26)
    return img.resize((W, H), Image.LANCZOS)


# ─────────────────────────── 02 — drawer настроек ───────────────────────────
def chip(d, x, y, label, active, fsz=13, pad=12, h=34):
    w = d.textlength(label, font=font(SANS, s(fsz))) / SS + pad * 2
    fill = ACCENT if active else (0, 0, 0, 0)
    outline = ACCENT if active else SOFT
    d.rounded_rectangle([s(x), s(y), s(x + w), s(y + h)], radius=s(7), fill=fill if active else BG, outline=outline, width=s(2 if active else 1))
    text(d, (x + w / 2, y + (h - fsz) / 2 - 2), label, font(SANS, s(fsz)), (255, 255, 255) if active else TEXT, anchor='ma')
    return w


def chip_row(d, x, y, items):
    cx = x
    for label, active in items:
        w = chip(d, cx, y, label, active)
        cx += w + 10


def shot_settings():
    img, d = new()
    px, py, pw, ph = 200, 60, 880, 680
    head_h = panel_frame(d, px, py, pw, ph, 'Фрейм · Открытые позиции')
    # шапка drawer
    dy = py + head_h
    d.rectangle([s(px) + s(2), s(dy), s(px + pw) - s(2), s(dy + 52)], fill=PANEL)
    d.line([s(px), s(dy + 52), s(px + pw), s(dy + 52)], fill=SOFT, width=s(1))
    text(d, (px + 22, dy + 16), '⚙', font(SANS, s(17)), ACCENT)
    text(d, (px + 50, dy + 15), 'Настройки', font(SANS, s(15)), TEXT)
    tw = d.textlength('Настройки', font=font(SANS, s(15))) / SS
    text(d, (px + 62 + tw, dy + 17), '· Открытые позиции', font(SANS_R, s(12)), DIM)
    d.rounded_rectangle([s(px + pw - 44), s(dy + 14), s(px + pw - 20), s(dy + 38)], radius=s(4), outline=SOFT, width=s(1))
    text(d, (px + pw - 32, dy + 15), '×', font(SANS_R, s(14)), DIM, anchor='ma')

    cx = px + 30
    cy = dy + 78

    def section(title, items, gap=46):
        nonlocal cy
        text(d, (cx, cy), title, font(SANS, s(11)), DIM)
        chip_row(d, cx, cy + 22, items)
        cy += gap + 26

    section('ТАЙМФРЕЙМ', [('5 мин', False), ('1 час', False), ('1 день', True)])
    section('ПЕРИОД', [('1Д', False), ('1Н', False), ('1М', False), ('3М', False), ('6М', True), ('1Г', False), ('2Г', False), ('5Л', False), ('Всё', False)])
    section('ГРУППА УЧАСТНИКОВ', [('Физлица', False), ('Юрлица', True)])
    section('РЕЖИМ', [('Объём позиций', True), ('Число трейдеров', False)])
    text(d, (cx, cy), 'ПОКАЗАТЕЛЬ', font(SANS, s(11)), DIM)
    chip_row(d, cx, cy + 22, [('Открытый интерес', False), ('Покупки', False), ('Продажи', False), ('Покупки + Продажи', False)])
    chip_row(d, cx, cy + 22 + 44, [('Чистая позиция', True)])
    return img.resize((W, H), Image.LANCZOS)


# ─────────────────────────── 03 — Сила рынка ───────────────────────────
def shot_strength():
    img, d = new()
    px, py, pw, ph = 150, 70, 980, 660
    head_h = panel_frame(d, px, py, pw, ph, 'Фрейм · Сила рынка')
    text(d, (px + 22, py + head_h + 14), 'Сила рынка', font(SANS, s(15)), TEXT)
    tw = d.textlength('Сила рынка', font=font(SANS, s(15))) / SS
    text(d, (px + 30 + tw, py + head_h + 16), '% выше EMA 100', font(SANS_R, s(12)), DIM)

    gx, gy = px + 56, py + head_h + 52
    gw = pw - 130
    # верх — индекс (линия)
    ih = 220
    text(d, (px + pw / 2, gy - 4), 'Индекс МосБиржи (IMOEX)', font(SANS_R, s(12)), DIM, anchor='ma')
    idx = seeded_walk(80, gy + ih * 0.4, 7, 0.4, 5, gy + 16, gy + ih)
    line_chart(d, gx, 0, gw, 0, idx, ORANGE, 2)
    for lbl, fr in [('3 017', 0.0), ('2 835', 0.33), ('2 653', 0.66), ('2 471', 1.0)]:
        yy = gy + fr * ih
        d.line([s(gx), s(yy), s(gx + gw), s(yy)], fill=GRID, width=s(1))
        text(d, (px + pw - 24, yy - 7), lbl, font(SANS_R, s(11)), DIM, anchor='ra')
    watermark(d, gx + 6, gy + ih * 0.55)

    # низ — breadth (гистограмма)
    by = gy + ih + 60
    bh = 200
    text(d, (px + pw / 2, by - 22), '% акций выше EMA100', font(SANS_R, s(12)), DIM, anchor='ma')
    for lbl, fr in [('80%', 0.1), ('60%', 0.32), ('40%', 0.55), ('20%', 0.78)]:
        yy = by + fr * bh
        d.line([s(gx), s(yy), s(gx + gw), s(yy)], fill=GRID, width=s(1))
        text(d, (px + pw - 24, yy - 7), lbl, font(SANS_R, s(11)), DIM, anchor='ra')
    random.seed(3)
    n = 120
    bw = gw / n
    for i in range(n):
        val = (math.sin(i / n * math.pi * 3) * 0.5 + 0.5)
        val = max(0.05, min(1, val + random.uniform(-0.25, 0.25)))
        # цвет: красный низ → жёлтый → зелёный верх
        if val > 0.6:
            col = GREEN
        elif val < 0.35:
            col = RED
        else:
            col = (210, 140, 60)
        bx = gx + i * bw
        top = by + (1 - val) * bh
        d.rectangle([s(bx), s(top), s(bx + bw - 1), s(by + bh)], fill=col)
    # даты
    for c, dt in enumerate(['6 июн. 25', '3 сент. 25', '1 дек. 25', '5 мар. 26', '3 июн. 26']):
        xx = gx + (c / 4) * gw
        text(d, (xx, by + bh + 12), dt, font(SANS_R, s(10)), DIM, anchor='ma')
    return img.resize((W, H), Image.LANCZOS)


def main():
    out = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'extension', 'store', 'screenshots'))
    os.makedirs(out, exist_ok=True)
    jobs = {'01-oi.png': shot_oi, '02-settings.png': shot_settings, '03-strength.png': shot_strength}
    for name, fn in jobs.items():
        p = os.path.join(out, name)
        fn().convert('RGB').save(p)  # RGB → без альфы
        print('wrote', p, '(1280×800, no alpha)')


if __name__ == '__main__':
    main()
