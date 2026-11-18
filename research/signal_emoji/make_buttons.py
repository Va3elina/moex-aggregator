"""Значки кнопок под сигналом @framesignalbot (Вадим 18.11.2026: «вставляют анимированные смайлики — на настройки
шестерёнку и что-то на график»). Наши, в цвете FRAME, анимированные:
  gear  — шестерёнка «Настройки», плавно вращается (зубцов 8 — поворот на 45° за период, петля без шва)
  chart — линия графика «Открыть график»: рисуется слева направо вверх, держится, гаснет

WEBM VP9 с прозрачностью 100×100, ≤3 с, ≤256 КБ (требования Telegram к своим эмодзи). Добавление в пак —
pack.py add (t.me/addemoji/frame_signals_by_framesignalbot).

    python research/signal_emoji/make_buttons.py   → out_buttons/gear.webm, chart.webm, preview.png
"""
from __future__ import annotations

import math
import pathlib
import shutil

from PIL import Image, ImageDraw

import subprocess
import tempfile

from make_emoji import FPS, ORANGE, SIZE, SS

MAX_KB = 60                                   # своим эмодзи Telegram ставит потолок 64 КБ («file is too big»)


def webm(frames, path: pathlib.Path) -> None:
    """Как make_emoji.webm, но жмём сильнее, пока не влезет в MAX_KB."""
    for crf in (32, 38, 44, 50):
        with tempfile.TemporaryDirectory() as d:
            for i, f in enumerate(frames):
                f.save(f"{d}/f{i:03d}.png")
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-framerate", str(FPS), "-i", f"{d}/f%03d.png",
                            "-c:v", "libvpx-vp9", "-pix_fmt", "yuva420p", "-b:v", "0", "-crf", str(crf), "-an",
                            "-auto-alt-ref", "0", str(path)], check=True)
        if path.stat().st_size / 1024 <= MAX_KB:
            return
    raise SystemExit(f"{path.name}: не влезает в {MAX_KB} КБ")

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out_buttons"
GEAR_SEC, CHART_SEC = 2.0, 2.4
WHITE = (255, 255, 255)


def gear_frame(t: float) -> Image.Image:
    big = SIZE * SS
    im = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    c = big / 2
    teeth, r_out, r_in, r_hole = 8, big * 0.46, big * 0.34, big * 0.15
    a0 = t * 2 * math.pi / teeth                          # за период — один зубец: петля без шва
    pts = []
    for k in range(teeth):
        base = a0 + k * 2 * math.pi / teeth
        for da, r in ((-0.22, r_in), (-0.13, r_out), (0.13, r_out), (0.22, r_in)):
            a = base + da
            pts.append((c + r * math.cos(a), c + r * math.sin(a)))
        a = base + math.pi / teeth
        pts.append((c + r_in * math.cos(a), c + r_in * math.sin(a)))
    d.polygon(pts, fill=(*ORANGE, 255))
    d.ellipse((c - r_in, c - r_in, c + r_in, c + r_in), fill=(*ORANGE, 255))
    d.ellipse((c - r_hole, c - r_hole, c + r_hole, c + r_hole), fill=(0, 0, 0, 0))
    return im.resize((SIZE, SIZE), Image.LANCZOS)


CHART = [(0.10, 0.78), (0.32, 0.58), (0.50, 0.68), (0.72, 0.36), (0.90, 0.22)]


def chart_frame(t: float) -> Image.Image:
    """0–0.55 — линия рисуется, 0.55–0.85 — держится, 0.85–1 — гаснет."""
    big = SIZE * SS
    im = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    draw = min(t / 0.55, 1.0)
    alpha = 255 if t < 0.85 else round(255 * (1 - (t - 0.85) / 0.15))
    pts = [(x * big, y * big) for x, y in CHART]
    seg = [math.dist(a, b) for a, b in zip(pts, pts[1:])]
    left = sum(seg) * (0.5 - 0.5 * math.cos(math.pi * draw))     # плавный разгон и торможение
    line = [pts[0]]
    for (a, b), L in zip(zip(pts, pts[1:]), seg):
        if left <= 0:
            break
        k = min(left / L, 1.0)
        line.append((a[0] + (b[0] - a[0]) * k, a[1] + (b[1] - a[1]) * k))
        left -= L
    w = round(big * 0.09)
    if len(line) > 1:
        d.line(line, fill=(*ORANGE, alpha), width=w, joint="curve")
        for p in (line[0], line[-1]):
            d.ellipse((p[0] - w / 2, p[1] - w / 2, p[0] + w / 2, p[1] + w / 2), fill=(*ORANGE, alpha))
        x, y = line[-1]                                            # точка «сейчас» на конце линии
        rr = w * 0.95
        d.ellipse((x - rr, y - rr, x + rr, y + rr), fill=(*ORANGE, alpha))
    return im.resize((SIZE, SIZE), Image.LANCZOS)


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir()
    n = round(GEAR_SEC * FPS)
    webm([gear_frame(i / n) for i in range(n)], OUT / "gear.webm")
    m = round(CHART_SEC * FPS)
    webm([chart_frame(i / m) for i in range(m)], OUT / "chart.webm")
    cell = 80
    prev = Image.new("RGBA", (6 * cell, 2 * cell), (23, 33, 43, 255))
    for k, t in enumerate((0.0, 0.2, 0.4, 0.6, 0.8, 0.95)):
        prev.alpha_composite(gear_frame(t).resize((cell - 10, cell - 10)), (k * cell + 5, 5))
        prev.alpha_composite(chart_frame(t).resize((cell - 10, cell - 10)), (k * cell + 5, cell + 5))
    prev.save(OUT / "preview.png")
    for p in sorted(OUT.iterdir()):
        print(p.name, f"{p.stat().st_size / 1024:.1f} КБ")


if __name__ == "__main__":
    main()
