"""Свои эмодзи шкалы силы сигналов @framesignalbot (Вадим 18.11.2026: «всегда 5 кружков»).

Рекорд позиции: 5 кружков, оранжевых столько, сколько силы, остальные серые; от 2 лет оранжевые переливаются волной,
«за всё время» — плюс горящий огонь. Нужны значки:
  gray      — серый кружок, без движения
  dot       — оранжевый кружок, без движения
  wave0..4  — оранжевый кружок, переливается; у каждого свой сдвиг фазы, поэтому кружки в ряд дают бегущий свет
  fire      — огонь из пака Frametool, горит

Анимированные — видео WEBM VP9 с прозрачностью 100×100, ≤3 с, ≤256 КБ (требования Telegram к своим эмодзи);
неподвижные — PNG 100×100. Огонь берётся из пака Frametool (fire_src.webp рядом), не рисуется заново.

    python research/signal_emoji/make_emoji.py   → out/*.webm, out/*.png, out/preview.png
Нужны Pillow и ffmpeg с libvpx-vp9.
"""
from __future__ import annotations

import math
import pathlib
import shutil
import subprocess
import tempfile

from PIL import Image, ImageDraw, ImageFilter

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "out"
SIZE, SS = 100, 4                      # итоговый размер и суперсэмплинг для гладких краёв
FPS = 30
WAVE_SEC, WAVE_SHIFT = 1.6, 0.13       # период волны и сдвиг между соседними кружками (как на холсте)
FIRE_SEC = 1.2
GRAY = (74, 86, 99)
ORANGE = (255, 92, 43)
DIM = ORANGE                           # в покое — как неподвижный оранжевый, на гребне ярче
BRIGHT = (255, 165, 127)               # кружок волны на гребне
R = 30                                 # радиус кружка в итоговых пикселях (диаметр 60 из 100)


def lerp(a, b, t):
    return tuple(round(x + (y - x) * t) for x, y in zip(a, b))


def circle(color, scale=1.0, glow=0.0) -> Image.Image:
    big = SIZE * SS
    im = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    c, r = big / 2, R * SS * scale
    if glow > 0:
        halo = Image.new("RGBA", (big, big), (0, 0, 0, 0))
        hr = r * 1.35
        ImageDraw.Draw(halo).ellipse((c - hr, c - hr, c + hr, c + hr), fill=(*ORANGE, round(150 * glow)))
        im = Image.alpha_composite(im, halo.filter(ImageFilter.GaussianBlur(9 * SS)))
    ImageDraw.Draw(im).ellipse((c - r, c - r, c + r, c + r), fill=(*color, 255))
    return im.resize((SIZE, SIZE), Image.LANCZOS)


def wave_frame(t: float) -> Image.Image:
    """t — доля периода 0..1. Гребень — короткий импульс, остальное время кружок спокоен (как keyframes wave)."""
    x = 0.5 - 0.5 * math.cos(2 * math.pi * t)              # плавно 0→1→0 за период
    k = x ** 3                                             # гребень узкий, покой длинный
    return circle(lerp(DIM, BRIGHT, k), 1.0 + 0.08 * k, k)


def fire_frame(src: Image.Image, t: float) -> Image.Image:
    x = 0.5 - 0.5 * math.cos(2 * math.pi * t)
    big = SIZE * SS
    im = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    halo = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    hr = big * (0.30 + 0.06 * x)
    ImageDraw.Draw(halo).ellipse((big / 2 - hr, big * 0.55 - hr, big / 2 + hr, big * 0.55 + hr), fill=(255, 120, 50, round(60 + 90 * x)))
    im = Image.alpha_composite(im, halo.filter(ImageFilter.GaussianBlur(10 * SS)))
    s = 0.92 + 0.1 * x                                     # пламя «дышит» от основания
    w = round(big * s)
    f = src.resize((w, w), Image.LANCZOS).rotate(-3 + 6 * x, resample=Image.BICUBIC, center=(w / 2, w * 0.9))
    im.alpha_composite(f, (round((big - w) / 2), round(big * 0.96 - w)))
    return im.resize((SIZE, SIZE), Image.LANCZOS)


def webm(frames: list[Image.Image], path: pathlib.Path) -> None:
    with tempfile.TemporaryDirectory() as d:
        for i, f in enumerate(frames):
            f.save(f"{d}/f{i:03d}.png")
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-framerate", str(FPS), "-i", f"{d}/f%03d.png",
                        "-c:v", "libvpx-vp9", "-pix_fmt", "yuva420p", "-b:v", "0", "-crf", "32", "-an",
                        "-auto-alt-ref", "0", str(path)], check=True)
    kb = path.stat().st_size / 1024
    assert kb <= 256, f"{path.name}: {kb:.0f} КБ > 256"


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir()
    circle(GRAY).save(OUT / "gray.png")
    circle(ORANGE).save(OUT / "dot.png")
    n = round(WAVE_SEC * FPS)
    for k in range(5):
        shift = WAVE_SHIFT * k / WAVE_SEC
        webm([wave_frame((i / n - shift) % 1) for i in range(n)], OUT / f"wave{k}.webm")
    src = Image.open(HERE / "fire_src.webp").convert("RGBA").resize((SIZE * SS, SIZE * SS), Image.LANCZOS)
    m = round(FIRE_SEC * FPS)
    webm([fire_frame(src, i / m) for i in range(m)], OUT / "fire.webm")
    # превью: строки шкалы, по 4 кадра времени
    rows = [["gray"] * 5, ["dot"] + ["gray"] * 4, ["dot"] * 2 + ["gray"] * 3, ["w"] * 3 + ["gray"] * 2,
            ["w"] * 4 + ["gray"], ["w"] * 5, ["w"] * 5 + ["fire"]]
    cell = 40
    prev = Image.new("RGBA", (4 * (6 * cell + 30), len(rows) * cell + 10), (23, 33, 43, 255))
    for ti, t in enumerate((0.0, 0.25, 0.5, 0.75)):
        for ri, row in enumerate(rows):
            for ci, kind in enumerate(row):
                if kind == "gray":
                    im = circle(GRAY)
                elif kind == "dot":
                    im = circle(ORANGE)
                elif kind == "fire":
                    im = fire_frame(src, t)
                else:
                    im = wave_frame((t - WAVE_SHIFT * ci / WAVE_SEC) % 1)
                prev.alpha_composite(im.resize((cell - 4, cell - 4), Image.LANCZOS), (ti * (6 * cell + 30) + ci * cell, ri * cell + 5))
    prev.save(OUT / "preview.png")
    for p in sorted(OUT.iterdir()):
        print(p.name, f"{p.stat().st_size / 1024:.1f} КБ")


if __name__ == "__main__":
    main()
