"""Фирменные стикеры Фрейма для бота @framesignalbot: оранжевая плитка с рамкой-логотипом и белым знаком,
под ней подпись. Прозрачный фон — стикер одинаково смотрится в светлой и тёмной теме.

Запуск: python research/bot_stickers/make_stickers.py → png/*.png (512×512, требование Telegram к стикерам)
и preview.png. Пак создаётся от имени бота (frame_by_framesignalbot) — см. api/services/tg_webapp.py.
Нужен rsvg-convert (brew install librsvg).
"""
from __future__ import annotations

import pathlib
import subprocess

HERE = pathlib.Path(__file__).resolve().parent
ORANGE, ORANGE_D, WHITE = "#FF5C2B", "#E8481A", "#FFFFFF"
LINE = 'fill="none" stroke="#FFFFFF" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"'

# Знаки в сетке 32×32, поле внутри рамки ≈ 10..22 (как у аватарок ботов)
GLYPHS = {
    "done": f'<path d="M10.5 16.4l3.8 3.8 7.4-8" {LINE}/>',
    # улыбка: два глаза и дуга
    "hello": (f'<circle cx="13.2" cy="13.6" r="1.5" fill="{WHITE}"/><circle cx="18.8" cy="13.6" r="1.5" fill="{WHITE}"/>'
              f'<path d="M11.6 17.4c1.1 2.3 2.7 3.4 4.4 3.4s3.3-1.1 4.4-3.4" {LINE}/>'),
    "signal": (f'<path d="M16 9.4c-3 0-5 2.2-5 5.2v3l-1.4 2.1h12.8l-1.4-2.1v-3c0-3-2-5.2-5-5.2z" fill="{WHITE}"/>'
               f'<path d="M14.2 20.6a1.8 1.8 0 0 0 3.6 0z" fill="{WHITE}"/>'),
    "record": (f'<path d="M16 9c.4 2.6 3.9 4.1 3.9 8a3.9 3.9 0 0 1-7.8 0c0-1.7.9-2.9 1.9-3.8.2 1.2.8 2 1.6 2.4-.3-2.5.1-4.7.4-6.6z" fill="{WHITE}"/>'),
}
LABELS = {"done": "ГОТОВО", "hello": "ПРИВЕТ", "signal": "СИГНАЛ", "record": "РЕКОРД"}


def frame(color: str) -> str:
    """Рамка-логотип Фрейма (как у аватарок): тонкие уголки с длинными плечами, в сетке 32×32."""
    lo, hi, t, arm = 6.2, 25.8, 2.1, 8.6
    out = []
    for x0, sx in ((lo, 1), (hi, -1)):
        for y0, sy in ((lo, 1), (hi, -1)):
            pts = [(x0, y0), (x0 + sx * arm, y0), (x0 + sx * arm, y0 + sy * t), (x0 + sx * t, y0 + sy * t),
                   (x0 + sx * t, y0 + sy * arm), (x0, y0 + sy * arm)]
            out.append('<path d="M' + " L".join(f"{x:.2f} {y:.2f}" for x, y in pts) + f' Z" fill="{color}"/>')
    return "".join(out)


def svg(name: str) -> str:
    # Плитка 380×380 с тенью, под ней подпись. Знак и рамка — в сетке 32×32, масштаб 380/32.
    k = 380 / 32
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
<defs><filter id="sh" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="10" stdDeviation="12" flood-color="#000" flood-opacity="0.28"/></filter></defs>
<rect x="66" y="16" width="380" height="380" rx="92" fill="{ORANGE}" filter="url(#sh)"/>
<rect x="66" y="16" width="380" height="380" rx="92" fill="none" stroke="{ORANGE_D}" stroke-width="4"/>
<g transform="translate(66 16) scale({k:.4f})">{frame(WHITE)}{GLYPHS[name]}</g>
<text x="256" y="478" text-anchor="middle" font-family="Inter, Helvetica, Arial, sans-serif" font-size="58" font-weight="900" letter-spacing="3" fill="{WHITE}" stroke="{ORANGE}" stroke-width="16" stroke-linejoin="round" paint-order="stroke">{LABELS[name]}</text>
</svg>'''


def main() -> None:
    out = HERE / "png"
    out.mkdir(exist_ok=True)
    for name in GLYPHS:
        src = out / f"{name}.svg"
        src.write_text(svg(name))
        subprocess.run(["rsvg-convert", str(src), "-o", str(out / f"{name}.png")], check=True)
        src.unlink()
    cells = "".join(f'<image href="{out / (n + ".png")}" x="{20 + i * 280}" y="20" width="256" height="256"/>' for i, n in enumerate(GLYPHS))
    for bg, suffix in (("#0E1621", "dark"), ("#FFFFFF", "light")):
        prev = HERE / f"preview_{suffix}.svg"
        prev.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{20 + len(GLYPHS) * 280}" height="296"><rect width="100%" height="100%" fill="{bg}"/>{cells}</svg>')
        subprocess.run(["rsvg-convert", str(prev), "-o", str(HERE / f"preview_{suffix}.png")], check=True)
        prev.unlink()


if __name__ == "__main__":
    main()
