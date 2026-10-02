"""Значки уведомлений роботов → свой пак эмодзи бота (PNG 100×100, прозрачный фон).

Дизайн — холст «Уведомления роботов» (https://claude.ai/artifact/CkPF3kiPWCijdey1QuwDxK), доработан под светлую тему:
серые значки залиты (контур #B8C3CE на белом не виден), «итоги» — плитка (не путать с лесенкой), пустые ступени — полупрозрачные.
Фон у пака один на обе темы, поэтому цвета средние: видны и на тёмном #182533, и на белом.
Запуск: python emoji/make_icons.py → emoji/png/*.png и emoji/icons.json (имя → файл, запасной эмодзи).
Нужен rsvg-convert (brew install librsvg).
"""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
ORANGE, BLUE, GREEN, RED, GREY, AMBER, INK = "#FF5C2B", "#3E8BFF", "#1FA463", "#E5484D", "#8B98A5", "#F5A524", "#1B1B1B"
LINE = 'fill="none" stroke-linecap="round" stroke-linejoin="round"'


def arrow_up(color):
    return f'<path d="M10 14.4V5.9M6.4 9.3L10 5.7l3.6 3.6" {LINE} stroke="{color}" stroke-width="2.2"/>'


def arrow_down(color):
    return f'<path d="M10 5.6v8.5M6.4 10.7L10 14.3l3.6-3.6" {LINE} stroke="{color}" stroke-width="2.2"/>'


def ladder(k: int) -> str:
    """Шесть ступеней лесенки, первые k исполнены (оранжевые), остальные ждут (серые полупрозрачные)."""
    out = []
    for i in range(6):
        h = 14 - 2 * i
        fill = f'fill="{ORANGE}"' if i < k else f'fill="{GREY}" fill-opacity="0.45"'
        out.append(f'<rect x="{0.9 + 3.2 * i:.1f}" y="{17 - h}" width="2.4" height="{h}" rx="0.9" {fill}/>')
    return "".join(out)


sys.path.insert(0, str(HERE.parent))
from robots_tg import ICONS as FALLBACK  # noqa: E402  запасные эмодзи — там же, где их подставляют в сообщения

DRAW: dict[str, tuple[str, str]] = {           # имя → (запасной эмодзи — для пака берётся из robots_tg.ICONS; рисунок)
    "вход": ("🔵", f'<circle cx="10" cy="10" r="9" fill="{BLUE}"/><circle cx="10" cy="10" r="3.4" fill="#fff"/>'),
    "прибыль": ("🟢", f'<circle cx="10" cy="10" r="9" fill="{GREEN}"/>' + arrow_up("#fff")),
    "убыток": ("🔴", f'<circle cx="10" cy="10" r="9" fill="{RED}"/>' + arrow_down("#fff")),
    "нет сделок": ("⚪️", f'<circle cx="10" cy="10" r="9" fill="{GREY}"/><path d="M6.3 10h7.4" {LINE} stroke="#fff" stroke-width="2.2"/>'),
    "итоги": ("📊", f'<rect x="1.5" y="1.5" width="17" height="17" rx="4.5" fill="{ORANGE}"/>'
                   '<rect x="5" y="9.5" width="2.6" height="5.5" rx="1" fill="#fff"/><rect x="8.7" y="5" width="2.6" height="10" rx="1" fill="#fff"/>'
                   '<rect x="12.4" y="7.5" width="2.6" height="7.5" rx="1" fill="#fff"/>'),
    "покупка": ("↗️", f'<path d="M4.8 15.2L15 5M8 4.8h7.2V12" {LINE} stroke="{GREEN}" stroke-width="2.6"/>'),
    "продажа": ("↘️", f'<path d="M4.8 4.8L15 15M15.2 8v7.2H8" {LINE} stroke="{RED}" stroke-width="2.6"/>'),
    "цель": ("🎯", f'<circle cx="10" cy="10" r="8.1" fill="none" stroke="{ORANGE}" stroke-width="1.9"/>'
                  f'<circle cx="10" cy="10" r="4.5" fill="none" stroke="{ORANGE}" stroke-width="1.9"/><circle cx="10" cy="10" r="1.7" fill="{ORANGE}"/>'),
    "таймер": ("⏱️", f'<rect x="8" y="0.3" width="4" height="2.2" rx="1" fill="{GREY}"/><circle cx="10" cy="10.9" r="8.4" fill="{GREY}"/>'
                    f'<path d="M10 6.6v4.6l3 1.9" {LINE} stroke="#fff" stroke-width="2"/>'),
    "стоп": ("⏹️", f'<rect x="2" y="2" width="16" height="16" rx="4.2" fill="{GREY}"/><rect x="6.6" y="6.6" width="6.8" height="6.8" rx="1.3" fill="#fff"/>'),
    "сбой": ("⚠️", f'<path d="M10 2.4L18.4 17H1.6z" fill="{AMBER}" stroke="{AMBER}" stroke-width="1.6" stroke-linejoin="round"/>'
                  f'<path d="M10 7.6v4.4" {LINE} stroke="{INK}" stroke-width="2.1"/><circle cx="10" cy="14.5" r="1.15" fill="{INK}"/>'),
    "внимание": ("ℹ️", f'<circle cx="10" cy="10" r="9" fill="{AMBER}"/><circle cx="10" cy="5.9" r="1.3" fill="{INK}"/>'
                      f'<path d="M10 9.2v5.2" {LINE} stroke="{INK}" stroke-width="2.2"/>'),
    **{f"лесенка {k}": ("", ladder(k)) for k in range(7)},
}
ICONS = {k: (FALLBACK[k], body) for k, (_, body) in DRAW.items()}


def main():
    (HERE / "svg").mkdir(exist_ok=True); (HERE / "png").mkdir(exist_ok=True)
    meta = {}
    for i, (name, (emoji, body)) in enumerate(ICONS.items()):
        slug = f"{i:02d}"
        svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 20 20">{body}</svg>'
        (HERE / "svg" / f"{slug}.svg").write_text(svg)
        subprocess.run(["rsvg-convert", "-w", "100", "-h", "100", "-o", str(HERE / "png" / f"{slug}.png"), str(HERE / "svg" / f"{slug}.svg")], check=True)
        meta[name] = dict(file=f"png/{slug}.png", emoji=emoji)
    (HERE / "icons.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1))
    print(f"значков: {len(meta)} → {HERE / 'png'}")


if __name__ == "__main__":
    main()
