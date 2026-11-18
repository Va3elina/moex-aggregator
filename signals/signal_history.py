"""История похожих случаев под сигналом @framesignalbot (Вадим 18.11.2026, макет «Уведомления FRAME · Сигналы»):
раскрывающийся блок <blockquote expandable> — прошлые такие же события и что было с ценой после.

Расчёт «что было после» — тот же, что у «Главного» сайта (api/services/hot.py: price_after, base_up, cluster_firsts,
_prices): изменение цены за N торговых дней от закрытия следующего за событием дня; «в обычный день» — доля всех
дней с ростом за тот же срок. Это факты прошлого, не прогноз.
"""
from __future__ import annotations

import datetime as dt
import html
from typing import Iterable, Sequence

MS = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]
MAX_ROWS = 5


def day_label(d) -> str:
    d = d if isinstance(d, dt.date) else dt.date.fromisoformat(str(d)[:10])
    return f"{d.day} {MS[d.month - 1]} {d.year}"


def pct(v: float | None, nd: int = 1) -> str:
    if v is None:
        return "—"
    v = round(v, nd) + 0.0
    s = f"{abs(v):.{nd}f}".replace(".", ",")
    return ("+" if v > 0 else "−" if v < 0 else "") + s + "%"


def plural(n: int, one: str, few: str, many: str) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def price_after(px: Sequence[tuple], day: dt.date, n: int) -> float | None:
    """Как hot.price_after, но без выдумки: событие раньше начала ряда цен или в дыре данных (ближайшая цена дальше
    недели) — None. Иначе hot отсчитал бы от первой цены в базе (ревью 18.11: золото до 2020 давало +4,9% на всё)."""
    from bisect import bisect_left
    from api.services.hot import price_after as pa
    if not px or day < px[0][0]:
        return None
    i = bisect_left([d for d, _ in px], day)
    if i >= len(px) or (px[i][0] - day).days > 7:
        return None
    return pa(px, day, n)


def base_up(px: Sequence[tuple], since: dt.date, n: int) -> int | None:
    from api.services.hot import base_up as bu
    return bu(px, since, n)


def episodes(hits: Sequence[int], last: int, gap: int) -> list[int]:
    """Первые дни прошлых эпизодов (подряд идущие попадания — один случай); нынешний эпизод — не прошлый."""
    from api.services.hot import past_episodes
    return past_episodes(list(hits), last, gap)


def block(title: str, horizons: Sequence[str], rows: Iterable[tuple], footer: str | None = None) -> str:
    """<blockquote expandable>: заголовок, строки «дата · через … · через …», подпись снизу. Пусто — пустая строка."""
    rows = list(rows)[-MAX_ROWS:][::-1]
    if not rows:
        return ""
    lines = [f"<b>{html.escape(title)}</b>", html.escape(" · ".join(["Дата", *horizons]))]
    lines += [html.escape(" · ".join([day_label(d), *[pct(v) for v in vals]])) for d, vals in rows]
    if footer:
        lines.append(html.escape(footer))
    return "\n<blockquote expandable>" + "\n".join(lines) + "</blockquote>"


def cases_title(n: int) -> str:
    t = f"История · {n} {plural(n, 'похожий случай', 'похожих случая', 'похожих случаев')}"
    return t + (f", последние {MAX_ROWS}" if n > MAX_ROWS else "")


def imoex(db) -> list[tuple]:
    """Индекс МосБиржи по дням: [(дата, закрытие)] — «что было с рынком после» у рыночных сигналов."""
    from sqlalchemy import text
    return [(r[0], float(r[1])) for r in db.execute(text(
        "SELECT trade_date, close FROM index_data WHERE secid = 'IMOEX' AND close > 0 ORDER BY 1"))]


def asset_prices(db, sec: str) -> list[tuple]:
    """Цена базового актива фьючерса — как у «Главного» сайта (hot._prices)."""
    from api.services.hot import _prices
    return _prices(db, [sec]).get(sec, [])


def session():
    import os
    url = os.environ.get("DB_URL", "")
    if "@db:" in url:
        os.environ["DB_URL"] = url.replace("@db:", "@127.0.0.1:")
    from api.database import SessionLocal
    return SessionLocal()
