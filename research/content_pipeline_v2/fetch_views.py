#!/usr/bin/env python3
"""Сбор ПРОСМОТРОВ исторических постов через веб-превью t.me — для перевывода
порога хайпа с репостов на просмотры.

ЗАЧЕМ. Детектор хайпа калиброван на репостах (fwd_15 против медианы), а веб-превью,
единственный доступный с прод-сервера путь, репостов НЕ отдаёт — только просмотры.
Чтобы не выдумывать порог, берём период, где у нас ЕСТЬ старая разметка
(tg_channel_watch, 5372 поста с флагом promoted за 14.07-11.08), и достаём для тех
же message_id сегодняшние просмотры.

⚠️ ЧЕСТНОЕ ОГРАНИЧЕНИЕ. Просмотры у постов месячной давности НАСЫЩЕНЫ — это
итоговый охват, а не скорость набора. Старый сигнал измерял именно СКОРОСТЬ
(репосты за первые 15 минут). Поэтому здесь можно узнать, различает ли охват
хайповые посты вообще, и получить стартовый порог, но точную замену «скорости»
это не даёт: для неё нужны чекпоинты просмотров по возрасту поста, а их надо
копить с нуля.

Пагинация: t.me/s/<канал>?before=<id> отдаёт посты ДО указанного id. Это позволяет
прыгнуть сразу в нужный диапазон, а не идти страницами от сегодняшнего дня.
"""
import argparse
import html as _html
import os
import re
import sys
import time
import urllib.request
from html.parser import HTMLParser

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def _parse_views(s: str):
    """«1.2K» / «3,4M» → int. Без разбора суффикса 1.2K стало бы 1."""
    if not s:
        return None
    s = s.replace(",", ".").strip()
    m = re.match(r"^([\d.]+)\s*([KMkm])?$", s)
    if not m:
        return None
    try:
        v = float(m.group(1))
    except ValueError:
        return None
    return int(v * {"k": 1_000, "m": 1_000_000}.get((m.group(2) or "").lower(), 1))


class _P(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows = []
        self._cur = None
        self._in_views = False

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        cls = a.get("class", "") or ""
        if "tgme_widget_message" in cls and a.get("data-post"):
            self._cur = {"post": a["data-post"], "views": None, "dt": None}
            self.rows.append(self._cur)
        if self._cur is None:
            return
        if tag == "time" and a.get("datetime") and not self._cur["dt"]:
            self._cur["dt"] = a["datetime"]
        if "tgme_widget_message_views" in cls:
            self._in_views = True

    def handle_endtag(self, tag):
        if self._in_views and tag == "span":
            self._in_views = False

    def handle_data(self, data):
        if self._cur is not None and self._in_views and self._cur["views"] is None:
            v = _parse_views(data.strip())
            if v is not None:
                self._cur["views"] = v


def fetch_page(channel: str, before: int | None):
    url = f"https://t.me/s/{channel}" + (f"?before={before}" if before else "")
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read().decode("utf-8", "replace")


def walk(channel: str, start_id: int, stop_id: int, delay: float, out):
    """Идём НАЗАД от start_id до stop_id. Останавливаемся также если страница не
    сдвинула минимальный id — иначе цикл на краю истории стал бы бесконечным."""
    before = start_id
    seen = 0
    pages = 0
    while before and before > stop_id:
        try:
            html_page = fetch_page(channel, before)
        except Exception as e:
            print(f"  {channel} before={before}: {type(e).__name__}: {e}", file=sys.stderr)
            break
        p = _P()
        p.feed(html_page)
        ids = []
        for row in p.rows:
            chan, _, mid = row["post"].partition("/")
            if not mid.isdigit():
                continue
            mid = int(mid)
            ids.append(mid)
            if row["views"] is not None and mid <= start_id:
                out.write(f"{channel}|{mid}|{row['views']}|{row['dt'] or ''}\n")
                seen += 1
        pages += 1
        if not ids:
            break
        nxt = min(ids)
        if nxt >= before:      # страница не сдвинулась — край истории
            break
        before = nxt
        if pages % 25 == 0:
            print(f"  {channel}: страниц {pages}, постов {seen}, дошли до id {before}",
                  file=sys.stderr)
        time.sleep(delay)
    print(f"  {channel}: готово — страниц {pages}, постов с просмотрами {seen}",
          file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--start", type=int, required=True, help="id, ОТ которого идём назад")
    ap.add_argument("--stop", type=int, required=True, help="id, ДО которого идём")
    ap.add_argument("--out", required=True)
    ap.add_argument("--delay", type=float, default=0.4)
    a = ap.parse_args()
    mode = "a" if os.path.exists(a.out) else "w"
    with open(a.out, mode, encoding="utf-8") as f:
        walk(a.channel, a.start, a.stop, a.delay, f)


if __name__ == "__main__":
    main()
