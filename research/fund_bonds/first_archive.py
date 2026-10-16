"""Ссылки на справки о СЧА Первой за 2021–2023 — разово, по снимкам веб-архива.

Первая держит на странице фонда только последние 3 года, но старые файлы на first-am.ru живы
(/download/<id>/<hash>.pdf). Ссылки на них сохранились в снимках страниц в web.archive.org.
Веб-архив — только чтобы найти ссылки; сами файлы качает signals/scha_reread.py --list с first-am.ru
(белый список доменов сборщика не трогаем).

    .venv/bin/python research/fund_bonds/first_archive.py            # → research/fund_bonds/first_archive/<TICKER>.tsv

Справка за 04.2022 у Первой подписана без года («… за апрель») — год берём у соседней справки
по странице (сайт сортирует свежие сверху). Ошибку поймает агент: дата в справке ≠ месяц задания.
"""
from __future__ import annotations

import html as _html
import json
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from Funds import scha_collector as C  # noqa: E402

OUT = Path(__file__).resolve().parent / "first_archive"
SNAPSHOTS = 4  # самые ранние снимки страницы — на них самые старые ссылки


def curl(url: str) -> bytes:
    return subprocess.run(["curl", "-sL", "--max-time", "90", url], capture_output=True).stdout


def docs_on_page(body: str, page_url: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    prev: tuple[int, int] | None = None
    for m in re.finditer(r'<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>', body, re.S):
        title = C._text(m.group(2))
        if not C._IS_SCHA.search(title) or C._NOT_SCHA.search(title):
            continue
        period, _ = C.date_from_text(title)
        if not period:
            mw = re.search(r"за\s+" + C._MONTH_WORD + r"\s*$", title, re.I)
            if not (mw and prev):
                continue
            mon = next(v for k, v in C._MONTHS.items() if mw.group(1).lower().startswith(k))
            year = prev[0] if mon < prev[1] else prev[0] - 1
            period = f"{year:04d}-{mon:02d}"
        y, mo = map(int, period.split("-"))
        prev = (y, mo)
        href = _html.unescape(m.group(1))
        href = re.sub(r"^https?://web\.archive\.org/web/\d+(?:id_)?/", "", href)
        out.append((period, urllib.parse.urljoin(page_url, href).replace("://www.first-am.ru", "://first-am.ru")))
    return out


def main() -> int:
    OUT.mkdir(exist_ok=True)
    for tk, (kind, page) in C.SOURCES.items():
        if "first-am.ru" not in page:
            continue
        path = page.split("first-am.ru/", 1)[1]
        cdx = json.loads(curl("https://web.archive.org/cdx/search/cdx?url=first-am.ru/" + path
                              + "&output=json&filter=statuscode:200&collapse=digest&limit=20") or b"[]")
        found: dict[str, str] = {}
        for row in cdx[1:1 + SNAPSHOTS]:
            body = curl(f"https://web.archive.org/web/{row[1]}id_/{row[2]}").decode("utf-8", "ignore")
            for p, u in docs_on_page(body, page):
                found.setdefault(p, u)
            time.sleep(2)
        live = {d.period for d in C.list_html(tk, page)}
        old = {p: u for p, u in found.items() if p not in live}
        (OUT / f"{tk}.tsv").write_text("".join(f"{p}\t{old[p]}\n" for p in sorted(old)))
        print(f"{tk:10} снимков {max(len(cdx) - 1, 0):2} · в архиве {len(found):3} · нет на сайте {len(old):3}"
              + (f" ({min(old)}..{max(old)})" if old else ""))
        time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
