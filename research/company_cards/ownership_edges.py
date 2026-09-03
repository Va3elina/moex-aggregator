#!/usr/bin/env python3
"""
Рёбра владения эмитент→эмитент из страниц акционеров smart-lab.

ЗАЧЕМ. Новость про Озон касается АФК Системы, потому что Система владеет Озоном.
Векторно эти новости далеки, словами связь не выводится — нужно ребро. Механика
хранения уже есть: world_facts, kind='связь' (миграция 061). В базе всего 2 ребра,
потому что не было чем резолвить владельцев, названных словами.

⚠️ РЁБРА КУРИРУЕМЫЕ. Скрипт НИЧЕГО не пишет в БД. Он готовит кандидатов и
раскладывает их на три корзины: уверенный матч, неоднозначный и не-эмитент.
Подтверждает человек — потому что цена ошибки здесь высокая: ложное ребро заставит
агента объяснять движение одной бумаги событиями чужой компании.

⚠️ ПОЧЕМУ НЕ ПОИСК ПО ПОДСТРОКЕ. «Газпром» матчит и Газпром нефть, «Система» —
любое слово с этим корнем. Поэтому: нормализация → ТОЧНОЕ совпадение по словарю →
всё остальное уходит человеку, а не угадывается.

Использование:
    python ownership_edges.py --tickers AFKS,OZON,MTSS      # проба
    python ownership_edges.py --all --out edges.json        # вся вселенная
"""

import argparse
import html
import json
import logging
import re
import sys
import time
import urllib.request
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
POLITE_DELAY = 0.5

log = logging.getLogger("ownership_edges")

# Владельцы, которые НЕ являются эмитентами Мосбиржи: физлица, госструктуры,
# непубличные холдинги, обобщения вроде «Американские инвесторы». Ребро
# бумага→бумага из них не получится, но для вопроса «кто за этим стоит» полезно.
NOT_ISSUER_MARKERS = (
    "прочие", "другие", "остальные", "free float", "free-float", "в свободном",
    "миноритар", "физ.лица", "физлица", "юр.лица", "юрлица", "нерезидент",
    "инвесторы", "акционеры", "казначейск", "квазиказначейск", "менеджмент",
    "правительство", "росимущество", "федеральное агентство", "министерств",
    "субъект", "администрац", "банк россии", "цб рф", "нпф", "пенсионн",
)

# Владельцы-холдинги, названные не так, как торгуемый эмитент. Это ЕДИНСТВЕННОЕ
# место, где связь задаётся руками, и каждая строка проверена глазами.
HOLDING_ALIASES = {
    "система": "AFKS",
    "афк система": "AFKS",
    "афк «система»": "AFKS",
    "газпром": "GAZP",
    "газпром энергохолдинг": "GAZP",   # 100% дочерняя Газпрома, сама не торгуется
    "интер рао": "IRAO",
    "лукойл": "LKOH",
    "россети": "FEES",
    "en+group": "ENPG",
    "en+ group": "ENPG",
    "эн+ груп": "ENPG",
    "втб": "VTBR",
    "банк втб": "VTBR",
    "сбербанк": "SBER",
    "интеррос": None,                  # непубличный холдинг Потанина
    "ростех": None,
    "росатом": None,
    "севергрупп": None,
}


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    day = date.today().strftime("%Y%m")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    for path, level in ((LOG_DIR / f"ownership_{day}.log", logging.DEBUG),
                        (LOG_DIR / f"ownership_errors_{day}.log", logging.WARNING)):
        h = logging.FileHandler(path, encoding="utf-8")
        h.setLevel(level); h.setFormatter(fmt); log.addHandler(h)
    c = logging.StreamHandler(sys.stderr)
    c.setLevel(logging.INFO); c.setFormatter(fmt); log.addHandler(c)


def strip_tags(s):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", s))).strip()


def normalize(name: str) -> str:
    """«ПАО "Газпром нефть"» → «газпром нефть». Организационная форма и кавычки — шум."""
    s = name.lower().replace("ё", "е")
    s = re.sub(r"[«»\"'`]", "", s)
    s = re.sub(r"\b(публичное|открытое|закрытое)\s+акционерное\s+общество\b", " ", s)
    s = re.sub(r"\b(пао|оао|зао|ооо|ao|ак|нк|гк|пjsc|jsc|plc|ltd|inc|group)\b", " ", s)
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^\w\s+]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def fetch(url, cache_dir):
    cached = None
    if cache_dir:
        cached = cache_dir / (re.sub(r"[^A-Za-z0-9]+", "_", url).strip("_") + ".html")
        if cached.exists():
            return cached.read_text(encoding="utf-8", errors="replace")
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as ex:
        if ex.code == 404:
            log.warning("404: %s", url)
            return None
        log.error("HTTP %s: %s", ex.code, url)
        return None
    except Exception as ex:
        log.error("сеть: %s — %s", url, ex)
        return None
    time.sleep(POLITE_DELAY)
    if cached:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cached.write_text(body, encoding="utf-8")
    return body


def parse_shareholders(page_html):
    holders = []
    for table in re.findall(r"<table[^>]*>.*?</table>", page_html, re.S):
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
            v = [strip_tags(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
            if len(v) == 2 and v[1].endswith("%"):
                pct = v[1].rstrip("%").replace(",", ".")
                try:
                    holders.append({"holder": v[0], "share_pct": float(pct)})
                except ValueError:
                    continue
    plain = strip_tags(re.sub(r"<(script|style)[^>]*>.*?</\1>", "", page_html, flags=re.S))
    m = re.search(r"Дата последнего обновления этой структуры:\s*([\d.]+)", plain)
    return holders, (m.group(1) if m else None)


def build_index(draft_path: Path):
    """Словарь «нормализованное имя → тикер эмитента» из справочника."""
    data = json.loads(draft_path.read_text(encoding="utf-8"))
    idx = {}
    for row in data["кандидаты"]:
        key = row["issuer_key"]
        for field in ("oi_display_name", "fund_asset_name", "smartlab_ticker", "issuer_key"):
            val = row.get(field)
            if not val:
                continue
            # Подписи вида «Сбербанк (прив)» относятся к бумаге, а не к владельцу —
            # скобочный хвост убираем нормализацией.
            n = normalize(val)
            if n and n not in idx:
                idx[n] = key
    for name, key in HOLDING_ALIASES.items():
        idx[normalize(name)] = key
    return idx


def classify(holder: str, idx: dict):
    """Вернуть (корзина, тикер|None). Никаких догадок по подстроке."""
    n = normalize(holder)
    low = holder.lower()
    if any(mark in low for mark in NOT_ISSUER_MARKERS):
        return "не эмитент", None
    if n in idx:
        return ("не эмитент", None) if idx[n] is None else ("уверенно", idx[n])
    # Кандидаты по вхождению — ТОЛЬКО чтобы показать человеку, не для записи.
    near = sorted({t for k, t in idx.items() if t and (k in n or n in k) and len(k) > 3})
    if near:
        return "неоднозначно", near
    return "не найден", None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", help="через запятую")
    ap.add_argument("--all", action="store_true", help="вся вселенная из issuer_draft.json")
    ap.add_argument("--draft", type=Path, default=BASE_DIR / "issuer_draft.json")
    ap.add_argument("--cache-dir", type=Path)
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()
    setup_logging()

    draft = json.loads(a.draft.read_text(encoding="utf-8"))
    idx = build_index(a.draft)
    if a.all:
        tickers = sorted({r["smartlab_ticker"] for r in draft["кандидаты"] if r.get("smartlab_ticker")})
    else:
        tickers = [t.strip().upper() for t in (a.tickers or "").split(",") if t.strip()]
    log.info("бумаг к обходу: %d, имён в индексе: %d", len(tickers), len(idx))

    result = {"собрано": datetime.now().isoformat(timespec="seconds"),
              "рёбра": [], "неоднозначно": [], "не_эмитенты": [], "без_страницы": []}
    for i, t in enumerate(tickers, 1):
        page = fetch(f"https://smart-lab.ru/q/{t}/shareholders/", a.cache_dir)
        if not page:
            result["без_страницы"].append(t)
            continue
        holders, as_of = parse_shareholders(page)
        if not holders:
            log.warning("%s: страница есть, держателей ноль", t)
            result["без_страницы"].append(t)
            continue
        for h in holders:
            bucket, val = classify(h["holder"], idx)
            rec = {"эмитент": t, "владелец": h["holder"], "доля": h["share_pct"],
                   "структура_от": as_of}
            if bucket == "уверенно" and val != t:
                rec["владелец_тикер"] = val
                result["рёбра"].append(rec)
            elif bucket == "неоднозначно":
                rec["кандидаты"] = val
                result["неоднозначно"].append(rec)
            elif bucket == "не эмитент":
                result["не_эмитенты"].append(rec)
        if i % 20 == 0:
            log.info("пройдено %d/%d, рёбер %d", i, len(tickers), len(result["рёбра"]))

    log.info("ИТОГ: рёбер=%d неоднозначных=%d не-эмитентов=%d без страницы=%d",
             len(result["рёбра"]), len(result["неоднозначно"]),
             len(result["не_эмитенты"]), len(result["без_страницы"]))
    out = json.dumps(result, ensure_ascii=False, indent=1)
    (a.out.write_text(out, encoding="utf-8") if a.out else print(out))


if __name__ == "__main__":
    main()
