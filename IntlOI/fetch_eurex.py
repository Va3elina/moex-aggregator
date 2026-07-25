#!/usr/bin/env python3
"""
Международный OI — Eurex Daily/Monthly Statistics (Германия). Только TOTAL —
Eurex не публикует разбивку по типу держателя ни в одном бесплатном продукте
(см. research_summary.md п.2: под MiFID II нет аналога COT-отчёта).

Источники (бесплатно, без ключа, сверено вживую 2026-07-24):
  Daily:   https://www.eurex.com/ex-en/data/statistics/trading-statistics
  Monthly: https://www.eurex.com/ex-en/data/statistics/monthly-statistics
Ссылки на .xls генерируются динамически (опаковые blob-ID в URL, не
предсказуемы по дате) — страница отдаёт их через внутренний search-API:
  /ex-en/data/statistics/trading-statistics/122!search?pageNum=N&hitsPerPage=50&sort=sDate%20desc
  /ex-en/data/statistics/monthly-statistics/3848!search?pageNum=N&hitsPerPage=10&sort=sDate%20desc
Ссылки лежат прямо в серверном HTML (не JS-рендеринг) — обычный httpx.get
+ regex, без браузера. hitsPerPage сервер молча ограничивает 50 (daily)
и явно отдаёт 10 (monthly) — выше не давали больше строк, проверено.

Формат XLS — фиксированная структура шаблона (сверено на живых файлах):
  Daily:   лист "Eurex Daily Statistics", строка заголовков #10, колонка 3 —
           имя продукта, колонка 4 — код, колонка 14 — "Open Interest (prev Day)".
  Monthly: лист "Eurex Monthly Statistics", та же раскладка колонок (3/4),
           колонка 35 — "Open Interest" (на конец месяца).
Активы: DAX Futures (FDAX), Euro-Bund (FGBL), Euro-Bobl (FGBM),
Euro-Schatz (FGBS), Euro-Buxl (FGBX), EURO STOXX 50 (FESX).

Запуск:
    python3 fetch_eurex.py --once                    # сегодняшний daily-файл (первая страница поиска)
    python3 fetch_eurex.py --daily-full-history       # весь daily-архив, сколько отдаёт пагинация
    python3 fetch_eurex.py --monthly-full-history     # весь monthly-архив (глубже daily, десятилетия)
    python3 fetch_eurex.py --daily-full-history --max-pages 5   # ограничить глубину (по 50 файлов/страница)
"""
import argparse
import logging
import re
import sys
from datetime import date, timedelta
from io import BytesIO

import httpx
import pandas as pd

from common import DB_URL, get_engine, upsert_oi_intl_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

DAILY_SEARCH_URL = "https://www.eurex.com/ex-en/data/statistics/trading-statistics/122!search"
MONTHLY_SEARCH_URL = "https://www.eurex.com/ex-en/data/statistics/monthly-statistics/3848!search"

# Канонические продукты: код → человекочитаемое имя для UI.
PRODUCTS: dict[str, str] = {
    "FDAX": "DAX Futures",
    "FGBL": "Euro-Bund",
    "FGBM": "Euro-Bobl",
    "FGBS": "Euro-Schatz",
    "FGBX": "Euro-Buxl",
    "FESX": "EURO STOXX 50",
}

# normalized-имя-в-XLS → код. Eurex несколько раз переименовывал продукты за
# 20+ лет — проверено вживую по годам 2003-2026:
#   2003-2004: старый плоский шаблон отчёта, ед.ч. "Future" (не "Futures"),
#     EURO STOXX 50 назывался "DJ Euro-STOXX 50SM Future" (буквально "SM"
#     как текст, не символ ℠).
#   ~2005-2009: индекс-продукт под совместным брендом Dow Jones — "DJ EURO
#     STOXX 50 Index Futures" (пробел вместо дефиса, "Index" в названии).
#   с 2010-03: "EURO STOXX 50® Index Futures" — бренд DJ отвалился (STOXX
#     стала отдельной от Dow Jones ~2009-2010), DAX/Bund/Bobl/Schatz/Buxl
#     остались во множественном числе "Futures".
# Если появится новый разрыв истории — сверяться так же: скачать живой файл
# за месяц ДО и ПОСЛЕ разрыва, искать все строки с "STOXX"/"DAX"/"BUND" и т.д.
NAME_ALIASES: dict[str, str] = {
    # текущее имя (с 2010-03)
    "DAX FUTURES": "FDAX",
    "EURO-BUND FUTURES": "FGBL",
    "EURO-BOBL FUTURES": "FGBM",
    "EURO-SCHATZ FUTURES": "FGBS",
    "EURO-BUXL FUTURES": "FGBX",
    "EURO STOXX 50 INDEX FUTURES": "FESX",
    # ~2005-2009: DJ-брендированное имя индексного фьючерса
    "DJ EURO STOXX 50 INDEX FUTURES": "FESX",
    # 2003-2004: старый плоский шаблон, единственное число
    "DAX FUTURE": "FDAX",
    "EURO-BUND FUTURE": "FGBL",
    "EURO-BOBL FUTURE": "FGBM",
    "EURO-SCHATZ FUTURE": "FGBS",
    "EURO-BUXL FUTURE": "FGBX",
    "DJ EURO-STOXX 50SM FUTURE": "FESX",
    # 2004-04..2004-08: тот же старый шаблон, но "SM" переставили перед "50"
    # внутри квартала — сверено вживую (monthlystat_200406.xls и соседние).
    "DJ EURO-STOXXSM 50 FUTURE": "FESX",
    # 2004-09..2004-12: та же перестановка "SM", но уже множественное число
    # "Futures" — сверено вживую (monthlystat_200409.xls). Похоже, Eurex в
    # 2004-м несколько раз правил шаблон отчёта прямо на ходу, без единой
    # логики — если появится ещё вариант, искать так же: скачать файл,
    # grep по "STOXX"+"50" в первых 10 колонках.
    "DJ EURO-STOXXSM 50 FUTURES": "FESX",
}


def _normalize_name(name: str) -> str:
    return re.sub(r"[®\s]+", " ", name).strip().upper()


def _resolve_product(normalized_name: str) -> tuple[str, str] | None:
    code = NAME_ALIASES.get(normalized_name)
    if not code:
        return None
    return code, PRODUCTS[code]


def discover_files(search_url: str, filename_pattern: str, hits_per_page: int, max_pages: int) -> list[str]:
    """Скроллит search-API постранично, собирает .xls ссылки, матчащие filename_pattern (regex)."""
    urls: list[str] = []
    seen = set()
    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for page in range(max_pages):
            params = {"pageNum": page, "hitsPerPage": hits_per_page, "sort": "sDate desc"}
            resp = client.get(search_url, params=params)
            if resp.status_code != 200:
                break
            links = re.findall(r'href="(https://www\.eurex\.com/resource/blob/[^"]+\.xls)"', resp.text)
            new_links = [l for l in links if re.search(filename_pattern, l) and l not in seen]
            if not new_links:
                break
            for l in new_links:
                seen.add(l)
                urls.append(l)
            log.info(f"  ...страница {page}: +{len(new_links)} файлов (всего {len(urls)})")
    return urls


def _find_header_row(df: pd.DataFrame) -> int:
    for r in range(min(20, len(df))):
        row = df.iloc[r].astype(str).tolist()
        if "Open Interest" in row or any(str(v).startswith("Open Interest") for v in row):
            return r
    raise ValueError("Не нашли строку заголовков с 'Open Interest'")


# Имя продукта живёт в разных колонках в зависимости от эпохи шаблона:
# плоский список 2003-2004 → колонка 0; дерево категорий (Equity Derivatives
# → Single Stock Futures → сам продукт) с 2005 по сегодня → колонка 3-8 в
# зависимости от глубины вложенности. Поэтому сканируем несколько первых
# колонок на каждой строке, а не полагаемся на фиксированный индекс.
NAME_COLUMN_SCAN_RANGE = 10


def parse_xls(content: bytes, oi_column_prefix: str) -> dict[str, int]:
    """Возвращает {product_code: open_interest} по NAME_ALIASES из содержимого XLS."""
    xl = pd.ExcelFile(BytesIO(content))
    sheet = [s for s in xl.sheet_names if s != "Cover"][0]
    df = xl.parse(sheet, header=None)

    header_row = _find_header_row(df)
    header = df.iloc[header_row].astype(str).tolist()
    oi_col = next((i for i, v in enumerate(header) if v.strip() == oi_column_prefix), None)
    if oi_col is None:
        raise ValueError(f"Колонка '{oi_column_prefix}' не найдена в заголовке: {header}")

    result: dict[str, int] = {}
    for r in range(header_row + 1, len(df)):
        matched = None
        for col_idx in range(min(NAME_COLUMN_SCAN_RANGE, df.shape[1])):
            cell = df.iat[r, col_idx]
            if pd.isna(cell):
                continue
            matched = _resolve_product(_normalize_name(str(cell)))
            if matched:
                break
        if not matched:
            continue
        oi_raw = df.iat[r, oi_col] if oi_col < df.shape[1] else None
        if pd.isna(oi_raw):
            continue
        code, _ = matched
        try:
            result[code] = int(float(oi_raw))
        except (ValueError, TypeError):
            continue
    return result


def build_rows(oi_by_code: dict[str, int], trade_date: date, granularity: str) -> list[dict]:
    rows = []
    for code, name in PRODUCTS.items():
        oi_val = oi_by_code.get(code)
        if oi_val is None:
            continue
        rows.append({
            "exchange": "EUREX", "country": "DE", "asset_code": code, "asset_name": name,
            "category": "TOTAL", "trade_date": trade_date, "granularity": granularity,
            "oi_long": None, "oi_short": None, "oi_total": oi_val,
        })
    return rows


def process_daily(client: httpx.Client, url: str) -> list[dict] | None:
    m = re.search(r"dailystat_(\d{8})\.xls", url)
    if not m:
        return None
    trade_date = date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:8]))
    resp = client.get(url)
    if resp.status_code != 200:
        log.warning(f"{url}: HTTP {resp.status_code}")
        return None
    try:
        oi_by_code = parse_xls(resp.content, "Open Interest (prev Day)")
    except Exception as e:
        log.warning(f"{url}: не распарсен ({e})")
        return None
    return build_rows(oi_by_code, trade_date, "daily")


def process_monthly(client: httpx.Client, url: str) -> list[dict] | None:
    m = re.search(r"monthlystat_(\d{6})\.xls", url)
    if not m:
        return None
    yyyy, mm = int(m.group(1)[:4]), int(m.group(1)[4:6])
    # Открытый интерес на конец месяца — берём последний календарный день месяца
    # как дату точки (приближение; точная дата отчёта Eurex — последний торговый
    # день, обычно совпадает или на 1-2 дня раньше). granularity='monthly'
    # (не 'daily'!) — эта точка НЕ конкурирует с daily-строкой на ту же дату,
    # они теперь разные ключи (см. db/migrations/042 — раньше тут была тихая
    # коллизия PK).
    next_month = date(yyyy + (mm == 12), (mm % 12) + 1, 1)
    trade_date = next_month - timedelta(days=1)
    resp = client.get(url)
    if resp.status_code != 200:
        log.warning(f"{url}: HTTP {resp.status_code}")
        return None
    try:
        oi_by_code = parse_xls(resp.content, "Open Interest")
    except Exception as e:
        log.warning(f"{url}: не распарсен ({e})")
        return None
    return build_rows(oi_by_code, trade_date, "monthly")


def main():
    parser = argparse.ArgumentParser(description="Eurex Daily/Monthly Statistics → open_interest_intl")
    parser.add_argument("--once", action="store_true", help="Только последний daily-файл")
    parser.add_argument("--daily-full-history", action="store_true", help="Весь daily-архив (пагинация)")
    parser.add_argument("--monthly-full-history", action="store_true", help="Весь monthly-архив (пагинация)")
    parser.add_argument("--max-pages", type=int, default=200, help="Лимит страниц пагинации (по 50 daily / 10 monthly файлов за страницу)")
    args = parser.parse_args()

    if not DB_URL:
        log.critical("DB_URL не задан (.env)")
        sys.exit(1)

    engine = get_engine()
    total = 0

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        if args.daily_full_history or args.once:
            max_pages = args.max_pages if args.daily_full_history else 1
            log.info("Ищу daily-файлы...")
            urls = discover_files(DAILY_SEARCH_URL, r"dailystat_\d{8}\.xls", hits_per_page=50, max_pages=max_pages)
            if args.once:
                urls = urls[:1]
            log.info(f"Найдено {len(urls)} daily-файлов, парсю и записываю...")
            for url in urls:
                rows = process_daily(client, url)
                if rows:
                    total += upsert_oi_intl_rows(engine, rows)

        if args.monthly_full_history:
            log.info("Ищу monthly-файлы...")
            urls = discover_files(MONTHLY_SEARCH_URL, r"monthlystat_\d{6}\.xls", hits_per_page=10, max_pages=args.max_pages)
            log.info(f"Найдено {len(urls)} monthly-файлов, парсю и записываю...")
            for url in urls:
                rows = process_monthly(client, url)
                if rows:
                    total += upsert_oi_intl_rows(engine, rows)

    log.info(f"✓ Записано/обновлено {total} строк в open_interest_intl")


if __name__ == "__main__":
    main()
