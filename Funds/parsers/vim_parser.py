"""
Парсер сайта ВИМ Инвестиции (wealthim.ru) для daily holdings.

Что даёт ВИМ vs Cbonds:
  - daily (T+1) обновление vs monthly (T-30) у Cbonds
  - реальные `positions` (штуки) vs только `weight` (%) у Cbonds
  - delta_weight уже посчитан УК — можно сверять с нашим diff'ом

Структура страницы (на 2026-05):
  URL: https://www.wealthim.ru/products/bpif/{slug}/structure/

  HTML:
    div.fund-portfolio
      span.italic_bold_medium → "Портфель фонда (24.05.2026 г.)" → snapshot_date

    div.table-dynamics
      div.row.tr-select × N (каждая = одна позиция)
        div.td-name-item a → "ЛУКОЙЛ, ао, 1-01-00077-A" → asset_name
        div.d-none.d-xl-flex × 3 (desktop-версия колонок):
          [0] <b>15.73 %</b>   → weight
          [1] <b>0.00 %</b>    → delta_weight
          [2] <b>415593</b>    → positions (raw, без separator)

Tickers и slug'и:
  LQDT → wimfl   (Ликвидность — money market, обычно 1 строка)
  EQMX → wimfimb (Индекс Мосбиржи — 40+ акций, главная цель)
  GOLD → wimfg   (Золото — слитки, ~150 позиций по номерам)
  OBLG → wimfrb  (Российские облигации — бонус)
  ESGE → wimfsrc (ESG акции — бонус)
  CNYM → cnym    (Ликвидность в юанях — money market)

Risk: ВИМ может поменять структуру страницы → парсер сломается.
Mitigation: graceful failure (логируем ошибку, возвращаем []), не блокируем
основной cron Cbonds.
"""
import logging
import re
from datetime import date as date_cls
from typing import Optional

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


# Tickers → slug'и на сайте wealthim.ru.
VIM_FUND_SLUGS: dict[str, str] = {
    "LQDT": "wimfl",
    "EQMX": "wimfimb",
    "GOLD": "wimfg",
    "OBLG": "wimfrb",
    "ESGE": "wimfsrc",
    "CNYM": "cnym",
}

BASE_URL = "https://www.wealthim.ru/products/bpif/{slug}/structure/"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Frame-Bot/1.0"
)


def _parse_date(text: str) -> Optional[date_cls]:
    """Извлекает дату из текста вида 'Портфель фонда (24.05.2026 г.)'."""
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text or "")
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date_cls(yyyy, mm, dd)
    except ValueError:
        return None


def _parse_float(text: str) -> Optional[float]:
    """'15.73 %' → 15.73 ; '0.00 %' → 0.0 ; '−2.3%' → -2.3."""
    if not text:
        return None
    # Замена минус-знака (en-dash, minus sign) на ASCII '-'.
    cleaned = text.replace("−", "-").replace("–", "-").strip()
    m = re.search(r"-?\d+(?:[.,]\d+)?", cleaned)
    if not m:
        return None
    return float(m.group(0).replace(",", "."))


def _parse_int(text: str) -> Optional[int]:
    """'415593' → 415593 ; '5,700,363' → 5700363 ; '415 593' → 415593."""
    if not text:
        return None
    cleaned = re.sub(r"[\s, ]", "", text.strip())
    m = re.match(r"-?\d+", cleaned)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def fetch_vim_holdings(ticker: str, timeout: int = 30) -> tuple[Optional[date_cls], list[dict]]:
    """
    Скачивает и парсит структуру одного БПИФ ВИМ.

    Returns:
        (snapshot_date, holdings). holdings — список dict'ов
        с полями name/weight/delta_weight/positions.
        В случае ошибки → (None, []).
    """
    slug = VIM_FUND_SLUGS.get(ticker.upper())
    if not slug:
        log.warning("vim_parser: unknown ticker %s", ticker)
        return None, []

    url = BASE_URL.format(slug=slug)
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        log.warning("vim_parser %s: fetch failed: %s", ticker, e)
        return None, []

    return parse_vim_html(resp.text)


def parse_vim_html(html: str) -> tuple[Optional[date_cls], list[dict]]:
    """
    Парсит HTML страницы /structure/ ВИМ.

    Чистая функция — можно тестировать unit'ами без сети. Принимает
    raw HTML string, возвращает (snapshot_date, holdings).
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1. Дата snapshot'а.
    snapshot_date = None
    date_el = soup.select_one(".fund-portfolio span.italic_bold_medium")
    if date_el:
        snapshot_date = _parse_date(date_el.get_text(" ", strip=True))

    # 2. Строки таблицы.
    # Каждая строка `<div class="row tr-select ...">`. Внутри:
    #   .td-name-item a       → название актива
    #   .d-none.d-xl-flex     → desktop-блоки с числовыми значениями
    #     [0] доля, [1] изменение, [2] штук
    holdings = []
    for row in soup.select(".table-dynamics .row.tr-select"):
        name_el = row.select_one(".td-name-item a")
        if not name_el:
            continue
        name = name_el.get_text(" ", strip=True)
        if not name:
            continue

        # Desktop колонки с числами — отфильтровываем по точному наличию обоих классов.
        # ВИМ использует Bootstrap utility classes; mobile-блоки имеют d-xl-none.
        cells = row.select("div.d-none.d-xl-flex b")
        # Ожидаем 3 значения. Если меньше — graceful skip числовых полей.
        weight = _parse_float(cells[0].get_text(strip=True)) if len(cells) > 0 else None
        delta_weight = _parse_float(cells[1].get_text(strip=True)) if len(cells) > 1 else None
        positions = _parse_int(cells[2].get_text(strip=True)) if len(cells) > 2 else None

        # Пропускаем строки без weight (footnotes, summary rows).
        if weight is None:
            continue

        holdings.append({
            "name": name,
            "weight": weight,
            "delta_weight": delta_weight,
            "positions": positions,
        })

    return snapshot_date, holdings
