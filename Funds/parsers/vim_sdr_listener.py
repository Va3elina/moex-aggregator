"""
ВИМ SDR Listener — извлекает архивные SCHA с сайта ВИМ.

URL pattern:
  https://www.wealthim.ru/about/disclosure/pif/bpif/{slug}/reports/?PAGEN_1=N
  https://www.wealthim.ru/about/disclosure/pif/opif/{slug}/reports/?PAGEN_1=N
                                                  ↑
                                       bpif (биржевой) или opif (открытый)

Slug'и (ВИМ-фонды в нашем whitelist):
  LQDT      → bpif/wimfl         (БПИФ)
  EQMX      → bpif/wimfimb       (БПИФ)
  GOLD      → bpif/wimfg         (БПИФ)
  OPIF-1003 → opif/wim-fonds-aktsii    (ОПИФ ВИМ-Акции)
  OPIF-54   → opif/wim-fonds-kaznachey (ОПИФ Казначейский)
  OPIF-9165 → opif/wim-fonds-rante     (ОПИФ Облигации Рантье)

NB: реальные slug'и для ОПИФ надо проверить — если URL даёт 404, пробуем
альтернативы или ищем через главную страницу /products/opif/.

Файлы на странице — ZIP'ы с PDF внутри (или прямо PDF). Имя файла
содержит дату + тип:
  26_04_30_SCHA_BPIF_IndeksMosBirzhi.pdf      ← SCHA за 30 апреля
  26_04_30_Prirost_BPIF_IndeksMosBirzhi.pdf   ← Прирост

Парсим: имя → дата (YY_MM_DD) + тип (SCHA / Prirost) + ticker.
Для ВИМ-сайта это самый надёжный путь, потому что названия фондов
в URL стабильны.
"""
import logging
import re
import subprocess
from datetime import date as date_cls
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


BASE_URL = "https://www.wealthim.ru"

# ticker → (фонд_тип, slug) для ВИМ.
VIM_TICKER_TO_SLUG = {
    "LQDT": ("bpif", "wimfl"),
    "EQMX": ("bpif", "wimfimb"),
    "GOLD": ("bpif", "wimfg"),
    # ОПИФ — slug'и требуют ручной проверки. Дополним когда выяснится.
    "OPIF-1003": ("opif", "wim-fonds-aktsii"),    # ВИМ-Акции
    "OPIF-54": ("opif", "wim-fonds-kaznachey"),   # Казначейский
    "OPIF-9165": ("opif", "wim-fonds-rante"),     # Облигации Рантье
}


# Регекс для парсинга имени файла: 26_04_30_SCHA_BPIF_IndeksMosBirzhi.pdf
# Группы: год2, месяц, день, тип, фонд_тип, остальное
FILENAME_PATTERN = re.compile(
    r"(\d{2})_(\d{2})_(\d{2})_(\w+?)_(BPIF|OPIF)_(.+?)\.(?:pdf|zip)",
    re.IGNORECASE,
)


def _curl_get(url: str, timeout: int = 60) -> Optional[bytes]:
    """curl subprocess (e-disclosure имел anti-scraping, ВИМ — нет)."""
    try:
        proc = subprocess.run(
            [
                "curl", "-skL",
                "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                "--max-time", str(timeout),
                url,
            ],
            capture_output=True,
            timeout=timeout + 5,
        )
        if proc.returncode != 0:
            log.error("curl failed (exit=%d): %s", proc.returncode, proc.stderr.decode()[:200])
            return None
        return proc.stdout
    except Exception as e:
        log.error("curl exception: %s", e)
        return None


def _parse_filename(filename: str) -> Optional[dict]:
    """
    '26_04_30_SCHA_BPIF_IndeksMosBirzhi.pdf' →
        {'snapshot_date': date(2026, 4, 30), 'doc_type': 'scha', 'fund_type': 'bpif'}

    None если файл не соответствует SDR-формату.
    """
    m = FILENAME_PATTERN.search(filename)
    if not m:
        return None
    yy, mm, dd, doc_kw, fund_type, _rest = m.groups()
    try:
        year = 2000 + int(yy)
        snap = date_cls(year, int(mm), int(dd))
    except ValueError:
        return None

    doc_lower = doc_kw.lower()
    if "scha" in doc_lower or "sсha" in doc_lower:
        doc_type = "scha"
    elif "prirost" in doc_lower:
        doc_type = "prirost"
    else:
        return None

    return {
        "snapshot_date": snap,
        "doc_type": doc_type,
        "fund_type": fund_type.lower(),
    }


def list_vim_scha_for_ticker(
    ticker: str,
    max_pages: int = 6,
    timeout: int = 60,
) -> list[dict]:
    """
    Обходит pagination архива одного ВИМ-фонда и возвращает все SCHA + Prirost.

    Returns:
        Список dict'ов:
        [{
            'ticker': 'EQMX',
            'doc_type': 'scha' | 'prirost',
            'snapshot_date': date(2026, 4, 30),
            'download_url': 'https://www.wealthim.ru/upload/...26_04_30_SCHA_BPIF_IndeksMosBirzhi.pdf',
            'filename': '...',
        }]
    """
    info = VIM_TICKER_TO_SLUG.get(ticker)
    if not info:
        log.warning("vim_sdr_listener: unknown ticker %s", ticker)
        return []
    fund_type, slug = info

    documents = []
    seen_urls: set = set()

    for page_no in range(1, max_pages + 1):
        url = f"{BASE_URL}/about/disclosure/pif/{fund_type}/{slug}/reports/?PAGEN_1={page_no}&SIZEN_1=20"
        content = _curl_get(url, timeout=timeout)
        if not content:
            break
        html = content.decode("utf-8", errors="replace")

        page_docs = _parse_reports_page(html, ticker)
        if not page_docs:
            # Пустая страница — дальше не идём
            break

        new_count = 0
        for d in page_docs:
            if d["download_url"] not in seen_urls:
                seen_urls.add(d["download_url"])
                documents.append(d)
                new_count += 1

        # Если ни одного нового документа — нет смысла продолжать.
        if new_count == 0:
            break

    return documents


def _parse_reports_page(html: str, ticker: str) -> list[dict]:
    """Парсит одну страницу /reports/?PAGEN_1=N, возвращает SCHA + Prirost."""
    soup = BeautifulSoup(html, "html.parser")
    documents = []

    # Ссылки на файлы — все href с .pdf или .zip в /upload/...
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/upload/" not in href:
            continue
        if not (href.endswith(".pdf") or href.endswith(".zip")):
            continue

        # Полный URL
        full_url = urljoin(BASE_URL, href)
        # Имя файла — последняя часть пути.
        filename = href.rsplit("/", 1)[-1]

        parsed = _parse_filename(filename)
        if not parsed:
            continue

        documents.append({
            "ticker": ticker,
            "doc_type": parsed["doc_type"],
            "snapshot_date": parsed["snapshot_date"],
            "fund_type": parsed["fund_type"],
            "download_url": full_url,
            "filename": filename,
        })

    return documents


def download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Скачивает SCHA-файл с сайта ВИМ. Это либо PDF, либо ZIP."""
    return _curl_get(url, timeout=timeout)


# ─────────────────────────────────────────────────────────────────────
# CLI для тестов
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from collections import Counter

    tickers = sys.argv[1:] if len(sys.argv) > 1 else list(VIM_TICKER_TO_SLUG)
    all_docs = []
    for t in tickers:
        docs = list_vim_scha_for_ticker(t)
        print(f"  {t:12s}: {len(docs)} документов")
        all_docs.extend(docs)
    print()
    print(f"Total: {len(all_docs)}")
    print(f"By ticker: {dict(Counter(d['ticker'] for d in all_docs))}")
    print(f"By type: {dict(Counter(d['doc_type'] for d in all_docs))}")
    print()
    # Show top 3 SCHA per ticker
    by_ticker_scha: dict = {}
    for d in all_docs:
        if d["doc_type"] == "scha":
            by_ticker_scha.setdefault(d["ticker"], []).append(d)
    for ticker, lst in by_ticker_scha.items():
        lst.sort(key=lambda d: d["snapshot_date"], reverse=True)
        print(f"\n=== {ticker} ({len(lst)} SCHA) ===")
        for d in lst[:3]:
            print(f"  {d['snapshot_date']}  {d['filename']}")
