"""
НРД (e-disclosure.ru) listener — извлекает список SCHA-документов фондов.

Архитектура:
  files.aspx?id={company_id}&type=23 = страница "Раскрытие УК ПИФов".

  На странице — стандартизированная структура:
    <div>УК ПИФ</div>                 ← раздел
    <div>{fund_name}</div>            ← название фонда #1
    <div class="spaceTbl">
      <table class="files-table">
        <tr><th>№</th><th>Тип документа</th>...</tr>
        <tr>
          <td>1</td>
          <td class="type-cell">Справка о стоимости чистых активов</td>
          <td>Отчётная дата: 30.04.2026</td>
          <td class="date-cell">18.05.2026</td>
          <td><a class="file-link" href=".../FileLoad.ashx?Fileid=1927973">zip, 162 KB</a></td>
        </tr>
        ...
      </table>
    </div>
    <div></div>                       ← разделитель
    <div>{fund_name}</div>            ← название фонда #2
    ...

list_scha_documents(company_id, fund_filter) обходит все блоки фондов и
возвращает плоский список SCHA + Prirost документов для нужных фондов.

Сопоставление fund_name → ticker:
  УК публикуют официальные названия типа "Биржевой паевой инвестиционный
  фонд рыночных финансовых инструментов «Индекс МосБиржи»". У нас в БД
  фонды по тикеру: EQMX. Маппинг через подстроки в названии — см.
  TICKER_PATTERNS ниже.
"""
import logging
import re
import subprocess
from datetime import date as date_cls
from typing import Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


def _curl_get(url: str, timeout: int = 60) -> Optional[bytes]:
    """
    Скачивает URL через curl (subprocess).

    Зачем не requests: e-disclosure.ru блокирует TLS fingerprint
    Python urllib3 (JA3). curl имеет другой fingerprint и работает.
    """
    try:
        proc = subprocess.run(
            [
                "curl", "-skL",
                "-A", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
    except subprocess.TimeoutExpired:
        log.error("curl timeout for %s", url)
        return None
    except FileNotFoundError:
        log.error("curl not installed — install via apt-get install curl")
        return None


# Маппинг: ticker → паттерны в названии фонда на НРД.
# Сначала проверяется более специфичная подстрока. Порядок важен!
TICKER_PATTERNS: list[tuple[str, list[str]]] = [
    # ВИМ-фонды. Имя на НРД содержит специфичную часть в кавычках.
    ("LQDT", ["«Ликвидность»", '"Ликвидность"']),
    ("CNYM", ["«Ликвидность. Юань»", '"Ликвидность. Юань"']),
    ("EQMX", ["БИРЖЕВОЙ ПАЕВОЙ", "Индекс МосБиржи"]),  # Биржевой + Индекс
    ("OPIF-1003", ["ОТКРЫТЫЙ ПАЕВОЙ", "ВИМ – Акции"]),  # ОПИФ ВИМ - Акции
    ("OPIF-54", ["ОТКРЫТЫЙ ПАЕВОЙ", "Казначейский"]),
    ("OPIF-9165", ["ОТКРЫТЫЙ ПАЕВОЙ", "Облигации Рантье"]),
    ("GOLD", ["Золото. Биржевой", "Золото"]),
    ("OBLG", ["Российские облигации"]),
    ("ESGE", ["Устойчивое развитие"]),
]


# Document type identifiers (text content of td.type-cell).
DOC_TYPE_SCHA = "Справка о стоимости чистых активов"
DOC_TYPE_PRIROST = "Отчет о приросте"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Frame-Bot/1.0"
)
BASE_URL = "https://www.e-disclosure.ru/portal"


def _ticker_from_fund_name(fund_name: str) -> Optional[str]:
    """
    Сопоставляет официальное название фонда → наш тикер.
    Возвращает None если фонд не в нашем whitelist.

    Match идёт по подстрокам — сначала самый специфичный (нп. для ОПИФ
    нужно "ОТКРЫТЫЙ ПАЕВОЙ" + конкретное название), иначе попадёт под
    более общий паттерн (Индекс МосБиржи у БПИФ и ОПИФ — разные).
    """
    fn_lower = fund_name.lower()
    for ticker, patterns in TICKER_PATTERNS:
        # Все паттерны должны присутствовать (AND логика для дисамбигуации).
        if all(p.lower() in fn_lower for p in patterns):
            return ticker
    return None


def _parse_iso_date(text: str) -> Optional[date_cls]:
    """'30.04.2026' → date(2026, 4, 30)."""
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text or "")
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date_cls(yyyy, mm, dd)
    except ValueError:
        return None


def list_scha_documents(
    company_id: int,
    fund_filter: Optional[set[str]] = None,
    timeout: int = 60,
) -> list[dict]:
    """
    Скачивает страницу `files.aspx?id={X}&type=23` и парсит список
    SCHA + Prirost документов для фондов из whitelist.

    Args:
        company_id: ID компании на e-disclosure (для ВИМ = 28433).
        fund_filter: множество тикеров для фильтрации. Если None — все
            фонды у которых ticker не None.
        timeout: HTTP timeout в секундах.

    Returns:
        Список dict'ов с метаданными каждого документа:
        [{
            'ticker': 'EQMX',
            'fund_name': 'Биржевой паевой ... «Индекс МосБиржи»',
            'doc_type': 'scha' | 'prirost',
            'snapshot_date': date(2026, 4, 30),
            'placement_date': date(2026, 5, 18),
            'file_id': 1927973,
            'download_url': 'https://.../FileLoad.ashx?Fileid=1927973',
            'file_size_kb': 162.73,
        }, ...]
    """
    url = f"{BASE_URL}/files.aspx?id={company_id}&type=23"
    content = _curl_get(url, timeout=timeout)
    if not content:
        return []
    try:
        html = content.decode("utf-8", errors="replace")
    except Exception as e:
        log.error("nrd_listener: decode failed: %s", e)
        return []
    return parse_files_html(html, fund_filter)


def parse_files_html(html: str, fund_filter: Optional[set[str]] = None) -> list[dict]:
    """
    Чистая функция парсера HTML — отделена для unit-тестов.

    Принимает raw HTML страницы files.aspx, возвращает список документов.
    """
    soup = BeautifulSoup(html, "html.parser")
    documents = []

    # Каждая таблица фонда обёрнута в div.spaceTbl, перед ним div с названием.
    for wrapper in soup.select("div.spaceTbl"):
        fund_header = wrapper.find_previous_sibling("div")
        if not fund_header:
            continue
        fund_name = fund_header.get_text(" ", strip=True)
        if not fund_name or len(fund_name) > 300:
            continue

        ticker = _ticker_from_fund_name(fund_name)
        if not ticker:
            continue
        if fund_filter and ticker not in fund_filter:
            continue

        # Парсим строки таблицы.
        table = wrapper.select_one("table.files-table")
        if not table:
            continue

        for row in table.select("tbody > tr"):
            type_cell = row.select_one("td.type-cell")
            if not type_cell:
                continue
            doc_type_text = type_cell.get_text(" ", strip=True)

            if DOC_TYPE_SCHA in doc_type_text:
                doc_type = "scha"
            elif DOC_TYPE_PRIROST in doc_type_text:
                doc_type = "prirost"
            else:
                continue

            # Все td в строке.
            tds = row.find_all("td", recursive=False)
            if len(tds) < 5:
                continue

            # Колонка [2] = "Сведения о документе" с текстом
            # "Отчётная дата: 30.04.2026"
            info_text = tds[2].get_text(" ", strip=True)
            snapshot_date = _parse_iso_date(info_text)
            if not snapshot_date:
                continue

            # Колонка [3] = date-cell (дата размещения на сайте).
            placement_text = tds[3].get_text(" ", strip=True)
            placement_date = _parse_iso_date(placement_text)

            # File link.
            link = row.select_one("a.file-link")
            if not link or not link.get("href"):
                continue
            file_id_match = re.search(r"Fileid=(\d+)", link["href"])
            if not file_id_match:
                continue
            file_id = int(file_id_match.group(1))

            # Парсим размер из текста "zip, 162.73 КБ"
            size_text = link.get_text(" ", strip=True)
            size_match = re.search(r"([\d,]+\.?\d*)\s*КБ", size_text)
            file_size_kb = None
            if size_match:
                try:
                    file_size_kb = float(size_match.group(1).replace(",", "."))
                except ValueError:
                    pass

            documents.append({
                "ticker": ticker,
                "fund_name": fund_name,
                "doc_type": doc_type,
                "snapshot_date": snapshot_date,
                "placement_date": placement_date,
                "file_id": file_id,
                "download_url": link["href"],
                "file_size_kb": file_size_kb,
            })

    return documents


def download_zip(file_id: int, timeout: int = 60) -> Optional[bytes]:
    """Скачивает ZIP по fileId с НРД через curl. Возвращает bytes или None."""
    url = f"{BASE_URL}/FileLoad.ashx?Fileid={file_id}"
    return _curl_get(url, timeout=timeout)


def extract_pdf_from_zip(zip_bytes: bytes) -> Optional[bytes]:
    """Распаковывает ZIP и возвращает первый PDF внутри как bytes."""
    import zipfile
    import io
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            pdf_names = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
            if not pdf_names:
                return None
            return zf.read(pdf_names[0])
    except Exception as e:
        log.error("nrd_listener: unzip failed: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────
# CLI для тестов
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import json
    from collections import Counter

    company_id = int(sys.argv[1]) if len(sys.argv) > 1 else 28433  # ВИМ
    fund_filter = {"LQDT", "EQMX", "GOLD", "OPIF-1003", "OPIF-54", "OPIF-9165"}

    docs = list_scha_documents(company_id, fund_filter=fund_filter)
    print(f"Total documents: {len(docs)}")
    by_ticker = Counter(d["ticker"] for d in docs)
    by_type = Counter(d["doc_type"] for d in docs)
    print(f"By ticker: {dict(by_ticker)}")
    print(f"By type: {dict(by_type)}")
    print()
    # Show last 5 SCHA per ticker
    schas = [d for d in docs if d["doc_type"] == "scha"]
    by_ticker_scha: dict = {}
    for d in schas:
        by_ticker_scha.setdefault(d["ticker"], []).append(d)
    for ticker, lst in by_ticker_scha.items():
        lst.sort(key=lambda d: d["snapshot_date"], reverse=True)
        print(f"\n=== {ticker} ({len(lst)} SCHA) ===")
        for d in lst[:3]:
            print(f"  {d['snapshot_date']}  file_id={d['file_id']}  {d['file_size_kb']} KB")
