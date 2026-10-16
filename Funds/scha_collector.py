"""
Сборщик месячных справок о СЧА (форма ЦБ № 0420502) прямо с сайтов УК.

Заменяет ручное «зайти на сайт → найти справку → скачать → переименовать» для /fund-trades.
Браузер не нужен: у всех УК ссылки на справки лежат в HTML (или в JSON, который отдаёт сайт),
сами файлы отдаются обычным GET с браузерным User-Agent (проба 15.10.2026).

Что делает:
  1. По каждому фонду из SOURCES читает страницу раскрытия → список (месяц, ссылка).
  2. Сверяет с тем, какие месяцы уже есть в БД (interfax_manual / vim_sdr).
  3. Качает недостающие в <out>/TICKER_YYYY-MM-DD.<ext> — имя в формате manual_scha_backfill.py.
  4. --check: прогоняет файл через тот же parse_any и печатает дату, число бумаг, Σ бумаг / активы.

Это библиотека и ручной инструмент. Ежедневный конвейер (очередь, проверки, заливка, уведомления) —
signals/scha_pipeline.py, он берёт отсюда SOURCES, адаптеры и download().
Работает и с мака (БД через ssh), и на сервере (БД по DB_URL). Все 7 сайтов УК пускают прод-IP.
e-disclosure.ru не используем.

    .venv/bin/python Funds/scha_collector.py                 # что нового, без скачивания
    .venv/bin/python Funds/scha_collector.py --download --check
    .venv/bin/python Funds/scha_collector.py --tickers TMOS,EQMX --months 2026-08,2026-09 --download --check
    .venv/bin/python Funds/scha_collector.py --all-months ...  # не сверяться с БД (перекачать историю)
"""
from __future__ import annotations

import argparse
import calendar
import html as _html
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "data" / "scha_inbox"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")
PAUSE = 1.0  # сек между запросами к одному сайту — мы гости, не краулер

SSH = ["ssh", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "ConnectTimeout=30",
       "-i", str(Path.home() / ".ssh/id_ed25519"), "root@103.88.243.232"]
MONTHLY_SOURCES = ("interfax_manual", "vim_sdr")

# Фонд → (адаптер, страница раскрытия). Тикер = funds.ticker в БД.
# Адаптеры: html — ссылки <a> с текстом «Справка о СЧА …» (Рекорд, Т-Капитал, ВИМ, Первая);
#                  ВИМ показывает 20 документов на страницу — листаем ?PAGEN_1=N;
#           aton — JSON страницы Атона (__NEXT_DATA__): ссылками выведен только текущий год, в JSON — все;
#           ihc  — /disclosure.json Героев (секция по имени фонда);
#           alfa — JSON состояния страницы Альфы (вкладки monthly + quarterly, вся история сразу).
# Облигационные фонды (funds.category='bonds') — research/fund_bonds/RESEARCH.md.
SOURCES: dict[str, tuple[str, str]] = {
    # Атон
    "OPIF-63": ("aton", "https://www.am-aton.ru/legaldocs/docpif/pyotr_stolypin"),
    # Герои
    "RU000A108AB5": ("ihc", "Флагманский"),
    # Рекорд Капитал
    "RU000A104M43": ("html", "https://recordcap.ru/documents/funds/alenka"),
    # Альфа-Капитал
    "AKME": ("alfa", "bpif_akmrs"),
    "OPIF-432": ("alfa", "opifa_akliq"),
    "RU000A10BZ69": ("alfa", "opif-bitkogan"),
    "RU000A10B8Z2": ("alfa", "opif-blackline"),
    "RU000A10B917": ("alfa", "opif-matryoshka"),
    "RU000A10B909": ("alfa", "opif-magsev"),
    "RU000A10D1E0": ("alfa", "opif-consstratme"),
    "RU000A10EBY8": ("alfa", "opif-long-invest"),
    "RU000A10D5D3": ("alfa", "opif-balancopp"),
    # Т-Капитал
    "TMOS": ("html", "https://t-capital-funds.ru/documents/mutual_funds/TMOS/"),
    # ВИМ
    "OPIF-1003": ("html", "https://www.wealthim.ru/about/disclosure/pif/opif/wimfa/documents/reports/"),
    "EQMX": ("html", "https://www.wealthim.ru/about/disclosure/pif/bpif/wimfimb/reports/"),
    # Первая
    "OPIF-43": ("html", "https://first-am.ru/individuals/fund/opif-aktsiy-fond-ros-aktsiy/documents"),
    "OPIF-4979": ("html", "https://first-am.ru/individuals/fund/mixed-fund-with-income-payment/documents"),
    "OPIF-8123": ("html", "https://first-am.ru/individuals/fund/stock-fund-with-income-payment/documents"),
    "SBMX": ("html", "https://first-am.ru/individuals/etf/etf-moex/documents"),

    # ── Облигации ──
    "AMGB": ("aton", "https://www.am-aton.ru/legaldocs/docpif/amgb"),
    "RU000A109SR1": ("ihc", "Рублевые перспективы"),
    "AKMB": ("alfa", "bpif-ctrlbonds"),
    "OPIF-33": ("alfa", "opif_akop"),
    "OPIF-9113": ("alfa", "opif_akipo"),
    "OPIF-11259": ("alfa", "bpif_varcoup_bonds"),
    "TBRU": ("html", "https://t-capital-funds.ru/documents/mutual_funds/TBRU/"),
    "TOFZ": ("html", "https://t-capital-funds.ru/documents/mutual_funds/TOFZ/"),
    "OBLG": ("html", "https://www.wealthim.ru/about/disclosure/pif/bpif/wimfrb/reports/"),
    "WOFZ": ("html", "https://www.wealthim.ru/about/disclosure/pif/bpif/wofz/reports/"),
    "WFLO": ("html", "https://www.wealthim.ru/about/disclosure/pif/bpif/wflo/reports/"),
    "OPIF-54": ("html", "https://www.wealthim.ru/about/disclosure/pif/opif/wimfk/documents/reports/"),
    "OPIF-9165": ("html", "https://www.wealthim.ru/about/disclosure/pif/opif/wimor/documents/reports/"),
    "SBGB": ("html", "https://first-am.ru/individuals/etf/etf-ofz/documents"),
    "SBLB": ("html", "https://first-am.ru/individuals/etf/etf-sblb/documents"),
    "SBRB": ("html", "https://first-am.ru/individuals/etf/corporate-bonds-rub/documents"),
    "SBFR": ("html", "https://first-am.ru/individuals/etf/etf-sbfr/documents"),
    "SAFE": ("html", "https://first-am.ru/individuals/etf/fond-konservativnyi-smart/documents"),
    "OPIF-47": ("html", "https://first-am.ru/individuals/fund/opif-obligatsiy-fond-ros-obligatsiy/documents"),
    "OPIF-8119": ("html", "https://first-am.ru/individuals/fund/bond-fund-with-income-payment/documents"),
    "OPIF-4995": ("html", "https://first-am.ru/individuals/fund/pif-nakopitelniy/documents"),
}

_MONTHS = {
    "январ": 1, "феврал": 2, "март": 3, "апрел": 4, "ма": 5, "июн": 6,
    "июл": 7, "август": 8, "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
}
_MONTHS_EN = {calendar.month_name[i].upper(): i for i in range(1, 13)}
_MONTH_WORD = r"(январ\w*|феврал\w*|март\w*|апрел\w*|ма[йя]\w*|июн\w*|июл\w*|август\w*|сентябр\w*|октябр\w*|ноябр\w*|декабр\w*)"

# Текст ссылки = справка о СЧА (а не правила расчёта СЧА, не голосование «≥5% СЧА», не график).
_IS_SCHA = re.compile(r"справк\w*\s+(о\s+)?(стоимост\w*\s+чист\w*\s+актив|СЧА)|^\s*СЧА\s", re.I)
_NOT_SCHA = re.compile(r"правил|порядок|голос|динамик|промежуточн|исправлен", re.I)


@dataclass
class Doc:
    ticker: str
    period: str           # YYYY-MM — месяц справки (ключ сверки с БД)
    day: date | None      # точная дата «на …», если есть в тексте
    url: str
    title: str
    corrected: bool = False  # «Скорректированная справка» — побеждает исходную за тот же месяц

    @property
    def file_date(self) -> date:
        """Дата для имени файла: точная, иначе последний день месяца (manual_scha_backfill
        всё равно берёт дату из самой справки, имя — запасной вариант)."""
        if self.day:
            return self.day
        y, m = map(int, self.period.split("-"))
        return date(y, m, calendar.monthrange(y, m)[1])


# ── HTTP ──────────────────────────────────────────────────────────────────

_SSL = None
# am-aton.ru НЕ присылает промежуточный сертификат GlobalSign RSA OV SSL CA 2018 (openssl:
# Verify return code 21). Браузер и curl на маке достраивают цепочку сами, Python и curl на
# сервере — нет → CERTIFICATE_VERIFY_FAILED, что выглядит как «Атон банит сервер». Добавляем
# ИМЕННО промежуточный: он подписан корнем GlobalSign R3 из certifi, доверие не расширяется.
# Корень Минцифры сюда не подключаем — сайтам УК он не нужен (см. moex_tls.py про точечное доверие).
_GS_INTERMEDIATE = ROOT / "certs" / "globalsign_rsa_ov_ssl_ca_2018.pem"

# Ходим ТОЛЬКО на сайты УК и только по https. Ссылки на файлы берутся со страниц УК — без этого
# списка подменённая ссылка (http://127.0.0.1:…, внутренний хост) превратила бы сборщик на сервере
# в прокси к нашим же сервисам (SSRF). Проверяется и каждый редирект (Рекорд отдаёт файл через 302).
ALLOWED_DOMAINS = ("am-aton.ru", "ih-capital.ru", "recordcap.ru", "alfacapital.ru",
                   "t-capital-funds.ru", "wealthim.ru", "first-am.ru")


def _check_url(url: str) -> None:
    u = urllib.parse.urlsplit(url)
    host = (u.hostname or "").lower()
    if u.scheme != "https" or not any(host == d or host.endswith("." + d) for d in ALLOWED_DOMAINS):
        raise RuntimeError(f"ссылка не на сайт УК (или не https), не качаем: {url[:120]}")


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _check_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _opener():
    global _SSL
    if _SSL is None:
        import ssl
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
        if _GS_INTERMEDIATE.is_file():
            ctx.load_verify_locations(cafile=str(_GS_INTERMEDIATE))
        _SSL = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx), _SafeRedirect())
    return _SSL


def http_get(url: str, timeout: int = 60) -> tuple[bytes, dict]:
    # В путях Первой — сырая кириллица и пробелы: без quote будет 404.
    url = urllib.parse.quote(url, safe=":/?&=%#,-._~+")
    _check_url(url)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9"})
    with _opener().open(req, timeout=timeout) as r:
        return r.read(), dict(r.headers)


def _text(fragment: str) -> str:
    t = re.sub(r"<!--.*?-->", " ", fragment, flags=re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", _html.unescape(t)).strip()


# ── Дата справки из текста ссылки ─────────────────────────────────────────

def date_from_text(t: str) -> tuple[str | None, date | None]:
    """«… на 31.08.2026» → ('2026-08', 2026-08-31); «… за август 2026 года» → ('2026-08', None).
    «на» важнее любой другой даты в тексте (у ВИМ перед названием стоит дата публикации).
    У Героев в 2024 дата в начале через пробелы: «28 12 2024 Справка…»."""
    for pat in (r"на\s+(\d{2})\.(\d{2})\.(\d{4})", r"(\d{2})\.(\d{2})\.(\d{4})", r"^\s*(\d{2})\s+(\d{2})\s+(\d{4})\b"):
        m = re.search(pat, t)
        if m:
            d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            return f"{d:%Y-%m}", d
    m = re.search(r"за\s+" + _MONTH_WORD + r"\s+(\d{4})", t, re.I)
    if m:
        word = m.group(1).lower()
        mon = next(v for k, v in _MONTHS.items() if word.startswith(k))
        return f"{int(m.group(2)):04d}-{mon:02d}", None
    # Квартальная/годовая справка без точной даты: «за III квартал 2026», «за 2 кв. 2026», «за 2025 год»
    # → последний месяц периода (справка на конец квартала/года = обычный месячный срез).
    m = re.search(r"\b(IV|III|II|I|[1-4])\s*(?:-?й\s*)?кв\w*\.?\s*(\d{4})", t, re.I)
    if m:
        q = {"I": 1, "II": 2, "III": 3, "IV": 4}.get(m.group(1).upper()) or int(m.group(1))
        return f"{m.group(2)}-{q * 3:02d}", None
    m = re.search(r"за\s+(\d{4})\s*(?:г\.|год)", t, re.I)
    if m:
        return f"{m.group(1)}-12", None
    return None, None


# ── Адаптеры ──────────────────────────────────────────────────────────────

def list_html(ticker: str, page_url: str) -> list[Doc]:
    if "wealthim.ru" not in page_url:
        return _list_html_page(ticker, page_url)
    # ВИМ: 20 документов на страницу (SIZEN_1 игнорирует), за последней снова отдаёт первую —
    # листаем, пока страница приносит новые месяцы. История на сайте — с 08.2022 (6 страниц).
    docs: list[Doc] = []
    seen: set[str] = set()
    for n in range(1, 16):
        page = _list_html_page(ticker, page_url + (f"?PAGEN_1={n}" if n > 1 else ""))
        fresh = {d.period for d in page} - seen
        if not fresh:
            break
        seen |= fresh
        docs += page
        time.sleep(PAUSE)
    return docs


def _list_html_page(ticker: str, page_url: str) -> list[Doc]:
    body, _ = http_get(page_url)
    t = body.decode("utf-8", "ignore")
    docs = []
    for m in re.finditer(r'<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>', t, re.S):
        title = _text(m.group(2))
        if not _IS_SCHA.search(title) or _NOT_SCHA.search(title):
            continue
        period, day = date_from_text(title)
        if not period:
            continue
        docs.append(Doc(ticker, period, day, urllib.parse.urljoin(page_url, _html.unescape(m.group(1))), title))
    return docs


def list_aton(ticker: str, page_url: str) -> list[Doc]:
    """Атон (Next.js): на странице ссылками только текущий год, все годы — в <script id="__NEXT_DATA__">
    → props.pageProps.data[] — разделы; справки в разделе с code «otchet_<slug>». Месяц — из названия
    («на 31.08.2026»), не из dateDocument (там бывает 28-е вместо 31-го)."""
    body, _ = http_get(page_url)
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body.decode("utf-8", "ignore"), re.S)
    if not m:
        raise RuntimeError("aton: нет __NEXT_DATA__ (сменили вёрстку?)")
    docs = []
    for sec in json.loads(m.group(1))["props"]["pageProps"]["data"]:
        if not (sec.get("code") or "").startswith("otchet_"):
            continue
        for it in sec.get("items") or []:
            title = it.get("name") or ""
            url = (it.get("file") or {}).get("url")
            if not url or not _IS_SCHA.search(title) or _NOT_SCHA.search(title):
                continue
            period, day = date_from_text(title)
            if period:
                docs.append(Doc(ticker, period, day, urllib.parse.urljoin(page_url, url), title))
    return docs


def list_ihc(ticker: str, section_name: str) -> list[Doc]:
    body, _ = http_get("https://ih-capital.ru/disclosure.json")
    docs = []
    for sec in json.loads(body):
        if section_name not in (sec.get("section") or ""):
            continue
        for f in sec.get("files") or []:
            title = f.get("name") or ""
            if not _IS_SCHA.search(title) or _NOT_SCHA.search(title):
                continue
            period, day = date_from_text(title)
            if period:
                docs.append(Doc(ticker, period, day, urllib.parse.urljoin("https://ih-capital.ru/", f["url"]), title))
    return docs


def list_alfa(ticker: str, slug: str) -> list[Doc]:
    """Месяцы, кроме конца квартала, — на вкладке monthly; март/июнь/сентябрь/декабрь — только на
    quarterly («II квартал» = справка на конец июня). Страница отдаёт в window.__SERVER_STATE__
    ВСЕ годы сразу (categoryId вида monthly-2025-month-NOVEMBER / quarterly-2025-quarter-2)."""
    docs = []
    for tab in ("monthly", "quarterly", "yearly"):
        try:
            body, _ = http_get(f"https://www.alfacapital.ru/disclosure/pifs/{slug}/{tab}")
        except urllib.error.HTTPError as e:
            if tab == "yearly" and e.code == 404:  # годовой вкладки у фонда может не быть
                continue
            raise
        t = body.decode("utf-8", "ignore")
        m = re.search(r"window\.__SERVER_STATE__\s*=\s*", t)
        if not m and tab == "yearly":  # годовой вкладки нет — сайт отдаёт другую страницу
            continue
        if not m:
            raise RuntimeError(f"alfa {slug}/{tab}: нет __SERVER_STATE__ (сменили вёрстку?)")
        state, _ = json.JSONDecoder().raw_decode(t[m.end():])
        for d in state["disclosure"]["fund"]["documents"]["documents"]:
            title = d.get("name") or ""
            corrected = title.lower().startswith("скорректированная справка о стоимости чистых активов")
            if not (title.startswith("Справка о стоимости чистых активов") or corrected) or d.get("isArchived"):
                continue
            cat = d.get("categoryId") or ""
            mm = re.match(r"monthly-(\d{4})-month-([A-Z]+)$", cat)
            mq = re.match(r"quarterly-(\d{4})-quarter-([1-4])$", cat)
            my = re.match(r"yearly-(\d{4})", cat)
            if mm:
                period = f"{mm.group(1)}-{_MONTHS_EN[mm.group(2)]:02d}"
            elif mq:
                period = f"{mq.group(1)}-{int(mq.group(2)) * 3:02d}"
            elif my:
                period = f"{my.group(1)}-12"
            else:
                continue
            docs.append(Doc(ticker, period, None, "https://www.alfacapital.ru" + d["path"], f"{title[:40]}… [{cat}]",
                            corrected))
        time.sleep(PAUSE)
    return docs


ADAPTERS = {"html": list_html, "aton": list_aton, "ihc": list_ihc, "alfa": list_alfa}


def one_per_month(docs: list[Doc]) -> dict[str, Doc]:
    """Если за месяц несколько справок (дубль ссылки, поздняя перепубликация) — скорректированная
    важнее исходной, дальше — с самой поздней датой; одинаковые — первую по странице (сайты
    сортируют свежие сверху)."""
    best: dict[str, Doc] = {}
    for d in docs:
        cur = best.get(d.period)
        if (cur is None or (d.corrected and not cur.corrected)
                or (d.corrected == cur.corrected and d.day and cur.day and d.day > cur.day)):
            best[d.period] = d
    return best


# ── Что уже есть в БД ─────────────────────────────────────────────────────

def known_periods() -> dict[str, set[str]]:
    sql = (
        "SELECT f.ticker, to_char(h.snapshot_date,'YYYY-MM') FROM fund_holdings_history h "
        "JOIN funds f ON f.fund_id=h.fund_id "
        f"WHERE h.source IN ({','.join(repr(s) for s in MONTHLY_SOURCES)}) "
        "GROUP BY 1,2"
    )
    if os.environ.get("DB_URL"):
        from sqlalchemy import create_engine, text
        with create_engine(os.environ["DB_URL"]).connect() as c:
            rows = c.execute(text(sql)).all()
    else:
        cmd = f'docker exec frame-db-1 psql -U postgres -d moex_db -At -F"|" -c "{sql}"'
        out = subprocess.run(SSH + [cmd], capture_output=True, text=True, timeout=120, check=True).stdout
        rows = [ln.split("|") for ln in out.splitlines() if "|" in ln]
    have: dict[str, set[str]] = {}
    for tk, per in rows:
        have.setdefault(tk, set()).add(per)
    return have


# ── Скачивание и проверка ─────────────────────────────────────────────────

_EXT_BY_TYPE = {"application/pdf": ".pdf", "application/vnd.ms-excel": ".xls",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                "application/zip": ".zip"}


def _sniff_ext(data: bytes, headers: dict, url: str) -> str:
    """Расширение по содержимому: у Альфы/Рекорда в URL его нет, а Content-Type бывает общий."""
    if data[:4] == b"%PDF":
        return ".pdf"
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return ".xls"
    if data[:2] == b"PK":
        low = data[:4000]
        if b"word/" in low:
            return ".docx"
        if b"xl/" in low:
            return ".xlsx"
        return ".zip"
    ctype = (headers.get("Content-Type") or "").split(";")[0].strip()
    if ctype in _EXT_BY_TYPE:
        return _EXT_BY_TYPE[ctype]
    return Path(urllib.parse.urlparse(url).path).suffix.lower() or ".bin"


def download(doc: Doc, out: Path) -> Path:
    data, headers = http_get(doc.url, timeout=120)
    if data[:200].lstrip().lower().startswith((b"<!doctype", b"<html")):
        raise RuntimeError("вместо файла пришла HTML-страница (защита от ботов или битая ссылка)")
    path = out / f"{doc.ticker}_{doc.file_date.isoformat()}{_sniff_ext(data, headers, doc.url)}"
    path.write_bytes(data)
    return path


def _backfill():
    """manual_scha_backfill на импорте создаёт engine из DB_URL. С мака для одного парса БД не
    нужна — подставляем заглушку (create_engine ленивый, соединение не открывается)."""
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DB_URL", "postgresql+pg8000://offline@127.0.0.1:1/none")
    import Funds.manual_scha_backfill as mb
    return mb


COVERAGE_MIN, COVERAGE_MAX = 80.0, 101.0  # Σ бумаг / «Общая стоимость активов», %


def verdict(path: Path, doc: Doc) -> tuple[bool, str, dict | None]:
    """Парс тем же parse_any, что и импорт, + проверки, без которых справку нельзя лить
    автоматически. Ничего не пишет в БД. → (можно_лить, строка_для_отчёта, результат_парса)."""
    try:
        r = _backfill().parse_any(path)
    except Exception as e:  # noqa: BLE001 — показать и идти дальше
        return False, f"❌ парсер: {e}", None
    assets = r.get("assets") or []
    gross = r.get("gross_assets_rub")
    s = sum(a.get("value_rub") or 0 for a in assets)
    cov = s / gross * 100 if gross else None
    snap = r.get("snapshot_date")
    problems = []
    if not assets:
        problems.append("нет бумаг")
    if cov is None:
        problems.append("нет «Общей стоимости активов»")
    elif not COVERAGE_MIN <= cov <= COVERAGE_MAX:
        problems.append(f"Σ бумаг / активы {cov:.1f}% вне {COVERAGE_MIN:.0f}–{COVERAGE_MAX:.0f}%")
    if not snap:
        problems.append("в справке нет даты")
    elif snap[:7] != doc.period:
        problems.append(f"дата в справке {snap} ≠ месяц ссылки {doc.period}")
    cov_s = f"{cov:.1f}%" if cov is not None else "—"
    line = f"на {snap} · бумаг {len(assets)} · Σ/активы {cov_s} · {r.get('parser_strategy')}"
    if problems:
        return False, "⚠️ " + line + " · " + "; ".join(problems), r
    return True, "✅ " + line, r


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tickers", help="через запятую; по умолчанию все из SOURCES")
    ap.add_argument("--months", help="YYYY-MM через запятую — только эти месяцы")
    ap.add_argument("--since", help="YYYY-MM — не раньше этого месяца")
    ap.add_argument("--all-months", action="store_true", help="не сверяться с БД")
    ap.add_argument("--download", action="store_true", help="скачать найденное")
    ap.add_argument("--check", action="store_true", help="после скачивания прогнать парсер")
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="куда класть файлы")
    a = ap.parse_args()

    tickers = a.tickers.split(",") if a.tickers else list(SOURCES)
    months = set(a.months.split(",")) if a.months else None
    have = {} if a.all_months else known_periods()
    out = Path(a.out).expanduser()
    download_ = a.download
    if download_:
        out.mkdir(parents=True, exist_ok=True)

    failed = 0
    for tk in tickers:
        kind, arg = SOURCES[tk]
        try:
            found = one_per_month(ADAPTERS[kind](tk, arg))
            if not found:
                raise RuntimeError("на странице не нашлось ни одной справки (сменили вёрстку?)")
        except Exception as e:  # noqa: BLE001
            print(f"{tk:14} ❌ страница: {e}")
            failed += 1
            continue
        new = sorted(p for p in found
                     if p not in have.get(tk, set())
                     and (months is None or p in months)
                     and (a.since is None or p >= a.since))
        last = max(found)
        print(f"{tk:14} справок на сайте {len(found):3} (последняя {last}) · новых {len(new)}"
              + (f": {', '.join(new)}" if new else ""))
        for p in new:
            d = found[p]
            if not download_:
                print(f"    {p}  {d.url}")
                continue
            try:
                path = download(d, out)
                line = f"    {p}  ⬇ {path.name} ({path.stat().st_size // 1024} КБ)"
                if a.check:
                    line += "  " + verdict(path, d)[1]
                print(line)
            except Exception as e:  # noqa: BLE001
                print(f"    {p}  ❌ {e}")
                failed += 1
            time.sleep(PAUSE)
        time.sleep(PAUSE)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
