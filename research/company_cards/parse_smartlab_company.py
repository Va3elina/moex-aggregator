#!/usr/bin/env python3
"""
Парсер карточки компании smart-lab → JSON для company_metrics и соседних таблиц.

ЗАЧЕМ. Второй мозг агентов, которые пишут посты: агенту нужен фундамент компании,
а не одна новость. Это первый шаг — вертикальный срез на ОДНОЙ бумаге (SBER),
от HTML до готовой к загрузке структуры.

ЧТО ВЫЯСНЕНО РАЗВЕДКОЙ (и почему парсер устроен именно так):

1. Машинный код показателя лежит ПРЯМО В СТРОКЕ таблицы: `?field=net_interest_income`.
   Словарь по русским подписям не нужен — он был бы источником вечных расхождений.
   Побочно: набор кодов ШИРЕ, чем 48 кодов со страниц `?field=`. У Сбера 57 строк,
   и банковские (loan_portfolio, cost_of_risk_ratio, NPL) на тех страницах не встречаются.
   Значит справочник метрик наполняется НА ЛЕТУ из того, что реально пришло.

2. Один и тот же показатель может иметь разные коды на разных страницах: `p_b`
   на карточке против `p_bv` на `?field=`. Канонизируем через CODE_ALIASES.

3. Данные префа живут на карточке ОБЫЧКИ под отдельными кодами: priv_share,
   number_of_priv_shares, dividend_pr, div_yield_priv. Поэтому у метрики есть
   share_class: строка уезжает к SBERP, а не к SBER.

4. Ловушка нуля (HANDOFF, Роснефть: чистый долг 5416 → 0.00 → 2700). Ноль у
   балансового показателя = «не заполнено», а не «нуля долга нет». Пишем NULL и
   помечаем zero_as_missing. Но у дивидендов ноль настоящий (Сбер за 2021 не платил),
   поэтому TRUE_ZERO_OK — белый список.

Использование:
    python parse_smartlab_company.py SBER --out sber.json
    python parse_smartlab_company.py SBER --cache-dir ./cache   # не ходить в сеть повторно
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

BASE = "https://smart-lab.ru/q"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
POLITE_DELAY = 0.5  # в разведке 223 бумаги × 2 страницы прошли без блокировок

log = logging.getLogger("smartlab_parser")


def setup_logging(verbose: bool = False):
    """
    Двойное логирование по образцу Macro/fetch_market_cap.py: полный лог отдельно,
    ошибки отдельно. Причина ровно та же, что и у фетчеров: прогон по 86 эмитентам
    идёт ~30 минут, и при разборе «почему у компании X пусто» листать общий лог
    бессмысленно — нужен файл, в котором только то, что сломалось.
    """
    LOG_DIR.mkdir(exist_ok=True)
    day = date.today().strftime("%Y%m")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    full = logging.FileHandler(LOG_DIR / f"smartlab_parser_{day}.log", encoding="utf-8")
    full.setLevel(logging.DEBUG)
    full.setFormatter(fmt)
    log.addHandler(full)

    errors = logging.FileHandler(LOG_DIR / f"smartlab_parser_errors_{day}.log", encoding="utf-8")
    errors.setLevel(logging.WARNING)
    errors.setFormatter(fmt)
    log.addHandler(errors)

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    log.addHandler(console)

# Показатели, у которых ноль почти наверняка означает «не заполнено».
# Всё это уровни/остатки: у работающей компании они нулём не бывают.
ZERO_IS_MISSING = {
    "net_debt", "debt", "cash", "assets", "bank_assets", "capital", "book_value",
    "net_assets", "revenue", "ebitda", "ebitda_margin", "ev_ebitda", "debt_ebitda",
    "loan_portfolio", "corporate_loans", "retail_loans", "deposits",
    "corporate_deposits", "retail_deposits", "provision_for_loan_impairment",
    "market_cap", "ev", "number_of_shares", "number_of_priv_shares", "employees",
    "opex", "cost_of_production", "operating_income", "net_operating_income",
    "net_interest_income", "commission_income",
}
# Здесь ноль — содержательный факт (не платили дивиденд, нет R&D).
TRUE_ZERO_OK = {
    "dividend", "dividend_pr", "dividend_payout", "div_yield", "div_yield_priv",
    "div_payout_ratio", "fcf_yield", "r_and_d_capex", "free_float",
}

# Один показатель — один код, даже если smart-lab зовёт его по-разному на разных страницах.
CODE_ALIASES = {"p_b": "p_bv"}

# Коды, значения которых относятся к привилегированной акции, а не к обыкновенной.
PREF_CODES = {
    "priv_share": "common_share",
    "number_of_priv_shares": "number_of_shares",
    "dividend_pr": "dividend",
    "div_yield_priv": "div_yield",
}

# Служебные строки шапки таблицы — не метрики.
HEADER_ROWS = {"Дата отчета", "Валюта отчета", "Финансовый отчет",
               "Годовой отчет", "Презентация"}
DOC_ROWS = {"Финансовый отчет": "financial_report",
            "Годовой отчет": "annual_report",
            "Презентация": "presentation"}


# ---------------------------------------------------------------- утилиты HTML

def strip_tags(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", s))).strip()


def fetch(url: str, cache_dir: Path | None) -> str | None:
    """Возвращает HTML или None, если страницы нет (404 — норма: WTCM, JETL, MONO)."""
    if cache_dir:
        cached = cache_dir / (re.sub(r"[^A-Za-z0-9]+", "_", url).strip("_") + ".html")
        if cached.exists():
            return cached.read_text(encoding="utf-8", errors="replace")
    if cache_dir and cached.exists():
        log.debug("кэш: %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode("utf-8", errors="replace")
        log.debug("получено %d Б: %s", len(body), url)
    except urllib.error.HTTPError as ex:
        if ex.code == 404:
            # 404 — норма, а не сбой: у WTCM, JETL, MONO карточки нет.
            log.warning("404 (страницы нет): %s", url)
            return None
        log.error("HTTP %s: %s", ex.code, url)
        raise
    except Exception as ex:
        log.error("сеть упала на %s: %s", url, ex)
        raise
    time.sleep(POLITE_DELAY)
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cached.write_text(body, encoding="utf-8")
    return body


def parse_number(raw: str):
    """'2 501' → 2501.0, '0.0%' → 0.0, '-517.2' → -517.2, '' → None."""
    if raw is None:
        return None
    s = raw.replace("\xa0", " ").replace("−", "-").strip()
    if s in ("", "-", "—", "n/a", "?"):
        return None
    s = s.rstrip("%").replace(" ", "").replace(",", ".")
    if not re.fullmatch(r"-?\d+(\.\d+)?", s):
        return None
    return float(s)


def row_cells(tr: str):
    """[(текст, field_code|None, [ссылки])] по ячейкам строки."""
    out = []
    for m in re.finditer(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S):
        cell = m.group(1)
        code = re.search(r"field=([a-z0-9_]+)", cell)
        links = re.findall(r'href="([^"]+)"', cell)
        out.append((strip_tags(cell), code.group(1) if code else None, links))
    return out


# ------------------------------------------------------- таблица отчётности

def parse_financials(page_html: str, period_type: str):
    """
    period_type: 'year' | 'quarter'.
    Возвращает (periods, metrics, report_dates, documents).
    """
    table = re.search(r'<table[^>]*class="[^"]*financials[^"]*"[^>]*>.*?</table>',
                      page_html, re.S)
    if not table:
        # Страница есть, а таблицы нет — либо смена вёрстки, либо у компании нет
        # отчётности этого стандарта. Тихо возвращать пустоту нельзя: так пропадёт
        # ровно тот случай, когда сайт переехал и парсер надо чинить.
        log.warning("таблица financials не найдена (%s) — вёрстка или нет данных", period_type)
        return [], [], {}, []
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", table.group(0), re.S)

    # Шапка: первая строка, где встречается год ('2021') или квартал ('2025Q2').
    pat = r"^\d{4}Q[1-4]$" if period_type == "quarter" else r"^\d{4}$"
    periods, header_idx, header_len = [], None, 0
    for i, tr in enumerate(trs):
        cs = row_cells(tr)
        cols = [(j, c[0]) for j, c in enumerate(cs)
                if re.match(pat, c[0]) or c[0].startswith("LTM")]
        if cols:
            periods = [(j, "LTM" if t.startswith("LTM") else t) for j, t in cols]
            header_idx, header_len = i, len(cs)
            break
    if header_idx is None:
        log.warning("шапка периодов не распознана (%s) — вёрстка изменилась", period_type)
        return [], [], {}, []

    report_dates, documents, metrics = {}, [], []
    for tr in trs[header_idx + 1:]:
        cs = row_cells(tr)
        if not cs:
            continue
        label = cs[0][0]
        code = next((c[1] for c in cs if c[1]), None)

        off = len(cs) - header_len
        if label == "Дата отчета":
            for col, per in periods:
                if 0 <= col + off < len(cs):
                    report_dates[per] = cs[col + off][0] or None
            continue
        if label in DOC_ROWS:
            for col, per in periods:
                if 0 <= col + off < len(cs) and cs[col + off][2]:
                    for href in cs[col + off][2]:
                        documents.append({"doc_type": DOC_ROWS[label],
                                          "period": per, "url": href})
            continue
        if label in HEADER_ROWS or not code:
            continue

        # ⚠️ СТРОКА ДАННЫХ ШИРЕ ШАПКИ. У шапки 9 ячеек, у строки показателя 10: в
        # строке есть лишняя ячейка со знаком «?» и ссылкой на код показателя, а в
        # шапке её нет. Брать значение по индексу колонки из шапки нельзя — весь ряд
        # уезжает НА ГОД: чистая прибыль 2021 года встаёт в 2022, и так до конца
        # таблицы. Ошибка тихая: числа настоящие, просто не за тот период.
        # Поэтому сдвиг считается для КАЖДОЙ строки: у «Изм. за год, %» его нет вовсе.
        offset = len(cs) - header_len
        if offset < 0:
            log.warning("строка уже шапки (%s): %d против %d — пропущена",
                        label, len(cs), header_len)
            continue

        code = CODE_ALIASES.get(code, code)
        share_class = "pref" if code in PREF_CODES else "common"
        canon = PREF_CODES.get(code, code)
        unit = label.split(",", 1)[1].strip() if "," in label else None

        for col, per in periods:
            idx = col + offset
            if idx >= len(cs):
                continue
            raw = cs[idx][0]
            value = parse_number(raw)
            if value is None and raw.strip() not in ("", "-", "—", "?"):
                # Текст, который не разобрался в число: единица измерения уехала,
                # появился новый формат. Молча терять такое нельзя.
                log.warning("не разобрано число: %s %s = %r", canon, per, raw)
            note = None
            if value == 0.0 and canon in ZERO_IS_MISSING and canon not in TRUE_ZERO_OK:
                # Ловушка Роснефти: ноль здесь означает «smart-lab не заполнил».
                value, note = None, "zero_as_missing"
                log.info("ноль как пропуск: %s %s (%s)", canon, per, label)
            if value is None and not note and raw.strip() in ("", "-", "—"):
                note = "no_data"
            metrics.append({
                "metric_code": canon, "smartlab_code": code, "label": label,
                "unit": unit, "share_class": share_class,
                "period_type": "ltm" if per == "LTM" else period_type,
                "period": None if per == "LTM" else per,
                "value": value, "raw": raw, "note": note,
            })
    # СТОРОЖ ПРОТИВ СДВИГА КОЛОНОК. Ошибка выравнивания тихая: числа остаются
    # настоящими, просто уезжают на период. Её характерная подпись — крайний столбец
    # пустеет целиком, потому что данные съезжают за границу таблицы. Проверяем это
    # прямо здесь, а не надеемся заметить глазами на 86 эмитентах.
    filled = {}
    for m in metrics:
        key = "LTM" if m["period"] is None else m["period"]
        filled[key] = filled.get(key, 0) + (1 if m["value"] is not None else 0)
    if len(filled) > 2:
        last = ("LTM" if periods[-1][1] == "LTM" else periods[-1][1])
        others = [v for k, v in filled.items() if k != last]
        if filled.get(last, 0) == 0 and max(others) > 0:
            log.error("крайний период %s пуст целиком при заполненных остальных — "
                      "похоже на сдвиг колонок, проверьте выравнивание (%s)",
                      last, period_type)

    return [p for _, p in periods], metrics, report_dates, documents


# -------------------------------------------------------------- дивиденды

def parse_dividends(page_html: str):
    """Таблица выплат (по бумагам, с отсечками) + годовая сводка."""
    payments, annual = [], []
    for table in re.findall(r"<table[^>]*>.*?</table>", page_html, re.S):
        trs = re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S)
        head = [c[0] for c in row_cells(trs[1])] if len(trs) > 1 else []
        if "дата отсечки" in [h.lower() for h in head]:
            for tr in trs[2:]:
                v = [c[0] for c in row_cells(tr)]
                if len(v) < 7 or not re.fullmatch(r"[A-Z0-9]{3,6}", v[0]):
                    continue
                payments.append({
                    "secid": v[0], "t_minus_1": v[1], "record_date": v[2],
                    "period": v[3], "dividend": parse_number(v[4].replace("₽", "")),
                    "price": parse_number(v[5]), "div_yield": parse_number(v[6]),
                })
        elif any(re.fullmatch(r"\d{4}", h) for h in head):
            cols = [(j, h) for j, h in enumerate(head)
                    if re.fullmatch(r"\d{4}", h) or h.startswith("LTM")]
            for tr in trs[2:]:
                cs = row_cells(tr)
                if not cs:
                    continue
                code = next((c[1] for c in cs if c[1]), None)
                label = cs[0][0]
                for j, per in cols:
                    if j < len(cs):
                        annual.append({
                            "metric_code": CODE_ALIASES.get(code, code), "label": label,
                            "share_class": "pref" if code in PREF_CODES else "common",
                            "period": None if per.startswith("LTM") else per,
                            "period_type": "ltm" if per.startswith("LTM") else "year",
                            "value": parse_number(cs[j][0]), "raw": cs[j][0],
                        })
    return payments, annual


# ------------------------------------------------ список документов /f/l/

# Заголовки секций на /q/<T>/f/l/ → тип документа. Это ОТДЕЛЬНАЯ страница, и брать
# её обязательно: сводная таблица /f/y/ показывает ссылки только за те 5 лет, что в
# ней помещаются (у Сбера 12 штук), а /f/l/ отдаёт весь архив — у Сбера 132 ссылки,
# 83 из них PDF, с 2011 по 2026 год.
DOC_SECTIONS = [
    ("Годовые отчеты МСФО",      "financial_report_msfo_year"),
    ("Квартальные отчеты МСФО",  "financial_report_msfo_quarter"),
    ("Годовые отчеты РСБУ",      "financial_report_rsbu_year"),
    ("Квартальные отчеты РСБУ",  "financial_report_rsbu_quarter"),
    ("Годовые презентации",      "presentation"),
    ("Годовые отчеты",           "annual_report"),   # ПОСЛЕДНИМ: подстрока остальных
]


def parse_documents_list(page_html: str):
    """Все ссылки на первоисточники с разбивкой по секциям и годам."""
    body = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", page_html, flags=re.S)

    # Границы секций по заголовкам. Порядок в DOC_SECTIONS важен: «Годовые отчеты» —
    # подстрока «Годовые отчеты МСФО», поэтому проверяется последним.
    marks = []
    for m in re.finditer(r"<h[123][^>]*>(.*?)</h[123]>", body, re.S):
        title = strip_tags(m.group(1))
        for name, doc_type in DOC_SECTIONS:
            if title.startswith(name):
                marks.append((m.start(), doc_type))
                break
    marks.sort()

    def section_of(pos: int):
        cur = None
        for start, doc_type in marks:
            if start <= pos:
                cur = doc_type
            else:
                break
        return cur

    docs, seen = [], set()
    for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", body, re.S):
        cells = row_cells(tr.group(1))
        # ⚠️ СМЕШАННЫЙ АЛФАВИТ. Квартал в списке документов подписан КИРИЛЛИЧЕСКОЙ «К»:
        # «2026К2», а не «2026Q2» — при этом в таблицах отчётности тот же квартал
        # написан латиницей. Без этого 71 квартальный документ приезжал без периода.
        period = next((c[0] for c in cells
                       if re.fullmatch(r"20\d\d([QКK][1-4])?", c[0])), None)
        if period:
            period = re.sub(r"[КK](?=[1-4]$)", "Q", period)
        for _, _, links in cells:
            for href in links:
                if not href.startswith("http") or (href, period) in seen:
                    continue
                seen.add((href, period))
                docs.append({"doc_type": section_of(tr.start()) or "other",
                             "period": period or "", "url": href})
    if not docs:
        log.warning("страница /f/l/ отдалась, но ссылок на документы в ней нет")
    return docs


# ------------------------------------------------------------ акционеры

def parse_shareholders(page_html: str):
    holders = []
    for table in re.findall(r"<table[^>]*>.*?</table>", page_html, re.S):
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
            v = [c[0] for c in row_cells(tr)]
            if len(v) == 2 and v[1].endswith("%") and parse_number(v[1]) is not None:
                holders.append({"holder": v[0], "share_pct": parse_number(v[1])})
    # ⚠️ Без этой даты агент напишет, что треть Сбера у американских инвесторов:
    # структура Сбера на smart-lab не обновлялась с 08.05.2020.
    m = re.search(r"Дата последнего обновления этой структуры:\s*([\d.]+)",
                  strip_tags(re.sub(r"<(script|style)[^>]*>.*?</\1>", "", page_html, flags=re.S)))
    return holders, (m.group(1) if m else None)


# ------------------------------------------------- факторы роста и падения

def parse_factors(page_html: str):
    """Датированные тезисы. Видно 5 из 10 — остальные за подпиской Mozgovik."""
    out, total = [], None
    for direction, cls in (("growth", "growth"), ("fall", "fall")):
        block = re.search(rf'<ul class="stock-factors__{cls}">(.*?)</ul>', page_html, re.S)
        if not block:
            continue
        for li in re.findall(r"<li[^>]*>(.*?)</li>", block.group(1), re.S):
            dt = re.search(r"<span>\((\d{2}\.\d{2}\.\d{4})\)</span>", li)
            text = strip_tags(re.sub(r"<span>.*?</span>", "", li, flags=re.S))
            if text:
                out.append({"direction": direction, "statement": text,
                            "stated_date": dt.group(1) if dt else None})
    m = re.search(r"Смотреть все факторы \((\d+)\)", strip_tags(page_html))
    if m:
        total = int(m.group(1))
    return out, total


# ------------------------------------------------------------------ сборка

def parse_company(ticker: str, standards=("MSFO", "RSBU"), cache_dir: Path | None = None):
    """
    standards: какие стандарты отчётности забирать. У Сбера МСФО даёт 52 кода, РСБУ —
    49 со своими датами отчётов; у части компаний МСФО нет вовсе, и РСБУ — единственное,
    что есть. Поэтому по умолчанию берём оба, а не только МСФО.

    ЧЕГО ЗДЕСЬ СОЗНАТЕЛЬНО НЕТ (пересчитано по всем 57 маршрутам /q/<T>/*):
      • 43 страницы вида /q/SBER/MSFO/<код>/ — по одному показателю каждая. Проверено
        на net_income: глубина ТА ЖЕ, 5 лет + LTM. Нового там только CAGR за 5 лет и
        изменения г/г, а они считаются из значений, которые уже забраны. 43 лишних
        запроса на бумагу — это 3 700 запросов на вселенную ради нуля новых данных;
      • /r/y/ и /r/q/ — те же таблицы в процентах изменений, тоже вычисляемо;
      • /ir-rating/ — строки ir_* уже приходят в основной таблице;
      • /f/y/MSFO/en — английская версия той же страницы.
    """
    if isinstance(standards, str):
        standards = (standards,)
    res = {"ticker": ticker, "standards": list(standards),
           "captured_at": datetime.now().isoformat(timespec="seconds"),
           "pages": {}, "metrics": [], "documents": [],
           "report_dates_year": {}, "report_dates_quarter": {}}

    for standard in standards:
        y = fetch(f"{BASE}/{ticker}/f/y/{standard}/", cache_dir)
        res["pages"][f"year_{standard}"] = y is not None
        if y:
            periods, metrics, rdates, docs = parse_financials(y, "year")
            res[f"year_periods_{standard}"] = periods
            res["report_dates_year"][standard] = rdates
            for m in metrics:
                m["standard"] = standard
            res["metrics"] += metrics
            if standard == "MSFO":
                # Факторы и ссылки в таблице не зависят от стандарта — берём один раз.
                res["factors"], res["factors_total"] = parse_factors(y)

        q = fetch(f"{BASE}/{ticker}/f/q/{standard}/", cache_dir)
        res["pages"][f"quarter_{standard}"] = q is not None
        if q:
            periods, metrics, rdates, _ = parse_financials(q, "quarter")
            res[f"quarter_periods_{standard}"] = periods
            res["report_dates_quarter"][standard] = rdates
            for m in metrics:
                m["standard"] = standard
            res["metrics"] += metrics

    # Полный архив первоисточников — отдельной страницей, а не тем огрызком,
    # что помещается в сводную таблицу.
    fl = fetch(f"{BASE}/{ticker}/f/l/", cache_dir)
    res["pages"]["documents"] = fl is not None
    if fl:
        res["documents"] = parse_documents_list(fl)

    d = fetch(f"{BASE}/{ticker}/dividend/", cache_dir)
    res["pages"]["dividend"] = d is not None
    if d:
        res["dividend_payments"], res["dividend_annual"] = parse_dividends(d)

    s = fetch(f"{BASE}/{ticker}/shareholders/", cache_dir)
    res["pages"]["shareholders"] = s is not None
    if s:
        res["shareholders"], res["shareholders_as_of"] = parse_shareholders(s)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ticker")
    ap.add_argument("--standards", default="MSFO,RSBU",
                    help="какие стандарты забирать, через запятую (MSFO,RSBU)")
    ap.add_argument("--cache-dir", type=Path)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()

    setup_logging(a.verbose)
    standards = tuple(x.strip() for x in a.standards.split(",") if x.strip())
    log.info("старт: %s (%s)", a.ticker, ",".join(standards))
    try:
        res = parse_company(a.ticker, standards, a.cache_dir)
    except Exception:
        log.exception("парсер упал на %s", a.ticker)
        raise
    codes = {m["metric_code"] for m in res["metrics"]}
    filled = sum(1 for m in res["metrics"] if m["value"] is not None)
    zeros = sum(1 for m in res["metrics"] if m["note"] == "zero_as_missing")
    log.info("%s: страницы=%s метрик=%d кодов=%d заполнено=%d zero_as_missing=%d "
             "факторов=%s/%s выплат=%d акционеров=%d (структура от %s) документов=%d",
             a.ticker, res["pages"], len(res["metrics"]), len(codes), filled, zeros,
             len(res.get("factors", [])), res.get("factors_total"),
             len(res.get("dividend_payments", [])), len(res.get("shareholders", [])),
             res.get("shareholders_as_of"), len(res["documents"]))

    # Пустая карточка — не успех. Если страница отдалась, а метрик нет, это сбой парсера.
    if res["pages"].get("year_MSFO") and not res["metrics"]:
        log.error("%s: годовая страница есть, а метрик ноль — парсер сломан", a.ticker)
    if res.get("shareholders") and not res.get("shareholders_as_of"):
        log.warning("%s: структура акционеров без даты обновления — отдавать агенту нельзя",
                    a.ticker)
    out = json.dumps(res, ensure_ascii=False, indent=1)
    if a.out:
        a.out.write_text(out, encoding="utf-8")
    else:
        print(out)


if __name__ == "__main__":
    main()
