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
import re
import sys
import time
import urllib.request
from datetime import date, datetime
from pathlib import Path

BASE = "https://smart-lab.ru/q"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
POLITE_DELAY = 0.5  # в разведке 223 бумаги × 2 страницы прошли без блокировок

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
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as ex:
        if ex.code == 404:
            return None
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
        return [], [], {}, []
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", table.group(0), re.S)

    # Шапка: первая строка, где встречается год ('2021') или квартал ('2025Q2').
    pat = r"^\d{4}Q[1-4]$" if period_type == "quarter" else r"^\d{4}$"
    periods, header_idx = [], None
    for i, tr in enumerate(trs):
        cs = row_cells(tr)
        cols = [(j, c[0]) for j, c in enumerate(cs)
                if re.match(pat, c[0]) or c[0].startswith("LTM")]
        if cols:
            periods = [(j, "LTM" if t.startswith("LTM") else t) for j, t in cols]
            header_idx = i
            break
    if header_idx is None:
        return [], [], {}, []

    report_dates, documents, metrics = {}, [], []
    for tr in trs[header_idx + 1:]:
        cs = row_cells(tr)
        if not cs:
            continue
        label = cs[0][0]
        code = next((c[1] for c in cs if c[1]), None)

        if label == "Дата отчета":
            for col, per in periods:
                if col < len(cs):
                    report_dates[per] = cs[col][0] or None
            continue
        if label in DOC_ROWS:
            for col, per in periods:
                if col < len(cs) and cs[col][2]:
                    for href in cs[col][2]:
                        documents.append({"doc_type": DOC_ROWS[label],
                                          "period": per, "url": href})
            continue
        if label in HEADER_ROWS or not code:
            continue

        code = CODE_ALIASES.get(code, code)
        share_class = "pref" if code in PREF_CODES else "common"
        canon = PREF_CODES.get(code, code)
        unit = label.split(",", 1)[1].strip() if "," in label else None

        for col, per in periods:
            if col >= len(cs):
                continue
            raw = cs[col][0]
            value = parse_number(raw)
            note = None
            if value == 0.0 and canon in ZERO_IS_MISSING and canon not in TRUE_ZERO_OK:
                # Ловушка Роснефти: ноль здесь означает «smart-lab не заполнил».
                value, note = None, "zero_as_missing"
            if value is None and not note and raw.strip() in ("", "-", "—"):
                note = "no_data"
            metrics.append({
                "metric_code": canon, "smartlab_code": code, "label": label,
                "unit": unit, "share_class": share_class,
                "period_type": "ltm" if per == "LTM" else period_type,
                "period": None if per == "LTM" else per,
                "value": value, "raw": raw, "note": note,
            })
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

def parse_company(ticker: str, standard: str = "MSFO", cache_dir: Path | None = None):
    res = {"ticker": ticker, "standard": standard,
           "captured_at": datetime.now().isoformat(timespec="seconds"),
           "pages": {}, "metrics": [], "documents": []}

    y = fetch(f"{BASE}/{ticker}/f/y/{standard}/", cache_dir)
    res["pages"]["year"] = y is not None
    if y:
        periods, metrics, rdates, docs = parse_financials(y, "year")
        res["year_periods"], res["report_dates_year"] = periods, rdates
        res["metrics"] += metrics
        res["documents"] += docs
        res["factors"], res["factors_total"] = parse_factors(y)

    q = fetch(f"{BASE}/{ticker}/f/q/{standard}/", cache_dir)
    res["pages"]["quarter"] = q is not None
    if q:
        periods, metrics, rdates, _ = parse_financials(q, "quarter")
        res["quarter_periods"], res["report_dates_quarter"] = periods, rdates
        res["metrics"] += metrics

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
    ap.add_argument("--standard", default="MSFO", choices=["MSFO", "RSBU"])
    ap.add_argument("--cache-dir", type=Path)
    ap.add_argument("--out", type=Path)
    a = ap.parse_args()

    res = parse_company(a.ticker, a.standard, a.cache_dir)
    codes = {m["metric_code"] for m in res["metrics"]}
    filled = sum(1 for m in res["metrics"] if m["value"] is not None)
    print(f"{a.ticker}: страницы={res['pages']} метрик={len(res['metrics'])} "
          f"кодов={len(codes)} заполнено={filled} "
          f"zero_as_missing={sum(1 for m in res['metrics'] if m['note']=='zero_as_missing')} "
          f"факторов={len(res.get('factors', []))}/{res.get('factors_total')} "
          f"выплат={len(res.get('dividend_payments', []))} "
          f"акционеров={len(res.get('shareholders', []))} "
          f"(структура от {res.get('shareholders_as_of')}) "
          f"документов={len(res['documents'])}", file=sys.stderr)
    out = json.dumps(res, ensure_ascii=False, indent=1)
    if a.out:
        a.out.write_text(out, encoding="utf-8")
    else:
        print(out)


if __name__ == "__main__":
    main()
