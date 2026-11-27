"""SCHA DOCX parser — справка о стоимости чистых активов (форма ЦБ № 0420502) в
формате Word (.docx).

Используется УК, публикующими справку в DOCX (напр. «Герои» / ih-capital.ru), где
PDF/XLSX недоступны. Возвращает ТОТ ЖЕ result-dict, что `parse_scha` (PDF), — чтобы
`manual_scha_backfill` работал без изменений в логике сохранения.

⚠️ Особенность DOCX: секция ценных бумаг (2.7 акции, 2.1 облигации и т.д.) часто
разбита Word'ом на НЕСКОЛЬКО `<w:tbl>` — шапка с колонками только в ПЕРВОЙ таблице,
продолжения идут БЕЗ шапки (сразу строки данных). Поэтому:
  • распознали колонки по шапке (`_detect_columns`) → запоминаем маппинг и число колонок;
  • таблицу без шапки с ТЕМ ЖЕ числом колонок и валидным ISIN в isin-колонке считаем
    ПРОДОЛЖЕНИЕМ той же секции и переиспользуем маппинг.
Так же безопасно отсекаются не-бумаги (РЕПО/дебиторка): у них в isin-колонке не ISIN.

⚠️ У ОПИФ форма 0420502 обычно НЕ несёт ISIN инвестиционных паёв → `isin_pif=None`;
фонд резолвится при ингесте по имени/тикеру (см. manual_scha_backfill).

Переиспользуем единый источник истины парсинга из scha_parser: COL_PATTERNS через
`_detect_columns`, числовые парсеры, извлечение «Общей стоимости активов».
"""
from __future__ import annotations

import re
import zipfile
from io import BytesIO
from xml.etree import ElementTree as ET

from .scha_parser import _detect_columns, _parse_int, _parse_float, _norm

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def _extract_gross_assets(full_text: str):
    # В DOCX метка идёт как «…Общая стоимость активов … - Сумма на текущую
    # отчетную дату 1 Значение 811 194 747,78». Надёжный якорь суммы — слово
    # «Значение» непосредственно перед числом (между меткой и им стоит
    # цифра-код строки «1», поэтому [^0-9] не годится). Тысячи разделены
    # пробелом/nbsp — \s в Unicode-режиме ловит и обычный пробел, и \xa0.
    m = re.search(
        r"Общая стоимость активов.{0,160}?Значение\s+(\d[\d\s]{4,}[,.]\d{2})",
        full_text,
    )
    if not m:
        return None
    raw = re.sub(r"\s", "", m.group(1)).replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _cell_text(tc) -> str:
    """Весь текст ячейки (склейка всех <w:t> внутри <w:tc>)."""
    return " ".join((t.text or "") for t in tc.iter(_W + "t")).strip()


def _table_rows(tbl) -> list[list[str]]:
    return [[_cell_text(tc) for tc in tr.findall(_W + "tc")]
            for tr in tbl.findall(_W + "tr")]


def _col(cols: dict, row: list[str], key: str):
    i = cols.get(key)
    return row[i] if (i is not None and i < len(row)) else None


def _infer_isin_col(data_rows: list[list[str]], taken: set[int]) -> "int | None":
    """Индекс колонки с ISIN, когда шапка её не назвала.

    В подразделе «Иные ценные бумаги» формы 0420502 колонка озаглавлена
    «Сведения, позволяющие установить ценные бумаги» — слова ISIN в шапке НЕТ,
    поэтому `_detect_columns` её не находит, а требование name+isin+positions
    роняет ВСЮ секцию молча (ГЕРОИ, июль-2026: потерялся ВЭБ2Р-58 на 51.7 млн ₽
    = 7% фонда). Ищем колонку по ДАННЫМ: где больше всего ячеек — валидный ISIN.
    Колонки, уже занятые шапкой (эмитент/ИНН/ОГРН/количество/стоимость),
    пропускаем — иначе можно перехватить чужую.
    """
    best, best_hits = None, 0
    for i in range(max((len(r) for r in data_rows), default=0)):
        if i in taken:
            continue
        hits = sum(1 for r in data_rows if i < len(r) and _ISIN.match(_norm(r[i])))
        if hits > best_hits:
            best, best_hits = i, hits
    return best if best_hits else None


def parse_scha_docx(docx_bytes: bytes) -> dict:
    """Парсит SCHA-DOCX (форма 0420502) → result-dict как у parse_scha."""
    result = {
        "snapshot_date": None,
        "fund_name": None,
        "isin_pif": None,
        "assets": [],
        "total_assets": 0,
        "gross_assets_rub": None,
        "pages": 0,
        "parser_strategy": "docx",
    }
    root = ET.fromstring(zipfile.ZipFile(BytesIO(docx_bytes)).read("word/document.xml"))
    full = " ".join((t.text or "") for t in root.iter(_W + "t"))

    # Отчётная дата (ДД.ММ.ГГГГ → ISO).
    dm = re.search(r"\b(\d{2})\.(\d{2})\.(2\d{3})\b", full)
    if dm:
        result["snapshot_date"] = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"
    # Имя фонда — «…» после слова «фонд».
    fm = re.search(r"фонд[а-я ]*«([^»]+)»", full)
    if fm:
        result["fund_name"] = fm.group(1).strip()
    # ISIN паёв — до раздела с составом (обычно у ОПИФ отсутствует → None).
    head = full.split("Раздел 3")[0]
    im = re.search(r"\b([A-Z]{2}[A-Z0-9]{10})\b", head)
    if im:
        result["isin_pif"] = im.group(1)
    # Общая стоимость активов (знаменатель весов) — Раздел 3. Подраздел 10.
    result["gross_assets_rub"] = _extract_gross_assets(full)

    # Ценные бумаги: все таблицы с колонками эмитент+ISIN+количество (+ продолжения).
    last_cols, last_n = None, None
    seen: set[tuple] = set()
    for tbl in root.iter(_W + "tbl"):
        rows = _table_rows(tbl)
        if not rows:
            continue
        ncols = len(rows[0])
        cols = _detect_columns(rows[0])
        # Шапка назвала эмитента и количество, но не ISIN («Иные ценные бумаги»)
        # → достаём индекс колонки из самих данных, иначе секция потеряется.
        if "isin" not in cols and all(k in cols for k in ("name", "positions")):
            inferred = _infer_isin_col(rows[1:], set(cols.values()))
            if inferred is not None:
                cols["isin"] = inferred
        if all(k in cols for k in ("name", "isin", "positions")):
            last_cols, last_n = cols, ncols
            data = rows[1:]                       # пропускаем строку-шапку
        elif (last_cols and ncols == last_n and last_cols["isin"] < ncols
              and _ISIN.match(_norm(rows[0][last_cols["isin"]]))):
            cols = last_cols                      # таблица-продолжение без шапки
            data = rows
        else:
            continue

        mx = max(cols.values())
        for r in data:
            if len(r) <= mx:
                continue
            isin = _norm(_col(cols, r, "isin"))
            if not _ISIN.match(isin):             # строка-нумерация (1,2,3…), не-бумаги
                continue
            value_rub = _parse_float(_col(cols, r, "value")) if "value" in cols else None
            if value_rub is not None and value_rub > 5e11:   # колонки считаны неверно
                continue
            name = _norm(_col(cols, r, "name")) or "(name from ISIN)"
            key = (isin, name)
            if key in seen:
                continue
            seen.add(key)
            result["assets"].append({
                "asset_name": name,
                "isin": isin,
                "ogrn": _norm(_col(cols, r, "ogrn")) or None,
                "inn": _norm(_col(cols, r, "inn")) or None,
                "regnum": None,
                "positions": _parse_int(_col(cols, r, "positions")),
                "value_rub": value_rub,
                "maturity": None,
            })

    result["total_assets"] = sum(a.get("value_rub") or 0 for a in result["assets"])
    return result
