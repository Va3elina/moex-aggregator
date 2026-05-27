"""
SCHA PDF parser — извлечение состава активов из обязательной отчётности УК
по форме Банка России № 0420502 «Справка о стоимости чистых активов».

Файл публикуется ежемесячно каждой УК на:
  - e-disclosure.ru (НРД) — для всех ПИФ
  - Сайтах самих УК — например /about/disclosure/pif/bpif/{slug}/reports/ у ВИМ

Структура SCHA PDF:
  Раздел I. Реквизиты ПИФ (название, ISIN паёв, ОКПО)
  Раздел II. Параметры справки (отчётная дата, валюта)
  Раздел III. Активы — детальные таблицы по подразделам:
    1. Денежные средства (счета, депозиты, платформы)
    2. Ценные бумаги российских эмитентов:
       2.1. Облигации хозобществ
       2.2. ОФЗ
       2.3. Региональные облигации
       2.4. Муниципальные
       2.5. Депозитарные расписки
       2.6. Паи ПИФ
       2.7. Акции российских АО ← главный для индексных фондов
       2.8. Векселя
    3. Ценные бумаги иностранных эмитентов
    4. Недвижимое имущество
    5. Имущественные права
    ...
  Раздел IV. Обязательства
  Раздел V. NAV (total)

Каждая таблица — 13-14 колонок: №, эмитент, ОГРН, ИНН, правовая форма,
[субординированные], регномер, ISIN, [дата погашения], количество, стоимость,
биржа, иерархия справедливой стоимости, примечание.

Парсер ищет ВСЕ таблицы с ISIN-колонкой и сливает их в один список assets.
"""
import io
import re
from typing import Iterable, Optional

import pdfplumber


# Регексы для определения колонок по заголовку (case-insensitive).
COL_PATTERNS = {
    "name": re.compile(r"наимен[ао]вание\s*эмитента", re.I),
    "ogrn": re.compile(r"ОГРН", re.I),
    "inn": re.compile(r"ИНН|идентификационный\s+номер\s+налог", re.I),
    "regnum": re.compile(r"регистрационный\s+номер[^A-Za-z]*\(идентификационный", re.I),
    "isin": re.compile(r"ISIN", re.I),
    "maturity": re.compile(r"дата\s+погашения", re.I),
    "positions": re.compile(r"количество.*актив|кол[\.\-]во.*актив", re.I | re.S),
    "value": re.compile(r"стоимость\s+актив", re.I),
    "exchange": re.compile(r"наименование\s+биржи", re.I),
}

# ISIN формат: 2 буквы страны + 10 alphanumeric. RU000A101EJ5 / RU0009024277.
# Допускаем пробелы в средине (Cbonds иногда форматирует).
ISIN_PATTERN = re.compile(r"^[A-Z]{2}\w{10}$")


def _norm(s: Optional[str]) -> str:
    """Normalize cell string for matching."""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def _detect_columns(header_row: list) -> dict:
    """
    Для каждой колонки header'а проверяем match с COL_PATTERNS.
    Возвращает {col_key → col_index}.
    """
    cols = {}
    for i, cell in enumerate(header_row):
        cell_norm = _norm(cell).replace("\n", " ")
        for key, pattern in COL_PATTERNS.items():
            if key in cols:
                continue  # уже нашли
            if pattern.search(cell_norm):
                cols[key] = i
                break
    return cols


def _parse_int(text: Optional[str]) -> Optional[int]:
    """'1 286 800' → 1286800. None если parsing fails."""
    if not text:
        return None
    cleaned = re.sub(r"[\s\xa0]", "", text)
    cleaned = re.sub(r"[^\d\-]", "", cleaned)
    if not cleaned or cleaned in ("-", "—"):
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_float(text: Optional[str]) -> Optional[float]:
    """'1 133 194 684,00' → 1133194684.0. Russian decimal comma."""
    if not text:
        return None
    cleaned = re.sub(r"[\s\xa0]", "", text).replace(",", ".")
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)
    if not cleaned or cleaned in ("-", "—", "."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_isin(text: Optional[str]) -> Optional[str]:
    """Извлекает ISIN из ячейки. Может содержать переносы строк."""
    if not text:
        return None
    cleaned = re.sub(r"\s+", "", text)
    if ISIN_PATTERN.match(cleaned):
        return cleaned
    return None


def _extract_pos_value(chunk: str) -> tuple[Optional[int], Optional[float]]:
    """
    В chunk (текст после ISIN) найти последовательность цифровых токенов,
    оканчивающуюся decimal-токеном (запятая для рублей).

    Проблема: positions и value разделены ОДНИМ пробелом, но внутри каждого
    числа тоже пробелы (тысячные разделители).
        Например: "обыкновенные 3 894 300 504 467 622,00"
        positions=3894300, value=504467622.00 (цена ~130 ₽/акция)

    Эвристика: перебираем splits и берём первый, где price = value/positions
    в диапазоне (0.5, 100_000) ₽ — реалистичный диапазон цен российских
    акций/облигаций.

    Используем только запятую как decimal separator (точку нет — это даты
    в формате 01.01.2030).
    """
    # Группа цифровых токенов + один decimal-токен с запятой.
    group_pattern = re.compile(
        r"(?:\b\d+\b[\s\xa0]+)+\b\d+,\d{1,2}\b"
    )
    g = group_pattern.search(chunk)
    if not g:
        return None, None

    tokens = g.group(0).split()
    decimal_token = tokens[-1]
    digit_tokens = tokens[:-1]

    # Перебираем splits: split_at = размер positions в токенах.
    # split_at=1 → max positions, минимум value tokens (price большой).
    # Берём первый split с разумной ценой.
    for split_at in range(1, len(digit_tokens) + 1):
        pos_tokens = digit_tokens[:split_at]
        val_tokens = digit_tokens[split_at:] + [decimal_token]
        try:
            positions = int("".join(pos_tokens))
            value_rub = float("".join(val_tokens).replace(",", "."))
        except ValueError:
            continue
        if positions <= 0 or value_rub <= 0:
            continue
        price = value_rub / positions
        if 0.5 <= price <= 100_000:
            return positions, value_rub

    # Не нашли разумного split — возможно вся строка это value (без positions)
    try:
        full_value = float("".join(tokens).replace(",", "."))
        return None, full_value
    except ValueError:
        return None, None


def _parse_assets_from_text(text: str) -> list[dict]:
    """
    Извлекает activs из raw text через regex.

    pdfplumber.extract_tables() даёт криво структурированные таблицы для
    БПИФ-SCHA где имена эмитентов рендерятся вертикальным шрифтом
    (каждая буква в отдельной ячейке). extract_text() ISIN'ы и числа
    извлекаются корректно — используем их.

    Структура одной строки в БПИФ-SCHA (выявлена из EQMX):
        N имя_вертикально ОГРН(13) ИНН(10) акцион... регномер
        ISIN [тип] positions value,XX биржа...

    Имя эмитента в БПИФ-формате нечитаемо (вертикальный фонт) — оставляем
    placeholder, имена дозаполняются из MOEX-метаданных по ISIN.
    """
    assets = []
    isin_pattern = re.compile(r"\bRU\d{3}[A-Z0-9]{4,8}\b")

    matches = list(isin_pattern.finditer(text))
    for i, m in enumerate(matches):
        isin = m.group(0)

        # chunk после ISIN до следующего ISIN (или 400 chars если последний)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else min(len(text), start + 400)
        chunk = text[start:end]

        positions, value_rub = _extract_pos_value(chunk)
        if positions is None and value_rub is None:
            continue

        # prev_chunk до ISIN — ищем ОГРН/ИНН/регномер
        prev_chunk = text[max(0, m.start() - 250) : m.start()]
        ogrn_match = re.search(r"\b(\d{13})\b", prev_chunk)
        inn_match = re.search(r"\b(\d{10})\b", prev_chunk)
        regnum_match = re.search(r"\b(\d-\d{2}-\d{4,5}-[A-Z0-9]+|\d{8,9}[A-Z])\b", prev_chunk)

        # Имя — placeholder. Восстановим через MOEX API по ISIN на этапе backfill.
        name = "(name from ISIN)"

        # Дедуп — иногда ISIN фонда (своего пая) встречается в шапке
        if any(a["isin"] == isin for a in assets):
            continue

        # Sanity filter: одна позиция не может стоить > 100 млрд ₽ (NAV любого
        # фонда не превышает этот порог для одной позиции). Если value_rub
        # астрономический — split не нашёлся правильно, пропускаем строку.
        if value_rub is not None and value_rub > 1e11:
            continue

        assets.append({
            "asset_name": name,
            "isin": isin,
            "ogrn": ogrn_match.group(1) if ogrn_match else None,
            "inn": inn_match.group(1) if inn_match else None,
            "regnum": regnum_match.group(1) if regnum_match else None,
            "positions": positions,
            "value_rub": value_rub,
            "maturity": None,
        })

    return assets


def parse_scha(pdf_bytes: bytes) -> dict:
    """
    Основная функция парсера.

    Двухэтапный подход:
      1. Пробуем extract_tables() — корректно работает для ОПИФ (Казначейский).
      2. Если ничего не нашли (БПИФ-формат с вертикальным текстом) —
         fallback на text+regex парсинг.

    Args:
        pdf_bytes: содержимое PDF файла как bytes.

    Returns:
        {
            'snapshot_date': '2026-04-30' (ISO) или None,
            'fund_name': str (полное название),
            'isin_pif': str | None (ISIN самого пая фонда),
            'assets': [{
                'asset_name': str,
                'isin': str | None,
                'ogrn': str | None,
                'inn': str | None,
                'regnum': str | None,
                'positions': int | None,
                'value_rub': float | None,
                'maturity': str | None,
            }],
            'total_assets': int,
            'pages': int,
            'parser_strategy': 'tables' | 'text+regex',
        }
    """
    result = {
        "snapshot_date": None,
        "fund_name": None,
        "isin_pif": None,
        "assets": [],
        "total_assets": 0,
        "pages": 0,
        "parser_strategy": None,
    }

    # Аккумулируем full text для fallback-стратегии.
    full_text_parts: list[str] = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        result["pages"] = len(pdf.pages)

        for page_no, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            full_text_parts.append(text)

            # Метаданные — на первой странице.
            if page_no == 0:
                date_match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", text)
                if date_match:
                    result["snapshot_date"] = (
                        f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                    )
                # ISIN пая фонда — 12 символов: RU + 3 digits + A + 6 alphanum.
                # Например RU000A101EJ5 (EQMX), RU000A1004T8 (Казначейский).
                isin_match = re.search(r"\bRU\d{3}A[A-Z0-9]{6}\b", text)
                if isin_match:
                    result["isin_pif"] = isin_match.group(0)

            # Парсим все таблицы на странице.
            tables = page.extract_tables() or []
            for table in tables:
                if len(table) < 2:
                    continue
                header = table[0]
                cols = _detect_columns(header)
                # Нам нужны таблицы с ISIN + положения. Без них — это
                # денежные средства / недвижимость / прочее.
                if "isin" not in cols or "positions" not in cols:
                    continue
                if "name" not in cols:
                    continue

                # Парсим data rows.
                for row in table[1:]:
                    if not row or len(row) < max(cols.values()) + 1:
                        continue

                    # SCHA имеет "column-numbering row" сразу после header'а:
                    # ['', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', ...].
                    # Это технический row для контроля формы — пропускаем.
                    # Эвристика: если все непустые ячейки — это короткие числа 1-20,
                    # это column numbering row.
                    non_empty = [c for c in row if c and _norm(c)]
                    if non_empty and all(
                        _norm(c).isdigit() and int(_norm(c)) <= 20
                        for c in non_empty
                    ):
                        continue

                    # Пропускаем строки "Итого" и пустые.
                    first_cell = _norm(row[0] or "")
                    if first_cell.startswith("Итог"):
                        continue

                    isin = _parse_isin(row[cols["isin"]] if cols.get("isin") is not None else None)
                    # Без ISIN — пропускаем (это footer/comment row)
                    if not isin:
                        continue

                    name = _norm(row[cols["name"]])
                    ogrn = _norm(row[cols.get("ogrn", -1)]) if "ogrn" in cols else None
                    inn = _norm(row[cols.get("inn", -1)]) if "inn" in cols else None
                    regnum = _norm(row[cols.get("regnum", -1)]) if "regnum" in cols else None
                    maturity = _norm(row[cols.get("maturity", -1)]) if "maturity" in cols else None

                    positions = _parse_int(row[cols["positions"]])
                    value_rub = _parse_float(row[cols["value"]]) if "value" in cols else None

                    # Sanity: должно быть хотя бы одно из positions/value
                    if positions is None and value_rub is None:
                        continue

                    result["assets"].append({
                        "asset_name": name,
                        "isin": isin,
                        "ogrn": ogrn or None,
                        "inn": inn or None,
                        "regnum": regnum or None,
                        "positions": positions,
                        "value_rub": value_rub,
                        "maturity": maturity or None,
                    })

    # Fallback: если extract_tables() ничего не нашёл ИЛИ нашёл мало активов
    # (< 20 — это признак БПИФ с вертикальным шрифтом где таблицы парсятся
    # частично), пробуем text+regex стратегию.
    tables_count = len(result["assets"])
    if tables_count < 20:
        full_text = "\n".join(full_text_parts)
        text_assets = _parse_assets_from_text(full_text)
        # Из text+regex имя пая фонда может попасть в список activs — отфильтруем.
        if result.get("isin_pif"):
            text_assets = [a for a in text_assets if a["isin"] != result["isin_pif"]]

        # Если text+regex нашёл больше активов — используем его.
        if len(text_assets) > tables_count:
            result["assets"] = text_assets

        # Sanity check: для cash-фондов (LQDT/GOLD) ISIN в SCHA фигурируют
        # только эпизодически (REPO-контрагенты) — не holdings.
        # Индексные фонды (EQMX) имеют ≥20 позиций.
        if len(result["assets"]) < 20:
            result["assets"] = []
            result["parser_strategy"] = "cash_or_repo_only"
        elif tables_count == 0:
            result["parser_strategy"] = "text+regex"
        else:
            # tables нашёл < 20, text+regex дал ≥20 — используем text+regex
            result["parser_strategy"] = "text+regex(upgraded from tables)"
    else:
        result["parser_strategy"] = "tables"

    result["total_assets"] = len(result["assets"])
    return result


def parse_scha_file(path: str) -> dict:
    """Удобный wrapper для парсинга файла."""
    with open(path, "rb") as f:
        return parse_scha(f.read())


# ─────────────────────────────────────────────────────────────────────
# CLI для тестов
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python scha_parser.py <path_to_pdf>")
        sys.exit(1)
    r = parse_scha_file(sys.argv[1])
    print(f"Snapshot date: {r['snapshot_date']}")
    print(f"ISIN пая: {r['isin_pif']}")
    print(f"Всего активов: {r['total_assets']}")
    print(f"Pages: {r['pages']}")
    print()
    # Top 10 by value
    top = sorted(r["assets"], key=lambda a: -(a["value_rub"] or 0))[:10]
    print(f'{"ISIN":<14} {"Штук":>14} {"Руб":>20} Актив')
    print("─" * 100)
    for a in top:
        pos = f'{a["positions"]:>13,}' if a["positions"] else "           N/A"
        val = f'{a["value_rub"]:>19,.0f}' if a["value_rub"] else "                N/A"
        print(f'{a["isin"] or "":<14} {pos} {val} {a["asset_name"][:50]}')
