"""
SCHA XLS/XLSX parser — для УК которые публикуют справку о СЧА в Excel-формате
(Альфа-Капитал, и предположительно Сбер/Т-Капитал/Атон тоже).

Структура файла строго по форме Банка России № 0420502 (Excel-вариант):
  Раздел 1. Реквизиты ПИФ      (строки 4-7)
  Раздел 2. Параметры справки   (строки 9-12) — отчётная дата
  Раздел 3. Активы:
    Подраздел 1.    Денежные средства
    Подраздел 2.    Ценные бумаги российских эмитентов
    Подраздел 3.    Ценные бумаги иностранных эмитентов
    Подраздел 4.    Недвижимость
    ... и т.д.
  Раздел 4. Обязательства
  Раздел 5. Стоимость чистых активов

Расшифровки активов идут после раздела 6 с шапкой:
  «Расшифровки раздела 3 «Активы». Подраздел 2.1. Облигации российских хоз.обществ»
  «Расшифровки раздела 3 «Активы». Подраздел 2.7. Акции российских АО»
  ... и т.п.

В каждой расшифровке таблица имеет колонки (нумерация по индексам ячеек,
0-based, всё что не «1» в строке-нумерации в реальных данных колонка):
  1 - Наименование эмитента
  2 - ОГРН
  4 - ИНН
  6 - Правовая форма
  9 - Субординированные (Да/Нет)  [только для облигаций]
  11 - Регистрационный номер
  14 - ISIN
  15 - Дата погашения  [только для облигаций]
  16 - Количество в составе активов  ← positions
  17 - Стоимость актива в рублях  ← value_rub
  18 - Биржа
  19 - Уровень иерархии справедливой стоимости

ВАЖНО: колонки 16 и 17 могут СЛЕГКА сдвигаться у разных УК. Парсер ищет
их по тексту заголовка строки 179 (или ближайшего header row), не по
фиксированным позициям.
"""
import re
from datetime import date as _date
from pathlib import Path
from typing import Optional

import io

import pandas as pd

# ISIN российских эмитентов
ISIN_PATTERN = re.compile(r"^RU\d{3}[A-Z0-9]{7}$")
ISIN_PIF_PATTERN = re.compile(r"^RU\d{3}A[A-Z0-9]{6}$")  # инвестпаи (с буквой A в начале NSIN)
# Любой 12-символьный ISIN (вкл. иностранные) — для row-wise обхода.
ISIN_ANY = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
# Дата в ячейке: 28.02.2029 ИЛИ ISO 2038-05-19 (pandas Timestamp str) — НЕ число.
_DATE_RE_XLS = re.compile(r"\d{1,2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2}")


def _norm_number(v) -> Optional[float]:
    """Excel может хранить число как float или строку с пробелами/запятыми."""
    if v is None or pd.isna(v):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s in ("-", "—", "N/A"):
        return None
    # Русский формат: "140 725 205,40" → 140725205.40
    s = s.replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _find_metadata(df: pd.DataFrame) -> dict:
    """Извлечь метаданные из верхних строк XLS."""
    meta = {"fund_name": None, "isin_pif": None, "snapshot_date": None}
    # Поиск имени фонда + ISIN-пая в первых 50 строках (архивный формат держит
    # параметры справки в строках 15-20, не в 5-10).
    for i in range(min(50, len(df))):
        row = [str(v) if not pd.isna(v) else "" for v in df.iloc[i].values]
        joined = " ".join(row)
        # Имя фонда: "Биржевой/Открытый паевой инвестиционный фонд..."
        if not meta["fund_name"]:
            m = re.search(
                r"((?:Биржевой|Открытый|Закрытый|Интервальный)\s+паевой\s+инвестиционный\s+фонд[^|]{0,200})",
                joined,
                re.I,
            )
            if m:
                meta["fund_name"] = m.group(1).strip().rstrip("|").strip()
        # ISIN пая (12 chars, начинается RU000A...)
        if not meta["isin_pif"]:
            for v in row:
                vs = v.strip()
                if ISIN_PIF_PATTERN.match(vs):
                    meta["isin_pif"] = vs
                    break
        # Snapshot date — два формата:
        #   1. Новый/промежуточный: "2026.01.30" (YYYY.MM.DD)
        #   2. Архивный 2021: "31.03.2021" (DD.MM.YYYY)
        # Берём ПЕРВУЮ найденную — обычно это "Отчётная дата" в шапке,
        # но в архиве col 4 может содержать "Предыдущая отчётная дата" (берём её
        # только если в col 0 не нашли).
        if not meta["snapshot_date"]:
            m_iso = re.search(r"\b(\d{4})\.(\d{2})\.(\d{2})\b", joined)
            if m_iso:
                meta["snapshot_date"] = f"{m_iso.group(1)}-{m_iso.group(2)}-{m_iso.group(3)}"
            else:
                # DD.MM.YYYY — берём из первой непустой ячейки слева
                for v in row:
                    m_ru = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", v.strip())
                    if m_ru:
                        meta["snapshot_date"] = f"{m_ru.group(3)}-{m_ru.group(2)}-{m_ru.group(1)}"
                        break
        if all(meta.values()):
            break
    return meta


def _find_asset_sections(df: pd.DataFrame) -> list[dict]:
    """
    Найти все секции с активами и их границы.

    Поддерживает три формата формы ЦБ № 0420502:
    1. **Новый формат** (с 2024+): «Расшифровки раздела 3 «Активы». Подраздел X.Y. ...»
    2. **Промежуточный** (2022–2023): «Расшифровки раздела III «Активы»» — общий
       заголовок, а внутри подразделы маркированы только номером «X.Y. ...» в
       начале строки (без префикса «Подраздел»).
    3. **Архивный** (2021): нет блока «Расшифровки» вообще. Holdings лежат прямо
       внутри «Раздел III. Активы» в подразделах «Подраздел N. …» с ЦЕЛЫМ номером.
       Подраздел 1 — денежные средства (skip), Подраздел 2 — ЦБ российских эмитентов,
       Подраздел 3 — ЦБ иностранных эмитентов, Подраздел 4 — недвижимость (skip).
    """
    sections = []
    # Новый формат: явно «Подраздел X.Y» в названии расшифровки
    new_pattern = re.compile(
        r"Расшифровки раздела (?:3|III) .Активы.* Подраздел (\d+\.\d+)",
        re.I,
    )
    # Промежуточный: sub-header типа «2.7. Акции российских акционерных обществ»
    # ВНУТРИ блока «Расшифровки раздела III»
    old_sub_pattern = re.compile(r"^(\d+\.\d+)\.\s+\S", re.I)
    old_расшифровки_pattern = re.compile(
        r"Расшифровки раздела (?:3|III) .Активы.",
        re.I,
    )
    # Архивный 2021: «Подраздел 2.» с целым номером, либо в начале строки,
    # либо после «\n» если ячейка multi-line («Раздел III. Активы\nПодраздел 1»).
    archive_sub_pattern = re.compile(
        r"(?:^|\n)\s*Подраздел\s+(\d+)\.\s+(.+?)(?:\n|$)",
        re.I,
    )
    # Маркер начала Раздела III (включая multi-line)
    razdel_iii_pattern = re.compile(
        r"Раздел\s+(?:3|III)\.\s+Активы",
        re.I,
    )
    # Маркер выхода из Раздела III
    razdel_iv_pattern = re.compile(
        r"Раздел\s+(?:4|IV)\.\s+Обязательств",
        re.I,
    )

    in_old_расшифровки = False
    in_archive_razdel_iii = False
    saw_расшифровки_anywhere = False

    # Первый проход — определяем есть ли вообще «Расшифровки» в файле
    for i in range(min(len(df), 1000)):
        row = df.iloc[i].dropna()
        if row.empty:
            continue
        text = str(row.iloc[0])
        if old_расшифровки_pattern.search(text) or new_pattern.search(text):
            saw_расшифровки_anywhere = True
            break

    for i in range(len(df)):
        row = df.iloc[i].dropna()
        if row.empty:
            continue
        text = str(row.iloc[0]).strip()
        # Новый формат — explicit section
        m = new_pattern.search(text)
        if m:
            sections.append({"start": i, "subsection": m.group(1), "label": text[:200]})
            continue
        # Открытие промежуточного блока «Расшифровки раздела III «Активы»»
        if old_расшифровки_pattern.search(text):
            in_old_расшифровки = True
            in_archive_razdel_iii = False  # archive logic уже не нужна
            continue
        # Закрытие блока (переходим к Обязательствам)
        if in_old_расшифровки and "Расшифровки раздела" in text and "Обязательств" in text:
            in_old_расшифровки = False
            continue
        # Внутри промежуточного блока: «2.7. Акции...»
        if in_old_расшифровки:
            m = old_sub_pattern.match(text)
            if m:
                sections.append({"start": i, "subsection": m.group(1), "label": text[:200]})
            continue
        # Архивный формат — fallback если в файле нет «Расшифровок» вообще
        if not saw_расшифровки_anywhere:
            if razdel_iii_pattern.search(text):
                in_archive_razdel_iii = True
            if in_archive_razdel_iii and razdel_iv_pattern.search(text):
                in_archive_razdel_iii = False
                continue
            if in_archive_razdel_iii:
                # Ищем «Подраздел N.» в ячейке (возможно multi-line)
                for m in archive_sub_pattern.finditer(text):
                    sub_num = m.group(1)
                    # Skip Подраздел 1 (денежные средства) и 4 (недвижимость) —
                    # они не содержат ISIN-based holdings
                    if sub_num in ("1", "4"):
                        continue
                    sections.append({
                        "start": i,
                        "subsection": f"{sub_num}.0",  # canonicalize: 2 → 2.0
                        "label": text[:200],
                    })
                    break  # одна секция на строку
    # Заполним end_row
    for idx, s in enumerate(sections):
        s["end"] = sections[idx + 1]["start"] if idx + 1 < len(sections) else len(df)
    return sections


def _detect_column_positions(df: pd.DataFrame, start: int, end: int) -> Optional[dict]:
    """
    Найти позиции колонок в данной секции по тексту шапки.
    Возвращает {field_name: col_index} или None.
    """
    # Шапка обычно на 1-3 строки после section header
    for i in range(start, min(start + 6, end)):
        row = [str(v) if not pd.isna(v) else "" for v in df.iloc[i].values]
        # `\n` в ячейках Excel — обычное дело для шапок ("Наименование\nэмитента"),
        # нормализуем перед поиском
        joined_norm = "|".join(c.lower().replace("\n", " ") for c in row)
        if "наименование эмитента" not in joined_norm:
            continue
        # Найдём индексы колонок по ключевым словам в тексте
        cols = {}
        for j, cell in enumerate(row):
            c = cell.lower().replace("\n", " ")
            if "наименование эмитента" in c and "name" not in cols:
                cols["name"] = j
            elif "огрн" in c and "ogrn" not in cols:
                cols["ogrn"] = j
            elif "инн" in c and "inn" not in cols:
                cols["inn"] = j
            elif "регистрационный номер" in c and "reg" not in cols:
                cols["reg"] = j
            elif "isin" in c and "isin" not in cols:
                cols["isin"] = j
            elif "дата погашения" in c and "maturity" not in cols:
                cols["maturity"] = j
            elif "количество" in c and "положения" not in c and "положений" not in c and "positions" not in cols:
                cols["positions"] = j
            elif "стоимость актива" in c and "value" not in cols:
                cols["value"] = j
            elif "биржа" in c and "exchange" not in cols:
                cols["exchange"] = j
        if "isin" in cols and "positions" in cols and "value" in cols:
            return cols
    return None


def _parse_asset_rows(df: pd.DataFrame, start: int, end: int, cols: dict) -> list[dict]:
    """Извлечь строки активов из секции по позициям колонок."""
    assets = []
    for i in range(start, end):
        row = df.iloc[i]
        # Возьмём ISIN из колонки isin
        isin_cell = row.iloc[cols["isin"]] if cols["isin"] < len(row) else None
        if pd.isna(isin_cell):
            continue
        isin_str = str(isin_cell).strip().replace(" ", "")
        if not ISIN_PATTERN.match(isin_str):
            continue
        positions = _norm_number(row.iloc[cols["positions"]]) if cols["positions"] < len(row) else None
        value_rub = _norm_number(row.iloc[cols["value"]]) if cols["value"] < len(row) else None
        if positions is None and value_rub is None:
            continue
        name = str(row.iloc[cols["name"]]).strip() if cols.get("name") is not None and cols["name"] < len(row) else None
        ogrn = str(row.iloc[cols["ogrn"]]).strip() if cols.get("ogrn") is not None and cols["ogrn"] < len(row) else None
        if ogrn == "nan":
            ogrn = None
        inn = str(row.iloc[cols["inn"]]).strip() if cols.get("inn") is not None and cols["inn"] < len(row) else None
        if inn == "nan":
            inn = None
        regnum = str(row.iloc[cols["reg"]]).strip() if cols.get("reg") is not None and cols["reg"] < len(row) else None
        if regnum == "nan":
            regnum = None
        maturity = str(row.iloc[cols["maturity"]]).strip() if cols.get("maturity") is not None and cols["maturity"] < len(row) else None
        if maturity == "nan" or maturity == "01.01.0001":
            maturity = None
        assets.append({
            "asset_name": name,
            "isin": isin_str,
            "ogrn": ogrn,
            "inn": inn,
            "regnum": regnum,
            "positions": int(positions) if positions is not None and positions == int(positions) else (positions),
            "value_rub": value_rub,
            "maturity": maturity,
        })
    return assets


def _xls_cell_number(v):
    """Число из ячейки, НО отвергаем даты (дата погашения / ISO Timestamp)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if _DATE_RE_XLS.search(str(v)):
        return None
    return _norm_number(v)


def _detect_rasshifr_cols(df: pd.DataFrame, hdr_max: int = 10) -> Optional[dict]:
    """
    Найти колонки в листе-расшифровке XBRL-Excel (Атон) по ТЕКСТУ шапки.

    Возвращает {'isin': j, 'positions': j, 'value': j} (любой ключ может
    отсутствовать) если в первых hdr_max строках нашлась строка-шапка формы
    0420502, иначе None (лист без распознаваемой шапки → legacy-эвристика).

    🐞 Подраздел 3.4 «Иностранные депозитарные расписки»: в строке ДВА блока
    ISIN — самой расписки («…ISIN») и представляемых бумаг («…ISIN
    представляемых ценных бумаг»). Берём ПЕРВЫЙ (саму расписку), исключая
    «представляем». Иначе строка ГДР атрибутируется underlying-бумаге (ФосАгро
    ао RU000A0JRKT8, у которой есть СОБСТВЕННАЯ строка в Подразделе 2 акций),
    а её настоящие количество/стоимость теряются. Старая эвристика «два
    последних числа» брала тут ИНН эмитента (col «ИНН представляемых») за
    positions (~7.7e9) и код валюты 643-RUB за стоимость.
    """
    for i in range(min(hdr_max, len(df))):
        row = [("" if pd.isna(v) else str(v)).lower().replace("\n", " ")
               for v in df.iloc[i].values]
        joined = " | ".join(row)
        # Шапку узнаём по столбцу-имени эмитента ИЛИ по столбцу «Количество».
        if "наименование эмитента" not in joined and "количество" not in joined:
            continue
        cols: dict = {}
        for j, c in enumerate(row):
            if "isin" in c and "представляем" not in c and "isin" not in cols:
                cols["isin"] = j
            elif "количество" in c and "positions" not in cols:
                cols["positions"] = j
            elif "стоимость" in c and "чистых активов" not in c and "value" not in cols:
                cols["value"] = j
        return cols
    return None


def _parse_rasshifr_rows(df: pd.DataFrame, cols: dict, isin_pif, seen: set) -> list[dict]:
    """Header-aware разбор holdings-строк листа-расшифровки по найденным колонкам."""
    out: list[dict] = []
    ic, pc, vc = cols["isin"], cols["positions"], cols["value"]
    for _, r in df.iterrows():
        cells = ["" if pd.isna(v) else str(v) for v in r.values]
        if ic >= len(cells):
            continue
        isin = re.sub(r"\s", "", cells[ic])
        if not ISIN_ANY.match(isin) or isin == isin_pif or isin in seen:
            continue
        low = " ".join(cells).lower()
        if "итого" in low or "репо" in low:
            continue
        positions = _xls_cell_number(cells[pc]) if pc < len(cells) else None
        value_rub = _xls_cell_number(cells[vc]) if vc < len(cells) else None
        if positions is None and value_rub is None:
            continue
        if value_rub is not None and value_rub > 5e11:
            continue
        seen.add(isin)
        out.append({
            "asset_name": "(name from ISIN)",
            "isin": isin,
            "ogrn": None, "inn": None, "regnum": None,
            # Количество допускается дробным (ФосАгро 5947.66) → округляем для bigint.
            "positions": int(round(positions)) if positions is not None else None,
            "value_rub": value_rub,
            "maturity": None,
        })
    return out


def _parse_rowwise_legacy(df: pd.DataFrame, isin_pif, seen: set) -> list[dict]:
    """
    Legacy-эвристика для листов БЕЗ распознаваемой шапки (архивный формат без
    `Rasshifr_Akt`): в строке-позиции **количество и стоимость = два ПОСЛЕДНИХ
    числа** (ОГРН/ИНН/регномер/дата погашения идут раньше, биржа/«уровень 1» —
    текст в хвосте). Строки «итого» и РЕПО пропускаем; количество обязано быть
    целым.
    """
    out: list[dict] = []
    for _, r in df.iterrows():
        cells = ["" if pd.isna(v) else str(v) for v in r.values]
        isin = None
        for c in cells:
            cc = re.sub(r"\s", "", c)
            if ISIN_ANY.match(cc):
                isin = cc
                break
        if not isin or isin == isin_pif or isin in seen:
            continue
        low = " ".join(cells).lower()
        if "итого" in low or "репо" in low:
            continue
        nums = []
        for c in cells:
            if any(ch.isalpha() for ch in c):
                continue
            v = _xls_cell_number(c)
            if v is not None and v > 0:
                nums.append(v)
        if len(nums) < 2:
            continue
        positions, value_rub = nums[-2], nums[-1]
        if positions != int(positions):
            continue
        if value_rub > 5e11:
            continue
        seen.add(isin)
        out.append({
            "asset_name": "(name from ISIN)",
            "isin": isin,
            "ogrn": None, "inn": None, "regnum": None,
            "positions": int(positions),
            "value_rub": value_rub,
            "maturity": None,
        })
    return out


def _parse_xls_rowwise(sheets: dict, isin_pif=None) -> list[dict]:
    """
    Обход многолистового XBRL-Excel формы 0420502 (Атон): каждая расшифровка
    активов — отдельный лист `N; sr_0420502_Rasshifr_Akt/Ob_*`, а single-sheet
    section-логика `parse_scha_xls` их не видит (даёт 0).

    Читаем ПО ШАПКЕ (header-aware): находим колонки ISIN / Количество /
    Стоимость по тексту заголовка КАЖДОГО листа. Лист считается таблицей
    holdings ТОЛЬКО если в шапке есть И «Количество», И «Стоимость». Это
    отсекает вспомогательные таблицы расшифровки — напр. «Сведения об
    организациях, осуществляющих хранение/учёт ценных бумаг» (там есть
    «Количество ценных бумаг», но НЕТ «Стоимости»): раньше из них ИНН
    спецдепозитария (7710198911) попадал в positions ~7.7e9.

    Для листов без распознаваемой шапки (архивный формат) — legacy-эвристика
    «два последних числа».
    """
    assets: list[dict] = []
    seen: set[str] = set()
    for _name, df in sheets.items():
        if df is None or df.empty:
            continue
        cols = _detect_rasshifr_cols(df)
        if cols is not None:
            # Header-aware. holdings-лист — только при наличии И qty, И value, И isin.
            if "isin" not in cols or "positions" not in cols or "value" not in cols:
                continue  # вспомогательная/служебная таблица — пропускаем целиком
            assets.extend(_parse_rasshifr_rows(df, cols, isin_pif, seen))
        else:
            assets.extend(_parse_rowwise_legacy(df, isin_pif, seen))
    return assets


def _find_gross_assets(df: pd.DataFrame) -> Optional[float]:
    """
    «Раздел 3. Подраздел 10. Общая стоимость активов» → стоимость на ТЕКУЩУЮ
    отчётную дату (float, ₽). Это знаменатель для доли (как у investfunds),
    а НЕ Σ(value_rub) по бумагам — последняя не включает деньги/прочие активы.

    Раскладка (форма 0420502): строка-заголовок «…Подраздел 10. Общая стоимость
    активов», ниже шапка колонки, затем строка «Значение» с числом. Берём ПЕРВОЕ
    число > 1e6 в строке «Значение» (первая колонка = текущая дата; «1» —
    индекс колонки, отсекаем порогом). None если секции нет (старый формат).
    """
    marker = None
    for i in range(len(df)):
        joined = " ".join("" if pd.isna(v) else str(v) for v in df.iloc[i].values).lower()
        if "общая стоимость активов" in joined:
            marker = i
            break
    if marker is None:
        return None
    for i in range(marker + 1, min(marker + 12, len(df))):
        cells = df.iloc[i].values
        joined = " ".join("" if pd.isna(v) else str(v) for v in cells).lower()
        # Дошли до следующего раздела без «Значения» — выходим.
        if "раздел" in joined and "значение" not in joined:
            break
        if "значение" in joined:
            for v in cells:
                n = _norm_number(v)
                if n is not None and n > 1e6:
                    return n
    return None


def parse_scha_xls(xls_bytes_or_path) -> dict:
    """
    Парсит XLS/XLSX файл СЧА.

    Принимает:
        - bytes (содержимое файла) ИЛИ
        - str/Path (путь к файлу)

    Returns:
        {
            'snapshot_date': '2026-01-30' (ISO) или None,
            'fund_name': str (полное название),
            'isin_pif': str | None (ISIN пая фонда),
            'assets': [{
                'asset_name': str,
                'isin': str,
                'ogrn': str | None,
                'inn': str | None,
                'regnum': str | None,
                'positions': int | float | None,
                'value_rub': float | None,
                'maturity': str | None,
            }],
            'total_assets': int,
            'sections_parsed': int,
            'parser_strategy': 'xls',
        }
    """
    result = {
        "snapshot_date": None,
        "fund_name": None,
        "isin_pif": None,
        "assets": [],
        "total_assets": 0,
        "gross_assets_rub": None,
        "sections_parsed": 0,
        "parser_strategy": "xls",
    }

    try:
        src = io.BytesIO(xls_bytes_or_path) if isinstance(xls_bytes_or_path, (bytes, bytearray)) else xls_bytes_or_path
        xl = pd.ExcelFile(src)
        names = xl.sheet_names
        df = xl.parse(names[0], header=None)
    except Exception as e:
        result["error"] = f"read_excel: {e}"
        return result

    # 1. Метаданные. Для одноЛистового Альфа — на листе 0; для многоЛистового
    #    Атон лист 0 пуст → дочитываем реквизиты/параметры с первых листов.
    meta = _find_metadata(df)
    if not (meta["isin_pif"] and meta["snapshot_date"] and meta["fund_name"]):
        for n in names[1:7]:
            try:
                d2 = xl.parse(n, header=None)
            except Exception:
                continue
            m2 = _find_metadata(d2)
            for k in ("fund_name", "isin_pif", "snapshot_date"):
                if not meta[k] and m2[k]:
                    meta[k] = m2[k]
            if all(meta.values()):
                break
    result["snapshot_date"] = meta["snapshot_date"]
    result["fund_name"] = meta["fund_name"]
    result["isin_pif"] = meta["isin_pif"]

    # 2a. Стратегия A — single-sheet section-detection (Альфа human-readable XLS).
    section_assets = []
    sections = _find_asset_sections(df)
    for s in sections:
        cols = _detect_column_positions(df, s["start"], s["end"])
        if not cols:
            continue
        for a in _parse_asset_rows(df, s["start"], s["end"], cols):
            if a["isin"] in {x["isin"] for x in section_assets}:
                continue
            section_assets.append(a)

    # 2b. Стратегия B — multi-sheet row-wise (Атон XBRL-Excel: расшифровки на
    #     отдельных листах `…Rasshifr_Akt/Ob_*`).
    rowwise_assets = []
    # Берём ТОЛЬКО расшифровки АКТИВОВ формы 0420502 (листы «…Rasshifr_Akt_P*»).
    # Широкий числовой-префикс фильтр раньше затягивал лишние листы: «…0420503_R4_2»
    # (ДРУГАЯ форма 0420503, Раздел 4) давал фантомные +1.3 млрд → Σ активов
    # превышала «Общую стоимость активов» (Подраздел 10) у OPIF-63 (двойной счёт).
    akt = [n for n in names if re.search(r"Rasshifr_Akt", str(n), re.I)]
    if akt:
        rasshifr = akt
    else:
        # Старый формат (2021) без «Rasshifr_Akt» в имени листа — fallback на числовой
        # префикс, но исключаем форму 0420503 и Раздел 4/обязательства (не активы).
        rasshifr = [n for n in names
                    if ("asshifr" in n.lower() or re.match(r"^\s*\d+[;\s]", str(n)))
                    and "0420503" not in str(n)
                    and not re.search(r"(_R4|_Ob)", str(n), re.I)]
    if rasshifr:
        sheets = {}
        for n in rasshifr:
            try:
                sheets[n] = xl.parse(n, header=None)
            except Exception:
                pass
        rowwise_assets = _parse_xls_rowwise(sheets, meta["isin_pif"])

    # Берём стратегию с бо́льшим числом активов.
    if len(rowwise_assets) > len(section_assets):
        result["assets"] = rowwise_assets
        result["parser_strategy"] = "xls_rowwise"
        result["sections_parsed"] = len(rasshifr)
    else:
        result["assets"] = section_assets
        result["parser_strategy"] = "xls"
        result["sections_parsed"] = len(sections)

    result["total_assets"] = len(result["assets"])

    # Подраздел 10 «Общая стоимость активов» — знаменатель доли. Сначала на листе 0
    # (Альфа/Сбер single-sheet), затем фолбэк на прочие листы (Атон XBRL multi-sheet).
    gross = _find_gross_assets(df)
    if gross is None:
        for n in names[1:]:
            try:
                gross = _find_gross_assets(xl.parse(n, header=None))
            except Exception:
                continue
            if gross is not None:
                break
    result["gross_assets_rub"] = gross

    return result


def parse_scha_xls_file(path: str) -> dict:
    """Удобный wrapper для парсинга файла по пути."""
    return parse_scha_xls(path)


# ─────────────────────────────────────────────────────────────────────
# CLI для тестов
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python scha_xls_parser.py <path_to_xls>")
        sys.exit(1)
    r = parse_scha_xls_file(sys.argv[1])
    print(f"Snapshot date: {r['snapshot_date']}")
    print(f"Fund name: {r['fund_name']}")
    print(f"ISIN пая: {r['isin_pif']}")
    print(f"Sections parsed: {r['sections_parsed']}")
    print(f"Всего активов: {r['total_assets']}")
    print()
    # Top 10 по стоимости
    top = sorted(r["assets"], key=lambda a: -(a["value_rub"] or 0))[:10]
    print(f'{"ISIN":<14} {"Штук":>14} {"Руб":>20} Актив')
    print("─" * 100)
    for a in top:
        pos = f'{a["positions"]:>13,}' if a["positions"] else "           N/A"
        val = f'{a["value_rub"]:>19,.0f}' if a["value_rub"] else "                N/A"
        print(f'{a["isin"]:<14} {pos} {val} {(a["asset_name"] or "")[:50]}')
