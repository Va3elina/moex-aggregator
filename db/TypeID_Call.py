#!/usr/bin/env python3
"""
Загрузка фьючерсов в таблицу instruments.
- Правильные группы (Акции, Валюта, Сырьё, Индексы, Зарубежные активы)
- Исправленные sectype
- Особые случаи: GL/GLDRUBF, EU/Eu
"""

import re
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# === Настройки БД ===
DB_URL = "postgresql+pg8000://postgres:1803@localhost:5432/moex_db"
engine = create_engine(DB_URL)

# === Активы по группам ===
# Формат: (name, sectype, group)

ASSETS = [
    # === Акции ===
    ("Сбербанк", "SR", "Акции"),
    ("Газпром", "GZ", "Акции"),
    ("ВТБ", "VB", "Акции"),
    ("Сбербанк (вечн)", "SB", "Акции"),
    ("Т-Технологии (Тинькофф)", "TB", "Акции"),
    ("Лукойл", "LK", "Акции"),
    ("НОВАТЭК", "NK", "Акции"),
    ("Газпром (вечн)", "GA", "Акции"),
    ("СПБ биржа", "SE", "Акции"),
    ("Норильский никель", "GK", "Акции"),
    ("ПИК", "PI", "Акции"),
    ("Сбербанк (прив)", "SP", "Акции"),
    ("Яндекс", "YD", "Акции"),
    ("Полюс Золото", "PX", "Акции"),
    ("Роснефть", "RN", "Акции"),
    ("Татнефть", "TT", "Акции"),
    ("Аэрофлот", "AF", "Акции"),
    ("Московская биржа", "ME", "Акции"),
    ("Самолет", "SS", "Акции"),
    ("Сургутнефтегаз (прив)", "SG", "Акции"),
    ("X5 Group", "X5", "Акции"),
    ("ММК", "MG", "Акции"),
    ("Совкомбанк", "SC", "Акции"),
    ("Магнит", "MN", "Акции"),
    ("НЛМК", "NM", "Акции"),
    ("Северсталь", "CH", "Акции"),
    ("Сургутнефтегаз", "SN", "Акции"),
    ("Интер РАО", "IR", "Акции"),
    ("Мечел", "MC", "Акции"),
    ("АФК Система", "AK", "Акции"),
    ("Ростелеком", "RT", "Акции"),
    ("Русал", "RL", "Акции"),
    ("М.Видео", "MV", "Акции"),
    ("ВКонтакте", "VK", "Акции"),
    ("Whoosh", "WU", "Акции"),
    ("Транснефть", "TN", "Акции"),
    ("Белуга Групп", "NB", "Акции"),
    ("ФосАгро", "PH", "Акции"),
    ("МТС", "MT", "Акции"),
    ("Сегежа Групп", "SZ", "Акции"),
    ("АЛРОСА", "AL", "Акции"),
    ("Positive Technologies", "PS", "Акции"),
    ("Астра", "AS", "Акции"),
    ("ДВМП (FESCO)", "FE", "Акции"),
    ("ФСК Россети", "FS", "Акции"),
    ("РуссНефть", "RU", "Акции"),
    ("РусГидро", "HY", "Акции"),
    ("Совкомфлот", "FL", "Акции"),
    ("Банк Санкт-Петербург", "BS", "Акции"),
    ("Распадская", "RA", "Акции"),
    ("Татнефть (прив)", "TP", "Акции"),
    ("Юнипро", "UN", "Акции"),
    ("Европлан", "LE", "Акции"),
    ("Ренессанс Страхование", "RD", "Акции"),
    ("HeadHunter", "HD", "Акции"),
    ("ЭсЭфАй", "SH", "Акции"),
    ("МКБ", "CM", "Акции"),
    ("Газпром нефть", "SO", "Акции"),  # SO - буква O
    ("Башнефть", "BN", "Акции"),
    ("КАМАЗ", "KM", "Акции"),
    ("Артген", "IS", "Акции"),
    ("Софтлайн", "S0", "Акции"),  # S0 - цифра 0

    # === Валюта ===
    ("USD/RUB", "Si", "Валюта"),
    ("CNY/RUB", "CR", "Валюта"),
    ("CNY/RUB (вечн)", "CN", "Валюта"),
    ("USD/RUB (вечн)", "US", "Валюта"),
    ("EUR/RUB (вечн)", "EU", "Валюта"),  # EU - большие буквы!
    ("EUR/USD", "ED", "Валюта"),
    ("EUR/RUB", "Eu", "Валюта"),  # Eu - маленькая u!
    ("EUR/GBP", "EG", "Валюта"),
    ("USD/CNY", "UC", "Валюта"),
    ("HKD/RUB", "HK", "Валюта"),
    ("KZT/RUB", "KZ", "Валюта"),
    ("AED/RUB", "AE", "Валюта"),
    ("EUR/CAD", "EC", "Валюта"),
    ("USD/CAD", "CA", "Валюта"),
    ("USD/TRY", "TR", "Валюта"),
    ("USD/JPY", "JP", "Валюта"),
    ("USD/CHF", "CF", "Валюта"),
    ("GBP/USD", "GU", "Валюта"),
    ("AUD/USD", "AU", "Валюта"),

    # === Сырьё ===
    ("Нефть Brent", "BR", "Сырьё"),
    ("Нефть Brent (мини)", "BM", "Сырьё"),
    ("Золото", "GD", "Сырьё"),
    ("Серебро", "SV", "Сырьё"),
    ("Газ", "NG", "Сырьё"),
    ("Газ TTF", "FF", "Сырьё"),
    ("Газ (микро)", "NR", "Сырьё"),
    ("Платина", "PT", "Сырьё"),
    ("Паладий", "PD", "Сырьё"),
    ("Никель", "NC", "Сырьё"),
    ("Цинк", "ZC", "Сырьё"),
    ("Медь", "CE", "Сырьё"),
    ("Алюминий", "AN", "Сырьё"),
    ("Какао", "CC", "Сырьё"),
    ("Кофе", "KC", "Сырьё"),
    ("Апельсиновый сок", "OJ", "Сырьё"),
    ("Пшеница", "W4", "Сырьё"),
    ("Сахар", "SA", "Сырьё"),
    # GL - особый случай, обрабатывается отдельно (два актива с одним sectype)

    # === Индексы ===
    ("Индекс MOEX", "MX", "Индексы"),
    ("Индекс RTS", "RI", "Индексы"),
    ("Индекс МосБиржи (вечн)", "IM", "Индексы"),
    ("Индекс MOEX (мини)", "MM", "Индексы"),
    ("Индекс RTS (мини)", "RM", "Индексы"),
    ("Индекс RGBI", "RB", "Индексы"),
    ("RVI (индекс волатильности)", "VI", "Индексы"),
    ("Индекс Нефти и газа", "OG", "Индексы"),
    ("Индекс Металлов и добычи", "MA", "Индексы"),
    ("Индекс Финансов", "FN", "Индексы"),
    ("Индекс Потребительского сектора", "CS", "Индексы"),
    ("Индекс МосБиржи IPO", "IP", "Индексы"),
    ("Индекс МосБиржи в юанях", "MY", "Индексы"),
    ("Индекс московской недвижимости Дом Клик", "HO", "Индексы"),

    # === Зарубежные активы ===
    ("S&P500", "SF", "Зарубежные активы"),
    ("Tracker Fund of Hong Kong", "HS", "Зарубежные активы"),
    ("iShares Core EURO STOXX 50", "SX", "Зарубежные активы"),
    ("iShares Core DAX UCITS", "DX", "Зарубежные активы"),
    ("iShares Core Nikkei 225", "N2", "Зарубежные активы"),
    ("DJ Industrial Average", "DJ", "Зарубежные активы"),
    ("iShares Russell 2000", "R2", "Зарубежные активы"),
    ("iShares MSCI Emerging Markets", "EM", "Зарубежные активы"),
    ("iShares MSCI India UCITS", "ND", "Зарубежные активы"),
    ("Invesco PHLX Semiconductor", "SQ", "Зарубежные активы"),
    ("iShares Bitcoin", "IB", "Зарубежные активы"),
    ("ETHA Trust", "ET", "Зарубежные активы"),
    ("TLT", "TL", "Зарубежные активы"),
    ("Tencent", "TC", "Зарубежные активы"),
    ("Alibaba Group", "BB", "Зарубежные активы"),
    ("Xiaomi", "XI", "Зарубежные активы"),
    ("Baidu", "BD", "Зарубежные активы"),
]

# Особые случаи: один sectype, разные secid
SPECIAL_CASES = [
    # GL - два разных актива с одним sectype
    ("Золото (в руб за грамм)", "GL", "Сырьё", None),  # обычные контракты: GLH, GLM, etc.
    ("Золото (в руб за грамм) ВЕЧН", "GL", "Сырьё", "GLDRUBF"),  # только GLDRUBF
]


def fetch_all_futures():
    """Загружает список фьючерсов с MOEX ISS"""
    url = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
    print(f"📥 Загружаю фьючерсы с MOEX...")

    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"❌ Ошибка: HTTP {resp.status_code}")
        return None

    data = resp.json()
    sec = data.get("securities")
    if not sec:
        print("❌ Не найден раздел 'securities'")
        return None

    df = pd.DataFrame(sec["data"], columns=sec["columns"])
    print(f"✅ Загружено {len(df)} фьючерсов с биржи")
    return df


def extract_secid_base(secid: str) -> str:
    """Убирает год из secid: SRM5 -> SRM"""
    return re.sub(r'\d+$', '', secid)


def clear_futures_from_instruments():
    """Удаляет все фьючерсы из instruments"""
    with engine.begin() as conn:
        result = conn.execute(text("DELETE FROM instruments WHERE type = 'futures'"))
        print(f"🗑️  Удалено {result.rowcount} записей фьючерсов из instruments")


def insert_instruments(df_moex: pd.DataFrame):
    """Вставляет инструменты в БД"""

    # Создаём маппинг sectype -> (name, group)
    sectype_map = {sectype: (name, group) for name, sectype, group in ASSETS}

    # Собираем все sectype которые нам нужны
    needed_sectypes = set(sectype_map.keys())

    # Фильтруем данные с биржи
    df_filtered = df_moex[df_moex["SECTYPE"].isin(needed_sectypes)].copy()

    # Добавляем базовый secid (без года)
    df_filtered["secid_base"] = df_filtered["SECID"].apply(extract_secid_base)

    # Уникальные комбинации sectype + secid_base
    unique_contracts = df_filtered[["SECTYPE", "secid_base"]].drop_duplicates()

    print(f"📊 Найдено {len(unique_contracts)} уникальных контрактов для {len(needed_sectypes)} базовых активов")

    # Вставляем в БД
    inserted = 0
    with engine.begin() as conn:
        for _, row in unique_contracts.iterrows():
            sectype = row["SECTYPE"]
            secid = row["secid_base"]
            name, group = sectype_map.get(sectype, (None, None))

            if not name:
                continue

            conn.execute(text("""
                              INSERT INTO instruments (name, sectype, secid, type, "group")
                              VALUES (:name, :sectype, :secid, 'futures', :group) ON CONFLICT DO NOTHING
                              """), {
                             "name": name,
                             "sectype": sectype,
                             "secid": secid,
                             "group": group
                         })
            inserted += 1

    print(f"✅ Вставлено {inserted} записей")

    # Обработка особых случаев (GL)
    handle_special_cases(df_moex)


def handle_special_cases(df_moex: pd.DataFrame):
    """Обработка особых случаев: GL с разными secid"""

    print("\n📌 Обработка особых случаев...")

    with engine.begin() as conn:
        for name, sectype, group, special_secid in SPECIAL_CASES:
            if special_secid:
                # Вечный контракт с фиксированным secid
                conn.execute(text("""
                                  INSERT INTO instruments (name, sectype, secid, type, "group")
                                  VALUES (:name, :sectype, :secid, 'futures', :group) ON CONFLICT DO NOTHING
                                  """), {
                                 "name": name,
                                 "sectype": sectype,
                                 "secid": special_secid,
                                 "group": group
                             })
                print(f"  ✓ {name} -> {sectype}/{special_secid}")
            else:
                # Обычные контракты (уже добавлены в основном цикле, но с другим name)
                # Нужно обновить name для контрактов GL которые НЕ GLDRUBF
                df_gl = df_moex[df_moex["SECTYPE"] == sectype].copy()
                df_gl["secid_base"] = df_gl["SECID"].apply(extract_secid_base)

                for secid_base in df_gl["secid_base"].unique():
                    if secid_base != "GLDRUBF":
                        conn.execute(text("""
                                          INSERT INTO instruments (name, sectype, secid, type, "group")
                                          VALUES (:name, :sectype, :secid, 'futures', :group) ON CONFLICT DO NOTHING
                                          """), {
                                         "name": name,
                                         "sectype": sectype,
                                         "secid": secid_base,
                                         "group": group
                                     })

                print(f"  ✓ {name} -> {sectype}/* (кроме GLDRUBF)")


def verify_data():
    """Проверка загруженных данных"""

    print("\n" + "=" * 60)
    print("ПРОВЕРКА ДАННЫХ")
    print("=" * 60)

    with engine.connect() as conn:
        # Общая статистика
        result = conn.execute(text("""
                                   SELECT COUNT(*)                as total,
                                          COUNT(DISTINCT sectype) as sectypes,
                                          COUNT(DISTINCT "group") as groups
                                   FROM instruments
                                   WHERE type = 'futures'
                                   """))
        row = result.fetchone()
        print(f"\nВсего записей: {row[0]}")
        print(f"Уникальных sectype: {row[1]}")
        print(f"Групп: {row[2]}")

        # По группам
        result = conn.execute(text("""
                                   SELECT "group", COUNT(*) as cnt, COUNT(DISTINCT sectype) as sectypes
                                   FROM instruments
                                   WHERE type = 'futures'
                                   GROUP BY "group"
                                   ORDER BY cnt DESC
                                   """))

        print(f"\n{'Группа':<30} {'Записей':<10} {'Активов'}")
        print("-" * 55)
        for row in result.fetchall():
            print(f"{row[0] or 'Без группы':<30} {row[1]:<10} {row[2]}")

        # Проверка дублей sectype
        result = conn.execute(text("""
                                   SELECT sectype, array_agg(DISTINCT name) as names
                                   FROM instruments
                                   WHERE type = 'futures'
                                   GROUP BY sectype
                                   HAVING COUNT (DISTINCT name) > 1
                                   """))

        dups = result.fetchall()
        if dups:
            print(f"\n⚠️  Найдены sectype с разными name:")
            for row in dups:
                print(f"  {row[0]}: {row[1]}")
        else:
            print("\n✓ Конфликтов sectype нет")

        # Проверка EU vs Eu
        result = conn.execute(text("""
                                   SELECT name, sectype
                                   FROM instruments
                                   WHERE sectype IN ('EU', 'Eu')
                                     AND type = 'futures'
                                   """))
        eu_rows = result.fetchall()
        print(f"\nПроверка EU/Eu:")
        for row in eu_rows:
            print(f"  {row[0]} -> {row[1]}")


def main():
    print("=" * 60)
    print("ЗАГРУЗКА ФЬЮЧЕРСОВ В instruments")
    print("=" * 60)

    # 1. Загружаем данные с биржи
    df = fetch_all_futures()
    if df is None:
        return 1

    # 2. Очищаем старые данные
    response = input("\n🗑️  Удалить все фьючерсы из instruments? (yes/no): ").strip().lower()
    if response != 'yes':
        print("Отменено")
        return 0

    clear_futures_from_instruments()

    # 3. Вставляем новые данные
    insert_instruments(df)

    # 4. Проверяем
    verify_data()

    print("\n✅ Готово!")
    return 0


if __name__ == "__main__":
    exit(main())