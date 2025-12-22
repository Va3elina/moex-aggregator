#!/usr/bin/env python3
"""
Скачивание данных MSC Insider по ID активов
Запись в таблицу open_interest
"""

import requests
from sqlalchemy import create_engine, text
from datetime import datetime
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация БД
DB_URL = "postgresql+pg8000://postgres:1803@localhost:5432/moex_db"

BASE_URL = "https://mscinsider.com/api/v1"

# Credentials для авторизации
MSC_EMAIL = "ermolaeffvadick@yandex.ru"
MSC_PASSWORD = "zgsD8YWvFh.mSiu"  # <-- УКАЖИ ПАРОЛЬ

def login_msc():
    """Авторизация в MSC Insider"""
    url = f"{BASE_URL}/auth/login"

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    })

    # ВАЖНО: используем 'login', а не 'email'
    payload = {
        'login': MSC_EMAIL,  # <-- login!
        'password': MSC_PASSWORD
    }

    try:
        response = session.post(url, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get('success'):
            logger.info(f"✓ Авторизация успешна: {data['data']['name']}")
            logger.info(f"  Подписка: {data['data']['role']['display_name']}")

            # Проверяем статус подписки
            sub_response = session.get(f"{BASE_URL}/subscription/current", timeout=30)
            if sub_response.ok:
                sub_data = sub_response.json()
                if sub_data.get('success'):
                    logger.info(f"  Статус: {sub_data['data']['status']}")
                    logger.info(f"  Активна до: {sub_data['data']['expires_at']}")

            return session
        else:
            logger.error(f"Ошибка авторизации: {data.get('error', 'Unknown')}")
            return None

    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        try:
            logger.error(f"Ответ сервера: {response.text[:200]}")
        except:
            pass
        return None

# Импортируем полный маппинг
from OI.MSCINSIDER.assets_mapping import ASSETS_MAP

# Для быстрого теста - популярные активы
QUICK_TEST = {
    221: 'USD/RUB',
    216: 'Сбербанк',
    200: 'Лукойл',
    243: 'Газпром',
    273: 'Московская биржа',
    227: 'Нефть Brent',
    195: 'Золото',
    280: 'Индекс MOEX (мини)',
}


def get_db_engine():
    """Создать engine SQLAlchemy"""
    return create_engine(DB_URL)


def get_instrument_id(engine, name):
    """Получить ID инструмента из БД по имени"""
    query = text("SELECT id FROM instruments WHERE name = :name LIMIT 1")
    with engine.connect() as conn:
        result = conn.execute(query, {"name": name}).fetchone()
        return result[0] if result else None


def check_existing_data(engine, instrument_id, date_from, date_to):
    """
    Проверить какие даты уже есть в БД
    Возвращает список дат которые НЕ нужно загружать
    """
    query = text("""
        SELECT DISTINCT tradedate 
        FROM open_interest 
        WHERE instrument_id = :instrument_id
          AND tradedate BETWEEN :date_from AND :date_to
        ORDER BY tradedate
    """)

    # Конвертируем строки в даты
    from datetime import datetime
    date_from_obj = datetime.strptime(date_from, '%Y%m%d').date()
    date_to_obj = datetime.strptime(date_to, '%Y%m%d').date()

    with engine.connect() as conn:
        result = conn.execute(query, {
            "instrument_id": instrument_id,
            "date_from": date_from_obj,
            "date_to": date_to_obj
        })
        existing_dates = set(row[0] for row in result)

    return existing_dates


def fetch_data(session, asset_id, endpoint, iz_fiz, date_from, date_to, timeframe='day'):
    """
    Запрос данных из MSC API

    session: requests.Session с авторизацией
    endpoint: 'legal' или 'individual'
    iz_fiz: True/False
    timeframe: 'day' или 'hour'
    """
    url = f"{BASE_URL}/assets/{asset_id}/{endpoint}"

    # Настройка таймфрейма
    if timeframe == 'day':
        interval = 1
        tf_type = 'day'
    else:  # hour
        interval = 60
        tf_type = 'minute'

    params = {
        'from': date_from,
        'to': date_to,
        'interval': interval,
        'type': tf_type,
        'iz_fiz': 'true' if iz_fiz else 'false',
        'timestamp': int(time.time() * 1000),
    }

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get('success'):
            return data.get('data', [])
        else:
            return []
    except Exception as e:
        logger.debug(f"Ошибка запроса {asset_id}/{endpoint}: {e}")
        return []


def save_to_db(engine, records):
    """Сохранить записи в БД"""
    if not records:
        return 0

    query = text("""
        INSERT INTO open_interest (
            instrument_id, sess_id, seqnum,
            tradedate, tradetime, ticker, clgroup,
            pos, pos_long, pos_short,
            pos_long_num, pos_short_num, systime
        ) VALUES (
            :instrument_id, NULL, NULL,
            :tradedate, :tradetime, :ticker, :clgroup,
            :pos, :pos_long, :pos_short,
            :pos_long_num, :pos_short_num, :systime
        )
    """)

    # Конвертируем tuple в dict для SQLAlchemy
    records_dict = [
        {
            'instrument_id': r[0],
            'tradedate': r[1],
            'tradetime': r[2],
            'ticker': r[3],
            'clgroup': r[4],
            'pos': r[5],
            'pos_long': r[6],
            'pos_short': r[7],
            'pos_long_num': r[8],
            'pos_short_num': r[9],
            'systime': r[10],
        }
        for r in records
    ]

    try:
        with engine.begin() as conn:
            conn.execute(query, records_dict)
        return len(records)
    except Exception as e:
        logger.error(f"Ошибка сохранения: {e}")
        logger.debug(f"Первая запись: {records[0]}")
        return 0


def process_asset(session, engine, asset_id, asset_name, date_from, date_to, timeframe='day'):
    """Обработать один актив"""

    logger.info(f"\n{'='*60}")
    logger.info(f"Актив ID={asset_id}: {asset_name}")
    logger.info(f"{'='*60}")

    # Получаем instrument_id из БД
    instrument_id = get_instrument_id(engine, asset_name)

    if not instrument_id:
        logger.warning(f"❌ Инструмент '{asset_name}' не найден в БД!")
        return

    logger.info(f"✓ Найден в БД: instrument_id={instrument_id}")

    # Проверяем что уже есть в БД
    existing_dates = check_existing_data(engine, instrument_id, date_from, date_to)

    if existing_dates:
        logger.info(f"ℹ В БД уже есть {len(existing_dates)} дней данных")
        logger.info(f"  От: {min(existing_dates)}, До: {max(existing_dates)}")

        # Спрашиваем что делать
        # Для автоматической загрузки можно убрать этот блок
        # и всегда пропускать существующие даты

    all_records = []

    # Конфигурации запросов
    configs = [
        ('legal', False, 'YUR'),      # Юр.лица - сделки
        ('legal', True, 'FIZ'),       # Физ.лица - сделки
        ('individual', False, 'YUR'), # Юр.лица - участники
        ('individual', True, 'FIZ'),  # Физ.лица - участники
    ]

    for endpoint, iz_fiz, clgroup in configs:
        data = fetch_data(session, asset_id, endpoint, iz_fiz, date_from, date_to, timeframe)

        if not data:
            logger.info(f"  {endpoint}/{clgroup}: нет данных")
            continue

        logger.info(f"  {endpoint}/{clgroup}: получено {len(data)} записей")

        for item in data:
            try:
                moment = datetime.strptime(item['moment'], '%Y-%m-%d %H:%M:%S')

                # Пропускаем если дата уже есть в БД
                if moment.date() in existing_dates:
                    continue

                # MSC: long и short положительные
                # MOEX: short должен быть отрицательным
                pos_long = int(item['value']['long'])
                pos_short = -abs(int(item['value']['short']))
                pos = pos_long + pos_short

                # Количество участников (только для individual)
                if endpoint == 'individual':
                    pos_long_num = int(item['value']['long'])
                    pos_short_num = int(item['value']['short'])
                else:
                    pos_long_num = None
                    pos_short_num = None

                record = (
                    instrument_id,      # instrument_id
                    moment.date(),      # tradedate
                    moment.time(),      # tradetime
                    asset_name,         # ticker (имя из БД)
                    clgroup,            # clgroup
                    pos,                # pos
                    pos_long,           # pos_long
                    pos_short,          # pos_short
                    pos_long_num,       # pos_long_num
                    pos_short_num,      # pos_short_num
                    datetime.now(),     # systime
                )
                all_records.append(record)

            except Exception as e:
                logger.error(f"Ошибка парсинга: {e}")
                continue

        time.sleep(0.5)  # Пауза между запросами

    # Сохраняем
    if all_records:
        saved = save_to_db(engine, all_records)
        logger.info(f"\n✓ Сохранено: {saved} новых записей")
    else:
        logger.warning(f"\n⚠ Нет новых данных для сохранения")

        for item in data:
            try:
                moment = datetime.strptime(item['moment'], '%Y-%m-%d %H:%M:%S')

                # MSC: long и short положительные
                # MOEX: short должен быть отрицательным
                pos_long = int(item['value']['long'])
                pos_short = -abs(int(item['value']['short']))
                pos = pos_long + pos_short

                # Количество участников (только для individual)
                if endpoint == 'individual':
                    pos_long_num = int(item['value']['long'])
                    pos_short_num = int(item['value']['short'])
                else:
                    pos_long_num = None
                    pos_short_num = None

                record = (
                    instrument_id,      # instrument_id
                    moment.date(),      # tradedate
                    moment.time(),      # tradetime
                    asset_name,         # ticker (имя из БД)
                    clgroup,            # clgroup
                    pos,                # pos
                    pos_long,           # pos_long
                    pos_short,          # pos_short
                    pos_long_num,       # pos_long_num
                    pos_short_num,      # pos_short_num
                    datetime.now(),     # systime
                )
                all_records.append(record)

            except Exception as e:
                logger.error(f"Ошибка парсинга: {e}")
                continue

        time.sleep(0.5)  # Пауза между запросами

    # Сохраняем
    if all_records:
        saved = save_to_db(engine, all_records)
        logger.info(f"\n✓ Сохранено: {saved} записей")
    else:
        logger.warning(f"\n❌ Нет данных для сохранения")


def main():
    """Главная функция"""

    # === НАСТРОЙКИ ===

    # Период загрузки: 2007-2021
    date_from = '20070101'
    date_to = '20211231'

    # Таймфрейм
    # 'day' - дневные данные (рекомендуется для исторических)
    # 'hour' - часовые данные (больше данных, дольше загрузка)
    timeframe = 'day'  # <-- ИЗМЕНИ НА 'hour' для часовых

    # РЕЖИМ ТЕСТА: используйте QUICK_TEST для проверки
    use_quick_test = False  # True = тест на 8 активах, False = все активы

    assets_map = QUICK_TEST if use_quick_test else ASSETS_MAP

    print("="*60)
    print(f"MSC Insider -> PostgreSQL")
    print(f"Период: {date_from} - {date_to}")
    print(f"Таймфрейм: {timeframe.upper()}")
    print(f"Режим: {'QUICK TEST' if use_quick_test else 'ПОЛНАЯ ЗАГРУЗКА'}")
    print("="*60)

    # Авторизация в MSC
    print("\nАвторизация...")
    session = login_msc()

    if not session:
        print("❌ Не удалось авторизоваться!")
        return

    print()

    # Подключение к БД
    engine = get_db_engine()

    print(f"Активов к обработке: {len(assets_map)}")
    print("="*60)

    # Обрабатываем
    for i, (asset_id, asset_name) in enumerate(assets_map.items(), 1):
        print(f"\n[{i}/{len(assets_map)}]")
        process_asset(session, engine, asset_id, asset_name, date_from, date_to, timeframe)
        time.sleep(1)  # Пауза между активами

    print("\n" + "="*60)
    print("Загрузка завершена!")
    print("="*60)

    # Статистика
    query = text("""
        SELECT 
            ticker,
            COUNT(*) as records,
            MIN(tradedate) as date_from,
            MAX(tradedate) as date_to
        FROM open_interest
        WHERE instrument_id IS NOT NULL
        GROUP BY ticker
        ORDER BY ticker
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()

    print("\nЗагружено данных:")
    print(f"{'Актив':<30} {'Записей':<10} {'От':<12} {'До'}")
    print("-"*60)
    for row in rows:
        print(f"{row[0]:<30} {row[1]:<10} {row[2]} {row[3]}")


if __name__ == "__main__":
    main()