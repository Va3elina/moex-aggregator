"""Исправление 5min данных для ВСЕХ 61 активов (2020-2025)"""
import requests
from sqlalchemy import create_engine, text
from datetime import datetime
import time
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")
MSC_EMAIL = "pnado157@gmail.com"
MSC_PASSWORD = "Qwghty56"
BASE_URL = "https://mscinsider.com/api/v1"

engine = create_engine(DB_URL)

# ПОЛНЫЙ маппинг (61 актив)
MSC_TO_SECTYPE = {
    283: 'AF', 301: 'AK', 282: 'AL', 227: 'BR', 388: 'BR', 385: 'CC',
    330: 'CR', 333: 'CR', 235: 'ED', 239: 'Eu', 332: 'Eu', 293: 'GD',
    195: 'GD', 349: 'GD', 243: 'GZ', 382: 'GZ', 328: 'GZ', 198: 'HY',
    362: 'IMOEXF', 305: 'IR', 200: 'LK', 203: 'MC', 273: 'ME', 274: 'MG',
    292: 'MM', 326: 'MT', 201: 'MX', 280: 'MX', 296: 'NG', 389: 'NG',
    395: 'NG', 318: 'NG', 302: 'NK', 204: 'NM', 281: 'NR', 208: 'PD',
    323: 'PH', 311: 'PI', 209: 'PT', 329: 'RB', 210: 'RI', 310: 'RI',
    213: 'RL', 240: 'RN', 241: 'RT', 228: 'SF', 219: 'SN', 218: 'SN',
    216: 'SR', 381: 'SR', 215: 'SR', 217: 'SV', 351: 'SZ', 221: 'Si',
    331: 'Si', 222: 'TT', 225: 'VB', 275: 'VI', 334: 'W4', 393: 'X5',
    378: 'YD'
}

ASSET_NAMES = {
    283: 'Аэрофлот', 301: 'АФК Система', 282: 'АЛРОСА', 227: 'Нефть Brent',
    388: 'Нефть Brent (мини)', 385: 'Медь', 330: 'CNY/RUB', 333: 'CNY/RUB (вечн)',
    235: 'EUR/USD', 239: 'EUR/RUB', 332: 'EUR/RUB (вечн)', 293: 'Полюс Золото',
    195: 'Золото', 349: 'Золото (руб)', 243: 'Газпром', 382: 'Газпром (вечн)',
    328: 'Газпром нефть', 198: 'РусГидро', 362: 'IMOEX (вечн)',
    305: 'Интер РАО', 200: 'Лукойл', 203: 'МТС', 273: 'МосБиржа',
    274: 'Магнит', 292: 'ММК', 326: 'Мечел', 201: 'MOEX', 280: 'MOEX (мини)',
    296: 'Газ', 389: 'Газ (микро)', 395: 'Газ TTF', 318: 'Нефть и газ',
    302: 'Норникель', 204: 'НОВАТЭК', 281: 'НЛМК', 208: 'Паладий',
    323: 'ФосАгро', 311: 'ПИК', 209: 'Платина', 329: 'RGBI',
    210: 'RTS', 310: 'RTS (мини)', 213: 'Русал', 240: 'Роснефть',
    241: 'Ростелеком', 228: 'Северсталь', 219: 'Сургут', 218: 'Сургут (прив)',
    216: 'Сбербанк', 381: 'Сбер (вечн)', 215: 'Сбер (прив)', 217: 'Серебро',
    351: 'Сегежа', 221: 'USD/RUB', 331: 'USD/RUB (вечн)', 222: 'Татнефть',
    225: 'ВТБ', 275: 'RVI', 334: 'Пшеница', 393: 'X5', 378: 'Яндекс'
}

# Авторизация
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})
resp = session.post(f"{BASE_URL}/auth/login",
                    json={'login': MSC_EMAIL, 'password': MSC_PASSWORD}, timeout=30)

if not resp.json().get('success'):
    print("❌ Ошибка авторизации!")
    exit()

print("✅ Авторизация OK")

# Периоды по годам
YEARS = [
    ('20200101', '20201231'),
    ('20210101', '20211231'),
    ('20220101', '20221231'),
    ('20230101', '20231231'),
    ('20240101', '20241231'),
    ('20250101', '20251215'),
]

def load_asset(asset_id, sectype):
    """Загрузка одного актива"""
    total = 0

    for date_from, date_to in YEARS:
        for iz_fiz, clgroup in [(True, 'FIZ'), (False, 'YUR')]:
            params = {
                'from': date_from,
                'to': date_to,
                'interval': 5,
                'type': 'minute',
                'iz_fiz': 'true' if iz_fiz else 'false',
                'timestamp': int(time.time() * 1000),
            }

            try:
                resp = session.get(f"{BASE_URL}/assets/{asset_id}/legal",
                                   params=params, timeout=180)

                if not resp.ok:
                    continue

                data = resp.json().get('data', [])
                if not data:
                    continue

                records = []
                for item in data:
                    try:
                        moment = datetime.strptime(item['moment'], '%Y-%m-%d %H:%M:%S')
                        pos_long = int(item['value']['long'])
                        pos_short = -abs(int(item['value']['short']))

                        records.append({
                            'tradedate': moment.date(),
                            'tradetime': moment.time(),
                            'sectype': sectype,
                            'clgroup': clgroup,
                            'pos': pos_long + pos_short,
                            'pos_long': pos_long,
                            'pos_short': pos_short,
                            'pos_long_num': None,
                            'pos_short_num': None,
                            'systime': datetime.now(),
                            'intv': 5,
                        })
                    except:
                        pass

                if records:
                    with engine.connect() as conn:
                        for rec in records:
                            conn.execute(text("""
                                INSERT INTO open_interest 
                                (tradedate, tradetime, sectype, clgroup, pos, pos_long, pos_short,
                                 pos_long_num, pos_short_num, systime, interval)
                                VALUES (:tradedate, :tradetime, :sectype, :clgroup, :pos, :pos_long, :pos_short,
                                        :pos_long_num, :pos_short_num, :systime, :intv)
                                ON CONFLICT (sectype, tradedate, tradetime, clgroup, interval) 
                                DO UPDATE SET pos = EXCLUDED.pos, pos_long = EXCLUDED.pos_long, 
                                              pos_short = EXCLUDED.pos_short, systime = EXCLUDED.systime
                            """), rec)
                        conn.commit()
                    total += len(records)

            except Exception as e:
                print(f"    ⚠️ {e}")

            time.sleep(0.2)

    return total


# Главный цикл
print(f"\n{'='*60}")
print(f"🚀 ИСПРАВЛЕНИЕ 5min ДЛЯ ВСЕХ 61 АКТИВОВ")
print(f"{'='*60}")

total_all = 0
count = len(MSC_TO_SECTYPE)

for idx, (asset_id, sectype) in enumerate(MSC_TO_SECTYPE.items(), 1):
    name = ASSET_NAMES.get(asset_id, str(asset_id))
    print(f"\n[{idx}/{count}] {name} ({sectype})...")

    inserted = load_asset(asset_id, sectype)
    total_all += inserted

    if inserted > 0:
        print(f"  ✅ {inserted:,} записей")
    else:
        print(f"  ⚠️ нет новых данных")

    time.sleep(0.5)

print(f"\n{'='*60}")
print(f"🎉 ИТОГО: {total_all:,} записей")
print(f"{'='*60}")