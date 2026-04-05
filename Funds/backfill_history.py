"""
Одноразовый скрипт: докачка исторических данных NAV из Cbonds API.

Для каждого фонда в таблице funds (с cbonds_share_id):
1. Получает дату запуска из Cbonds share_class_information
2. Сравнивает с первой датой в fund_data
3. Если есть пропуск — скачивает NAV за пропущенный период
4. Вставляет в fund_data (ON CONFLICT — не перезаписывает существующие)
"""

import os
import sys
import time
import requests
from datetime import datetime, date, timedelta
from collections import defaultdict

# --- Настройки ---
DB_URL = os.environ.get("DB_URL", "postgresql://postgres:1803@localhost:5432/moex_db")
CBONDS_LOGIN = os.environ.get("CBONDS_LOGIN", "ermolaeffvadick@yandex.ru")
CBONDS_PASSWORD = os.environ.get("CBONDS_PASSWORD", "Qwghty56")
CBONDS_UA = "Cbonds.K/3.0.8 (ru.cbonds.cbonds; build:636; Android 9) OkHttp/4.12.0"
CBONDS_BASE = "https://rest2.cbonds.info"

DRY_RUN = "--dry-run" in sys.argv  # если True — только показывает что будет скачано

# --- DB ---
import pg8000
from urllib.parse import urlparse

def get_connection():
    p = urlparse(DB_URL)
    return pg8000.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        database=p.path.lstrip("/")
    )

# --- Cbonds API ---
session = requests.Session()
session.headers["User-Agent"] = CBONDS_UA

def cbonds_auth():
    r = session.post(
        f"{CBONDS_BASE}/m/auth/tariffs/global/json/logout=1?lang=rus",
        json={"login": CBONDS_LOGIN, "password": CBONDS_PASSWORD}
    )
    if r.status_code != 200:
        print(f"❌ Auth failed: {r.status_code}")
        sys.exit(1)
    print("✅ Cbonds auth OK")

def get_launch_date(cbonds_share_id: int) -> date | None:
    """Дата запуска фонда из share_class_information"""
    r = session.post(
        f"{CBONDS_BASE}/m/exchange_traded_funds/share_class_information/global/json/{cbonds_share_id}/0/?lang=rus"
    )
    items = r.json().get("response", {}).get("items", [])
    if items and items[0].get("launched_date"):
        return datetime.fromtimestamp(items[0]["launched_date"]).date()
    return None

def fetch_nav_history(cbonds_share_id: int, date_from: date, date_to: date) -> list:
    """Скачивает NAV историю из Cbonds за период"""
    results = []
    # Cbonds может ограничивать период — качаем по годам
    current_from = date_from
    while current_from < date_to:
        current_to = min(current_from.replace(year=current_from.year + 1), date_to)
        url = f"{CBONDS_BASE}/m/exchange_traded_funds/nav/global/json/{cbonds_share_id}/{current_from.isoformat()}/{current_to.isoformat()}/?lang=rus"
        r = session.post(url)
        if r.status_code != 200:
            print(f"  ⚠️  HTTP {r.status_code} for {cbonds_share_id} {current_from}-{current_to}")
            current_from = current_to
            continue
        items = r.json().get("response", {}).get("items", [])
        for item in items:
            d = datetime.fromtimestamp(item["date"]).date()
            nav = item.get("nav")
            pay = item.get("nav_per_share")
            if nav and pay:
                results.append((d, float(nav), float(pay)))
        current_from = current_to
        time.sleep(0.3)  # Rate limiting
    return results


def main():
    if DRY_RUN:
        print("🔍 DRY RUN — ничего не будет записано в БД\n")

    cbonds_auth()
    conn = get_connection()
    cur = conn.cursor()

    # Получаем все фонды
    cur.execute("""
        SELECT f.fund_id, f.ticker, f.name, f.cbonds_share_id,
               MIN(fd.trade_date) as first_data
        FROM funds f
        LEFT JOIN fund_data fd ON f.fund_id = fd.fund_id
        WHERE f.cbonds_share_id IS NOT NULL
        GROUP BY f.fund_id, f.ticker, f.name, f.cbonds_share_id
        ORDER BY f.ticker
    """)
    funds = cur.fetchall()

    total_inserted = 0

    for fund_id, ticker, name, cbonds_share_id, first_data_in_db in funds:
        # Дата запуска из Cbonds
        launch = get_launch_date(cbonds_share_id)
        if not launch:
            print(f"⚠️  {ticker}: не удалось получить дату запуска")
            continue

        if first_data_in_db is None:
            gap_days = 9999
            fetch_to = date.today()
        else:
            gap_days = (first_data_in_db - launch).days
            fetch_to = first_data_in_db - timedelta(days=1)

        if gap_days <= 5:
            print(f"✅ {ticker:12} launched={launch}  first_in_db={first_data_in_db}  gap={gap_days}д — ОК")
            continue

        print(f"⚠️  {ticker:12} launched={launch}  first_in_db={first_data_in_db}  gap={gap_days}д — ДОКАЧИВАЕМ {launch} → {fetch_to}")

        if DRY_RUN:
            continue

        # Скачиваем историю
        history = fetch_nav_history(cbonds_share_id, launch, fetch_to)
        if not history:
            print(f"   Нет данных от Cbonds за этот период")
            continue

        # Вставляем в БД
        inserted = 0
        for trade_date, nav, pay in history:
            try:
                cur.execute("""
                    INSERT INTO fund_data (fund_id, trade_date, nav, pay)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (fund_id, trade_date) DO NOTHING
                """, (fund_id, trade_date, nav, pay))
                if cur.rowcount > 0:
                    inserted += 1
            except Exception as e:
                print(f"   ❌ Ошибка вставки {trade_date}: {e}")
                conn.rollback()
                continue

        conn.commit()
        total_inserted += inserted
        print(f"   ✅ Скачано {len(history)} записей, вставлено {inserted} новых")
        time.sleep(0.5)

    cur.close()
    conn.close()
    print(f"\n{'='*50}")
    print(f"ИТОГО: вставлено {total_inserted} новых записей")


if __name__ == "__main__":
    main()
