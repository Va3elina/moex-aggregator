#!/usr/bin/env python3
"""
Реалтайм обновление данных фондов с ISS MOEX.

Источник: ISS MOEX API (https://iss.moex.com)
Данные:
- ПАЙ (CLOSE) берётся с торгов (market=shares, board=TQTF)
- СЧА (CAPITALIZATION) берётся с iNAV (market=index)

Расписание (МСК):
- Раз в день: 09:00 (данные за вчера появляются к утру)
- Выходные пропускаются

Использование:
  python fetch_funds_realtime.py --once --force    # Разово
  python fetch_funds_realtime.py                   # Демон
"""

import asyncio
import aiohttp
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List
import logging
import sys
from pathlib import Path
import argparse

# === Импорт из корневой папки ===
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

DB_URL = os.getenv("DB_URL")

try:
    from moex_calendar import get_moscow_time, is_trading_day
except ImportError:
    def get_moscow_time():
        return datetime.utcnow() + timedelta(hours=3)
    def is_trading_day(check_date=None):
        check = check_date or get_moscow_time().date()
        if check.weekday() in [5, 6]:
            return False, "Выходной"
        return True, "Торговый день"


# ═══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

# Время обновления (МСК)
UPDATE_HOUR = 9
UPDATE_MINUTE = 0

# Директория логов
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

CHUNK_SIZE_DAYS = 100

# URL шаблоны
URL_SHARES = (
    "https://iss.moex.com/iss/history/engines/stock/markets/shares"
    "/boards/TQTF/securities/{secid}.json"
)
URL_INDEX = (
    "https://iss.moex.com/iss/history/engines/stock/markets/index"
    "/securities/{secid}.json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# Конфигурация фондов (ISS MOEX — БПИФы)
# ID соответствует `fund_id` в базе данных (таблица funds)
FUNDS = {
    # Денежный рынок
    8181:  {"shares": "AKMM", "inav": "AKMMA", "name": "Альфа-Капитал Денежный рынок"},
    8628:  {"shares": "TMON", "inav": "TMONA", "name": "Т-Капитал Денежный Рынок"},
    7373:  {"shares": "SBMM", "inav": "SBMMA", "name": "Первая Сберегательный"},
    5973:  {"shares": "LQDT", "inav": "LQDTM", "name": "ВИМ Ликвидность"},
    10053: {"shares": "AMNR", "inav": "AMNRA", "name": "АТОН Накопительный в рублях"},

    # Акции
    6333:  {"shares": "TMOS", "inav": "TMOSA", "name": "Т-Капитал Индекс МосБиржи"},
    5247:  {"shares": "SBMX", "inav": "SBMXA", "name": "Первая Топ Российских акций"},
    6073:  {"shares": "EQMX", "inav": "EQMXE", "name": "Индекс МосБиржи ВИМ"},
    6575:  {"shares": "AKME", "inav": "AKMEI", "name": "Альфа-Капитал Управляемые акции"},

    # Облигации
    6225:  {"shares": "AKMB", "inav": "AKMBA", "name": "Альфа Управляемые облигации"},
    10331: {"shares": "SBLB", "inav": "SBLBA", "name": "Первая Долгосрочные гособлигации"},
    11445: {"shares": "TOFZ", "inav": "TOFZA", "name": "Т-Капитал ОФЗ"},
    11705: {"shares": "AMGB", "inav": "AMGBA", "name": "АТОН Длинные ОФЗ"},
    10113: {"shares": "SBFR", "inav": "SBFRA", "name": "Первая Облигации флоатеры"},
    7067:  {"shares": "TBRU", "inav": "TBRUA", "name": "Т-Капитал Облигации"},
    7007:  {"shares": "SAFE", "inav": "SAFEA", "name": "Первая Консерватив"},
    5713:  {"shares": "SBRB", "inav": "SBRBA", "name": "Первая Корпоративные облигации"},

    # Золото
    4713:  {"shares": "AKGD", "inav": "AKGDA", "name": "Альфа-Капитал Золото"},
    4038:  {"shares": "GOLD", "inav": "GOLDO", "name": "Золото Биржевой"},
    5061:  {"shares": "SBGD", "inav": "SBGDA", "name": "Первая Доступное золото"},
    4098:  {"shares": "TGLD", "inav": "TGLDB", "name": "Т-Капитал Золото"},
}

# ═══════════════════════════════════════════════════════════════════════════════
# Cbonds API — ПИФы (не торгуются на MOEX, данные через Cbonds REST API)
# fund_id: тот же что в таблице funds, cbonds_id: ID в системе Cbonds
# ═══════════════════════════════════════════════════════════════════════════════

CBONDS_URL = "https://rest2.cbonds.info"
CBONDS_UA = "Cbonds.K/3.0.8 (ru.cbonds.cbonds; build:636; Android 9) OkHttp/4.12.0"
CBONDS_LOGIN = os.getenv("CBONDS_LOGIN", "ermolaeffvadick@yandex.ru")
CBONDS_PASSWORD = os.getenv("CBONDS_PASSWORD", "Qwghty56")

CBONDS_FUNDS = {
    # Акции (управляемые)
    8123:  {"cbonds_id": 209397, "name": "Первая - Фонд акций с выплатой дохода"},
    432:   {"cbonds_id": 206895, "name": "Альфа-Капитал Ликвидные акции"},
    43:    {"cbonds_id": 206601, "name": "Первая - Фонд российских акций"},
    281:   {"cbonds_id": 206781, "name": "Райффайзен - Акции"},
    1003:  {"cbonds_id": 207285, "name": "ВИМ - Акции"},
    282:   {"cbonds_id": 206783, "name": "Райффайзен - Компании роста"},
    63:    {"cbonds_id": 206625, "name": "Атон - Петр Столыпин"},

    # Облигации (смешанные)
    8119:  {"cbonds_id": 209395, "name": "Первая - Фонд облигаций с выплатой дохода"},
    9113:  {"cbonds_id": 209453, "name": "Альфа-Капитал Облигации с выплатой дохода"},
    9165:  {"cbonds_id": 219221, "name": "ВИМ - Облигации. Рантье"},
    47:    {"cbonds_id": 206607, "name": "Первая - Фонд Рублевые сбережения"},
    33:    {"cbonds_id": 206593, "name": "Альфа-Капитал Облигации Плюс"},
    11259: {"cbonds_id": 231147, "name": "Альфа-Капитал Облигации с переменным купоном"},
    54:    {"cbonds_id": 206617, "name": "ВИМ - Казначейский"},
    4995:  {"cbonds_id": 208851, "name": "Первая - Накопительный"},
}


# ═══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
    detailed_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(console_fmt)
    root.addHandler(ch)

    fh = logging.FileHandler(
        LOG_DIR / f"funds_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(detailed_fmt)
    root.addHandler(fh)

    fh_err = logging.FileHandler(
        LOG_DIR / f"funds_errors_{datetime.now():%Y%m%d}.log",
        encoding='utf-8'
    )
    fh_err.setLevel(logging.WARNING)
    fh_err.setFormatter(detailed_fmt)
    root.addHandler(fh_err)

    return logging.getLogger(__name__)


log = setup_logging()


# ═══════════════════════════════════════════════════════════════════════════════
# ISS MOEX API
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_chunk(session, url_template, secid, date_from, date_to):
    """Получить данные за один чанк (до ~100 записей)."""
    params = {
        "from": date_from.isoformat(), "till": date_to.isoformat(),
        "limit": 100, "iss.meta": "off", "iss.json": "extended", "lang": "ru",
    }
    url = url_template.format(secid=secid)
    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=30) as resp:
            if resp.status != 200:
                log.warning(f"  [{secid}] HTTP {resp.status}")
                return []
            raw = await resp.json()
            return raw[1].get("history", []) if len(raw) >= 2 else []
    except Exception as e:
        log.error(f"  [{secid}] Ошибка: {e}")
        return []


async def fetch_full(session, url_template, secid, date_from, date_to):
    """Получить все данные с пагинацией по чанкам."""
    all_records = []
    current = date_from
    while current <= date_to:
        chunk_end = min(current + timedelta(days=CHUNK_SIZE_DAYS), date_to)
        records = await fetch_chunk(session, url_template, secid, current, chunk_end)
        all_records.extend(records)
        log.debug(f"  [{secid}] {current}—{chunk_end}: {len(records)}")
        current = chunk_end + timedelta(days=1)
        await asyncio.sleep(0.2)
    return all_records


# ═══════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def get_last_date(engine, fund_id: int) -> Optional[date]:
    """Получить последнюю дату для фонда в БД"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(trade_date) FROM fund_data WHERE fund_id = :fund_id
        """), {"fund_id": fund_id}).fetchone()
        return result[0] if result and result[0] else None


def merge_shares_inav(shares_rows, inav_rows):
    """Объединить данные shares и iNAV по дате → {date: {pay, nav}}"""
    merged = {}
    for r in shares_rows:
        td = r.get("TRADEDATE")
        if td:
            merged.setdefault(td, {})["pay"] = r.get("CLOSE")
    for r in inav_rows:
        td = r.get("TRADEDATE")
        if td:
            merged.setdefault(td, {})["nav"] = r.get("CAPITALIZATION")
    return merged


def save_fund_data(engine, fund_id: int, merged: Dict[str, dict]) -> int:
    """Сохранить данные в fund_data."""
    if not merged:
        return 0
    with engine.connect() as conn:
        n = 0
        for td_str, vals in merged.items():
            pay = vals.get("pay")
            nav = vals.get("nav")
            if pay is None and nav is None:
                continue
            conn.execute(text("""
                INSERT INTO fund_data (fund_id, trade_date, pay, nav)
                VALUES (:fid, :td, :pay, :nav)
                ON CONFLICT (fund_id, trade_date) DO UPDATE SET
                    pay = COALESCE(EXCLUDED.pay, fund_data.pay),
                    nav = COALESCE(EXCLUDED.nav, fund_data.nav),
                    created_at = CURRENT_TIMESTAMP
            """), {"fid": fund_id, "td": td_str, "pay": pay, "nav": nav})
            n += 1
        conn.commit()
        return n


# ═══════════════════════════════════════════════════════════════════════════════
# CBONDS API
# ═══════════════════════════════════════════════════════════════════════════════

async def cbonds_auth(session) -> bool:
    """Авторизация в Cbonds API. Устанавливает PHPSESSID в сессии."""
    url = f"{CBONDS_URL}/m/auth/tariffs/global/json/logout=1?lang=rus"
    headers = {"Content-Type": "application/json; charset=UTF-8", "User-Agent": CBONDS_UA}
    body = {"login": CBONDS_LOGIN, "password": CBONDS_PASSWORD}
    try:
        async with session.post(url, json=body, headers=headers, timeout=15) as resp:
            data = await resp.json()
            err = data.get("error", {}).get("err_no", -1)
            if err == 0:
                user = data.get("auth", {}).get("title", "?")
                log.info(f"  Cbonds авторизация: ✅ {user}")
                return True
            else:
                log.error(f"  Cbonds авторизация: ❌ {data.get('error', {}).get('err_str', '?')}")
                return False
    except Exception as e:
        log.error(f"  Cbonds авторизация: ❌ {e}")
        return False


async def fetch_cbonds_nav(session, cbonds_id: int, date_from: date, date_to: date) -> Dict[str, dict]:
    """Получить историю NAV фонда из Cbonds API → {date_str: {pay, nav}}"""
    url = (f"{CBONDS_URL}/m/exchange_traded_funds/nav/global/json/"
           f"{cbonds_id}/{date_from.isoformat()}/{date_to.isoformat()}/?lang=rus")
    headers = {"Content-Type": "application/json; charset=UTF-8", "User-Agent": CBONDS_UA}
    try:
        async with session.post(url, headers=headers, timeout=30) as resp:
            data = await resp.json()
            err = data.get("error", {}).get("err_no", -1)
            if err != 0:
                log.warning(f"  Cbonds NAV {cbonds_id}: err={err}")
                return {}
            items = data.get("response", {}).get("items", [])
            merged = {}
            for item in items:
                ts = item.get("date")
                if not ts:
                    continue
                # date — unix timestamp, конвертируем
                dt = datetime.utcfromtimestamp(ts).date()
                nav_per_share = item.get("nav_per_share")  # СЧА на пай
                nav_total = item.get("nav")  # Общая СЧА фонда
                if nav_per_share is not None:
                    merged[dt.isoformat()] = {
                        "pay": nav_per_share,  # пай = nav_per_share
                        "nav": nav_total,       # общая СЧА
                    }
            return merged
    except Exception as e:
        log.error(f"  Cbonds NAV {cbonds_id}: ❌ {e}")
        return {}


async def update_cbonds_funds(engine, force: bool = False) -> Dict[int, int]:
    """Обновить ПИФы через Cbonds API"""
    if not CBONDS_FUNDS:
        return {}

    results = {}
    today = date.today()

    log.info("")
    log.info("=" * 60)
    log.info("📊 ОБНОВЛЕНИЕ ПИФов (Cbonds API)")
    log.info("=" * 60)

    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
        if not await cbonds_auth(session):
            log.error("  Пропуск Cbonds: не удалось авторизоваться")
            return results

        for fund_id, info in CBONDS_FUNDS.items():
            cbonds_id = info["cbonds_id"]
            name = info["name"]

            log.info(f"\n── cbonds:{cbonds_id} │ fund_id={fund_id} │ {name}")

            last_date = get_last_date(engine, fund_id)
            if last_date and not force:
                start = last_date - timedelta(days=3)
                log.info(f"   Докачка с {start} (последняя: {last_date})")
            else:
                start = today - timedelta(days=365 * 3)  # ПИФы — берём 3 года
                log.info(f"   Полная загрузка с {start}")

            merged = await fetch_cbonds_nav(session, cbonds_id, start, today)
            log.info(f"   Cbonds: {len(merged)} дат")

            saved = save_fund_data(engine, fund_id, merged)
            results[fund_id] = saved

            if saved > 0:
                log.info(f"   ✅ Сохранено: {saved}")
            else:
                log.debug(f"   — нет новых данных")

            await asyncio.sleep(0.5)  # Не спамим Cbonds

    total = sum(results.values())
    log.info(f"\n✅ Cbonds ГОТОВО: {total} записей из {len(CBONDS_FUNDS)} фондов")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# СОСТАВ ФОНДОВ (Cbonds API)
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_cbonds_holdings(session, parent_fund_id: int) -> List[dict]:
    """Получить состав фонда из Cbonds API."""
    url = f"{CBONDS_URL}/m/exchange_traded_funds/structure/global/json/{parent_fund_id}/?lang=rus"
    headers = {"Content-Type": "application/json; charset=UTF-8", "User-Agent": CBONDS_UA}
    body = {"quantity": {"limit": 50, "offset": 0}}
    try:
        async with session.post(url, json=body, headers=headers, timeout=30) as resp:
            data = await resp.json()
            items = data.get("response", {}).get("items", [])
            # Дедуп по имени (Cbonds может вернуть дубли с пустым name)
            seen = set()
            result = []
            for i in items:
                name = str(i.get("asset_name") or i.get("name") or "Прочее").strip()
                weight = float(i.get("weight", 0))
                if weight > 0 and name not in seen:
                    seen.add(name)
                    result.append({"name": name, "weight": weight})
            return result
    except Exception as e:
        log.warning(f"  Cbonds holdings {parent_fund_id}: {e}")
        return []


def save_fund_holdings(engine, fund_id: int, holdings: List[dict]) -> int:
    """Сохранить состав фонда в fund_holdings."""
    if not holdings:
        return 0
    with engine.connect() as conn:
        # Удаляем старый состав
        conn.execute(text("DELETE FROM fund_holdings WHERE fund_id = :fid"), {"fid": fund_id})
        n = 0
        for h in holdings:
            conn.execute(text("""
                INSERT INTO fund_holdings (fund_id, asset_name, weight, updated_at)
                VALUES (:fid, :name, :weight, NOW())
            """), {"fid": fund_id, "name": h["name"], "weight": h["weight"]})
            n += 1
        conn.commit()
        return n


async def update_all_holdings(engine) -> int:
    """Обновить состав всех фондов через Cbonds API."""
    log.info("")
    log.info("=" * 60)
    log.info("📊 ОБНОВЛЕНИЕ СОСТАВА ФОНДОВ (Cbonds API)")
    log.info("=" * 60)

    # Получаем все фонды с cbonds_parent_id из БД
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT fund_id, ticker, cbonds_parent_id FROM funds WHERE cbonds_parent_id IS NOT NULL"
        )).fetchall()

    if not rows:
        log.info("  Нет фондов с cbonds_parent_id")
        return 0

    total_saved = 0
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
        if not await cbonds_auth(session):
            log.error("  Пропуск: не удалось авторизоваться")
            return 0

        for fund_id, ticker, parent_id in rows:
            holdings = await fetch_cbonds_holdings(session, parent_id)
            if holdings:
                saved = save_fund_holdings(engine, fund_id, holdings)
                log.info(f"  {ticker:12s} fund_id={fund_id:>6d}: {saved} позиций")
                total_saved += saved
            else:
                log.debug(f"  {ticker:12s} fund_id={fund_id:>6d}: нет данных")
            await asyncio.sleep(0.3)

    log.info(f"\n✅ Состав ГОТОВО: {total_saved} позиций для {len(rows)} фондов")
    return total_saved


# ═══════════════════════════════════════════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА
# ═══════════════════════════════════════════════════════════════════════════════

async def update_all_funds(force: bool = False) -> Dict[int, int]:
    """Обновить все фонды"""
    engine = get_engine()
    results = {}
    today = date.today()

    log.info("=" * 60)
    log.info("📊 ОБНОВЛЕНИЕ ФОНДОВ (ISS MOEX)")
    log.info("=" * 60)

    async with aiohttp.ClientSession() as session:
        for fund_id, info in FUNDS.items():
            name = info["name"]
            ticker = info["shares"]
            secid_inav = info["inav"]

            log.info(f"\n── {ticker:6s} │ iNAV={secid_inav} │ fund_id={fund_id}")

            # Определяем дату начала
            last_date = get_last_date(engine, fund_id)
            if last_date and not force:
                start = last_date - timedelta(days=3)
                log.info(f"   Докачка с {start} (последняя: {last_date})")
            else:
                start = today - timedelta(days=365)
                log.info(f"   Полная загрузка с {start}")

            # Параллельно грузим shares и iNAV (с пагинацией)
            shares_task = fetch_full(session, URL_SHARES, ticker, start, today)
            inav_task = fetch_full(session, URL_INDEX, secid_inav, start, today)
            shares_rows, inav_rows = await asyncio.gather(shares_task, inav_task)

            merged = merge_shares_inav(shares_rows, inav_rows)
            log.info(f"   shares: {len(shares_rows)}, iNAV: {len(inav_rows)}, дат: {len(merged)}")

            saved = save_fund_data(engine, fund_id, merged)
            results[fund_id] = saved

            if saved > 0:
                log.info(f"   ✅ Сохранено: {saved}")
            else:
                log.debug(f"   — нет новых данных")

            await asyncio.sleep(0.3)

    # Cbonds API — ПИФы
    cbonds_results = await update_cbonds_funds(engine, force=force)
    results.update(cbonds_results)

    # Состав фондов (раз в день)
    await update_all_holdings(engine)

    engine.dispose()

    total = sum(results.values())
    log.info("=" * 60)
    log.info(f"✅ ГОТОВО: {total} записей (ISS: {len(FUNDS)}, Cbonds: {len(CBONDS_FUNDS)} фондов)")
    log.info("=" * 60)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# ДЕМОН
# ═══════════════════════════════════════════════════════════════════════════════

async def run_daemon():
    """Запуск в режиме демона"""
    log.info("🚀 Запуск демона обновления фондов")
    log.info(f"  Расписание: {UPDATE_HOUR:02d}:{UPDATE_MINUTE:02d} МСК")

    last_update_date = None

    while True:
        now = get_moscow_time()
        today = now.date()

        # Проверяем расписание
        is_trade_day, reason = is_trading_day(today)

        if is_trade_day:
            if now.hour >= UPDATE_HOUR and now.minute >= UPDATE_MINUTE:
                if last_update_date != today:
                    log.info(f"⏰ [{now:%H:%M:%S} МСК] Запуск обновления...")
                    try:
                        await update_all_funds(force=False)
                        last_update_date = today
                    except Exception as e:
                        log.error(f"Ошибка обновления: {e}")

        await asyncio.sleep(60)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="Обновление данных фондов (ISS MOEX)")
    parser.add_argument("--once", action="store_true", help="Однократный запуск")
    parser.add_argument("--force", action="store_true", help="Принудительное обновление")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("ЗАПУСК СКРИПТА ФОНДОВ (БПИФ) - ISS MOEX")
    log.info(f"Время: {datetime.now()}")
    log.info(f"МСК: {get_moscow_time()}")
    log.info(f"Режим: {'однократный' if args.once else 'daemon'}")
    log.info("=" * 60)

    # Проверка торгового дня в режиме --once
    if args.once and not args.force:
        is_trading, reason = is_trading_day()
        if not is_trading:
            log.info(f"⏭️ Пропуск: {reason}")
            log.info("  (используйте --force для принудительного запуска)")
            return

    if args.once:
        await update_all_funds(force=args.force)
    else:
        await run_daemon()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено")
    except Exception as e:
        log.critical(f"Ошибка: {e}")
        sys.exit(1)
