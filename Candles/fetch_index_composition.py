#!/usr/bin/env python3
"""
История баз расчёта индексов МосБиржи (index_composition).

Кто потребитель: «Сила рынка» (Candles/compute_breadth_history.py и
api/routers/breadth.py, вселенные imoex/imoex_usd). Без истории состава широта
считалась по сегодняшнему списку бумаг на всю глубину графика — классический
look-ahead: в 2015-м «в индексе» оказывались бумаги, попавшие туда в 2024-м, а
выбывшие не учитывались никогда.

Источник — ISS analytics:
  /statistics/engines/stock/markets/index/analytics/{IDX}.json?date=D
Один запрос = полный состав индекса на дату D (тикер + вес, %). Глубина по
IMOEX — с 2001-01-03.

Обход дат идёт по курсору ISS: в analytics.cursor приходит NEXT_DATE —
следующий день, на который у биржи есть срез. Поэтому по неторговым дням
запросов не делаем вовсе, а бэкфилл стоит ровно столько запросов, сколько
торговых дней в диапазоне (~4.9 тыс. с 2007 года, ~20 минут с паузой 0.15с).
Когда NEXT_DATE = null — дошли до последнего доступного среза.

Запуск:
  python Candles/fetch_index_composition.py --once            # догнать хвост
  python Candles/fetch_index_composition.py --full            # бэкфилл с 2007
  python Candles/fetch_index_composition.py --from 2015-01-01 --till 2015-12-31
  python Candles/fetch_index_composition.py --check           # покрытие по годам

--once идемпотентен: перезаписывает последнюю сохранённую дату (вдруг срез был
неполным) и добирает всё, что появилось после неё. Оркестратор гоняет его в
дневном цикле перед пересчётом широты.
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).parent.parent
load_dotenv(PROJECT_DIR / ".env")
DB_URL = os.getenv("DB_URL")

INDEX_ID = "IMOEX"

# Нижняя граница бэкфилла. Совпадает со стартом полного пересчёта широты
# (compute_breadth_history.py --full считает с 2007-01-01); глубже дневных
# свечей по бумагам всё равно нет.
BACKFILL_FROM = date(2007, 1, 1)

ISS_URL = ("https://iss.moex.com/iss/statistics/engines/stock/markets/index/"
           "analytics/{idx}.json?iss.meta=off&date={d}&limit=100&start={start}")
_UA = {"User-Agent": "Mozilla/5.0 (compatible; FrameBot/1.0)"}

ISS_PAUSE = 0.15      # сек между запросами — не душить ISS
ISS_RETRIES = 3       # ретраи на сетевых ошибках/5xx
MIN_ROWS = 8          # меньше бумаг в базе расчёта индекса не бывает — считаем срез битым

# Потолок глубины для --once на пустой таблице. Полный бэкфилл с 2007 — это
# ~4.9 тыс. запросов (~20 минут), он не должен запускаться сам из оркестратора
# (таймаут задачи 5 минут → убитый прогон и шум в ошибках). Первичное
# наполнение делается руками через --full.
ONCE_COLD_START_DAYS = 400

log = logging.getLogger("index_composition")


def setup_logging():
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def get_engine():
    if not DB_URL:
        raise ValueError("DB_URL не установлен в .env")
    return create_engine(DB_URL, connect_args={"ssl_context": False})


def ensure_table(engine):
    """Схема — как в db/migrations/052_index_composition.sql (идемпотентно)."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS index_composition (
                index_id   VARCHAR(16) NOT NULL,
                trade_date DATE        NOT NULL,
                ticker     VARCHAR(16) NOT NULL,
                weight     REAL,
                PRIMARY KEY (index_id, trade_date, ticker)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_index_composition_date
            ON index_composition(index_id, trade_date DESC)
        """))


def _iss_json(url: str) -> dict:
    last_err = None
    for attempt in range(ISS_RETRIES):
        try:
            req = urllib.request.Request(url, headers=_UA)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as e:  # сеть/5xx/таймаут — ISS периодически моргает
            last_err = e
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"ISS недоступен после {ISS_RETRIES} попыток: {last_err}")


def fetch_day(index_id: str, d: date) -> tuple[list[tuple[str, float | None]], date | None]:
    """
    Состав индекса на дату d.

    Возвращает (rows, next_date), где rows = [(ticker, weight), ...] (пусто, если
    день неторговый), а next_date — подсказка ISS о следующей дате со срезом
    (None = дальше данных нет).
    """
    rows: list[tuple[str, float | None]] = []
    next_date: date | None = None
    start = 0

    while True:
        data = _iss_json(ISS_URL.format(idx=index_id, d=d.isoformat(), start=start))
        block = data.get("analytics", {})
        cols = block.get("columns", [])
        data_rows = block.get("data", [])

        cursor = (data.get("analytics.cursor") or {}).get("data") or []
        if cursor:
            cur_cols = data["analytics.cursor"]["columns"]
            row = cursor[0]
            nd = row[cur_cols.index("NEXT_DATE")] if "NEXT_DATE" in cur_cols else None
            if nd:
                next_date = datetime.strptime(str(nd), "%Y-%m-%d").date()

        if not data_rows:
            break

        i_ticker = cols.index("ticker")
        i_weight = cols.index("weight") if "weight" in cols else None
        for r in data_rows:
            ticker = r[i_ticker]
            if not ticker:
                continue
            weight = None
            if i_weight is not None and r[i_weight] is not None:
                try:
                    weight = float(r[i_weight])
                except (TypeError, ValueError):
                    weight = None
            rows.append((str(ticker), weight))

        # Пагинация (IMOEX укладывается в одну страницу, но состав других индексов — нет)
        if cursor:
            cur_cols = data["analytics.cursor"]["columns"]
            idx_pos = cursor[0][cur_cols.index("INDEX")]
            total = cursor[0][cur_cols.index("TOTAL")]
            pagesize = cursor[0][cur_cols.index("PAGESIZE")]
            if idx_pos + pagesize >= total:
                break
            start = idx_pos + pagesize
            time.sleep(ISS_PAUSE)
        else:
            break

    return rows, next_date


def save_day(engine, index_id: str, d: date, rows: list[tuple[str, float | None]]) -> None:
    """Перезапись состава на дату целиком (DELETE+INSERT).

    Именно перезапись, а не upsert: при повторном прогоне дня бумага могла
    выбыть из базы расчёта, и upsert оставил бы её в составе навсегда.
    """
    with engine.begin() as conn:
        conn.execute(text(
            "DELETE FROM index_composition WHERE index_id = :i AND trade_date = :d"
        ), {"i": index_id, "d": d})
        conn.execute(text("""
            INSERT INTO index_composition (index_id, trade_date, ticker, weight)
            VALUES (:i, :d, :t, :w)
        """), [{"i": index_id, "d": d, "t": t, "w": w} for t, w in rows])


def get_last_date(engine, index_id: str) -> date | None:
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT MAX(trade_date) FROM index_composition WHERE index_id = :i"
        ), {"i": index_id}).fetchone()
    return row[0] if row and row[0] else None


def walk(engine, index_id: str, date_from: date, date_till: date, pause: float,
         max_seconds: float | None = None) -> int:
    """
    Идёт по торговым дням от date_from к date_till, сохраняя состав каждого дня.

    max_seconds ограничивает длительность прогона (для оркестратора, у задачи
    таймаут): дойдя до лимита, останавливаемся — хвост доберётся на следующем
    запуске, прогресс не теряется.

    Прыжки между датами делаются по NEXT_DATE из курсора ISS, поэтому пустых
    запросов по выходным и праздникам не бывает. Если ISS курсор не отдал
    (старые срезы иногда молчат) — падаем на шаг +1 день.
    """
    d = date_from
    saved_days = 0
    saved_rows = 0
    t0 = time.time()

    while d <= date_till:
        try:
            rows, next_date = fetch_day(index_id, d)
        except Exception as e:
            log.error(f"  {d}: {e} — прерываем прогон (хвост доберём на следующем запуске)")
            break

        if rows:
            if len(rows) < MIN_ROWS:
                log.warning(f"  {d}: всего {len(rows)} бумаг — срез выглядит битым, пропуск")
            else:
                save_day(engine, index_id, d, rows)
                saved_days += 1
                saved_rows += len(rows)
                if saved_days % 250 == 0:
                    log.info(f"  … {d}: сохранено дней {saved_days}, строк {saved_rows:,}")

        if max_seconds and time.time() - t0 > max_seconds:
            log.info(f"  {d}: лимит времени {max_seconds:.0f}с — хвост доберём следующим запуском")
            break

        if next_date and next_date > d:
            d = next_date
        else:
            if not rows and not next_date:
                # Ни данных, ни подсказки — дальше срезов нет.
                break
            d = d + timedelta(days=1)

        time.sleep(pause)

    log.info(f"{index_id}: сохранено дней {saved_days}, строк {saved_rows:,} "
             f"за {time.time() - t0:.1f}с")
    return saved_days


def check_coverage(engine, index_id: str) -> None:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT EXTRACT(YEAR FROM trade_date)::int AS y,
                   COUNT(DISTINCT trade_date) AS days,
                   COUNT(*) AS rows_cnt,
                   ROUND(AVG(cnt), 1) AS avg_tickers
            FROM (
                SELECT trade_date, COUNT(*) OVER (PARTITION BY trade_date) AS cnt
                FROM index_composition WHERE index_id = :i
            ) t
            GROUP BY 1 ORDER BY 1
        """), {"i": index_id}).fetchall()
    if not rows:
        log.info(f"{index_id}: таблица пуста")
        return
    log.info(f"Покрытие {index_id}:")
    for y, days, rows_cnt, avg_tickers in rows:
        log.info(f"  {y}: {days:>3} дней, {rows_cnt:>6,} строк, в среднем {avg_tickers} бумаг")


def main() -> None:
    parser = argparse.ArgumentParser(description="История баз расчёта индексов МосБиржи")
    parser.add_argument("--index", default=INDEX_ID, help="ID индекса (по умолчанию IMOEX)")
    parser.add_argument("--full", action="store_true",
                        help=f"Бэкфилл с {BACKFILL_FROM} (первый запуск)")
    parser.add_argument("--from", dest="date_from", default=None, help="Дата начала YYYY-MM-DD")
    parser.add_argument("--till", dest="date_till", default=None, help="Дата конца YYYY-MM-DD")
    parser.add_argument("--once", action="store_true", help="Однократный запуск (для оркестратора)")
    parser.add_argument("--force", action="store_true", help="Не проверять торговый день (совместимость)")
    parser.add_argument("--check", action="store_true", help="Показать покрытие и выйти")
    parser.add_argument("--sleep", type=float, default=ISS_PAUSE, help="Пауза между запросами ISS, сек")
    args = parser.parse_args()

    setup_logging()
    engine = get_engine()
    ensure_table(engine)

    if args.check:
        check_coverage(engine, args.index)
        engine.dispose()
        return

    today = date.today()
    date_till = date.fromisoformat(args.date_till) if args.date_till else today

    if args.date_from:
        date_from = date.fromisoformat(args.date_from)
        log.info(f"Режим: диапазон {date_from} … {date_till}")
    elif args.full:
        date_from = BACKFILL_FROM
        log.info(f"Режим: полный бэкфилл с {date_from}")
    else:
        last = get_last_date(engine, args.index)
        if last:
            # Перезапрашиваем последний сохранённый день: срез мог быть снят до
            # закрытия основной сессии.
            date_from = last
            log.info(f"Режим: инкрементальный, последняя дата={last}")
        else:
            date_from = today - timedelta(days=ONCE_COLD_START_DAYS)
            log.warning(f"Таблица пуста — берём только последние {ONCE_COLD_START_DAYS} дней "
                        f"(с {date_from}). Полную историю залить вручную: "
                        f"python Candles/fetch_index_composition.py --full")

    # Инкремент из оркестратора не должен упираться в таймаут задачи:
    # ограничиваем прогон, недобранное подхватится в следующий раз.
    max_seconds = 240.0 if (args.once and not args.full and not args.date_from) else None
    walk(engine, args.index, date_from, date_till, args.sleep, max_seconds)
    engine.dispose()


if __name__ == "__main__":
    main()
