"""
Fund Trades — отслеживание покупок/продаж в БПИФах через snapshot diff.

Endpoints (все требуют Pro):
  GET /api/fund-trades/funds              — список фондов с holdings + last_snapshot
  GET /api/fund-trades/fund/{ticker}      — изменения по конкретному фонду
  GET /api/fund-trades/movers             — топ-аккумуляция/распродажа across всех фондов
  GET /api/fund-trades/asset/{asset_name} — кто покупает/продаёт конкретный актив

Логика дельт: сравниваем latest snapshot с предыдущим (or N дней назад).
Δ weight = current.weight - previous.weight
  > 0  → накопление (актив "купили")
  < 0  → распродажа (актив "продали")
  new  → впервые появился в фонде
  gone → полностью продан

Источники данных в `fund_holdings_history`:
  - 'cbonds' — месячные snapshot'ы из API. Уже работает для ~16 фондов.
  - 'vim' — daily HTML-парсинг сайта ВИМ для LQDT/EQMX/GOLD. WIP.
  - 'nrd_scha' — НРД ежемесячные SCHA PDF (по форме ЦБ № 0420502).

WHITELIST (beta): сейчас показываем только 6 ВИМ-фондов для тестирования
методологии. После того как backfill+intraday отлажены — расширим на
остальные УК (Первая, Альфа, Т-Капитал) через тот же НРД-парсер.
"""
from datetime import date, timedelta

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_pro

router = APIRouter(prefix="/api/fund-trades", tags=["fund-trades"])


# Beta whitelist: 6 ВИМ-фондов для тестирования. Остальные не показываем
# в UI пока не отлажены парсеры. БД может содержать snapshot'ы других
# фондов — фильтр работает на API-уровне, данные сохраняем.
WHITELIST_TICKERS = (
    # ВИМ Инвестиции — есть SCHA-история через wealthim.ru
    "EQMX",       # БПИФ Индекс МосБиржи — акции (3.75 года SCHA)
    "OBLG",       # БПИФ Российские облигации — bonds (3.75 года SCHA)
    "OPIF-1003",  # ВИМ - Акции
    "OPIF-54",    # ВИМ - Казначейский
    "OPIF-9165",  # ВИМ - Облигации Рантье
    # Альфа-Капитал — Cbonds + reconstruct
    "AKMB",       # Управляемые облигации
    "AKME",       # Управляемые акции
    "OPIF-11259", # Облигации с переменным купоном
    "OPIF-9113",  # Облигации с выплатой дохода
    "OPIF-33",    # Облигации Плюс
    "OPIF-432",   # Ликвидные акции
    # Т-Капитал — Cbonds + reconstruct
    "TBRU",       # Облигации
    "TMOS",       # Индекс МосБиржи
    # Сбер/Первая — Cbonds + reconstruct
    "SBMX",       # Фонд Топ Российских акций
    "SBRB",       # Корпоративные облигации
    "SBFR",       # Облигации флоатеры
    "SAFE",       # Консерватив
    "OPIF-47",    # Фонд Рублёвые сбережения
    "OPIF-4995",  # Накопительный
    "OPIF-43",    # Фонд российских акций
    "OPIF-8119",  # Фонд облигаций с выплатой дохода
    "OPIF-8123",  # Фонд акций с выплатой дохода
    # Атон, Райффайзен — Cbonds + reconstruct
    "OPIF-63",    # Атон - Петр Столыпин
    "OPIF-281",   # Райффайзен - Акции
    "OPIF-282",   # Райффайзен - Компании роста
    # LQDT/GOLD/TGLD/AKGD/SBGD — cash/REPO/ОМС-фонды без classical holdings,
    # скрыты из UI. Intraday-данные ВИМ-БПИФ собираются отдельно.
)


# Источники данных для /fund-trades.
#
# MONTHLY_SOURCES — для diff'ов между снапшотами фонда.
#   vim_sdr  — SCHA-PDF с wealthim.ru (точные данные для 2 ВИМ-БПИФ)
#   cbonds   — Cbonds Mobile API с restored positions (NAV × weight / price).
#              Точность 0.5-2% по сравнению с SCHA (см. cbonds_reconstruct.py).
#              Покрывает ~24 фонда от 7 УК — Альфа, Т-Капитал, Сбер/Первая,
#              Атон, Райффайзен, ВИМ.
# vim intraday (raw имена с госномерами) → не подходит для месячных diff'ов.
MONTHLY_SOURCES = ("vim_sdr", "cbonds", "cbonds_baseline")


# Периоды для diff-расчёта (label → days назад).
PERIOD_DAYS = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
}


def _parse_period(period: str) -> int:
    if period not in PERIOD_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"period must be one of {list(PERIOD_DAYS)}, got '{period}'",
        )
    return PERIOD_DAYS[period]


@router.get("/funds")
def list_funds_with_history(
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Список фондов из WHITELIST с history-метаданными.

    Beta-фильтр: возвращаем только 6 ВИМ-фондов (см. WHITELIST_TICKERS).
    Даже если в БД есть snapshot'ы других фондов — UI их не покажет.
    EXISTS на history сейчас опущен чтобы whitelist-фонды без snapshot'ов
    тоже отображались (с count=0) — UI разрулит "пока данных нет".
    """
    rows = db.execute(text("""
        SELECT
            f.fund_id,
            f.ticker,
            f.name,
            f.uk,
            f.category,
            f.subcategory,
            (SELECT MAX(snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)) AS last_snapshot_date,
            (SELECT COUNT(DISTINCT snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)) AS snapshot_count
        FROM funds f
        WHERE f.ticker = ANY(:tickers)
        ORDER BY f.uk NULLS LAST, f.category, f.ticker
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES)}).mappings().all()
    return {
        "funds": [
            {
                **dict(r),
                "last_snapshot_date": r["last_snapshot_date"].isoformat()
                if r["last_snapshot_date"] else None,
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.get("/fund/{ticker}")
def fund_trades_detail(
    ticker: str = Path(..., min_length=1, max_length=20),
    period: str = Query("3m", description="1m | 3m | 6m | 1y"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Детальная аналитика дельт по фонду:
      - current_holdings: latest snapshot (что в фонде сейчас)
      - previous_holdings: snapshot N дней назад
      - diff: список изменений с типом (accumulated/reduced/new/sold_out)

    Если истории нет (только один snapshot) — diff пустой, current'ы заполнены.
    """
    # Beta whitelist — другие фонды возвращают 404 (не светим что они в БД).
    if ticker.upper() not in {t.upper() for t in WHITELIST_TICKERS}:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not available in beta")

    days = _parse_period(period)

    # Найти fund_id по тикеру (case-insensitive).
    fund_row = db.execute(text("""
        SELECT fund_id, ticker, name, category, subcategory
        FROM funds WHERE UPPER(ticker) = UPPER(:t)
    """), {"t": ticker}).mappings().first()
    if not fund_row:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")

    fund_id = fund_row["fund_id"]

    # Latest snapshot date.
    latest_row = db.execute(text("""
        SELECT MAX(snapshot_date) AS d FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES)}).first()

    if not latest_row or not latest_row[0]:
        # Нет истории вообще
        return {
            "fund": dict(fund_row),
            "period": period,
            "current_snapshot_date": None,
            "previous_snapshot_date": None,
            "current_holdings": [],
            "diff": [],
            "summary": {"new": 0, "sold_out": 0, "accumulated": 0, "reduced": 0},
        }

    current_date = latest_row[0]
    threshold = current_date - timedelta(days=days)

    # Previous snapshot = latest snapshot перед threshold.
    # Если такого нет — берём самый старый snapshot.
    prev_row = db.execute(text("""
        SELECT snapshot_date FROM fund_holdings_history
        WHERE fund_id = :fid AND snapshot_date <= :threshold AND source = ANY(:sources)
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"fid": fund_id, "threshold": threshold, "sources": list(MONTHLY_SOURCES)}).first()

    previous_date = prev_row[0] if prev_row else None

    # Current holdings.
    current_rows = db.execute(text("""
        SELECT asset_name, weight, positions, amount_rub, isin
        FROM fund_holdings_history
        WHERE fund_id = :fid AND snapshot_date = :d AND source = ANY(:sources)
        ORDER BY weight DESC NULLS LAST
    """), {"fid": fund_id, "d": current_date, "sources": list(MONTHLY_SOURCES)}).mappings().all()

    current_holdings = [
        {
            "asset_name": r["asset_name"],
            "isin": r.get("isin"),
            "weight": float(r["weight"]) if r["weight"] is not None else None,
            "positions": int(r["positions"]) if r["positions"] is not None else None,
            "amount_rub": float(r["amount_rub"]) if r["amount_rub"] is not None else None,
        }
        for r in current_rows
    ]

    if not previous_date:
        # Нет предыдущего snapshot для сравнения.
        return {
            "fund": dict(fund_row),
            "period": period,
            "current_snapshot_date": current_date.isoformat(),
            "previous_snapshot_date": None,
            "current_holdings": current_holdings,
            "diff": [],
            "summary": {"new": 0, "sold_out": 0, "accumulated": 0, "reduced": 0},
        }

    # Diff через FULL OUTER JOIN между current и previous.
    # Каждая строка — один актив с его change_type и delta_weight.
    diff_rows = db.execute(text("""
        WITH curr AS (
            SELECT asset_name, weight, positions, amount_rub, isin
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :curr_d AND source = ANY(:sources)
        ),
        prev AS (
            SELECT asset_name, weight, positions, amount_rub, isin
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :prev_d AND source = ANY(:sources)
        )
        SELECT
            COALESCE(curr.asset_name, prev.asset_name) AS asset_name,
            curr.weight AS curr_weight,
            prev.weight AS prev_weight,
            curr.positions AS curr_positions,
            prev.positions AS prev_positions,
            curr.amount_rub AS curr_amount,
            prev.amount_rub AS prev_amount,
            CASE
                WHEN prev.asset_name IS NULL THEN 'new'
                WHEN curr.asset_name IS NULL THEN 'sold_out'
                WHEN curr.weight > prev.weight THEN 'accumulated'
                WHEN curr.weight < prev.weight THEN 'reduced'
                ELSE 'unchanged'
            END AS change_type,
            CASE
                WHEN prev.asset_name IS NULL THEN curr.weight
                WHEN curr.asset_name IS NULL THEN -prev.weight
                ELSE curr.weight - prev.weight
            END AS delta_weight
        FROM curr
        FULL OUTER JOIN prev USING (asset_name)
        WHERE COALESCE(curr.weight, 0) <> COALESCE(prev.weight, 0)
        ORDER BY ABS(
            CASE
                WHEN prev.asset_name IS NULL THEN curr.weight
                WHEN curr.asset_name IS NULL THEN -prev.weight
                ELSE curr.weight - prev.weight
            END
        ) DESC NULLS LAST
    """), {"fid": fund_id, "curr_d": current_date, "prev_d": previous_date, "sources": list(MONTHLY_SOURCES)}).mappings().all()

    diff = []
    summary = {"new": 0, "sold_out": 0, "accumulated": 0, "reduced": 0}
    for r in diff_rows:
        change = r["change_type"]
        if change in summary:
            summary[change] += 1
        diff.append({
            "asset_name": r["asset_name"],
            "change_type": change,
            "delta_weight": float(r["delta_weight"]) if r["delta_weight"] is not None else None,
            "current_weight": float(r["curr_weight"]) if r["curr_weight"] is not None else None,
            "previous_weight": float(r["prev_weight"]) if r["prev_weight"] is not None else None,
            "current_positions": int(r["curr_positions"]) if r["curr_positions"] else None,
            "previous_positions": int(r["prev_positions"]) if r["prev_positions"] else None,
        })

    return {
        "fund": dict(fund_row),
        "period": period,
        "current_snapshot_date": current_date.isoformat(),
        "previous_snapshot_date": previous_date.isoformat(),
        "current_holdings": current_holdings,
        "diff": diff,
        "summary": summary,
    }


@router.get("/movers")
def top_movers(
    period: str = Query("1m", description="1m | 3m | 6m | 1y"),
    category: str | None = Query(None, description="stocks | bonds | money_market | gold"),
    limit: int = Query(20, ge=1, le=100),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Топ-аккумуляция / распродажа за период — сводно по всем фондам.

    Возвращает rank-таблицу: какие активы суммарно купили больше всего
    (sum of positive deltas weighted by NAV), какие — продали.

    Use case: "За месяц SBER суммарно накоплен на +3.4 п.п. across 7 фондов
    (TMOS +1.2, SBMX +0.9, ...)".
    """
    days = _parse_period(period)
    if category and category not in ("stocks", "bonds", "money_market", "gold"):
        raise HTTPException(status_code=400, detail="category invalid")

    category_filter = "AND f.category = :cat" if category else ""
    # Beta: whitelist 6 ВИМ-фондов
    whitelist_filter = "AND f.ticker = ANY(:tickers)"
    params = {
        "days": days,
        "limit": limit,
        "tickers": list(WHITELIST_TICKERS),
        "sources": list(MONTHLY_SOURCES),
    }
    if category:
        params["cat"] = category

    # Для каждого фонда: latest snapshot + snapshot N дней назад.
    # Diff weight per (fund, asset) → агрегируем по asset.
    # Это complex CTE но даёт точную картину.
    rows = db.execute(text(f"""
        WITH fund_dates AS (
            -- Для каждого фонда: latest snapshot + previous (N дней назад).
            SELECT
                f.fund_id,
                f.ticker,
                f.name AS fund_name,
                MAX(h.snapshot_date) AS curr_date,
                (
                    SELECT MAX(h2.snapshot_date)
                    FROM fund_holdings_history h2
                    WHERE h2.fund_id = f.fund_id
                      AND h2.snapshot_date <= MAX(h.snapshot_date) - (:days || ' days')::interval
                      AND h2.source = ANY(:sources)
                ) AS prev_date
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            WHERE 1=1 {category_filter} {whitelist_filter}
            GROUP BY f.fund_id, f.ticker, f.name
            HAVING MAX(h.snapshot_date) IS NOT NULL
        ),
        per_fund_diff AS (
            -- Дельта weight per (fund, asset) между curr и prev snapshot.
            SELECT
                fd.fund_id,
                fd.ticker,
                fd.fund_name,
                COALESCE(curr.asset_name, prev.asset_name) AS asset_name,
                COALESCE(curr.weight, 0) - COALESCE(prev.weight, 0) AS delta_weight
            FROM fund_dates fd
            LEFT JOIN fund_holdings_history curr
                ON curr.fund_id = fd.fund_id AND curr.snapshot_date = fd.curr_date
                AND curr.source = ANY(:sources)
            FULL OUTER JOIN fund_holdings_history prev
                ON prev.fund_id = fd.fund_id AND prev.snapshot_date = fd.prev_date
                AND prev.source = ANY(:sources)
                AND (curr.asset_name = prev.asset_name OR curr.asset_name IS NULL)
            WHERE fd.prev_date IS NOT NULL
              AND COALESCE(curr.asset_name, prev.asset_name) IS NOT NULL
        ),
        aggregated AS (
            -- Суммарная дельта per asset (across всех фондов).
            SELECT
                asset_name,
                SUM(delta_weight) AS total_delta_weight,
                COUNT(DISTINCT fund_id) FILTER (WHERE delta_weight > 0) AS funds_buying,
                COUNT(DISTINCT fund_id) FILTER (WHERE delta_weight < 0) AS funds_selling
            FROM per_fund_diff
            WHERE delta_weight <> 0
            GROUP BY asset_name
        )
        (
            SELECT 'top_accumulated' AS bucket, asset_name, total_delta_weight,
                   funds_buying, funds_selling
            FROM aggregated
            WHERE total_delta_weight > 0
            ORDER BY total_delta_weight DESC
            LIMIT :limit
        )
        UNION ALL
        (
            SELECT 'top_reduced' AS bucket, asset_name, total_delta_weight,
                   funds_buying, funds_selling
            FROM aggregated
            WHERE total_delta_weight < 0
            ORDER BY total_delta_weight ASC
            LIMIT :limit
        )
    """), params).mappings().all()

    top_accumulated = []
    top_reduced = []
    for r in rows:
        item = {
            "asset_name": r["asset_name"],
            "total_delta_weight": float(r["total_delta_weight"]),
            "funds_buying": r["funds_buying"],
            "funds_selling": r["funds_selling"],
        }
        if r["bucket"] == "top_accumulated":
            top_accumulated.append(item)
        else:
            top_reduced.append(item)

    return {
        "period": period,
        "category": category,
        "top_accumulated": top_accumulated,
        "top_reduced": top_reduced,
    }


@router.get("/intraday/{ticker}")
def fund_intraday_events(
    ticker: str = Path(..., min_length=1, max_length=20),
    days: int = Query(7, ge=1, le=30, description="Глубина окна в днях"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Intraday-события по позициям ВИМ-фонда: «когда и что УК торговала».

    Логика:
      - Берём все snapshot_timestamp за последние N дней
      - Сравниваем смежные snapshot'ы → находим изменения positions
      - Возвращаем массив событий с типом (new/sold_out/accumulated/reduced)
        и точным timestamp

    Доступно только для ВИМ-БПИФ (intraday-парсер собирает данные
    каждые 30 мин в торговое время).
    """
    if ticker.upper() not in {t.upper() for t in WHITELIST_TICKERS}:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not in beta")

    fund_row = db.execute(text("""
        SELECT fund_id, ticker, name, category, subcategory
        FROM funds WHERE UPPER(ticker) = UPPER(:t)
    """), {"t": ticker}).mappings().first()
    if not fund_row:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")

    fund_id = fund_row["fund_id"]

    # Все snapshot_timestamps за окно
    snaps_rows = db.execute(text("""
        SELECT DISTINCT snapshot_timestamp
        FROM fund_holdings_intraday
        WHERE fund_id = :fid
          AND snapshot_timestamp >= NOW() - (:days || ' days')::interval
        ORDER BY snapshot_timestamp ASC
    """), {"fid": fund_id, "days": days}).fetchall()
    timestamps = [r[0] for r in snaps_rows]

    if len(timestamps) < 2:
        # Нечего сравнивать.
        return {
            "fund": dict(fund_row),
            "days": days,
            "snapshots_count": len(timestamps),
            "events": [],
            "latest_timestamp": timestamps[0].isoformat() if timestamps else None,
        }

    # Diff между соседними snapshot'ами: только positions changes.
    events_rows = db.execute(text("""
        WITH ordered_snaps AS (
            SELECT snapshot_timestamp,
                   LAG(snapshot_timestamp) OVER (ORDER BY snapshot_timestamp) AS prev_ts
            FROM (
                SELECT DISTINCT snapshot_timestamp FROM fund_holdings_intraday
                WHERE fund_id = :fid
                  AND snapshot_timestamp >= NOW() - (:days || ' days')::interval
            ) s
        ),
        diff AS (
            SELECT
                os.snapshot_timestamp,
                os.prev_ts,
                COALESCE(curr.asset_name, prev.asset_name) AS asset_name,
                curr.positions AS curr_pos,
                prev.positions AS prev_pos,
                COALESCE(curr.positions, 0) - COALESCE(prev.positions, 0) AS delta_pos,
                curr.weight AS curr_weight
            FROM ordered_snaps os
            LEFT JOIN fund_holdings_intraday curr
                ON curr.fund_id = :fid AND curr.snapshot_timestamp = os.snapshot_timestamp
            FULL OUTER JOIN fund_holdings_intraday prev
                ON prev.fund_id = :fid AND prev.snapshot_timestamp = os.prev_ts
                AND prev.asset_name = curr.asset_name
            WHERE os.prev_ts IS NOT NULL
        )
        SELECT * FROM diff
        WHERE delta_pos <> 0
           OR (prev_pos IS NULL AND curr_pos IS NOT NULL)
           OR (curr_pos IS NULL AND prev_pos IS NOT NULL)
        ORDER BY snapshot_timestamp DESC, ABS(delta_pos) DESC NULLS LAST
        LIMIT 500
    """), {"fid": fund_id, "days": days}).mappings().all()

    events = []
    for r in events_rows:
        delta = r["delta_pos"]
        if r["prev_pos"] is None and r["curr_pos"] is not None:
            change_type = "new"
        elif r["curr_pos"] is None and r["prev_pos"] is not None:
            change_type = "sold_out"
        elif delta > 0:
            change_type = "accumulated"
        else:
            change_type = "reduced"

        events.append({
            "timestamp": r["snapshot_timestamp"].isoformat(),
            "asset_name": r["asset_name"],
            "change_type": change_type,
            "delta_positions": int(delta) if delta else 0,
            "current_positions": int(r["curr_pos"]) if r["curr_pos"] else None,
            "previous_positions": int(r["prev_pos"]) if r["prev_pos"] else None,
            "current_weight": float(r["curr_weight"]) if r["curr_weight"] is not None else None,
        })

    return {
        "fund": dict(fund_row),
        "days": days,
        "snapshots_count": len(timestamps),
        "events": events,
        "latest_timestamp": timestamps[-1].isoformat(),
        "earliest_timestamp": timestamps[0].isoformat(),
    }


@router.get("/asset/{asset_name}")
def asset_buyers(
    asset_name: str = Path(..., min_length=1, max_length=255),
    period: str = Query("3m", description="1m | 3m | 6m | 1y"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Reverse view: какие фонды покупают/продают конкретный актив за период.

    Use case: "Я держу Сбер — какие БПИФ его аккумулируют, какие распродают?"
    """
    days = _parse_period(period)

    rows = db.execute(text("""
        WITH fund_pairs AS (
            -- Для каждого фонда у которого есть актив:
            -- latest snapshot + ближайший до (latest - days).
            SELECT
                f.fund_id,
                f.ticker,
                f.name AS fund_name,
                f.category,
                MAX(h.snapshot_date) AS curr_date,
                (
                    SELECT MAX(h2.snapshot_date)
                    FROM fund_holdings_history h2
                    WHERE h2.fund_id = f.fund_id
                      AND h2.snapshot_date <= MAX(h.snapshot_date) - (:days || ' days')::interval
                      AND h2.source = ANY(:sources)
                ) AS prev_date
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            WHERE h.asset_name = :asset
              AND f.ticker = ANY(:tickers)
            GROUP BY f.fund_id, f.ticker, f.name, f.category
        )
        SELECT
            fp.ticker,
            fp.fund_name,
            fp.category,
            fp.curr_date,
            fp.prev_date,
            curr.weight AS curr_weight,
            prev.weight AS prev_weight,
            curr.positions AS curr_positions,
            prev.positions AS prev_positions,
            curr.amount_rub AS curr_amount,
            prev.amount_rub AS prev_amount,
            COALESCE(curr.weight, 0) - COALESCE(prev.weight, 0) AS delta_weight,
            COALESCE(curr.positions, 0) - COALESCE(prev.positions, 0) AS delta_positions
        FROM fund_pairs fp
        LEFT JOIN fund_holdings_history curr
            ON curr.fund_id = fp.fund_id
            AND curr.snapshot_date = fp.curr_date
            AND curr.asset_name = :asset
            AND curr.source = ANY(:sources)
        LEFT JOIN fund_holdings_history prev
            ON prev.fund_id = fp.fund_id
            AND prev.snapshot_date = fp.prev_date
            AND prev.asset_name = :asset
            AND prev.source = ANY(:sources)
        ORDER BY delta_weight DESC NULLS LAST
    """), {
        "asset": asset_name,
        "days": days,
        "tickers": list(WHITELIST_TICKERS),
        "sources": list(MONTHLY_SOURCES),
    }).mappings().all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Asset '{asset_name}' not found in any fund history",
        )

    return {
        "asset_name": asset_name,
        "period": period,
        "funds": [
            {
                "ticker": r["ticker"],
                "fund_name": r["fund_name"],
                "category": r["category"],
                "current_snapshot_date": r["curr_date"].isoformat() if r["curr_date"] else None,
                "previous_snapshot_date": r["prev_date"].isoformat() if r["prev_date"] else None,
                "current_weight": float(r["curr_weight"]) if r["curr_weight"] is not None else None,
                "previous_weight": float(r["prev_weight"]) if r["prev_weight"] is not None else None,
                "delta_weight": float(r["delta_weight"]) if r["delta_weight"] is not None else None,
            }
            for r in rows
        ],
    }


# ────────────────────────────────────────────────────────────────────
# Snapshot review endpoints — обзор каждого снапшота фонда
# ────────────────────────────────────────────────────────────────────


@router.get("/snapshots/{ticker}")
def list_snapshots(
    ticker: str,
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Список всех snapshot_date для одного фонда (по убыванию даты).
    Используется для навигации в UI: ◀ янв-26 ◀ фев-26 ▶.
    """
    if ticker.upper() not in {t.upper() for t in WHITELIST_TICKERS}:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not in beta")

    fund_row = db.execute(text("""
        SELECT fund_id, ticker, name, category
        FROM funds WHERE UPPER(ticker) = UPPER(:t)
    """), {"t": ticker}).first()
    if not fund_row:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")

    fund_id = fund_row[0]
    rows = db.execute(text("""
        SELECT snapshot_date, COUNT(*) AS asset_count
        FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES)}).mappings().all()

    return {
        "ticker": fund_row[1],
        "fund_name": fund_row[2],
        "snapshots": [
            {
                "snapshot_date": r["snapshot_date"].isoformat(),
                "asset_count": r["asset_count"],
            }
            for r in rows
        ],
    }


@router.get("/snapshot/{ticker}")
def snapshot_review(
    ticker: str,
    date: Optional[str] = Query(None, description="ISO date; default = latest"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Обзор одного снапшота фонда: diff к ПРЕДЫДУЩЕМУ снапшоту.

    Возвращает 4 группы изменений (MECE):
      - added: позиции с положительной дельтой positions
      - reduced: позиции с отрицательной дельтой positions
      - new: позиции которых не было в предыдущем снапшоте
      - sold_out: позиции которые исчезли (были в предыдущем, нет в текущем)

    Для каждой строки:
      - positions / amount_rub / weight (для текущего)
      - delta_positions = curr - prev
      - delta_amount_rub = delta_positions * current_price
        (price рассчитывается как amount_rub / positions из current snapshot)
    """
    if ticker.upper() not in {t.upper() for t in WHITELIST_TICKERS}:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not in beta")

    fund_row = db.execute(text("""
        SELECT fund_id, ticker, name, category
        FROM funds WHERE UPPER(ticker) = UPPER(:t)
    """), {"t": ticker}).first()
    if not fund_row:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")

    fund_id = fund_row[0]

    # Резолвим current date.
    if date:
        try:
            from datetime import date as _date
            current_date = _date.fromisoformat(date)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="date must be ISO format")
    else:
        latest = db.execute(text("""
            SELECT MAX(snapshot_date) FROM fund_holdings_history
            WHERE fund_id = :fid AND source = ANY(:sources)
        """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES)}).first()
        if not latest or not latest[0]:
            raise HTTPException(status_code=404, detail="No snapshots for this fund")
        current_date = latest[0]

    # Резолвим previous date = ближайший snapshot до current_date.
    prev_row = db.execute(text("""
        SELECT snapshot_date FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources) AND snapshot_date < :curr
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES), "curr": current_date}).first()
    previous_date = prev_row[0] if prev_row else None

    # FULL OUTER JOIN current vs previous → 4 группы.
    if previous_date is None:
        diff_rows = []
        # Просто весь current как "initial composition"
        rows = db.execute(text("""
            SELECT asset_name, isin, positions, amount_rub, weight
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :d AND source = ANY(:sources)
            ORDER BY amount_rub DESC NULLS LAST
        """), {"fid": fund_id, "d": current_date, "sources": list(MONTHLY_SOURCES)}).mappings().all()
        current_holdings = [
            {
                "asset_name": r["asset_name"],
                "isin": r["isin"],
                "positions": int(r["positions"]) if r["positions"] is not None else None,
                "amount_rub": float(r["amount_rub"]) if r["amount_rub"] is not None else None,
                "weight": float(r["weight"]) if r["weight"] is not None else None,
            }
            for r in rows
        ]
        return {
            "fund": {"ticker": fund_row[1], "name": fund_row[2], "category": fund_row[3]},
            "current_snapshot_date": current_date.isoformat(),
            "previous_snapshot_date": None,
            "current_holdings": current_holdings,
            "added": [], "reduced": [], "new": [], "sold_out": [],
            "totals": {
                "current_assets": len(current_holdings),
                "previous_assets": 0,
                "total_added_rub": 0.0,
                "total_reduced_rub": 0.0,
                "total_new_rub": 0.0,
                "total_sold_out_rub": 0.0,
            },
        }

    diff_rows = db.execute(text("""
        WITH curr AS (
            SELECT asset_name, isin, positions, amount_rub, weight
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :curr_d AND source = ANY(:sources)
        ),
        prev AS (
            SELECT asset_name, isin, positions, amount_rub, weight
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :prev_d AND source = ANY(:sources)
        )
        SELECT
            COALESCE(curr.asset_name, prev.asset_name) AS asset_name,
            COALESCE(curr.isin, prev.isin) AS isin,
            curr.positions AS curr_pos,
            prev.positions AS prev_pos,
            curr.amount_rub AS curr_amt,
            prev.amount_rub AS prev_amt,
            curr.weight AS curr_wt,
            prev.weight AS prev_wt
        FROM curr
        FULL OUTER JOIN prev USING (asset_name)
    """), {"fid": fund_id, "curr_d": current_date, "prev_d": previous_date,
           "sources": list(MONTHLY_SOURCES)}).mappings().all()

    added, reduced, new_pos, sold_out = [], [], [], []
    current_holdings = []

    for r in diff_rows:
        curr_pos = int(r["curr_pos"]) if r["curr_pos"] is not None else None
        prev_pos = int(r["prev_pos"]) if r["prev_pos"] is not None else None
        curr_amt = float(r["curr_amt"]) if r["curr_amt"] is not None else None
        prev_amt = float(r["prev_amt"]) if r["prev_amt"] is not None else None

        # Цена в текущем снапшоте (если есть данные)
        curr_price = None
        if curr_pos and curr_pos > 0 and curr_amt:
            curr_price = curr_amt / curr_pos

        delta_pos = None
        delta_amount_rub = None
        if curr_pos is not None and prev_pos is not None:
            delta_pos = curr_pos - prev_pos
            if curr_price:
                delta_amount_rub = delta_pos * curr_price

        row_data = {
            "asset_name": r["asset_name"],
            "isin": r["isin"],
            "curr_positions": curr_pos,
            "prev_positions": prev_pos,
            "curr_amount_rub": curr_amt,
            "prev_amount_rub": prev_amt,
            "curr_weight": float(r["curr_wt"]) if r["curr_wt"] is not None else None,
            "prev_weight": float(r["prev_wt"]) if r["prev_wt"] is not None else None,
            "delta_positions": delta_pos,
            "delta_amount_rub": delta_amount_rub,
        }

        # Классификация (MECE)
        if curr_pos is not None and prev_pos is None:
            # Новая позиция
            new_pos.append(row_data)
        elif curr_pos is None and prev_pos is not None:
            # Полностью вышел
            sold_out.append(row_data)
        elif curr_pos is not None and prev_pos is not None:
            if curr_pos > prev_pos:
                added.append(row_data)
            elif curr_pos < prev_pos:
                reduced.append(row_data)
            # curr_pos == prev_pos → unchanged, skip

        # Current holdings (для секции состава)
        if curr_pos is not None:
            current_holdings.append({
                "asset_name": r["asset_name"],
                "isin": r["isin"],
                "positions": curr_pos,
                "amount_rub": curr_amt,
                "weight": float(r["curr_wt"]) if r["curr_wt"] is not None else None,
            })

    # Сортируем по |delta_amount_rub| (по сумме денег)
    added.sort(key=lambda x: -(x["delta_amount_rub"] or 0))
    reduced.sort(key=lambda x: (x["delta_amount_rub"] or 0))  # отрицательные → большее |delta| первое
    new_pos.sort(key=lambda x: -(x["curr_amount_rub"] or 0))
    sold_out.sort(key=lambda x: -(x["prev_amount_rub"] or 0))
    current_holdings.sort(key=lambda x: -(x["amount_rub"] or 0))

    # Totals для сводки
    total_added_rub = sum(x["delta_amount_rub"] or 0 for x in added)
    total_reduced_rub = sum(x["delta_amount_rub"] or 0 for x in reduced)
    total_new_rub = sum(x["curr_amount_rub"] or 0 for x in new_pos)
    total_sold_out_rub = sum(x["prev_amount_rub"] or 0 for x in sold_out)

    return {
        "fund": {"ticker": fund_row[1], "name": fund_row[2], "category": fund_row[3]},
        "current_snapshot_date": current_date.isoformat(),
        "previous_snapshot_date": previous_date.isoformat(),
        "current_holdings": current_holdings,
        "added": added,
        "reduced": reduced,
        "new": new_pos,
        "sold_out": sold_out,
        "totals": {
            "current_assets": len(current_holdings),
            "previous_assets": sum(1 for r in diff_rows if r["prev_pos"] is not None),
            "total_added_rub": total_added_rub,
            "total_reduced_rub": total_reduced_rub,
            "total_new_rub": total_new_rub,
            "total_sold_out_rub": total_sold_out_rub,
        },
    }


@router.get("/asset-history/{ticker}")
def asset_position_history(
    ticker: str,
    asset_name: Optional[str] = Query(None, description="Asset name (asset_name in DB)"),
    isin: Optional[str] = Query(None, description="ISIN — alternative to asset_name"),
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Полная история одной позиции в одном фонде: график positions/amount/weight
    по всем снапшотам где эта позиция была.

    Можно искать по asset_name ИЛИ ISIN (более стабильный ключ — имя могло
    меняться когда мы дозаполняли через MOEX ISS).
    """
    if ticker.upper() not in {t.upper() for t in WHITELIST_TICKERS}:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not in beta")
    if not asset_name and not isin:
        raise HTTPException(status_code=400, detail="asset_name or isin required")

    fund_row = db.execute(text("""
        SELECT fund_id, ticker, name FROM funds WHERE UPPER(ticker) = UPPER(:t)
    """), {"t": ticker}).first()
    if not fund_row:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")

    fund_id = fund_row[0]

    # Поиск по ISIN или имени.
    if isin:
        filter_sql = "AND isin = :isin"
        filter_params = {"isin": isin}
    else:
        filter_sql = "AND asset_name = :name"
        filter_params = {"name": asset_name}

    rows = db.execute(text(f"""
        SELECT snapshot_date, asset_name, isin, positions, amount_rub, weight
        FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources) {filter_sql}
        ORDER BY snapshot_date ASC
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES), **filter_params}).mappings().all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No history found for this asset in {ticker}",
        )

    # Берём asset_name из самой свежей строки (могут быть варианты в БД).
    latest_name = rows[-1]["asset_name"]
    asset_isin = next((r["isin"] for r in reversed(rows) if r["isin"]), None)

    # Считаем дельты между смежными snapshots.
    timeline = []
    prev_positions = None
    for r in rows:
        curr_pos = int(r["positions"]) if r["positions"] is not None else None
        delta_pos = (
            curr_pos - prev_positions
            if curr_pos is not None and prev_positions is not None
            else None
        )
        # Цена в текущей точке (для расчёта delta_amount).
        curr_amt = float(r["amount_rub"]) if r["amount_rub"] is not None else None
        curr_price = (curr_amt / curr_pos) if (curr_amt and curr_pos and curr_pos > 0) else None
        delta_amount_rub = delta_pos * curr_price if (delta_pos and curr_price) else None

        timeline.append({
            "snapshot_date": r["snapshot_date"].isoformat(),
            "positions": curr_pos,
            "amount_rub": curr_amt,
            "weight": float(r["weight"]) if r["weight"] is not None else None,
            "price_rub": round(curr_price, 2) if curr_price else None,
            "delta_positions": delta_pos,
            "delta_amount_rub": delta_amount_rub,
        })
        prev_positions = curr_pos

    return {
        "fund": {"ticker": fund_row[1], "name": fund_row[2]},
        "asset_name": latest_name,
        "isin": asset_isin,
        "snapshots_count": len(timeline),
        "first_seen": timeline[0]["snapshot_date"],
        "last_seen": timeline[-1]["snapshot_date"],
        "timeline": timeline,
    }
