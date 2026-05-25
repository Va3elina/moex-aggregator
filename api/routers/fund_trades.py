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
    "LQDT",       # money market
    "EQMX",       # индекс МосБиржи (БПИФ)
    "GOLD",       # золото биржевой
    "OPIF-1003",  # ВИМ - Акции (управляемые)
    "OPIF-54",    # ВИМ - Казначейский
    "OPIF-9165",  # ВИМ - Облигации Рантье
)


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
            f.category,
            f.subcategory,
            (SELECT MAX(snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id) AS last_snapshot_date,
            (SELECT COUNT(DISTINCT snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id) AS snapshot_count
        FROM funds f
        WHERE f.ticker = ANY(:tickers)
        ORDER BY f.category, f.ticker
    """), {"tickers": list(WHITELIST_TICKERS)}).mappings().all()
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
        WHERE fund_id = :fid
    """), {"fid": fund_id}).first()

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
        WHERE fund_id = :fid AND snapshot_date <= :threshold
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"fid": fund_id, "threshold": threshold}).first()

    previous_date = prev_row[0] if prev_row else None

    # Current holdings.
    current_rows = db.execute(text("""
        SELECT asset_name, weight, positions, amount_rub
        FROM fund_holdings_history
        WHERE fund_id = :fid AND snapshot_date = :d
        ORDER BY weight DESC NULLS LAST
    """), {"fid": fund_id, "d": current_date}).mappings().all()

    current_holdings = [
        {
            "asset_name": r["asset_name"],
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
            SELECT asset_name, weight, positions, amount_rub
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :curr_d
        ),
        prev AS (
            SELECT asset_name, weight, positions, amount_rub
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :prev_d
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
    """), {"fid": fund_id, "curr_d": current_date, "prev_d": previous_date}).mappings().all()

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
                ) AS prev_date
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id
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
            FULL OUTER JOIN fund_holdings_history prev
                ON prev.fund_id = fd.fund_id AND prev.snapshot_date = fd.prev_date
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
                ) AS prev_date
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id
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
            COALESCE(curr.weight, 0) - COALESCE(prev.weight, 0) AS delta_weight
        FROM fund_pairs fp
        LEFT JOIN fund_holdings_history curr
            ON curr.fund_id = fp.fund_id
            AND curr.snapshot_date = fp.curr_date
            AND curr.asset_name = :asset
        LEFT JOIN fund_holdings_history prev
            ON prev.fund_id = fp.fund_id
            AND prev.snapshot_date = fp.prev_date
            AND prev.asset_name = :asset
        ORDER BY delta_weight DESC NULLS LAST
    """), {
        "asset": asset_name,
        "days": days,
        "tickers": list(WHITELIST_TICKERS),
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
