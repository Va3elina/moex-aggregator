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
import math
from datetime import date, timedelta

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import get_current_user_optional

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
    "TOFZ",       # ОФЗ (SCHA с e-disclosure)
    # Сбер/Первая — Cbonds + reconstruct
    "SBMX",       # Фонд Топ Российских акций
    "SBRB",       # Корпоративные облигации
    "SBFR",       # Облигации флоатеры
    "SAFE",       # Консерватив
    "OPIF-47",    # Фонд Рублёвые сбережения
    "OPIF-4995",  # Накопительный
    "OPIF-43",    # Фонд российских акций
    "OPIF-4979",  # Фонд смешанный с выплатой дохода (SCHA interfax_manual)
    "OPIF-8119",  # Фонд облигаций с выплатой дохода
    "OPIF-8123",  # Фонд акций с выплатой дохода
    # Атон, Райффайзен — Cbonds + reconstruct
    "AMGB",       # Атон - Длинные ОФЗ (SCHA с e-disclosure)
    "OPIF-63",    # Атон - Петр Столыпин
    "OPIF-281",   # Райффайзен - Акции
    "OPIF-282",   # Райффайзен - Компании роста
    # LQDT/GOLD/TGLD/AKGD/SBGD — cash/REPO/ОМС-фонды без classical holdings,
    # скрыты из UI. Intraday-данные ВИМ-БПИФ собираются отдельно.
    # ── Авторские (блогерские) фонды акций — SCHA импортирован 2026-06
    #    (interfax_manual). ticker=ISIN (биржевого короткого нет).
    "RU000A10BZ69",  # Биткоган (Альфа)
    "RU000A10B8Z2",  # Блэк лайн (Альфа)
    "RU000A10B917",  # Матрёшка а-ля Рус (Альфа)
    "RU000A10B909",  # Великолепная семёрка (Альфа)
    "RU000A10EBY8",  # Долгосрочные инвестиции (Альфа)
    "RU000A10D1E0",  # Консервативная стратегия на МосБирже (Альфа)
    "RU000A10D5D3",  # Сбалансированные Возможности (Альфа)
    "RU000A104M43",  # Алёнка-Капитал (Рекорд Капитал)
    "RU000A108AB5",  # Флагманский (ГЕРОИ)
    # «Поляков Инвестиции» (RU000A10ERX6, Финам) УДАЛЁН ПОЛНОСТЬЮ 2026-07-03
    # по решению Вадима (вместе со строками funds/fund_data в БД). НЕ возвращать.
)


# Фонды, у которых данные о выплатах в ИСТОЧНИКЕ (Cbonds) повреждены → total-return
# считается неверно. Показываем «—» вместо вводящего в заблуждение числа (на финансовом
# продукте честнее скрыть, чем сфабриковать). Re-fetch НЕ помогает — баг на стороне Cbonds.
#   OPIF-9113 (Альфа-Капитал «Облигации с выплатой дохода»): с 07.2025 Cbonds публикует
#   выплаты в ~10–100× меньше реальных (0.2–2.4 ₽ вместо ~30–44 ₽/квартал — подтверждено
#   live-фетчем 2026-06-07). Фонд платит ~15%/год (NAV/пай ~flat 1005→1028 + рост AUM
#   3.5→30 млрд ₽), но total-return выходит +1.6% вместо ~+18%. Убрать тикер, когда
#   Cbonds починит данные или подтянем выплаты из другого источника.
RETURNS_UNRELIABLE_TICKERS = frozenset({"OPIF-9113"})


def _guard_returns(ticker, returns: dict) -> dict:
    """Если у фонда выплаты в источнике повреждены — доходность недостоверна → все
    периоды в None (фронт покажет «—»). Иначе returns без изменений."""
    if ticker in RETURNS_UNRELIABLE_TICKERS:
        return {k: None for k in returns}
    return returns


# Источники данных для /fund-trades.
#
# MONTHLY_SOURCES — для diff'ов между снапшотами фонда.
# ТОЛЬКО ДОКУМЕНТЫ (точный SCHA, форма ЦБ № 0420502):
#   vim_sdr          — SCHA-PDF с wealthim.ru (ВИМ-БПИФ).
#   interfax_manual  — SCHA-PDF/XLS с e-disclosure / own-site УК (Первая first-am,
#                      Т-Капитал, Атон, Альфа). ТОЧНЫЕ positions из самой УК.
# Cbonds (`cbonds`/`cbonds_baseline`/`cbonds_calc`) ОТКЛЮЧЁН (2026-05-31): реконструкция
# NAV×weight/price была подстраховкой, но для облигаций ненадёжна (эмитент↔выпуск),
# а по акциям документы точнее. Строки остаются в БД — чтобы вернуть, добавь обратно сюда.
MONTHLY_SOURCES = ("vim_sdr", "interfax_manual")

# Стандартные коэффициенты сплита акций. При коррекции сплита берём ближайший
# (в лог-шкале) к наблюдаемому, а НЕ сырое отношение количеств — иначе реальная
# торговля на границе сплита «съедается» (T 1:10: наблюдаем ~10.03 → берём 10).
_SPLIT_RATIOS = (2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 100, 150, 200, 500, 1000)


def _nearest_split_ratio(observed: float) -> float:
    cands = _SPLIT_RATIOS if observed >= 1 else [1.0 / r for r in _SPLIT_RATIOS]
    return min(cands, key=lambda c: abs(math.log(observed) - math.log(c)))


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


# Периоды для diff-расчёта в МЕСЯЦАХ (label → months назад). SCHA-снапшоты —
# месячные (month-end), поэтому дельты надо считать по выравненным месяцам, а НЕ
# «curr − N дней»: при дрейфе дня месяца (29 мая vs 30 апр) день-арифметика
# перепрыгивает через месяц (29 мая − 30д = 29 апр < 30 апр → prev уезжает в март).
PERIOD_MONTHS = {
    "1m": 1,
    "3m": 3,
    "6m": 6,
    "1y": 12,
}


def _parse_period_months(period: str) -> int:
    if period not in PERIOD_MONTHS:
        raise HTTPException(
            status_code=400,
            detail=f"period must be one of {list(PERIOD_MONTHS)}, got '{period}'",
        )
    return PERIOD_MONTHS[period]


def _calc_return(last, prev):
    """Период-доходность % по nav_per_share (pay). NULL если истории не хватает."""
    if last is not None and prev is not None and float(prev) > 0:
        return round((float(last) - float(prev)) / float(prev) * 100, 2)
    return None


def _calc_total_return(last, prev, dist=0):
    """Полная доходность % = (last + Σвыплат − prev)/prev.

    Для накопительных фондов Σвыплат=0 → совпадает с _calc_return (price-return
    пая). Для фондов «с выплатой дохода» dist = сумма выплат (₽/пай, fund_distributions)
    за период — иначе доходность пая занижает реальную (NAV падает на дату выплаты).
    Источник выплат — Cbonds (record_date в окне периода). NULL если истории не хватает."""
    if last is not None and prev is not None and float(prev) > 0:
        return round((float(last) + float(dist or 0) - float(prev)) / float(prev) * 100, 2)
    return None


def _fund_performance(db: Session, fund_id: int) -> dict:
    """
    Доходность фонда по nav_per_share (fund_data.pay):
      - timeline: вся история pay (ASC, дневная), прореживаем только >4000 точек → ~3000.
      - returns: m1/m3/m6/y1 % по КАЛЕНДАРНЫМ месяцам от последней даты данных
        (fd_last.td − 1/3/6/12 мес) — совпадает с investfunds. pay = СЧА на пай (НЕ nav=AUM).
    """
    ret_row = db.execute(text("""
        SELECT fd_last.pay AS last_pay,
               fd_1m.pay AS pay_1m, fd_3m.pay AS pay_3m,
               fd_6m.pay AS pay_6m, fd_1y.pay AS pay_1y, fd_first.pay AS pay_first,
               (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
                WHERE d.fund_id = f.fund_id AND d.record_date <= fd_last.td) AS dist_all,
               (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
                WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '1 month'   AND d.record_date <= fd_last.td) AS dist_1m,
               (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
                WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '3 months'  AND d.record_date <= fd_last.td) AS dist_3m,
               (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
                WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '6 months'  AND d.record_date <= fd_last.td) AS dist_6m,
               (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
                WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '12 months' AND d.record_date <= fd_last.td) AS dist_1y
        FROM (SELECT CAST(:fid AS integer) AS fund_id) f
        LEFT JOIN LATERAL (
            SELECT pay, trade_date AS td FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        ) fd_last ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '1 month' ORDER BY trade_date DESC LIMIT 1
        ) fd_1m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '3 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_3m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '6 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_6m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '12 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_1y ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            ORDER BY trade_date ASC LIMIT 1
        ) fd_first ON true
    """), {"fid": fund_id}).mappings().first()

    pay_rows = db.execute(text("""
        SELECT trade_date, pay FROM fund_data
        WHERE fund_id = :fid AND pay IS NOT NULL
        ORDER BY trade_date ASC
    """), {"fid": fund_id}).mappings().all()

    # Дневной ряд: fund_data.pay обновляется КАЖДЫЙ торговый день. Отдаём строго
    # дневным; прореживаем только сверх-длинные истории (>4000 дней ≈ 16 лет — у нас
    # только OPIF-43 с 1997, 7184 точки), целясь ~3000, чтобы payload/рендер не пухли.
    # 16 из 17 фондов (вкл. SBMX 1890) — каждый день без прореживания. SimpleChart-
    # навигатор справляется с плотным рядом.
    n = len(pay_rows)
    if n > 4000:
        step = max(1, n // 3000)
        sampled = pay_rows[::step]
        if sampled and sampled[-1] is not pay_rows[-1]:
            sampled.append(pay_rows[-1])
        pay_rows = sampled

    timeline = [
        {"date": pr["trade_date"].isoformat(), "pay": float(pr["pay"])}
        for pr in pay_rows
    ]

    last_pay = ret_row["last_pay"] if ret_row else None
    returns = {
        "m1": _calc_total_return(last_pay, ret_row["pay_1m"], ret_row["dist_1m"]) if ret_row else None,
        "m3": _calc_total_return(last_pay, ret_row["pay_3m"], ret_row["dist_3m"]) if ret_row else None,
        "m6": _calc_total_return(last_pay, ret_row["pay_6m"], ret_row["dist_6m"]) if ret_row else None,
        "y1": _calc_total_return(last_pay, ret_row["pay_1y"], ret_row["dist_1y"]) if ret_row else None,
        "all": _calc_total_return(last_pay, ret_row["pay_first"], ret_row["dist_all"]) if ret_row else None,
    }
    # Guard: у фондов с битыми выплатами в источнике доходность недостоверна → «—».
    tic = db.execute(text("SELECT ticker FROM funds WHERE fund_id = :fid"),
                     {"fid": fund_id}).scalar()
    return {"timeline": timeline, "returns": _guard_returns(tic, returns)}


@router.get("/funds")
def list_funds_with_history(
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Список фондов из WHITELIST с history-метаданными.

    Beta-фильтр: возвращаем только 6 ВИМ-фондов (см. WHITELIST_TICKERS).
    Даже если в БД есть snapshot'ы других фондов — UI их не покажет.
    EXISTS на history сейчас опущен чтобы whitelist-фонды без snapshot'ов
    тоже отображались (с count=0) — UI разрулит "пока данных нет".

    Добавлено (редизайн): nav_rub (объём = полная СЧА), returns (доходность %
    по fund_data.pay, КАЛЕНДАРНЫЕ месяцы от последней даты — совпадает с investfunds),
    top_holdings (топ-10 позиций последнего snapshot, weight 0..1, короткое имя).
    """
    rows = db.execute(text("""
        SELECT
            f.fund_id,
            f.ticker,
            f.name,
            f.uk,
            f.uk_id,
            f.category,
            f.subcategory,
            (SELECT MAX(snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)) AS last_snapshot_date,
            (SELECT COUNT(DISTINCT snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)) AS snapshot_count,
            (SELECT COUNT(DISTINCT COALESCE(NULLIF(h.isin, ''), h.asset_name))
             FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)
               AND h.snapshot_date = (SELECT MAX(h2.snapshot_date) FROM fund_holdings_history h2
                                      WHERE h2.fund_id = f.fund_id AND h2.source = ANY(:sources))
            ) AS holdings_count,
            fd_last.nav AS nav_rub,
            fd_last.pay AS last_pay,
            fd_1m.pay AS pay_1m,
            fd_3m.pay AS pay_3m,
            fd_6m.pay AS pay_6m,
            fd_1y.pay AS pay_1y,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '1 month'   AND d.record_date <= fd_last.td) AS dist_1m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '3 months'  AND d.record_date <= fd_last.td) AS dist_3m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '6 months'  AND d.record_date <= fd_last.td) AS dist_6m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '12 months' AND d.record_date <= fd_last.td) AS dist_1y,
            EXISTS (SELECT 1 FROM fund_distributions d WHERE d.fund_id = f.fund_id) AS has_distributions
        FROM funds f
        LEFT JOIN LATERAL (
            SELECT nav, pay, trade_date AS td FROM fund_data WHERE fund_id = f.fund_id AND nav IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        ) fd_last ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '1 month' ORDER BY trade_date DESC LIMIT 1
        ) fd_1m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '3 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_3m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '6 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_6m ON true
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '12 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_1y ON true
        WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks'
          AND EXISTS (SELECT 1 FROM fund_holdings_history h2
                      WHERE h2.fund_id = f.fund_id AND h2.source = ANY(:sources))
        ORDER BY f.uk NULLS LAST, f.category, f.ticker
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES)}).mappings().all()

    fund_ids = [r["fund_id"] for r in rows]

    # Топ-10 позиций последнего snapshot каждого фонда (короткое имя, weight DESC).
    # Один доп. запрос на все ~18 фондов. Короткое имя — как везде в этом файле:
    # самое короткое asset_name по ISIN среди ВСЕХ фондов/источников.
    holdings_map = {}
    if fund_ids:
        h_rows = db.execute(text("""
            WITH names AS (
                SELECT h.isin, COALESCE(MAX(sr.short_name),
                       (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1]) AS short_name
                FROM fund_holdings_history h LEFT JOIN securities_ref sr ON sr.isin = h.isin
                WHERE COALESCE(h.isin, '') <> '' GROUP BY h.isin
            ),
            last_snap AS (
                SELECT fund_id, MAX(snapshot_date) AS d
                FROM fund_holdings_history
                WHERE fund_id = ANY(:fids) AND source = ANY(:sources)
                GROUP BY fund_id
            ),
            ranked AS (
                SELECT h.fund_id,
                       COALESCE(n.short_name, h.asset_name) AS name,
                       NULLIF(h.isin, '') AS isin,
                       h.weight,
                       ROW_NUMBER() OVER (PARTITION BY h.fund_id ORDER BY h.weight DESC NULLS LAST) AS rn
                FROM fund_holdings_history h
                JOIN last_snap ls ON ls.fund_id = h.fund_id AND ls.d = h.snapshot_date
                LEFT JOIN names n ON n.isin = h.isin
                WHERE h.source = ANY(:sources)
            )
            SELECT fund_id, name, isin, weight FROM ranked WHERE rn <= 10
            ORDER BY fund_id, rn
        """), {"fids": fund_ids, "sources": list(MONTHLY_SOURCES)}).mappings().all()
        for hr in h_rows:
            holdings_map.setdefault(hr["fund_id"], []).append({
                "name": hr["name"],
                "isin": hr["isin"],
                "weight": float(hr["weight"]) if hr["weight"] is not None else 0.0,
            })

    funds = []
    for r in rows:
        last_pay = r["last_pay"]
        funds.append({
            "fund_id": r["fund_id"],
            "ticker": r["ticker"],
            "name": r["name"],
            "uk": r["uk"],
            "uk_id": r["uk_id"],
            "category": r["category"],
            "subcategory": r["subcategory"],
            "last_snapshot_date": r["last_snapshot_date"].isoformat()
            if r["last_snapshot_date"] else None,
            "snapshot_count": r["snapshot_count"],
            "holdings_count": int(r["holdings_count"]) if r["holdings_count"] is not None else 0,
            "nav_rub": float(r["nav_rub"]) if r["nav_rub"] is not None else None,
            "returns": _guard_returns(r["ticker"], {
                "m1": _calc_total_return(last_pay, r["pay_1m"], r["dist_1m"]),
                "m3": _calc_total_return(last_pay, r["pay_3m"], r["dist_3m"]),
                "m6": _calc_total_return(last_pay, r["pay_6m"], r["dist_6m"]),
                "y1": _calc_total_return(last_pay, r["pay_1y"], r["dist_1y"]),
            }),
            "top_holdings": holdings_map.get(r["fund_id"], []),
            "has_distributions": bool(r["has_distributions"]),
        })

    return {
        "funds": funds,
        "count": len(funds),
    }


@router.get("/fund/{ticker}")
def fund_trades_detail(
    ticker: str = Path(..., min_length=1, max_length=20),
    period: str = Query("3m", description="1m | 3m | 6m | 1y"),
    user: Optional[User] = Depends(get_current_user_optional),
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

    months = _parse_period_months(period)

    # Найти fund_id по тикеру (case-insensitive).
    fund_row = db.execute(text("""
        SELECT fund_id, ticker, name, category, subcategory
        FROM funds WHERE UPPER(ticker) = UPPER(:t)
    """), {"t": ticker}).mappings().first()
    if not fund_row:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not found")
    # Облигационные/денежные/золотые фонды скрыты — фича показывает только акции.
    if fund_row["category"] != "stocks":
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not available (only stock funds)")

    fund_id = fund_row["fund_id"]

    # Доходность по nav_per_share — нужна для нового layout модалки (редизайн).
    performance = _fund_performance(db, fund_id)

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
            "performance": performance,
        }

    current_date = latest_row[0]

    # Previous snapshot = снапшот, выровненный по МЕСЯЦУ на N месяцев назад
    # (а не «curr − N дней» — день-арифметика перепрыгивает месяц при дрейфе
    # дня месяца, см. _parse_period_months). Берём последний снапшот СТРОГО до
    # начала месяца, который на (N−1) месяцев младше текущего: для 1m это
    # «последний снапшот до текущего месяца» = прошлый месяц-энд.
    prev_row = db.execute(text("""
        SELECT snapshot_date FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
          AND snapshot_date < date_trunc('month', CAST(:curr AS date))
                              - make_interval(months => CAST(:months AS integer) - 1)
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"fid": fund_id, "curr": current_date, "months": months,
           "sources": list(MONTHLY_SOURCES)}).first()

    previous_date = prev_row[0] if prev_row else None

    # Current holdings.
    current_rows = db.execute(text("""
        WITH names AS (
            SELECT h.isin, COALESCE(MAX(sr.short_name),
                   (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1]) AS short_name
            FROM fund_holdings_history h LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE COALESCE(h.isin, '') <> '' GROUP BY h.isin
        )
        SELECT COALESCE(n.short_name, h.asset_name) AS asset_name, h.weight, h.positions, h.amount_rub, h.isin
        FROM fund_holdings_history h
        LEFT JOIN names n ON n.isin = h.isin
        WHERE h.fund_id = :fid AND h.snapshot_date = :d AND h.source = ANY(:sources)
        ORDER BY h.weight DESC NULLS LAST
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
            "performance": performance,
        }

    # Diff через FULL OUTER JOIN между current и previous.
    # Каждая строка — один актив с его change_type и delta_weight.
    # Матч между снапшотами — по ISIN (mkey), с fallback на имя для строк без
    # ISIN. Раньше джойнили по asset_name → одна бумага с разным именем в
    # соседних снапшотах (особенно при смене источника vim_sdr/interfax/cbonds
    # или у облигаций) давала фантомные 'sold_out'+'new'. ISIN стабилен.
    diff_rows = db.execute(text("""
        WITH names AS (
            SELECT h.isin, COALESCE(MAX(sr.short_name),
                   (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1]) AS short_name
            FROM fund_holdings_history h LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE COALESCE(h.isin, '') <> '' GROUP BY h.isin
        ),
        curr AS (
            SELECT COALESCE(NULLIF(isin, ''), asset_name) AS mkey,
                   asset_name, weight, positions, amount_rub, isin
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :curr_d AND source = ANY(:sources)
        ),
        prev AS (
            SELECT COALESCE(NULLIF(isin, ''), asset_name) AS mkey,
                   asset_name, weight, positions, amount_rub, isin
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :prev_d AND source = ANY(:sources)
        )
        SELECT
            COALESCE(n.short_name, curr.asset_name, prev.asset_name) AS asset_name,
            COALESCE(curr.isin, prev.isin) AS isin,
            curr.weight AS curr_weight,
            prev.weight AS prev_weight,
            curr.positions AS curr_positions,
            prev.positions AS prev_positions,
            curr.amount_rub AS curr_amount,
            prev.amount_rub AS prev_amount,
            CASE
                WHEN prev.mkey IS NULL THEN 'new'
                WHEN curr.mkey IS NULL THEN 'sold_out'
                WHEN curr.weight > prev.weight THEN 'accumulated'
                WHEN curr.weight < prev.weight THEN 'reduced'
                ELSE 'unchanged'
            END AS change_type,
            CASE
                WHEN prev.mkey IS NULL THEN curr.weight
                WHEN curr.mkey IS NULL THEN -prev.weight
                ELSE curr.weight - prev.weight
            END AS delta_weight
        FROM curr
        FULL OUTER JOIN prev USING (mkey)
        LEFT JOIN names n ON n.isin = COALESCE(curr.isin, prev.isin)
        WHERE COALESCE(curr.weight, 0) <> COALESCE(prev.weight, 0)
        ORDER BY ABS(
            CASE
                WHEN prev.mkey IS NULL THEN curr.weight
                WHEN curr.mkey IS NULL THEN -prev.weight
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
            "isin": r["isin"],
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
        "performance": performance,
    }


@router.get("/movers")
def top_movers(
    period: str = Query("1m", description="1m | 3m | 6m | 1y"),
    category: str | None = Query(None, description="stocks | bonds | money_market | gold"),
    as_of: str | None = Query(None, description="YYYY-MM-DD — целевой месяц-снапшот (default=последний)"),
    manager: str | None = Query(None, description="фильтр по УК: comma-separated uk_id (напр. '34,5,3597'); один id тоже ок; пусто=все"),
    funds: str | None = Query(None, description="фильтр по конкретным фондам: comma-separated тикеры (напр. 'TMOS,SBMX'); приоритет над manager; пусто=все"),
    sort: str = Query("weight", description="weight | amount — метрика ранжирования и знака"),
    limit: int = Query(20, ge=1, le=100),
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Топ-аккумуляция / распродажа за период — сводно по всем фондам.

    Возвращает rank-таблицу: какие активы суммарно купили больше всего
    (sum of positive deltas weighted by NAV), какие — продали.

    Use case: "За месяц SBER суммарно накоплен на +3.4 п.п. across 7 фондов
    (TMOS +1.2, SBMX +0.9, ...)".
    """
    months = _parse_period_months(period)
    # Фича показывает ТОЛЬКО акции — облигации/деньги/золото скрыты целиком.
    # Параметр category игнорируется (всегда stocks).
    category_filter = "AND f.category = 'stocks'"
    whitelist_filter = "AND f.ticker = ANY(:tickers)"
    # funds: comma-separated список ТИКЕРОВ фондов (напр. "TMOS,SBMX"). Фильтр на
    # уровне самого фонда (f.ticker), а НЕ его УК. Имеет приоритет над manager.
    # Пусто / не задан / только разделители → не применяется (падаем на manager/все).
    fund_tickers = []
    if funds:
        for part in funds.split(","):
            part = part.strip()
            if part:
                fund_tickers.append(part)

    # manager: comma-separated список uk_id (напр. "34,5,3597"). Один id — частный
    # случай (обратная совместимость). Пусто / не задан / только разделители → все УК.
    # Фильтр по uk_id (числовой), а не по имени УК — фронт шлёт uk_id из fundConfig.
    manager_ids = []
    if manager:
        for part in manager.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                manager_ids.append(int(part))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"manager must be comma-separated integer uk_id, got '{part}'",
                )

    # Приоритет: funds (тикеры фондов) → manager (uk_id) → все фонды.
    # manager игнорируется, когда задан непустой funds.
    if fund_tickers:
        manager_filter = "AND f.ticker = ANY(:fund_tickers)"
    elif manager_ids:
        manager_filter = "AND f.uk_id = ANY(:managers)"
    else:
        manager_filter = ""
    # order_col server-controlled (валидируется ниже) → безопасно для f-string ORDER BY.
    order_col = "total_delta_amount" if sort == "amount" else "total_delta_weight"
    params = {
        "months": months,
        "limit": limit,
        "tickers": list(WHITELIST_TICKERS),
        "sources": list(MONTHLY_SOURCES),
        "as_of": as_of,
    }
    if fund_tickers:
        params["fund_tickers"] = fund_tickers
    elif manager_ids:
        params["managers"] = manager_ids

    # Для каждого фонда: latest snapshot + snapshot N дней назад.
    # Diff weight per (fund, asset) → агрегируем по asset.
    # Это complex CTE но даёт точную картину.
    rows = db.execute(text(f"""
        WITH anchor AS (
            -- Якорь = выбранный месяц (as_of) ТОЧНО, иначе — самый свежий месяц набора.
            -- Если юзер выбрал май, а у фондов набора майского снапшота нет — target=май,
            -- ни один фонд не пройдёт HAVING → пустой консенсус (фронт скажет «нет данных
            -- за месяц»), а НЕ молчаливый откат к апрелю под майской подписью.
            -- Консенсус считаем ТОЛЬКО по фондам, у которых ЕСТЬ снапшот target-месяца —
            -- иначе фонд со старым снапшотом тянет дельту другого периода и раздувает счёт.
            SELECT COALESCE(
                       date_trunc('month', CAST(:as_of AS date)),
                       date_trunc('month', MAX(h.snapshot_date))
                   ) AS target_month
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            WHERE 1=1 {category_filter} {whitelist_filter} {manager_filter}
              AND (CAST(:as_of AS date) IS NULL OR h.snapshot_date <= CAST(:as_of AS date))
        ),
        fund_dates AS (
            -- curr = снапшот target-месяца (нет → фонд выпадает из консенсуса);
            -- prev = снапшот, выровненный по МЕСЯЦУ на N месяцев назад от target.
            SELECT
                f.fund_id,
                f.ticker,
                f.name AS fund_name,
                MAX(h.snapshot_date) FILTER (
                    WHERE date_trunc('month', h.snapshot_date) = a.target_month
                ) AS curr_date,
                (
                    SELECT MAX(h2.snapshot_date)
                    FROM fund_holdings_history h2
                    WHERE h2.fund_id = f.fund_id
                      AND h2.source = ANY(:sources)
                      AND h2.snapshot_date < a.target_month - make_interval(months => CAST(:months AS integer) - 1)
                ) AS prev_date
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            CROSS JOIN anchor a
            WHERE 1=1 {category_filter} {whitelist_filter} {manager_filter}
              AND (CAST(:as_of AS date) IS NULL OR h.snapshot_date <= CAST(:as_of AS date))
            GROUP BY f.fund_id, f.ticker, f.name, a.target_month
            HAVING MAX(h.snapshot_date) FILTER (
                WHERE date_trunc('month', h.snapshot_date) = a.target_month
            ) IS NOT NULL
        ),
        per_fund_diff AS (
            -- Дельта weight per (fund, asset) между curr и prev snapshot.
            -- Матч curr↔prev по ISIN (akey), fallback на имя — устойчиво к разнице имён.
            SELECT
                fd.fund_id,
                fd.ticker,
                fd.fund_name,
                COALESCE(NULLIF(curr.isin, ''), NULLIF(prev.isin, ''),
                         curr.asset_name, prev.asset_name) AS akey,
                COALESCE(curr.asset_name, prev.asset_name) AS asset_name,
                COALESCE(curr.weight, 0) - COALESCE(prev.weight, 0) AS delta_weight,
                COALESCE(curr.amount_rub, 0) - COALESCE(prev.amount_rub, 0) AS delta_amount
            FROM fund_dates fd
            LEFT JOIN fund_holdings_history curr
                ON curr.fund_id = fd.fund_id AND curr.snapshot_date = fd.curr_date
                AND curr.source = ANY(:sources)
            FULL OUTER JOIN fund_holdings_history prev
                ON prev.fund_id = fd.fund_id AND prev.snapshot_date = fd.prev_date
                AND prev.source = ANY(:sources)
                AND (COALESCE(NULLIF(curr.isin, ''), curr.asset_name)
                     = COALESCE(NULLIF(prev.isin, ''), prev.asset_name)
                     OR curr.asset_name IS NULL)
            WHERE fd.prev_date IS NOT NULL
              AND COALESCE(curr.asset_name, prev.asset_name) IS NOT NULL
        ),
        aggregated AS (
            -- Суммарная дельта per asset (across всех фондов), агрегируем по ISIN-ключу.
            SELECT
                akey,
                -- Имя per akey: каноническое из securities_ref (по ISIN), fallback —
                -- самое короткое свободное. Различает ао/ап, схлопывает написания.
                COALESCE(MAX(sr.short_name),
                         (array_agg(asset_name ORDER BY length(asset_name), asset_name))[1]) AS asset_name,
                SUM(delta_weight) AS total_delta_weight,
                SUM(delta_amount) AS total_delta_amount,
                COUNT(DISTINCT fund_id) FILTER (WHERE delta_weight > 0) AS funds_buying,
                COUNT(DISTINCT fund_id) FILTER (WHERE delta_weight < 0) AS funds_selling
            FROM per_fund_diff
            LEFT JOIN securities_ref sr ON sr.isin = akey
            WHERE delta_weight <> 0
            GROUP BY akey
        )
        (
            SELECT 'top_accumulated' AS bucket, akey, asset_name, total_delta_weight, total_delta_amount,
                   funds_buying, funds_selling
            FROM aggregated
            WHERE {order_col} > 0
            ORDER BY {order_col} DESC
            LIMIT :limit
        )
        UNION ALL
        (
            SELECT 'top_reduced' AS bucket, akey, asset_name, total_delta_weight, total_delta_amount,
                   funds_buying, funds_selling
            FROM aggregated
            WHERE {order_col} < 0
            ORDER BY {order_col} ASC
            LIMIT :limit
        )
    """), params).mappings().all()

    top_accumulated = []
    top_reduced = []
    for r in rows:
        item = {
            "akey": r["akey"],
            "asset_name": r["asset_name"],
            "total_delta_weight": float(r["total_delta_weight"]),
            "total_delta_amount": float(r["total_delta_amount"] or 0),
            "funds_buying": r["funds_buying"],
            "funds_selling": r["funds_selling"],
        }
        if r["bucket"] == "top_accumulated":
            top_accumulated.append(item)
        else:
            top_reduced.append(item)

    # Доступные месяцы для month-picker: один пункт на КАЛЕНДАРНЫЙ месяц.
    # У разных УК разные дни конца месяца (27/28/30/31) → схлопываем по месяцу
    # и берём MAX-дату месяца как as_of (каждый фонд внутри возьмёт свой <= неё).
    available_month_dates = db.execute(text("""
        SELECT MAX(h.snapshot_date) AS d
        FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id
        WHERE f.category = 'stocks' AND f.ticker = ANY(:tickers) AND h.source = ANY(:sources)
        GROUP BY date_trunc('month', h.snapshot_date)
        ORDER BY d DESC
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES)}).scalars().all()

    # resolved_month = фактический target-месяц консенсуса (выбранный as_of, иначе последний
    # доступный для набора); funds_in_month = сколько фондов набора РЕАЛЬНО имеют снапшот
    # этого месяца. Фронт по funds_in_month==0 показывает «нет данных за месяц для выбранных
    # фондов» вместо «нет движений» (когда выбран месяц, которого у фондов ещё нет).
    meta_row = db.execute(text(f"""
        WITH anchor AS (
            SELECT COALESCE(
                       date_trunc('month', CAST(:as_of AS date)),
                       date_trunc('month', MAX(h.snapshot_date))
                   ) AS target_month
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            WHERE 1=1 {category_filter} {whitelist_filter} {manager_filter}
              AND (CAST(:as_of AS date) IS NULL OR h.snapshot_date <= CAST(:as_of AS date))
        )
        SELECT
            a.target_month::date AS resolved_month,
            (SELECT COUNT(DISTINCT f.fund_id)
             FROM funds f
             JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
             WHERE 1=1 {category_filter} {whitelist_filter} {manager_filter}
               AND date_trunc('month', h.snapshot_date) = a.target_month
            ) AS funds_in_month
        FROM anchor a
    """), params).first()
    resolved_month = (
        meta_row[0].isoformat() if meta_row and meta_row[0] else None
    )
    funds_in_month = int(meta_row[1]) if meta_row and meta_row[1] is not None else 0

    return {
        "period": period,
        "category": category,
        "as_of": as_of,
        "resolved_month": resolved_month,
        "funds_in_month": funds_in_month,
        "manager": manager,
        "funds": funds,
        "sort": sort,
        "available_months": [m.isoformat() for m in available_month_dates],
        "top_accumulated": top_accumulated,
        "top_reduced": top_reduced,
    }


@router.get("/asset/{asset_name}")
def asset_buyers(
    asset_name: str = Path(..., min_length=1, max_length=255),
    period: str = Query("3m", description="1m | 3m | 6m | 1y"),
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Reverse view: какие фонды покупают/продают конкретный актив за период.

    Use case: "Я держу Сбер — какие БПИФ его аккумулируют, какие распродают?"
    """
    months = _parse_period_months(period)

    rows = db.execute(text("""
        WITH anchor AS (
            -- Якорь — МЕСЯЦ самого свежего снапшота среди фондов, держащих актив.
            -- Считаем дельты ТОЛЬКО по фондам со снапшотом этого месяца (как /movers),
            -- prev выравниваем по месяцу — иначе мешаем периоды и перепрыгиваем месяц.
            SELECT date_trunc('month', MAX(h.snapshot_date)) AS target_month
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            WHERE h.asset_name = :asset
              AND f.ticker = ANY(:tickers)
        ),
        fund_pairs AS (
            -- curr = снапшот актива в target-месяце (нет → фонд выпадает);
            -- prev = снапшот актива, выровненный на N месяцев назад от target.
            SELECT
                f.fund_id,
                f.ticker,
                f.name AS fund_name,
                f.category,
                MAX(h.snapshot_date) FILTER (
                    WHERE date_trunc('month', h.snapshot_date) = a.target_month
                ) AS curr_date,
                (
                    SELECT MAX(h2.snapshot_date)
                    FROM fund_holdings_history h2
                    WHERE h2.fund_id = f.fund_id
                      AND h2.asset_name = :asset
                      AND h2.source = ANY(:sources)
                      AND h2.snapshot_date < a.target_month - make_interval(months => CAST(:months AS integer) - 1)
                ) AS prev_date
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            CROSS JOIN anchor a
            WHERE h.asset_name = :asset
              AND f.ticker = ANY(:tickers)
            GROUP BY f.fund_id, f.ticker, f.name, f.category, a.target_month
            HAVING MAX(h.snapshot_date) FILTER (
                WHERE date_trunc('month', h.snapshot_date) = a.target_month
            ) IS NOT NULL
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
        "months": months,
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
    user: Optional[User] = Depends(get_current_user_optional),
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
    user: Optional[User] = Depends(get_current_user_optional),
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
            WITH names AS (
                SELECT h.isin, COALESCE(MAX(sr.short_name),
                       (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1]) AS short_name
                FROM fund_holdings_history h LEFT JOIN securities_ref sr ON sr.isin = h.isin
                WHERE COALESCE(h.isin, '') <> '' GROUP BY h.isin
            )
            SELECT COALESCE(n.short_name, h.asset_name) AS asset_name, h.isin, h.positions, h.amount_rub, h.weight
            FROM fund_holdings_history h
            LEFT JOIN names n ON n.isin = h.isin
            WHERE h.fund_id = :fid AND h.snapshot_date = :d AND h.source = ANY(:sources)
            ORDER BY h.amount_rub DESC NULLS LAST
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

    # Матч curr↔prev по ISIN (mkey), с fallback на имя для строк без ISIN.
    # КРИТИЧНО: НЕ джойнить USING (asset_name) — у двухклассовых бумаг (Сбербанк
    # ао+ап, Татнефть ао+ап, Сургут ао+ап и т.д.) ОБА класса имеют ОДНО имя
    # «…СБЕРБАНК РОССИИ» → декартово произведение спаривало curr-ао с prev-ап
    # → фантомный «докупил +4.5M шт». ISIN уникален per класс. (Тот же фикс уже
    # в /fund и /movers.)
    # names: самое КОРОТКОЕ имя по ISIN среди ВСЕХ фондов/источников (вкл. cbonds) —
    # как в /movers. Иначе Первая/Т-Капитал пишут длинные юр. названия («Публичное
    # акционерное общество "СБЕРБАНК РОССИИ"»), а у ВИМ/cbonds есть короткие тикерные
    # («Сбербанк», «Сургнфгз-п»). Отображаем короткое — единообразно с консенсусом.
    diff_rows = db.execute(text("""
        WITH names AS (
            SELECT h.isin, COALESCE(MAX(sr.short_name),
                   (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1]) AS short_name
            FROM fund_holdings_history h LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE COALESCE(h.isin, '') <> '' GROUP BY h.isin
        ),
        curr AS (
            SELECT COALESCE(NULLIF(isin, ''), asset_name) AS mkey,
                   asset_name, isin, positions, amount_rub, weight
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :curr_d AND source = ANY(:sources)
        ),
        prev AS (
            SELECT COALESCE(NULLIF(isin, ''), asset_name) AS mkey,
                   asset_name, isin, positions, amount_rub, weight
            FROM fund_holdings_history
            WHERE fund_id = :fid AND snapshot_date = :prev_d AND source = ANY(:sources)
        )
        SELECT
            COALESCE(n.short_name, curr.asset_name, prev.asset_name) AS asset_name,
            COALESCE(curr.isin, prev.isin) AS isin,
            curr.positions AS curr_pos,
            prev.positions AS prev_pos,
            curr.amount_rub AS curr_amt,
            prev.amount_rub AS prev_amt,
            curr.weight AS curr_wt,
            prev.weight AS prev_wt
        FROM curr
        FULL OUTER JOIN prev USING (mkey)
        LEFT JOIN names n ON n.isin = COALESCE(curr.isin, prev.isin)
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

        # Сплит-коррекция: если между prev и curr был сплит (кол-во ×R, цена ÷R,
        # стоимость непрерывна), приводим prev к ПОСТ-сплит масштабу — иначе дельта
        # и классификация покажут фантом на сам факт сплита (T 1:10: «+5.3M докупил»,
        # хотя реально фонд чуть продал). Множитель = ближайший стандартный коэф.
        adj_prev_pos = prev_pos
        if curr_pos and prev_pos and curr_amt and prev_amt and curr_price and prev_pos > 0:
            prev_price = prev_amt / prev_pos
            pos_r = curr_pos / prev_pos
            price_r = prev_price / curr_price if curr_price else 0
            amt_r = curr_amt / prev_amt if prev_amt else 1
            if (price_r and abs(amt_r - 1) < 0.4 and abs(pos_r / price_r - 1) < 0.4
                    and ((pos_r > 1.8 and price_r > 1.8) or (pos_r < 0.55 and price_r < 0.55))):
                adj_prev_pos = round(prev_pos * _nearest_split_ratio((pos_r * price_r) ** 0.5))

        delta_pos = None
        delta_amount_rub = None
        if curr_pos is not None and adj_prev_pos is not None:
            delta_pos = curr_pos - adj_prev_pos
            if curr_price:
                delta_amount_rub = delta_pos * curr_price

        row_data = {
            "asset_name": r["asset_name"],
            "isin": r["isin"],
            "curr_positions": curr_pos,
            "prev_positions": adj_prev_pos,
            "curr_amount_rub": curr_amt,
            "prev_amount_rub": prev_amt,
            "curr_weight": float(r["curr_wt"]) if r["curr_wt"] is not None else None,
            "prev_weight": float(r["prev_wt"]) if r["prev_wt"] is not None else None,
            "delta_positions": delta_pos,
            "delta_amount_rub": delta_amount_rub,
        }

        # Классификация (MECE) — по СПЛИТ-СКОРРЕКТИРОВАННОЙ дельте позиций.
        if curr_pos is not None and prev_pos is None:
            # Новая позиция
            new_pos.append(row_data)
        elif curr_pos is None and prev_pos is not None:
            # Полностью вышел
            sold_out.append(row_data)
        elif curr_pos is not None and adj_prev_pos is not None:
            if curr_pos > adj_prev_pos:
                added.append(row_data)
            elif curr_pos < adj_prev_pos:
                reduced.append(row_data)
            # curr_pos == adj_prev_pos → unchanged, skip

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
    user: Optional[User] = Depends(get_current_user_optional),
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


# ────────────────────────────────────────────────────────────────────
# Редизайн: селектор бумаг + «Потоки по компании» (помесячные Δ позиции
# по всем фондам, что держат бумагу).
# ────────────────────────────────────────────────────────────────────


# sec_type (ISS) → крупная категория для табов пикера «Потоки по компании».
# Дробных типов ISS много (common_share/preferred_share/ofz_bond/exchange_bond/
# exchange_ppif/...), но пользователю в фильтре нужны 5-6 корзин. None/неизвестный
# → 'other' (строки без ISIN/не найденные в securities_ref).
def _asset_category(sec_type: Optional[str]) -> str:
    if not sec_type:
        return "other"
    t = sec_type.lower()
    if t in ("common_share", "preferred_share", "depositary_receipt"):
        return "share"
    if "bond" in t:                       # ofz_bond / exchange_bond / corporate_bond / ...
        return "bond"
    if "ppif" in t or t == "etf":         # биржевые/открытые ПИФы, ETF
        return "fund"
    if t == "currency":
        return "currency"
    if "futures" in t or "commodity" in t:
        return "commodity"
    if t == "index":
        return "index"
    return "other"


@router.get("/assets")
def list_fund_trade_assets(
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Список бумаг для селектора «Потоки по компании».

    Все активы по всем WHITELIST-фондам (source=MONTHLY_SOURCES) за всю историю,
    сгруппированы по mkey = COALESCE(NULLIF(isin,''), asset_name).
      - funds_count = в скольких разных фондах встречается.
      - last_amount_rub = суммарная последняя стоимость позиции (для сортировки) —
        сумма по фондам amount_rub из последнего snapshot каждого фонда, где бумага есть.
    Сортировка: funds_count DESC, last_amount_rub DESC. Имя — короткое.
    """
    rows = db.execute(text("""
        WITH scoped AS (
            -- mkey по КАНОНИЧЕСКОМУ isin: редомициль-пары (старый ГДР + новая
            -- локальная акция) сливаются в одну строку пикера (canonical_isin).
            SELECT h.fund_id, h.snapshot_date, h.asset_name, h.amount_rub, h.weight,
                   COALESCE(sr.canonical_isin, NULLIF(h.isin, ''), h.asset_name) AS mkey
            FROM fund_holdings_history h
            JOIN funds f ON f.fund_id = h.fund_id
            LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks'
              AND h.source = ANY(:sources)
        ),
        names AS (
            SELECT s.mkey,
                   COALESCE(MAX(sr2.short_name),
                            (array_agg(s.asset_name ORDER BY length(s.asset_name), s.asset_name))[1]) AS short_name,
                   MAX(CASE WHEN char_length(s.mkey) = 12 THEN s.mkey END) AS isin,
                   MAX(sr2.sec_type) AS sec_type
            FROM scoped s LEFT JOIN securities_ref sr2 ON sr2.isin = s.mkey
            GROUP BY s.mkey
        ),
        last_per_fund AS (
            -- последний snapshot ЭТОЙ бумаги В каждом фонде (где она есть).
            SELECT DISTINCT ON (mkey, fund_id) mkey, fund_id, amount_rub, weight
            FROM scoped
            ORDER BY mkey, fund_id, snapshot_date DESC
        ),
        agg AS (
            SELECT mkey,
                   COUNT(DISTINCT fund_id) AS funds_count,
                   SUM(amount_rub) AS last_amount_rub,
                   AVG(weight) AS avg_weight_pct
            FROM last_per_fund GROUP BY mkey
        )
        SELECT a.mkey AS key, n.short_name AS asset_name, n.isin, n.sec_type,
               a.funds_count, a.last_amount_rub, a.avg_weight_pct
        FROM agg a JOIN names n ON n.mkey = a.mkey
        ORDER BY a.funds_count DESC, a.last_amount_rub DESC NULLS LAST, n.short_name
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES)}).mappings().all()

    return {
        "assets": [
            {
                "key": r["key"],
                "asset_name": r["asset_name"],
                "isin": r["isin"],
                "sec_type": r["sec_type"],
                "category": _asset_category(r["sec_type"]),
                "funds_count": int(r["funds_count"]),
                "last_amount_rub": float(r["last_amount_rub"]) if r["last_amount_rub"] is not None else None,
                "avg_weight_pct": float(r["avg_weight_pct"]) if r["avg_weight_pct"] is not None else None,
            }
            for r in rows
        ],
    }


@router.get("/company-flows")
def company_flows(
    isin: Optional[str] = Query(None, description="ISIN бумаги (предпочтительно)"),
    asset_name: Optional[str] = Query(None, description="Имя бумаги — если нет ISIN"),
    metric: str = Query("amount", description="amount | weight"),
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Помесячные потоки выбранной бумаги по всем WHITELIST-фондам, что её держат.

    Поток за месяц = Δ к ПРЕДЫДУЩЕМУ снапшоту ЭТОГО фонда:
      - metric='amount' → delta_amount_rub (₽); metric='weight' → delta_weight (доля).
      - Самый ПЕРВЫЙ снапшот фонда в истории бумаги → null (нет базы → не спайк).
      - Появление новой позиции в более позднем снапшоте → Δ = текущее значение (приток).
    Дельта split-adjusted по той же логике что snapshot_review/asset-history (для
    metric='amount' приводим prev-positions к пост-сплит масштабу; amount_rub/weight
    непрерывны через сплит, поэтому коррекция для них фактически нейтральна).

    Месячная ось = union всех snapshot-месяцев («YYYY-MM») всех фондов бумаги, ASC.
    values выровнено по months (null где нет снапшота в месяце). total = сумма по фондам.
    """
    if metric not in ("amount", "weight"):
        raise HTTPException(status_code=400, detail="metric must be 'amount' or 'weight'")
    if not isin and not asset_name:
        raise HTTPException(status_code=400, detail="isin or asset_name required")

    # Матч по ISIN (если задан), иначе по asset_name. Берём ВСЕ строки этой бумаги
    # во всех WHITELIST-фондах, по всем снапшотам — считаем дельты per fund в Python
    # (чтобы переиспользовать ту же split-логику что в snapshot_review).
    if isin:
        # Матч по КАНОНИЧЕСКОМУ isin: выбранная бумага = canonical, берём и сам
        # canonical, и все старые ISIN, заалиасенные на него (редомициль-пары) →
        # потоки старой ГДР + новой акции в одном графике.
        match_sql = ("(h.isin = :isin OR h.isin IN "
                     "(SELECT isin FROM securities_ref WHERE canonical_isin = :isin))")
        match_params = {"isin": isin}
    else:
        match_sql = "h.asset_name = :aname"
        match_params = {"aname": asset_name}

    rows = db.execute(text(f"""
        WITH names AS (
            SELECT h.isin, COALESCE(MAX(sr.short_name),
                   (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1]) AS short_name
            FROM fund_holdings_history h LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE COALESCE(h.isin, '') <> '' GROUP BY h.isin
        )
        SELECT f.fund_id, f.ticker, f.name AS fund_name, f.uk_id,
               h.snapshot_date, h.positions, h.amount_rub, h.weight, h.isin,
               COALESCE(n.short_name, h.asset_name) AS asset_name
        FROM fund_holdings_history h
        JOIN funds f ON f.fund_id = h.fund_id
        LEFT JOIN names n ON n.isin = h.isin
        WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks'
          AND h.source = ANY(:sources)
          AND {match_sql}
        ORDER BY f.fund_id, h.snapshot_date ASC
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES), **match_params}).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="Asset not found in any whitelist fund")

    # Резолвим отображаемое имя/ISIN (самое свежее непустое).
    resolved_name = rows[-1]["asset_name"]
    resolved_isin = isin or next((r["isin"] for r in reversed(rows) if r["isin"]), None)

    # Группируем по фонду, считаем Δ к предыдущему снапшоту фонда (split-adjusted).
    # month «YYYY-MM» по дате снапшота. Если у фонда 2 снапшота в одном месяце —
    # берём последний (perevious остаётся предыдущим хронологически).
    from collections import defaultdict
    per_fund_rows = defaultdict(list)
    fund_meta = {}
    for r in rows:
        fid = r["fund_id"]
        per_fund_rows[fid].append(r)
        fund_meta[fid] = {"ticker": r["ticker"], "fund_name": r["fund_name"], "uk_id": r["uk_id"]}

    all_months = set()
    # fund_id -> { "YYYY-MM": value|None }
    fund_month_val = {}
    for fid, frows in per_fund_rows.items():
        # Дельты считаем ОТДЕЛЬНО по каждому ISIN (линии инструмента). Старый ISIN
        # расписки (ГДР) и новый ISIN акции склеены в один фонд через canonical_isin,
        # чтобы показать их потоки на одном графике — но вычитать один из другого
        # НЕЛЬЗЯ: это разные инструменты с разным масштабом позиций. Иначе на стыке
        # расконвертации (или на битой строке-остатке) (curr_pos − prev_pos)×price
        # взрывается. Поток фонда по бумаге за месяц = сумма потоков по каждому ISIN.
        isin_groups = defaultdict(list)
        for r in frows:
            isin_groups[r["isin"] or r["asset_name"]].append(r)

        month_val = {}
        for _key, irows in isin_groups.items():
            irows.sort(key=lambda x: x["snapshot_date"])
            prev_pos = None
            prev_amt = None
            prev_weight = None
            for r in irows:
                month = r["snapshot_date"].strftime("%Y-%m")
                all_months.add(month)
                curr_pos = int(r["positions"]) if r["positions"] is not None else None
                curr_amt = float(r["amount_rub"]) if r["amount_rub"] is not None else None
                curr_weight = float(r["weight"]) if r["weight"] is not None else None
                curr_price = (curr_amt / curr_pos) if (curr_amt and curr_pos and curr_pos > 0) else None

                if prev_pos is None and prev_amt is None and prev_weight is None:
                    # Первый снапшот этого ISIN в истории → null (нет базы → не спайк).
                    value = None
                else:
                    if metric == "amount":
                        # Split-adjust prev_pos (та же логика, что snapshot_review):
                        # стоимость непрерывна → коррекция нейтральна для ₽-дельты,
                        # но держим структуру идентичной для согласованности.
                        adj_prev_pos = prev_pos
                        if (curr_pos and prev_pos and curr_amt and prev_amt and curr_price
                                and prev_pos > 0):
                            prev_price = prev_amt / prev_pos
                            pos_r = curr_pos / prev_pos
                            price_r = prev_price / curr_price if curr_price else 0
                            amt_r = curr_amt / prev_amt if prev_amt else 1
                            if (price_r and abs(amt_r - 1) < 0.4 and abs(pos_r / price_r - 1) < 0.4
                                    and ((pos_r > 1.8 and price_r > 1.8) or (pos_r < 0.55 and price_r < 0.55))):
                                adj_prev_pos = round(prev_pos * _nearest_split_ratio((pos_r * price_r) ** 0.5))
                        if curr_amt is not None and prev_amt is not None:
                            if curr_pos is not None and adj_prev_pos is not None and curr_price:
                                # Δ стоимости = Δ позиций (split-adj) × текущая цена.
                                value = (curr_pos - adj_prev_pos) * curr_price
                            else:
                                value = curr_amt - prev_amt
                        elif curr_amt is not None:
                            value = curr_amt  # новая позиция (приток)
                        else:
                            value = None
                    else:  # weight
                        if curr_weight is not None and prev_weight is not None:
                            value = curr_weight - prev_weight
                        elif curr_weight is not None:
                            value = curr_weight  # новая позиция
                        else:
                            value = None

                # Несколько ISIN одной бумаги (ГДР + акция) → суммируем их вклад в месяц.
                # value=None (нет базы) — месяц помечаем, но в сумму не берём (ось X
                # не теряет точку, фантомного нуля не возникает).
                if value is not None:
                    month_val[month] = (month_val.get(month) or 0.0) + value
                else:
                    month_val.setdefault(month, None)
                # Обновляем prev только если в текущем снапшоте бумага реально есть.
                if curr_pos is not None or curr_amt is not None or curr_weight is not None:
                    prev_pos = curr_pos
                    prev_amt = curr_amt
                    prev_weight = curr_weight
        fund_month_val[fid] = month_val

    months = sorted(all_months)

    funds_out = []
    # Стабильный порядок фондов: по тикеру.
    for fid in sorted(per_fund_rows.keys(), key=lambda i: fund_meta[i]["ticker"]):
        mv = fund_month_val[fid]
        funds_out.append({
            "ticker": fund_meta[fid]["ticker"],
            "fund_name": fund_meta[fid]["fund_name"],
            "uk_id": fund_meta[fid]["uk_id"],
            "values": [mv.get(m) for m in months],
        })

    # total = сумма по фондам на каждый месяц (None → 0; месяц без любых данных → null).
    total = []
    for i, m in enumerate(months):
        present = [f["values"][i] for f in funds_out if f["values"][i] is not None]
        total.append(sum(present) if present else None)

    return {
        "asset_name": resolved_name,
        "isin": resolved_isin,
        "metric": metric,
        "funds_count": len(funds_out),
        "months": months,
        "funds": funds_out,
        "total": total,
    }
