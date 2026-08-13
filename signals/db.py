"""Database helpers for signal-engine.

Использует те же SessionLocal/ORM-модели что и FastAPI, но без Depends —
функции работают вне FastAPI request context (cron job).
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Optional, List

from sqlalchemy import text
from api.database import SessionLocal
from api.models import OpenInterest, Instrument, Candle
from api.services.contract_calendar import front_windows, resolve_day


@dataclass(frozen=True)
class OIPoint:
    tradedate: date
    pos_long: int
    pos_short: int   # хранится отрицательным числом в БД
    net: int         # pos_long + pos_short (с учётом знака)


@dataclass(frozen=True)
class CandlePoint:
    begin_time: datetime
    close: float


def get_oi_daily(
    sectype: str,
    clgroup: str,
    days: int,
    as_of_date: Optional[date] = None,
) -> List[OIPoint]:
    """Daily OI snapshots отсортированные по возрастанию даты (старые → новые).

    `as_of_date` — для backtest: вернуть данные «как если бы сегодня была эта дата».
    Если не задан — today().
    """
    end = as_of_date or date.today()
    cutoff = end - timedelta(days=days)
    with SessionLocal() as session:
        rows = (
            session.query(OpenInterest)
            .filter(
                OpenInterest.sectype == sectype,
                OpenInterest.clgroup == clgroup,
                OpenInterest.interval == 24,
                OpenInterest.tradedate >= cutoff,
                OpenInterest.tradedate <= end,
            )
            .order_by(OpenInterest.tradedate.asc())
            .all()
        )
    return [
        OIPoint(
            tradedate=r.tradedate,
            pos_long=r.pos_long,
            pos_short=r.pos_short,
            net=r.pos_long + r.pos_short,
        )
        for r in rows
    ]


def get_position_series(sectype: str, clgroup: str, days: int,
                        as_of_date: Optional[date] = None,
                        interval: int = 24) -> List[tuple]:
    """Дневной ряд (date, net, npart, pos_long, pos_short) для ATR-детектора
    «резкого движения позиции».
    net = pos_long + pos_short (pos_short отрицательный); npart = число участников
    (pos_long_num + pos_short_num) — для guard'а ликвидности. pos_long/pos_short —
    сами ноги (длинная положительная, короткая отрицательная) — чтобы текст алерта
    мог сказать, какая нога двинулась. Порядок элементов 0-2 неизменен — на них
    индексируются compute_position_atr (net) и compute_participants_atr (npart).

    `interval` — таймфрейм источника бара:
      24 → дневная публикация (одна точка/день — текущее поведение);
      5/60 → внутридневные бары. Берём ПОСЛЕДНИЙ бар каждого дня (закрытие дня)
      через DISTINCT ON (tradedate) … ORDER BY tradetime DESC. Для прошлых дней это
      их EOD-значение; для СЕГОДНЯ (незакрытый день) последний элемент = бегущий
      внутридневной бар → даёт «net сейчас» для раннего срабатывания дневного сигнала.
      Итог отсортирован по tradedate ASC (как и при interval=24)."""
    end = as_of_date or date.today()
    cutoff = end - timedelta(days=days)
    with SessionLocal() as session:
        rows = session.execute(
            text("""
                SELECT tradedate,
                       (pos_long + pos_short) AS net,
                       (pos_long_num + pos_short_num) AS npart,
                       pos_long, pos_short
                FROM (
                    SELECT DISTINCT ON (tradedate)
                        tradedate, pos_long, pos_short, pos_long_num, pos_short_num
                    FROM open_interest
                    WHERE sectype = :sectype
                      AND clgroup = :clgroup
                      AND interval = :interval
                      AND tradedate >= :cutoff
                      AND tradedate <= :end
                      -- ТОЛЬКО будни в сигнальном ряду. Торги выходного дня дают
                      -- interval=5 строки за сб/вс (для внутридневных ГРАФИКОВ), но
                      -- в ATR-ряд детектора их пускать нельзя: ряд станет нерегулярным
                      -- (сб есть, вс часто нет) → сдвиг diff'ов Пт→Сб→Пн исказил бы
                      -- ATR-базу для 5m/1h-алертов. Дневной сигнал (interval=24) и так
                      -- из openpositions (только будни) — фильтр для него no-op.
                      AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
                    ORDER BY tradedate, tradetime DESC
                ) t
                ORDER BY tradedate ASC
            """),
            {"sectype": sectype, "clgroup": clgroup, "interval": interval,
             "cutoff": cutoff, "end": end},
        ).fetchall()
    return [
        (r[0], (r[1] or 0), (r[2] or 0), (r[3] or 0), (r[4] or 0))
        for r in rows
    ]


# Категории фондов → бенчмарк-индекс (зеркало CATEGORY_INDEX_MAP в
# api/routers/funds.py). Юань включён (2026-06-19): NAV юаневых фондов хранится в
# ₽ (как у рублёвых), цена пая тоже ₽ → переоценка пая снимает курс CNY/RUB
# автоматически (та же математика net_flow, что у остальных категорий), отдельная
# CNY-логика не нужна. money_market/gold с min_date=2022-01-09 (ранняя история
# шумная), но для ATR-серии берём окно ~30-45д назад — все категории к этой дате
# давно «чистые», поэтому min_date тут не нужен.
FUND_CATEGORIES = {
    "money_market": "RUSFAR3M",
    "stocks": "IMOEX",
    "bonds": "RGBITR",
    "gold": "GLDRUB_TOM",
    "yuan": "CNYRUB_TOM",
}


def get_fund_flow_series(
    fund_ids: Optional[List[int]] = None,
    category: Optional[str] = None,
    days: int = 60,
) -> List[tuple]:
    """Дневной ряд (date, net_flow_total) суммарного «аномального потока» набора
    фондов — для ATR-детектора fund-сигналов. Зеркало net_flow-логики из
    api/routers/funds.py get_funds_flows (timeframe='1d').

    Набор фондов определяется так (приоритет сверху вниз):
      - fund_ids задан → именно эти фонды (заморожённый произвольный набор,
        возможно кросс-категория — asset='custom');
      - elif category задан → все фонды категории (динамически — backward-compat
        со старыми fund-алертами money_market|stocks|bonds|gold);
      - else → ВСЕ фонды 4 категорий (FUND_CATEGORIES, asset='all').
    Во всех случаях применяем фильтр «>=2 точки NAV» (как load_fund_categories
    в API) и оставляем только фонды известных категорий — у каждого фонда есть
    свой бенчмарк-индекс категории (FUND_CATEGORIES), переоценка пая per-fund.

    net_flow фонда за день = ΔNAV − market_change, где
    market_change = prev_nav · (curr_pay − prev_pay) / prev_pay — «бумажная»
    переоценка пая ЭТОГО фонда (рост СЧА за счёт цены пая, а не притока денег).
    Цена пая (pay) индивидуальна для каждого фонда и уже отражает динамику его
    бенчмарк-индекса категории, поэтому суммировать кросс-категорию корректно:
    каждый фонд переоценивается по своей цене пая. Итоговый дневной net_flow_total
    = Σ net_flow по всем фондам набора.

    Логика повторяет ветку `timeframe == "1d"` в API:
      - forward-fill nav/pay по последней доступной точке ДО даты (фонд мог не
        обновиться в конкретный день);
      - фонд учитывается в дне только если есть И prev_d, И curr_d (иначе появление/
        исчезновение фонда даёт ложный «поток» — структурное изменение, не деньги);
      - market_change вычитается лишь когда обе pay > 0 (иначе net_flow = ΔNAV).

    Возвращает список (date, net_flow_rub) по возрастанию даты. net_flow в рублях
    (НЕ делим на 1e9 как API — ATR-кратность безразмерна, масштаб не важен).
    """
    cutoff = date.today() - timedelta(days=days)
    with SessionLocal() as session:
        # Резолвим набор fund_id'ов с историей (>=2 точки NAV) — тот же фильтр, что
        # load_fund_categories в API (фонд с одной точкой не даёт «изменения»).
        # Дополнительно ограничиваем известными категориями FUND_CATEGORIES
        # (юань пока «Скоро» — не входит), чтобы у каждого фонда был бенчмарк.
        known_cats = list(FUND_CATEGORIES.keys())
        if fund_ids:
            # Произвольный набор: фильтруем по факту наличия истории и категории.
            rows = session.execute(
                text("""
                    SELECT f.fund_id FROM funds f
                    WHERE f.fund_id = ANY(:ids)
                      AND f.category = ANY(:cats)
                      AND (SELECT count(*) FROM fund_data fd
                           WHERE fd.fund_id = f.fund_id AND fd.nav IS NOT NULL) >= 2
                """),
                {"ids": list(fund_ids), "cats": known_cats},
            ).fetchall()
        elif category:
            if category not in FUND_CATEGORIES:
                return []
            rows = session.execute(
                text("""
                    SELECT f.fund_id FROM funds f
                    WHERE f.category = :cat
                      AND (SELECT count(*) FROM fund_data fd
                           WHERE fd.fund_id = f.fund_id AND fd.nav IS NOT NULL) >= 2
                """),
                {"cat": category},
            ).fetchall()
        else:
            # asset='all' → все фонды всех известных категорий.
            rows = session.execute(
                text("""
                    SELECT f.fund_id FROM funds f
                    WHERE f.category = ANY(:cats)
                      AND (SELECT count(*) FROM fund_data fd
                           WHERE fd.fund_id = f.fund_id AND fd.nav IS NOT NULL) >= 2
                """),
                {"cats": known_cats},
            ).fetchall()
        fund_ids = [r[0] for r in rows]
        if not fund_ids:
            return []
        # Per-fund nav/pay с forward-fill (зеркало all_dates/fund_filled из API).
        rows = session.execute(
            text("""
                WITH all_dates AS (
                    SELECT DISTINCT trade_date FROM fund_data
                    WHERE fund_id = ANY(:fund_ids) AND trade_date >= :cutoff
                )
                SELECT f.fund_id, d.trade_date,
                    COALESCE(fd.nav,
                        (SELECT fd2.nav FROM fund_data fd2
                         WHERE fd2.fund_id = f.fund_id AND fd2.trade_date < d.trade_date
                           AND fd2.nav IS NOT NULL
                         ORDER BY fd2.trade_date DESC LIMIT 1)) AS nav,
                    COALESCE(fd.pay,
                        (SELECT fd2.pay FROM fund_data fd2
                         WHERE fd2.fund_id = f.fund_id AND fd2.trade_date < d.trade_date
                           AND fd2.pay IS NOT NULL
                         ORDER BY fd2.trade_date DESC LIMIT 1)) AS pay
                FROM all_dates d
                CROSS JOIN (SELECT DISTINCT fund_id FROM fund_data
                            WHERE fund_id = ANY(:fund_ids)) f
                LEFT JOIN fund_data fd ON fd.fund_id = f.fund_id AND fd.trade_date = d.trade_date
                ORDER BY f.fund_id, d.trade_date
            """),
            {"fund_ids": fund_ids, "cutoff": cutoff},
        ).fetchall()
    # date -> {fund_id: (nav, pay)} (только дни с nav, как API: WHERE nav IS NOT NULL)
    by_fund: dict = {}
    for fid, d, nav, pay in rows:
        if nav is None:
            continue
        by_fund.setdefault(fid, {})[d] = (float(nav), float(pay or 0))
    all_dates = sorted({d for fmap in by_fund.values() for d in fmap})
    series: List[tuple] = []
    for i in range(1, len(all_dates)):
        prev_d, curr_d = all_dates[i - 1], all_dates[i]
        total_net = 0.0
        for fid, date_map in by_fund.items():
            if prev_d not in date_map or curr_d not in date_map:
                continue  # фонд отсутствует в одном из дней — не поток
            prev_nav, prev_pay = date_map[prev_d]
            curr_nav, curr_pay = date_map[curr_d]
            total_flow = curr_nav - prev_nav
            if prev_pay > 0 and curr_pay > 0:
                market_change = prev_nav * (curr_pay - prev_pay) / prev_pay
                net_flow = total_flow - market_change
            else:
                net_flow = total_flow
            total_net += net_flow
        series.append((curr_d, total_net))
    return series


def get_candles_continuous(
    sectype: str, days: int, end_date: Optional[date] = None
) -> List[CandlePoint]:
    """Continuous daily price series, склеенная из контрактов sectype по КАЛЕНДАРЮ.

    На каждый день берём свечу календарного фронт-контракта
    (futures_contracts.lsttrade); если его свечей за день нет (историч. дни,
    собранные старым объёмным фетчером) — откат на макс-объём. Единый источник
    истины — api/services/contract_calendar (без преждевременных роллов/пропусков).

    `end_date` — верхняя граница (для бэктеста/showcase «как если бы сегодня было
    end_date»; согласовано с get_oi_daily.as_of_date). По умолчанию today().
    """
    end = end_date or date.today()
    cutoff = end - timedelta(days=days)
    with SessionLocal() as session:
        windows = front_windows(session, sectype)
        rows = session.execute(
            text("""
                SELECT begin_time, close, sec_id, volume
                FROM candles
                WHERE secid LIKE :prefix
                  AND interval = 24
                  AND type = 'futures'
                  AND begin_time::date >= :cutoff
                  AND begin_time::date <= :end
                  AND close > 0
                ORDER BY begin_time ASC
            """),
            {"prefix": f"{sectype}%", "cutoff": cutoff, "end": end},
        ).fetchall()

    # День → {sec_id: (begin_time, close)} (свеча с макс. объёмом контракта за день)
    # и день → {sec_id: суммарный объём} (для resolve_day).
    by_day: dict = {}
    vol_by_day: dict = {}
    best_vol: dict = {}  # (day, sec_id) -> макс одиночный объём, для выбора свечи
    for begin_time, close, sec_id, volume in rows:
        day = begin_time.date()
        v = float(volume or 0)
        vslot = vol_by_day.setdefault(day, {})
        vslot[sec_id] = vslot.get(sec_id, 0.0) + v
        slot = by_day.setdefault(day, {})
        if sec_id not in slot or v > best_vol.get((day, sec_id), -1.0):
            slot[sec_id] = (begin_time, float(close))
            best_vol[(day, sec_id)] = v

    out: List[CandlePoint] = []
    prev = None
    for day in sorted(by_day):
        chosen = resolve_day(windows, day, vol_by_day[day], prev=prev)
        if chosen is None or chosen not in by_day[day]:
            continue
        prev = chosen
        bt, cl = by_day[day][chosen]
        out.append(CandlePoint(begin_time=bt, close=cl))
    return out


def get_latest_price(sectype: str) -> Optional[tuple]:
    """Самая свежая цена sectype — intraday, когда актив ликвиден.

    Берём свечу с НАИБОЛЕЕ свежим begin_time среди интервалов 5/60/24 (внутри одного
    begin_time — с макс. объёмом = фронт-контракт). Во время сессии это 5-мин свеча
    (цена «сейчас»); у неликвидных активов 5м/60м может не быть → откатывается на
    дневную. Возвращает (close, begin_time, interval) или None (нет свежих данных).

    interval в ответе: 5/60 → intraday, 24 → дневная (EOD) — чтобы предупредить,
    что у актива нет внутридневных данных.

    ⚡ Производительность: резолвим sec_id через крошечную instruments + окно
    `begin_time >= now()-14д`. Тогда запрос ложится на индекс
    idx_candles_sec_interval_time (sec_id, interval, begin_time) → ~2 мс вместо
    12-130 с (наивный `secid LIKE` делал Seq Scan по 23М строк / 8 ГБ). Окно 14 дней
    переживает новогодние каникулы; если последняя свеча старше — актив «мёртв» → None.
    """
    with SessionLocal() as session:
        row = session.execute(
            text("""
                SELECT close, begin_time, interval
                FROM candles
                WHERE sec_id IN (SELECT sec_id FROM instruments
                                 WHERE sectype = :sectype AND type = 'futures')
                  AND interval IN (5, 60, 24) AND close > 0
                  AND begin_time >= now() - interval '14 days'
                ORDER BY begin_time DESC, volume DESC NULLS LAST
                LIMIT 1
            """),
            {"sectype": sectype},
        ).fetchone()
    if not row:
        return None
    return float(row[0]), row[1], int(row[2])


def get_asset_name(sectype: str) -> Optional[str]:
    """Human-readable название инструмента (None если нет в instruments)."""
    with SessionLocal() as session:
        row = (
            session.query(Instrument)
            .filter(Instrument.sectype == sectype)
            .first()
        )
    return row.name if row else None


def has_intraday_oi(sectype: str) -> bool:
    """True если у тикера есть интрадей-снэпшоты ОИ (interval 5/60) за последнюю
    неделю, False если данные только дневные (interval=24, одна точка в сутки —
    напр. PX «Полюс мини», проверено вживую 2026-07-13). Используется content-
    пайплайном (signals/content_match.py), чтобы не тратить лишние проверки на
    активы, где новых данных внутри дня физически не появится."""
    with SessionLocal() as session:
        exists = session.execute(
            text("""
                SELECT EXISTS(
                    SELECT 1 FROM open_interest
                    WHERE sectype = :sectype AND interval IN (5, 60)
                      AND tradedate >= CURRENT_DATE - 7
                )
            """),
            {"sectype": sectype},
        ).scalar()
    return bool(exists)


def get_buffett_current(mode: str = "cap_gdp") -> Optional[tuple]:
    """Текущий индикатор Баффета: (ratio_%, cap_млрд) на последнюю доступную дату.

    ratio = 100 * MARKET_CAP_TOTAL / знаменатель:
      • mode='cap_gdp' → ВВП TTM (сумма 4 последних кварталов GDP_QUARTERLY);
      • mode='cap_m2'  → последний M2_MONTHLY.
    Считаем СЫРОЕ последнее значение (без EMA-сглаживания/интерполяции графика) —
    для порога алерта этого достаточно; индикатор двигается медленно (мес. данные).
    None если данных не хватает. Зеркалит формулу api/routers/buffett.py.
    """
    with SessionLocal() as s:
        cap = s.execute(text(
            "SELECT value FROM macro_data WHERE indicator='MARKET_CAP_TOTAL' "
            "AND value > 0 ORDER BY period_date DESC LIMIT 1"
        )).scalar()
        if cap is None:
            return None
        cap = float(cap)
        if mode == "cap_m2":
            denom = s.execute(text(
                "SELECT value FROM macro_data WHERE indicator='M2_MONTHLY' "
                "AND value > 0 ORDER BY period_date DESC LIMIT 1"
            )).scalar()
        else:
            rows = s.execute(text(
                "SELECT value FROM macro_data WHERE indicator='GDP_QUARTERLY' "
                "ORDER BY period_date DESC LIMIT 4"
            )).fetchall()
            denom = sum(float(r[0]) for r in rows) if rows and len(rows) >= 4 else None
        if not denom or float(denom) <= 0:
            return None
        ratio = 100.0 * cap / float(denom)
        return (ratio, cap)


def get_breadth_current(ema_period: int = 200, universe: str = "imoex") -> Optional[float]:
    """Текущая «сила рынка» — % акций выше EMA на последнюю дату из breadth_history.

    ema_period ∈ {20,50,100,200}; universe ∈ {all,imoex,all_usd,imoex_usd}
    (база + '_usd' для долларового режима). Берём предвычисленную дневную строку
    (наполняется ~19:10 МСК) — дёшево и совпадает с графиком. None если нет.
    """
    with SessionLocal() as s:
        v = s.execute(text(
            "SELECT percent_above FROM breadth_history "
            "WHERE ema_period = :e AND universe = :u "
            "ORDER BY trade_date DESC LIMIT 1"
        ), {"e": int(ema_period), "u": universe}).scalar()
        return float(v) if v is not None else None
