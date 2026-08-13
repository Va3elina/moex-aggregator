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
from api.billing.tiers import user_tier
from api.billing.features import get_indicator_limits

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


# ─────────────────────────────────────────────────────────────────────────────
# Фильтр релевантности активов в пикере «Потоки по компании».
#
# Прячем бумаги, которые фонды почти не торгуют: суммарный ОБОРОТ покупок и
# продаж за последние 3 года по всем WHITELIST-фондам не превышает порога. Оборот
# считаем как Σ|Δпозиций × цена| (та же величина, что рисует график, но по модулю)
# — это отсекает пассивный крупный холд и переоценку по цене, меряя именно
# торговую активность. Основную «грязь» в списке дают облигации из смешанных и
# авторских фондов — у них оборот ≈ 0, и они уходят первыми.
#
# Порог ДИНАМИЧЕСКИЙ по построению: окно скользящее («последние 3 года»), а сам
# расчёт живёт в SQL эндпоинта /assets и выполняется на КАЖДЫЙ запрос. Значит
# ревизия происходит сама собой — после каждого месячного снапшота набор
# пересчитывается: бумага перешагнула порог → вернулась в пикер, просела → ушла.
# Отдельного крона/материализованной таблицы не нужно. Сам /company-flows НЕ
# фильтруем: прямая ссылка или эмбед на уже скрытую бумагу продолжает работать.
CF_MIN_GROSS_FLOW_3Y_RUB = 30_000_000  # 30 млн ₽ оборота за 3 года


# ─────────────────────────────────────────────────────────────────────────────
# Порог полноты снапшота: Σ weight (% СЧА) ≥ 80 → отчёт считаем полным.
#
# Частичные импорты SCHA (парсер терял часть строк: EQMX 2023-12..2024-12 и
# 2025-04..08, TMOS 2023-2024 — Σ весов 35-65% вместо ~98) НЕ считаются
# свидетельством отсутствия бумаги: дифф против них рисует фантомные сделки на
# весь размер позиции («индексный фонд продал весь Сбер на 943 млн»). Полные
# отчёты живых фондов дают Σ 90-100 (Алёнка 98.5, ГЕРОИ 100.0); ниже 80 — только
# дырявые импорты и AKME 2021-24 (кэш ~30% — исключение консервативно: месяц
# мостится, фантомов нет). Распределение по 473 снапшотам: 393 ≥ 90, 46 < 80.
FT_COMPLETE_WSUM_MIN = 80


# ─────────────────────────────────────────────────────────────────────────────
# Историческая нижняя граница фичи: снапшоты РАНЬШЕ этой даты не показываем ни
# в одном эндпоинте — ни в списках месяцев/снапшотов, ни как базу для диффов.
# У AKME/TMOS реально есть отчёты с марта 2021 (проверено на проде: 45 и 88
# снапшотов старше границы), но старые SCHA за 2021-2022 — самые дырявые (см.
# FT_COMPLETE_WSUM_MIN выше) и не проходят порог качества методологии.
# Фонды, у которых собственная история короче границы, ничего не теряют —
# UI и так не показывает месяцы, которых нет.
FT_HISTORY_FLOOR = date(2021, 9, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Задержка данных для Free/гостя (пилот delayed-data freemium).
#
# Free/гость видят fund-trades на 1 снапшот позади: свежая месячная выборка «что
# фонды купили» открывается по подписке (любой платный тир = realtime). Правило
# берётся из features.py INDICATOR_FEATURES["fund_trades"]["<tier>"]["snapshot_delay"].
#
# Реализация — ГЛОБАЛЬНЫЙ cutoff по дате свежести выборки: даты месячных выборок у
# фондов whitelist в основном выровнены (все month-end), поэтому «<= (offset+1)-я
# по свежести дата» ≈ per-fund «−1 снапшот». Edge: у уже-отстающих фондов (их
# latest = глобальная 2-я дата) отсечка не срабатывает — но свежее данных всё
# равно нет, так что скрывать нечего. При раскате на весь проект и добавлении
# разно-периодичных фондов — пересмотреть на строгий per-fund.
_FAR_FUTURE = date(9999, 12, 31)


# Ключ матрицы для «Общего портфеля» — своя задержка, независимая от остальных
# разделов (features.py: fund_trades.portfolio_snapshot_delay, сейчас 0 на всех
# тирах). Витрины портфеля обязаны ходить именно с ним, иначе вернётся общий гейт.
PORTFOLIO_DELAY_KEY = "portfolio_snapshot_delay"

# Ключ матрицы для вкладки «По бумаге» (потоки по компании) — свежесть там не
# режем никому (решение владельца 2026-08-10, features.py:
# fund_trades.company_snapshot_delay = 0). Читают его /company-flows и
# /company-weights; остальные ручки раздела продолжают ходить с snapshot_delay.
COMPANY_DELAY_KEY = "company_snapshot_delay"

# Ключ матрицы для ВИТРИНЫ (вкладка «Витрина» — каталог фондов со СЧА,
# доходностью и датой последнего отчёта): свежесть там не режем никому
# (решение владельца 2026-08-10, features.py: fund_trades.showcase_snapshot_delay
# = 0). Читает его /funds. Карточка фонда («что купили и продали») — уже НЕ
# витрина, её ручки продолжают ходить с общим snapshot_delay.
SHOWCASE_DELAY_KEY = "showcase_snapshot_delay"


def _default_nonindex_funds(db) -> str | None:
    """Продуктовый дефолт набора «Общего портфеля»: все фонды, КРОМЕ индексных.

    Индексные фонды механически повторяют индекс — их сделки это ребалансировка,
    а не решения управляющих, и в консенсусе они забивают сигнал. Тумблер «Без
    индексных фондов» в пикере включён по умолчанию, и это касается ВСЕХ тиров
    (решение владельца 2026-08-10): тиры без fund_picker свой набор прислать не
    могут, поэтому дефолт применяется здесь, на бэке, вместо «всех фондов».
    Матч подкатегории по префиксу — зеркало frontend isIndexSubcategory
    (fundConfig.ts): в данных встречается «Индекс МосБиржи»/«Мосбиржи».

    Возвращает comma-строку тикеров в формате параметра `funds` (или None,
    если по какой-то причине не-индексных фондов не нашлось — тогда «все»).
    """
    rows = db.execute(text("""
        SELECT ticker FROM funds
        WHERE ticker = ANY(:tickers) AND category = 'stocks'
          AND LOWER(TRIM(COALESCE(subcategory, ''))) NOT LIKE 'индекс%'
    """), {"tickers": list(WHITELIST_TICKERS)}).scalars().all()
    return ",".join(rows) if rows else None


def _snapshot_offset(user, key: str = "snapshot_delay") -> int:
    """На сколько снапшотов свежести назад видит юзер (0 = свежий срез).

    `key` выбирает поле матрицы: 'snapshot_delay' — общий гейт раздела,
    PORTFOLIO_DELAY_KEY — отдельный (нулевой) гейт «Общего портфеля».
    """
    limits = get_indicator_limits(user_tier(user), "fund_trades")
    return int(limits.get(key, 0) or 0)


def _snapshot_cutoff(db, user, key: str = "snapshot_delay") -> date:
    """Дата-отсечка свежести для тира. offset 0 → _FAR_FUTURE (без задержки).
    offset N → N-я по свежести дата месячной выборки среди whitelist-фондов
    (Free видит снапшоты `<= cutoff`). Если истории меньше — без задержки."""
    offset = _snapshot_offset(user, key)
    if offset <= 0:
        return _FAR_FUTURE
    cutoff = db.execute(text("""
        SELECT snapshot_date FROM (
            SELECT DISTINCT h.snapshot_date
            FROM fund_holdings_history h
            JOIN funds f ON f.fund_id = h.fund_id
            WHERE f.category = 'stocks'
              AND f.ticker = ANY(:tickers)
              AND h.source = ANY(:sources)
            ORDER BY h.snapshot_date DESC
            OFFSET :offset LIMIT 1
        ) x
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES),
           "offset": offset}).scalar()
    return cutoff or _FAR_FUTURE

# Стандартные коэффициенты сплита акций. При коррекции сплита берём ближайший
# (в лог-шкале) к наблюдаемому, а НЕ сырое отношение количеств — иначе реальная
# торговля на границе сплита «съедается» (T 1:10: наблюдаем ~10.03 → берём 10).
_SPLIT_RATIOS = (2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30, 40, 50, 100, 150, 200, 500, 1000)


def _month_range(start: str, end: str) -> list[str]:
    """Непрерывный список месяцев «YYYY-MM» от start до end включительно.

    Ось «Потоков по компании» должна быть настоящим таймлайном, а не списком
    месяцев, где бумага случайно оказалась в фондах: пропуски внутри и хвост
    после полной распродажи рисуются пустыми (нулевыми) барами.
    """
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    out: list[str] = []
    while (y, m) <= (ey, em):
        out.append("%04d-%02d" % (y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def _nearest_split_ratio(observed: float) -> float:
    cands = _SPLIT_RATIOS if observed >= 1 else [1.0 / r for r in _SPLIT_RATIOS]
    return min(cands, key=lambda c: abs(math.log(observed) - math.log(c)))


# Периоды для diff-расчёта (label → days назад).
PERIOD_DAYS = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "3y": 1095,
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
    "3y": 36,
}


def _parse_period_months(period: str) -> int:
    if period not in PERIOD_MONTHS:
        raise HTTPException(
            status_code=400,
            detail=f"period must be one of {list(PERIOD_MONTHS)}, got '{period}'",
        )
    return PERIOD_MONTHS[period]


def _month_start(value: str, field: str) -> date:
    """'YYYY-MM' | 'YYYY-MM-DD' → первое число месяца.

    Границы произвольного диапазона выравниваем по МЕСЯЦУ (данные месячные, день
    конца месяца у разных УК свой), поэтому день во входной дате игнорируется.
    """
    raw = (value or "").strip()
    try:
        if len(raw) == 7:
            return date(int(raw[:4]), int(raw[5:7]), 1)
        return date.fromisoformat(raw).replace(day=1)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be YYYY-MM or YYYY-MM-DD, got '{value}'",
        )


def _next_month(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


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
    cutoff = _snapshot_cutoff(db, user, SHOWCASE_DELAY_KEY)  # витрина — без задержки на всех тирах
    rows = db.execute(text("""
        SELECT
            f.fund_id,
            f.ticker,
            f.name,
            f.uk,
            f.uk_id,
            f.category,
            f.subcategory,
            -- Порядок подкатегорий — та же колонка, по которой их сортируют
            -- «Деньги в фондах» (Индекс Мосбиржи → Управляемые → Авторские).
            f.subcategory_order,
            (SELECT MAX(snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)
               AND h.snapshot_date <= :cutoff) AS last_snapshot_date,
            (SELECT COUNT(DISTINCT snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)
               AND h.snapshot_date >= :floor) AS snapshot_count,
            (SELECT COUNT(DISTINCT COALESCE(NULLIF(h.isin, ''), h.asset_name))
             FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)
               AND h.snapshot_date = (SELECT MAX(h2.snapshot_date) FROM fund_holdings_history h2
                                      WHERE h2.fund_id = f.fund_id AND h2.source = ANY(:sources)
                                        AND h2.snapshot_date <= :cutoff)
            ) AS holdings_count,
            fd_last.nav AS nav_rub,
            fd_last.pay AS last_pay,
            fd_1m.pay AS pay_1m,
            fd_3m.pay AS pay_3m,
            fd_6m.pay AS pay_6m,
            fd_1y.pay AS pay_1y,
            fd_5y.pay AS pay_5y,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '1 month'   AND d.record_date <= fd_last.td) AS dist_1m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '3 months'  AND d.record_date <= fd_last.td) AS dist_3m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '6 months'  AND d.record_date <= fd_last.td) AS dist_6m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '12 months' AND d.record_date <= fd_last.td) AS dist_1y,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '60 months' AND d.record_date <= fd_last.td) AS dist_5y,
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
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '60 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_5y ON true
        WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks'
          AND EXISTS (SELECT 1 FROM fund_holdings_history h2
                      WHERE h2.fund_id = f.fund_id AND h2.source = ANY(:sources))
        ORDER BY f.uk NULLS LAST, f.category, f.ticker
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES), "cutoff": cutoff,
           "floor": FT_HISTORY_FLOOR}).mappings().all()

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
                  AND snapshot_date <= :cutoff
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
        """), {"fids": fund_ids, "sources": list(MONTHLY_SOURCES), "cutoff": cutoff}).mappings().all()
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
            "subcategory_order": r["subcategory_order"],
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
                "y5": _calc_total_return(last_pay, r["pay_5y"], r["dist_5y"]),
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
    range_from: str | None = Query(
        None, alias="from",
        description="YYYY-MM(-DD) — БАЗОВЫЙ месяц произвольного диапазона; задаётся вместе с `to` и отменяет period",
    ),
    range_to: str | None = Query(
        None, alias="to",
        description="YYYY-MM(-DD) — ЦЕЛЕВОЙ месяц произвольного диапазона (сравнение from → to)",
    ),
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Детальная аналитика дельт по фонду:
      - current_holdings: latest snapshot (что в фонде сейчас)
      - previous_holdings: snapshot N дней назад
      - diff: список изменений с типом (accumulated/reduced/new/sold_out)

    Если истории нет (только один snapshot) — diff пустой, current'ы заполнены.

    Период сравнения задаётся либо пресетом (period=1m|3m|6m|1y), либо
    произвольным диапазоном месяцев (from→to) — семантика та же, что у /movers:
    сравниваем снапшот месяца `from` (база) со снапшотом месяца `to` (цель).
    """
    # Beta whitelist — другие фонды возвращают 404 (не светим что они в БД).
    if ticker.upper() not in {t.upper() for t in WHITELIST_TICKERS}:
        raise HTTPException(status_code=404, detail=f"Fund {ticker} not available in beta")

    months = _parse_period_months(period)

    # Произвольный диапазон — с Basic (матрица, fund_trades.custom_range).
    # Гасим оба параметра → тир считает по пресету period. Фронт запирает
    # кнопку-календарь, но URL правится руками — гейт живёт на бэке.
    if not get_indicator_limits(user_tier(user), "fund_trades").get("custom_range", True):
        range_from = None
        range_to = None

    # Произвольный диапазон (from → to) вместо пресета «N месяцев назад от свежего».
    # prev_bound = начало месяца, следующего за `from`: последний снапшот СТРОГО
    # раньше него = снапшот месяца `from` (или ближайший более ранний, как в пресетах).
    # curr_bound = конец целевого месяца: якорь текущего снапшота.
    prev_bound: date | None = None
    curr_bound: date | None = None
    range_from_norm: str | None = None
    range_to_norm: str | None = None
    if bool(range_from) != bool(range_to):
        raise HTTPException(status_code=400, detail="from и to задаются только вместе")
    if range_from and range_to:
        _f = _month_start(range_from, "from")
        _t = _month_start(range_to, "to")
        if _f >= _t:
            raise HTTPException(status_code=400, detail="from must be an earlier month than to")
        if _f < FT_HISTORY_FLOOR:
            raise HTTPException(status_code=400, detail=f"from earlier than {FT_HISTORY_FLOOR.isoformat()} is not available")
        prev_bound = _next_month(_f)
        curr_bound = _next_month(_t) - timedelta(days=1)
        range_from_norm, range_to_norm = _f.isoformat(), _t.isoformat()

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

    # Карточка фонда открывается КЛИКОМ ИЗ ВИТРИНЫ и показывает тот же каталог
    # (состав, доходность) → ходит с витринным ключом, без задержки на всех тирах.
    # Иначе список и карточка разъезжались бы по датам снапшота.
    cutoff = _snapshot_cutoff(db, user, SHOWCASE_DELAY_KEY)
    # Месяцы с данными по этому фонду — календарь произвольного диапазона на фронте
    # (месяц-энд каждого месяца, свежие сверху; ограничены тем же cutoff и floor).
    available_month_dates = db.execute(text("""
        SELECT MAX(snapshot_date) AS d FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
          AND snapshot_date <= :cutoff AND snapshot_date >= :floor
        GROUP BY date_trunc('month', snapshot_date)
        ORDER BY d DESC
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES),
           "cutoff": cutoff, "floor": FT_HISTORY_FLOOR}).scalars().all()
    available_months = [m.isoformat() for m in available_month_dates]

    # Якорь текущего снапшота: конец целевого месяца диапазона, но не свежее cutoff.
    curr_cap = cutoff if curr_bound is None else min(curr_bound, cutoff)
    latest_row = db.execute(text("""
        SELECT MAX(snapshot_date) AS d FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
          AND snapshot_date <= :cutoff
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES), "cutoff": curr_cap}).first()

    if not latest_row or not latest_row[0]:
        # Нет истории вообще
        return {
            "fund": dict(fund_row),
            "period": period,
            "range_from": range_from_norm,
            "range_to": range_to_norm,
            "available_months": available_months,
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
    # При произвольном диапазоне граница приходит готовой (prev_bound = начало
    # месяца после `from`), COALESCE в SQL выбирает её вместо арифметики месяцев.
    prev_row = db.execute(text("""
        SELECT snapshot_date FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
          AND snapshot_date < COALESCE(
                  CAST(:prev_bound AS date),
                  date_trunc('month', CAST(:curr AS date))
                  - make_interval(months => CAST(:months AS integer) - 1)
              )
          AND snapshot_date >= :floor
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"fid": fund_id, "curr": current_date, "months": months,
           "prev_bound": prev_bound.isoformat() if prev_bound else None,
           "sources": list(MONTHLY_SOURCES), "floor": FT_HISTORY_FLOOR}).first()

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
            "range_from": range_from_norm,
            "range_to": range_to_norm,
            "available_months": available_months,
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
            END AS delta_weight,
            -- Изменение позиции в рублях — та же величина, что в «Сделках»
            -- Общего портфеля (там total_delta_amount): вход/выход целиком,
            -- иначе разница объёмов. amount_rub есть у обоих MONTHLY_SOURCES
            -- (vim_sdr, interfax_manual), поэтому NULL тут — редкий край.
            CASE
                WHEN prev.mkey IS NULL THEN curr.amount_rub
                WHEN curr.mkey IS NULL THEN -prev.amount_rub
                ELSE curr.amount_rub - prev.amount_rub
            END AS delta_amount
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
            "delta_amount_rub": float(r["delta_amount"]) if r["delta_amount"] is not None else None,
            "current_amount_rub": float(r["curr_amount"]) if r["curr_amount"] is not None else None,
            "previous_amount_rub": float(r["prev_amount"]) if r["prev_amount"] is not None else None,
        })

    return {
        "fund": dict(fund_row),
        "period": period,
        "range_from": range_from_norm,
        "range_to": range_to_norm,
        "available_months": available_months,
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
    range_from: str | None = Query(
        None, alias="from",
        description="YYYY-MM(-DD) — БАЗОВЫЙ месяц произвольного диапазона; задаётся вместе с `to` и отменяет period/as_of",
    ),
    range_to: str | None = Query(
        None, alias="to",
        description="YYYY-MM(-DD) — ЦЕЛЕВОЙ месяц произвольного диапазона (сравнение from → to)",
    ),
    manager: str | None = Query(None, description="фильтр по УК: comma-separated uk_id (напр. '34,5,3597'); один id тоже ок; пусто=все"),
    funds: str | None = Query(None, description="фильтр по конкретным фондам: comma-separated тикеры (напр. 'TMOS,SBMX'); приоритет над manager; пусто=все"),
    sort: str = Query("weight", description="weight | amount — метрика ранжирования и знака"),
    limit: int = Query(20, ge=1, le=100),
    scope: str = Query(
        "movers",
        description="movers | portfolio — какой раздел спрашивает; выбирает КЛЮЧ задержки в матрице (сам размер задержки решает бэкенд)",
    ),
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

    # Произвольный диапазон (from → to) вместо пресета «N месяцев назад от свежего».
    # Семантика та же, что у пресетов: сравниваем снапшот месяца `from` (база) со
    # снапшотом месяца `to` (цель) — ровно то, что показывает подпись «апр – май».
    # prev_bound = начало месяца, следующего за `from`: последний полный снапшот
    # СТРОГО раньше него = снапшот месяца `from` (или ближайший более ранний, если
    # у фонда снапшота этого месяца нет — как и в пресетах).
    prev_bound: date | None = None
    range_from_norm: str | None = None
    range_to_norm: str | None = None
    # Произвольный диапазон — с Basic (матрица, fund_trades.custom_range); тот же
    # гейт в /fund/{ticker}. Гасим оба параметра → тир считает по пресету period.
    if not get_indicator_limits(user_tier(user), "fund_trades").get("custom_range", True):
        range_from = None
        range_to = None
    if bool(range_from) != bool(range_to):
        raise HTTPException(status_code=400, detail="from и to задаются только вместе")
    if range_from and range_to:
        _f = _month_start(range_from, "from")
        _t = _month_start(range_to, "to")
        if _f >= _t:
            raise HTTPException(status_code=400, detail="from must be an earlier month than to")
        if _f < FT_HISTORY_FLOOR:
            raise HTTPException(status_code=400, detail=f"from earlier than {FT_HISTORY_FLOOR.isoformat()} is not available")
        prev_bound = _next_month(_f)
        # as_of = КОНЕЦ целевого месяца: якорь режет снапшоты по `<= as_of`, с первым
        # числом месяца целевой month-end снапшот отсекся бы и консенсус вышел пустым.
        as_of = (_next_month(_t) - timedelta(days=1)).isoformat()
        range_from_norm, range_to_norm = _f.isoformat(), _t.isoformat()

    # Free/гость — задержка: целевой месяц-снапшот ограничиваем до cutoff (свежий
    # срез «что купили» — по подписке). ISO-даты сравниваются как строки = хронологически.
    # scope=portfolio (панель сделок на вкладке «Общий портфель») читает отдельный
    # ключ матрицы, у которого задержка нулевая. Фронт лишь называет раздел —
    # СКОЛЬКО резать, по-прежнему решает матрица на бэке; при возврате гейта
    # достаточно поправить features.py.
    _cut = _snapshot_cutoff(db, user, PORTFOLIO_DELAY_KEY if scope == "portfolio" else "snapshot_delay")
    if _cut != _FAR_FUTURE:
        _cs = _cut.isoformat()
        as_of = _cs if not as_of else min(as_of, _cs)
    # Фича показывает ТОЛЬКО акции — облигации/деньги/золото скрыты целиком.
    # Параметр category игнорируется (всегда stocks).
    category_filter = "AND f.category = 'stocks'"
    whitelist_filter = "AND f.ticker = ANY(:tickers)"

    # Своя подвыборка фондов — с Basic (матрица, fund_trades.fund_picker).
    # Гасим ОБА параметра: manager — тот же выбор пула, только через УК, и
    # оставленный открытым он был бы дырой в гейте. Фронт запирает таблетку
    # пикера замком, но URL правится руками — ограничение живёт на бэке.
    # Вместо пула юзера — продуктовый дефолт «без индексных» (2026-08-10).
    if not get_indicator_limits(user_tier(user), "fund_trades").get("fund_picker", True):
        funds = _default_nonindex_funds(db)
        manager = None

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
        "wmin": FT_COMPLETE_WSUM_MIN,
        "floor": FT_HISTORY_FLOOR,
        # NULL для пресетов → prev считается арифметикой месяцев (COALESCE в SQL).
        "prev_bound": prev_bound.isoformat() if prev_bound else None,
    }
    if fund_tickers:
        params["fund_tickers"] = fund_tickers
    elif manager_ids:
        params["managers"] = manager_ids

    # Для каждого фонда: latest snapshot + snapshot N дней назад.
    # Diff weight per (fund, asset) → агрегируем по asset.
    # Это complex CTE но даёт точную картину.
    rows = db.execute(text(f"""
        WITH snap_ok AS (
            -- Только ПОЛНЫЕ снапшоты (Σ weight не ниже FT_COMPLETE_WSUM_MIN) НЕ РАНЬШЕ
            -- исторической границы (FT_HISTORY_FLOOR): дифф против частичного импорта
            -- SCHA рисует фантомные сделки, а самые старые отчёты (2021-2022) — самые
            -- дырявые. Диффы против снапшотов старше границы не строим вовсе.
            SELECT fund_id, snapshot_date
            FROM fund_holdings_history
            WHERE source = ANY(:sources)
              AND snapshot_date >= :floor
            GROUP BY fund_id, snapshot_date
            HAVING COALESCE(SUM(weight), 0) >= :wmin
        ),
        anchor AS (
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
            JOIN snap_ok so ON so.fund_id = h.fund_id AND so.snapshot_date = h.snapshot_date
            WHERE 1=1 {category_filter} {whitelist_filter} {manager_filter}
              AND (CAST(:as_of AS date) IS NULL OR h.snapshot_date <= CAST(:as_of AS date))
        ),
        fund_dates AS (
            -- curr = снапшот target-месяца (нет → фонд выпадает из консенсуса);
            -- prev = снапшот, выровненный по МЕСЯЦУ на N месяцев назад от target,
            -- либо (произвольный диапазон) снапшот месяца from по границе prev_bound.
            -- NB: имена бинд-параметров в комментариях писать БЕЗ двоеточия —
            -- text() подставит %s и внутри комментария, а Postgres его не увидит:
            -- «bind message supplies N parameters, but prepared statement requires N-1».
            -- Оба только из snap_ok (полные).
            SELECT
                f.fund_id,
                f.ticker,
                f.name AS fund_name,
                MAX(h.snapshot_date) FILTER (
                    WHERE date_trunc('month', h.snapshot_date) = a.target_month
                ) AS curr_date,
                (
                    SELECT MAX(so2.snapshot_date)
                    FROM snap_ok so2
                    WHERE so2.fund_id = f.fund_id
                      AND so2.snapshot_date < COALESCE(
                              CAST(:prev_bound AS date),
                              a.target_month - make_interval(months => CAST(:months AS integer) - 1))
                ) AS prev_date
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            JOIN snap_ok so ON so.fund_id = h.fund_id AND so.snapshot_date = h.snapshot_date
            CROSS JOIN anchor a
            WHERE 1=1 {category_filter} {whitelist_filter} {manager_filter}
              AND (CAST(:as_of AS date) IS NULL OR h.snapshot_date <= CAST(:as_of AS date))
            GROUP BY f.fund_id, f.ticker, f.name, a.target_month
            HAVING MAX(h.snapshot_date) FILTER (
                WHERE date_trunc('month', h.snapshot_date) = a.target_month
            ) IS NOT NULL
        ),
        curr_h AS (
            -- Холдинги curr-снапшота каждого фонда. Ключ akey = КАНОНИЧЕСКИЙ
            -- ISIN (securities_ref.canonical_isin), fallback сырой ISIN, fallback
            -- имя — как в /assets и /company-flows. Без канонизации редомициль-
            -- пара (АДР US69269L1044 → акция RU000A10CW95, Озон окт-2025) на
            -- годовом окне давала ДВЕ строки «Озон»: −5.8 млрд в продажах (АДР
            -- sold_out) и +6.4 млрд в покупках (акция new) вместо нетто +0.6.
            -- GROUP BY: АДР и акция могут сосуществовать в одном снапшоте на
            -- стыке конвертации — суммируем, иначе FULL JOIN размножит строки.
            SELECT fd.fund_id,
                   COALESCE(sr.canonical_isin, NULLIF(h.isin, ''), h.asset_name) AS akey,
                   MIN(h.asset_name) AS asset_name,
                   SUM(h.weight) AS weight,
                   SUM(h.amount_rub) AS amount_rub
            FROM fund_dates fd
            JOIN fund_holdings_history h
              ON h.fund_id = fd.fund_id AND h.snapshot_date = fd.curr_date
             AND h.source = ANY(:sources)
            LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE fd.prev_date IS NOT NULL
            GROUP BY fd.fund_id, COALESCE(sr.canonical_isin, NULLIF(h.isin, ''), h.asset_name)
        ),
        prev_h AS (
            SELECT fd.fund_id,
                   COALESCE(sr.canonical_isin, NULLIF(h.isin, ''), h.asset_name) AS akey,
                   MIN(h.asset_name) AS asset_name,
                   SUM(h.weight) AS weight,
                   SUM(h.amount_rub) AS amount_rub
            FROM fund_dates fd
            JOIN fund_holdings_history h
              ON h.fund_id = fd.fund_id AND h.snapshot_date = fd.prev_date
             AND h.source = ANY(:sources)
            LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE fd.prev_date IS NOT NULL
            GROUP BY fd.fund_id, COALESCE(sr.canonical_isin, NULLIF(h.isin, ''), h.asset_name)
        ),
        per_fund_diff AS (
            -- Дельта per (fund, asset): FULL OUTER JOIN по (fund_id, akey), обе
            -- стороны сохраняются — new (только curr) И sold_out (только prev).
            -- Старая форма (fund_dates LEFT JOIN curr + FULL OUTER JOIN prev с
            -- WHERE fd.prev_date IS NOT NULL) теряла sold_out целиком: непарные
            -- prev-строки NULL-расширялись по fd и вырезались WHERE — полные
            -- ликвидации позиций не попадали в «Чистые продажи» (кейс: Алёнка
            -- слила АФК Система −112 млн ₽ за июнь-2026, топ-продажа невидима).
            SELECT
                COALESCE(c.fund_id, p.fund_id) AS fund_id,
                COALESCE(c.akey, p.akey) AS akey,
                COALESCE(c.asset_name, p.asset_name) AS asset_name,
                COALESCE(c.weight, 0) - COALESCE(p.weight, 0) AS delta_weight,
                COALESCE(c.amount_rub, 0) - COALESCE(p.amount_rub, 0) AS delta_amount
            FROM curr_h c
            FULL OUTER JOIN prev_h p
                ON p.fund_id = c.fund_id AND p.akey = c.akey
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

    # Доступные месяцы для month-picker и календаря периода: один пункт на
    # КАЛЕНДАРНЫЙ месяц. У разных УК разные дни конца месяца (27/28/30/31) →
    # схлопываем по месяцу и берём MAX-дату месяца как as_of (каждый фонд внутри
    # возьмёт свой <= неё).
    #
    # Список считаем по ВЫБРАННОМУ набору (те же category/whitelist/manager
    # фильтры, что и у консенсуса) и только по ПОЛНЫМ снапшотам: иначе календарь
    # предлагал месяцы, которых у выбранных УК нет, — клик по такому месяцу давал
    # пустой результат («нет данных за месяц»).
    available_month_dates = db.execute(text(f"""
        WITH snap_ok AS (
            -- Полные снапшоты не раньше границы — тот же набор, что у консенсуса.
            SELECT fund_id, snapshot_date
            FROM fund_holdings_history
            WHERE source = ANY(:sources)
              AND snapshot_date >= :floor
            GROUP BY fund_id, snapshot_date
            HAVING COALESCE(SUM(weight), 0) >= :wmin
        )
        SELECT MAX(h.snapshot_date) AS d
        FROM fund_holdings_history h
        JOIN funds f ON f.fund_id = h.fund_id
        JOIN snap_ok so ON so.fund_id = h.fund_id AND so.snapshot_date = h.snapshot_date
        WHERE h.source = ANY(:sources) {category_filter} {whitelist_filter} {manager_filter}
        GROUP BY date_trunc('month', h.snapshot_date)
        ORDER BY d DESC
    """), params).scalars().all()

    # resolved_month = фактический target-месяц консенсуса (выбранный as_of, иначе последний
    # доступный для набора); funds_in_month = сколько фондов набора РЕАЛЬНО имеют снапшот
    # этого месяца. Фронт по funds_in_month==0 показывает «нет данных за месяц для выбранных
    # фондов» вместо «нет движений» (когда выбран месяц, которого у фондов ещё нет).
    meta_row = db.execute(text(f"""
        WITH snap_ok AS (
            -- Полные снапшоты не раньше границы (см. FT_COMPLETE_WSUM_MIN / FT_HISTORY_FLOOR)
            -- — консистентно с rows.
            SELECT fund_id, snapshot_date
            FROM fund_holdings_history
            WHERE source = ANY(:sources)
              AND snapshot_date >= :floor
            GROUP BY fund_id, snapshot_date
            HAVING COALESCE(SUM(weight), 0) >= :wmin
        ),
        anchor AS (
            SELECT COALESCE(
                       date_trunc('month', CAST(:as_of AS date)),
                       date_trunc('month', MAX(h.snapshot_date))
                   ) AS target_month
            FROM funds f
            JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
            JOIN snap_ok so ON so.fund_id = h.fund_id AND so.snapshot_date = h.snapshot_date
            WHERE 1=1 {category_filter} {whitelist_filter} {manager_filter}
              AND (CAST(:as_of AS date) IS NULL OR h.snapshot_date <= CAST(:as_of AS date))
        )
        SELECT
            a.target_month::date AS resolved_month,
            (SELECT COUNT(DISTINCT f.fund_id)
             FROM funds f
             JOIN fund_holdings_history h ON h.fund_id = f.fund_id AND h.source = ANY(:sources)
             JOIN snap_ok so ON so.fund_id = h.fund_id AND so.snapshot_date = h.snapshot_date
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
        # Границы произвольного диапазона (нормализованные к первому числу месяца);
        # null = период задан пресетом.
        "range_from": range_from_norm,
        "range_to": range_to_norm,
        "resolved_month": resolved_month,
        "funds_in_month": funds_in_month,
        "manager": manager,
        "funds": funds,
        "sort": sort,
        "available_months": [m.isoformat() for m in available_month_dates],
        # Free/гость — дата-отсечка: месяцы с макс-датой > cutoff заблокированы (свежий срез
        # «что купили» по подписке). null для платных (без задержки). Фронт помечает locked.
        "snapshot_cutoff": (None if _cut == _FAR_FUTURE else _cut.isoformat()),
        "top_accumulated": top_accumulated,
        "top_reduced": top_reduced,
    }


@router.get("/portfolio")
def combined_portfolio(
    manager: str | None = Query(None, description="фильтр по УК: comma-separated uk_id; пусто=все"),
    funds: str | None = Query(None, description="фильтр по конкретным фондам: comma-separated тикеры; приоритет над manager; пусто=все"),
    as_of: str | None = Query(None, description="YYYY-MM-DD — целевой месяц-снапшот (default=последний); каждый фонд берёт свой снапшот <= этой даты"),
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Общий портфель — все выбранные фонды акций слиты в ОДИН портфель, «как будто
    ими управляет один управляющий».

    Для каждой бумаги берём ОТЧЁТНУЮ рублёвую стоимость позиции amount_rub из
    последней Справки о СЧА (fallback nav × доля, если суммы нет) и суммируем across
    выбранных фондов. Это те же цифры, что в пикере «Потоки по компании» и во вкладке
    «Сделки фондов» — портфель НЕ переоценивается на сегодняшнюю СЧА. Размер портфеля
    = total_value_rub (Σ amount_rub, «Объём в фондах»). Отдельную «Суммарную СЧА» НЕ
    показываем: nav (Cbonds) и amount_rub (SCHA) из разных источников и не всегда
    сходятся, из-за чего СЧА могла бы оказаться < стоимости акций (ложный «баг»).
    Срез САМЫЙ СВЕЖИЙ на всех тирах: с 2026-08-09 «Общий портфель» выведен
    из-под задержки раздела (матрица: fund_trades.portfolio_snapshot_delay=0),
    остальные разделы «Сделок фондов» продолжают резать по snapshot_delay.

    Отдаём ДВА веса per бумага:
      weight_rub — доля в общем портфеле ПО ДЕНЬГАМ (value-weighted; крупные фонды
                   весомее — это буквально один общий портфель);
      weight_avg — средняя доля по выбранным фондам (equal-weight; фонд не держит
                   бумагу → 0%, поэтому суммируется к ~100 по всем фондам набора).

    Плюс суммарная СЧА, суммарная стоимость акций и nav-взвешенная доходность набора.
    Фильтр фондов: funds (тикеры) приоритетнее manager (uk_id) — как в /movers; фронт
    резолвит выбранные УК в тикеры и шлёт их в `funds`.
    """
    # «Общий портфель» свежесть не режет никому (матрица: portfolio_snapshot_delay=0).
    # Вызов оставлен обобщённым — вернут гейт в матрице, ручка снова начнёт резать.
    gate_cutoff = _snapshot_cutoff(db, user, PORTFOLIO_DELAY_KEY)
    # Эффективная граница выбора снапшота = min(выбранный месяц as_of, gate_cutoff).
    # as_of двигает портфель на исторический месяц; gate_cutoff не даёт Free/гостю
    # заглянуть в свежий срез. Каждый фонд внутри берёт свой снапшот <= bound.
    bound = gate_cutoff
    if as_of:
        try:
            as_of_d = date.fromisoformat(as_of)
        except ValueError:
            raise HTTPException(status_code=400, detail="as_of must be YYYY-MM-DD")
        bound = min(gate_cutoff, as_of_d)
    cutoff = bound  # ниже SQL выбирает снапшоты <= :cutoff

    # Своя подвыборка фондов — с Basic (матрица, fund_trades.fund_picker); см.
    # тот же гейт в /movers. Гасим и manager: это тот же выбор пула, но через УК.
    # Вместо пула юзера — продуктовый дефолт «без индексных» (2026-08-10).
    if not get_indicator_limits(user_tier(user), "fund_trades").get("fund_picker", True):
        funds = _default_nonindex_funds(db)
        manager = None

    # funds (тикеры фондов) приоритетнее manager (uk_id). Пусто = все whitelist-акции.
    fund_tickers = [p.strip() for p in funds.split(",")] if funds else []
    fund_tickers = [p for p in fund_tickers if p]
    manager_ids: list[int] = []
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
    if fund_tickers:
        manager_filter = "AND f.ticker = ANY(:fund_tickers)"
    elif manager_ids:
        manager_filter = "AND f.uk_id = ANY(:managers)"
    else:
        manager_filter = ""

    params = {
        "tickers": list(WHITELIST_TICKERS),
        "sources": list(MONTHLY_SOURCES),
        "cutoff": cutoff,
    }
    if fund_tickers:
        params["fund_tickers"] = fund_tickers
    elif manager_ids:
        params["managers"] = manager_ids

    # ── Дефолтный месяц среза: САМЫЙ ПОЛНЫЙ, а не просто последний ──────────────
    # Отчёты УК приходят вразнобой (обычно к 20-м числам, часть — позже), поэтому
    # у свежего месяца нередко 2-3 состава из 19: портфель на нём почти пустой.
    # Без явного as_of открываем месяц, где состав опубликовало больше всего фондов
    # НАБОРА; при равенстве берём свежий. Окно поиска — последние 12 месяцев с
    # данными, иначе набор мог бы уехать в старый год, где фондов было больше.
    if not as_of:
        best_month = db.execute(text(f"""
            WITH cov AS (
                SELECT date_trunc('month', h.snapshot_date)::date AS m,
                       COUNT(DISTINCT h.fund_id) AS n
                FROM fund_holdings_history h
                JOIN funds f ON f.fund_id = h.fund_id
                WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks' {manager_filter}
                  AND h.source = ANY(:sources)
                  AND h.snapshot_date >= :floor
                  AND h.snapshot_date <= :cutoff
                GROUP BY 1
            )
            SELECT m FROM cov
            WHERE m >= (SELECT MAX(m) FROM cov) - INTERVAL '11 months'
            ORDER BY n DESC, m DESC
            LIMIT 1
        """), {**params, "floor": FT_HISTORY_FLOOR}).scalar()
        if best_month:
            bound = min(bound, _next_month(best_month) - timedelta(days=1))
            cutoff = bound
            params["cutoff"] = cutoff

    # ── Выбранные фонды: nav (полная СЧА) + доходность пая + дата снапшота ──
    fund_rows = db.execute(text(f"""
        SELECT
            f.fund_id, f.ticker, f.name, f.uk, f.uk_id,
            (SELECT MAX(h.snapshot_date) FROM fund_holdings_history h
             WHERE h.fund_id = f.fund_id AND h.source = ANY(:sources)
               AND h.snapshot_date <= :cutoff) AS snap_date,
            fd_last.nav AS nav_rub,
            fd_last.pay AS last_pay,
            fd_last.td AS last_pay_date,
            fd_1m.pay AS pay_1m, fd_3m.pay AS pay_3m, fd_6m.pay AS pay_6m, fd_1y.pay AS pay_1y,
            fd_3y.pay AS pay_3y,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '1 month'   AND d.record_date <= fd_last.td) AS dist_1m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '3 months'  AND d.record_date <= fd_last.td) AS dist_3m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '6 months'  AND d.record_date <= fd_last.td) AS dist_6m,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '12 months' AND d.record_date <= fd_last.td) AS dist_1y,
            (SELECT COALESCE(SUM(amount_per_unit), 0) FROM fund_distributions d
             WHERE d.fund_id = f.fund_id AND d.record_date > fd_last.td - INTERVAL '36 months' AND d.record_date <= fd_last.td) AS dist_3y
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
        -- 3 года: фонды моложе трёх лет вернут NULL и в средневзвешенную не попадут.
        LEFT JOIN LATERAL (
            SELECT pay FROM fund_data WHERE fund_id = f.fund_id AND pay IS NOT NULL
            AND trade_date <= fd_last.td - INTERVAL '36 months' ORDER BY trade_date DESC LIMIT 1
        ) fd_3y ON true
        WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks' {manager_filter}
          AND EXISTS (SELECT 1 FROM fund_holdings_history h2
                      WHERE h2.fund_id = f.fund_id AND h2.source = ANY(:sources)
                        AND h2.snapshot_date <= :cutoff)
        ORDER BY fd_last.nav DESC NULLS LAST
    """), params).mappings().all()

    # Месяц среза = месяц самого свежего снапшота набора (не позже bound). Фонд,
    # не опубликовавший состав за этот месяц, в портфель НЕ включается: подстановка
    # его прошлого состава давала бы «июньский» портфель, наполовину собранный из
    # майских данных, под свежей подписью месяца. Так же устроен консенсус /movers
    # (там фонд без снапшота target-месяца выпадает из дельт). Такие фонды
    # возвращаем отдельным списком excluded_funds — фронт показывает «11 из 19».
    _snaps = [r["snap_date"] for r in fund_rows if r["snap_date"]]
    target_month = max(_snaps).replace(day=1) if _snaps else None

    included_funds = []
    excluded_funds = []
    total_nav = 0.0
    for r in fund_rows:
        snap = r["snap_date"]
        if target_month is None or snap is None or snap.replace(day=1) != target_month:
            excluded_funds.append({
                "ticker": r["ticker"], "name": r["name"], "uk": r["uk"], "uk_id": r["uk_id"],
                "nav_rub": float(r["nav_rub"]) if r["nav_rub"] is not None else None,
                "snapshot_date": snap.isoformat() if snap else None,
            })
            continue
        nav = float(r["nav_rub"]) if r["nav_rub"] is not None else None
        if nav:
            total_nav += nav
        last_pay = r["last_pay"]
        rets = _guard_returns(r["ticker"], {
            "m1": _calc_total_return(last_pay, r["pay_1m"], r["dist_1m"]),
            "m3": _calc_total_return(last_pay, r["pay_3m"], r["dist_3m"]),
            "m6": _calc_total_return(last_pay, r["pay_6m"], r["dist_6m"]),
            "y1": _calc_total_return(last_pay, r["pay_1y"], r["dist_1y"]),
            "y3": _calc_total_return(last_pay, r["pay_3y"], r["dist_3y"]),
        })
        included_funds.append({
            "ticker": r["ticker"], "name": r["name"], "uk": r["uk"], "uk_id": r["uk_id"],
            "nav_rub": nav,
            "snapshot_date": r["snap_date"].isoformat() if r["snap_date"] else None,
            "_returns": rets,
            "_last_pay_date": r["last_pay_date"],
        })

    num_funds = len(included_funds)

    # nav-взвешенная доходность портфеля per период (фонды без данных за период не
    # тянут вес — исключаются из числителя И знаменателя).
    def _wavg(key: str):
        num = den = 0.0
        for fnd in included_funds:
            v = fnd["_returns"].get(key)
            nav = fnd["nav_rub"]
            if v is not None and nav:
                num += nav * v
                den += nav
        return round(num / den, 2) if den > 0 else None

    # y3 — накопленная (не годовая) доходность за 3 года; фонды моложе трёх лет
    # в неё не входят, поэтому она считается по подмножеству набора.
    portfolio_returns = {k: _wavg(k) for k in ("m1", "m3", "m6", "y1", "y3")}

    # Дата, на которую актуальна доходность портфеля: САМАЯ СТАРАЯ из последних
    # дат цены пая (fund_data) среди фондов, вошедших в портфель. Не самая свежая —
    # именно отстающий фонд ограничивает, на какую дату взвешенная доходность
    # честна для всего набора (после этой даты не все фонды учтены равно).
    _pay_dates = [f["_last_pay_date"] for f in included_funds if f["_last_pay_date"]]
    returns_as_of = min(_pay_dates).isoformat() if _pay_dates else None
    for fnd in included_funds:
        fnd.pop("_last_pay_date", None)

    # ── Агрегированные холдинги: суммируем рублёвую стоимость per бумага (ISIN-ключ) ──
    # target_month = None (снапшотов нет вовсе) → HAVING по NULL не пропустит ни один
    # фонд, holdings придут пустыми — как и included_funds.
    params["target_month"] = target_month
    hold_rows = db.execute(text(f"""
        WITH sel AS (
            SELECT f.fund_id FROM funds f
            WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks' {manager_filter}
        ),
        last_snap AS (
            -- HAVING: фонд участвует, только если его последний снапшот (<= cutoff)
            -- принадлежит месяцу среза. Отставшие УК выпадают целиком, а не тянут
            -- в портфель прошлый состав (см. target_month выше).
            SELECT h.fund_id, MAX(h.snapshot_date) AS d
            FROM fund_holdings_history h JOIN sel ON sel.fund_id = h.fund_id
            WHERE h.source = ANY(:sources) AND h.snapshot_date <= :cutoff
            GROUP BY h.fund_id
            HAVING date_trunc('month', MAX(h.snapshot_date))
                   = date_trunc('month', CAST(:target_month AS date))
        ),
        fund_nav AS (
            SELECT sel.fund_id, fd.nav
            FROM sel
            LEFT JOIN LATERAL (
                SELECT nav FROM fund_data WHERE fund_id = sel.fund_id AND nav IS NOT NULL
                ORDER BY trade_date DESC LIMIT 1
            ) fd ON true
        ),
        holdings AS (
            SELECT h.fund_id,
                   COALESCE(NULLIF(h.isin, ''), h.asset_name) AS akey,
                   h.asset_name, h.isin, h.weight,
                   COALESCE(h.amount_rub, fn.nav * h.weight / 100.0) AS value_rub
            FROM last_snap ls
            JOIN fund_holdings_history h
                 ON h.fund_id = ls.fund_id AND h.snapshot_date = ls.d AND h.source = ANY(:sources)
            JOIN fund_nav fn ON fn.fund_id = h.fund_id
        ),
        names AS (
            SELECT h.isin, COALESCE(MAX(sr.short_name),
                   (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1]) AS short_name
            FROM fund_holdings_history h LEFT JOIN securities_ref sr ON sr.isin = h.isin
            WHERE COALESCE(h.isin, '') <> '' GROUP BY h.isin
        )
        SELECT hd.akey,
               COALESCE(MAX(n.short_name),
                        (array_agg(hd.asset_name ORDER BY length(hd.asset_name), hd.asset_name))[1]) AS asset_name,
               MAX(NULLIF(hd.isin, '')) AS isin,
               SUM(hd.value_rub) AS value_rub,
               SUM(hd.weight) AS sum_weight,
               COUNT(DISTINCT hd.fund_id) AS funds_holding
        FROM holdings hd
        LEFT JOIN names n ON n.isin = hd.akey
        GROUP BY hd.akey
        ORDER BY value_rub DESC NULLS LAST
    """), params).mappings().all()

    total_value = sum(float(r["value_rub"]) for r in hold_rows if r["value_rub"] is not None)
    holdings = []
    for r in hold_rows:
        value_rub = float(r["value_rub"]) if r["value_rub"] is not None else 0.0
        sum_weight = float(r["sum_weight"]) if r["sum_weight"] is not None else 0.0
        holdings.append({
            "akey": r["akey"],
            "asset_name": r["asset_name"],
            "isin": r["isin"],
            "value_rub": value_rub,
            "weight_rub": round(value_rub / total_value * 100, 4) if total_value > 0 else 0.0,
            "weight_avg": round(sum_weight / num_funds, 4) if num_funds > 0 else 0.0,
            "funds_holding": int(r["funds_holding"]),
        })

    for fnd in included_funds:
        fnd.pop("_returns", None)

    # Доступные месяцы для month-picker: один пункт на КАЛЕНДАРНЫЙ месяц (у разных
    # УК разные дни конца месяца → схлопываем и берём MAX-дату месяца как as_of).
    # Список — по ВЫБРАННОМУ набору УК/фондов (как в /movers): месяцы, которых у
    # выбранных УК нет, в пикере не показываем — раньше союз по всему whitelist
    # предлагал месяцы, где набор пуст.
    available_month_dates = db.execute(text(f"""
        SELECT MAX(h.snapshot_date) AS d
        FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id
        WHERE f.category = 'stocks' AND f.ticker = ANY(:tickers) AND h.source = ANY(:sources)
          AND h.snapshot_date >= :floor {manager_filter}
        GROUP BY date_trunc('month', h.snapshot_date)
        ORDER BY d DESC
    """), {**params, "floor": FT_HISTORY_FLOOR}).scalars().all()

    # resolved_month — фактический месяц среза набора: самый свежий снапшот выбранных
    # фондов, не позже bound. Именно его показывает пилюля актуальности данных.
    resolved_month = None
    snap_dates = [f["snapshot_date"] for f in included_funds if f["snapshot_date"]]
    if snap_dates:
        resolved_month = max(snap_dates)

    return {
        "num_funds": num_funds,
        # Сколько фондов набора вообще имеют снапшот <= bound (включая отставшие).
        # num_funds < funds_total → срез неполный, фронт помечает «11 из 19».
        "funds_total": num_funds + len(excluded_funds),
        "num_assets": len(holdings),
        "total_value_rub": total_value,
        "total_nav_rub": total_nav,
        "returns": portfolio_returns,
        # Самая старая из дат последней цены пая среди фондов портфеля — на неё
        # реально честна взвешенная доходность (см. комментарий у returns_as_of выше).
        "returns_as_of": returns_as_of,
        "as_of": as_of,
        "resolved_month": resolved_month,
        # Фонды, не опубликовавшие состав за месяц среза (в портфель НЕ включены).
        "excluded_funds": excluded_funds,
        "available_months": [m.isoformat() for m in available_month_dates],
        # Free/гость — дата-отсечка гейтинга (НЕ as_of): месяцы > неё заблокированы.
        "snapshot_cutoff": (None if gate_cutoff == _FAR_FUTURE else gate_cutoff.isoformat()),
        "funds": included_funds,
        "holdings": holdings,
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
    cutoff = _snapshot_cutoff(db, user, SHOWCASE_DELAY_KEY)  # drill-down витрины — без задержки

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
              AND h.snapshot_date <= :cutoff
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
        "cutoff": cutoff,
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
    # Free/гость: свежие срезы (> cutoff) НЕ прячем из списка, а помечаем locked —
    # дата видна (навигатор покажет «29 мая 🔒»), а холдинги/цифры — по подписке
    # (snapshot_review на locked-дату вернёт маркер без данных). Цифры не уходят.
    cutoff = _snapshot_cutoff(db, user)
    rows = db.execute(text("""
        SELECT snapshot_date, COUNT(*) AS asset_count
        FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
          AND snapshot_date >= :floor
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES), "floor": FT_HISTORY_FLOOR}).mappings().all()

    return {
        "ticker": fund_row[1],
        "fund_name": fund_row[2],
        "snapshots": [
            {
                "snapshot_date": r["snapshot_date"].isoformat(),
                "asset_count": r["asset_count"],
                "locked": r["snapshot_date"] > cutoff,
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
    from datetime import date as _date

    # Free/гость — задержка: свежие срезы (> cutoff) ЗАБЛОКИРОВАНЫ (по подписке).
    cutoff = _snapshot_cutoff(db, user)
    # Реальная последняя дата — только МЕТАДАННЫЕ (для метки «свежий срез — по подписке»).
    latest_all = db.execute(text("""
        SELECT MAX(snapshot_date) FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES)}).scalar()
    if latest_all is None:
        raise HTTPException(status_code=404, detail="No snapshots for this fund")

    # Резолвим current date. Default (без ?date) = последний ДОСТУПНЫЙ тиру срез:
    # платный = latest_all; Free = последний <= cutoff (свежий показываем locked, не данными).
    if date:
        try:
            current_date = _date.fromisoformat(date)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="date must be ISO format")
        # ?date= — прямой параметр API, а не выбор из (уже отфильтрованного) навигатора
        # /snapshots — старые снапшоты могут реально существовать в БД (см. FT_HISTORY_FLOOR).
        if current_date < FT_HISTORY_FLOOR:
            raise HTTPException(status_code=404, detail=f"No snapshots before {FT_HISTORY_FLOOR.isoformat()}")
    elif latest_all > cutoff:
        current_date = db.execute(text("""
            SELECT MAX(snapshot_date) FROM fund_holdings_history
            WHERE fund_id = :fid AND source = ANY(:sources) AND snapshot_date <= :cutoff
        """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES), "cutoff": cutoff}).scalar()
    else:
        current_date = latest_all

    # Запрошен свежий (locked) срез → маркер БЕЗ холдингов (пейволл: цифры НЕ уходят на фронт).
    if current_date is not None and current_date > cutoff:
        return {
            "fund": {"ticker": fund_row[1], "name": fund_row[2], "category": fund_row[3]},
            "current_snapshot_date": current_date.isoformat(),
            "previous_snapshot_date": None,
            "locked": True,
            "required_tier": "basic",
            "latest_snapshot_date": latest_all.isoformat(),
            "current_holdings": [], "added": [], "reduced": [], "new": [], "sold_out": [],
            "totals": None,
        }
    if current_date is None:
        raise HTTPException(status_code=404, detail="No snapshots for this fund")

    # Резолвим previous date = ближайший snapshot до current_date.
    prev_row = db.execute(text("""
        SELECT snapshot_date FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources)
          AND snapshot_date < :curr AND snapshot_date >= :floor
        ORDER BY snapshot_date DESC
        LIMIT 1
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES), "curr": current_date,
           "floor": FT_HISTORY_FLOOR}).first()
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

    cutoff = _snapshot_cutoff(db, user, SHOWCASE_DELAY_KEY)  # drill-down карточки фонда — без задержки
    rows = db.execute(text(f"""
        SELECT snapshot_date, asset_name, isin, positions, amount_rub, weight
        FROM fund_holdings_history
        WHERE fund_id = :fid AND source = ANY(:sources) {filter_sql}
          AND snapshot_date <= :cutoff AND snapshot_date >= :floor
        ORDER BY snapshot_date ASC
    """), {"fid": fund_id, "sources": list(MONTHLY_SOURCES), "cutoff": cutoff,
           "floor": FT_HISTORY_FLOOR, **filter_params}).mappings().all()

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
      - gross_flow_3y = оборот покупок/продаж за 3 года (для фильтра релевантности).
    Сортировка: funds_count DESC, last_amount_rub DESC. Имя — короткое.

    Фильтр релевантности (CF_MIN_GROSS_FLOW_3Y_RUB): прячем бумаги с оборотом
    покупок/продаж ≤ порога за последние 3 года. Окно скользящее, расчёт на каждый
    запрос ⇒ набор авто-ревизуется после каждого месячного снапшота (см. константу).
    """
    rows = db.execute(text("""
        WITH scoped AS (
            -- mkey по КАНОНИЧЕСКОМУ isin: редомициль-пары (старый ГДР + новая
            -- локальная акция) сливаются в одну строку пикера (canonical_isin).
            SELECT h.fund_id, h.snapshot_date, h.asset_name, h.amount_rub, h.weight,
                   h.positions, NULLIF(h.isin, '') AS isin,
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
        ),
        flow_seq AS (
            -- Оборот покупок/продаж за последние 3 года: Δпозиций×цена по модулю
            -- (как в /company-flows). Дельты считаем ОТДЕЛЬНО по каждому фонду и
            -- raw-ISIN (редомициль-пары не вычитаем друг из друга), окно скользящее
            -- ⇒ фильтр пересчитывается на каждый запрос (авто-ревизия по снапшотам).
            SELECT s.mkey, s.positions, s.amount_rub,
                   LAG(s.positions)  OVER w AS pp,
                   LAG(s.amount_rub) OVER w AS pa
            FROM scoped s
            WHERE s.snapshot_date >= (CURRENT_DATE - INTERVAL '3 years')
            WINDOW w AS (
                PARTITION BY s.fund_id, COALESCE(s.isin, s.asset_name)
                ORDER BY s.snapshot_date
            )
        ),
        flow_agg AS (
            SELECT mkey, SUM(
                CASE
                    WHEN pp IS NULL AND pa IS NULL THEN 0  -- первый снапшот линии → нет базы
                    WHEN positions IS NOT NULL AND pp IS NOT NULL AND positions > 0 AND amount_rub IS NOT NULL
                        THEN ABS((positions - pp) * (amount_rub / positions))
                    WHEN amount_rub IS NOT NULL AND pa IS NOT NULL
                        THEN ABS(amount_rub - pa)
                    ELSE 0
                END
            ) AS gross3y
            FROM flow_seq GROUP BY mkey
        )
        SELECT a.mkey AS key, n.short_name AS asset_name, n.isin, n.sec_type,
               a.funds_count, a.last_amount_rub, a.avg_weight_pct,
               COALESCE(fa.gross3y, 0) AS gross_flow_3y
        FROM agg a
        JOIN names n ON n.mkey = a.mkey
        LEFT JOIN flow_agg fa ON fa.mkey = a.mkey
        -- Фильтр релевантности: прячем бумаги с оборотом ≤ порога за последние 3 года.
        WHERE COALESCE(fa.gross3y, 0) > :min_flow
        ORDER BY a.funds_count DESC, a.last_amount_rub DESC NULLS LAST, n.short_name
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES),
           "min_flow": CF_MIN_GROSS_FLOW_3Y_RUB}).mappings().all()

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
                "gross_flow_3y": float(r["gross_flow_3y"]) if r["gross_flow_3y"] is not None else None,
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
      - Бумага есть в самом ПЕРВОМ отчёте фонда → null (нет базы → не спайк).
      - Появление бумаги, когда фонд уже отчитывался без неё (новая покупка или
        повторный вход после разрыва) → приток ПОЛНОЙ стоимости позиции.
      - Исчезновение бумаги из отчёта (полная распродажа) → отток всей позиции
        в первый месяц отсутствия; база после разрыва сбрасывается — иначе
        ре-вход считался бы микро-дельтой к протухшему снапшоту многолетней
        давности, а сама распродажа не рисовалась вовсе (кейс Алёнка×АФК Система).
    Дельта split-adjusted по той же логике что snapshot_review/asset-history (для
    metric='amount' приводим prev-positions к пост-сплит масштабу; amount_rub/weight
    непрерывны через сплит, поэтому коррекция для них фактически нейтральна).

    Месячная ось = НЕПРЕРЫВНЫЙ ряд «YYYY-MM» от первого месяца бумаги до горизонта
    данных (последний снапшот по whitelist-фондам; тирной задержки тут нет —
    company_snapshot_delay=0 на всех тирах), ASC.
    Не обрезаем справа по последнему месяцу владения: полностью проданная бумага
    иначе обрывала бы график на дате распродажи. Пропуски внутри и хвост после
    распродажи → values=None → нулевые (пустые) бары.
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

    cutoff = _snapshot_cutoff(db, user, COMPANY_DELAY_KEY)  # «По бумаге» — без задержки на всех тирах
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
          AND h.snapshot_date <= :cutoff AND h.snapshot_date >= :floor
          AND {match_sql}
        ORDER BY f.fund_id, h.snapshot_date ASC
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES), "cutoff": cutoff,
           "floor": FT_HISTORY_FLOOR, **match_params}).mappings().all()

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

    # Месяцы ПОЛНЫХ снапшотов каждого фонда (не только где бумага есть) — чтобы
    # видеть исчезновение позиции: фонд отчитался, а бумаги в отчёте нет.
    # Частичные импорты SCHA (Σ weight < FT_COMPLETE_WSUM_MIN, парсер терял
    # строки) отсутствием бумаги НЕ считаются — иначе фантомный отток+ре-вход
    # на весь размер позиции (EQMX «продавал» весь Сбер на 943 млн в дек-2023).
    fund_snap_rows = db.execute(text("""
        SELECT fund_id, snapshot_date
        FROM fund_holdings_history
        WHERE fund_id = ANY(:fids) AND source = ANY(:sources)
          AND snapshot_date <= :cutoff AND snapshot_date >= :floor
        GROUP BY fund_id, snapshot_date
        HAVING COALESCE(SUM(weight), 0) >= :wmin
    """), {"fids": list(per_fund_rows.keys()), "sources": list(MONTHLY_SOURCES),
           "cutoff": cutoff, "wmin": FT_COMPLETE_WSUM_MIN, "floor": FT_HISTORY_FLOOR}).fetchall()
    fund_months_all = defaultdict(set)
    for _fid, _d in fund_snap_rows:
        fund_months_all[_fid].add(_d.strftime("%Y-%m"))

    all_months = set()
    # fund_id -> { "YYYY-MM": value|None }
    fund_month_val = {}
    for fid, frows in per_fund_rows.items():
        fund_months = sorted(fund_months_all.get(fid, ()))
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
            prev_month = None  # месяц последнего отчёта, где бумага была
            for r in irows:
                month = r["snapshot_date"].strftime("%Y-%m")
                all_months.add(month)
                curr_pos = int(r["positions"]) if r["positions"] is not None else None
                curr_amt = float(r["amount_rub"]) if r["amount_rub"] is not None else None
                curr_weight = float(r["weight"]) if r["weight"] is not None else None
                curr_price = (curr_amt / curr_pos) if (curr_amt and curr_pos and curr_pos > 0) else None

                # Разрыв владения: между прошлым и текущим появлением бумаги фонд
                # отчитывался БЕЗ неё → позицию полностью закрывали. Рисуем отток
                # всей позиции в первый «пустой» месяц и сбрасываем базу — текущее
                # появление пойдёт как новая покупка, а не как микро-дельта к
                # протухшему снапшоту (Алёнка×АФК: май-26 против июля-23 давал
                # +0.06 п.п. вместо входа на ~10% фонда).
                if prev_month is not None:
                    gap = [m for m in fund_months if prev_month < m < month]
                    if gap:
                        out_val = prev_amt if metric == "amount" else prev_weight
                        if out_val is not None:
                            gm = gap[0]
                            all_months.add(gm)
                            month_val[gm] = (month_val.get(gm) or 0.0) - out_val
                        prev_pos = None
                        prev_amt = None
                        prev_weight = None

                if prev_pos is None and prev_amt is None and prev_weight is None:
                    if prev_month is None and (not fund_months or month <= fund_months[0]):
                        # Бумага в самом первом отчёте фонда → null: не знаем,
                        # куплена она тогда или древнее нашей истории (не спайк).
                        value = None
                    else:
                        # Фонд уже отчитывался без бумаги → появление = покупка
                        # всей позиции (новая или ре-вход после разрыва).
                        value = curr_amt if metric == "amount" else curr_weight
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
                prev_month = month

            # Полная распродажа в конце истории: после последнего появления бумаги
            # у фонда есть более свежие отчёты → отток всей позиции в первый из них.
            if prev_month is not None:
                tail = [m for m in fund_months if m > prev_month]
                out_val = prev_amt if metric == "amount" else prev_weight
                if tail and out_val is not None:
                    tm = tail[0]
                    all_months.add(tm)
                    month_val[tm] = (month_val.get(tm) or 0.0) - out_val
        fund_month_val[fid] = month_val

    # Ось месяцев — НЕПРЕРЫВНЫЙ ряд от первого месяца бумаги до горизонта данных
    # (последний снапшот, который у нас вообще есть по whitelist-фондам, с учётом
    # тирной задержки). Раньше брали sorted(all_months) = только месяцы, где бумага
    # присутствовала хоть в одном фонде: полностью проданная бумага обрывала график
    # на дате распродажи (будто истории дальше нет), а внутри оси зияли дыры.
    # Месяцы без строк по бумаге → values=None → нулевые (пустые) бары. Так честнее.
    last_global = db.execute(text("""
        SELECT MAX(h.snapshot_date)
        FROM fund_holdings_history h
        JOIN funds f ON f.fund_id = h.fund_id
        WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks'
          AND h.source = ANY(:sources)
          AND h.snapshot_date <= :cutoff
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES),
           "cutoff": cutoff}).scalar()

    first_month = min(all_months)
    last_month = max(all_months)
    if last_global:
        last_month = max(last_month, last_global.strftime("%Y-%m"))
    months = _month_range(first_month, last_month)

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


# Пары «обыкновенная ↔ привилегированная» для режима «От капитализации»:
# знаменатель — free-float капа ВСЕЙ компании (решение владельца 2026-08-10),
# а в freefloat_cap (индекс широкого рынка MOEXBMI) классы акций лежат
# отдельными тикерами. Курируемый список вместо срезания хвостовой «P»:
# GAZP−P=GAZ (ГАЗ) — эвристика склеила бы РАЗНЫЕ компании.
_PREF_PAIRS_BASE = {
    "SBERP": "SBER", "SNGSP": "SNGS", "TATNP": "TATN", "RTKMP": "RTKM",
    "BANEP": "BANE", "MTLRP": "MTLR", "NKNCP": "NKNC", "LSNGP": "LSNG",
    "KAZTP": "KAZT", "PMSBP": "PMSB", "KRKNP": "KRKN", "MGTSP": "MGTS",
    "CNTLP": "CNTL", "VRSBP": "VRSB",
}
# Оба направления: держат преф → добавить обычку, держат обычку → добавить преф.
PREF_PAIRS: dict[str, str] = {**_PREF_PAIRS_BASE,
                              **{v: k for k, v in _PREF_PAIRS_BASE.items()}}

# Досклейка тикера предшественника, когда securities_ref отдаёт НЕ тот secid,
# под которым бумага торговалась на основном режиме (и лежит в freefloat_cap).
# У ГДР переехавших компаний ISS резолвит ISIN на строку «-ME» (сегмент МСФО-
# расписок), а история капы у биржи — под основным тикером: TCS-ME vs TCSG.
# Из-за этого Т-Технологии теряли 36 месяцев истории «От капитализации»
# (данные с 2021-09 были, но лежали под TCSG и в группу не попадали).
# Правим здесь, а не в securities_ref: тот справочник задаёт МАТЧИНГ холдингов
# по ISIN, его secid для этого не используется, и трогать его рискованнее.
FFCAP_SECID_ALIAS: dict[str, str] = {
    "TCS-ME": "TCSG",   # TCS Group ГДР → Т-Технологии
    "ETLN-ME": "ETLN",  # Etalon Group ГДР → Эталон
}


def _company_ffcap_by_month(db, resolved_isin: Optional[str],
                            months: list[str]) -> list[Optional[float]]:
    """Free-float капитализация компании (руб) по месяцам оси — знаменатель
    режима «От капитализации». null = бумаги нет в MOEXBMI в этом месяце
    (или это облигация/ОФЗ без ISIN-акции — фронт скрывает режим).

    Тикеры компании: все secid ISIN-группы канонической бумаги из securities_ref
    (склейка редомициляции: FIVE + X5 — историческая капа лежит под старым
    тикером ГДР) + предшественники из FFCAP_SECID_ALIAS + парный класс акций
    из PREF_PAIRS. FF-капы классов суммируются.
    """
    if not resolved_isin or not months:
        return [None] * len(months)
    try:
        secids = db.execute(text("""
            SELECT DISTINCT secid FROM securities_ref
            WHERE secid IS NOT NULL AND canonical_isin = (
                SELECT COALESCE(MAX(canonical_isin), :i)
                FROM securities_ref WHERE isin = :i)
        """), {"i": resolved_isin}).scalars().all()
        company = set(secids)
        company |= {FFCAP_SECID_ALIAS[s] for s in secids if s in FFCAP_SECID_ALIAS}
        for s in list(company):
            pair = PREF_PAIRS.get(s)
            if pair:
                company.add(pair)
        if not company:
            return [None] * len(months)
        rows = db.execute(text("""
            SELECT to_char(month, 'YYYY-MM') AS m, SUM(ffcap) AS cap
            FROM freefloat_cap
            WHERE sec_id = ANY(:secids)
              AND month >= to_date(:lo, 'YYYY-MM') AND month <= to_date(:hi, 'YYYY-MM')
            GROUP BY 1
        """), {"secids": list(company), "lo": months[0], "hi": months[-1]}).fetchall()
        by_month = {m: float(cap) for m, cap in rows}
        return [by_month.get(m) for m in months]
    except Exception:
        # Таблицы freefloat_cap может не быть (свежий dev) — режим просто
        # недоступен, остальная ручка живёт. Rollback, чтобы не отравить сессию.
        db.rollback()
        return [None] * len(months)


@router.get("/company-weights")
def company_weights(
    isin: Optional[str] = Query(None, description="ISIN бумаги (предпочтительно)"),
    asset_name: Optional[str] = Query(None, description="Имя бумаги — если нет ISIN"),
    user: Optional[User] = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    """
    Помесячная ИСТОРИЯ ДОЛИ бумаги в портфелях whitelist-фондов — режим «Доля»
    в «Потоках по компании». В отличие от /company-flows (дельты-сделки), здесь
    УРОВНИ: доля позиции в % от СЧА фонда на каждый месячный снапшот.

    Семантика значений per fund per month (weights[i]):
      - число  — доля в % на последний ПОЛНЫЙ снапшот фонда в этом месяце;
                 0.0 = фонд отчитался, бумаги в отчёте нет (продана / ещё не
                 куплена) — честный ноль;
      - null   — у фонда нет полного снапшота в месяце (дыра данных или
                 частичный импорт SCHA, Σweight < FT_COMPLETE_WSUM_MIN) —
                 фронт рвёт ряд, а не рисует ложный ноль.

    Частичные снапшоты игнорируются целиком: на уровнях они дают ложный провал
    доли (у дельт это гасилось соседним месяцем, здесь — нет).

    navs[i] — полная СЧА фонда на тот же снапшот, выведенная ИЗ САМОГО снапшота:
    Σ amount_rub / (Σ weight / 100). Нужна фронту для веса «по капиталу»
    (Σ nav×доля / Σ nav). Не берём fund_data.nav (Cbonds): источник другой,
    даты не совпадают со снапшотами, а внутри одного снапшота числитель и
    знаменатель обязаны быть из одного отчёта.

    ISIN-алиасы (редомициль-пары, ГДР + акция в переходный месяц) суммируются
    в одну долю — как /company-flows склеивает потоки по canonical_isin.

    Ось месяцев — непрерывный ряд от первого появления бумаги до последнего
    полного снапшота держателей (без тирной задержки — company_snapshot_delay=0
    на всех тирах).

    ffcap[i] — free-float капитализация компании (руб) на месяц, знаменатель
    режима «От капитализации» (см. _company_ffcap_by_month). null = бумаги нет
    в MOEXBMI в этом месяце — фронт скрывает режим целиком, если null'ы все.
    """
    if not isin and not asset_name:
        raise HTTPException(status_code=400, detail="isin or asset_name required")

    if isin:
        match_sql = ("(h.isin = :isin OR h.isin IN "
                     "(SELECT isin FROM securities_ref WHERE canonical_isin = :isin))")
        match_params = {"isin": isin}
    else:
        match_sql = "h.asset_name = :aname"
        match_params = {"aname": asset_name}

    cutoff = _snapshot_cutoff(db, user, COMPANY_DELAY_KEY)  # «По бумаге» — без задержки на всех тирах

    # Строки бумаги: per fund per snapshot_date, доля и стоимость СУММОЙ по всем
    # ISIN-алиасам (ГДР + акция одновременно → складываем, не выбираем одну).
    rows = db.execute(text(f"""
        SELECT f.fund_id, f.ticker, f.name AS fund_name, f.uk_id,
               h.snapshot_date,
               SUM(h.weight) AS weight,
               (array_agg(h.asset_name ORDER BY length(h.asset_name), h.asset_name))[1] AS aname,
               MAX(h.isin) AS isin
        FROM fund_holdings_history h
        JOIN funds f ON f.fund_id = h.fund_id
        WHERE f.ticker = ANY(:tickers) AND f.category = 'stocks'
          AND h.source = ANY(:sources)
          AND h.snapshot_date <= :cutoff AND h.snapshot_date >= :floor
          AND {match_sql}
        GROUP BY f.fund_id, f.ticker, f.name, f.uk_id, h.snapshot_date
        ORDER BY f.fund_id, h.snapshot_date ASC
    """), {"tickers": list(WHITELIST_TICKERS), "sources": list(MONTHLY_SOURCES), "cutoff": cutoff,
           "floor": FT_HISTORY_FLOOR, **match_params}).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail="Asset not found in any whitelist fund")

    resolved_name = rows[-1]["aname"]
    resolved_isin = isin or next((r["isin"] for r in reversed(rows) if r["isin"]), None)

    fund_meta = {}
    # (fund_id, snapshot_date) -> доля бумаги в % на этот снапшот.
    asset_w: dict = {}
    for r in rows:
        fund_meta[r["fund_id"]] = {"ticker": r["ticker"], "fund_name": r["fund_name"], "uk_id": r["uk_id"]}
        if r["weight"] is not None:
            asset_w[(r["fund_id"], r["snapshot_date"])] = float(r["weight"])

    # Полные снапшоты держателей (гейт FT_COMPLETE_WSUM_MIN) + implied СЧА.
    snaps = db.execute(text("""
        SELECT fund_id, snapshot_date,
               COALESCE(SUM(weight), 0) AS wsum,
               COALESCE(SUM(amount_rub), 0) AS asum
        FROM fund_holdings_history
        WHERE fund_id = ANY(:fids) AND source = ANY(:sources)
          AND snapshot_date <= :cutoff AND snapshot_date >= :floor
        GROUP BY fund_id, snapshot_date
        HAVING COALESCE(SUM(weight), 0) >= :wmin
    """), {"fids": list(fund_meta.keys()), "sources": list(MONTHLY_SOURCES),
           "cutoff": cutoff, "wmin": FT_COMPLETE_WSUM_MIN,
           "floor": FT_HISTORY_FLOOR}).fetchall()

    # fund_id -> { "YYYY-MM": (snapshot_date, nav|None) } — последний полный
    # снапшот месяца (два отчёта в одном месяце → берём поздний).
    from collections import defaultdict
    fund_month_snap: dict = defaultdict(dict)
    for fid, d, wsum, asum in snaps:
        m = d.strftime("%Y-%m")
        prev = fund_month_snap[fid].get(m)
        if prev is None or d > prev[0]:
            nav = float(asum) / (float(wsum) / 100.0) if asum and wsum else None
            fund_month_snap[fid][m] = (d, nav)

    if not fund_month_snap:
        raise HTTPException(status_code=404, detail="No complete snapshots for holders")

    # Ось: от первого месяца, где бумага есть на ПОЛНОМ снапшоте, до последнего
    # полного месяца держателей (правее данных всё равно нет).
    appear_months = [
        m for fid, by_month in fund_month_snap.items()
        for m, (d, _nav) in by_month.items() if asset_w.get((fid, d))
    ]
    if not appear_months:
        raise HTTPException(status_code=404, detail="Asset only in partial snapshots")
    first_month = min(appear_months)
    last_month = max(m for by_month in fund_month_snap.values() for m in by_month)
    months = _month_range(first_month, last_month)

    funds_out = []
    for fid in sorted(fund_meta.keys(), key=lambda i: fund_meta[i]["ticker"]):
        by_month = fund_month_snap.get(fid, {})
        weights: list = []
        navs: list = []
        for m in months:
            snap = by_month.get(m)
            if snap is None:
                weights.append(None)
                navs.append(None)
            else:
                d, nav = snap
                weights.append(round(asset_w.get((fid, d), 0.0), 4))
                navs.append(nav)
        # Фонд без единого полного снапшота на оси — не отдаём пустышку.
        if any(w is not None for w in weights):
            funds_out.append({
                "ticker": fund_meta[fid]["ticker"],
                "fund_name": fund_meta[fid]["fund_name"],
                "uk_id": fund_meta[fid]["uk_id"],
                "weights": weights,
                "navs": navs,
            })

    return {
        "asset_name": resolved_name,
        "isin": resolved_isin,
        "funds_count": len(funds_out),
        "months": months,
        "funds": funds_out,
        # Free-float капа компании (руб) по месяцам — знаменатель режима
        # «От капитализации» (Σ позиций фондов / ffcap). null = нет в MOEXBMI.
        "ffcap": _company_ffcap_by_month(db, resolved_isin, months),
    }


# Коэффициент конвертации при редомициляции: сколько НОВЫХ акций дали за одну
# СТАРУЮ бумагу (расписку). Цены старой серии умножаем на 1/k, чтобы склеенная
# линия была в масштабе текущей акции.
#
# Почти везде обмен шёл 1:1 (Яндекс, Хэдхантер, X5, Циан, ВК) — их тут нет,
# дефолт 1.0. Исключения:
#
# Русагро: 1 ГДР ROS AGRO PLC = 5 акций ПАО «Русагро» (ГДР закрылась 1083.8 ₽
# 2024-12-02, акция открылась 216.3 ₽ 2025-02-17 — отношение 5.01; размер
# выпуска 958 749 600 делится на 5 ровно).
#
# Фикс Прайс: 1 ГДР Fix Price Group PLC = 158 акций ПАО «Фикс Прайс»
# (коэффициент с сайта обмена exchange.fix-price.com — отражает долю
# российского бизнеса ~74% от активов группы; ГДР закрылась 137.4 ₽ 2025-06-20,
# акция на третий день торгов закрылась 0.865 ₽ 2025-08-22 — 137.4/158 = 0.870).
# Без коэффициента склеенная линия рухнула бы в 158 раз.
# ВНИМАНИЕ: ПАО «Фикс Прайс» анонсировало консолидацию 1:1000 (100 млрд → 100 млн
# акций). Когда она пройдёт, k станет 0.158 — если, конечно, MOEX оставит secid
# FIXR и продолжит ту же серию свечей.
#
# Ключ — СТАРЫЙ secid (делистингованная бумага), значение — k.
REDOMICILE_RATIO: dict[str, float] = {
    "AGRO": 5.0,     # ГДР ROS AGRO PLC → Русагро (RAGR)
    "FIXP": 158.0,   # ГДР Fix Price Group PLC → Фикс Прайс (FIXR)
}


@router.get("/price-weekly")
def price_weekly(
    ticker: str = Query(..., description="Тикер акции (secid MOEX)"),
    db: Session = Depends(get_db),
):
    """
    Недельные закрытия акции из дневных свечей — фон режима «Карта сделок»
    в «Потоках по компании» (линия цены, на которую фронт сажает кругляши
    месячных нетто-сделок фондов).

    Агрегация date_trunc('week') по interval=24 / type='stock': закрытие
    недели = close последней дневной свечи внутри недели. Тикер резолвит
    фронт (resolveFundTicker по ISIN) — облигации/ОФЗ тикера не имеют и
    сюда не приходят. Нет строк (не акция / нет истории в candles) → 404,
    фронт показывает empty-state.

    СКЛЕЙКА РЕДОМИЦИЛЯЦИИ. У переехавших бумаг история до переезда лежит под
    СТАРЫМ secid (HEAD ← HHRU, YDEX ← YNDX, X5 ← FIVE, RAGR ← AGRO,
    CNRU ← CIAN, VKCO ← MAIL, FIXR ← FIXP), потому что на бирже это разные
    инструменты.
    /company-flows склеивает потоки по canonical_isin, поэтому без такой же
    склейки цены гистограмма показывала сделки с 2021 года, а карта начиналась
    с даты листинга новой бумаги (у X5 разрыв был 46 месяцев).

    Группу берём из securities_ref (canonical_isin → все ISIN → их secid) —
    ничего не хардкодим, кроме коэффициента обмена (REDOMICILE_RATIO), который
    из данных не выводится. При пересечении дат (день делистинга старой = день
    листинга новой) выигрывает ЗАПРОШЕННАЯ бумага. Разрыв между делистингом и
    листингом (у X5 девять месяцев) не заполняем: торгов там не было, фронт
    соединит точки прямой.

    Без тирного гейта: дневная цена публична (гостям /api/candles отдаёт
    interval=24), задержка снапшотов сделок к цене отношения не имеет.
    """
    from api.schemas.validators import validate_safe_id
    try:
        ticker = validate_safe_id(ticker, "ticker")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Старые secid той же бумаги: canonical_isin запрошенного тикера → вся
    # группа ISIN → их secid, кроме самого тикера. Нет записи в securities_ref
    # (или бумага не переезжала) → пустой список, работаем как раньше.
    legacy = [r[0] for r in db.execute(text("""
        SELECT DISTINCT sr.secid
        FROM securities_ref sr
        WHERE sr.canonical_isin = (
                SELECT canonical_isin FROM securities_ref
                WHERE secid = :t AND canonical_isin IS NOT NULL LIMIT 1
              )
          AND sr.secid IS NOT NULL AND sr.secid <> :t
    """), {"t": ticker}).all()]

    # Недельные закрытия по каждому secid отдельно: масштаб старой серии
    # приводим к текущей акции, а «кто победил» на пересечении решаем ниже.
    def weekly(secid: str) -> list[tuple]:
        return db.execute(text("""
            SELECT (date_trunc('week', begin_time))::date AS week,
                   (array_agg(close ORDER BY begin_time DESC))[1] AS close
            FROM candles
            WHERE secid = :t AND interval = 24 AND type = 'stock' AND close > 0
            GROUP BY 1
            ORDER BY 1
        """), {"t": secid}).all()

    by_week: dict = {}
    for old in legacy:
        k = REDOMICILE_RATIO.get(old, 1.0)
        for wk, close in weekly(old):
            by_week[wk] = float(close) / k
    # Запрошенная бумага идёт последней и перетирает пересечения.
    for wk, close in weekly(ticker):
        by_week[wk] = float(close)

    if not by_week:
        raise HTTPException(status_code=404, detail="Нет истории цены по этой бумаге")

    # МЕДИАННЫЙ ДНЕВНОЙ ОБОРОТ — знаменатель режима «Навес» в «Потоках по
    # компании» (позиция фондов ₽ / оборот ₽ в день = дней на выход).
    #
    # Медиана, а не среднее: один аукционный день с аномальным оборотом не
    # должен схлопывать навес. Оборот (value) уже в рублях, поэтому склейка
    # редомициляции идёт БЕЗ коэффициента обмена (REDOMICILE_RATIO масштабирует
    # только цены за штуку); на пересечении дат побеждает запрошенная бумага —
    # тем же правилом, что и цены выше.
    #
    # ОКНО — СКОЛЬЗЯЩИЕ ТРИ МЕСЯЦА, а не календарный месяц (решение владельца
    # 2026-08-13). Числитель (позиция фондов) — медленный ряд месячных
    # снапшотов, он ходит ~6% в месяц; месячная медиана оборота на бумагах с
    # узким рынком ходит на порядок сильнее (замер по истории с 2022:
    # Мать и дитя 58% мес-к-мес и скачки до 8.5x, Ренессанс и ПИК 62%, при 27%
    # у Лукойла). При таком разрыве вся дисперсия «Навеса» приходила со стороны
    # знаменателя, и график фондов по сути показывал перевёрнутый график
    # оборота. Трёхмесячное окно режет колебания до 24% и скачки до 3.4x,
    # сохраняя настоящие сдвиги режима ликвидности (просадка мая-июня 2025
    # остаётся видна). Точность «в моменте» тут всё равно иллюзорна: снапшоты
    # составов приходят раз в месяц и с задержкой.
    #
    # Окно берётся по КАЛЕНДАРЮ (месяц + два предыдущих), а не по позиции в
    # списке: у бумаги может не быть торгов целый месяц, и окно «три соседних
    # элемента» молча растянулось бы на полгода.
    #
    # ТОЛЬКО БУДНИЕ СЕССИИ (isodow <= 5). МосБиржа торгует акциями и по
    # выходным, но ПИФы в эти дни сделок не совершают, поэтому выходная
    # ликвидность к «дням на выход фондов» отношения не имеет. Технически это
    # ещё и грубо смещало медиану: выходные дают четверть наблюдений окна, а
    # оборот в них на порядок меньше буднего (замер за апрель-июнь 2026:
    # Хэдхантер 346 млн в будни против 22 млн в выходные, Полюс 1770 против 96),
    # и медиана пула съезжала примерно на 30-й перцентиль будних значений.
    # Навес из-за этого завышался на 15-40% (Хэдхантер 28.7 дн вместо 24.0,
    # Позитив 5.5 вместо 3.5). Фильтр заодно отсекает редкие рабочие субботы
    # доковидных лет — фонды в них тоже не торговали.
    def daily_value(secid: str) -> list[tuple]:
        return db.execute(text("""
            SELECT begin_time::date AS day, value
            FROM candles
            WHERE secid = :t AND interval = 24 AND type = 'stock' AND value > 0
              AND extract(isodow FROM begin_time) <= 5
            ORDER BY 1
        """), {"t": secid}).all()

    by_day: dict = {}
    for old in legacy:
        for d, v in daily_value(old):
            by_day[d] = float(v)
    for d, v in daily_value(ticker):
        by_day[d] = float(v)

    from collections import defaultdict
    from statistics import median
    vals_by_month: dict = defaultdict(list)
    for d, v in by_day.items():
        vals_by_month[d.strftime("%Y-%m")].append(v)
    turnover_months = sorted(vals_by_month)

    def _shift_month(m: str, back: int) -> str:
        t = int(m[:4]) * 12 + (int(m[5:7]) - 1) - back
        return "%04d-%02d" % (t // 12, t % 12 + 1)

    # Медиана считается по ПУЛУ сырых дневных оборотов окна, а не как среднее
    # трёх месячных медиан: месяцы разной длины (январские каникулы), и среднее
    # медиан дало бы короткому месяцу тот же вес, что полному.
    med_turnover = []
    for m in turnover_months:
        pool = [v for k in (m, _shift_month(m, 1), _shift_month(m, 2))
                for v in vals_by_month.get(k, ())]
        med_turnover.append(round(median(pool)))

    weeks = sorted(by_week)
    return {
        "ticker": ticker,
        "weeks": [w.isoformat() for w in weeks],
        "closes": [by_week[w] for w in weeks],
        # "YYYY-MM" + медианный дневной оборот (руб) за скользящее окно этого
        # месяца и двух предыдущих, только будние сессии, выровнены.
        "turnover_months": turnover_months,
        "med_turnover": med_turnover,
    }
