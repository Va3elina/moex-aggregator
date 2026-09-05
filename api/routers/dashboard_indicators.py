"""
Провалиться в индикатор: /api/admin/dashboard/indicators/*

⚠️ ОДИН ШАБЛОН НА ВОСЕМЬ ИНДИКАТОРОВ. У каждого — объект (фьючерс, бумага, фонд,
индекс, вселенная), ряд-другой на графике, паспорт (из чего собран, кто пишет,
насколько свежо) и переходы. Всё, что различается между индикаторами, живёт
в реестре ИНДИКАТОРЫ и в маленькой функции ряда; экран один.

⚠️ СВЕЖЕСТЬ И ПРОПУСКИ — ПО ТОРГОВЫМ ДНЯМ, А НЕ ПО КАЛЕНДАРЮ. «Нет точки за
субботу» — не пропуск. Эталон торговых дней — index_data по IMOEX: биржа
торговала, значит, у дневного ряда обязана быть точка.

⚠️ РЯД ОТКРЫТОГО ИНТЕРЕСА — ИЗ ТОГО ЖЕ РАСЧЁТА, ЧТО НА САЙТЕ. Склейка контрактов
(CONFIRM_DAYS, COOLDOWN_DAYS, поправка на сплиты) — единственный экземпляр в
chart._compute_chart_data; второй копии ролловера здесь нет и не будет. Засечки
смены контракта — оттуда же (contract_switches), экспирации — из календаря.

⚠️ ПАРАМЕТР ДНЕЙ — ЧЕРЕЗ CAST(:д AS int): pg8000 шлёт его нетипизированным,
и «CURRENT_DATE - :д» у Postgres превращается в date - date = integer, а дальше
«date > integer» — ошибка. То же правило, что и с CAST(:x AS date) в срезах.

⚠️ MAX(begin_time) ПО candles — ТОЛЬКО С УСЛОВИЕМ interval=24: под него есть
частичный индекс (interval, begin_time). Без условия это проход по 32 млн строк.

Только чтение, только админ.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_admin
from api.routers.chart import _compute_chart_data

router = APIRouter(prefix="/api/admin/dashboard/indicators", tags=["admin-dashboard"])

ПЕРИОДЫ = {"3m": 92, "1y": 366, "3y": 1100}


@dataclass(frozen=True)
class Таблица:
    имя: str
    дата: str                 # колонка даты
    условие: str = "TRUE"     # чтобы MAX лёг на индекс
    подпись: str = ""


@dataclass(frozen=True)
class Индикатор:
    id: str
    имя: str
    путь: str
    методология: str | None
    описание: str             # как считается — человеческим языком
    таблицы: tuple[Таблица, ...]
    пайплайны: tuple[str, ...]
    узел_карты: str
    срезы: tuple[tuple[str, str], ...]   # (подпись, адрес среза базы)
    ряды: Callable                       # (db, obj, дней) -> dict
    объекты: Callable | None = None      # (db) -> [{значение, подпись}]
    объект_по_умолчанию: str | None = None
    объект_подпись: str = "Объект"
    период_по_умолчанию: str = "1y"


def _f(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _т(d) -> str:
    """Точка времени — ISO-дата; SimpleChart на фронте ест строки."""
    if isinstance(d, datetime):
        return d.date().isoformat()
    return d.isoformat()


def _ряд(db: Session, sql: str, параметры: dict) -> list:
    return [[_т(r[0]), _f(r[1])] for r in db.execute(text(sql), параметры).all() if r[1] is not None]


# ── объекты ─────────────────────────────────────────────────────────────────

def _фьючерсы(db):
    return [{"значение": r[0], "подпись": f"{r[0]} · {r[1]}"} for r in db.execute(text("""
        SELECT sectype, MIN(name) FROM instruments
         WHERE type = 'futures' AND NOT hidden GROUP BY sectype ORDER BY sectype""")).all()]


def _акции(db):
    return [{"значение": r[0], "подпись": f"{r[0]} · {r[1]}"} for r in db.execute(text("""
        SELECT sec_id, name FROM instruments WHERE type = 'stock' AND NOT hidden ORDER BY sec_id""")).all()]


def _фонды(db):
    return [{"значение": r[0], "подпись": f"{r[0]} · {r[1]}"} for r in db.execute(text("""
        SELECT ticker, name FROM funds ORDER BY ticker""")).all()]


def _индексы(db):
    return [{"значение": r[0], "подпись": f"{r[0]} · {r[1]}"} for r in db.execute(text("""
        SELECT secid, name FROM indices ORDER BY secid""")).all()]


def _вселенные(db):
    return [{"значение": f"{r[0]}:{r[1]}", "подпись": f"{r[0]} · EMA {r[1]}"} for r in db.execute(text("""
        SELECT universe, ema_period FROM breadth_history GROUP BY 1, 2 ORDER BY 1, 2""")).all()]


def _потоки(db):
    return [{"значение": f"{r[0]}|{r[1]}", "подпись": f"{r[0]} · {r[1]}"} for r in db.execute(text("""
        SELECT instrument_type, category FROM cbr_flows GROUP BY 1, 2 ORDER BY 1, 2""")).all()]


# ── ряды ────────────────────────────────────────────────────────────────────

def _ряды_ои(db, obj, дней):
    период = "3m" if дней <= 92 else "1y" if дней <= 366 else "5y"   # ключи PERIODS в chart.py
    r = _compute_chart_data(db, obj, obj, "futures", 24, "FIZ", True, период, None, None)
    с = (date.today() - timedelta(days=дней)).isoformat()
    цена = [[c["time"][:10], _f(c.get("close"))] for c in r.get("candles", []) if c.get("close") is not None and c["time"][:10] >= с]
    нетто = [[o["time"][:10], _f(o.get("net_position"))] for o in r.get("open_interest", []) if o["time"][:10] >= с]
    засечки = [{"время": s["date"], "метка": s["to"], "описание": f"смена контракта {s['from'] or '—'} → {s['to']}"}
               for s in r.get("contract_switches", []) if s["date"] >= с]
    for k in db.execute(text("""
        SELECT secid, lsttrade FROM futures_contracts
         WHERE sectype = :s AND lsttrade BETWEEN CAST(:с AS date) AND CURRENT_DATE + 120
         ORDER BY lsttrade"""), {"s": obj, "с": с}).all():
        засечки.append({"время": k[1].isoformat(), "метка": k[0][-2:], "описание": f"экспирация {k[0]} · {k[1].strftime('%d.%m.%Y')}"})
    return {
        "ряды": [
            {"имя": "цена, непрерывный фьючерс", "ось": "left", "точки": цена},
            {"имя": "нетто-позиция физлиц", "ось": "right", "точки": нетто},
        ],
        "засечки": засечки,
        "примечание": f"режим склейки: {r.get('mode')} · контрактов в ряду: {len(r.get('contracts') or [])}",
    }


def _ряды_акции(db, obj, дней):
    п = {"s": obj, "д": дней}
    return {"ряды": [
        {"имя": "закрытие, ₽", "ось": "left", "точки": _ряд(db, """
            SELECT begin_time, close FROM candles
             WHERE secid = :s AND "interval" = 24 AND begin_time > NOW() - make_interval(days => CAST(:д AS int))
             ORDER BY begin_time""", п)},
        {"имя": "капитализация, ₽", "ось": "right", "точки": _ряд(db, """
            SELECT period_date, market_cap FROM stock_market_cap
             WHERE sec_id = :s AND period_date > CURRENT_DATE - CAST(:д AS int) ORDER BY period_date""", п)},
    ], "засечки": []}


def _ряды_фонда(db, obj, дней):
    п = {"t": obj, "д": дней}
    return {"ряды": [
        {"имя": "СЧА, ₽", "ось": "left", "точки": _ряд(db, """
            SELECT d.trade_date, d.nav FROM fund_data d JOIN funds f ON f.fund_id = d.fund_id
             WHERE f.ticker = :t AND d.trade_date > CURRENT_DATE - CAST(:д AS int) ORDER BY d.trade_date""", п)},
        {"имя": "пай, ₽", "ось": "right", "точки": _ряд(db, """
            SELECT d.trade_date, d.pay FROM fund_data d JOIN funds f ON f.fund_id = d.fund_id
             WHERE f.ticker = :t AND d.trade_date > CURRENT_DATE - CAST(:д AS int) ORDER BY d.trade_date""", п)},
    ], "засечки": []}


def _ряды_состава(db, obj, дней):
    п = {"t": obj, "д": дней}
    return {"ряды": [
        {"имя": "позиций в снимке", "ось": "left", "тип": "bars", "точки": _ряд(db, """
            SELECT h.snapshot_date, COUNT(*) FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id
             WHERE f.ticker = :t AND h.snapshot_date > CURRENT_DATE - CAST(:д AS int) GROUP BY 1 ORDER BY 1""", п)},
        {"имя": "сумма весов, %", "ось": "right", "точки": _ряд(db, """
            SELECT h.snapshot_date, SUM(h.weight) FROM fund_holdings_history h JOIN funds f ON f.fund_id = h.fund_id
             WHERE f.ticker = :t AND h.snapshot_date > CURRENT_DATE - CAST(:д AS int) GROUP BY 1 ORDER BY 1""", п)},
    ], "засечки": [], "без_пропусков": True, "примечание": "снимки месячные — пропуски по торговым дням не считаются"}


def _ряды_ширины(db, obj, дней):
    вселенная, ema = obj.split(":")
    п = {"u": вселенная, "e": int(ema), "д": дней}
    return {"ряды": [
        {"имя": f"доля бумаг выше EMA {ema}, %", "ось": "left", "точки": _ряд(db, """
            SELECT trade_date, percent_above FROM breadth_history
             WHERE universe = :u AND ema_period = :e AND trade_date > CURRENT_DATE - CAST(:д AS int) ORDER BY trade_date""", п)},
        {"имя": "бумаг в расчёте", "ось": "right", "точки": _ряд(db, """
            SELECT trade_date, count_total FROM breadth_history
             WHERE universe = :u AND ema_period = :e AND trade_date > CURRENT_DATE - CAST(:д AS int) ORDER BY trade_date""", п)},
    ], "засечки": []}


def _ряды_индекса(db, obj, дней):
    п = {"s": obj, "д": дней}
    return {"ряды": [
        {"имя": "закрытие", "ось": "left", "точки": _ряд(db, """
            SELECT trade_date, close FROM index_data
             WHERE secid = :s AND trade_date > CURRENT_DATE - CAST(:д AS int) ORDER BY trade_date""", п)},
        {"имя": "оборот, ₽", "ось": "right", "тип": "bars", "точки": _ряд(db, """
            SELECT trade_date, value FROM index_data
             WHERE secid = :s AND trade_date > CURRENT_DATE - CAST(:д AS int) ORDER BY trade_date""", п)},
    ], "засечки": []}


def _ряды_потоков(db, obj, дней):
    тип, категория = obj.split("|", 1)
    п = {"t": тип, "c": категория, "д": дней}
    return {"ряды": [
        {"имя": f"{категория}, млрд ₽", "ось": "left", "тип": "bars", "точки": _ряд(db, """
            SELECT period_end_date, value FROM cbr_flows
             WHERE instrument_type = :t AND category = :c AND period_end_date > CURRENT_DATE - CAST(:д AS int)
             ORDER BY period_end_date""", п)},
    ], "засечки": [], "без_пропусков": True, "примечание": "ряд месячный, ингест ручной (XLSX ЦБ) — пропуски по торговым дням не считаются"}


def _ряды_баффетта(db, obj, дней):
    п = {"д": дней}
    return {"ряды": [
        {"имя": "капитализация рынка, млрд ₽", "ось": "left", "точки": _ряд(db, """
            SELECT period_date, value FROM macro_data
             WHERE indicator = 'MARKET_CAP_TOTAL' AND period_date > CURRENT_DATE - CAST(:д AS int) ORDER BY period_date""", п)},
        {"имя": "ВВП, квартальный", "ось": "right", "тип": "bars", "точки": _ряд(db, """
            SELECT period_date, value FROM macro_data
             WHERE indicator = 'GDP_QUARTERLY' AND period_date > CURRENT_DATE - CAST(:д AS int) - 400 ORDER BY period_date""", п)},
    ], "засечки": []}


ИНДИКАТОРЫ: tuple[Индикатор, ...] = (
    Индикатор(
        id="oi", имя="Открытые позиции", путь="/oi", методология="/methodology/oi",
        описание="Нетто-позиция физлиц и юрлиц по фьючерсам (лонг + шорт, шорт со знаком минус) поверх непрерывного ряда цены. Контракты склеиваются как в TradingView: лидер по объёму, подтверждение два дня, откат назад запрещён 45 дней.",
        таблицы=(Таблица("open_interest", "tradedate", "clgroup = 'FIZ' AND \"interval\" = 24", "позиции по группам"),
                 Таблица("candles", "begin_time", "\"interval\" = 24", "свечи (дневные)"),
                 Таблица("futures_contracts", "last_seen", "TRUE", "календарь контрактов")),
        пайплайны=("oi_5min", "oi_daily", "oi_aggregate", "candles_futures", "contract_calendar"),
        узел_карты="p_market",
        срезы=(("открытый интерес", "/admin/dashboard/db/oi"), ("свечи", "/admin/dashboard/db/candles")),
        ряды=_ряды_ои, объекты=_фьючерсы, объект_по_умолчанию="SR", объект_подпись="Фьючерс",
    ),
    Индикатор(
        id="heatmap", имя="Карта рынка", путь="/heatmap", методология="/methodology/heatmap",
        описание="Плитки всех акций: цвет — изменение цены за период, размер — капитализация или оборот. Считается из дневных и пятиминутных свечей и капитализации по бумагам.",
        таблицы=(Таблица("candles", "begin_time", "\"interval\" = 24", "свечи (дневные)"),
                 Таблица("stock_market_cap", "period_date", "TRUE", "капитализация по бумагам"),
                 Таблица("instruments", "", "TRUE", "справочник бумаг")),
        пайплайны=("candles_spot", "market_cap_daily"),
        узел_карты="p_market",
        срезы=(("свечи", "/admin/dashboard/db/candles"), ("капитализация", "/admin/dashboard/db/market_cap")),
        ряды=_ряды_акции, объекты=_акции, объект_по_умолчанию="SBER", объект_подпись="Бумага",
    ),
    Индикатор(
        id="funds", имя="Деньги в фондах", путь="/funds", методология="/methodology/funds-money",
        описание="Приток и отток денег в БПИФ: из СЧА и цены пая по дням выводится число паёв, его изменение — и есть поток. Cbonds отдаёт дату расчёта УК, биржа — дату торгов: сдвиг на день заложен.",
        таблицы=(Таблица("fund_data", "trade_date", "TRUE", "СЧА и пай по дням"),
                 Таблица("funds", "", "TRUE", "справочник фондов")),
        пайплайны=("funds_daily", "distributions"),
        узел_карты="p_funds",
        срезы=(("СЧА и паи", "/admin/dashboard/db/fund_nav"),),
        ряды=_ряды_фонда, объекты=_фонды, объект_по_умолчанию="LQDT", объект_подпись="Фонд",
    ),
    Индикатор(
        id="fund_trades", имя="Сделки фондов", путь="/fund-trades", методология=None,
        описание="Разница двух месячных снимков состава фонда — оценка того, что фонд купил и продал. Это СНИМКИ, не сделки: внутри месяца движение невидимо, а сплиты учитываются в трёх местах.",
        таблицы=(Таблица("fund_holdings_history", "snapshot_date", "TRUE", "снимки состава"),
                 Таблица("index_composition", "trade_date", "TRUE", "состав индексов (для сравнения с бенчмарком)")),
        пайплайны=("funds_daily",),
        узел_карты="p_funds",
        срезы=(("состав фондов", "/admin/dashboard/db/fund_holdings"), ("состав индексов", "/admin/dashboard/db/index_composition")),
        ряды=_ряды_состава, объекты=_фонды, объект_по_умолчанию="EQMX", объект_подпись="Фонд",
    ),
    Индикатор(
        id="strength", имя="Сила рынка", путь="/strength", методология="/methodology/strength",
        описание="Ширина рынка: доля бумаг вселенной, торгующихся выше своей EMA (20/50/100/200). Считается СТРОГО после обновления состава индекса — иначе история пересчиталась бы по сегодняшнему составу.",
        таблицы=(Таблица("breadth_history", "trade_date", "TRUE", "ширина по дням"),
                 Таблица("index_composition", "trade_date", "TRUE", "состав индексов")),
        пайплайны=("breadth_daily", "index_composition_daily", "candles_spot"),
        узел_карты="p_index",
        срезы=(("состав индексов", "/admin/dashboard/db/index_composition"),),
        ряды=_ряды_ширины, объекты=_вселенные, объект_по_умолчанию="imoex:50", объект_подпись="Вселенная · EMA",
    ),
    Индикатор(
        id="seasonality", имя="Сезонность", путь="/seasonality", методология="/methodology/seasonality",
        описание="Средняя траектория года по истории дневных закрытий индекса или бумаги. Здесь показан сам ряд, из которого она считается.",
        таблицы=(Таблица("index_data", "trade_date", "TRUE", "индексы по дням"),
                 Таблица("candles", "begin_time", "\"interval\" = 24", "свечи (дневные)")),
        пайплайны=("indices_daily", "candles_spot"),
        узел_карты="p_index",
        срезы=(("индексы", "/admin/dashboard/db/index_data"), ("свечи", "/admin/dashboard/db/candles")),
        ряды=_ряды_индекса, объекты=_индексы, объект_по_умолчанию="IMOEX", объект_подпись="Индекс", период_по_умолчанию="3y",
    ),
    Индикатор(
        id="cbr_flows", имя="Поток капитала", путь="/cbr-flows", методология="/methodology/cbr-flows",
        описание="Обзор рисков финансовых рынков ЦБ: нетто-покупки по группам участников помесячно. Ингест РУЧНОЙ — XLSX с сайта ЦБ, формат листов плавает.",
        таблицы=(Таблица("cbr_flows", "period_end_date", "TRUE", "потоки по группам"),),
        пайплайны=(),
        узел_карты="cbr",
        срезы=(),
        ряды=_ряды_потоков, объекты=_потоки, объект_подпись="Инструмент · группа", период_по_умолчанию="3y",
    ),
    Индикатор(
        id="buffett", имя="Индикатор Баффетта", путь="/buffett", методология="/methodology/buffett",
        описание="Капитализация всего рынка к ВВП. Капитализация — ежедневно со Смартлаба, ВВП — квартальный Росстат, годовая сумма скользящим окном.",
        таблицы=(Таблица("macro_data", "period_date", "indicator = 'MARKET_CAP_TOTAL'", "капитализация рынка"),
                 Таблица("macro_data", "period_date", "indicator = 'GDP_QUARTERLY'", "ВВП")),
        пайплайны=("macro_daily", "market_cap_daily"),
        узел_карты="p_macro",
        срезы=(("макро-ряды", "/admin/dashboard/db/macro"),),
        ряды=_ряды_баффетта, объекты=None, период_по_умолчанию="3y",
    ),
)
ПО_ID = {и.id: и for и in ИНДИКАТОРЫ}


def _свежесть(db: Session, и: Индикатор, now: date) -> list[dict]:
    out = []
    for т in и.таблицы:
        if not т.дата:
            out.append({"таблица": т.имя, "подпись": т.подпись, "последняя": None, "дней": None})
            continue
        d = db.execute(text(f"SELECT MAX({т.дата}) FROM {т.имя} WHERE {т.условие}")).scalar()
        if isinstance(d, datetime):
            d = d.date()
        out.append({"таблица": т.имя, "подпись": т.подпись, "последняя": d.isoformat() if d else None,
                    "дней": (now - d).days if d else None})
    return out


def _торговые_дни(db: Session, дней: int = 30) -> list[date]:
    return [r[0] for r in db.execute(text("""
        SELECT trade_date FROM index_data WHERE secid = 'IMOEX' AND trade_date > CURRENT_DATE - CAST(:д AS int)
         ORDER BY trade_date"""), {"д": дней}).all()]


def _паспорт(и: Индикатор) -> dict:
    return {
        "id": и.id, "имя": и.имя, "путь": и.путь, "методология": и.методология, "описание": и.описание,
        "пайплайны": list(и.пайплайны), "узел_карты": и.узел_карты,
        "срезы": [{"подпись": п, "адрес": а} for п, а in и.срезы],
        "объект_подпись": и.объект_подпись, "есть_объекты": и.объекты is not None,
        "период_по_умолчанию": и.период_по_умолчанию,
    }


@router.get("")
def список(db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    """Восемь индикаторов с паспортом и свежестью таблиц."""
    now = date.today()
    return {"индикаторы": [{**_паспорт(и), "свежесть": _свежесть(db, и, now)} for и in ИНДИКАТОРЫ]}


@router.get("/{indicator_id}")
def индикатор(
    indicator_id: str,
    obj: str | None = Query(None, description="объект: фьючерс, бумага, фонд, индекс…"),
    period: str = Query("", description="3m | 1y | 3y"),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Ряды, засечки, свежесть и пропуски для одного индикатора и объекта."""
    и = ПО_ID.get(indicator_id)
    if и is None:
        raise HTTPException(404, "нет такого индикатора")
    now = date.today()
    период = period if period in ПЕРИОДЫ else и.период_по_умолчанию
    дней = ПЕРИОДЫ[период]

    объекты = и.объекты(db) if и.объекты else []
    выбран = None
    if и.объекты:
        допустимые = {о["значение"] for о in объекты}
        выбран = obj if obj in допустимые else (и.объект_по_умолчанию if и.объект_по_умолчанию in допустимые
                                                 else (объекты[0]["значение"] if объекты else None))
    if и.объекты and выбран is None:
        raise HTTPException(404, "объектов нет")

    р = и.ряды(db, выбран, дней)

    # ── пропуски: по первому ряду, только за 30 торговых дней
    первый = р["ряды"][0]["точки"] if р["ряды"] else []
    есть = {p[0] for p in первый}
    торговые = _торговые_дни(db)
    # Последний торговый день может ещё не иметь точки — ряд пишется вечером;
    # пропуском считаем только дни строго до последнего торгового.
    проверяемые = торговые[:-1] if торговые else []
    пропущено = [d.isoformat() for d in проверяемые if d.isoformat() not in есть]
    считать_пропуски = not р.get("без_пропусков")

    return {
        "индикатор": _паспорт(и),
        "объекты": объекты,
        "выбран": выбран,
        "период": период,
        "ряды": р["ряды"],
        "засечки": р.get("засечки", []),
        "примечание": р.get("примечание"),
        "свежесть": _свежесть(db, и, now),
        "пропуски": {
            "торговых_дней": len(проверяемые),
            "пропущено": пропущено if считать_пропуски else [],
            "считается": считать_пропуски,
        },
    }
