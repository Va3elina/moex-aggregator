"""
API endpoints для открытого интереса
С валидацией входных данных
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, date

from api.database import get_db
from api.models import OpenInterest
from api.schemas import OpenInterestResponse, OpenInterestListResponse
from api.schemas.validators import ClgroupType, validate_safe_id
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_tier_limits, get_effective_end_date

router = APIRouter(prefix="/api/openinterest", tags=["open_interest"])

# Отдельный лёгкий роутер под /api/oi/* — справочные ручки про доступность
# внутридневных данных. Не путать с /api/openinterest/* (выдача рядов позиций).
oi_router = APIRouter(prefix="/api/oi", tags=["open_interest"])


@oi_router.get("/screener")
def get_oi_screener(
        clgroup: ClgroupType = Query("FIZ", description="Группа: FIZ (физлица) или YUR (юрлица)"),
        db: Session = Depends(get_db),
):
    """Скринер сигналов ОИ: чистая позиция группы + ATR14-кратность дневного
    движения по всем фьючерсам разом (вкладка «Скринер сигналов» на /oi).

    Данные дневные (T+1), меняются раз в сутки → single-flight кэш 30 мин.
    Открытый доступ (как публичная лента аномалий): дневная агрегированная
    статистика, не премиум-данные.
    """
    from api.cache import get_or_compute
    from api.services.oi_screener import compute_screener
    # v9 = групповой порог ликвидности (физ 50 / юр 15) + npart/min_part в ответе.
    # v8 = окна рекордов 1/2/3/4/5 лет + всё время. v7 = фикс интрадей-бара только за
    # торговые дни (ISODOW 1–5). v6 = + интрадей (бегущий 5-мин бар). TTL 5 мин.
    return get_or_compute(f"oi_screener:v9:{clgroup}", lambda: compute_screener(db, clgroup), ttl=300)


@oi_router.get("/intraday-assets")
def get_intraday_assets(db: Session = Depends(get_db)):
    """
    Список sectype, у которых ЕСТЬ свежие внутридневные данные позиций
    (open_interest interval=5 за последние 14 дней).

    Это «активы, поддерживающие внутридневные сигналы» — используется фронтом
    для бейджа «intraday» в пикере инструментов. ВАЖНО: реальный внутридневной
    ДЕТЕКТОР сигналов пока не готов (алерты считаются по дневным данным), поэтому
    это лишь пометка о потенциальной поддержке, а не функциональный таймфрейм.

    Лёгкий индексный DISTINCT-запрос по первичному ключу (interval, tradedate).
    """
    rows = db.execute(text(
        """
        SELECT DISTINCT sectype
        FROM open_interest
        WHERE interval = 5
          AND tradedate >= CURRENT_DATE - INTERVAL '14 days'
        ORDER BY sectype
        """
    )).all()
    return {"sectypes": [r[0] for r in rows]}


# --- Порог релевантности актива для пикера ОИ -------------------------------
# Сплит физ/юр по фьючерсу, которым торгует мало людей, статистически шумный:
# значимость выборки зависит от АБСОЛЮТНОГО числа участников, а не от доли рынка.
# Поэтому базовый порог фиксированный (floor), а динамическая часть лишь
# приподнимает его, если рынок в целом сильно раздуется (доля общей активности).
# Сглаживание — медиана числа физлиц-трейдеров за 5 торговых дней (анти-фликер:
# актив не мигает в списке из-за одного тихого/шумного дня).
OI_RELEVANCE_FLOOR = 500        # мин. число физлиц-трейдеров (абсолютная релевантность)
OI_RELEVANCE_DYN_SHARE = 0.002  # +динамика: 0.2% суммарной активности рынка
OI_RELEVANCE_SMOOTH_DAYS = 5    # окно медианы (торговых дней)


@oi_router.get("/low-activity-assets")
def get_low_activity_assets(db: Session = Depends(get_db)):
    """sectype фьючерсов с НИЗКОЙ активностью физлиц — прячутся в пикере ОИ.

    Активность актива = медиана числа физлиц-трейдеров (pos_long_num +
    pos_short_num, clgroup=FIZ) за последние OI_RELEVANCE_SMOOTH_DAYS торговых
    дней. Актив малоактивен, если эта медиана < порога. Порог =
    max(floor, dyn_share × сумма медиан по рынку): фиксированный низ релевантности
    плюс запас на общий рост рынка. Фронт прячет такие активы из дефолтного
    списка, но раскрывает их поиском/избранным; при росте активности медиана
    снова переходит порог и актив возвращается сам (пересчёт дневной).

    Дневные данные (T+1), меняются раз в сутки → single-flight кэш 1 час.
    Интрадей НЕ трогаем: пересчитывать чаще незачем, лишней нагрузки нет.
    """
    from api.cache import get_or_compute

    def _compute():
        rows = db.execute(text(
            """
            WITH daily AS (
                SELECT DISTINCT ON (sectype, tradedate)
                    sectype, tradedate,
                    (pos_long_num + pos_short_num) AS fiz
                FROM open_interest
                WHERE clgroup = 'FIZ' AND interval = 24
                  AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
                  AND tradedate >= CURRENT_DATE - INTERVAL '20 days'
                ORDER BY sectype, tradedate, tradetime DESC
            ),
            ranked AS (
                SELECT sectype, fiz,
                       ROW_NUMBER() OVER (
                           PARTITION BY sectype ORDER BY tradedate DESC
                       ) AS rn
                FROM daily
            )
            SELECT sectype,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY fiz) AS median_fiz
            FROM ranked
            WHERE rn <= :win
            GROUP BY sectype
            """
        ), {"win": OI_RELEVANCE_SMOOTH_DAYS}).fetchall()

        medians = {r[0]: float(r[1] or 0) for r in rows}
        total = sum(medians.values())
        threshold = max(OI_RELEVANCE_FLOOR, round(OI_RELEVANCE_DYN_SHARE * total))
        low = sorted(s for s, m in medians.items() if m < threshold)
        return {
            "threshold": threshold,
            "floor": OI_RELEVANCE_FLOOR,
            "smooth_days": OI_RELEVANCE_SMOOTH_DAYS,
            "sectypes": low,
        }

    return get_or_compute("oi_low_activity:v1", _compute, ttl=3600)


@router.get("/{sectype}", response_model=OpenInterestListResponse)
def get_open_interest(
        sectype: str,
        clgroup: ClgroupType = Query("FIZ", description="Группа: FIZ (физлица) или YUR (юрлица)"),
        interval: int = Query(60, description="Таймфрейм: 5, 60 или 24 минут"),
        date_from: date | None = Query(None, description="Дата начала (YYYY-MM-DD)"),
        date_to: date | None = Query(None, description="Дата окончания (YYYY-MM-DD)"),
        limit: int = Query(10000, description="Максимум записей", ge=1, le=50000),
        db: Session = Depends(get_db),
        user = Depends(get_current_user_optional)
):
    """Получить открытый интерес по sectype"""

    # Валидация interval
    if interval not in {5, 60, 24}:
        raise HTTPException(status_code=400, detail="interval должен быть 5, 60 или 24")

    # Валидация sectype
    try:
        sectype = validate_safe_id(sectype, "sectype")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Tier-ограничения: asset whitelist, interval, clgroup, max history
    enforce_tier_limits(
        user, "open_interest",
        asset=sectype, interval=interval, clgroup=clgroup,
    )

    # 24h задержка для Free — backend подменяет date_to, не доверяя фронту
    effective_end = get_effective_end_date(user, "open_interest")
    if effective_end is not None:
        if date_to is None or date_to > effective_end:
            date_to = effective_end

    # Валидация диапазона дат
    if date_from and date_to and date_to < date_from:
        raise HTTPException(status_code=400, detail="date_to не может быть раньше date_from")

    query = db.query(OpenInterest).filter(
        OpenInterest.sectype == sectype,
        OpenInterest.clgroup == clgroup,
        OpenInterest.interval == interval
    )

    # Фильтр по датам
    if date_from:
        query = query.filter(OpenInterest.tradedate >= date_from)
    if date_to:
        query = query.filter(OpenInterest.tradedate <= date_to)

    # Сортировка и лимит
    query = query.order_by(OpenInterest.tradedate.asc(), OpenInterest.tradetime.asc()).limit(limit)

    records = query.all()

    if not records:
        raise HTTPException(status_code=404, detail=f"Данные OI для {sectype} не найдены")

    # Преобразуем в формат ответа
    oi_list = [
        OpenInterestResponse(
            time=datetime.combine(r.tradedate, r.tradetime),
            pos=r.pos,
            pos_long=r.pos_long,
            pos_short=r.pos_short,
            pos_long_num=r.pos_long_num,
            pos_short_num=r.pos_short_num
        )
        for r in records
    ]

    return OpenInterestListResponse(
        sectype=sectype,
        clgroup=clgroup,
        interval=interval,
        count=len(oi_list),
        data=oi_list
    )