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
    # v7 = фикс: интрадей-бар берём только за торговые дни (ISODOW 1–5). До v7 на
    # выходных застоявшийся сб/вс-бар вплетался поверх пятничного дневного close и
    # ХОРОНИЛ реальные пятничные сигналы у всех интрадей-активов (см. _bulk_intraday_now).
    # v6 = + интрадей (has_intraday + бегущий 5-мин бар как «net сейчас»). TTL 5 мин.
    return get_or_compute(f"oi_screener:v7:{clgroup}", lambda: compute_screener(db, clgroup), ttl=300)


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