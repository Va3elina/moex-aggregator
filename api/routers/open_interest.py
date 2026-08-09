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
from api.billing.features import get_indicator_limits
from api.billing.tiers import user_tier

router = APIRouter(prefix="/api/openinterest", tags=["open_interest"])

# Отдельный лёгкий роутер под /api/oi/* — справочные ручки про доступность
# внутридневных данных. Не путать с /api/openinterest/* (выдача рядов позиций).
oi_router = APIRouter(prefix="/api/oi", tags=["open_interest"])


def build_oi_screener(db: Session, clgroup: str, horizon: str):
    """Собирает (или достаёт из кеша) ленту скринера.

    Вынесено из роута, чтобы прогрев кеша (_warmup_cache/_periodic_warm в
    main.py) наполнял его напрямую, минуя tier-проверку — ровно как
    build_stocks_heatmap. Прогрев ходит по HTTP без токена, т.е. выглядит
    гостем, и после закрытия скринера (2026-08-09) получал бы locked-маркер:
    кэш не грелся бы вовсе, а первый платный запрос каждые 5 минут платил бы
    за холодный пересчёт (0.9–1.1с против 0.2с). Tier-gating остаётся на роуте.
    """
    from api.cache import get_or_compute
    from api.services.oi_screener import compute_screener
    # v13 = + горизонт short/medium (ключ раздельный, иначе ленты смешаются).
    # v12 = + intraday_date (честная подпись «дневные за X · интрадей за Y»).
    # v11 = + net_record (истор. экстремум сырой чистой позиции). v10 = скрытие
    # малоактивных активов. v9 = групповой порог ликвидности + npart/min_part.
    # v8 = окна рекордов. v7 = интрадей только за торг. дни. v6 = + интрадей.
    return get_or_compute(
        f"oi_screener:v13:{clgroup}:{horizon}",
        lambda: compute_screener(db, clgroup, horizon),
        ttl=300,
    )


@oi_router.get("/screener")
def get_oi_screener(
        clgroup: ClgroupType = Query("FIZ", description="Группа: FIZ (физлица) или YUR (юрлица)"),
        horizon: str = Query("short", pattern="^(short|medium)$", description="Горизонт: short (день) или medium (14 дней)"),
        db: Session = Depends(get_db),
        user = Depends(get_current_user_optional),
):
    """Скринер сигналов ОИ: чистая позиция группы + кратность движения по всем
    фьючерсам разом (вкладка «Скринер сигналов» на /oi).

    horizon short — движение за день против ATR(14) (как алерты); medium —
    сдвиг за 14 торговых дней против нормы за 60 дней до окна. Это две разные
    ленты, фронт переключает их тумблером; кэшируются раздельно.

    Данные дневные (T+1), меняются раз в сутки → single-flight кэш 5 мин;
    _periodic_warm в main.py перегревает все 4 комбинации каждые 240с, так
    что живой пересчёт на потоке запроса — только сразу после деплоя/flush.
    Открытый доступ (как публичная лента аномалий): дневная агрегированная
    статистика, не премиум-данные.

    ⚠️ РАЗВОРОТ 2026-08-09 (решение владельца). Абзац выше оставлен как есть —
    это ход рассуждения, по которому скринер когда-то открыли, и его полезно
    видеть. Но решение изменено: лента закрыта для гостя и free (матрица,
    ключ `oi_screener.open`). Аргумент против прежнего: агрегат тут и есть
    продукт — это готовый вывод «что резко сдвинулось сегодня по всему рынку»,
    тот же, что уходит в платные Telegram-алерты; «не сырые данные» перестаёт
    быть доводом за бесплатность ровно в тот момент, когда за агрегат платят.

    Locked-тиру отдаём МАРКЕР БЕЗ СТРОК (rows=[], locked=true) — как свежему
    срезу в /fund-trades. Блюр рисует фронт по фейковому скелетону; настоящих
    цифр под ним нет, снять их инспектором нечего.
    """
    limits = get_indicator_limits(user_tier(user), "oi_screener")
    if not limits.get("open", True):
        return {
            "locked": True,
            "required_tier": "basic",
            "signal_date": None,
            "intraday_date": None,
            "clgroup": clgroup,
            "horizon": horizon,
            "window_days": 14 if horizon == "medium" else 1,
            "sharp_ratio": 3 if horizon == "medium" else 2,
            "min_part": 0,
            "rows": [],
        }

    return build_oi_screener(db, clgroup, horizon)


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


@oi_router.get("/low-activity-assets")
def get_low_activity_assets(db: Session = Depends(get_db)):
    """sectype фьючерсов с НИЗКОЙ активностью физлиц — прячутся в пикере ОИ.

    Активность актива = медиана числа физлиц-трейдеров (pos_long_num +
    pos_short_num, clgroup=FIZ) за последние дни. Актив малоактивен, если эта
    медиана < порога = max(floor, dyn_share × сумма медиан по рынку). Фронт
    прячет такие активы из дефолтного списка, но раскрывает их поиском/избранным;
    при росте активности медиана снова переходит порог и актив возвращается сам.

    Тот же расчёт (общий кэш 1 час) прячет их и из ленты «Скринер сигналов» —
    единый принцип релевантности. Логика в services/oi_screener.low_activity.
    """
    from api.services.oi_screener import low_activity
    return low_activity(db)


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