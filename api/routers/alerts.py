"""
/api/alerts/* — Telegram alert-bot.

Phase 1 (привязка Telegram):
  POST   /api/alerts/telegram/link    — сгенерировать deep-link токен привязки (auth)
  GET    /api/alerts/telegram/status  — привязан ли Telegram у юзера (auth)
  DELETE /api/alerts/telegram         — отвязать Telegram (auth)

Phase 2 добавит CRUD алертов (создать/список/пауза/удалить) с квота-гейтом по тарифу
(free=0 → апгрейд, basic=20, pro=∞ — см. features.py telegram_alerts_quota).

Привязка работает через одноразовый токен (telegram_link_tokens) + deep-link
t.me/<bot>?start=<token>: бот не может писать юзеру первым, поэтому юзер сам жмёт /start.
"""
import os
import secrets
from datetime import datetime, timedelta, timezone

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User, Alert
from api.models.telegram_link_token import TelegramLinkToken
from api.routers.auth import get_current_user
from api.billing.tiers import user_tier
from api.billing.features import get_common_features

router = APIRouter(prefix="/api/alerts", tags=["alerts"])

LINK_TOKEN_TTL_MIN = 10
# @username бота (без @) для deep-link t.me/<username>. Дефолт = реальный бот
# (публичный стабильный факт, не секрет); env ALERT_BOT_USERNAME может переопределить.
ALERT_BOT_USERNAME = os.getenv("ALERT_BOT_USERNAME", "framesignalbot")


class LinkResponse(BaseModel):
    deep_link: str
    token: str
    expires_at: str


class TelegramStatus(BaseModel):
    linked: bool
    username: str | None = None


@router.post("/telegram/link", response_model=LinkResponse)
def create_link(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Одноразовый токен привязки. Старые токены этого юзера чистим (один активный)."""
    db.query(TelegramLinkToken).filter(TelegramLinkToken.user_id == user.id).delete()
    token = secrets.token_urlsafe(24)
    expires = datetime.now(timezone.utc) + timedelta(minutes=LINK_TOKEN_TTL_MIN)
    db.add(TelegramLinkToken(token=token, user_id=user.id, expires_at=expires))
    db.commit()
    bot = ALERT_BOT_USERNAME or "framesignalbot"
    return LinkResponse(
        deep_link=f"https://t.me/{bot}?start={token}",
        token=token,
        expires_at=expires.isoformat(),
    )


@router.get("/telegram/status", response_model=TelegramStatus)
def telegram_status(user: User = Depends(get_current_user)):
    return TelegramStatus(
        linked=user.telegram_chat_id is not None,
        username=user.telegram_username,
    )


@router.delete("/telegram")
def unlink_telegram(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user.telegram_chat_id = None
    user.telegram_username = None
    user.telegram_linked_at = None
    db.commit()
    return {"ok": True}


# ─── Контекст для окна создания (текущая цена + intraday-доступность) ────────
# Лёгкий read-эндпоинт: фронт подтягивает свежую цену фьючерса, чтобы показать
# «Сейчас: <value> руб», задать placeholder порога и решить, есть ли у актива
# внутридневные данные (intraday) — от этого зависит частота проверки алерта.

class AlertPriceContext(BaseModel):
    value: Optional[float] = None
    ts: Optional[str] = None
    interval: Optional[int] = None
    intraday: bool = False


class AlertContextOut(BaseModel):
    price: AlertPriceContext


@router.get("/context", response_model=AlertContextOut)
def alert_context(
    indicator: str = Query(...),
    asset: str = Query(...),
    clgroup: str = Query("FIZ"),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Свежайшая цена фьючерса для окна создания алерта.

    Быстрый индексный запрос (sec_id IN (...), НЕ secid LIKE — тот даёт Seq-Scan
    по 8ГБ candles). Окно begin_time >= now()-14д ложится на индекс
    idx_candles_sec_interval_time. interval ∈ {5,60} → intraday=true.
    """
    row = db.execute(
        text("""
            SELECT close, begin_time, interval
            FROM candles
            WHERE sec_id IN (SELECT sec_id FROM instruments
                             WHERE sectype = :asset AND type = 'futures')
              AND interval IN (5, 60, 24)
              AND close > 0
              AND begin_time >= now() - interval '14 days'
            ORDER BY begin_time DESC, volume DESC NULLS LAST
            LIMIT 1
        """),
        {"asset": asset},
    ).first()

    if row is None:
        return AlertContextOut(price=AlertPriceContext())

    close, begin_time, interval = row[0], row[1], row[2]
    interval = int(interval) if interval is not None else None
    return AlertContextOut(price=AlertPriceContext(
        value=float(close) if close is not None else None,
        ts=begin_time.isoformat() if begin_time is not None else None,
        interval=interval,
        intraday=interval in (5, 60),
    ))


# ─── Алерты (CRUD) ──────────────────────────────────────────────────────────
# Квота по тарифу (features.py telegram_alerts_quota): free=0, basic=20, pro=∞(None).
# Гейт на СОЗДАНИИ: 0 → апгрейд до Basic; лимит достигнут → апгрейд до Pro.
# 403-сообщения содержат «тарифе»+«Basic/Pro» (под фронтовый tierError.ts).

class AlertCreate(BaseModel):
    # max_length зеркалит лимиты колонок БД — иначе длинная строка даёт 500
    # (Postgres DataError) вместо чистого 422.
    indicator: str = Field(max_length=24)        # 'price' | 'oi_zscore'
    asset: str = Field(max_length=20)
    asset_name: Optional[str] = Field(default=None, max_length=128)
    metric: str = Field(max_length=24)           # 'close' | 'zscore'
    clgroup: Optional[str] = Field(default=None, max_length=3)   # OI: 'FIZ'|'YUR'
    op: str = Field(max_length=16)               # 'gt'|'lt'|'cross_up'|'cross_down'
    threshold: float
    mode: str = "once"                   # 'once'|'repeat'
    cooldown_hours: int = Field(default=24, ge=1, le=720)
    timeframe: str = Field(default="1d", max_length=4)   # '5m'|'1h'|'1d' (OI-метрики)


class AlertOut(BaseModel):
    id: int
    indicator: str
    asset: str
    asset_name: Optional[str] = None
    metric: str
    clgroup: Optional[str] = None
    op: str
    threshold: float
    mode: str
    status: str
    timeframe: str = "1d"
    last_fired_at: Optional[str] = None
    created_at: Optional[str] = None


def _to_out(a: Alert) -> AlertOut:
    return AlertOut(
        id=a.id, indicator=a.indicator, asset=a.asset, asset_name=a.asset_name,
        metric=a.metric, clgroup=a.clgroup, op=a.op, threshold=float(a.threshold),
        mode=a.mode, status=a.status, timeframe=getattr(a, "timeframe", "1d") or "1d",
        last_fired_at=a.last_fired_at.isoformat() if a.last_fired_at else None,
        created_at=a.created_at.isoformat() if a.created_at else None,
    )


# Валидация значений (op/mode/indicator/clgroup) — общая для одиночного и
# пакетного создания. Возвращает текст ошибки или None если всё ок.
def _validate_alert_body(b: AlertCreate) -> Optional[str]:
    if b.op not in ("gt", "lt", "cross_up", "cross_down"):
        return "некорректное условие"
    if b.mode not in ("once", "repeat"):
        return "некорректный режим"
    if b.indicator not in ("price", "oi_zscore", "oi_move", "oi_participants"):
        return "неизвестный индикатор"
    if b.clgroup not in (None, "FIZ", "YUR", "ALL"):
        return "некорректная группа участников"
    if b.timeframe not in ("5m", "1h", "1d"):
        return "некорректный таймфрейм"
    return None


@router.get("", response_model=list[AlertOut])
def list_alerts(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    rows = db.query(Alert).filter(Alert.user_id == user.id).order_by(Alert.created_at.desc()).all()
    return [_to_out(a) for a in rows]


@router.post("", response_model=AlertOut)
def create_alert(
    body: AlertCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    quota = get_common_features(user_tier(user)).get("telegram_alerts_quota", 0)
    if quota == 0:
        raise HTTPException(status_code=403, detail="Алерты доступны на тарифе Basic и Pro")
    if isinstance(quota, int):  # не None (безлимит) → проверяем лимит
        used = db.query(Alert).filter(Alert.user_id == user.id).count()
        if used >= quota:
            raise HTTPException(
                status_code=403,
                detail=f"Достигнут лимит {quota} алертов на вашем тарифе — перейдите на Pro для безлимита",
            )
    err = _validate_alert_body(body)
    if err:
        raise HTTPException(status_code=422, detail=err.capitalize())

    alert = Alert(
        user_id=user.id,
        indicator=body.indicator,
        asset=body.asset,
        asset_name=body.asset_name,
        metric=body.metric,
        clgroup=body.clgroup,
        op=body.op,
        threshold=body.threshold,
        mode=body.mode,
        cooldown_hours=body.cooldown_hours,
        timeframe=body.timeframe,
        status="active",
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return _to_out(alert)


class AlertBatchCreate(BaseModel):
    # Пакетное создание (группа активов) — ОДИН HTTP-запрос вместо N, иначе
    # N параллельных POST'ов бьются о nginx rate-limit (burst=20) и часть
    # падает с 503. Квота проверяется один раз — без гонки счётчика.
    alerts: list[AlertCreate] = Field(min_length=1, max_length=200)


class AlertBatchResult(BaseModel):
    created: int
    skipped: int = 0      # пропущено как дубли уже существующих
    errors: list[str]


def _alert_key(indicator, asset, clgroup, metric, op, threshold, timeframe="1d"):
    # timeframe в ключе: 1d и 5m/1h на один актив — РАЗНЫЕ алерты (разный источник
    # «net сейчас»), не дубли. Дефолт '1d' для существующих строк без таймфрейма.
    return (indicator, asset, clgroup or "", metric, op,
            round(float(threshold), 6), timeframe or "1d")


@router.post("/batch", response_model=AlertBatchResult)
def create_alerts_batch(
    body: AlertBatchCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    quota = get_common_features(user_tier(user)).get("telegram_alerts_quota", 0)
    if quota == 0:
        raise HTTPException(status_code=403, detail="Алерты доступны на тарифе Basic и Pro")
    existing = db.query(Alert).filter(Alert.user_id == user.id).all()
    used = len(existing)
    # remaining=None → безлимит (Pro). Иначе сколько ещё можно создать.
    remaining = None if quota is None else max(0, quota - used)
    # Дедуп: повторный «Создать» на той же группе не плодит дубли.
    seen = {_alert_key(x.indicator, x.asset, x.clgroup, x.metric, x.op, x.threshold,
                       getattr(x, "timeframe", "1d")) for x in existing}

    created = 0
    skipped = 0
    errors: list[str] = []
    for a in body.alerts:
        err = _validate_alert_body(a)
        if err:
            errors.append(f"{a.asset}: {err}")
            continue
        k = _alert_key(a.indicator, a.asset, a.clgroup, a.metric, a.op, a.threshold, a.timeframe)
        if k in seen:
            skipped += 1
            continue
        if remaining is not None and created >= remaining:
            errors.append(f"{a.asset}: достигнут лимит {quota} алертов (Pro — безлимит)")
            continue
        db.add(Alert(
            user_id=user.id,
            indicator=a.indicator, asset=a.asset, asset_name=a.asset_name,
            metric=a.metric, clgroup=a.clgroup, op=a.op, threshold=a.threshold,
            mode=a.mode, cooldown_hours=a.cooldown_hours, timeframe=a.timeframe,
            status="active",
        ))
        seen.add(k)
        created += 1
    db.commit()
    return AlertBatchResult(created=created, skipped=skipped, errors=errors)


@router.patch("/{alert_id}", response_model=AlertOut)
def update_alert(
    alert_id: int,
    status: Optional[str] = None,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    a = db.query(Alert).filter(Alert.id == alert_id, Alert.user_id == user.id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Алерт не найден")
    if status in ("active", "paused"):
        a.status = status
    db.commit()
    db.refresh(a)
    return _to_out(a)


@router.delete("")
def delete_all_alerts(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Удалить ВСЕ алерты пользователя — массовая очистка (если случайно создал
    группу из 100). Возвращает число удалённых."""
    n = db.query(Alert).filter(Alert.user_id == user.id).delete(synchronize_session=False)
    db.commit()
    return {"deleted": int(n)}


@router.delete("/{alert_id}")
def delete_alert(
    alert_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    a = db.query(Alert).filter(Alert.id == alert_id, Alert.user_id == user.id).first()
    if not a:
        raise HTTPException(status_code=404, detail="Алерт не найден")
    db.delete(a)
    db.commit()
    return {"ok": True}
