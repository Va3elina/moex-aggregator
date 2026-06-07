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
# @username бота (без @). Берётся из .env при деплое; для deep-link t.me/<username>.
ALERT_BOT_USERNAME = os.getenv("ALERT_BOT_USERNAME", "")


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
    bot = ALERT_BOT_USERNAME or "BOT_USERNAME"
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
    last_fired_at: Optional[str] = None
    created_at: Optional[str] = None


def _to_out(a: Alert) -> AlertOut:
    return AlertOut(
        id=a.id, indicator=a.indicator, asset=a.asset, asset_name=a.asset_name,
        metric=a.metric, clgroup=a.clgroup, op=a.op, threshold=float(a.threshold),
        mode=a.mode, status=a.status,
        last_fired_at=a.last_fired_at.isoformat() if a.last_fired_at else None,
        created_at=a.created_at.isoformat() if a.created_at else None,
    )


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
    if body.op not in ("gt", "lt", "cross_up", "cross_down"):
        raise HTTPException(status_code=422, detail="Некорректное условие")
    if body.mode not in ("once", "repeat"):
        raise HTTPException(status_code=422, detail="Некорректный режим")

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
        status="active",
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return _to_out(alert)


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
