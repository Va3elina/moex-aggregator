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

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.models.telegram_link_token import TelegramLinkToken
from api.routers.auth import get_current_user

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
