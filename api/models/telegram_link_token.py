# api/models/telegram_link_token.py
"""
Одноразовый токен привязки Telegram (deep-link /start <token>).

Сайт генерит токен на POST /api/alerts/telegram/link → юзер открывает
t.me/<bot>?start=<token> → host-side бот-поллер (signals/alert_bot.py) ловит
/start, по токену находит user_id, ставит users.telegram_chat_id и удаляет токен.

В БД (а не Redis): бот живёт на ХОСТЕ, host↔DB надёжен (как signal-engine),
host↔Redis не гарантирован.
"""
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.sql import func

from api.database import Base


class TelegramLinkToken(Base):
    __tablename__ = "telegram_link_tokens"

    token = Column(String(64), primary_key=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)  # now() + 10 мин
