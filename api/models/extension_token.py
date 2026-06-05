"""
ExtensionToken — токен для браузерного расширения (терминал Т-Инвестиций).

Архитектура security (зеркалит ApiKey):
  - При создании генерируем `ext_<40_chars>`.
  - В БД храним sha256(token) + token_prefix (первые 12 char для display).
  - Plain-токен показываем юзеру РОВНО ОДИН РАЗ при создании.

Отличие от ApiKey: нет mode — ext-токен всегда «live» (требует PRO). Проверка
PRO делается ВЖИВУЮ при обмене (/api/extension/exchange), а не «зашита» в токен.

Связь с User: CASCADE при удалении аккаунта.
"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.sql import func

from api.database import Base


class ExtensionToken(Base):
    __tablename__ = "extension_tokens"
    __table_args__ = (
        Index("idx_ext_tokens_user", "user_id"),
        Index("idx_ext_tokens_hash", "token_hash"),  # lookup при обмене
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    token_hash = Column(String(64), nullable=False, unique=True)
    token_prefix = Column(String(20), nullable=False)
    name = Column(String(100), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_used_at = Column(DateTime, nullable=True)

    is_revoked = Column(Boolean, nullable=False, default=False)
    revoked_at = Column(DateTime, nullable=True)
