"""
ApiKey — модель для personal API-ключей Pro-юзеров.

Архитектура security:
  - При создании генерируем `pk_live_<32_chars>` (43 char total).
  - В БД сохраняем sha256(key) + key_prefix (первые 12 char для display).
  - Юзеру plain-text показываем РОВНО ОДИН РАЗ при создании.
  - Subsequent list-запросы возвращают только prefix + metadata.

Аутентификация:
  - Frontend SPA — JWT (Authorization: Bearer ...)
  - Programmatic access — API key (X-API-Key: pk_live_...)
  - Один юзер может иметь несколько ключей (separate envs/scripts).

Связь с User: CASCADE при удалении аккаунта.
"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.sql import func

from api.database import Base


class ApiKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (
        Index("idx_api_keys_user", "user_id"),
        Index("idx_api_keys_hash", "key_hash"),  # для lookup при auth
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # sha256(plain_key) — 64 hex chars. Поиск при auth через WHERE key_hash = ...
    key_hash = Column(String(64), nullable=False, unique=True)

    # Первые 12 chars от plain key — для display в UI ("pk_live_abcd1234...").
    # Не secret — даже если утечёт, не позволяет создать valid ключ.
    key_prefix = Column(String(20), nullable=False)

    # Юзерское имя для удобной идентификации ("prod-бот", "test-script").
    name = Column(String(100), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_used_at = Column(DateTime, nullable=True)

    # Soft delete — оставляем строку для audit, но key_hash больше не работает.
    is_revoked = Column(Boolean, nullable=False, default=False)
    revoked_at = Column(DateTime, nullable=True)
