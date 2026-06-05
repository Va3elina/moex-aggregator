# api/models/user.py
"""
Модель пользователя для базы данных.
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, UniqueConstraint
from sqlalchemy.sql import func
from enum import Enum

from api.database import Base


class UserRole(str, Enum):
    """
    Роли пользователей.
    ВАЖНО: значения должны быть lowercase — так они хранятся в PostgreSQL!
    """
    USER = "user"  # ← lowercase!
    PRO = "pro"
    ADMIN = "admin"


class User(Base):
    """Таблица пользователей."""
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint('oauth_provider', 'oauth_id', name='uq_oauth_provider_id'),
    )

    # === Идентификация ===
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)

    # === Безопасность ===
    hashed_password = Column(String(255), nullable=True)  # nullable для OAuth

    # === Профиль ===
    display_name = Column(String(255), nullable=True)
    avatar_url = Column(String(500), nullable=True)

    # === OAuth ===
    oauth_provider = Column(String(20), nullable=True)  # "google", "telegram", "vk"
    oauth_id = Column(String(255), nullable=True)         # ID у провайдера

    # === Роль и доступ ===
    # Используем String вместо SQLEnum для совместимости с существующим enum в БД
    role = Column(String(20), default="user", nullable=False)

    is_active = Column(Boolean, default=True, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)
    # Одноразовая retention-скидка при отмене автопродления — использована?
    retention_used = Column(Boolean, default=False, nullable=False, server_default="false")

    # === Подтверждение email (Phase 2 — SMTP Yandex 360) ===
    # Заполняются только для email+password юзеров. OAuth-юзеры приходят с
    # is_verified=True (провайдер подтвердил) и этих полей не касаются.
    email_verify_code = Column(String(6), nullable=True)               # 6-значный код
    email_verify_expires_at = Column(DateTime(timezone=True), nullable=True)  # срок кода (30 мин)
    email_verify_attempts = Column(Integer, default=0, nullable=False)  # неверные попытки
    email_verify_sent_at = Column(DateTime(timezone=True), nullable=True)     # время последней отправки (кулдаун)

    # === Защита от брутфорса ===
    failed_login_attempts = Column(Integer, default=0, nullable=False)
    locked_until = Column(DateTime(timezone=True), nullable=True)

    # === Метаданные ===
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )

    last_login_at = Column(DateTime(timezone=True), nullable=True)
    last_login_ip = Column(String(45), nullable=True)

    def __repr__(self):
        return f"<User {self.email} ({self.role})>"


class RefreshToken(Base):
    """Таблица refresh токенов."""
    __tablename__ = "refresh_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)
    token_hash = Column(String(255), unique=True, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    is_revoked = Column(Boolean, default=False, nullable=False)
    user_agent = Column(String(500), nullable=True)
    ip_address = Column(String(45), nullable=True)