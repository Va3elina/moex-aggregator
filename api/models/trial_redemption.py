# api/models/trial_redemption.py
"""
Анти-абуз пробного периода: один триал на идентичность.

Запись создаётся в момент УСПЕШНОЙ привязки карты (не на старте — иначе
брошенная привязка сожгла бы право на триал). Кросс-аккаунтные ключи дедупа
хранятся ТОЛЬКО как солёные SHA-256 (152-ФЗ: не сырьё, env TRIAL_HASH_SALT):
  oauth_hash       — sha256('vk:322700506' + salt)
  email_hash       — sha256(нормализованный email + salt) (gmail: срезаны точки
                     и +алиасы ДО хеширования)
  card_fingerprint — sha256(маскированный PAN + salt). Стабилен между
                     CustomerKey'ями (в отличие от rebill_id). Заполняется ТОЛЬКО
                     если T-Bank подтвердит стабильный отпечаток, иначе NULL.

См. db/migrations/014_trial.sql.
"""
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.sql import func

from api.database import Base


class TrialRedemption(Base):
    """Факт использования пробного периода — переживает удаление подписки."""
    __tablename__ = "trial_redemptions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="SET NULL"),
        nullable=True,
    )
    tier = Column(String(16), nullable=False)            # basic / pro
    oauth_hash = Column(String(64), nullable=True)       # sha256(provider:oauth_id + salt)
    email_hash = Column(String(64), nullable=True)       # sha256(email_norm + salt)
    card_fingerprint = Column(String(64), nullable=True) # sha256(masked PAN + salt)
    ip = Column(String(64), nullable=True)               # мягкий сигнал (не блок)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("user_id", name="uq_trial_redemptions_user"),
        Index("idx_trial_redemptions_oauth", "oauth_hash"),
        Index("idx_trial_redemptions_email", "email_hash"),
        Index("idx_trial_redemptions_card", "card_fingerprint"),
    )

    def __repr__(self):
        return f"<TrialRedemption user={self.user_id} tier={self.tier}>"
