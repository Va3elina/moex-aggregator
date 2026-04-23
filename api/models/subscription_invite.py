# api/models/subscription_invite.py
"""
Модели для redeem-ссылок (комплимент-подписки).

SubscriptionInvite — токен, который генерирует админ. Может быть одноразовым
(max_uses=1) или многоразовым (max_uses=N, например для конкурсов).

InviteRedemption — история "кто когда какой токен применил". Уникальная пара
(token, user_id) чтобы один user не мог применить один токен дважды.
"""
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from api.database import Base


class SubscriptionInvite(Base):
    """Токен-ссылка для комплимент-активации подписки."""
    __tablename__ = "subscription_invites"

    token = Column(String(64), primary_key=True)
    tier = Column(String(16), nullable=False)                       # basic/pro/premium
    duration_days = Column(Integer, nullable=False)                 # срок подписки после redeem'а
    max_uses = Column(Integer, nullable=False, default=1)
    uses_count = Column(Integer, nullable=False, default=0)
    expires_at = Column(DateTime(timezone=True), nullable=False)    # токен сам по себе истекает
    created_by_user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    revoked_at = Column(DateTime(timezone=True), nullable=True)     # если админ отозвал
    note = Column(String(255), nullable=True)                        # заметка админа
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("idx_invites_by_admin", "created_by_user_id", "created_at"),
    )

    def __repr__(self):
        return (
            f"<Invite token={self.token[:8]}... tier={self.tier} "
            f"uses={self.uses_count}/{self.max_uses}>"
        )


class InviteRedemption(Base):
    """Кто какой токен применил."""
    __tablename__ = "subscription_invite_redemptions"

    id = Column(Integer, primary_key=True)
    token = Column(
        String(64),
        ForeignKey("subscription_invites.token", ondelete="CASCADE"),
        nullable=False,
    )
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
    redeemed_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("token", "user_id", name="uq_redemption_token_user"),
    )
