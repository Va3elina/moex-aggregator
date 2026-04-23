# api/models/subscription.py
"""
Модель подписки пользователя.

Источник истины для истории платежей и дат истечения Pro-доступа.
Поле users.role ('user' / 'pro' / 'admin') — derived state, обновляется
webhook'ами от ЮKassa и cron-джобой при истечении подписок.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Numeric,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from api.database import Base


# Статусы подписки — соответствуют lifecycle'у ЮKassa payment:
#   pending   — платёж создан, ждём подтверждения от пользователя
#   active    — оплачен, pro-доступ включён, идёт период действия
#   cancelled — пользователь отменил авто-продление (access до expires_at)
#   expired   — период истёк, авто-продление не удалось
#   failed    — платёж не прошёл (отказ банка, отмена)
#   refunded  — возврат средств (access отозван)
SUBSCRIPTION_STATUSES = (
    "pending",
    "active",
    "cancelled",
    "expired",
    "failed",
    "refunded",
)

# Периоды (как часто платит пользователь)
SUBSCRIPTION_PERIODS = (
    "monthly",
    "yearly",
)

# Tier-уровни — источник истины в api/billing/plans.py::TIER_LEVELS
SUBSCRIPTION_TIERS = (
    "basic",
    "pro",
    "premium",
)


class Subscription(Base):
    """Платёж/подписка пользователя."""
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # === Параметры подписки ===
    tier = Column(String(16), nullable=False)              # 'basic' / 'pro' / 'premium' — уровень доступа
    period = Column(String(16), nullable=False)            # 'monthly' / 'yearly' — период оплаты
    plan_id = Column(String(32), nullable=False)           # 'pro_monthly' — связка tier+period (SKU)
    amount = Column(Numeric(10, 2), nullable=False)        # сумма в рублях
    currency = Column(String(3), default="RUB", nullable=False)

    # === Lifecycle ===
    status = Column(String(16), default="pending", nullable=False, index=True)
    started_at = Column(DateTime(timezone=True), nullable=True)   # когда включился Pro
    expires_at = Column(DateTime(timezone=True), nullable=True)   # когда заканчивается
    cancelled_at = Column(DateTime(timezone=True), nullable=True)

    # === ЮKassa реквизиты ===
    # yk_payment_id — id платежа в ЮKassa, заполняется при создании checkout-сессии.
    # Используется в webhook'е для лукапа нашей подписки по id от провайдера.
    yk_payment_id = Column(String(64), unique=True, nullable=True, index=True)
    # yk_method — каким способом оплачено: 'bank_card' / 'sbp' / 'sberpay' / 'yoo_money' / ...
    # Заполняется в webhook, используется для аналитики конверсии.
    yk_method = Column(String(32), nullable=True)

    # === Метаданные ===
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # === Индексы ===
    # Для cron'а "кто скоро истекает" — WHERE status='active' ORDER BY expires_at
    __table_args__ = (
        Index("idx_subs_active_expires", "status", "expires_at"),
    )

    def __repr__(self):
        return f"<Subscription id={self.id} user={self.user_id} plan={self.plan_id} status={self.status}>"
