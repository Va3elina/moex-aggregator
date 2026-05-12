"""
Модель сохранённого способа оплаты пользователя (карта для рекуррентов).

T-Bank flow:
1. Юзер первый раз оплачивает с галочкой "Сохранить карту"
   → backend Init с Recurrent="Y" + CustomerKey=str(user_id)
2. После CONFIRMED T-Bank в webhook'е возвращает RebillId — токен карты
3. Создаём запись в user_payment_methods (или находим уже существующую
   через UNIQUE (user_id, rebill_id) WHERE deleted_at IS NULL)
4. Привязываем subscription.payment_method_id к этой записи

Для последующих списаний:
- /v2/Charge с PaymentId нового Init'а + сохранённый RebillId
- 3DS не запрашивается (карта уже верифицирована при первом платеже)
- Юзер ничего не делает — auto-renewal cron работает сам

Soft delete:
- deleted_at NULL → карта активна, видна в UI, можно использовать для charge
- deleted_at NOT NULL → юзер удалил, не показываем, не используем
  (не дропаем строку т.к. на неё ссылаются subscriptions для истории)
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from api.database import Base


class UserPaymentMethod(Base):
    """Сохранённая карта пользователя для рекуррентных списаний."""
    __tablename__ = "user_payment_methods"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    # === Провайдер ===
    # 'tbank' (на сегодня единственный поддерживающий рекурренты у нас).
    # YooKassa тоже умеет (payment_method_data), но мы её не активируем
    # для recurrent flow. Stub-режим вообще не имеет такой сущности.
    provider = Column(String(16), nullable=False, default="tbank")

    # === Токен карты у провайдера ===
    # T-Bank RebillId — постоянный идентификатор сохранённой карты,
    # привязанный к CustomerKey терминала.
    rebill_id = Column(String(64), nullable=False)

    # CustomerKey, под которым карта зарегистрирована в T-Bank.
    # У нас это str(user_id). Сохраняем для дебага и для случая если
    # потребуется удалить карту через T-Bank API (DeleteCard endpoint
    # требует CustomerKey + CardId, последний приходит в Pan).
    customer_key = Column(String(64), nullable=True)

    # === Display info (для UI «Сохранённые карты») ===
    # T-Bank возвращает Pan вида "430000******0333" — last4 = "0333"
    card_last4 = Column(String(4), nullable=True)
    # CardType: 'VISA' / 'MASTERCARD' / 'MIR' / 'MAESTRO' / 'JCB' / 'UNIONPAY'
    card_brand = Column(String(16), nullable=True)

    # === Behavior flags ===
    # Если у юзера >1 карты — какую использовать для auto-renewal.
    # Default: первая привязанная (создаём с True если ни одной активной нет).
    is_default = Column(Boolean, nullable=False, default=False)

    # === Lifecycle ===
    # Last_used_at обновляется при каждом успешном Charge.
    # Полезно для UI ("использовалась 3 дня назад") и для cleanup'а
    # неактивных карт (если 12+ месяцев не использовалась).
    last_used_at = Column(DateTime(timezone=True), nullable=True)
    # Soft delete — юзер удалил карту через UI. Не дропаем т.к. на запись
    # ссылаются subscriptions.payment_method_id (для истории).
    deleted_at = Column(DateTime(timezone=True), nullable=True)

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

    __table_args__ = (
        # Одна карта (по rebill_id) у юзера может быть привязана только один раз
        # из числа НЕ удалённых. Если удалили и снова привязали — две строки,
        # но активная (без deleted_at) опять только одна.
        # Создаётся миграцией как partial unique index `uq_payment_methods_active`.
        Index(
            "idx_payment_methods_user_active",
            "user_id",
            postgresql_where="deleted_at IS NULL",
        ),
    )

    def __repr__(self):
        return (
            f"<UserPaymentMethod id={self.id} user={self.user_id} "
            f"{self.card_brand or '?'} ****{self.card_last4 or '?'}>"
        )

    @property
    def display_name(self) -> str:
        """Для UI: 'VISA ****0333' либо 'Карта ****0333' если brand неизвестен."""
        brand = self.card_brand or "Карта"
        last4 = self.card_last4 or "????"
        return f"{brand} ····{last4}"
