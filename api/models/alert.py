# api/models/alert.py
"""
Пользовательский алерт + история срабатываний (Telegram alert-bot, Phase 2).

Юзер задаёт условие на сайте (актив/метрика/op/порог) → host-side eval-loop
(signals/alerts_run.py, Phase 3) проверяет → бот шлёт пуш. Квота по тарифу
(features.py telegram_alerts_quota): free=0, basic=20, pro=∞.
"""
from sqlalchemy import Column, Integer, String, Numeric, DateTime, Date, ForeignKey, Index
from sqlalchemy.sql import func

from api.database import Base


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    indicator = Column(String(24), nullable=False)   # 'price' | 'oi_zscore'
    asset = Column(String(20), nullable=False)        # sectype
    asset_name = Column(String(120), nullable=True)
    metric = Column(String(24), nullable=False)       # price: 'close'; oi: 'zscore'
    clgroup = Column(String(3), nullable=True)        # OI: 'FIZ'|'YUR'

    op = Column(String(12), nullable=False)           # 'gt'|'lt'|'cross_up'|'cross_down'
    threshold = Column(Numeric(20, 6), nullable=False)

    mode = Column(String(8), nullable=False, default="once", server_default="once")
    cooldown_hours = Column(Integer, nullable=False, default=24, server_default="24")
    status = Column(String(10), nullable=False, default="active", server_default="active")

    last_value = Column(Numeric(20, 6), nullable=True)   # prev-снимок для cross
    last_fired_at = Column(DateTime(timezone=True), nullable=True)
    # Дата ДАННЫХ, на которой алерт сработал в последний раз (для дневных
    # OI-метрик): не пере-выстреливать тот же торговый день. NULL = ещё не срабатывал.
    last_fired_date = Column(Date, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_alerts_user", "user_id"),
    )


class AlertFire(Base):
    __tablename__ = "alert_fires"

    id = Column(Integer, primary_key=True, index=True)
    alert_id = Column(Integer, ForeignKey("alerts.id", ondelete="CASCADE"), nullable=False, index=True)
    fired_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    value = Column(Numeric(20, 6), nullable=True)
    message_text = Column(String, nullable=True)
