"""
Watchlist — список избранных тикеров на главной странице.

Каждый юзер имеет личный watchlist (до ~50 тикеров). UI на overview
показывает мини-карточки с текущей ценой, change%, sparkline, и dot-
индикаторами (above EMA, volume spike, etc). Тапа — deep-link на
соответствующий индикатор с предзаполненным фокусом.

Связь с User — CASCADE при удалении аккаунта.
"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.sql import func

from api.database import Base


class UserWatchlist(Base):
    __tablename__ = "user_watchlists"
    __table_args__ = (
        # Один тикер на юзера — не дублируем.
        UniqueConstraint('user_id', 'ticker', name='uq_user_watchlist_ticker'),
        # Index для быстрого list-запроса с сортировкой по position.
        Index('idx_user_watchlist_user_position', 'user_id', 'position'),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    ticker = Column(String(20), nullable=False)
    # Position позволяет юзеру переупорядочить список (drag-n-drop в UI).
    # 0 = первый. При добавлении нового — auto-increment max+1.
    position = Column(Integer, nullable=False, default=0)
    added_at = Column(DateTime, server_default=func.now(), nullable=False)
