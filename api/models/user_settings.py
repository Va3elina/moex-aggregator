# api/models/user_settings.py
"""
Синхронизация настроек юзера между устройствами (браузерами).

Один JSONB-блоб на юзера: плоский словарь {localStorage-ключ: сырая строка
значения}. Что именно отслеживать, решает фронт (services/settingsSync.ts) —
бэкенд хранит блоб как есть, last-write-wins целиком.
"""
from sqlalchemy import Column, Integer, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from api.database import Base


class UserSettings(Base):
    __tablename__ = "user_settings"

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    data = Column(JSONB, nullable=False, default=dict, server_default="{}")
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
