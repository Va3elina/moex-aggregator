"""«Главное» (/hot) — витрина находок: позиции физлиц, деньги и сделки фондов, сезонность.

Пока только для админов (страница в меню «+» тестовых индикаторов). Отбор и подписи — api/services/hot.py,
графики фронт берёт из тех же ручек, что и страницы индикаторов.
"""
from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.cache import get_or_compute
from api.database import get_db
from api.models import User
from api.routers.auth import require_admin
from api.services.hot import compute_hot

router = APIRouter(prefix="/api/admin/hot", tags=["hot"])


@router.get("")
def hot(admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    """Карточки витрины с рядами для графиков. Пересчёт — раз в 30 минут: позиции и потоки обновляются
    раз в день. Версия в ключе кэша — формат ответа (v9: прошлые случаи — периоды и зоны)."""
    return get_or_compute(f"hot:v9:{date.today().isoformat()}", lambda: compute_hot(db, admin), ttl=1800)
