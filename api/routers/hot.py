"""«Главное» (/hot) — витрина находок: позиции физлиц, деньги и сделки фондов, сезонность.

Пока только для админов (страница в меню «+» тестовых индикаторов). Отбор и подписи — api/services/hot.py,
графики фронт берёт из тех же ручек, что и страницы индикаторов.
"""
import threading

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.cache import _get_redis, get_or_compute, get_or_set
from api.database import SessionLocal, get_db
from api.logger import get_logger
from api.models import User
from api.routers.auth import require_admin
from api.services.hot import compute_hot

router = APIRouter(prefix="/api/admin/hot", tags=["hot"])
log = get_logger()

# Версия — формат ответа (v13: сезонность по всем активам, серии фондов с перерывами).
VER = "hot:v13"
LATEST, FRESH, LOCK = f"{VER}:latest", f"{VER}:fresh", f"{VER}:refresh"
FRESH_TTL = 1800          # через 30 минут витрину пересчитываем — в фоне
KEEP_TTL = 3 * 24 * 3600  # последняя готовая витрина живёт трое суток — её отдаём сразу


def _refresh(admin_id: int) -> None:
    """Пересчёт в фоне со своей сессией БД; один на всех воркеров (Redis-лок)."""
    try:
        if not _get_redis().set(LOCK, "1", nx=True, ex=300):
            return
    except Exception:  # noqa: BLE001 — без Redis фоновый пересчёт не делаем, отдадим что есть
        return
    try:
        with SessionLocal() as db:
            admin = db.get(User, admin_id)
            out = compute_hot(db, admin)
        get_or_set(LATEST, out, KEEP_TTL)
        get_or_set(FRESH, 1, FRESH_TTL)
    except Exception as e:  # noqa: BLE001
        log.warning(f"hot: фоновый пересчёт упал: {e}")
    finally:
        try:
            _get_redis().delete(LOCK)
        except Exception:  # noqa: BLE001
            pass


@router.get("")
def hot(admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    """Карточки витрины с рядами для графиков. Отдаём последнюю готовую витрину сразу (расчёт ~10 с);
    если ей больше 30 минут — пересчитываем в фоне, следующий заход увидит свежую."""
    latest = get_or_set(LATEST)
    if latest is not None:
        if get_or_set(FRESH) is None:
            threading.Thread(target=_refresh, args=(admin.id,), daemon=True).start()
        return latest

    def first():
        out = compute_hot(db, admin)
        get_or_set(LATEST, out, KEEP_TTL)
        get_or_set(FRESH, 1, FRESH_TTL)
        return out
    return get_or_compute(f"{VER}:first", first, ttl=FRESH_TTL)
