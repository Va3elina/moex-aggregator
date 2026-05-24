"""
API keys management endpoints для Pro-юзеров.

Endpoints:
  POST   /api/keys              — создать новый ключ (returns plain key один раз)
  GET    /api/keys              — list ключей юзера (без plain text)
  DELETE /api/keys/{id}         — revoke ключ
  GET    /api/keys/usage        — статистика использования за последние 30 дней

Все require_pro. Лимит: 10 active keys на юзера.
"""
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from api.cache import _get_redis as get_redis
from api.database import get_db
from api.models import ApiKey, User
from api.routers.auth import get_current_user, require_pro
from api.security.api_key import generate_api_key

router = APIRouter(prefix="/api/keys", tags=["api-keys"])

MAX_KEYS_PER_USER = 10


class ApiKeyCreateRequest(BaseModel):
    name: str | None = Field(None, max_length=100)
    # 'live' (default, требует Pro) или 'test' (для разработки, без подписки)
    mode: str = Field("live", pattern="^(live|test)$")


class ApiKeyOut(BaseModel):
    """Public-safe representation — без plain text."""
    id: int
    name: str | None
    key_prefix: str
    mode: str
    created_at: datetime
    last_used_at: datetime | None
    is_revoked: bool

    class Config:
        from_attributes = True


class ApiKeyCreateResponse(BaseModel):
    """Возвращается ОДИН раз при создании ключа — содержит plain text."""
    id: int
    name: str | None
    key_prefix: str
    mode: str
    plain_key: str
    created_at: datetime


@router.get("", response_model=list[ApiKeyOut])
def list_keys(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Список ключей юзера (active + revoked, без plain text).

    Доступно любому залогиненному юзеру (не require_pro) — потому что
    test-ключи мог создать ещё-не-Pro юзер и должен иметь доступ к их
    управлению.
    """
    rows = db.execute(
        select(ApiKey)
        .where(ApiKey.user_id == user.id)
        .order_by(ApiKey.created_at.desc())
    ).scalars().all()
    return rows


@router.post("", response_model=ApiKeyCreateResponse, status_code=status.HTTP_201_CREATED)
def create_key(
    body: ApiKeyCreateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Создать новый API-ключ. Plain text возвращается ОДИН раз — юзер должен
    сохранить. Subsequent GET возвращает только metadata.

    Gating:
      - mode='live': требует Pro tier (require_pro check внутри).
      - mode='test': любой залогиненный юзер — для разработки без оплаты.
    """
    if body.mode == "live" and user.role not in ("pro", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "Live-ключи доступны только на тарифе Pro. "
                "Создайте test-ключ для разработки или оформите Pro."
            ),
        )

    # Лимит active keys на юзера — общий для live + test.
    active_count = db.execute(
        select(func.count(ApiKey.id))
        .where(ApiKey.user_id == user.id, ApiKey.is_revoked == False)  # noqa: E712
    ).scalar_one()
    if active_count >= MAX_KEYS_PER_USER:
        raise HTTPException(
            status_code=400,
            detail=f"Лимит активных ключей: {MAX_KEYS_PER_USER}. Удалите ненужные.",
        )

    plain_key, key_hash, key_prefix = generate_api_key(mode=body.mode)

    new_key = ApiKey(
        user_id=user.id,
        key_hash=key_hash,
        key_prefix=key_prefix,
        name=body.name,
        mode=body.mode,
    )
    db.add(new_key)
    db.commit()
    db.refresh(new_key)

    return ApiKeyCreateResponse(
        id=new_key.id,
        name=new_key.name,
        key_prefix=new_key.key_prefix,
        mode=new_key.mode,
        plain_key=plain_key,
        created_at=new_key.created_at,
    )


@router.get("/usage")
def get_usage_stats(
    days: int = 30,
    user: User = Depends(get_current_user),
):
    """
    Статистика использования API за последние `days` дней.

    Returns:
        {
            "days": 30,
            "total": 1245,
            "by_day": [
                {"date": "2026-04-25", "count": 42},
                {"date": "2026-04-26", "count": 38},
                ...
            ]
        }

    Источник: Redis counters api_usage:{user_id}:{YYYY-MM-DD} (TTL 32 дня),
    обновляются в `check_rate_limit` при каждом успешном API-запросе.
    Если Redis недоступен — возвращает пустую серию.
    """
    if days < 1 or days > 90:
        raise HTTPException(status_code=400, detail="days must be between 1 and 90")

    redis = get_redis()
    by_day = []
    total = 0

    if redis:
        today = date.today()
        keys = []
        for i in range(days):
            d = today - timedelta(days=i)
            keys.append((d.isoformat(), f"api_usage:{user.id}:{d.isoformat()}"))
        try:
            # MGET — атомарный batch lookup, гораздо быстрее N отдельных GET'ов.
            values = redis.mget([k for _, k in keys])
            for (date_str, _), val in zip(keys, values):
                count = 0
                if val is not None:
                    try:
                        count = int(val.decode() if isinstance(val, bytes) else val)
                    except (ValueError, AttributeError):
                        count = 0
                by_day.append({"date": date_str, "count": count})
                total += count
        except Exception:
            # Redis ошибка — возвращаем пустую серию вместо 500.
            by_day = [{"date": d.isoformat(), "count": 0}
                      for d in (date.today() - timedelta(days=i) for i in range(days))]

    # Сортируем по дате ASC чтобы chart рендерился в правильном порядке.
    by_day.sort(key=lambda x: x["date"])
    return {"days": days, "total": total, "by_day": by_day}


@router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_key(
    key_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Soft delete — отзывает ключ. Запись остаётся для audit."""
    api_key = db.execute(
        select(ApiKey).where(ApiKey.id == key_id, ApiKey.user_id == user.id)
    ).scalar_one_or_none()
    if not api_key:
        raise HTTPException(status_code=404, detail="Ключ не найден")
    if api_key.is_revoked:
        return None  # idempotent
    api_key.is_revoked = True
    api_key.revoked_at = datetime.utcnow()
    db.commit()
    return None
