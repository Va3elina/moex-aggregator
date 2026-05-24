"""
API keys management endpoints для Pro-юзеров.

Endpoints:
  POST   /api/keys              — создать новый ключ (returns plain key один раз)
  GET    /api/keys              — list ключей юзера (без plain text)
  DELETE /api/keys/{id}         — revoke ключ

Все require_pro. Лимит: 10 active keys на юзера.
"""
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import ApiKey, User
from api.routers.auth import require_pro
from api.security.api_key import generate_api_key

router = APIRouter(prefix="/api/keys", tags=["api-keys"])

MAX_KEYS_PER_USER = 10


class ApiKeyCreateRequest(BaseModel):
    name: str | None = Field(None, max_length=100)


class ApiKeyOut(BaseModel):
    """Public-safe representation — без plain text."""
    id: int
    name: str | None
    key_prefix: str
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
    plain_key: str
    created_at: datetime


@router.get("", response_model=list[ApiKeyOut])
def list_keys(
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """Список ключей юзера (active + revoked, без plain text)."""
    rows = db.execute(
        select(ApiKey)
        .where(ApiKey.user_id == user.id)
        .order_by(ApiKey.created_at.desc())
    ).scalars().all()
    return rows


@router.post("", response_model=ApiKeyCreateResponse, status_code=status.HTTP_201_CREATED)
def create_key(
    body: ApiKeyCreateRequest,
    user: User = Depends(require_pro),
    db: Session = Depends(get_db),
):
    """
    Создать новый API-ключ. Plain text возвращается ОДИН раз — юзер должен
    сохранить. Subsequent GET возвращает только metadata.
    """
    # Лимит active keys на юзера.
    active_count = db.execute(
        select(func.count(ApiKey.id))
        .where(ApiKey.user_id == user.id, ApiKey.is_revoked == False)  # noqa: E712
    ).scalar_one()
    if active_count >= MAX_KEYS_PER_USER:
        raise HTTPException(
            status_code=400,
            detail=f"Лимит активных ключей: {MAX_KEYS_PER_USER}. Удалите ненужные.",
        )

    plain_key, key_hash, key_prefix = generate_api_key()

    new_key = ApiKey(
        user_id=user.id,
        key_hash=key_hash,
        key_prefix=key_prefix,
        name=body.name,
    )
    db.add(new_key)
    db.commit()
    db.refresh(new_key)

    return ApiKeyCreateResponse(
        id=new_key.id,
        name=new_key.name,
        key_prefix=new_key.key_prefix,
        plain_key=plain_key,
        created_at=new_key.created_at,
    )


@router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
def revoke_key(
    key_id: int,
    user: User = Depends(require_pro),
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
