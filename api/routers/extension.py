"""
/api/extension/* — токены для браузерного расширения (терминал Т-Инвестиций).

  GET    /api/extension/token        — список активных токенов юзера (auth)
  POST   /api/extension/token        — сгенерировать токен (PRO-only)
  DELETE /api/extension/token/{id}   — отозвать токен (auth, свой)
  POST   /api/extension/exchange     — обменять ext-токен на JWT access (публичный;
                                        валидирует токен + проверяет PRO вживую)

Дизайн: ext-токен — долговечный отзываемый credential, хранится у юзера в
расширении. Embed обменивает его на короткий JWT и дальше работает штатным auth.
"""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import ExtensionToken, User
from api.routers.auth import get_current_user, require_pro
from api.security.extension_token import (
    generate_extension_token,
    lookup_extension_token,
    revoke_extension_token,
)
from api.security.jwt import ACCESS_TOKEN_EXPIRE_MINUTES, create_access_token

router = APIRouter(prefix="/api/extension", tags=["extension"])


class TokenInfo(BaseModel):
    id: int
    token_prefix: str
    name: str | None
    created_at: datetime
    last_used_at: datetime | None


class GeneratedToken(BaseModel):
    id: int
    token: str  # plain — показываем РОВНО ОДИН РАЗ
    token_prefix: str


class ExchangeRequest(BaseModel):
    token: str


class ExchangeResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    role: str


@router.get("/token", response_model=list[TokenInfo])
def list_tokens(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    rows = (
        db.execute(
            select(ExtensionToken)
            .where(ExtensionToken.user_id == user.id, ExtensionToken.is_revoked == False)  # noqa: E712
            .order_by(ExtensionToken.created_at.desc())
        )
        .scalars()
        .all()
    )
    return [
        TokenInfo(
            id=t.id,
            token_prefix=t.token_prefix,
            name=t.name,
            created_at=t.created_at,
            last_used_at=t.last_used_at,
        )
        for t in rows
    ]


@router.post("/token", response_model=GeneratedToken)
def create_token(user: User = Depends(require_pro), db: Session = Depends(get_db)):
    """Генерация ext-токена. require_pro → 403 «Требуется PRO подписка» для
    не-PRO (фронт показывает предупреждение/upsell)."""
    plain, token_hash, token_prefix = generate_extension_token()
    row = ExtensionToken(
        user_id=user.id,
        token_hash=token_hash,
        token_prefix=token_prefix,
        name="Терминал Т-Инвестиций",
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return GeneratedToken(id=row.id, token=plain, token_prefix=token_prefix)


@router.delete("/token/{token_id}")
def revoke_token(token_id: int, user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    t = db.get(ExtensionToken, token_id)
    if not t or t.user_id != user.id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Токен не найден")
    t.is_revoked = True
    t.revoked_at = datetime.now(timezone.utc)
    db.commit()
    return {"ok": True}


@router.post("/revoke")
def revoke_by_token(req: ExchangeRequest, db: Session = Depends(get_db)):
    """Отзыв ext-токена по самому токену — для popup расширения (оно не залогинено,
    у него есть только plain-токен). Публичный, идемпотентный: всегда {ok:true},
    чтобы не раскрывать существование токена. Доступа не добавляет — отзыв возможен
    лишь тому, кто УЖЕ владеет токеном (т.е. и так имел бы полный доступ)."""
    revoke_extension_token(req.token, db)
    return {"ok": True}


@router.post("/exchange", response_model=ExchangeResponse)
def exchange(req: ExchangeRequest, db: Session = Depends(get_db)):
    """Обмен ext-токена на короткий JWT. Публичный (сам валидирует токен).
    PRO проверяется ВЖИВУЮ — даунгрейд с PRO сразу ломает обмен."""
    row = lookup_extension_token(req.token, db)
    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Недействительный токен расширения")
    ext, user = row
    if user.role not in ("pro", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Токен требует активную подписку PRO. Возобновите подписку — токен снова заработает.",
        )
    ext.last_used_at = datetime.now(timezone.utc)
    db.commit()
    return ExchangeResponse(
        access_token=create_access_token(user.id, user.role),
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        role=user.role,
    )
