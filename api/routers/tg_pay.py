"""
/api/tg-pay/* — привязка чата бота оплаты @FrameDataPayBot к аккаунту (docs/tg-pay-bot.md).

Бот выдаёт ссылку framedata.ru/pricing?src=tg_bot&tgc=<token>. Фронт сохраняет
токен и, когда человек вошёл на сайт (VK/Яндекс/почта), шлёт его сюда. Это НЕ
вход через Telegram (406-ФЗ): аккаунт уже авторизован обычным способом, мы лишь
запоминаем, какому чату писать об оплате.
"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import get_current_user

router = APIRouter(prefix="/api/tg-pay", tags=["tg-pay"])


class ClaimRequest(BaseModel):
    token: str = Field(min_length=16, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")


def claim_token(db: Session, token: str, user_id: int) -> bool:
    """Привязать чат по токену. Последний вход побеждает: чат, привязанный к
    другому аккаунту, переезжает на текущий. Токен одноразовый."""
    row = db.execute(text("""
        UPDATE tg_pay_claims SET used_at = now()
        WHERE token = :t AND used_at IS NULL AND expires_at > now()
        RETURNING chat_id
    """), {"t": token}).first()
    if not row:
        return False
    chat_id = row[0]
    db.execute(text("""
        UPDATE tg_pay_leads SET user_id = :u, linked_at = now()
        WHERE chat_id = :c AND user_id IS DISTINCT FROM :u
    """), {"u": user_id, "c": chat_id})
    db.execute(text("INSERT INTO tg_pay_events (chat_id, user_id, kind) VALUES (:c, :u, 'claim')"),
               {"c": chat_id, "u": user_id})
    db.commit()
    return True


@router.post("/claim")
def claim(
    body: ClaimRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Всегда {ok: true}: по ответу нельзя понять, существует ли токен.
    Подбор бессмыслен: токен 32 символа token_urlsafe, живёт сутки."""
    claim_token(db, body.token, user.id)
    return {"ok": True}
