"""
Генерация и валидация токенов браузерного расширения.

Поток:
  - Генерация: PRO-юзер в личном кабинете → POST /api/extension/token →
    generate_extension_token() → plain показываем один раз, hash в БД.
  - Обмен: embed с `?token=ext_…` → POST /api/extension/exchange →
    lookup_extension_token() → (если PRO) выдаём JWT access-токен.

Формат: ext_<40 random url-safe chars>.
"""
import hashlib
import secrets

from sqlalchemy import select
from sqlalchemy.orm import Session

from api.models import ExtensionToken, User

EXT_PREFIX = "ext_"
PLAIN_LEN = 40  # без префикса


def generate_extension_token() -> tuple[str, str, str]:
    """Создаёт новый ext-токен.

    Returns:
        (plain_token, token_hash, token_prefix_for_display)
        plain — показываем юзеру один раз, в БД НЕ храним.
        token_hash — sha256 hex (64), хранится в БД.
        token_prefix — "ext_<первые 8>" (12 char) для display.
    """
    random_part = secrets.token_urlsafe(PLAIN_LEN)[:PLAIN_LEN]
    plain = f"{EXT_PREFIX}{random_part}"
    token_hash = hashlib.sha256(plain.encode()).hexdigest()
    token_prefix = plain[:12]
    return plain, token_hash, token_prefix


def lookup_extension_token(plain_token: str | None, db: Session):
    """Чистый lookup без побочных эффектов.

    Returns:
        (ExtensionToken, User) если токен валиден и не отозван, иначе None.
        Тариф (PRO) тут НЕ проверяется — это делает вызывающий (exchange).
    """
    if not plain_token or not plain_token.startswith(EXT_PREFIX):
        return None
    token_hash = hashlib.sha256(plain_token.encode()).hexdigest()
    return db.execute(
        select(ExtensionToken, User)
        .join(User, User.id == ExtensionToken.user_id)
        .where(ExtensionToken.token_hash == token_hash, ExtensionToken.is_revoked == False)  # noqa: E712
    ).first()
