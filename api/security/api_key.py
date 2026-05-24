"""
API-key authentication для programmatic access (Pro tier).

Использование в endpoints:

    from api.security.api_key import get_user_by_api_key

    @router.get("/v1/public/heatmap")
    def public_heatmap(user: User = Depends(get_user_by_api_key)):
        ...

Header: `X-API-Key: pk_live_<32_chars>`.
Header: `Authorization: Bearer <api_key>` тоже принимается (для curl/Postman
удобно — стандартный auth header).
"""
import hashlib
import secrets
from datetime import datetime, timezone

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import select

from api.database import get_db
from api.models import ApiKey, User


# Format: pk_{live|test}_<32 random chars>. Префиксы pk_live_/pk_test_ —
# Stripe-style чтобы юзер визуально различал ключи по mode при copy-paste.
LIVE_PREFIX = "pk_live_"
TEST_PREFIX = "pk_test_"
PLAIN_KEY_LENGTH = 32  # без префикса

# Backward-compat алиас для импортов из других модулей.
KEY_PREFIX = LIVE_PREFIX


def generate_api_key(mode: str = "live") -> tuple[str, str, str]:
    """
    Создаёт новый API-ключ.

    Args:
        mode: 'live' (default, требует Pro tier) или 'test' (для разработки,
              не требует подписки). Префикс ключа отражает mode.

    Returns:
        (plain_key, key_hash, key_prefix_for_display)
        plain_key — показываем юзеру один раз, в БД НЕ сохраняем.
        key_hash — sha256 в hex (64 char), сохраняется в БД.
        key_prefix_for_display — pk_{live|test}_<первые 4 char>... (12 char total).
    """
    if mode not in ("live", "test"):
        raise ValueError(f"mode must be 'live' or 'test', got '{mode}'")
    prefix = LIVE_PREFIX if mode == "live" else TEST_PREFIX
    random_part = secrets.token_urlsafe(PLAIN_KEY_LENGTH)[:PLAIN_KEY_LENGTH]
    plain_key = f"{prefix}{random_part}"
    key_hash = hashlib.sha256(plain_key.encode()).hexdigest()
    # Display prefix: "pk_live_abcd…" / "pk_test_abcd…" — первые 12 chars.
    key_prefix = plain_key[:12]
    return plain_key, key_hash, key_prefix


def _extract_api_key(
    x_api_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> str | None:
    """Pull API key из X-API-Key или Authorization: Bearer header."""
    if x_api_key:
        return x_api_key.strip()
    if authorization:
        # "Bearer pk_live_xxx" → pk_live_xxx
        parts = authorization.split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1].strip()
            # Принимаем только если это API-key (pk_live_ или pk_test_) —
            # иначе это JWT (другая auth), отдаём None и пусть другой
            # dependency обработает.
            if token.startswith(LIVE_PREFIX) or token.startswith(TEST_PREFIX):
                return token
    return None


def get_user_by_api_key(
    x_api_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> User:
    """
    Authenticates request through API key.

    Возвращает User или 401/403. Также обновляет last_used_at для аналитики.

    Modes:
      - live (pk_live_*): требует Pro tier. Если подписка истекла →
        403 с понятным сообщением. Ключ остаётся в БД — после возобновления
        подписки автоматически снова работает (без пересоздания).
      - test (pk_test_*): для разработки. НЕ требует Pro tier — может
        использоваться любым залогиненным юзером для разработки ботов,
        тестирования интеграций, CI/CD pipelines.

    Mode detection: по `api_key.mode` поле (источник истины) — не по
    префиксу строки. Префикс — это display, mode — это authorization.
    """
    plain_key = _extract_api_key(x_api_key, authorization)
    if not plain_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required (X-API-Key header или Authorization: Bearer ...)",
        )

    key_hash = hashlib.sha256(plain_key.encode()).hexdigest()
    row = db.execute(
        select(ApiKey, User)
        .join(User, User.id == ApiKey.user_id)
        .where(ApiKey.key_hash == key_hash, ApiKey.is_revoked == False)  # noqa: E712
    ).first()

    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

    api_key, user = row

    # Pro-tier enforcement только для live-keys. Test-keys пропускаем —
    # это разработческий режим, юзер может ещё не купить Pro.
    if api_key.mode == "live" and user.role not in ("pro", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "API доступен только на тарифе Pro. Возобновите подписку — "
                "ваш ключ продолжит работать без пересоздания."
            ),
        )

    # Update last_used_at — async-friendly write, не блокирует response.
    api_key.last_used_at = datetime.now(timezone.utc)
    db.commit()

    return user
