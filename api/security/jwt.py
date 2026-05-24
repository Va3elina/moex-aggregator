# api/security/jwt.py
"""
JWT (JSON Web Token) — система токенов для аутентификации.

ЧТО ЭТО: JWT — это подписанный JSON объект, который содержит информацию
о пользователе и может быть проверен без обращения к базе данных.

КАК ВЫГЛЯДИТ JWT:
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.
  eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ.
  SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c

  Это три части через точку:
  1. Header (заголовок): {"alg": "HS256", "typ": "JWT"}
  2. Payload (данные): {"sub": "user_id", "exp": 1234567890, ...}
  3. Signature (подпись): HMACSHA256(header + payload, SECRET_KEY)

ПОЧЕМУ JWT БЕЗОПАСЕН:
  - Подпись создаётся с SECRET_KEY, который знает только сервер
  - Если кто-то изменит payload — подпись не совпадёт
  - Сервер проверяет подпись при каждом запросе

FLOW АУТЕНТИФИКАЦИИ:
  1. POST /auth/login {email, password}
  2. Сервер проверяет → выдаёт access_token (15 мин) + refresh_token (7 дней)
  3. Клиент сохраняет токены
  4. Каждый запрос: Authorization: Bearer <access_token>
  5. Когда access истёк → POST /auth/refresh с refresh_token → новый access

ПОЧЕМУ 2 ТОКЕНА:
  Access token:
    - Короткоживущий (15 минут)
    - Хранится в памяти JavaScript
    - Если украдут — ущерб ограничен 15 минутами

  Refresh token:
    - Долгоживущий (7 дней)
    - Хранится в httpOnly cookie (JavaScript не имеет доступа!)
    - Используется только для получения нового access token
    - При подозрении на утечку — можно отозвать в БД

УСТАНОВКА: pip install pyjwt
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
import jwt
from pydantic import BaseModel
import os

# === КОНФИГУРАЦИЯ ===
# В production эти значения должны быть в .env файле!

# Секретный ключ для подписи токенов
# КРИТИЧЕСКИ ВАЖНО: должен быть случайным и храниться в секрете!
# Генерация: python -c "import secrets; print(secrets.token_urlsafe(32))"
SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "")
if not SECRET_KEY:
    import warnings
    warnings.warn("JWT_SECRET_KEY not set! Using insecure default for local development only.", stacklevel=2)
    SECRET_KEY = "LOCAL_DEV_ONLY_NOT_FOR_PRODUCTION_KEY"

# Старый секрет для grace-period ротации.
# Используется ТОЛЬКО для verify (не для sign). Если задан — при decode
# пробуем сначала текущий SECRET_KEY, при fail — fallback на OLD.
# Это позволяет ротировать секрет без logout'а всех юзеров:
#   1. Сгенерировал новый ключ → JWT_SECRET_KEY=NEW, JWT_SECRET_KEY_OLD=OLD
#   2. Все новые токены подписываются NEW, старые токены валидны через OLD.
#   3. Через 7 дней (max refresh-token lifetime) — все OLD-токены истекли.
#   4. Убрать JWT_SECRET_KEY_OLD из .env, перезапустить API.
# Пустая строка = grace-period неактивен (нормальное состояние).
SECRET_KEY_OLD = os.environ.get("JWT_SECRET_KEY_OLD", "")

# Алгоритм подписи
# HS256 — HMAC с SHA-256, симметричный (один ключ для подписи и проверки)
# RS256 — RSA, асимметричный (приватный для подписи, публичный для проверки)
ALGORITHM = "HS256"

# Время жизни токенов
ACCESS_TOKEN_EXPIRE_MINUTES = 15  # Короткий срок!
REFRESH_TOKEN_EXPIRE_DAYS = 7  # Неделя


class TokenPayload(BaseModel):
    """
    Структура данных внутри JWT токена.

    sub (subject): ID пользователя — главный идентификатор
    exp (expiration): Когда токен истекает
    iat (issued at): Когда токен создан
    type: "access" или "refresh" — чтобы нельзя было использовать refresh как access
    role: Роль пользователя — для проверки доступа без запроса к БД
    """
    sub: str  # user_id
    exp: datetime  # expiration time
    iat: datetime  # issued at
    type: str  # "access" или "refresh"
    role: str = "user"  # роль пользователя


class TokenPair(BaseModel):
    """Пара токенов, возвращаемая при логине."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # секунд до истечения access token


def create_access_token(user_id: int, role: str = "user") -> str:
    """
    Создаёт access token — используется для авторизации запросов.

    Args:
        user_id: ID пользователя из БД
        role: Роль пользователя (user/pro/admin)

    Returns:
        JWT строка, например: "eyJhbGciOiJIUzI1NiIs..."

    Пример использования:
        token = create_access_token(user_id=123, role="pro")
        # Клиент отправляет: Authorization: Bearer <token>
    """
    now = datetime.now(timezone.utc)
    expires = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    payload = {
        "sub": str(user_id),  # Subject — кто владелец токена
        "exp": expires,  # Expiration — когда истекает
        "iat": now,  # Issued at — когда создан
        "type": "access",  # Тип токена
        "role": role,  # Роль для быстрой проверки доступа
    }

    # jwt.encode создаёт подписанный токен
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def create_refresh_token(user_id: int) -> str:
    """
    Создаёт refresh token — используется для обновления access token.

    ВАЖНО: Refresh token НЕ содержит role, потому что:
    - Используется редко (раз в 15 мин)
    - При обновлении мы всё равно проверяем БД
    - Роль могла измениться с момента создания токена

    Args:
        user_id: ID пользователя

    Returns:
        JWT строка для refresh token
    """
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    payload = {
        "sub": str(user_id),
        "exp": expires,
        "iat": now,
        "type": "refresh",  # Отличаем от access токена
    }

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def verify_token(token: str, token_type: str = "access") -> Optional[TokenPayload]:
    """
    Проверяет и декодирует JWT токен.

    Args:
        token: JWT строка
        token_type: Ожидаемый тип ("access" или "refresh")

    Returns:
        TokenPayload если токен валидный, None если нет

    Что проверяется:
    1. Подпись — токен не был изменён
    2. Срок действия — токен не истёк
    3. Тип — access нельзя использовать как refresh и наоборот

    Возможные ошибки:
    - jwt.ExpiredSignatureError — токен истёк
    - jwt.InvalidTokenError — подпись неверная или токен повреждён
    """
    payload = None
    used_old_key = False

    # Попытка 1: текущий SECRET_KEY.
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        # Токен истёк — это нормально, нужен refresh. Fallback на OLD не помогает.
        return None
    except jwt.InvalidTokenError:
        # Подпись не проходит с current key — возможно токен из grace-period.
        # Попытка 2: fallback на SECRET_KEY_OLD (если активен).
        if SECRET_KEY_OLD:
            try:
                payload = jwt.decode(token, SECRET_KEY_OLD, algorithms=[ALGORITHM])
                used_old_key = True
            except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
                # И с OLD не проходит — токен подделан или ну совсем сломан.
                return None
        else:
            return None

    # Проверяем тип токена
    if payload.get("type") != token_type:
        return None

    # Если декодировали через OLD — логируем для отслеживания progress'а ротации.
    # Когда warning'и в логах прекратятся, можно убирать JWT_SECRET_KEY_OLD из .env.
    if used_old_key:
        import logging
        logging.getLogger("moex_api").info(
            "JWT decoded via OLD secret (rotation in progress)",
            extra={"extra_data": {"sub": payload.get("sub"), "type": payload.get("type")}},
        )

    return TokenPayload(
        sub=payload["sub"],
        exp=datetime.fromtimestamp(payload["exp"], tz=timezone.utc),
        iat=datetime.fromtimestamp(payload["iat"], tz=timezone.utc),
        type=payload["type"],
        role=payload.get("role", "user"),
    )


def decode_token_unsafe(token: str) -> Optional[dict]:
    """
    Декодирует токен БЕЗ проверки подписи.

    ВНИМАНИЕ: Использовать только для отладки!
    Никогда не доверять данным из этой функции!

    Полезно для:
    - Просмотра содержимого токена в логах
    - Отладки проблем с аутентификацией
    """
    try:
        return jwt.decode(
            token,
            options={"verify_signature": False}
        )
    except Exception:
        return None


def create_token_pair(user_id: int, role: str = "user") -> TokenPair:
    """
    Создаёт пару токенов для пользователя.

    Вызывается при:
    - Успешном логине
    - Обновлении токенов через refresh

    Returns:
        TokenPair с access и refresh токенами
    """
    return TokenPair(
        access_token=create_access_token(user_id, role),
        refresh_token=create_refresh_token(user_id),
        token_type="bearer",
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,  # в секундах
    )