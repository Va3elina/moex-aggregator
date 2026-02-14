# api/routers/auth.py
"""
API endpoints для аутентификации.

ENDPOINTS:
  POST /api/auth/register  — регистрация нового пользователя
  POST /api/auth/login     — вход (получение токенов)
  POST /api/auth/refresh   — обновление access токена
  POST /api/auth/logout    — выход (инвалидация refresh токена)
  GET  /api/auth/me        — информация о текущем пользователе
  PUT  /api/auth/profile   — обновление профиля
  POST /api/auth/change-password — смена пароля

БЕЗОПАСНОСТЬ:
- Пароли хэшируются Argon2
- Rate limit 10 req/min на auth endpoints (в middleware)
- Блокировка после 5 неудачных попыток
- Логирование всех попыток входа
"""

from fastapi import APIRouter, HTTPException, status, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import Optional

# База данных
from api.database import get_db

# Модели
from api.models.user import User, RefreshToken, UserRole

# Схемы
from api.schemas.auth import (
    UserRegister,
    UserLogin,
    TokenResponse,
    RefreshTokenRequest,
    UserResponse,
    UserUpdate,
    PasswordChange,
)

# Безопасность
from api.security import (
    hash_password,
    verify_password,
    create_token_pair,
    verify_token,
    check_account_locked,
    should_lock_account,
    calculate_lockout_time,
    log_failed_login,
    log_successful_login,
    check_ip_rate_limit,
    check_password_strength,
    generate_secure_token,
)

from api.logger import get_logger

logger = get_logger()

# Роутер
router = APIRouter(prefix="/auth", tags=["Authentication"])

# Bearer token security scheme (для Swagger UI)
security = HTTPBearer(auto_error=False)


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def get_client_ip(request: Request) -> str:
    """Получить IP клиента (учитывая прокси)."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def get_user_by_email(db: Session, email: str) -> Optional[User]:
    """Получить пользователя по email."""
    return db.query(User).filter(User.email == email.lower()).first()


def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    """Получить пользователя по ID."""
    return db.query(User).filter(User.id == user_id).first()


async def get_current_user(
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
        db: Session = Depends(get_db)
) -> User:
    """
    Dependency для получения текущего пользователя из токена.

    Использование:
        @router.get("/protected")
        async def protected_route(user: User = Depends(get_current_user)):
            return {"message": f"Hello, {user.email}"}
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Требуется авторизация",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    payload = verify_token(token, "access")

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный или истёкший токен",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = get_user_by_id(db, int(payload.sub))

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Пользователь не найден",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Аккаунт деактивирован",
        )

    return user


async def get_current_user_optional(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
        db: Session = Depends(get_db)
) -> Optional[User]:
    """Опциональная авторизация — не выбрасывает ошибку если токена нет."""
    if not credentials:
        return None

    payload = verify_token(credentials.credentials, "access")
    if not payload:
        return None

    return get_user_by_id(db, int(payload.sub))


# Проверка роли администратора
async def require_admin(user: User = Depends(get_current_user)) -> User:
    """Dependency для проверки прав администратора."""
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Требуются права администратора"
        )
    return user


# Проверка роли PRO или выше
async def require_pro(user: User = Depends(get_current_user)) -> User:
    """Dependency для проверки PRO подписки."""
    if user.role not in ["pro", "admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Требуется PRO подписка"
        )
    return user


# ============================================================================
# ENDPOINTS
# ============================================================================

@router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    response_model=UserResponse,
    summary="Регистрация нового пользователя",
    responses={
        201: {"description": "Пользователь создан"},
        400: {"description": "Email уже занят или невалидные данные"},
    }
)
async def register(
        data: UserRegister,
        request: Request,
        db: Session = Depends(get_db)
):
    """
    Регистрация нового пользователя.

    - Проверяет, что email не занят
    - Хэширует пароль (Argon2)
    - Создаёт пользователя в БД
    """
    ip = get_client_ip(request)

    # Проверяем IP rate limit
    if not check_ip_rate_limit(ip, max_attempts=10, window_minutes=1):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много запросов. Попробуйте позже.",
        )

    # Проверяем, не занят ли email
    if get_user_by_email(db, data.email):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь с таким email уже существует",
        )

    # Проверяем сложность пароля
    strength = check_password_strength(data.password)
    if not strength["is_valid"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Слабый пароль: {', '.join(strength['errors'])}",
        )

    # Создаём пользователя
    user = User(
        email=data.email.lower(),
        hashed_password=hash_password(data.password),
        username=data.username,
        role="user",
        is_active=True,
        is_verified=False,
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    logger.info(
        f"New user registered: {data.email}",
        extra={"extra_data": {"event": "user_registered", "user_id": user.id}}
    )

    return UserResponse(
        id=user.id,
        email=user.email,
        username=user.username,
        role=user.role,
        is_verified=user.is_verified,
        created_at=user.created_at,
    )


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Вход в систему",
    responses={
        200: {"description": "Успешный вход, возвращены токены"},
        401: {"description": "Неверный email или пароль"},
        423: {"description": "Аккаунт заблокирован"},
    }
)
async def login(
        data: UserLogin,
        request: Request,
        db: Session = Depends(get_db)
):
    """
    Вход в систему.

    При успешном входе возвращает:
    - **access_token** — для авторизации запросов (живёт 15 минут)
    - **refresh_token** — для обновления access_token (живёт 7 дней)

    **Защита от брутфорса:**
    - 5 неудачных попыток → блокировка на 15 минут
    """
    ip = get_client_ip(request)
    user_agent = request.headers.get("User-Agent", "")

    # Проверяем IP rate limit
    if not check_ip_rate_limit(ip, max_attempts=20, window_minutes=5):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много попыток входа. Попробуйте позже.",
        )

    # Ищем пользователя
    user = get_user_by_email(db, data.email)

    if not user:
        log_failed_login(data.email, ip, user_agent, 1, "user_not_found")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный email или пароль",
        )

    # Проверяем блокировку
    is_locked, remaining = check_account_locked(
        user.failed_login_attempts,
        user.locked_until
    )

    if is_locked:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail=f"Аккаунт заблокирован. Попробуйте через {remaining} секунд.",
        )

    # Проверяем пароль
    if not verify_password(data.password, user.hashed_password):
        # Увеличиваем счётчик неудачных попыток
        user.failed_login_attempts += 1

        log_failed_login(
            data.email, ip, user_agent,
            user.failed_login_attempts,
            "wrong_password"
        )

        # Блокируем если превышен лимит
        if should_lock_account(user.failed_login_attempts):
            user.locked_until = calculate_lockout_time()
            db.commit()

            logger.warning(
                f"Account locked: {data.email}",
                extra={"extra_data": {"event": "account_locked", "ip": ip}}
            )

            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail="Аккаунт заблокирован на 15 минут из-за множества неудачных попыток.",
            )

        db.commit()

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный email или пароль",
        )

    # Проверяем активность аккаунта
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Аккаунт деактивирован",
        )

    # Успешный вход — сбрасываем счётчик
    user.failed_login_attempts = 0
    user.locked_until = None
    user.last_login_at = datetime.now(timezone.utc)
    user.last_login_ip = ip
    db.commit()

    log_successful_login(user.id, user.email, ip, user_agent)

    # Создаём токены
    tokens = create_token_pair(user.id, user.role)

    return TokenResponse(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type="bearer",
        expires_in=tokens.expires_in,
    )


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Обновление токенов",
)
async def refresh_tokens(
        data: RefreshTokenRequest,
        request: Request,
        db: Session = Depends(get_db)
):
    """Обновление access_token с помощью refresh_token."""
    payload = verify_token(data.refresh_token, "refresh")

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный или истёкший refresh токен",
        )

    user = get_user_by_id(db, int(payload.sub))

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Пользователь не найден или деактивирован",
        )

    # Создаём новую пару токенов
    tokens = create_token_pair(user.id, user.role)

    logger.info(
        f"Tokens refreshed for user {user.id}",
        extra={"extra_data": {"event": "tokens_refreshed", "user_id": user.id}}
    )

    return TokenResponse(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type="bearer",
        expires_in=tokens.expires_in,
    )


@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Выход из системы",
)
async def logout(
        request: Request,
        user: User = Depends(get_current_user)
):
    """Выход из системы."""
    logger.info(
        f"User logged out: {user.id}",
        extra={"extra_data": {"event": "logout", "user_id": user.id}}
    )
    # TODO: Добавить refresh_token в blacklist в таблице refresh_tokens
    return None


@router.get(
    "/me",
    response_model=UserResponse,
    summary="Информация о текущем пользователе",
)
async def get_me(user: User = Depends(get_current_user)):
    """Возвращает информацию о текущем авторизованном пользователе."""
    return UserResponse(
        id=user.id,
        email=user.email,
        username=user.username,
        role=user.role,
        is_verified=user.is_verified,
        created_at=user.created_at,
    )


@router.put(
    "/profile",
    response_model=UserResponse,
    summary="Обновление профиля",
)
async def update_profile(
        data: UserUpdate,
        user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Обновление профиля пользователя."""
    if data.username:
        # Проверяем, не занят ли username
        existing = db.query(User).filter(
            User.username == data.username,
            User.id != user.id
        ).first()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Этот username уже занят",
            )
        user.username = data.username

    db.commit()
    db.refresh(user)

    return UserResponse(
        id=user.id,
        email=user.email,
        username=user.username,
        role=user.role,
        is_verified=user.is_verified,
        created_at=user.created_at,
    )


@router.post(
    "/change-password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Смена пароля",
)
async def change_password(
        data: PasswordChange,
        request: Request,
        user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Смена пароля авторизованным пользователем."""
    # Проверяем текущий пароль
    if not verify_password(data.current_password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Неверный текущий пароль",
        )

    # Проверяем сложность нового пароля
    strength = check_password_strength(data.new_password)
    if not strength["is_valid"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Слабый пароль: {', '.join(strength['errors'])}",
        )

    # Обновляем пароль
    user.hashed_password = hash_password(data.new_password)
    db.commit()

    logger.info(
        f"Password changed for user {user.id}",
        extra={"extra_data": {"event": "password_changed", "user_id": user.id}}
    )

    return None