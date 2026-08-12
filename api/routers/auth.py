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
  POST /api/auth/change-password — смена пароля (зная старый)
  POST /api/auth/password-reset/request — код для забытого пароля
  POST /api/auth/password-reset/verify  — проверка кода (шаг перед паролем)
  POST /api/auth/password-reset/confirm — смена пароля по коду + вход

БЕЗОПАСНОСТЬ:
- Пароли хэшируются Argon2
- Rate limit 10 req/min на auth endpoints (в middleware)
- Блокировка после 5 неудачных попыток
- Логирование всех попыток входа
"""

from fastapi import APIRouter, HTTPException, status, Depends, Request, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
from typing import Optional
import hashlib
import secrets

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
    PasswordChange,
    AddEmailRequest,
    VerifyEmailRequest,
    PasswordResetRequest,
    PasswordResetVerify,
    PasswordResetConfirm,
)

# Email-сервис (SMTP Yandex 360)
from api.services.email import send_verification_email, send_password_reset_email

# Безопасность
from api.security import (
    hash_password,
    verify_password,
    dummy_verify,
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
    """Получить IP клиента (доверяя proxy-заголовкам только от nginx)."""
    from api.middleware import get_client_ip_safe
    return get_client_ip_safe(request)


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
# EMAIL VERIFICATION HELPERS (Phase 2 — SMTP Yandex 360)
# ============================================================================

EMAIL_VERIFY_TTL_MIN = 30                 # срок жизни кода
EMAIL_VERIFY_MAX_ATTEMPTS = 5             # неверных попыток до блокировки кода
EMAIL_VERIFY_RESEND_COOLDOWN_SEC = 60     # пауза между повторными отправками


def _generate_verify_code() -> str:
    """Криптостойкий 6-значный код подтверждения."""
    return f"{secrets.randbelow(1_000_000):06d}"


def _issue_email_verification(user: User, db: Session, background_tasks: BackgroundTasks) -> None:
    """Генерит новый код, сохраняет в БД, ставит отправку письма в фон (threadpool)."""
    code = _generate_verify_code()
    now = datetime.now(timezone.utc)
    user.email_verify_code = code
    user.email_verify_expires_at = now + timedelta(minutes=EMAIL_VERIFY_TTL_MIN)
    user.email_verify_attempts = 0
    user.email_verify_sent_at = now
    db.commit()
    # send_verification_email — sync; BackgroundTasks выполнит её в threadpool
    # после ответа, не блокируя event loop. Если SMTP не настроен/упал —
    # функция вернёт False и залогирует, регистрация при этом не падает.
    background_tasks.add_task(
        send_verification_email, user.email, code, getattr(user, "display_name", None)
    )


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
        background_tasks: BackgroundTasks,
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

    # Шлём код подтверждения email (email+password регистрация).
    # OAuth-юзеры сюда не попадают — они идут через /api/auth/oauth с is_verified=True.
    _issue_email_verification(user, db, background_tasks)

    return UserResponse(
        id=user.id,
        email=user.email,
        role=user.role,
        is_verified=user.is_verified,
        has_password=True,
        oauth_providers=[],
        created_at=user.created_at,
    )


def purge_expired_refresh_tokens(db: Session, user_id: int) -> None:
    """Удаляет истёкшие строки refresh_tokens пользователя (мусор ротации).

    Ротация пишет новую строку на каждый /refresh (~15 мин активной сессии),
    истёкшие никогда не удалялись — на 59 юзеров скопилось 5k+ строк (аудит
    21.07.2026). Истёкшая строка не участвует ни в одной проверке: /refresh
    отбрасывает просроченный JWT ДО запроса к БД (verify_token), так что
    удаление безопасно и для отзыва. Грейс 1 день — от граничных расхождений
    часов. Аналитика (MAX(created_at) как last_active) не страдает: у живой
    сессии свежая строка всегда моложе 7 дней, для остальных есть fallback
    на last_login_at/analytics_events. Вызывающий коммитит сам.
    """
    db.query(RefreshToken).filter(
        RefreshToken.user_id == user_id,
        RefreshToken.expires_at < datetime.now(timezone.utc) - timedelta(days=1),
    ).delete()


def persist_refresh_token(
        db: Session,
        user_id: int,
        refresh_token: str,
        request: Request | None = None,
) -> None:
    """Сохраняет hash refresh-токена при ВЫДАЧЕ (login/OAuth — как /refresh).

    Без записи logout не может отозвать токен (отзыв идёт по строкам
    RefreshToken), а сам токен можно реиграть все 7 дней жизни. После того как
    все живые сессии пройдут ротацию (~7 дней с деплоя), /refresh должен начать
    ТРЕБОВАТЬ наличие строки (unknown → 401) — см. TODO в refresh_tokens.
    Вызывающий коммитит сам (login делает db.commit() ниже)."""
    purge_expired_refresh_tokens(db, user_id)
    db.add(RefreshToken(
        user_id=user_id,
        token_hash=hashlib.sha256(refresh_token.encode()).hexdigest(),
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        ip_address=get_client_ip(request) if request else None,
        user_agent=(request.headers.get("user-agent", "")[:500] if request else ""),
    ))


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
        # Холостая argon2-проверка: выравниваем время ответа с веткой реальной
        # проверки пароля, иначе по латентности можно отличить зарегистрированный
        # email от незнакомого (user-enumeration по таймингу, CWE-204).
        dummy_verify()
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
    persist_refresh_token(db, user.id, tokens.refresh_token, request)
    db.commit()

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

    # Проверяем, не отозван ли токен
    token_hash = hashlib.sha256(data.refresh_token.encode()).hexdigest()
    stored = db.query(RefreshToken).filter(RefreshToken.token_hash == token_hash).first()
    # TODO(2026-06-18, шаг 3b плана FIX_PLAN_2026-06-11): после 7 дней с деплоя
    # persist_refresh_token (login/OAuth теперь пишут строку при выдаче) —
    # ТРЕБОВАТЬ stored is not None: `if stored is None or stored.is_revoked: 401`.
    # Раньше нельзя: токены сессий, выданных до деплоя, не в БД — был бы
    # массовый разлогин. Сейчас неизвестный токен проходит без проверки отзыва.
    if stored and stored.is_revoked:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh токен отозван",
        )

    user = get_user_by_id(db, int(payload.sub))

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Пользователь не найден или деактивирован",
        )

    # Отзываем старый refresh токен (ротация).
    # ⚠️ БЕЗ промежуточного db.commit(): отзыв старого и вставка нового обязаны
    # лечь ОДНОЙ транзакцией. Раньше здесь коммитился отзыв, и если вставка ниже
    # падала, старый токен оставался отозванным — клиент получал 500, держал на
    # руках уже мёртвый токен, а его ретрай ловил 401 и юзера выкидывало.
    # Теперь любая ошибка вставки откатывает и отзыв: клиент повторяет запрос
    # тем же токеном и продолжает работать.
    if stored:
        stored.is_revoked = True

    # Создаём новую пару токенов
    tokens = create_token_pair(user.id, user.role)

    # Сохраняем новый refresh токен в БД (+ попутно чистим истёкший мусор ротации)
    purge_expired_refresh_tokens(db, user.id)
    new_token_hash = hashlib.sha256(tokens.refresh_token.encode()).hexdigest()
    db.add(RefreshToken(
        user_id=user.id,
        token_hash=new_token_hash,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("user-agent", "")[:500],
    ))
    db.commit()

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
        data: Optional[RefreshTokenRequest] = None,
        user: User = Depends(get_current_user),
        db: Session = Depends(get_db),
):
    """Выход из системы — отзывает refresh token."""
    # Отзываем конкретный токен если передан
    if data and data.refresh_token:
        token_hash = hashlib.sha256(data.refresh_token.encode()).hexdigest()
        stored = db.query(RefreshToken).filter(RefreshToken.token_hash == token_hash).first()
        if stored:
            stored.is_revoked = True

    # Отзываем все токены пользователя (полный logout)
    db.query(RefreshToken).filter(
        RefreshToken.user_id == user.id,
        RefreshToken.is_revoked == False,
    ).update({"is_revoked": True})
    db.commit()

    logger.info(
        f"User logged out: {user.id}",
        extra={"extra_data": {"event": "logout", "user_id": user.id}}
    )
    return None


def is_synthetic_oauth_email(email: str) -> bool:
    """
    Returns True если email — placeholder для OAuth-юзера без реального email.

    Создаётся в api/routers/oauth.py:169 как `{provider}_{oauth_id}@oauth.local`
    для случаев когда провайдер не отдал email (Telegram by design, VK часто).
    Такие email — несуществующие, на них письма не дойдут, чеки 54-ФЗ от T-Bank
    тоже. Frontend проверяет этот флаг и редиректит на /add-email.
    """
    return email.endswith("@oauth.local")


@router.get(
    "/me",
    response_model=UserResponse,
    summary="Информация о текущем пользователе",
)
async def get_me(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Возвращает информацию о текущем авторизованном пользователе."""
    # Собираем список OAuth провайдеров
    oauth_providers = []
    if user.oauth_provider:
        oauth_providers.append(user.oauth_provider)

    return UserResponse(
        id=user.id,
        email=user.email,
        display_name=getattr(user, 'display_name', None),
        role=user.role,
        is_verified=user.is_verified,
        avatar_url=user.avatar_url,
        has_password=bool(user.hashed_password),
        oauth_providers=oauth_providers,
        created_at=user.created_at,
        requires_email_setup=is_synthetic_oauth_email(user.email),
    )


@router.post(
    "/add-email",
    response_model=UserResponse,
    summary="Привязка реального email к OAuth-аккаунту",
    responses={
        400: {"description": "Email уже привязан (не synthetic)"},
        409: {"description": "Email занят другим пользователем"},
    }
)
async def add_email(
    data: AddEmailRequest,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Привязка реального email для OAuth-юзеров без него (Telegram-всегда, VK-часто).

    Без реального email T-Bank не может выдать фискальный чек по 54-ФЗ.
    После привязки СРАЗУ шлём код подтверждения (is_verified=False) — юзер
    подтверждает email на /verify-email. До подтверждения оплатить нельзя
    (серверный гейт в billing/service.py).
    """
    if not is_synthetic_oauth_email(user.email):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email уже привязан. Для смены email обратитесь в поддержку.",
        )

    new_email = data.email.lower().strip()

    # Проверка уникальности — другой юзер мог уже использовать этот адрес
    existing = db.query(User).filter(User.email == new_email).first()
    if existing and existing.id != user.id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Этот email уже используется другим аккаунтом.",
        )

    logger.info(
        f"User {user.id} adding real email (was synthetic): {user.email} → {new_email}",
        extra={"extra_data": {"event": "email_setup", "user_id": user.id}},
    )

    user.email = new_email
    user.is_verified = False  # реальный email привязан, но ещё НЕ подтверждён кодом
    _issue_email_verification(user, db, background_tasks)  # генерит код + commit + шлёт письмо
    db.refresh(user)

    oauth_providers = [user.oauth_provider] if user.oauth_provider else []
    return UserResponse(
        id=user.id,
        email=user.email,
        display_name=getattr(user, "display_name", None),
        role=user.role,
        is_verified=user.is_verified,
        avatar_url=user.avatar_url,
        has_password=bool(user.hashed_password),
        oauth_providers=oauth_providers,
        created_at=user.created_at,
        requires_email_setup=False,  # email уже реальный; теперь нужен код (см. /verify-email)
    )


@router.post(
    "/verify-email",
    response_model=UserResponse,
    summary="Подтверждение email кодом из письма",
    responses={
        200: {"description": "Email подтверждён"},
        400: {"description": "Неверный/истёкший код или уже подтверждён"},
        429: {"description": "Слишком много попыток"},
    },
)
async def verify_email(
    data: VerifyEmailRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Подтверждает email пользователя 6-значным кодом из письма."""
    if user.is_verified:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email уже подтверждён")
    if is_synthetic_oauth_email(user.email):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Сначала привяжите реальный email")
    if not user.email_verify_code or not user.email_verify_expires_at:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Код не запрошен. Запросите новый код.")

    now = datetime.now(timezone.utc)
    expires = user.email_verify_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    if now > expires:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Код истёк. Запросите новый.")

    if user.email_verify_attempts >= EMAIL_VERIFY_MAX_ATTEMPTS:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Слишком много попыток. Запросите новый код.")

    if data.code != user.email_verify_code:
        user.email_verify_attempts += 1
        db.commit()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Неверный код")

    # Успех — подтверждаем и очищаем поля кода
    user.is_verified = True
    user.email_verify_code = None
    user.email_verify_expires_at = None
    user.email_verify_attempts = 0
    user.email_verify_sent_at = None
    db.commit()
    db.refresh(user)

    logger.info(
        f"Email verified: user {user.id}",
        extra={"extra_data": {"event": "email_verified", "user_id": user.id}},
    )

    oauth_providers = [user.oauth_provider] if user.oauth_provider else []
    return UserResponse(
        id=user.id,
        email=user.email,
        display_name=getattr(user, "display_name", None),
        role=user.role,
        is_verified=user.is_verified,
        avatar_url=user.avatar_url,
        has_password=bool(user.hashed_password),
        oauth_providers=oauth_providers,
        created_at=user.created_at,
        requires_email_setup=is_synthetic_oauth_email(user.email),
    )


@router.post(
    "/resend-verification",
    summary="Повторная отправка кода подтверждения email",
    responses={
        200: {"description": "Код отправлен"},
        400: {"description": "Email уже подтверждён или не привязан реальный"},
        429: {"description": "Слишком частые запросы (кулдаун 60с)"},
    },
)
async def resend_verification(
    background_tasks: BackgroundTasks,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Генерит и заново отправляет код подтверждения. Кулдаун 60 сек между отправками."""
    if user.is_verified:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email уже подтверждён")
    if is_synthetic_oauth_email(user.email):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Сначала привяжите реальный email")

    now = datetime.now(timezone.utc)
    if user.email_verify_sent_at:
        sent = user.email_verify_sent_at
        if sent.tzinfo is None:
            sent = sent.replace(tzinfo=timezone.utc)
        elapsed = (now - sent).total_seconds()
        if elapsed < EMAIL_VERIFY_RESEND_COOLDOWN_SEC:
            wait = int(EMAIL_VERIFY_RESEND_COOLDOWN_SEC - elapsed)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Слишком часто. Подождите {wait} сек.",
            )

    _issue_email_verification(user, db, background_tasks)
    return {"success": True, "message": "Код отправлен на ваш email"}


@router.get("/guest-limits")
async def get_guest_limits():
    """Возвращает ограничения для неавторизованных пользователей."""
    from api.security.access_control import GUEST_LIMITS
    return GUEST_LIMITS


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

# ============================================================================
# ВОССТАНОВЛЕНИЕ ЗАБЫТОГО ПАРОЛЯ
# ============================================================================
#
# Механика та же, что у подтверждения email: 6-значный код на почту, TTL 30 мин,
# 5 попыток, кулдаун 60с между отправками (константы EMAIL_VERIFY_* переиспользуем
# намеренно — два разных набора правил для двух одноразовых кодов рано или поздно
# разъедутся).
#
# ⚠️ Оба эндпоинта отвечают ОДИНАКОВО независимо от того, существует ли аккаунт.
# Иначе форма «забыли пароль» превращается в оракул для перебора: злоумышленник
# скармливает список адресов и по ответу узнаёт, кто у нас зарегистрирован.


def _check_reset_code(user: User | None, code: str, db: Session) -> None:
    """Валидирует код сброса. Бросает HTTPException, если что-то не так.

    ⚠️ Формулировка ошибки ОДНА на все случаи (нет юзера / нет кода / истёк /
    не совпал) — иначе по тексту можно отличить «такого аккаунта нет» от
    «код неверный» и перебором собрать список зарегистрированных адресов.
    """
    invalid = HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Неверный или истёкший код. Запросите новый.",
    )
    if not user or not user.hashed_password or not user.password_reset_code:
        raise invalid
    if not user.password_reset_expires_at:
        raise invalid

    expires = user.password_reset_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) > expires:
        raise invalid

    if user.password_reset_attempts >= EMAIL_VERIFY_MAX_ATTEMPTS:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Исчерпаны попытки ввода кода. Запросите новый.",
        )

    # compare_digest, а не == : время сравнения не должно подсказывать,
    # сколько первых цифр угадано.
    if not secrets.compare_digest(user.password_reset_code, code):
        user.password_reset_attempts += 1
        db.commit()
        raise invalid


@router.post(
    "/password-reset/request",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Запросить код для смены забытого пароля",
    responses={
        204: {"description": "Если аккаунт существует — код отправлен на почту"},
        429: {"description": "Слишком много запросов с этого IP"},
    },
)
async def password_reset_request(
    data: PasswordResetRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """Отправляет код восстановления, если такой аккаунт есть и у него есть пароль.

    Всегда 204 — по ответу нельзя понять, зарегистрирован ли адрес.
    """
    ip = get_client_ip(request)
    if not check_ip_rate_limit(ip, max_attempts=10, window_minutes=5):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много запросов. Попробуйте через несколько минут.",
        )

    email = data.email.lower().strip()
    user = db.query(User).filter(User.email == email).first()

    # Чистым OAuth-юзерам (hashed_password пуст) сбрасывать нечего — они входят
    # через провайдера. Synthetic-адреса (Telegram/VK без почты) тоже мимо:
    # письмо физически некуда слать.
    if not user or not user.hashed_password or is_synthetic_oauth_email(user.email):
        logger.info(
            "Password reset requested for unusable account",
            extra={"extra_data": {"event": "password_reset_noop", "ip": ip}},
        )
        return None

    now = datetime.now(timezone.utc)
    if user.password_reset_sent_at:
        sent_at = user.password_reset_sent_at
        if sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)
        if (now - sent_at).total_seconds() < EMAIL_VERIFY_RESEND_COOLDOWN_SEC:
            # Кулдаун — молча выходим тем же 204: сообщать «подождите N секунд»
            # значило бы подтвердить, что аккаунт существует.
            return None

    code = _generate_verify_code()
    user.password_reset_code = code
    user.password_reset_expires_at = now + timedelta(minutes=EMAIL_VERIFY_TTL_MIN)
    user.password_reset_attempts = 0
    user.password_reset_sent_at = now
    db.commit()

    background_tasks.add_task(
        send_password_reset_email, user.email, code, getattr(user, "display_name", None)
    )
    logger.info(
        f"Password reset code issued for user {user.id}",
        extra={"extra_data": {"event": "password_reset_requested", "user_id": user.id, "ip": ip}},
    )
    return None


@router.post(
    "/password-reset/verify",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Проверить код, не меняя пароль",
    responses={
        204: {"description": "Код верный — можно переходить к вводу пароля"},
        400: {"description": "Неверный или истёкший код"},
        429: {"description": "Исчерпаны попытки ввода кода"},
    },
)
async def password_reset_verify(
    data: PasswordResetVerify,
    request: Request,
    db: Session = Depends(get_db),
):
    """Отдельный шаг проверки кода — чтобы не заставлять придумывать пароль
    вслепую и узнавать об опечатке в коде только после этого.

    Неудачная проверка тратит попытку так же, как в confirm: иначе этот
    эндпоинт стал бы бесплатным оракулом для перебора шестизначного кода.
    """
    ip = get_client_ip(request)
    if not check_ip_rate_limit(ip, max_attempts=20, window_minutes=5):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много попыток. Попробуйте через несколько минут.",
        )
    email = data.email.lower().strip()
    user = db.query(User).filter(User.email == email).first()
    _check_reset_code(user, data.code, db)
    return None


@router.post(
    "/password-reset/confirm",
    response_model=TokenResponse,
    summary="Сменить пароль по коду и войти",
    responses={
        200: {"description": "Пароль изменён, прежние сессии разлогинены, выдана новая"},
        400: {"description": "Неверный или истёкший код"},
        429: {"description": "Исчерпаны попытки ввода кода"},
    },
)
async def password_reset_confirm(
    data: PasswordResetConfirm,
    request: Request,
    db: Session = Depends(get_db),
):
    """Ставит новый пароль, гасит прежние сессии и сразу впускает в аккаунт.

    Токены возвращаются намеренно: человек только что доказал владение почтой
    и задал пароль — гнать его после этого на форму входа вводить тот же
    пароль ещё раз незачем.
    """
    ip = get_client_ip(request)
    if not check_ip_rate_limit(ip, max_attempts=20, window_minutes=5):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много попыток. Попробуйте через несколько минут.",
        )

    email = data.email.lower().strip()
    user = db.query(User).filter(User.email == email).first()
    _check_reset_code(user, data.code, db)

    user.hashed_password = hash_password(data.new_password)
    user.password_reset_code = None
    user.password_reset_expires_at = None
    user.password_reset_attempts = 0
    # Сброс пароля снимает и блокировку по неудачным входам: человек доказал
    # владение почтой, держать его в лок-ауте больше незачем.
    user.failed_login_attempts = 0
    user.locked_until = None

    # ⚠️ Отзываем ВСЕ refresh-токены. Если пароль меняют потому, что аккаунт
    # увели, чужая сессия обязана умереть вместе со старым паролем — иначе
    # злоумышленник продолжит обновлять токены как ни в чём не бывало.
    revoked = (
        db.query(RefreshToken)
        .filter(RefreshToken.user_id == user.id, RefreshToken.is_revoked.is_(False))
        .update({"is_revoked": True}, synchronize_session=False)
    )
    db.commit()

    # Новая пара выдаётся ПОСЛЕ отзыва старых — иначе массовый update погасил бы
    # и её тоже, и человек остался бы с мёртвым refresh сразу после сброса.
    tokens = create_token_pair(user.id, user.role)
    persist_refresh_token(db, user.id, tokens.refresh_token, request)
    user.last_login_at = datetime.now(timezone.utc)
    user.last_login_ip = ip
    db.commit()

    logger.info(
        f"Password reset completed for user {user.id} ({revoked} sessions revoked)",
        extra={"extra_data": {"event": "password_reset_completed", "user_id": user.id, "ip": ip}},
    )
    log_successful_login(user.id, user.email, ip, request.headers.get("user-agent", ""))

    return TokenResponse(
        access_token=tokens.access_token,
        refresh_token=tokens.refresh_token,
        token_type="bearer",
        expires_in=tokens.expires_in,
    )
