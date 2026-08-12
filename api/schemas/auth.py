# api/schemas/auth.py
"""
Pydantic схемы для аутентификации.

ЧТО ЭТО: Схемы определяют структуру запросов и ответов API.
Pydantic автоматически валидирует данные и возвращает понятные ошибки.

ЗАЧЕМ:
1. Валидация — email должен быть email, пароль не пустой
2. Документация — Swagger автоматически показывает структуру
3. Типизация — IDE подсказывает поля
4. Безопасность — отсекаем невалидные данные на входе
"""

from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional
from datetime import datetime


# === РЕГИСТРАЦИЯ ===

class UserRegister(BaseModel):
    """
    Схема для регистрации нового пользователя.

    Пример запроса:
    POST /api/auth/register
    {
        "email": "user@example.com",
        "password": "SecurePass123!"
    }
    """
    email: EmailStr = Field(
        ...,
        description="Email пользователя",
        examples=["user@example.com"]
    )

    password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Пароль (минимум 8 символов)",
        examples=["SecurePass123!"]
    )

    # Требований к составу пароля намеренно нет: единственное правило —
    # минимум 8 символов из Field выше. Обязательные цифра/буква (а в
    # check_password_strength ещё и регистры) отсеивали живых людей на
    # регистрации, что для этого сервиса дороже, чем выигрыш в стойкости.


# === ЛОГИН ===

class UserLogin(BaseModel):
    """
    Схема для входа.

    Пример запроса:
    POST /api/auth/login
    {
        "email": "user@example.com",
        "password": "SecurePass123!"
    }
    """
    email: EmailStr = Field(
        ...,
        description="Email пользователя"
    )

    password: str = Field(
        ...,
        min_length=1,  # Не проверяем сложность при логине
        max_length=128,
        description="Пароль"
    )


# === ТОКЕНЫ ===

class TokenResponse(BaseModel):
    """
    Ответ с токенами после успешного логина.

    Пример ответа:
    {
        "access_token": "eyJhbGciOiJIUzI1NiIs...",
        "refresh_token": "eyJhbGciOiJIUzI1NiIs...",
        "token_type": "bearer",
        "expires_in": 900
    }
    """
    access_token: str = Field(..., description="JWT access токен")
    refresh_token: str = Field(..., description="JWT refresh токен")
    token_type: str = Field(default="bearer", description="Тип токена")
    expires_in: int = Field(..., description="Время жизни access токена в секундах")


class RefreshTokenRequest(BaseModel):
    """
    Запрос на обновление токенов.

    Пример запроса:
    POST /api/auth/refresh
    {
        "refresh_token": "eyJhbGciOiJIUzI1NiIs..."
    }
    """
    refresh_token: str = Field(..., description="Refresh токен")


# === ПОЛЬЗОВАТЕЛЬ ===

class UserResponse(BaseModel):
    """
    Информация о пользователе (без чувствительных данных).

    Пример ответа:
    {
        "id": 123,
        "email": "user@example.com",
        "role": "user",
        "is_verified": true,
        "created_at": "2025-01-18T12:00:00Z"
    }
    """
    id: int
    email: str
    display_name: Optional[str] = None
    role: str
    is_verified: bool
    avatar_url: Optional[str] = None
    has_password: bool = True
    oauth_providers: list[str] = []
    created_at: datetime
    # True если email — synthetic placeholder вроде telegram_123@oauth.local
    # (OAuth-провайдер не дал email и был создан fallback). Frontend
    # редиректит таких юзеров на /add-email до тех пор пока не введут реальный
    # email — без него T-Bank не сможет выдать чек по 54-ФЗ.
    requires_email_setup: bool = False


# === ПРИВЯЗКА EMAIL (для OAuth-юзеров без него) ===

class AddEmailRequest(BaseModel):
    """
    Запрос на привязку реального email к OAuth-аккаунту.

    Используется юзерами зарегистрировавшимися через Telegram (всегда без email)
    или VK (часто без email). После Phase 2 фичи добавится verification — пока
    что просто сохраняем email без подтверждения.
    """
    email: EmailStr = Field(..., description="Реальный email пользователя")

    class Config:
        from_attributes = True


# === ПОДТВЕРЖДЕНИЕ EMAIL (код из письма) ===

class VerifyEmailRequest(BaseModel):
    """Запрос на подтверждение email 6-значным кодом из письма."""
    code: str = Field(..., description="6-значный код из письма", examples=["123456"])

    @field_validator("code")
    @classmethod
    def validate_code(cls, v: str) -> str:
        v = v.strip()
        if not (v.isdigit() and len(v) == 6):
            raise ValueError("Код должен состоять из 6 цифр")
        return v


# === ВОССТАНОВЛЕНИЕ ПАРОЛЯ ===

class PasswordResetRequest(BaseModel):
    """Запрос кода для смены забытого пароля (эндпоинт отвечает 200 всегда)."""
    email: EmailStr = Field(..., description="Email аккаунта", examples=["user@example.com"])


class PasswordResetVerify(BaseModel):
    """Проверка кода отдельным шагом, до ввода нового пароля."""
    email: EmailStr = Field(..., description="Email аккаунта")
    code: str = Field(..., description="6-значный код из письма", examples=["123456"])

    @field_validator("code")
    @classmethod
    def validate_code(cls, v: str) -> str:
        v = v.strip()
        if not (v.isdigit() and len(v) == 6):
            raise ValueError("Код должен состоять из 6 цифр")
        return v


class PasswordResetConfirm(BaseModel):
    """Смена пароля по коду из письма.

    Требования к новому паролю те же, что при регистрации: только длина от 8.
    Ужесточать правила именно здесь нельзя — человек и так восстанавливает
    доступ, дополнительный барьер тут дороже выигрыша в стойкости.
    """
    email: EmailStr = Field(..., description="Email аккаунта")
    code: str = Field(..., description="6-значный код из письма", examples=["123456"])
    new_password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Новый пароль (минимум 8 символов)",
    )

    @field_validator("code")
    @classmethod
    def validate_code(cls, v: str) -> str:
        v = v.strip()
        if not (v.isdigit() and len(v) == 6):
            raise ValueError("Код должен состоять из 6 цифр")
        return v


# === СМЕНА ПАРОЛЯ ===

class PasswordChange(BaseModel):
    """
    Схема для смены пароля.

    Пример запроса:
    POST /api/auth/change-password
    {
        "current_password": "OldPass123!",
        "new_password": "NewSecurePass456!"
    }
    """
    current_password: str = Field(
        ...,
        min_length=1,
        description="Текущий пароль"
    )

    new_password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Новый пароль"
    )

    # Требований к составу нет — см. комментарий в UserRegister.


# === СБРОС ПАРОЛЯ ===

class PasswordResetRequest(BaseModel):
    """
    Запрос на сброс пароля (отправка email).

    Пример запроса:
    POST /api/auth/forgot-password
    {
        "email": "user@example.com"
    }
    """
    email: EmailStr


class PasswordReset(BaseModel):
    """
    Установка нового пароля по токену из email.

    Пример запроса:
    POST /api/auth/reset-password
    {
        "token": "abc123...",
        "new_password": "NewSecurePass456!"
    }
    """
    token: str = Field(..., min_length=20)
    new_password: str = Field(..., min_length=8, max_length=128)

    # Требований к составу нет — см. комментарий в UserRegister.


# === ОШИБКИ ===

class AuthError(BaseModel):
    """
    Стандартный формат ошибки аутентификации.

    Пример:
    {
        "detail": "Неверный email или пароль",
        "code": "invalid_credentials"
    }
    """
    detail: str
    code: str