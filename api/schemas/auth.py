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
import re


# === РЕГИСТРАЦИЯ ===

class UserRegister(BaseModel):
    """
    Схема для регистрации нового пользователя.

    Пример запроса:
    POST /api/auth/register
    {
        "email": "user@example.com",
        "password": "SecurePass123!",
        "username": "myusername"
    }
    """
    email: EmailStr = Field(
        ...,  # ... означает обязательное поле
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

    username: Optional[str] = Field(
        None,
        min_length=3,
        max_length=50,
        description="Имя пользователя (опционально)",
        examples=["trader123"]
    )

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        """
        Проверяет сложность пароля.

        Требования:
        - Минимум 8 символов (уже проверено в Field)
        - Хотя бы одна цифра
        - Хотя бы одна буква
        """
        if not any(c.isdigit() for c in v):
            raise ValueError("Пароль должен содержать хотя бы одну цифру")
        if not any(c.isalpha() for c in v):
            raise ValueError("Пароль должен содержать хотя бы одну букву")
        return v

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: Optional[str]) -> Optional[str]:
        """
        Проверяет формат username.

        Разрешены: буквы, цифры, подчёркивание, дефис
        """
        if v is None:
            return v
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError("Username может содержать только буквы, цифры, _ и -")
        return v.lower()  # Приводим к нижнему регистру


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
        "username": "trader123",
        "role": "user",
        "is_verified": true,
        "created_at": "2025-01-18T12:00:00Z"
    }
    """
    id: int
    email: str
    username: Optional[str]
    role: str
    is_verified: bool
    created_at: datetime

    class Config:
        from_attributes = True  # Позволяет создавать из SQLAlchemy модели


class UserUpdate(BaseModel):
    """
    Схема для обновления профиля.

    Пример запроса:
    PUT /api/auth/profile
    {
        "username": "new_username"
    }
    """
    username: Optional[str] = Field(
        None,
        min_length=3,
        max_length=50
    )

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError("Username может содержать только буквы, цифры, _ и -")
        return v.lower()


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

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        if not any(c.isdigit() for c in v):
            raise ValueError("Пароль должен содержать хотя бы одну цифру")
        if not any(c.isalpha() for c in v):
            raise ValueError("Пароль должен содержать хотя бы одну букву")
        return v


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

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        if not any(c.isdigit() for c in v):
            raise ValueError("Пароль должен содержать хотя бы одну цифру")
        if not any(c.isalpha() for c in v):
            raise ValueError("Пароль должен содержать хотя бы одну букву")
        return v


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