# api/security/__init__.py
"""
Модуль безопасности MOEX Analytics.

Содержит:
- password.py — хэширование паролей (Argon2)
- jwt.py — создание и проверка JWT токенов
- brute_force.py — защита от перебора паролей
"""

from .password import (
    hash_password,
    verify_password,
    generate_secure_token,
    check_password_strength,
)

from .jwt import (
    create_access_token,
    create_refresh_token,
    verify_token,
    create_token_pair,
    TokenPayload,
    TokenPair,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_DAYS,
)

from .brute_force import (
    check_account_locked,
    should_lock_account,
    calculate_lockout_time,
    log_failed_login,
    log_successful_login,
    check_ip_rate_limit,
)

__all__ = [
    # Password
    "hash_password",
    "verify_password",
    "generate_secure_token",
    "check_password_strength",

    # JWT
    "create_access_token",
    "create_refresh_token",
    "verify_token",
    "create_token_pair",
    "TokenPayload",
    "TokenPair",
    "ACCESS_TOKEN_EXPIRE_MINUTES",
    "REFRESH_TOKEN_EXPIRE_DAYS",

    # Brute force
    "check_account_locked",
    "should_lock_account",
    "calculate_lockout_time",
    "log_failed_login",
    "log_successful_login",
    "check_ip_rate_limit",
]