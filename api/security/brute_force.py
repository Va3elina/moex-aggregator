# api/security/brute_force.py
"""
Защита от брутфорс-атак.

ЧТО ТАКОЕ БРУТФОРС:
  Атака перебором — хакер пробует тысячи паролей:
  - password, password1, password123...
  - qwerty, 123456, admin...
  - Имя + год рождения...

  Автоматизированные инструменты могут делать сотни попыток в секунду.

КАК ЗАЩИЩАЕМСЯ:

1. БЛОКИРОВКА АККАУНТА:
   - 5 неправильных попыток → блокировка на 15 минут
   - Даже если хакер угадает пароль — не сможет войти

2. RATE LIMITING (уже есть в middleware.py):
   - 10 запросов/мин на /api/auth/*
   - Замедляет перебор

3. ЛОГИРОВАНИЕ:
   - Записываем все неудачные попытки
   - Можно отслеживать подозрительную активность

4. УВЕДОМЛЕНИЯ (TODO):
   - Telegram алерт если много попыток с одного IP
   - Email пользователю если кто-то пытается войти

КОНФИГУРАЦИЯ:
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from api.logger import get_logger

logger = get_logger()

# Настройки защиты
MAX_FAILED_ATTEMPTS = 5  # Максимум неудачных попыток
LOCKOUT_DURATION_MINUTES = 15  # Время блокировки
RESET_ATTEMPTS_AFTER_MINUTES = 30  # Сброс счётчика после успешного входа


def check_account_locked(
        failed_attempts: int,
        locked_until: Optional[datetime]
) -> tuple[bool, Optional[int]]:
    """
    Проверяет, заблокирован ли аккаунт.

    Args:
        failed_attempts: Количество неудачных попыток
        locked_until: Время до которого заблокирован (или None)

    Returns:
        (is_locked, seconds_remaining)
        - is_locked: True если аккаунт заблокирован
        - seconds_remaining: сколько секунд осталось до разблокировки

    Пример:
        is_locked, remaining = check_account_locked(5, locked_until_time)
        if is_locked:
            return {"error": f"Аккаунт заблокирован. Попробуйте через {remaining} сек"}
    """
    if locked_until is None:
        return False, None

    now = datetime.now(timezone.utc)

    # Приводим к timezone-aware если нужно
    if locked_until.tzinfo is None:
        locked_until = locked_until.replace(tzinfo=timezone.utc)

    if now < locked_until:
        remaining = int((locked_until - now).total_seconds())
        return True, remaining

    # Блокировка истекла
    return False, None


def should_lock_account(failed_attempts: int) -> bool:
    """
    Проверяет, нужно ли заблокировать аккаунт.

    Args:
        failed_attempts: Текущее количество неудачных попыток

    Returns:
        True если достигнут лимит попыток
    """
    return failed_attempts >= MAX_FAILED_ATTEMPTS


def calculate_lockout_time() -> datetime:
    """
    Вычисляет время окончания блокировки.

    Returns:
        datetime когда блокировка закончится
    """
    return datetime.now(timezone.utc) + timedelta(minutes=LOCKOUT_DURATION_MINUTES)


def log_failed_login(
        email: str,
        ip_address: str,
        user_agent: str,
        attempt_number: int,
        reason: str = "wrong_password"
):
    """
    Логирует неудачную попытку входа.

    ЗАЧЕМ:
    - Анализ атак
    - Выявление паттернов
    - Доказательная база при расследовании

    Args:
        email: Email который пытались использовать
        ip_address: IP адрес
        user_agent: Браузер/устройство
        attempt_number: Номер попытки
        reason: Причина (wrong_password, user_not_found, account_locked)
    """
    logger.warning(
        f"Failed login attempt",
        extra={
            "extra_data": {
                "event": "login_failed",
                "email": email,
                "ip": ip_address,
                "user_agent": user_agent[:100] if user_agent else None,  # Обрезаем длинные
                "attempt": attempt_number,
                "reason": reason,
            }
        }
    )

    # Если много попыток — это подозрительно
    if attempt_number >= 3:
        logger.warning(
            f"Multiple failed login attempts for {email}",
            extra={
                "extra_data": {
                    "event": "brute_force_suspected",
                    "email": email,
                    "ip": ip_address,
                    "attempts": attempt_number,
                }
            }
        )


def log_successful_login(
        user_id: int,
        email: str,
        ip_address: str,
        user_agent: str
):
    """
    Логирует успешный вход.

    Args:
        user_id: ID пользователя
        email: Email
        ip_address: IP адрес
        user_agent: Браузер/устройство
    """
    logger.info(
        f"Successful login for user {user_id}",
        extra={
            "extra_data": {
                "event": "login_success",
                "user_id": user_id,
                "email": email,
                "ip": ip_address,
            }
        }
    )


def log_account_locked(email: str, ip_address: str, locked_until: datetime):
    """
    Логирует блокировку аккаунта.

    Это важное событие безопасности — возможна атака!
    """
    logger.warning(
        f"Account locked due to multiple failed attempts: {email}",
        extra={
            "extra_data": {
                "event": "account_locked",
                "email": email,
                "ip": ip_address,
                "locked_until": locked_until.isoformat(),
            }
        }
    )


# === Дополнительная защита: отслеживание IP ===

# В production лучше использовать Redis для хранения
_ip_attempts: dict[str, list[datetime]] = {}


def check_ip_rate_limit(ip_address: str, max_attempts: int = 20, window_minutes: int = 5) -> bool:
    """
    Rate limit по IP для auth-путей. Счётчик в Redis (общий для всех
    gunicorn-воркеров) — fixed-window INCR+EXPIRE, тот же паттерн, что в
    middleware.RateLimitMiddleware. Прежний in-memory dict при workers=3 давал
    ~3× реального лимита и сбрасывался на каждом деплое/рестарте воркера.

    fail-open: при недоступности Redis падаем на in-memory fallback — моргнувший
    Redis не должен ронять все логины (account-lockout остаётся жёстким backstop).

    Returns: True если лимит НЕ превышен (можно продолжать), False если превышен.
    """
    window_seconds = window_minutes * 60
    try:
        from api.cache import _get_redis
        r = _get_redis()
        rkey = f"auth_iprl:{ip_address}:{max_attempts}:{window_minutes}"
        count = r.incr(rkey)
        if count == 1:
            r.expire(rkey, window_seconds)
        if count > max_attempts:
            logger.warning(
                f"IP rate limit exceeded: {ip_address}",
                extra={"extra_data": {
                    "event": "ip_rate_limit",
                    "ip": ip_address,
                    "attempts": count,
                    "window_minutes": window_minutes,
                }},
            )
            return False
        return True
    except Exception as e:
        logger.warning(f"check_ip_rate_limit: Redis недоступен, fallback in-memory: {e}")
        return _check_ip_rate_limit_inmemory(ip_address, max_attempts, window_minutes)


def _check_ip_rate_limit_inmemory(ip_address: str, max_attempts: int = 20, window_minutes: int = 5) -> bool:
    """
    In-memory fallback для check_ip_rate_limit (per-worker, НЕ cross-worker).
    Проверяет rate limit по IP (дополнительно к middleware).

    Защищает от:
    - Перебора разных email с одного IP
    - Distributed брутфорса одного аккаунта

    Args:
        ip_address: IP адрес
        max_attempts: Максимум попыток в окне
        window_minutes: Размер окна в минутах

    Returns:
        True если лимит НЕ превышен (можно продолжать)
        False если лимит превышен (блокировать)
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=window_minutes)

    # Получаем историю попыток с этого IP
    attempts = _ip_attempts.get(ip_address, [])

    # Фильтруем только недавние
    recent_attempts = [a for a in attempts if a > window_start]

    # Обновляем историю
    _ip_attempts[ip_address] = recent_attempts

    if len(recent_attempts) >= max_attempts:
        logger.warning(
            f"IP rate limit exceeded: {ip_address}",
            extra={
                "extra_data": {
                    "event": "ip_rate_limit",
                    "ip": ip_address,
                    "attempts": len(recent_attempts),
                    "window_minutes": window_minutes,
                }
            }
        )
        return False

    # Записываем новую попытку
    recent_attempts.append(now)
    _ip_attempts[ip_address] = recent_attempts

    return True


def cleanup_ip_attempts():
    """
    Очищает старые записи IP (вызывать периодически).

    В production лучше использовать Redis с TTL.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)

    for ip in list(_ip_attempts.keys()):
        _ip_attempts[ip] = [a for a in _ip_attempts[ip] if a > cutoff]
        if not _ip_attempts[ip]:
            del _ip_attempts[ip]