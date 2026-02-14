# api/security/password.py
"""
Хэширование паролей с помощью Argon2.

ЧТО ЭТО: Функции для безопасной работы с паролями.

ЗАЧЕМ: Пароли НИКОГДА нельзя хранить в открытом виде!
Если база данных утечёт (а это случается даже с крупными компаниями),
хакер не должен получить доступ к паролям пользователей.

КАК РАБОТАЕТ ХЭШИРОВАНИЕ:
  "qwerty123" → hash() → "$argon2id$v=19$m=65536,t=3,p=4$abc123..."

  Особенности:
  - Необратимо: из хэша нельзя получить исходный пароль
  - Детерминировано: один и тот же пароль даёт разные хэши (соль!)
  - Медленно: специально, чтобы усложнить перебор

ПОЧЕМУ ARGON2, А НЕ MD5/SHA256/BCRYPT:
  - MD5/SHA256: слишком быстрые, GPU может перебирать миллиарды хэшей/сек
  - bcrypt: хороший, но старый (1999), оптимизирован для CPU
  - Argon2: победитель конкурса PHC 2015, защита от GPU и ASIC атак,
            использует много памяти (memory-hard)

ПРИМЕР АТАКИ БЕЗ ARGON2:
  Хакер украл базу с MD5 хэшами:
  - Запускает перебор на GPU: 10 миллиардов хэшей/сек
  - Пароль "qwerty123" взломан за 0.001 секунды

С ARGON2:
  - Каждая проверка занимает ~0.3 сек и 64 МБ памяти
  - Даже простой пароль взламывается годами

УСТАНОВКА: pip install passlib[argon2] argon2-cffi
"""

from passlib.context import CryptContext
import secrets
import string

# Настройка контекста хэширования
pwd_context = CryptContext(
    schemes=["argon2"],  # Используем только Argon2
    deprecated="auto",  # Автоматически обновлять старые хэши

    # Параметры Argon2 (можно настроить под мощность сервера):
    # argon2__memory_cost=65536,  # 64 МБ памяти
    # argon2__time_cost=3,        # 3 итерации
    # argon2__parallelism=4,      # 4 потока
)


def hash_password(password: str) -> str:
    """
    Создаёт хэш пароля для хранения в БД.

    Args:
        password: Пароль в открытом виде (от пользователя)

    Returns:
        Хэш пароля, например:
        "$argon2id$v=19$m=65536,t=3,p=4$randomsalt$longhashstring"

    Пример:
        hash_password("qwerty123")
        → "$argon2id$v=19$m=65536,t=3,p=4$Kx7..."

        hash_password("qwerty123")  # Тот же пароль
        → "$argon2id$v=19$m=65536,t=3,p=4$Yz9..."  # Другой хэш! (разная соль)
    """
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Проверяет, соответствует ли пароль хэшу.

    Args:
        plain_password: Пароль, который ввёл пользователь
        hashed_password: Хэш из базы данных

    Returns:
        True если пароль верный, False если нет

    Пример:
        stored_hash = "$argon2id$v=19$m=65536,t=3,p=4$..."

        verify_password("qwerty123", stored_hash)  → True
        verify_password("wrongpass", stored_hash)  → False

    ВАЖНО: Эта функция специально работает за константное время,
    чтобы защитить от timing-атак (нельзя угадать длину пароля
    по времени ответа).
    """
    return pwd_context.verify(plain_password, hashed_password)


def generate_secure_token(length: int = 32) -> str:
    """
    Генерирует криптографически безопасный токен.

    Используется для:
    - Refresh токенов
    - Токенов сброса пароля
    - Токенов подтверждения email

    Args:
        length: Длина токена в байтах (32 байта = 256 бит)

    Returns:
        URL-safe строка, например: "Kj8_xY2-mN4pQ7rS..."

    ПОЧЕМУ secrets, А НЕ random:
        random.random() — предсказуем, использует системное время
        secrets.token_urlsafe() — использует криптографический генератор ОС
    """
    return secrets.token_urlsafe(length)


def generate_temp_password(length: int = 12) -> str:
    """
    Генерирует временный пароль (для сброса пароля через email).

    Args:
        length: Длина пароля

    Returns:
        Случайный пароль из букв и цифр, например: "Kj8xY2mN4pQ7"
    """
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def check_password_strength(password: str) -> dict:
    """
    Проверяет надёжность пароля.

    Returns:
        {
            "is_valid": True/False,
            "score": 0-5,
            "errors": ["Пароль слишком короткий", ...]
        }
    """
    errors = []
    score = 0

    # Минимальная длина
    if len(password) < 8:
        errors.append("Минимум 8 символов")
    else:
        score += 1

    # Есть заглавные буквы
    if not any(c.isupper() for c in password):
        errors.append("Добавьте заглавную букву")
    else:
        score += 1

    # Есть строчные буквы
    if not any(c.islower() for c in password):
        errors.append("Добавьте строчную букву")
    else:
        score += 1

    # Есть цифры
    if not any(c.isdigit() for c in password):
        errors.append("Добавьте цифру")
    else:
        score += 1

    # Есть спецсимволы
    special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
    if any(c in special_chars for c in password):
        score += 1

    return {
        "is_valid": len(errors) == 0,
        "score": score,
        "errors": errors
    }