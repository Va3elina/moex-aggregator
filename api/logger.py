"""
Модуль логирования для MOEX Analytics API
- JSON формат для продакшена (легко парсить)
- Ротация файлов (не забьёт диск)
- Цветной вывод в консоль для разработки
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler
import json
from typing import Any

# Папка для логов
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


class JSONFormatter(logging.Formatter):
    """JSON форматтер для структурированных логов"""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Добавляем extra поля (request_id, user_id и т.д.)
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)

        # Добавляем exception если есть
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """Цветной форматтер для консоли (dev режим)"""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname:8}{self.RESET}"
        return super().format(record)


def setup_logging(
        level: str = "INFO",
        json_logs: bool = False,
        log_to_file: bool = True,
) -> logging.Logger:
    """
    Настройка логирования

    Args:
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR)
        json_logs: Использовать JSON формат (для продакшена)
        log_to_file: Писать логи в файл

    Returns:
        Настроенный logger
    """
    logger = logging.getLogger("moex_api")
    logger.setLevel(getattr(logging, level.upper()))

    # Очищаем старые handlers
    logger.handlers.clear()

    # Консольный handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    if json_logs:
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(ColoredFormatter(
            fmt="%(asctime)s │ %(levelname)s │ %(name)s │ %(message)s",
            datefmt="%H:%M:%S"
        ))

    logger.addHandler(console_handler)

    # Файловый handler с ротацией
    if log_to_file:
        # Основной лог (все уровни)
        file_handler = RotatingFileHandler(
            LOG_DIR / "api.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)

        # Отдельный лог для ошибок
        error_handler = RotatingFileHandler(
            LOG_DIR / "errors.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(JSONFormatter())
        logger.addHandler(error_handler)

    return logger


def get_logger(name: str = "moex_api") -> logging.Logger:
    """Получить logger по имени"""
    return logging.getLogger(name)


class LoggerAdapter(logging.LoggerAdapter):
    """Адаптер для добавления extra данных в логи"""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict]:
        # Добавляем extra_data для JSON форматтера
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        kwargs["extra"]["extra_data"] = self.extra
        return msg, kwargs


def get_request_logger(request_id: str, user_id: str | None = None) -> LoggerAdapter:
    """
    Создаёт logger с привязкой к конкретному запросу

    Args:
        request_id: ID запроса
        user_id: ID пользователя (опционально)
    """
    logger = get_logger()
    extra = {"request_id": request_id}
    if user_id:
        extra["user_id"] = user_id
    return LoggerAdapter(logger, extra)