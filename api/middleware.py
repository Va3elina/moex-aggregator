"""
Middleware для логирования HTTP запросов
- Логирует все входящие запросы и ответы
- Измеряет время выполнения
- Генерирует request_id для трейсинга
"""

import time
import uuid
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from api.logger import get_logger

logger = get_logger()


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware для логирования всех HTTP запросов"""

    def __init__(self, app: ASGIApp, exclude_paths: list[str] | None = None):
        super().__init__(app)
        # Пути, которые не логируем (health checks, статика)
        self.exclude_paths = exclude_paths or ["/health", "/assets", "/favicon.ico"]

    async def dispatch(self, request: Request, call_next) -> Response:
        # Пропускаем excluded пути
        if any(request.url.path.startswith(p) for p in self.exclude_paths):
            return await call_next(request)

        # Генерируем уникальный ID запроса
        request_id = str(uuid.uuid4())[:8]

        # Сохраняем в state для использования в эндпоинтах
        request.state.request_id = request_id

        # Данные запроса
        client_ip = request.client.host if request.client else "unknown"
        method = request.method
        path = request.url.path
        query = str(request.query_params) if request.query_params else ""

        # Логируем входящий запрос
        logger.info(
            f"→ {method} {path}",
            extra={
                "extra_data": {
                    "request_id": request_id,
                    "type": "request",
                    "method": method,
                    "path": path,
                    "query": query,
                    "client_ip": client_ip,
                    "user_agent": request.headers.get("user-agent", ""),
                }
            }
        )

        # Замеряем время
        start_time = time.perf_counter()

        try:
            response = await call_next(request)

            # Время выполнения
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

            # Логируем ответ
            log_method = logger.info if response.status_code < 400 else logger.warning
            log_method(
                f"← {response.status_code} {path} ({duration_ms}ms)",
                extra={
                    "extra_data": {
                        "request_id": request_id,
                        "type": "response",
                        "method": method,
                        "path": path,
                        "status_code": response.status_code,
                        "duration_ms": duration_ms,
                    }
                }
            )

            # Добавляем request_id в headers ответа (для дебага)
            response.headers["X-Request-ID"] = request_id

            return response

        except Exception as e:
            # Время до ошибки
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

            # Логируем ошибку
            logger.error(
                f"✕ {method} {path} - {type(e).__name__}: {str(e)}",
                extra={
                    "extra_data": {
                        "request_id": request_id,
                        "type": "error",
                        "method": method,
                        "path": path,
                        "duration_ms": duration_ms,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    }
                },
                exc_info=True,
            )
            raise


class SlowRequestMiddleware(BaseHTTPMiddleware):
    """Middleware для отслеживания медленных запросов"""

    def __init__(self, app: ASGIApp, threshold_ms: float = 1000):
        super().__init__(app)
        self.threshold_ms = threshold_ms

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start_time) * 1000

        if duration_ms > self.threshold_ms:
            logger.warning(
                f"🐢 Slow request: {request.method} {request.url.path} took {duration_ms:.0f}ms",
                extra={
                    "extra_data": {
                        "type": "slow_request",
                        "path": request.url.path,
                        "duration_ms": round(duration_ms, 2),
                        "threshold_ms": self.threshold_ms,
                    }
                }
            )

        return response