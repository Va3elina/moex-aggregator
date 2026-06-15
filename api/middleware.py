"""
Middleware для MOEX Analytics API
- Логирование запросов
- Security Headers
- Rate Limiting
- Exception Handlers
"""

import time
import uuid
import asyncio
import os
import traceback
from collections import defaultdict
from fastapi import Request, Response, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from starlette.exceptions import HTTPException as StarletteHTTPException
import ipaddress

# Доверяем X-Forwarded-For ТОЛЬКО от reverse-proxy внутри docker-сети.
# Сужено до docker-bridge диапазона (172.16/12) + loopback: этот стек НЕ
# использует 10.0.0.0/8 и 192.168.0.0/16, держать их в доверенных — лишний
# простор для IP-спуфинга в логах/rate-limit, если бы что-то из этих диапазонов
# смогло достучаться к :8000 напрямую (CWE-348).
_TRUSTED_PROXY_NETS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("172.16.0.0/12"),
]


def _is_trusted_proxy(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
        return any(addr in net for net in _TRUSTED_PROXY_NETS)
    except ValueError:
        return False


def get_client_ip_safe(request: Request) -> str:
    """Получить IP клиента, доверяя proxy-заголовкам только от nginx/Docker."""
    direct_ip = request.client.host if request.client else "unknown"
    if _is_trusted_proxy(direct_ip):
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()
    return direct_ip

from api.logger import get_logger

logger = get_logger()


# ═══════════════════════════════════════════════════════════════
# Логирование запросов
# ═══════════════════════════════════════════════════════════════

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware для логирования всех HTTP запросов"""

    def __init__(self, app: ASGIApp, exclude_paths: list[str] | None = None):
        super().__init__(app)
        self.exclude_paths = exclude_paths or ["/health", "/assets", "/favicon.ico"]

    async def dispatch(self, request: Request, call_next) -> Response:
        if any(request.url.path.startswith(p) for p in self.exclude_paths):
            return await call_next(request)

        request_id = str(uuid.uuid4())[:8]
        request.state.request_id = request_id

        client_ip = self._get_client_ip(request)
        method = request.method
        path = request.url.path
        query = str(request.query_params) if request.query_params else ""

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

        start_time = time.perf_counter()

        try:
            response = await call_next(request)
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

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

            response.headers["X-Request-ID"] = request_id
            return response

        except Exception as e:
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
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

    def _get_client_ip(self, request: Request) -> str:
        return get_client_ip_safe(request)


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


# ═══════════════════════════════════════════════════════════════
# Security Headers
# ═══════════════════════════════════════════════════════════════

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Добавляет HTTP заголовки безопасности:
    - X-Content-Type-Options: защита от MIME sniffing
    - X-Frame-Options: защита от clickjacking
    - X-XSS-Protection: защита от XSS (legacy браузеры)
    - Referrer-Policy: контроль передачи referrer
    """

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Защита от MIME type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # Защита от clickjacking
        response.headers["X-Frame-Options"] = "DENY"

        # XSS фильтр (для старых браузеров)
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # Контроль Referrer
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # Ограничение API браузера
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=()"
        )

        # HSTS — принудительный HTTPS (только в production)
        if os.getenv("ENV") == "production":
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )

        return response


# ═══════════════════════════════════════════════════════════════
# Rate Limiting
# ═══════════════════════════════════════════════════════════════

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Ограничение запросов по IP адресу

    - Общий лимит: 100 запросов в минуту
    - Auth эндпоинты: 10 запросов в минуту
    - Тяжёлые эндпоинты: 30 запросов в минуту
    """

    def __init__(
        self,
        app,
        requests_per_minute: int = 100,
        auth_requests_per_minute: int = 10,
        heavy_requests_per_minute: int = 30,
    ):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.auth_requests_per_minute = auth_requests_per_minute
        self.heavy_requests_per_minute = heavy_requests_per_minute

        self.requests: dict[str, list[float]] = defaultdict(list)
        self.lock = asyncio.Lock()

        self.heavy_endpoints = ["/api/heatmap", "/api/stats", "/api/chart"]
        self.auth_endpoints = ["/api/auth"]

    def _get_client_ip(self, request: Request) -> str:
        return get_client_ip_safe(request)

    def _get_limit_for_path(self, path: str) -> int:
        # /api/auth/me и /api/auth/oauth/providers — read-only, общий лимит
        if path in ("/api/auth/me", "/api/auth/oauth/providers"):
            return self.requests_per_minute
        if any(path.startswith(ep) for ep in self.auth_endpoints):
            return self.auth_requests_per_minute
        if any(path.startswith(ep) for ep in self.heavy_endpoints):
            return self.heavy_requests_per_minute
        return self.requests_per_minute

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Статика: /assets, /logos, /videos, /showcase, favicon — не считаем в rate limit.
        # Модалка выбора актива может параллельно запросить ~100 лого-файлов;
        # HLS-стрим тянет десятки .ts сегментов на каждое видео карточки.
        # Без exclude'а они съедают лимит 100 req/min и ломают API.
        if (
            path in ["/health", "/favicon.ico"]
            or path.startswith("/assets")
            or path.startswith("/logos")
            or path.startswith("/videos")
            or path.startswith("/showcase")
        ):
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        limit = self._get_limit_for_path(path)

        # Счётчик в Redis (общий для всех gunicorn-воркеров): прежний in-memory
        # defaultdict считал НА КАЖДЫЙ воркер отдельно → реальный лимит был ×3
        # (workers=3). Fixed-window: ключ на минутный бакет, INCR + EXPIRE(60).
        # fail-OPEN: если Redis недоступен — пропускаем (nginx limit_req на 80/443
        # остаётся жёстким backstop'ом; класть сайт из-за моргнувшего Redis нельзя).
        # auth-пути НАМЕРЕННО не fail-closed — иначе блинк Redis ронял бы все логины.
        now = time.time()
        over_limit = False
        retry_after = 60
        remaining = limit
        try:
            import redis as _redis_mod
            from api.cache import _get_redis
            r = _get_redis()
            bucket = int(now // 60)
            rkey = f"ratelimit:{client_ip}:{limit}:{bucket}"
            count = r.incr(rkey)
            if count == 1:
                r.expire(rkey, 60)
            if count > limit:
                over_limit = True
                ttl = r.ttl(rkey)
                retry_after = ttl if isinstance(ttl, int) and ttl > 0 else 60
            else:
                remaining = max(0, limit - count)
        except (_redis_mod.RedisError, ConnectionError, OSError) as e:
            logger.warning(f"RateLimit: Redis недоступен, fail-open: {e}")
            remaining = limit  # пропускаем запрос

        if over_limit:
            logger.warning(
                f"Rate limit exceeded for {client_ip} on {path}",
                extra={
                    "extra_data": {
                        "type": "rate_limit",
                        "client_ip": client_ip,
                        "path": path,
                        "limit": limit,
                    }
                }
            )
            return JSONResponse(
                status_code=429,
                content={
                    "success": False,
                    "error": {
                        "code": 429,
                        "message": "Слишком много запросов. Попробуйте позже.",
                        "retry_after": retry_after
                    }
                },
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(limit),
                    "X-RateLimit-Remaining": "0",
                }
            )

        response = await call_next(request)
        # /api/v1/public/* устанавливает свои X-RateLimit-* headers через
        # check_rate_limit dependency (per-key, не per-IP). IP-лимит здесь
        # остаётся как backstop в случае compromised key, но headers не
        # перезаписываем — иначе клиенты увидят 100/min вместо реальных
        # 60/min на ключ.
        if not path.startswith("/api/v1/public"):
            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(remaining)

        return response


# ═══════════════════════════════════════════════════════════════
# Exception Handlers
# ═══════════════════════════════════════════════════════════════

async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Обработка HTTP ошибок"""
    if exc.status_code >= 500:
        logger.error(f"HTTP {exc.status_code}: {exc.detail}")

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": {
                "code": exc.status_code,
                "message": exc.detail
            }
        }
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Обработка ошибок валидации"""
    errors = []
    for error in exc.errors():
        field_path = ".".join(str(loc) for loc in error["loc"] if loc != "body")
        msg = error["msg"]
        error_type = error["type"]

        if error_type == "missing":
            msg = "Обязательное поле"
        elif error_type == "string_too_short":
            msg = "Слишком короткое значение"
        elif error_type == "string_too_long":
            msg = "Слишком длинное значение"
        elif error_type == "literal_error":
            msg = "Недопустимое значение"
        elif "pattern" in error_type:
            msg = "Недопустимые символы"

        errors.append({"field": field_path, "message": msg})

    logger.warning(f"Validation error on {request.url.path}: {errors}")

    return JSONResponse(
        status_code=422,
        content={
            "success": False,
            "error": {
                "code": 422,
                "message": "Ошибка валидации данных",
                "details": errors
            }
        }
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Обработка непредвиденных ошибок
    ВАЖНО: НЕ раскрываем stack trace клиенту!
    """
    logger.error(
        f"Unhandled exception: {type(exc).__name__}: {str(exc)}",
        extra={
            "extra_data": {
                "type": "unhandled_exception",
                "path": request.url.path,
                "traceback": traceback.format_exc()
            }
        }
    )

    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": {
                "code": 500,
                "message": "Внутренняя ошибка сервера"
            }
        }
    )


def setup_exception_handlers(app):
    """Регистрация обработчиков ошибок"""
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, generic_exception_handler)