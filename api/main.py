"""
MOEX Analytics API
Главный файл приложения
"""

import asyncio
import mimetypes
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException

# HLS mime types — Python mimetypes по умолчанию не знает .m3u8/.ts (в обоих
# случаях не video). Регистрируем явно, иначе Safari не понимает HLS playlist.
mimetypes.add_type("application/vnd.apple.mpegurl", ".m3u8")
mimetypes.add_type("video/mp2t", ".ts")
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from api.routers import (
    instruments_router,
    candles_router,
    open_interest_router,
    oi_router,
    chart_router,
    heatmap_router,
    funds_router,
    breadth_router,
    buffett_router,
    seasonality_router,
    billing_router,
    cbr_flows_router,
    csp_report_router,
    exports_router,
    api_keys_router,
    public_api_router,
    fund_trades_router,
    oi_intl_router,
)
from api.routers import stats
from api.routers import health_monitor  # ← /api/health/data (барометр свежести, admin/X-API-Key)
from api.routers import auth  # ← НОВОЕ: Аутентификация
from api.routers import oauth  # ← OAuth (Google, VK, Telegram)
from api.routers import events  # ← SSE real-time events
from api.routers import analytics  # ← Custom analytics: POST /event + GET /stats
from api.routers import extension  # ← /api/extension/* (токены для расширения терминала)
from api.routers import alerts  # ← /api/alerts/* (Telegram alert-bot: привязка + алерты)
from api.routers import anomalies  # ← /api/anomalies/* (лента всплывающих аномалий: тосты+колокол)
from api.routers import content_news  # ← /api/admin/content-candidates/* (Kanban «Новости»)
from api.routers import mandate_scan  # ← /api/internal/mandate-scan/* (еженедельный Routine-скаут вынужденных потоков)

# Логирование
from api.logger import setup_logging, get_logger

# Middleware
from api.middleware import (
    RequestLoggingMiddleware,
    SlowRequestMiddleware,
    SecurityHeadersMiddleware,
    RateLimitMiddleware,
    setup_exception_handlers,
)

# ═══════════════════════════════════════════════════════════════
# Определение среды
# ═══════════════════════════════════════════════════════════════

IS_PRODUCTION = os.getenv("ENV", "development") == "production"

# ═══════════════════════════════════════════════════════════════
# Настройка логирования (до создания app!)
# ═══════════════════════════════════════════════════════════════

setup_logging(
    level=os.getenv("LOG_LEVEL", "INFO"),
    json_logs=IS_PRODUCTION or os.getenv("JSON_LOGS", "false").lower() == "true",
    log_to_file=not IS_PRODUCTION,  # На Render файловая система ephemeral
)
logger = get_logger()

# ═══════════════════════════════════════════════════════════════
# Создание приложения
# ═══════════════════════════════════════════════════════════════

# Swagger/ReDoc/OpenAPI выключены по умолчанию (fail-safe): на проде они раскрывали
# всю карту API анонимно (включая admin/billing-эндпоинты) — лишняя разведка.
# Включаются ТОЛЬКО явным ENABLE_API_DOCS=1 (локальная разработка). Прод флаг не
# ставит → /api/docs, /api/redoc, /api/openapi.json отдают 404.
_DOCS_ENABLED = os.getenv("ENABLE_API_DOCS", "").strip().lower() in ("1", "true", "yes", "on")

app = FastAPI(
    title="Фрейм API",
    description="API аналитики Московской биржи: инструменты, свечи, открытые позиции",
    version="1.0.0",
    docs_url="/api/docs" if _DOCS_ENABLED else None,
    redoc_url="/api/redoc" if _DOCS_ENABLED else None,
    openapi_url="/api/openapi.json" if _DOCS_ENABLED else None,
)

# ═══════════════════════════════════════════════════════════════
# Обработчики ошибок (без утечки stack trace)
# ═══════════════════════════════════════════════════════════════

setup_exception_handlers(app)

# ═══════════════════════════════════════════════════════════════
# MIDDLEWARE (порядок важен — выполняются снизу вверх!)
# ═══════════════════════════════════════════════════════════════

# 6. GZip сжатие
app.add_middleware(GZipMiddleware, minimum_size=500)

# 5. CORS
# ВАЖНО: В production заменить "*" на конкретные домены!
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

if ALLOWED_ORIGINS == ["*"]:
    # Development mode — credentials=False с wildcard (CORS spec требует)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    # Production mode
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["Authorization", "Content-Type"],
        expose_headers=["X-Request-ID", "X-RateLimit-Remaining"],
        max_age=600,
    )

# 4. Security Headers
app.add_middleware(SecurityHeadersMiddleware)

# 3. Rate Limiting
# heavy=60: /api/{heatmap,chart,stats} теперь Redis-кэш + анти-stampede
# (get_or_compute) — запрос почти всегда дешёвый cache-hit, прежний «дорогой»
# лимит 30 устарел. Гости (без токена) ключуются по IP, а на мобилке ВСЕ гости
# приземляются на /heatmap → за общим VPN/NAT-IP пачка гостей могла коллективно
# выбить 30/мин. 60/мин даёт запас, не теряя защиту (nginx limit_req — backstop).
app.add_middleware(
    RateLimitMiddleware,
    requests_per_minute=100,
    auth_requests_per_minute=10,
    heavy_requests_per_minute=60,
)

# 2. Логирование медленных запросов (> 1 сек)
app.add_middleware(SlowRequestMiddleware, threshold_ms=1000)

# 1. Логирование всех запросов
app.add_middleware(
    RequestLoggingMiddleware,
    exclude_paths=["/health", "/assets", "/favicon.ico", "/vite.svg"]
)

# ═══════════════════════════════════════════════════════════════
# События жизненного цикла
# ═══════════════════════════════════════════════════════════════

_notify_task = None

@app.on_event("startup")
async def startup_event():
    global _notify_task
    # Гарантируем существование таблицы календаря контрактов ДО приёма трафика:
    # front_windows на свежем контейнере иначе отравит транзакцию Session
    # (PostgreSQL 25P02 «relation does not exist») → 500 на каждом futures
    # /api/chart, включая warmup ниже. Идемпотентно (CREATE TABLE IF NOT EXISTS).
    try:
        from api.database import engine as _engine
        from sqlalchemy import text as _sql
        with _engine.begin() as _conn:
            _conn.execute(_sql("""
                CREATE TABLE IF NOT EXISTS futures_contracts (
                    secid VARCHAR(32) PRIMARY KEY, sectype VARCHAR(20) NOT NULL,
                    sec_id VARCHAR(20) NOT NULL, assetcode VARCHAR(20),
                    frsttrade DATE, lsttrade DATE, lstdeldate DATE, expiration_time TIME,
                    is_perpetual BOOLEAN NOT NULL DEFAULT FALSE,
                    is_traded BOOLEAN NOT NULL DEFAULT TRUE,
                    last_seen TIMESTAMP, updated_at TIMESTAMP DEFAULT now()
                )
            """))
            _conn.execute(_sql(
                "CREATE INDEX IF NOT EXISTS idx_futures_contracts_roll "
                "ON futures_contracts (sectype, lsttrade)"))
            # securities_ref — каноничные имена бумаг (fund_trades LEFT JOIN'ит её).
            # Создаём пустой каркас, чтобы запросы не падали, если справочник ещё
            # не наполнен (Funds/build_securities_ref.py).
            _conn.execute(_sql("""
                CREATE TABLE IF NOT EXISTS securities_ref (
                    isin VARCHAR(20) PRIMARY KEY, secid VARCHAR(40),
                    short_name VARCHAR(120), sec_type VARCHAR(40), is_traded BOOLEAN,
                    canonical_isin VARCHAR(20), updated_at TIMESTAMP DEFAULT now()
                )
            """))
        logger.info("futures_contracts + securities_ref tables ensured")
    except Exception as e:
        logger.warning(f"ensure ref tables failed: {e}")
    # Запускаем NOTIFY listener для SSE
    from api.notify_listener import start_notify_listener
    _notify_task = asyncio.create_task(start_notify_listener())
    # Прогреваем кэш в фоне
    asyncio.create_task(_warmup_cache())
    # Периодический прогрев горячих кэшей с коротким TTL (скринер ОИ), чтобы юзеры
    # не ловили ленивый пересчёт (get_or_compute считает на потоке запроса).
    asyncio.create_task(_periodic_warm())
    logger.info("🚀 MOEX Analytics API запущен (SSE enabled)", extra={
        "extra_data": {"type": "startup", "version": "1.0.0"}
    })


async def _warmup_cache():
    """Прогрев кэша при старте — дёргает тяжёлые эндпоинты чтобы пользователи не ждали."""
    import httpx
    await asyncio.sleep(2)  # дать uvicorn полностью подняться

    # Карта «Все акции» (/api/heatmap/stocks) — tier-gated (Basic+), поэтому
    # прогреть её через HTTP guest-клиентом нельзя (403 до записи в кеш).
    # Наполняем кеш напрямую через build_stocks_heatmap (минует tier-проверку).
    # Ключи должны совпадать с фронтом: getHeatmapData('market_cap','change_1d',groupBy).
    try:
        from api.routers.heatmap import build_stocks_heatmap
        for gb in ("sector", "none"):
            await asyncio.to_thread(build_stocks_heatmap, "market_cap", "change_1d", gb)
    except Exception as e:
        logger.warning(f"Heatmap stocks warmup failed: {e}")

    urls = [
        "/api/heatmap/imoex?color_by=change_1d&group_by=sector",
        "/api/funds/chart?category=money_market&period=6m",
        "/api/funds/catalog",
        "/api/breadth/current?ema_period=200&universe=imoex",
        "/api/breadth/history?ema_period=200&days=365&universe=imoex",
        "/api/chart/SR?sectype=SR&inst_type=futures&interval=24&clgroup=FIZ&show_oi=true&period=6m",
        "/api/buffett/cap-gdp?period=10y&smooth=false",
    ]

    # Скринер ОИ — tier-gated (Basic+) с 2026-08-09, поэтому греть его через
    # HTTP guest-клиентом нельзя: роут вернул бы locked-маркер, не тронув кеш.
    # Наполняем напрямую через build_oi_screener (минует tier-проверку) — тот же
    # приём, что с картой «Все акции» выше.
    await asyncio.to_thread(_warm_screener_cache)
    try:
        async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=30) as client:
            for url in urls:
                try:
                    await client.get(url)
                except Exception:
                    pass
        logger.info(f"Cache warmup done ({len(urls)} endpoints)")
    except Exception as e:
        logger.warning(f"Cache warmup failed: {e}")


def _warm_screener_cache():
    """Прогрев кеша скринера ОИ по всем 4 комбинациям clgroup × horizon.

    Синхронная (SQLAlchemy-сессия) — вызывается через asyncio.to_thread.
    Своя сессия, а не Depends(get_db): прогрев живёт вне цикла запросов.
    """
    from api.database import SessionLocal
    from api.routers.open_interest import build_oi_screener
    db = SessionLocal()
    try:
        for clgroup in ("FIZ", "YUR"):
            for horizon in ("short", "medium"):
                try:
                    build_oi_screener(db, clgroup, horizon)
                except Exception as e:
                    logger.warning(f"OI screener warm failed ({clgroup}/{horizon}): {e}")
    finally:
        db.close()


# (interval, period) — ровно то, что фронт запрашивает по умолчанию для ТФ:
# при переключении интервала ставится максимальный доступный период (см.
# MAX_PERIODS_BY_INTERVAL на странице ОИ).
_WARM_CHART_COMBOS = ((5, "6m"), (60, "1y"), (24, "1y"))

# Сколько секунд за один цикл тратим на прогрев графика. Все видимые активы
# (91 шт на 2026-08-10) стоят ≈12 минут сплошного CPU — залпом их греть нельзя,
# это ровно та нагрузка, от которой лечимся. Идём по кругу: за цикл успеваем
# сколько успеваем, следующий продолжает с того же места. Активы отсортированы
# по спросу, поэтому востребованные попадают в первые же циклы.
_WARM_CHART_BUDGET_SEC = 60


def _visible_oi_assets(db) -> list[str]:
    """Активы ОИ, видимые пользователю: все минус скрытые по низкой активности.

    Тот же принцип, что у пикера и скринера (services/oi_screener.low_activity_set),
    поэтому прогрев покрывает ровно то, что человек может открыть.
    """
    from sqlalchemy import text
    from api.services.oi_screener import low_activity_set
    every = {r[0] for r in db.execute(text("SELECT DISTINCT sectype FROM open_interest")).fetchall()}
    try:
        hidden = set(low_activity_set(db))
    except Exception as e:
        logger.warning(f"low_activity_set failed, warming all assets: {e}")
        hidden = set()
    return sorted(every - hidden)


def _warm_chart_cache():
    """Прогрев графика ОИ: все видимые активы, по кругу, в рамках бюджета.

    Греем ДВА варианта ключа на каждую пару: date_to=None (тариф без задержки) и
    date_to=вчера (Free/гость — роутер подменяет потолок в get_effective_end_date).
    Ключи разные, и без второго гость платил бы за холодный пересчёт сам.

    Считаем напрямую через _compute_chart_data + set_gzip, минуя HTTP: у прогрева
    нет токена, а через себя по HTTP получился бы только гостевой вариант. Тот же
    приём, что у карты «Все акции» и скринера выше.

    Порядок — по спросу (cache.record_demand на каждом запросе графика), хвост —
    по алфавиту. Курсор круга живёт в Redis, поэтому перезапуск контейнера не
    начинает обход заново с самых дорогих активов.

    Уже тёплые ключи пропускаются по одной команде Redis — основная масса циклов
    почти бесплатна. Синхронная: вызывается через asyncio.to_thread.
    """
    import json
    import time as _time
    from datetime import date, timedelta
    from api.cache import (get_gzip, set_gzip, get_or_set, top_demand,
                           start_chart_refresh, take_chart_refresh_slot, DEFAULT_TTL)
    from api.database import SessionLocal
    from api.routers.chart import _compute_chart_data

    yesterday = date.today() - timedelta(days=1)
    deadline = _time.monotonic() + _WARM_CHART_BUDGET_SEC
    db = SessionLocal()
    warmed = 0
    try:
        assets = _visible_oi_assets(db)
        if not assets:
            return
        # Спрос вперёд, остальное — следом, в стабильном порядке.
        hot = [a for a in top_demand() if a in assets]
        order = hot + [a for a in assets if a not in set(hot)]

        # Пришёл daily → открываем ровно len(order) слотов принудительного
        # пересчёта: круг пройдётся один раз и режим выключится сам.
        if start_chart_refresh(len(order)):
            logger.info(f"Chart refresh after daily: {len(order)} активов в очереди")

        # Продолжаем с места прошлого цикла, а не с начала списка.
        start = int(get_or_set("chart:warm:cursor") or 0) % len(order)
        idx = start

        for step in range(len(order)):
            idx = (start + step) % len(order)
            if _time.monotonic() > deadline:
                break
            sectype = order[idx]
            # Режим принудительного пересчёта после daily-обновления: тёплые ключи
            # не пропускаем, а перезаписываем свежим расчётом (ролловер контракта,
            # available_intervals, склейка — их инкрементальный апдейт не трогает).
            # Слот берём один на актив, счётчик сам выключит режим, обойдя круг.
            force = take_chart_refresh_slot()
            for interval, period in _WARM_CHART_COMBOS:
                for date_to in (None, yesterday):
                    key = (f"chart:{sectype}:{sectype}:futures:{interval}:FIZ:True:"
                           f"{period}:None:{date_to}")
                    if not force and get_gzip(key) is not None:
                        continue
                    try:
                        data = _compute_chart_data(
                            db, sectype, sectype, "futures", interval,
                            "FIZ", True, period, None, date_to,
                        )
                        set_gzip(key, json.dumps(data, default=str), ttl=DEFAULT_TTL)
                        warmed += 1
                    except Exception as e:
                        logger.warning(f"Chart warm failed ({sectype}/{interval}/{period}): {e}")

        get_or_set("chart:warm:cursor", (idx + 1) % len(order), ttl=86400)
    finally:
        db.close()
    if warmed:
        logger.info(f"Chart cache warm: {warmed} keys computed (cursor {idx + 1}/{len(assets)})")


async def _periodic_warm():
    """Держит горячими кэши с коротким TTL, чтобы пользователи не платили за
    ленивый пересчёт. Скринер ОИ: TTL 300с → перегреваем каждые 240с (оба clgroup);
    тяжёлый скан истории (oi_extremes, TTL 30мин) при этом уезжает в фон, а не на
    поток запроса. Компонент идемпотентен и per-container (лишний прогрев безвреден).

    ⚠️ Греем ВНУТРЕННИМ вызовом (_warm_screener_cache), а не HTTP-запросом к
    себе: с 2026-08-09 скринер закрыт для гостя, а у прогрева токена нет —
    роут отдал бы locked-маркер и кеш остался бы холодным. Все 4 комбинации
    clgroup × horizon: кэш-ключи раздельные, и без явного horizon тёплым
    держался только дневной (замер: 0.9–1.1с против 0.2с)."""
    await asyncio.sleep(45)  # после стартового прогрева
    while True:
        try:
            await asyncio.to_thread(_warm_screener_cache)
        except Exception as e:
            logger.warning(f"Periodic warm failed: {e}")

        # График ОИ — под локом: этот хук крутится в КАЖДОМ из трёх воркеров, а
        # холодный пересчёт 5м/6м стоит секунды CPU. Втроём они выели бы 4 ядра
        # ровно на том, что и так лечим. Лок короче интервала цикла, поэтому
        # следующий круг снова возьмёт кто-то один.
        try:
            from api.cache import try_lock
            if try_lock("warm:chart", ttl=200):
                await asyncio.to_thread(_warm_chart_cache)
        except Exception as e:
            logger.warning(f"Chart warm failed: {e}")

        await asyncio.sleep(240)

@app.on_event("shutdown")
async def shutdown_event():
    global _notify_task
    if _notify_task:
        _notify_task.cancel()
        try:
            await _notify_task
        except asyncio.CancelledError:
            pass
    logger.info("👋 MOEX Analytics API остановлен", extra={
        "extra_data": {"type": "shutdown"}
    })

# ═══════════════════════════════════════════════════════════════
# Роутеры API
# ═══════════════════════════════════════════════════════════════

app.include_router(instruments_router)
app.include_router(candles_router)
app.include_router(open_interest_router)
app.include_router(oi_router)  # ← /api/oi/* (справка: внутридневная доступность)
app.include_router(chart_router)
app.include_router(stats.router)
app.include_router(heatmap_router)
app.include_router(funds_router)
app.include_router(breadth_router)
app.include_router(buffett_router)
app.include_router(seasonality_router)
app.include_router(cbr_flows_router)  # ← /api/cbr-flows/* (ОРФР ЦБ — потоки участников)
app.include_router(csp_report_router)  # ← /api/csp-report (browser violation reports)
app.include_router(health_monitor.router)  # ← /api/health/data (always-on, НЕ под kill-switch)
# KILL-SWITCH: публичный API + CSV-экспорт скрыты до официального запуска.
# По умолчанию (PUBLIC_API_CSV_ENABLED не задан) роутеры НЕ монтируются →
# /api/export/*, /api/keys/*, /api/v1/public/* отдают 404 ДЛЯ ВСЕХ (включая
# прямые вызовы мимо UI). Вернуть: env PUBLIC_API_CSV_ENABLED=1 + recreate api.
from api.billing.features import PUBLIC_API_CSV_ENABLED as _API_CSV_ON
if _API_CSV_ON:
    app.include_router(exports_router)  # ← /api/export/*.csv (Pro-only data download)
    app.include_router(api_keys_router)  # ← /api/keys/* (manage personal API keys)
    app.include_router(public_api_router)  # ← /api/v1/public/* (programmatic JSON access)
app.include_router(fund_trades_router)  # ← /api/fund-trades/* (диффы holdings БПИФов)
app.include_router(oi_intl_router)  # ← /api/admin/oi-intl/* (международный ОИ, admin-only)
app.include_router(billing_router)  # ← /api/billing/* (подписки через ЮKassa)
app.include_router(auth.router, prefix="/api")  # ← НОВОЕ: /api/auth/*
app.include_router(oauth.router, prefix="/api")  # ← OAuth: /api/auth/oauth/*
app.include_router(events.router)  # ← SSE: /api/events/*
app.include_router(analytics.router)  # ← Analytics: /api/analytics/*
app.include_router(extension.router)  # ← /api/extension/* (ext-токены: генерация/обмен)
app.include_router(alerts.router)  # ← /api/alerts/* (Telegram alert-bot: привязка Telegram)
app.include_router(anomalies.router)  # ← /api/anomalies/* (лента всплывающих аномалий)
app.include_router(content_news.router)  # ← /api/admin/content-candidates/* (Kanban «Новости»)
app.include_router(content_news.internal_router)  # ← /api/internal/content-news/* (ИИ-callback, shared secret)
app.include_router(mandate_scan.internal_router)  # ← /api/internal/mandate-scan/* (Routine-скаут, shared secret)

# ═══════════════════════════════════════════════════════════════
# Служебные эндпоинты
# ═══════════════════════════════════════════════════════════════

def _health_payload():
    result = {"status": "ok"}

    # Проверяем подключение к БД
    try:
        from api.database import get_engine
        from sqlalchemy import text
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        result["database"] = "ok"
    except Exception:
        result["database"] = "error"
        result["status"] = "degraded"

    return result


@app.get("/health")
def health():
    """Проверка работоспособности (для load balancer)"""
    return _health_payload()


@app.get("/api/health")
def api_health():
    """Health endpoint для фронтенда (через /api/*)"""
    return _health_payload()


@app.get("/api/info")
def api_info():
    """Информация об API"""
    return {
        "name": "MOEX Analytics API",
        "version": "1.0.0",
        "status": "running",
    }

# ═══════════════════════════════════════════════════════════════
# Раздача фронтенда (SPA)
# ═══════════════════════════════════════════════════════════════

FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"

# Валидные клиентские маршруты, у которых НЕТ пререндера (нет записи в SEO_META,
# поэтому scripts/prerender-meta.ts не создаёт для них dist/<route>/index.html):
# динамические сегменты и чистые редиректы из frontend/src/App.tsx. Всё прочее,
# что не статика и не пререндерено, — несуществующий URL → честный 404.
# Иначе SPA-fallback отдавал 200 на ЛЮБУЮ опечатку (/nonexistent-page → 200),
# и Yandex/Google ругаются «404 настроен некорректно» и хуже индексируют сайт.
# ВАЖНО: добавил НЕ-пререндеренный route в App.tsx — добавь префикс/путь сюда.
SPA_VALID_PREFIXES = ("embed/", "auth/callback/", "admin/")
SPA_VALID_EXACT = frozenset({"verify-email", "funds", "security"})

if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.api_route("/{path:path}", methods=["GET", "HEAD"])
    async def serve_spa(path: str):
        # Неизвестные /api/* — это НЕ маршруты SPA: отдаём честный 404, а не
        # HTML-оболочку. Иначе любой опечатанный/удалённый эндпоинт (как
        # выключенные /api/docs, /api/openapi.json) возвращал 200 с SPA-HTML,
        # что путает API-клиентов и маскирует 404.
        if path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not Found")
        file_path = FRONTEND_DIR / path
        if file_path.exists() and file_path.is_file():
            # sw.js и manifest — никогда не кешировать
            if path in ("sw.js", "manifest.json"):
                return FileResponse(
                    file_path,
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
                )
            return FileResponse(file_path)
        # Pre-rendered HTML per route — кладётся scripts/prerender-meta.ts в postbuild.
        # dist/<route>/index.html содержит правильные <title>/<meta description>/
        # canonical/og/twitter/JSON-LD для конкретного URL. Без этого Yandex/Google
        # видят одинаковый title на всех страницах SPA (duplicate content penalty).
        if path:
            prerendered = FRONTEND_DIR / path / "index.html"
            if prerendered.exists() and prerendered.is_file():
                return FileResponse(
                    prerendered,
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
                )
        # SPA fallback — dist/index.html. Тело то же самое (React-роутер сам
        # отрисует нужный экран), но СТАТУС зависит от валидности пути:
        #   - "" (главная), известный не-пререндеренный route → 200
        #   - всё остальное → 404 (для краулеров; пользователь видит ту же оболочку)
        is_known_route = (
            not path
            or path in SPA_VALID_EXACT
            or path.startswith(SPA_VALID_PREFIXES)
        )
        return FileResponse(
            FRONTEND_DIR / "index.html",
            status_code=200 if is_known_route else 404,
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )