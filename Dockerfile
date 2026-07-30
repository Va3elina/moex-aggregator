# ═══════════════════════════════════════════════════════════════
# Stage 1: Build frontend
# ═══════════════════════════════════════════════════════════════
FROM node:20-alpine AS frontend-build

# python3 + Pillow нужны для frontend/package.json::prebuild хука
# (scripts/build-sprite.py — собирает sprite.png из logos/*.png).
# bash нужен для postbuild хука (find dist/logos ... cleanup).
# Без них `npm run build` падает с "python3: not found" / "bash: not found".
RUN apk add --no-cache python3 py3-pip py3-pillow bash

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci
COPY frontend/ ./
# scripts/build-sprite.py использует Path(__file__).parent.parent / 'frontend'/...
# То есть ожидает структуру /app/{frontend/, scripts/}. WORKDIR=/app/frontend,
# COPY scripts/ → /app/scripts/.
COPY scripts/ /app/scripts/
RUN npm run build

# ═══════════════════════════════════════════════════════════════
# Stage 2: Python app
# ═══════════════════════════════════════════════════════════════
FROM python:3.11-slim

# Системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Non-root user для API-сервиса. uid=1000 — стандарт, совместим с host
# bind-mount'ами. /sbin/nologin блокирует interactive shell. orchestrator
# и tg-bot переопределяют user через docker-compose (или работают как root —
# они batch-процессы без HTTP exposure, низкая attack surface).
RUN useradd --system --uid 1000 --no-create-home --shell /usr/sbin/nologin appuser

# Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# gunicorn — production process manager (graceful restart, worker timeouts,
# max-requests recycling). Установлен отдельно от requirements.txt, потому
# что requirements.txt uncommitted (signal-engine territory, Вадимов patch).
RUN pip install --no-cache-dir gunicorn==21.2.0

# ─── TLS-бандлы с цепочкой Минцифры ──────────────────────────────────────
# T-Bank, VK ID и rosstat.gov.ru сидят на Russian Trusted Root CA, которого
# нет ни в certifi, ни в системном store. Собираем ОТДЕЛЬНЫЕ бандлы — они
# подключаются явным verify= ровно там, где нужны (api/ru_tls.py). В
# /usr/local/share/ca-certificates НЕ кладём и update-ca-certificates НЕ
# зовём сознательно: этот CA может выпустить сертификат на любой домен, и
# глобальное доверие распространило бы его на весь исходящий трафик (MOEX,
# Telegram, GitHub).
#
# Бандлов два, и они РАЗНЫЕ по составу:
#   ru-trusted-bundle.pem  = certifi + корень.        Биллинг + OAuth.
#       Промежуточный сертификат эти вендоры присылают сами в хендшейке,
#       поэтому пин интермедиата был бы лишней точкой отказа при ротации.
#   rosstat-bundle.pem     = certifi + корень + Sub CA.  Фетчер ВВП.
#       Росстат присылает ТОЛЬКО лист (`Verify return code: 21`), достроить
#       цепочку нечем → интермедиат обязан лежать у нас.
# Держим их врозь, чтобы пин интермедиата не протёк в платёжный путь.
#
# Шаг обязан идти ПОСЛЕ pip install — нужен установленный certifi.
# Финальный python — build-time проверка: если PEM битый, пустой или в бандле
# не оказалось нужного звена, сборка падает здесь, а не в проде на первом
# платеже (и не тихо, через квартал, на обновлении ВВП).
COPY certs/russian_trusted_root_ca.pem certs/russian_trusted_sub_ca.pem /etc/ssl/frame/
RUN CERTIFI="$(python3 -c 'import certifi; print(certifi.where())')" && \
    cat "$CERTIFI" /etc/ssl/frame/russian_trusted_root_ca.pem \
        > /etc/ssl/frame/ru-trusted-bundle.pem && \
    cat "$CERTIFI" /etc/ssl/frame/russian_trusted_root_ca.pem \
        /etc/ssl/frame/russian_trusted_sub_ca.pem \
        > /etc/ssl/frame/rosstat-bundle.pem && \
    chmod 0644 /etc/ssl/frame/ru-trusted-bundle.pem /etc/ssl/frame/rosstat-bundle.pem && \
    python3 -c "import ssl, sys; \
c = ssl.create_default_context(cafile='/etc/ssl/frame/ru-trusted-bundle.pem').get_ca_certs(); \
sys.exit(0) if len(c) > 100 else sys.exit(f'ru-trusted-bundle.pem: только {len(c)} сертификатов')" && \
    python3 -c "import ssl, sys; \
c = ssl.create_default_context(cafile='/etc/ssl/frame/rosstat-bundle.pem').get_ca_certs(); \
cn = {v for x in c for rdn in x.get('subject', ()) for k, v in rdn if k == 'commonName'}; \
gone = [n for n in ('Russian Trusted Root CA', 'Russian Trusted Sub CA') if n not in cn]; \
sys.exit(f'rosstat-bundle.pem: в бандле нет {gone}') if gone else None; \
sys.exit(0) if len(c) > 100 else sys.exit(f'rosstat-bundle.pem: только {len(c)} сертификатов')"

# Код приложения
COPY api/ ./api/
COPY OI/ ./OI/
COPY Candles/ ./Candles/
COPY Funds/ ./Funds/
COPY Macro/ ./Macro/
COPY Commodity/ ./Commodity/
COPY CBR/ ./CBR/
COPY Crypto/ ./Crypto/
COPY main_orchestrator.py .
COPY pipeline_heartbeat.py .
COPY moex_calendar.py .
COPY tg_bot.py .
COPY backup_db.sh .

# Frontend из stage 1
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# КРИТИЧНО: chown ПОСЛЕ всех COPY (которые делают файлы owner=root). Без
# этого uid=1000 не сможет читать `/app/api/*.py`, импорт упадёт. /app/logs
# создаётся pre-emptively (api/logger.py делает mkdir при импорте) — если
# не создать тут, runtime упадёт на PermissionError. Первая попытка
# (commit 231411a) сломалась именно из-за этого.
RUN mkdir -p /app/logs && chown -R appuser:appuser /app

# Порт
EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Запуск — gunicorn + uvicorn worker class.
# gunicorn vs uvicorn-only:
#   - graceful-timeout 30s: при SIGTERM ждём 30с in-flight requests
#     (zero-downtime deploy через SIGHUP reload)
#   - timeout 60s: kill worker если завис на одном запросе (защита от hang)
#   - worker class = api.uvicorn_worker.GracefulSSEWorker (обёртка UvicornWorker
#     с timeout_graceful_shutdown=20): вечные SSE-стримы (/api/events/stream)
#     принудительно закрываются при recycle/деплое, иначе воркер зависал до
#     --timeout и умирал по SIGABRT, роняя всех своих SSE-клиентов
#   - max-requests 1000 + jitter 50: каждый worker recycle после ~1k
#     запросов — защита от Python memory leaks в long-running процессах
#   - forwarded-allow-ips=* : доверять X-Forwarded-* от nginx (через docker
#     network IP, не 127.0.0.1). Без этого RateLimitMiddleware видит
#     всех юзеров под одним IP 172.18.x.x и сразу лимитит.
#   - workers 3: 4-ядерная VM (8ГБ, апгрейд 2026-07-12 с 2 ядер). 3 uvicorn-воркера
#     (async, IO-bound) свободно ложатся на 4 ядра; число к ядрам жёстко не привязано —
#     воркеры простаивают на await (сеть/БД), а не жгут CPU. При росте нагрузки можно 4-5.
# Non-root запуск через docker-compose `user: 1000:1000` (api сервис).
CMD ["gunicorn", "api.main:app", \
     "-k", "api.uvicorn_worker.GracefulSSEWorker", \
     "-w", "3", \
     "-b", "0.0.0.0:8000", \
     "--graceful-timeout", "30", \
     "--timeout", "60", \
     "--max-requests", "1000", \
     "--max-requests-jitter", "50", \
     "--forwarded-allow-ips=*", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
