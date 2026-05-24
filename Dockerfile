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

# Код приложения
COPY api/ ./api/
COPY OI/ ./OI/
COPY Candles/ ./Candles/
COPY Funds/ ./Funds/
COPY Macro/ ./Macro/
COPY Commodity/ ./Commodity/
COPY CBR/ ./CBR/
COPY main_orchestrator.py .
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

# Запуск
# --workers 3: 4-ядерная VM, 3 worker'a используют 75% CPU (4-й оставляем
# OS/Postgres/Redis). Каждый worker ~200MB RSS → ~600MB total, безопасно на
# 4GB RAM. Подтверждено аудитом 2026-05-24.
# Non-root запуск выставляется через docker-compose `user: 1000:1000` (только
# для api-сервиса, orchestrator/tg-bot остаются root для backward-compat).
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "3"]
