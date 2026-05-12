# ═══════════════════════════════════════════════════════════════
# Stage 1: Build frontend
# ═══════════════════════════════════════════════════════════════
FROM node:20-alpine AS frontend-build

# python3 + Pillow нужны для frontend/package.json::prebuild хука
# (scripts/build-sprite.py — собирает sprite.png из logos/*.png).
# Без них `npm run build` падает с "python3: not found".
RUN apk add --no-cache python3 py3-pip py3-pillow

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

# Python зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код приложения
COPY api/ ./api/
COPY OI/ ./OI/
COPY Candles/ ./Candles/
COPY Funds/ ./Funds/
COPY Macro/ ./Macro/
COPY main_orchestrator.py .
COPY moex_calendar.py .
COPY tg_bot.py .
COPY backup_db.sh .

# Frontend из stage 1
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Порт
EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Запуск
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
