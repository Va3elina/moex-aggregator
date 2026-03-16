#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# deploy.sh — Полная настройка сервера для таймфрейм.рф
# Запуск: bash deploy.sh
# ═══════════════════════════════════════════════════════════════

set -e

DOMAIN="xn--80aagfbdcefbpg5j.xn--p1ai"   # таймфрейм.рф в punycode
EMAIL="your-email@example.com"             # ← СМЕНИТЬ на свой email

echo "══════════════════════════════════════════════"
echo "  Деплой таймфрейм.рф"
echo "══════════════════════════════════════════════"

# ─── 1. Обновление системы ─────────────────────────────────────
echo "→ Обновление системы..."
apt-get update && apt-get upgrade -y

# ─── 2. Установка Docker ──────────────────────────────────────
if ! command -v docker &> /dev/null; then
    echo "→ Установка Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
else
    echo "→ Docker уже установлен"
fi

# ─── 3. Установка Docker Compose ──────────────────────────────
if ! command -v docker compose &> /dev/null; then
    echo "→ Установка Docker Compose plugin..."
    apt-get install -y docker-compose-plugin
else
    echo "→ Docker Compose уже установлен"
fi

# ─── 4. Клонирование репозитория ───────────────────────────────
APP_DIR="/opt/frame"
if [ ! -d "$APP_DIR" ]; then
    echo "→ Клонирование репозитория..."
    git clone https://github.com/Va3elina/moex-aggregator.git "$APP_DIR"
else
    echo "→ Обновление репозитория..."
    cd "$APP_DIR" && git pull
fi
cd "$APP_DIR"

# ─── 5. Настройка .env ────────────────────────────────────────
if [ ! -f .env ]; then
    echo "→ Создание .env из шаблона..."
    cp .env.production .env
    # Генерируем безопасный JWT secret
    JWT_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))" 2>/dev/null || openssl rand -hex 32)
    sed -i "s/СМЕНИТЬ_СГЕНЕРИРОВАТЬ_НОВЫЙ/$JWT_SECRET/" .env
    echo ""
    echo "⚠️  ВАЖНО: Отредактируй .env перед запуском!"
    echo "   nano /opt/frame/.env"
    echo "   - DB_PASSWORD — надёжный пароль для PostgreSQL"
    echo "   - ALGOPACK_API_KEY — ключ MOEX API"
    echo "   - GOOGLE_CLIENT_ID/SECRET — для OAuth"
    echo ""
    read -p "Нажми Enter когда .env будет готов..."
fi

# ─── 6. Получение SSL сертификата ──────────────────────────────
echo "→ Получение SSL сертификата..."

# Сначала запускаем nginx без SSL для прохождения challenge
mkdir -p nginx/conf.d
cat > nginx/conf.d/default.conf.tmp << 'TMPEOF'
server {
    listen 80;
    server_name _;
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }
    location / {
        return 200 'OK';
    }
}
TMPEOF

# Временный nginx для certbot
cp nginx/conf.d/default.conf nginx/conf.d/default.conf.bak
cp nginx/conf.d/default.conf.tmp nginx/conf.d/default.conf

docker compose up -d nginx

# Получаем сертификат
docker compose run --rm certbot certonly \
    --webroot \
    --webroot-path=/var/www/certbot \
    -d "$DOMAIN" \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email

# Возвращаем полный конфиг nginx
cp nginx/conf.d/default.conf.bak nginx/conf.d/default.conf
rm nginx/conf.d/default.conf.tmp nginx/conf.d/default.conf.bak

docker compose down

# ─── 7. Сборка и запуск ───────────────────────────────────────
echo "→ Сборка Docker образов..."
docker compose build

echo "→ Запуск всех сервисов..."
docker compose up -d

# ─── 8. Проверка ──────────────────────────────────────────────
echo ""
echo "→ Ожидание запуска (10 сек)..."
sleep 10

if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo "✅ API работает"
else
    echo "⚠️  API ещё запускается, проверь: docker compose logs api"
fi

# ─── 9. Cron для обновления SSL ───────────────────────────────
CRON_JOB="0 3 * * 0 cd /opt/frame && docker compose run --rm certbot renew && docker compose restart nginx"
(crontab -l 2>/dev/null | grep -v certbot; echo "$CRON_JOB") | crontab -

# ─── 10. Firewall ─────────────────────────────────────────────
echo "→ Настройка файрвола..."
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
ufw --force enable

echo ""
echo "══════════════════════════════════════════════"
echo "  ✅ Деплой завершён!"
echo "══════════════════════════════════════════════"
echo ""
echo "  🌐 https://таймфрейм.рф"
echo ""
echo "  Полезные команды:"
echo "  docker compose logs -f api        — логи API"
echo "  docker compose logs -f orchestrator — логи оркестратора"
echo "  docker compose restart api         — перезапуск API"
echo "  docker compose down && docker compose up -d — полный рестарт"
echo ""
