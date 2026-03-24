#!/bin/bash
set -eu

BACKUP_DIR="/tmp/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/moex_db_$DATE.sql.gz"
BOT_TOKEN="${BOT_TOKEN:?BOT_TOKEN env var is required}"
CHAT_ID="${ADMIN_CHAT_ID:?ADMIN_CHAT_ID env var is required}"

mkdir -p "$BACKUP_DIR"

send_file() {
  local file="$1"
  local caption="$2"
  curl -s -F "chat_id=$CHAT_ID" \
    -F "document=@$file" \
    -F "caption=$caption" \
    "https://api.telegram.org/bot$BOT_TOKEN/sendDocument" > /dev/null
}

send_msg() {
  curl -s -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
    -d "chat_id=$CHAT_ID" -d "text=$1" > /dev/null
}

# 1. Dump DB (подключаемся к db по сети — работает и из контейнера, и с хоста)
echo "-> Dumping DB..."
# Извлекаем пароль из DB_URL (формат: postgresql://user:pass@host:port/db)
DB_PASS=$(echo "${DB_URL:-}" | sed -n 's|.*://[^:]*:\([^@]*\)@.*|\1|p')
PGPASSWORD="${DB_PASS:-${POSTGRES_PASSWORD:-postgres}}" pg_dump -h db -U postgres moex_db | gzip > "$BACKUP_FILE" 2>&1 || {
  send_msg "❌ Ошибка бэкапа: pg_dump failed"
  exit 1
}
DB_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)

# Telegram лимит 50MB для файлов
FILE_BYTES=$(stat -c%s "$BACKUP_FILE" 2>/dev/null || stat -f%z "$BACKUP_FILE" 2>/dev/null)
if [ "$FILE_BYTES" -gt 50000000 ]; then
  send_msg "⚠️ Бэкап слишком большой для Telegram ($DB_SIZE). Сохранён на сервере: $BACKUP_FILE"
else
  send_file "$BACKUP_FILE" "💾 DB backup moex_db
Size: $DB_SIZE
Date: $(date '+%d.%m.%Y %H:%M')"
fi

# .env НЕ отправляем — содержит секреты, нельзя передавать через Telegram

# Чистим старые бэкапы (оставляем 2 последних)
ls -t "$BACKUP_DIR"/*.sql.gz 2>/dev/null | tail -n +3 | xargs -r rm --

echo "Done: DB=$DB_SIZE"
