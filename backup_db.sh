#!/bin/bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Бэкап PostgreSQL для Фрейм (таймфрейм.рф).
# Запускается cron'ом каждый день в 03:00 МСК.
#
# Архитектура:
#   1) pg_dump через docker exec → gzip → GPG AES-256 → .sql.gz.gpg
#   2) ОДНИМ файлом в «Избранное» Вадима через MTProto (аккаунт с Premium,
#      лимит 4 ГБ) — backup_tg_upload.py
#   3) Фолбэк: split по 48 МБ + sendDocument ботом (Bot API limit 50 МБ)
#   4) Если все части ушли → удалить старые дампы (retention=1)
#   5) Если упало на отправке → оставить старый дамп как safety net
#
# Шифрование: симметричный AES-256 с passphrase из BACKUP_GPG_PASSPHRASE
# в .env. Без passphrase backup НЕ ВОССТАНОВИТЬ — храни passphrase в
# password manager + физическая копия (на случай потери .env).
#
# Restore (если когда-то понадобится):
#   1) Скачать .sql.gz.gpg из «Избранного» (или все .part_*, если сработал фолбэк,
#      и склеить: cat moex_db_YYYYMMDD_HHMM.sql.gz.gpg.part_* > moex_db.sql.gz.gpg).
#   2) —
#   3) gpg --batch --pinentry-mode loopback --passphrase 'PASSPHRASE' \
#          --decrypt moex_db.sql.gz.gpg > moex_db.sql.gz
#   4) gunzip moex_db.sql.gz
#   5) psql -U postgres -d moex_db < moex_db.sql
# ─────────────────────────────────────────────────────────────

BACKUP_DIR="/opt/frame/backups/dumps"
CONTAINER="frame-db-1"
DB_NAME="moex_db"
DB_USER="postgres"
SPLIT_SIZE="48M"  # под Telegram bot API лимит 50 МБ

# Загрузить env (BOT_TOKEN, ADMIN_CHAT_ID, BACKUP_GPG_PASSPHRASE)
if [ -f /opt/frame/.env ]; then
  set -a
  # shellcheck disable=SC1091
  source /opt/frame/.env
  set +a
fi

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# curl -6: форсируем IPv6 на Telegram. Из московского ДЦ IPv4-маршрут до
# api.telegram.org мёртв (таймаут), а IPv6 работает. Без -6 curl по Happy Eyeballs
# иногда выбирал IPv4 → часть частей бэкапа доставлялась, часть падала с
# {"ok":false} (фолбэк при curl-фейле) и висла по 2 мин/часть. --max-time 90
# страхует если IPv6 тоже заблокируют (Москва): часть быстро падает, локальный
# дамп остаётся (offsite — best-effort). Если IPv6 отрубят — менять источник.
send_msg() {
  if [ -n "${BOT_TOKEN:-}" ] && [ -n "${ADMIN_CHAT_ID:-}" ]; then
    local chat
    # ADMIN_CHAT_ID — один id или список через запятую.
    for chat in ${ADMIN_CHAT_ID//,/ }; do
      curl -6 -s --max-time 30 -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        -d "chat_id=${chat}" \
        -d "text=$1" \
        -d "parse_mode=Markdown" > /dev/null || true
    done
  fi
}

# Отправка одного документа в Telegram С РЕТРАЯМИ. Moscow→Telegram IPv6
# периодически флапает, плюс Telegram может 429-ить при заливке десятка файлов
# подряд. Раньше один блип (curl-фейл → {"ok":false}) ронял ВЕСЬ бэкап (break в
# цикле ниже) → 0/N доставлено, дамп «обрывался». Теперь до 4 попыток с backoff
# 0/5/15/30с. -w пишет http-код для диагностики (429=rate-limit, пусто=сеть).
# Возврат: 0 = успех, 1 = все попытки исчерпаны.
send_doc() {
  local chat ok=1
  for chat in ${ADMIN_CHAT_ID//,/ }; do
    send_doc_to "$chat" "$1" "$2" && ok=0
  done
  return $ok
}

send_doc_to() {
  local chat="$1"
  local file="$2"
  local caption="$3"
  local delays=(0 5 15 30)
  local attempt resp http
  for attempt in "${!delays[@]}"; do
    if [ "${delays[$attempt]}" -gt 0 ]; then sleep "${delays[$attempt]}"; fi
    resp=$(curl -6 -s --max-time 120 -w '\n%{http_code}' \
      -F "chat_id=${chat}" \
      -F "document=@${file}" \
      -F "caption=${caption}" \
      "https://api.telegram.org/bot${BOT_TOKEN}/sendDocument" 2>/dev/null) || true
    http=$(printf '%s' "$resp" | tail -n1)
    if printf '%s' "$resp" | grep -q '"ok":true'; then
      return 0
    fi
    log "    ⚠ part upload failed (http=${http:-net}), попытка $((attempt + 1))/${#delays[@]}"
  done
  return 1
}

# ─── Проверка credentials ───
if [ -z "${BOT_TOKEN:-}" ] || [ -z "${ADMIN_CHAT_ID:-}" ]; then
  log "ERROR: BOT_TOKEN или ADMIN_CHAT_ID не заданы в /opt/frame/.env"
  exit 1
fi

if [ -z "${BACKUP_GPG_PASSPHRASE:-}" ]; then
  log "ERROR: BACKUP_GPG_PASSPHRASE не задан в /opt/frame/.env — backup не зашифрован, отказ"
  send_msg "❌ *Frame backup failed*: BACKUP_GPG_PASSPHRASE not set"
  exit 1
fi

DATE=$(date +%Y%m%d_%H%M)
BASENAME="moex_db_${DATE}.sql.gz.gpg"
BACKUP_FILE="$BACKUP_DIR/$BASENAME"

mkdir -p "$BACKUP_DIR"

log "Starting encrypted backup of $DB_NAME from $CONTAINER..."

# ─── 1) Проверка БД ───
if ! docker exec "$CONTAINER" pg_isready -U "$DB_USER" -d "$DB_NAME" -q 2>/dev/null; then
  log "ERROR: container $CONTAINER or DB not ready"
  send_msg "❌ *Frame backup failed*: DB not ready"
  exit 1
fi

# ─── 2) Дамп → gzip → GPG → .tmp файл ───
# Pipeline: pg_dump → gzip → gpg AES256. set -o pipefail ловит любой crash.
# GPG 2.x: --batch --pinentry-mode loopback нужен чтобы взять passphrase
# из CLI а не из TTY (которого в cron-job нет).
log "Dumping + encrypting..."
if ! docker exec "$CONTAINER" pg_dump -U "$DB_USER" --no-owner --no-acl "$DB_NAME" 2>/dev/null \
     | gzip \
     | gpg --batch --yes --pinentry-mode loopback \
           --symmetric --cipher-algo AES256 \
           --passphrase "$BACKUP_GPG_PASSPHRASE" \
           --output "$BACKUP_FILE.tmp"; then
  rm -f "$BACKUP_FILE.tmp"
  log "ERROR: pg_dump/gzip/gpg pipeline failed"
  send_msg "❌ *Frame backup failed*: pg_dump/gzip/gpg error"
  exit 1
fi

SIZE_BYTES=$(stat -c%s "$BACKUP_FILE.tmp")
if [ "$SIZE_BYTES" -lt 10000000 ]; then
  rm -f "$BACKUP_FILE.tmp"
  log "ERROR: backup too small ($SIZE_BYTES bytes)"
  send_msg "❌ *Frame backup failed*: dump too small (\`${SIZE_BYTES}\` bytes)"
  exit 1
fi

# Atomic rename — после этой точки дамп считается готовым
mv "$BACKUP_FILE.tmp" "$BACKUP_FILE"
SIZE_HUMAN=$(du -h "$BACKUP_FILE" | cut -f1)
log "Encrypted backup OK: $BACKUP_FILE ($SIZE_HUMAN)"

# ─── 3) Одним файлом от аккаунта Вадима (Telegram Premium, MTProto, до 4 ГБ) ───
# Bot API режет файлы до 50 МБ даже при Premium у получателя — поэтому основной
# путь не бот, а пользовательская сессия сканера хайпа (см. backup_tg_upload.py).
# Файл уходит в «Избранное» Вадима. Если не вышло — откат на части через бота.
MODE=""
log "Uploading as one file via MTProto (Premium)..."
if timeout -k 30 2700 /opt/frame/signals/.venv/bin/python /opt/frame/backup_tg_upload.py \
     "$BACKUP_FILE" "🔐 ${BASENAME} (${SIZE_HUMAN}) — целиком" >> /opt/frame/backups/backup.log 2>&1; then
  MODE="one"
  TOTAL=1
  ALL_OK=true
  log "  ✓ Sent as one file to Saved Messages"
else
  log "  ⚠ one-file upload failed — fallback на части через бота"
fi

if [ -z "$MODE" ]; then
MODE="parts"
# ─── 3b) Split на части ≤48 МБ ───
log "Splitting into chunks of $SPLIT_SIZE..."
split -b "$SPLIT_SIZE" "$BACKUP_FILE" "$BACKUP_FILE.part_"
mapfile -t PARTS < <(ls "$BACKUP_FILE".part_*)
TOTAL=${#PARTS[@]}
log "Created $TOTAL parts"

# ─── 4) Отправка частей в Telegram ───
log "Sending parts to Telegram..."
ALL_OK=true
LAST_N=0
for i in "${!PARTS[@]}"; do
  N=$((i + 1))
  LAST_N=$N
  PART="${PARTS[$i]}"
  PART_SIZE=$(du -h "$PART" | cut -f1)
  CAPTION="🔐 ${BASENAME} — часть ${N}/${TOTAL} (${PART_SIZE})"
  if ! send_doc "$PART" "$CAPTION"; then
    log "ERROR sending part $N/$TOTAL: исчерпаны все ретраи"
    ALL_OK=false
    break
  fi
  log "  ✓ Sent part $N/$TOTAL ($PART_SIZE)"
  sleep 3
done

# ─── 5) Cleanup частей (всегда) ───
rm -f "$BACKUP_FILE".part_*
fi

# ─── 6) Финал ───
if $ALL_OK; then
  # Retention=1: удалить все старые дампы кроме текущего
  find "$BACKUP_DIR" -maxdepth 1 -name "moex_db_*.sql.gz*" -not -name "$BASENAME" -delete
  log "Retention: removed previous dumps (kept only $BASENAME)"
  send_msg "✅ *Frame backup OK (encrypted)*
$( [ "$MODE" = one ] && echo "Одним файлом в «Избранное» Вадима." || echo "Все \`${TOTAL}\` частей отправлены ботом." )
Размер: \`${SIZE_HUMAN}\` (AES-256 + gzip)
Дата: \`$(date '+%d.%m.%Y %H:%M')\`

_Restore см. в backup_db.sh — нужна passphrase из BACKUP_GPG_PASSPHRASE_"
else
  log "WARNING: only $((LAST_N-1))/$TOTAL parts delivered; keeping all dumps as safety net"
  send_msg "⚠️ *Frame backup PARTIAL*
Доставлено \`$((LAST_N-1))/${TOTAL}\` частей.
Дамп на сервере: \`${BACKUP_FILE}\` (\`${SIZE_HUMAN}\`)
Старые дампы оставлены как safety."
  exit 1
fi

log "Done."
