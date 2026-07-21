#!/bin/bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Host-side алертер падений пайплайнов ингеста (таймфрейм.рф).
# Cron (каждые 15 мин): шлёт в Telegram при НОВЫХ падениях/восстановлениях
# пайплайнов — edge-triggered через state-файл, без спама на повторах.
#
#   */15 * * * * /opt/frame/monitor_alert.sh >> /opt/frame/logs/monitor_alert.log 2>&1
#
# Почему с хоста, а не из контейнера: РКН блокирует IPv4 Telegram, а контейнеры
# до api.telegram.org не достают вообще (проверено: оркестратор → 000). Поэтому
# curl -6 с хоста, как backup_db.sh и signals/alerts_run.sh. Токен из /opt/frame/.env.
#
# Источник правды — pipeline_runs (пишется оркестратором/демонами через
# pipeline_heartbeat). «Проблемный» = last_status<>'ok' ИЛИ не запускался дольше
# своего порога (ловит и упавший скрипт, и полностью вставший оркестратор/SPOF).
# Порог: 26ч по умолчанию, НЕДЕЛЬНЫМ (distributions, пн-cron) — 9 дней.
# ⚠️ Урок 07.2026: единый порог 26ч сделал недельный distributions «проблемным»
# каждые вт-вс → он навечно осел в state-файле, и когда его cron реально умер
# (frame-api-1 → frame-api-N), edge-а не было — алерт так и не ушёл. Порог
# обязан быть >= худшего легитимного разрыва КАЖДОГО пайплайна, иначе
# edge-trigger насыщается шумом и слепнет.
# ─────────────────────────────────────────────────────────────

cd /opt/frame
if [ -f /opt/frame/.env ]; then
  set -a
  # shellcheck disable=SC1091
  source /opt/frame/.env
  set +a
fi

CONTAINER="frame-db-1"
STATE_FILE="/opt/frame/logs/monitor_alert_state"
mkdir -p /opt/frame/logs
touch "$STATE_FILE"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }

send_msg() {
  if [ -z "${BOT_TOKEN:-}" ] || [ -z "${ADMIN_CHAT_ID:-}" ]; then
    log "WARN: BOT_TOKEN/ADMIN_CHAT_ID не заданы — пропускаю отправку"
    return 0
  fi
  curl -6 -s --max-time 30 -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    -d "chat_id=${ADMIN_CHAT_ID}" \
    --data-urlencode "text=$1" \
    -d "parse_mode=Markdown" > /dev/null || log "WARN: send_msg curl failed"
}

# Текущие проблемные пайплайны: строки «name|status|lag_h».
ROWS=$(docker exec "$CONTAINER" psql -U postgres -d moex_db -tA -F'|' -c \
  "SELECT pipeline, last_status, round(EXTRACT(EPOCH FROM now()-last_run_at)/3600, 1)
   FROM pipeline_runs
   WHERE last_status <> 'ok'
      OR last_run_at < now() - (CASE pipeline
           WHEN 'distributions' THEN interval '9 days'
           ELSE interval '26 hours' END)
   ORDER BY pipeline" 2>/dev/null || true)

CURR_NAMES=$(printf '%s\n' "$ROWS" | awk -F'|' '$1 != "" {print $1}' | sort -u)
PREV_NAMES=$(grep -v '^$' "$STATE_FILE" 2>/dev/null | sort -u || true)

NEW=$(comm -13 <(printf '%s\n' "$PREV_NAMES") <(printf '%s\n' "$CURR_NAMES") | grep -v '^$' || true)
RECOVERED=$(comm -23 <(printf '%s\n' "$PREV_NAMES") <(printf '%s\n' "$CURR_NAMES") | grep -v '^$' || true)

if [ -n "$NEW" ]; then
  DETAIL=""
  while IFS='|' read -r name status lag; do
    if [ -z "$name" ]; then continue; fi
    if printf '%s\n' "$NEW" | grep -qx "$name"; then
      DETAIL="${DETAIL}• \`${name}\` — ${status}, посл. запуск ${lag}ч назад
"
    fi
  done <<< "$ROWS"
  log "ALERT new failures: $(echo "$NEW" | tr '\n' ' ')"
  send_msg "🔴 *Frame: пайплайн упал*
${DETAIL}
_Источник: pipeline_runs. Смотри \`docker logs frame-orchestrator-1\` / лог скрипта._"
fi

if [ -n "$RECOVERED" ]; then
  LIST=$(printf '%s\n' "$RECOVERED" | sed 's/^/• `/; s/$/`/')
  log "RECOVERED: $(echo "$RECOVERED" | tr '\n' ' ')"
  send_msg "🟢 *Frame: пайплайн восстановился*
${LIST}"
fi

# Сохраняем текущее проблемное множество для следующего прогона (edge-trigger).
printf '%s\n' "$CURR_NAMES" | grep -v '^$' > "$STATE_FILE" || true

if [ -z "$NEW" ] && [ -z "$RECOVERED" ]; then
  log "ok (проблемных: $(printf '%s\n' "$CURR_NAMES" | grep -vc '^$' || true), изменений нет)"
fi
