#!/usr/bin/env bash
# Прод-деплой (выполняется НА сервере через SSH из deploy-prod workflow).
# Идемпотентен: reset --hard origin/main + rebuild + recreate. Безопасно
# перезапускать (ретраи SSH при флапающей сети могут вызвать его повторно).
set -e
cd /opt/frame

before=$(git rev-parse HEAD)
# reset --hard, НЕ pull: деплой-таргет = ЧИСТЫЙ checkout origin/main.
git fetch origin main
git reset --hard origin/main
after=$(git rev-parse HEAD)
changed=$(git diff --name-only "$before" "$after")
echo "=== Changed files ==="; echo "$changed"

# Убрать висячие renamed-контейнеры от прерванного предыдущего recreate
# (иначе "container name ..._frame-api-1 already in use" → деплой падал).
docker rm -f $(docker ps -aq --filter 'name=_frame-api-1') 2>/dev/null || true

# api всегда (запекает frontend dist + Python код api)
docker compose build api
docker compose up -d --force-recreate api

# orchestrator — ОТДЕЛЬНЫЙ image; пересобираем только если менялся его код
if echo "$changed" | grep -qE '^(OI/|Funds/|Candles/|Macro/|Commodity/|main_orchestrator\.py|requirements\.txt|Dockerfile$)'; then
  echo "=== Orchestrator code changed -> rebuild ==="
  docker rm -f $(docker ps -aq --filter 'name=_frame-orchestrator-1') 2>/dev/null || true
  docker compose build orchestrator
  docker compose up -d --force-recreate orchestrator
fi

# alert-bot — host-side systemd; рестарт только если менялся его код
if echo "$changed" | grep -qE '^signals/alert_bot\.py$'; then
  echo "=== alert_bot changed -> systemctl restart ==="
  systemctl restart frame-alert-bot
fi

echo "=== HEAD на проде: $(git rev-parse --short HEAD) ==="
