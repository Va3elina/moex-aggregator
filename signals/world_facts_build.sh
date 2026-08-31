#!/bin/bash
# Wrapper: пересборка world_facts из macro_data. Идемпотентно, можно гонять часто.
# Ставить на крон необязательно — ряд обновляется медленно; достаточно после
# обновления макро-данных или руками.
set -eu
cd /opt/frame
export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
exec /opt/frame/signals/.venv/bin/python -m signals.world_facts_build "$@"
