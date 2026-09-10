#!/bin/bash
# Wrapper: ночной аудит разметки второго мозга — стреляет Routine frame-brain-audit.
#   20 22,23,0,1 * * * /bin/bash /opt/frame/signals/brain_audit_fire.sh >> /opt/frame/logs/brain_audit.log 2>&1
# Стрелять ли, решает сам скрипт: ночь, конвейер постов свободен, прошлая партия
# вернулась, первый проход не закончен или воскресенье (см. brain_audit_fire.py).
set -eu
cd /opt/frame
export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
export BRAIN_WARMUP=0
exec timeout -k 30 120 /opt/frame/signals/.venv/bin/python -m signals.brain_audit_fire "$@"
