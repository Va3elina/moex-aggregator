#!/bin/bash
# Диспетчер общей очереди Claude Code Routines — единственный, кто вызывает /fire (см. routine_dispatch.py).
#   * * * * * /bin/bash /opt/frame/signals/routine_dispatch.sh >> /opt/frame/logs/routine_dispatch.log 2>&1
set -eu
cd /opt/frame
exec flock -n /tmp/routine_dispatch.lock /opt/frame/signals/.venv/bin/python -m signals.routine_dispatch "$@"
