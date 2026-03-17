#!/usr/bin/env bash
set -euo pipefail
cd "/Users/vadim/PyCharmMiscProject/MOEX"
./stop.sh >/dev/null 2>&1 || true
sleep 1
./start.sh >> logs/hourly_restart.log 2>&1
