#!/bin/bash
# Конвейер справок о СЧА с сайтов УК (см. signals/scha_pipeline.py).
#   10 16 * * *  /bin/bash /opt/frame/signals/scha_pipeline.sh scan >> /opt/frame/logs/scha_pipeline.log 2>&1
#   */5 * * * *  /bin/bash /opt/frame/signals/scha_pipeline.sh work >> /opt/frame/logs/scha_pipeline.log 2>&1
# SCHA_AUTO_IMPORT=1 в .env — заливать зелёные; без него — режим отчёта.
set -eu
cd /opt/frame
exec flock -n "/tmp/scha_pipeline_$1.lock" /opt/frame/signals/.venv/bin/python -m signals.scha_pipeline "$@"
