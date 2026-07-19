#!/bin/bash
# Wrapper: Этап 6 content-пайплайна — репост-хайп TG-каналов (MTProto),
# см. config.TG_HYPE_CHANNELS (markettwits, newssmartlab).
# Раз в 5 минут (чаще, чем раньше — нужно поймать узкое окно измерительного
# чекпоинта +3мин; чекпоинты дискретные, см. модульную докстроку):
#   */5 * * * * /opt/frame/signals/tg_hype_scan.sh >> /opt/frame/logs/tg_hype_scan.log 2>&1
set -eu

# Найдено 2026-07-19 (Вадим): decision-чекпоинт закоммитил fwd_3 + promoted=TRUE
# для поста, у которого в content_candidates так и не появилась запись, без
# единой строки ошибки в логе. Крон гоняет этот скрипт раз в 2 минуты, а сам
# прогон делает живые запросы к Telegram (видели тайминги/таймауты в логах) —
# при долгом прогоне два инстанса пересекаются и гонятся за одной и той же
# pending-строкой без блокировки. flock не даёт второму инстансу стартовать,
# пока первый не закончил — тот же паттерн, что и остальные signals/*.sh.
exec 200>/tmp/tg_hype_scan.lock
flock -n 200 || { echo "[tg_hype_scan] предыдущий запуск ещё не завершился, пропускаю тик"; exit 0; }

cd /opt/frame

export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')

exec /opt/frame/signals/.venv/bin/python -m signals.tg_hype_scan "$@"
