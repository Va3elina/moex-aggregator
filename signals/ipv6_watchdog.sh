#!/bin/bash
# Сторож постоянного IPv6-адреса сервера. Telegram по IPv4 с сервера закрыт (см. память
# telegram_egress_block), поэтому сканер хайпа (signals/tg_hype_scan.py, use_ipv6=True) ходит в
# Telegram ТОЛЬКО по IPv6. 15.10.2026 ~09:00 UTC адрес пропал с eth0 (в конфиге networkd он был),
# и сканер 5 часов падал с Errno 113; причину по журналам восстановить не удалось.
# Адрес берём из конфига netplan сервера — в репозиторий он не попадает.
#   */5 * * * * /bin/bash /opt/frame/signals/ipv6_watchdog.sh >> /opt/frame/logs/ipv6_watchdog.log 2>&1
set -u
CONF=/etc/netplan/52-static-ipv6.yaml
ADDR=$(grep -oE '[0-9a-fA-F:]+:[0-9a-fA-F]+/[0-9]+' "$CONF" 2>/dev/null | head -1)
[ -z "$ADDR" ] && exit 0
if ! ip -6 addr show dev eth0 | grep -q "inet6 ${ADDR} "; then
    if ip -6 addr add "$ADDR" dev eth0; then
        echo "$(date -u +%FT%TZ) IPv6-адрес пропал с eth0 — вернул"
        /usr/bin/python3 -c "import sys; sys.path.insert(0, '/opt/frame/research/robots_tg'); import robots_tg; robots_tg.send('⚠️ Сервер потерял постоянный IPv6-адрес — вернул автоматически (сканер хайпа ходит в Telegram только по IPv6). Если повторяется — разобрать с Timeweb.')"
    else
        echo "$(date -u +%FT%TZ) IPv6-адрес пропал и не вернулся"
    fi
fi
