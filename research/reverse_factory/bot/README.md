# Бот «спокойный Kamaz»

Сервер: `/opt/kamaz-bot`, служба `kamaz-bot` (systemd), пользователь `kamaz`, состояние в `bot/state/`.

| Что | Команда (на сервере) |
|---|---|
| статус | `/opt/kamaz-bot/venv/bin/python /opt/kamaz-bot/bot/run.py status` |
| итоги | `… run.py report` |
| СТОП (снять заявки, закрыть позиции) | `… run.py stop` |
| ключ (демо / реальный) | `/opt/kamaz-bot/bot/setup_keys.sh demo` или `live` — запускает Вадим сам |
| проверить ключ | `… run.py check --mode demo` |
| журнал службы | `journalctl -u kamaz-bot -f` |

Режим задаётся в `/etc/systemd/system/kamaz-bot.service` (`--mode paper|demo|live`), после правки — `systemctl daemon-reload && systemctl restart kamaz-bot`.
Проверки ядра: `bot/test_core.py` (книга = бэктест), `bot/test_live.py` (исполнитель = книга на имитации биржи).
