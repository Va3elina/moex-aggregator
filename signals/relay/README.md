# Telegram-релей через Cloudflare Worker

Обход блокировки Telegram РКН **без VPN и без зависимости от IPv6 хоста**.
Хост Фрейма ходит в Worker по обычному HTTPS (не заблокирован), Worker форвардит в Telegram.

```
[хост Фрейма, РФ] --HTTPS--> [Cloudflare Worker, вне РФ] --HTTPS--> [api.telegram.org]
   (alert_bot, eval-loop пуши, канал signal-engine)
```

## Деплой Worker (делает Вадим под своим Cloudflare-аккаунтом)

1. **Cloudflare** → бесплатный аккаунт, если нет: <https://dash.cloudflare.com>.
2. **Workers & Pages → Create → Create Worker** → имя `frame-tg-relay` → **Deploy** (создастся hello-world).
3. **Edit code** → удали шаблон, вставь содержимое `cf-worker.js` (рядом) → **Deploy**.
4. **Settings → Variables and Secrets → Add** →
   - Name: `RELAY_SECRET`
   - Type: **Secret** (encrypted)
   - Value: случайная строка. Можно эту: `2daa4ac1f624c27d63ee265bde547bad`
     (или свою: `openssl rand -hex 16`)
   → **Deploy**.
5. Скопируй URL воркера: `https://frame-tg-relay.<твой-сабдомен>.workers.dev`.
6. **Пришли мне:** URL воркера + значение `RELAY_SECRET`.

## Что делаю я после этого

В `/opt/frame/.env` на проде добавлю:
```
TELEGRAM_API_ROOT=https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>
```
→ перезапущу `frame-alert-bot`, eval-loop и канал подхватят на следующем запуске.
Весь Telegram-трафик пойдёт через релей. От IPv6 хоста больше не зависим.

## Проверка (после настройки)

```bash
# с хоста — должен вернуть {"ok":true,...}, НЕ форсируя IPv6:
curl -s "https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/bot<TOKEN>/getMe"
```

## Если `workers.dev` начнёт тормозить (РКН иногда придушивает)

Повесить Worker на свой домен через Cloudflare (Workers Routes / Custom Domain) —
произвольный домен РКН не трогает. Тогда поменять только `TELEGRAM_API_ROOT`.

## Откат

Убрать `TELEGRAM_API_ROOT` из `.env` (или закомментировать) → код вернётся к прямому
`https://api.telegram.org` (через IPv6 хоста). Дефолт в коде — прямой Telegram.
