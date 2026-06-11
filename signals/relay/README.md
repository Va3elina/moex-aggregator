# Релей через Cloudflare Worker (Telegram + Yahoo)

Обход блокировки РКН **без VPN и без зависимости от IPv6 хоста**.
Хост/контейнер Фрейма ходит в Worker по обычному HTTPS (не заблокирован), Worker
форвардит в апстрим. Один воркер обслуживает ДВА сервиса, маршрут по пути:

```
[Фрейм, РФ] --HTTPS--> [Cloudflare Worker, вне РФ] --+--> api.telegram.org         (бот/каналы/пуши)
                                                     +--> query1.finance.yahoo.com (сырьё/сезонность)
```

- `/<secret>/bot<TOKEN>/<method>` → Telegram (как раньше).
- `/<secret>/yahoo/<path>`        → Yahoo chart-API (воркер сам ставит браузерный UA).

> Yahoo из дата-центра в РФ недоступен по IPv4 вообще (не только Telegram-блок) —
> поэтому commodity-фетчер тоже идёт через релей.

## Деплой / ОБНОВЛЕНИЕ Worker (делает Вадим под своим Cloudflare-аккаунтом)

Воркер `frame-tg-relay` уже задеплоен (для Telegram). Чтобы добавить Yahoo —
**просто обнови его код**:

1. **Cloudflare** → <https://dash.cloudflare.com> → **Workers & Pages** → `frame-tg-relay`.
   *(Если воркера ещё нет: Create → Create Worker → имя `frame-tg-relay` → Deploy.)*
2. **Edit code** → выдели всё, удали, вставь актуальное содержимое `cf-worker.js`
   (рядом) → **Deploy**. Telegram-путь не меняется — обратная совместимость.
3. `RELAY_SECRET` (Settings → Variables and Secrets) — **уже есть**, ничего не трогай.
   *(Если ставишь с нуля: Add → Name `RELAY_SECRET`, Type Secret, Value
   `2daa4ac1f624c27d63ee265bde547bad` или `openssl rand -hex 16` → Deploy.)*
4. **Скажи мне: «воркер обновлён»** — URL и секрет я уже знаю из `TELEGRAM_API_ROOT`.

## Что делаю я после этого

В `/opt/frame/.env` на проде добавлю (URL/секрет = как у TELEGRAM_API_ROOT + `/yahoo`):
```
COMMODITY_API_ROOT=https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/yahoo
```
→ пересоберу `frame-orchestrator` (фетчер в его образе), прогоню backfill сырья,
проверю что все 10 тикеров (Brent/Gold/Silver/… ) снова обновляются глобальными
данными Yahoo. От IPv6 не зависим.

## Проверка (после настройки)

```bash
# Telegram (как было) — {"ok":true,...}:
curl -s "https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/bot<TOKEN>/getMe"
# Yahoo через релей — JSON с {"chart":{"result":[...]}}:
curl -s "https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/yahoo/v8/finance/chart/GC=F?range=5d&interval=1d" | head -c 120
```

## Если `workers.dev` начнёт тормозить (РКН иногда придушивает)

Повесить Worker на свой домен через Cloudflare (Workers Routes / Custom Domain) —
произвольный домен РКН не трогает. Тогда поменять только `TELEGRAM_API_ROOT`.

## Откат

Убрать `TELEGRAM_API_ROOT` из `.env` (или закомментировать) → код вернётся к прямому
`https://api.telegram.org` (через IPv6 хоста). Дефолт в коде — прямой Telegram.
