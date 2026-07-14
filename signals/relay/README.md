# Релей через Cloudflare Worker (Telegram + Yahoo + Anthropic)

Обход блокировки РКН/гео-блоков **без VPN и без зависимости от IPv6 хоста**.
Хост/контейнер Фрейма ходит в Worker по обычному HTTPS (не заблокирован), Worker
форвардит в апстрим. Один воркер обслуживает ТРИ сервиса, маршрут по пути:

```
[Фрейм, РФ] --HTTPS--> [Cloudflare Worker, вне РФ] --+--> api.telegram.org         (бот/каналы/пуши)
                                                     +--> query1.finance.yahoo.com (сырьё/сезонность)
                                                     +--> api.anthropic.com       (Claude Routine /fire)
```

- `/<secret>/bot<TOKEN>/<method>` → Telegram (как раньше).
- `/<secret>/yahoo/<path>`        → Yahoo chart-API (воркер сам ставит браузерный UA).
- `/<secret>/anthropic/<path>`    → Anthropic API (воркер пробрасывает Authorization +
  anthropic-version/anthropic-beta — без них Routine `/fire` не аутентифицируется).

> Yahoo из дата-центра в РФ недоступен по IPv4 вообще (не только Telegram-блок).
> `api.anthropic.com` отдаёт 403 "Request not allowed" с российских IP (подтверждено
> 2026-07-13) — поэтому content-пайплайн (`signals/content_ai.py`) тоже идёт через релей.

## Деплой / ОБНОВЛЕНИЕ Worker (делает Вадим под своим Cloudflare-аккаунтом)

Воркер `frame-tg-relay` уже задеплоен (для Telegram + Yahoo). Чтобы добавить Anthropic —
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

В `/opt/frame/.env` на проде добавлю (URL/секрет = как у TELEGRAM_API_ROOT + `/anthropic`):
```
CLAUDE_ROUTINE_API_ROOT=https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/anthropic
```
→ `signals/content_ai.py` пойдёт на `/fire` через релей вместо прямого
`api.anthropic.com`, проверю живым вызовом на реальном кандидате, включу крон.

## Проверка (после настройки)

```bash
# Telegram (как было) — {"ok":true,...}:
curl -s "https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/bot<TOKEN>/getMe"
# Yahoo через релей — JSON с {"chart":{"result":[...]}}:
curl -s "https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/yahoo/v8/finance/chart/GC=F?range=5d&interval=1d" | head -c 120
# Anthropic через релей (нужен реальный bearer-токен Routine) — {"claude_code_session_id":...}:
curl -s -X POST "https://frame-tg-relay.<сабдомен>.workers.dev/<RELAY_SECRET>/anthropic/v1/claude_code/routines/<trigger_id>/fire" \
  -H "Authorization: Bearer <токен>" -H "anthropic-version: 2023-06-01" \
  -H "anthropic-beta: experimental-cc-routine-2026-04-01" -H "Content-Type: application/json" \
  -d '{"text": "ping"}'
```

## Если `workers.dev` начнёт тормозить (РКН иногда придушивает)

Повесить Worker на свой домен через Cloudflare (Workers Routes / Custom Domain) —
произвольный домен РКН не трогает. Тогда поменять только `TELEGRAM_API_ROOT`.

## Откат

Убрать `TELEGRAM_API_ROOT` из `.env` (или закомментировать) → код вернётся к прямому
`https://api.telegram.org` (через IPv6 хоста). Дефолт в коде — прямой Telegram.
