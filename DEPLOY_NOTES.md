# Deployment Notes — Фрейм (таймфрейм.рф)

## Проект — кратко
**Фрейм** — аналитическая платформа для рынка MOEX (фьючерсы, OI, свечи, индексы).
FastAPI бэкенд + React фронтенд, данные из Algopack API, хранятся в PostgreSQL.

**Локально:** `/Users/vadim/PyCharmMiscProject/MOEX/`
**Сервер:** `root@103.88.243.232` (Timeweb Cloud, пароль: `rN+-HpZEnsNqU4`)
**Сайт:** `https://таймфрейм.рф` ✅ (SSL Let's Encrypt, до 15 июня 2026)
**GitHub:** `git@github.com:Va3elina/moex-aggregator.git`

---

## Ключи и токены

| Сервис | Значение |
|--------|----------|
| Сервер SSH | `root@103.88.243.232`, пароль: `rN+-HpZEnsNqU4` |
| Telegram бот | `@frameadminbot` |
| Telegram bot token | `8643169343:AAHIHE4iILha8Wz6v0Wzy7GPf4tG_ILRM7U` |
| Telegram chat_id | `6013767784` |
| БД (прод) | `postgresql://postgres:<пароль из .env>@localhost:5432/moex_db` |
| БД (локал) | `postgresql://postgres:1803@localhost:5432/moex_db` |

**Env файл на сервере:** `/opt/frame/.env` (не перезаписывать через rsync!)

---

## Архитектура (Docker Compose на сервере)

| Контейнер | Роль |
|-----------|------|
| `frame-db-1` | PostgreSQL 17, порт 5432 (только локально) |
| `frame-api-1` | FastAPI + фронтенд (собирается из Dockerfile) |
| `frame-orchestrator-1` | main_orchestrator.py — качает данные каждые 5 мин |
| `frame-nginx-1` | Reverse proxy, порты 80 + 443 (HTTPS) ✅ |
| `frame-certbot-1` | Let's Encrypt авторенью каждые 12ч |

**Файлы на сервере:** `/opt/frame/`

---

## Как деплоить изменения

```bash
# Шаг 1: загрузить код (БЕЗ .env!)
rsync -avz \
  --exclude='node_modules' --exclude='.git' --exclude='__pycache__' \
  --exclude='.env' --exclude='.env.*' \
  -e "sshpass -p 'rN+-HpZEnsNqU4' ssh -o StrictHostKeyChecking=no -o PubkeyAuthentication=no" \
  ./ root@103.88.243.232:/opt/frame/

# Шаг 2: пересобрать образ (ОБЯЗАТЕЛЬНО --no-cache если менялись Python файлы!)
sshpass -p 'rN+-HpZEnsNqU4' ssh -o StrictHostKeyChecking=no -o PubkeyAuthentication=no \
  root@103.88.243.232 "cd /opt/frame && docker compose build --no-cache api"

# Шаг 3: перезапустить нужный контейнер
sshpass -p 'rN+-HpZEnsNqU4' ssh -o StrictHostKeyChecking=no -o PubkeyAuthentication=no \
  root@103.88.243.232 "cd /opt/frame && docker compose up -d --force-recreate api orchestrator"
```

> ⚠️ `docker compose restart` НЕ перечитывает обновлённые файлы из образа!
> Нужен `--force-recreate`. А `docker cp` — только временно, теряется при пересборке.

---

## ⚠️ ЖЁСТКИЙ ПЕРЕЗАПУСК

```bash
sshpass -p 'rN+-HpZEnsNqU4' ssh -o StrictHostKeyChecking=no -o PubkeyAuthentication=no \
  root@103.88.243.232 "
cd /opt/frame
docker compose down
docker rmi frame-api frame-orchestrator 2>/dev/null || true
docker compose build --no-cache api
docker compose up -d
docker compose ps
docker compose logs --tail=20 orchestrator
"
```

> **БД НЕ ТРОГАЕТ** — том `postgres_data` сохраняется при `down`/`up`.

---

## Бэкап БД

**Скрипт:** `/opt/frame/backup_db.sh`
**Cron:** каждый день в 3:00 ночи (серверное время)
**Доставка:** Telegram `@frameadminbot` — 3 файла:
- `moex_db_*.sql.gz` — дамп БД (~566 МБ)
- `logs_*.txt` — логи всех контейнеров за 24ч
- `.env` — продакшн переменные

На сервере хранится только последний бэкап (предыдущий удаляется).

**Запустить вручную:**
```bash
sshpass -p 'rN+-HpZEnsNqU4' ssh -o StrictHostKeyChecking=no -o PubkeyAuthentication=no \
  root@103.88.243.232 "cd /opt/frame && bash backup_db.sh"
```

---

## SSL сертификат

**Домен:** `таймфрейм.рф` = `xn--80aklbnczmv.xn--p1ai`
**Выдан:** Let's Encrypt (E7)
**Истекает:** 15 июня 2026
**Авторенью:** certbot контейнер проверяет каждые 12ч
**Cert path:** `/etc/letsencrypt/live/xn--80aklbnczmv.xn--p1ai/`

**Получить новый сертификат вручную (если нужно):**
```bash
sshpass -p 'rN+-HpZEnsNqU4' ssh -o StrictHostKeyChecking=no -o PubkeyAuthentication=no \
  root@103.88.243.232 "cd /opt/frame && \
  docker compose run --rm --entrypoint certbot certbot certonly \
    --webroot -w /var/www/certbot \
    -d xn--80aklbnczmv.xn--p1ai \
    --email ermolaeffvadick@yandex.ru --agree-tos --non-interactive"
```

---

## Полезные команды на сервере

```bash
# Логи в реальном времени
docker compose logs -f orchestrator
docker compose logs -f api

# Статус данных в БД
docker compose exec db psql -U postgres -d moex_db -c \
  "SELECT MAX(tradedate) FROM open_interests WHERE interval=5;"

# Нагрузка
htop

# Проверить DNS
dig +short xn--80aklbnczmv.xn--p1ai

# Проверить сайт и сертификат
curl -s https://xn--80aklbnczmv.xn--p1ai/health
echo | openssl s_client -connect xn--80aklbnczmv.xn--p1ai:443 2>/dev/null | openssl x509 -noout -dates
```

---

## Локальная разработка

- Локальный .env: `/Users/vadim/PyCharmMiscProject/MOEX/.env`
- БД: `postgresql://postgres:1803@localhost:5432/moex_db`
- **Из Германии (университет):** SSL перехватывается Cisco Umbrella — использовать мобильный хотспот
- Запуск скриптов: `/Users/vadim/PyCharmMiscProject/.venv/bin/python OI/fetch_oi_5min_realtime.py --once`

---

## Google OAuth — TODO

Обновить redirect URI в Google Console:
`https://таймфрейм.рф/api/auth/google/callback`

---

## Сделано (17.03.2026)

- ✅ HTTPS + SSL Let's Encrypt для таймфрейм.рф
- ✅ nginx исправлен (HTTP→HTTPS редирект, правильный punycode)
- ✅ Tooltip на графиках: интерполяция по линии, привязка к ближайшей точке
- ✅ Гостевой доступ расширен до 1 года (было 1 месяц)
- ✅ Требования к паролю при регистрации (live checklist)
- ✅ `/funds` → редирект на `/funds-money`
- ✅ GitHub пуш (Va3elina/moex-aggregator), .env исключён из трекинга
- ✅ Ежедневный бэкап БД + логи + .env в Telegram (@frameadminbot)

## Сделано (27.03.2026)

- ✅ Фикс OI на графиках (alignment по дате/timestamp, скользящее окно)
- ✅ Удалены ~950к фейковых свечей/OI за ночные часы + фильтр 07:00-23:50
- ✅ Необратимый ролловер фьючерсов (как TradingView) + дедуп бордов
- ✅ Ленивый кеш с инкрементальным обновлением (TTL 30мин, NOTIFY дописывает)
- ✅ Dict вместо Pydantic в chart endpoint (сериализация 400ms → 15ms)
- ✅ Fast path для интрадей запросов (cold 13.5с → 2.5с)
- ✅ Скрипты `dev-start.sh` / `dev-stop.sh` для локальной разработки
- ✅ Техническая документация `TECHNICAL.md`
