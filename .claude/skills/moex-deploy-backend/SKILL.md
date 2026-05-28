---
name: moex-deploy-backend
description: Deploy Python backend changes to Фрейм production. Use when user says "задеплой бэк", "обнови X.py на проде", "залей роутер", "update backend", or when any file in api/ folder has been changed and needs to go live. Also triggers when user wants to test API changes on production.
---

# Deploy Backend to Фрейм Production

**Главное правило** (с 2026-05-12):
**Деплой = `git pull` → `docker compose build api` → `up -d api`.**
**Никаких `docker cp` Python-файлов.**

## Почему именно так

Backend код (`api/*`, `main_orchestrator.py`, `requirements.txt`) — в git.
Docker image содержит Python interpreter + dependencies + копию api/ кода.

Раньше делали `docker cp api/file.py frame-api-1:/app/api/file.py + restart`. Это работало, но при recreate container терялось. **12 мая 2026** контейнер пересоздался → откат к Apr 14 image, потеря всех правок последнего месяца на проде. Восстановили rebuild'ом из git.

## ⚠️ Critical Rules

1. **Изменения только через git → image rebuild.** Никаких docker cp.
2. **`requirements.txt`** должен быть UTF-8 без BOM (pip не парсит UTF-16 LE).
3. **SSH preamble обязателен**: `-o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519`.
4. **При rebuild — image тащит и frontend stage**. Если только backend изменился — docker layer cache переиспользует frontend-build. Если хочешь чисто backend rebuild — `docker compose build --no-cache=false api`.

## Standard Deploy Sequence

### Один Python-файл (типичный case)

```bash
# 1. Локально
git add api/routers/myfile.py
git commit -m "fix(api): описание"
git push origin main

# 2. Сервер — одной SSH командой
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "cd /opt/frame && \
   git stash push -m deploy-wip && git pull origin main && git stash pop && \
   docker compose build api && \
   docker compose up -d --force-recreate api && \
   docker restart frame-nginx-1 && \
   sleep 8 && \
   docker exec frame-api-1 python3 -c 'import urllib.request; print(urllib.request.urlopen(\"http://localhost:8000/health\").read().decode())'"
```

**⚠️ Два обязательных шага (грабли 2026-05-28, см. moex-deploy-frontend):**
- **`git stash push … && git pull && git stash pop`** — на сервере есть незакоммиченный
  WIP (`docker-compose.yml` volume `manual_scha`, `signals/`, `requirements.txt`) →
  голый `git pull` падает «local changes would be overwritten». `stash pop` обычно auto-merge'ит.
- **`docker restart frame-nginx-1` после `--force-recreate api`** — recreate даёт
  контейнеру новый IP, nginx кеширует старый upstream → **502 на всех `/api/*`**.
  restart nginx (5 сек) чинит. ВСЕГДА после recreate.

**⚠️ Если правка касается `.env` (новый секрет/флаг)** — добавь переменную в
`docker-compose.yml` (блок `environment:` сервиса `api`, через `${VAR:-default}`),
впиши значение в `/opt/frame/.env`, и деплой ОБЯЗАТЕЛЬНО с `--force-recreate`
(env читается только при создании контейнера). Иначе код увидит пустую переменную.

**⚠️ Если правка в OI/Candles/signals/main_orchestrator** — нужен ещё
`docker compose build orchestrator && up -d --force-recreate orchestrator`
(отдельный image `frame-orchestrator`, `build api` его НЕ трогает). См. deploy_manual.md.

### Несколько файлов / новый роутер

То же. `docker compose build api` собирает image с COPY всех api/* за один проход.

### При новом pip-пакете

```bash
# 1. Edit requirements.txt, добавить строку
# 2. Локально проверить что файл UTF-8: file requirements.txt
# 3. git commit + push + (Sequence как выше)
```

`docker compose build api` пройдёт `RUN pip install -r requirements.txt` → пакет в image permanently.

## Verify after deploy

### Health
```bash
ssh ... "docker exec frame-api-1 python3 -c 'import urllib.request; print(urllib.request.urlopen(\"http://localhost:8000/health\").read().decode())'"
```
Ждём: `{"status":"ok","database":"ok"}`

### Endpoint возвращает JSON (не SPA HTML fallback)
```bash
ssh ... "docker exec frame-api-1 python3 -c '
import urllib.request
r = urllib.request.urlopen(\"http://localhost:8000/api/YOUR_ENDPOINT\")
body = r.read()
print(\"OK: JSON\" if body[:1] in (b\"{\", b\"[\") else \"BROKEN: HTML (SPA fallback)!\")
'"
```

Полная проверка endpoints через sub-agent `moex-deploy-verifier`.

## Common Issues

### `ModuleNotFoundError: No module named 'redis'` (или другой пакет)
**Причина**: пакет добавили в `requirements.txt`, но image не пересобрали (или dock cp в running был эфемерен).
**Fix**: убедиться что пакет в requirements.txt + `docker compose build api && up -d api`.

### TS errors не относящиеся к backend
**Причина**: `docker compose build api` тащит и frontend stage (multi-stage), TS ошибки в frontend ломают весь build.
**Fix**: исправить TS отдельно либо проверить что нет ghost-файлов на `/opt/frame/` от прошлых docker cp.

### Restart frame-api-1 не нужен сам по себе
После `docker compose up -d api` контейнер автоматически recreate'ится с новым image. `docker restart frame-api-1` без rebuild = откат к старому image-state (но кода всё равно в image current).

## Special cases

### Adding a new router

`include_router` order важен. В одном image rebuild всё пройдёт атомарно — ему safe.

### Removing a router

Удалить файл + `git rm` + commit. После rebuild image не содержит. SPA fallback сработает на удалённом endpoint — `moex-deploy-verifier` это поймает.

### Database migration (новая таблица)

1. Миграцию SQL пишем в `db/migrations/*.sql` или применяем напрямую.
2. На сервере: `ssh ... "docker exec -i frame-db-1 psql -U postgres -d moex_db < /opt/frame/db/migration.sql"`.
3. Потом deploy backend как обычно — он работает с новой схемой.

## Project-specific paths

- **Server**: `root@103.88.243.232`
- **Repo на сервере**: `/opt/frame/` (git checkout main)
- **Container**: `frame-api-1`
- **API path**: `/app/api/` (внутри image)
- **DB**: `docker exec frame-db-1 psql -U postgres -d moex_db`

## Known env vars (в контейнере)

- `DB_URL` = `postgresql+pg8000://...` (для SQLAlchemy sync)
- `DB_URL_ASYNC` = `postgresql://...` (для asyncpg)
- `JWT_SECRET_KEY` — для access/refresh tokens
- `ALGOPACK_API_KEY` — для Algopack
- `GOOGLE_CLIENT_ID/SECRET`, `VK_*`, `YANDEX_*`, `TELEGRAM_BOT_TOKEN` — OAuth
- `TBANK_TERMINAL_KEY`, `TBANK_PASSWORD` — billing T-Bank
- `YOOKASSA_SHOP_ID`, `YOOKASSA_SECRET_KEY` — billing fallback

Все через `os.environ["X"]`. **Не хардкодить значения в коде.**

## Anti-pattern

❌ `docker exec frame-api-1 pip install somepackage` + `docker restart` — эфемерно, потеряется на recreate.
❌ `docker cp api/file.py frame-api-1:/app/api/...` — то же эфемерно.
✅ git commit + push + `docker compose build api && up -d api` — все изменения в image, persistent.
