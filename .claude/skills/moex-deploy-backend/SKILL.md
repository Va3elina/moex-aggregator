---
name: moex-deploy-backend
description: Deploy Python backend changes to Фрейм production. Use when user says "задеплой бэк", "обнови X.py на проде", "залей роутер", "update backend", or when any file in api/ folder has been changed and needs to go live. Also triggers when user wants to test API changes on production.
---

# Deploy Backend to Фрейм Production

> ## ⚠️ ДЕПЛОЙ ТЕПЕРЬ АВТО-CI (с 2026-06-09)
>
> **Деплой = `git push` в `main`. И всё.**
>
> - `git push` → GitHub Actions **build-check** → (если зелёный) → **deploy-prod** (SSH на прод сам делает `git reset --hard` + rebuild). **НИКТО не деплоит руками по SSH.**
> - Сериализация через `concurrency: deploy-prod`: два пуша — по очереди, не сталкиваются. Битый билд **не выкатывается** (deploy ждёт зелёный build-check).
> - **Сервер `/opt/frame` = чистый deploy-target, НЕ воркспейс.** Деплой делает `git reset --hard origin/main` → **любая правка/scp прямо на проде СТИРАЕТСЯ** следующим деплоем. Все изменения — через `git push`, не на сервере. **Не scp-ить** черновики/research на `/opt/frame`.
> - **SSH на прод всё ещё нужен** для: логов, SQL/БД, инспекции, аварийного деплоя (если CI недоступен).
> - Детали в памяти: `ci_cd.md`, `deploy_manual.md`.

**Главное правило** (с 2026-06-09):
**Закоммить → `git push origin main` → CI сам соберёт `docker compose build api` и пересоздаст контейнер на проде.**
**Никаких ручных SSH-деплоев. Никаких `docker cp` Python-файлов. Никаких правок на сервере.**

## Почему именно так

Backend код (`api/*`, `main_orchestrator.py`, `requirements.txt`) — в git.
Docker image содержит Python interpreter + dependencies + копию api/ кода.
CI после зелёного build-check заходит на прод по SSH и делает `git reset --hard origin/main` → `docker compose build api` → `up -d --force-recreate api`.

Раньше делали `docker cp api/file.py frame-api-1:/app/api/file.py + restart`. Это работало, но при recreate container терялось. **12 мая 2026** контейнер пересоздался → откат к Apr 14 image, потеря всех правок последнего месяца на проде. Восстановили rebuild'ом из git. **С 2026-06-09 ручной путь больше не основной** — деплоит CI из git, поэтому прод всегда == `origin/main`.

## ⚠️ Critical Rules

1. **Изменения только через `git push` в `main`.** CI деплоит сам. Никаких docker cp, никаких правок на сервере, никакого ручного SSH-деплоя (кроме аварии — см. ниже).
2. **Прод == `origin/main`.** Сервер делает `git reset --hard` каждый деплой → всё, что не в git, стирается. Не оставляй на `/opt/frame` черновиков/research/scp-файлов.
3. **`requirements.txt`** должен быть UTF-8 без BOM (pip не парсит UTF-16 LE) — иначе CI-build упадёт на красном гейте и деплой не выкатится.
4. **SSH preamble** (для логов/SQL/аварийного деплоя): `-o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i ~/.ssh/id_ed25519 root@103.88.243.232`.
5. **При rebuild — image тащит и frontend stage**. TS-ошибки в frontend ломают весь build → красный build-check → деплой не пойдёт. Чини TS отдельно.

## Standard Deploy Sequence (основной путь — `git push`)

### Один Python-файл (типичный case)

```bash
# Локально — это ВЕСЬ деплой:
git add api/routers/myfile.py
git commit -m "fix(api): описание"
git push origin main
```

Дальше всё делает CI:
1. **build-check** собирает image (frontend + backend stage). Красный → деплой не выкатывается, прод не трогается.
2. **deploy-prod** (после зелёного build-check) заходит на прод по SSH и делает
   `git fetch && git reset --hard origin/main && docker compose build api && docker compose up -d --force-recreate api`.
3. `concurrency: deploy-prod` сериализует параллельные пуши — по очереди, без гонок.

**Проверить статус деплоя:**
```bash
gh run list --workflow=deploy-prod --limit 3
gh run watch        # следить за текущим прогоном
```

**⚠️ Если правка касается `.env` (новый секрет/флаг)** — переменную надо добавить в
`docker-compose.yml` (блок `environment:` сервиса `api`, через `${VAR:-default}`) **и закоммитить**,
а само значение вписать в `/opt/frame/.env` на сервере **вручную по SSH** (это единственное,
что НЕ в git и переживает `reset --hard`). После этого `--force-recreate` обязателен — CI делает его сам
(env читается только при создании контейнера).

**⚠️ Если правка в OI/Candles/signals/main_orchestrator** — у orchestrator **отдельный image**
`frame-orchestrator`, и `build api` его НЕ трогает. CI пересобирает только `api`. Если менялся код
orchestrator — пересобрать его придётся аварийным путём по SSH (см. ниже) или расширить workflow. См. `deploy_manual.md`.

---

## ⚙️ Аварийный ручной деплой (только если CI недоступен)

Использовать **только** когда GitHub Actions недоступен/сломан, а выкатить надо срочно.
В норме — `git push` и CI. Два варианта аварии:

**А) Перезапустить workflow руками (предпочтительно):**
```bash
gh workflow run deploy-prod
```

**Б) Напрямую на сервере (reset --hard, НЕ git pull):**
```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "cd /opt/frame && \
   git fetch && git reset --hard origin/main && \
   docker compose build api && \
   docker compose up -d --force-recreate api && \
   sleep 8 && \
   docker exec frame-api-1 python3 -c 'import urllib.request; print(urllib.request.urlopen(\"http://localhost:8000/health\").read().decode())'"
```

- **`git reset --hard origin/main`, НЕ `git pull`.** Сервер — чистый target, не воркспейс. `pull` спотыкается на расхождениях; `reset --hard` гарантированно даёт прод == `origin/main`. (Раньше тут был `git stash push/pop` для WIP на сервере — WIP больше не держим, сервер чистый.)
- **nginx НЕ трогать** — resolver-фикс (`c40017f`) убрал необходимость рестартить nginx после recreate. (Историческая грабля 2026-05-28: recreate давал контейнеру новый IP, nginx кешировал старый upstream → 502; resolver-фикс это снял. `docker restart frame-nginx-1` больше не нужен.)
- **orchestrator пересобирать отдельно**, если менялся его код:
  `docker compose build orchestrator && docker compose up -d --force-recreate orchestrator`
  (`build api` его НЕ трогает).

### Несколько файлов / новый роутер

То же — один `git push`. CI-шный `docker compose build api` собирает image с COPY всех api/* за один проход.

### При новом pip-пакете

```bash
# 1. Edit requirements.txt, добавить строку
# 2. Локально проверить что файл UTF-8: file requirements.txt
# 3. git commit + git push origin main
```

CI-шный `docker compose build api` пройдёт `RUN pip install -r requirements.txt` → пакет в image permanently.

## Verify after deploy

CI сам делает health-check в конце deploy-prod — зелёный прогон = прод поднялся.
Ниже — ручные проверки по SSH, если хочешь убедиться сам (или после аварийного деплоя).

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
**Причина**: пакет добавили в `requirements.txt`, но не запушили (или старый docker cp в running был эфемерен).
**Fix**: убедиться что пакет в requirements.txt → `git commit + push` → CI пересоберёт image.

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

1. Миграцию SQL пишем в `db/migrations/*.sql`, коммитим и пушим (файл попадёт на прод через `reset --hard`).
2. SQL применяем **вручную по SSH** (миграции CI автоматически не накатывает):
   `ssh ... "docker exec -i frame-db-1 psql -U postgres -d moex_db < /opt/frame/db/migration.sql"`.
   (БД переживает `reset --hard` — она в named volume, не в git.)
3. Код, работающий с новой схемой, выкатывается обычным `git push` → CI.

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
❌ Любая правка/scp прямо на `/opt/frame` — сотрётся следующим `git reset --hard origin/main`.
❌ Ручной SSH-деплой «по привычке» — деплоит CI; ручной путь только для аварии.
✅ `git commit + git push origin main` → CI собирает image и пересоздаёт контейнер. Все изменения в image, persistent, прод == `origin/main`.
