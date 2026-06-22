---
name: moex-server-access
description: Безопасная работа с production-сервером Фрейм (таймфрейм.рф) через SSH — деплой кода (frontend/backend) теперь авто-CI (git push в main → GitHub Actions), SSH остаётся для логов, БД-запросов, команд в контейнерах и аварийного ручного деплоя. Соблюдает rate-limit и fail2ban защиту. Триггер когда пользователь говорит «задеплой на сервер», «обнови сайт», «посмотри логи прода», «выполни SQL на проде», «restart api», «check production», «прод не работает».
---

# MOEX Server Access

Безопасная работа с сервером Фрейм для AI-помощников.

> # ⚠️ ДЕПЛОЙ ТЕПЕРЬ АВТО-CI (с 2026-06-09)
>
> **Деплой = попадание в `main` → CI. НЕ деплоить руками по SSH — CI делает сам.**
> Дефолт — **ветка → PR → мёрж** (мёрж = тот push, что триггерит CI; см.
> `CONTRIBUTING.md`). Прямой `git push origin main` — solo-исключение.
>
> - `git push origin main` → GitHub Actions **build-check** → (если зелёный) → **deploy-prod** (SSH на прод сам, `git reset --hard origin/main` + rebuild + recreate).
> - Сериализация через `concurrency: deploy-prod` — два пуша идут по очереди, не сталкиваются. Битый билд **НЕ** выкатывается (deploy ждёт зелёный build-check).
> - **Сервер `/opt/frame` = чистый deploy-target, НЕ воркспейс.** Деплой делает `git reset --hard origin/main` → **любая** ручная правка/`scp` прямо на проде **стирается** следующим деплоем. Все изменения — только через `git push`. НЕ scp-ить черновики/research на `/opt/frame`.
> - SSH на прод **остаётся** нужен для: логов, SQL/БД, инспекции, аварийного деплоя.
> - Детали в памяти: `ci_cd.md`, `deploy_manual.md`.

**Эфемерность (с 2026-05-12, по-прежнему в силе)**: НИКАКИХ `docker cp` в живой контейнер, НИКАКОГО `pip install`/vim/sed в контейнере — теряются при recreate. Единственный source of truth — image, собранный из git.

Сервер защищён жёстким SSH rate-limit и fail2ban — нарушение SSH-правил приводит к 24-часовому бану IP.

---

## ⚠️ SSH правила (не нарушать!)

### 1. ВСЕГДА полный SSH preamble

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i <ПУТЬ_К_КЛЮЧУ> <USER>@103.88.243.232 "..."
```

**Почему `IdentitiesOnly=yes`**: SSH-агент по умолчанию предлагает все ключи подряд → каждый ключ = 1 auth attempt → fail2ban ban на 24 часа после 3+ failed.

**Какой USER**: `root` (Вадим, для управления контейнерами и git) или `alexgondon` (коллега, для тех же целей с sudo).

### 2. НЕ probe'и сервер

❌ `ssh ... "echo ok"` — тратит SSH connection попусту.

✅ Для проверки live-state — HTTPS (порт 443 не рейт-лимитнут):
```bash
curl -sk -o /dev/null -w "%{http_code}\n" "https://103.88.243.232/" \
  -H "Host: xn--80aklbnczmv.xn--p1ai" --max-time 8
# 200 → сервер работает
```

### 3. ОДНА SSH connection на операцию

iptables drop'ит если 5+ NEW connections за 60s. Делать **chained command** в одном `ssh "..."`.

### 4. НЕ retry после `Connection timed out`

Если timeout — IP попал в rate-limit (60s drop) или fail2ban (24h ban). Retry в loop'е продлевает drop. Подожди минуту, проверь через `nc -z -w 5 103.88.243.232 22`. Если `BLOCKED` 5+ минут — нужен unban через VNC.

---

## 🚀 Главный deploy flow (с 9 июня 2026 — АВТО-CI)

**Деплой = `git push`. Больше ничего руками по SSH не нужно.**

### Единственный шаг: commit + push

```bash
# Bump SW версии руками НЕ нужно — postbuild подставляет хэш автоматически
# (см. MEMORY: SW cache auto frame-<sha1>). Просто правь фронт.

git add <files>
git commit -m "scope: description"
git push origin main
```

Дальше **сам CI**:
1. **build-check** (GitHub Actions) собирает фронт+бэк. Если красный — деплоя НЕ будет.
2. Если зелёный — **deploy-prod** заходит на прод по SSH, делает `git reset --hard origin/main`, `docker compose build api`, `docker compose up -d --force-recreate api`, health-check.
3. `concurrency: deploy-prod` сериализует параллельные пуши — выкатываются по очереди.

**Проверить статус прогона**:
```bash
gh run list --workflow deploy-prod --limit 3
gh run watch          # follow последнего прогона
```

**Время**: build-check + deploy ~3-5 мин суммарно.

> ⚠️ orchestrator (`frame-orchestrator-1`) CI пересобирает отдельно, только если менялся его код. nginx CI **не трогает** (resolver-фикс — рестарт nginx ломает DNS-резолвинг внутри контейнера).

---

## 🆘 Аварийный ручной деплой (ТОЛЬКО если CI недоступен)

Историческая/fallback-процедура. В обычной жизни **не использовать** — пуш делает всё сам.

**Вариант A — перезапустить workflow вручную**:
```bash
gh workflow run deploy-prod
```

**Вариант B — напрямую на сервере** (если GitHub Actions лежит):
```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "cd /opt/frame && git fetch && git reset --hard origin/main && \
   docker compose build api && \
   docker compose up -d --force-recreate api && \
   sleep 10 && \
   docker exec frame-api-1 python3 -c 'import urllib.request; print(urllib.request.urlopen(\"http://localhost:8000/health\").read().decode())' && \
   curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep CACHE_NAME | head -1 && \
   curl -sk 'https://localhost/' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -oE 'index-[A-Za-z0-9_-]+\\.js' | head -1"
```

**Важно**: `git reset --hard origin/main`, **НЕ** `git pull` — сервер чистый target, локальных правок там быть не должно, pull может упереться в конфликт. orchestrator пересобирай отдельно (`docker compose build orchestrator && docker compose up -d --force-recreate orchestrator`) только если менял его код. nginx не трогай.

**Время**: build ~1-2 мин с кешем (~3-5 без), downtime пересоздания ~30 сек.

---

## 🔍 Чтение логов / диагностика (без deploy)

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519 \
    root@103.88.243.232 \
  "docker logs frame-api-1 --tail 50 --since 10m 2>&1 | grep -iE 'error|exception|warning' | tail -20"
```

Контейнеры: `frame-api-1`, `frame-db-1`, `frame-redis-1`, `frame-orchestrator-1`, `frame-nginx-1`, `frame-tg-bot-1`.

---

## 🗄️ SQL queries

### Простой query
```bash
ssh ... 'docker exec frame-db-1 psql -U postgres -d moex_db -c "SELECT COUNT(*) FROM users"'
```

### Длинный multi-line SQL через stdin
```bash
cat <<'SQL' | ssh ... 'docker exec -i frame-db-1 psql -U postgres -d moex_db'
SELECT u.email, COUNT(ae.event_id) AS events
FROM users u
LEFT JOIN analytics_events ae ON ae.user_id = u.id
WHERE ae.server_ts >= NOW() - INTERVAL '7 days'
GROUP BY u.email
ORDER BY events DESC LIMIT 10;
SQL
```

### Apply migration
```bash
cat /path/to/migration.sql | ssh ... 'docker exec -i frame-db-1 psql -U postgres -d moex_db'
```

---

## 🐛 Troubleshooting

### `Permission denied (publickey)`
Ключ не зарегистрирован. Проверь `-i <path>`, попроси админа добавить твой публичный ключ в `/home/USER/.ssh/authorized_keys`.

### `Too many authentication failures`
SSH agent перебирает ключи → fail2ban после 3+. **Решение**: `-o IdentitiesOnly=yes -o IdentityAgent=none`. **Если уже случилось** — IP забанен на 24ч, нужен unban через VNC.

### `Connection timed out`
- **iptables rate-limit** (5+ conn/60s): подожди 60s.
- **fail2ban**: подожди 24ч или VNC unban.
- Diagnose: `nc -z -w 5 103.88.243.232 22 && echo OPEN || echo BLOCKED`.

### `ModuleNotFoundError: No module named 'X'`
Пакет в `requirements.txt`? Если нет — добавь в `requirements.txt`, закоммить и **`git push`** (CI пересоберёт image сам). Руками `docker compose build api` — только в аварийном ручном деплое.

### Сайт раздаёт SPA HTML вместо JSON для /api/...
Endpoint не зарегистрирован (router не include или удалён). Проверь `docker logs frame-api-1` на ImportError при startup.

### Container постоянно restarting
`docker logs frame-api-1 --tail 50` покажет crash reason. Часто — `ModuleNotFoundError` после rebuild без обновления requirements.txt.

### `Permission denied while trying to connect to Docker daemon`
User не в группе `docker`. Под `alexgondon` — префиксуй `sudo`: `sudo docker exec ...`. Под `root` — без sudo.

---

## ⚠️ Anti-patterns (не делать!)

### ❌ `docker cp` живого контейнера для деплоя
```bash
tar -cz dist | ssh ... "docker cp /tmp/dist/. frame-api-1:/app/frontend/dist/"
```
Это работает **5 секунд**, но при любом `docker compose up` или recreate container — **исчезает**. Так мы 12 мая получили откат к Apr 14.

### ❌ `docker exec frame-api-1 pip install pkg`
Эфемерно. Пакет пропадёт при recreate. Добавлять в `requirements.txt` + rebuild.

### ❌ Редактирование файлов прямо в контейнере (vim, sed)
Не попадает в git, не попадает в image — потеряется. Всегда правки в git → push → CI rebuild.

### ❌ Правки / `scp` / черновики прямо в `/opt/frame` на сервере
`/opt/frame` — чистый deploy-target. Деплой делает `git reset --hard origin/main` → **любая** ручная правка там **затрётся** следующим пушем. Никаких research-скриптов, черновиков, `scp` на прод. Всё — через `git push`.

### ❌ `docker compose down` без согласования
Контейнеры удаляются вместе с ephemeral state (если он был). Сейчас не страшно (всё в image), но раньше было катастрофой.

---

## 🗺️ Project structure

### Сервер
- **IP**: `103.88.243.232`
- **Домен**: `xn--80aklbnczmv.xn--p1ai` (punycode для `таймфрейм.рф`)
- **Repo path**: `/opt/frame/` — чистый deploy-target, git checkout main. Деплой = `git reset --hard origin/main` (CI). НЕ воркспейс — ничего руками тут не править.
- **VNC console** (для unban): https://timeweb.cloud/my/servers/7006331/console

### Контейнеры
- `frame-api-1` — FastAPI + frontend dist (multi-stage build), путь в image: `/app/`
- `frame-db-1` — PostgreSQL 16, db `moex_db`, user `postgres`
- `frame-redis-1` — cache + SSE
- `frame-orchestrator-1` — `main_orchestrator.py`, cron-like jobs
- `frame-nginx-1` — reverse proxy + HTTPS
- `frame-tg-bot-1` — Telegram bot

### Структура в image (`/app/`)
- `/app/api/` — FastAPI код (main.py + routers/)
- `/app/frontend/dist/` — frontend production build (baked в image, не редактировать docker cp)
- `/app/Commodity/`, `/app/Funds/`, `/app/OI/`, `/app/Macro/`, `/app/Candles/` — fetch-скрипты
- `/app/main_orchestrator.py` — оркестратор

### GitHub
- **Repo**: `git@github.com:Va3elina/moex-aggregator.git`
- **Branch**: `main` (один — нет dev/staging)
- **Что в git**: source code, `frontend/public/sw.js`, `requirements.txt`, `Dockerfile`, `docker-compose.yml`
- **Что НЕ в git**: `frontend/dist/` (build artifact), `.env*` (секреты), `__pycache__`, `node_modules`, `scripts/landing-videos/`

---

## 📖 Reference quick-cards

### Deploy frontend/backend (АВТО-CI)
```bash
# Это весь деплой. CI собирает и выкатывает сам.
git push origin main
gh run watch   # опционально — следить за прогоном
```

### Аварийный ручной деплой (только если CI лежит)
```bash
gh workflow run deploy-prod   # вариант A

# вариант B — напрямую на сервере (reset --hard, НЕ pull):
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519 \
    root@103.88.243.232 \
  "cd /opt/frame && git fetch && git reset --hard origin/main && \
   docker compose build api && docker compose up -d --force-recreate api"
```

### Health check
```bash
ssh ... "docker exec frame-api-1 python3 -c 'import urllib.request; print(urllib.request.urlopen(\"http://localhost:8000/health\").read().decode())'"
```

### Verify SW version + index hash
```bash
ssh ... "curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep CACHE_NAME | head -1"
ssh ... "curl -sk 'https://localhost/' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -oE 'index-[A-Za-z0-9_-]+\\.js' | head -1"
```

### SQL query
```bash
ssh ... 'docker exec frame-db-1 psql -U postgres -d moex_db -c "SELECT ..."'
```

### Restart api без rebuild (rarely needed)
```bash
ssh ... "docker restart frame-api-1"
```

### Tail recent errors
```bash
ssh ... "docker logs frame-api-1 --tail 50 --since 10m 2>&1 | grep -iE 'error|exception'"
```

---

## 🏛️ Исторический контекст (для AI-помощника)

С марта 2026 до 12 мая 2026 проект использовал **анти-pattern `docker cp` deploys**:
- Frontend строился локально, копировался через `docker cp` в running container
- Backend файлы копировались таким же способом
- Pip-пакеты ставились через `docker exec pip install`

Это работало пока контейнеры не пересоздавались. 12 мая в 18:59 кто-то сделал `docker compose up` — все ephemeral правки за **месяц работы** исчезли, прод откатился к 14 апреля.

**Восстановление** заняло час: git init в `/opt/frame/`, reset к `origin/main`, fix Dockerfile (python3+Pillow+bash в node-alpine), fix requirements.txt (UTF-16 → UTF-8), cleanup ghost files, rebuild image.

**Урок**: deploy должен идти через git + image rebuild. Image — единственный source of truth для production.

С **12 мая** ручной шаг был SSH `git pull + docker compose build + up` (теперь раздел «Аварийный ручной деплой»). С **9 июня 2026** даже этот шаг автоматизирован: `git push` → GitHub Actions build-check → deploy-prod (SSH на прод сам, `git reset --hard origin/main`). Никто не деплоит руками по SSH; SSH остаётся только для логов/SQL/инспекции/аварий.

---

## ⚙️ Инфраструктура надёжности (2026-06-21)

**Postgres-тюнинг (в `docker-compose.yml` db.command, версионируется).** `shared_buffers=1GB, work_mem=24MB, effective_cache_size=2560MB, maintenance_work_mem=256MB` (под 4ГБ-VM). Раньше были дефолты (128MB/4MB) → сортировки лились на диск, холодные страницы. Замер: sort-запрос 737мс→246мс. ⚠️ **Деплой db-сервис НЕ пересоздаёт** (только api/orchestrator) — менял compose db → вручную `cd /opt/frame && docker compose up -d --force-recreate db` (~15с). При апгрейде VM до 8ГБ: shared_buffers=2GB, work_mem=32MB, effective_cache_size=6GB.

**Алерт на падение пайплайна.** `monitor_alert.sh` (репо + `/opt/frame/`, host-cron `*/15 * * * * /bin/bash`): edge-triggered пуш в Telegram при падении/восстановлении пайплайна (state-файл `/opt/frame/logs/monitor_alert_state`, лог `monitor_alert.log`). Источник — `pipeline_runs` (`last_status<>'ok'` ИЛИ молчит >26ч). `health_monitor.py` теперь `overall=fail>stale>ok`. **Шлёт с ХОСТА** (`curl -6`): контейнеры до api.telegram.org НЕ достают (РКН блочит IPv4, проверено: оркестратор→000) — как `backup_db.sh`/`signals`.

**Retry дневного ингеста.** `main_orchestrator.run_script` ретраит ключи `RETRYABLE_DAILY` (дневной каскад 19:10) до 2× backoff 30/120с — транзиентный блип ISS/Algopack не теряет день. 5-мин фетчеры НЕ ретраятся.

**Backup ретраит чанки** (`backup_db.sh`): дамп БД (9.6ГБ→460МБ→10 чанков по 48МБ) шлётся в TG с ретраем каждого чанка (Moscow→TG IPv6 флапает). Cron `0 3 * * *`.

**Cache single-flight** (`api/cache.py:get_or_compute`): heatmap/chart/funds под защитой от cache-stampede. [[monitoring_system]]
