---
name: moex-server-access
description: Безопасная работа с production-сервером Фрейм (таймфрейм.рф) через SSH — деплой кода (frontend/backend) через git pull + image rebuild, выполнение команд в контейнерах, чтение логов, БД-запросы. Соблюдает rate-limit и fail2ban защиту. Триггер когда пользователь говорит «задеплой на сервер», «обнови сайт», «посмотри логи прода», «выполни SQL на проде», «restart api», «check production», «прод не работает».
---

# MOEX Server Access

Безопасная работа с сервером Фрейм для AI-помощников.

**Главное правило (с 2026-05-12)**: деплой = **git pull на сервере + `docker compose build` + `docker compose up -d`**. НИКАКИХ `docker cp` в живой контейнер — они эфемерные и теряются при recreate.

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

## 🚀 Главный deploy flow (с 12 мая 2026)

### Шаг 1: Локально — commit + push

```bash
# Bump SW version если frontend меняли
# Edit frontend/public/sw.js: const CACHE_NAME = 'frame-vNNN+1';

git add <files>
git commit -m "scope: description"
git push origin main
```

### Шаг 2: На сервере — pull + rebuild + recreate

**Одной SSH командой**:
```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "cd /opt/frame && git pull origin main && \
   docker compose build api && \
   docker compose up -d api && \
   sleep 10 && \
   docker exec frame-api-1 python3 -c 'import urllib.request; print(urllib.request.urlopen(\"http://localhost:8000/health\").read().decode())' && \
   curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep CACHE_NAME | head -1 && \
   curl -sk 'https://localhost/' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -oE 'index-[A-Za-z0-9_-]+\\.js' | head -1"
```

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
Пакет в `requirements.txt`? Если да — нужен `docker compose build api && up -d api`. Если нет — добавить, push, rebuild.

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
Не попадает в git, не попадает в image — потеряется. Всегда правки в git → push → rebuild.

### ❌ `docker compose down` без согласования
Контейнеры удаляются вместе с ephemeral state (если он был). Сейчас не страшно (всё в image), но раньше было катастрофой.

---

## 🗺️ Project structure

### Сервер
- **IP**: `103.88.243.232`
- **Домен**: `xn--80aklbnczmv.xn--p1ai` (punycode для `таймфрейм.рф`)
- **Repo path**: `/opt/frame/` — git checkout main (синхронизирован с GitHub)
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

### Deploy frontend/backend (full pipeline)
```bash
# Local
git push origin main

# Server (одна SSH)
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519 \
    root@103.88.243.232 \
  "cd /opt/frame && git pull && docker compose build api && docker compose up -d api"
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

**Урок**: deploy должен идти через git + image rebuild. Image — единственный source of truth для production. Делай всегда так.
