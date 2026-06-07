---
name: moex-deploy-frontend
description: Deploy the Фрейм frontend to production via proper image rebuild flow. Use when user says "задеплой фронт", "обнови сайт", "залей фронтенд", "выложи на прод фронт", "update frontend", or when frontend TypeScript/TSX/CSS files have been changed and need to be deployed to production server. Also use when SimpleChart, pages, or any frontend components are modified.
---

# Deploy Frontend to Фрейм Production

**Главное правило** (с 2026-05-12 после инцидента «откат к v12»):
**Деплой = `git pull` на сервере → `docker compose build api` → `up -d api`.**
**Никаких `docker cp` в живой контейнер.**

## Почему именно так

Frontend source (`frontend/src/*`, `sw.js`, etc.) — в git. `frontend/dist/` — в `.gitignore` (build artifact). Docker image собирает dist через multi-stage Dockerfile из актуального source.

**Раньше делали** `npm run build` локально + `docker cp dist/. frame-api-1:/app/frontend/dist/`. Это было быстро (~30 сек), но **эфемерно**: при любом `docker compose down/up` контейнер пересобирается из image, dist из docker cp **исчезает**, прод откатывается к state на момент последнего image rebuild.

12 мая 2026: контейнер пересоздался → прод откатился к Apr 14 image (v12). 500+ инкрементов SW (v12 → v535) **жили только в running container**.

**Сейчас**: `docker compose build api` пересобирает image из source, dist baked в image, переживает recreations.

## ⚠️ Critical Rules

1. **Билд на сервере, не на Mac.** Через `docker compose build api` — Dockerfile multi-stage запустит `npm run build` ВНУТРИ image. Источник истины — git.
2. **SW cache version — АВТОМАТИЧЕСКИ** (с ab14d93, 07.06.2026): postbuild (`scripts/prerender-meta.ts`) подставляет `frame-<sha1(имена dist/assets)[:8]>` в плейсхолдер `__SW_VERSION__` в `public/sw.js`. **Руками НЕ бампить.**
3. **Path в container**: `/app/frontend/dist/` — baked в image, не editable через docker cp (точнее можно, но эфемерно).
4. **SSH preamble** см. ниже: `IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519` — иначе fail2ban банит на 24ч.

## Standard Deploy Sequence

### Step 1: Локально — commit + push (SW бампить НЕ нужно, авто на build)

```bash
git add frontend/src/...  # изменённые файлы. sw.js трогать НЕ нужно
git commit -m "fix(scope): описание"
git push origin main
```

### Step 2: На сервере — pull + rebuild + recreate

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "cd /opt/frame && \
   git stash push -m deploy-wip && git pull origin main && git stash pop && \
   docker compose build api && \
   docker compose up -d --force-recreate api && \
   sleep 8 && \
   curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep CACHE_NAME | head -1"
```

**⚠️ Два обязательных шага, которых легко не заметить (грабли 2026-05-28):**

1. **`git stash push … && git pull && git stash pop`** — на сервере почти всегда
   есть незакоммиченный WIP (в `docker-compose.yml` смонтирован volume
   `manual_scha`, плюс `signals/` research, `requirements.txt`, `backup_db.sh`).
   Без stash `git pull` падает: *«Your local changes would be overwritten by merge»*.
   `stash pop` обычно **auto-merge'ит** (мой коммит и WIP — в разных секциях файла);
   если конфликт — резолвить вручную. Подробнее: `feedback_server_wip_sync.md`.
2. **nginx restart БОЛЬШЕ НЕ НУЖЕН** (с c40017f, 07.06.2026). Раньше при recreate
   api получал новый docker-IP, а nginx кешировал старый upstream-IP → **502 на
   всех `/api/*`** пока не `docker restart frame-nginx-1`. Теперь `default.conf`
   использует `resolver 127.0.0.11 valid=10s ipv6=off` + `set $api_upstream api`
   + `proxy_pass http://$api_upstream:8000` → nginx сам ре-резолвит новый IP за
   **≤10с без рестарта** (проверено: recreate api без рестарта → `/health`
   восстановился за ~4с). Рестарт/reload nginx нужен ТОЛЬКО когда меняется САМ
   `nginx/conf.d/default.conf` (тогда: `docker exec frame-nginx-1 nginx -t &&
   docker exec frame-nginx-1 nginx -s reload`, без `docker restart`).

**Время**: build ~1-2 мин с кешем; без кеша ~3-5 мин. Downtime recreate ~30 сек
(nginx сам подхватит новый IP за ≤10с, рестарт не нужен).

### Step 3: Локальный typecheck ПЕРЕД push (важно — иначе деплой упадёт на build!)

Docker-сборка прерывается, если TS не проходит (`tsc -b && vite build`).
Проверяй локально ДО push'а:
```bash
cd frontend && npx tsc -b --force   # честный typecheck, как в Docker (быстрее полного build)
# или полный: npm run build (tsc -b + vite + prebuild sprite + postbuild logos)
```

⚠️ **НЕ используй `npx tsc --noEmit` — это ЛОЖНЫЙ pass (exit 0 на любом коде)!**
Корневой `tsconfig.json` тут — project-references файл (только `references`, без
`files`), поэтому `--noEmit` не компилирует НИЧЕГО. Реальную проверку даёт только
**`tsc -b`** (build mode идёт по ссылкам в `tsconfig.app.json`). Docker гоняет
именно `tsc -b`. Инцидент 2026-05-28: `--noEmit` дал exit 0, а прод-build упал на
ошибке типа-кортежа `[string,string,string,string][]` в PricingPage → пришлось
фиксить и передеплоивать.

## Verify deployment

После Step 2 ассертим:
- SW версия = `frame-<hash>` (авто, хэш от dist/assets) — изменилась при реальной правке фронта
- Frontend hash (`index-XXX.js`) изменился (cache busting)
- `docker exec frame-api-1 python3 -c 'import urllib.request; print(urllib.request.urlopen("http://localhost:8000/health").read().decode())'` → `{"status":"ok","database":"ok"}`

Для подробной проверки: вызови sub-agent `moex-deploy-verifier`.

## Common Issues

### `python3: not found` / `bash: not found` в frontend-build stage
**Причина**: docker `node:20-alpine` минимален. `npm run build` вызывает `python3 ../scripts/build-sprite.py` (prebuild) и bash в postbuild.
**Fix** (уже в Dockerfile с d12c4b7): `RUN apk add --no-cache python3 py3-pip py3-pillow bash`.

### `COPY scripts/ → /scripts not found`
**Причина**: `scripts/` был в `.gitignore` целиком, не попадал на сервер при `git pull`.
**Fix** (уже в gitignore с f3d4408): игнорим только `scripts/landing-videos/` + `scripts/*.flow`, остальное tracked.

### `ModuleNotFoundError: No module named 'redis'` (и т.п.)
**Причина**: pip-deps в running container ephemeral — `docker exec pip install` теряются при recreate.
**Fix**: добавить пакет в `requirements.txt`, commit, push, `docker compose build api && up -d api`.

### `Invalid requirement` при `pip install -r requirements.txt`
**Причина**: `requirements.txt` мог быть в UTF-16 LE (Windows IDE). Pip ждёт ASCII/UTF-8.
**Fix**: перекодировать в UTF-8 без BOM. См. commit 3fb9dbe для примера.

### TS errors на старые удалённые pages (FearIndexPage, etc.)
**Причина**: ghost-файлы остались на `/opt/frame/` от ad-hoc docker cp до того как мы перешли на git pull.
**Fix**: `cd /opt/frame && rm -f frontend/src/pages/FearIndexPage.tsx ...` (см. cleanup в сессии 2026-05-12).

### `Permission denied` / `Too many authentication failures` / `Connection timed out`
**Причина**: SSH agent предлагает все ключи → fail2ban после 3 failed.
**Fix**: ВСЕГДА используй полный preamble `-o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519`. Если случилось — НЕ retry, ждать 24ч или попросить unban через VNC.

## Project-specific paths

- **Server**: `root@103.88.243.232` (key `~/.ssh/id_ed25519`)
- **Repo на сервере**: `/opt/frame/` (git checkout `main`)
- **Container**: `frame-api-1`
- **Container path**: `/app/frontend/dist/` (baked в image, не редактировать docker cp)
- **Production domain**: `xn--80aklbnczmv.xn--p1ai` (punycode for таймфрейм.рф)
- **VNC console** (для unban): https://timeweb.cloud/my/servers/7006331/console

## Anti-pattern: docker cp

❌ **Не делать**:
```bash
tar -cz -C dist . | ssh ... "docker cp /tmp/dist/. frame-api-1:/app/frontend/dist/"
```

Это работает мгновенно но **эфемерно**. При recreate container — потеря. См. инцидент 12 мая 2026.

✅ **Правильно**:
```bash
git push  # local
ssh ... "cd /opt/frame && git pull && docker compose build api && docker compose up -d api"
```
