---
name: moex-deploy-frontend
description: Deploy the Фрейм frontend to production via proper image rebuild flow. Use when user says "задеплой фронт", "обнови сайт", "залей фронтенд", "выложи на прод фронт", "update frontend", or when frontend TypeScript/TSX/CSS files have been changed and need to be deployed to production server. Also use when SimpleChart, pages, or any frontend components are modified.
---

# Deploy Frontend to Фрейм Production

> ## ⚠️ ДЕПЛОЙ ТЕПЕРЬ АВТО-CI (с 2026-06-09)
> **Деплой = `git push` в `main`. ВСЁ.**
> GitHub Actions сам прогоняет `build-check` → (если зелёный) `deploy-prod` по SSH на прод.
> **НЕ деплоить руками по SSH** — CI делает это сам.
> Сервер `/opt/frame` = **чистый deploy-target**, НЕ воркспейс: деплой делает
> `git reset --hard origin/main`, поэтому **любая правка/scp прямо на проде стирается**
> следующим деплоем. Все изменения — только через `git push`.
> Ручной SSH-деплой ниже — **аварийный** (только если CI недоступен).

**Главное правило** (с 2026-06-09): **запушил в `main` → CI задеплоил.**
Триггеры скилла те же — когда юзер говорит «задеплой фронт», ты делаешь
`git push origin main` (после typecheck + commit) и **наблюдаешь за CI**, а не лезешь на сервер.

## Почему именно так

Frontend source (`frontend/src/*`, `sw.js`, etc.) — в git. `frontend/dist/` — в `.gitignore` (build artifact). Docker image собирает dist через multi-stage Dockerfile из актуального source.

**Раньше делали** `npm run build` локально + `docker cp dist/. frame-api-1:/app/frontend/dist/`. Это было быстро (~30 сек), но **эфемерно**: при любом `docker compose down/up` контейнер пересобирается из image, dist из docker cp **исчезает**, прод откатывается к state на момент последнего image rebuild.

12 мая 2026: контейнер пересоздался → прод откатился к Apr 14 image (v12). 500+ инкрементов SW (v12 → v535) **жили только в running container**.

**Сейчас**: image собирается из git-source (dist baked в image, переживает recreations),
а **сборку и выкатку делает CI на сервере** — не Mac, не руки.

### Как работает авто-деплой (с 2026-06-09)

1. `git push origin main` → GitHub Actions запускает **`build-check`** (typecheck + build,
   тот же `tsc -b` + `vite build`, что в Docker). Битый билд = красный гейт.
2. Только если `build-check` **зелёный** → запускается job **`deploy-prod`**: сам ходит
   по SSH на прод, делает `git reset --hard origin/main` + `docker compose build api` +
   recreate. **Битый билд НЕ выкатывается** (deploy ждёт зелёный build-check).
3. **Сериализация через `concurrency: deploy-prod`**: два пуша подряд деплоятся **по очереди**,
   не сталкиваются на сервере.
4. Сервер `/opt/frame` — **чистый target**: `deploy-prod` делает `git reset --hard origin/main`,
   так что **ничего не коммить/scp прямо на проде** — сотрётся. НЕ scp-ить черновики/research
   на `/opt/frame`. Все изменения идут через `git push`.

Детали пайплайна — в памяти: **`ci_cd.md`**, **`deploy_manual.md`**.

## ⚠️ Critical Rules

1. **Деплой = `git push`, CI собирает на сервере.** Build делает GitHub Actions
   (`build-check` → `deploy-prod`), а не Mac и не руки. Источник истины — git.
   Сервер `/opt/frame` — чистый target (`reset --hard`), правки на проде стираются.
2. **SW cache version — АВТОМАТИЧЕСКИ** (с ab14d93, 07.06.2026): postbuild (`scripts/prerender-meta.ts`) подставляет `frame-<sha1(имена dist/assets)[:8]>` в плейсхолдер `__SW_VERSION__` в `public/sw.js`. **Руками НЕ бампить.**
3. **Path в container**: `/app/frontend/dist/` — baked в image, не editable через docker cp (точнее можно, но эфемерно — и сотрётся следующим деплоем).
4. **SSH preamble** (для логов/SQL/инспекции/аварийного деплоя) см. ниже: `IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519` — иначе fail2ban банит на 24ч.

## Standard Deploy Sequence (с 2026-06-09 — авто-CI)

### Step 1: Локальный typecheck ПЕРЕД push (важно — иначе CI build-check упадёт!)

(полные детали по typecheck — секцией ниже). Коротко: `cd frontend && npx tsc -b --force`.

### Step 2: Commit + push (SW бампить НЕ нужно, авто на build)

```bash
git add frontend/src/...  # изменённые файлы. sw.js трогать НЕ нужно
git commit -m "fix(scope): описание"
git push origin main
```

**Всё. Дальше CI сам:** `build-check` → (зелёный) → `deploy-prod` по SSH на прод
(`git reset --hard origin/main` + `docker compose build api` + recreate).
Никаких ручных шагов на сервере.

### Step 3: Наблюдай за CI (вместо ручного SSH-деплоя)

```bash
gh run watch                      # следить за текущим прогоном
gh run list --branch main -L 3    # последние прогоны build-check + deploy-prod
```

- **Красный `build-check`** → деплой НЕ поедет. Чини типы/билд, пушь снова.
- **Зелёный** оба job → прод обновлён. Проверь SW/health (секция «Verify deployment»).
- Два пуша подряд деплоятся **по очереди** (`concurrency: deploy-prod`), не сталкиваются.

---

### 🚑 Аварийный ручной деплой (ТОЛЬКО если CI недоступен)

Обычно НЕ нужно — CI деплоит сам. Используй, только если GitHub Actions лежит/недоступен.

**Вариант A — переспустить деплой через CI вручную:**

```bash
gh workflow run deploy-prod    # форсит deploy-prod на текущем main
```

**Вариант B — напрямую по SSH на сервере** (`reset --hard`, НЕ `git pull`!):

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "cd /opt/frame && \
   git fetch origin && git reset --hard origin/main && \
   docker compose build api && \
   docker compose up -d --force-recreate api && \
   sleep 8 && \
   curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep CACHE_NAME | head -1"
```

**Грабли аварийного деплоя:**

1. **`git reset --hard origin/main`, а НЕ `git pull`.** Сервер — чистый target, не
   воркспейс: на нём нет WIP, который надо сохранять (это и есть смысл нового процесса —
   больше нет stash-плясок из старого `feedback_server_wip_sync.md`). `reset --hard`
   гарантированно приводит `/opt/frame` к состоянию `origin/main`.
2. **orchestrator пересобирать отдельно**, если менялся его код:
   `docker compose build orchestrator && docker compose up -d --force-recreate orchestrator`.
3. **nginx НЕ трогать** (resolver-фикс c40017f, 07.06.2026). При recreate api nginx
   сам ре-резолвит новый docker-IP за ≤10с (`resolver 127.0.0.11 valid=10s ipv6=off`
   + `set $api_upstream api` + `proxy_pass http://$api_upstream:8000`) — **рестарт не
   нужен**. Рестарт/reload nginx нужен ТОЛЬКО при правке самого
   `nginx/conf.d/default.conf` (тогда: `docker exec frame-nginx-1 nginx -t &&
   docker exec frame-nginx-1 nginx -s reload`, без `docker restart`).

**Время**: build ~1-2 мин с кешем; без кеша ~3-5 мин. Downtime recreate ~30 сек
(nginx сам подхватит новый IP за ≤10с, рестарт не нужен).

---

### Про typecheck перед push (важно — иначе CI build-check упадёт!)

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

После того как `deploy-prod` (CI) стал зелёным — ассертим (без захода на сервер можно
проверить SW по HTTP; health/hash — по SSH-преамбуле или через verifier):
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
**Fix**: добавить пакет в `requirements.txt`, commit, `git push` — CI пересоберёт image с новым deps.

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

- **Деплой**: авто через GitHub Actions (`build-check` → `deploy-prod`), триггер — `git push origin main`
- **Server**: `root@103.88.243.232` (key `~/.ssh/id_ed25519`) — SSH нужен для логов / SQL-БД / инспекции / аварийного деплоя
- **Repo на сервере**: `/opt/frame/` — **чистый deploy-target** (`git reset --hard origin/main`), НЕ воркспейс
- **Container**: `frame-api-1` (orchestrator — отдельно, пересобирать если менялся его код)
- **Container path**: `/app/frontend/dist/` (baked в image, не редактировать docker cp)
- **Production domain**: `xn--80aklbnczmv.xn--p1ai` (punycode for таймфрейм.рф)
- **VNC console** (для unban): https://timeweb.cloud/my/servers/7006331/console
- **Память**: детали пайплайна — `ci_cd.md`, `deploy_manual.md`

## Anti-patterns

❌ **docker cp в живой контейнер**:
```bash
tar -cz -C dist . | ssh ... "docker cp /tmp/dist/. frame-api-1:/app/frontend/dist/"
```
Работает мгновенно но **эфемерно**: при recreate container — потеря (инцидент 12 мая 2026).

❌ **Правки/scp/commit прямо на проде** (`/opt/frame`):
сервер — чистый target, `deploy-prod` делает `git reset --hard origin/main` →
**любая локальная правка на сервере стирается** следующим деплоем. НЕ scp-ить
черновики/research на `/opt/frame`.

❌ **Ручной `git pull` на сервере для деплоя**: больше не нужен (CI деплоит сам).
Если уж лезешь вручную в аварийном режиме — это `git reset --hard origin/main`,
НЕ `git pull` (см. «Аварийный ручной деплой»).

✅ **Правильно** — обычный деплой:
```bash
git push origin main   # local → CI сам: build-check → deploy-prod
gh run watch           # наблюдаем за CI
```
