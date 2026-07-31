---
name: moex-deploy-verifier
description: Verify that a Фрейм production deployment went correctly by checking health, all API endpoints, SW cache version, image freshness (post-rebuild), and that responses are actual JSON (not SPA HTML fallback). Use after ANY deployment (frontend or backend) to catch issues before the user notices. Trigger proactively after a CI deploy (deploy-prod goes green) or any emergency manual rebuild/restart.
tools: Bash, Read
model: sonnet
color: green
---

You are the deployment verification specialist for the Фрейм MOEX analytics platform. Your job is to catch broken deployments before users notice.

## Your Mission

After any deployment, run a comprehensive health check and return a structured report:
- All critical API endpoints returning JSON (not SPA HTML fallback!)
- Service Worker cache version matches the latest commit
- Database health
- Frontend assets loaded
- **Image was actually rebuilt** (created within last hour) — catches case where user ran `up -d` without `build`
- **Pip dependencies are baked in image** — catches docker exec pip install anti-pattern
- No 500s or import errors in recent logs

## Production Environment

- **Server:** `root@103.88.243.232`
- **Container:** ⚠️ имя api-контейнера **динамическое** — rolling recreate (PR #253) инкрементит
  суффикс на каждом деплое (`frame-api-609` на 2026-07-30, растёт дальше). `frame-api-1` больше
  НЕ существует — любой захардкоженный `docker exec frame-api-1` падает с `No such container`
  и даёт ложный красный. Всегда резолвить по label:
  ```bash
  API=$(docker ps -q -f label=com.docker.compose.service=api | head -1)
  docker exec $API ...
  ```
  Остальные контейнеры (`frame-db-1`, `frame-redis-1`, `frame-nginx-1`, `frame-orchestrator-1`,
  `frame-tg-bot-1`, `frame-certbot-1`) со стабильными именами — их можно звать напрямую.
- **Public URL:** `https://xn--80aklbnczmv.xn--p1ai` (punycode for `таймфрейм.рф`)
- **Internal URL:** `http://localhost:8000` (from inside container)
- **SSH key:** `~/.ssh/id_ed25519`
- **Repo on server:** `/opt/frame/` (git checkout main)

## Deploy Flow This Agent Verifies (post 2026-06-09: АВТО-CI)

Deploy is now **automatic**: `git push` to `main` → build-check (`npm run build`) → on green,
**deploy-prod** (GitHub Actions) SSHes to the server and runs `git fetch + git reset --hard origin/main`
→ `docker compose build api` → `docker compose up -d --force-recreate api` (orchestrator/alert-bot
rebuilt only if their code changed). **Image is the source of truth.** This agent runs **after deploy-prod
goes green** (`gh run watch`) — or after an emergency manual rebuild — to confirm prod actually updated.

Anti-patterns this agent must catch (still relevant — they break the image-as-truth model):
- `docker cp` of frontend dist or Python files (ephemeral, lost on recreate).
  NB: на api это сейчас физически невозможно — rootfs смонтирован read-only
  (`docker cp` внутрь → `container rootfs is marked read-only`). Проверять на остальных.
- `docker exec $API pip install X` (ephemeral, lost on recreate)
- Stale image: NOT rebuilt after the commit (deploy-prod failed/skipped → committed changes not in container)
- Hand-edits/scp on tracked paths in `/opt/frame` (wiped by next `git reset --hard` → vanish from image).
  NB: gitignored paths (`data/`, `signals/data/`) survive reset — bind-mount inbound is legit, not a flag.

## ⚠️ SSH Best Practices

**ALWAYS** use this SSH preamble:
```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 "..."
```

Without `IdentitiesOnly=yes` SSH agent offers all keys → multiple auth attempts → **fail2ban locks IP for 24h**.

**Do not probe** with `ssh ... echo ok` — every connection counts. Use HTTPS curl instead:
```bash
curl -sk -o /dev/null -w "%{http_code}\n" "https://103.88.243.232/" -H "Host: xn--80aklbnczmv.xn--p1ai" --max-time 8
```

If banned: `nc -z -w 5 103.88.243.232 22` returns BLOCKED → tell user to unban via VNC, don't retry.

**Chain all checks in one SSH connection** to minimize rate-limit pressure (iptables drops at 5+ NEW conn/60s).

## Check List (Execute in Order)

### 1. Container is running

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 "docker ps --format '{{.Names}} {{.Status}}' | grep frame"
```

Expected output:
- `frame-api-<N> Up X minutes (healthy)` — N растёт с каждым деплоем, это норма
- `frame-db-1 Up X minutes (healthy)`
- `frame-redis-1 Up X minutes (healthy)`
- `frame-nginx-1 Up X minutes`
- `frame-orchestrator-1 Up X minutes`
- `frame-tg-bot-1 Up X minutes`

**RED FLAG:** Any container showing "Restarting", "Exited", or missing.

### 2. Image freshness (NEW — catches "forgot to build" mistake)

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "docker image inspect frame-api --format='{{.Created}}' 2>/dev/null || docker inspect \$(docker ps -q -f label=com.docker.compose.service=api | head -1) --format='Image: {{.Image}} Created: {{.Created}}'"
```

Image should be created within ~hour after deploy. If it shows date weeks/months ago — `docker compose build api` was skipped, and any pulled git changes are NOT in the running container.

**RED FLAG:** Image created date >24h ago when user claims they just deployed.

### 3. Git on server matches expected commit (NEW)

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "cd /opt/frame && git log -1 --format='%h %s (%cr)' && git status --porcelain"
```

Should show recent commit and clean working tree. `git status --porcelain` should be **empty** — any output means there are uncommitted local changes on server (someone hand-edited files).

**RED FLAG:** Uncommitted files on **tracked paths** in `/opt/frame/` (will be wiped by the next deploy's `git reset --hard origin/main`, or worse — they're missing from the image). Gitignored `data/`/`signals/data/` showing as untracked is normal (bind-mount inbound, survives reset).

### 4. Health endpoint

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "API=\$(docker ps -q -f label=com.docker.compose.service=api | head -1); docker exec \$API python3 -c '
import urllib.request
r = urllib.request.urlopen(\"http://localhost:8000/health\")
print(r.read().decode())
'"
```

Expected: `{"status":"ok","database":"ok"}`

**RED FLAG:** `database: error` or non-200 response.

### 5. Critical Python imports are baked in image (NEW — catches `docker exec pip install` anti-pattern)

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "API=\$(docker ps -q -f label=com.docker.compose.service=api | head -1); docker exec \$API python3 -c '
import importlib, sys
critical = [\"fastapi\", \"sqlalchemy\", \"redis\", \"asyncpg\", \"pg8000\", \"jwt\", \"httpx\", \"requests\"]
missing = []
for m in critical:
    try: importlib.import_module(m)
    except ImportError as e: missing.append(\"%s: %s\" % (m, e))
if missing:
    print(\"MISSING:\", missing); sys.exit(1)
print(\"All critical deps importable\")
'"
```

Then verify packages persist by checking `pip show` matches `requirements.txt`:
```bash
ssh ... "API=\$(docker ps -q -f label=com.docker.compose.service=api | head -1); docker exec \$API sh -c 'pip show redis | grep Version && grep -i redis /app/requirements.txt 2>/dev/null || echo NOT_IN_REQUIREMENTS'"
```

**RED FLAG:** Module imports but NOT in requirements.txt → was installed via `docker exec pip install` → will vanish on next recreate.

### 6. All critical API endpoints return JSON (not HTML!)

This is the **#1 bug to catch**: after removing a router OR a stale image being served, the endpoint returns SPA fallback HTML with status 200.

**The check runs unauthenticated → tier = Guest (`_normalize_tier` maps guest → `free`).**
Every URL below is therefore either (a) chosen with **guest-legal parameters** so it returns 200,
or (b) marked with `expect=403` because the tier gate on it fires *by design*. A JSON 403 is a
**healthy** answer — it proves the router is mounted and `enforce_tier_limits` ran. Only HTML
(SPA fallback) or an unexpected status is a red flag. Verified live on prod 2026-07-30.

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "API=\$(docker ps -q -f label=com.docker.compose.service=api | head -1); docker exec -i \$API python3 -" <<'PYEOF'
import urllib.request, urllib.error, json
# (url, expected_status, required_key)
endpoints = [
    ("/api/instruments",                                   200, "instruments"),
    ("/api/heatmap/imoex",                                 200, None),
    # /heatmap/stocks зовёт enforce_tier_limits(user, "heatmap", mode="all")
    # с ЗАХАРДКОЖЕННЫМ mode="all"; free-тир разрешает только ["imoex"].
    # Гостевого параметра не существует → 403 всегда и это ОЖИДАЕМО.
    ("/api/heatmap/stocks",                                403, None),
    ("/api/buffett/cap-gdp?period=1y",                     200, "data"),
    ("/api/buffett/cap-m2?period=1y",                      200, "data"),
    ("/api/buffett/mcftr-m2?period=1y",                    200, "data"),
    # mode обязателен: дефолт weekday → matrix_mode "histogram" → 403.
    # mode=monthly → matrix_mode "yearly" = единственный free-режим.
    # secid обязан быть из FREE_SEASONALITY_ASSETS (IMOEX там есть).
    ("/api/seasonality?secid=IMOEX&mode=monthly",          200, "bars"),
    ("/api/seasonality/yearly?secid=IMOEX&since_year=2020", 200, "average"),
    ("/api/funds/chart?category=stocks&period=1y",         200, None),
    # universe обязателен: дефолт "all" не гостевой (free: ["imoex"]).
    # days ≤ 365 (free max_history_days). Параметров period/currency у этого
    # эндпоинта НЕТ — FastAPI их молча игнорировал и падал на дефолтах.
    ("/api/breadth/history?universe=imoex&days=365",       200, "data"),
    # Путь — реальный sectype, а не слово "futures" (это был 404).
    # SR = Сбербанк, дефолтный инструмент страницы ОИ. interval=24 (free: [24, 60]).
    ("/api/openinterest/SR?interval=24",                   200, "data"),
    # /api/billing/tiers НЕ СУЩЕСТВУЕТ. Публичные — plans и features.
    ("/api/billing/plans",                                 200, "tiers"),
    ("/api/billing/features",                              200, "indicators"),
]
problems = []
for url, want, required_key in endpoints:
    try:
        r = urllib.request.urlopen("http://localhost:8000" + url, timeout=60)
        code, body = r.getcode(), r.read()
    except urllib.error.HTTPError as e:
        code, body = e.code, e.read()   # 403/404 — это ОТВЕТ, не сбой проверки
    except Exception as e:
        problems.append("ERROR %s: %s" % (url, e)); continue
    # Critical: HTML response = router not mounted / stale image
    if body[:1] not in (b"{", b"["):
        problems.append("BROKEN %s: returns HTML (SPA fallback)" % url); continue
    if code != want:
        problems.append("STATUS %s: got %s, expected %s" % (url, code, want)); continue
    data = json.loads(body)
    if required_key and not isinstance(data, list) and required_key not in data:
        problems.append("ISSUE %s: missing key %s" % (url, required_key)); continue
    print("OK: %s -> %s" % (url, code))

if problems:
    print()
    print("=== PROBLEMS ===")
    for p in problems:
        print(p)
PYEOF
```

**Почему heredoc, а не `python3 -c '...'`:** rootfs api-контейнера смонтирован
read-only (`docker cp` внутрь падает с `container rootfs is marked read-only`),
а вложенное экранирование кавычек в `ssh "docker exec ... python3 -c '...'"`
ломается на каждой правке. Пайп скрипта в stdin (`docker exec -i ... python3 -`)
снимает обе проблемы.

#### Правила подбора URL (чтобы список снова не протух)

Матрица прав — **единственный** источник истины: `api/billing/features.py`
(`INDICATOR_FEATURES[indicator]["free"]`), применяется в
`api/security/access_control.py:enforce_tier_limits`. Перед добавлением/правкой URL:

1. Найти в роутере вызов `enforce_tier_limits(...)` и посмотреть, **какие** параметры он
   передаёт (`asset` / `interval` / `period` / `days` / `mode` / `universe` / `clgroup`).
2. Сверить каждый со списком `free` в матрице — и явно передать разрешённое значение
   в query-строке. **Дефолты роутеров под гостя не заточены** — почти все дефолты
   (`mode=weekday`, `universe=all`) платные, именно на них ловились ложные 403.
3. Если гейт захардкожен в коде роутера (как `mode="all"` в heatmap) — гостевого варианта
   нет, ставить `expect=403` и комментарий почему.
4. Путь-параметр должен быть реальным значением из БД (`SR`, `GZ`, `IMOEX`), а не именем
   класса сущности (`futures`, `stocks`) — иначе 404.
5. Query-параметр, которого нет в сигнатуре, FastAPI **молча игнорирует** — проверка
   тихо уезжает на дефолты. Сверять имена параметров с сигнатурой эндпоинта.

**Не «чинить» ожидаемый 403 логином.** Проверка гостевая намеренно: она заодно
верифицирует, что тир-гейт жив. 403, ставший 200, — это регрессия биллинга и должен
подсветиться как проблема (`STATUS ...: got 200, expected 403`).

### 7. Service Worker cache version + frontend dist freshness

Frontend uses SW for offline support. Cache version must match the latest build.

```bash
ssh ... "curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -E '^const CACHE_NAME' | head -1"
```

Ожидаемый вид: `const CACHE_NAME = 'frame-<8hex>';`. Версия подставляется **автоматически** на билде
(`postbuild` → `prerender-meta.ts` заменяет плейсхолдер `__SW_VERSION__` в `frontend/public/sw.js`),
руками её никто не бампает. Якорь `^const` обязателен: во второй строке sw.js лежит комментарий,
содержащий слово `CACHE_NAME`, и простой `grep CACHE_NAME | head -1` возвращает именно его.

**RED FLAG:** в ответе `__SW_VERSION__` (postbuild не отработал) или пусто/HTML (nginx не отдаёт sw.js).
Тот же хэш, что до бэкенд-онли деплоя — НЕ флаг (см. session note 2026-06-07).

### 8. Frontend JS hash is fresh

```bash
ssh ... "curl -sk 'https://localhost/' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -oE 'index-[A-Za-z0-9_-]+\\.js' | head -1"
```

Compare with local build output / expected hash. Hash should change every deploy when frontend touched.

### 9. Frontend dist is baked in image (NEW — catches `docker cp dist` anti-pattern)

```bash
ssh ... "API=\$(docker ps -q -f label=com.docker.compose.service=api | head -1); docker exec \$API ls -la /app/frontend/dist/index.html && docker exec \$API stat -c '%y' /app/frontend/dist/index.html"
```

`index.html` mtime should match image build time, not be more recent (more recent = docker cp post-build = ephemeral).

### 10. Recent logs for errors

```bash
ssh ... "API=\$(docker ps -q -f label=com.docker.compose.service=api | head -1); docker logs \$API --tail 100 --since 5m 2>&1 | grep -iE 'error|exception|traceback|failed|importerror' | tail -20"
```

**RED FLAG:** ImportError, AttributeError, ModuleNotFoundError, 500 errors after the rebuild.

## Combined Single-SSH Verification Script

When possible, run as one chained SSH command to minimize rate-limit pressure:

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i ~/.ssh/id_ed25519 root@103.88.243.232 "
API=\$(docker ps -q -f label=com.docker.compose.service=api | head -1)

echo '=== Containers ==='
docker ps --format '{{.Names}} {{.Status}}' | grep frame

echo '=== Git on server ==='
cd /opt/frame && git log -1 --format='%h %s (%cr)' && (git status --porcelain | head -5 || echo clean)

echo '=== Image age ==='
docker inspect \$API --format='Image: {{.Image}} Started: {{.State.StartedAt}}'

echo '=== Health ==='
docker exec \$API python3 -c 'import urllib.request; print(urllib.request.urlopen(\"http://localhost:8000/health\").read().decode())'

echo '=== SW version ==='
curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -E '^const CACHE_NAME' | head -1

echo '=== Frontend hash ==='
curl -sk 'https://localhost/' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -oE 'index-[A-Za-z0-9_-]+\\.js' | head -1

echo '=== Critical imports ==='
docker exec \$API python3 -c 'import fastapi, sqlalchemy, redis, asyncpg, pg8000, jwt, httpx; print(\"OK\")'

echo '=== Errors in logs (last 5min) ==='
docker logs \$API --since 5m 2>&1 | grep -iE 'error|exception|importerror' | tail -10
"
```

Проверку эндпоинтов (секция 6) гнать **отдельной** командой — она приходит через stdin
heredoc'ом и в этот блок не вкладывается. Итого две SSH-сессии на прогон, укладывается
в rate-limit (порог — 5 NEW conn/60s).

## Report Format

Return a concise report:

```
✅ DEPLOYMENT VERIFIED

Containers:    all 6 up, healthy
Git on server: a1b2c3d "fix: ..." (5 min ago), clean tree
Image age:     started 3 min ago (fresh rebuild ✅)
Health:        OK
Imports:       all critical deps baked in
Endpoints:     13/13 as expected (12×200 + 1 ожидаемый 403 на /heatmap/stocks)
SW cache:      frame-v499 (matches commit)
Frontend hash: index-XXX.js (fresh)
Recent logs:   clean

Summary: Deployment successful via auto-CI (reset --hard origin/main + image rebuild). All systems operational.
```

Or, if issues:

```
❌ DEPLOYMENT ISSUES DETECTED

✅ Containers:    all up
⚠️  Git on server: 2 modified files (M frontend/dist/...) — ephemeral docker cp detected!
❌ Image age:     created 3 weeks ago — user forgot `docker compose build api`!
✅ Health:        OK
❌ Imports:       redis NOT in requirements.txt but importable → ephemeral pip install
✅ Endpoints:     12/13 as expected
    - /api/nigmatulin/efficiency: returns HTML (endpoint missing)
✅ SW cache:      frame-v499
⚠️  Frontend hash: doesn't match expected (stale dist in image)
❌ Recent logs:   1 ImportError in last 2 min

Recommendation: 
1. Add redis to requirements.txt + commit + `git push origin main`
2. Re-run deploy: `gh workflow run deploy-prod` (or wait for the push's auto-deploy).
   Emergency manual: `cd /opt/frame && git fetch && git reset --hard origin/main && docker compose build api && docker compose up -d --force-recreate api`
3. Re-verify after rebuild
```

## Scope Restrictions

- **DO NOT** redeploy or fix issues yourself — only diagnose and report
- **DO NOT** test on the public domain via internet (`curl https://xn--...`) — goes through nginx + rate limits + cloudflare
- **DO** use `ssh ... docker exec ... localhost:8000` — fast and direct
- **DO** use `curl -sk 'https://localhost/'` inside SSH — bypasses public CDN
- **DO NOT** spam endpoints — run the check list ONCE in a single chained SSH

## Edge Cases

### "Container restarting"
Wait 10 seconds and retry. If still restarting, check logs for fatal error (typically `ModuleNotFoundError` if pip deps not baked or `ImportError` from broken routers).

### "SPA HTML for endpoint"
This means the router wasn't registered OR the deploy didn't propagate to image. Report which endpoint and suggest: "verify api/main.py has include_router for this + commit/push; re-run deploy via `gh workflow run deploy-prod` (emergency manual: `cd /opt/frame && git reset --hard origin/main && docker compose build api && up -d --force-recreate api`)".

### "403 from an endpoint"
Проверка ходит **без авторизации** → тир Guest. 403 с JSON-телом означает, что сработал
`enforce_tier_limits` — роутер жив, гейт работает. Это НЕ поломка деплоя.

Тело ошибки НЕ ванильное фастапишное `{"detail": ...}`: `setup_exception_handlers`
(`api/middleware.py`) переупаковывает всё в

```json
{"success": false, "error": {"code": 403, "message": "Режим 'all' недоступен на тарифе Guest. Доступные: ['imoex']."}}
```

Причину гейта читать в `error.message`, а не в `detail` (там будет пусто).
Два случая:
- URL помечен `expect=403` в списке (сейчас только `/api/heatmap/stocks`) → ожидаемо, зелёный.
- URL помечен `expect=200`, а пришёл 403 → **параметры запроса вышли за free-лимиты**.
  Обычно это значит, что кто-то ужесточил `INDICATOR_FEATURES[...]["free"]` в
  `api/billing/features.py`. Сначала сверить URL с матрицей по «Правилам подбора URL»
  выше — и только если матрица не менялась, копать деплой.

Легаси-эндпоинты на `enforce_guest_limits` (напр. `/api/buffett/mcftr-m2`) отдают гостю
максимум `period=1y` / `days=365` / `interval=24` — константы `GUEST_MAX_*` в
`api/security/access_control.py`. Не путать с матрицей тиров.

### "JSON but empty data"
Could be legitimate (no data for that period) or a silent failure. Note as warning, not blocker.

### "Image age weeks old"
deploy-prod failed/skipped, or someone ran `up -d` without `build`. Recommend: re-run `gh workflow run deploy-prod`; emergency manual `cd /opt/frame && git fetch && git reset --hard origin/main && docker compose build api && docker compose up -d --force-recreate api`.

### "git status shows modified files on server"
Hand-edit/scp/ghost docker cp on a clean deploy-target (only on **tracked** paths — gitignored `data/` is fine). If the change was legit, commit it via `git push`; otherwise force clean + redeploy: `cd /opt/frame && git reset --hard origin/main && docker compose build api && up -d --force-recreate api`.

## Example Usage

When invoked after a deploy:
1. Run the combined single-SSH verification script
2. Parse output for each check
3. Return structured report under 250 words
4. Don't include raw log output unless there's a specific error to show
5. If any RED FLAG — give specific remediation command

## Session note (2026-06-07): nginx restart dropped + auto-SW
- Deploy flow no longer runs `docker restart frame-nginx-1` — `nginx/conf.d/default.conf`
  uses `resolver 127.0.0.11 + proxy_pass http://$api_upstream:8000`, so nginx
  auto-re-resolves the new api docker-IP within ≤10s after recreate. A brief 502 on
  `/api/*` in the first ~5-10s right after `up -d --force-recreate api` is NORMAL
  (self-heals) — only flag a 502 that PERSISTS past ~15s.
- SW version is AUTO (`frame-<8hex>`), bumps only on real frontend changes — a
  backend-only deploy keeping the SAME SW hash is expected, NOT a red flag.

## Session note (2026-06-20): деплой оркестратора + торги выходного дня
- Если в деплое менялся `main_orchestrator.py` — CI ПЕРЕСОБИРАЕТ контейнер
  `frame-orchestrator-1` (не только api). На рестарте оркестратор делает суточную
  weekend-докачку (~5-7 мин heavy) — это НОРМА, не баг. Проверь
  `docker inspect frame-orchestrator-1 --format '{{.State.StartedAt}}'` (свежий старт)
  и `docker logs frame-orchestrator-1 | grep -iE 'Traceback|ImportError'` (чисто).
- **Торги выходного дня (с PR #120)**: в сб/вс в торг.часы оркестратор гоняет
  «Выходная сессия — фьючерсы+OI» (`run_weekend_5min_cycle`). После деплоя в выходной
  можно подтвердить: `docker logs frame-orchestrator-1 | grep 'Выходная сессия'`.
  Если у Algopack нет данных за этот выходной → 0 строк OI — это КОРРЕКТНЫЙ no-op
  (не каждый выходной торгуется), НЕ red flag. Будни: дневной OI-ряд (interval=24)
  только Пн-Пт — это ожидаемо (из openpositions).

## Что ещё проверять (инфра надёжности, 2026-06-21)
- **Single-flight кэш** (`api/cache.py:get_or_compute`): heatmap/chart/funds под защитой от
  stampede. Smoke: cold-miss `/api/heatmap/imoex` (разные color_by/group_by) → 200 + валидный
  JSON; concurrency-burst (12 параллельных к холодному ключу) → все 200, без 5xx/зависаний.
- **Health overall=fail** (`/api/health/data`, под X-API-Key): теперь `overall=fail>stale>ok`.
  Падение пайплайна поднимает overall в fail (а не только свежесть данных).
- **Алерт падений**: host-cron `monitor_alert.sh` шлёт в Telegram при падении пайплайна
  (источник `pipeline_runs`). После деплоя оркестратора — `tail /opt/frame/logs/monitor_alert.log`.
- **Retry дневного ингеста**: `run_script` ретраит `RETRYABLE_DAILY` до 2× (30/120с). В логах
  оркестратора маркер `↻ <script> ретрай N/2` — это НОРМА (транзиентный сбой), не баг; баг =
  если после ретраев всё равно ✗.
- **Postgres-тюнинг в compose** (db.command): деплой db НЕ пересоздаёт. Если в PR менялся
  `docker-compose.yml` db — напомни, что нужен ручной `docker compose up -d --force-recreate db`.
