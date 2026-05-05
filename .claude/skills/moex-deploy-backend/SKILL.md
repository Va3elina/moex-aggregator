---
name: moex-deploy-backend
description: Deploy a Python backend file to Фрейм production. Use when user says "задеплой бэк", "обнови X.py на проде", "залей роутер", "update backend", or when any file in api/ folder has been changed and needs to go live. Also triggers when user wants to test API changes on production.
---

# Deploy Backend File to Фрейм Production

Deploy a single Python file (usually a router or module) to the production FastAPI container.

## ⚠️ Critical Rules

1. Always restart container after deploy — FastAPI doesn't auto-reload in production
2. Wait ~5 seconds after restart before testing — FastAPI takes time to initialize routes
3. Check response is JSON, not HTML — if endpoint not registered, nginx serves SPA fallback (HTML)
4. Never edit files directly on server — always `scp` from local, so git stays source of truth
5. Multi-file deploys — upload all files, then restart ONCE at the end (not after each `cp`)
6. Rate-limit на SSH ≤5/60s — chain команды через `&&`, не делай параллельных ssh

## Standard Deploy Sequence (single file)

```bash
# 1. Upload
scp <LOCAL_PROJECT_PATH>/api/routers/{FILE}.py alexgondon@103.88.243.232:/tmp/{FILE}.py

# 2. Copy into container + restart
ssh alexgondon@103.88.243.232 "docker cp /tmp/{FILE}.py frame-api-1:/app/api/routers/{FILE}.py && docker restart frame-api-1"

# 3. Wait
sleep 5
```

## Standard Deploy Sequence (multiple files)

```bash
scp file1.py file2.py file3.py alexgondon@103.88.243.232:/tmp/

ssh alexgondon@103.88.243.232 "docker cp /tmp/file1.py frame-api-1:/app/api/routers/file1.py \
  && docker cp /tmp/file2.py frame-api-1:/app/api/routers/file2.py \
  && docker cp /tmp/file3.py frame-api-1:/app/api/main.py \
  && docker restart frame-api-1"
```

## Verification After Deploy

### Basic health

```bash
ssh alexgondon@103.88.243.232 "docker exec frame-api-1 python3 -c '
import urllib.request
r = urllib.request.urlopen(\"http://localhost:8000/health\")
print(r.read().decode())
'"
```

Expected: `{"status":"ok","database":"ok"}`

### Endpoint validation (JSON not HTML!)

```bash
ssh alexgondon@103.88.243.232 "docker exec frame-api-1 python3 -c '
import urllib.request
r = urllib.request.urlopen(\"http://localhost:8000/api/YOUR_ENDPOINT\")
body = r.read()
if body[:1] in (b\"{\", b\"[\"):
    print(\"OK: endpoint returns JSON\")
else:
    print(\"BROKEN: endpoint returns HTML (SPA fallback)!\")
'"
```

Why this matters: If you delete an endpoint from a router but forget to restart, or if there's an import error in your new file, the endpoint returns SPA HTML — not a 404.

## Project-specific paths

- Server: `alexgondon@103.88.243.232`
- Container: `frame-api-1`
- Container path: `/app/api/`
- Routers directory: `/app/api/routers/`

## Special cases

### Adding a new router

Deploy в этом порядке чтобы избежать import errors:

1. Сначала router файл (`api/routers/new_router.py`)
2. Потом `api/routers/__init__.py` (с import'ом)
3. Потом `api/main.py` (с `include_router`)
4. Restart ONCE в конце

### Removing a router

Reverse order:

1. Deploy `api/main.py` без include
2. Deploy `api/routers/__init__.py` без import
3. Restart
4. Удалить файл: `docker exec frame-api-1 rm -f /app/api/routers/old_router.py`

### Dependency added (new pip package)

```bash
ssh alexgondon@103.88.243.232 "docker exec frame-api-1 pip install PACKAGE && docker restart frame-api-1"
```

⚠️ Эфемерно — package потеряется при следующем full rebuild. Для постоянного: update `requirements.txt` и пересобрать image.

## Known environment variables (в контейнере)

- `DB_URL` = `postgresql+pg8000://postgres:***@db:5432/moex_db` — для SQLAlchemy
- `DB_URL_ASYNC` = `postgresql://postgres:***@db:5432/moex_db` — для asyncpg
- `ALGOPACK_API_KEY` = JWT token для Algopack API

Не хардкодь — всегда `os.environ["DB_URL"]` и т.д.
