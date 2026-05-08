---
name: moex-deploy-backend
description: Deploy a Python backend file to Фрейм production. Use when user says "задеплой бэк", "обнови X.py на проде", "залей роутер", "update backend", or when any file in api/ folder has been changed and needs to go live. Also triggers when user wants to test API changes on production.
---

# Deploy Backend File to Фрейм Production

Deploy Python files (usually a router or module) to the production FastAPI container.

## ⚠️ Critical Rules

1. **Always restart container after deploy** — FastAPI doesn't auto-reload in production
2. **Wait ~5 seconds after restart** before testing — FastAPI takes time to initialize routes
3. **Check response is JSON, not HTML** — if endpoint not registered, nginx serves SPA fallback
4. **Never edit files directly on server** — always upload from local, git is source of truth
5. **Multi-file deploys** — upload all files, then restart ONCE at the end
6. **Single SSH connection per deploy** — see SSH section in moex-deploy-frontend skill

## ⚠️ SSH Best Practices

See `moex-deploy-frontend/SKILL.md` § "SSH Best Practices" — same rules apply:
- Always use `IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519`
- Use `root@103.88.243.232` (most reliable)
- Single connection per deploy via tar pipe
- Don't probe — use HTTPS curl if you need to check connectivity
- fail2ban bans for 24 hours — ask user to unban via VNC if locked out

## Single-connection deploy (single file)

```bash
tar -cz -C <LOCAL_PROJECT_PATH>/api/routers {FILE}.py | \
  ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
      -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "rm -rf /tmp/router-deploy && mkdir -p /tmp/router-deploy && \
   tar -xz -C /tmp/router-deploy && \
   docker cp /tmp/router-deploy/{FILE}.py frame-api-1:/app/api/routers/{FILE}.py && \
   docker restart frame-api-1 && \
   rm -rf /tmp/router-deploy && \
   sleep 5 && \
   docker exec frame-api-1 python3 -c 'import urllib.request; r=urllib.request.urlopen(\"http://localhost:8000/health\"); print(r.read().decode())'"
```

## Single-connection deploy (multiple files)

```bash
tar -cz -C <LOCAL_PROJECT_PATH>/api file1.py routers/file2.py main.py | \
  ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
      -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "rm -rf /tmp/api-deploy && mkdir -p /tmp/api-deploy && \
   tar -xz -C /tmp/api-deploy && \
   docker cp /tmp/api-deploy/file1.py frame-api-1:/app/api/file1.py && \
   docker cp /tmp/api-deploy/routers/file2.py frame-api-1:/app/api/routers/file2.py && \
   docker cp /tmp/api-deploy/main.py frame-api-1:/app/api/main.py && \
   docker restart frame-api-1 && \
   rm -rf /tmp/api-deploy && \
   sleep 5 && \
   docker exec frame-api-1 python3 -c 'import urllib.request; r=urllib.request.urlopen(\"http://localhost:8000/health\"); print(r.read().decode())'"
```

## Verification After Deploy

Health is auto-checked at end of deploy commands above. For deeper verification call `moex-deploy-verifier` agent.

### Manual endpoint validation (JSON not HTML!)

```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "docker exec frame-api-1 python3 -c '
import urllib.request
r = urllib.request.urlopen(\"http://localhost:8000/api/YOUR_ENDPOINT\")
body = r.read()
if body[:1] in (b\"{\", b\"[\"):
    print(\"OK: endpoint returns JSON\")
else:
    print(\"BROKEN: endpoint returns HTML (SPA fallback)!\")
'"
```

Why this matters: If you delete an endpoint from a router but forget to restart, or there's an import error in your new file, the endpoint returns SPA HTML — not a 404.

## Project-specific paths

- Server: `root@103.88.243.232`
- Container: `frame-api-1`
- Container path: `/app/api/`
- Routers directory: `/app/api/routers/`
- SSH key: `~/.ssh/id_ed25519`

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
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "docker exec frame-api-1 pip install PACKAGE && docker restart frame-api-1"
```

⚠️ Эфемерно — package потеряется при следующем full rebuild. Для постоянного: update `requirements.txt` и пересобрать image.

## Known environment variables (в контейнере)

- `DB_URL` = `postgresql+pg8000://postgres:***@db:5432/moex_db` — для SQLAlchemy
- `DB_URL_ASYNC` = `postgresql://postgres:***@db:5432/moex_db` — для asyncpg
- `ALGOPACK_API_KEY` = JWT token для Algopack API

Не хардкодь — всегда `os.environ["DB_URL"]` и т.д.
