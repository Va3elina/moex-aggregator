---
name: moex-deploy-frontend
description: Deploy the Фрейм frontend to production. Use when user says "задеплой фронт", "обнови сайт", "залей фронтенд", "выложи на прод фронт", "update frontend", or when frontend TypeScript/TSX/CSS files have been changed and need to be deployed to production server. Also use when SimpleChart, pages, or any frontend components are modified.
---

# Deploy Frontend to Фрейм Production

Strict deployment procedure for the Frame analytics platform frontend. Server resources are tight (4GB RAM), and there are specific gotchas that have caused problems before.

## ⚠️ Critical Rules (don't skip!)

1. Build LOCALLY only — the server has 4GB RAM and OOMs if you try `npm run build` there
2. Path in container is `/app/frontend/dist/` — NOT `/app/static/` (static folder is unused by FastAPI)
3. Always bump Service Worker cache version before deploying — otherwise users see cached old version
4. Deploy means frontend is unavailable for 1-2 seconds during `docker cp` — plan accordingly
5. Current SW version: check `sw.js` and bump на единицу при следующем деплое
6. Rate-limit на сервере: ≤5 SSH-подключений за 60 сек. Не делай параллельных SSH calls — chain через `&&` в одной команде

## Standard Deploy Sequence

### Step 1: Bump Service Worker cache version

Edit `<LOCAL_PROJECT_PATH>/frontend/public/sw.js`:

```js
const CACHE_NAME = 'frame-v{NEW_VERSION}';
```

### Step 2: Build locally

```bash
cd <LOCAL_PROJECT_PATH>/frontend && npm run build
```

### Step 3: Upload and copy into container

```bash
ssh alexgondon@103.88.243.232 "mkdir -p /tmp/dist-new && rm -rf /tmp/dist-new/*" \
  && scp -r <LOCAL_PROJECT_PATH>/frontend/dist/. alexgondon@103.88.243.232:/tmp/dist-new/ \
  && ssh alexgondon@103.88.243.232 "docker cp /tmp/dist-new/. frame-api-1:/app/frontend/dist/ && rm -rf /tmp/dist-new"
```

No docker restart needed — frontend is static files served by FastAPI.

### Step 4: Verify deployment

```bash
curl -sk "https://103.88.243.232/sw.js" -H "Host: xn--80aklbnczmv.xn--p1ai" | grep "CACHE_NAME"
curl -sk "https://103.88.243.232/" -H "Host: xn--80aklbnczmv.xn--p1ai" | grep -oE 'index-[A-Za-z0-9_-]+\.js'
```

Hash should match `npm run build` output.

## Common Issues

- **Old frontend served**: forgot to bump `CACHE_NAME` → bump and redeploy
- **TS build fails**: run `tsc -b` locally, fix errors first
- **OOM during build**: never run `npm run build` на сервере — only locally
- **SSH таймауты**: попал в rate-limit (5+/60s) — подожди минуту, не запускай параллельные ssh

## Project-specific paths

- Server: `alexgondon@103.88.243.232`
- Container: `frame-api-1`
- Container path: `/app/frontend/dist/`
- Production domain: `xn--80aklbnczmv.xn--p1ai` (punycode for таймфрейм.рф)
