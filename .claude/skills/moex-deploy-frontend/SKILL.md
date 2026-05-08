---
name: moex-deploy-frontend
description: Deploy the Фрейм frontend to production. Use when user says "задеплой фронт", "обнови сайт", "залей фронтенд", "выложи на прод фронт", "update frontend", or when frontend TypeScript/TSX/CSS files have been changed and need to be deployed to production server. Also use when SimpleChart, pages, or any frontend components are modified.
---

# Deploy Frontend to Фрейм Production

Strict deployment procedure for the Frame analytics platform frontend. Server resources are tight (4GB RAM), there is fail2ban with 24-hour ban window, and there are specific gotchas that have caused problems before.

## ⚠️ Critical Rules (don't skip!)

1. **Build LOCALLY only** — server has 4GB RAM and OOMs if you try `npm run build` there
2. **Path in container** is `/app/frontend/dist/` — NOT `/app/static/`
3. **Always bump SW cache version** before deploying — иначе users see cached old version
4. **Single SSH connection per deploy** — see SSH section below (fail2ban triggers on auth failures)
5. **NEVER probe SSH** without `IdentitiesOnly=yes` first — see SSH section
6. **Current SW version**: read `frontend/public/sw.js` and bump на единицу

## ⚠️ SSH Best Practices (LEARNED THE HARD WAY)

### Why this matters
Server has fail2ban configured to **ban for 24 hours** after 3 failed auth attempts. Default SSH client offers ALL keys from your agent + `~/.ssh/` — each key tried = 1 auth attempt. With 3+ keys you get banned in **one command**.

### Always use this SSH preamble
```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -i ~/.ssh/id_ed25519 root@103.88.243.232 "..."
```

- `IdentitiesOnly=yes` — use ONLY `-i` key, ignore agent
- `IdentityAgent=none` — disable SSH agent entirely
- `ConnectTimeout=20` — fail fast if banned (don't hang)
- `-i ~/.ssh/id_ed25519` — explicit key path
- `root@103.88.243.232` — current working user (alexgondon@ may not work depending on key state)

### Don't probe with `ssh ... echo ok`
Each probe is a connection attempt. If you're unsure connection works, use HTTPS curl as a probe (port 443 isn't rate-limited):
```bash
curl -sk -o /dev/null -w "%{http_code}\n" "https://103.88.243.232/" -H "Host: xn--80aklbnczmv.xn--p1ai" --max-time 8
```

### Single-connection deploy (preferred)
Stream tar through SSH stdin — ONE connection for everything:
```bash
cd <LOCAL_PROJECT_PATH>/frontend && tar -cz -C dist . | \
  ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 \
      -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  "rm -rf /tmp/dist-new && mkdir -p /tmp/dist-new && \
   tar -xz -C /tmp/dist-new && \
   docker cp /tmp/dist-new/. frame-api-1:/app/frontend/dist/ && \
   rm -rf /tmp/dist-new && \
   echo '--- SW:' && \
   curl -sk 'https://localhost/sw.js' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep CACHE_NAME && \
   echo '--- Index:' && \
   curl -sk 'https://localhost/' -H 'Host: xn--80aklbnczmv.xn--p1ai' | grep -oE 'index-[A-Za-z0-9_-]+\\.js'"
```

This is **1 SSH connection**, includes upload + container copy + verify.

### If you get fail2ban'd
1. Check first via `nc -z -w 5 103.88.243.232 22` — if BLOCKED, you're banned
2. **Tell the user** — don't keep retrying. User must unban via TimeWeb VNC:
   ```bash
   # On server via VNC console:
   fail2ban-client unban <YOUR_IP>
   # or unban all:
   fail2ban-client status sshd
   fail2ban-client set sshd unbanip <IP>
   ```
3. Default ban duration: **24 hours** (per `server_security.md`)

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

Note the new hash from output (e.g., `index-DHF9n17S.js`) — used for verify step.

### Step 3: Single-connection deploy + verify

Use the tar-pipe command from "SSH Best Practices" section above. Replace nothing — paste verbatim.

Expected output ending:
```
--- SW:
const CACHE_NAME = 'frame-v494';
--- Index:
index-DHF9n17S.js
```

If hash matches local build → deploy successful. No docker restart needed (FastAPI serves static files).

## Common Issues

- **Old frontend served**: forgot to bump `CACHE_NAME` → bump and redeploy
- **TS build fails**: run `tsc -b` locally, fix errors first
- **OOM during build**: never run `npm run build` на сервере — only locally
- **`Too many authentication failures`**: forgot `IdentitiesOnly=yes` → likely fail2ban'd, use HTTPS probe
- **`Connection timeout`**: fail2ban active OR network issue. Verify with `nc -z`. If banned, ask user to unban via VNC

## Project-specific paths

- Server: `root@103.88.243.232` (alexgondon@ also works in theory, but root@ + explicit key is more reliable)
- Container: `frame-api-1`
- Container path: `/app/frontend/dist/`
- Production domain: `xn--80aklbnczmv.xn--p1ai` (punycode for таймфрейм.рф)
- SSH key: `~/.ssh/id_ed25519` (Vadim's Mac mini)
- VNC console (для unban): https://timeweb.cloud/my/servers/7006331/console
