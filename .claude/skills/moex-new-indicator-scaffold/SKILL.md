---
name: moex-new-indicator-scaffold
description: Create a new indicator in Фрейм following the project's established patterns. Use when user says "сделай новый индикатор", "добавь индикатор X", "создай индикатор как Buffett", "add new indicator", or when a new analytical metric needs to be added to the platform (backend API + frontend page + routing + navigation). Also use when removing an indicator to ensure all pieces are cleaned up.
---

# Create New Indicator — Фрейм Pattern

The project follows a consistent structure across all 8 indicators. This skill captures the exact checklist — miss a step and you get a broken indicator (404, blank page, or import error).

## ⚠️ Critical Rules

1. **13 steps** — all required, none optional. Skipping one = broken indicator.
2. **Register in BOTH `__init__.py` AND `main.py`** — they're separate, easy to forget the second
3. **Use existing patterns** — read `api/routers/buffett.py` and `pages/BuffettPage.tsx` first for the cleanest template
4. **Never skip `enforce_guest_limits`** — security + UX requirement for all indicators

## Full Checklist

### Backend (5 steps)

**Step 1: Create router file `api/routers/{name}.py`**

Template:
```python
"""API роутер для Индикатора X."""

from fastapi import APIRouter, Query, Depends
from sqlalchemy import text
from typing import Literal
from datetime import date, timedelta
import time

from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import get_current_user_optional
from api.security.access_control import enforce_guest_limits

router = APIRouter(prefix="/api/{name}", tags=["{name}"])
log = get_logger()

PeriodType = Literal["1m", "1y", "2y", "3y", "5y", "10y", "20y", "all"]
PERIODS = {"1m": 30, "1y": 365, "2y": 730, "3y": 1095, "5y": 1825, "10y": 3650, "20y": 7300, "all": None}


@router.get("/data")
async def get_data(
    period: PeriodType = Query("3y", description="Период"),
    user=Depends(get_current_user_optional),
):
    enforce_guest_limits(user, period=period)

    start_time = time.time()
    engine = get_engine()
    # ... logic
    
    duration = time.time() - start_time
    log.info(f"GET /{name}/data period={period} -> {len(data_points)} points, {duration:.2f}s")
    return {"data": data_points, "period": period}
```

**Step 2: Register in `api/routers/__init__.py`**

```python
from api.routers.{name} import router as {name}_router

__all__ = [
    # ... existing
    "{name}_router",
]
```

**Step 3: Include in `api/main.py`**

Two edits:
```python
# Add to imports
from api.routers import (
    # ... existing
    {name}_router,
)

# Add to app.include_router calls (near bottom, BEFORE auth)
app.include_router({name}_router)
```

**Step 4: Test endpoint locally if possible, or after deploy**

**Step 5: (Optional) Data loading if new data source**

If indicator needs new data — see `moex-data-*` skills for source-specific parsers.

### Frontend (6 steps)

**Step 6: Add types to `frontend/src/services/api.ts`**

```typescript
// ==================== {NAME} ====================

export interface {Name}Point {
  date: string;
  // ... domain fields
}

export interface {Name}Response {
  data: {Name}Point[];
  period: string;
}

export type {Name}Period = '1m' | '1y' | '2y' | '3y' | '5y' | '10y' | '20y' | 'all';

export async function get{Name}Data(
  period: {Name}Period = '3y'
): Promise<{Name}Response> {
  const params = new URLSearchParams({ period });
  const response = await apiFetch(`${API_BASE}/api/{name}/data?${params}`);
  if (!response.ok) throw new Error('Failed to fetch {Name} data');
  return response.json();
}
```

**Step 7: Create page `frontend/src/pages/{Name}Page.tsx`**

Use `OpenInterestPage.tsx` или `BuffettPage.tsx` как template — оба на editorial design system (после 2026-05).

**Обязательная структура** (см. memory/design_system.md):
```tsx
import { useFitToViewport } from '../hooks/useFitToViewport';
import Dropdown, { type DropdownOption } from '../components/Dropdown';

const chartAnchorRef = useRef<HTMLDivElement>(null);
const chartHeight = useFitToViewport(chartAnchorRef, { min: 360, max: 720, bottomBuffer: 96 });

return (
  <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
    <PageHeader icon={X} title="..." subtitle="..." help={METHODOLOGY.x} helpLink="/methodology/x" />

    <div className="editorial-frame">
      <div className="flex flex-wrap mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
        <Dropdown<Period>
          options={...}
          value={period}
          onChange={setPeriod}
        />
      </div>

      <div ref={chartAnchorRef}>
        <SimpleChart data={...} height={chartHeight} loading={loading} ... />
      </div>
    </div>
  </div>
);
```

**Что НЕ делать:**
- ❌ `<button className="btn-control">` — legacy, использовать `<Dropdown>`
- ❌ `height={450}` хардкод — useFitToViewport
- ❌ `primaryColor="#6366f1"` хардкод — defaults уже theme-aware (`var(--accent)`)
- ❌ `max-w-7xl` — должно быть `max-w-[1408px]`

**Step 8: Register route in `frontend/src/App.tsx`**

Two edits:
```typescript
// Add import
import {Name}Page from './pages/{Name}Page';

// Add Route (inside <Route element={<Layout />}>)
<Route path="/{name}" element={<{Name}Page />} />
```

**Step 9: Add navigation in `frontend/src/components/Layout.tsx`**

```typescript
const NAV_ITEMS = [
  // ... existing
  { path: '/{name}', label: 'Индикатор X' },
];
```

**Step 10: Type-check build**

```bash
cd C:/MOEX/frontend && npm run build
```

Fix any TS errors BEFORE deploying.

### Deploy (push → авто-CI, с 2026-06-09)

**Step 11: Commit + PR** — деплой автоматический, НЕ руками по SSH. Новый
индикатор трогает `api/` + `frontend/` (нетривиально/рисково) → **дефолт ветка+PR**,
не прямой push в `main` (см. `CONTRIBUTING.md`):
```bash
git checkout -b feat/<indicator>
git add <конкретные файлы>            # НЕ `git add -A` вслепую
git commit -m "feat(<scope>): <indicator>"
git push -u origin feat/<indicator>
gh pr create --base main --fill       # build-check прогонится на PR
gh pr merge --auto --squash --delete-branch  # авто-мёрж когда build-check позеленеет = деплой
```
build-check (`npm run build`) → если зелёный → **deploy-prod** сам по SSH:
`git reset --hard origin/main` → `docker compose build api` (api **запекает** frontend dist
И Python-код) → recreate api. orchestrator пересобирается, если менялся его код — твой
новый fetch-скрипт в `OI/`/`Funds/`/`Macro/`/`Commodity/`/`Candles/` попадёт под grep и
триггернёт rebuild orchestrator автоматически. SW-версию бампить НЕ нужно (postbuild
подставляет хэш). Следи за деплоем: `gh run watch` / `gh run list`.
(детали и аварийный ручной путь — skills `moex-deploy-backend` / `moex-git-workflow`)

**Step 12: Verify on production**

```bash
# Check endpoint returns JSON
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519 root@103.88.243.232 "docker exec frame-api-1 python3 -c '
import urllib.request
r = urllib.request.urlopen(\"http://localhost:8000/api/{name}/data?period=1y\")
print(r.read()[:200])
'"
```

Visit `https://таймфрейм.рф/{name}` in browser and confirm page loads + data displays.

## Removal Checklist (Reverse)

If user wants to REMOVE an indicator:

1. **First: check data reuse!** Grep the codebase for `{NAME}_DATA_TYPE` indicators before deleting DB data. Some data (like `GDP_QUARTERLY`) is shared across indicators.

2. Delete files:
   - `api/routers/{name}.py`
   - `frontend/src/pages/{Name}Page.tsx`
   - Scripts in `scripts/load_{name}_data.py` if any

3. Remove references:
   - `api/routers/__init__.py` — import + `__all__`
   - `api/main.py` — import + `include_router`
   - `frontend/src/App.tsx` — import + Route
   - `frontend/src/components/Layout.tsx` — nav item
   - `frontend/src/services/api.ts` — types + functions

4. Delete DB data (ONLY if not used elsewhere):
```python
conn.execute(text("DELETE FROM macro_data WHERE indicator = :i"), {"i": "XXX"})
conn.execute(text("DELETE FROM macro WHERE indicator = :i"), {"i": "XXX"})
```

5. Deploy + verify endpoint returns SPA HTML (not JSON) — confirms removal

6. Grep the whole project for remaining references:
```bash
grep -r "{Name}\|{name}" C:/MOEX \
  --include="*.py" --include="*.ts" --include="*.tsx"
```

## Existing Indicators (reference templates)

| Indicator | Backend file | Frontend page | Data source |
|-----------|--------------|---------------|-------------|
| Heatmap | `heatmap.py` | `HeatmapPage.tsx` | ISS + Algopack |
| Open Interest | `open_interest.py` | `OpenInterestPage.tsx` | MOEX OI tables |
| Funds Money | `funds.py` | `FundsMoneyPage.tsx` | Cbonds + internal |
| Strength | `breadth.py` | `StrengthPage.tsx` | `breadth_history` |
| Buffett | `buffett.py` | `BuffettPage.tsx` | `macro_data` (GDP, M2, Cap) |
| Seasonality | `seasonality.py` | `SeasonalityPage.tsx` | `index_data` + `candles` |
| Fear Index | (in chart.py) | `FearIndexPage.tsx` | IMOEX volatility |
| Funds Catalog | `funds.py` | `FundsCatalogPage.tsx` | Same as Funds Money |

**Cleanest templates for new indicators:**
- **Simple (one chart):** BuffettPage.tsx
- **Multi-chart:** NigmatulinPage.tsx (removed but was a good example)
- **With controls/modes:** SeasonalityPage.tsx
- **Heavy data:** FundsMoneyPage.tsx (with table + chart + events)

## Common Mistakes

- **Forgot `include_router` in main.py** → endpoint returns SPA HTML (not 404!)
- **Wrong URL in apiFetch** → check `${API_BASE}/api/{name}` pattern
- **Missing `enforce_guest_limits`** → security hole, guests can access premium periods
- **No `setPeriod('1y')` fallback in catch block** → guest hitting 403 gets stuck
- **TypeScript PeriodType mismatch between api.ts and page** → build fails

## Кэширование тяжёлых эндпоинтов (2026-06-21)

Если новый эндпоинт делает дорогой расчёт (агрегаты/window-функции/много строк) —
кэшируй через **`get_or_compute`** (single-flight), а НЕ голый `get_or_set`:

```python
from api.cache import get_or_compute

@router.get("/my-indicator")
def get_my_indicator(param: str, user=Depends(get_current_user_optional)):
    enforce_guest_limits(...)              # tier-логика ДО кэша (per-user, не кэшируется!)
    cache_key = f"my_indicator:{param}"
    return get_or_compute(cache_key, lambda: _compute(param), ttl=300)

def _compute(param):                       # тяжёлый расчёт вынесен отдельно
    ...
    return result                          # non-None
```

Почему: при истечении ключа `get_or_set` даёт **cache-stampede** — все воркеры считают
разом (давало 502-штормы). `get_or_compute` = первый считает, остальные ждут результат
(Redis-лок, fail-open). Паттерн уже в heatmap/chart/funds. Tier-проверки ОСТАВЛЯЙ на роуте
до single-flight (иначе у Free/Paid общий кэш-ключ → утечка). [[recent_changes]]
