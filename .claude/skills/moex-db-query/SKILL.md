---
name: moex-db-query
description: Query the Фрейм production PostgreSQL database. Use when user asks to "покажи данные", "сколько строк в X", "что в таблице Y", "проверь БД", "посмотри данные по IMOEX", or any request that requires reading/inspecting data from the production database. Also use when data loading scripts need to be run or SQL queries need to be executed via the production container.
---

# Query Фрейм Production Database

The DB is accessed **only through SSH + docker exec** — there's no direct network port exposed. This skill covers the correct escaping, schema knowledge, and common patterns.

## ⚠️ Critical Rules

1. **Triple-level escaping hell**: you're running `python3 -c` inside `docker exec` inside `ssh`. Quotes are painful.
2. **ALWAYS use `os.environ["DB_URL"]`** — never hardcode credentials
3. **Use `%s` formatting, NOT f-strings** with quotes — f-strings break on nested quotes
4. **Use `chr(X)` for tricky characters** — e.g. `chr(100)+chr(97)+chr(116)+chr(97)` instead of `"data"` inside an f-string
5. **Don't use `print(f"...{dict['key']}...")`** — breaks on Python 3.11 (f-string can't contain `[` inside quotes)

## Standard Query Template

```bash
ssh root@103.88.243.232 "docker exec frame-api-1 python3 -c '
import os
from sqlalchemy import create_engine, text

engine = create_engine(os.environ[\"DB_URL\"])
with engine.connect() as conn:
    rows = conn.execute(text(
        \"SELECT ... FROM ... WHERE col = :param\"
    ), {\"param\": \"value\"}).fetchall()
    
    for r in rows:
        print(r)
'"
```

Key escaping rules:
- Outer `"..."` for ssh arg
- Inside, escape `"` as `\"` to keep Python string syntax
- Single quotes `'...'` for `python3 -c` arg
- SQL itself uses `"..."` (escaped to `\"...\"`)
- Python dict keys use `\"...\"` escaped

## Database Schema (verified, don't trust memory!)

### `index_data` — Historical index/currency closes (daily)
```
trade_date   DATE          -- NOT `date`!
open         NUMERIC
high         NUMERIC
low          NUMERIC
close        NUMERIC        -- NOT `close_price`!
value        NUMERIC
capitalization NUMERIC
secid        VARCHAR        -- "IMOEX", "USD000UTSTOM", "GLDRUB_TOM" etc.
```
Known secids: `IMOEX`, `RTSI`, `MCFTR`, `RGBI`, `RGBITR`, `RVI`, `USD000UTSTOM`, `EUR_RUB__TOM`, `CNYRUB_TOM`, `GLDRUB_TOM`, `RUSFAR3M`

### `candles` — Intraday candles for stocks/futures
```
begin_time   TIMESTAMP      -- NOT `trade_date`!
end_time     TIMESTAMP
open, high, low, close  NUMERIC
value, volume   NUMERIC
interval     INTEGER        -- 1, 10, 60, 24 (minutes) — 24 means daily
type         VARCHAR        -- "stock" or "futures"
secid        VARCHAR        -- "SBER", "USDRUBF", etc.
sec_id       VARCHAR        -- duplicate, use `secid`
```

### `macro_data` — Quarterly/monthly macroeconomic indicators
```
indicator    VARCHAR        -- "GDP_QUARTERLY", "M2_MONTHLY", "MARKET_CAP_TOTAL"
period_date  DATE
value        NUMERIC
source       VARCHAR        -- data source label
```
Preserved indicators: `GDP_QUARTERLY` (1995+, quarterly), `M2_MONTHLY` (1993+, monthly), `MARKET_CAP_TOTAL` (1997+, daily from SmartLab)

### `macro` — Metadata for macro indicators
```
id           SERIAL
indicator    VARCHAR UNIQUE
name         VARCHAR
frequency    VARCHAR        -- "quarterly", "monthly", "daily"
source       VARCHAR
start_date   DATE
```

### `dividends` — Per-share dividends
```
secid        VARCHAR
ex_date      DATE           -- ex-dividend date
value        NUMERIC        -- rubles per share
```

### `breadth_history` — Pre-computed market breadth
```
date         DATE
ema_period   INTEGER        -- 50, 100, or 200
universe     VARCHAR        -- "all" or "imoex"
currency     VARCHAR        -- "rub" or "usd"
pct_above    NUMERIC        -- percent of stocks above EMA
```

### `instruments` — Tradable instruments registry
```
sec_id       VARCHAR PRIMARY KEY
sectype      VARCHAR
name         VARCHAR
iss_code     VARCHAR
type         VARCHAR
sector       VARCHAR
```

### `fund_data` — Fund NAV timeseries
```
fund_id      INTEGER
date         DATE
nav          NUMERIC        -- net asset value in rubles
```

### `funds` — Fund registry
```
fund_id      SERIAL
ticker       VARCHAR
name         VARCHAR
category     VARCHAR        -- "stocks", "bonds", "gold", "money_market"
subcategory  VARCHAR
uk_id        VARCHAR        -- key in UK_LOGOS mapping
```

### Other tables
- `open_interest` — futures OI data
- `stock_market_cap` — per-stock market cap
- `refresh_tokens` — auth tokens
- `users` — registered users
- `fund_holdings` — fund composition

## Common Query Patterns

### Count rows by indicator
```python
r = conn.execute(text(
    \"SELECT indicator, COUNT(*) FROM macro_data GROUP BY indicator\"
)).fetchall()
for ind, cnt in r:
    print(\"%s: %d\" % (ind, cnt))
```

### Get per-year summary of an index
```python
rows = conn.execute(text(\"\"\"
    SELECT EXTRACT(YEAR FROM trade_date) AS yr,
           MIN(close) AS lo,
           MAX(close) AS hi,
           COUNT(*) AS days
    FROM index_data
    WHERE secid = :s
    GROUP BY yr
    ORDER BY yr
\"\"\"), {\"s\": \"IMOEX\"}).fetchall()
```

### Check coverage for a new indicator
```python
r = conn.execute(text(
    \"SELECT MIN(period_date), MAX(period_date), COUNT(*) \"
    \"FROM macro_data WHERE indicator = :i\"
), {\"i\": \"GDP_QUARTERLY\"}).fetchone()
print(\"Range: %s -> %s, %d rows\" % (r[0], r[1], r[2]))
```

### Upsert pattern (ON CONFLICT)
```python
conn.execute(text(\"\"\"
    INSERT INTO macro_data (indicator, period_date, value, source)
    VALUES (:ind, :pd, :val, :src)
    ON CONFLICT (indicator, period_date) DO UPDATE SET
        value = EXCLUDED.value, source = EXCLUDED.source
\"\"\"), {\"ind\": \"GDP_QUARTERLY\", \"pd\": date(2025, 9, 30), \"val\": 54501.35, \"src\": \"ROSSTAT\"})
conn.commit()  # Don't forget!
```

### When query is too complex for inline python

Create a temporary script on server:

```bash
# Upload script
scp /tmp/my_query.py root@103.88.243.232:/tmp/

# Run inside container
ssh root@103.88.243.232 "docker exec frame-api-1 python3 /tmp/my_query.py"
```

## Known Gotchas

### "column X does not exist"
Check actual column names — schema section above has verified names. Common mistakes:
- `close_price` → actually `close`
- `date` in index_data → actually `trade_date`
- `date` in candles → actually `begin_time`
- `macro_id` → doesn't exist! There's no FK, `macro_data.indicator` is the join key (text, not int)

### f-string syntax error on `{...["key"]...}`
Python 3.11 can't have `[` or `"` inside f-string braces when the f-string is already quoted. Use `.format()` or `%` instead:
```python
# BROKEN on Python 3.11:
print(f\"count: {data[\\\"data\\\"]}\")

# WORKS:
print(\"count: %s\" % data[\"data\"])
```

### "This Connection is closed"
You used `conn` after `with engine.connect() as conn:` block ended. Keep all operations inside the `with`.

### HTTP 403 from API tests
Guest limits enforce — tests via `urllib.request` from container act as guest. Use `_fetch_annual` or similar direct function imports to bypass.

## Bypassing guest limits for testing

```python
# Import the internal function, skip enforce_guest_limits
import sys
sys.path.insert(0, \"/app\")
from api.routers.buffett import _fetch_data_or_whatever
from api.database import get_engine

engine = get_engine()
with engine.connect() as conn:
    data = _fetch_data_or_whatever(conn)
    print(data)
```

---

## ⚙️ Postgres настроен (2026-06-21)

Прод-БД больше НЕ на дефолтах. Тюнинг в `docker-compose.yml` (db.command):
`shared_buffers=512MB, work_mem=24MB, effective_cache_size=2560MB, maintenance_work_mem=256MB`
(под 4ГБ-VM; 512MB а не 1GB — OOM-safety, build api идёт на проде). Поэтому при `EXPLAIN (ANALYZE, BUFFERS)`:
- сортировки до ~24МБ идут в памяти (`quicksort Memory`), а не на диск — раньше при `work_mem=4MB` всё лилось (`external merge Disk`);
- горячие таблицы графиков (~274МБ) кэшируются в `shared_buffers` → меньше `read=` в Buffers.

Менял тюнинг → правь **compose** (не `ALTER SYSTEM`), затем на проде `cd /opt/frame && docker compose up -d --force-recreate db` (~15с рестарт; деплой db НЕ трогает). При апгрейде VM до 8ГБ: shared_buffers=2GB, work_mem=32MB, effective_cache_size=6GB. [[server_constraints]]
