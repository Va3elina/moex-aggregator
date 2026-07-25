---
name: moex-crypto
description: Крипто-данные Фрейма (Bybit + Coinbase, пока только BTC) — где что лежит, как фетчится, какие лимиты у источников. Use when user says «крипта», "биткоин", "OI по биткоину", "халвинг", "Bybit", "Coinbase крипта", "добавь монету", "почему нет крипто-данных", "крипто-бэкфилл", or works with Crypto/*.py, crypto_candles, crypto_open_interest, btc_network_stats.
---

# Крипто-данные Фрейма — архитектура и справочник

Первый шаг крипто-раздела (начат 2026-07-22, PR #716/#717). Пока **только BTC**,
осознанно — «сначала добьём биткоин, потом посмотрим» (решение Вадима). Полная
хронология решений — memory `crypto_expansion_idea.md`, этот скилл — оперативный
справочник «где лежит / как работает / что проверить».

## Файлы и что они делают
- `Crypto/fetch_bybit_realtime.py` — **реалтайм**, host-cron раз в 5 минут (`--once`).
  Тянет с Bybit (публичный API, без ключей) свечи + OI на 5m/1h/1d для BTCUSDT-perp.
  Плюс раз за прогон — сеть BTC (высота блока/сложность/хешрейт) через mempool.space
  → `btc_network_stats`.
- `Crypto/backfill_bybit_history.py` — **разовый** глубокий бэкфилл Bybit (candles+OI)
  вглубь до запуска контракта. Переиспользует функции из `fetch_bybit_realtime.py`
  (импортирует их как модуль) — не дублирует логику паджинации/upsert.
- `Crypto/backfill_coinbase_history.py` — **разовый** бэкфилл глубокой спот-истории
  Coinbase (`api.exchange.coinbase.com`, без ключей). Параметризован `--products`
  (сейчас только `BTC-USD`, готов расширяться на другие монеты).

Все три пишут в общие таблицы `crypto_candles`/`crypto_open_interest` с разным
`exchange` ('bybit' vs 'coinbase') — поле exchange заложено с первого дня именно
под мультибиржевость, миграция для новой биржи не нужна.

## Схема БД
```sql
crypto_candles (exchange, symbol, interval, open_time, open, high, low, close,
                volume, quote_volume)  -- UNIQUE(exchange, symbol, interval, open_time)
crypto_open_interest (exchange, symbol, interval, ts, oi_contracts, oi_usd)
                -- UNIQUE(exchange, symbol, interval, ts); oi_usd пока не заполняется
btc_network_stats (ts, block_height, difficulty, hashrate_eh)  -- UNIQUE(ts), глобальная
```
`interval` — строка ('5m','1h','1d'), не число как в MOEX-таблице `candles`.

## Bybit — деривативы (BTCUSDT linear perpetual)
Зачем нужен именно он: **единственный источник крипто-специфичных метрик** —
OI, funding rate (funding пока не собираем, но эндпоинт есть), ликвидации. У спота
этих понятий не существует в принципе.

- Base URL: `https://api.bybit.com/v5/market` (kline, open-interest) — public, без ключей
- Коды гранулярности: candles `{'5m':'5','1h':'60','1d':'D'}`, OI `{'5m':'5min','1h':'1h','1d':'1d'}`
- Реальная глубина истории (проверено зондированием, НЕ дефолт скрипта):
  candles с **2020-03-25** (запуск контракта), OI с **2020-07-20** (5m/1h) /
  **2020-08-05** (1d) — эндпоинт OI включили на несколько месяцев позже контракта,
  это реальная граница источника.
- ⚠️ **ГОТЧА пагинации**: `kline`/`open-interest` игнорируют чистый `start`/`startTime`
  без парного `end`/`endTime` — без обоих отдают последние `limit` записей от ТЕКУЩЕГО
  момента, а не окно от `start`. Всегда передавать оба параметра окном
  `[cursor, cursor+limit*step]`.
- ⚠️ **ГОТЧА пустого окна**: пустой ответ ≠ «данных больше нет» — может означать
  «ещё рано» (окно до запуска контракта). Пагинация должна на пустом ответе
  двигать курсор вперёд и продолжать сканировать, `break` — только на реальной
  ошибке запроса (`result is None`). Раньше был баг именно тут (ломал глубокий
  бэкфилл для узких гранулярностей 5m/1h, 1d проходил случайно из-за широкого окна).
- ⚠️ **OI НЕ капается на 2 года** — это была ложная гипотеза (артефакт собственного
  `BACKFILL_DAYS=730` в реалтайм-скрипте, не ограничение биржи). Реальный потолок —
  запуск контракта, см. выше.

## Coinbase — спот (`BTC-USD`)
Зачем нужен: самая длинная чистая ценовая история из бесплатных источников (Coinbase —
одна из старейших регулируемых бирж, база CME BRR fixing). **У спота НЕТ и не может
быть OI** — открытый интерес существует только у деривативов.

- Base URL: `https://api.exchange.coinbase.com` (`/products/{id}/candles`) — public
- Лимит: **300 свечей за запрос** — паджинация окнами `300 * granularity_sec`
- Реальный старт данных: **2015-07-20** (раньше API отдаёт пусто, хотя биржа
  торгует с 2015 в целом)
- 481 USD-пара листингована — расширение на другие монеты = флаг `--products`,
  без изменений кода

## Проверенная глубина по всем источникам (не гадать заново!)
| Источник | Что | Реальная глубина |
|---|---|---|
| Bybit | candles (5m/1h/1d) | с 2020-03-25 |
| Bybit | OI (5m/1h) | с 2020-07-20 |
| Bybit | OI (1d) | с 2020-08-05 |
| Coinbase | spot candles (1d) | с 2015-07-20 |
| Binance | OI (`openInterestHist`) | **только 30 дней** — строго хуже Bybit, не добавлять ради истории |
| Coinbase Intl (CIX, деривативы) | OI | есть, но на 1-2 порядка меньше Bybit по размеру — не годится ни основным, ни в агрегат |

## Частые заблуждения (проверяй факт, не полагайся на память)
- Coinbase НЕ агрегирует OI с нескольких бирж (ни retail, ни CIX) — это не так,
  агрегация по биржам (как у Coinglass) — отдельная задача, мы её не строили
- У спота (Coinbase BTC-USD) OI не бывает вообще — не пытаться туда его прикрутить
- Bybit OI НЕ капается на 2 года — реальный потолок = запуск эндпоинта OI (2020-07/08)

## Деплой / где крутится
- Код фетчеров — часть образа `frame-orchestrator` (НЕ MOEX-оркестратор в смысле
  цикла — тот же контейнер, но крипто-скрипты запускаются НЕЗАВИСИМО от него, крипта
  торгует 24/7, а `main_orchestrator.py` гейтится MOEX trading hours)
- `Dockerfile` — построчный `COPY Crypto/ ./Crypto/` (если добавляешь новый файл
  в `Crypto/`, ничего доп. делать не надо — вся папка копируется целиком)
- `scripts/prod_deploy.sh` — `Crypto/` в regex ребилда orchestrator-образа
  (иначе push в main не пересоберёт образ с новым кодом фетчера)
- host-cron: `*/5 * * * * docker exec $(docker ps -q -f label=com.docker.compose.service=orchestrator) python3 Crypto/fetch_bybit_realtime.py --once >> /var/log/frame-crypto.log 2>&1`

## Как проверить текущее покрытие
```sql
SELECT exchange, symbol, interval, COUNT(*), MIN(open_time)::date, MAX(open_time)::date
FROM crypto_candles GROUP BY 1,2,3 ORDER BY 1,2,3;

SELECT exchange, symbol, interval, COUNT(*), MIN(ts)::date, MAX(ts)::date
FROM crypto_open_interest GROUP BY 1,2,3 ORDER BY 1,2,3;
```

## Разовые скрипты — как запускать вручную
```bash
# реалтайм разово (например после правки кода, до передеплоя)
docker exec frame-orchestrator-1 python3 Crypto/fetch_bybit_realtime.py --once

# глубокий бэкфилл Bybit (candles+OI, с 2020-01-01 по умолчанию)
docker exec frame-orchestrator-1 python3 Crypto/backfill_bybit_history.py --dry-run   # сначала проверить
docker exec frame-orchestrator-1 python3 Crypto/backfill_bybit_history.py             # потом реально

# бэкфилл Coinbase (спот)
docker exec frame-orchestrator-1 python3 Crypto/backfill_coinbase_history.py --products BTC-USD --dry-run
```
⚠️ Долгий прогон (`backfill_bybit_history.py` без флагов — часы на все монеты/гранулярности)
через `docker exec` по SSH без `-d`/nohup: если SSH-пайп обрывается, лог (шедший через
`tee`/stdout) обрывается вместе с ним, но сам процесс на сервере может успеть доработать
и завершиться успешно. **Не доверяй хвосту лога** — после подозрительного обрыва проверяй
факт в БД (запрос выше), а не перезапускай вслепую.

## Пока НЕ сделано (на будущее, не начато)
- Другие монеты (ETH и т.д.) — ждут отдельного решения, сейчас только BTC
- `data.binance.vision` (bulk-архив, много монет с 2017) / Kraken-Bitstamp (BTC глубже
  июля 2015) — если понадобится ещё глубже/шире
- Funding rate, ликвидации — эндпоинты у Bybit есть, фетчер не написан
- Фронтенд/индикатор — не начат, есть только данные в БД

Связано: [[data_sources]], [[ingestion_map]], [[oi_futures_charts]] (тот же паттерн
для MOEX-фьючерсов, откуда взята структура фетчера).
