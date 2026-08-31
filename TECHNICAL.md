# Техническая документация — Фрейм

## Источники данных

| Данные | Источник | Интервал обновления |
|--------|----------|---------------------|
| Свечи фьючерсов 1мин | Algopack (apim.moex.com) | каждые 5 мин |
| Свечи фьючерсов 5мин | Агрегация из 1мин (`aggregate_to_5min`) | каждые 5 мин |
| Свечи фьючерсов 1час | Algopack напрямую (interval=60) | каждые 5 мин |
| Свечи фьючерсов 1день | ISS (iss.moex.com) — для акций; Algopack — для фьючерсов | ежедневно |
| Свечи акций | Algopack (interval=24 через ISS для дневных) | каждые 5 мин |
| OI 5мин | Algopack (65 тикеров) | каждые 5 мин |
| OI часовой | Агрегация из 5мин (`aggregate_oi_hourly.py`) | каждый час |
| OI дневной | ISS MOEX (133 инструмента) | ежедневно |
| Фонды, индексы | ISS MOEX | ежедневно |
| Макро (M2, GDP) | ЦБ РФ | ежедневно |
| Рыночная капитализация | ISS MOEX | ежедневно |

**Ключевые файлы сборщиков:**
- `Candles/fetch_candles_futures_realtime.py` — свечи фьючерсов (содержит `aggregate_to_5min`)
- `Candles/fetch_candles_spot_realtime.py` — свечи акций
- `OI/fetch_oi_5min_realtime.py` — OI 5мин
- `OI/aggregate_oi_hourly.py` — агрегация OI 5мин → 1час
- `main_orchestrator.py` — оркестратор, запускает всё по расписанию

---

## Склейка фьючерсных контрактов (ролловер)

**Файл:** `api/routers/chart.py`, секция 6

**Проблема:** у каждого фьючерса 4 контракта (H, M, U, Z) с разными ценами. При переходе между контрактами на графике появляется "забор" — цена прыгает туда-сюда.

**Решение (необратимый ролловер, как TradingView):**
1. Для каждого дня определяем контракт с максимальным объёмом
2. После первого переключения на новый контракт — **не возвращаемся** на старый
3. Дедупликация: ISS иногда отдаёт дубли с разных бордов (vol=269k и vol=20 для одного sec_id на одну дату) — оставляем свечу с максимальным volume

**Пример:**
```
GZU лидирует → GZU → GZU → GZZ обогнал → GZZ → GZZ (необратимо)
```

**Защита от фейковых свечей (интрадей):** свечи с `volume=0` отбрасываются для interval != 24.

---

## Агрегация 5мин свечей (`aggregate_to_5min`)

**Файл:** `Candles/fetch_candles_futures_realtime.py`, функция `aggregate_to_5min`

**Что делает:** агрегирует 1мин свечи в 5мин (OHLCV groupby + zero-fill пропусков).

**Фильтр торговых часов:** `full_idx` ограничен 07:00-23:50 (торговые часы MOEX фьючерсов). Без этого фильтра `pd.date_range` создаёт фейковые свечи за 00:00-06:55 с O=H=L=C (forward fill последнего close). Аналогичный фильтр в `OI/fetch_oi_5min_realtime.py`.

---

## Выравнивание OI и свечей (`alignToCandles`)

**Файл:** `frontend/src/pages/OpenInterestPage.tsx`

**Проблема:** свечи и OI имеют разные timestamp-форматы:
- Дневные свечи: `2024-09-02T00:00:00`
- Дневной OI: `2024-09-02T23:50:00`
- 5мин свечи: `2024-09-02T09:05:00`
- 5мин OI: `2024-09-02T09:05:00` (совпадают)

**Решение:**
- Для **дневного** интервала: ключ совмещения по дате `YYYY-MM-DD` (slice 0,10)
- Для **интрадей** (5мин/1час): ключ по полному timestamp — сохраняет внутридневную гранулярность OI

Если использовать только дату для 5мин, все 112 внутридневных OI значений схлопываются в одно — график показывает ступеньки вместо кривой.

---

## Скользящее окно (navigator) и OI

**Файл:** `frontend/src/components/SimpleChart.tsx`

`displaySecondaryData` и `displayThirdData` фильтруют OI по видимому диапазону навигатора. Сравнение по дате `.slice(0, 10)` — потому что свечи `T00:00:00`, а OI `T23:50:00`, и строковое сравнение полных timestamp теряет данные.

---

## Система кеширования

### Два уровня кеша

**1. Серверный кеш (in-memory)**
- **Файл:** `api/cache.py`
- **TTL:** 1800 сек (30 минут)
- **Ключ:** `chart:{sec_id}:{sectype}:{inst_type}:{interval}:{clgroup}:{show_oi}:{period}:{date_from}:{date_to}`
- **Стратегия:** ленивый — кешируется только то, что запросил пользователь
- **Обновление:** инкрементальное при NOTIFY (дописываем новые свечи/OI, не пересчитываем)
- Функции: `get_or_set()`, `set_cache()`, `get_all_by_prefix()`, `invalidate()`, `cache_stats()`

**2. Браузерный кеш (Service Worker)**
- **Файл:** `frontend/public/sw.js`
- **Стратегия:** network-first — сначала сервер, кеширует ответ, при офлайне из кеша
- **Эффект:** повторные переключения таймфреймов мгновенные (~0ms)
- **Версия кеша:** `CACHE_NAME = 'frame-v11'` — при деплое нового фронта старый кеш удаляется

### Инкрементальное обновление кеша

**Файл:** `api/cache_updater.py`

При получении NOTIFY source="5min":
1. Получает все живые записи `chart:*` из кеша
2. Для каждой: запрашивает из БД только новые свечи/OI (после последней точки в кеше)
3. Дописывает в конец массива `candles`/`open_interest`
4. Обновляет метаданные и TTL

Для source="daily" — полная инвалидация (раз в день допустимо).

### NOTIFY система

**Файл:** `api/notify_listener.py`

PostgreSQL NOTIFY на канале `data_updated`. Оркестратор отправляет после записи новых данных.

| Source | Действие |
|--------|----------|
| `5min` | chart: инкрементальное обновление; candles, oi: инвалидация |
| `hourly` | chart: инкрементальное обновление; oi: инвалидация |
| `daily` | Полная инвалидация всего кеша |
| `mv_refresh` | heatmap, stats: инвалидация |
| `funds` | funds_chart, fear: инвалидация |

---

## Оптимизации скорости

### 1. Fast path для интрадей запросов
**Файл:** `api/routers/chart.py`

Для 5мин/1час с фиксированным period (не "all") — пропускаем тяжёлые `MIN/MAX` bounds запросы (~2 сек каждый) и считаем даты от сегодня. Если данных нет — возвращаем пустой ответ с warning логом.

**Эффект:** SR 5min cold 13.5с → 2.5с

### 2. Dict вместо Pydantic
Chart endpoint возвращает plain dict вместо Pydantic моделей (ChartResponse, CandleResponse, OpenInterestResponse). Pydantic валидация 13000 объектов занимала ~400ms, dict build — 15ms.

### 3. Gzip сжатие
Nginx сжимает JSON ответы: 833KB → 154KB (82% экономия).

### 4. Предрассчитанные таблицы
- `breadth_history` — сила рынка (пересчитывается ежедневно ~2 мин)
- `index_composition` — историческая база расчёта IMOEX (состав индекса на каждый
  торговый день, ISS analytics). Знаменатель вселенной imoex у «Силы рынка»:
  без неё широта считалась бы по сегодняшнему составу на всю глубину графика
- `delisted_securities` — реестр исторических бумаг (Уралкалий, Мегафон, РАО ЕЭС…).
  Их свечи лежат в `candles`, но в `instruments` их быть не должно: оттуда мёртвый
  тикер уехал бы в пикеры и карту рынка как живой актив
- `mv_heatmap_stocks` — materialized view для карты рынка (refresh каждые 5 мин)
- `mv_oi_daily_stats` — materialized view для OI статистики

---

## Фильтрация нерабочего времени

### Бэкенд (`chart.py`)
- Свечи с `volume=0` отбрасываются для интрадей (interval != 24)
- `aggregate_to_5min`: reindex ограничен 07:00-23:50

### Фронтенд (`OpenInterestPage.tsx`)
- Выходные (Сб, Вс) фильтруются
- Праздники MOEX (захардкожены на 2024-2026)
- Пре-маркет до 09:00 — только для 5мин

---

## Локальная разработка

**Скрипты:**
- `dev-start.sh` — поднимает PostgreSQL + бэкенд + фронтенд
- `dev-stop.sh` — останавливает всё, освобождает порты

**Требования:**
- PostgreSQL 18 (homebrew): `LC_ALL=en_US.UTF-8` обязательно при запуске
- Python venv: `.venv/`
- Node.js: `frontend/node_modules/`

**Vite proxy:** `frontend/vite.config.ts` проксирует `/api` на `localhost:8000`

---

## БД — таблица candles

- **Размер:** ~8 ГБ (2.2 ГБ данные + 5.9 ГБ индексы)
- **Строк:** ~20М
- **15 индексов**, ключевые:
  - `idx_candles_sec_interval_time` (sec_id, interval, begin_time) — основной для chart.py
  - `idx_candles_secid_interval_time` (secid, interval, begin_time DESC) — для fetcher
  - `candles_pkey1` (secid, begin_time, interval, type) — уникальность

---

## Android эмулятор + mitmproxy (перехват API мобильных приложений)

**Когда нужен:** для reverse-engineering API мобильных приложений (Cbonds, investfunds и др.).

**Установка (одноразово):**
```bash
brew install mitmproxy
brew install --cask android-commandlinetools
brew install openjdk@17

export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
export ANDROID_HOME=/opt/homebrew/share/android-commandlinetools

# SDK компоненты
yes | sdkmanager --sdk_root=$ANDROID_HOME "platform-tools" "emulator" \
  "platforms;android-28" "system-images;android-28;google_apis;arm64-v8a" \
  "build-tools;34.0.0"

# Создание виртуального устройства (ARM64 для M-серии Mac)
echo "no" | avdmanager create avd --name "mitm_device" \
  --package "system-images;android-28;google_apis;arm64-v8a" \
  --device "pixel_2" --force
```

**Запуск сессии перехвата:**
```bash
# 1. Запустить mitmproxy (записывает в файл)
mitmdump -w capture.flow --set block_global=false -p 8080 &

# 2. Запустить эмулятор
$ANDROID_HOME/emulator/emulator -avd mitm_device \
  -http-proxy http://10.0.2.2:8080 -no-snapshot -no-boot-anim \
  -gpu swiftshader_indirect &

# 3. Установить сертификат (после загрузки устройства)
ADB=$ANDROID_HOME/platform-tools/adb
$ADB wait-for-device
$ADB root  # работает только на google_apis (НЕ google_apis_playstore)
$ADB push ~/.mitmproxy/mitmproxy-ca-cert.cer /sdcard/Download/mitmproxy.cer
# Далее вручную: Settings → Security → Install from storage

# 4. Для приложений с targetSdk >= 24 — патч APK:
apktool d app.apk -o patched -f
# Редактировать res/xml/network_security_config.xml:
#   <base-config><trust-anchors>
#     <certificates src="system" /><certificates src="user" />
#   </trust-anchors></base-config>
apktool b patched -o patched.apk
$ANDROID_HOME/build-tools/34.0.0/zipalign -v 4 patched.apk final.apk
keytool -genkeypair -v -keystore debug.keystore -alias debug -keyalg RSA \
  -keysize 2048 -validity 10000 -storepass android -keypass android -dname "CN=Debug"
$ANDROID_HOME/build-tools/34.0.0/apksigner sign --ks debug.keystore \
  --ks-pass pass:android final.apk
$ADB install final.apk
```

**Чтение перехвата:**
```bash
mitmdump -r capture.flow --set flow_detail=4 -n 2>&1 | grep "rest2.cbonds"
```

**Важно:**
- `google_apis` образ → поддерживает `adb root`, но нет Play Store
- `google_apis_playstore` → есть Play Store, но нет root (нужен патч APK)
- API 28 (Android 9) — последний где приложения доверяют пользовательским CA по умолчанию
- На M-серии Mac использовать только `arm64-v8a` образы
- Прокси из эмулятора: `10.0.2.2` = хост-машина (не `127.0.0.1`)
- PIN для установки сертификата: `adb shell locksettings set-pin 1234`

---

## Cbonds API (перехваченный из мобильного приложения)

**Базовый URL:** `https://rest2.cbonds.info`
**User-Agent:** `Cbonds.K/3.0.8 (ru.cbonds.cbonds; build:636; Android 9) OkHttp/4.12.0`
**Авторизация:** cookie-based (`PHPSESSID`)

### Эндпоинты

| Эндпоинт | Метод | Описание |
|---|---|---|
| `/m/auth/tariffs/global/json/logout=1?lang=rus` | POST | Авторизация. Body: `{"login":"...","password":"..."}` → `Set-Cookie: PHPSESSID` |
| `/m/exchange_traded_funds/suggest/global/json/{query}?lang=rus` | POST | Поиск фондов по названию (URL-encoded кириллица) |
| `/m/exchange_traded_funds/nav/global/json/{fund_id}/{date_from}/{date_to}/?lang=rus` | POST | **История NAV**. Возвращает: `date` (unix ts), `nav` (общая СЧА), `nav_per_share` (пай) |
| `/m/exchange_traded_funds/structure/global/json/{parent_fund_id}/?lang=rus` | POST | Состав фонда (активы + доли). Body: `{"quantity":{"limit":50,"offset":0}}` |
| `/m/exchange_traded_funds/share_class_information/global/json/{fund_id}/0/?lang=rus` | POST | Информация о фонде (УК, валюта, тип) |
| `/m/exchange_traded_funds/quotes/global/json/{fund_id}/1/{from}/{to}/?lang=rus` | POST | Котировки (если торгуется на бирже) |

### Маппинг фондов (investfunds ID → Cbonds ID)

**ПИФы (данные только через Cbonds API):**

| investfunds | cbonds_id | Название | Категория |
|---|---|---|---|
| 8123 | 209397 | Первая - Фонд акций с выплатой дохода | Акции (управляемые) |
| 432 | 206895 | Альфа-Капитал Ликвидные акции | Акции (управляемые) |
| 43 | 206601 | Первая - Фонд российских акций | Акции (управляемые) |
| 281 | 206781 | Райффайзен - Акции | Акции (управляемые) |
| 1003 | 207285 | ВИМ - Акции | Акции (управляемые) |
| 282 | 206783 | Райффайзен - Компании роста | Акции (управляемые) |
| 63 | 206625 | Атон - Петр Столыпин | Акции (управляемые) |
| 8119 | 209395 | Первая - Фонд облигаций с выплатой дохода | Облигации (смешанные) |
| 9113 | 209453 | Альфа-Капитал Облигации с выплатой дохода | Облигации (смешанные) |
| 9165 | 219221 | ВИМ - Облигации. Рантье | Облигации (смешанные) |
| 47 | 206607 | Первая - Фонд Рублевые сбережения | Облигации (смешанные) |
| 33 | 206593 | Альфа-Капитал Облигации Плюс | Облигации (смешанные) |
| 11259 | 231147 | Альфа-Капитал Облигации с переменным купоном | Облигации (смешанные) |
| 54 | 206617 | ВИМ - Казначейский | Облигации (смешанные) |
| 4995 | 208851 | Первая - Накопительный | Облигации (смешанные) |

**БПИФы (данные через ISS MOEX + дублируются в Cbonds):**

| investfunds | cbonds_id | Тикер MOEX | Название | Категория |
|---|---|---|---|---|
| 10053 | 209519 | AMNR | АТОН - Накопительный в рублях | Денежный рынок |
| 10113 | — | SBFR | Первая Облигации флоатеры | Облигации (ОФЗ) |
| 7067 | — | TBRU | Т-Капитал Облигации | Облигации (смешанные) |
| 7007 | — | SAFE | Первая Консерватив | Облигации (смешанные) |
| 5713 | — | SBRB | Первая Корп облигации | Облигации (смешанные) |
| 11259 | — | AKFB | Альфа-Капитал Облигации с перем. купоном | Облигации (смешанные) |

### Учётные данные

Хранятся в `.env` (не коммитятся): `CBONDS_LOGIN`, `CBONDS_PASSWORD`.
См. `.env.example` для списка переменных.

---

## Changelog (27.03.2026)

- Фикс OI alignment (дневной по дате, интрадей по timestamp)
- Фикс скользящего окна (navigator) для OI
- Удалены 681к фейковых свечей и 266к фейковых OI за ночные часы (00:00-06:55)
- Фильтр торговых часов в aggregate_to_5min и OI fetcher
- Необратимый ролловер фьючерсов (как TradingView) + дедуп бордов
- Ленивый кеш с инкрементальным обновлением (TTL 60с → 30мин)
- Dict вместо Pydantic в chart endpoint (27x быстрее сериализация)
- Fast path для интрадей (skip bounds SQL, cold 13.5с → 2.5с)
