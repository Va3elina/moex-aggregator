# Миграция фондов на Cbonds API

## Проблема

Данные БПИФов (21 фонд) берутся с ISS MOEX:
- **PAY** = биржевая цена закрытия (CLOSE с market=shares, board=TQTF)
- **NAV** = CAPITALIZATION из iNAV (market=index) — индикативная оценка биржи

Для расчёта чистых притоков/оттоков нужны **данные от УК** (управляющей компании):
- **nav_per_share** — расчётная стоимость пая (отражает реальную динамику активов)
- **nav** — официальная СЧА фонда

Биржевая цена содержит шум (спред, премия/дисконт к СЧА), что искажает расчёт потоков.

## Маппинг fund_id → cbonds_id

| # | fund_id | Тикер | cbonds_id | Название | Категория |
|---|---------|-------|-----------|----------|-----------|
| 1 | 8181 | AKMM | 209403 | Альфа-Капитал Денежный рынок | Денежный рынок |
| 2 | 8628 | TMON | 209423 | Т-Капитал Денежный Рынок | Денежный рынок |
| 3 | 7373 | SBMM | 209297 | Первая Сберегательный | Денежный рынок |
| 4 | 5973 | LQDT | 209099 | ВИМ Ликвидность | Денежный рынок |
| 5 | 10053 | AMNR | 209519 | АТОН Накопительный в рублях | Денежный рынок |
| 6 | 6333 | TMOS | 209133 | Т-Капитал Индекс МосБиржи | Акции |
| 7 | 5247 | SBMX | 208941 | Первая Топ Российских акций | Акции |
| 8 | 6073 | EQMX | 209105 | Индекс МосБиржи ВИМ | Акции |
| 9 | 6575 | AKME | 209155 | Альфа-Капитал Управляемые акции | Акции |
| 10 | 6225 | AKMB | 209119 | Альфа Управляемые облигации | Облигации |
| 11 | 10331 | SBLB | 220497 | Первая Долгосрочные гособлигации | Облигации |
| 12 | 11445 | TOFZ | 232159 | Т-Капитал ОФЗ | Облигации |
| 13 | 11705 | AMGB | 232647 | АТОН Длинные ОФЗ | Облигации |
| 14 | 10113 | SBFR | 209525 | Первая Облигации флоатеры | Облигации |
| 15 | 7067 | TBRU | 209253 | Т-Капитал Облигации | Облигации |
| 16 | 7007 | SAFE | 209239 | Первая Консерватив | Облигации |
| 17 | 5713 | SBRB | 198837 | Первая Корпоративные облигации | Облигации |
| 18 | 4713 | AKGD | 209355 | Альфа-Капитал Золото | Золото |
| 19 | 4038 | GOLD | 209117 | Золото Биржевой | Золото |
| 20 | 5061 | SBGD | 209413 | Первая Доступное золото | Золото |
| 21 | 4098 | TGLD | 209129 | Т-Капитал Золото | Золото |

## Что берём с Cbonds

### 1. NAV (ежедневно) — основные данные
**Endpoint:** `POST /m/exchange_traded_funds/nav/global/json/{cbonds_id}/{date_from}/{date_to}/?lang=rus`

Возвращает массив `response.items[]`:
```json
{
  "date": 1774904400,          // unix timestamp (UTC)
  "nav": 20749155534.15,       // общая СЧА фонда (₽)
  "nav_per_share": 18.80       // расчётная стоимость пая от УК (₽)
}
```

**Маппинг в fund_data:**
- `nav` → `fund_data.nav`
- `nav_per_share` → `fund_data.pay`

### 2. Состав фондов (раз в день)
**Endpoint:** `POST /m/exchange_traded_funds/structure/global/json/{parent_fund_id}/?lang=rus`
**Body:** `{"quantity":{"limit":50,"offset":0}}`

Возвращает активы фонда (название, доля). Уже используется для ПИФов.

**Важно:** `parent_fund_id` ≠ `cbonds_id`. Для БПИФов `parent_fund_id` берётся из `share_class_information.fund_id` (например, SBMX: cbonds_id=208941, fund_id=204061).

## Что НЕ берём (но есть в API, на будущее)

### share_class_information
**Endpoint:** `POST /m/exchange_traded_funds/share_class_information/global/json/{cbonds_id}/0/?lang=rus`

Метаданные фонда. Полезные поля:
- `ticker`, `isin`, `name` — идентификация
- `amc_name` — управляющая компания
- `expense_ratio` — комиссия фонда (%)
- `management_style_name` — Пассивный/Активный
- `investment_object_name` — Акции/Облигации/Золото
- `geography_investment_name` — Россия
- `sector_name` — Широкий рынок / Высокая капитализация
- `fund_description` — описание стратегии
- `launched_date` — дата запуска (unix)
- `etf_funds_nav`, `etf_funds_nav_date` — последняя СЧА
- `pays_dividend_payments`, `frequency_of_dividend_payments` — дивиденды
- `qualified_only` — только для квалов
- `fund_id` — parent fund ID (для запроса состава)

### suggest (поиск)
**Endpoint:** `POST /m/exchange_traded_funds/suggest/global/json/{query}?lang=rus`

Поиск фондов по названию. Возвращает cbonds_id, name, fund_id.

### quotes (котировки)
**Endpoint:** `POST /m/exchange_traded_funds/quotes/global/json/{cbonds_id}/1/{from}/{to}/?lang=rus`

Биржевые котировки (если торгуется). Не нужны — мы используем nav_per_share.

## План миграции

### Шаг 1: Добавить cbonds_id в таблицу funds
```sql
ALTER TABLE funds ADD COLUMN cbonds_id INTEGER;
```
Заполнить из маппинга выше.

### Шаг 2: Переписать fetch_funds_realtime.py
- Убрать dict `FUNDS` (21 БПИФ с ISS MOEX конфигом)
- Убрать dict `CBONDS_FUNDS` (15 ПИФов с отдельным конфигом)
- Единый пайплайн: для всех фондов из таблицы `funds` WHERE `cbonds_id IS NOT NULL`
  - Загрузка NAV через `/m/exchange_traded_funds/nav/`
  - Сохранение `nav` → `fund_data.nav`, `nav_per_share` → `fund_data.pay`
  - Загрузка состава через `/m/exchange_traded_funds/structure/`

### Шаг 3: Проверить данные
- Загрузить историю за 3 года для всех 21+15 фондов
- Сравнить потоки акций с @moex_stats (референс)
- Убедиться что даты корректны (Cbonds может отдавать T-1)

### Шаг 4: Очистить старые данные
- DELETE FROM fund_data WHERE fund_id IN (список 21 БПИФа)
- Перезагрузить с Cbonds

### Шаг 5: Убрать ISS MOEX зависимость
- Удалить URL_SHARES, URL_INDEX, fetch_chunk, fetch_full, merge_shares_inav
- Весь код работает через Cbonds API

## Учётные данные Cbonds
- **URL:** https://rest2.cbonds.info
- **User-Agent:** `Cbonds.K/3.0.8 (ru.cbonds.cbonds; build:636; Android 9) OkHttp/4.12.0`
- **Login:** см. .env (CBONDS_LOGIN)
- **Password:** см. .env (CBONDS_PASSWORD)
- **Auth endpoint:** `POST /m/auth/tariffs/global/json/logout=1?lang=rus`
- **Авторизация:** cookie-based (PHPSESSID), одна сессия на весь batch
