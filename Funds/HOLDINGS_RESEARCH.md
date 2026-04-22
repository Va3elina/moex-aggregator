# Отслеживание покупок активов в БПИФ — исследование источников

**Дата исследования:** 2026-04-22
**Статус:** разведка завершена, имплементация отложена
**Цель проекта:** построить мониторинг "какие акции УК купили/продали в БПИФах, по возможности в реальном времени"

---

## TL;DR

| Что проверили | Вердикт |
|---|---|
| Cbonds мобильный API (используем сейчас) | ❌ не даёт `positions` и history, snapshot месячный с задержкой ~1 мес |
| Cbonds Enterprise API | ✅ даёт positions+history, но **платная подписка** (~$500+/мес) |
| T-Invest API (Т-Банк) | ❌ holdings БПИФ не отдаёт (только метаданные + GetPortfolio только свой) |
| InvestFunds.ru API | ❌ платный (через Cbonds) |
| MOEX ISS API | ❌ биржа не публикует состав фондов |
| **Прямой парсинг сайтов УК** | ✅ **это путь** |

**Итоговое покрытие через парсинг сайтов УК:**
- 🥇 **3 фонда ежедневно** (T+1): LQDT, EQMX, GOLD — через HTML таблицу ВИМ
- 🥈 **16 фондов ежемесячно** (задержка 10–20 дней): Первая, Т-Капитал, Альфа
- 🔴 **2 фонда** (АТОН) требуют обхода Cloudflare

**~55% AUM покрываются ежедневно**, остальное раз в месяц.

---

## Контекст задачи

Пользователь (Вадим) хочет видеть на сайте [таймфрейм.рф](https://xn--80aklbnczmv.xn--p1ai) **движения активов в БПИФах в реальном времени**. То есть:

> "За март УК Первая докупила 53 673 шт Сбера в фонд SBMX"
> "В апреле ВИМ в EQMX сократил Газпром на 12 000 шт"
> "По всем 15 БПИФам за март: суммарный приток в Сбер = +X млн штук"

### Фонды, которые отслеживаем (21 шт.)

Из `Funds/CBONDS_MIGRATION.md`:

| УК | Фонды (21) |
|---|---|
| Альфа-Капитал (4) | AKMM, AKME, AKMB, AKGD |
| Т-Капитал (5) | TMON, TMOS, TOFZ, TBRU, TGLD |
| Первая / Сбер (7) | SBMM, SBMX, SBLB, SBFR, SAFE, SBRB, SBGD |
| ВИМ (3) | LQDT, EQMX, GOLD |
| АТОН (2) | AMNR, AMGB |

---

## Что проверили и отвергли

### 1. Cbonds мобильный API (используемый сейчас)

**URL:** `rest2.cbonds.info/m/...`
**Endpoint:** `/m/exchange_traded_funds/structure/global/json/{parent_fund_id}/`
**Что отдаёт:** snapshot состава фонда — 9 полей: `id, date, asset_name, weight, new_portfolio_full_entity_id, portfolio_full_asset_type_id, portfolio_full_asset_type_name, portfolio_full_asset_id, link`

**Проблемы:**
- ❌ Нет поля `positions` (штуки) — только `weight` (%)
- ❌ Нет истории snapshot'ов — только latest
- ❌ Задержка ~1 месяц (29 марта видел snapshot от 26 февраля)
- ❌ Фильтр `{"field": "date", ...}` игнорируется
- ❌ Явный запрос `fields: [{"field": "positions"}]` → 0 items (поле отвергается)
- ❌ Endpoint `/structure/history/...` **существует**, но требует неизвестный `ext2` arg (видимо только для Pro-тарифа — `isPro: false` у нашего логина, `level: 1`)
- ❌ Endpoints `/portfolio/`, `/holdings/`, `/funds_property_assets/` — `"Can not find requested operation"`

**Проверено экспериментально:** в `fetch_funds_realtime.py::fetch_cbonds_holdings` берётся только `asset_name` и `weight`. Capture 4802 запросов (56MB в `cbonds_capture.flow`) — все 4653 `structure` запроса вернули один и тот же snapshot.

### 2. Cbonds Enterprise (`ws2.cbonds.info/services/json/`)

**Доступ:** Test/Test даёт схему всех 247 операций.

**Ключевые методы для БПИФ:**
- `get_funds_property_assets` (34 поля!) — **есть `positions` + `funds_property_date` + фильтры**
- `get_investfunds_portfolio` (20 полей) — аналогично
- `get_funds` (75 полей) — карточка фонда
- `get_funds_dividends`, `get_funds_splits` — доп. события

**Пример полей `get_funds_property_assets`:**
```
funds_property_funds_id   Fund ID
funds_property_date       Asset Structure Date   ← фильтруется!
report_name               Asset name
security_id               ISIN
branch_name_rus           Industry
amount                    Amount, rub.
share                     Share, %
positions                 Number of securities  ← ЭТО ТО ЧТО НУЖНО!
last_nav                  NAV at structure date
update_time               Update time
```

**Отвергнуто:** требуется платная подписка. Demo (Test/Test) даёт ограниченный dataset.

**Контакт:** `pro@cbonds.info`, `database@cbonds.info`, +7 812 336-97-21 доб.174

### 3. T-Invest API (Т-Банк public API)

**URL:** `tinkoff.github.io/investAPI/`, `developer.tbank.ru/invest/api`
**Формат:** gRPC + REST, бесплатный
**Proto:** `github.com/RussianInvestments/investAPI`

**Что есть (instruments.proto — 40 методов):**
- `EtfBy` / `Etfs` — метаданные ETF: ISIN, ticker, УК, expense_ratio
- `GetAssetFundamentals` — фундаментальные показатели (для БПИФ не применимо)
- `GetPortfolio` — **только свой брокерский счёт**, не состав фонда

**Отвергнуто:** **holdings БПИФ не отдаёт**. Т-Банк показывает состав в UI (`tbank.ru/invest/etfs/EQMX/`), но не через публичный API.

### 4. InvestFunds.ru API

**URL:** `investfunds.ru/api/`
**Статус:** платно через регистрацию, кнопка "Запросить доступ"
**Фактически:** это тот же Cbonds — email `database@cbonds.info` на странице API
**Отвергнуто:** платно.

### 5. MOEX ISS API

**URL:** `iss.moex.com/iss/reference/`
**Проверено:** в 247 endpoint'ах нет ни одного для fund composition / holdings / portfolio / constituents.
**Отвергнуто:** биржа котировки даёт, состав фондов — нет.

---

## Что работает: источники по УК

### 🥇 Tier A: ВИМ Инвестиции (3 фонда, ЕЖЕДНЕВНО T+1)

**Фонды:** LQDT (ликвидность), EQMX (индекс МосБиржи), GOLD (золото)
**Базовый URL:** `https://www.wealthim.ru/products/bpif/{slug}/structure/`
**Slug'и:**
- LQDT → `wimfl`
- EQMX → `wimfimb`
- GOLD → нужно уточнить при имплементации

**Что доступно:**

1. **HTML-таблица с positions** прямо на странице `/structure/`
   - Колонки: Актив + ISIN, Доля %, Δдоли, **Кол-во бумаг в портф.**
   - Дата snapshot'а (например 22.04.2026 для EQMX)
   - Реальный пример: ЛУКОЙЛ 15.89%, **410 707 штук**

2. **Hidden xlsx endpoint для NAV history:**
   ```
   GET https://www.wealthim.ru/local/templates/new/components/articul/quotes/.default/xlsx.php
   ?SHOWALL_1=1&from=&to=
   &QUOTES_IBLOCK_TYPE=vtb_am_fonds&QUOTES_IBLOCK_ID=25
   &FOND_ID=170898   ← EQMX (ID известный)
   &FONDS_IBLOCK_TYPE=vtb_am_fonds&FONDS_IBLOCK_ID=19
   ```
   Возвращает **1501 строку** всей истории: дата / стоимость пая / СЧА / кол-во паёв. Параметры `from`/`to` поддерживаются — можно фильтровать.

**FOND_ID известные:**
- EQMX = 170898
- LQDT, GOLD — вытащить из исходника их формы `/structure/`

**Трудности:** минимальные, ~1 день реализации.

### 🥈 Tier B: УК Первая / Сбер (7 фондов, ЕЖЕМЕСЯЧНО)

**Фонды:** SBMM, SBMX, SBLB, SBFR, SAFE, SBRB, SBGD
**Сайт:** `first-am.ru`

**Индекс-страница** с документами фонда:
```
https://first-am.ru/individuals/etf/{slug}/documents
```
Slug'и:
- SBMX → `etf-moex`
- SBMM → `etf-sbmm`
- остальные — аналогично

**Формат URL файлов:**
```
/download/{doc_id}/БПИФ РФИ Первая - Фонд ... Справка о стоимости чистых активов_за_{месяц}_{год}г.PDF
```
Пример: `/download/24869/БПИФ РФИ Первая - Фонд Топ Российских акций _Справка о стоимости чистых активов_за_март_2026г.PDF`

**Формат файлов:** PDF ОКУД 0420502 (см. ниже)

**Частота:** месячный snapshot на последний день месяца, публикуется через ~15-20 дней
- За март 2026 уже доступен 22.04.2026 → **задержка ~3 недели**

**Трудности:**
- HTTP 403 на default User-Agent → обходится `Mozilla/5.0 Chrome/125`
- `doc_id` динамические — нужно скрейпить index-страницу. В index-странице мы вытащили 140+ ссылок (архив с 2023 года).
- Кириллица в URL → `urllib.parse.quote` с `safe=':/?&=%#,-._~+'`

**Сложность реализации:** 1 день (скрейпер index + PDF parser)

### 🥈 Tier C: Т-Капитал / Т-Банк (5 фондов, ЕЖЕМЕСЯЧНО)

**Фонды:** TMON, TMOS, TOFZ, TBRU, TGLD
**Сайт:** `t-capital-funds.ru`

**Индекс-страница:** `https://t-capital-funds.ru/documents/mutual_funds/{TICKER}/`

**Формат URL файлов (прямой, предсказуемый):**
```
https://cdn.t-capital-funds.ru/static/documents/otchet-scha-{ticker}-DD-MM-YYYY.PDF
```
Пример: `otchet-scha-tmos-31-03-2026.PDF`

**Типы документов:**
- `otchet-scha-` — справка о СЧА (ОКУД 0420502) — **это нам и нужно**
- `otchet-prirost-` — отчёт о приросте
- `otchet-voznagrajdenie-` — годовой о вознаграждениях

**Частота:** ежемесячно, на последнее число месяца

**Трудности:**
- Без антибот, работает через обычный HTTPS
- URL предсказуем — не нужен скрейп индекса (даты знаем заранее)
- Архив с 2020 года

**Сложность реализации:** 0.5 дня после готового PDF-парсера

### 🥈 Tier D: Альфа-Капитал (4 фонда, ЕЖЕМЕСЯЧНО)

**Фонды:** AKMM, AKME, AKMB, AKGD
**Сайт:** `alfacapital.ru`

**Индекс-страница:**
```
https://www.alfacapital.ru/disclosure/pifs/{slug}/{period}
```
где `{period}` = `monthly`, `quarterly`, `yearly`, `nav-rules`
Slug'и:
- AKMM → `bpif-akmm`
- AKME → `bpif_akmrs` (исторический тикер AKMRS)
- остальные — проверить

**Формат URL файлов:**
```
/disclosure/file/{numeric_id}
```
Пример: `/disclosure/file/53591` (СЧА за март 2026)
`/disclosure/file/53593` (отчёт о приросте за тот же период)

**Формат:** PDF ОКУД 0420502

**Частота:** ежемесячно + ежеквартально отдельно

**Трудности:**
- Numeric ID → нужно скрейпить index
- Архив 2022–2026

**Сложность реализации:** 0.5 дня (переиспользуем PDF parser)

### 🔴 Tier E: АТОН-Менеджмент (2 фонда, требует Cloudflare bypass)

**Фонды:** AMNR (RUONIA индекс, ребьютер РЕПО), AMGB (длинные ОФЗ)
**Основной сайт:** `aton.ru` — за **Cloudflare** (403 на curl/python requests)
**Старый сайт:** `am-aton.ru` — есть страницы `/info/pif/fond{N}/`, но slug'и неясны

**Централизованные реестры с данными АТОН:**
- `e-disclosure.ru/portal/company.aspx?id=12603` — Интерфакс (тоже 403 в тест'е)
- `e-disclosure.azipi.ru/messages/{id}/` — **работает без защиты**, но там только **ежедневные сообщения о СЧА**, не holdings

**Состояние:** **требует отдельной проработки** — Playwright/headless browser, либо ручной подход через `cf_clearance` cookies

**Сложность реализации:** 1–2 дня
**Альтернатива:** пока игнорировать — 2 фонда с небольшим AUM

---

## Формат ОКУД 0420502 — законодательный стандарт РФ

**Полное название:** "Справка о стоимости чистых активов, в том числе стоимости активов (имущества), акционерного инвестиционного фонда (паевого инвестиционного фонда)"

**Регулятор:** ЦБ РФ, Указание 6121-У / 4956-У

**Единый формат для всех УК РФ** — поэтому **один PDF parser работает для Первой + Т-Капитал + Альфа + АТОН**.

### Структура (35 страниц у SBMX, 14 у TMOS — зависит от числа активов)

**Раздел 1. Реквизиты фонда** — ОКПО, ISIN паёв, номер правил
**Раздел 2. Параметры справки** — отчётная дата, валюта
**Раздел 3. Активы:**
- Подраздел 1: Денежные средства (банки + валюты)
- Подраздел 2: Ценные бумаги РФ (облигации, госы, расписки, **акции**)
- Подраздел 3: Ценные бумаги иностранных эмитентов
- Подраздел 4–9: Недвижимость, долевое, депозиты, драгметаллы и т.д.
- Подраздел 10: Общая стоимость активов
**Раздел 4. Обязательства** — кредиторка, резервы
**Раздел 5. СЧА** — итог, включая кол-во паёв и расчётная стоимость одного пая

### Таблица "Акции российских АО" (та что нас интересует)

Колонки:
```
1. Наименование эмитента ("ПАО СБЕРБАНК РОССИИ")
2. ОГРН (1027700132195)
3. ИНН (7707083893)
4. Организационно-правовая форма
5. Регистрационный номер выпуска (10301481B)
6. ISIN (RU0009029540)
7. Категория (Обыкновенные / Привилегированные)
8. Количество в составе активов (8 305 478) ← positions
9. Стоимость актива (2 610 827 009) ← market value, RUB
10. Наименование биржи
11. Уровень иерархии справедливой стоимости
12. Примечание
```

**Особенности при парсинге PDF через pypdf:**
- ISIN может быть разрезан переносом строки: `RU0009062\n285` → нужно склеить
- Числа с пробелами как разделителями тысяч: `2 257 230` → убрать пробелы
- Между quantity и value часто \n или multi-space — но иногда склеиваются (нужна heuristic или pdfplumber)

---

## Реальные данные (примеры)

### SBMX (УК Первая) — DIFF февраль → март 2026 (20 изменений)

```
Эмитент                         Кт   Февраль          Март          Δ штук         Тип
ММК                             Обык         0    3 806 520    +3 806 520  🆕 ВОШЛА
АЛРОСА                          Обык         0    2 843 380    +2 843 380  🆕 ВОШЛА
ТАТНЕФТЬ                        Обык         0    1 583 290    +1 583 290  🆕 ВОШЛА
Т-ТЕХНОЛОГИИ                    Обык   300 529            0      -300 529  ❌ ВЫШЛА
ГАЗПРОМ                         Обык 15 898 330  15 792 660      -105 670  ➖ ПРОДАЖА
МКБ                             Обык 14 712 100  14 614 300       -97 800  ➖ ПРОДАЖА
ЛУКОЙЛ                          Обык    541 555     600 071       +58 516  ➕ ДОКУПКА
СБЕРБАНК                        Обык  8 251 805   8 305 478       +53 673  ➕ ДОКУПКА
ЯНДЕКС                          Обык    337 167     290 770       -46 397  ➖ ПРОДАЖА
...
ЛЕНТА                           Обык         0       27 657       +27 657  🆕 ВОШЛА
```

### TMOS (Т-Капитал) — DIFF февраль → март 2026 (10 изменений)

```
Эмитент                         Кт     Февраль         Март      Δ штук         Тип
МКБ                             Обык  10 523 900   9 913 900  -610 000  ➖ ПРОДАЖА
ЯНДЕКС                          Обык     229 355           0  -229 355  ❌ ВЫШЛА
ПИК                             Обык           0     194 154  +194 154  🆕 ВОШЛА
ТАТНЕФТЬ прив                   Прив     191 896           0  -191 896  ❌ ВЫШЛА
ГАЗПРОМ                         Обык  10 884 667  10 792 167   -92 500  ➖ ПРОДАЖА
ЛУКОЙЛ                          Обык     372 564     412 129   +39 565  ➕ ДОКУПКА
СБЕРБАНК                        Обык   5 666 321   5 699 235   +32 914  ➕ ДОКУПКА
ЛЕНТА                           Обык           0      18 310   +18 310  🆕 ВОШЛА
```

**Ключевой вывод:** snapshot не подсвечивает транзакции напрямую, но **DIFF двух последовательных snapshot'ов** даёт точную карту покупок/продаж. Внутри-месячные движения не видны (только net за период).

---

## Технический стек для реализации

| Задача | Технология | Обоснование |
|---|---|---|
| PDF парсинг | `pdfplumber` (не pypdf) | Лучше работает с таблицами — не склеивает числа |
| HTML парсинг (ВИМ) | `BeautifulSoup` / `lxml` | Стандарт |
| HTTP клиент | `aiohttp` | Уже используется в проекте (`fetch_funds_realtime.py`) |
| Антибот (АТОН) | `playwright` | Headless Chromium |
| Парсинг ISIN/чисел | regex + heuristics | Формат ОКУД полуструктурирован |
| Scheduling | cron (в проекте) | Уже есть daemon pattern |
| Retry | exponential backoff | На сайтах бывают downtime |

### Архитектурный скетч

```
Funds/
├── fetch_vim_holdings.py          # HTML parser для 3 фондов ВИМ (ежедневно)
├── fetch_ocsa_pdf.py              # Универсальный PDF parser OКУД 0420502
├── vendors/
│   ├── first_am.py                # Индекс-скрейпер + download для SBM*
│   ├── t_capital.py               # URL-генератор (прямой паттерн)
│   ├── alfacapital.py             # Индекс-скрейпер для AK*
│   └── aton.py                    # Playwright-based для AM*
└── HOLDINGS_RESEARCH.md           # (этот документ)
```

### Схема данных (проектные)

```sql
CREATE TABLE fund_holdings_daily (
    fund_id           INT NOT NULL,
    snapshot_date     DATE NOT NULL,
    isin              VARCHAR(12),
    asset_name        TEXT,
    asset_type        VARCHAR(32),      -- 'stock_common', 'stock_pref', 'bond_corp', 'bond_govt', 'cash', 'reit', ...
    category          VARCHAR(16),      -- 'Обык' / 'Прив' для акций
    ogrn              VARCHAR(13),
    inn               VARCHAR(12),
    positions         NUMERIC(20, 4),   -- штуки (для облигаций — номинал*шт)
    value_rub         NUMERIC(20, 2),
    weight            NUMERIC(7, 4),    -- % в портфеле
    nav_at_date       NUMERIC(20, 2),
    source            VARCHAR(32),      -- 'vim', 'first_am', 't_capital', 'alfa', 'aton'
    source_url        TEXT,
    ingested_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fund_id, snapshot_date, isin, category)
);

CREATE INDEX idx_holdings_fund_date ON fund_holdings_daily (fund_id, snapshot_date DESC);
CREATE INDEX idx_holdings_asset ON fund_holdings_daily (isin, snapshot_date DESC);

-- View для transactions (diff между последовательными snapshot'ами одного фонда)
CREATE VIEW fund_transactions AS
SELECT
    curr.fund_id,
    curr.snapshot_date,
    curr.isin,
    curr.asset_name,
    curr.category,
    COALESCE(prev.positions, 0) AS positions_prev,
    curr.positions AS positions_curr,
    curr.positions - COALESCE(prev.positions, 0) AS delta_positions,
    CASE
      WHEN prev.positions IS NULL AND curr.positions > 0 THEN 'entry'
      WHEN curr.positions IS NULL OR curr.positions = 0 THEN 'exit'
      WHEN curr.positions > COALESCE(prev.positions, 0) THEN 'buy'
      ELSE 'sell'
    END AS action,
    ABS(curr.positions - COALESCE(prev.positions, 0)) * curr.value_rub / NULLIF(curr.positions, 0) AS estimated_value_delta
FROM fund_holdings_daily curr
LEFT JOIN LATERAL (
    SELECT positions FROM fund_holdings_daily p
    WHERE p.fund_id = curr.fund_id AND p.isin = curr.isin AND p.category = curr.category
      AND p.snapshot_date < curr.snapshot_date
    ORDER BY p.snapshot_date DESC LIMIT 1
) prev ON TRUE
WHERE curr.positions IS DISTINCT FROM COALESCE(prev.positions, 0);
```

### API endpoints для frontend (проектные)

```
GET /api/funds/{id}/transactions?from=2026-01-01&to=2026-03-31
  → список действий УК (buy/sell/entry/exit) с датами и дельтами

GET /api/funds/{id}/holdings?date=2026-03-31
  → полный snapshot на дату

GET /api/assets/{isin}/fund-flows?from=...&to=...
  → "куда этот актив покупался/продавался по всем фондам"
  → например Сбер: +53k SBMX, +32k TMOS = нетто-приток в БПИФ
```

---

## План реализации (отложен)

### Фаза 1: ВИМ (1 день)
1. Парсер HTML страницы `/structure/` — таблица с positions
2. Парсер hidden xlsx endpoint для NAV history (доп. фича)
3. Миграция БД: `fund_holdings_daily`
4. Cron job в 23:30 МСК (после публикации T+1)
5. Интеграция с существующим `Funds/fetch_funds_realtime.py`

**Результат:** 3 фонда (LQDT, EQMX, GOLD) ежедневно.

### Фаза 2: PDF parser ОКУД 0420502 (1 день)
1. Универсальный парсер на `pdfplumber` — таблицы "Акции РФ АО", "Облигации", etc.
2. Robust парсинг чисел и ISIN (учесть переносы строк)
3. Unit-тесты на реальных PDF (есть примеры в `/tmp/holdings-sources/`)

### Фаза 3: Первая + Т-Капитал (1 день)
1. `vendors/first_am.py` — скрейпер `/documents` страниц + downloader
2. `vendors/t_capital.py` — URL generator (даты известны)
3. Интеграция с парсером Фазы 2

**Результат:** +12 фондов ежемесячно.

### Фаза 4: Альфа (0.5 дня)
1. `vendors/alfacapital.py` — скрейпер `/disclosure/pifs/{slug}/monthly`
2. Переиспользование PDF parser

**Результат:** +4 фонда.

### Фаза 5: АТОН (1-2 дня)
1. Playwright-based получение PDF
2. `cf_clearance` cookie handling
3. Обработка `e-disclosure.azipi.ru` как fallback

**Результат:** +2 фонда.

### Фаза 6: Frontend (1-2 дня)
1. API endpoints
2. Новая страница "Движения активов УК" или секция внутри Funds
3. Карточка фонда с историей действий

---

## Известные подводные камни

### PDF парсинг
- **pypdf склеивает числа** из таблиц: "2 257 230 106 992 702" → нужно отличить quantity от value
- **ISIN разбит переносом:** `RU0009062\n285` → pre-processing
- **Лучше использовать pdfplumber** — он сохраняет табличную структуру

### HTTP
- УК Первая отдаёт **HTTP 403** на default User-Agent → `Mozilla/5.0 Chrome/125`
- АТОН за Cloudflare → Playwright
- Кириллица в URL → `urllib.parse.quote`

### Версионирование и устойчивость
- **Сайты УК меняют вёрстку** (типовая проблема scrape'а)
- **ЦБ обновляет форму ОКУД** раз в 3-5 лет → придётся переписывать regex'ы
- **УК может ввести антибот** в любой момент → мониторинг

### Мониторинг parser'ов
Нужен telegram-alert если:
- PDF скачан, но не содержит ISIN'ов
- Количество записей резко упало (был 45, стало 5)
- Snapshot_date не сдвинулся за ожидаемое окно (задержка)
- HTTP 4xx/5xx ошибки

### Juridical
- **Законная публичная информация** (ОКУД 0420502 — обязательное раскрытие по закону РФ)
- Парсинг сайтов — grey area, но данные open
- Rate limit: держать ≤1 запрос/сек на одну УК, retry с backoff

---

## Полезные файлы на момент исследования

**В `/tmp/holdings-sources/`** (НЕ под git, временные):
- `sbmx_feb.pdf`, `sbmx_march.pdf` — реальные SBMX за 2 месяца (для тестов парсера)
- `tmos_feb.pdf`, `tmos_mar.pdf` — TMOS за 2 месяца

**В `/tmp/` (временные):**
- `cbonds_api.pdf` — PDF документация Cbonds Enterprise API
- `cbonds_schema.json` — 1.1MB JSON-схема всех 247 операций Enterprise API
- `cbonds-flows/dump.har` — HAR export 4802 запросов мобильного Cbonds

**В проекте:**
- `Funds/CBONDS_MIGRATION.md` — предыдущая миграция ISS MOEX → Cbonds (базовые NAV)
- `Funds/fetch_funds_realtime.py` — текущий daemon, пользуется `fetch_cbonds_holdings` (отдаёт только weight, не positions)
- `cbonds_capture.flow` — 56MB mitmproxy capture (в gitignore, не коммитить)

---

## Альтернативы (не рекомендуемые сейчас)

### Cbonds Pro подписка
- Email: `pro@cbonds.info`
- Даст немедленный доступ ко всем полям (positions, history, фильтры)
- **Стоимость:** уточнять. По опыту таких enterprise подписок — $500+/мес
- **Плюс:** немедленный прогресс без парсинга
- **Минус:** деньги каждый месяц

### InvestFunds.ru подписка
- Фактически тот же Cbonds
- Не рассматривать отдельно

### Обращение в УК напрямую
- Запросить у УК прямую выгрузку через партнёрство
- Долго, бюрократично
- Но потенциально — стабильный API без scraping

### Mobile app reverse engineering (глубокое)
- Cbonds Pro приложение может иметь endpoints с positions если у user Pro-подписка
- Но это обратный путь — лучше сразу Pro

---

## Состояние на момент разведки (2026-04-22)

- ✅ Все 21 фонд имеют **публичный ОКУД 0420502** по закону ЦБ РФ
- ✅ Паттерны URL определены для 19 из 21 фонда
- ⚠️ АТОН (2 фонда) требуют Cloudflare bypass
- ✅ Формат PDF единый — один парсер на все УК
- ✅ Реально протестирован парсер на SBMX + TMOS за 2 месяца — diff работает
- ⏸️ Имплементация отложена

---

## При возвращении к задаче

**Первый шаг:** прочитать этот документ + `Funds/CBONDS_MIGRATION.md`.

**Быстрый контекст для нового чата:**

> "Продолжаем задачу отслеживания покупок активов в БПИФ. Полная разведка в `Funds/HOLDINGS_RESEARCH.md`. Коротко: Cbonds мобильный не даёт positions, но парсинг сайтов УК работает — ОКУД 0420502 единый формат. Начинаем с Фазы 1 (ВИМ, ежедневно)."

**Артефакты для продолжения:**
- Пример рабочего PDF parser'а (version 1) есть в истории чата — давал правильные diff'ы для Сбера/Лукойла/Газпрома
- Пример URL для ВИМ xlsx endpoint известен: `FOND_ID=170898` для EQMX
- Реальные PDF в `/tmp/holdings-sources/` можно использовать для unit-тестов

---

*Документ подготовлен Claude (сессия 2026-04-22) на основе исследования 4 часа.*
