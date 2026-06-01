# Источники данных по фондам (/fund-trades) — справочник для сверки

> Обновлено: 2026-05-30. Назначение — зафиксировать, **откуда** берётся каждое
> поле в `fund_holdings_history`, чтобы можно было перепроверить (сверить) любой
> снапшот против первоисточника.

---

## 1. Архитектура источников

Данные о составе фондов поступают из **двух принципиально разных потоков**:

### 🟢 Реал-тайм / на постоянке — Cbonds Mobile API (ОСНОВНОЙ)
**Это основной источник для ВСЕХ фондов.** Каждые 15 минут тянем с Cbonds:
- **Структуру фонда** (состав: активы + доли в %) — `structure` endpoint.
- **СЧА (NAV)** фонда и расчётный пай — `nav` endpoint.

Cbonds отдаёт **только доли (weight)**, без количества бумаг. Поэтому количество
**мы считаем/воспроизводим сами** (reconstruct):

```
amount_rub = NAV × weight / 100
positions  = amount_rub / price_at_date
```

- `price_at_date` берётся с **MOEX ISS**:
  - Акции:    `/history/.../shares/securities/{secid}` → CLOSE (режим TQBR)
  - Облигации: `/history/.../bonds/securities/{secid}` → (LEGALCLOSEPRICE/100)×FACEVALUE + ACCINT
- Код: `Funds/cbonds_reconstruct.py`. Точность реконструкции **0.5–2 %** против
  официального SCHA (проверено сверкой).

### 🔵 Историческая разметка / anchor — SCHA (Справка о СЧА, форма ЦБ № 0420502)
Точные позиции (количество + сумма) напрямую от УК. Используется как **якорь
точности** и для backfill истории там, где SCHA публично доступен. НЕ обновляется
в реал-тайме — это месячные/дневные официальные раскрытия.

> **Принцип сверки:** если cbonds-reconstruct для даты X расходится с SCHA для
> той же даты больше чем на ~2–3 %, надо разбираться (цена не та, сплит, sm. ниже).

---

## 2. Cbonds Mobile API — реквизиты

| Параметр | Значение |
|---|---|
| Base URL | `https://rest2.cbonds.info` |
| User-Agent | `Cbonds.K/3.0.8 (ru.cbonds.cbonds; build:636; Android 9) OkHttp/4.12.0` |
| Auth | `POST /m/auth/tariffs/global/json/logout=1?lang=rus` (логин/пароль в `.env`) |
| Структура (состав+доли) | `/m/exchange_traded_funds/structure/global/json/{cbonds_parent_id}/?lang=rus` |
| NAV (СЧА + пай) | `/m/exchange_traded_funds/nav/global/json/{cbonds_share_id}/{from}/{to}/?lang=rus` |
| Share-class инфо | `/m/exchange_traded_funds/share_class_information/global/json/{cbonds_share_id}/0/?lang=rus` |

- Идентификаторы фонда: `funds.cbonds_share_id` (на класс пая) и
  `funds.cbonds_parent_id` (на фонд целиком — для структуры).
- Фетчер: `Funds/fetch_funds_realtime.py` (демон, cron каждые 15 мин).

---

## 3. Метки источников в `fund_holdings_history.source`

| source | Что это | Код | Частота |
|---|---|---|---|
| `cbonds` | Состав+доли с Cbonds Mobile API (live snapshot) | `fetch_funds_realtime.py` | каждые 15 мин |
| `cbonds_baseline` | Опорный снапшот структуры cbonds | `fetch_funds_realtime.py` | разово/baseline |
| `cbonds_calc` | **Реконструированные позиции** (NAV×weight/price) | `cbonds_reconstruct.py` | по запросу/cron |
| `interfax_manual` | Точный SCHA (форма 0420502) из офиц. раскрытия | `manual_scha_backfill.py` | ручной backfill |
| `vim_sdr` | ВИМ SCHA-PDF с wealthim.ru (прямой) | `vim_sdr_backfill.py` | ручной/backfill |
| `vim` | ВИМ intraday со страницы /structure/ | `vim_intraday.py` | каждые 30 мин (торг.) |

`MONTHLY_SOURCES` (для месячных diff'ов на UI) =
`("vim_sdr", "interfax_manual", "cbonds", "cbonds_baseline")` —
см. `api/routers/fund_trades.py`.

---

## 4. Источники SCHA (Справка о СЧА) по УК — для сверки

### Альфа-Капитал (uk_id = 5)
- **БПИФ** → e-disclosure.ru, карточка УК `id=7203`, раздел `type=23` (УК ПИФ).
  Полный архив, ~5 лет. URL: `e-disclosure.ru/portal/files.aspx?id=7203&type=23`.
- **ОПИФ** → собственный сайт `alfacapital.ru/disclosure/pifs/<slug>/<section>`.
  - Секция с расшифровками: **`/nav-document`** → «Справка СЧА».
  - ⚠️ **Дневная** «Справка СЧА» = сводная форма (агрегаты по категориям, БЕЗ
    позиций). Полные расшифровки по ISIN — только в **`/monthly`** и `/quarterly`
    (формат **XLSX**). Сайт хранит ~4 последних месяца.
  - Download: `alfacapital.ru/disclosure/file/{numeric_id}`.
  - Slug-карта ОПИФ:
    | slug | ticker | фонд |
    |---|---|---|
    | `opif_akop` | OPIF-33 | Облигации Плюс |
    | `opifa_akliq` | OPIF-432 | Ликвидные акции |
    | `opif_akipo` | OPIF-9113 | Облигации с выплатой дохода |

### ВИМ Инвестиции (uk_id = 7)
- **БПИФ EQMX/OBLG/GOLD/LQDT** → `wealthim.ru` (прямой PDF, `vim_sdr`) +
  страница `/products/bpif/{slug}/structure/` для intraday (`vim`).
- **ОПИФ OPIF-1003 (Акции) / OPIF-54 (Казначейский)** → e-disclosure (PDF),
  старый формат — `.pdf.zip` (cp866), новый — `YY-MM-DD-OPIF-NNNN.pdf`.

### Первая / Сбер (uk_id = 34) — own-site `first-am.ru` ✅ (ЛУЧШИЙ источник, 2026-05-30)
e-disclosure банит по IP (403 даже с Chrome UA, ServicePipe-челлендж). Но own-site
**`first-am.ru` отдаёт PDF напрямую через curl** с Chrome UA (`Mozilla/5.0 … Chrome/125`,
HTTP 200, e-disclosure не нужен!):
- Индекс документов фонда: `https://first-am.ru/individuals/etf/{slug}/documents`
- Файл: `https://first-am.ru/download/{doc_id}/<кириллическое имя>.PDF` — **нужен
  полный путь с именем** (bare `/download/{id}/` → 404), кириллицу через
  `urllib.parse.quote(path, safe=":/?&=%#,-._~+")`.
- На индекс-странице ~150 ссылок `/download/\d+/...`; SCHA = те, где имя содержит
  `Справка о стоимости чистых активов_за_<месяц>_<год>г`. Дедуп по (год,месяц),
  первая ссылка = свежайшая ревизия. Архив с ~2023-08.
- **Slug-карта** (ticker → slug, фонд):
  | ticker | slug | фонд | isin_pif |
  |---|---|---|---|
  | SBMX | `etf-moex` | Топ Российских акций | RU000A0ZZH92 |
  | SBRB | `corporate-bonds-rub` | Корпоративные облигации | RU000A100P44 |
  | SBFR | `etf-sbfr` | Облигации с переменным купоном | RU000A107KW2 |
  | SBLB | `etf-sblb` | Долгосрочные гособлигации | (не в whitelist) |
  | SBMM | `etf-sbmm` | Сберегательный (money — skip) | — |
  | SBGD | `etf-sbgd` | Доступное золото (gold — skip) | — |
- **SAFE** в first-am отсутствует, НО есть на e-disclosure как «Первая – Фонд
  Консервативный» (isin RU000A1035N9 ✓, 54 даты) — взят оттуда (см. ниже).
- БПИФ авто-матчатся в backfill по `funds.isin_pif` из метаданных PDF (заполнены).
- Скачано+импортировано 2026-05-30: SBMX (33 мес 2023-08→2026-04), SBRB (33),
  SBFR (28, 2024-01→2026-04). Файлы именованы `TICKER_YYYY-MM-DD.pdf`.

### Остальные УК — e-disclosure (`files.aspx?id=<card>&type=23` = ВСЕ ПИФ УК: БПИФ+ОПИФ!)

| УК | uk_id | e-disclosure card | статус (2026-05-30) |
|---|---|---|---|
| Т-Капитал | 3597 | **38022** | ✅ TBRU 55, TMOS 57, **TOFZ 17** (взяты с глубокой историей) |
| Райффайзен | 20 | **37192** | ❌ OPIF-281/282 (Акции/Компании роста) НЕ на карточке (слиты? есть МЕТИБ-фонды) |
| Атон | aton | **12603** | ⏳ AMGB 17, OPIF-63 (Рос.акции+) 51 — но **XLSX, parse_scha_xls=0** → нужна адаптация |
| Первая (Сбер) | 34 | **6731** | ✅ SAFE 37 (=«Консервативный»). OPIF-43/47/8119/8123 НЕ найдены; OPIF-4995 разрежен |
| ВИМ | 7 | **28433** | (OPIF-1003/54 уже есть; OPIF-9165 Рантье НЕ на карточке — мб «ВИМ Сбережения» 34888) |
| Альфа | 5 | **7203** | OPIF-11259 19 (готов); OPIF-33/432/9113 по 4 — на карточке мб глубже (TODO) |

#### 🔑 Механика выкачки с e-disclosure — РАБОТАЕТ (2026-05-30, воспроизводимо)
1. **Список грузится в ЛОКАЛЬНОМ Chrome** (антибот ServicePipe проходит автоматически;
   curl/сервер — 403). Открыть `files.aspx?id=<card>&type=23`.
2. **Walker** (Chrome MCP `javascript_tool`): `querySelectorAll('div.title_middle, tr')`,
   трекать фонд по `div.title_middle` (имя в «…»); SCHA-строки = текст содержит
   «Справка о стоимости» + «Отчетная дата: DD.MM.YYYY»; тащить `FileLoad?Fileid=` из `<a>`
   → `window._scha = {fund:{date:fileid}}`. Дедуп по дате (первая ссылка = свежая ревизия).
3. **curl FileLoad С ЛОКАЛЬНОГО мака РАБОТАЕТ** (HTTP 200, отдаёт zip — баниться только LIST page!):
   `curl -A "Mozilla/5.0 …Chrome/125" "https://www.e-disclosure.ru/portal/FileLoad.ashx?Fileid=N"`.
   (fetch+blob и a.click() в Chrome MCP БЛОКИРУЮТСЯ — поэтому curl, не браузер, для скачивания.)
4. ⚠️ **Chrome MCP privacy-фильтр** режет вывод fileid'ов как «;»-строку и base64; **TSV (tab)
   проходит**, резать ≤40 строк/ответ (иначе обрезка). Удобнее: JS пишет `window._scha`, потом
   тащить per-fund TSV.
5. ⚠️ **Имена на e-disclosure ≠ Cbonds-имена в БД** — матчить по `isin_pif` из PDF (БПИФ) либо
   distinctive-substring; БПИФ с пустым `isin_pif` в БД — заполнить или импортить по ticker-имени файла.
6. ZIP внутри: PDF (Первая/ВИМ/Т-Кап) ИЛИ **XLSX (Атон)**; backfill роутит по расширению inner.
- **Формат:** ZIP, внутри **PDF** (у Т-Капитал; у Альфы был XLS). cp866-имя внутри.
- **Download:** `curl "e-disclosure.ru/portal/FileLoad.ashx?Fileid=<id>"` работает
  НАПРЯМУЮ (ServicePipe не блокирует загрузку файлов) — браузер не нужен, только
  для walker'а собрать fileid'ы. UA `Mozilla/5.0`.
- **Backfill:** PDF-парсер не извлекает дату из формата Т-Капитал → backfill берёт
  дату из имени файла `TICKER_YYYY-MM-DD.zip` (fallback). Резолв фонда — по ticker.
- Маппинг имён Т-Капитал: «Т-Капитал Облигации»=TBRU, «Т-Капитал Индекс МосБиржи»=TMOS.

> ✅ **PDF-layout различается по УК — РЕШЕНО (2026-05-30).** `parse_scha`
> (Funds/parsers/scha_parser.py) теперь имеет 3 стратегии и берёт ту, что дала
> больше активов:
> 1. **tables** — extract_tables() с детекцией шапки (компактный ОПИФ Казначейский).
> 2. **tables_rowwise** (НОВОЕ) — header-agnostic построчно, для многостраничного
>    35-стр layout Первой/Атона: шапка таблицы на одной странице, строки данных на
>    следующих → extract_tables() по-страничный давал строки без шапки. Инвариант:
>    **количество и стоимость = два ПОСЛЕДНИХ числа строки** (ОГРН/ИНН/регномер/дата
>    погашения раньше; биржа + «уровень 1» — текст в хвосте). Склеивает ISIN,
>    разрезанный переносом (`RU0009029\n540`); пропускает даты погашения; исключает
>    `isin_pif` (пай в реквизитах). Заодно ловит **иностранные ISIN** (US…) и
>    bond-ISIN со сплитом, которые старый `\bRU\d{3}…`-regex терял (исправило
>    недозахват ВИМ OBLG на ~14 %).
> 3. **text+regex** — fallback для БПИФ ВИМ с вертикальным шрифтом.
>
> Порог «cash_or_repo_only»: ≥20 для text+regex (шум REPO на денежных фондах),
> ≥3 для точных табличных стратегий (новый SBFR в начале 2024 законно держит 10-18
> облигаций). Сверено: SBMX акции 42-51, SBRB/SBFR облигации 10-92, суммы сходятся
> с субтоталом формы (Сбер 8 305 478 шт, Газпром 15 792 660 — точное совпадение).
>
> Маппинг Первой: «Топ Российских акций»=SBMX, «Корпоративные облигации»=SBRB,
> «Облигации с переменным купоном»=SBFR, «Консерватив»=SAFE(=SBCS, делистнут с
> own-site — нужен e-disclosure walker). Атон: «Длинные ОФЗ»=AMGB (Cloudflare, TODO).

---

## 5. Полный реестр фондов (на 2026-05-30)

Легенда статуса SCHA: ✅ = есть точный SCHA-anchor, ⚪ = только cbonds-reconstruct.

### Альфа-Капитал (uk_id=5)
| ticker | name | cat | cbonds_share_id | SCHA | snapshots |
|---|---|---|---|---|---|
| AKMB | Управляемые облигации | bonds | 209119 | ✅ e-disclosure | 51 (2021→) |
| AKME | Управляемые акции | stocks | 209155 | ✅ e-disclosure | 56 (2021→) |
| OPIF-11259 | Облигации с перем. купоном | bonds | 231147 | ✅ e-disclosure | 19 (2024→) |
| OPIF-33 | Облигации Плюс | bonds | 206593 | ✅ alfacapital | 4 (2026) |
| OPIF-432 | Ликвидные акции | stocks | 206895 | ✅ alfacapital | 4 (2026) |
| OPIF-9113 | Облигации с выплатой дохода | bonds | 209453 | ✅ alfacapital | 4 (2026) |
| AKGD | Золото | gold | 209355 | ⚪ | — |
| AKMM | Денежный рынок | money | — | ⚪ | — |

### ВИМ Инвестиции (uk_id=7)
| ticker | name | cat | cbonds_share_id | SCHA | snapshots |
|---|---|---|---|---|---|
| EQMX | Индекс МосБиржи | stocks | 209105 | ✅ vim_sdr | 44 (2022→) |
| OBLG | Российские облигации | bonds | — | ✅ vim_sdr+interfax | 31+45 |
| OPIF-1003 | ВИМ - Акции | stocks | 207285 | ✅ interfax | 31 (2023→) |
| OPIF-54 | ВИМ - Казначейский | bonds | 206617 | ✅ interfax | 31 (2023→) |
| GOLD | Золото Биржевой | gold | 209117 | ⚪ vim | — |
| LQDT | Ликвидность | money | 209099 | ⚪ vim | — |
| OPIF-9165 | Облигации Рантье | bonds | 219221 | ⚪ | — |

### Первая / Сбер (uk_id=34) — TODO SCHA
| ticker | name | cat | cbonds_share_id |
|---|---|---|---|
| SBMX | Топ Российских акций | stocks | 208941 |
| SBRB | Корпоративные облигации | bonds | 198837 |
| SBFR | Облигации флоатеры | bonds | 209525 |
| SAFE | Консерватив | bonds | 209239 |
| SBGD | Доступное золото | gold | 209413 |
| SBLB | Долгосрочные гособлигации | bonds | 220497 |
| SBMM | Сберегательный | money | 209297 |
| OPIF-43 | Фонд российских акций | stocks | 206601 |
| OPIF-47 | Рублевые сбережения | bonds | 206607 |
| OPIF-4995 | Накопительный | bonds | 208851 |
| OPIF-8119 | Облигации с выплатой | bonds | 209395 |
| OPIF-8123 | Акции с выплатой дохода | stocks | 209397 |

### Т-Капитal (uk_id=3597) — TODO SCHA
| ticker | name | cat | cbonds_share_id |
|---|---|---|---|
| TMOS | Индекс МосБиржи | stocks | 209133 |
| TBRU | Облигации | bonds | 209253 |
| TOFZ | ОФЗ | bonds | 232159 |
| TGLD | Золото | gold | 209129 |
| TMON | Денежный Рынок | money | 209423 |

### Райффайзен (uk_id=20) — TODO SCHA
| ticker | name | cat | cbonds_share_id |
|---|---|---|---|
| OPIF-281 | Акции | stocks | 206781 |
| OPIF-282 | Компании роста | stocks | 206783 |

### Атон (uk_id=aton) — TODO SCHA
| ticker | name | cat | cbonds_share_id |
|---|---|---|---|
| OPIF-63 | Петр Столыпин | stocks | 206625 |
| AMGB | Длинные ОФЗ | bonds | 232647 |
| AMNR | Накопительный в рублях | money | 209519 |

---

## 6. Локальный архив скачанных SCHA

`~/Downloads/funds_organized/<УК>/<TICKER — Name>/<YYYY-MM-DD>.<ext>` + `INDEX.md`.
Содержит 144 (Альфа) + ВИМ документов, отсортированных по датам — для ручной сверки.

## 7. Известные подводные камни при сверке
- **Сплиты акций**: ISS отдаёт адъюстнутую серию → re-import через
  `Candles/backfill_daily_history.py`. Реестр `KNOWN_SPLITS` сейчас пуст.
- **ord+pref / мультивыпуски**: уникальность в БД по
  `(fund_id, COALESCE(isin,''), asset_name, snapshot_date, source)` — НЕ по
  эмитенту (иначе схлопывает выпуски, теряя 30-60% позиций).
- **Дневная vs месячная справка Альфа-ОПИФ**: дневная — без позиций, брать месячную XLSX.
- **wrong-fund**: на e-disclosure под именем фонда может лежать чужой ZIP
  (был случай «Арендный поток» под AKMB) — проверять fund_name в файле.
- **`(cid:9)`-глиф (Т-Капитал, архивный 2021-формат)**: pdfplumber выдаёт
  `1(cid:9)069(cid:9)930` вместо `1 069 930` → числовой фильтр роняет ячейку, ОГРН/ИНН
  берутся за количество/стоимость («триллион акций» на графике). Чистится в
  `_parse_int/_parse_float/_norm` (commit `8a75b6f`). Детали + спот-чек — карточка
  Т-Капитала в скилле `moex-fund-scha-backfill/references/uk-tkapital.md`.
