# Все ссылки Вадима дословно + карта кодов показателей

Собрано 03.09.2026. Проверены все 50 ссылок с `?field=`: **работают 50/50**, покрывают 48 разных показателей.

⚠️ Главная ценность этих ссылок не в данных (там только последний год и LTM, истории нет), а в том, что они дают **машинные коды показателей**. В таблицах на страницах бумаги показатели подписаны по-русски («Опер.денежный поток, млрд руб»), а здесь у того же показателя есть код `ocf`. Это и есть готовый `metric_code` для длинного формата — иначе пришлось бы придумывать свои имена и городить словарь по русским подписям.

Две страницы = два разреза одних и тех же показателей:

- `shares_fundamental` — последний отчётный **год** + колонка «Изм. %, г/г»;
- `shares_fundamental2` — **LTM**, скользящие 12 месяцев + колонка «отчет» (напр. `LTM-МСФО`).

Разница видна на Роснефти: добыча нефти 181,1 млн т за год против 181,6 за LTM.

## Карта: код → показатель

| код | показатель | страницы | бумаг с данными |
|---|---|---|---|
| `market_cap` | Капитализация, млрд руб | 1 | 232 |
| `ev` | EV, млрд руб | 1 | 232 |
| `book_value` | Баланс стоимость, млрд руб | 1 | 232 |
| `number_of_shares` | Число акций ао, млн | 1 | 231 |
| `div_yield` | Див доход, ао, % | 1 | 226 |
| `roe` | ROE, % | 1 | 224 |
| `net_income` | Чистая прибыль, млрд руб | 2 | 223 |
| `p_e` | P/E | 2 | 223 |
| `common_share` | Цена акции ао, руб | 1 | 222 |
| `opex` | Опер. расходы, млрд руб | 1 | 219 |
| `roa` | ROA, % | 1 | 219 |
| `net_assets` | Чистые активы, млрд руб | 1 | 218 |
| `bv_share` | BV/акцию, руб | 1 | 217 |
| `employment_expenses` | Расх на персонал, млрд руб | 1 | 211 |
| `assets` | Активы, млрд руб | 2 | 211 |
| `p_bv` | P/BV | 2 | 211 |
| `operating_income` | Операционная прибыль, млрд руб | 1 | 210 |
| `capex` | CAPEX, млрд руб | 1 | 209 |
| `revenue` | Выручка, млрд руб | 2 | 208 |
| `ocf` | Операционный денежный поток, млрд руб | 1 | 207 |
| `cash` | Наличность, млрд руб | 2 | 207 |
| `net_debt` | Чистый долг, млрд руб | 2 | 207 |
| `net_margin` | Чистая рентаб, % | 2 | 207 |
| `p_s` | P/S | 2 | 207 |
| `capex_revenue` | CAPEX/Выручка, % | 1 | 206 |
| `debt` | Долг, млрд руб | 2 | 204 |
| `amortization` | Амортизация, млрд руб | 1 | 196 |
| `interest_expenses` | Процентные расходы, млрд руб | 1 | 194 |
| `fcf_share` | FCF/акцию, руб | 1 | 193 |
| `p_fcf` | P/FCF | 1 | 189 |
| `cost_of_production` | Себестоимость, млрд руб | 2 | 189 |
| `fcf_yield` | Доходность FCF, % | 1 | 186 |
| `ebitda` | EBITDA, млрд руб | 2 | 182 |
| `ebitda_margin` | Рентаб EBITDA, % | 2 | 182 |
| `ev_ebitda` | EV/EBITDA | 2 | 182 |
| `debt_ebitda` | Долг/EBITDA | 2 | 177 |
| `employees` | Персонал, чел | 1 | 164 |
| `labour_productivity` | Производительность труда, млн руб/чел/год | 1 | 161 |
| `expenses_per_employee` | Расходы/чел/год, тыс.р | 1 | 158 |
| `dividend_payout` | Див.выплата, млрд руб | 1 | 103 |
| `div_payout_ratio` | Дивиденды/прибыль, % | 1 | 103 |
| `dividend` | Дивиденд, руб/акцию | 1 | 100 |
| `free_float` | Free Float, % | 1 | 92 |
| `net_income_ns` | Чистая прибыль н/с, млрд руб | 1 | 19 |
| `r_and_d_capex` | R&D/CAPEX, % | 1 | 14 |
| `oil_production` | Добыча нефти, млн т | 1, 2 | 4 |
| `oil_refining` | Переработка нефти, млн т | 1, 2 | 3 |
| `gas_production` | Добыча газа, млрд м3 | 1 | 2 |

Страница 1 = `shares_fundamental`, 2 = `shares_fundamental2`. «Бумаг с данными» — сколько тикеров реально отдаёт страница; для отраслевых метрик это единицы (добыча нефти — 4), для универсальных 200+.

## Ссылки дословно, как присланы

### Точка входа и разбор одной компании (ROSN)

- https://smart-lab.ru/q/shares_fundamental2/ — сводная таблица, LTM — точка входа
- https://smart-lab.ru/q/ROSN/f/ — годовая МСФО (редирект на /f/y/)
- https://smart-lab.ru/q/ROSN/f/y/ — годовая отчётность, 5 лет + LTM
- https://smart-lab.ru/q/ROSN/f/y/MSFO/ — то же явно МСФО; здесь же ссылки на PDF и «факторы роста и падения»
- https://smart-lab.ru/q/ROSN/f/q/MSFO/ — квартальная МСФО, 5 кварталов + LTM
- https://smart-lab.ru/q/ROSN/r/y/MSFO/ — изменения год к году, в % и п.п.
- https://smart-lab.ru/q/ROSN/r/q/MSFO/ — изменения квартал к кварталу
- https://smart-lab.ru/q/ROSN/shareholders/ — структура акционеров + дата обновления структуры
- https://smart-lab.ru/q/ROSN/dividend/ — история дивидендов

Вкладки отчётности на тех же страницах: `/MSFO/`, `/RSBU/`, у некоторых `/US GAAP/` (у Роснефти есть все три).

### financemarker (решено не брать: ПРО 998 ₽/мес только для личного использования)

- https://financemarker.ru/api/ — описание API
- https://financemarker.ru/api/fm/v2/stocks — информация о компании: сектор, отрасль GICS, описание, сайт, ссылка на раскрытие
- https://financemarker.ru/api/fm/v2/stocks/{exchange}:{code}?include=reports — отчётность, 3 формы, с 2011
- https://financemarker.ru/api/fm/v2/stocks/{exchange}:{code}?include=ratios — мультипликаторы
- https://financemarker.ru/api/fm/v2/stocks/{exchange}:{code}?include=summary — сводные метрики: рост 3/5 лет, Грэм, Линч, консенсус
- https://financemarker.ru/api/fm/v2/stocks/{exchange}:{code}?include=operations — операционные показатели с единицами измерения
- https://financemarker.ru/api/fm/v2/stocks/{exchange}:{code}?include=dividends — дивиденды с датами отсечек
- https://financemarker.ru/api/fm/v2/insider_transactions — сделки инсайдеров (в примерах только США)
- https://financemarker.ru/api/fm/v2/calendar — календарь событий: отчёты, СД, ГОСА

Чего у financemarker есть, а у smart-lab нет: описание компании и классификация GICS, единый календарь событий, история отчётности с 2011 года, единицы измерения у операционных метрик. Если понадобится — запрашивать коммерческое КП, а не ПРО.

### Все 50 ссылок с `?field=`

**Последний год + изм. г/г** (`shares_fundamental`):

- https://smart-lab.ru/q/shares_fundamental/?field=oil_production — Добыча нефти, млн т
- https://smart-lab.ru/q/shares_fundamental/?field=oil_refining — Переработка нефти, млн т
- https://smart-lab.ru/q/shares_fundamental/?field=gas_production — Добыча газа, млрд м3
- https://smart-lab.ru/q/shares_fundamental/?field=operating_income — Операционная прибыль, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=net_income_ns — Чистая прибыль н/с, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=ocf — Операционный денежный поток, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=capex — CAPEX, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=dividend_payout — Див.выплата, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=dividend — Дивиденд, руб/акцию
- https://smart-lab.ru/q/shares_fundamental/?field=div_yield — Див доход, ао, %
- https://smart-lab.ru/q/shares_fundamental/?field=div_payout_ratio — Дивиденды/прибыль, %
- https://smart-lab.ru/q/shares_fundamental/?field=opex — Опер. расходы, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=amortization — Амортизация, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=employment_expenses — Расх на персонал, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=interest_expenses — Процентные расходы, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=net_assets — Чистые активы, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=common_share — Цена акции ао, руб
- https://smart-lab.ru/q/shares_fundamental/?field=number_of_shares — Число акций ао, млн
- https://smart-lab.ru/q/shares_fundamental/?field=free_float — Free Float, %
- https://smart-lab.ru/q/shares_fundamental/?field=market_cap — Капитализация, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=ev — EV, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=book_value — Баланс стоимость, млрд руб
- https://smart-lab.ru/q/shares_fundamental/?field=fcf_share — FCF/акцию, руб
- https://smart-lab.ru/q/shares_fundamental/?field=bv_share — BV/акцию, руб
- https://smart-lab.ru/q/shares_fundamental/?field=fcf_yield — Доходность FCF, %
- https://smart-lab.ru/q/shares_fundamental/?field=roe — ROE, %
- https://smart-lab.ru/q/shares_fundamental/?field=roa — ROA, %
- https://smart-lab.ru/q/shares_fundamental/?field=p_fcf — P/FCF
- https://smart-lab.ru/q/shares_fundamental/?field=employees — Персонал, чел
- https://smart-lab.ru/q/shares_fundamental/?field=labour_productivity — Производительность труда, млн руб/чел/год
- https://smart-lab.ru/q/shares_fundamental/?field=expenses_per_employee — Расходы/чел/год, тыс.р
- https://smart-lab.ru/q/shares_fundamental/?field=r_and_d_capex — R&D/CAPEX, %
- https://smart-lab.ru/q/shares_fundamental/?field=capex_revenue — CAPEX/Выручка, %

**LTM** (`shares_fundamental2`):

- https://smart-lab.ru/q/shares_fundamental2/?field=oil_production — Добыча нефти, млн т
- https://smart-lab.ru/q/shares_fundamental2/?field=oil_refining — Переработка нефти, млн т
- https://smart-lab.ru/q/shares_fundamental2/?field=revenue — Выручка, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=ebitda — EBITDA, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=net_income — Чистая прибыль, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=cost_of_production — Себестоимость, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=assets — Активы, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=debt — Долг, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=cash — Наличность, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=net_debt — Чистый долг, млрд руб
- https://smart-lab.ru/q/shares_fundamental2/?field=ebitda_margin — Рентаб EBITDA, %
- https://smart-lab.ru/q/shares_fundamental2/?field=net_margin — Чистая рентаб, %
- https://smart-lab.ru/q/shares_fundamental2/?field=p_e — P/E
- https://smart-lab.ru/q/shares_fundamental2/?field=p_s — P/S
- https://smart-lab.ru/q/shares_fundamental2/?field=p_bv — P/BV
- https://smart-lab.ru/q/shares_fundamental2/?field=ev_ebitda — EV/EBITDA
- https://smart-lab.ru/q/shares_fundamental2/?field=debt_ebitda — Долг/EBITDA

## Как это использовать

1. **Коды в `metric_code`.** Показатель в базе хранится под кодом smart-lab (`revenue`, `ebitda`, `ocf`, `net_debt`, `p_e`, `oil_production`…), русская подпись — в справочнике метрик рядом с единицей измерения.
2. **Основной забор — со страниц бумаги** (`/f/y/` и `/f/q/`): там история за 5 лет и 5 кварталов. Страницы `?field=` истории не дают.
3. **`?field=` как перекрёстная проверка**: 50 запросов покрывают все 220+ бумаг разом. Удобно сверять, что построчный парсинг не разъехался с сайтом, и ловить бумаги, которых нет в сводной таблице (по `market_cap` и `number_of_shares` страница отдаёт 232 тикера против 223 в сводной).
4. **Отраслевые метрики видны сразу**: `oil_production` — 4 бумаги, `gas_production` — 2, `r_and_d_capex` — 14. Это готовый список «у кого вообще есть эта строка».

---
Сгенерировано `links_probe.py` → `field_codes.json`.
