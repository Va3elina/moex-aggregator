# Ежемесячное обновление SCHA (состав фондов /fund-trades)

> **Назначение:** раз в месяц добрать свежий месячный снапшот состава фондов
> (Справка о СЧА, форма ЦБ № 0420502) и занести в `fund_holdings_history`
> (`source='interfax_manual'`). Публикуется ~через **15-25 дней** после конца
> месяца. Парсер уже умеет PDF (Первая/ВИМ/Т-Капитал) и XBRL-Excel (Атон).

---

## Как это работает (workflow раз в месяц)

1. **Часть фондов Claude забирает сам** (сайты отдаются прямым curl — браузер не нужен):
   Первая (first-am.ru), Т-Капитал (cdn), ВИМ (wealthim.ru). Просто скажи
   «добери SCHA за <месяц>» — я скачаю, распаршу, занесу.
2. **Часть нужно скачать тебе в браузере** (e-disclosure банит сервер по IP,
   а твой локальный Chrome проходит): **Атон**, и при желании — глубокая история
   Альфа-ОПИФ. Открой ссылку → скачай «Справка о стоимости чистых активов» за
   нужный месяц → пришли мне файлы (zip/pdf/xlsx как есть).
3. Я проверяю: assets>0, Σвесов≈100%, спот-чек значений → пишу в БД.

**Именование при отправке** (если шлёшь файлы): удобно `ТИКЕР_ГГГГ-ММ-ДД`
(напр. `SBMX_2026-05-31`), но не обязательно — матчу по `isin_pif` из метаданных.

---

## Ссылки по УК

### 🟢 Первая / Сбер — own-site `first-am.ru` (Claude curl'ит сам)
Индекс: `https://first-am.ru/individuals/etf/{slug}/documents` → файл
`Справка о стоимости чистых активов_за_<месяц>_<год>`.

| Тикер | slug | Имя фонда |
|---|---|---|
| SBMX | `etf-moex` | Топ Российских акций |
| SBRB | `corporate-bonds-rub` | Корпоративные облигации |
| SBFR | `etf-sbfr` | Облигации с переменным купоном |

**SAFE** на first-am НЕТ → e-disclosure (ниже), имя **«Первая – Фонд Консервативный»**.

> ⚠️ **ОПИФ Первой — на first-am под ДРУГИМ путём** `/individuals/fund/{slug}` (не `/etf/`),
> и их **НЕТ на Интерфаксе** (карточка 6731 = только БПИФ/ЗПИФ!). Акционерные ОПИФ:
> | Тикер | slug (`/individuals/fund/…`) | Имя |
> |---|---|---|
> | OPIF-43 | `opif-aktsiy-fond-ros-aktsiy` | Фонд российских акций |
> | OPIF-8123 | `stock-fund-with-income-payment` | Фонд акций с выплатой дохода |
> У ОПИФ `isin_pif` в PDF не извлекается → backfill матчит по тикеру из имени файла
> (`OPIF-43_ГГГГ-ММ-ДД.pdf`). Прочие ОПИФ Первой (облигационные) скрыты (фича = только акции).

### 🟢 Т-Капитал — own-site (предсказуемый URL, Claude curl'ит сам)
`https://cdn.t-capital-funds.ru/static/documents/otchet-scha-{ticker}-ДД-ММ-ГГГГ.PDF`
(напр. `otchet-scha-tmos-31-05-2026.PDF`). Тикеры: **TBRU** (Облигации),
**TMOS** (Индекс МосБиржи), **TOFZ** (ОФЗ).

### 🟡 ВИМ — own-site `wealthim.ru` (листинг curl'ится, но PDF — через браузер/аттач)
Только акционерные (видимые в фиче):
| Тикер | Ссылка (own-site reports) |
|---|---|
| EQMX (Индекс МосБиржи, БПИФ) | `wealthim.ru/about/disclosure/pif/bpif/wimfimb/reports/` |
| OPIF-1003 (Фонд Акций, ОПИФ) | `wealthim.ru/about/disclosure/pif/opif/wimfa/documents/reports/` |
Имена PDF: `ГГ_ММ_ДД_SCHA_BPIF_IndeksMosBirzhi.pdf` / `..._SCHA_OPIF_Aktsii.pdf`.
(OPIF-1003=slug `wimfa` подтверждён сверкой; `wimfeqr` = ДРУГОЙ фонд «Рос. эмитенты».)

> ⚠️ **curl листинга `/reports/` отдаёт имена файлов, но прямой GET самого PDF
> возвращает SPA-shell (~74 КБ HTML), не PDF** (проверено 2026-05-31, хранятся
> только последние ~9 мес). → качай EQMX/OPIF-1003 PDF в браузере или пришли мне
> аттачем. **Парсить строго `parse_scha` (rowwise), НЕ старый vim_sdr-скрейпер** —
> у него баг ×1000 (см. ниже). Заносить как `source='vim_sdr'` (чтобы фича подхватила),
> но позиции из `parse_scha`.

### 🟢 Альфа — own-site `alfacapital.ru` (раздел «Ежемесячно» = XLSX с позициями)
| Тикер | Ссылка |
|---|---|
| AKME (Управляемые акции, БПИФ) | `alfacapital.ru/disclosure/pifs/bpif_akmrs/monthly` |
| OPIF-432 (Ликвидные акции, ОПИФ) | `alfacapital.ru/disclosure/pifs/opifa_akliq/monthly` |
Скачивание: `/disclosure/file/{numeric_id}`. Брать «Справка о СЧА» из «Ежемесячно»
(дневная = сводка без позиций). Slug AKME = `bpif_akmrs` (истор. тикер AKMRS).

### 🔴 Атон — НУЖЕН ТВОЙ БРАУЗЕР (e-disclosure, формат XLSX)
`https://www.e-disclosure.ru/portal/files.aspx?id=12603&type=23`
| Тикер | Имя на e-disclosure |
|---|---|
| AMGB | АТОН – Длинные ОФЗ |

> ⚠️ **OPIF-63 (Петр Столыпин, isin RU000A0JSA15) на карточке НЕТ.** «АТОН –
> Российские акции +» = **AMRE** (isin RU000A102XX4) — ДРУГОЙ фонд (БПИФ, 2021),
> у нас не трекается. Ошибочно залитый SCHA AMRE→OPIF-63 удалён. OPIF-63 →
> cbonds-reconstruct (акции, ~1:1). Файлы AMRE лежат в `funds_organized/Атон/AMRE …`.

### 🔴 Альфа — e-disclosure card 7203 (БПИФ) + alfacapital.ru (ОПИФ)
`https://www.e-disclosure.ru/portal/files.aspx?id=7203&type=23`
AKMB, AKME, OPIF-11259 (БПИФ) · OPIF-33/432/9113 (ОПИФ, own-site monthly XLSX).

---

## Все карточки e-disclosure (если нужен fallback по любому УК)
`files.aspx?id=<card>&type=23` = ВСЕ ПИФ компании (и БПИФ, и ОПИФ).

| УК | card | прямой curl списка? |
|---|---|---|
| Первая | **6731** | ❌ (нужен браузер) — но first-am.ru ✅ |
| Т-Капитал | **38022** | ❌ — но cdn.t-capital-funds.ru ✅ |
| Атон | **12603** | ❌ нужен браузер |
| Альфа | **7203** | ❌ — но alfacapital.ru ✅ |
| ВИМ | **28433** | ❌ — но wealthim.ru ✅ |
| Райффайзен | **37192** | ❌ (наши фонды 281/282 на карточке не найдены) |

> ⚠️ **Имена на e-disclosure ≠ имена в Cbonds/БД** — бери по столбцу «Имя фонда»
> выше, а не по тикеру. Матч в БД идёт по `isin_pif` из самого файла, так что
> даже при разнице имён всё ляжет в правильный фонд.

---

## Что НЕ берём с SCHA (нет на e-disclosure)
Райфф OPIF-281/282, Первая OPIF-43/47/8119/8123, ВИМ OPIF-9165 (Рантье),
Первая OPIF-4995 (cash). Для них (если акции) — fallback на cbonds-reconstruct.

## Текущее покрытие (на 2026-05-31): 18 фондов с точным SCHA
Первая SBMX/SBRB/SBFR/SAFE · Т-Кап TBRU/TMOS/TOFZ · Атон AMGB/OPIF-63 ·
ВИМ OBLG/OPIF-1003/OPIF-54/EQMX · Альфа AKMB/AKME/OPIF-11259/33/432/9113.

---

## ⚠️ Баг vim_sdr ×1000 в позициях (найден и исправлен 2026-05-31)

**Симптом:** старый импорт `source='vim_sdr'` в EQMX **занижал `positions` ровно
в 1000×** на копеечных бумагах (Сургут ао/ап, Мосэнерго, ЮГК, Совкомбанк, НЛМК, МКБ).
Парсер vim_sdr срезал последнюю разрядную группу «000» из чисел вида «47 407 000» →
хранил «47407». **`value`/`weight`/`Σвесов`=100% — ВЕРНЫ**, баг ТОЛЬКО в `positions`.
Затрагивал только **short-name-формат** месяцы (имена «Сургнфгз»/«ЯНДЕКС»):
2025-01/02/03 + 2025-09→2026-04. Long-name месяцы (2023-04→2024-12, 2025-04→08) и
ранний 2022-08→2023-02 — корректны (спот-чек: Газпром 2024-12 = 129.6₽, полные позиции).

**Почему важно:** фронт показывает `positions` («+X шт», «было X шт», график позиций).
Сам диф покупок/продаж считается по `weight` → направление/проценты были верны, но
абсолютные «штуки» врали ×1000.

**Как пофикшено:** хирургический `UPDATE positions` (value/weight не трогал):
- 3 месяца (есть PDF) — точные позиции из `parse_scha`.
- 5+3 месяцев без PDF — `positions*1000` где `ratio>300` против эталонной цены
  по isin из 3 пофикшенных месяцев (`ratio>300` отделяет ×1000-баг от сплита 1:100,
  который даёт ratio~100; сплиты Транснефть/Норникель были 2024 → пред-сплитовые
  цены НЕ ложно-флагаются). Итого 29 строк. Остаточных in-index багов: **0**.

**Детектор для повторной проверки** (любой vim_sdr stock-фонд):
```sql
WITH eqmx AS (SELECT fund_id FROM funds WHERE ticker='EQMX'),
ref AS (SELECT h.isin, AVG(h.amount_rub/NULLIF(h.positions,0)) rp FROM fund_holdings_history h, eqmx
        WHERE h.fund_id=eqmx.fund_id AND h.source='vim_sdr'
          AND h.snapshot_date IN ('2026-02-27','2026-03-31','2026-04-30') AND h.positions>0 GROUP BY h.isin)
SELECT f.ticker, t.snapshot_date, t.asset_name, t.positions
FROM fund_holdings_history t JOIN funds f ON f.fund_id=t.fund_id JOIN ref r ON r.isin=t.isin
WHERE t.source='vim_sdr' AND t.positions>0
  AND (t.amount_rub/NULLIF(t.positions,0))/NULLIF(r.rp,0) > 300;  -- 0 строк = чисто
```

**Остатки (не критично):** orphan-бумаги, вышедшие из индекса до 2026 (напр. РусГидро
в 2022) — их старый ×1000 НЕ ловится референсом и **в UI невидим** (drill-down
открывается только по текущим холдингам). OBLG имеет vim_sdr(45 снап) с тем же багом,
но OBLG — облигации → **скрыт** в фиче. Прочие видимые фонды на `interfax_manual`
(корректный `parse_scha`). **EQMX был единственным видимым фондом на vim_sdr.**
