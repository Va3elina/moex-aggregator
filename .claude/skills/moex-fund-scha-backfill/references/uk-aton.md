# Атон (uk_id=aton, e-disclosure 12603) — SCHA reference

**TL;DR:** own-site **am-aton.ru теперь ОТКРЫТ** (migrated на Next.js в 2026, Cloudflare снят) —
прямой `curl -sk` + Chrome UA, браузер НЕ нужен. e-disclosure 12603 — fallback (там только AMGB).
Формат — **XBRL-Excel (XLSX), multi-sheet**. ⚠️ OPIF-63 ≠ «Рос. акции +» (см. ниже).

## Откуда данные
- 🟢 **am-aton.ru напрямую (curl, БЕЗ браузера/Cloudflare)** — лучший источник, глубокий архив:
  страница фонда `https://www.am-aton.ru/legaldocs/docpif/{code}`, ссылки на файлы — в JSON:
  `https://www.am-aton.ru/_next/data/{buildId}/legaldocs/docpif/{code}.json`
  → `pageProps.data[*].items[*]`: `name`=«Справка о стоимости чистых активов на DD.MM.YYYY…»,
  `file.url`=`/upload/iblock/<hash>.xlsx`. buildId — из `"buildId":"…"` на любой странице
  (меняется при редеплое сайта, тогда взять свежий). Сортировка items — свежие вперёд.
  | code (слаг) | ticker | фонд | cat |
  |---|---|---|---|
  | `pyotr_stolypin` | OPIF-63 | Петр Столыпин | stocks (61 снапшот) |
  | `AMRE` | — | Российские акции + (БПИФ, у нас НЕ трекается!) | stocks |
  | `infrastructure` | — | Эшелон 2.0 | stocks |
  | `high_technology_fund` | — | Фонд высоких технологий | stocks |
  | `global_markets` | — | Дивидендные: Рантье | stocks |
  - ⚠️ В части файлов (2025-08..12) `fund_name` парсером НЕ извлекается (иной layout метаданных) —
    косметика: резолв фонда идёт по тикеру из имени файла. Спот-чек состава (Сбер/Лукойл) = OK.
- 🔴 **e-disclosure card `12603` (fallback, нужен браузер):**
  `https://www.e-disclosure.ru/portal/files.aspx?id=12603&type=23` — там ТОЛЬКО AMGB (Длинные ОФЗ),
  OPIF-63 ОТСУТСТВУЕТ (поэтому раньше числился «нет SCHA»).

## Формат
- **XBRL-Excel (XLSX) форма 0420502, multi-sheet:** каждая расшифровка = отдельный лист
  (`N; sr_…Rasshifr_*` или старый `N 0420502Справк…`). Парсер `scha_xls_parser.parse_scha_xls`
  + multi-sheet rowwise. ZIP внутри = XLSX (backfill роутит inner по расширению).
- source=`interfax_manual`.

## Где нужные поля
multi-sheet rowwise: на каждом листе-расшифровке — строки с ISIN + количество/стоимость
(два последних числа). AMGB Σвесов=100% ✓ после адаптации парсера.

## Трудности и как избежать
- 🐞 **Заблокированные ГДР / спецдепозитарий → ИНН в positions (~7.7e9)** (OPIF-63, снапы 2025-08+).
  XBRL-лист `…Rasshifr_Akt_P3` (Подраздел 3.4 «Иностранные депозитарные расписки») — ШИРОКИЙ:
  в строке ДВА блока ISIN (расписка + представляемая бумага) и ИНН эмитента представляемых.
  Старая эвристика «два последних числа» брала ИНН (7736216869 ФосАгро / 7710198911 спецдеп)
  за количество и код валюты `643-RUB` за стоимость. Плюс отдельный лист «Сведения об
  организациях, осуществляющих учёт» (есть «Количество», нет «Стоимости») тоже читался как
  holdings. **Фикс:** `_parse_xls_rowwise` теперь header-aware — колонки ISIN/Количество/
  Стоимость по тексту шапки листа; лист = holdings ТОЛЬКО если есть И «Количество», И
  «Стоимость» (учётные таблицы отсекаются); для ГДР берётся ISIN расписки (не «представляемых»).
  Дробное количество (ФосАгро ао 5947.66) округляется, а не отбрасывается. Backstop в
  `manual_scha_backfill.is_implausible_row` (positions>1e8 И цена<0.0001₽ → строка отвергается).
  **Спот-чек:** ФосАгро RU000A0JRKT8 ≈ 40M ₽ / 0.51% (2026-04), не «−23 трлн».
- 🚨 **OPIF-63 ≠ «АТОН Российские акции +».** Настоящий OPIF-63 = «Петр Столыпин»
  (isin RU000A0JSA15) — на карточке 12603 ОТСУТСТВУЕТ. «Российские акции +» = **AMRE**
  (isin RU000A102XX4, БПИФ 2021) — ДРУГОЙ фонд, у нас не трекается. Ошибочно залитый
  SCHA AMRE→OPIF-63 удалён (1755 строк, поймал `verify_reconstruct.py` med 1071% diff).
  **OPIF-63 теперь ИМЕЕТ SCHA** с am-aton.ru `pyotr_stolypin` (НЕ с AMRE!) — 18 снапшотов
  залито 2026-06 (2024-11..2026-04, Σвесов=100% ✓, Сбер≈320₽). Файлы AMRE — `funds_organized/Атон/AMRE …`.
- ~~Own-site Cloudflare-protected~~ УСТАРЕЛО (2026): am-aton.ru на Next.js, Cloudflare снят,
  прямой curl отдаёт XLSX (см. «Откуда данные»). e-disclosure нужен только для AMGB.

## Фонды
| ticker | name | cat | в фиче? | покрытие |
|---|---|---|---|---|
| AMGB | Длинные ОФЗ | bonds | скрыт bonds | 17 (Σвесов=100% ✓) |
| OPIF-63 | Петр Столыпин | stocks | ✅ SCHA | 18 (am-aton.ru, Σвесов=100% ✓) |
| AMNR | Накопительный в рублях | money | — | — |

## Резолв в backfill
isin_pif из XLSX. source=`interfax_manual`. При импорте — проверить fund_name (AMGB vs AMRE!).
