# Атон (uk_id=aton, e-disclosure 12603) — SCHA reference

**TL;DR:** ТОЛЬКО e-disclosure (нужен ТВОЙ браузер; own-site под Cloudflare).
Формат — **XBRL-Excel (XLSX), multi-sheet**. ⚠️ OPIF-63 ≠ «Рос. акции +» (см. ниже).

## Откуда данные
- 🔴 **e-disclosure card `12603`, нужен браузер** (own-site Cloudflare-protected):
  `https://www.e-disclosure.ru/portal/files.aspx?id=12603&type=23`
  | ticker | имя на e-disclosure |
  |---|---|
  | AMGB | АТОН – Длинные ОФЗ |
  - Скачать «Справка о стоимости чистых активов» за месяц (walker → curl FileLoad) → прислать zip/xlsx.

## Формат
- **XBRL-Excel (XLSX) форма 0420502, multi-sheet:** каждая расшифровка = отдельный лист
  (`N; sr_…Rasshifr_*` или старый `N 0420502Справк…`). Парсер `scha_xls_parser.parse_scha_xls`
  + multi-sheet rowwise. ZIP внутри = XLSX (backfill роутит inner по расширению).
- source=`interfax_manual`.

## Где нужные поля
multi-sheet rowwise: на каждом листе-расшифровке — строки с ISIN + количество/стоимость
(два последних числа). AMGB Σвесов=100% ✓ после адаптации парсера.

## Трудности и как избежать
- 🚨 **OPIF-63 ≠ «АТОН Российские акции +».** Настоящий OPIF-63 = «Петр Столыпин»
  (isin RU000A0JSA15) — на карточке 12603 ОТСУТСТВУЕТ. «Российские акции +» = **AMRE**
  (isin RU000A102XX4, БПИФ 2021) — ДРУГОЙ фонд, у нас не трекается. Ошибочно залитый
  SCHA AMRE→OPIF-63 удалён (1755 строк, поймал `verify_reconstruct.py` med 1071% diff).
  OPIF-63 → cbonds-reconstruct (акции ~1:1). Файлы AMRE лежат в `funds_organized/Атон/AMRE …`.
- Own-site Cloudflare-protected → ТОЛЬКО e-disclosure через локальный браузер.

## Фонды
| ticker | name | cat | в фиче? | покрытие |
|---|---|---|---|---|
| AMGB | Длинные ОФЗ | bonds | скрыт bonds | 17 (Σвесов=100% ✓) |
| OPIF-63 | Петр Столыпин | stocks | ✅ (cbonds-reconstruct) | нет SCHA |
| AMNR | Накопительный в рублях | money | — | — |

## Резолв в backfill
isin_pif из XLSX. source=`interfax_manual`. При импорте — проверить fund_name (AMGB vs AMRE!).
