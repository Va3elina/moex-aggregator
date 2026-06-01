# Альфа-Капитал (uk_id=5) — SCHA reference

**TL;DR:** own-site `alfacapital.ru` — раздел «Ежемесячно» = XLSX с позициями.
⚠️ **Дневная справка = БЕЗ позиций** (только агрегаты по категориям) — бери месячную.

## Откуда данные
- 🟢 **ОПИФ → own-site `alfacapital.ru`** (Claude curl'ит):
  - `https://alfacapital.ru/disclosure/pifs/{slug}/monthly` — раздел «Ежемесячно» (XLSX с расшифровками по ISIN).
  - ⚠️ `/nav-document` (дневная «Справка СЧА») = сводная форма, агрегаты по категориям, **БЕЗ позиций**.
    Полные расшифровки только в `/monthly` (и `/quarterly`). Сайт хранит ~4 последних месяца.
  - Download файла: `https://alfacapital.ru/disclosure/file/{numeric_id}`.
- 🔵 **БПИФ → e-disclosure card `7203`** (или own-site): AKMB, AKME, OPIF-11259.

## Слаг-карта ОПИФ (own-site)
| slug | ticker | фонд | в фиче? |
|---|---|---|---|
| bpif_akmrs | AKME | Управляемые акции (БПИФ; истор. тикер AKMRS) | ✅ stocks |
| opifa_akliq | OPIF-432 | Ликвидные акции | ✅ stocks |
| opif_akop | OPIF-33 | Облигации Плюс | скрыт bonds |
| opif_akipo | OPIF-9113 | Облигации с выплатой дохода | скрыт bonds |

## Формат
- **XLSX** (own-site monthly) → парсер `scha_xls_parser.parse_scha_xls` (**single-sheet**,
  в отличие от Атона multi-sheet). С e-disclosure БПИФ был XLS.
- source=`interfax_manual`.

## Где нужные поля
XLS-парсер детектит колонки количество/стоимость по шапке листа. Все расшифровки на одном листе.

## Трудности и как избежать
- **Дневная vs месячная — главная ловушка:** дневная справка не содержит позиций.
  Брать ТОЛЬКО месячную XLSX из раздела «Ежемесячно».
- AKME / OPIF-432 — проверены point-for-point, ЧИСТЫ (бага ×1000 нет).

## Фонды
| ticker | name | cat | в фиче? | покрытие |
|---|---|---|---|---|
| AKME | Управляемые акции | stocks | ✅ | 56 (2021→) |
| OPIF-432 | Ликвидные акции | stocks | ✅ | 4 (2026) — TODO глубже с e-disclosure 7203 |
| AKMB | Управляемые облигации | bonds | скрыт | 51 (2021→) |
| OPIF-11259 | Облигации перем. купон | bonds | скрыт | 19 (2024→) |
| OPIF-33 | Облигации Плюс | bonds | скрыт | 4 (2026) |
| OPIF-9113 | Облигации с выплатой | bonds | скрыт | 4 (2026) |
| AKGD / AKMM | золото / деньги | — | — | — |

## Резолв в backfill
isin_pif (БПИФ) или ticker. source=`interfax_manual`.
⚠️ wrong-fund: на e-disclosure под именем фонда A бывает чужой ZIP (был «Арендный поток»
под AKMB) — проверять fund_name из распарсенного файла.
