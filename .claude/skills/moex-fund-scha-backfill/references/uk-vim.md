# ВИМ Инвестиции (uk_id=7) — SCHA reference

**TL;DR:** own-site `wealthim.ru` — **ПОЛНЫЙ архив через Bitrix-пагинацию
`?PAGEN_1=N`, и листинг, и PDF качаются чистым curl** (браузер НЕ нужен).
⚠️ **Баг ×1000 в `positions`** (см. ниже) — ВСЕГДА парсить `parse_scha` + прогонять детектор.

## Откуда данные
- 🟢 **own-site `wealthim.ru` — полный архив, весь curl** (проверено 2026-07-18,
  бэкфилл EQMX 2023-2025):
  1. Листинг `/reports/` показывает 9 записей на страницу, но это ПАГИНАЦИЯ:
     `...reports/?PAGEN_1=2`, `=3`, ... — уходит вглубь до 2022+.
  2. Из HTML листинга взять `href="/upload/iblock/<hash>/<hash>/<file>.pdf"` —
     пути хэшированные, НЕ угадываются, только из листинга (или Wayback-снапшота
     листинга — старые /upload-файлы живут и после делистинга).
  3. **Прямой curl `https://www.wealthim.ru/upload/iblock/...pdf` с Chrome UA
     отдаёт настоящий PDF** (SPA-shell — миф, /upload это статика Bitrix).
  | ticker | reports URL |
  |---|---|
  | EQMX (Индекс МосБиржи, БПИФ) | `wealthim.ru/about/disclosure/pif/bpif/wimfimb/reports/` |
  | OPIF-1003 (Фонд Акций, ОПИФ) | `wealthim.ru/about/disclosure/pif/opif/wimfa/documents/reports/` |
  Имена PDF: `ГГ_ММ_ДД_SCHA_BPIF_IndeksMosBirzhi.pdf` / `..._SCHA_OPIF_Aktsii.pdf`.
  (slug `wimfa`=OPIF-1003 подтверждён сверкой; `wimfeqr` = ДРУГОЙ фонд «Рос. эмитенты»!)
- 🔴 e-disclosure card `28433` — **на 2026-07 закрыт интерактивной капчей
  (ServicePipe «разверните картинку»)** и с локального браузера, и curl'ом;
  с прод-сервера (датацентр-IP) соединение просто RST-ится. Использовать
  только если own-site не помог, и решать капчу должен человек.
  OPIF-9165 «Рантье» на карточке НЕТ (мб «ВИМ Сбережения» card 34888).

## Формат
- **PDF** (compact-формат, у БПИФ — вертикальный шрифт). Парсер `parse_scha` → `text+regex` либо `tables_rowwise`.
- source-метки: EQMX=`vim_sdr`, OPIF-1003=`interfax_manual`, OBLG=`vim_sdr`+`interfax`.

## 🐞 Трудности — БАГ ×1000 (КРИТИЧНО, исправлен 2026-05-31)
Старый `vim_sdr`-скрейпер срезал последнюю разрядную группу «000» → `positions` ×1000
ниже (на ультра-копейках Россети 420M шт — даже ×1 000 000). Бумаги: Сургут ао/ап,
Мосэнерго, ЮГК, Совкомбанк/флот, НЛМК, МКБ, ВТБ, РУСАЛ, Ростелеком, РусГидро, Юнипро и т.д.
- **`value`/`weight`/Σвесов=100% ВЕРНЫ — баг ТОЛЬКО в `positions`** → проверки count/NAV/Σвесов
  его НЕ ловят. Нужен position-diff или implied-price-скан.
- **НЕ привязан к source-метке:** затронуты ОБА stock-фонда — EQMX (`vim_sdr`) И
  OPIF-1003 (`interfax_manual`).
- **Правило:** парсить строго `parse_scha` (rowwise), НЕ старый vim_sdr-скрейпер.
  После любого импорта ВИМ-PDF прогнать **канонический детектор** (implied-price vs
  global-ref по чистым фондам, `ratio>300`) — см. `Funds/MONTHLY_SCHA_REFRESH.md`
  раздел «Баг ×1000». 0 строк = чисто. Тот же `UPDATE ... positions*POWER(1000,ROUND(...))` чинит.

## Фонды
| ticker | name | cat | source | в фиче? | покрытие |
|---|---|---|---|---|---|
| EQMX | Индекс МосБиржи | stocks | vim_sdr | ✅ | 44 (2022→) |
| OPIF-1003 | Фонд Акций | stocks | interfax_manual | ✅ | 31 (2023→) |
| OBLG | Российские облигации | bonds | vim_sdr+interfax | скрыт bonds | 31+45 (vim_sdr тоже ×1000, но скрыт) |
| OPIF-54 | Казначейский | bonds | interfax | скрыт | 31 |
| GOLD / LQDT | золото / деньги | — | vim | — | — |
| OPIF-9165 | Облигации Рантье | bonds | — | скрыт | нет SCHA |

## Резолв в backfill
own-site имена / e-disclosure. Заносить как `source='vim_sdr'` (чтобы фича подхватила EQMX),
но **позиции из `parse_scha`**, не из старого скрейпера. OPIF-1003 — `interfax_manual`.
