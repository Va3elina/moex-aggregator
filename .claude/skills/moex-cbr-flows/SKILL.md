---
name: moex-cbr-flows
description: Ручной ингест данных «Потоки капитала» (/cbr-flows) из отчёта ОРФР ЦБ. Use when user says «пришёл файл ОРФР», «обнови потоки капитала», «загрузи cbr-flows», «новый обзор рисков ЦБ», «данные по участникам биржи», or присылает ORFR xlsx. Covers ручную загрузку XLSX (авто-скачивание убрано), формат-дрейф листов ЦБ, обязательную чистку кэша, заморозку FX-таба.
---

# /cbr-flows — потоки участников биржи (ОРФР ЦБ)

Индикатор «Потоки капитала», 3 таба: stocks/ofz/fx. Источник — ежемесячный
**«Обзор рисков финансовых рынков» (ОРФР)** ЦБ: https://cbr.ru/analytics/finstab/orfr/ .
Таблица `cbr_flows` (unique `(instrument_type, period_end_date, category)`, idempotent).
Роутер `api/routers/cbr_flows.py`, фетчер `CBR/fetch_orfr_flows.py`.

## ⚠️ РЕЖИМ — РУЧНОЙ (с 2026-06-09)
Вадим **сам присылает XLSX-файл**, авто-скачивание убрано.
**Why:** cbr.ru таймаутит с прод-сервера + ЦБ нестабильно меняет формат. Авто-крон
только плодил ошибки. **НЕ восстанавливать автоскачивание** ([[feedback_no_data_autofetch]]).

## Загрузка присланного файла
```bash
# 1. Файл на прод → в оркестратор (writable rootfs, есть deps+DB; api read-only — туда нельзя)
scp ORFR_YYYY-M.xlsx root@103.88.243.232:/tmp/
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i ~/.ssh/id_ed25519 root@103.88.243.232 '
  docker cp /tmp/ORFR_YYYY-M.xlsx frame-orchestrator-1:/tmp/X.xlsx &&
  docker exec frame-orchestrator-1 python3 -m CBR.fetch_orfr_flows --xlsx /tmp/X.xlsx 2>&1 | tail -20 &&
  # 2. ОБЯЗАТЕЛЬНО почистить кэш (иначе API отдаёт старое до 1ч).
  #    ⚠️ Redis ПОД ПАРОЛЕМ — голый redis-cli падает NOAUTH. Пароль берём из
  #    REDIS_URL api-контейнера (имя динамическое → резолвим по label):
  PW=$(docker exec $(docker ps -q -f label=com.docker.compose.service=api | head -1) printenv REDIS_URL | sed -E "s#.*://:([^@]+)@.*#\1#") &&
  docker exec -e REDISCLI_AUTH="$PW" frame-redis-1 redis-cli --scan --pattern "cbr_flows:*" |
    xargs -r docker exec -e REDISCLI_AUTH="$PW" frame-redis-1 redis-cli del
'
# 3. Verify — в periods[-1] нужный месяц, updated_at ≈ время ингеста
curl -s "https://xn--80aklbnczmv.xn--p1ai/api/cbr-flows?type=stocks" | head -c 300
```

## ⚠️ Формат-дрейф ЦБ (КРИТИЧНО — главная причина сбоев парса)
Имена листов ЦБ меняются. Фетчер резолвит лист по точному имени → fallback regex
заголовка (`resolve_sheet_name` + `title_re` в `SHEETS_CONFIG`). Маркеры рынков:
- **РА** = акции, **РО** = ОФЗ, **РК** = корпбонды, **РВ** = валюта.
- До апр-2026: номера фигур (`рис. 32`=акции, `рис. 14`=ОФЗ). С `ORFR_2026-5` (май) —
  семантические имена (`РА По участникам`, `РО По участникам`), отчёт переструктурирован (43 листа).
Если парс упал «лист не найден» → открыть xlsx, найти новое имя листа-«По участникам»,
дополнить `SHEETS_CONFIG` (точное имя + regex-fallback) → ветка → PR (правка в `CBR/`
пересоберёт orchestrator).

## FX заморожен на апреле-2026 (знать, не «чинить»)
ЦБ УБРАЛ разбивку валют по участникам (`РВ По участникам` больше нет). fx-таб показывает
историю по апрель, новых данных НЕТ — это не баг парсера. Открытый вопрос (решение Вадима):
freeze / скрыть таб / remap на новые РВ-серии. Потенциальная фича — 4-й таб «Корпоблигации»
из `РК Операции участников`.

Связано: [[cbr_flows]], [[indicator_patterns]], [[data_sources]].
