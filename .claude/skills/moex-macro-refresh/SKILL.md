---
name: moex-macro-refresh
description: Обновить макро-данные Фрейма — ВВП (Росстат) и M2 (ЦБ). Use when user says «вышли данные по ВВП», «обнови ВВП», «новый квартал ВВП», «скачай ВВП», «обнови M2 / денежную массу», «ВВП не обновился», «данные Росстата встали», «индикатор Баффетта на старом ВВП». Covers the fetch_macro_realtime pipeline, Rosstat auto-discover (хрупкий — имя файла меняется), ручной импорт и проверку, что данные доехали до индикатора Баффетта.
---

# Обновление макро-данных: ВВП (Росстат) + M2 (ЦБ) + ключевая ставка

Скрипт: **`Macro/fetch_macro_realtime.py`** (вшит в образ `frame-orchestrator`).
Крутится ежедневно как пайплайн `macro_daily`. M2 — раз в месяц, ВВП — раз в квартал,
но фетчер гоняется каждый день (идемпотентно, пере-парсит весь xlsx).

С 2026-07-21 macro_daily также гоняет **KEY_RATE** (`Candles/fetch_key_rate.py`,
дневной ряд ставки ЦБ с https://www.cbr.ru/hd_base/KeyRate/, окно 30 дней,
subprocess-шаг в `update_macro()`). До этого скрипт был ТОЛЬКО ручным и ряд
молча простоял 40 дней (проспал снижение 14.50→14.25); в health-мониторе у него
kind=t_plus_1 (было event = всегда ok). Если KEY_RATE stale — смотреть лог
macro_daily (строка `KEY_RATE: rc=…`), чинить обычно парс-регэксп HTML-таблицы ЦБ.

## TL;DR — «вышли данные по ВВП, обнови»
Обычно новый квартал подтягивается САМ (ежедневный `macro_daily`). Если не подтянулся —
почти всегда сломался auto-discover Росстата (меняют имя файла). Быстрый путь:

```bash
# 1. Проверить текущую свежесть ВВП в БД
cat <<'SQL' | ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i ~/.ssh/id_ed25519 root@103.88.243.232 'docker exec -i frame-db-1 psql -U postgres -d moex_db'
SELECT indicator, MAX(period_date) FROM macro_data
WHERE indicator IN ('GDP_QUARTERLY','M2_MONTHLY') GROUP BY indicator;
SQL

# 2. Запустить фетчер на проде (M2 + ВВП, идемпотентно)
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i ~/.ssh/id_ed25519 root@103.88.243.232 \
  'docker exec frame-orchestrator-1 python3 Macro/fetch_macro_realtime.py --once --force 2>&1 | tail -30'
```

Если в логе `Ссылка на VVP_kvartal_s_*.xlsx не найдена` → discover сломался, см. ниже.

## ⚠️ Ключевой факт: GDP-сбой НЕ роняет пайплайн (тихое зависание)
`macro_daily` остаётся **`ok`** даже если ВВП не скачался (GDP-фетч некритичен, M2
обновляется отдельно). → «зелёный статус, а ВВП на старом квартале». Единственный
надёжный признак — сравнить `MAX(period_date) WHERE indicator='GDP_QUARTERLY'` с
календарём публикаций Росстата (Q1→~май-июнь, Q2→~сентябрь, Q3→~декабрь, Q4→~март).

## Как устроен ВВП (Росстат)
- Источник: страница `rosstat.gov.ru/statistics/accounts`, файл `VVP_kvartal_s*.xlsx`.
- `discover_gdp_url()` ищет ссылку регуляркой `GDP_XLSX_HREF_RE` по HTML.
- ⚠️ **Имя файла ПЛАВАЕТ**: `VVP_kvartal_s_1995-2025` → `s1995` → `s-1995-2026` (дефис,
  инцидент 02.07.2026, PR #288). Регэксп теперь `s[-_]?\d{4}`. Если Росстат снова
  сменит паттерн — discover отвалится, чинить регэксп.
- ⚠️ **SSL**: сертификат Росстата подписан Russian Trusted Root CA (Минцифры), и вдобавок
  Росстат **не присылает промежуточный сертификат** (`Verify return code: 21`). Поэтому нужен
  `_rosstat_ssl_context()` → `api/ru_tls.py::rosstat_ssl_context()`: бандл
  `/etc/ssl/frame/rosstat-bundle.pem` = certifi + корень + Sub CA. WebFetch и обычный curl
  БЕЗ этого бандла страницу НЕ возьмут.
  С 30.07.2026 проверка сертификата **включена** (до этого стоял `CERT_NONE`, PR #858).
  Если фетчер ляжет с `CERTIFICATE_VERIFY_FAILED` — Росстату выписали лист под новым Sub CA:
  порядок обновления в шапке `certs/russian_trusted_sub_ca.pem`, проверка —
  `python scripts/check_ru_tls.py` (Росстат там отдельной строкой).
- Индикатор в БД: `macro_data.indicator = 'GDP_QUARTERLY'`, млрд ₽, `source=ROSSTAT_XLSX`.

## Ручной путь, если auto-discover сломался
```bash
# А. Найти актуальный URL файла — качаем HTML тем же SSL-контекстом, грепаем ссылки
ssh ... 'docker exec frame-orchestrator-1 python3 -c "
import sys; sys.path.insert(0,\"/app\")
from Macro.fetch_macro_realtime import _rosstat_ssl_context, ROSSTAT_GDP_PAGE_URL
import urllib.request, re
req = urllib.request.Request(ROSSTAT_GDP_PAGE_URL, headers={\"User-Agent\":\"Mozilla/5.0\"})
html = urllib.request.urlopen(req, timeout=30, context=_rosstat_ssl_context()).read().decode(\"utf-8\",\"replace\")
for l in re.findall(r'\''href=\"([^\"]+\.xlsx)\"'\'', html, re.I):
    if re.search(r'\''vvp|kvartal'\'', l, re.I): print(l)
"'

# Б. Скачать найденный файл и импортировать (подставь URL из шага А)
ssh ... 'docker exec frame-orchestrator-1 python3 -c "
import sys; sys.path.insert(0,\"/app\")
from Macro.fetch_macro_realtime import _rosstat_ssl_context, _download_to_tempfile, ROSSTAT_BASE_URL, import_gdp_from_xlsx, get_engine
url = ROSSTAT_BASE_URL + \"/storage/mediabank/<ФАЙЛ>.xlsx\"
p = _download_to_tempfile(url, ssl_context=_rosstat_ssl_context())
print(\"IMPORTED:\", import_gdp_from_xlsx(get_engine(), p))
"'

# В. Поправить регэксп GDP_XLSX_HREF_RE в Macro/fetch_macro_realtime.py → ветка → PR
#    (правка в Macro/ пересоберёт orchestrator: prod_deploy.sh грепает ^Macro/).
```
Флаг `--import-gdp FILE.xlsx` — если Вадим прислал файл вручную.

## Как устроен M2 (ЦБ)
- Источник: `cbr.ru/vfs/statistics/credit_statistics/monetary_agg.xlsx` (URL стабилен).
- `M2_MONTHLY`, млрд ₽. Валидатор скачков (>30% м/м → warning, не блок).
- Ломается редко (URL фиксированный) — если встал, смотреть доступность cbr.ru.

## Проверка, что данные ДОЕХАЛИ до индикатора (обязательно)
ВВП питает **Индикатор Баффетта** (капитализация/ВВП). Знаменатель — `gdp_ttm` (сумма
4 последних кварталов). Проверить, что новый квартал вошёл:
```bash
curl -s "https://xn--80aklbnczmv.xn--p1ai/api/buffett/cap-gdp?period=5y&timeframe=1q" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data'][-1])"
# gdp_ttm должен вырасти на (новый_квартал − выпавший_квартал)/1000 трлн.
```
Кэш Баффетта обычно самообновляется; если завис — чистить Redis `chart:*`/`buffett:*`
(см. [[oi_futures_charts]] про чистку кэша).

## Что НЕ делать
- НЕ строить cron/walker сверх существующего `macro_daily` — он уже ежедневный.
  (Отличать от УК-раскрытия, где автофетч ЗАПРЕЩЁН — [[feedback_no_data_autofetch]].
  Росстат/ЦБ = гос-статистика, их авто-пайплайн санкционирован.)
- НЕ править файл прямо на проде — только git → PR (образ пересоберётся).

Связано: [[ingestion_map]] (карта фетчеров), [[indicator_patterns]] (Баффетт),
[[deploy_502_window]] (динамическое имя api-контейнера).
