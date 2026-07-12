---
name: moex-fund-scha-backfill
description: Download and import fund holdings (SCHA / Справка о СЧА, форма ЦБ № 0420502) for /fund-trades. Use when user asks to "скачай SCHA", "докачай фонды/УК", "загрузи состав фондов", "backfill SCHA", "добавь УК", or wants to extend fund holdings history from e-disclosure.ru / УК disclosure sites. Covers the e-disclosure walker, curl download, multi-format parser, and DB import pipeline.
---

# Fund SCHA Backfill (/fund-trades)

Импорт точного состава фондов (SCHA) как **исторического якоря**. Постоянный поток —
Cbonds Mobile API (reconstruct), см. `Funds/DATA_SOURCES.md`. Этот скилл — про backfill SCHA.

## Архитектура (КРАТКО — детали в `Funds/DATA_SOURCES.md`)
- **source='interfax_manual'** = точный SCHA из офиц. раскрытия (этот пайплайн).
- **source='cbonds'/'cbonds_calc'** = реал-тайм реконструкция (NAV×weight/price), отдельно.
- Таблица `fund_holdings_history`, unique key **`(fund_id, COALESCE(isin,''), asset_name, snapshot_date, source)`**
  — НЕ по эмитенту (иначе схлопывает выпуски, теряя 30-60% позиций). Не менять обратно!

## 📇 Справочники по УК (per-УК карточки — открой ПЕРЕД backfill'ом)
Точные URL/слаги/паттерны имён, формат файлов, где лежат количество/стоимость и
подводные камни — в отдельной карточке на каждую УК (`references/uk-*.md`). Открой
карточку нужной УК до начала работы.

| УК | uk_id | главный источник | ключевая трудность | карточка |
|---|---|---|---|---|
| Первая / Сбер | 34 | first-am.ru (curl) | БПИФ/ОПИФ разные пути; e-disclosure IP-бан | `references/uk-pervaya.md` |
| Т-Капитал | 3597 | cdn.t-capital-funds.ru (curl) | `(cid:9)` в архивном 2021-формате → «триллион акций» | `references/uk-tkapital.md` |
| ВИМ | 7 | wealthim.ru (браузер/аттач) | баг ×1000 в positions | `references/uk-vim.md` |
| Альфа | 5 | alfacapital.ru (monthly XLSX) | дневная справка без позиций | `references/uk-alfa.md` |
| Атон | 12603 | e-disclosure (браузер) | XBRL multi-sheet XLSX; OPIF-63 ≠ AMRE | `references/uk-aton.md` |
| Райффайзен | 20 | e-disclosure 37192 | фонды OPIF-281/282 не найдены | `references/uk-raiffeisen.md` |

## Шаг 1. Определить НУЖНЫЕ фонды (сверка с whitelist)
НЕ качать всё подряд. Нужны только фонды из `WHITELIST_TICKERS` в
`api/routers/fund_trades.py` И с торгуемыми позициями:
- **Качаем**: stocks (акции), bonds (облигации).
- **Пропускаем**: gold (ОМС/золото), money_market (РЕПО/депозиты) — нет ISIN-позиций.

## Шаг 2. Источник SCHA по типу фонда
- **БПИФ** (биржевые, тикер 4 буквы типа SBMX/TMOS) → **e-disclosure.ru**, полный архив.
- **ОПИФ** (открытые, тикер OPIF-NNNN) → **own-site УК** (e-disclosure их НЕ содержит!),
  напр. alfacapital.ru/disclosure/pifs/<slug>/nav-document (месячный XLSX).

e-disclosure card-id по УК (в `DATA_SOURCES.md`): Альфа=7203, Т-Капитал=38022,
Первая=6731, Райфф=37192, Атон=12603.

> ⭐ **Первая/Сбер БПИФ → own-site `first-am.ru` напрямую curl'ом** (e-disclosure
> банит по IP, но first-am отдаёт 200 с Chrome UA — браузер НЕ нужен!). Индекс
> `first-am.ru/individuals/etf/{slug}/documents`, файл `/download/{doc_id}/<имя>.PDF`
> (нужен полный путь + `urllib.parse.quote(safe=":/?&=%#,-._~+")`). Slug-карта,
> формат имён и подробности — `DATA_SOURCES.md` §4 «Первая / Сбер … first-am.ru».
> Т-Капитал own-site: `cdn.t-capital-funds.ru/static/documents/otchet-scha-{tk}-DD-MM-YYYY.PDF`.

## Шаг 3. Walker (Chrome MCP) — собрать fileid'ы
На `e-disclosure.ru/portal/files.aspx?id=<card>&type=23`:
```javascript
// Заголовок фонда = div.title_middle с «...»; SCHA-строки имеют "Отчетная дата:"
const nodes = document.querySelectorAll('div.title_middle, table.files-table tr');
let cur=null; const byFund={};
nodes.forEach(n=>{
  if(n.classList?.contains('title_middle')){
    const m=n.textContent.match(/[«"]([^»"]{5,80})[»"]/);
    if(m && /фонд|ПИФ/.test(n.textContent)) cur=m[1].trim(); return;
  }
  const fl=n.querySelector?.('a[href*="FileLoad"]');
  if(!fl||!cur) return;
  const txt=n.textContent.replace(/\s+/g,' ');
  if(!txt.includes('Справка о стоимости')) return;
  const fid=(fl.href.match(/[Ff]ileid=(\d+)/)||[])[1];
  const dm=txt.match(/Отчетная дата:\s*(\d{2})\.(\d{2})\.(\d{4})/);
  const snap=dm?`${dm[3]}-${dm[2]}-${dm[1]}`:null;
  if(snap)(byFund[cur]=byFund[cur]||{})[snap]=fid;  // дедуп по дате
});
```
Маппинг имени фонда → тикер сверять с `funds` таблицей. Брать последние 12 month-end.

## Шаг 4. Download — curl НАПРЯМУЮ (браузер не нужен!)
e-disclosure отдаёт файл без ServicePipe-блока:
```python
url = f"https://www.e-disclosure.ru/portal/FileLoad.ashx?Fileid={fid}"
req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
# → /tmp/reimport_X/TICKER_<snapshot>.zip  (имя по тикеру+дате — backfill резолвит фонд)
```

## Шаг 5. Parse + verify ПЕРЕД импортом
```python
from Funds.parsers.scha_xls_parser import parse_scha_xls  # XLS
from Funds.parsers.scha_parser import parse_scha           # PDF
# zip: распаковать inner (cp866 decode), роутить по расширению (.xls/.xlsx→xls-parser, .pdf→pdf-parser)
```
Проверить: assets > 0, snapshot_date корректна. Если PDF не даёт дату — backfill возьмёт из имени файла.

## Шаг 6. Import на прод
```bash
scp -rq -o IdentitiesOnly=yes -o IdentityAgent=none -i ~/.ssh/id_ed25519 \
  /tmp/reimport_X root@103.88.243.232:/opt/frame/data/manual_scha/reimport_X
ssh ... 'docker exec frame-api-1 python3 -m Funds.manual_scha_backfill /data/manual_scha/reimport_X/ 2>&1 | tail -8'
# api контейнер read-only rootfs → /data/manual_scha bind-mount единственный inbound-канал.
# data/ в .gitignore → scp сюда переживает deploy (reset --hard) — это OK, НЕ черновик в tracked-зону.
# Если меняешь Funds/*.py — сначала git push (CI пересоберёт api), дождись зелёного deploy-prod,
# ПОТОМ backfill. НЕ docker compose build руками на проде — сервер чистый target (reset --hard).
ssh ... 'rm -rf /opt/frame/data/manual_scha/reimport_X'  # cleanup после
```
Verify: `SELECT ticker, COUNT(DISTINCT snapshot_date), AVG(assets) ... WHERE source='interfax_manual'`.
Локальный архив (per-оператор, свой на каждой машине — на Mac Вадима был
`~/Downloads/funds_organized/...`, на Windows заведи аналог, напр.
`C:/MOEX/local_archive/funds_organized/`): `<УК>/<TICKER — Name>/<date>.<ext>` + INDEX.md.

## Подводные камни (data-integrity — спот-чек при КАЖДОМ импорте)
- **wrong-fund**: ZIP под именем фонда A может содержать фонд B (был «Арендный поток»
  под AKMB) — проверять fund_name из распарсенного файла.
- **PDF-layout различается по УК.** `parse_scha` = 3 стратегии (tables / **tables_rowwise**
  header-agnostic для 35-стр Первой/Атона / text+regex), берёт лучшую. Rowwise склеивает ISIN
  с переносом, ловит иностранные/сплит ISIN. Cash-отсечка: ≥3 табличные, ≥20 text+regex.
- **🐞 `(cid:9)`-глиф (Т-Капитал архив 2021)**: pdfplumber выдаёт разделитель разрядов как
  `1(cid:9)069(cid:9)930` → числовой фильтр ронял ячейку (буквы c,i,d), ОГРН/ИНН шли за qty/value
  («1 трлн акций»; cid:9 ещё и цифру 9 вставлял). Чистится в `_parse_int/_parse_float/_norm`
  (`re.sub(r"\(cid:\d+\)","")`). **Спот-чек:** Сбербанк ~цена 300-400₽, не «триллион штук».
- **🐞 Иностранные ГДР/АДР — код страны вместо количества**: в подразделе «Акции иностранных
  эмитентов» в строке ДВА кода ISO — депозитария (840=США/BoNY) + эмитента (196=Кипр FIX PRICE/
  AGRO/CIAN/HH/TCS/GLTR; 528=NL Яндекс/X5; 784=ОАЭ GLTR-редом). `parse_scha` исключает их по
  `_ISO_COUNTRY_CODES`, количество = первое СПРАВА число с вменяемой ценой (потолок **500k ₽**
  для доспличных Транснефть ~105k). **Спот-чек:** `positions<1000` у US/JE/GB/NL-isin = недобитый код.
- **🐞 Frozen ГДР после конвертации** (Русагро/X5 → RU-акции, 2024-26): стоимость в СЧА=0 →
  исключена из nums → последним числом остаётся код страны. `parse_scha`: если nums[-1]=код страны,
  value=0. positions = реальный замороженный остаток, weight=0 (косметика на нулевом весе).
- **🐞 ×1000 ВИМ-фонды**: старый vim_sdr-скрейпер срезал группу «000» → positions ÷1000.
  Парсить `parse_scha`, после заливки гнать детектор implied-price (см. `MONTHLY_SCHA_REFRESH.md`
  «Баг ×1000»). Затронуты были EQMX (vim_sdr) + OPIF-1003 (interfax) — НЕ привязано к source.
- **Дневная vs месячная** (Альфа ОПИФ): дневная Справка = сводка без позиций, брать месячную XLSX.
- **Reconstruct-приём** (если parser не извлёк positions из фрагментированной строки, но amount
  корректен): `positions=ROUND(amount_rub/candles.close)`, candles `secid`=тикер MOEX, `interval=24`,
  `begin_time::date=snapshot_date`. Так чинили VK-гдр (US5603172082) в TMOS 2022.

## ISIN — единственный ключ матчинга (НЕ имя!)
- Diff и консенсус матчат бумаги по **ISIN** (`mkey`/`akey` = `COALESCE(NULLIF(isin,''),asset_name)`).
  У одной бумаги разные написания в разных УК/источниках; **ао/ап = РАЗНЫЕ ISIN** (Сбер ао
  RU0009029540, ап RU0009029557). `USING (asset_name)` → декартово произведение у дубль-классов
  (ао+ап делят имя «…СБЕРБАНК РОССИИ») → фантомные дельты («+4.5M докупил»). Все 3 эндпоинта
  (`/fund`, `/movers`, `/snapshot`) матчат по ISIN, `USING (asset_name)` НЕ возвращать.
- **Отображаемое имя = самое короткое по ISIN среди ВСЕХ фондов** (`(array_agg(asset_name
  ORDER BY length))[1]`). Первая/Т-Кап пишут длинные юр.имена, ВИМ/cbonds — короткие тикерные
  («Т-Техно ао», «Сургнфгз-п»). `/snapshot` и `/fund` тоже резолвят короткое (CTE `names`).
- ⚠️ Остаточный декарт — только OBLG (дубль ISIN из 2 источников vim_sdr+interfax, фонд скрыт).

## Сплиты акций fund-trades (T 1:10 апр-2026) — покрыты в 3 местах
- **Детект:** кол-во ×R И цена ÷R И стоимость≈const (отличает сплит от покупки — там amount растёт).
  Множитель = ближайший СТАНДАРТНЫЙ коэф. (`_nearest_split_ratio`, geomean posR&priceR), НЕ сырое
  отношение количеств (иначе боковая торговля на границе → Δ=0).
- **Drill-down график** (`splitAdjustPositions` в FundTradesPage.tsx): back-adjust истории ×R +
  маркер «Сплит» через `SimpleChart.annotations`.
- **`/snapshot` бэкенд**: приводит prev к пост-сплит масштабу → реальная дельта + классификация
  докупил/продал. Фронт в режиме «% веса» ре-бакетит по знаку Δдоли.
- **`/movers`**: сплит-безопасен (weight/amount непрерывны через сплит).
- ⚠️ KNOWN_SPLITS + re-import candle — это для ИНДИКАТОРОВ (breadth и т.п.), НЕ для fund-trades
  (там сырые СЧА-positions + on-the-fly адъюст).
