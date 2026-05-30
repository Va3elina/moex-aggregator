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
# Если меняешь Funds/*.py — docker compose build api && up -d ПЕРЕД backfill.
ssh ... 'rm -rf /opt/frame/data/manual_scha/reimport_X'  # cleanup после
```
Verify: `SELECT ticker, COUNT(DISTINCT snapshot_date), AVG(assets) ... WHERE source='interfax_manual'`.
Локальный архив: `~/Downloads/funds_organized/<УК>/<TICKER — Name>/<date>.<ext>` + INDEX.md.

## Подводные камни
- **wrong-fund**: ZIP под именем фонда A может содержать фонд B (был «Арендный поток»
  под AKMB) — проверять fund_name из распарсенного файла.
- **PDF-layout различается по УК — РЕШЕНО (2026-05-30).** `parse_scha` имеет 3 стратегии
  (tables / **tables_rowwise** header-agnostic для 35-стр Первой/Атона / text+regex) и берёт
  лучшую. Rowwise склеивает ISIN с переносом, берёт «два последних числа строки» как
  количество+стоимость, ловит иностранные/сплит ISIN. Порог cash-отсечки: ≥3 для табличных
  стратегий, ≥20 для text+regex. Детали — `DATA_SOURCES.md` §4 (блок про parse_scha).
- **Дневная vs месячная** (Альфа ОПИФ): дневная Справка = сводка без позиций, брать месячную XLSX.
- **Сплиты акций**: re-import через `Candles/backfill_daily_history.py`, не комбинировать с KNOWN_SPLITS.
