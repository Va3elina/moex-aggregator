# Очередь целей

Отмечать ✅ после прогона (+ ссылка на отчёт). Самоописание трейдера = проверка классификатора.

## Проверочный набор: механика известна из описания (Bybit, 26.09.2026)
✅ — разобран (`targets/<slug>/`), 🔒 — история на Bybit скрыта, только внешняя страница

| трейдер | leaderMark | что заявляет | ожидаемый тип | внешняя история |
|---|---|---|---|---|
| ✅ Algo-Executor | `/wQF7+GkKrFNJGsT/+8rhw==` | тренд BTC, «частые мелкие убытки, редкие крупные прибыли» | тренд | alperenlcr.github.io/trade_bot_results/ |
| 🔒 DMX-Bot LOW-RISK | `lFWr+8xq0KaJbunlPUuayQ==` | тренд ETH лонг/шорт без плеча: линрег, Боллинджер, дивергенции | тренд | damianhunziker.net/dmx-bot/ (история на Bybit скрыта) |
| ✅ Quant-S | `4Pf1hj/Su6XoH51r/fnIrQ==` | сетка/DCA по TRX, вебхуки TradingView, 20x | сетка / докупка | — |
| 🔒 Nirquant | `UjO7UULgExmQQzROcULETg==` | риск 1% на сделку, стоп всегда | со стопом | eclectic-sopapillas-96d84b.netlify.app (история скрыта) |
| ✅ H_trendHunter | `BO2WE5lm2vnXvGLuLpxEUg==` | тренд, плечо 1.5–2, бэктест с 2012 | тренд | — |
| ✅ Wasolldas 🏄🏻‍♀️ | `rp9DqDnaeagerRcZXQr8kA==` | тренд лонг/шорт, боты TradingView | тренд | — |
| ✅ safemoneymaker | `keHsDrRVyVZZYz6HubAw8w==` | XAUT, «100% побед», просадка 77.5% | докупка | — |
| SyndicateOfTraders | (см. invvo 9) | «возврат к среднему, роботы с 2018», 3Commas | докупка | invvo.com/strategy/9 ✅ |

## Разобраны вручную до завода (перегнать через завод)
- Meridian Trend edge / BOT 1 strategy — сканер 6ч ✅ `targets/meridian-trend-edge/`
- Papai (докупка BNB/ETH/HYPE), HYPE-мартингейл ×2, ONDO/DOGE-мартингейл ×1.42 (Bybit)
- BadBoy15xa, honeywave, XRPUSDT — «люди» (Bybit), `research/crypto_btc/copytrade/humans/`
- D.GOLD - N — сессионный пробой золота (Bybit TradFi)

## Внешние площадки с длинной историей
- invvo.com — 13 стратегий (id 5–27), все через `run.py add invvo all`
- Кандидаты найти: myfxbook-подобные для крипты, публичные страницы ботов (GitHub Pages, Netlify), TradingView-стратегии с открытым кодом
