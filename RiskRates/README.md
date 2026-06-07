# RiskRates — риск-параметры НКЦ для «индекса страха»

Дневные ставки риска НКЦ/MOEX как мера рыночного страха: НКЦ повышает
ставку при росте волатильности/стресса. Высокая ставка = страх,
низкая = покой/жадность. Ряд **ступенчатый** (меняется редко, скачками).

## Источники (ISS MOEX, бесплатные, без ключа)

| Источник | Эндпоинт | Поле | Глубина |
|---|---|---|---|
| Срочный рынок | `rms/engines/futures/objects/limits` | `mr1/mr2/mr3` | с 2018-05-22 |
| Валютный рынок | `rms/engines/currency/objects/marketrates` | `discount` | с 2014-12-26 |

`limits` покрывает индекс МосБиржи (`MIX`), нефть (`BR`), газ (`NG`/`TTF`),
Газпром (`GAZR`), ВТБ (`VTBR`), Сбер (`SBRF`), доллар (`Si`), юань (`CNY`) и др.

## Сбор данных

```bash
python3 RiskRates/fetch_risk_rates.py              # максимальная глубина
python3 RiskRates/fetch_risk_rates.py --from 2020-01-01
python3 RiskRates/fetch_risk_rates.py --only currency
```

Сырьё пишется в `data/` (gitignored). Курируемые ряды и сгенерированные
PineScript-индикаторы — в `datasets/` и `pine/`.
