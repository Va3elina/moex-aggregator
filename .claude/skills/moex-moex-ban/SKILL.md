---
name: moex-moex-ban
description: Диагностика бана IP сервера у MOEX — когда ВСЕ MOEX-фетчеры разом виснут по таймауту. Use when user says «MOEX не отвечает», «все данные MOEX встали», «OI/свечи/индексы разом пропали», «таймауты у всех биржевых скриптов», «MOEX забанил», «apim/iss не отвечает». Отличает бан IP от сетевого блэкхола и от протухшего токена; помнит что это транзиент на часы; что делать и чего НЕ делать.
---

# MOEX IP-бан / сетевой блэкхол — рунбук

**Паттерн (2 случая июнь + 01.07.2026): транзиентный блэкхол, самолечится за ЧАСЫ.**
Не паниковать, не менять IP вслепую. Сервер `103.88.243.232`.

## Симптом
`pipeline_runs` показывает `Таймаут` у ВСЕХ MOEX-пайплайнов разом с одного времени T
(oi_5min 900с, candles_futures/spot 300с, index_intraday, futures_turnover,
contract_calendar). Non-MOEX (funds/cbonds, commodity/Yahoo, macro, dividends) — `ok`.
Один «5-мин» цикл висит ~28 мин (запросы висят до таймаута).

## Диагностика (~3 SSH-команды в одной сессии)
```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=30 -i ~/.ssh/id_ed25519 root@103.88.243.232 '
echo "=== iss с сервера ==="; curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" --max-time 12 https://iss.moex.com/iss/index.json
echo "=== apim с сервера ==="; curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" --max-time 12 https://apim.moex.com/iss/index.json
echo "=== нейтральный egress ==="; curl -s -o /dev/null -w "cloudflare %{http_code}\n" --max-time 8 https://1.1.1.1
echo "=== пинг iss ==="; ping -c 3 iss.moex.com 2>&1 | tail -2
'
```
Интерпретация:
- **iss/apim таймаут 12с + ping 100% loss, а cloudflare 200** → блэкхол именно на MOEX
  (не нехватка ресурсов — она тормозила бы ВСЁ исходящее одинаково).
- **iss=200 быстро** → бан УЖЕ снят (или его не было) → это «хвост дня-после», см. ниже.
- Токен НИ ПРИ ЧЁМ: валиден до 2027-05-31; при протухшем ключе молчал бы ТОЛЬКО apim, не iss.

## Почему (корень)
НЕ burst (конкурентность скромная: Semaphore(10)), а **sustained rate** — ровный высокий
поток в торг.часы (тысячи req/час, apim/Algopack у грани). MOEX не шлёт 429/warning —
сразу тихий L4-дроп на сетевой кромке (трассировка гаснет за DataIX `ix.dataix.eu`).
Предсказать из логов нельзя by design.

## ⚠️ «MOEX сказал бана нет» — не противоречие
Их 1-я линия смотрит прикладной блок-лист (чисто), а пакеты дропаются на СЕТЕВОЙ кромке
(edge/anti-DDoS ИЛИ транзит Timeweb→DataIX→MOEX). Обе правды одновременно. Ответ «бана нет»
УСИЛИВАЕТ версию сетевого транзита, а не снимает вопрос.

## Что делать
1. **Проверить доступность** (команды выше). Если iss уже 200 → связь вернулась, просто
   дать gap-safe фетчерам догнать (не трогать IP).
2. **Данные НЕ теряются**: MOEX хранит историю, фетчеры gap-safe / добираются `--once --force`.
   После восстановления — один прогон на пайплайн закрывает дыру.
3. **Хвост «дня-после»** (частый!): наутро мониторинг кричит «3 скрипта упали и не встали»,
   но это ДНЕВНЫЕ пайплайны (indices_daily/dividends_daily/contract_calendar, блок 19:10 МСК),
   упавшие в окне бана — у них НЕТ новых попыток до вечера, `last_status=fail` «липнет» сутки.
   Диф за 1 SSH: 5-минутки `ok` + iss curl 200 = сеть жива = хвост. Закрыть идемпотентно:
   ```bash
   ssh ... 'docker exec frame-orchestrator-1 sh -c "
     python3 Funds/fetch_indices_realtime.py --once;           # БЕЗ --force!
     python3 Candles/fetch_contract_calendar.py --once --force;
     python3 Candles/fetch_dividends.py --once --force"'        # ~36с несмотря на таймаут 900с
   ```
   `pipeline_runs` руками НЕ править — вечерний блок 19:10 сам перепишет на ok.

## Что делать ВАДИМУ (я не могу)
- Тикет в Algopack/MOEX на снятие бана IP + whitelist (в июне сняли за ~сутки).
- Если рецидив с traceroute-пруфом: письмо MOEX (проверить edge/anti-DDoS по 103.88.243.0/24
  / AS9123) + тикет Timeweb (маршрут до 85.118.181.0/24).

## Профилактика (я могу, backend-only)
Снижать sustained rate: вынести contract_calendar в дневной блок, индексы внутридневно реже,
срезать вложенные ретраи. Риск рецидива ↓ (не 0 — порог MOEX непрозрачен).

Связано: [[moex_ip_ban]], [[ingestion_map]], [[timeweb_api]], [[monitoring_system]].
