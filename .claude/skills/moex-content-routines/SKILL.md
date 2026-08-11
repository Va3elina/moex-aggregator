---
name: moex-content-routines
description: Управление Claude Code Routine-агентами content-пайплайна («завод постов») через инструмент RemoteTrigger — Шаг А/В/Н (frame-content-step-a/-c/-n) + frame-monitor. Use when user says «создай Routine-агента», «почини Шаг А/В/Н», «Routine не отвечает», «заведи нового агента для пайплайна», «промпт Routine не долетает», «egress/сеть у Routine заблокирована», «посмотри триггеры», или когда нужно диагностировать/чинить/создавать Claude Code Routine программно, не через веб-UI claude.ai/code/routines.
---

# Content-пайплайн: Routine-агенты через RemoteTrigger

**Найдено 2026-07-16** (после инцидента с Шагом Н, см. [[content_pipeline_design]] п.14-15).
`RemoteTrigger` — deferred-инструмент, прямой доступ к `claude.ai/code/routines` API
(`list`/`get`/`create`/`update`/`run`) **из сессии Claude Code, без похода в веб-UI**.
Загрузить в начале работы: `ToolSearch("select:RemoteTrigger")`.

⚠️ Старое убеждение из докстринга `signals/content_ai.py` («Internal RemoteTrigger
НЕ передаёт per-run данные — тупиковый путь») — **неверно** для `action=run`: тело
`{"text": "..."}` добавляется как user-сообщение поверх статичного промпта Routine,
точно так же, как публичный `fire`-эндпоинт (`_fire()` в `content_ai.py`), и
возвращает `session_id`. Единственное, чего РЕАЛЬНО нет — доступа к
live-транскрипту/логам уже идущей сессии (только факт запуска + то, что сама
Routine решит записать через наш собственный callback-эндпоинт).

## Два способа "выстрелить" Routine — не путать

1. **Публичный `fire`-эндпоинт** (`signals/content_ai.py:_fire()`, host-side скрипты
   `tg_hype_scan.py`/`content_ai.py`) — `POST /v1/claude_code/routines/{trigger_id}/fire`
   с bearer-токеном КОНКРЕТНОГО триггера (в `.env`: `CLAUDE_ROUTINE_FIRE_TOKEN_STEP_A`
   и т.д.). Через Cloudflare-релей (`CLAUDE_ROUTINE_API_ROOT` в `.env` — api.anthropic.com
   отдаёт 403 с российских IP). Это то, чем production-пайплайн стреляет сам себя
   на каждый новый кандидат.
2. **`RemoteTrigger` (этот скилл)** — из сессии Claude Code, `action=run` с телом
   `{"text": "..."}`. Использовать для: ручного теста/бэкфилла конкретного кандидата,
   диагностики, создания НОВОГО триггера. НЕ подмена продовому firing-механизму
   (тот работает независимо, через `.env`-токены).

## Действующие триггеры (список через `RemoteTrigger action=list`)

| name | trigger_id | environment_id | allowed_tools | роль |
|---|---|---|---|---|
| frame-content-step-a | `trig_01CTyFze4rXBRGwPKVFtSooj` | `env_01GNzSXGSPBc76secixtxtZu` | Bash, WebSearch | тикер/значимость (для "завода") |
| frame-content-step-c | `trig_01KPtMNbEYNfqewKvwhdo4rj` | `env_01GNzSXGSPBc76secixtxtZu` | Bash | синтез черновика поста |
| frame-content-step-n | `trig_01HpPTBmK6egXVdjETZw48FA` | `env_01GNzSXGSPBc76secixtxtZu` | Bash | шум/новость (для бота коллеги, НЕ трогает статус кандидата) |
| frame-monitor | `trig_01Cen2AL3BQYsHvf99QDMMuG` | `env_01GNzSXGSPBc76secixtxtZu` | Bash, Read, Write, Edit, Glob, Grep, WebFetch, WebSearch | ежедневный health-аудит (cron `30 7 * * *`) |

Проверить актуальный список/детали (`get` тоже отдаёт полный промпт триггера):
```
RemoteTrigger action=list
RemoteTrigger action=get trigger_id=trig_...
```

## ⚠️⚠️ Главная ловушка: environment_id должен совпадать с рабочими триггерами

**Живой инцидент 2026-07-16**: новый триггер (`frame-content-step-n`), созданный
через веб-UI, получил ДРУГОЙ `environment_id` (`env_01MNVpX2zAcQ9LxCW1A3frA9`) и
более широкий `allowed_tools` (`WebFetch`/`WebSearch` вместо чистого `Bash`), чем у
рабочих Шага А/В. У этого нового окружения egress-прокси блокирует
`xn--80aklbnczmv.xn--p1ai` (403 на CONNECT) — классификация отрабатывала верно,
но callback физически не мог улететь. Симптом: Routine "успешно завершается"
(нет ошибки со стороны Anthropic), но `PATCH .../hype-filter` (или `step-a`/`step-c`)
никогда не приходит в логи api-контейнера — **отличить от Routine, которая просто
долго думает (до 5+ мин — это нормально), можно только по факту, что колбэк НЕ
приходит уже 10+ минут при повторных перезапусках**.

**Диагностика**: `RemoteTrigger action=list` → сравнить `job_config.ccr.environment_id`
и `session_context.allowed_tools` проблемного триггера с рабочим (Шаг А/В).

**Фикс — `RemoteTrigger action=update`**, тело:
```json
{"job_config": {"ccr": {
  "environment_id": "env_01GNzSXGSPBc76secixtxtZu",
  "events": [...тот же промпт-event, что уже был...],
  "session_context": {"allowed_tools": ["Bash"], "sources": [{"git_repository": {"url": "https://github.com/Va3elina/moex-aggregator"}}]}
}}}
```
⚠️ `update` — partial, но `job_config.ccr` нужно передать ЦЕЛИКОМ (включая
`events` с полным текстом промпта) — иначе рискуешь стереть промпт. Скопировать
текущее значение из `action=get` перед правкой.

После фикса — `action=run` с тестовым payload, проверить в логах api-контейнера
(`journalctl -t frame-api --since '10 min ago' | grep hype-filter`), что колбэк
дошёл с реалистичной длительностью (200-400мс — реальный исходящий HTTP-вызов, не
мгновенный отказ и не зависание).

## Шаблон промпта нового Routine-агента (проверенный, копировать структуру)

Все три рабочих промпта (Шаг А/В/Н) следуют одному каркасу — не отклоняться:

1. Роль + ЕДИНСТВЕННАЯ узкая задача (одно решение, не пиши прозу/пост, если это не
   твоя работа).
2. Явный список входных полей, которые должны быть в payload — velit "если данных
   нет, сообщи явно, не выдумывай".
3. Конкретные правила оценки/калибровка (не оставлять модели простор для дрейфа
   критериев между запусками).
4. `⚠️ ВАЖНО: домен в URL — строго framedata.ru (ASCII, без кириллицы и без
   punycode-форм прежнего адреса). Кириллические домены ломают запрос в
   некоторых окружениях (проверено).`
   ⚠️ Все пять промптов переведены на `framedata.ru` 11.08.2026. Прежний адрес
   пока тоже отвечает — `/api/` и `/embed/` исключены из 301 ради установленного
   расширения 0.6.2 (`nginx/nginx.conf`), — но исключение временное: как снимут,
   любой оставшийся punycode-URL в промпте молча оборвёт колбэк.
5. **Явный `curl` через bash**, не абстрактное "сделай PATCH" — модель иначе может
   попробовать WebFetch/другой инструмент, который упрётся в другой сетевой путь.
   ```
   curl -s -X PATCH https://framedata.ru/api/internal/content-news/CANDIDATE_ID/<endpoint> \
     -H "X-Internal-Token: TOKEN" -H "Content-Type: application/json" \
     -d '{...}'
   ```
6. "Если curl завершится ошибкой — сообщи об этом явно, не молчи."
7. "Ничего, кроме этого одного PATCH-вызова, не публикуй/не отправляй."

Полный актуальный текст всех трёх промптов — в [[content_pipeline_design]] (там же
живая история находок) или через `RemoteTrigger action=get`.

## Создание нового триггера

```
RemoteTrigger action=create body={
  "name": "frame-content-step-X",
  "job_config": {"ccr": {
    "environment_id": "env_01GNzSXGSPBc76secixtxtZu",
    "events": [{"data": {"message": {"content": "<промпт по шаблону выше>", "role": "user"}}}],
    "session_context": {"allowed_tools": ["Bash"], "sources": [{"git_repository": {"url": "https://github.com/Va3elina/moex-aggregator"}}]}
  }}
}
```
Ответ содержит `trigger_id` и bearer-токен (`api_token_hint` — полный токен только
при создании, потом только hint) — если новый шаг должен стрелять из production-кода
(host-скрипта), токен и `trigger_id` уходят в `.env` (`TRIGGER_ID_<ИМЯ>`,
`CLAUDE_ROUTINE_FIRE_TOKEN_<ИМЯ>`) — код читает их через `os.environ.get(...)` с
пустым дефолтом (не хардкодить, деплой кода не должен зависеть от того, создан ли
уже Routine — см. `TRIGGER_ID_HYPE_FILTER` в `content_ai.py` как образец).

## Ограничения

- Нет доступа к live-логам/транскрипту сессии — только факт запуска (`session_id`
  из ответа `run`) и то, что Routine сама запишет через наш callback.
- `RemoteTrigger` работает от ИМЕНИ Вадима (его Anthropic-аккаунт) — те же дневные
  лимиты/квоты, что и у интерактивных Claude Code сессий этого аккаунта (см.
  [[content_pipeline_design]] про инцидент с исчерпанием подписки утром 16.07 —
  Шаг В не смог дописать 2 черновика, бэкстоп откатил их в `pending`, драфты
  пришлось руками вписывать в БД).
- Каждый `run`/`fire` — отдельная, независимо оплачиваемая облачная AI-сессия. Не
  спамить тестовыми прогонами без нужды.

Связано: [[content_pipeline_design]], [[signal_engine]].
