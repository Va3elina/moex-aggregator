---
name: moex-analytics
description: Анализ продуктовой аналитики Фрейма — воронка, сессии, гости/зарегистрированные/платящие, источники, retention, поведение, "невидимки". Use when user asks "сколько сессий/людей", "откуда пришли", "что смотрят", "воронка", "конверсия", "сколько гостей vs зарегистрированных", "retention", "что делал юзер X", "сколько зарегались но ничего не делали", или хочет любой срез по analytics_events.
---

# Аналитика Фрейма

Продуктовая аналитика лежит в таблице `analytics_events` (прод-БД). Доступ — через
SSH + `docker exec` (см. [[moex-db-query]] для шаблона экранирования). Есть готовый
дашборд `/admin/stats` (эндпоинты `/api/analytics/stats` и `/api/analytics/funnel`)
— этот скилл для произвольных срезов, которых на дашборде нет.

## ⚠️ Ключевые факты (без них выводы будут неверные)

1. **Сессия = открытие вкладки, НЕ человек.** `session_id` — UUID в `sessionStorage`,
   умирает при закрытии вкладки. Один человек = много сессий (вернулся / новая вкладка).
   **Кол-во сессий ≠ кол-во людей.**
2. **Гостей нельзя дедуплицировать.** Нет постоянного visitor-id и точного IP (только
   `ip_country`). Уникальных гостей посчитать нельзя — только сессии (верхняя граница).
   Уникальные ЛЮДИ считаются только среди залогиненных (по `user_id`).
3. **Аналитика consent-gated** → есть «невидимки»: юзер зарегистрировался, но отклонил
   cookie → его действия НЕ пишутся в `analytics_events` (выглядит как «0 действий»).
   Server-side активности пока нет (фаза 3 модуля).
4. **Источники (referrer/UTM)** собираются с фазы 2 (2026-06): на ПЕРВОМ событии
   сессии пишется `payload.acq = {ref:<хост реферера>, utm_source, utm_medium, ...}`.
   Только новые сессии после деплоя имеют `acq` (старые — без источника).
5. **Боты в основном отфильтрованы:** события пишутся клиентским JS → не-JS краулеры
   сюда не попадают. Это «человеческий» прокси (с поправкой на consent-ghosts).

## Схема `analytics_events`
```
event_id    — PK
user_id     — NULL у гостей; FK на users (залогиненные)
session_id  — UUID v4, per-вкладка (sessionStorage)
event_type  — pageview | instrument_select | seasonality_mode | chart_export | theme_toggle | session_heartbeat
event_path  — путь страницы ('/oi', '/funds-money', ...)
payload     — jsonb (instrument_select: {from, secid}; theme_toggle: {to}; ...)
client_ts   — время на клиенте
server_ts   — время на сервере (для фильтра по дате используй ЕГО)
ip_country  — код страны (НЕ точный IP)
device      — desktop | mobile | tablet
```

## Шаблон запуска (read-only)
```bash
ssh -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 \
    -i ~/.ssh/id_ed25519 root@103.88.243.232 \
    "docker exec -i frame-api-1 python3 -" < /tmp/analytics_q.py
```
Скрипт пишем локально в `/tmp/*.py` и пайпим — меньше боли с экранированием.
Внутри: `from sqlalchemy import create_engine, text; engine=create_engine(os.environ["DB_URL"])`.
Сеть до РФ-хоста флапает → оборачивай в retry-цикл (6 попыток, пауза 6с).

## Готовые срезы (SQL)

### Люди vs сессии
```sql
-- залогиненные ЛЮДИ (точно) + их сессии
SELECT COUNT(DISTINCT user_id) people, COUNT(DISTINCT session_id) sessions
FROM analytics_events WHERE user_id IS NOT NULL;
-- гостевые сессии (= верхняя граница гостей-людей, дедуп невозможен)
SELECT COUNT(DISTINCT session_id) FROM analytics_events WHERE user_id IS NULL;
```

### Воронка визит → регистрация → активный → платящий
```sql
SELECT
 (SELECT COUNT(DISTINCT session_id) FROM analytics_events)                       AS sessions,
 (SELECT COUNT(*) FROM users)                                                    AS registered,
 (SELECT COUNT(DISTINCT user_id) FROM analytics_events WHERE user_id IS NOT NULL) AS active,
 (SELECT COUNT(*) FROM subscriptions WHERE status='active' AND expires_at>now())  AS paying;
```

### «Невидимки» — зарегались, но 0 событий (consent-отказники / отскок)
```sql
SELECT u.id, u.created_at::date, u.oauth_provider
FROM users u
WHERE NOT EXISTS (SELECT 1 FROM analytics_events a WHERE a.user_id=u.id)
ORDER BY u.created_at DESC;
```

### Гость / зарегистрированный / платящий — разрез
```sql
SELECT CASE WHEN user_id IS NULL THEN 'guest' ELSE 'registered' END grp,
       COUNT(DISTINCT session_id) sessions, COUNT(*) events
FROM analytics_events GROUP BY grp;
```

### Что смотрят (топ-страниц) + глубина
```sql
SELECT event_path, COUNT(*) views, COUNT(DISTINCT session_id) sessions,
       ROUND(COUNT(*)::numeric/NULLIF(COUNT(DISTINCT session_id),0),1) depth
FROM analytics_events WHERE event_type='pageview'
GROUP BY event_path ORDER BY sessions DESC LIMIT 25;
```

### Куда пошли дальше (поток переходов страница → следующая)
```sql
WITH seq AS (
  SELECT session_id, event_path,
         LEAD(event_path) OVER (PARTITION BY session_id ORDER BY client_ts) next_path
  FROM analytics_events WHERE event_type='pageview')
SELECT event_path AS "из", next_path AS "в", COUNT(*) n
FROM seq WHERE next_path IS NOT NULL
GROUP BY 1,2 ORDER BY n DESC LIMIT 30;
```

### Что делал конкретный юзер (таймлайн)
```sql
SELECT event_type, event_path, payload, server_ts
FROM analytics_events WHERE user_id=:uid ORDER BY server_ts DESC LIMIT 50;
```

### Топ-тикеров (instrument_select)
```sql
SELECT payload->>'secid' sec, COUNT(*) n FROM analytics_events
WHERE event_type='instrument_select' AND payload->>'secid' IS NOT NULL
GROUP BY sec ORDER BY n DESC LIMIT 20;
```

### Глубина сессий (вовлечённость = люди, а не отскок)
```sql
SELECT COUNT(*) FILTER (WHERE v=1) bounce,
       COUNT(*) FILTER (WHERE v BETWEEN 2 AND 5) light,
       COUNT(*) FILTER (WHERE v>5) deep
FROM (SELECT session_id, COUNT(*) v FROM analytics_events
      WHERE event_type='pageview' GROUP BY session_id) s;
```

### Источники — откуда пришли (фаза 2: payload.acq на первом событии сессии)
```sql
-- топ источников (хост реферера); 'direct' = прямой заход/без реферера
SELECT COALESCE(payload->'acq'->>'ref', '(direct)') AS source,
       COUNT(DISTINCT session_id) AS sessions
FROM analytics_events
WHERE payload ? 'acq'
GROUP BY source ORDER BY sessions DESC LIMIT 25;

-- UTM-кампании (если ставим метки в ссылках)
SELECT payload->'acq'->>'utm_source'   AS utm_source,
       payload->'acq'->>'utm_campaign' AS campaign,
       COUNT(DISTINCT session_id) AS sessions
FROM analytics_events
WHERE payload->'acq' ? 'utm_source'
GROUP BY 1,2 ORDER BY sessions DESC;
```
Примечание: `acq` есть только у сессий ПОСЛЕ деплоя фазы 2. Долю покрытия проверяй
`COUNT(*) FILTER (WHERE payload ? 'acq') / COUNT(DISTINCT session_id)`.

## Гайд по интерпретации
- «Сколько людей?» — залогиненных по `user_id`; гостей — НЕ дать (только сессии). Не
  выдавай число сессий за число людей.
- «Конверсия» — считай от того, что реально измеримо; помни про consent-ghosts
  (реальная активность выше видимой).
- Перед выводами проверь окно дат (`server_ts`) и фильтр admin/own-IP (наши заходы
  на /admin/* зашумляют).

## Связанные
[[moex-db-query]] (шаблон SSH+psql), [[monitoring_system]] (routine-подход для агента).
Модуль аналитики (фазы): 1 этот скилл · 2 источники referrer/utm · 3 server-side
активность (ghosts) · 4 visitor-id (уникальные гости/retention) + поток + агент-routine.
