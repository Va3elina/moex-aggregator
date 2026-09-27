-- Метрики завода постов (Вадим 27.09: «метрики хотя бы сейчас: от получения новости до черновика; хорошо ли мы всё
-- делаем, верные ли факты, хорошие ли механизмы отбора»). Окно — :days дней (по умолчанию 30). Только чтение.
--   ssh root@103.88.243.232 'docker exec -i frame-db-1 psql -U postgres -d moex_db -v days=30' < research/content_pipeline_v2/factory_metrics.sql
\pset pager off
\if :{?days} \else \set days 30 \endif

\echo === 1. Скорость, минуты (медиана; у новостей Telegram ещё 90-й перцентиль)
WITH c AS (
  SELECT c.id, CASE WHEN c.source IN ('markettwits','newssmartlab') THEN 'новости tg'
                    WHEN c.source IN ('insight','combo') THEN c.source ELSE 'новости прочие' END AS конвейер,
         c.created_at, c.judge_checked_at, c.reviewer_notified_at,
         (SELECT min(t.created_at) FROM agent_trace t WHERE t.candidate_id = c.id AND t.step = 'писатель'
                                                         AND t.result_note LIKE 'черновик%') AS drafted,
         w.posted_at AS news_at
  FROM content_candidates c
  LEFT JOIN tg_channel_watch w ON c.source_url = 'https://t.me/' || w.channel || '/' || w.message_id
  WHERE c.created_at > now() - make_interval(days => :days) AND c.draft_text_ai IS NOT NULL)
SELECT конвейер, count(*) черновиков,
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM created_at - news_at)/60)::numeric) "новость→кандидат",
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM drafted - news_at)/60)::numeric) "новость→черновик",
  round(percentile_cont(0.9) WITHIN GROUP (ORDER BY extract(epoch FROM drafted - news_at)/60)::numeric) "…90-й перц.",
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM drafted - created_at)/60)::numeric) "кандидат→черновик",
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM judge_checked_at - drafted)/60)::numeric) "черновик→судья",
  round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM reviewer_notified_at - drafted)/60)::numeric) "черновик→бот"
FROM c GROUP BY 1 ORDER BY 2 DESC;

\echo === 2. Воронка по источникам
SELECT source,
  count(*) кандидатов,
  count(*) FILTER (WHERE status = 'discarded' AND importance_1_5 < 3) "Шаг А: неважно",
  count(*) FILTER (WHERE status = 'discarded' AND importance_1_5 >= 3 AND position('тикер не резолвился' in coalesce(reasoning,'')) > 0) "Шаг А: нет тикера",
  count(*) FILTER (WHERE status = 'discarded' AND synth_declined_reason IS NOT NULL) "отсевы",
  count(*) FILTER (WHERE status = 'no_data') "Шаг Б: нет всплеска",
  count(*) FILTER (WHERE status IN ('pending', 'candidate')) "в работе",
  count(*) FILTER (WHERE draft_text_ai IS NOT NULL) черновиков,
  count(*) FILTER (WHERE status = 'published') опубликовано
FROM content_candidates WHERE created_at > now() - make_interval(days => :days) GROUP BY 1 ORDER BY 2 DESC;

\echo === 3. Отсевы перед писателем по причинам
SELECT CASE WHEN synth_declined_reason LIKE 'повтор%' THEN 'повтор тикера 3 дня'
            WHEN synth_declined_reason LIKE 'новость устарела%' THEN 'новость старше 36 ч'
            WHEN synth_declined_reason LIKE '%R16%' THEN 'R16 цена не отреагировала'
            WHEN synth_declined_reason LIKE '%R17%' THEN 'R17 дивиденд меньше 6%'
            WHEN synth_declined_reason LIKE 'Routine%' THEN 'писатель не ответил'
            ELSE 'прочее: ' || left(synth_declined_reason, 40) END AS причина, count(*)
FROM content_candidates WHERE synth_declined_reason IS NOT NULL AND created_at > now() - make_interval(days => :days)
GROUP BY 1 ORDER BY 2 DESC;

\echo === 4. Факты: провалы ворот A у живого судьи
SELECT CASE WHEN source IN ('insight','combo') THEN source ELSE 'новости' END AS конвейер, count(*) "с судьёй",
  count(*) FILTER (WHERE (judge_items->'items'->>'numbers_traceable') = 'false') "число/дата без опоры",
  count(*) FILTER (WHERE (judge_items->'items'->>'no_invented_facts') = 'false') "додумка",
  count(*) FILTER (WHERE (judge_items->'items'->>'no_self_contradiction') = 'false') "противоречие",
  count(*) FILTER (WHERE (judge_items->'items'->>'time_arrow_ok') = 'false') "время",
  count(*) FILTER (WHERE judge_fixed_at IS NOT NULL) "поправил судья"
FROM content_candidates WHERE created_at > now() - make_interval(days => :days) AND judge_items IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

\echo === 5. Решения людей
SELECT CASE WHEN c.source IN ('insight','combo') THEN c.source ELSE 'новости' END конвейер, f.event, count(*),
       string_agg(DISTINCT f.reason_code, ', ') FILTER (WHERE f.reason_code IS NOT NULL AND f.reason_code <> 'judge') коды
FROM content_feedback f JOIN content_candidates c ON c.id = f.candidate_id
WHERE f.created_at > now() - make_interval(days => :days) AND f.event IN ('approved', 'rejected', 'comment', 'edited')
GROUP BY 1, 2 ORDER BY 1, 2;

\echo === 6. Хвост в боте: черновики без решения
SELECT count(*) "без решения", min(reviewer_notified_at)::date "самый старый"
FROM content_candidates WHERE status = 'draft_ready' AND reviewer_notified_at IS NOT NULL
  AND created_at > now() - make_interval(days => :days);
