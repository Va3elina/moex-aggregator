-- 111 добавила статус 'no_securities' (13 символов), а колонка status — VARCHAR(12): каждая справка
-- «бумаг нет» падала на UPDATE (value too long) и после 3 попыток уходила в 'failed' (19 заданий:
-- SAFE 2021–2024, WFLO 09.2026). Ответ агента у них сохранён — возвращаем на шаг заливки, конвейер
-- (work, */5) сам поставит 'no_securities'. notified_at не трогаем: о них уже сообщали.
--   cat db/migrations/112_scha_jobs_status_width.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db
ALTER TABLE scha_jobs ALTER COLUMN status TYPE VARCHAR(16);

UPDATE scha_jobs
   SET status = 'agent_done', attempts = 0, last_error = NULL, next_try_at = now(), updated_at = now()
 WHERE status = 'failed' AND last_error LIKE '%''no_securities''%' AND agent_ok;
