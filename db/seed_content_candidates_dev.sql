-- Тестовые данные для /admin/content-news (Kanban content-пайплайна) — ТОЛЬКО
-- для локальной разработки UI. Пайплайн сбора/оценки ещё не подключён к реальным
-- источникам (см. db/migrations/022_content_candidates.sql), поэтому карточки
-- здесь — ручной фикстур для проверки всех 8 колонок доски + группировки тредов.
--
-- НЕ ПРИМЕНЯТЬ НА ПРОДЕ. Перед применением накатить миграции 015 и 022:
--   psql -U postgres -d moex_db -f db/migrations/015_anomaly_feed.sql
--   psql -U postgres -d moex_db -f db/migrations/022_content_candidates.sql
--   psql -U postgres -d moex_db -f db/seed_content_candidates_dev.sql
--
-- Повторный запуск: аномалии защищены ON CONFLICT DO NOTHING (уник-индекс
-- миграции 015); строки content_candidates НЕ идемпотентны — перед повтором
-- either TRUNCATE content_candidates RESTART IDENTITY CASCADE; либо игнорировать
-- дубли (для UI-теста не критично).

-- Две "реальные" аномалии — под matched_anomaly_id черновиков (Шаг Б пайплайна:
-- совпадение кандидата с open-interest/потоками).
INSERT INTO anomalies (scope, type, asset_id, asset_name, clgroup, direction, headline, context, severity_value, signal_date, deep_link)
VALUES
  ('public', 'oi_move', 'SR', 'Сбербанк', 'FIZ', 'up',
   'Резкий рост ОИ физлиц по SBER', 'Открытый интерес +18% за день на фоне дивидендной новости', 4.2,
   CURRENT_DATE, '{"route":"/oi","secid":"SR","interval":24,"period":"1m","clgroup":"FIZ"}'),
  ('public', 'funds_flow', 'GAZP', 'Газпром', NULL, 'down',
   'Отток из фондов на ГАЗПРОМ', 'Чистый отток 3 торговых дня подряд', 3.1,
   CURRENT_DATE - 1, '{"route":"/funds-money","secid":"GAZP"}')
ON CONFLICT DO NOTHING;

-- 18 кандидатов, по 2-3 на каждую из 8 колонок. Два треда сквозь несколько
-- статусов: 'sber-div-2026' (кандидат → черновик → ревью → опубликовано) и
-- 'gazp-outflow' (кандидат → черновик → ревью) — проверяют цветную связку
-- карточек по thread_key на доске.
INSERT INTO content_candidates (
  status, source, headline, raw_text, tickers, futures_ticker, event_type,
  importance_1_5, reasoning, matched_anomaly_id, draft_text, synth_declined_reason,
  thread_key, parent_candidate_id, forwards_count, created_at, updated_at,
  pending_expires_at, published_at, reviewer_action, reviewer_id
) VALUES
  -- ── candidate ──────────────────────────────────────────────────────────
  ('candidate', 'rss:kommersant', 'СБЕР объявил дивиденды за 2026 год',
   'Наблюдательный совет Сбербанка рекомендовал выплатить дивиденды за 2026 год в размере ... на акцию.',
   ARRAY['SBER'], 'SR', 'dividends', 4,
   'Крупная дивидендная новость по голубой фишке, есть основания ждать реакции в открытом интересе',
   NULL, NULL, NULL, 'sber-div-2026', NULL, NULL,
   now() - interval '2 hours', now() - interval '2 hours', NULL, NULL, NULL, NULL),

  ('candidate', 'rss:vedomosti', 'Газпром снижает добычу на фоне падения экспорта',
   'Компания сократила добычу газа на N% за квартал на фоне снижения экспортных поставок в Европу.',
   ARRAY['GAZP'], 'GZ', 'production', 3,
   'Умеренно значимо, требуется проверка вторичных индикаторов',
   NULL, NULL, NULL, 'gazp-outflow', NULL, NULL,
   now() - interval '5 hours', now() - interval '5 hours', NULL, NULL, NULL, NULL),

  ('candidate', 'markettwits', 'ЦБ РФ созывает внеочередное заседание по ставке',
   'По неподтверждённым данным, совет директоров ЦБ проведёт внеочередное заседание на следующей неделе.',
   ARRAY[]::text[], NULL, 'macro', 5,
   'Макрособытие высокой значимости для всего рынка, ждёт оценки ИИ',
   NULL, NULL, NULL, NULL, NULL, 340,
   now() - interval '20 minutes', now() - interval '20 minutes', NULL, NULL, NULL, NULL),

  -- ── discarded ──────────────────────────────────────────────────────────
  ('discarded', 'rss:finam', 'Ежедневный обзор рынка от аналитика',
   'Общий обзор торгов за день без конкретного триггера.',
   ARRAY[]::text[], NULL, 'commentary', 1,
   'Общий комментарий без конкретного рыночного триггера — не тянет на новость',
   NULL, NULL, NULL, NULL, NULL, NULL,
   now() - interval '1 day', now() - interval '1 day', NULL, NULL, NULL, NULL),

  ('discarded', 'rss:rbc', 'ВТБ обновил фирменный стиль и логотип',
   'Банк представил обновлённый логотип и айдентику.',
   ARRAY['VTBR'], NULL, 'other', 1,
   'Не рыночно значимое событие, отброшено на Шаге А',
   NULL, NULL, NULL, NULL, NULL, NULL,
   now() - interval '2 days', now() - interval '2 days', NULL, NULL, NULL, NULL),

  -- ── pending (окно ожидания ≤5 дней) ────────────────────────────────────
  ('pending', 'markettwits', 'Норникель объявил о крупной сделке M&A',
   'Компания подтвердила переговоры о приобретении профильного актива.',
   ARRAY['GMKN'], 'GK', 'ma_deal', 4,
   'Релевантно, ждём подтверждения через открытый интерес',
   NULL, NULL, NULL, NULL, NULL, 87,
   now() - interval '4 days 20 hours', now() - interval '4 days 20 hours',
   now() - interval '4 days 20 hours' + interval '5 days', NULL, NULL, NULL),

  ('pending', 'rss:kommersant', 'Лукойл начинает байбек акций',
   'Совет директоров одобрил программу обратного выкупа акций на сумму ...',
   ARRAY['LKOH'], 'LK', 'buyback', 4,
   'Релевантно, ждём подтверждения через открытый интерес/потоки фондов',
   NULL, NULL, NULL, NULL, NULL, NULL,
   now() - interval '1 day', now() - interval '1 day',
   now() - interval '1 day' + interval '5 days', NULL, NULL, NULL),

  ('pending', 'rss:vedomosti', 'Новатэк отчитался о падении экспорта СПГ',
   'Экспорт сжиженного газа снизился на фоне санкционных ограничений.',
   ARRAY['NVTK'], 'NM', 'production', 3,
   'Релевантно, ждём подтверждения в данных',
   NULL, NULL, NULL, NULL, NULL, NULL,
   now() - interval '3 days', now() - interval '3 days',
   now() - interval '3 days' + interval '5 days', NULL, NULL, NULL),

  -- ── draft_ready (совпадение найдено, черновик написан) ─────────────────
  ('draft_ready', 'rss:kommersant', 'СБЕР: резкий рост открытого интереса физлиц',
   'Наблюдательный совет Сбербанка рекомендовал дивиденды за 2026 год.',
   ARRAY['SBER'], 'SR', 'dividends', 4,
   'Совпадение подтверждено: аномалия по OI на следующий день после новости',
   (SELECT id FROM anomalies WHERE asset_id = 'SR' ORDER BY id DESC LIMIT 1),
   'Физлица резко нарастили длинные позиции по фьючерсу SR на фоне дивидендной новости SBER — открытый интерес вырос на 18% за день.',
   NULL, 'sber-div-2026',
   (SELECT id FROM content_candidates WHERE thread_key = 'sber-div-2026' AND status = 'candidate' LIMIT 1),
   NULL, now() - interval '1 hour', now() - interval '1 hour', NULL, NULL, NULL, NULL),

  ('draft_ready', 'rss:vedomosti', 'Отток капитала из фондов ГАЗПРОМ подтверждён потоками',
   'Компания сократила добычу газа на фоне падения экспорта.',
   ARRAY['GAZP'], 'GZ', 'production', 3,
   'Совпадение подтверждено: устойчивый отток в БПИФах на акции Газпрома',
   (SELECT id FROM anomalies WHERE asset_id = 'GAZP' ORDER BY id DESC LIMIT 1),
   'Потоки фондов показывают чистый отток из ГАЗПРОМ третий день подряд — коррелирует с новостью о снижении добычи.',
   NULL, 'gazp-outflow',
   (SELECT id FROM content_candidates WHERE thread_key = 'gazp-outflow' AND status = 'candidate' LIMIT 1),
   NULL, now() - interval '3 hours', now() - interval '3 hours', NULL, NULL, NULL, NULL),

  -- ── no_data (окно истекло, совпадение не нашлось) ──────────────────────
  ('no_data', 'rss:rbc', 'Слух о делистинге иностранной компании',
   'Появились неподтверждённые слухи о делистинге бумаги с биржи.',
   ARRAY[]::text[], NULL, 'other', 2,
   'Окно ожидания истекло (5 дней) — совпадение в открытом интересе/потоках не найдено',
   NULL, NULL, NULL, NULL, NULL, NULL,
   now() - interval '6 days', now() - interval '1 day', now() - interval '1 day', NULL, NULL, NULL),

  ('no_data', 'rss:finam', 'Плюс Банк объявил ребрендинг',
   'Банк представил новое название и стратегию развития розницы.',
   ARRAY[]::text[], NULL, 'other', 1,
   'Окно ожидания истекло — связанных данных по OI/потокам не найдено',
   NULL, NULL, NULL, NULL, NULL, NULL,
   now() - interval '7 days', now() - interval '2 days', now() - interval '2 days', NULL, NULL, NULL),

  -- ── in_review (черновик отправлен в Telegram-бот, ждёт человека) ───────
  ('in_review', 'rss:kommersant', 'СБЕР: черновик поста отправлен на ревью',
   'Наблюдательный совет Сбербанка рекомендовал дивиденды за 2026 год.',
   ARRAY['SBER'], 'SR', 'dividends', 4, NULL,
   (SELECT id FROM anomalies WHERE asset_id = 'SR' ORDER BY id DESC LIMIT 1),
   'Физлица резко нарастили длинные позиции по фьючерсу SR на фоне дивидендной новости SBER — открытый интерес вырос на 18% за день.',
   NULL, 'sber-div-2026',
   (SELECT id FROM content_candidates WHERE thread_key = 'sber-div-2026' AND status = 'draft_ready' LIMIT 1),
   NULL, now() - interval '30 minutes', now() - interval '30 minutes', NULL, NULL, NULL, NULL),

  ('in_review', 'rss:vedomosti', 'Отток ГАЗПРОМ — пост на ревью у модератора',
   'Компания сократила добычу газа на фоне падения экспорта.',
   ARRAY['GAZP'], 'GZ', 'production', 3, NULL,
   (SELECT id FROM anomalies WHERE asset_id = 'GAZP' ORDER BY id DESC LIMIT 1),
   'Потоки фондов показывают чистый отток из ГАЗПРОМ третий день подряд — коррелирует с новостью о снижении добычи.',
   NULL, 'gazp-outflow',
   (SELECT id FROM content_candidates WHERE thread_key = 'gazp-outflow' AND status = 'draft_ready' LIMIT 1),
   NULL, now() - interval '45 minutes', now() - interval '45 minutes', NULL, NULL, NULL, NULL),

  -- ── published ───────────────────────────────────────────────────────────
  ('published', 'rss:kommersant', 'Опубликовано: рост ОИ по SBER после дивидендов',
   'Наблюдательный совет Сбербанка рекомендовал дивиденды за 2026 год.',
   ARRAY['SBER'], 'SR', 'dividends', 4, NULL,
   (SELECT id FROM anomalies WHERE asset_id = 'SR' ORDER BY id DESC LIMIT 1),
   'Физлица резко нарастили длинные позиции по фьючерсу SR на фоне дивидендной новости SBER.',
   NULL, 'sber-div-2026',
   (SELECT id FROM content_candidates WHERE thread_key = 'sber-div-2026' AND status = 'in_review' LIMIT 1),
   NULL, now() - interval '1 day', now() - interval '2 hours', NULL, now() - interval '2 hours', 'approved', 4),

  ('published', 'rss:kommersant', 'Опубликовано: Лукойл байбек — разбор',
   'Совет директоров одобрил программу обратного выкупа акций.',
   ARRAY['LKOH'], 'LK', 'buyback', 3, NULL, NULL,
   'Лукойл начал обратный выкуп акций — разбор влияния на free-float и котировки.',
   NULL, NULL, NULL, NULL, now() - interval '2 days', now() - interval '5 hours', NULL, now() - interval '5 hours', 'approved', 4),

  -- ── rejected ────────────────────────────────────────────────────────────
  ('rejected', 'rss:rbc', 'Отклонено: слишком спекулятивный пост про ВТБ',
   'Появились слухи о смене менеджмента в ВТБ.',
   ARRAY['VTBR'], NULL, 'other', 2, NULL, NULL, NULL,
   NULL, NULL, NULL, NULL,
   now() - interval '3 days', now() - interval '3 days', NULL, NULL, 'rejected', 4),

  ('rejected', 'rss:vedomosti', 'Отклонено: пост про Новатэк не прошёл фактчек',
   'Экспорт сжиженного газа снизился на фоне санкционных ограничений.',
   ARRAY['NVTK'], 'NM', 'production', 2, NULL, NULL, NULL,
   'Источник не подтверждён вторым независимым каналом', NULL, NULL, NULL,
   now() - interval '4 days', now() - interval '4 days', NULL, NULL, 'rejected', 4);

-- parent_candidate_id донабивается отдельными UPDATE: субзапросы внутри одного
-- многострочного INSERT видят снимок ДО начала statement'а (строки той же
-- команды друг другу не видны), поэтому связать «предыдущего кандидата треда»
-- напрямую в VALUES нельзя — тред всё равно рендерится по thread_key
-- (parent_candidate_id — доп. связь для полноты фикстуры).
UPDATE content_candidates c SET parent_candidate_id = p.id
FROM content_candidates p
WHERE c.thread_key = p.thread_key AND c.thread_key IS NOT NULL AND c.id <> p.id
  AND p.status = 'candidate' AND c.status = 'draft_ready';

UPDATE content_candidates c SET parent_candidate_id = p.id
FROM content_candidates p
WHERE c.thread_key = p.thread_key AND c.thread_key IS NOT NULL AND c.id <> p.id
  AND p.status = 'draft_ready' AND c.status = 'in_review';

UPDATE content_candidates c SET parent_candidate_id = p.id
FROM content_candidates p
WHERE c.thread_key = p.thread_key AND c.thread_key IS NOT NULL AND c.id <> p.id
  AND p.status = 'in_review' AND c.status = 'published';
