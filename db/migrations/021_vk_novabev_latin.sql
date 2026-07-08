-- 021_vk_novabev_latin.sql — VK и Novabev латиницей во всех разделах
-- (решение владельца 2026-07-08, поверх 020_unify_asset_names.sql).
-- Компании ребрендировались на латинские написания; каноничное имя — латиница.
-- Frontend-зеркало: frontend/src/config/fundConfig.ts (BELU, VKCO).
-- Идемпотентно.

BEGIN;

-- ОИ (фьючерсы)
UPDATE instruments SET name = 'VK'      WHERE type = 'futures' AND sectype = 'VK';
UPDATE instruments SET name = 'Novabev' WHERE type = 'futures' AND sectype = 'NB';

-- Сезонность (акции)
UPDATE instruments SET name = 'VK'      WHERE sec_id = 'VKCO';
UPDATE instruments SET name = 'Novabev' WHERE sec_id = 'BELU';

COMMIT;
