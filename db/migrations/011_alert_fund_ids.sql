-- Гибкий таргет fund-алерта по произвольному набору фондов.
--
-- alerts.fund_ids — CSV из fund_id (через запятую), nullable. Семантика
-- (в паре с alerts.asset, разбирается в signals/detectors/funds.py и
-- signals/alerts_run.py compute_value):
--   • asset='all'        + fund_ids NULL → ВСЕ фонды всех категорий (динамически)
--   • asset=<категория>  + fund_ids NULL → вся категория (динамически;
--       backward-compat со старыми fund-алертами money_market|stocks|bonds|gold)
--   • fund_ids=<список>  → именно эти фонды (asset='custom', заморожённый набор)
-- asset_name — человеко-метка выбора («Все фонды» / «Акции, Облигации» /
-- «N фондов» / по УК) — для текста сигнала и кабинета.
--
-- NULL = «по asset» (динамический набор). Колонка nullable, без дефолта —
-- добавление безопасно и обратносовместимо: старые fund-алерты (asset=категория,
-- fund_ids NULL) продолжают работать как «вся категория».
--
-- Идемпотентно (IF NOT EXISTS).
-- Применение: cat db/migrations/011_alert_fund_ids.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

ALTER TABLE alerts ADD COLUMN IF NOT EXISTS fund_ids TEXT;
