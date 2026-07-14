-- Категорийный матчинг для Шага Б content-пайплайна (найдено 2026-07-14, session 3):
-- сейчас content_match.py матчит СТРОГО futures_ticker = anomalies.asset_id — новость
-- без названной компании (санкции против «нефтяного сектора», «рынок акций растёт»,
-- «рубль укрепляется») никогда не матчится, хотя данные для соответствующей аномалии
-- у нас уже есть (OI_ASSETS их сканирует). Разбор 90-дневной выборки поста-ориентира
-- (Thor) показал: конкретный тикер назван лишь в ~17% контента, ~40% — рынок целиком,
-- ~27% — валюта, ~8% — сектор без названной компании.
--
-- asset_category_map — источник истины (живой SQL-запрос, НЕ хардкод-копия в Python —
-- тот самый урок с OI_ASSETS/ticker_futures_map: одна таблица, оба потребителя
-- (content_ai.py для грунтинга Шага А, content_match.py для fallback-матчинга)
-- читают её напрямую).
--
-- tier:
--   'exact'  — категория = 1-2 актива без двусмысленности (валютная пара, золото→Полюс,
--              IMOEX) — доверие как у точного тикера.
--   'sector' — категория покрывает 3+ актива (нефть и газ, финансы, сталь) — Шаг В
--              обязан использовать обобщённые формулировки, не приписывать причинность
--              конкретно найденному активу. Различие используется и в тексте поста,
--              и в бейдже карточки ревью («🔶 категория, не тикер»).
--
-- Покрытие: 115 из 118 живых sectype (открытый интерес за 14 дней на 2026-07-14).
-- Не покрыты: DX/HS/NA — коды без имени/группы в instruments, происхождение неясно.
--
-- Идемпотентно. Применение:
--   cat db/migrations/027_content_category.sql | docker exec -i frame-db-1 psql -U postgres -d moex_db

CREATE TABLE IF NOT EXISTS asset_category_map (
  sectype   VARCHAR(24) PRIMARY KEY,          -- open_interest.sectype / anomalies.asset_id
  category  VARCHAR(64) NOT NULL,
  tier      VARCHAR(10) NOT NULL CHECK (tier IN ('exact', 'sector')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_asset_category_map_category ON asset_category_map (category);

INSERT INTO asset_category_map (sectype, category, tier) VALUES
    ('RL', 'Металлы: алюминий', 'exact'),
    ('AN', 'Металлы: алюминий', 'exact'),
    ('PX', 'Металлы: золото', 'exact'),
    ('GD', 'Металлы: золото', 'exact'),
    ('GL', 'Металлы: золото', 'exact'),
    ('GLDRUBF', 'Металлы: золото', 'exact'),
    ('GK', 'Металлы: никель и палладий', 'exact'),
    ('NC', 'Металлы: никель и палладий', 'exact'),
    ('PD', 'Металлы: никель и палладий', 'exact'),
    ('PT', 'Металлы: никель и палладий', 'exact'),
    ('AL', 'Металлы: алмазы', 'exact'),
    ('RA', 'Металлы: уголь', 'exact'),
    ('SV', 'Металлы: серебро', 'exact'),
    ('CE', 'Металлы: медь', 'exact'),
    ('ZC', 'Металлы: цинк', 'exact'),
    ('CH', 'Металлы: сталь', 'sector'),
    ('MG', 'Металлы: сталь', 'sector'),
    ('NM', 'Металлы: сталь', 'sector'),
    ('BN', 'Нефть и газ', 'sector'),
    ('GAZPF', 'Нефть и газ', 'sector'),
    ('LK', 'Нефть и газ', 'sector'),
    ('NV', 'Нефть и газ', 'sector'),
    ('RN', 'Нефть и газ', 'sector'),
    ('SG', 'Нефть и газ', 'sector'),
    ('SN', 'Нефть и газ', 'sector'),
    ('SO', 'Нефть и газ', 'sector'),
    ('TP', 'Нефть и газ', 'sector'),
    ('TT', 'Нефть и газ', 'sector'),
    ('BR', 'Нефть и газ', 'sector'),
    ('BM', 'Нефть и газ', 'sector'),
    ('NG', 'Нефть и газ', 'sector'),
    ('NR', 'Нефть и газ', 'sector'),
    ('FF', 'Нефть и газ', 'sector'),
    ('OG', 'Нефть и газ', 'sector'),
    ('BS', 'Финансы', 'sector'),
    ('CM', 'Финансы', 'sector'),
    ('LE', 'Финансы', 'sector'),
    ('ME', 'Финансы', 'sector'),
    ('RD', 'Финансы', 'sector'),
    ('SBERF', 'Финансы', 'sector'),
    ('SC', 'Финансы', 'sector'),
    ('SH', 'Финансы', 'sector'),
    ('SP', 'Финансы', 'sector'),
    ('TB', 'Финансы', 'sector'),
    ('VB', 'Финансы', 'sector'),
    ('FN', 'Финансы', 'sector'),
    ('AS', 'IT', 'sector'),
    ('HD', 'IT', 'sector'),
    ('PS', 'IT', 'sector'),
    ('VK', 'IT', 'sector'),
    ('YD', 'IT', 'sector'),
    ('AK', 'Потребительский сектор', 'sector'),
    ('MN', 'Потребительский сектор', 'sector'),
    ('NB', 'Потребительский сектор', 'sector'),
    ('X5', 'Потребительский сектор', 'sector'),
    ('CS', 'Потребительский сектор', 'sector'),
    ('AF', 'Транспорт', 'sector'),
    ('FE', 'Транспорт', 'sector'),
    ('FL', 'Транспорт', 'sector'),
    ('TN', 'Транспорт', 'sector'),
    ('MT', 'Телеком', 'sector'),
    ('RT', 'Телеком', 'sector'),
    ('PI', 'Застройщики', 'sector'),
    ('SS', 'Застройщики', 'sector'),
    ('PH', 'Химия', 'exact'),
    ('KM', 'Машиностроение', 'exact'),
    ('FS', 'Энергетика (электро)', 'sector'),
    ('HY', 'Энергетика (электро)', 'sector'),
    ('IR', 'Энергетика (электро)', 'sector'),
    ('UN', 'Энергетика (электро)', 'sector'),
    ('Si', 'Валюта: USD/RUB', 'exact'),
    ('USDRUBF', 'Валюта: USD/RUB', 'exact'),
    ('Eu', 'Валюта: EUR/RUB', 'exact'),
    ('EURRUBF', 'Валюта: EUR/RUB', 'exact'),
    ('CR', 'Валюта: CNY/RUB', 'exact'),
    ('CNYRUBF', 'Валюта: CNY/RUB', 'exact'),
    ('AE', 'Валюта: AED/RUB', 'exact'),
    ('HK', 'Валюта: HKD/RUB', 'exact'),
    ('KZ', 'Валюта: KZT/RUB', 'exact'),
    ('AU', 'Валюта: AUD/USD', 'exact'),
    ('CA', 'Валюта: USD/CAD', 'exact'),
    ('CF', 'Валюта: USD/CHF', 'exact'),
    ('ED', 'Валюта: EUR/USD', 'exact'),
    ('GU', 'Валюта: GBP/USD', 'exact'),
    ('JP', 'Валюта: USD/JPY', 'exact'),
    ('UC', 'Валюта: USD/CNY', 'exact'),
    ('IMOEXF', 'Рынок целиком (IMOEX)', 'exact'),
    ('MX', 'Рынок целиком (IMOEX)', 'exact'),
    ('MM', 'Рынок целиком (IMOEX)', 'exact'),
    ('RI', 'Рынок целиком (РТС)', 'exact'),
    ('RM', 'Рынок целиком (РТС)', 'exact'),
    ('RB', 'Облигации (RGBI)', 'exact'),
    ('VI', 'Волатильность (RVI)', 'exact'),
    ('SF', 'Рынок США (S&P 500)', 'exact'),
    ('HO', 'Недвижимость (индекс)', 'exact'),
    ('IP', 'IPO (индекс)', 'exact'),
    ('MY', 'Рынок целиком (IMOEX в юанях)', 'exact'),
    ('BT', 'Крипто', 'exact'),
    ('IB', 'Крипто', 'exact'),
    ('CC', 'Сырьё: какао', 'exact'),
    ('KC', 'Сырьё: кофе', 'exact'),
    ('OJ', 'Сырьё: апельсиновый сок', 'exact'),
    ('SA', 'Сырьё: сахар', 'exact'),
    ('W4', 'Сырьё: пшеница', 'exact'),
    ('CK', 'Металлы: сталь', 'sector'),
    ('GZ', 'Нефть и газ', 'sector'),
    ('SR', 'Финансы', 'sector'),
    ('MC', 'Металлы: уголь', 'exact'),
    ('RU', 'Нефть и газ', 'sector'),
    ('S0', 'IT', 'sector'),
    ('SE', 'Финансы', 'sector'),
    ('SZ', 'Лесная промышленность', 'exact'),
    ('WU', 'Транспорт', 'sector'),
    ('MV', 'Потребительский сектор', 'sector'),
    ('IS', 'Здравоохранение', 'exact')
ON CONFLICT (sectype) DO NOTHING;

-- content_candidates: категория (когда Шаг А не смог назвать конкретный тикер, но
-- определил тему) + тип матча (для бейджа на карточке ревью и для Шага В — hedged
-- формулировки при 'category').
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS category VARCHAR(64);
ALTER TABLE content_candidates ADD COLUMN IF NOT EXISTS match_type VARCHAR(10)
  CHECK (match_type IN ('ticker', 'category'));
