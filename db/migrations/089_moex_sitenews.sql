-- Лента объявлений Московской биржи (ISS /iss/sitenews) → кандидаты в посты.
--
-- Зачем. 09.09.2026 биржа в 14:00 МСК объявила о включении ДОМ.РФ в Индекс
-- МосБиржи создания стоимости; мы узнали об этом из MarketTwits в 14:06 и то
-- случайно — новость не набрала репостов и кандидатом бы не стала. При этом
-- первоисточник открыт, бесплатен и машиночитаем, а мы его не читали вовсе.
-- Изменение базы расчёта индекса — это вынужденный поток (индексные фонды
-- обязаны докупить), то есть ровно наш жанр.
--
-- id — идентификатор новости на стороне ISS, он же ключ дедупа: лента отдаёт
-- последние 50 записей на каждый запрос, и без него мы бы перезаводили одни и
-- те же объявления каждые 15 минут.
CREATE TABLE IF NOT EXISTS moex_sitenews (
    id            BIGINT PRIMARY KEY,          -- id новости в ISS
    published_at  TIMESTAMPTZ NOT NULL,
    modified_at   TIMESTAMPTZ,
    tag           VARCHAR(32),
    title         TEXT NOT NULL,
    body          TEXT,                        -- тело подтягиваем только у интересных
    rubric        VARCHAR(32),                 -- наша разметка: индекс | листинг | прочее
    tickers       TEXT[] DEFAULT '{}',         -- распознанные бумаги из нашей вселенной
    candidate_id  INTEGER REFERENCES content_candidates(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_moex_sitenews_published ON moex_sitenews (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_moex_sitenews_rubric ON moex_sitenews (rubric) WHERE rubric IS NOT NULL;
