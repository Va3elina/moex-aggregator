-- Вектора второго мозга: эмбеддинг на узел, отдельной таблицей, чтобы пересчёт
-- модели не трогал brain_nodes. Модель — minishlab/potion-multilingual-128M
-- (model2vec, 256 измерений); индекс HNSW по косинусу.
-- ⚠️ Требует образа frame-db:pg17-vector (Dockerfile.db): в postgres:17-alpine
-- расширения vector нет.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS brain_embeddings (
    node_id    TEXT PRIMARY KEY REFERENCES brain_nodes (id) ON DELETE CASCADE,
    model      TEXT NOT NULL,
    embedding  vector(256) NOT NULL,
    text_hash  TEXT NOT NULL,                 -- md5 текста, по которому считали: изменился — пересчитать
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_brain_embeddings_hnsw ON brain_embeddings USING hnsw (embedding vector_cosine_ops);
