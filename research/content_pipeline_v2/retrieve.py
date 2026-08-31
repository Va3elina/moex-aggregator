#!/usr/bin/env python3
"""Подбор похожих постов из корпуса — «чувство» вместо правил стиля.

Заменяет 4 статичных образца в промпте Шага В на 2-3 поста, подобранных под
КОНКРЕТНУЮ ситуацию. Основание: 13 стилевых правил проиграли четырём картинкам —
модель скопировала образцы (и то криво: все образцы на 2-3 абзаца, а черновики
схлопнулись в 4). Голос показывают, а не описывают.

⚠️ ПОЧЕМУ НЕТ ВЕКТОРНОЙ БАЗЫ КАК СЕРВИСА. Корпус — 1459 постов. Матрица
1459×256 float32 это 1,5 МБ; косинус ко всему корпусу считается точно и за
микросекунды обычным numpy-умножением. Chroma/FAISS/Qdrant здесь дали бы
приближённый поиск, отдельный процесс и лишнюю зависимость — ради ускорения
того, что и так мгновенно. Вектора (суть просьбы) есть; сервер (церемония) —
нет. Порог, за которым это меняется, — примерно сотни тысяч документов.

Эмбеддинги: model2vec (статические, инференс на ЧИСТОМ numpy — без torch и GPU).
Весь корпус кодируется за ~0,3с, модель качается один раз (~110 МБ).

Гибрид трёх сигналов, объединяются через RRF (reciprocal rank fusion — не требует
подгонки весов, устойчив к разным шкалам оценок):
  • плотный   — семантическая близость (о чём пост);
  • лексический BM25 — точные совпадения: тикеры, имена, термины;
  • фильтры по метаданным — жанр/рубрика, наличие тикера.

Запуск:
  .venv/bin/python retrieve.py --build            # посчитать эмбеддинги корпуса
  .venv/bin/python retrieve.py --eval             # leave-one-out: что лучше
  .venv/bin/python retrieve.py --query "текст"    # посмотреть выдачу
"""
import argparse
import gzip
import json
import math
import os
import re
import sys
from collections import Counter

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.path.join(HERE, "dataset", "corpus.json.gz")
VECTORS = os.path.join(HERE, "dataset", "corpus_vectors.npz")
MODEL_NAME = "minishlab/potion-multilingual-128M"
GENRE_TAGS = ("#открытыйинтерес", "#открытыепозиции")
# Тикер в хэштеге: латиница 3-6 букв. #SBER/#GAZP/#VKCO — да, #дкп/#россия — нет.
TICKER_RE = re.compile(r"^#([A-Z]{3,6})$")


def tickers_of(post) -> set:
    """Тикеры поста — из хэштегов. У Thor разметка по тикерам плотная (#IMOEX 213,
    #SBER 173), у FRAME — реже, там теги по рубрике индикатора."""
    return {m.group(1) for t in post["hashtags"] if (m := TICKER_RE.match(t.upper()))}


def load_corpus():
    with gzip.open(CORPUS, "rt", encoding="utf-8") as f:
        return json.load(f)


# ── лексическая часть: BM25 на чистом numpy ────────────────────────────────
# Своя реализация вместо rank_bm25: 25 строк, ноль зависимостей, и нам нужен
# контроль над токенизацией (русская морфология — грубый стеммер по 5 буквам,
# чтобы «шорта/шорты/шортов» попадали в один токен).
def tokenize(text: str) -> list:
    words = re.findall(r"[а-яёa-z0-9]+", text.lower())
    return [w[:5] if len(w) > 5 and re.match(r"^[а-яё]+$", w) else w for w in words]


class BM25:
    def __init__(self, docs, k1=1.5, b=0.75):
        self.k1, self.b = k1, b
        self.docs = [tokenize(d) for d in docs]
        self.lens = np.array([len(d) for d in self.docs], dtype=np.float32)
        self.avg = float(self.lens.mean()) or 1.0
        self.df = Counter()
        for d in self.docs:
            self.df.update(set(d))
        self.n = len(self.docs)
        self.tf = [Counter(d) for d in self.docs]

    def scores(self, query: str) -> np.ndarray:
        q = tokenize(query)
        out = np.zeros(self.n, dtype=np.float32)
        for term in set(q):
            df = self.df.get(term, 0)
            if not df:
                continue
            idf = math.log(1 + (self.n - df + 0.5) / (df + 0.5))
            for i, tf in enumerate(self.tf):
                f = tf.get(term, 0)
                if f:
                    out[i] += idf * f * (self.k1 + 1) / (
                        f + self.k1 * (1 - self.b + self.b * self.lens[i] / self.avg))
        return out


def rrf(rank_lists, k=60) -> dict:
    """Reciprocal rank fusion: складываем 1/(k+ранг) по каждому источнику.
    Выбран вместо взвешенной суммы, потому что не требует подгонки весов под
    разные шкалы (косинус 0..1 против BM25 0..30) — а подгонять их нам не на чем."""
    out = {}
    for ranks in rank_lists:
        for pos, idx in enumerate(ranks):
            out[idx] = out.get(idx, 0.0) + 1.0 / (k + pos + 1)
    return out


def build_vectors():
    from model2vec import StaticModel
    corpus = load_corpus()
    model = StaticModel.from_pretrained(MODEL_NAME)
    vecs = model.encode([p["text"] for p in corpus]).astype(np.float32)
    vecs /= (np.linalg.norm(vecs, axis=1, keepdims=True) + 1e-9)
    np.savez_compressed(VECTORS, vectors=vecs)
    print(f"эмбеддинги: {vecs.shape} → {VECTORS} "
          f"({os.path.getsize(VECTORS)/1e6:.2f} МБ)")


def load_vectors():
    if not os.path.exists(VECTORS):
        sys.exit("нет эмбеддингов — сначала: retrieve.py --build")
    return np.load(VECTORS)["vectors"]


# Пулы для поиска. ⚠️ Жанровый срез (38 постов, две рубрики) слишком мал и
# однороден, чтобы на нём можно было ЗАМЕРИТЬ качество подбора: «угадать
# рубрику» там случайно выходит в 54% случаев. Для замера нужен пул, где есть
# что различать — все рубрики FRAME или корпус Thor с разметкой по тикерам.
POOLS = {
    "genre": lambda p: p["channel"].upper().startswith("FRAME")
                        and any(t in p["hashtags"] for t in GENRE_TAGS),
    "frame": lambda p: p["channel"].upper().startswith("FRAME"),
    "thor":  lambda p: p["channel"].startswith("Thor"),
    "all":   lambda p: True,
}


class Retriever:
    """Решение Вадима 31.08: **FRAME как основа, Thor только для редких тикеров.**
    Причина: у Thor голос личный («я», ценовые цели, оффтопы), а нам нужен голос
    Frame (команда, «мы»). Его 1377 постов берём не ради стиля, а ради покрытия —
    когда по нужному тикеру у FRAME нет вообще ничего.
    """

    def __init__(self, pool="genre", fallback_thor=True):
        corpus = load_corpus()
        vecs = load_vectors()
        keep = [i for i, p in enumerate(corpus) if POOLS[pool](p)]
        self.posts = [corpus[i] for i in keep]
        self.vecs = vecs[keep]
        self.bm25 = BM25([p["text"] for p in self.posts])
        self._model = None
        self.frame_tickers = {t for p in self.posts for t in tickers_of(p)}
        self._thor = None
        if fallback_thor:
            thor_idx = [i for i, p in enumerate(corpus) if POOLS["thor"](p)]
            self._thor = [corpus[i] for i in thor_idx]
            self._thor_vecs = vecs[thor_idx]

    def rare_ticker_backup(self, tickers, k=1) -> list:
        """Посты Thor по тикерам, которых у FRAME нет вообще. Не «дополнить
        выдачу», а именно закрыть дыру покрытия: если Thor начнёт подмешиваться
        к тикерам, которые FRAME освещает, мы затащим в примеры чужой голос."""
        if not self._thor or not tickers:
            return []
        missing = {t.upper() for t in tickers} - self.frame_tickers
        if not missing:
            return []
        hits = [p for p in self._thor if tickers_of(p) & missing]
        hits.sort(key=lambda p: p["date"], reverse=True)
        return hits[:k]

    def embed(self, text: str) -> np.ndarray:
        if self._model is None:
            from model2vec import StaticModel
            self._model = StaticModel.from_pretrained(MODEL_NAME)
        v = self._model.encode([text]).astype(np.float32)[0]
        return v / (np.linalg.norm(v) + 1e-9)

    def search_with_backup(self, query: str, tickers=None, k=3, mode="hybrid") -> tuple:
        """Основная выдача — FRAME. Если тикер запроса FRAME не покрывает вообще,
        ОДИН пост Thor добавляется как запасной и помечается: он для покрытия,
        а не для подражания голосу."""
        hits, _ = self.search(query, k=k, mode=mode)
        backup = self.rare_ticker_backup(tickers or [], k=1)
        return hits, backup

    def search(self, query: str, k=3, mode="hybrid", exclude=None) -> list:
        exclude = exclude if exclude is not None else set()
        n = len(self.posts)
        lists = []
        if mode in ("hybrid", "dense"):
            sims = self.vecs @ self.embed(query)
            lists.append([i for i in np.argsort(-sims) if i not in exclude])
        if mode in ("hybrid", "lexical"):
            bs = self.bm25.scores(query)
            lists.append([i for i in np.argsort(-bs) if i not in exclude])
        fused = rrf(lists)
        order = sorted(fused, key=lambda i: -fused[i])
        return [self.posts[i] for i in order[:k]], [i for i in order[:k]]


def _tags(p):
    """Только содержательные теги: рубрика или тикер. Служебные вроде #россия
    ничего не говорят о типе поста."""
    return {t.lower() for t in p["hashtags"]
            if t.lower() not in ("#россия", "#рф", "#новости")}


def evaluate(pool="frame", sample=None):
    """Leave-one-out: прячем пост, ищем соседей среди остальных. Метрики — ровно
    то, ради чего подбор и нужен:
      • общий тег — попали ли в ту же рубрику/тот же тикер;
      • |Δ абзацев| — структурная близость примера, который увидит модель. Чем
        ниже, тем меньше шансов, что модель снова схлопнется в одно число.
    База сравнения — случайные посты: это и есть то, чем по сути были 4 статичных
    образца в промпте."""
    r = Retriever(pool=pool)
    n = len(r.posts)
    idxs = list(range(n))
    if sample and n > sample:
        idxs = list(np.random.default_rng(0).choice(n, size=sample, replace=False))
    ntags = len({t for p in r.posts for t in _tags(p)})
    print(f"пул «{pool}»: {n} постов, {ntags} различных содержательных тегов; "
          f"замер на {len(idxs)} запросах\n")
    print(f"{'режим':<10} {'общий тег':>11} {'|Δ абзацев|':>13} {'|Δ длины|':>11}")
    rows = {}
    for mode in ("lexical", "dense", "hybrid"):
        same, dpara, dlen, cnt = 0, [], [], 0
        for i in idxs:
            p = r.posts[int(i)]
            hits, _ = r.search(p["text"], k=3, mode=mode, exclude={int(i)})
            for h in hits:
                cnt += 1
                same += len(_tags(p) & _tags(h)) > 0
                dpara.append(abs(p["n_paras"] - h["n_paras"]))
                dlen.append(abs(p["length"] - h["length"]))
        rows[mode] = same / cnt
        print(f"{mode:<10} {same/cnt:>10.0%} {np.mean(dpara):>13.2f} {np.mean(dlen):>11.0f}")
    rng = np.random.default_rng(0)
    same, dpara, dlen, cnt = 0, [], [], 0
    for i in idxs:
        p = r.posts[int(i)]
        for j in rng.choice([x for x in range(n) if x != int(i)], size=3, replace=False):
            h = r.posts[int(j)]
            cnt += 1
            same += len(_tags(p) & _tags(h)) > 0
            dpara.append(abs(p["n_paras"] - h["n_paras"]))
            dlen.append(abs(p["length"] - h["length"]))
    base = same / cnt
    print(f"{'случайно':<10} {base:>10.0%} {np.mean(dpara):>13.2f} {np.mean(dlen):>11.0f}"
          f"   ← чем по сути были 4 статичных образца")
    best = max(rows, key=rows.get)
    print(f"\nлучший режим: {best} — {rows[best]:.0%} против {base:.0%} случайного "
          f"(×{rows[best]/base:.1f})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--eval", action="store_true")
    ap.add_argument("--pool", default="frame", choices=list(POOLS))
    ap.add_argument("--sample", type=int, default=None)
    ap.add_argument("--query")
    ap.add_argument("--mode", default="hybrid", choices=["hybrid", "dense", "lexical"])
    ap.add_argument("-k", type=int, default=3)
    a = ap.parse_args()
    if a.build:
        build_vectors()
        return
    if a.eval:
        evaluate(pool=a.pool, sample=a.sample)
        return
    if a.query:
        r = Retriever(pool=a.pool)
        hits, _ = r.search(a.query, k=a.k, mode=a.mode)
        for h in hits:
            print(f"\n─── [{h['date']}] абзацев={h['n_paras']} длина={h['length']} "
                  f"{' '.join(h['hashtags'][:3])}")
            print(h["text"][:400])
        return
    ap.print_help()


if __name__ == "__main__":
    main()
