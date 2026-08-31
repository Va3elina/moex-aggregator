"""Подбор примеров постов канала для Шага В — вместо статичных образцов в промпте.

Зачем: 13 стилевых правил в промпте проиграли четырём картинкам-образцам, причём
модель скопировала образцы криво (все четыре на 2-3 абзаца, а черновики схлопнулись
в 4 у 16 из 23). Голос показывают, а не описывают.

⚠️ ПОЧЕМУ БЕЗ ВЕКТОРНОЙ МОДЕЛИ, хотя вектора посчитаны и лежат в research/.
Три причины, и главная — не про размер:

1. На нашем размере пула лексический поиск НЕ ХУЖЕ плотного. Замер leave-one-out
   (research/content_pipeline_v2/retrieve.py --eval): пул FRAME 82 поста — общий
   тег у 49% и у гибрида, и у лексического (случайный выбор 14%); пул Thor 1377 —
   лексический 40% против 38% у гибрида. Плотный выигрывал ТОЛЬКО по структурной
   близости примера (|Δ абзацев| 1,30 против 1,61).
2. А структуру честнее задать НАПРЯМУЮ, чем надеяться, что её поймает эмбеддинг.
   Причём нам нужна не близость, а РАЗНООБРАЗИЕ: жалоба Вадима была «модель
   повторяет количество абзацев, а не выбирает сколько их». Три структурно похожих
   примера учат шаблону; три примера с РАЗНЫМ числом абзацев учат тому, что число
   бывает разным. Плотный поиск оптимизирует ровно не то, что нужно.
3. Модель `potion-multilingual-128M` — 489 МБ (мультиязычный словарь × 256
   измерений). Образ пересобирается на КАЖДОМ деплое и собирается на самом сервере
   (scripts/prod_deploy.sh через SSH), где 39 ГБ свободно. Платить этим за
   ухудшающий нашу задачу сигнал — плохая сделка.
   (Проверено 31.08: сам LFS-файл с прода доступен, дело не в сети.)

Если однажды окажется, что структурного разнообразия недостаточно — вектора уже
посчитаны, и путь открыт: см. retrieve.py.
"""
import gzip
import json
import math
import os
import re
import statistics
from collections import Counter
from functools import lru_cache

GENRE_TAGS = ("#открытыйинтерес", "#открытыепозиции")
TICKER_RE = re.compile(r"^#([A-Z]{3,6})$")

# Порядок кандидатов: путь из env → путь внутри образа → путь в репозитории (для
# локального запуска без Docker). Без последнего сервис не поднимется на маке.
_CANDIDATE_PATHS = [
    os.environ.get("CORPUS_PATH", ""),
    "/app/data/corpus/corpus.json.gz",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), "research", "content_pipeline_v2", "dataset",
        "corpus.json.gz"),
]


def _corpus_path() -> str | None:
    for p in _CANDIDATE_PATHS:
        if p and os.path.exists(p):
            return p
    return None


def _tokenize(text: str) -> list:
    """Грубый стеммер по 5 буквам: «шорта/шорты/шортов» должны попадать в один
    токен, иначе лексический поиск промахивается на русской морфологии.

    ⚠️ Чистые числа выбрасываются. Они редки, получают огромный IDF и утаскивают
    выдачу на посты, совпавшие случайной цифрой. Живой промах: по заголовку
    «АКЦИИ ГАЗПРОМА ПРОБИЛИ 17-ЛЕТНЕЕ ДНО» первым выходил пост про биткоин —
    из-за токена «17». Тему числа не несут, а тикеры у нас идут отдельным полем."""
    words = re.findall(r"[а-яёa-z0-9]+", text.lower())
    out = []
    for w in words:
        if w.isdigit():
            continue
        out.append(w[:5] if len(w) > 5 and re.match(r"^[а-яё]+$", w) else w)
    return out


class _BM25:
    """Своя реализация вместо зависимости: 25 строк, и нужен контроль над
    токенизацией (см. _tokenize)."""

    def __init__(self, docs, k1=1.5, b=0.75):
        self.k1, self.b = k1, b
        toks = [_tokenize(d) for d in docs]
        self.tf = [Counter(t) for t in toks]
        self.lens = [len(t) for t in toks]
        self.avg = (sum(self.lens) / len(self.lens)) if self.lens else 1.0
        self.df = Counter()
        for t in toks:
            self.df.update(set(t))
        self.n = len(docs)

    def scores(self, query: str) -> list:
        q = set(_tokenize(query))
        out = [0.0] * self.n
        for term in q:
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


def _tickers_of(post) -> set:
    return {m.group(1) for t in post.get("hashtags", [])
            if (m := TICKER_RE.match(t.upper()))}


@lru_cache(maxsize=1)
def _load():
    """Корпус читается один раз на процесс. 1459 постов ≈ 3,6 МБ распакованных —
    держать в памяти дешевле, чем открывать gzip на каждый запрос."""
    path = _corpus_path()
    if not path:
        return None
    with gzip.open(path, "rt", encoding="utf-8") as f:
        corpus = json.load(f)
    frame_genre = [p for p in corpus
                   if p.get("channel", "").upper().startswith("FRAME")
                   and any(t in p.get("hashtags", []) for t in GENRE_TAGS)]
    thor = [p for p in corpus if p.get("channel", "").startswith("Thor")]
    return {
        "path": path,
        "frame": frame_genre,
        "thor": thor,
        "bm25": _BM25([p["text"] for p in frame_genre]) if frame_genre else None,
        "frame_tickers": {t for p in frame_genre for t in _tickers_of(p)},
        "stats": _stats(frame_genre),
    }


def _stats(posts) -> dict:
    """Распределение структуры канала отдаётся В ОТВЕТЕ вместе с примерами.
    Примеры показывают КАК, распределение — В КАКИХ ПРЕДЕЛАХ. Без второго модель
    видит три конкретных поста и решает, что так надо всегда."""
    if not posts:
        return {}
    paras = [p.get("n_paras", 0) for p in posts]
    lens = sorted(p.get("length", 0) for p in posts)
    n = len(lens)
    return {
        "постов_в_эталоне": n,
        "абзацев_типично": statistics.mode(paras),
        "абзацев_диапазон": [min(paras), max(paras)],
        "абзацев_распределение": dict(sorted(Counter(paras).items())),
        "длина_медиана": int(statistics.median(lens)),
        "длина_коридор": [lens[max(0, int(0.1 * n))], lens[min(n - 1, int(0.9 * n))]],
    }


def _pick_diverse(ranked, k: int, pool_factor: int = 4) -> list:
    """k примеров с РАЗНЫМ числом абзацев — по одному лучшему из каждой структурной
    группы. Это лечит жалобу «повторяет количество абзацев, а не выбирает»: если все
    примеры одной формы, модель копирует форму.

    ⚠️ Разнообразие ищется ТОЛЬКО среди релевантной верхушки (k × pool_factor), а не
    по всему корпусу. Иначе требование «разные формы» перебивает релевантность:
    поймано на живом запросе про Газпром — третьим примером подтянулся пост про
    биткоин просто потому, что он был единственным с нужным числом абзацев.
    Голос он показывает не хуже, но тему уводит.

    Если внутри верхушки разных форм меньше k — доливаем следующими по релевантности
    и НЕ лезем глубже: релевантность важнее полной коллекции форм, а распределение
    структуры всё равно уходит агенту отдельным полем (см. _stats)."""
    pool = ranked[:max(k, k * pool_factor)]
    picked, seen_shapes = [], set()
    for item in pool:
        shape = item[1].get("n_paras")
        if shape not in seen_shapes:
            picked.append(item)
            seen_shapes.add(shape)
        if len(picked) == k:
            return picked
    for item in pool:
        if item not in picked:
            picked.append(item)
        if len(picked) == k:
            break
    return picked


def find_examples(query: str, tickers=None, k: int = 3) -> dict:
    data = _load()
    if not data or not data["bm25"]:
        return {"examples": [], "stats": {}, "note": "корпус недоступен на этом хосте"}

    k = max(1, min(k, 5))
    scores = data["bm25"].scores(query or "")

    # ⚠️ Надбавка за ТУ ЖЕ БУМАГУ. Без неё чисто лексический поиск промахивается на
    # коротких запросах: по заголовку «АКЦИИ ГАЗПРОМА ПРОБИЛИ 17-ЛЕТНЕЕ ДНО» первым
    # выходил пост про биткоин, потому что делил с запросом редкие «проби» и «дно»
    # (idf 3.26 и 2.41), а тематическое «газпр» их не перевешивало. BM25 отработал
    # честно — просто «та же бумага» для нас сигнал сильнее любого слова, и его надо
    # задать явно, а не надеяться, что он проступит через лексику.
    # Надбавка в долях от максимума — иначе она зависела бы от длины запроса.
    want = {t.upper() for t in (tickers or [])}
    if want:
        peak = max(scores) if scores else 0.0
        bonus = peak * 0.75 if peak > 0 else 1.0
        scores = [sc + (bonus if _tickers_of(p) & want else 0.0)
                  for sc, p in zip(scores, data["frame"])]

    ranked = sorted(zip(scores, data["frame"]), key=lambda x: -x[0])
    picked = _pick_diverse(ranked, k)

    examples = [{
        "дата": p.get("date"),
        "абзацев": p.get("n_paras"),
        "знаков": p.get("length"),
        "рубрика": [t for t in p.get("hashtags", []) if t in GENRE_TAGS],
        "текст": p.get("text"),
    } for _s, p in picked]

    note = ("Примеры подобраны под ваш запрос и НАМЕРЕННО имеют РАЗНОЕ число "
            "абзацев — это показывает, что число абзацев определяется числом "
            "мыслей, а не шаблоном. Не копируйте структуру примеров, копируйте "
            "интонацию и плотность.")

    # Запасной пример из Thor — ТОЛЬКО если тикера нет у FRAME целиком. Иначе
    # личный голос Thor («я», ценовые цели) затечёт в примеры по бумагам, которые
    # FRAME и так освещает.
    backup = None
    want = {t.upper() for t in (tickers or [])}
    missing = want - data["frame_tickers"]
    if missing and data["thor"]:
        hits = [p for p in data["thor"] if _tickers_of(p) & missing]
        if hits:
            b = max(hits, key=lambda p: p.get("date", ""))
            backup = {
                "дата": b.get("date"), "абзацев": b.get("n_paras"),
                "знаков": b.get("length"), "текст": b.get("text"),
                "предупреждение": ("Это пост ЛИЧНОГО канала автора, не канала Frame. "
                                    "Дан только потому, что по тикерам "
                                    f"{sorted(missing)} у канала Frame постов нет. "
                                    "Брать оттуда факты и голос («я», ценовые цели) "
                                    "НЕЛЬЗЯ — только как ориентир по теме."),
            }

    return {"examples": examples, "stats": data["stats"],
            "backup_from_personal_channel": backup, "note": note}
