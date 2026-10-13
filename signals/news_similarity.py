"""
Похожесть новостных постов для дедупа хайп-бота (tg_hype_scan).

Найдено 13.10.2026: одна цитата Пескова («не слышали о плане урегулирования,
который обсуждают США и Европа») прошла порог репостов и в newssmartlab, и в
markettwits, в канал ушли обе. Тексты пересказаны разными словами, поэтому
посимвольное совпадение слабое; сравниваем мешки «основ» слов:
  • ссылки, хэштеги, «Читать далее» и markdown вырезаются;
  • основа = первые 5 букв (4 для слов до 5 букв): «Европа»/«Европой»,
    «плане»/«плана», «Украине»/«Украиной» совпадают без морфологии;
  • вес основы = IDF по свежим постам каналов, чтобы общие слова ленты
    («прибыль», «сентябрь», «увеличил») не склеивали разные новости,
    а редкие («урегулирование», «Песков») решали.
Мера = вес общих основ / вес основ более короткого поста (containment):
перепост с добавленным абзацем не прячется за чужим хвостом.

Отдельно сверяются имена (слова с заглавной: компании, люди, страны).
«Сбербанк в сентябре увеличил чистую прибыль по РСБУ» и «ВТБ в сентябре
увеличил чистую прибыль по МСФО» по словам совпадают на 67%, но имена
разные, это две новости.
"""
import math
import re
from collections import Counter

_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_URL_RE = re.compile(r"(?:https?://|t\.me/|www\.)\S+", re.IGNORECASE)
_HASHTAG_RE = re.compile(r"#\w+")
_READ_MORE_RE = re.compile(r"читать\s+далее", re.IGNORECASE)
_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+")

# Служебные слова и штампы лент, которые не несут сути новости.
_STOP = {
    "и", "в", "во", "на", "по", "с", "со", "к", "ко", "о", "об", "от", "до", "за", "из",
    "у", "не", "ни", "но", "а", "что", "как", "это", "его", "ее", "их", "мы", "вы", "он",
    "она", "они", "оно", "для", "при", "про", "под", "над", "без", "или", "же", "ли", "бы",
    "уже", "еще", "так", "там", "тут", "все", "всё", "был", "была", "были", "было", "будет",
    "который", "которая", "которые", "которое", "которого", "которой", "которых", "якобы",
    "также", "ничего", "этот", "эта", "эти", "того", "тем", "чем", "где", "когда", "после",
    "более", "менее", "около", "между", "через", "свой", "свои", "своих", "года", "году",
    "the", "and", "for", "with",
}


def _stem(word: str) -> str:
    if word.isdigit():
        return word
    return word[:4] if len(word) <= 5 else word[:5]


def _terms(text: str):
    """(основа, это имя) для каждого значимого слова поста."""
    t = _MD_LINK_RE.sub(r"\1", text or "")
    t = _URL_RE.sub(" ", t)
    t = _HASHTAG_RE.sub(" ", t)
    t = _READ_MORE_RE.sub(" ", t)
    for raw in _WORD_RE.findall(t):
        w = raw.lower().replace("ё", "е")
        if w in _STOP:
            continue
        if w.isdigit() and len(w) < 2:
            continue
        if not w.isdigit() and len(w) < 3:
            continue
        yield _stem(w), raw[0].isupper()


def stems(text: str) -> set:
    """Множество основ поста без ссылок, тегов и служебных слов."""
    return {s for s, _ in _terms(text)}


def names(text: str) -> set:
    """Основы слов с заглавной буквы (сюда попадает и первое слово фразы, это шум,
    но он только снижает совпадение имён, а не склеивает разные новости)."""
    return {s for s, is_name in _terms(text) if is_name}


def names_match(a: set, b: set, min_share: float) -> bool:
    """Имена не противоречат: у кого-то их нет или общих ≥ min_share от меньшего."""
    if not a or not b:
        return True
    return len(a & b) / min(len(a), len(b)) >= min_share


class Idf:
    """IDF основ по корпусу свежих постов. Незнакомая основа = максимальный вес."""

    def __init__(self, texts):
        df = Counter()
        n = 0
        for t in texts:
            n += 1
            df.update(stems(t))
        self._df = df
        self._n = n

    def __call__(self, stem: str) -> float:
        return math.log((self._n + 1) / (self._df.get(stem, 0) + 1)) + 1.0


def similarity(a: set, b: set, idf) -> float:
    """Взвешенная доля основ короткого поста, найденных в длинном (0..1)."""
    if not a or not b:
        return 0.0
    wa = sum(idf(s) for s in a)
    wb = sum(idf(s) for s in b)
    shared = sum(idf(s) for s in a & b)
    return shared / min(wa, wb)


def find_duplicate(text: str, others, idf, min_score: float, min_shared: int,
                   min_names_share: float):
    """Самый похожий из others (итерируемое (key, text)) при score ≥ min_score,
    не меньше min_shared общих основ и совпадающих именах. (key, score) или None."""
    a, a_names = stems(text), names(text)
    best = None
    for key, other_text in others:
        b = stems(other_text)
        if len(a & b) < min_shared:
            continue
        if not names_match(a_names, names(other_text), min_names_share):
            continue
        score = similarity(a, b, idf)
        if score >= min_score and (best is None or score > best[1]):
            best = (key, score)
    return best
