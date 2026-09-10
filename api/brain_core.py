"""
Ядро второго мозга — без FastAPI, чтобы его мог импортировать и content_ai на хосте
(signals/.venv без fastapi и redis). Роутер api/routers/brain.py — тонкая обёртка над этим модулем.

Второй мозг для агентов и панели: /api/internal/brain/*

⚠️ ОДИН ВОПРОС — ОДИН ВЫЗОВ. «Что мы знаем о Сбере» раньше было пятью запросами
в пять таблиц; здесь это node/company:SBER: страница узла и его кольца по типам
связей со счётчиками. Ответы маленькие (id, заголовок, время), а не выгрузка
данных: полное — по ссылке в исходную таблицу.

⚠️ ВХОД ВСЕГДА ЧЕРЕЗ УЗЕЛ. Соседи читаются двумя индексными запросами (src=id и
dst=id) — OR по двум колонкам заставил бы планировщик идти по всей таблице.
Кратчайшая связь между двумя узлами ищется обходом в ширину в Python с жёстким
бюджетом: у компании тысячи новостных соседей, а рекурсивный CTE не умеет
ограничивать веер на уровень.

⚠️ pg8000 НЕ ВЫВОДИТ ТИП ПАРАМЕТРА в «:x IS NULL» — каждый необязательный параметр
обёрнут в CAST(:x AS …), иначе «could not determine data type of parameter».

⚠️ ДОСТУП: внутренний токен (Routine-агенты) ИЛИ сессия админа (панель). Каждый
вызов с candidate_id оставляет строку в agent_trace — след агента должен
показывать, что он спрашивал у мозга, а не только у таблиц.
"""

import os
import re
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from api.agent_trace import ВЗЯТО, ПУСТО, трассировать

try:
    from api.cache import get_or_set          # в api есть Redis
except Exception:  # noqa: BLE001 — на хосте (signals/.venv) Redis-клиента нет: живём без кэша
    def get_or_set(key, value=None, ttl=None):  # noqa: ARG001
        return None


class Ошибка(Exception):
    """Ошибка ядра с HTTP-кодом: роутер превращает её в ответ с этим кодом, скрипты — в текст."""
    def __init__(self, код: int, текст: str):
        super().__init__(текст)
        self.код = код
        self.текст = текст


def Query(default=None, **kw):  # noqa: N802 — имя как у FastAPI: сигнатуры ядра совпадают с роутером
    return None if default is ... else default

_ID = re.compile(r"^[a-z]+:[A-Za-z0-9_./\-]{1,120}$")
_ВИДЫ_УЗЛОВ = ("company", "news", "candidate", "post", "doc", "fund", "index", "fact", "anomaly", "signal", "holder")
# Порядок расширения при поиске пути: сначала структурные связи, новости — последними.
_ПРИОРИТЕТ = {"владеет": 0, "владеет_долей": 1, "держит": 2, "включает": 3, "факт_о": 4, "о": 5,
              "из_новости": 6, "сигнал_о": 7, "аномалия_по": 8, "отчитался": 9, "упоминает": 10,
              "в_секторе": 3.5, "вместе_в_новостях": 9.5}
_ЛИМИТ_СОСЕДЕЙ_В_ПУТИ = 300
_БЮДЖЕТ_ПУТИ = 20_000
_TTL = 60
_MODEL_DIR = os.environ.get("EMBED_MODEL_DIR", "/app/models/potion-multilingual-128M-int8")
_модель = None
_замок = threading.Lock()


def _загрузить_модель():
    """Один раз на процесс. Квантованная копия на диске (…-int8) грузится ~3,5 с;
    из fp32 с квантованием на лету — 10 с и 512 МБ пикового чтения."""
    global _модель
    with _замок:
        if _модель is None:
            from model2vec import StaticModel
            if _MODEL_DIR.endswith("-int8"):
                _модель = StaticModel.from_pretrained(_MODEL_DIR)
            else:
                _модель = StaticModel.from_pretrained(_MODEL_DIR, quantize_to="int8")
    return _модель


def _прогрев():
    try:
        _загрузить_модель()
    except Exception:  # noqa: BLE001 — модели нет: смысловой поиск ответит 503, остальное живёт
        pass


# ⚠️ ИНЦИДЕНТ 06.09.2026: МОДЕЛЬ ВЕСИТ НЕ 150, А ~810 МБ НА ПРОЦЕСС. Замер в живом
# воркере: 153 МБ после импорта app → 962 МБ после прогрева (словарь потиона —
# полмиллиона токенов, разжимается в float32). Три воркера × 1 ГБ = 3 ГБ на контейнер;
# rolling-деплой ставит второй контейнер рядом → 6 ГБ + база 3 ГБ > 8 ГБ → OOM-killer
# бьёт воркеры, те перезапускаются и снова грузят модель → шторм диска, load 78, сайт
# 503 на 25 минут, SSH не отвечает.
#
# Лечение: модель грузится ОДИН раз в мастере gunicorn до форка (`--preload` в
# Dockerfile + BRAIN_WARMUP=sync), воркеры получают её через copy-on-write и не
# трогают страницы (numpy-буферы неизменяемы, refcount живёт вне них). Один контейнер
# ≈ 1 ГБ + 3×150 МБ вместо 3 ГБ. Режимы:
#   sync   — синхронно при импорте (для preload; +3,5 с к старту мастера);
#   1      — фоновый поток в каждом процессе (старое поведение, БЕЗ preload);
#   0      — не греть, загрузка по первому смысловому запросу (прод с 06.09: смысловой
#            поиск нужен раз в месяц, 810 МБ на воркер того не стоят).
# ⚠️ Поток и preload несовместимы: fork копирует только форкающий поток, и воркер
# получил бы недогруженную модель — и снова грузил бы свою.
_РЕЖИМ_ПРОГРЕВА = os.environ.get("BRAIN_WARMUP", "1")
if os.path.isdir(_MODEL_DIR):
    if _РЕЖИМ_ПРОГРЕВА == "sync":
        _прогрев()
    elif _РЕЖИМ_ПРОГРЕВА == "1":
        threading.Thread(target=_прогрев, name="brain-warmup", daemon=True).start()


def _вектор(текст: str) -> str:
    """Вектор запроса строкой для CAST(:v AS vector)."""
    try:
        m = _загрузить_модель()
    except Exception as e:  # noqa: BLE001
        raise Ошибка(503, f"модель эмбеддингов недоступна: {type(e).__name__}")
    import numpy as np
    v = np.asarray(m.encode([текст]), dtype=np.float32)[0]
    n = float(np.linalg.norm(v)) or 1.0
    return "[" + ",".join(f"{x / n:.6f}" for x in v) + "]"




def _json(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _узел(r) -> dict:
    return {"id": r["id"], "вид": r["kind"], "заголовок": r["title"], "кратко": r["summary"],
            "время": _json(r["ts"]), "данные": r["payload"]}


_УРОВНИ_ОПИСАНИЕ = {
    "A": "факт из первоисточника с датой — можно утверждать со ссылкой",
    "B": "факт от посредника (FinanceMarker, smart-lab, хэштег автора) — утверждать с указанием источника и даты",
    "C": "наш вывод по правилу (разметка по имени, детектор) — только как «по нашей разметке»",
    "D": "статистическая подсказка (вместе в новостях, вектор) — не утверждать, использовать для поиска",
}


def _текст_узла(db: Session, id_: str, kind: str) -> Optional[str]:
    """Полный текст источника — дословно (уровень A). Только по запросу: узел хранит 160 символов."""
    ключ = id_.split(":", 1)[1]
    if kind == "news" and "/" in ключ:
        канал, mid = ключ.split("/", 1)
        r = db.execute(text("""
            SELECT text FROM news_archive
             WHERE message_id = CAST(:mid AS bigint)
               AND (channel = :ch OR channel = CASE :ch WHEN 'markettwits' THEN 'MarketTwits' WHEN 'newssmartlab' THEN 'СМАРТЛАБ НОВОСТИ' ELSE :ch END)
             ORDER BY imported_at DESC LIMIT 1
        """), {"mid": mid, "ch": канал}).scalar()
        return r
    if kind in ("candidate", "post"):
        r = db.execute(text("SELECT COALESCE(draft_text, raw_text, headline) FROM content_candidates WHERE id = CAST(:id AS int)"), {"id": ключ}).scalar()
        return r
    if kind == "fact":
        return db.execute(text("SELECT statement FROM world_facts WHERE id = CAST(:id AS bigint)"), {"id": ключ}).scalar()
    return None


def _проверить_id(id_: str) -> str:
    if not _ID.match(id_ or ""):
        raise Ошибка(400, "id узла: вид:ключ, например company:SBER")
    return id_


def _с(days: Optional[int]):
    return (datetime.now(timezone.utc) - timedelta(days=days)) if days else None


def _соседи_sql(id_: str, kind: Optional[str], с, limit: int, offset: int, db: Session):
    """Две индексные выборки (исходящие и входящие), склеенные UNION ALL."""
    п = {"id": id_, "kind": kind, "с": с, "limit": limit, "offset": offset}
    return db.execute(text("""
        SELECT x.kind AS ребро, x.напр, x.ts, x.weight, x.source, x.level, x.method, x.snapshot_date,
               n.id, n.kind, n.title, n.summary, n.payload, n.ts AS nts
          FROM (
                SELECT dst AS other, kind, 'исх' AS напр, ts, weight, source, level, method, snapshot_date FROM brain_edges
                 WHERE src = :id AND (CAST(:kind AS text) IS NULL OR kind = CAST(:kind AS text)) AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
                UNION ALL
                SELECT src, kind, 'вх', ts, weight, source, level, method, snapshot_date FROM brain_edges
                 WHERE dst = :id AND (CAST(:kind AS text) IS NULL OR kind = CAST(:kind AS text)) AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
               ) x
          JOIN brain_nodes n ON n.id = x.other
         ORDER BY x.ts DESC NULLS LAST, n.id
         LIMIT :limit OFFSET :offset
    """), п).mappings().all()


def _след(db, candidate_id, вопрос: str, n: int, params: dict, t0: float, итог: Optional[str] = None):
    """Строка следа: вопрос + человеческий итог (result_note), чтобы в разборе поста было видно
    не «context SBER — 37», а «Сбер · Банки · владельцев 3 · фондов 12 · новостей 25 · аномалий 2»."""
    if candidate_id is None:
        return
    трассировать(db, candidate_id, "мозг").record(
        "brain", вопрос, outcome=ВЗЯТО if n else ПУСТО, result_count=n, params=params,
        result_note=итог, duration_ms=int((time.time() - t0) * 1000))
    db.commit()


def статистика(db: Session = None, _who: str = "agent"):
    """Сколько чего в карте и когда синхронизировалась — для панели."""
    ключ = "brain:stats"
    cached = get_or_set(ключ)
    if cached is not None:
        return cached
    узлы = {r[0]: int(r[1]) for r in db.execute(text("SELECT kind, COUNT(*) FROM brain_nodes GROUP BY kind")).all()}
    рёбра = {r[0]: int(r[1]) for r in db.execute(text("SELECT kind, COUNT(*) FROM brain_edges GROUP BY kind")).all()}
    синк = [{"источник": r[0], "до": _json(r[1]), "строк": r[2], "когда": _json(r[3])}
            for r in db.execute(text("SELECT source, watermark, rows_last, updated_at FROM brain_sync_state ORDER BY source")).all()]
    out = {"узлов": узлы, "рёбер": рёбра, "синхронизация": синк,
           "всего_узлов": sum(узлы.values()), "всего_рёбер": sum(рёбра.values())}
    get_or_set(ключ, out, ttl=_TTL)
    return out


def самые_связанные(
    kind: str = Query("company"),
    limit: int = Query(30, ge=1, le=100),
    db: Session = None,
    _who: str = "agent",
):
    """Узлы вида с наибольшим числом связей — стартовый экран карты."""
    if kind not in _ВИДЫ_УЗЛОВ:
        raise Ошибка(400, f"вид узла: {', '.join(_ВИДЫ_УЗЛОВ)}")
    ключ = f"brain:top:{kind}:{limit}"
    cached = get_or_set(ключ)
    if cached is not None:
        return cached
    строки = db.execute(text("""
        SELECT n.id, n.kind, n.title, n.summary, n.ts, n.payload, COUNT(e.*) AS связей
          FROM brain_nodes n
          JOIN brain_edges e ON e.dst = n.id OR e.src = n.id
         WHERE n.kind = :kind
         GROUP BY n.id ORDER BY связей DESC LIMIT :limit
    """), {"kind": kind, "limit": limit}).mappings().all()
    out = {"вид": kind, "узлы": [{**_узел(r), "связей": int(r["связей"])} for r in строки]}
    get_or_set(ключ, out, ttl=300)
    return out


def узел(
    node_id: str,
    since: Optional[int] = Query(None, ge=1, le=3650, description="кольца только за N дней"),
    per_ring: int = Query(3, ge=1, le=40, description="сколько соседей отдать в каждом кольце"),
    text_: bool = Query(False, alias="text", description="вернуть полный текст новости/кандидата (уровень A — дословно)"),
    candidate_id: Optional[int] = Query(None),
    db: Session = None,
    _who: str = "agent",
):
    """Страница узла: сам узел + кольца по типам связей (счётчик, свежесть, последние N)."""
    t0 = time.time()
    _проверить_id(node_id)
    r = db.execute(text("SELECT id, kind, title, summary, ts, payload FROM brain_nodes WHERE id = :id"), {"id": node_id}).mappings().first()
    if r is None:
        raise Ошибка(404, "узла нет в карте")
    с = _с(since)
    текст = _текст_узла(db, r["id"], r["kind"]) if text_ else None
    кольца = db.execute(text("""
        -- «вместе в новостях» симметрична (пара хранится один раз как a<b): направление
        -- у неё не значит ничего, и два кольца «вх»/«исх» были бы одним, разрезанным пополам.
        SELECT x.kind AS ребро, CASE WHEN x.kind = 'вместе_в_новостях' THEN 'обе' ELSE x.напр END AS напр,
               COUNT(*) AS n, MAX(x.ts) AS свежее, MIN(x.ts) AS старое,
               MIN(x.level) AS лучший, MAX(x.level) AS худший
          FROM (
                SELECT dst AS other, kind, 'исх' AS напр, ts, level FROM brain_edges WHERE src = :id AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
                UNION ALL
                SELECT src, kind, 'вх', ts, level FROM brain_edges WHERE dst = :id AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
               ) x
         GROUP BY x.kind, CASE WHEN x.kind = 'вместе_в_новостях' THEN 'обе' ELSE x.напр END ORDER BY n DESC
    """), {"id": node_id, "с": с}).mappings().all()
    out = {"узел": _узел(r), "кольца": [], "текст": текст, "уровни": _УРОВНИ_ОПИСАНИЕ}
    for к in кольца:
        последние = _соседи_sql(node_id, к["ребро"], с, per_ring, 0, db)
        out["кольца"].append({
            "связь": к["ребро"], "направление": к["напр"], "сколько": int(к["n"]),
            "свежее": _json(к["свежее"]), "старое": _json(к["старое"]),
            "уровень": к["лучший"], "уровень_худший": к["худший"],
            "последние": [{"id": s["id"], "вид": s["kind"], "заголовок": s["title"], "время": _json(s["ts"]),
                           "вес": _json(s["weight"]), "уровень": s["level"], "способ": s["method"],
                           "на_дату": _json(s["snapshot_date"]), "данные": s["payload"]}
                          for s in последние if s["ребро"] == к["ребро"] and (к["напр"] == "обе" or s["напр"] == к["напр"])],
        })
    _след(db, candidate_id, f"node {node_id}" + (f" since={since}" if since else ""), len(кольца), {"id": node_id, "since": since}, t0)
    return out


def соседи(
    id: str = Query(..., description="узел, например company:SBER"),
    kind: Optional[str] = Query(None, description="вид связи: упоминает, о, держит, владеет…"),
    since: Optional[int] = Query(None, ge=1, le=3650),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    candidate_id: Optional[int] = Query(None),
    db: Session = None,
    _who: str = "agent",
):
    """Кольцо целиком: соседи по виду связи за период, свежие первыми."""
    t0 = time.time()
    _проверить_id(id)
    с = _с(since)
    строки = _соседи_sql(id, kind, с, limit, offset, db)
    всего = db.execute(text("""
        SELECT (SELECT COUNT(*) FROM brain_edges WHERE src = :id AND (CAST(:kind AS text) IS NULL OR kind = CAST(:kind AS text)) AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz)))
             + (SELECT COUNT(*) FROM brain_edges WHERE dst = :id AND (CAST(:kind AS text) IS NULL OR kind = CAST(:kind AS text)) AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz)))
    """), {"id": id, "kind": kind, "с": с}).scalar()
    out = {"узел": id, "связь": kind, "всего": int(всего or 0), "предел": limit, "смещение": offset,
           "соседи": [{"id": s["id"], "вид": s["kind"], "заголовок": s["title"], "кратко": s["summary"],
                       "связь": s["ребро"], "направление": s["напр"], "время": _json(s["ts"]), "вес": _json(s["weight"]),
                       "уровень": s["level"], "способ": s["method"], "на_дату": _json(s["snapshot_date"]),
                       "данные": s["payload"]} for s in строки]}
    _след(db, candidate_id, f"neighbors {id} kind={kind or '*'} since={since or '∞'}", out["всего"],
          {"id": id, "kind": kind, "since": since}, t0)
    return out


def поиск(
    q: str = Query(..., min_length=2, max_length=300),
    kind: Optional[str] = Query(None),
    mode: str = Query("word", description="word — по слову (тикер, pg_trgm); meaning — по смыслу (вектор)"),
    limit: int = Query(20, ge=1, le=100),
    candidate_id: Optional[int] = Query(None),
    db: Session = None,
    _who: str = "agent",
):
    """Вход по слову: тикер → компания напрямую; иначе похожесть заголовка (pg_trgm) и подстрока.
    По смыслу (mode=meaning): вектор запроса против HNSW-индекса эмбеддингов узлов."""
    t0 = time.time()
    if kind and kind not in _ВИДЫ_УЗЛОВ:
        raise Ошибка(400, f"вид узла: {', '.join(_ВИДЫ_УЗЛОВ)}")
    qq = q.strip()
    найдено = []
    if mode == "meaning":
        v = _вектор(qq)
        try:
            строки = db.execute(text("""
                SELECT n.id, n.kind, n.title, n.summary, n.ts, n.payload,
                       1 - (e.embedding <=> CAST(:v AS vector)) AS sim
                  FROM brain_embeddings e JOIN brain_nodes n ON n.id = e.node_id
                 WHERE (CAST(:kind AS text) IS NULL OR n.kind = CAST(:kind AS text))
                 ORDER BY e.embedding <=> CAST(:v AS vector)
                 LIMIT :limit
            """), {"v": v, "kind": kind, "limit": limit}).mappings().all()
        except Exception as e:  # noqa: BLE001 — нет расширения/таблицы: вектора ещё не включены
            db.rollback()
            raise Ошибка(503, f"вектора недоступны: {str(e)[:120]}")
        найдено = [{**_узел(r), "почему": f"смысл {float(r['sim']):.2f}"} for r in строки]
        _след(db, candidate_id, f"search по смыслу «{qq[:60]}» kind={kind or '*'}", len(найдено), {"q": qq, "kind": kind, "mode": mode}, t0,
              "; ".join(f"{x['заголовок']} ({x['почему']})" for x in найдено) or None)
        return {"запрос": qq, "режим": "meaning", "найдено": найдено}
    # тикер или код фьючерса — прямой вход в компанию
    if re.fullmatch(r"[A-Za-z0-9_]{2,10}", qq):
        r = db.execute(text("""
            SELECT n.id, n.kind, n.title, n.summary, n.ts, n.payload FROM brain_ticker_map m
              JOIN brain_nodes n ON n.id = m.company_id WHERE m.ticker = :t OR m.ticker = upper(:t) LIMIT 1
        """), {"t": qq}).mappings().first()
        if r and (not kind or kind == "company"):
            найдено.append({**_узел(r), "почему": "тикер"})
    строки = db.execute(text("""
        SELECT id, kind, title, summary, ts, payload, similarity(title, :q) AS sim
          FROM brain_nodes
         WHERE (CAST(:kind AS text) IS NULL OR kind = CAST(:kind AS text)) AND (title ILIKE :like OR title %% :q)  -- «%%»: pg8000 читает одиночный % как плейсхолдер; оператор нужен ради GIN-индекса
         ORDER BY (title ILIKE :like) DESC, sim DESC, ts DESC NULLS LAST
         LIMIT :limit
    """), {"q": qq, "like": f"%{qq}%", "kind": kind, "limit": limit}).mappings().all()
    ids = {x["id"] for x in найдено}
    for r in строки:
        if r["id"] in ids:
            continue
        найдено.append({**_узел(r), "почему": f"похожесть {float(r['sim']):.2f}"})
    _след(db, candidate_id, f"search «{qq}» kind={kind or '*'}", len(найдено), {"q": qq, "kind": kind}, t0,
          "; ".join(f"{x['заголовок']} ({x['почему']})" for x in найдено[:limit]) or None)
    return {"запрос": qq, "найдено": найдено[:limit]}


def похожие(
    id: str = Query(..., description="узел-образец"),
    kind: Optional[str] = Query(None),
    limit: int = Query(12, ge=1, le=60),
    candidate_id: Optional[int] = Query(None),
    db: Session = None,
    _who: str = "agent",
):
    """Похожие по смыслу узлы: ближайшие эмбеддинги к эмбеддингу образца. Это не связь —
    это «на что похоже», якорь для новости без тикера и для поиска прошлых поводов."""
    t0 = time.time()
    _проверить_id(id)
    if kind and kind not in _ВИДЫ_УЗЛОВ:
        raise Ошибка(400, f"вид узла: {', '.join(_ВИДЫ_УЗЛОВ)}")
    try:
        строки = db.execute(text("""
            SELECT n.id, n.kind, n.title, n.summary, n.ts, n.payload,
                   1 - (e.embedding <=> q.embedding) AS sim
              FROM brain_embeddings q
              JOIN brain_embeddings e ON e.node_id <> q.node_id
              JOIN brain_nodes n ON n.id = e.node_id
             WHERE q.node_id = :id AND (CAST(:kind AS text) IS NULL OR n.kind = CAST(:kind AS text))
             ORDER BY e.embedding <=> q.embedding
             LIMIT :limit
        """), {"id": id, "kind": kind, "limit": limit}).mappings().all()
    except Exception as e:  # noqa: BLE001
        db.rollback()
        raise Ошибка(503, f"вектора недоступны: {str(e)[:120]}")
    out = {"узел": id, "похожие": [{**_узел(r), "сходство": round(float(r["sim"]), 3)} for r in строки]}
    _след(db, candidate_id, f"similar {id} kind={kind or '*'}", len(out["похожие"]), {"id": id, "kind": kind}, t0)
    return out


def контекст(
    ticker: str = Query(..., min_length=1, max_length=20, description="тикер бумаги или код фьючерса"),
    days: int = Query(14, ge=1, le=365),
    candidate_id: Optional[int] = Query(None),
    db: Session = None,
    _who: str = "agent",
):
    """Всё, что мозг знает о компании, одним ответом и с уровнем у каждого блока —
    чтобы агент вставил в промпт и не перепутал факт с подсказкой."""
    t0 = time.time()
    cid = db.execute(text("SELECT company_id FROM brain_ticker_map WHERE ticker = :t OR ticker = upper(:t) LIMIT 1"), {"t": ticker.strip()}).scalar()
    if not cid:
        raise Ошибка(404, "тикер не в справочнике")
    у = db.execute(text("SELECT id, kind, title, summary, ts, payload FROM brain_nodes WHERE id = :id"), {"id": cid}).mappings().first()
    с = _с(days)
    if у is not None and у["kind"] == "index":
        # RGBI, IMOEX — индекс, не компания: акционеров и фондов у него нет, есть состав и новости.
        rows = _соседи_sql(cid, "включает", None, 60, 0, db)
        return {"индекс": {**_узел(у), "уровень": "A", "источник": "МосБиржа"},
                "состав": {"уровень": "A", "источник": "МосБиржа, последний состав", "всего": len(rows),
                           "элементы": [{"id": r["id"], "заголовок": r["title"], "вес": _json(r["weight"]), "на_дату": _json(r["snapshot_date"])} for r in rows]},
                "правило_для_агента": _УРОВНИ_ОПИСАНИЕ}
    def кольцо(kind, limit, since=None, напр=None):
        rows = _соседи_sql(cid, kind, since, limit, 0, db)
        return [{"id": s["id"], "заголовок": s["title"], "время": _json(s["ts"]), "вес": _json(s["weight"]),
                 "уровень": s["level"], "способ": s["method"], "на_дату": _json(s["snapshot_date"]), "направление": s["напр"]}
                for s in rows if напр is None or s["напр"] == напр]

    def всего(kind, since=None):
        # Счётчик отдельно от элементов: «кандидатов за 60 дней — 5» при лимите 5 читалось как ровно пять.
        return int(db.execute(text("""
            SELECT (SELECT COUNT(*) FROM brain_edges WHERE src = :id AND kind = :k AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz)))
                 + (SELECT COUNT(*) FROM brain_edges WHERE dst = :id AND kind = :k AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz)))
        """), {"id": cid, "k": kind, "с": since}).scalar() or 0)
    блоки = {
        "компания": {**_узел(у), "уровень": "B", "источник": "справочник эмитентов"},
        "сектор": {"уровень": "B", "источник": "классификация smart-lab", "элементы": кольцо("в_секторе", 3)},
        "владеет": {"уровень": "A/B", "источник": "world_facts, акционеры FinanceMarker", "элементы": кольцо("владеет", 20, напр="исх")},
        "владельцы": {"уровень": "A/B", "источник": "world_facts, акционеры FinanceMarker", "элементы": кольцо("владеет", 20, напр="вх") + кольцо("владеет_долей", 10)},
        "фонды_держатели": {"уровень": "A", "источник": "раскрытие УК, последний снимок", "всего": всего("держит"), "элементы": кольцо("держит", 15)},
        "индексы": {"уровень": "A", "источник": "МосБиржа", "всего": всего("включает"), "элементы": кольцо("включает", 5)},
        "новости": {"уровень": "B/C", "источник": "каналы; хэштег автора — B, разметка по имени — C", "всего": всего("упоминает", с), "элементы": кольцо("упоминает", 8, с)},
        "кандидаты": {"уровень": "B", "источник": "завод постов", "всего": всего("о", _с(60)), "элементы": кольцо("о", 5, _с(60))},
        "аномалии": {"уровень": "C", "источник": "детектор", "всего": всего("аномалия_по", _с(60)), "элементы": кольцо("аномалия_по", 5, _с(60))},
        "вместе_в_новостях": {"уровень": "D", "источник": "совместные упоминания, не отношение", "элементы": кольцо("вместе_в_новостях", 6)},
        # Официальные события — все, не только ставшие кандидатами в пост (Вадим 10.09).
        "раскрытия": {"уровень": "B", "источник": "официальные раскрытия через FinanceMarker", "всего": всего("раскрытие_о", с), "элементы": кольцо("раскрытие_о", 5, с)},
        "объявления_биржи": {"уровень": "A/C", "источник": "МосБиржа; компания по тикеру — A, по имени — C", "всего": всего("объявление_о", с), "элементы": кольцо("объявление_о", 5, с)},
        "отчёты": {"уровень": "A", "источник": "отчёт компании целиком, цифры со страницей", "всего": всего("отчёт_о"), "элементы": кольцо("отчёт_о", 3)},
        # История, а не последний срез: когда вошла в индекс, когда фонды меняли позицию.
        "индексы_события": {"уровень": "A", "источник": "МосБиржа, изменения состава индекса", "всего": всего("событие_индекса"), "элементы": кольцо("событие_индекса", 3)},
        "фонды_события": {"уровень": "A", "источник": "раскрытия УК — между месячными срезами, не сделки", "всего": всего("событие_фонда", с), "элементы": кольцо("событие_фонда", 5, с)},
        "правило_для_агента": _УРОВНИ_ОПИСАНИЕ,
    }
    # Для отчёта заголовок пуст без цифр: сводка узла — то, что извлёк агент-читатель.
    if блоки["отчёты"]["элементы"]:
        сводки = dict(db.execute(text("SELECT id, summary FROM brain_nodes WHERE id = ANY(:ids)"),
                                 {"ids": [e["id"] for e in блоки["отчёты"]["элементы"]]}).all())
        for e in блоки["отчёты"]["элементы"]:
            e["сводка"] = (сводки.get(e["id"]) or "")[:500]
    n = sum(len(v.get("элементы", [])) for v in блоки.values() if isinstance(v, dict))
    def имена(k, m=3):
        return ", ".join(e["заголовок"] for e in блоки[k]["элементы"][:m])
    итог = " · ".join(x for x in [
        f"{у['title']} [{blok_level(блоки['компания'])}]",
        (имена("сектор") + " [B]") if блоки["сектор"]["элементы"] else "",
        f"владельцев {len(блоки['владельцы']['элементы'])}" + (f" ({имена('владельцы')}) [A/B]" if блоки["владельцы"]["элементы"] else ""),
        f"фондов-держателей {блоки['фонды_держатели']['всего']} [A]",
        f"индексов {блоки['индексы']['всего']} [A]",
        f"новостей за {days} дн: {блоки['новости']['всего']} [B/C]",
        f"кандидатов за 60 дн: {блоки['кандидаты']['всего']}",
        f"аномалий за 60 дн: {блоки['аномалии']['всего']} [C]",
        (f"рядом в новостях: {имена('вместе_в_новостях')} [D]") if блоки["вместе_в_новостях"]["элементы"] else "",
    ] if x)
    # Структура — для «рассказа» в разборе поста: там нужны слова, а не разбор строки итога.
    структура = {
        "компания": у["title"],
        "сектор": блоки["сектор"]["элементы"][0]["заголовок"] if блоки["сектор"]["элементы"] else None,
        "сектор_наш": bool(блоки["сектор"]["элементы"]) and блоки["сектор"]["элементы"][0].get("уровень") == "C",
        "владельцы": [{"имя": e["заголовок"], "доля": e.get("вес"), "на_дату": e.get("на_дату")} for e in блоки["владельцы"]["элементы"][:4]],
        "владеет": [e["заголовок"] for e in блоки["владеет"]["элементы"][:6]],
        "фондов": блоки["фонды_держатели"]["всего"],
        "фонды_топ": [e["заголовок"] for e in sorted(блоки["фонды_держатели"]["элементы"], key=lambda e: -(e.get("вес") or 0))[:3]],
        "индексы": [e["заголовок"] for e in блоки["индексы"]["элементы"][:5]],
        "новостей": блоки["новости"]["всего"], "дней": days,
        "кандидатов_60": блоки["кандидаты"]["всего"],
        "аномалий_60": блоки["аномалии"]["всего"],
        "аномалии": [f"{(e.get('время') or '')[:10]} {e['заголовок']}" for e in блоки["аномалии"]["элементы"][:3]],
        "рядом": [e["заголовок"] for e in блоки["вместе_в_новостях"]["элементы"][:4]],
    }
    _след(db, candidate_id, f"context {ticker} days={days}", n,
          {"ticker": ticker, "days": days, "company_id": cid, "структура": структура}, t0, итог)
    return блоки


def blok_level(b: dict) -> str:  # noqa: N802 — короткий помощник для итога следа
    return b.get("уровень", "?")


# ── очереди: держатели и правила имён (только админ)


def очередь_держателей(status: str = Query("на_проверке"), limit: int = Query(100, ge=1, le=500),
                       db: Session = None, _who: str = "agent"):
    rows = db.execute(text("""
        SELECT h.holder_norm, h.holder, h.company_id, h.method, h.confidence, h.status, h.candidates, h.reviewed_at, h.note,
               (SELECT string_agg(replace(c.dst, 'company:', '') || ' ' || COALESCE(s.share_pct::text, '') || '%', ', ')
                  FROM company_shareholders s JOIN issuers i USING (issuer_id)
                  JOIN LATERAL (SELECT 'company:' || i.smartlab_ticker AS dst) c ON TRUE
                 WHERE brain_norm(s.holder) = h.holder_norm AND i.smartlab_ticker IS NOT NULL) AS держит_в
          FROM brain_holder_map h WHERE h.status = :st ORDER BY h.confidence DESC NULLS LAST, h.holder LIMIT :lim
    """), {"st": status, "lim": limit}).mappings().all()
    по_статусам = {r[0]: int(r[1]) for r in db.execute(text("SELECT status, COUNT(*) FROM brain_holder_map GROUP BY status")).all()}
    return {"по_статусам": по_статусам, "очередь": [dict(r) for r in rows]}


def решить_держателя(holder_norm: str, decision: str = Query(..., description="подтверждено | отклонено"),
                     company_id: Optional[str] = Query(None), note: Optional[str] = Query(None),
                     db: Session = None, _who: str = "admin"):
    """Решение человека переживает пересборку; рёбра пересчитаются следующим синком (≤ 15 мин)."""
    if decision not in ("подтверждено", "отклонено"):
        raise Ошибка(400, "decision: подтверждено | отклонено")
    if decision == "подтверждено":
        cid = company_id or db.execute(text("SELECT company_id FROM brain_holder_map WHERE holder_norm = :h"), {"h": holder_norm}).scalar()
        if not cid or not db.execute(text("SELECT 1 FROM brain_nodes WHERE id = :c AND kind = 'company'"), {"c": cid}).scalar():
            raise Ошибка(400, "нужен company_id существующей компании")
    else:
        cid = None
    r = db.execute(text("""
        UPDATE brain_holder_map SET status = :d, company_id = :c, method = 'ручное', confidence = 1.0,
               reviewed_at = NOW(), note = COALESCE(:n, note), updated_at = NOW()
         WHERE holder_norm = :h
    """), {"d": decision, "c": cid, "n": note, "h": holder_norm})
    if r.rowcount == 0:
        raise Ошибка(404, "держателя нет в карте")
    db.commit()
    return {"holder_norm": holder_norm, "status": decision, "company_id": cid}


def правила_имён(ambiguous: Optional[bool] = Query(None), db: Session = None, _who: str = "agent"):
    rows = db.execute(text("""
        SELECT r.id, r.pattern, r.company_id, r.ambiguous, r.enabled, r.source, r.note,
               (SELECT COUNT(*) FROM brain_edges e WHERE e.dst = r.company_id AND e.method = 'имя') AS размечено
          FROM brain_name_rules r
         WHERE (CAST(:a AS boolean) IS NULL OR r.ambiguous = CAST(:a AS boolean))
         ORDER BY r.ambiguous DESC, r.enabled, r.pattern
    """), {"a": ambiguous}).mappings().all()
    return {"правила": [dict(r) for r in rows]}


def править_правило(rule_id: int, enabled: Optional[bool] = Query(None), ambiguous: Optional[bool] = Query(None),
                    note: Optional[str] = Query(None), db: Session = None, _who: str = "admin"):
    r = db.execute(text("""
        UPDATE brain_name_rules SET enabled = COALESCE(CAST(:e AS boolean), enabled), ambiguous = COALESCE(CAST(:a AS boolean), ambiguous),
               note = COALESCE(:n, note), manual = TRUE, updated_at = NOW() WHERE id = :id
    """), {"e": enabled, "a": ambiguous, "n": note, "id": rule_id})
    if r.rowcount == 0:
        raise Ошибка(404, "правила нет")
    db.commit()
    return {"id": rule_id, "enabled": enabled, "ambiguous": ambiguous}


# ── Аудит разметки по имени (Routine frame-brain-audit) ─────────────────────────
# Правила имён размечают новости без хэштега тикера (уровень C). Точность меряли один
# раз, 05.09: ~90 % после правок. Агент читает новость и отвечает, про эту ли компанию
# она: первый раз — по всей базе, дальше раз в неделю новое плюс выборка старого.
# Неверные связи синк больше не строит (brain_sync: brain_edge_reviews); повторяющиеся
# ошибки агент предлагает закрыть исключением в правиле — решает человек.
# Запуск — signals/brain_audit_fire.py, инструкция — research/brain/prompt_name_audit_routine.md.
_ВЕРДИКТЫ = ("верно", "неверно", "неясно")
_НЕПРОВЕРЕННЫЕ = """
    FROM brain_edges e
   WHERE e.kind = 'упоминает' AND e.method = 'имя'
     AND NOT EXISTS (SELECT 1 FROM brain_edge_reviews r
                      WHERE r.src = e.src AND r.dst = e.dst AND r.kind = e.kind)
"""
# ⚠️ ВТОРОЕ МНЕНИЕ (Вадим 10.09). Проверочный прогон: из 40 вердиктов единственное
# «неверно» было ошибкой агента — «Sitronics (входит в АФК Систему)» признано чужим.
# «Верно» ничего не меняет, а «неверно» удаляет связь, поэтому одного агента для
# удаления мало: связь уходит только при согласии двух независимых проверок или по
# решению человека. Второй проход — отдельная сессия и вслепую: к подозрительным
# подмешано столько же случайных «верно», порядок перемешан, причина первого не видна.
_ВТОРОЕ_ЖДЁТ = """
    FROM brain_edge_reviews r JOIN brain_edges e ON e.src = r.src AND e.dst = r.dst AND e.kind = r.kind
   WHERE r.verdict = 'неверно' AND r.second_verdict IS NULL AND r.human_decision IS NULL
"""
# Спор — ровно один из двух сказал «неверно». Связь остаётся, решает человек.
_СПОРНО = "((r.verdict = 'неверно') <> (r.second_verdict = 'неверно')) AND r.human_decision IS NULL"


def _текст_новости(alias: str) -> str:
    """Полный текст новости из архива по id узла `news:<канал>/<id>` (узел хранит 160 симв.).
    Исторический экспорт лежит под старыми именами каналов — отсюда второй вариант."""
    return f"""
        LEFT JOIN LATERAL (
            SELECT x.text FROM news_archive x
             WHERE x.message_id = CAST(split_part({alias}.src, '/', 2) AS bigint)
               AND x.channel IN (split_part(substr({alias}.src, 6), '/', 1),
                                 CASE split_part(substr({alias}.src, 6), '/', 1)
                                      WHEN 'markettwits' THEN 'MarketTwits'
                                      WHEN 'newssmartlab' THEN 'СМАРТЛАБ НОВОСТИ' END)
             LIMIT 1) a ON TRUE"""


def _окно(текст: str, имена: Optional[str], ширина: int = 600) -> str:
    """Кусок новости, где стоит имя компании. Первые 600 символов резали упоминание: в
    прогоне 10.09 агент не нашёл Совкомбанк в анонсе конференции — имя было дальше, и
    честный вердикт был только «неясно». Основа имени без двух последних букв ловит
    падежи («Совкомбанка»)."""
    t = " ".join((текст or "").split())
    if len(t) <= ширина:
        return t
    низ = t.lower()
    позиции = [низ.find(n.strip().lower()[:max(4, len(n.strip()) - 2)])
               for n in (имена or "").split("|") if n.strip()]
    p = min([x for x in позиции if x >= 0], default=0)
    if p < ширина - 150:
        return t[:ширина]
    return "…" + t[max(0, p - 200):max(0, p - 200) + ширина]


def аудит_имён_партия(limit: int = Query(50, ge=1, le=200), resample: int = Query(0, ge=0, le=100),
                      mode: str = Query("main"), db: Session = None, _who: str = "agent"):
    """Партия для агента.

    main — связи «по имени», которых никто не проверял (старые вперёд — первый проход
    идёт по всей базе), плюс resample случайных, признанных верными больше месяца назад:
    заметить, что разметка поплыла.
    second — второе мнение: «неверно» первого прохода вперемешку с таким же числом
    случайных «верно»; метки «перепроверка» нет ни у одной — проверка вслепую.

    Текст новости обрезан до 600 символов: вывод Bash у агента конечен."""
    if mode not in ("main", "second"):
        raise Ошибка(400, "mode: main | second")
    if mode == "second":
        ждут = int(db.execute(text(f"SELECT COUNT(*) {_ВТОРОЕ_ЖДЁТ}")).scalar() or 0)
        половина = min(ждут, max(1, limit // 2))
        pick = f"""
            (SELECT r.src, r.dst, e.ts, FALSE AS повтор {_ВТОРОЕ_ЖДЁТ} ORDER BY r.reviewed_at LIMIT :half)
            UNION ALL
            (SELECT r.src, r.dst, e.ts, FALSE FROM brain_edge_reviews r
               JOIN brain_edges e ON e.src = r.src AND e.dst = r.dst AND e.kind = r.kind
              WHERE r.verdict = 'верно' AND r.second_verdict IS NULL AND r.human_decision IS NULL
              ORDER BY random() LIMIT :half)"""
        параметры = {"half": половина if ждут else 0}
    else:
        pick = f"""
            (SELECT e.src, e.dst, e.ts, FALSE AS повтор {_НЕПРОВЕРЕННЫЕ} ORDER BY e.ts LIMIT :lim)
            UNION ALL
            (SELECT e.src, e.dst, e.ts, TRUE FROM brain_edges e
               JOIN brain_edge_reviews r ON r.src = e.src AND r.dst = e.dst AND r.kind = e.kind
              WHERE e.method = 'имя' AND r.verdict = 'верно' AND r.reviewed_at < NOW() - INTERVAL '30 days'
              ORDER BY random() LIMIT :rs)"""
        параметры = {"lim": limit, "rs": resample}
    rows = db.execute(text(f"""
        WITH pick AS ({pick})
        SELECT p.src, p.dst, p.ts, p.повтор, c.title AS компания,
               -- Короткие имена: у ВТБ в правилах есть и «VTBR-9.25 (Фьючерсный контракт…)» из
               -- названий активов фондов — агенту это шум, а вывод у него конечен.
               (SELECT string_agg(q.pattern, ' | ') FROM (
                    SELECT DISTINCT x.pattern FROM brain_name_rules x
                     WHERE x.company_id = p.dst AND x.enabled AND NOT x.ambiguous AND length(x.pattern) <= 40
                     ORDER BY 1 LIMIT 6) q) AS имена,
               COALESCE(a.text, nn.title) AS текст
          FROM pick p
          JOIN brain_nodes c ON c.id = p.dst
          LEFT JOIN brain_nodes nn ON nn.id = p.src
          {_текст_новости("p")}
         ORDER BY random()
    """), параметры).mappings().all()
    осталось = db.execute(text(f"SELECT COUNT(*) {_ВТОРОЕ_ЖДЁТ if mode == 'second' else _НЕПРОВЕРЕННЫЕ}")).scalar()
    return {
        "партия": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M"),
        "режим": mode,
        "непроверенных_всего": int(осталось or 0),
        "связи": [{"id": f"{r['src']}|{r['dst']}", "компания": r["компания"],
                   "имена_в_правилах": r["имена"], "дата": str(r["ts"])[:10],
                   "перепроверка": bool(r["повтор"]),
                   "текст": _окно(r["текст"], r["имена"])} for r in rows],
    }


def аудит_имён_решения(body: dict, db: Session = None, _who: str = "agent"):
    """Вердикты агента. Повторная проверка перезаписывает прежний вердикт; мусор в теле
    (не тот вердикт, не тот id) отбрасывается и считается — агент увидит это в ответе."""
    import json
    партия = str(body.get("партия") or "")[:40]
    кто = "routine" if _who == "agent" else _who
    второе = body.get("режим") == "second"
    принято = отброшено = предложено = 0
    for d in body.get("решения") or []:
        src, _, dst = str(d.get("id") or "").partition("|")
        вердикт = d.get("вердикт")
        if вердикт not in _ВЕРДИКТЫ or not src.startswith("news:") or not dst.startswith("company:"):
            отброшено += 1
            continue
        п = {"s": src, "d": dst, "v": вердикт, "r": str(d.get("причина") or "")[:300] or None,
             "who": кто, "b": партия}
        if второе:
            # Второе мнение пишется рядом с первым и только один раз — по связи, которую
            # первый проход уже видел. Чужой id или повтор — отброшено.
            r = db.execute(text("""
                UPDATE brain_edge_reviews SET second_verdict = :v, second_reason = :r, second_at = NOW()
                 WHERE src = :s AND dst = :d AND kind = 'упоминает' AND second_verdict IS NULL
            """), п)
            if not r.rowcount:
                отброшено += 1
                continue
        else:
            # Новый вердикт первого прохода (еженедельная перепроверка) обнуляет второе
            # мнение, если вердикт сменился: оно было про другой ответ.
            db.execute(text("""
                INSERT INTO brain_edge_reviews (src, dst, kind, verdict, reason, reviewer, batch, reviewed_at)
                VALUES (:s, :d, 'упоминает', :v, :r, :who, :b, NOW())
                ON CONFLICT (src, dst, kind) DO UPDATE SET verdict = EXCLUDED.verdict, reason = EXCLUDED.reason,
                       reviewer = EXCLUDED.reviewer, batch = EXCLUDED.batch, reviewed_at = NOW(),
                       second_verdict = CASE WHEN brain_edge_reviews.verdict = EXCLUDED.verdict
                                             THEN brain_edge_reviews.second_verdict END,
                       second_reason = CASE WHEN brain_edge_reviews.verdict = EXCLUDED.verdict
                                            THEN brain_edge_reviews.second_reason END,
                       second_at = CASE WHEN brain_edge_reviews.verdict = EXCLUDED.verdict
                                        THEN brain_edge_reviews.second_at END
            """), п)
        принято += 1
    for p in ([] if второе else body.get("предложения") or []):
        cid, rx = str(p.get("company_id") or ""), str(p.get("исключение") or "")
        if not cid.startswith("company:") or not rx or len(rx) > 200:
            continue
        try:
            # Регэксп должен компилироваться именно в Postgres — синк применяет его там.
            with db.begin_nested():
                db.execute(text("SELECT '' ~* :rx"), {"rx": rx})
        except Exception:  # noqa: BLE001 — кривое предложение просто не принимаем
            continue
        r = db.execute(text("""
            INSERT INTO brain_rule_proposals (company_id, exclude_regex, examples, reason)
            SELECT :c, :rx, CAST(:ex AS jsonb), :why
             WHERE NOT EXISTS (SELECT 1 FROM brain_rule_proposals WHERE company_id = :c AND exclude_regex = :rx)
        """), {"c": cid, "rx": rx, "ex": json.dumps((p.get("примеры") or [])[:5], ensure_ascii=False),
               "why": str(p.get("причина") or "")[:300] or None})
        предложено += r.rowcount
    db.commit()
    return {"принято": принято, "отброшено": отброшено, "предложено": предложено}


def аудит_имён_сводка(db: Session = None, _who: str = "agent"):
    """Для панели: точность разметки по неделям, сколько ещё не проверено, предложения."""
    недели = db.execute(text("""
        SELECT date_trunc('week', reviewed_at)::date AS неделя,
               COUNT(*) FILTER (WHERE verdict = 'верно') AS верно,
               COUNT(*) FILTER (WHERE verdict = 'неверно') AS неверно,
               COUNT(*) FILTER (WHERE verdict = 'неясно') AS неясно
          FROM brain_edge_reviews GROUP BY 1 ORDER BY 1 DESC LIMIT 12
    """)).mappings().all()
    предложения = db.execute(text("""
        SELECT p.id, p.company_id, c.title AS компания, p.exclude_regex, p.examples, p.reason, p.created_at
          FROM brain_rule_proposals p LEFT JOIN brain_nodes c ON c.id = p.company_id
         WHERE p.status = 'на_проверке' ORDER BY p.created_at DESC LIMIT 50
    """)).mappings().all()
    осталось = db.execute(text(f"SELECT COUNT(*) {_НЕПРОВЕРЕННЫЕ}")).scalar()
    ждут = db.execute(text(f"SELECT COUNT(*) {_ВТОРОЕ_ЖДЁТ}")).scalar()
    убрано = db.execute(text("""
        SELECT COUNT(*) FROM brain_edge_reviews r
         WHERE r.human_decision = 'убрать'
            OR (r.human_decision IS NULL AND r.verdict = 'неверно' AND r.second_verdict = 'неверно')
    """)).scalar()
    спорные = db.execute(text(f"""
        SELECT r.src || '|' || r.dst AS id, c.title AS компания, r.verdict AS первое, r.reason AS первая_причина,
               r.second_verdict AS второе, r.second_reason AS вторая_причина,
               left(regexp_replace(COALESCE(a.text, nn.title, ''), '\\s+', ' ', 'g'), 400) AS текст
          FROM brain_edge_reviews r
          LEFT JOIN brain_nodes c ON c.id = r.dst
          LEFT JOIN brain_nodes nn ON nn.id = r.src
          {_текст_новости("r")}
         WHERE r.second_verdict IS NOT NULL AND {_СПОРНО}
         ORDER BY r.second_at DESC LIMIT 50
    """)).mappings().all()

    ярлыки = db.execute(text("""
        SELECT COUNT(*) FILTER (WHERE тип_источник = 'правило'),
               COUNT(*) FILTER (WHERE тип IS NOT NULL AND тип_источник <> 'правило'),
               COUNT(*) FILTER (WHERE тип IS NULL)
          FROM brain_news_labels
    """)).first()

    def точность(r):
        n = r["верно"] + r["неверно"]
        return round(100 * r["верно"] / n) if n else None

    return {"непроверенных": int(осталось or 0),
            "ждут_второго_мнения": int(ждут or 0),
            "убрано": int(убрано or 0),
            "ярлыки": {"по_правилам": int(ярлыки[0] or 0), "от_агента": int(ярлыки[1] or 0),
                       "без_типа": int(ярлыки[2] or 0)},
            "недели": [{**dict(r), "точность": точность(r)} for r in недели],
            "спорные": [dict(r) for r in спорные],
            "предложения": [dict(r) for r in предложения]}


def решить_спор(id: str = Query(...), decision: str = Query(..., description="убрать | оставить"),
                db: Session = None, _who: str = "admin"):
    """Решение человека по спорной связи (агенты разошлись). Переживает пересборку и
    еженедельную перепроверку; «убрать» синк применит в ближайший прогон (≤ 15 мин)."""
    if decision not in ("убрать", "оставить"):
        raise Ошибка(400, "decision: убрать | оставить")
    src, _, dst = id.partition("|")
    r = db.execute(text("""
        UPDATE brain_edge_reviews SET human_decision = :h, human_at = NOW()
         WHERE src = :s AND dst = :d AND kind = 'упоминает'
    """), {"h": decision, "s": src, "d": dst})
    if r.rowcount == 0:
        raise Ошибка(404, "такой проверенной связи нет")
    db.commit()
    return {"id": id, "decision": decision}


def решить_предложение(pid: int, decision: str = Query(..., description="принять | отклонить"),
                       db: Session = None, _who: str = "admin"):
    """Принятое исключение дописывается к правилам компании (manual — пересев не трогает)
    и работает для новых новостей; старые неверные связи уже сняты вердиктами агента."""
    if decision not in ("принять", "отклонить"):
        raise Ошибка(400, "decision: принять | отклонить")
    p = db.execute(text("SELECT company_id, exclude_regex FROM brain_rule_proposals WHERE id = :i AND status = 'на_проверке'"),
                   {"i": pid}).mappings().first()
    if not p:
        raise Ошибка(404, "предложения нет или оно уже решено")
    if decision == "принять":
        db.execute(text("""
            UPDATE brain_name_rules
               SET exclude_regex = CASE WHEN COALESCE(exclude_regex, '') = '' THEN :rx ELSE exclude_regex || '|' || :rx END,
                   manual = TRUE, updated_at = NOW()
             WHERE company_id = :c AND enabled AND NOT ambiguous
        """), {"rx": p["exclude_regex"], "c": p["company_id"]})
    статус = "принято" if decision == "принять" else "отклонено"
    db.execute(text("UPDATE brain_rule_proposals SET status = :s, decided_at = NOW() WHERE id = :i"), {"s": статус, "i": pid})
    db.commit()
    return {"id": pid, "status": статус}


# ── Ярлыки новостей: тип события и роль компании (режим labels аудита) ─────────────
# Правила синка (Brain/brain_sync.py:ярлыки_новостей) размечают ~47 % новостей за 90 дней;
# остаток — ночной агент. Список типов — тот же, что в правилах синка (тест сверяет),
# плюс «прочее» для агента: натянутый тип хуже честного «прочее».
_ТИПЫ_НОВОСТЕЙ = ("отчётность", "дивиденды", "выкуп акций", "размещение акций", "облигации", "санкции",
                  "суд", "рейтинг", "мнение аналитиков", "сделка", "управление", "операционные", "прочее")
_РОЛИ = ("главная", "упоминание")
_БЕЗ_ТИПА = """
    FROM brain_news_labels l JOIN brain_nodes b ON b.id = l.node_id
   WHERE l.тип IS NULL AND b.ts > NOW() - INTERVAL '90 days'
"""


def ярлыки_партия(limit: int = Query(40, ge=1, le=200), db: Session = None, _who: str = "agent"):
    """Новости за 90 дней, которым правила не нашли тип: свежие вперёд — их первыми
    увидит писатель. У каждой — компании и роль, которую поставили правила."""
    rows = db.execute(text(f"""
        WITH p AS (SELECT l.node_id AS src, b.ts, l.роли {_БЕЗ_ТИПА} ORDER BY b.ts DESC LIMIT :lim)
        SELECT p.src, p.ts, COALESCE(a.text, nn.title) AS текст,
               (SELECT jsonb_agg(jsonb_build_object('company_id', e.dst, 'название', c.title,
                                                    'роль_по_правилу', p.роли ->> e.dst))
                  FROM brain_edges e JOIN brain_nodes c ON c.id = e.dst
                 WHERE e.src = p.src AND e.kind = 'упоминает') AS компании
          FROM p LEFT JOIN brain_nodes nn ON nn.id = p.src
          {_текст_новости("p")}
    """), {"lim": limit}).mappings().all()
    осталось = db.execute(text(f"SELECT COUNT(*) {_БЕЗ_ТИПА}")).scalar()
    return {"партия": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M"), "режим": "labels",
            "осталось": int(осталось or 0), "типы": list(_ТИПЫ_НОВОСТЕЙ),
            "новости": [{"id": r["src"], "дата": str(r["ts"])[:10], "компании": r["компании"] or [],
                         "текст": " ".join((r["текст"] or "").split())[:600]} for r in rows]}


def ярлыки_решения(body: dict, db: Session = None, _who: str = "agent"):
    """Тип и роли от агента. Пишутся один раз — только новости без типа; тип правил агент
    не перетирает. Не тот тип, не та роль, чужой id — отброшено и посчитано."""
    import json
    кто = "агент" if _who == "agent" else _who
    принято = отброшено = 0
    for d in body.get("ярлыки") or []:
        id_, тип, роли = str(d.get("id") or ""), d.get("тип"), d.get("роли") or {}
        if (not id_.startswith("news:") or тип not in _ТИПЫ_НОВОСТЕЙ or not isinstance(роли, dict)
                or any(not str(k).startswith("company:") or v not in _РОЛИ for k, v in роли.items())):
            отброшено += 1
            continue
        # ⚠️ Без литералов '{}'::jsonb: pg8000 портит их (прогон 10.09 с '[]'::jsonb).
        r = db.execute(text("""
            UPDATE brain_news_labels
               SET тип = :t, тип_источник = :who,
                   роли = COALESCE(роли, jsonb_build_object()) || CAST(:r AS jsonb),
                   роли_источник = CASE WHEN CAST(:r AS jsonb) = jsonb_build_object() THEN роли_источник ELSE :who END,
                   updated_at = NOW()
             WHERE node_id = :id AND тип IS NULL
        """), {"t": тип, "who": кто, "r": json.dumps(роли, ensure_ascii=False), "id": id_})
        if not r.rowcount:
            отброшено += 1
            continue
        принято += 1
    db.commit()
    return {"принято": принято, "отброшено": отброшено}


def _соседи_для_пути(db: Session, id_: str) -> list[tuple[str, str, str]]:
    """(сосед, связь, направление), структурные связи первыми, не больше лимита."""
    строки = db.execute(text("""
        SELECT other, kind, напр FROM (
            SELECT dst AS other, kind, 'исх' AS напр, ts FROM brain_edges WHERE src = :id
            UNION ALL
            SELECT src, kind, 'вх', ts FROM brain_edges WHERE dst = :id
        ) x ORDER BY ts DESC NULLS LAST LIMIT 3000
    """), {"id": id_}).all()
    строки.sort(key=lambda r: _ПРИОРИТЕТ.get(r[1], 99))
    return [(r[0], r[1], r[2]) for r in строки[:_ЛИМИТ_СОСЕДЕЙ_В_ПУТИ]]


def путь(
    a: str = Query(...), b: str = Query(...),
    max_depth: int = Query(3, ge=1, le=4),
    candidate_id: Optional[int] = Query(None),
    db: Session = None,
    _who: str = "agent",
):
    """Кратчайшая связь между двумя узлами: обход в ширину с бюджетом, структурные связи в приоритете."""
    t0 = time.time()
    _проверить_id(a); _проверить_id(b)
    if a == b:
        return {"от": a, "до": b, "путь": [], "шагов": 0}
    пришли = {a: None}      # узел → (предыдущий, связь, направление)
    очередь = deque([(a, 0)])
    расширено = 0
    найден = None
    while очередь and расширено < _БЮДЖЕТ_ПУТИ and найден is None:
        cur, d = очередь.popleft()
        if d >= max_depth:
            continue
        for other, kind, напр in _соседи_для_пути(db, cur):
            расширено += 1
            if other in пришли:
                continue
            пришли[other] = (cur, kind, напр)
            if other == b:
                найден = other
                break
            очередь.append((other, d + 1))
    if найден is None:
        _след(db, candidate_id, f"path {a} → {b}", 0, {"a": a, "b": b}, t0)
        return {"от": a, "до": b, "путь": None, "шагов": None, "просмотрено": расширено}
    шаги = []
    x = b
    while пришли[x] is not None:
        prev, kind, напр = пришли[x]
        шаги.append({"от": prev, "связь": kind, "направление": напр, "к": x})
        x = prev
    шаги.reverse()
    ids = [a] + [ш["к"] for ш in шаги]
    имена = {r[0]: {"вид": r[1], "заголовок": r[2]} for r in db.execute(
        text("SELECT id, kind, title FROM brain_nodes WHERE id = ANY(string_to_array(:ids, '|'))"), {"ids": "|".join(ids)}).all()}
    for ш in шаги:
        ш["узел"] = имена.get(ш["к"])
    _след(db, candidate_id, f"path {a} → {b}", len(шаги), {"a": a, "b": b}, t0)
    return {"от": a, "до": b, "путь": шаги, "шагов": len(шаги), "просмотрено": расширено}


# ---------------------------------------------------------------------------
# Граф для силовой раскладки (вид «как в Obsidian»)
# ---------------------------------------------------------------------------
_СТРУКТУРНЫЕ_ВИДЫ = ("company", "sector", "holder", "fund", "index")
_СТРУКТУРНЫЕ_СВЯЗИ = ("в_секторе", "владеет", "владеет_долей", "держит", "включает")
_ЛИМИТ_ГРАФА = 600


def _рёбра_между(db: Session, ids: list[str]) -> list:
    """Все рёбра, у которых оба конца в наборе. Набор ≤ 600 — массив в ANY, по индексам src/dst."""
    if not ids:
        return []
    return db.execute(text("""
        SELECT src, dst, kind, level, weight
          FROM brain_edges
         WHERE src = ANY(string_to_array(:ids, '|')) AND dst = ANY(string_to_array(:ids, '|'))
    """), {"ids": "|".join(ids)}).mappings().all()


def _узлы_по_id(db: Session, ids: list[str]) -> dict:
    if not ids:
        return {}
    return {r["id"]: r for r in db.execute(text("""
        SELECT n.id, n.kind, n.title, n.ts,
               (SELECT COUNT(*) FROM brain_edges e WHERE e.src = n.id) + (SELECT COUNT(*) FROM brain_edges e WHERE e.dst = n.id) AS степень
          FROM brain_nodes n WHERE n.id = ANY(string_to_array(:ids, '|'))
    """), {"ids": "|".join(ids)}).mappings().all()}


def граф(
    center: Optional[str] = Query(None, description="узел-центр; без него — весь структурный слой"),
    depth: int = Query(2, ge=1, le=3),
    per_node: int = Query(40, ge=5, le=200, description="сколько соседей брать у каждого узла при обходе"),
    news: bool = Query(False, description="включать новости/кандидатов/посты (иначе только структурные узлы)"),
    limit: int = Query(_ЛИМИТ_ГРАФА, ge=20, le=1500),
    db: Session = None,
    _who: str = "agent",
):
    """Подграф для силовой раскладки: {узлы:[{id, вид, заголовок, степень}], рёбра:[{от, к, связь, уровень, вес}]}.

    Без центра — структурный слой целиком (компании, секторы, держатели, фонды, индексы и
    связи между ними — ≈550 узлов): его можно рисовать весь. С центром — обход в ширину на
    depth шагов с веером per_node на узел, структурные связи первыми, новости — только по флагу.
    Рёбра добираются вторым запросом «между набором», чтобы показать и боковые связи.
    """
    t0 = time.time()
    if center is None:
        ключ = f"brain:graph:структура:{limit}"
        cached = get_or_set(ключ)
        if cached is not None:
            return cached
        узлы = db.execute(text("""
            SELECT n.id, n.kind, n.title, n.ts,
                   (SELECT COUNT(*) FROM brain_edges e WHERE e.src = n.id) + (SELECT COUNT(*) FROM brain_edges e WHERE e.dst = n.id) AS степень
              FROM brain_nodes n
             WHERE n.kind = ANY(string_to_array(:kinds, ','))
               AND EXISTS (SELECT 1 FROM brain_edges e WHERE (e.src = n.id OR e.dst = n.id) AND e.kind = ANY(string_to_array(:ek, ',')))
             ORDER BY степень DESC LIMIT :limit
        """), {"kinds": ",".join(_СТРУКТУРНЫЕ_ВИДЫ), "ek": ",".join(_СТРУКТУРНЫЕ_СВЯЗИ), "limit": limit}).mappings().all()
        ids = [r["id"] for r in узлы]
        рёбра = [r for r in _рёбра_между(db, ids) if r["kind"] in _СТРУКТУРНЫЕ_СВЯЗИ or r["kind"] == "вместе_в_новостях"]
        out = {
            "центр": None, "глубина": 0,
            "узлы": [{"id": r["id"], "вид": r["kind"], "заголовок": r["title"], "степень": int(r["степень"])} for r in узлы],
            "рёбра": [{"от": r["src"], "к": r["dst"], "связь": r["kind"], "уровень": r["level"], "вес": _json(r["weight"])} for r in рёбра],
            "мс": int((time.time() - t0) * 1000),
        }
        get_or_set(ключ, out, ttl=900)
        return out

    _проверить_id(center)
    if not db.execute(text("SELECT 1 FROM brain_nodes WHERE id = :id"), {"id": center}).first():
        raise Ошибка(404, "узла нет")
    видимые_виды = None if news else _СТРУКТУРНЫЕ_ВИДЫ
    взяты = {center: 0}
    очередь = deque([(center, 0)])
    while очередь and len(взяты) < limit:
        cur, d = очередь.popleft()
        if d >= depth:
            continue
        соседи = db.execute(text("""
            SELECT other, kind FROM (
                SELECT e.dst AS other, e.kind, e.ts FROM brain_edges e WHERE e.src = :id
                UNION ALL
                SELECT e.src, e.kind, e.ts FROM brain_edges e WHERE e.dst = :id
            ) x ORDER BY ts DESC NULLS LAST LIMIT 3000
        """), {"id": cur}).all()
        соседи.sort(key=lambda r: _ПРИОРИТЕТ.get(r[1], 99))
        n = 0
        for other, kind in соседи:
            if n >= per_node or len(взяты) >= limit:
                break
            if other in взяты:
                continue
            if видимые_виды is not None and other.split(":", 1)[0] not in видимые_виды:
                continue
            взяты[other] = d + 1
            очередь.append((other, d + 1))
            n += 1
    ids = list(взяты)
    инфо = _узлы_по_id(db, ids)
    рёбра = _рёбра_между(db, ids)
    return {
        "центр": center, "глубина": depth,
        "узлы": [{"id": i, "вид": инфо[i]["kind"], "заголовок": инфо[i]["title"], "степень": int(инфо[i]["степень"]), "шаг": взяты[i]}
                 for i in ids if i in инфо],
        "рёбра": [{"от": r["src"], "к": r["dst"], "связь": r["kind"], "уровень": r["level"], "вес": _json(r["weight"])} for r in рёбра],
        "мс": int((time.time() - t0) * 1000),
    }
