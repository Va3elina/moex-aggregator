"""
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
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.agent_trace import ВЗЯТО, ПУСТО, трассировать
from api.cache import get_or_set
from api.database import get_db
from api.models import User
from api.routers.auth import get_current_user_optional

router = APIRouter(prefix="/api/internal/brain", tags=["brain"])

_ID = re.compile(r"^[a-z]+:[A-Za-z0-9_./\-]{1,120}$")
_ВИДЫ_УЗЛОВ = ("company", "news", "candidate", "post", "doc", "fund", "index", "fact", "anomaly", "signal", "holder")
# Порядок расширения при поиске пути: сначала структурные связи, новости — последними.
_ПРИОРИТЕТ = {"владеет": 0, "владеет_долей": 1, "держит": 2, "включает": 3, "факт_о": 4, "о": 5,
              "из_новости": 6, "сигнал_о": 7, "аномалия_по": 8, "отчитался": 9, "упоминает": 10}
_ЛИМИТ_СОСЕДЕЙ_В_ПУТИ = 300
_БЮДЖЕТ_ПУТИ = 20_000
_TTL = 60


def _доступ(
    x_internal_token: str = Header(default=""),
    user: Optional[User] = Depends(get_current_user_optional),
) -> str:
    ожидаемый = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if ожидаемый and x_internal_token == ожидаемый:
        return "agent"
    if user is not None and user.role == "admin":
        return "admin"
    raise HTTPException(status_code=403, detail="нужен internal token или сессия администратора")


def _json(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _узел(r) -> dict:
    return {"id": r["id"], "вид": r["kind"], "заголовок": r["title"], "кратко": r["summary"],
            "время": _json(r["ts"]), "данные": r["payload"]}


def _проверить_id(id_: str) -> str:
    if not _ID.match(id_ or ""):
        raise HTTPException(400, "id узла: вид:ключ, например company:SBER")
    return id_


def _с(days: Optional[int]):
    return (datetime.now(timezone.utc) - timedelta(days=days)) if days else None


def _соседи_sql(id_: str, kind: Optional[str], с, limit: int, offset: int, db: Session):
    """Две индексные выборки (исходящие и входящие), склеенные UNION ALL."""
    п = {"id": id_, "kind": kind, "с": с, "limit": limit, "offset": offset}
    return db.execute(text("""
        SELECT x.kind AS ребро, x.напр, x.ts, x.weight, x.source,
               n.id, n.kind, n.title, n.summary, n.payload, n.ts AS nts
          FROM (
                SELECT dst AS other, kind, 'исх' AS напр, ts, weight, source FROM brain_edges
                 WHERE src = :id AND (CAST(:kind AS text) IS NULL OR kind = CAST(:kind AS text)) AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
                UNION ALL
                SELECT src, kind, 'вх', ts, weight, source FROM brain_edges
                 WHERE dst = :id AND (CAST(:kind AS text) IS NULL OR kind = CAST(:kind AS text)) AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
               ) x
          JOIN brain_nodes n ON n.id = x.other
         ORDER BY x.ts DESC NULLS LAST, n.id
         LIMIT :limit OFFSET :offset
    """), п).mappings().all()


def _след(db, candidate_id, вопрос: str, n: int, params: dict, t0: float):
    if candidate_id is None:
        return
    трассировать(db, candidate_id, "мозг").record(
        "brain", вопрос, outcome=ВЗЯТО if n else ПУСТО, result_count=n, params=params,
        duration_ms=int((time.time() - t0) * 1000))
    db.commit()


@router.get("/stats")
def статистика(db: Session = Depends(get_db), _who: str = Depends(_доступ)):
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


@router.get("/top")
def самые_связанные(
    kind: str = Query("company"),
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
    _who: str = Depends(_доступ),
):
    """Узлы вида с наибольшим числом связей — стартовый экран карты."""
    if kind not in _ВИДЫ_УЗЛОВ:
        raise HTTPException(400, f"вид узла: {', '.join(_ВИДЫ_УЗЛОВ)}")
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


@router.get("/node/{node_id:path}")
def узел(
    node_id: str,
    since: Optional[int] = Query(None, ge=1, le=3650, description="кольца только за N дней"),
    per_ring: int = Query(3, ge=1, le=40, description="сколько соседей отдать в каждом кольце"),
    candidate_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    _who: str = Depends(_доступ),
):
    """Страница узла: сам узел + кольца по типам связей (счётчик, свежесть, последние N)."""
    t0 = time.time()
    _проверить_id(node_id)
    r = db.execute(text("SELECT id, kind, title, summary, ts, payload FROM brain_nodes WHERE id = :id"), {"id": node_id}).mappings().first()
    if r is None:
        raise HTTPException(404, "узла нет в карте")
    с = _с(since)
    кольца = db.execute(text("""
        SELECT x.kind AS ребро, x.напр, COUNT(*) AS n, MAX(x.ts) AS свежее, MIN(x.ts) AS старое
          FROM (
                SELECT dst AS other, kind, 'исх' AS напр, ts FROM brain_edges WHERE src = :id AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
                UNION ALL
                SELECT src, kind, 'вх', ts FROM brain_edges WHERE dst = :id AND (CAST(:с AS timestamptz) IS NULL OR ts >= CAST(:с AS timestamptz))
               ) x
         GROUP BY x.kind, x.напр ORDER BY n DESC
    """), {"id": node_id, "с": с}).mappings().all()
    out = {"узел": _узел(r), "кольца": []}
    for к in кольца:
        последние = _соседи_sql(node_id, к["ребро"], с, per_ring, 0, db)
        out["кольца"].append({
            "связь": к["ребро"], "направление": к["напр"], "сколько": int(к["n"]),
            "свежее": _json(к["свежее"]), "старое": _json(к["старое"]),
            "последние": [{"id": s["id"], "вид": s["kind"], "заголовок": s["title"], "время": _json(s["ts"]),
                           "вес": _json(s["weight"]), "данные": s["payload"]}
                          for s in последние if s["ребро"] == к["ребро"] and s["напр"] == к["напр"]],
        })
    _след(db, candidate_id, f"node {node_id}" + (f" since={since}" if since else ""), len(кольца), {"id": node_id, "since": since}, t0)
    return out


@router.get("/neighbors")
def соседи(
    id: str = Query(..., description="узел, например company:SBER"),
    kind: Optional[str] = Query(None, description="вид связи: упоминает, о, держит, владеет…"),
    since: Optional[int] = Query(None, ge=1, le=3650),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    candidate_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    _who: str = Depends(_доступ),
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
                       "данные": s["payload"]} for s in строки]}
    _след(db, candidate_id, f"neighbors {id} kind={kind or '*'} since={since or '∞'}", out["всего"],
          {"id": id, "kind": kind, "since": since}, t0)
    return out


@router.get("/search")
def поиск(
    q: str = Query(..., min_length=2, max_length=120),
    kind: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    candidate_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    _who: str = Depends(_доступ),
):
    """Вход по слову: тикер → компания напрямую; иначе похожесть заголовка (pg_trgm) и подстрока."""
    t0 = time.time()
    if kind and kind not in _ВИДЫ_УЗЛОВ:
        raise HTTPException(400, f"вид узла: {', '.join(_ВИДЫ_УЗЛОВ)}")
    qq = q.strip()
    найдено = []
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
    _след(db, candidate_id, f"search «{qq}» kind={kind or '*'}", len(найдено), {"q": qq, "kind": kind}, t0)
    return {"запрос": qq, "найдено": найдено[:limit]}


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


@router.get("/path")
def путь(
    a: str = Query(...), b: str = Query(...),
    max_depth: int = Query(3, ge=1, le=4),
    candidate_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    _who: str = Depends(_доступ),
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
