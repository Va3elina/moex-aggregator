"""
Граф владения для админской панели: /api/admin/dashboard/graph

⚠️ ЭТО АРХИПЕЛАГ, А НЕ ПАУТИНА. 28 компаний, 23 ребра, семь компонент связности,
максимальная степень 4. Один силовой клубок на таких данных рисует пыль по холсту
и каждый раз по-разному. Поэтому ручка отдаёт компоненты (кластеры) явно — экран
раскладывает каждый кластер отдельно и детерминированно.

⚠️ НАПРАВЛЕНИЕ — ИЗ КЛЮЧА ФАКТА, ДОЛЯ — ИЗ ТЕКСТА. Отдельных колонок владелец/цель/
доля в world_facts нет (миграции 057/061/063 сознательно держат таблицу плоской).
Направление надёжно кодирует fact_key: `own:ВЛАДЕЛЕЦ:ЦЕЛЬ`, `link:A:B` — той же
формы. Доля живёт прозой в statement; здесь она вытаскивается регуляркой ТОЛЬКО для
показа в панели с пометкой «из текста факта» — толщину ребра по ней не строим:
в одной из миграций прямо записано «точная доля в базе не зафиксирована».

⚠️ КАЗНАЧЕЙСКИЙ ПАКЕТ — АТРИБУТ УЗЛА, НЕ РЕБРО. Компания владеет собой; петля к
себе была бы неправдой. Отдаётся полем узла, экран рисует кольцом.

Только чтение, только админ.
"""

import re
from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/dashboard/graph", tags=["admin-dashboard"])

# ⚠️ ТОЛЬКО ФОРМУЛИРОВКА «ДОЛЕЙ N%» / «ВЛАДЕЕТ … N%». Общий поиск «N%» у ребра
# AFKS → SGZH выхватил «12%» — это было движение цены из новости, не доля.
# Для рёбер из новостей (link:*) долю не извлекаем вовсе: там проза, не структура.
_ДОЛЯ = re.compile(r"(?:дол[ея][йю]?|владеет[^.%]{0,60}?|пакет[^.%]{0,30}?)\s*(\d{1,3}(?:[.,]\d{1,2})?)\s*%", re.I)
_КЛЮЧ = re.compile(r"^(own|link):([A-Z0-9_]+):([A-Z0-9_]+)$")


def _доля_из_текста(statement: str | None):
    if not statement:
        return None
    m = _ДОЛЯ.search(statement)
    return float(m.group(1).replace(",", ".")) if m else None


def _возраст_дней(d: date | None, today: date):
    return (today - d).days if d else None


def _компоненты(узлы: list[str], рёбра: list[tuple[str, str]]) -> dict[str, int]:
    """Union-find: номер компоненты для каждого узла. Нумерация по размеру, крупные первыми."""
    родитель = {u: u for u in узлы}

    def корень(x):
        while родитель[x] != x:
            родитель[x] = родитель[родитель[x]]
            x = родитель[x]
        return x

    for a, b in рёбра:
        ra, rb = корень(a), корень(b)
        if ra != rb:
            родитель[rb] = ra
    группы: dict[str, list[str]] = {}
    for u in узлы:
        группы.setdefault(корень(u), []).append(u)
    порядок = sorted(группы.values(), key=lambda g: (-len(g), sorted(g)[0]))
    return {u: i for i, g in enumerate(порядок) for u in g}


@router.get("")
def граф(
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    today = date.today()

    связи = db.execute(text("""
        SELECT id, fact_key, statement, entities, valid_from, valid_until,
               source, source_url, confidence
          FROM world_facts
         WHERE kind = 'связь' AND valid_until IS NULL AND superseded_by IS NULL
         ORDER BY id
    """)).mappings().all()

    казна = db.execute(text("""
        SELECT fact_key, statement, entities, valid_from, confidence
          FROM world_facts
         WHERE kind = 'казначейский пакет' AND valid_until IS NULL AND superseded_by IS NULL
    """)).mappings().all()

    рёбра = []
    тикеры: set[str] = set()
    for r in связи:
        m = _КЛЮЧ.match(r["fact_key"] or "")
        ent = list(r["entities"] or [])
        if m:
            владелец, цель, вид = m.group(2), m.group(3), m.group(1)
        elif len(ent) == 2:
            # Направление не закодировано — рисуем без стрелки, честно помечаем.
            владелец, цель, вид = ent[0], ent[1], "без_направления"
        else:
            continue
        тикеры.update((владелец, цель))
        st = r["statement"] or ""
        рёбра.append({
            "id": r["id"],
            "от": владелец,
            "к": цель,
            "вид": вид,
            "косвенно": "косвенн" in st.lower(),
            "спорно": "СПОРНО" in st,
            "доля_из_текста": _доля_из_текста(st) if вид == "own" else None,
            "снимок": r["valid_from"].isoformat() if r["valid_from"] else None,
            "снимку_дней": _возраст_дней(r["valid_from"], today),
            "уверенность": float(r["confidence"]) if r["confidence"] is not None else None,
            "источник": r["source"],
            "ссылка": r["source_url"],
            "текст": st,
        })

    пакеты: dict[str, dict] = {}
    for r in казна:
        ent = list(r["entities"] or [])
        if not ent:
            continue
        t = ent[0]
        пакеты[t] = {
            "доля_из_текста": _доля_из_текста(r["statement"]),
            "снимок": r["valid_from"].isoformat() if r["valid_from"] else None,
            "снимку_дней": _возраст_дней(r["valid_from"], today),
            "текст": r["statement"],
        }
        тикеры.add(t)

    # Имена и сектора — через справочник эмитентов; чего нет в справочнике (GPB),
    # остаётся голым тикером, и это видно.
    имена = {row["secid"]: dict(row) for row in db.execute(text("""
        SELECT s.secid, i.name_short, i.sector
          FROM issuer_securities s JOIN issuers i ON i.issuer_id = s.issuer_id
         WHERE s.secid = ANY(:t)
    """), {"t": sorted(тикеры)}).mappings()}

    компонента = _компоненты(sorted(тикеры), [(e["от"], e["к"]) for e in рёбра])
    степень: dict[str, int] = {t: 0 for t in тикеры}
    for e in рёбра:
        степень[e["от"]] += 1
        степень[e["к"]] += 1

    узлы = [{
        "тикер": t,
        "имя": имена.get(t, {}).get("name_short") or t,
        "сектор": имена.get(t, {}).get("sector"),
        "в_справочнике": t in имена,
        "компонента": компонента[t],
        "степень": степень[t],
        "казначейский": пакеты.get(t),
    } for t in sorted(тикеры)]

    размеры: dict[int, int] = {}
    for u in узлы:
        размеры[u["компонента"]] = размеры.get(u["компонента"], 0) + 1

    return {
        "узлы": узлы,
        "рёбра": рёбра,
        "компоненты": [{"номер": k, "узлов": v} for k, v in sorted(размеры.items())],
        "итого": {
            "узлов": len(узлы), "рёбер": len(рёбра), "казначейских": len(пакеты),
            "рёбер_без_направления": sum(1 for e in рёбра if e["вид"] == "без_направления"),
            "рёбер_старше_2лет": sum(1 for e in рёбра if (e["снимку_дней"] or 0) > 730),
        },
    }
