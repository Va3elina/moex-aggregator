"""Внутренний эндпоинт второго мозга: что было верно на дату.

Отвечает на вопрос, которого не хватало генератору. Черновик 700 писал «что именно
спровоцировало пробой — в переданных данных не указано», хотя вокруг той даты в
архиве лежали запрет Китая на экспорт гелия, переговоры Россия-Иран по газу, газ в
Европе выше $600 и атака БПЛА на промзону Салавата. Модель не «не смогла
объяснить» — ей не показали мир.

Отдаёт два разных по природе слоя, и смешивать их нельзя:
  • facts — структурные факты из своей БД (ставка, M2, ВВП) с точными интервалами
    действия. Им можно верить как есть;
  • news_context — заголовки новостей вокруг даты. Это НЕ факты, а поводы: они не
    проверены, могут противоречить друг другу и служат только фоном.

⚠️ ОТСЕЧКА ПО ВРЕМЕНИ ОБЯЗАТЕЛЬНА В ОБОИХ СЛОЯХ. Ровно на её отсутствии сломался
черновик 845: сигнал ОИ был датирован на три дня ПОЗЖЕ новости, а пост утверждал
«толпа шла в шорт ещё до ралли». Ничего с датой позже as_of наружу не уходит.
"""
import os
from datetime import date, timedelta

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db

internal_router = APIRouter(prefix="/api/internal/world-facts",
                            tags=["internal-world-facts"])


def _require_internal_token(x_internal_token: str = Header(default="")) -> None:
    expected = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=403, detail="Неверный или отсутствующий internal token")


_FACTS = text("""
    SELECT statement, kind, entities, valid_from, valid_until, source, confidence
    FROM world_facts
    WHERE valid_from <= :as_of
      AND (valid_until IS NULL OR valid_until >= :as_of)
      AND (CAST(:kinds AS text[]) IS NULL OR kind = ANY(CAST(:kinds AS text[])))
    ORDER BY kind, valid_from DESC
""")

# ⚠️ Верхняя граница ЖЁСТКАЯ: новость позже as_of в бриф попасть не должна ни при
# каких параметрах окна. Касты ::text[] обязательны — без них драйвер не выведет
# тип у NULL-параметра и запрос упадёт на пустом фильтре сущностей.
_NEWS = text("""
    SELECT posted_at, channel, tickers,
           left(replace(text, chr(10), ' '), 200) AS headline
    FROM news_archive
    WHERE posted_at >= :as_of_start
      AND posted_at <  :as_of_end
      AND (CAST(:entities AS text[]) IS NULL OR tickers && CAST(:entities AS text[]))
    ORDER BY posted_at DESC
    LIMIT :limit_news
""")


@internal_router.get("", dependencies=[Depends(_require_internal_token)])
def world_facts(
    as_of: date = Query(..., description="дата новости — ничего позже неё не вернётся"),
    entities: str = Query("", description="тикеры через запятую: GAZP,VKCO"),
    kinds: str = Query("", description="виды фактов через запятую: ключевая ставка,ввп"),
    window_days: int = Query(7, ge=1, le=90,
                              description="за сколько дней ДО as_of брать новостной фон"),
    limit_news: int = Query(12, ge=0, le=50),
    db: Session = Depends(get_db),
):
    ent = [e.strip().upper() for e in entities.split(",") if e.strip()] or None
    knd = [k.strip() for k in kinds.split(",") if k.strip()] or None

    facts = db.execute(_FACTS, {"as_of": as_of, "kinds": knd}).mappings().all()

    news = []
    if limit_news:
        start = as_of - timedelta(days=window_days)
        news = db.execute(_NEWS, {
            "as_of_start": start,
            # Конец дня as_of: posted_at это timestamptz, а as_of — дата. Без этого
            # новости самого дня события отсеклись бы целиком.
            "as_of_end": as_of + timedelta(days=1),
            "entities": ent, "limit_news": limit_news,
        }).mappings().all()

    return {
        "as_of": str(as_of),
        "facts": [{
            "факт": f["statement"], "вид": f["kind"],
            "действует_с": str(f["valid_from"]),
            "действует_по": str(f["valid_until"]) if f["valid_until"] else "по сей день",
            "источник": f["source"], "уверенность": float(f["confidence"]),
        } for f in facts],
        "news_context": [{
            "когда": str(n["posted_at"])[:16], "канал": n["channel"],
            "заголовок": n["headline"], "тикеры": n["tickers"],
        } for n in news],
        "note": ("facts — проверенные факты из собственной БД, им можно верить. "
                  "news_context — НЕ факты, а информационный фон: заголовки не "
                  "проверены и могут противоречить друг другу. Ничего с датой позже "
                  f"{as_of} здесь нет и быть не может — это гарантия запроса, а не "
                  "обещание. Не утверждайте причинность на основании соседства по "
                  "времени."),
    }
