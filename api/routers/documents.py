"""
Документы компаний для Routine-читателя и панели: /api/internal/documents/*

⚠️ АГЕНТ НЕ ЧИТАЕТ ОТЧЁТ ПОДРЯД. 68–170 страниц МСФО — это 40–120 тысяч токенов, и формат
у каждого эмитента свой. Поэтому ручки устроены как карта и лупа: сначала карта страниц
(знаков, таблиц, заголовок каждой), потом полнотекстовый поиск по якорям («чистая
прибыль», «сегмент», «прогноз») с фрагментами, и только затем страницы по диапазону —
не больше восьми за вызов. Формат может меняться сколько угодно: поиск идёт по словам,
а не по номерам страниц.

⚠️ ФАКТ БЕЗ СТРАНИЦЫ И ЦИТАТЫ НЕ ПРИНИМАЕТСЯ. Цифра из отчёта в посте должна иметь
опору, которую человек проверит за десять секунд. Числовые факты сверяются с карточкой
FM по тому же периоду: расхождение больше 2 % помечается mismatch — это провал
извлечения, а не «особенность документа».

Доступ: внутренний токен (Routine) или сессия администратора.
"""

import os
import re
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import get_current_user_optional

router = APIRouter(prefix="/api/internal/documents", tags=["documents"])

МАКС_СТРАНИЦ_ЗА_ВЫЗОВ = 8
_ПЕРИОД_FM = {"6m": "{y}H1", "9m": "{y}-9M", "y": "{y}", "q": "{y}Q{q}"}
# Поля схемы ↔ коды FM для сверки (значения FM в единицах отчёта эмитента, см. content_ai._SELECT_SCALE)
_ПОЛЕ_FM = {
    "net_profit": "earnings", "revenue": "revenue", "net_interest_income": "interest_net",
    "net_fee_income": "commission_net", "equity": "equity", "total_assets": "total_assets",
    "eps": "earnings_ps", "cash": "cash_and_equiv", "ebitda": "ebitda", "net_debt": "net_debt", "capex": "capex",
}


def _доступ(x_internal_token: str = Header(default=""),
            user: Optional[User] = Depends(get_current_user_optional)) -> str:
    ожидаемый = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if ожидаемый and x_internal_token == ожидаемый:
        return "agent"
    if user is not None and user.role == "admin":
        return "admin"
    raise HTTPException(status_code=403, detail="нужен internal token или сессия администратора")


def _версия(db: Session, doc_id: int):
    v = db.execute(text("""
        SELECT id, url, secid, doc_type, standard, period_code, year, month, file_name, pages, text_chars, tables, note,
               fetched_at, superseded_at
          FROM document_versions WHERE id = :id
    """), {"id": doc_id}).mappings().first()
    if v is None:
        raise HTTPException(404, "документа нет")
    return v


def _период_fm(v) -> Optional[str]:
    ш = _ПЕРИОД_FM.get(v["period_code"] or "")
    if not ш or not v["year"]:
        return None
    return ш.format(y=v["year"], q=max(1, (v["month"] or 3) // 3))


def _заголовок(t: Optional[str]) -> str:
    t = re.sub(r"\s+", " ", (t or "")).strip()
    return t[:90]


@router.get("/{doc_id}")
def карта(doc_id: int, db: Session = Depends(get_db), who: str = Depends(_доступ)):
    """Паспорт документа и карта страниц: сколько знаков и таблиц на каждой, первые слова."""
    v = _версия(db, doc_id)
    стр = db.execute(text("""
        SELECT page, length(COALESCE(text, '')) AS знаков, COALESCE(jsonb_array_length(tables), 0) AS таблиц, left(text, 120) AS начало
          FROM document_pages WHERE version_id = :id ORDER BY page
    """), {"id": doc_id}).mappings().all()
    return {
        "id": v["id"], "компания": v["secid"], "вид": v["doc_type"], "стандарт": v["standard"],
        "период": v["period_code"], "год": v["year"], "месяц": v["month"], "файл": v["file_name"],
        "страниц": v["pages"], "знаков": v["text_chars"], "таблиц": v["tables"], "заметка": v["note"],
        "период_fm": _период_fm(v), "устарел": v["superseded_at"] is not None,
        "карта": [{"стр": s["page"], "знаков": s["знаков"], "таблиц": s["таблиц"], "начало": _заголовок(s["начало"])} for s in стр],
        "как_читать": (f"Не читай подряд. Ищи якоря через /search?q=…, затем бери страницы по диапазону "
                       f"(/pages?from=&to=, не больше {МАКС_СТРАНИЦ_ЗА_ВЫЗОВ} за вызов). Числа сверяй с /fm."),
    }


@router.get("/{doc_id}/pages")
def страницы(doc_id: int, from_: int = Query(..., alias="from", ge=1), to: Optional[int] = Query(None, ge=1),
             db: Session = Depends(get_db), who: str = Depends(_доступ)):
    _версия(db, doc_id)
    to = to or from_
    if to < from_:
        raise HTTPException(400, "to < from")
    if to - from_ + 1 > МАКС_СТРАНИЦ_ЗА_ВЫЗОВ:
        raise HTTPException(400, f"не больше {МАКС_СТРАНИЦ_ЗА_ВЫЗОВ} страниц за вызов")
    rows = db.execute(text("""
        SELECT page, text, tables FROM document_pages WHERE version_id = :id AND page BETWEEN :a AND :b ORDER BY page
    """), {"id": doc_id, "a": from_, "b": to}).mappings().all()
    return {"id": doc_id, "страницы": [{"стр": r["page"], "текст": r["text"], "таблицы": r["tables"]} for r in rows]}


@router.get("/{doc_id}/search")
def поиск(doc_id: int, q: str = Query(..., min_length=2, max_length=200), limit: int = Query(10, ge=1, le=30),
          db: Session = Depends(get_db), who: str = Depends(_доступ)):
    """Страницы, где встречается фраза (русская морфология), с фрагментом вокруг совпадения."""
    _версия(db, doc_id)
    rows = db.execute(text("""
        SELECT page,
               ts_rank(to_tsvector('russian', COALESCE(text, '')), plainto_tsquery('russian', :q)) AS ранг,
               ts_headline('russian', COALESCE(text, ''), plainto_tsquery('russian', :q),
                           'MaxWords=40, MinWords=20, MaxFragments=2, FragmentDelimiter= … ') AS фрагмент,
               COALESCE(jsonb_array_length(tables), 0) AS таблиц
          FROM document_pages
         WHERE version_id = :id AND to_tsvector('russian', COALESCE(text, '')) @@ plainto_tsquery('russian', :q)
         ORDER BY ранг DESC, page LIMIT :limit
    """), {"id": doc_id, "q": q, "limit": limit}).mappings().all()
    return {"id": doc_id, "запрос": q, "найдено": [{"стр": r["page"], "ранг": round(float(r["ранг"]), 4),
                                                   "фрагмент": re.sub(r"\s+", " ", r["фрагмент"]), "таблиц": r["таблиц"]} for r in rows]}


@router.get("/{doc_id}/fm")
def карточка_fm(doc_id: int, db: Session = Depends(get_db), who: str = Depends(_доступ)):
    """Что карточка FinanceMarker знает о том же отчёте — для сверки извлечённых цифр.
    Значения в единицах отчёта эмитента (у Сбера — млн руб)."""
    v = _версия(db, doc_id)
    п = _период_fm(v)
    if not п or not v["standard"]:
        return {"id": doc_id, "период_fm": п, "показатели": [], "примечание": "период или стандарт не разобраны из имени файла"}
    rows = db.execute(text("""
        SELECT m.metric_code, r.label_ru, r.unit, m.value
          FROM company_metrics m LEFT JOIN metrics_ref r USING (metric_code)
         WHERE m.secid = :s AND m.standard = :std AND m.period_label = :p AND m.source = 'financemarker' AND m.value IS NOT NULL
         ORDER BY m.metric_code
    """), {"s": v["secid"], "std": v["standard"], "p": п}).mappings().all()
    return {"id": doc_id, "период_fm": п, "стандарт": v["standard"],
            "единицы": "как в отчёте эмитента (обычно млн руб; у Роснефти млрд, у Магнита тыс.)",
            "поля_схемы": _ПОЛЕ_FM,
            "показатели": [{"код": r["metric_code"], "подпись": r["label_ru"], "значение": r["value"]} for r in rows]}


@router.post("/{doc_id}/facts")
def записать_факты(doc_id: int, body: dict = Body(...), db: Session = Depends(get_db), who: str = Depends(_доступ)):
    """Результат чтения: {schema, model, summary, pages_read, candidate_id, facts:[{field, value, unit, page, quote, confidence}]}.
    Факт без страницы или без цитаты отклоняется. Числа сверяются с FM по тому же периоду."""
    v = _версия(db, doc_id)
    факты = body.get("facts") or []
    if not isinstance(факты, list):
        raise HTTPException(400, "facts должен быть списком")
    плохие = [f for f in факты if not isinstance(f, dict) or not f.get("field") or not f.get("page") or not (f.get("quote") or "").strip()]
    if плохие:
        raise HTTPException(400, f"у {len(плохие)} фактов нет поля, страницы или цитаты — такие не принимаются")
    fm = {}
    п = _период_fm(v)
    if п and v["standard"]:
        fm = {r[0]: float(r[1]) for r in db.execute(text("""
            SELECT metric_code, value FROM company_metrics
             WHERE secid = :s AND standard = :std AND period_label = :p AND source = 'financemarker' AND value IS NOT NULL
        """), {"s": v["secid"], "std": v["standard"], "p": п}).all()}
    read_id = db.execute(text("""
        INSERT INTO document_reads (version_id, schema_name, model, summary, candidate_id, pages_read)
        VALUES (:v, :schema, :model, CAST(:summary AS jsonb), :cid, :pages)
        RETURNING id
    """), {"v": doc_id, "schema": str(body.get("schema") or "unknown")[:40], "model": str(body.get("model") or "")[:80] or None,
           "summary": __import__("json").dumps(body.get("summary") or {}, ensure_ascii=False),
           "cid": body.get("candidate_id"), "pages": body.get("pages_read")}).scalar()
    расхождений = 0
    итог = []
    for f in факты:
        value = f.get("value")
        num = None
        if isinstance(value, (int, float)):
            num = float(value)
        elif isinstance(value, str):
            try:
                num = float(value.replace(" ", "").replace(",", "."))
            except ValueError:
                num = None
        fm_code = _ПОЛЕ_FM.get(f["field"])
        fm_val = fm.get(fm_code) if fm_code else None
        mismatch = None
        if num is not None and fm_val is not None:
            mismatch = abs(num - fm_val) > 0.02 * max(abs(fm_val), 1e-9)
            расхождений += int(mismatch)
        db.execute(text("""
            INSERT INTO document_facts (read_id, version_id, field, value_num, value_text, unit, page, quote, fm_value, mismatch, confidence)
            VALUES (:r, :v, :field, :num, :txt, :unit, :page, :quote, :fm, :mm, :conf)
        """), {"r": read_id, "v": doc_id, "field": str(f["field"])[:80], "num": num,
               "txt": None if num is not None else (str(value)[:2000] if value is not None else None),
               "unit": (str(f.get("unit") or "")[:24] or None), "page": int(f["page"]), "quote": str(f["quote"])[:1500],
               "fm": fm_val, "mm": mismatch, "conf": (str(f.get("confidence") or "")[:8] or None)})
        итог.append({"field": f["field"], "fm": fm_val, "mismatch": mismatch})
    db.execute(text("UPDATE document_reads SET facts_count = :n, mismatches = :m WHERE id = :id"),
               {"n": len(факты), "m": расхождений, "id": read_id})
    db.commit()
    return {"read_id": read_id, "фактов": len(факты), "расхождений_с_fm": расхождений, "сверка": итог}


@router.get("/{doc_id}/facts")
def факты(doc_id: int, db: Session = Depends(get_db), who: str = Depends(_доступ)):
    _версия(db, doc_id)
    чтения = db.execute(text("""
        SELECT id, schema_name, model, summary, facts_count, mismatches, pages_read, created_at
          FROM document_reads WHERE version_id = :id ORDER BY id DESC
    """), {"id": doc_id}).mappings().all()
    факты = db.execute(text("""
        SELECT read_id, field, value_num, value_text, unit, page, quote, fm_value, mismatch, confidence
          FROM document_facts WHERE version_id = :id ORDER BY read_id DESC, id
    """), {"id": doc_id}).mappings().all()
    return {"id": doc_id, "чтения": [dict(r) for r in чтения], "факты": [dict(f) for f in факты]}


@router.get("")
def список(secid: Optional[str] = Query(None), limit: int = Query(50, ge=1, le=500),
           db: Session = Depends(get_db), who: str = Depends(_доступ)):
    rows = db.execute(text("""
        SELECT v.id, v.secid, v.doc_type, v.standard, v.period_code, v.year, v.month, v.pages, v.text_chars, v.note, v.fetched_at,
               (SELECT COUNT(*) FROM document_reads r WHERE r.version_id = v.id) AS чтений
          FROM document_versions v
         WHERE v.superseded_at IS NULL AND (CAST(:s AS text) IS NULL OR v.secid = CAST(:s AS text))
         ORDER BY v.year DESC NULLS LAST, v.month DESC NULLS LAST, v.secid LIMIT :limit
    """), {"s": secid, "limit": limit}).mappings().all()
    return {"документы": [dict(r) for r in rows]}
