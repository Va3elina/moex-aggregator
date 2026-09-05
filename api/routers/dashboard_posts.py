"""
Разбор кандидата в посты: /api/admin/dashboard/posts/*

⚠️ СМОТРИМ И ОПУБЛИКОВАННЫЕ, И ОТКАЗЫ. Опубликованных всего пять, и все они
сделаны до появления судьи — у них пустой вердикт и нет поабзацного разбора.
Соблазн показывать только те кандидаты, где след полный, велик, но тогда экран
теряет главное: из 1163 кандидатов 1069 отсеяны, и вопрос «почему именно так»
задаётся как раз к ним. Поэтому список отдаёт все статусы, а карточка честно
пишет, чего в следе нет, вместо пустого блока.

⚠️ СВЯЗЬ С НОВОСТЬЮ — ЧЕРЕЗ РАЗБОР URL, А НЕ ПО КЛЮЧУ. Формального внешнего
ключа на news_archive нет: у кандидата лежит только source_url вида
`https://t.me/newssmartlab/130070`. Пара (канал, id сообщения) совпадает с
первичным ключом архива, поэтому разбираем URL здесь, а не заводим join по
тексту. Как появится колонка — заменить.

⚠️ ДВА ИМЕНИ ОДНОГО КАНАЛА. Исторический экспорт лежит под `MarketTwits` и
`СМАРТЛАБ НОВОСТИ`, живой ингест — под `markettwits` и `newssmartlab`. Кандидаты
ссылаются на живые имена, поэтому здесь совпадёт; но любой фильтр по каналу
обязан помнить про обе формы.

Только чтение, только админ.
"""

import re
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/dashboard/posts", tags=["admin-dashboard"])

# Путь кандидата: восемь машинных статусов сворачиваются в пять этапов воронки.
# ⚠️ Порядок здесь — не рейтинг по величине, а путь: сначала вход, потом потери.
ЭТАПЫ = [
    ("pending", "ждут разбора"),
    ("candidate", "только пришли"),
    ("no_data", "нечем подтвердить"),
    ("discarded", "отсеяны судьёй релевантности"),
    ("rejected", "отклонены человеком"),
    ("draft_ready", "черновик готов"),
    ("in_review", "на ревью"),
    ("published", "опубликованы"),
]
ПОДПИСЬ_СТАТУСА = dict(ЭТАПЫ)

_URL_ПОСТА = re.compile(r"t\.me/(?:s/)?([A-Za-z0-9_]+)/(\d+)")


def _разобрать_url(url: str | None):
    """(канал, id сообщения) из ссылки на пост, либо (None, None)."""
    if not url:
        return None, None
    m = _URL_ПОСТА.search(url)
    return (m.group(1), int(m.group(2))) if m else (None, None)


def _возраст(t, now):
    if t is None:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return round((now - t).total_seconds(), 1)


@router.get("")
def список(
    status: str | None = Query(None, description="фильтр по статусу"),
    ticker: str | None = Query(None, description="тикер в списке кандидата"),
    verdict: str | None = Query(None, description="вердикт судьи: годится/спорно/брак"),
    q: str | None = Query(None, description="поиск по заголовку"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Список кандидатов с фильтрами + разбивка по статусам."""
    условия, параметры = ["1 = 1"], {"limit": limit, "offset": offset}
    if status:
        условия.append("c.status = :status")
        параметры["status"] = status
    if ticker:
        # Пересечение массивов, а не ANY: под tickers лежит GIN, и обратиться к
        # нему может только `&&`. Проверено: на нынешних 1163 строках планировщик
        # всё равно выбирает полный проход — таблица слишком мала, чтобы индекс
        # окупился. Оператор оставлен правильный, чтобы он заработал сам, когда
        # кандидатов станет на порядок больше.
        условия.append("c.tickers && ARRAY[:ticker]::text[]")
        параметры["ticker"] = ticker.upper()
    if verdict:
        условия.append("c.judge_verdict = :verdict")
        параметры["verdict"] = verdict
    if q:
        условия.append("c.headline ILIKE :q")
        параметры["q"] = f"%{q}%"
    где = " AND ".join(условия)

    строки = db.execute(text(f"""
        SELECT c.id, c.status, c.source, c.headline, c.tickers, c.futures_ticker,
               c.event_type, c.importance_1_5, c.judge_verdict, c.brief_version,
               c.created_at, c.published_at, c.matched_anomaly_id,
               (c.draft_text IS NOT NULL)      AS есть_черновик,
               (c.judge_paragraphs IS NOT NULL) AS есть_разбор,
               (SELECT COUNT(*) FROM agent_trace t WHERE t.candidate_id = c.id) AS шагов
          FROM content_candidates c
         WHERE {где}
         ORDER BY c.created_at DESC
         LIMIT :limit OFFSET :offset
    """), параметры).mappings().all()

    по_статусам = {r[0]: r[1] for r in db.execute(text(
        "SELECT status, COUNT(*) FROM content_candidates GROUP BY status")).all()}

    всего = db.execute(text(f"""
        SELECT COUNT(*) FROM content_candidates c WHERE {где}
    """), {k: v for k, v in параметры.items() if k not in ("limit", "offset")}).scalar()

    return {
        "всего_по_фильтру": всего,
        "по_статусам": по_статусам,
        "этапы": [{"код": к, "подпись": п, "сколько": по_статусам.get(к, 0)}
                  for к, п in ЭТАПЫ],
        "кандидаты": [{
            "id": r["id"],
            "статус": r["status"],
            "статус_подпись": ПОДПИСЬ_СТАТУСА.get(r["status"], r["status"]),
            "источник": r["source"],
            "заголовок": r["headline"],
            "тикеры": list(r["tickers"] or []),
            "фьючерс": r["futures_ticker"],
            "тип_события": r["event_type"],
            "важность": r["importance_1_5"],
            "вердикт": r["judge_verdict"],
            "версия_брифа": r["brief_version"],
            "создан": r["created_at"].isoformat() if r["created_at"] else None,
            "опубликован": r["published_at"].isoformat() if r["published_at"] else None,
            "есть_аномалия": r["matched_anomaly_id"] is not None,
            "есть_черновик": r["есть_черновик"],
            "есть_разбор": r["есть_разбор"],
            "шагов_следа": r["шагов"],
        } for r in строки],
    }


@router.get("/{candidate_id}")
def карточка(
    candidate_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Полный путь кандидата: новость → судья релевантности → данные → черновик → судья → человек."""
    now = datetime.now(timezone.utc)

    c = db.execute(text("""
        SELECT * FROM content_candidates WHERE id = :id
    """), {"id": candidate_id}).mappings().first()
    if c is None:
        raise HTTPException(status_code=404, detail="кандидат не найден")

    # ── исходная новость и разгон репостов
    канал, mid = _разобрать_url(c["source_url"])
    новость = разгон = None
    if канал and mid:
        новость = db.execute(text("""
            SELECT channel, message_id, posted_at, text, views, tickers, hashtags
              FROM news_archive WHERE channel = :ch AND message_id = :mid
        """), {"ch": канал, "mid": mid}).mappings().first()
        разгон = db.execute(text("""
            SELECT fwd_3, fwd_5, fwd_6, fwd_7, fwd_8, fwd_15, fwd_90, promoted
              FROM tg_channel_watch WHERE channel = :ch AND message_id = :mid
        """), {"ch": канал, "mid": mid}).mappings().first()

    # ── аномалия, на которой стоит пост
    аномалия = None
    if c["matched_anomaly_id"]:
        аномалия = db.execute(text("""
            SELECT id, type, asset_id, asset_name, clgroup, direction,
                   headline, context, severity_value, signal_date, deep_link
              FROM anomalies WHERE id = :id
        """), {"id": c["matched_anomaly_id"]}).mappings().first()

    # ── что агент спрашивал у базы
    след = db.execute(text("""
        SELECT step, seq, source, question, result_count, result_note,
               outcome, outcome_reason, duration_ms, created_at
          FROM agent_trace WHERE candidate_id = :id ORDER BY seq, id
    """), {"id": candidate_id}).mappings().all()

    # ── решения человека и правки судьи
    журнал = db.execute(text("""
        SELECT event, reason_code, reason_text, judge_verdict, created_at
          FROM content_feedback WHERE candidate_id = :id ORDER BY created_at
    """), {"id": candidate_id}).mappings().all()

    # ⚠️ Чего в следе НЕТ — говорим прямо. Пустой блок читается как «всё хорошо,
    # просто ничего не было», а на деле у постов до 01.09 судьи ещё не существовало,
    # и снимок брифа не сохраняется вовсе. Молчать об этом — врать умолчанием.
    пробелы = []
    if not след:
        пробелы.append("Следа агента нет: он пишется с 04.09.2026, более ранние "
                       "кандидаты проходили конвейер до его появления")
    if c["judge_verdict"] is None and c["draft_text"]:
        пробелы.append("Судья черновика не выносил вердикт: он появился 31.08.2026, "
                       "а этот черновик написан раньше")
    if c["draft_text"] and not c["annotation"]:
        пробелы.append("Аннотации с числами нет — она появилась позже")
    пробелы.append("Снимок брифа не сохраняется: числа, на которых стоял пост "
                   "(позиции физлиц, цена), собирались на лету и не остались нигде")

    return {
        "кандидат": {
            "id": c["id"],
            "статус": c["status"],
            "статус_подпись": ПОДПИСЬ_СТАТУСА.get(c["status"], c["status"]),
            "источник": c["source"],
            "ссылка": c["source_url"],
            "заголовок": c["headline"],
            "текст_новости": c["raw_text"],
            "тикеры": list(c["tickers"] or []),
            "фьючерс": c["futures_ticker"],
            "тип_события": c["event_type"],
            "важность": c["importance_1_5"],
            "версия_брифа": c["brief_version"],
            "тред": c["thread_key"],
            "родитель": c["parent_candidate_id"],
            "создан": c["created_at"].isoformat() if c["created_at"] else None,
            "опубликован": c["published_at"].isoformat() if c["published_at"] else None,
            "возраст_сек": _возраст(c["created_at"], now),
        },
        "шаг_н_хайп": {
            "решение": c["hype_filter_result"],
            "когда": c["hype_filter_checked_at"].isoformat() if c["hype_filter_checked_at"] else None,
            "сдался": c["hype_filter_gave_up_at"] is not None,
        },
        "шаг_а_релевантность": {
            "обоснование": c["reasoning"],
            "истекает": c["pending_expires_at"].isoformat() if c["pending_expires_at"] else None,
        },
        "шаг_б_данные": {
            "проверен": c["step_b_checked_at"].isoformat() if c["step_b_checked_at"] else None,
            "аномалия": dict(аномалия) | {
                "signal_date": аномалия["signal_date"].isoformat() if аномалия["signal_date"] else None,
                "severity_value": (float(аномалия["severity_value"])
                                   if аномалия["severity_value"] is not None else None),
            } if аномалия else None,
        },
        "шаг_в_черновик": {
            "текст": c["draft_text"],
            "текст_ии": c["draft_text_ai"],
            "правил_человек": bool(c["draft_text"] and c["draft_text_ai"]
                                   and c["draft_text"] != c["draft_text_ai"]),
            "отказ": c["synth_declined_reason"],
            "аннотация": c["annotation"],
            "профиль_стиля": c["style_profile"],
        },
        "шаг_г_судья": {
            "вердикт": c["judge_verdict"],
            "провалено": list(c["judge_failed"] or []),
            "замечания": list(c["judge_defects"] or []),
            "заметка": c["judge_note"],
            "пункты": c["judge_items"],
            "абзацы": c["judge_paragraphs"],
            "правил": c["judge_fixed_at"].isoformat() if c["judge_fixed_at"] else None,
            "что_поправил": c["judge_fix_note"],
            "когда": c["judge_checked_at"].isoformat() if c["judge_checked_at"] else None,
        },
        "человек": {
            "решение": c["reviewer_action"],
            "причина": c["reviewer_reason"],
            "код_причины": c["reviewer_reason_code"],
            "уведомлён": c["reviewer_notified_at"].isoformat() if c["reviewer_notified_at"] else None,
        },
        "новость": (dict(новость) | {
            "posted_at": новость["posted_at"].isoformat() if новость["posted_at"] else None,
            "tickers": list(новость["tickers"] or []),
            "hashtags": list(новость["hashtags"] or []),
        }) if новость else None,
        "разгон_репостов": (
            [{"минута": м, "репостов": разгон[f"fwd_{м}"]}
             for м in (3, 5, 6, 7, 8, 15, 90) if разгон[f"fwd_{м}"] is not None]
            if разгон else []
        ),
        "признан_хайпом": bool(разгон["promoted"]) if разгон else None,
        "мозг": [{
            "шаг": t["step"], "номер": t["seq"], "источник": t["source"], "вопрос": t["question"],
            "нашлось": t["result_count"], "результат": t["result_note"], "исход": t["outcome"],
            "почему": t["outcome_reason"], "мс": t["duration_ms"],
        } for t in след if t["step"] == "мозг"],
        "след": [{
            "шаг": t["step"],
            "номер": t["seq"],
            "источник": t["source"],
            "вопрос": t["question"],
            "нашлось": t["result_count"],
            "результат": t["result_note"],
            "исход": t["outcome"],
            "почему": t["outcome_reason"],
            "мс": t["duration_ms"],
        } for t in след if t["step"] != "мозг"],
        "журнал": [{
            "событие": j["event"],
            "код": j["reason_code"],
            "причина": j["reason_text"],
            "вердикт_тогда": j["judge_verdict"],
            "когда": j["created_at"].isoformat() if j["created_at"] else None,
        } for j in журнал],
        "чего_нет": пробелы,
    }
