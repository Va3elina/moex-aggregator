"""
/api/admin/content-candidates/* — админ-Kanban состояний content-пайплайна
(«Новости», страница /admin/content-news на фронте).
/api/internal/content-news/* — callback для ИИ-шагов (Routine/прямой API), см.
блок в конце файла.

Пайплайн (сбор — отдельные скрипты, здесь только состояния + приёмка результата ИИ):
  Календарь MOEX (signals/moex_calendar_scan.py) + TG-каналы-хайп (signals/tg_hype_scan.py:
  markettwits, newssmartlab) → Шаг А (ИИ: релевантность/значимость, пишет сюда через
  /api/internal/.../step-a)
  → Шаг Б (signals/content_match.py: сверка с anomalies, БЕЗ ИИ) → Шаг В (ИИ:
  синтез черновика, пишет сюда через /api/internal/.../step-c) → ревью в
  Telegram-боте → публикация. Публикация с кнопками ревью — тоже отдельная задача.

  GET   /api/admin/content-candidates       — карточки одной колонки (пагинация)
  GET   /api/admin/content-candidates/{id}  — полная карточка + связанный тред
  PATCH /api/admin/content-candidates/{id}  — ручная смена статуса (валидация
                                               переходов по state machine ниже)

Админ-эндпоинты — require_admin (как в analytics.py). Таблица —
content_candidates (db/migrations/022_content_candidates.sql) — там же полное
описание state machine статусов.
"""
import difflib
import html
import json
import os
import re
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import User
from api.routers.auth import require_admin

router = APIRouter(prefix="/api/admin/content-candidates", tags=["admin-content"])

# Порядок = порядок колонок Kanban-доски на фронте.
STATUS_VALUES = (
    "candidate", "discarded", "pending", "draft_ready",
    "no_data", "in_review", "published", "rejected",
)

_COLUMNS = """
  id, status, source, headline, tickers, futures_ticker,
  event_type, importance_1_5, reasoning, matched_anomaly_id, thread_key,
  parent_candidate_id, forwards_count, created_at, updated_at, pending_expires_at,
  published_at, reviewer_action
"""


class ContentCandidateOut(BaseModel):
    id: int
    status: str
    source: Optional[str] = None
    headline: str
    tickers: list[str] = []
    futures_ticker: Optional[str] = None
    event_type: Optional[str] = None
    importance_1_5: Optional[int] = None
    reasoning: Optional[str] = None
    matched_anomaly_id: Optional[int] = None
    thread_key: Optional[str] = None
    parent_candidate_id: Optional[int] = None
    forwards_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    pending_expires_at: Optional[str] = None
    published_at: Optional[str] = None
    reviewer_action: Optional[str] = None


class ContentCandidateListOut(BaseModel):
    items: list[ContentCandidateOut]
    total: int
    limit: int
    offset: int


class ContentCandidateDetailOut(ContentCandidateOut):
    raw_text: Optional[str] = None
    draft_text: Optional[str] = None
    synth_declined_reason: Optional[str] = None
    reviewer_id: Optional[int] = None
    thread: list[ContentCandidateOut] = []  # остальные кандидаты того же thread_key


def _row_to_out(r) -> ContentCandidateOut:
    return ContentCandidateOut(
        id=r["id"], status=r["status"], source=r["source"], headline=r["headline"],
        tickers=list(r["tickers"] or []), futures_ticker=r["futures_ticker"],
        event_type=r["event_type"], importance_1_5=r["importance_1_5"],
        reasoning=r["reasoning"], matched_anomaly_id=r["matched_anomaly_id"],
        thread_key=r["thread_key"], parent_candidate_id=r["parent_candidate_id"],
        forwards_count=r["forwards_count"],
        created_at=r["created_at"].isoformat() if r["created_at"] else None,
        updated_at=r["updated_at"].isoformat() if r["updated_at"] else None,
        pending_expires_at=r["pending_expires_at"].isoformat() if r["pending_expires_at"] else None,
        published_at=r["published_at"].isoformat() if r["published_at"] else None,
        reviewer_action=r["reviewer_action"],
    )


@router.get("", response_model=ContentCandidateListOut)
def list_candidates(
    status: str = Query(..., description="одна из колонок доски — см. STATUS_VALUES"),
    source: Optional[str] = Query(None, description="фильтр по источнику (markettwits/newssmartlab/moex_calendar)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Карточки одной колонки Kanban, свежие сначала. Фронт зовёт это по разу
    на каждую из 8 колонок (без общего barrier-запроса — колонки independent).
    source — необязательный фильтр (переключатель источника на доске), не путать
    со status: колонка = status, вкладка над доской = source."""
    if status not in STATUS_VALUES:
        raise HTTPException(status_code=422, detail=f"Неизвестный статус: {status}")

    where_source = "AND source = :source" if source else ""
    params = {"status": status, "limit": limit, "offset": offset}
    if source:
        params["source"] = source

    total = db.execute(
        text(f"SELECT count(*) FROM content_candidates WHERE status = :status {where_source}"),
        params,
    ).scalar() or 0

    rows = db.execute(
        text(f"""
            SELECT {_COLUMNS} FROM content_candidates
            WHERE status = :status {where_source}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).mappings().all()

    return ContentCandidateListOut(
        items=[_row_to_out(r) for r in rows], total=total, limit=limit, offset=offset,
    )


class SourceStatsOut(BaseModel):
    source: str
    total: int
    by_status: dict[str, int]


@router.get("/stats/by-source", response_model=list[SourceStatsOut])
def stats_by_source(db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    """Статистика по источникам (markettwits/newssmartlab/moex_calendar) — сколько
    новостей каждый источник поставил и на какой стадии они сейчас. Источники
    считаются НЕЗАВИСИМО (одна и та же новость, пойманная и MarketTwits, и
    Smartlab, — это ДВЕ разные строки content_candidates, дубли не схлопываются
    здесь намеренно, чтобы видеть вклад каждого канала отдельно)."""
    rows = db.execute(text("""
        SELECT COALESCE(source, '(без источника)') AS source, status, count(*) AS cnt
        FROM content_candidates
        GROUP BY source, status
    """)).mappings().all()

    by_source: dict[str, dict[str, int]] = {}
    for r in rows:
        by_source.setdefault(r["source"], {})[r["status"]] = r["cnt"]

    return [
        SourceStatsOut(source=src, total=sum(counts.values()), by_status=counts)
        for src, counts in sorted(by_source.items(), key=lambda kv: -sum(kv[1].values()))
    ]


@router.get("/{candidate_id}", response_model=ContentCandidateDetailOut)
def get_candidate(
    candidate_id: int,
    db: Session = Depends(get_db),
    _admin: User = Depends(require_admin),
):
    """Полная карточка (клик по card → модалка) + остальные кандидаты того же
    треда (thread_key), отсортированные по времени — для связки повторных
    сигналов по одному тикеру в UI."""
    row = db.execute(
        text(f"""
            SELECT {_COLUMNS}, raw_text, draft_text, synth_declined_reason, reviewer_id
            FROM content_candidates WHERE id = :id
        """),
        {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")

    thread: list[ContentCandidateOut] = []
    if row["thread_key"]:
        thread_rows = db.execute(
            text(f"""
                SELECT {_COLUMNS} FROM content_candidates
                WHERE thread_key = :thread_key AND id != :id
                ORDER BY created_at ASC
            """),
            {"thread_key": row["thread_key"], "id": candidate_id},
        ).mappings().all()
        thread = [_row_to_out(r) for r in thread_rows]

    base = _row_to_out(row)
    return ContentCandidateDetailOut(
        **base.model_dump(),
        raw_text=row["raw_text"], draft_text=row["draft_text"],
        synth_declined_reason=row["synth_declined_reason"], reviewer_id=row["reviewer_id"],
        thread=thread,
    )


# Разрешённые переходы — зеркало state machine из шапки db/migrations/022.
# Строго вперёд по пайплайну; терминальные статусы (discarded/no_data/published/
# rejected) переходов не имеют — ручной откат не предусмотрен в v1 (если понадобится
# чинить ошибочный статус, это прямой SQL, не API).
_ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "candidate": {"discarded", "pending"},
    "pending": {"draft_ready", "no_data"},
    "draft_ready": {"in_review"},
    "in_review": {"published", "rejected"},
}


class ContentCandidatePatch(BaseModel):
    status: str


@router.patch("/{candidate_id}", response_model=ContentCandidateOut)
def update_candidate_status(
    candidate_id: int,
    body: ContentCandidatePatch,
    db: Session = Depends(get_db),
    admin: User = Depends(require_admin),
):
    """Ручная смена статуса из админ-Kanban (напр. отбраковать кандидата не
    дожидаясь ИИ, или подтвердить публикацию, сделанную не через бот). Только
    заявленные в state machine переходы — 422 на всё остальное."""
    if body.status not in STATUS_VALUES:
        raise HTTPException(status_code=422, detail=f"Неизвестный статус: {body.status}")

    row = db.execute(
        text("SELECT status FROM content_candidates WHERE id = :id"), {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")

    current = row["status"]
    allowed = _ALLOWED_TRANSITIONS.get(current, set())
    if body.status not in allowed:
        raise HTTPException(
            status_code=422,
            detail=f"Переход {current} → {body.status} не допускается state machine "
                   f"(разрешено: {sorted(allowed) or 'нет — терминальный статус'})",
        )

    is_review_decision = current == "in_review" and body.status in ("published", "rejected")
    db.execute(
        text(f"""
            UPDATE content_candidates
            SET status = :status,
                updated_at = now()
                {", reviewer_action = :reviewer_action, reviewer_id = :reviewer_id" if is_review_decision else ""}
                {", published_at = now()" if body.status == "published" else ""}
            WHERE id = :id
        """),
        {
            "id": candidate_id, "status": body.status,
            **({"reviewer_action": "approved" if body.status == "published" else "rejected",
                "reviewer_id": admin.id} if is_review_decision else {}),
        },
    )
    db.commit()

    updated = db.execute(
        text(f"SELECT {_COLUMNS} FROM content_candidates WHERE id = :id"), {"id": candidate_id},
    ).mappings().first()
    return _row_to_out(updated)


# ══════════════════════════════════════════════════════════════════════════
# /api/internal/content-news/* — callback для ИИ-шагов (Routine или прямой
# API, см. signals/content_ai.py). Server-to-server: НЕ require_admin (ИИ-шаг
# не залогинен как пользователь) — проверка через shared secret в заголовке.
# Не публиковать этот путь в открытой документации/сайтмапе.
# ══════════════════════════════════════════════════════════════════════════

internal_router = APIRouter(prefix="/api/internal/content-news", tags=["internal-content-ai"])


def _require_internal_token(x_internal_token: str = Header(default="")) -> None:
    expected = os.environ.get("CONTENT_AI_INTERNAL_TOKEN", "")
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=403, detail="Неверный или отсутствующий internal token")


class StepAResult(BaseModel):
    relevant: bool
    tickers: list[str] = []
    event_type: Optional[str] = None
    importance_1_5: int
    reasoning: str


class StepCResult(BaseModel):
    draft_text: Optional[str] = None
    declined_reason: Optional[str] = None


# Фирменные custom-emoji Frame (пак t.me/addemoji/Frametool) — та же карта, что
# в signals/publish/telegram.py, продублирована здесь: signals/ НЕ копируется в
# api-образ (см. Dockerfile), а объём кода мал. НЕ синхронизировать вручную —
# если пак поменяется, поправить в обоих местах.
_HYPE_EMOJI_MAP = {
    "🔥": "5454087765559909583",
    "⛔️": "5454074292247499750",
    "📣": "5453896063989620863",
    "💡": "5454396453449408225",
    "🔍": "5454392549324136501",
    "◽️": "5454003420992151507",
}
_HYPE_LOGO_SIGNATURE_IDS = (
    "5456376948768936418",  # скобка (без белого фона)
    "5456183761139961304",  # «FRA» (без белого фона)
    "5454037394183462308",  # «ME» (без белого фона)
)
_HYPE_KANBAN_URL = "https://framedata.ru/admin/content-news"


def _normalize_for_similarity(text_val: Optional[str]) -> str:
    """Заголовок → нижний регистр, без пунктуации/эмодзи/лишних пробелов —
    для difflib-сравнения дублей одной новости из разных источников
    (см. apply_step_a). Достаточно грубо — источники репостят почти
    verbatim, тонкая нормализация не нужна."""
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", (text_val or "").lower())).strip()


def _apply_hype_emoji(html_text: str) -> str:
    """Заменить НАШИ декоративные plain-эмодзи на премиальные custom-emoji Frame.
    Вызывать ПОСЛЕДНИМ шагом, когда HTML-разметка (<b>/<i>) уже настоящая —
    в отличие от apply_custom_emoji() (signals/publish/telegram.py) эта версия
    НЕ делает html.escape всей строки (иначе наши же теги превратились бы в текст)."""
    logo = "".join(f'<tg-emoji emoji-id="{eid}">😀</tg-emoji>' for eid in _HYPE_LOGO_SIGNATURE_IDS)
    result = html_text.replace("😀😀😀", logo)
    for emoji, emoji_id in _HYPE_EMOJI_MAP.items():
        result = result.replace(emoji, f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji>')
    return result


# Фото постов MarketTwits/Smartlab (Вадим 2026-07-16: "посты без фото приходят")
# — качает signals/tg_hype_scan.py (host-side, MTProto), api-контейнер только
# читает через volume-маунт (docker-compose.yml), см. media_filename.
_MEDIA_DIR = "/data/content_media"


def _notify_hype_colleague(source: Optional[str], headline: str, raw_text: Optional[str],
                            source_url: Optional[str], media_filename: Optional[str] = None) -> None:
    """Ставится в известность отдельный получатель (коллега) о каждом кандидате,
    прошедшем и хайп-фильтр (tg_hype_scan.py), и Шаг Н (независимый ИИ-фильтр
    «шутка/мусор vs реальная новость», apply_hype_filter ниже) — НИКАК не
    завязано на тикер/компанию/значимость завода постов (2026-07-16, прямой
    запрос Вадима: Шаг А калиброван под «можно ли написать пост», из-за чего
    реально важные, но нетикерные новости — макро/геополитика — molчa резались
    ДО уведомления; коллеге нужен независимый критерий).
    Минимум наших правок (запрос Вадима 2026-07-15) — сам пост идёт ВЕРБАТИМ, как
    в источнике: только html.escape, БЕЗ подстановки кастом-эмодзи Frame (та
    портила бы визуал оригинала, если в посте уже есть похожий эмодзи) и без
    декоративного обвеса/подписи. Однострочная шапка (источник) — это метаданные
    ОТ НАС, не часть поста. Best-effort: сбой отправки НЕ должен ронять приёмку."""
    token = os.environ.get("HYPE_NOTIFY_BOT_TOKEN", "")
    # Несколько получателей — через запятую (2026-07-16, добавление коллеги
    # №2 к рассылке @hypeframebot).
    chat_ids = [c.strip() for c in os.environ.get("HYPE_NOTIFY_CHAT_ID", "").split(",") if c.strip()]
    if not token or not chat_ids:
        return
    body = raw_text or headline or ""
    header = _apply_hype_emoji(f"<b>{html.escape(source or '?')}</b>")
    text_msg = f"{header}\n\n{html.escape(body)}"
    buttons = [{"text": "Открыть в Kanban", "url": _HYPE_KANBAN_URL}]
    if source_url:
        buttons.insert(0, {"text": "Открыть пост", "url": source_url})
    reply_markup = {"inline_keyboard": [buttons]}
    # api.telegram.org НЕ доступен напрямую с прод-сервера (РФ, РКН) — та же
    # проблема, что и у остальных ботов (см. signals/publish/telegram.py),
    # обходится тем же Cloudflare-релеем через TELEGRAM_API_ROOT. Живой
    # инцидент 2026-07-15: первые 2 реальных кандидата после деплоя фичи упали
    # с ConnectionError "Network is unreachable" при прямом обращении.
    api_root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
    media_path = os.path.join(_MEDIA_DIR, media_filename) if media_filename else None
    # ⚠️ Найдено 2026-07-17 (Вадим: несколько новостей с hype_filter_result='t'
    # не дошли, хотя Шаг Н отработал успешно) — ни один из этих requests.post()
    # не проверял response.status_code: Telegram/релей мог вернуть 400/403
    # (например HTML не распарсился, chat заблокировал бота) и это проходило
    # мимо except целиком — apply_hype_filter всё равно отвечал 200 вызывающей
    # Routine. raise_for_status() переводит такие ответы в исключение, которое
    # уже ловится и логируется ниже.
    for chat_id in chat_ids:
        try:
            if media_path and os.path.isfile(media_path):
                # caption лимит Telegram — 1024 символа (не 4096, как у text у sendMessage).
                with open(media_path, "rb") as photo_file:
                    resp = requests.post(
                        f"{api_root}/bot{token}/sendPhoto",
                        data={
                            "chat_id": chat_id, "caption": text_msg[:1024], "parse_mode": "HTML",
                            "reply_markup": json.dumps(reply_markup),
                        },
                        files={"photo": photo_file},
                        timeout=20,
                    )
            else:
                resp = requests.post(
                    f"{api_root}/bot{token}/sendMessage",
                    json={
                        "chat_id": chat_id, "text": text_msg[:4000], "parse_mode": "HTML",
                        "reply_markup": reply_markup,
                    },
                    timeout=10,
                )
            resp.raise_for_status()
        except Exception as e:
            body = getattr(e, "response", None)
            body_text = body.text[:300] if body is not None else ""
            print(f"[content_news] hype-notify failed for chat_id={chat_id}: "
                  f"{type(e).__name__}: {e} {body_text}")


class HypeFilterResult(BaseModel):
    is_news: bool
    reason: Optional[str] = None


@internal_router.patch("/{candidate_id}/hype-filter", dependencies=[Depends(_require_internal_token)])
def apply_hype_filter(candidate_id: int, body: HypeFilterResult, db: Session = Depends(get_db)):
    """Приёмка результата Шага Н — независимого от Шага А фильтра «шутка/мусор
    vs реальная новость» (см. signals/content_ai.py:_hype_filter_payload,
    TRIGGER_ID_HYPE_FILTER). Единственная цель — решить, уведомлять ли коллегу;
    статус самого кандидата (state machine завода постов) НЕ трогает — это
    параллельный, независимый side-канал, Шаг А продолжает решать судьбу
    кандидата в pipeline сам по себе."""
    row = db.execute(
        text("SELECT source, headline, raw_text, source_url, media_filename FROM content_candidates WHERE id = :id"),
        {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")
    db.execute(text("""
        UPDATE content_candidates SET hype_filter_result = :result, hype_filter_checked_at = now()
        WHERE id = :id
    """), {"id": candidate_id, "result": body.is_news})
    db.commit()
    if body.is_news:
        _notify_hype_colleague(row["source"], row["headline"], row["raw_text"], row["source_url"],
                                row["media_filename"])
    return {"notified": body.is_news}


@internal_router.patch("/{candidate_id}/step-a", dependencies=[Depends(_require_internal_token)])
def apply_step_a(candidate_id: int, body: StepAResult, db: Session = Depends(get_db)):
    """Приёмка результата Шага А (извлечение). Тут же — Шаг А2 (маппинг тикера,
    БЕЗ ИИ): резолвим первый известный тикер через ticker_futures_map. Категорийный
    фолбэк убран (2026-07-15) — матчим СТРОГО по тикеру, поэтому если ничего не
    резолвилось, кандидат отбрасывается сразу, а не зависает в pending на 5 дней
    без единого шанса когда-либо подтвердиться (content_match.py требует
    futures_ticker IS NOT NULL)."""
    row = db.execute(
        text("SELECT status, headline FROM content_candidates WHERE id = :id"),
        {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")
    if row["status"] != "candidate":
        raise HTTPException(status_code=409, detail=f"Ожидался статус 'candidate', сейчас '{row['status']}'")

    if not body.relevant:
        db.execute(text("""
            UPDATE content_candidates
            SET status = 'discarded', tickers = :tickers, event_type = :event_type,
                importance_1_5 = :importance, reasoning = :reasoning, updated_at = now()
            WHERE id = :id
        """), {"id": candidate_id, "tickers": body.tickers, "event_type": body.event_type,
               "importance": body.importance_1_5, "reasoning": body.reasoning})
        db.commit()
        return {"status": "discarded"}

    if body.importance_1_5 < 3:
        db.execute(text("""
            UPDATE content_candidates
            SET status = 'discarded', tickers = :tickers, event_type = :event_type,
                importance_1_5 = :importance, reasoning = :reasoning, updated_at = now()
            WHERE id = :id
        """), {"id": candidate_id, "tickers": body.tickers, "event_type": body.event_type,
               "importance": body.importance_1_5, "reasoning": body.reasoning})
        db.commit()
        return {"status": "discarded"}

    futures_ticker = None
    for t in body.tickers:
        m = db.execute(
            text("SELECT futures_sectype FROM ticker_futures_map WHERE stock_ticker = :t"), {"t": t},
        ).scalar()
        if m:
            futures_ticker = m
            break

    if not futures_ticker:
        db.execute(text("""
            UPDATE content_candidates
            SET status = 'discarded', tickers = :tickers, event_type = :event_type,
                importance_1_5 = :importance,
                reasoning = :reasoning, updated_at = now()
            WHERE id = :id
        """), {
            "id": candidate_id, "tickers": body.tickers, "event_type": body.event_type,
            "importance": body.importance_1_5,
            "reasoning": (body.reasoning or "") +
                         " [конкретный тикер не резолвился — категорийный матчинг убран, "
                         "точного совпадения для Шага Б нет]",
        })
        db.commit()
        return {"status": "discarded"}

    # Дубликат одной новости в разных каналах (Вадим 2026-07-16: "VK продаёт
    # RuStore" пришла и от MarketTwits, и от Smartlab почти одновременно —
    # два кандидата на заводе про один и тот же тикер/событие). ⚠️ Пересечение
    # тикеров САМО ПО СЕБЕ ненадёжно — проверено на реальных данных: id 715
    # ("#X5 #отчетность", пустая метка) и 717 ("X5 объявляет о росте выручки
    # на 9,9%...", реальная новость) созданы с разницей 7 минут, тот же
    # тикер, но это РАЗНЫЕ события — по одному тикеру совпадение отбросило
    # бы настоящую новость 717 как "дубликат" пустышки 715. Поэтому тикер —
    # только предварительный фильтр кандидатов, финальное решение — по
    # текстовому сходству заголовков (difflib, без ИИ: оба live-примера
    # дают чистое разделение — реальный дубликат VKCO ratio=0.98, разные
    # X5/GAZP-новости ratio=0.15-0.25, порог 0.6 берёт с запасом).
    ticker_matches = db.execute(text("""
        SELECT id, headline FROM content_candidates
        WHERE id != :id AND tickers && :tickers
          AND created_at > now() - interval '6 hours'
        ORDER BY created_at ASC
    """), {"id": candidate_id, "tickers": body.tickers}).mappings().all()
    own_norm = _normalize_for_similarity(row["headline"])
    duplicate_of = None
    for m in ticker_matches:
        ratio = difflib.SequenceMatcher(None, own_norm, _normalize_for_similarity(m["headline"])).ratio()
        if ratio >= 0.6:
            duplicate_of = m["id"]
            break
    if duplicate_of:
        db.execute(text("""
            UPDATE content_candidates
            SET status = 'discarded', tickers = :tickers, futures_ticker = :futures_ticker,
                event_type = :event_type, importance_1_5 = :importance,
                thread_key = COALESCE(thread_key, :thread_key),
                parent_candidate_id = :parent_id,
                reasoning = :reasoning, updated_at = now()
            WHERE id = :id
        """), {
            "id": candidate_id, "tickers": body.tickers, "futures_ticker": futures_ticker,
            "event_type": body.event_type, "importance": body.importance_1_5,
            "thread_key": f"ticker:{body.tickers[0]}" if body.tickers else None,
            "parent_id": duplicate_of,
            "reasoning": (body.reasoning or "") + f" [дубликат кандидата #{duplicate_of} — "
                         "тот же тикер + похожий текст (та же новость из другого источника) в пределах 6ч]",
        })
        db.commit()
        return {"status": "discarded", "duplicate_of": duplicate_of}

    db.execute(text("""
        UPDATE content_candidates
        SET status = 'pending', tickers = :tickers, futures_ticker = :futures_ticker,
            event_type = :event_type, importance_1_5 = :importance, reasoning = :reasoning,
            thread_key = COALESCE(thread_key, :thread_key),
            pending_expires_at = now() + make_interval(days => :pending_days),
            updated_at = now()
        WHERE id = :id
    """), {
        "id": candidate_id, "tickers": body.tickers, "futures_ticker": futures_ticker,
        "event_type": body.event_type, "importance": body.importance_1_5,
        "reasoning": body.reasoning,
        "thread_key": f"ticker:{body.tickers[0]}" if body.tickers else None,
        "pending_days": 5,
    })
    db.commit()
    return {"status": "pending", "futures_ticker": futures_ticker}


@internal_router.patch("/{candidate_id}/step-c", dependencies=[Depends(_require_internal_token)])
def apply_step_c(candidate_id: int, body: StepCResult, db: Session = Depends(get_db)):
    """Приёмка результата Шага В (синтез). Если модель отказалась писать
    («недостаточно данных») — НЕ дожидаемся веры на слово, возвращаем кандидата
    в pending на повторную попытку (новые данные могут появиться до истечения
    pending_expires_at), а не тупик — ровно как обсуждали для watch-thread."""
    row = db.execute(
        text("SELECT status FROM content_candidates WHERE id = :id"), {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")
    if row["status"] != "draft_ready":
        raise HTTPException(status_code=409, detail=f"Ожидался статус 'draft_ready', сейчас '{row['status']}'")

    if body.draft_text:
        # draft_text_ai пишется ЗДЕСЬ и только здесь (миграция 054): это
        # единственная точка, где текст заведомо принадлежит ИИ. Дальше его
        # может править человек — но правит он draft_text, а draft_text_ai
        # остаётся нетронутым, и дифф между ними и есть обратная связь
        # (см. research/content_pipeline_v2/RESEARCH_2026-08-31.md §2).
        # Повторный fire Шага В перезаписывает ОБА: новый черновик — это новый
        # вывод ИИ, а не правка человека.
        db.execute(text("""
            UPDATE content_candidates
            SET draft_text = :draft_text, draft_text_ai = :draft_text,
                synth_declined_reason = NULL, updated_at = now()
            WHERE id = :id
        """), {"id": candidate_id, "draft_text": body.draft_text})
        db.commit()
        return {"status": "draft_ready", "has_draft": True}

    db.execute(text("""
        UPDATE content_candidates
        SET status = 'pending', synth_declined_reason = :reason, updated_at = now()
        WHERE id = :id
    """), {"id": candidate_id, "reason": body.declined_reason or "модель не указала причину"})
    db.commit()
    return {"status": "pending", "has_draft": False}
