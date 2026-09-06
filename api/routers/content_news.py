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
import hashlib
import json
import os
import re
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import AliasChoices, BaseModel, Field
from sqlalchemy import text
from api.services import style_profile
from api.agent_trace import ВЗЯТО, НЕ_ВЗЯТО, ПУСТО, трассировать
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
    """⚠️ `draft` — синоним `draft_text`, и он здесь не для красоты.

    Живой случай 01.09: у эндпоинта /style-check поле называлось `draft`, у step-c —
    `draft_text`. Писатель собрал один JSON-файл и естественно переиспользовал его для
    обоих вызовов — черновик молча потерялся, а кандидат уехал в pending как «модель
    отказалась писать». Два соседних эндпоинта одного потока не должны называть одну и
    ту же вещь по-разному; пока имена расходятся, принимаем оба.
    """
    model_config = {"populate_by_name": True}

    draft_text: Optional[str] = Field(default=None, validation_alias=AliasChoices(
        "draft_text", "draft"))
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


# Версия контракта брифа — держать в согласии с signals/content_ai.py:BRIEF_VERSION.
# Поднимать при каждом изменении набора полей брифа.
_BRIEF_VERSION = 18   # v18: числа FinanceMarker (см. signals/content_ai.py)


# ── Шаг Г: судья ───────────────────────────────────────────────────────────
# Рубрика и обоснование групп — research/content_pipeline_v2/RUBRIC.md.
# ВОРОТА A (фактура): любой провал = брак.
_JUDGE_GATES_A = ("numbers_traceable", "no_invented_facts", "no_self_contradiction",
                   "time_arrow_ok")
# ВОРОТА B (смысл): два провала = брак, один = спорно.
# claim_falsifiable и event_matters добавлены 01.09 по разбору 1638: рубрика не
# спрашивала ни «проверяемо ли это утверждение», ни «значит ли вообще что-нибудь
# это событие для ЭТОГО рынка» — а обе претензии Вадима были именно про это.
_JUDGE_GATES_B = ("has_thesis", "link_earned", "no_redundancy",
                   "claim_falsifiable", "event_matters")
# ЧЕК-ЛИСТ ПРОИЗВОДСТВА C: на вердикт НЕ влияет. Эти пункты срабатывают и на
# реальных постах канала (длинное тире у 11%, плотность >2 у 18%) — браковать по
# ним значило бы забраковать сам канал. Но именно они говорят, что чинить.
# sentences_short — чек-лист, а не ворота: длинное предложение портит чтение, но
# фактуру не ломает, а у самого канала медиана 11 слов при максимуме 34 — браковать
# по нему значило бы браковать и канал. Вадим 01.09 выбрал между двумя черновиками
# ОДИНАКОВОЙ длины тот, где предложения короче.
_JUDGE_CHECKLIST_C = ("conclusion_not_boilerplate", "no_brand_selfref",
                       "numbers_density", "format_ok", "no_methodology_talk",
                       "length_ok", "sentences_short")


class JudgeParagraph(BaseModel):
    """Разбор ОДНОГО абзаца. Требование назвать опору и сомнение по каждому абзацу
    в отдельности — не украшение отчёта, а способ не дать проскользнуть: пост
    целиком модель просматривает, абзац по абзацу — разбирает."""
    n: int
    claim: str = ""            # что абзац утверждает, своими словами
    supported_by: str = ""     # чем это держится в брифе (поле, число, дата)
    supported: bool = True     # опора найдена?
    doubt: Optional[str] = None  # чем утверждение можно оспорить


class JudgeResult(BaseModel):
    """Модель заполняет ТОЛЬКО пункты и поабзацный разбор. Вердикт считает код —
    так он воспроизводим и видно, из чего собран."""
    items: dict[str, bool]
    evidence: dict[str, str] = {}
    paragraphs: list[JudgeParagraph] = []
    note: Optional[str] = None
    # Отпечаток текста, который судья реально читал (echo из payload). См. проверку
    # в apply_step_g: без неё поздний вердикт ложится на уже переписанный черновик.
    draft_hash: Optional[str] = None
    # Исправленный судьёй текст. Принимается ТОЛЬКО когда вердикт не «годится» —
    # править нечего в чистом черновике, а лишняя правка вносит лишний риск.
    fixed_draft: Optional[str] = None
    fix_note: Optional[str] = None
    # ⚠️ Независимый ответ судьи, записанный ДО чтения черновика (anti-anchoring).
    # arXiv:2607.05904: судья, которому сразу показали готовый текст, оценивает
    # убедительность, а не правильность — проходимость 0,72 → 0,94 при истинной
    # точности 0,20, и ансамбль из трёх судей принимал 55% взломанных ответов.
    # Ломает это только независимое суждение ПЕРЕД показом. Храним, чтобы можно было
    # проверить, что судья действительно его давал, а не пересказывал черновик.
    own_take: Optional[str] = None


def _derive_judge_verdict(items: dict, paragraphs=()) -> tuple:
    """Вердикт из пунктов + поабзацного разбора.

    Абзац без опоры в брифе — это ворота B, не чек-лист: утверждение, которое не на
    чём держится, читатель принимает за факт. Счёт пропорциональный (один абзац =
    один провал ворот B), поэтому один абзац без опоры даёт «спорно», два и больше —
    «брак», ровно как два обычных провала смысла.

    ⚠️ Абзацы НЕ ужимаются в один синтетический провал: три абзаца без опоры — это
    хуже, чем один, и вердикт должен это различать."""
    failed_a = [k for k in _JUDGE_GATES_A if items.get(k) is False]
    failed_b = [k for k in _JUDGE_GATES_B if items.get(k) is False]
    failed_b += [f"абзац_{p.n}_без_опоры" for p in paragraphs if p.supported is False]
    defects = [k for k in _JUDGE_CHECKLIST_C if items.get(k) is False]
    if failed_a:
        return "брак", failed_a + failed_b, defects
    if len(failed_b) >= 2:
        return "брак", failed_b, defects
    if len(failed_b) == 1:
        return "спорно", failed_b, defects
    return "годится", [], defects



_SELECT_REAL_REJECTIONS = text("""
    SELECT f.candidate_id, c.headline, f.event, f.draft_ai, f.draft_human,
           f.reason_code, f.reason_text, f.judge_verdict, f.brief_version,
           f.created_at::date AS d
    FROM content_feedback f
    JOIN content_candidates c ON c.id = f.candidate_id
    WHERE f.reason_text IS NOT NULL AND length(btrim(f.reason_text)) >= 15
    ORDER BY f.created_at DESC
    LIMIT :limit
""")



class StyleCheckIn(BaseModel):
    """Принимает и `draft`, и `draft_text` — см. докстринг StepCResult."""
    model_config = {"populate_by_name": True}

    draft: str = Field(validation_alias=AliasChoices("draft", "draft_text"))


@internal_router.post("/style-check", dependencies=[Depends(_require_internal_token)])
def style_check(body: StyleCheckIn):
    """Профиль черновика против постов канала — для САМОПРОВЕРКИ Шага В до отправки.

    ⚠️ Почему это не ворота, а подсказка. Скор измеряет похожесть на канал, а не
    качество: в корпусе нет оценок качества. Отклонять по нему PATCH значило бы
    браковать текст за непохожесть на медиану — а канал сам разбросан широко
    (длина 661 ± 319). Поэтому эндпоинт только считает и советует.

    ⚠️ Советы намеренно про ТЕКСТ, а не про цифру. «Плотность +1,4σ» модель
    исправила бы механически — выкинула бы число и сломала мысль. Поэтому в
    что_поправить сказано, что именно делать: добавить связок, а не срезать факты.

    Зачем вообще: замер 01.09 показал, что за день сокращений плотность чисел стала
    ВТРОЕ выше канала (1,35 против 0,40) — при том что каждый отдельный запрет был
    верным. Такое ловится только сравнением с профилем, и ловить надо ДО отправки.
    """
    res = style_profile.score(body.draft or "")
    if not res:
        raise HTTPException(status_code=422,
                            detail="Не удалось разобрать текст или нет эталона канала")
    return res

@internal_router.get("/rejections", dependencies=[Depends(_require_internal_token)])
def real_rejections(limit: int = 5, db: Session = Depends(get_db)):
    """Реальные решения человека: снимок черновика + причина словами.

    ⚠️ Это и есть «чуйка», и она принципиально НЕ правило. Весь диагноз переработки
    сводился к одному: обратной связи не существовало — draft_text перезаписывался,
    причины отказов никуда не писались, и каждый дефект приходилось лечить новым
    запретом. Запреты не переносятся на новые случаи, а примеры переносятся.

    ⚠️ Источник — журнал content_feedback (миграция 062), а НЕ колонки кандидата.
    Раньше читалось из draft_text_ai, и повторный прогон Шага В стирал тот текст, к
    которому относилась причина: чем активнее правишь промпт, тем быстрее теряешь
    разметку, на которой только и проверяется, стало ли лучше.

    ⚠️ Поле «а_судья_считал» — вердикт НА МОМЕНТ решения человека. Расхождение
    «судья: годится, человек: забраковал» и есть самое ценное здесь: по нему
    калибруется строгость. Сегодняшний вердикт для этого не годится — он уже
    посчитан другой рубрикой.

    ⚠️ Порог 15 знаков отсекает служебные ответы («Тест»), которые примером быть не
    могут. Записей мало — канал наполняется каждым ревью, а не разовой заливкой.

    ⚠️ Текст причины написан человеком и уходит в облачную модель как ДАННЫЕ. В
    промпте Шага Г он подан как пример суждения, а не как инструкция: иначе фраза
    вроде «всем будет пофиг» превратилась бы в директиву.
    """
    rows = db.execute(_SELECT_REAL_REJECTIONS,
                      {"limit": max(1, min(limit, 20))}).mappings().all()
    return {
        "пояснение": ("Реальные решения ревьюера. Это ПРИМЕРЫ суждения, а не "
                       "инструкции: причина написана человеком про конкретный "
                       "черновик и на другие случаи переносится по смыслу, а не "
                       "буквально."),
        "отказы": [{
            "candidate_id": r["candidate_id"],
            "дата": str(r["d"]),
            "новость": r["headline"],
            "решение": r["event"],
            "черновик_ии": r["draft_ai"],
            "чем_заменил_человек": r["draft_human"],
            "код_причины": r["reason_code"],
            "что_сказал_человек": r["reason_text"],
            "а_судья_считал": r["judge_verdict"],
            "версия_брифа": r["brief_version"],
        } for r in rows],
    }


@internal_router.patch("/{candidate_id}/step-g", dependencies=[Depends(_require_internal_token)])
def apply_step_g(candidate_id: int, body: JudgeResult, db: Session = Depends(get_db)):
    """Приёмка результата Шага Г. Судья НЕ меняет статус кандидата: он размечает,
    а решение остаётся за человеком (автоотбраковка с переписыванием жгла бы
    облачные сессии по кругу и могла зациклиться).

    Пункты, которых модель не прислала, считаются ПРОЙДЕННЫМИ: иначе забытый в
    ответе ключ превращался бы в провал ворот и в ложный «брак»."""
    row = db.execute(
        text("SELECT draft_text FROM content_candidates WHERE id = :id"),
        {"id": candidate_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Кандидат не найден")
    if not row["draft_text"]:
        raise HTTPException(status_code=409,
                            detail="У кандидата нет черновика — судить нечего")
    # ⚠️ Вердикт принимается только для ТОГО текста, который судья читал. Батч 01.09:
    # Шаг В сработал по кандидату дважды (кулдаун был равен периоду крона), судья
    # прочитал первый черновик, а его вердикт лёг на второй — и в карточке это
    # выглядело как настоящая претензия к числам, которых в тексте нет. Отличить
    # такое от реальной ошибки по логу невозможно, поэтому проверяем отпечатком.
    if body.draft_hash:
        current = hashlib.md5((row["draft_text"] or "").encode("utf-8")).hexdigest()
        if body.draft_hash != current:
            raise HTTPException(
                status_code=409,
                detail="Черновик изменился после отправки судье — вердикт устарел")

    known = set(_JUDGE_GATES_A) | set(_JUDGE_GATES_B) | set(_JUDGE_CHECKLIST_C)
    unknown = sorted(set(body.items) - known)
    items = {k: bool(v) for k, v in body.items.items() if k in known}
    verdict, failed, defects = _derive_judge_verdict(items, body.paragraphs)

    # ⚠️ Правка судьи применяется, но вердикт остаётся выданным на ИСХОДНЫЙ текст:
    # судья не перепроверяет сам себя, иначе независимость второго прохода исчезает
    # (он ставил бы «годится» себе). Оригинал писателя остаётся в draft_text_ai, обе
    # версии уходят в журнал content_feedback. Одна попытка, без цикла.
    fix = (body.fixed_draft or "").strip()
    apply_fix = bool(fix) and verdict != "годится" and fix != (row["draft_text"] or "")
    if apply_fix:
        db.execute(text("""
            INSERT INTO content_feedback (candidate_id, event, draft_ai, draft_human,
                                           reason_code, reason_text, brief_version,
                                           judge_verdict, judge_failed, judge_defects)
            SELECT c.id, 'judge_fixed', coalesce(c.draft_text_ai, c.draft_text), :fix,
                   'judge', :note, c.brief_version, :verdict,
                   CAST(:failed AS text[]), CAST(:defects AS text[])
            FROM content_candidates c WHERE c.id = :id
        """), {"id": candidate_id, "fix": fix, "note": body.fix_note,
               "verdict": verdict, "failed": failed, "defects": defects})

    db.execute(text("""
        UPDATE content_candidates
        SET judge_items = CAST(:items AS jsonb), judge_verdict = :verdict,
            judge_failed = :failed, judge_defects = :defects, judge_note = :note,
            judge_paragraphs = CAST(:paragraphs AS jsonb),
            draft_text = CASE WHEN :apply_fix THEN :fix ELSE draft_text END,
            judge_fixed_at = CASE WHEN :apply_fix THEN now() ELSE judge_fixed_at END,
            judge_fix_note = CASE WHEN :apply_fix THEN :note ELSE judge_fix_note END,
            judge_checked_at = now(), updated_at = now()
        WHERE id = :id
    """), {
        "id": candidate_id,
        "items": json.dumps({"items": items, "evidence": body.evidence,
                              "own_take": body.own_take}, ensure_ascii=False),
        "verdict": verdict, "failed": failed, "defects": defects,
        "note": body.note,
        "paragraphs": json.dumps([p.model_dump() for p in body.paragraphs],
                                  ensure_ascii=False) if body.paragraphs else None,
        "apply_fix": apply_fix, "fix": fix or None, "note": body.fix_note,
    })
    # ── След судьи. Вердикт — одна строка, разбор абзацев — по строке на абзац.
    # ⚠️ Абзацы пишутся ВСЕ, а не только проваленные: «абзац 2 опирается на долю из
    # world_facts» объясняет пост не хуже, чем «абзац 3 без опоры», и без принятых
    # абзацев в дашборде получился бы список одних претензий.
    _t = трассировать(db, candidate_id, "судья")
    _t.record("judge", "вердикт по черновику",
              outcome=(ВЗЯТО if verdict == "годится" else НЕ_ВЗЯТО),
              result_count=len(items),
              result_note="%s · проверено пунктов: %d" % (verdict, len(items)),
              reason=(", ".join(failed) if failed else None),
              params={"вердикт": verdict, "провалы": failed, "недочёты": defects,
                      "правка_применена": apply_fix})
    for para in body.paragraphs:
        _t.record("judge", "абзац %d" % para.n,
                  outcome=(ВЗЯТО if para.supported else НЕ_ВЗЯТО),
                  result_count=1,
                  result_note=(para.supported_by or para.claim or "")[:200],
                  reason=(None if para.supported else (para.doubt or "опора в брифе не найдена")))

    db.commit()
    return {"verdict": verdict, "failed_gates": failed, "defects": defects,
            "paragraphs_without_support": [p.n for p in body.paragraphs
                                            if p.supported is False],
            "fix_applied": apply_fix,
            "ignored_unknown_keys": unknown}


def _style_snapshot(draft: str):
    """Компактный профиль стиля для хранения. None, если посчитать не удалось.

    Храним только то, что нужно для агрегатов: сами признаки, отклонения в сигмах и
    среднее. Советы (что_поправить) не храним — они для писателя в моменте, а не
    исторические данные.
    """
    res = style_profile.score(draft or "")
    if not res:
        return None
    return json.dumps({"профиль": res["профиль"],
                        "отклонение_сигм": res["отклонение_сигм"],
                        "среднее_отклонение": res["среднее_отклонение"]},
                       ensure_ascii=False)


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

    # ⚠️ Отказ писать должен быть ЯВНЫМ. Раньше пустой draft_text молча трактовался
    # как «модель отказалась» и откатывал кандидата в pending — и именно это спрятало
    # расхождение имён полей (draft против draft_text): черновик был написан, прошёл
    # style-check чисто, но исчез, а в базе осталось «модель не указала причину».
    # Молчаливая интерпретация ошибки как решения — худший вид сбоя: он выглядит
    # штатным.
    if not body.draft_text and not body.declined_reason:
        raise HTTPException(
            status_code=422,
            detail="Нужен либо draft_text с текстом поста, либо declined_reason с "
                   "причиной отказа. Пустое тело отказом НЕ считается.")

    if body.draft_text:
        # draft_text_ai пишется ЗДЕСЬ и только здесь (миграция 054): это
        # единственная точка, где текст заведомо принадлежит ИИ. Дальше его
        # может править человек — но правит он draft_text, а draft_text_ai
        # остаётся нетронутым, и дифф между ними и есть обратная связь
        # (см. research/content_pipeline_v2/RESEARCH_2026-08-31.md §2).
        # Повторный fire Шага В перезаписывает ОБА: новый черновик — это новый
        # вывод ИИ, а не правка человека.
        # brief_version помечается ЗДЕСЬ: только эта точка знает, что черновик
        # написан по актуальному контракту брифа. Судья (Шаг Г) берёт лишь
        # актуальную версию — иначе он валит старые черновики на воротах фактуры
        # за поля, которых в новом брифе уже нет (миграция 059).
        db.execute(text("""
            UPDATE content_candidates
            SET draft_text = :draft_text, draft_text_ai = :draft_text,
                brief_version = :brief_version,
                style_profile = CAST(:style AS jsonb),
                synth_declined_reason = NULL, updated_at = now()
            WHERE id = :id
        """), {"id": candidate_id, "draft_text": body.draft_text,
                "brief_version": _BRIEF_VERSION,
                # ⚠️ Профиль считается ЗДЕСЬ, а не доверяется модели: писатель может
                # его не запросить или соврать. Дрейф стиля виден только на ряде
                # черновиков (01.09: плотность чисел уехала втрое, хотя каждый
                # отдельный запрет был верным), поэтому нужен снимок КАЖДОГО.
                "style": _style_snapshot(body.draft_text)})
        # ⚠️ Длина в следе — не украшение. Замер 01.09: черновик разъехался до 1 105
        # знаков против медианы жанра 661, и заметили это постфактум по одному посту.
        # В следе дрейф виден рядом на всех черновиках подряд.
        трассировать(db, candidate_id, "писатель").record(
            "writer", "написать пост по брифу", outcome=ВЗЯТО, result_count=1,
            result_note="черновик %d знаков" % len(body.draft_text),
            params={"версия_брифа": _BRIEF_VERSION})
        db.commit()
        return {"status": "draft_ready", "has_draft": True}

    _reason = body.declined_reason or "модель не указала причину"
    db.execute(text("""
        UPDATE content_candidates
        SET status = 'pending', synth_declined_reason = :reason, updated_at = now()
        WHERE id = :id
    """), {"id": candidate_id, "reason": _reason})
    # ⚠️ Отказ писателя — САМАЯ ценная строка следа на этом шаге. В воронке такой
    # кандидат просто возвращается в pending и внешне неотличим от того, до которого
    # ещё не дошли руки. Причина отказа («данных мало», «сюжет не складывается»)
    # существует только здесь и только в этот момент.
    трассировать(db, candidate_id, "писатель").record(
        "writer", "написать пост по брифу", outcome=ПУСТО, result_count=0,
        result_note="отказался писать", reason=_reason,
        params={"версия_брифа": _BRIEF_VERSION})
    db.commit()
    return {"status": "pending", "has_draft": False}
