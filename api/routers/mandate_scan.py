"""
/api/internal/mandate-scan/* — callback для еженедельного Routine-скаута
(frame-mandate-scan), который ищет новые мандаты/законы вынужденных потоков
на рынке РФ (сетка участник×домен права, WebSearch) и классифицирует их по
6 критериям отбора (запрос Вадима, 2026-07-22: источник / тип принуждения /
статус сейчас / механический триггер / Pine-тестируемость / шум).

Кандидаты, прошедшие фильтр, уведомляются в @frameadminbot (ADMIN_CHAT_ID) —
итоговая торговая гипотеза строится вручную из этого потока, эндпоинт только
хранит + уведомляет, ничего не решает сам.

Server-to-server: НЕ require_admin (Routine не залогинен как пользователь) —
проверка через shared secret в заголовке X-Internal-Token, как в content_news.py.

Таблица — mandate_candidates (db/migrations/039_..., 040_mandate_candidates_dedup_key.sql).
Дедуп по dedup_key (НЕ source_ref — оказался хрупким к перефразировке одного и
того же акта между разными еженедельными прогонами, см. 040_*.sql): source_key
(короткий канонический номер акта, напр. "6433-У") нормализуется здесь (только
буквы/цифры, верхний регистр) ДО сравнения — устойчиво к пробелам/регистру/дефисам.
GET /known — Routine читает это ПЕРЕД поиском, чтобы не тратить эффект на уже
известное (растущий список в БД, а не замороженный снимок в промпте триггера).
"""
import logging
import os
import re
from typing import Optional

import requests
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger("moex_api")

_DEDUP_STRIP_RE = re.compile(r"[^0-9A-Za-zА-Яа-яЁё]")


def _dedup_key(source_key: str) -> str:
    return _DEDUP_STRIP_RE.sub("", source_key).upper()


# Длины varchar-колонок mandate_candidates. Скаут — языковая модель, и она
# периодически пишет в короткое поле развёрнутую фразу: 31.08.2026 в asset
# (varchar 120) приехало «TMOS, EQMX, AMIX; для средней/малой капитализации —
# SBSC; для потребсектора — AKCN (Альфа-Капитал).» → 22001 value too long →
# 500 на весь эндпоинт, находка недели потеряна. Обрезаем по границе колонки:
# укороченная строка в поле лучше, чем выпавший кандидат, а полный текст акта
# всё равно лежит в hypothesis/trigger_description (они text, без лимита).
_FIELD_LIMITS = {
    "source_ref": 200,
    "source_key": 64,
    "dedup_key": 64,
    "sector": 64,
    "asset": 120,
}


def _fit(field: str, value: Optional[str]) -> Optional[str]:
    """Подрезать значение под лимит колонки, шумнув в лог.

    Режем по последнему разделителю перед лимитом, а не посреди слова: поле
    asset обычно перечисление тикеров через «;» или «,», и обрыв на «...в
    индекс сре» читается как мусор. Многоточие в конце — явный признак, что
    значение неполное; полный текст остаётся в логе и в пуше админу (он
    формируется из тела запроса, до подрезки).
    """
    limit = _FIELD_LIMITS.get(field)
    if value is None or limit is None or len(value) <= limit:
        return value

    head = value[:limit - 1]
    cut = max(head.rfind("; "), head.rfind(", "))
    if cut > limit // 2:            # граница нашлась и не съела больше половины
        head = head[:cut]
    logger.warning(
        f"mandate_scan: поле {field} длиной {len(value)} > {limit}, обрезано. "
        f"Исходное значение: {value!r}"
    )
    return head + "…"

internal_router = APIRouter(prefix="/api/internal/mandate-scan", tags=["internal-mandate-scan"])


def _require_internal_token(x_internal_token: str = Header(default="")) -> None:
    expected = os.environ.get("MANDATE_SCAN_INTERNAL_TOKEN", "")
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=403, detail="Неверный или отсутствующий internal token")


class MandateCandidate(BaseModel):
    source_ref: str                     # "Указание Банка России №6433-У от 01.06.2023" (полная цитата, для людей)
    source_key: str                     # "6433-У" — короткий канонический номер акта, ключ дедупа
    source_url: Optional[str] = None
    mandate_type: str                   # physical_necessity|government_decree|regulator_rule|index_methodology|corporate_policy
    participant: str                    # exporters|banks|insurers|pension_funds|nonresidents|market_makers|state_entities|issuers|retail|exchange
    sector: Optional[str] = None
    asset: str                          # USDRUB, ОФЗ, конкретный тикер, IMOEX...
    status_now: str = "active"          # active|suspended|zeroed|pending
    mechanical_trigger: bool = False
    trigger_description: Optional[str] = None
    pine_testable: str = "low"          # high|medium|low
    hypothesis: str
    durability_note: Optional[str] = None


def _notify_admin(candidate: MandateCandidate) -> bool:
    """Пуш в @frameadminbot. Та же связка BOT_TOKEN/ADMIN_CHAT_ID, что у
    tg_bot.py, + TELEGRAM_API_ROOT relay (api.telegram.org недоступен напрямую
    с прод-сервера, РКН — см. _notify_hype_colleague в content_news.py, живой
    инцидент 2026-07-15). Возвращает True/False — вызывающий код решает, ставить
    ли notified_at; сбой не должен молча теряться (тот же урок, что и с
    hype_filter_dispatch_attempts, db/migrations/035_*.sql)."""
    token = os.environ.get("BOT_TOKEN", "")
    chat_id = os.environ.get("ADMIN_CHAT_ID", "")
    if not token or not chat_id:
        return False
    api_root = os.environ.get("TELEGRAM_API_ROOT", "https://api.telegram.org")
    trigger_line = f"\n⚙️ Триггер: {candidate.trigger_description}" if candidate.trigger_description else ""
    text_msg = (
        f"🏛 <b>Новый мандат-кандидат</b>\n"
        f"{candidate.source_ref}\n\n"
        f"Участник: {candidate.participant} · Сектор: {candidate.sector or '—'}\n"
        f"Актив: <b>{candidate.asset}</b> · Статус: {candidate.status_now}\n"
        f"Pine-тестируемость: <b>{candidate.pine_testable}</b>{trigger_line}\n\n"
        f"{candidate.hypothesis}"
    )
    payload = {"chat_id": chat_id, "text": text_msg[:4000], "parse_mode": "HTML"}
    if candidate.source_url:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": "Открыть источник", "url": candidate.source_url}]]
        }
    try:
        resp = requests.post(f"{api_root}/bot{token}/sendMessage", json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        body = getattr(e, "response", None)
        body_text = body.text[:300] if body is not None else ""
        print(f"[mandate_scan] notify failed for source_ref={candidate.source_ref}: "
              f"{type(e).__name__}: {e} {body_text}")
        return False


def _record_heartbeat(db: Session, success: bool, note: str) -> None:
    """Пульс запуска в pipeline_runs — то же, что pipeline_heartbeat.py пишет
    для скриптов ингеста, продублировано здесь через уже открытую Session
    (pipeline_heartbeat.py не импортируется в api/ — та же причина, что и у
    _HYPE_EMOJI_MAP в content_news.py: signals/root-модули не гарантированно
    доступны из api-образа единообразно, объём кода мал). /api/health/data
    подхватывает строку АВТОМАТИЧЕСКИ (чистый SELECT, без хардкода имён
    пайплайнов) — недельный порог для mandate_scan уже добавлен в
    _PIPELINE_MAX_AGE_H (health_monitor.py), по образцу distributions.
    Best-effort: сбой пульса не должен ронять сам эндпоинт."""
    try:
        status = "ok" if success else "fail"
        db.execute(text("""
            INSERT INTO pipeline_runs (pipeline, last_run_at, last_success_at, last_status, last_note, updated_at)
            VALUES ('mandate_scan', now(), CASE WHEN :success THEN now() ELSE NULL END, :status, :note, now())
            ON CONFLICT (pipeline) DO UPDATE SET
                last_run_at = EXCLUDED.last_run_at,
                last_success_at = COALESCE(EXCLUDED.last_success_at, pipeline_runs.last_success_at),
                last_status = EXCLUDED.last_status,
                last_note = EXCLUDED.last_note,
                updated_at = EXCLUDED.updated_at
        """), {"success": success, "status": status, "note": (note or "")[:500]})
        db.commit()
    except Exception as e:
        print(f"[mandate_scan] heartbeat write failed: {type(e).__name__}: {e}")


class RunDone(BaseModel):
    success: bool = True
    candidates_found: int = 0
    note: Optional[str] = None


@internal_router.post("/done", dependencies=[Depends(_require_internal_token)])
def mark_run_done(body: RunDone, db: Session = Depends(get_db)):
    """Routine вызывает это ПОСЛЕДНИМ действием каждого прогона (нашёл он
    что-то или нет) — без этого /api/health/data и, следом, ежедневный
    frame-monitor не отличат «прогон был и честно ничего не нашёл» от
    «Routine вообще не запустился» (тот же класс проблемы, что решали для
    content-пайплайна через hype_filter_dispatch_attempts, db/migrations/035_*.sql,
    только тут бэкстопом служит уже существующий frame-monitor, не отдельный)."""
    note = body.note or f"{body.candidates_found} находок прошли отбор"
    _record_heartbeat(db, body.success, note)
    return {"status": "recorded"}


@internal_router.get("/known", dependencies=[Depends(_require_internal_token)])
def list_known(db: Session = Depends(get_db)):
    """Routine дёргает это ПЕРВЫМ шагом каждого еженедельного прогона — растущий
    в БД список уже найденного (не замороженный снимок в промпте триггера на
    момент создания). Компактно: только то, что нужно, чтобы не искать/не
    отправлять повторно то же самое другими словами."""
    rows = db.execute(text("""
        SELECT source_key, participant, asset, status_now
        FROM mandate_candidates ORDER BY found_at DESC
    """)).mappings().all()
    return {"count": len(rows), "known": [dict(r) for r in rows]}


@internal_router.post("/candidate", dependencies=[Depends(_require_internal_token)])
def submit_candidate(body: MandateCandidate, db: Session = Depends(get_db)):
    """Приёмка одного кандидата от еженедельного Routine-скаута. Дедуп по
    dedup_key (нормализованный source_key, НЕ source_ref — см. модуль docstring) —
    ON CONFLICT DO NOTHING, повторная находка того же акта не шлёт уведомление
    заново, даже если сформулирована другими словами. Уведомляем только на
    свежей вставке."""
    dedup_key = _fit("dedup_key", _dedup_key(body.source_key))
    row = db.execute(text("""
        INSERT INTO mandate_candidates (
            source_ref, source_key, dedup_key, source_url, mandate_type, participant, sector, asset,
            status_now, mechanical_trigger, trigger_description, pine_testable,
            hypothesis, durability_note
        ) VALUES (
            :source_ref, :source_key, :dedup_key, :source_url, :mandate_type, :participant, :sector, :asset,
            :status_now, :mechanical_trigger, :trigger_description, :pine_testable,
            :hypothesis, :durability_note
        )
        ON CONFLICT (dedup_key) DO NOTHING
        RETURNING id
    """), {
        "source_ref": _fit("source_ref", body.source_ref),
        "source_key": _fit("source_key", body.source_key), "dedup_key": dedup_key,
        "source_url": body.source_url,
        "mandate_type": body.mandate_type, "participant": body.participant,
        "sector": _fit("sector", body.sector),
        "asset": _fit("asset", body.asset), "status_now": body.status_now,
        "mechanical_trigger": body.mechanical_trigger, "trigger_description": body.trigger_description,
        "pine_testable": body.pine_testable, "hypothesis": body.hypothesis,
        "durability_note": body.durability_note,
    }).mappings().first()
    db.commit()

    if not row:
        return {"status": "duplicate"}

    notified = _notify_admin(body)
    if notified:
        db.execute(text("UPDATE mandate_candidates SET notified_at = now() WHERE id = :id"), {"id": row["id"]})
        db.commit()

    return {"status": "created", "id": row["id"], "notified": notified}
