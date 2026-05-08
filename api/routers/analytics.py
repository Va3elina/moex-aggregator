"""
Analytics API — приём событий с фронта + admin-stats.

Endpoints:
- POST /api/analytics/event   — batch insert events (любой user, в т.ч. гость)
- GET  /api/analytics/stats   — aggregated metrics (admin only)

Принципы:
- Никаких PII в логах (event payload не должен содержать email/name/etc.)
- Fire-and-forget: ошибка БД не должна ломать UI → возвращаем 204 даже при partial fail
- Опт-аут через client (cookie / Profile setting) — frontend сам не отправляет когда optout
- Retention 180 дней — cleanup в orchestrator
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import text

from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import get_current_user_optional, require_admin

log = get_logger()
router = APIRouter(prefix="/api/analytics", tags=["analytics"])


# ════════════════════════════════════════════════════════════════════════════
# POST /event — batch event ingestion
# ════════════════════════════════════════════════════════════════════════════

class AnalyticsEvent(BaseModel):
    """Одно событие. Frontend отправляет batch'ем 1-50 за раз."""
    session_id: str = Field(..., min_length=36, max_length=36)  # UUID v4
    event_type: str = Field(..., min_length=1, max_length=50)
    event_path: Optional[str] = Field(None, max_length=255)
    payload: Optional[dict[str, Any]] = None
    client_ts: datetime
    device: Optional[str] = Field(None, max_length=20)


class AnalyticsBatch(BaseModel):
    events: list[AnalyticsEvent] = Field(..., min_length=1, max_length=50)


# Whitelist of event types — preventing arbitrary type spam.
# Если фронт пытается отправить unknown type — игнорим (без 4xx, чтобы не ломать UI).
ALLOWED_EVENT_TYPES = {
    "pageview",
    "indicator_view",
    "instrument_select",
    "seasonality_mode",
    "chart_export",
    "chart_annotate",
    "period_change",
    "theme_toggle",
    "session_heartbeat",
    "session_end",
}


def _detect_device(user_agent: str) -> str:
    """Грубая классификация по UA. Не для precision, только для распределения."""
    ua = (user_agent or "").lower()
    if "bot" in ua or "spider" in ua or "crawl" in ua:
        return "bot"
    if "mobi" in ua or "iphone" in ua or "android" in ua and "mobile" in ua:
        return "mobile"
    if "tablet" in ua or "ipad" in ua:
        return "tablet"
    return "desktop"


def _detect_country(req: Request) -> Optional[str]:
    """Извлекает country code из proxy headers если есть.
    nginx может прокидывать через CF-IPCountry / X-Country-Code.
    Без этого — None (точный IP мы НЕ парсим, это privacy violation)."""
    for header in ("CF-IPCountry", "X-Country-Code", "X-Geo-Country"):
        v = req.headers.get(header)
        if v and len(v) == 2:
            return v.upper()
    return None


@router.post("/event", status_code=204)
async def post_events(
    batch: AnalyticsBatch,
    request: Request,
    user=Depends(get_current_user_optional),
):
    """Принимает batch событий. Status 204 — fire-and-forget.

    Ошибки БД логируем но НЕ возвращаем 5xx (чтобы UI не паниковал).
    """
    user_id = user.id if user else None
    user_agent = request.headers.get("User-Agent", "")
    device = _detect_device(user_agent)
    country = _detect_country(request)

    # Фильтруем события по whitelist'у — unknown типы тихо отбрасываем
    valid = [e for e in batch.events if e.event_type in ALLOWED_EVENT_TYPES]
    if not valid:
        return None

    try:
        engine = get_engine()
        with engine.begin() as conn:
            # Один INSERT через executemany — быстрее чем N отдельных
            for ev in valid:
                conn.execute(
                    text("""
                        INSERT INTO analytics_events
                            (user_id, session_id, event_type, event_path,
                             payload, client_ts, ip_country, device)
                        VALUES (:user_id, :session_id, :event_type, :event_path,
                                :payload, :client_ts, :country, :device)
                    """),
                    {
                        "user_id": user_id,
                        "session_id": ev.session_id,
                        "event_type": ev.event_type,
                        "event_path": ev.event_path,
                        "payload": _serialize_jsonb(ev.payload),
                        "client_ts": ev.client_ts,
                        "country": country,
                        # Per-event device override (mobile может прислать tablet event если split-screen)
                        "device": ev.device or device,
                    },
                )
    except Exception as e:
        log.error(f"analytics insert failed: {e}")
        # 204 anyway — не ломаем UI.

    return None


def _serialize_jsonb(payload: Optional[dict]) -> Optional[str]:
    """Postgres JSONB через pg8000 driver требует JSON-string или dict.
    SQLAlchemy сама serialize'ит dict если JSONB column type. Безопасный fallback — None."""
    if payload is None:
        return None
    import json
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════════════
# GET /stats — admin-only aggregated metrics
# ════════════════════════════════════════════════════════════════════════════

@router.get("/stats")
async def get_stats(
    days: int = Query(7, ge=1, le=180, description="Период (дней)"),
    segment: str = Query("all", description="all / auth / guest / admin"),
    device: str = Query("all", description="all / mobile / desktop / tablet"),
    user=Depends(require_admin),
):
    """Aggregated metrics для admin-stats страницы.

    Возвращает структуру:
    {
      "summary": {
        "dau": 42,
        "sessions": 187,
        "avg_session_sec": 263,
        "events": 12453,
        "delta_dau": +5,                  # vs предыдущий равный период
        "delta_sessions_pct": +12,
        "delta_avg_session_sec": +8,
        "delta_events_pct": +18
      },
      "trends": [{"date": "2026-05-01", "dau": 38, "sessions": 142}, ...],
      "top_pages": [{"path": "/heatmap", "views": 432}, ...],
      "top_instruments": [{"secid": "SBER", "selects": 87}, ...],
      "top_exports": [{"indicator": "oi", "count": 23}, ...],
      "mode_distribution": [{"mode": "yearly", "count": 156}, ...]
    }
    """
    engine = get_engine()
    now = datetime.utcnow()
    period_start = now - timedelta(days=days)
    prev_period_start = now - timedelta(days=days * 2)

    # Filters
    where_clauses = ["server_ts >= :period_start"]
    params: dict[str, Any] = {"period_start": period_start, "now": now}

    if segment == "auth":
        where_clauses.append("user_id IS NOT NULL")
    elif segment == "guest":
        where_clauses.append("user_id IS NULL")
    elif segment == "admin":
        where_clauses.append("user_id IN (SELECT id FROM users WHERE role = 'admin')")

    if device != "all":
        where_clauses.append("device = :device")
        params["device"] = device

    where_sql = " AND ".join(where_clauses)

    with engine.connect() as conn:
        # === Summary ===
        summary_row = conn.execute(text(f"""
            SELECT
                COUNT(DISTINCT COALESCE(user_id::text, session_id))         AS dau,
                COUNT(DISTINCT session_id)                                   AS sessions,
                COUNT(*)                                                     AS events
            FROM analytics_events
            WHERE {where_sql}
        """), params).fetchone()

        # Average session duration: для каждой сессии считаем (max - min) client_ts.
        avg_session = conn.execute(text(f"""
            WITH sess AS (
                SELECT session_id,
                       EXTRACT(EPOCH FROM (MAX(client_ts) - MIN(client_ts)))::int AS dur
                FROM analytics_events
                WHERE {where_sql}
                GROUP BY session_id
                HAVING EXTRACT(EPOCH FROM (MAX(client_ts) - MIN(client_ts))) > 0
            )
            SELECT COALESCE(AVG(dur), 0)::int AS avg_dur, COUNT(*) AS sess_count FROM sess
        """), params).fetchone()
        avg_session_sec = int(avg_session[0]) if avg_session and avg_session[0] else 0

        # Previous period для delta
        prev_params = {**params, "period_start": prev_period_start, "period_end": period_start}
        # Replace condition: server_ts BETWEEN prev_start AND period_start
        prev_where_sql = where_sql.replace(
            "server_ts >= :period_start",
            "server_ts >= :period_start AND server_ts < :period_end",
        )
        prev_summary = conn.execute(text(f"""
            SELECT
                COUNT(DISTINCT COALESCE(user_id::text, session_id)) AS dau,
                COUNT(DISTINCT session_id)                          AS sessions,
                COUNT(*)                                            AS events
            FROM analytics_events
            WHERE {prev_where_sql}
        """), prev_params).fetchone()
        prev_avg_row = conn.execute(text(f"""
            WITH sess AS (
                SELECT session_id,
                       EXTRACT(EPOCH FROM (MAX(client_ts) - MIN(client_ts)))::int AS dur
                FROM analytics_events
                WHERE {prev_where_sql}
                GROUP BY session_id
                HAVING EXTRACT(EPOCH FROM (MAX(client_ts) - MIN(client_ts))) > 0
            )
            SELECT COALESCE(AVG(dur), 0)::int AS avg_dur FROM sess
        """), prev_params).fetchone()
        prev_avg_sec = int(prev_avg_row[0]) if prev_avg_row and prev_avg_row[0] else 0

        # === Trends (per-day series) ===
        trends_rows = conn.execute(text(f"""
            SELECT server_ts::date AS day,
                   COUNT(DISTINCT COALESCE(user_id::text, session_id)) AS dau,
                   COUNT(DISTINCT session_id) AS sessions
            FROM analytics_events
            WHERE {where_sql}
            GROUP BY server_ts::date
            ORDER BY day
        """), params).fetchall()

        # === Top pages ===
        top_pages = conn.execute(text(f"""
            SELECT event_path AS path, COUNT(*) AS views
            FROM analytics_events
            WHERE {where_sql}
              AND event_type = 'pageview'
              AND event_path IS NOT NULL
            GROUP BY event_path
            ORDER BY views DESC
            LIMIT 10
        """), params).fetchall()

        # === Top instruments (instrument_select events) ===
        top_instruments = conn.execute(text(f"""
            SELECT payload->>'secid' AS secid, COUNT(*) AS selects
            FROM analytics_events
            WHERE {where_sql}
              AND event_type = 'instrument_select'
              AND payload->>'secid' IS NOT NULL
            GROUP BY payload->>'secid'
            ORDER BY selects DESC
            LIMIT 10
        """), params).fetchall()

        # === Top exports ===
        top_exports = conn.execute(text(f"""
            SELECT payload->>'indicator' AS indicator, COUNT(*) AS count
            FROM analytics_events
            WHERE {where_sql}
              AND event_type = 'chart_export'
              AND payload->>'indicator' IS NOT NULL
            GROUP BY payload->>'indicator'
            ORDER BY count DESC
            LIMIT 10
        """), params).fetchall()

        # === Mode distribution (seasonality_mode events) ===
        mode_dist = conn.execute(text(f"""
            SELECT payload->>'mode' AS mode, COUNT(*) AS count
            FROM analytics_events
            WHERE {where_sql}
              AND event_type = 'seasonality_mode'
              AND payload->>'mode' IS NOT NULL
            GROUP BY payload->>'mode'
            ORDER BY count DESC
        """), params).fetchall()

    def pct_delta(curr: float, prev: float) -> Optional[int]:
        if prev <= 0:
            return None
        return int(round((curr - prev) / prev * 100))

    return {
        "period_days": days,
        "segment": segment,
        "device": device,
        "summary": {
            "dau": int(summary_row[0]) if summary_row else 0,
            "sessions": int(summary_row[1]) if summary_row else 0,
            "events": int(summary_row[2]) if summary_row else 0,
            "avg_session_sec": avg_session_sec,
            "delta_dau": (int(summary_row[0]) if summary_row else 0) - (int(prev_summary[0]) if prev_summary else 0),
            "delta_sessions_pct": pct_delta(
                int(summary_row[1]) if summary_row else 0,
                int(prev_summary[1]) if prev_summary else 0,
            ),
            "delta_events_pct": pct_delta(
                int(summary_row[2]) if summary_row else 0,
                int(prev_summary[2]) if prev_summary else 0,
            ),
            "delta_avg_session_sec": avg_session_sec - prev_avg_sec,
        },
        "trends": [
            {"date": r[0].isoformat(), "dau": int(r[1]), "sessions": int(r[2])}
            for r in trends_rows
        ],
        "top_pages": [{"path": r[0], "views": int(r[1])} for r in top_pages],
        "top_instruments": [{"secid": r[0], "selects": int(r[1])} for r in top_instruments],
        "top_exports": [{"indicator": r[0], "count": int(r[1])} for r in top_exports],
        "mode_distribution": [{"mode": r[0], "count": int(r[1])} for r in mode_dist],
    }
