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
# Только типы, которые фронт реально шлёт (см. AnalyticsContext + track()-вызовы).
# Мёртвые типы (indicator_view / chart_annotate / period_change / session_end)
# удалены 2026-06-16 — никогда не отправлялись.
ALLOWED_EVENT_TYPES = {
    "pageview",
    "instrument_select",
    "seasonality_mode",
    "chart_export",
    "theme_toggle",
    "session_heartbeat",
}


# Канон ключей indicator в событиях chart_export. Ключ берётся из имени файла
# экспорта ("frame-<indicator>-..."), и за историю накопился дрейф: один и тот
# же индикатор писался по-разному. Маппим алиас → канон, чтобы «Топ экспортов»
# не двоился (нормализация в SQL, см. _export_indicator_canon_sql).
EXPORT_INDICATOR_ALIASES = {
    "open_interest": "oi",
    "fund": "funds",
    "funds_money": "funds",
}


def _export_indicator_canon_sql(expr: str) -> str:
    """SQL-выражение CASE, сводящее алиасы indicator к канону.

    `expr` — SQL-выражение, дающее сырой indicator (e.g. "payload->>'indicator'").
    Возвращает CASE ... END. Ключи/значения берём из EXPORT_INDICATOR_ALIASES —
    хардкодим в строку безопасно: значения это фикс-литералы из кода (не user input).
    """
    whens = "\n".join(
        f"                WHEN {expr} = '{alias}' THEN '{canon}'"
        for alias, canon in EXPORT_INDICATOR_ALIASES.items()
    )
    return f"""CASE
{whens}
                ELSE {expr}
            END"""


def _detect_device(user_agent: str) -> str:
    """Грубая классификация по UA. Не для precision, только для распределения.

    Серверный fallback для device, когда фронт не прислал свой
    (см. post_events: `ev.device or device`). Tablet проверяем ПЕРВЫМ,
    т.к. iPad/Android-tablet UA содержат и mobi/android-токены.
    """
    ua = (user_agent or "").lower()
    if "bot" in ua or "spider" in ua or "crawl" in ua:
        return "bot"
    if "tablet" in ua or "ipad" in ua:
        return "tablet"
    # Явные скобки: без них `and` связывал бы крепче `or` и менял семантику.
    if "mobi" in ua or "iphone" in ua or ("android" in ua and "mobile" in ua):
        return "mobile"
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

    # Сессионная длительность считается ОДИНАКОВО для summary.sessions и avg:
    # суммируем gap'ы между соседними событиями сессии, КАПНУВ каждый gap на
    # SESSION_GAP_CAP_SEC (5 мин — стандарт GA). Без капа heartbeat'ы в фоновых
    # вкладках (вкладка открыта часами) раздували (MAX-MIN) до многочасовых
    # «сессий». Сессии с единственным событием имеют dur=0, но ВХОДЯТ в набор —
    # один знаменатель и для «Сессий», и для «Среднего времени».
    SESSION_GAP_CAP_SEC = 300

    def _session_durations_cte(w_sql: str) -> str:
        return f"""
            sess AS (
                SELECT session_id,
                       COALESCE(SUM(
                           LEAST(
                               EXTRACT(EPOCH FROM (client_ts - prev_ts)),
                               {SESSION_GAP_CAP_SEC}
                           )
                       ), 0)::int AS dur
                FROM (
                    SELECT session_id,
                           client_ts,
                           LAG(client_ts) OVER (
                               PARTITION BY session_id ORDER BY client_ts
                           ) AS prev_ts
                    FROM analytics_events
                    WHERE {w_sql}
                ) ordered
                GROUP BY session_id
            )
        """

    with engine.connect() as conn:
        # === Summary === (dau/events — прямой счёт; sessions — из того же
        # набора сессий, что и avg, чтобы знаменатели совпадали)
        summary_row = conn.execute(text(f"""
            WITH {_session_durations_cte(where_sql)}
            SELECT
                (SELECT COUNT(DISTINCT COALESCE(user_id::text, session_id))
                   FROM analytics_events WHERE {where_sql})  AS uniques,
                (SELECT COUNT(*) FROM sess)                   AS sessions,
                (SELECT COUNT(*) FROM analytics_events WHERE {where_sql}) AS events,
                (SELECT COALESCE(AVG(dur), 0)::int FROM sess) AS avg_dur
            FROM (SELECT 1) _
        """), params).fetchone()
        avg_session_sec = int(summary_row[3]) if summary_row and summary_row[3] else 0

        # Previous period для delta
        prev_params = {**params, "period_start": prev_period_start, "period_end": period_start}
        # Replace condition: server_ts BETWEEN prev_start AND period_start
        prev_where_sql = where_sql.replace(
            "server_ts >= :period_start",
            "server_ts >= :period_start AND server_ts < :period_end",
        )
        prev_summary = conn.execute(text(f"""
            WITH {_session_durations_cte(prev_where_sql)}
            SELECT
                (SELECT COUNT(DISTINCT COALESCE(user_id::text, session_id))
                   FROM analytics_events WHERE {prev_where_sql})  AS uniques,
                (SELECT COUNT(*) FROM sess)                        AS sessions,
                (SELECT COUNT(*) FROM analytics_events WHERE {prev_where_sql}) AS events,
                (SELECT COALESCE(AVG(dur), 0)::int FROM sess)      AS avg_dur
            FROM (SELECT 1) _
        """), prev_params).fetchone()
        prev_avg_sec = int(prev_summary[3]) if prev_summary and prev_summary[3] else 0

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
        # Нормализуем алиасы indicator в SQL (CASE), чтобы один индикатор не
        # двоился из-за исторического дрейфа ключей в filename экспорта:
        #   open_interest → oi,  fund / funds_money → funds.
        # См. EXPORT_INDICATOR_ALIASES. Группируем уже по канону → чинит и историю.
        top_exports = conn.execute(text(f"""
            WITH normalized AS (
                SELECT {_export_indicator_canon_sql("payload->>'indicator'")} AS indicator
                FROM analytics_events
                WHERE {where_sql}
                  AND event_type = 'chart_export'
                  AND payload->>'indicator' IS NOT NULL
            )
            SELECT indicator, COUNT(*) AS count
            FROM normalized
            GROUP BY indicator
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
            # «uniques» — уникальные посетители ЗА ВЕСЬ период (НЕ дневной DAU).
            # Это COUNT(DISTINCT user_id|session_id) по всему окну. Гость = одна
            # вкладка (session_id умирает при закрытии), поэтому это ближе к
            # «визитам-уникам», чем к людям — фронт подписывает честно.
            "uniques": int(summary_row[0]) if summary_row else 0,
            "sessions": int(summary_row[1]) if summary_row else 0,
            "events": int(summary_row[2]) if summary_row else 0,
            "avg_session_sec": avg_session_sec,
            "delta_uniques": (int(summary_row[0]) if summary_row else 0) - (int(prev_summary[0]) if prev_summary else 0),
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
            # Per-day: «uniques» здесь это настоящий дневной distinct-count
            # (GROUP BY server_ts::date) — честный daily-unique по дням.
            {"date": r[0].isoformat(), "uniques": int(r[1]), "sessions": int(r[2])}
            for r in trends_rows
        ],
        "top_pages": [{"path": r[0], "views": int(r[1])} for r in top_pages],
        "top_instruments": [{"secid": r[0], "selects": int(r[1])} for r in top_instruments],
        "top_exports": [{"indicator": r[0], "count": int(r[1])} for r in top_exports],
        "mode_distribution": [{"mode": r[0], "count": int(r[1])} for r in mode_dist],
    }


# ════════════════════════════════════════════════════════════════════════════
# GET /funnel — step-by-step конверсия
# ════════════════════════════════════════════════════════════════════════════
#
# Параметр steps — список шагов через запятую, формат:
#   "pageview:/,pageview:/seasonality,instrument_select,chart_export"
#
# Шаг = "event_type" ИЛИ "event_type:event_path".
# Для каждой сессии определяем последовательность достигнутых шагов
# (in-order, как в funnel — нужно дойти до step N через все предыдущие).
# Возвращаем число сессий на каждом шаге.

@router.get("/funnel")
async def get_funnel(
    steps: str = Query(..., description="Шаги через запятую: 'pageview:/,instrument_select'"),
    days: int = Query(7, ge=1, le=180),
    user=Depends(require_admin),
):
    """Conversion funnel: для каждого step возвращает число сессий, дошедших до него.

    Логика in-order: сессия засчитывается в step N только если она прошла steps 0..N-1
    (хотя бы по одному event на каждом шаге, и в правильном временном порядке).
    """
    period_start = datetime.utcnow() - timedelta(days=days)

    # Parse steps
    parsed_steps: list[tuple[str, Optional[str]]] = []
    for part in steps.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            t, p = part.split(":", 1)
            parsed_steps.append((t.strip(), p.strip()))
        else:
            parsed_steps.append((part, None))

    if not parsed_steps:
        raise HTTPException(400, "steps must contain at least one step")
    if len(parsed_steps) > 8:
        raise HTTPException(400, "max 8 steps")

    engine = get_engine()
    with engine.connect() as conn:
        # Для каждой сессии считаем earliest timestamp каждого шага.
        # Сессия засчитывается в step N если ts(N) > ts(N-1) > ... > ts(0).
        cte_parts = []
        params: dict[str, Any] = {"period_start": period_start}
        for i, (etype, epath) in enumerate(parsed_steps):
            params[f"t{i}"] = etype
            path_clause = ""
            if epath is not None:
                params[f"p{i}"] = epath
                path_clause = f" AND event_path = :p{i}"
            cte_parts.append(f"""
                step_{i} AS (
                    SELECT session_id, MIN(server_ts) AS ts
                    FROM analytics_events
                    WHERE server_ts >= :period_start
                      AND event_type = :t{i}{path_clause}
                    GROUP BY session_id
                )
            """)

        # JOIN всех CTE с условием прогресса по времени
        cte_sql = ", ".join(cte_parts)
        # Step 0 — все сессии прошедшие первый шаг
        # Step N (N>0) — те которые прошли N-1 + step N произошёл позже
        step_counts: list[dict] = []
        with conn.begin():
            for i, (etype, epath) in enumerate(parsed_steps):
                where_chain = ""
                joins = ""
                for k in range(i + 1):
                    if k == 0:
                        joins += f" FROM step_0"
                    else:
                        joins += f" JOIN step_{k} ON step_{k}.session_id = step_0.session_id"
                        where_chain += f" AND step_{k}.ts > step_{k - 1}.ts"
                query = text(f"""
                    WITH {cte_sql}
                    SELECT COUNT(DISTINCT step_0.session_id) {joins}
                    WHERE 1=1 {where_chain}
                """)
                result = conn.execute(query, params).fetchone()
                count = int(result[0]) if result else 0
                label = f"{etype}:{epath}" if epath else etype
                step_counts.append({"step": i, "label": label, "sessions": count})

    # Считаем conversion rate по отношению к step 0
    base = step_counts[0]["sessions"] if step_counts else 0
    for s in step_counts:
        s["conversion_pct"] = round(s["sessions"] / base * 100, 1) if base > 0 else 0.0

    return {
        "period_days": days,
        "steps": step_counts,
    }


# ════════════════════════════════════════════════════════════════════════════
# A/B testing — УДАЛЕНО (мёртвый код, 2026-06-16)
# ════════════════════════════════════════════════════════════════════════════
# Вся A/B-цепочка УДАЛЕНА 2026-06-16 как мёртвый код: фронт-хук useExperiment
# (0 вызовов) + getABAssignment/AssignResponse в services/api.ts + здешний
# эндпоинт GET /experiments/{name}/assign (assign_variant) — последний reader.
# Ранее в этот день удалены /cohort, /realtime и CRUD /experiments.
# Таблицы БД `ab_experiments` / `ab_assignments` теперь НИГДЕ не используются →
# можно дропнуть отдельной follow-up миграцией (DROP TABLE ab_assignments,
# ab_experiments) — по правилам проекта миграции отдельно от кода.


# ════════════════════════════════════════════════════════════════════════════
# GET /alerts-stats — admin-only трекинг алертов (поставили/убрали/конверсия)
# ════════════════════════════════════════════════════════════════════════════
#
# Источники:
#   alert_events  — лог жизненного цикла (created/deleted/paused/resumed) за период
#   alerts        — текущее состояние (active_now, by_source GROUP BY source)
#   alert_fires   — конверсия «сколько алертов хоть раз сработали» (DISTINCT alert_id)
#
# Денормализованные asset/indicator/source в alert_events → статистику строим
# без джойна на (возможно удалённый) алерт. Запросы агрегатные, без N+1.

@router.get("/alerts-stats")
async def get_alerts_stats(
    days: int = Query(7, ge=1, le=180, description="Период (дней) для created/deleted/paused/resumed"),
    user=Depends(require_admin),
):
    """Трекинг алертов для admin-stats: что люди ставят/убирают + конверсия.

    Возвращает:
      period_days  — окно для событий
      created/deleted/paused/resumed — COUNT по alert_events.event за period
      active_now   — COUNT alerts WHERE status='active' (текущее состояние)
      with_fires   — COUNT DISTINCT alert_id из alert_fires (хоть раз сработали)
      by_source    — active alerts GROUP BY source (разбивка кабинета)
      top_assets   — топ активов по числу активных алертов
    """
    engine = get_engine()
    period_start = datetime.utcnow() - timedelta(days=days)

    with engine.connect() as conn:
        # === События за период (один проход по alert_events) ===
        # FILTER (WHERE …) — агрегируем все 4 типа в одном запросе, без N+1.
        ev_row = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE event = 'created')  AS created,
                COUNT(*) FILTER (WHERE event = 'deleted')  AS deleted,
                COUNT(*) FILTER (WHERE event = 'paused')   AS paused,
                COUNT(*) FILTER (WHERE event = 'resumed')  AS resumed
            FROM alert_events
            WHERE created_at >= :period_start
        """), {"period_start": period_start}).fetchone()

        # === Текущее состояние (НЕ за период — снимок «сейчас») ===
        active_now = conn.execute(text("""
            SELECT COUNT(*) FROM alerts WHERE status = 'active'
        """)).scalar() or 0

        # Конверсия: сколько алертов хоть раз сработали. DISTINCT alert_id из
        # alert_fires — alert_fires.alert_id FK на alerts (каскад при удалении),
        # поэтому это «живые» алерты, что когда-либо стреляли.
        with_fires = conn.execute(text("""
            SELECT COUNT(DISTINCT alert_id) FROM alert_fires
        """)).scalar() or 0

        # Разбивка активных алертов по источнику (oi / funds / …).
        by_source_rows = conn.execute(text("""
            SELECT COALESCE(source, 'oi') AS source, COUNT(*) AS active
            FROM alerts
            WHERE status = 'active'
            GROUP BY COALESCE(source, 'oi')
            ORDER BY active DESC
        """)).fetchall()

        # Топ активов по числу активных алертов.
        top_assets_rows = conn.execute(text("""
            SELECT asset, COUNT(*) AS count
            FROM alerts
            WHERE status = 'active' AND asset IS NOT NULL
            GROUP BY asset
            ORDER BY count DESC
            LIMIT 10
        """)).fetchall()

    return {
        "period_days": days,
        "created": int(ev_row[0]) if ev_row else 0,
        "deleted": int(ev_row[1]) if ev_row else 0,
        "paused": int(ev_row[2]) if ev_row else 0,
        "resumed": int(ev_row[3]) if ev_row else 0,
        "active_now": int(active_now),
        "with_fires": int(with_fires),
        "by_source": [{"source": r[0], "active": int(r[1])} for r in by_source_rows],
        "top_assets": [{"asset": r[0], "count": int(r[1])} for r in top_assets_rows],
    }


# ════════════════════════════════════════════════════════════════════════════
# USERS — admin user inspection panel
# ════════════════════════════════════════════════════════════════════════════
#
# GET /users        — list всех зарегистрированных + aggregated stats
# GET /users/{id}   — detail одного user'а: profile + activity timeline + tops
#
# Privacy: данные включают email и активность — только role=admin.

@router.get("/users")
async def list_users(
    days: int = Query(30, ge=1, le=365, description="За сколько дней считать stats"),
    sort: str = Query("last_active", description="last_active / events / sessions / created"),
    search: str = Query("", description="Поиск по email или display_name"),
    user=Depends(require_admin),
):
    """List всех users с aggregated stats за последние N дней.

    Возвращает: id, email, display_name, role, created_at, last_login_at,
    is_active, oauth_provider, plan (active subscription tier),
    sessions_count, events_count, last_active_ts (из analytics_events),
    countries (последние 3 уникальных).
    """
    engine = get_engine()
    cutoff = datetime.utcnow() - timedelta(days=days)
    search_clean = (search or "").strip()

    # Order by mapping
    order_clauses = {
        "last_active": "last_active_ts DESC NULLS LAST",
        "events": "events_count DESC",
        "sessions": "sessions_count DESC",
        "created": "u.created_at DESC",
    }
    order_by = order_clauses.get(sort, order_clauses["last_active"])

    where_search = ""
    params: dict[str, Any] = {"cutoff": cutoff}
    if search_clean:
        where_search = " AND (u.email ILIKE :q OR u.display_name ILIKE :q OR u.username ILIKE :q)"
        params["q"] = f"%{search_clean}%"

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                u.id, u.email, u.display_name, u.username, u.role,
                u.is_active, u.is_verified, u.created_at, u.last_login_at,
                u.oauth_provider, u.avatar_url,
                (SELECT s.tier FROM subscriptions s
                  WHERE s.user_id = u.id AND s.status = 'active'
                  ORDER BY s.created_at DESC LIMIT 1) AS plan,
                (SELECT COUNT(DISTINCT ae.session_id) FROM analytics_events ae
                  WHERE ae.user_id = u.id AND ae.server_ts >= :cutoff) AS sessions_count,
                (SELECT COUNT(*) FROM analytics_events ae
                  WHERE ae.user_id = u.id AND ae.server_ts >= :cutoff) AS events_count,
                (SELECT MAX(ae.server_ts) FROM analytics_events ae
                  WHERE ae.user_id = u.id) AS last_active_ts
            FROM users u
            WHERE 1=1 {where_search}
            ORDER BY {order_by}
            LIMIT 200
        """), params).fetchall()

    return {
        "period_days": days,
        "users": [
            {
                "id": int(r[0]),
                "email": r[1],
                "display_name": r[2],
                "username": r[3],
                "role": r[4],
                "is_active": bool(r[5]),
                "is_verified": bool(r[6]),
                "created_at": r[7].isoformat() if r[7] else None,
                "last_login_at": r[8].isoformat() if r[8] else None,
                "oauth_provider": r[9],
                "avatar_url": r[10],
                "plan": r[11],
                "sessions_count": int(r[12]) if r[12] else 0,
                "events_count": int(r[13]) if r[13] else 0,
                "last_active_ts": r[14].isoformat() if r[14] else None,
            }
            for r in rows
        ],
    }


@router.get("/users/{user_id}")
async def user_detail(
    user_id: int,
    days: int = Query(30, ge=1, le=180),
    user=Depends(require_admin),
):
    """Детальный профиль одного user'а:
       - basic info + subscriptions
       - summary metrics (sessions/events/avg_session/total_time)
       - activity timeline (последние 100 events)
       - top pages / instruments / exports
       - device + country distribution
    """
    engine = get_engine()
    cutoff = datetime.utcnow() - timedelta(days=days)

    with engine.connect() as conn:
        # Basic profile
        prof = conn.execute(text("""
            SELECT id, email, display_name, username, role, is_active, is_verified,
                   created_at, updated_at, last_login_at, last_login_ip,
                   oauth_provider, oauth_id, avatar_url
            FROM users WHERE id = :id
        """), {"id": user_id}).fetchone()
        if not prof:
            raise HTTPException(404, "Пользователь не найден")

        # All subscriptions (история)
        subs = conn.execute(text("""
            SELECT id, tier, period, plan_id, amount, currency, status,
                   started_at, expires_at, cancelled_at, created_at
            FROM subscriptions
            WHERE user_id = :id
            ORDER BY created_at DESC
        """), {"id": user_id}).fetchall()

        # Summary metrics
        summary = conn.execute(text("""
            SELECT
                COUNT(*) AS events,
                COUNT(DISTINCT session_id) AS sessions,
                MIN(server_ts) AS first_active,
                MAX(server_ts) AS last_active
            FROM analytics_events
            WHERE user_id = :id AND server_ts >= :cutoff
        """), {"id": user_id, "cutoff": cutoff}).fetchone()

        # Капаем gap между событиями (5 мин), как в /stats — иначе heartbeat
        # в фоновой вкладке раздувает (MAX-MIN). avg И total считаем по ОДНОМУ
        # набору сессий (включая одно-событийные с dur=0 → совпадает с sessions).
        avg_session = conn.execute(text("""
            WITH sess AS (
                SELECT session_id,
                       COALESCE(SUM(
                           LEAST(EXTRACT(EPOCH FROM (client_ts - prev_ts)), 300)
                       ), 0)::int AS dur
                FROM (
                    SELECT session_id,
                           client_ts,
                           LAG(client_ts) OVER (
                               PARTITION BY session_id ORDER BY client_ts
                           ) AS prev_ts
                    FROM analytics_events
                    WHERE user_id = :id AND server_ts >= :cutoff
                ) ordered
                GROUP BY session_id
            )
            SELECT COALESCE(AVG(dur), 0)::int, COALESCE(SUM(dur), 0)::int FROM sess
        """), {"id": user_id, "cutoff": cutoff}).fetchone()

        # Activity timeline (последние 100 events)
        timeline = conn.execute(text("""
            SELECT event_type, event_path, payload, server_ts, ip_country, device, session_id
            FROM analytics_events
            WHERE user_id = :id AND server_ts >= :cutoff
            ORDER BY server_ts DESC
            LIMIT 100
        """), {"id": user_id, "cutoff": cutoff}).fetchall()

        # Top pages
        top_pages = conn.execute(text("""
            SELECT event_path, COUNT(*) AS views
            FROM analytics_events
            WHERE user_id = :id AND server_ts >= :cutoff
              AND event_type = 'pageview' AND event_path IS NOT NULL
            GROUP BY event_path
            ORDER BY views DESC
            LIMIT 10
        """), {"id": user_id, "cutoff": cutoff}).fetchall()

        # Top instruments selected
        top_instruments = conn.execute(text("""
            SELECT payload->>'secid' AS secid, COUNT(*) AS selects
            FROM analytics_events
            WHERE user_id = :id AND server_ts >= :cutoff
              AND event_type = 'instrument_select'
              AND payload->>'secid' IS NOT NULL
            GROUP BY payload->>'secid'
            ORDER BY selects DESC
            LIMIT 10
        """), {"id": user_id, "cutoff": cutoff}).fetchall()

        # Top exports — нормализуем алиасы indicator к канону (как в /stats),
        # чтобы один индикатор не двоился (open_interest→oi, fund/funds_money→funds).
        top_exports = conn.execute(text(f"""
            WITH normalized AS (
                SELECT {_export_indicator_canon_sql("payload->>'indicator'")} AS indicator
                FROM analytics_events
                WHERE user_id = :id AND server_ts >= :cutoff
                  AND event_type = 'chart_export'
                  AND payload->>'indicator' IS NOT NULL
            )
            SELECT indicator, COUNT(*) AS count
            FROM normalized
            GROUP BY indicator
            ORDER BY count DESC
            LIMIT 10
        """), {"id": user_id, "cutoff": cutoff}).fetchall()

        # Devices
        devices = conn.execute(text("""
            SELECT device, COUNT(DISTINCT session_id) AS sessions
            FROM analytics_events
            WHERE user_id = :id AND server_ts >= :cutoff AND device IS NOT NULL
            GROUP BY device
            ORDER BY sessions DESC
        """), {"id": user_id, "cutoff": cutoff}).fetchall()

        # Countries
        countries = conn.execute(text("""
            SELECT ip_country, COUNT(DISTINCT session_id) AS sessions
            FROM analytics_events
            WHERE user_id = :id AND server_ts >= :cutoff AND ip_country IS NOT NULL
            GROUP BY ip_country
            ORDER BY sessions DESC
        """), {"id": user_id, "cutoff": cutoff}).fetchall()

    return {
        "user": {
            "id": int(prof[0]),
            "email": prof[1],
            "display_name": prof[2],
            "username": prof[3],
            "role": prof[4],
            "is_active": bool(prof[5]),
            "is_verified": bool(prof[6]),
            "created_at": prof[7].isoformat() if prof[7] else None,
            "updated_at": prof[8].isoformat() if prof[8] else None,
            "last_login_at": prof[9].isoformat() if prof[9] else None,
            "last_login_ip": prof[10],  # admin видит — это для security audit
            "oauth_provider": prof[11],
            "oauth_id": prof[12],
            "avatar_url": prof[13],
        },
        "subscriptions": [
            {
                "id": int(s[0]),
                "tier": s[1], "period": s[2], "plan_id": s[3],
                "amount": float(s[4]) if s[4] is not None else 0,
                "currency": s[5], "status": s[6],
                "started_at": s[7].isoformat() if s[7] else None,
                "expires_at": s[8].isoformat() if s[8] else None,
                "cancelled_at": s[9].isoformat() if s[9] else None,
                "created_at": s[10].isoformat() if s[10] else None,
            } for s in subs
        ],
        "summary": {
            "period_days": days,
            "events": int(summary[0]) if summary else 0,
            "sessions": int(summary[1]) if summary else 0,
            "first_active_ts": summary[2].isoformat() if summary and summary[2] else None,
            "last_active_ts": summary[3].isoformat() if summary and summary[3] else None,
            "avg_session_sec": int(avg_session[0]) if avg_session and avg_session[0] else 0,
            "total_time_sec": int(avg_session[1]) if avg_session and avg_session[1] else 0,
        },
        "timeline": [
            {
                "event_type": t[0],
                "event_path": t[1],
                "payload": t[2],
                "server_ts": t[3].isoformat(),
                "country": t[4],
                "device": t[5],
                "session_id": t[6],
            } for t in timeline
        ],
        "top_pages": [{"path": r[0], "views": int(r[1])} for r in top_pages],
        "top_instruments": [{"secid": r[0], "selects": int(r[1])} for r in top_instruments],
        "top_exports": [{"indicator": r[0], "count": int(r[1])} for r in top_exports],
        "devices": [{"device": r[0], "sessions": int(r[1])} for r in devices],
        "countries": [{"country": r[0], "sessions": int(r[1])} for r in countries],
    }
