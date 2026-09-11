"""
Analytics API — приём событий с фронта + admin-stats.

Endpoints:
- POST /api/usage/log         — batch insert events (любой user, в т.ч. гость)
- POST /api/analytics/event   — тот же приём, легаси-путь (см. ingest_router)
- GET  /api/analytics/stats   — aggregated metrics (admin only)

Принципы:
- Никаких PII в логах (event payload не должен содержать email/name/etc.)
- Fire-and-forget: ошибка БД не должна ломать UI → возвращаем 204 даже при partial fail
- Опт-аут через client (cookie / Profile setting) — frontend сам не отправляет когда optout
- Retention 180 дней — cleanup в orchestrator
"""
from __future__ import annotations

import re
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import text

from api.cache import get_or_compute
from api.database import get_engine
from api.logger import get_logger
from api.routers.auth import get_current_user_optional, require_admin

log = get_logger()
router = APIRouter(prefix="/api/analytics", tags=["analytics"])

# Приём событий висит на нейтральном пути. Прежний /api/analytics/event попадал
# под стандартные списки блокировщиков рекламы (EasyPrivacy и клоны режут URL со
# словом analytics), из-за чего часть браузеров молчала: события не уходили, и в
# логах это было неотличимо от отказа. Старый путь оставлен рабочим — по нему
# ходят вкладки со старым бандлом из кэша.
ingest_router = APIRouter(prefix="/api/usage", tags=["analytics"])


# ════════════════════════════════════════════════════════════════════════════
# POST /usage/log — batch event ingestion
# ════════════════════════════════════════════════════════════════════════════

class AnalyticsEvent(BaseModel):
    """Одно событие. Frontend отправляет batch'ем 1-50 за раз."""
    session_id: str = Field(..., min_length=36, max_length=36)  # UUID v4
    # Постоянный ID браузера (localStorage + cookie на год). Нужен, чтобы гость
    # считался человеком, а не вкладкой, и чтобы видеть возвраты. Необязателен:
    # вкладки со старым бандлом его не шлют.
    visitor_id: Optional[str] = Field(None, min_length=36, max_length=36)
    event_type: str = Field(..., min_length=1, max_length=50)
    event_path: Optional[str] = Field(None, max_length=255)
    payload: Optional[dict[str, Any]] = None
    client_ts: datetime
    device: Optional[str] = Field(None, max_length=20)
    # IANA-зона браузера (Europe/Moscow). Из неё берём страну, когда прокси не
    # прислал заголовок с кодом — точный IP по-прежнему не смотрим.
    tz: Optional[str] = Field(None, max_length=64)


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
    # Просмотр актива на индикаторе: {secid, indicator}. Шлётся при каждой смене
    # актива, откуда бы он ни пришёл (поиск, ссылка, сохранённый выбор, мобильный
    # пикер) — в отличие от instrument_select, который ловит только поиск.
    "asset_view",
    # Воронка монетизации (намерение → оплата/триал) — добавлено 2026-06-27
    "checkout_start",
    "trial_start",
    "purchase_success",
    "trial_activated",
    # Согласие: тумблер в профиле. Пишем сам факт смены выбора, без payload —
    # иначе отказ виден только как тишина и его невозможно посчитать.
    "consent_optout",
    "consent_optin",
}

# Типы, которые принимаем ВНЕ зависимости от согласия: это не наблюдение за
# поведением, а запись самого решения по приватности.
CONSENT_EVENT_TYPES = {"consent_optout", "consent_optin"}


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


# Роботы и безголовые браузеры. После отказа от гейта по согласию события
# шлёт любой, кто исполняет JS: рендер-боты поисковиков, Lighthouse, наши же
# Playwright-туры. Такие запросы не пишем вовсе.
_BOT_UA = re.compile(
    r"bot|spider|crawl|slurp|headless|lighthouse|pagespeed|phantom|puppeteer|"
    r"playwright|selenium|prerender|yandex(?:metrika|renderresources|screenshot)|"
    r"python-requests|curl/|wget/",
    re.IGNORECASE,
)


def _is_bot(user_agent: str) -> bool:
    return not user_agent or bool(_BOT_UA.search(user_agent))


def _detect_country(req: Request) -> Optional[str]:
    """Извлекает country code из proxy headers если есть.
    nginx может прокидывать через CF-IPCountry / X-Country-Code.
    Без этого — None (точный IP мы НЕ парсим, это privacy violation)."""
    for header in ("CF-IPCountry", "X-Country-Code", "X-Geo-Country"):
        v = req.headers.get(header)
        if v and len(v) == 2:
            return v.upper()
    return None


def _build_tz_country_map() -> dict[str, str]:
    """IANA-зона → ISO-код страны, из таблиц pytz (обратный country_timezones).

    Нужен, потому что наш nginx страну не отдаёт: geoip2-модуля в образе нет,
    а поднимать базу MaxMind ради одного разреза не стоит. Зона браузера даёт
    ту же географию и не требует смотреть на IP.
    """
    out: dict[str, str] = {}
    try:
        import pytz

        for code, zones in pytz.country_timezones.items():
            for zone in zones:
                out[zone] = code.upper()
    except Exception:  # pragma: no cover — без pytz просто остаёмся без страны
        pass
    return out


_TZ_COUNTRY = _build_tz_country_map()


def _country_from_tz(tz: Optional[str]) -> Optional[str]:
    """Страна по зоне браузера. Неизвестная/подменённая зона → None."""
    if not tz:
        return None
    return _TZ_COUNTRY.get(tz.strip())


@ingest_router.post("/log", status_code=204)
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
    if _is_bot(user_agent):
        return None
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
                            (user_id, session_id, visitor_id, event_type, event_path,
                             payload, client_ts, ip_country, device)
                        VALUES (:user_id, :session_id, :visitor_id, :event_type, :event_path,
                                :payload, :client_ts, :country, :device)
                    """),
                    {
                        "user_id": user_id,
                        "session_id": ev.session_id,
                        "visitor_id": ev.visitor_id,
                        "event_type": ev.event_type,
                        "event_path": ev.event_path,
                        "payload": _serialize_jsonb(ev.payload),
                        "client_ts": ev.client_ts,
                        # Заголовок прокси приоритетнее: зона браузера легко
                        # съезжает (VPN, ручная настройка часов).
                        "country": country or _country_from_tz(ev.tz),
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
#
# Определения (переписаны 2026-09-11 после сверки с Яндекс Метрикой):
#
#   Посетитель — человек, насколько мы можем его узнать: аккаунт, если он
#     хоть раз входил с этого браузера; иначе постоянный ID браузера
#     (visitor_id, живёт в localStorage и cookie год); для старых событий без
#     visitor_id — вкладка (session_id). Гостевые события до входа склеиваются
#     с аккаунтом по вкладке и по браузеру, поэтому человек не двоится.
#   Визит — как в Метрике: серия действий одного посетителя, разрыв дольше
#     30 минут начинает новый визит. Вкладки одного человека сливаются в один
#     визит, открытая сутками вкладка режется на отдельные заходы.
#   Время визита — от первого до последнего события визита. Пульс
#     (session_heartbeat) уходит только пока вкладка на экране И человек что-то
#     делал за последние 5 минут (флаг act в payload). Старые пульсы без флага
#     слались и в простаивающей вкладке, поэтому они засчитываются только в
#     пределах 30 минут после настоящего действия.
#   Отказ — как в Метрике: визит с одним просмотром короче 15 секунд.
#   Дни — по московскому времени.
#
# По умолчанию админы исключены: их вкладки открыты часами и раньше давали
# пятую часть всего «времени на сайте».

# Москва без перехода на летнее время с 2014 — фиксированный сдвиг надёжнее,
# чем tzdata в slim-образе. В SQL зона берётся из базы Postgres.
MSK = timezone(timedelta(hours=3))
VISIT_GAP_MIN = 30
BOUNCE_MAX_SEC = 15
MAX_RANGE_DAYS = 366

# SQL-фрагменты сегментов. Фильтр по роли идёт по уже склеенному посетителю
# (uid), поэтому гостевые события админа до входа тоже не попадают в «Все».
_SEGMENT_SQL = {
    "all": "u.role IS DISTINCT FROM 'admin'",
    "auth": "ev.uid IS NOT NULL AND u.role IS DISTINCT FROM 'admin'",
    "guest": "ev.uid IS NULL",
    "admin": "u.role = 'admin'",
    "everyone": "TRUE",
}
_DEVICES = {"desktop", "mobile", "tablet"}


def _resolve_range(days: int, date_from: Optional[str], date_to: Optional[str]):
    """Период в московских календарных днях → границы в naive-UTC (как server_ts).

    days=N без явных дат — сегодня и N-1 предыдущих дней. Предыдущий период
    для дельт — столько же дней вплотную перед текущим.
    """
    today = datetime.now(MSK).date()
    if date_from and date_to:
        try:
            d0 = date.fromisoformat(date_from)
            d1 = date.fromisoformat(date_to)
        except ValueError:
            raise HTTPException(400, "Даты в формате YYYY-MM-DD")
        if d1 > today:
            d1 = today
        if d0 > d1:
            d0, d1 = d1, d0
        if (d1 - d0).days + 1 > MAX_RANGE_DAYS:
            d0 = d1 - timedelta(days=MAX_RANGE_DAYS - 1)
    else:
        d1 = today
        d0 = today - timedelta(days=max(1, days) - 1)
    n = (d1 - d0).days + 1

    def utc(d: date) -> datetime:
        return datetime.combine(d, dtime.min, MSK).astimezone(timezone.utc).replace(tzinfo=None)

    return {
        "d0": d0, "d1": d1, "n": n,
        "start": utc(d0), "end": utc(d1 + timedelta(days=1)),
        "pstart": utc(d0 - timedelta(days=n)), "pend": utc(d0),
    }


def _materialize_visits(conn, suffix: str, start: datetime, end: datetime,
                        segment: str, device: str) -> None:
    """Строит временные таблицы act_<suffix> (события после чистки) и
    visits_<suffix> (визиты). ON COMMIT DROP — живут до конца транзакции."""
    seg_sql = _SEGMENT_SQL.get(segment, _SEGMENT_SQL["all"])
    dev_sql = "AND e.device = :device" if device in _DEVICES else ""
    params: dict[str, Any] = {"start": start, "end": end}
    if dev_sql:
        params["device"] = device
    conn.execute(text(f"""
        CREATE TEMP TABLE act_{suffix} ON COMMIT DROP AS
        WITH base AS (
            SELECT e.user_id, e.session_id, e.visitor_id, e.event_type,
                   e.event_path, e.payload, e.server_ts, e.device
            FROM analytics_events e
            WHERE e.server_ts >= :start AND e.server_ts < :end {dev_sql}
        ),
        sess_user AS (
            SELECT session_id, MAX(user_id) AS uid
            FROM base WHERE user_id IS NOT NULL GROUP BY session_id
        ),
        vis_user AS (
            SELECT visitor_id, MAX(user_id) AS uid
            FROM analytics_events
            WHERE visitor_id IS NOT NULL AND user_id IS NOT NULL
            GROUP BY visitor_id
        ),
        ev AS (
            SELECT b.*, COALESCE(b.user_id, su.uid, vu.uid) AS uid
            FROM base b
            LEFT JOIN sess_user su ON su.session_id = b.session_id
            LEFT JOIN vis_user vu ON vu.visitor_id = b.visitor_id
        ),
        seg AS (
            SELECT ev.*,
                   COALESCE('u' || ev.uid::text, 'v' || ev.visitor_id, 's' || ev.session_id) AS ident,
                   (ev.event_type = 'session_heartbeat'
                    AND (ev.payload->>'act') IS NULL) AS legacy_hb
            FROM ev
            LEFT JOIN users u ON u.id = ev.uid
            WHERE {seg_sql}
        ),
        lr AS (
            SELECT seg.*,
                   MAX(CASE WHEN NOT legacy_hb THEN server_ts END) OVER (
                       PARTITION BY session_id ORDER BY server_ts
                       ROWS UNBOUNDED PRECEDING
                   ) AS last_real
            FROM seg
        )
        SELECT uid, ident, session_id, event_type, event_path, payload, server_ts, device
        FROM lr
        WHERE NOT legacy_hb
           OR (last_real IS NOT NULL
               AND server_ts - last_real <= INTERVAL '{VISIT_GAP_MIN} minutes')
    """), params)
    conn.execute(text(f"""
        CREATE TEMP TABLE visits_{suffix} ON COMMIT DROP AS
        WITH ord AS (
            SELECT a.*, LAG(server_ts) OVER (PARTITION BY ident ORDER BY server_ts) AS prev_ts
            FROM act_{suffix} a
        ),
        num AS (
            SELECT ord.*,
                   SUM(CASE WHEN prev_ts IS NULL
                              OR server_ts - prev_ts > INTERVAL '{VISIT_GAP_MIN} minutes'
                            THEN 1 ELSE 0 END) OVER (
                       PARTITION BY ident ORDER BY server_ts ROWS UNBOUNDED PRECEDING
                   ) AS vno
            FROM ord
        )
        SELECT ident, vno,
               MIN(server_ts) AS started,
               EXTRACT(EPOCH FROM (MAX(server_ts) - MIN(server_ts)))::int AS dur,
               COUNT(*) FILTER (WHERE event_type = 'pageview') AS pv,
               (ARRAY_AGG(payload ORDER BY server_ts))[1] AS first_payload
        FROM num
        GROUP BY ident, vno
    """))


def _summary(conn, suffix: str) -> dict:
    row = conn.execute(text(f"""
        SELECT
            (SELECT COUNT(DISTINCT ident) FROM act_{suffix}),
            (SELECT COUNT(*) FROM visits_{suffix}),
            (SELECT COUNT(*) FROM act_{suffix} WHERE event_type = 'pageview'),
            (SELECT COALESCE(AVG(dur), 0)::int FROM visits_{suffix}),
            (SELECT COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY dur), 0)::int
               FROM visits_{suffix}),
            (SELECT COUNT(*) FROM visits_{suffix} WHERE pv <= 1 AND dur < {BOUNCE_MAX_SEC}),
            (SELECT COUNT(*) FROM (
                SELECT ident FROM visits_{suffix}
                GROUP BY ident
                HAVING COUNT(DISTINCT (started AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::date) >= 2
            ) r),
            (SELECT COUNT(DISTINCT uid) FROM act_{suffix} WHERE uid IS NOT NULL)
    """)).fetchone()
    visits = int(row[1] or 0)
    visitors = int(row[0] or 0)
    return {
        "visitors": visitors,
        "visits": visits,
        "pageviews": int(row[2] or 0),
        "avg_visit_sec": int(row[3] or 0),
        "median_visit_sec": int(row[4] or 0),
        "bounce_pct": round(int(row[5] or 0) / visits * 100, 1) if visits else None,
        "returning": int(row[6] or 0),
        "returning_pct": round(int(row[6] or 0) / visitors * 100, 1) if visitors else None,
        "auth_visitors": int(row[7] or 0),
    }


# Источник визита по хосту реферера первого события визита.
_OWN_HOSTS = ("framedata.ru", "xn--80aklbnczmv.xn--p1ai", "localhost")
_SOURCE_RULES = [
    (("oauth.yandex.", "id.vk.", "oauth.vk.", "accounts.google.", "oauth.telegram."), "Возврат после входа"),
    (("yandex.", "ya.ru"), "Яндекс"),
    (("google.",), "Google"),
    (("t.me", "telegram"), "Telegram"),
    (("vk.com", "vk.ru"), "VK"),
    (("smart-lab.",), "Смартлаб"),
    (("tbank.", "tinkoff."), "Т-Банк"),
    (("dzen.",), "Дзен"),
    (("bing.", "duckduckgo."), "Другие поисковики"),
]


def _classify_source(acq: Any) -> str:
    if not isinstance(acq, dict):
        return "Прямые заходы"
    utm = (acq.get("utm_source") or "").strip()
    if utm:
        return f"utm: {utm[:40]}"
    ref = (acq.get("ref") or "").lower()
    if not ref or ref == "direct":
        return "Прямые заходы"
    if any(ref == h or ref.endswith("." + h) for h in _OWN_HOSTS):
        return "Внутренние переходы"
    for needles, label in _SOURCE_RULES:
        if any(n in ref for n in needles):
            return label
    return ref


def _asset_names(conn, secids: list[str]) -> dict[str, str]:
    """Человеческие имена активов для топа. Ошибка — просто без имён."""
    if not secids:
        return {}
    try:
        rows = conn.execute(text("""
            SELECT sectype, name FROM instruments WHERE sectype = ANY(:s)
            UNION ALL
            SELECT sec_id, name FROM instruments WHERE sec_id = ANY(:s)
        """), {"s": secids}).fetchall()
    except Exception:
        return {}
    out: dict[str, str] = {}
    for key, name in rows:
        if key and name and key not in out:
            out[key] = name
    return out


@router.get("/stats")
async def get_stats(
    days: int = Query(7, ge=1, le=MAX_RANGE_DAYS, description="Последние N дней, если нет дат"),
    date_from: Optional[str] = Query(None, description="Начало периода, YYYY-MM-DD по Москве"),
    date_to: Optional[str] = Query(None, description="Конец периода включительно, YYYY-MM-DD"),
    segment: str = Query("all", description="all (без админов) / auth / guest / admin / everyone"),
    device: str = Query("all", description="all / mobile / desktop / tablet"),
    user=Depends(require_admin),
):
    """Сводка для /admin/stats. Кэш в Redis 3 мин (single-flight)."""
    rng = _resolve_range(days, date_from, date_to)
    cache_key = f"admin:stats:v2:{rng['d0']}:{rng['d1']}:{segment}:{device}"
    return get_or_compute(cache_key, lambda: _compute_stats(rng, segment, device), ttl=180)


@router.get("/metrica")
async def get_metrica(
    days: int = Query(7, ge=1, le=MAX_RANGE_DAYS),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    user=Depends(require_admin),
):
    """Трафик из Яндекс Метрики за тот же период, что и /stats.

    connected=False — токена нет, страница показывает инструкцию. Кэш 5 минут
    (см. api/services/metrica.py). Роботов Метрика отсекает сама.
    """
    from api.services import metrica

    rng = _resolve_range(days, date_from, date_to)
    pd0 = rng["d0"] - timedelta(days=rng["n"])
    pd1 = rng["d0"] - timedelta(days=1)
    data = metrica.get_report(rng["d0"], rng["d1"], pd0, pd1)
    return {
        **data,
        "date_from": rng["d0"].isoformat(),
        "date_to": rng["d1"].isoformat(),
        "prev_date_from": pd0.isoformat(),
        "prev_date_to": pd1.isoformat(),
    }


def _compute_stats(rng: dict, segment: str, device: str) -> dict:
    engine = get_engine()
    with engine.begin() as conn:
        _materialize_visits(conn, "cur", rng["start"], rng["end"], segment, device)
        _materialize_visits(conn, "prev", rng["pstart"], rng["pend"], segment, device)
        cur = _summary(conn, "cur")
        prev = _summary(conn, "prev")

        day_rows = conn.execute(text("""
            WITH u AS (
                SELECT (server_ts AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::date AS d,
                       COUNT(DISTINCT ident) AS visitors,
                       COUNT(*) FILTER (WHERE event_type = 'pageview') AS pageviews
                FROM act_cur GROUP BY 1
            ),
            v AS (
                SELECT (started AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow')::date AS d,
                       COUNT(*) AS visits
                FROM visits_cur GROUP BY 1
            )
            SELECT COALESCE(u.d, v.d), COALESCE(u.visitors, 0), COALESCE(v.visits, 0),
                   COALESCE(u.pageviews, 0)
            FROM u FULL JOIN v ON v.d = u.d
        """)).fetchall()

        top_pages = conn.execute(text("""
            SELECT event_path, COUNT(DISTINCT ident) AS visitors, COUNT(*) AS views
            FROM act_cur
            WHERE event_type = 'pageview' AND event_path IS NOT NULL
            GROUP BY event_path
            ORDER BY visitors DESC, views DESC
            LIMIT 12
        """)).fetchall()

        top_assets = conn.execute(text("""
            SELECT payload->>'secid' AS secid,
                   COUNT(DISTINCT ident) AS visitors,
                   COUNT(*) AS views,
                   STRING_AGG(DISTINCT payload->>'indicator', ',') AS indicators
            FROM act_cur
            WHERE event_type = 'asset_view' AND payload->>'secid' IS NOT NULL
            GROUP BY 1
            ORDER BY visitors DESC, views DESC
            LIMIT 15
        """)).fetchall()

        top_search = conn.execute(text("""
            SELECT payload->>'secid' AS secid,
                   COUNT(DISTINCT ident) AS visitors,
                   COUNT(*) AS picks
            FROM act_cur
            WHERE event_type = 'instrument_select' AND payload->>'secid' IS NOT NULL
            GROUP BY 1
            ORDER BY picks DESC, visitors DESC
            LIMIT 15
        """)).fetchall()

        top_exports = conn.execute(text(f"""
            WITH normalized AS (
                SELECT ident, {_export_indicator_canon_sql("payload->>'indicator'")} AS indicator
                FROM act_cur
                WHERE event_type = 'chart_export' AND payload->>'indicator' IS NOT NULL
            )
            SELECT indicator, COUNT(*) AS cnt, COUNT(DISTINCT ident) AS visitors
            FROM normalized
            GROUP BY indicator
            ORDER BY cnt DESC
            LIMIT 10
        """)).fetchall()

        mode_dist = conn.execute(text("""
            SELECT payload->>'mode' AS mode, COUNT(*) AS cnt, COUNT(DISTINCT ident) AS visitors
            FROM act_cur
            WHERE event_type = 'seasonality_mode' AND payload->>'mode' IS NOT NULL
            GROUP BY 1
            ORDER BY cnt DESC
        """)).fetchall()

        source_rows = conn.execute(text("""
            SELECT first_payload->'acq' AS acq, COUNT(*) AS visits
            FROM visits_cur
            GROUP BY 1
        """)).fetchall()

        devices = conn.execute(text("""
            SELECT COALESCE(device, 'unknown'), COUNT(DISTINCT ident)
            FROM act_cur GROUP BY 1
        """)).fetchall()

        names = _asset_names(conn, [r[0] for r in top_assets] + [r[0] for r in top_search])

    def pct_delta(curr: Optional[float], prv: Optional[float]) -> Optional[int]:
        if curr is None or not prv:
            return None
        return int(round((curr - prv) / prv * 100))

    def pp_delta(curr: Optional[float], prv: Optional[float]) -> Optional[float]:
        if curr is None or prv is None:
            return None
        return round(curr - prv, 1)

    # Дни без событий — нулями, иначе линия перепрыгивает провалы.
    day_map = {r[0]: (int(r[1]), int(r[2]), int(r[3])) for r in day_rows if r[0]}
    trends: list[dict] = []
    d = rng["d0"]
    while d <= rng["d1"]:
        v, s, p = day_map.get(d, (0, 0, 0))
        trends.append({"date": d.isoformat(), "visitors": v, "visits": s, "pageviews": p})
        d += timedelta(days=1)

    sources: dict[str, int] = {}
    for acq, cnt in source_rows:
        label = _classify_source(acq)
        sources[label] = sources.get(label, 0) + int(cnt)

    return {
        "date_from": rng["d0"].isoformat(),
        "date_to": rng["d1"].isoformat(),
        "period_days": rng["n"],
        "prev_date_from": (rng["d0"] - timedelta(days=rng["n"])).isoformat(),
        "prev_date_to": (rng["d0"] - timedelta(days=1)).isoformat(),
        "segment": segment,
        "device": device,
        "summary": {
            **cur,
            "delta_visitors_pct": pct_delta(cur["visitors"], prev["visitors"]),
            "delta_visits_pct": pct_delta(cur["visits"], prev["visits"]),
            "delta_pageviews_pct": pct_delta(cur["pageviews"], prev["pageviews"]),
            "delta_avg_visit_sec": cur["avg_visit_sec"] - prev["avg_visit_sec"] if prev["visits"] else None,
            "delta_bounce_pp": pp_delta(cur["bounce_pct"], prev["bounce_pct"]),
            "delta_returning_pct": pct_delta(cur["returning"], prev["returning"]),
        },
        "prev_summary": prev,
        "trends": trends,
        "top_pages": [{"path": r[0], "visitors": int(r[1]), "views": int(r[2])} for r in top_pages],
        "top_assets": [
            {"secid": r[0], "name": names.get(r[0]), "visitors": int(r[1]), "views": int(r[2]),
             "indicators": (r[3] or "").split(",") if r[3] else []}
            for r in top_assets
        ],
        "top_search": [
            {"secid": r[0], "name": names.get(r[0]), "visitors": int(r[1]), "picks": int(r[2])}
            for r in top_search
        ],
        "top_exports": [{"indicator": r[0], "count": int(r[1]), "visitors": int(r[2])} for r in top_exports],
        "mode_distribution": [{"mode": r[0], "count": int(r[1]), "visitors": int(r[2])} for r in mode_dist],
        "sources": sorted(
            [{"source": k, "visits": v} for k, v in sources.items()],
            key=lambda x: -x["visits"],
        )[:15],
        "devices": sorted(
            [{"device": r[0], "visitors": int(r[1])} for r in devices],
            key=lambda x: -x["visitors"],
        ),
    }


# ════════════════════════════════════════════════════════════════════════════
# GET /funnel — УДАЛЕНО (2026-07-16, редизайн admin-stats)
# ════════════════════════════════════════════════════════════════════════════
# Воронка конверсии удалена по решению Вадима — не давала пользы (шаги
# «pageview:/ → pageview:/x» не отражали реальные пути), при этом была самым
# дорогим эндпоинтом страницы: до 5 последовательных multi-CTE запросов на
# каждое переключение периода. Вместе с ней вырезаны FunnelChart +
# getAnalyticsFunnel/FunnelResponse на фронте (AdminStatsPage / services/api.ts).


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
    days: int = Query(7, ge=1, le=MAX_RANGE_DAYS, description="Последние N дней, если нет дат"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
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
    rng = _resolve_range(days, date_from, date_to)

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
            WHERE created_at >= :start AND created_at < :end
        """), {"start": rng["start"], "end": rng["end"]}).fetchone()

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
        "period_days": rng["n"],
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
    days: int = Query(30, ge=1, le=MAX_RANGE_DAYS, description="Последние N дней, если нет дат"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    sort: str = Query("last_active", description="last_active / tier / plan / expires / visits / time / created"),
    search: str = Query("", description="Поиск по email или display_name"),
    flt: str = Query("all", alias="filter",
                     description="all / paid / paid_basic / paid_pro / invite / free / churned / pending / admin"),
    user=Depends(require_admin),
):
    """Список пользователей со статистикой за период.

    Фильтры по подписке (текущее состояние, от периода не зависят):
      paid        — активная подписка, купленная за деньги (любой тариф);
      paid_basic  — купленный Basic;  paid_pro — купленный Pro (legacy premium = pro);
      invite      — единственный доступ сейчас — подарок по пригласительной ссылке;
      free        — нет активной подписки (админы исключены);
      churned     — сейчас без подписки, но раньше платил (истекла или отменена);
      pending     — начинал оплату (pending/failed), но ни разу не заплатил;
      admin       — role=admin.

    Визиты и время считаются так же, как «Визиты» и «Время визита» в сводке:
    разрыв больше 30 минут = новый визит; только события под аккаунтом.
    Оплаченная подписка бьёт инвайт, даже если инвайт применён позже.
    """
    engine = get_engine()
    rng = _resolve_range(days, date_from, date_to)
    search_clean = (search or "").strip()

    tier_rank = (
        "CASE WHEN sub.tier IS NULL THEN 3 WHEN sub.period = 'invite' THEN 2 "
        "WHEN sub.tier IN ('pro', 'premium') THEN 0 ELSE 1 END"
    )
    order_clauses = {
        "last_active": "last_active_ts DESC NULLS LAST",
        "visits": "COALESCE(uv.visits, 0) DESC, last_active_ts DESC NULLS LAST",
        "time": "COALESCE(uv.time_sec, 0) DESC, last_active_ts DESC NULLS LAST",
        # Совместимость со старыми значениями сортировки из localStorage.
        "sessions": "COALESCE(uv.visits, 0) DESC, last_active_ts DESC NULLS LAST",
        "events": "COALESCE(uv.events, 0) DESC, last_active_ts DESC NULLS LAST",
        "created": "u.created_at DESC",
        # Сначала платные (купленные), потом инвайты, внутри — раньше истекающие.
        "plan": f"({tier_rank} < 2) DESC, ({tier_rank} = 2) DESC, sub.expires_at ASC NULLS LAST, "
                "last_active_ts DESC NULLS LAST",
        # По тарифу: Pro → Basic → инвайт → без подписки.
        "tier": f"{tier_rank} ASC, sub.expires_at ASC NULLS LAST, last_active_ts DESC NULLS LAST",
        # Скоро заканчивается — сверху те, у кого подписка истекает раньше.
        "expires": "sub.expires_at ASC NULLS LAST, last_active_ts DESC NULLS LAST",
    }
    order_by = order_clauses.get(sort, order_clauses["last_active"])

    params: dict[str, Any] = {"start": rng["start"], "end": rng["end"]}
    where_search = ""
    if search_clean:
        where_search = " AND (u.email ILIKE :q OR u.display_name ILIKE :q OR u.username ILIKE :q)"
        params["q"] = f"%{search_clean}%"

    ever_paid = (
        "EXISTS (SELECT 1 FROM subscriptions p WHERE p.user_id = u.id AND p.period <> 'invite' "
        "AND p.status IN ('active', 'expired', 'cancelled'))"
    )
    ever_tried = (
        "EXISTS (SELECT 1 FROM subscriptions p WHERE p.user_id = u.id AND p.period <> 'invite' "
        "AND p.status IN ('pending', 'failed'))"
    )
    filters = {
        "paid": "sub.tier IS NOT NULL AND sub.period <> 'invite'",
        "paid_basic": "sub.tier = 'basic' AND sub.period <> 'invite'",
        "paid_pro": "sub.tier IN ('pro', 'premium') AND sub.period <> 'invite'",
        "invite": "sub.tier IS NOT NULL AND sub.period = 'invite'",
        "free": "sub.tier IS NULL AND u.role <> 'admin'",
        # Админы — тестовые оплаты, в «бывших платных» и «не дошли» им не место.
        "churned": f"sub.tier IS NULL AND u.role <> 'admin' AND {ever_paid}",
        "pending": f"sub.tier IS NULL AND u.role <> 'admin' AND {ever_tried} AND NOT {ever_paid}",
        "admin": "u.role = 'admin'",
    }
    where_filter = f" AND {filters[flt]}" if flt in filters else ""

    sub_lateral = """
        LEFT JOIN LATERAL (
            SELECT s.id, s.tier, s.expires_at, s.period
            FROM subscriptions s
            WHERE s.user_id = u.id AND s.status = 'active'
            ORDER BY (s.period <> 'invite') DESC, s.created_at DESC
            LIMIT 1
        ) sub ON TRUE
    """

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            WITH ue AS (
                SELECT user_id, session_id, event_type, payload, server_ts,
                       (event_type = 'session_heartbeat' AND (payload->>'act') IS NULL) AS legacy_hb
                FROM analytics_events
                WHERE user_id IS NOT NULL AND server_ts >= :start AND server_ts < :end
            ),
            lr AS (
                SELECT ue.*,
                       MAX(CASE WHEN NOT legacy_hb THEN server_ts END) OVER (
                           PARTITION BY session_id ORDER BY server_ts ROWS UNBOUNDED PRECEDING
                       ) AS last_real
                FROM ue
            ),
            act AS (
                SELECT user_id, event_type, server_ts FROM lr
                WHERE NOT legacy_hb
                   OR (last_real IS NOT NULL
                       AND server_ts - last_real <= INTERVAL '{VISIT_GAP_MIN} minutes')
            ),
            num AS (
                SELECT a.*,
                       SUM(CASE WHEN prev_ts IS NULL
                                  OR server_ts - prev_ts > INTERVAL '{VISIT_GAP_MIN} minutes'
                                THEN 1 ELSE 0 END) OVER (
                           PARTITION BY user_id ORDER BY server_ts ROWS UNBOUNDED PRECEDING
                       ) AS vno
                FROM (SELECT act.*, LAG(server_ts) OVER (PARTITION BY user_id ORDER BY server_ts) AS prev_ts
                      FROM act) a
            ),
            v AS (
                SELECT user_id, vno,
                       EXTRACT(EPOCH FROM (MAX(server_ts) - MIN(server_ts)))::int AS dur,
                       COUNT(*) FILTER (WHERE event_type <> 'session_heartbeat') AS ev
                FROM num GROUP BY user_id, vno
            ),
            uv AS (
                SELECT user_id, COUNT(*) AS visits, SUM(dur) AS time_sec, SUM(ev) AS events
                FROM v GROUP BY user_id
            )
            SELECT
                u.id, u.email, u.display_name, u.username, u.role,
                u.is_active, u.is_verified, u.created_at, u.last_login_at,
                u.oauth_provider, u.avatar_url,
                sub.tier AS plan,
                sub.expires_at AS plan_expires_at,
                sub.period AS plan_period,
                (SELECT si.note
                   FROM subscription_invite_redemptions ir
                   JOIN subscription_invites si ON si.token = ir.token
                  WHERE ir.subscription_id = sub.id
                  LIMIT 1) AS invite_note,
                COALESCE(uv.visits, 0) AS visits,
                COALESCE(uv.events, 0) AS events,
                -- Не только события: у отказавшихся от статистики и у адблоков
                -- analytics_events пуст, но входы и ротация refresh-токенов видны.
                GREATEST(
                    (SELECT MAX(ae.server_ts) AT TIME ZONE 'UTC' FROM analytics_events ae
                      WHERE ae.user_id = u.id),
                    (SELECT MAX(rt.created_at) FROM refresh_tokens rt
                      WHERE rt.user_id = u.id),
                    u.last_login_at
                ) AS last_active_ts,
                COALESCE(uv.time_sec, 0) AS time_sec,
                -- Последняя неактивная подписка за деньги — для «бывших платных».
                (SELECT p.tier || ':' || p.status || ':' || COALESCE(TO_CHAR(p.expires_at, 'YYYY-MM-DD'), '')
                   FROM subscriptions p
                  WHERE p.user_id = u.id AND p.period <> 'invite' AND p.status <> 'active'
                  -- Сначала реально оплаченные (истекла/отменена), потом попытки.
                  ORDER BY (p.status IN ('expired', 'cancelled')) DESC, p.created_at DESC
                  LIMIT 1) AS last_paid_sub
            FROM users u
            {sub_lateral}
            LEFT JOIN uv ON uv.user_id = u.id
            WHERE 1=1 {where_search}{where_filter}
            ORDER BY {order_by}
            LIMIT 300
        """), params).fetchall()

        # Счётчики по каждому фильтру — по всей базе, без поиска.
        count_sql = ",\n".join(
            f"COUNT(*) FILTER (WHERE {cond}) AS {key}" for key, cond in filters.items()
        )
        counts_row = conn.execute(text(f"""
            SELECT COUNT(*) AS all_users, {count_sql}
            FROM users u
            {sub_lateral}
        """)).mappings().fetchone()

    counts = {k: int(v or 0) for k, v in dict(counts_row or {}).items()}
    counts["all"] = counts.pop("all_users", 0)

    def last_paid(val: Optional[str]) -> Optional[dict]:
        if not val:
            return None
        tier, status, exp = (val.split(":") + ["", "", ""])[:3]
        return {"tier": tier, "status": status, "expires_at": exp or None}

    return {
        "date_from": rng["d0"].isoformat(),
        "date_to": rng["d1"].isoformat(),
        "period_days": rng["n"],
        "total_count": counts.get("all", 0),
        "paid_count": counts.get("paid", 0),
        "invite_count": counts.get("invite", 0),
        "counts": counts,
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
                "plan_expires_at": r[12].isoformat() if r[12] else None,
                "plan_period": r[13],
                "is_invite": r[13] == "invite",
                "is_paid": r[11] is not None and r[13] != "invite",
                "invite_note": r[14] if r[13] == "invite" else None,
                # sessions_count = визиты (разрыв 30 мин), имя поля оставлено
                # ради совместимости со страницей пользователя.
                "sessions_count": int(r[15] or 0),
                "events_count": int(r[16] or 0),
                "last_active_ts": r[17].isoformat() if r[17] else None,
                "time_sec": int(r[18] or 0),
                "last_paid_sub": last_paid(r[19]) if r[11] is None else None,
            }
            for r in rows
        ],
    }


# Максимальный зазор между heartbeat'ами одного спана. Пульс идёт раз в 60с,
# но вкладка в фоне молчит — 5 минут прощают короткие отлучки, а всё что
# дольше честно рвёт спан на два «был на сайте».
_HB_GAP_SEC = 300


def _collapse_heartbeats(rows: list) -> list:
    """Схлопывает подряд идущие session_heartbeat в один спан.

    Вход/выход — кортежи формата timeline-запроса:
    (event_type, event_path, payload, server_ts, ip_country, device, session_id),
    отсортированные по server_ts DESC.

    Без этого таймлайн юзера в админке — монотонная простыня «сидит на сайте»:
    heartbeat идёт раз в минуту с КАЖДОЙ открытой вкладки (у каждой свой
    session_id), поэтому схлопываем по времени, не по сессии. Любое другое
    событие между пульсами рвёт спан — так виден переход между страницами.
    Спан наследует форму обычного события; факт агрегации и границы — в payload
    {beats, mins, from, to}, фронт рисует по нему «на сайте ~N мин».
    """
    out: list = []
    run: list = []  # текущая пачка heartbeat'ов (DESC: [0] — самый свежий)

    def flush() -> None:
        if not run:
            return
        if len(run) == 1:
            out.append(run[0])
        else:
            newest, oldest = run[0], run[-1]
            mins = max(1, round((newest[3] - oldest[3]).total_seconds() / 60))
            out.append((
                newest[0],
                newest[1],
                {
                    "beats": len(run),
                    "mins": mins,
                    "from": oldest[3].isoformat(),
                    "to": newest[3].isoformat(),
                },
                newest[3],
                newest[4],
                newest[5],
                newest[6],
            ))
        run.clear()

    for row in rows:
        if row[0] == "session_heartbeat":
            if run and (run[-1][3] - row[3]).total_seconds() > _HB_GAP_SEC:
                flush()
            run.append(row)
        else:
            flush()
            out.append(row)
    flush()
    return out


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

        # Summary: те же определения, что в сводке /stats. Визит — разрыв
        # больше 30 минут; старые пульсы без флага act засчитываются только
        # в пределах 30 минут после настоящего действия; «действия» — события
        # без служебного пульса.
        summary = conn.execute(text(f"""
            WITH ue AS (
                SELECT session_id, event_type, server_ts,
                       (event_type = 'session_heartbeat' AND (payload->>'act') IS NULL) AS legacy_hb
                FROM analytics_events
                WHERE user_id = :id AND server_ts >= :cutoff
            ),
            lr AS (
                SELECT ue.*,
                       MAX(CASE WHEN NOT legacy_hb THEN server_ts END) OVER (
                           PARTITION BY session_id ORDER BY server_ts ROWS UNBOUNDED PRECEDING
                       ) AS last_real
                FROM ue
            ),
            act AS (
                SELECT event_type, server_ts FROM lr
                WHERE NOT legacy_hb
                   OR (last_real IS NOT NULL
                       AND server_ts - last_real <= INTERVAL '{VISIT_GAP_MIN} minutes')
            ),
            num AS (
                SELECT a.*,
                       SUM(CASE WHEN prev_ts IS NULL
                                  OR server_ts - prev_ts > INTERVAL '{VISIT_GAP_MIN} minutes'
                                THEN 1 ELSE 0 END) OVER (ORDER BY server_ts ROWS UNBOUNDED PRECEDING) AS vno
                FROM (SELECT act.*, LAG(server_ts) OVER (ORDER BY server_ts) AS prev_ts FROM act) a
            ),
            v AS (
                SELECT vno, EXTRACT(EPOCH FROM (MAX(server_ts) - MIN(server_ts)))::int AS dur
                FROM num GROUP BY vno
            )
            SELECT
                (SELECT COUNT(*) FROM act WHERE event_type <> 'session_heartbeat'),
                (SELECT COUNT(*) FROM v),
                (SELECT MIN(server_ts) FROM act),
                (SELECT MAX(server_ts) FROM act),
                (SELECT COALESCE(AVG(dur), 0)::int FROM v),
                (SELECT COALESCE(SUM(dur), 0)::int FROM v)
        """), {"id": user_id, "cutoff": cutoff}).fetchone()
        avg_session = (summary[4], summary[5]) if summary else (0, 0)

        # Activity timeline. Сырых событий берём с запасом: heartbeat идёт раз в
        # минуту с каждой открытой вкладки, и лимит в 100 строк целиком съедался
        # монотонной простынёй «сидит на сайте». Подряд идущие heartbeat'ы ниже
        # схлопываются в один спан (см. _collapse_heartbeats), 100 — уже после.
        timeline_raw = conn.execute(text("""
            SELECT event_type, event_path, payload, server_ts, ip_country, device, session_id
            FROM analytics_events
            WHERE user_id = :id AND server_ts >= :cutoff
            ORDER BY server_ts DESC
            LIMIT 2000
        """), {"id": user_id, "cutoff": cutoff}).fetchall()
        timeline = _collapse_heartbeats(timeline_raw)[:100]

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

        # «Был(а) в сети» без событий: у consent-отказников analytics_events
        # пуст, но входы и ротация refresh-токенов есть (см. список юзеров).
        last_seen = conn.execute(text("""
            SELECT GREATEST(
                (SELECT MAX(ae.server_ts) AT TIME ZONE 'UTC' FROM analytics_events ae
                  WHERE ae.user_id = :id),
                (SELECT MAX(rt.created_at) FROM refresh_tokens rt
                  WHERE rt.user_id = :id),
                (SELECT us.last_login_at FROM users us WHERE us.id = :id)
            )
        """), {"id": user_id}).scalar()

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
            "last_seen_at": last_seen.isoformat() if last_seen else None,
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
