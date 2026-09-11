"""
Яндекс Метрика Reporting API — трафик сайта для /admin/stats.

Разделение труда (решение 2026-09-11): всё, что Метрика считает лучше нас —
посетители, визиты, время, отказы, новые посетители, источники, поисковые
фразы, география, устройства, популярные страницы, — берём у неё. Свой трекер
оставляем для того, чего у Метрики нет: какие активы смотрят, действия внутри
индикаторов, связь с аккаунтами и подписками, уведомления.

Токен: YANDEX_METRIKA_TOKEN — OAuth-токен со scope metrika:read от аккаунта,
у которого есть доступ к счётчику. Без токена модуль спит (connected=False),
страница показывает инструкцию и наш трекер как запасной вариант. Токен есть,
но Метрика его не принимает (живёт около полугода) — тоже connected=False,
плюс token_error с её ответом; такой ответ не кэшируется.

«Реальное время»: Метрика обновляет отчёты за сегодня с задержкой в несколько
минут, поэтому кэшируем на 5 минут — чаще спрашивать бессмысленно, а лимит
API (≈ 30 запросов в секунду, 5000 в сутки на токен) так не выбирается.
"""
from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any, Optional

import requests

from api.cache import get_or_compute
from api.logger import get_logger

log = get_logger()

STAT_URL = "https://api-metrika.yandex.net/stat/v1/data"
BYTIME_URL = "https://api-metrika.yandex.net/stat/v1/data/bytime"
DEFAULT_COUNTER = "109137033"
CACHE_TTL = 300
TIMEOUT = 20


def _token() -> Optional[str]:
    t = (os.environ.get("YANDEX_METRIKA_TOKEN") or "").strip()
    return t or None


def _counter() -> str:
    return (os.environ.get("YANDEX_METRIKA_COUNTER") or DEFAULT_COUNTER).strip()


def is_connected() -> bool:
    return _token() is not None


class MetricaAuthError(RuntimeError):
    """Метрика не принимает токен: истёк, отозван или нет доступа к счётчику."""


def _get(url: str, params: dict) -> dict:
    token = _token()
    base = {
        "ids": _counter(),
        "accuracy": "full",   # без сэмплирования: трафик у нас небольшой
        "lang": "ru",
    }
    r = requests.get(
        url,
        params={**base, **params},
        headers={"Authorization": f"OAuth {token}"},
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        msg = ""
        try:
            msg = r.json().get("message", "")
        except Exception:
            msg = r.text[:200]
        # 403 invalid_token — токен истёк или отозван, 403 access_denied — у
        # аккаунта нет доступа к счётчику. Оба лечатся только новым токеном.
        err = MetricaAuthError if r.status_code in (401, 403) else RuntimeError
        raise err(f"Метрика ответила {r.status_code}: {msg}")
    return r.json()


SUMMARY_METRICS = [
    ("users", "ym:s:users"),
    ("visits", "ym:s:visits"),
    ("pageviews", "ym:s:pageviews"),
    ("avg_visit_sec", "ym:s:avgVisitDurationSeconds"),
    ("bounce_pct", "ym:s:bounceRate"),
    ("page_depth", "ym:s:pageDepth"),
    ("new_users", "ym:s:newUsers"),
]


def _num(key: str, v: Any) -> float | int:
    """Отказы и глубина — с одним знаком после запятой, остальное целыми."""
    return round(float(v or 0), 1) if key in ("bounce_pct", "page_depth") else int(round(float(v or 0)))


def _with_filter(params: dict, flt: Optional[str]) -> dict:
    return {**params, "filters": flt} if flt else params


def _summary(d0: date, d1: date, flt: Optional[str] = None) -> dict:
    j = _get(STAT_URL, _with_filter({
        "metrics": ",".join(m for _, m in SUMMARY_METRICS),
        "date1": d0.isoformat(), "date2": d1.isoformat(),
    }, flt))
    totals = j.get("totals") or [0] * len(SUMMARY_METRICS)
    return {key: _num(key, v) for (key, _), v in zip(SUMMARY_METRICS, totals)}


# Источник визита — по последнему значимому переходу: так Метрика считает в
# интерфейсе по умолчанию. С «последним переходом» (ym:s:last…) прямых заходов
# выходит в полтора раза больше, а из поиска вдвое меньше, чем видно в Метрике.
SOURCE_DIM = "ym:s:lastsignTrafficSource"
WEEKLY_FROM_DAYS = 92   # с такого периода график по неделям: 90+ точек — частокол


def _by_source(d0: date, d1: date, flt: Optional[str] = None) -> dict:
    """График «По источникам трафика», как в Метрике: все показатели сводки по
    дням (на длинных периодах по неделям) для каждого источника и в сумме.

    Итог за период по источнику — отдельным запросом, а не суммой дней:
    посетителей и средние по дням не сложить.
    """
    keys = [k for k, _ in SUMMARY_METRICS]
    group = "week" if (d1 - d0).days + 1 >= WEEKLY_FROM_DAYS else "day"
    common = _with_filter({
        "dimensions": SOURCE_DIM,
        "metrics": ",".join(m for _, m in SUMMARY_METRICS),
        "date1": d0.isoformat(), "date2": d1.isoformat(),
        "limit": 20,
    }, flt)
    by_time = _get(BYTIME_URL, {**common, "group": group})
    by_period = _get(STAT_URL, {**common, "sort": "-ym:s:visits"})
    intervals = by_time.get("time_intervals", [])

    def pack(arrays: list) -> dict:
        return {k: [_num(k, v) for v in arr] for k, arr in zip(keys, arrays)}

    def source(row: dict) -> dict:
        return (row.get("dimensions") or [{}])[0]

    lines = {source(r).get("id"): r.get("metrics") for r in by_time.get("data", [])}
    series = []
    for row in by_period.get("data", []):   # порядок — по визитам за период
        dim = source(row)
        series.append({
            "id": str(dim.get("id") or "undefined"),
            "name": dim.get("name") or "не определено",
            "period": {k: _num(k, v) for k, v in zip(keys, row.get("metrics") or [])},
            "values": pack(lines.get(dim.get("id")) or [[0] * len(intervals)] * len(keys)),
        })
    return {
        "group": group,
        "dates": [iv[0] for iv in intervals],
        "ends": [iv[1] for iv in intervals],
        "total": pack(by_time.get("totals") or []),
        "series": series,
    }


def _table(d0: date, d1: date, dimension: str, metrics: str, limit: int = 12,
           filters: Optional[str] = None) -> list[dict]:
    params = {
        "dimensions": dimension,
        "metrics": metrics,
        "date1": d0.isoformat(), "date2": d1.isoformat(),
        "sort": "-" + metrics.split(",")[0],
        "limit": limit,
    }
    if filters:
        params["filters"] = filters
    j = _get(STAT_URL, params)
    rows = []
    for row in j.get("data", []):
        dim = (row.get("dimensions") or [{}])[0]
        vals = row.get("metrics") or []
        rows.append({
            "label": dim.get("name") or "не определено",
            "value": int(round(float(vals[0] or 0))) if vals else 0,
            "value2": int(round(float(vals[1] or 0))) if len(vals) > 1 else None,
        })
    return rows


# Отчёты-таблицы: ключ → (измерение, метрики). Каждый считается отдельно и
# падает отдельно: сломанный отчёт не валит всю страницу. Всё про источники —
# по последнему значимому переходу, как график (см. SOURCE_DIM).
TABLES = {
    "sources": (SOURCE_DIM, "ym:s:visits,ym:s:users"),
    "search_engines": ("ym:s:lastsignSearchEngineRoot", "ym:s:visits,ym:s:users"),
    "search_phrases": ("ym:s:lastsignSearchPhrase", "ym:s:visits,ym:s:users"),
    "referrers": ("ym:s:lastsignReferalSource", "ym:s:visits,ym:s:users"),
    "social": ("ym:s:lastsignSocialNetwork", "ym:s:visits,ym:s:users"),
    "cities": ("ym:s:regionCity", "ym:s:users,ym:s:visits"),
    "devices": ("ym:s:deviceCategory", "ym:s:users,ym:s:visits"),
    "browsers": ("ym:s:browser", "ym:s:users,ym:s:visits"),
    "entry_pages": ("ym:s:startURLPath", "ym:s:visits,ym:s:users"),
    "pages": ("ym:pv:URLPath", "ym:pv:users,ym:pv:pageviews"),
}
# Поисковые фразы Метрика прячет (contains_sensitive_data), как только в
# фильтре есть параметр посетителя, то есть при любом сегменте, кроме «все
# вместе с админами». Для них берём только фильтр устройства.
SEGMENT_BLIND = {"search_phrases"}


def _compute(d0: date, d1: date, pd0: date, pd1: date, flt: Optional[str] = None,
             flt_phrases: Optional[str] = None) -> dict:
    jobs: dict[str, Any] = {
        "summary": lambda: _summary(d0, d1, flt),
        "prev_summary": lambda: _summary(pd0, pd1, flt),
        "by_source": lambda: _by_source(d0, d1, flt),
    }
    for key, (dim, mets) in TABLES.items():
        f = flt_phrases if key in SEGMENT_BLIND else flt
        jobs[key] = (lambda dim=dim, mets=mets, f=f: _table(d0, d1, dim, mets, filters=f))

    result: dict[str, Any] = {"connected": True, "errors": {}}
    auth_error: Optional[MetricaAuthError] = None
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {k: pool.submit(fn) for k, fn in jobs.items()}
        for k, f in futures.items():
            try:
                result[k] = f.result()
            except Exception as e:  # отчёт не пришёл — остальные показываем
                log.warning(f"metrica {k} failed: {e}")
                result[k] = None
                result["errors"][k] = str(e)[:300]
                if isinstance(e, MetricaAuthError):
                    auth_error = e
    # Токен не принят — исключением, чтобы get_or_compute это не закэшировал:
    # после замены токена блок оживает сразу, а не через 5 минут.
    if auth_error is not None:
        raise auth_error
    return result


# Аккаунт в Метрике — параметр посетителя UserID: фронт шлёт его при входе
# (useYandexMetrica, с 11.09.2026). Метрика вешает параметр на браузер целиком,
# вместе с прошлыми визитами: браузер, где админ хоть раз вошёл после этой даты,
# исключается и из старых периодов. Как в нашем трекере, гость, который потом
# вошёл, считается авторизованным.
HAS_ACCOUNT = "ym:up:paramsLevel1=='UserID'"


def device_filter(device: str) -> Optional[str]:
    return f"ym:s:deviceCategory=='{device}'" if device in ("desktop", "mobile", "tablet") else None


def build_filter(segment: str, device: str, admin_ids: list[int]) -> Optional[str]:
    """Фильтр Метрики под сегмент и устройство из шапки /admin/stats."""
    parts = [p for p in (device_filter(device),) if p]
    ids = ",".join(f"'{int(i)}'" for i in admin_ids)
    # Админов нет — условие, которому не отвечает никто.
    admin = f"{HAS_ACCOUNT} AND ym:up:paramsLevel2=.({ids})" if ids else "ym:up:paramsLevel1=='-'"
    if segment == "guest":
        parts.append(f"NOT({HAS_ACCOUNT})")
    elif segment == "auth":
        parts.append(f"{HAS_ACCOUNT} AND NOT({admin})")
    elif segment == "admin":
        parts.append(admin)
    elif segment != "everyone":          # all — все без админов
        parts.append(f"NOT({admin})")
    return " AND ".join(f"({p})" for p in parts) or None


def get_report(d0: date, d1: date, pd0: date, pd1: date, segment: str = "everyone",
               device: str = "all", admin_ids: Optional[list[int]] = None) -> dict:
    """Все отчёты Метрики за период + сводка за предыдущий период (для дельт),
    с сегментом и устройством из шапки страницы."""
    if not is_connected():
        return {"connected": False, "counter": _counter()}
    flt = build_filter(segment, device, admin_ids or [])
    flt_phrases = device_filter(device)
    tag = hashlib.md5(flt.encode()).hexdigest()[:10] if flt else "all"
    key = f"metrica:v2:{_counter()}:{d0}:{d1}:{tag}"
    try:
        data = get_or_compute(key, lambda: _compute(d0, d1, pd0, pd1, flt, flt_phrases), ttl=CACHE_TTL)
    except MetricaAuthError as e:
        return {"connected": False, "token_error": str(e), "counter": _counter()}
    return {**data, "counter": _counter(), "phrases_unsegmented": flt != flt_phrases}
