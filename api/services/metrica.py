"""
Яндекс Метрика Reporting API — трафик сайта для /admin/stats.

Разделение труда (решение 2026-09-11): всё, что Метрика считает лучше нас —
посетители, визиты, время, отказы, новые посетители, источники, поисковые
фразы, география, устройства, популярные страницы, — берём у неё. Свой трекер
оставляем для того, чего у Метрики нет: какие активы смотрят, действия внутри
индикаторов, связь с аккаунтами и подписками, уведомления.

Токен: YANDEX_METRIKA_TOKEN — OAuth-токен со scope metrika:read от аккаунта,
у которого есть доступ к счётчику. Без токена модуль спит (connected=False),
страница показывает инструкцию и наш трекер как запасной вариант.

«Реальное время»: Метрика обновляет отчёты за сегодня с задержкой в несколько
минут, поэтому кэшируем на 5 минут — чаще спрашивать бессмысленно, а лимит
API (≈ 30 запросов в секунду, 5000 в сутки на токен) так не выбирается.
"""
from __future__ import annotations

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
        raise RuntimeError(f"Метрика ответила {r.status_code}: {msg}")
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


def _summary(d0: date, d1: date) -> dict:
    j = _get(STAT_URL, {
        "metrics": ",".join(m for _, m in SUMMARY_METRICS),
        "date1": d0.isoformat(), "date2": d1.isoformat(),
    })
    totals = j.get("totals") or [0] * len(SUMMARY_METRICS)
    out = {}
    for (key, _), v in zip(SUMMARY_METRICS, totals):
        out[key] = round(float(v or 0), 1) if key in ("bounce_pct", "page_depth") else int(round(float(v or 0)))
    return out


def _trends(d0: date, d1: date) -> list[dict]:
    j = _get(BYTIME_URL, {
        "metrics": "ym:s:users,ym:s:visits,ym:s:pageviews",
        "date1": d0.isoformat(), "date2": d1.isoformat(),
        "group": "day",
    })
    days = [iv[0] for iv in j.get("time_intervals", [])]
    series = ((j.get("data") or [{}])[0].get("metrics")) or [[], [], []]
    out = []
    for i, day in enumerate(days):
        out.append({
            "date": day[:10],
            "users": int(series[0][i]) if i < len(series[0]) else 0,
            "visits": int(series[1][i]) if i < len(series[1]) else 0,
            "pageviews": int(series[2][i]) if len(series) > 2 and i < len(series[2]) else 0,
        })
    return out


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
# падает отдельно: сломанный отчёт не валит всю страницу.
TABLES = {
    "sources": ("ym:s:lastTrafficSource", "ym:s:visits,ym:s:users"),
    "search_engines": ("ym:s:lastSearchEngineRoot", "ym:s:visits,ym:s:users"),
    "search_phrases": ("ym:s:lastSearchPhrase", "ym:s:visits,ym:s:users"),
    "referrers": ("ym:s:lastReferalSource", "ym:s:visits,ym:s:users"),
    "social": ("ym:s:lastSocialNetwork", "ym:s:visits,ym:s:users"),
    "cities": ("ym:s:regionCity", "ym:s:users,ym:s:visits"),
    "devices": ("ym:s:deviceCategory", "ym:s:users,ym:s:visits"),
    "browsers": ("ym:s:browser", "ym:s:users,ym:s:visits"),
    "entry_pages": ("ym:s:startURLPath", "ym:s:visits,ym:s:users"),
    "pages": ("ym:pv:URLPath", "ym:pv:users,ym:pv:pageviews"),
}


def _compute(d0: date, d1: date, pd0: date, pd1: date) -> dict:
    jobs: dict[str, Any] = {
        "summary": lambda: _summary(d0, d1),
        "prev_summary": lambda: _summary(pd0, pd1),
        "trends": lambda: _trends(d0, d1),
    }
    for key, (dim, mets) in TABLES.items():
        jobs[key] = (lambda dim=dim, mets=mets: _table(d0, d1, dim, mets))

    result: dict[str, Any] = {"connected": True, "errors": {}}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {k: pool.submit(fn) for k, fn in jobs.items()}
        for k, f in futures.items():
            try:
                result[k] = f.result()
            except Exception as e:  # отчёт не пришёл — остальные показываем
                log.warning(f"metrica {k} failed: {e}")
                result[k] = None
                result["errors"][k] = str(e)[:300]
    return result


def get_report(d0: date, d1: date, pd0: date, pd1: date) -> dict:
    """Все отчёты Метрики за период + сводка за предыдущий период (для дельт)."""
    if not is_connected():
        return {"connected": False, "counter": _counter()}
    key = f"metrica:v1:{_counter()}:{d0}:{d1}"
    data = get_or_compute(key, lambda: _compute(d0, d1, pd0, pd1), ttl=CACHE_TTL)
    return {**data, "counter": _counter()}
