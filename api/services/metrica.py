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

Скорость. Метрика режет параллельные запросы одного пользователя (429
«Превышена квота на количество параллельных запросов»; замер 11.09: 4 потока
без ошибок, 8 — уже 429), поэтому холодный отчёт из 12 запросов считается 2–3
секунды, и быстрее его не сделать. Отсюда stale-while-revalidate: отчёт лежит
в Redis до 6 часов; моложе 5 минут отдаём как есть, старше — тоже сразу, а
свежий считаем в фоне. Метрика сама отстаёт на несколько минут, так что чаще
спрашивать незачем, и лимиты API (≈ 30 запросов в секунду, 5000 в сутки) так
не выбираются.
"""
from __future__ import annotations

import hashlib
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional

import requests

from api.cache import get_or_compute, invalidate, set_cache, try_lock
from api.logger import get_logger

log = get_logger()

STAT_URL = "https://api-metrika.yandex.net/stat/v1/data"
BYTIME_URL = "https://api-metrika.yandex.net/stat/v1/data/bytime"
DEFAULT_COUNTER = "109137033"
FRESH_SEC = 300          # моложе — отдаём как есть
KEEP_SEC = 6 * 3600      # столько отчёт лежит в Redis: старше FRESH_SEC отдаём и пересчитываем в фоне
TIMEOUT = 20
PARALLEL = 4             # больше параллельных запросов Метрика не даёт (429)
# Семафор на процесс. Между gunicorn-воркерами он не спасает — там помогает
# повтор запроса на 429 в _get.
_SLOTS = threading.BoundedSemaphore(PARALLEL)


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
    for attempt in range(4):
        with _SLOTS:
            r = requests.get(
                url,
                params={**base, **params},
                headers={"Authorization": f"OAuth {token}"},
                timeout=TIMEOUT,
            )
        # Лимит параллельных запросов: обычно фоновый пересчёт совпал с запросом
        # страницы в другом воркере. Ждём и повторяем, а не отдаём дыру в отчёте.
        if r.status_code != 429 or attempt == 3:
            break
        time.sleep(0.4 * 2 ** attempt + random.random() * 0.3)
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
    посетителей и средние по дням не сложить. Его же итоговая строка — сводка
    за период (period_total), отдельный запрос сводки не нужен.
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
        "period_total": {k: _num(k, v) for k, v in zip(keys, by_period.get("totals") or [0] * len(keys))},
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
# по последнему значимому переходу, как график (см. SOURCE_DIM). Таблицу типов
# источников отдаёт _by_source, отдельного запроса у неё нет.
TABLES = {
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
        # Первым — самый длинный: два запроса подряд.
        "by_source": lambda: _by_source(d0, d1, flt),
        "prev_summary": lambda: _summary(pd0, pd1, flt),
    }
    for key, (dim, mets) in TABLES.items():
        f = flt_phrases if key in SEGMENT_BLIND else flt
        jobs[key] = (lambda dim=dim, mets=mets, f=f: _table(d0, d1, dim, mets, filters=f))

    result: dict[str, Any] = {"connected": True, "errors": {}}
    auth_error: Optional[MetricaAuthError] = None
    with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
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
    # Токен не принят — исключением, чтобы это не легло в кэш: после замены
    # токена блок оживает сразу.
    if auth_error is not None:
        raise auth_error

    bs = result["by_source"]
    if bs:
        result["summary"] = bs.pop("period_total")
        result["sources"] = [
            {"label": s["name"], "value": s["period"]["visits"], "value2": s["period"]["users"]}
            for s in bs["series"]
        ]
    else:
        result["summary"] = result["sources"] = None
        result["errors"]["summary"] = result["errors"]["sources"] = result["errors"]["by_source"]
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


class _Partial(Exception):
    """Отчёт с дырками (429, таймаут): отдаём как есть, но в кэш не кладём —
    иначе дыра висела бы на странице до следующего пересчёта."""

    def __init__(self, data: dict):
        super().__init__("partial report")
        self.data = data


def _stamped(compute: Callable[[], dict]) -> dict:
    data = {**compute(), "_ts": time.time()}
    if data.get("errors"):
        raise _Partial(data)
    return data


def _refresh_later(key: str, compute: Callable[[], dict]) -> None:
    """Пересчёт устаревшего отчёта в фоне — один на ключ на все воркеры."""
    if not try_lock(f"metrica-refresh:{key}", 60):
        return

    def run() -> None:
        try:
            set_cache(key, _stamped(compute), KEEP_SEC)
        except _Partial:
            pass                    # старый целый отчёт лучше нового с дырками
        except MetricaAuthError:
            invalidate(key)         # следующий запрос посчитает сам и покажет баннер
        except Exception as e:
            log.warning(f"metrica refresh failed: {e}")

    threading.Thread(target=run, name="metrica-refresh", daemon=True).start()


def get_report(d0: date, d1: date, pd0: date, pd1: date, segment: str = "everyone",
               device: str = "all", admin_ids: Optional[list[int]] = None) -> dict:
    """Все отчёты Метрики за период + сводка за предыдущий период (для дельт),
    с сегментом и устройством из шапки страницы."""
    if not is_connected():
        return {"connected": False, "counter": _counter()}
    flt = build_filter(segment, device, admin_ids or [])
    flt_phrases = device_filter(device)
    tag = hashlib.md5(flt.encode()).hexdigest()[:10] if flt else "all"
    key = f"metrica:v3:{_counter()}:{d0}:{d1}:{tag}"

    def compute() -> dict:
        return _compute(d0, d1, pd0, pd1, flt, flt_phrases)

    try:
        # Холодный промах — считает один воркер, остальные ждут его результат.
        data = get_or_compute(key, lambda: _stamped(compute), ttl=KEEP_SEC, wait_timeout=15)
        if time.time() - float(data.get("_ts") or 0) > FRESH_SEC:
            _refresh_later(key, compute)
    except _Partial as p:
        data = p.data
    except MetricaAuthError as e:
        return {"connected": False, "token_error": str(e), "counter": _counter()}
    ts = data.pop("_ts", None)
    return {
        **data,
        "counter": _counter(),
        "phrases_unsegmented": flt != flt_phrases,
        "updated_at": datetime.fromtimestamp(float(ts), timezone.utc).isoformat() if ts else None,
    }
