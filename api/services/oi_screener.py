"""
Скринер сигналов ОИ — расчёт «резкости» дневного изменения чистой позиции
выбранной группы (физ/юр) по ВСЕМ фьючерсам разом (вкладка «Скринер
сигналов» на /oi).

Математика = signals/detectors/oi.py::compute_position_atr (алерты «резкое
движение»): ratio = |Δnet за день| / ATR(14), ATR = среднее |дневных Δ| за 14
дней ДО последнего. ⚠️ api-контейнер не видит signals/ (не копируется в образ,
см. Dockerfile) — функция и константы продублированы; МЕНЯТЬ СИНХРОННО с
источником истины signals/detectors/oi.py, иначе скринер и алерты разойдутся
в показаниях на одних данных.

Группа FIZ/YUR (тумблер на фронте): net(FIZ) ≡ −net(YUR) тождественно, поэтому
кратность ×N у групп одинакова, НО знаки/проценты/ликвидность (npart!) — свои,
поэтому считаем честно по выбранной группе, а не флипаем знак FIZ. Лента
показывает ОДНУ группу за раз (не обе сразу), иначе сигналы задвоились бы
зеркалом (ср. signals/anomaly_scan.py — там для TG-алертов берётся только FIZ).
"""
from __future__ import annotations

import statistics
from datetime import date, timedelta
from typing import Any, Dict, List

from sqlalchemy import text

from api.services import contract_calendar

# --- Константы детектора: КОПИЯ signals/detectors/oi.py (менять синхронно) ---
ATR_WINDOW = 14
ATR_MIN_PART = 50        # ликвидность: «толпа», иначе шум
ATR_MIN_REL = 0.02       # материальность: |Δ|/|net| ≥ 2%
ATR_FLOOR_REL = 0.001    # ATR ≥ 0.1%·|net|, иначе позиция «заморожена»

# Дней истории в выборке: окно + запас на выходные/праздники (как в детекторе).
_HISTORY_DAYS = ATR_WINDOW + 30

# Строк-точек минимум для расчёта (как в compute_position_atr).
_MIN_POINTS = ATR_WINDOW + 3


def _bulk_series(db, clgroup: str) -> Dict[str, List[tuple]]:
    """Дневные ряды позиций группы (FIZ/YUR) по всем sectype одним запросом.

    Возвращает {sectype: [(tradedate, net, npart, oi), ...] по возрастанию даты}.
    Тот же смысл, что signals/db.get_position_series, но bulk: DISTINCT ON
    (sectype, tradedate) → последний бар дня, только будни.
    """
    cutoff = date.today() - timedelta(days=_HISTORY_DAYS)
    rows = db.execute(text(
        """
        SELECT sectype, tradedate,
               (pos_long + pos_short)         AS net,
               (pos_long_num + pos_short_num) AS npart,
               pos                            AS oi,
               pos_long, pos_short
        FROM (
            SELECT DISTINCT ON (sectype, tradedate)
                sectype, tradedate, pos, pos_long, pos_short,
                pos_long_num, pos_short_num
            FROM open_interest
            WHERE clgroup = :clgroup
              AND interval = 24
              AND tradedate >= :cutoff
              AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
            ORDER BY sectype, tradedate, tradetime DESC
        ) t
        ORDER BY sectype, tradedate ASC
        """
    ), {"cutoff": cutoff, "clgroup": clgroup}).fetchall()

    series: Dict[str, List[tuple]] = {}
    for r in rows:
        series.setdefault(r[0], []).append((
            r[1], (r[2] or 0), (r[3] or 0), (r[4] or 0),
            (r[5] or 0), (r[6] or 0),
        ))
    return series


def _row_signal(pts: List[tuple]) -> Dict[str, Any]:
    """Сигнальные поля одной строки скринера по дневному ряду позиций.

    Статусы:
      sharp    — ratio ≥ 2 и ВСЕ гарды детектора прошли (материальность,
                 ликвидность, ATR-floor) — ровно условия, при которых сработал
                 бы алерт «резкое движение»;
      normal   — движение в пределах обычного (ratio показываем, если база
                 не заморожена);
      illiquid — участников < 50, ×N не считаем (шум);
      nodata   — мало истории.
    """
    if len(pts) < _MIN_POINTS:
        return {"status": "nodata", "ratio": None, "direction": None}

    nets = [p[1] for p in pts]
    npart_now = pts[-1][2]
    diffs = [abs(nets[i] - nets[i - 1]) for i in range(1, len(nets))]
    if len(diffs) < ATR_WINDOW + 1:
        return {"status": "nodata", "ratio": None, "direction": None}

    last_signed = nets[-1] - nets[-2]
    net = nets[-1]
    direction = "up" if last_signed > 0 else "down"

    if npart_now < ATR_MIN_PART:
        return {"status": "illiquid", "ratio": None, "direction": None}

    atr = statistics.fmean(diffs[-(ATR_WINDOW + 1):-1])
    frozen = atr <= 0 or atr < ATR_FLOOR_REL * max(abs(net), 1)
    ratio = None if frozen else round(abs(last_signed) / atr, 2)

    # «Резко» — только при полном наборе гардов алерта (консистентность:
    # скринер не должен кричать там, где алерт бы промолчал).
    material = abs(last_signed) / max(abs(net), 1) >= ATR_MIN_REL
    if ratio is not None and ratio >= 2 and material and not frozen:
        return {"status": "sharp", "ratio": ratio, "direction": direction}
    return {"status": "normal", "ratio": ratio, "direction": direction}


def _prior_extremes(db, clgroup: str) -> Dict[str, Dict[str, Any]]:
    """Per-sectype ПРЕДЫДУЩИЕ экстремумы перекоса net_pct по окнам (для бейджа
    «новый рекорд»). Исключаем последний день каждого актива (tradedate < его
    последней даты) — чтобы сравнить сегодняшний перекос с рекордом ДО сегодня.
    Один агрегатный проход; net_pct = last-bar-of-day (pos_long+pos_short)/
    (pos_long−pos_short)×100."""
    d30 = (date.today() - timedelta(days=30)).isoformat()
    d90 = (date.today() - timedelta(days=90)).isoformat()
    rows = db.execute(text(
        """
        WITH daily AS (
            SELECT DISTINCT ON (sectype, tradedate) sectype, tradedate,
                   (pos_long + pos_short)::float
                       / NULLIF(pos_long - pos_short, 0) * 100 AS net_pct
            FROM open_interest
            WHERE clgroup = :clg AND interval = 24
              AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
            ORDER BY sectype, tradedate, tradetime DESC
        ),
        last AS (SELECT sectype, MAX(tradedate) AS ld FROM daily GROUP BY sectype)
        SELECT d.sectype,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d30 AND d.tradedate < l.ld) AS pmax_1m,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d30 AND d.tradedate < l.ld) AS pmin_1m,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d90 AND d.tradedate < l.ld) AS pmax_3m,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d90 AND d.tradedate < l.ld) AS pmin_3m,
          MAX(net_pct) FILTER (WHERE d.tradedate < l.ld) AS pmax_all,
          MIN(net_pct) FILTER (WHERE d.tradedate < l.ld) AS pmin_all
        FROM daily d JOIN last l USING (sectype)
        WHERE d.net_pct IS NOT NULL
        GROUP BY d.sectype
        """
    ), {"clg": clgroup, "d30": d30, "d90": d90}).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        out[r[0]] = {
            "max_1m": r[1], "min_1m": r[2], "max_3m": r[3],
            "min_3m": r[4], "max_all": r[5], "min_all": r[6],
        }
    return out


def _record_for(net_pct, ex: Dict[str, Any] | None) -> Dict[str, str] | None:
    """Сильнейший пробитый рекорд перекоса сегодня (всё время > 3мес > месяц),
    строго больше/меньше предыдущего экстремума. None если рекорда нет."""
    if net_pct is None or not ex:
        return None
    for period, mx, mn in (("all", ex["max_all"], ex["min_all"]),
                           ("3m", ex["max_3m"], ex["min_3m"]),
                           ("1m", ex["max_1m"], ex["min_1m"])):
        if mx is not None and net_pct > mx:
            return {"kind": "high", "period": period}
        if mn is not None and net_pct < mn:
            return {"kind": "low", "period": period}
    return None


def compute_screener(db, clgroup: str = "FIZ") -> Dict[str, Any]:
    """Полный ответ скринера: строки по всем фьючерсам с данными ОИ.

    clgroup FIZ/YUR: кратность ×N у групп зеркально идентична (net(FIZ) ≡
    −net(YUR)), но знаки/проценты/ликвидность (npart!) — свои, поэтому
    считаем честно по выбранной группе, а не флипаем знак.
    """
    series = _bulk_series(db, clgroup)
    extremes = _prior_extremes(db, clgroup)   # для бейджа «новый рекорд перекоса»

    # Метаданные инструментов: имя + группа (Индексы/Валюта/Товары/Акции).
    meta = {
        r[0]: {"name": r[1], "group": r[2]}
        for r in db.execute(text(
            'SELECT sectype, name, "group" FROM instruments'
        )).fetchall()
    }
    fronts = contract_calendar.front_secids_all(db)

    rows: List[Dict[str, Any]] = []
    signal_date: date | None = None
    for sectype, pts in series.items():
        last = pts[-1]
        d, net, _npart, oi, pos_long, pos_short = last
        if signal_date is None or d > signal_date:
            signal_date = d

        # % перекоса: net / валовую (лонги + |шорты|); pos_short хранится < 0.
        gross = pos_long - pos_short
        net_pct = round(net / gross * 100, 1) if gross else None

        prev = pts[-2] if len(pts) >= 2 else None
        delta_net = (net - prev[1]) if prev else None
        oi_delta_pct = (
            round((oi - prev[3]) / prev[3] * 100, 1)
            if prev and prev[3] else None
        )
        # Вчерашний перекос — для визуализации «след кометы» (было → стало):
        # длина хвоста = |net_pct − net_pct_prev|. Считаем честно из вчерашнего
        # дня (prev[4]=pos_long, prev[5]=pos_short), не приближённо.
        net_pct_prev = None
        if prev:
            prev_gross = prev[4] - prev[5]
            if prev_gross:
                net_pct_prev = round(prev[1] / prev_gross * 100, 1)

        sig = _row_signal(pts)
        m = meta.get(sectype, {})
        rows.append({
            "sectype": sectype,
            "name": m.get("name") or sectype,
            "group": m.get("group"),
            "front_secid": fronts.get(sectype),
            "oi": oi,
            "oi_delta_pct": oi_delta_pct,
            "net": net,
            "net_pct": net_pct,
            "net_pct_prev": net_pct_prev,
            "delta_net": delta_net,
            "ratio": sig["ratio"],
            "direction": sig["direction"],
            "status": sig["status"],
            "record": _record_for(net_pct, extremes.get(sectype)),
            "signal_date": pts[-1][0].isoformat(),
        })

    # Свежесть: глобальная дата = максимум по рядам. Активы, чьи данные
    # отстали (умерший/эксп. контракт — напр. CH встал 18.06), не должны
    # светиться «резким движением» в СЕГОДНЯШНЕЙ ленте:
    #  - отстал больше чем на 5 дней → выкидываем строку целиком (мёртвый ряд);
    #  - отстал в пределах 5 дней → оставляем, но sharp гасим до normal
    #    (его «дневное движение» — не сегодняшнее).
    if signal_date is not None:
        fresh_cutoff = (signal_date - timedelta(days=5)).isoformat()
        latest = signal_date.isoformat()
        alive: List[Dict[str, Any]] = []
        for r in rows:
            if r["signal_date"] < fresh_cutoff:
                continue
            if r["signal_date"] != latest and r["status"] == "sharp":
                r["status"] = "normal"
            alive.append(r)
        rows = alive

    return {
        "signal_date": signal_date.isoformat() if signal_date else None,
        "clgroup": clgroup,
        "rows": rows,
    }
