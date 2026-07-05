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


def _intraday_assets(db) -> set:
    """sectype с СВЕЖИМИ внутридневными данными позиций (interval=5 за 14 дней) —
    те же, что бейджит пикер активов (/api/oi/intraday-assets)."""
    rows = db.execute(text(
        """
        SELECT DISTINCT sectype FROM open_interest
        WHERE interval = 5 AND tradedate >= CURRENT_DATE - INTERVAL '14 days'
        """
    )).fetchall()
    return {r[0] for r in rows}


def _bulk_intraday_now(db, clgroup: str, sectypes: set) -> Dict[str, tuple]:
    """Последний ВНУТРИДНЕВНОЙ бар (interval=5) по каждому интрадей-активу —
    «net сейчас» для раннего сигнала. Один tuple на актив в формате точки ряда
    (tradedate, net, npart, oi, pos_long, pos_short).

    ⚠️ ТОЛЬКО торговые дни (ISODOW 1–5), как в _bulk_series. Иначе на выходных
    сюда попадает застоявшийся бар с датой сб/вс (≈ пятничный close), который
    compute_screener вплетает поверх пятничного ДНЕВНОГО close как «сегодня» →
    last_signed = intra − дневной ≈ 0 → РЕАЛЬНОЕ пятничное движение хоронится, и
    все интрадей-активы гаснут до normal (баг «на выходных сигналы пропали»,
    напр. Транснефть физ +51k за 3.07 показывался как −48/normal)."""
    if not sectypes:
        return {}
    rows = db.execute(text(
        """
        SELECT DISTINCT ON (sectype)
            sectype, tradedate,
            (pos_long + pos_short) AS net,
            (pos_long_num + pos_short_num) AS npart,
            pos AS oi, pos_long, pos_short
        FROM open_interest
        WHERE clgroup = :clg AND interval = 5
          AND sectype = ANY(:secs)
          AND tradedate >= CURRENT_DATE - INTERVAL '5 days'
          AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
        ORDER BY sectype, tradedate DESC, tradetime DESC
        """
    ), {"clg": clgroup, "secs": list(sectypes)}).fetchall()
    return {
        r[0]: (r[1], (r[2] or 0), (r[3] or 0), (r[4] or 0), (r[5] or 0), (r[6] or 0))
        for r in rows
    }


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
    «новый рекорд»). Только редкие горизонты: 6 месяцев / год / всё время (месяц
    и 3 мес — шум). Исключаем последний день каждого актива (tradedate < его
    последней даты) — сравниваем сегодняшний перекос с рекордом ДО сегодня.
    Один агрегатный проход; net_pct = last-bar-of-day (pos_long+pos_short)/
    (pos_long−pos_short)×100."""
    d180 = (date.today() - timedelta(days=180)).isoformat()
    d365 = (date.today() - timedelta(days=365)).isoformat()
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
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d180 AND d.tradedate < l.ld) AS pmax_6m,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d180 AND d.tradedate < l.ld) AS pmin_6m,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d365 AND d.tradedate < l.ld) AS pmax_1y,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d365 AND d.tradedate < l.ld) AS pmin_1y,
          MAX(net_pct) FILTER (WHERE d.tradedate < l.ld) AS pmax_all,
          MIN(net_pct) FILTER (WHERE d.tradedate < l.ld) AS pmin_all
        FROM daily d JOIN last l USING (sectype)
        WHERE d.net_pct IS NOT NULL
        GROUP BY d.sectype
        """
    ), {"clg": clgroup, "d180": d180, "d365": d365}).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        out[r[0]] = {
            "max_6m": r[1], "min_6m": r[2], "max_1y": r[3],
            "min_1y": r[4], "max_all": r[5], "min_all": r[6],
        }
    return out


def _record_for(net_pct, ex: Dict[str, Any] | None) -> Dict[str, str] | None:
    """Сильнейший пробитый рекорд перекоса сегодня (всё время > год > 6 мес),
    строго больше/меньше предыдущего экстремума. None если рекорда нет."""
    if net_pct is None or not ex:
        return None
    for period, mx, mn in (("all", ex["max_all"], ex["min_all"]),
                           ("1y", ex["max_1y"], ex["min_1y"]),
                           ("6m", ex["max_6m"], ex["min_6m"])):
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
    intraday_set = _intraday_assets(db)
    intraday_now = _bulk_intraday_now(db, clgroup, intraday_set)

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
        has_intraday = sectype in intraday_set
        # Последний ДНЕВНОЙ перекос (до вплетения интрадея) — нужен для корректного
        # рекорда интрадей-актива: бегущий бар новее последнего дневного close,
        # значит его надо включить в «предыдущие» экстремумы (_prior_extremes их
        # исключает как «сегодня»).
        dl = pts[-1]
        dl_gross = dl[4] - dl[5]
        daily_last_pct = (dl[1] / dl_gross * 100) if dl_gross else None

        # Интрадей: вплетаем бегущий 5-мин бар как САМУЮ СВЕЖУЮ точку, если он
        # новее последнего дневного (в течение дня дневной ещё не опубликован).
        intra = intraday_now.get(sectype) if has_intraday else None
        if intra and intra[0] > dl[0]:
            pts = pts + [intra]

        last = pts[-1]
        _d, net, _npart, oi, pos_long, pos_short = last
        # Свежесть/«мёртвость контракта» считаем по ДНЕВНОЙ дате (dl[0]), а не по
        # интрадей-бару — иначе интрадей-строки задрали бы глобальную дату к
        # «сегодня» и дневные ряды (T+1) ложно выглядели бы отставшими.
        if signal_date is None or dl[0] > signal_date:
            signal_date = dl[0]

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
        # Рекорд: для интрадей-актива prior-экстремумы (они «до сегодня») надо
        # дополнить ПОСЛЕДНИМ дневным close — бегущий бар новее него, и он должен
        # соперничать со всей дневной историей включая вчера.
        ex = extremes.get(sectype)
        if has_intraday and intra and intra[0] > dl[0] and ex is not None and daily_last_pct is not None:
            ex = {
                "max_6m": max(ex["max_6m"], daily_last_pct) if ex["max_6m"] is not None else daily_last_pct,
                "min_6m": min(ex["min_6m"], daily_last_pct) if ex["min_6m"] is not None else daily_last_pct,
                "max_1y": max(ex["max_1y"], daily_last_pct) if ex["max_1y"] is not None else daily_last_pct,
                "min_1y": min(ex["min_1y"], daily_last_pct) if ex["min_1y"] is not None else daily_last_pct,
                "max_all": max(ex["max_all"], daily_last_pct) if ex["max_all"] is not None else daily_last_pct,
                "min_all": min(ex["min_all"], daily_last_pct) if ex["min_all"] is not None else daily_last_pct,
            }
        m = meta.get(sectype, {})
        rows.append({
            "sectype": sectype,
            "name": m.get("name") or sectype,
            "group": m.get("group"),
            "front_secid": fronts.get(sectype),
            "has_intraday": has_intraday,
            "oi": oi,
            "oi_delta_pct": oi_delta_pct,
            "net": net,
            "net_pct": net_pct,
            "net_pct_prev": net_pct_prev,
            "delta_net": delta_net,
            "ratio": sig["ratio"],
            "direction": sig["direction"],
            "status": sig["status"],
            "record": _record_for(net_pct, ex),
            "signal_date": dl[0].isoformat(),   # дневная дата (для свежести/футера)
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
