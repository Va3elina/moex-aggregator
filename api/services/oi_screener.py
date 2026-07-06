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
ATR_MIN_PART = 50        # ликвидность ФИЗ (розница): «толпа», иначе шум
ATR_MIN_PART_YUR = 15    # ЮР (институты): участников на 1-2 порядка меньше → свой порог
ATR_MIN_REL = 0.02       # материальность: |Δ|/|net| ≥ 2%
ATR_FLOOR_REL = 0.001    # ATR ≥ 0.1%·|net|, иначе позиция «заморожена»


# --- Порог релевантности активов ОИ (общий: пикер + скринер сигналов) --------
# Сплит физ/юр по фьючерсу, которым торгует мало людей, статистически шумный:
# значимость выборки зависит от АБСОЛЮТНОГО числа участников, а не от доли рынка.
# Базовый порог фиксированный (floor), динамическая часть лишь приподнимает его,
# если рынок в целом сильно раздуется. Сглаживание — медиана числа физлиц-
# трейдеров за N торговых дней (анти-фликер). Малоактивные активы прячутся ОДНИМ
# принципом и из пикера (ручка /low-activity-assets), и из ленты скринера.
OI_RELEVANCE_FLOOR = 500        # мин. число физлиц-трейдеров (абсолютная релевантность)
OI_RELEVANCE_DYN_SHARE = 0.002  # +динамика: 0.2% суммарной активности рынка
OI_RELEVANCE_SMOOTH_DAYS = 5    # окно медианы (торговых дней)


def _compute_low_activity(db) -> Dict[str, Any]:
    """Малоактивные активы + порог. Активность актива = медиана числа физлиц-
    трейдеров (pos_long_num+pos_short_num, FIZ) за OI_RELEVANCE_SMOOTH_DAYS
    торговых дней. Порог = max(floor, dyn_share × сумма медиан по рынку)."""
    rows = db.execute(text(
        """
        WITH daily AS (
            SELECT DISTINCT ON (sectype, tradedate)
                sectype, tradedate,
                (pos_long_num + pos_short_num) AS fiz
            FROM open_interest
            WHERE clgroup = 'FIZ' AND interval = 24
              AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
              AND tradedate >= CURRENT_DATE - INTERVAL '20 days'
            ORDER BY sectype, tradedate, tradetime DESC
        ),
        ranked AS (
            SELECT sectype, fiz,
                   ROW_NUMBER() OVER (
                       PARTITION BY sectype ORDER BY tradedate DESC
                   ) AS rn
            FROM daily
        )
        SELECT sectype,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY fiz) AS median_fiz
        FROM ranked
        WHERE rn <= :win
        GROUP BY sectype
        """
    ), {"win": OI_RELEVANCE_SMOOTH_DAYS}).fetchall()

    medians = {r[0]: float(r[1] or 0) for r in rows}
    total = sum(medians.values())
    threshold = max(OI_RELEVANCE_FLOOR, round(OI_RELEVANCE_DYN_SHARE * total))
    low = sorted(s for s, m in medians.items() if m < threshold)
    return {
        "threshold": threshold,
        "floor": OI_RELEVANCE_FLOOR,
        "smooth_days": OI_RELEVANCE_SMOOTH_DAYS,
        "sectypes": low,
    }


def low_activity(db) -> Dict[str, Any]:
    """Малоактивные активы, кэш 1 час (данные дневные T+1). Общий ключ для пикера
    и скринера — оба читают один результат, лишней нагрузки нет."""
    from api.cache import get_or_compute
    return get_or_compute("oi_low_activity:v1", lambda: _compute_low_activity(db), ttl=3600)


def low_activity_set(db) -> set:
    """Множество малоактивных sectype (для фильтрации строк скринера/пикера)."""
    return set(low_activity(db)["sectypes"])


def _min_part(clgroup: str) -> int:
    """Порог ликвидности по группе (у юрлиц институтов структурно меньше)."""
    return ATR_MIN_PART_YUR if clgroup == "YUR" else ATR_MIN_PART

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


def _row_signal(pts: List[tuple], min_part: int) -> Dict[str, Any]:
    """Сигнальные поля одной строки скринера по дневному ряду позиций.

    min_part — порог ликвидности для ЭТОЙ группы (физ 50 / юр 15).

    Статусы:
      sharp    — ratio ≥ 2 и ВСЕ гарды детектора прошли (материальность,
                 ликвидность, ATR-floor) — ровно условия, при которых сработал
                 бы алерт «резкое движение»;
      normal   — движение в пределах обычного (ratio показываем, если база
                 не заморожена);
      illiquid — участников < порога группы, ×N не считаем (шум);
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

    if npart_now < min_part:
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


def _record_windows() -> Dict[str, str]:
    """Окна рекордов → ISO-дата начала окна. Горизонты: 1..5 лет + всё время
    (короткие мес/полугода намеренно убраны — охотимся за редкими рекордами).
    Данные позиций у основных тикеров с 2019 → 5-летние окна реальны; у новых
    перпетуалов истории меньше, но _record_for честно вернёт им «всё время»."""
    return {
        "d1y": (date.today() - timedelta(days=365)).isoformat(),
        "d2y": (date.today() - timedelta(days=730)).isoformat(),
        "d3y": (date.today() - timedelta(days=1095)).isoformat(),
        "d4y": (date.today() - timedelta(days=1460)).isoformat(),
        "d5y": (date.today() - timedelta(days=1825)).isoformat(),
    }


def _prior_extremes(db, clgroup: str) -> Dict[str, Dict[str, Any]]:
    """Per-sectype ПРЕДЫДУЩИЕ экстремумы перекоса net_pct по окнам (для бейджа
    «новый рекорд»). Горизонты: 1 / 2 / 3 / 4 / 5 лет / всё время. Исключаем
    последний день каждого актива (tradedate < его последней даты) — сравниваем
    сегодняшний перекос с рекордом ДО сегодня. Один агрегатный проход; net_pct =
    last-bar-of-day (pos_long+pos_short)/(pos_long−pos_short)×100."""
    w = _record_windows()
    rows = db.execute(text(
        """
        WITH daily AS (
            SELECT DISTINCT ON (sectype, tradedate) sectype, tradedate,
                   (pos_long + pos_short)::float
                       / NULLIF(pos_long - pos_short, 0) * 100 AS net_pct,
                   (pos_long + pos_short) AS net
            FROM open_interest
            WHERE clgroup = :clg AND interval = 24
              AND EXTRACT(ISODOW FROM tradedate) BETWEEN 1 AND 5
            ORDER BY sectype, tradedate, tradetime DESC
        ),
        last AS (SELECT sectype, MAX(tradedate) AS ld FROM daily GROUP BY sectype)
        SELECT d.sectype,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d1y  AND d.tradedate < l.ld) AS pmax_1y,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d1y  AND d.tradedate < l.ld) AS pmin_1y,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d2y  AND d.tradedate < l.ld) AS pmax_2y,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d2y  AND d.tradedate < l.ld) AS pmin_2y,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d3y  AND d.tradedate < l.ld) AS pmax_3y,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d3y  AND d.tradedate < l.ld) AS pmin_3y,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d4y  AND d.tradedate < l.ld) AS pmax_4y,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d4y  AND d.tradedate < l.ld) AS pmin_4y,
          MAX(net_pct) FILTER (WHERE d.tradedate >= :d5y  AND d.tradedate < l.ld) AS pmax_5y,
          MIN(net_pct) FILTER (WHERE d.tradedate >= :d5y  AND d.tradedate < l.ld) AS pmin_5y,
          MAX(net_pct) FILTER (WHERE d.tradedate < l.ld) AS pmax_all,
          MIN(net_pct) FILTER (WHERE d.tradedate < l.ld) AS pmin_all,
          MAX(net) FILTER (WHERE d.tradedate < l.ld) AS net_max_all,
          MIN(net) FILTER (WHERE d.tradedate < l.ld) AS net_min_all
        FROM daily d JOIN last l USING (sectype)
        WHERE d.net_pct IS NOT NULL
        GROUP BY d.sectype
        """
    ), {"clg": clgroup, **w}).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        out[r[0]] = {
            "max_1y": r[1], "min_1y": r[2], "max_2y": r[3], "min_2y": r[4],
            "max_3y": r[5], "min_3y": r[6], "max_4y": r[7], "min_4y": r[8],
            "max_5y": r[9], "min_5y": r[10], "max_all": r[11], "min_all": r[12],
            "net_max_all": r[13], "net_min_all": r[14],
        }
    return out


# Приоритет: сильнейший (самый длинный) горизонт первым. Рекорд «за 5 лет»
# появится только если max_5y < max_all (есть данные СТАРШЕ 5 лет с бОльшим
# экстремумом) — т.е. пробили 5-летний пик, но не исторический. Для активов с
# короткой историей year-окна == all → всегда вернётся «всё время», не соврём.
_RECORD_PERIODS = ("all", "5y", "4y", "3y", "2y", "1y")


def _record_for(net_pct, ex: Dict[str, Any] | None) -> Dict[str, str] | None:
    """Сильнейший пробитый рекорд перекоса сегодня (всё время > 5 л > … > год),
    строго больше/меньше предыдущего экстремума. None если рекорда нет."""
    if net_pct is None or not ex:
        return None
    for period in _RECORD_PERIODS:
        mx, mn = ex[f"max_{period}"], ex[f"min_{period}"]
        if mx is not None and net_pct > mx:
            return {"kind": "high", "period": period}
        if mn is not None and net_pct < mn:
            return {"kind": "low", "period": period}
    return None


def _net_record_for(net, ex: Dict[str, Any] | None) -> Dict[str, str] | None:
    """Исторический (за ВСЁ время) экстремум СЫРОЙ чистой позиции (контракты).
    В отличие от _record_for (net_pct, %) — считаем по абсолютным контрактам, и
    сплиты НЕ учитываем (решение Вадима: все тикеры одинаково). Текущий net
    (последняя точка ряда, включая интрадей-бар) против all-time экстремума ДО
    сегодня. None если рекорда нет."""
    if net is None or not ex:
        return None
    mx, mn = ex.get("net_max_all"), ex.get("net_min_all")
    if mx is not None and net > mx:
        return {"kind": "high"}
    if mn is not None and net < mn:
        return {"kind": "low"}
    return None


def compute_screener(db, clgroup: str = "FIZ") -> Dict[str, Any]:
    """Полный ответ скринера: строки по всем фьючерсам с данными ОИ.

    clgroup FIZ/YUR: кратность ×N у групп зеркально идентична (net(FIZ) ≡
    −net(YUR)), но знаки/проценты/ликвидность (npart!) — свои, поэтому
    считаем честно по выбранной группе, а не флипаем знак.
    """
    series = _bulk_series(db, clgroup)
    # Экстремумы перекоса (рекорды) — самый ДОРОГОЙ запрос: скан ВСЕЙ истории
    # позиций (с 2019) по всем фьючерсам. Но меняются раз в день (T+1), не каждые
    # 5 мин → кэшируем отдельно с длинным TTL (30 мин), чтобы 5-минутный пересчёт
    # скринера (ради свежести интрадея) НЕ гонял скан истории. Интрадей-рекорды
    # всё равно ловятся: compute_screener подмешивает свежий дневной close в ex ниже.
    from api.cache import get_or_compute
    extremes = get_or_compute(
        f"oi_extremes:v2:{clgroup}", lambda: _prior_extremes(db, clgroup), ttl=1800
    )
    intraday_set = _intraday_assets(db)
    intraday_now = _bulk_intraday_now(db, clgroup, intraday_set)
    min_part = _min_part(clgroup)             # порог ликвидности группы (физ 50 / юр 15)
    low_set = low_activity_set(db)            # малоактивные активы прячем из ленты (как в пикере)

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
    intraday_date: date | None = None   # свежайшая интрадей-дата среди вплетённых 5-мин баров
    for sectype, pts in series.items():
        # Малоактивный актив (мало физлиц-трейдеров) — прячем из ленты скринера
        # тем же принципом, что и из пикера: резкое движение в фьючерсе, где
        # 2-3 физлица, это шум, а не сигнал. Общий кэш low_activity (1 час).
        if sectype in low_set:
            continue
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
            if intraday_date is None or intra[0] > intraday_date:
                intraday_date = intra[0]

        last = pts[-1]
        _d, net, npart, oi, pos_long, pos_short = last
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

        sig = _row_signal(pts, min_part)
        # Рекорд: для интрадей-актива prior-экстремумы (они «до сегодня») надо
        # дополнить ПОСЛЕДНИМ дневным close — бегущий бар новее него, и он должен
        # соперничать со всей дневной историей включая вчера.
        ex = extremes.get(sectype)
        if has_intraday and intra and intra[0] > dl[0] and ex is not None:
            # Интрадей-бар новее последнего ДНЕВНОГО → последний дневной должен войти
            # в «предыдущие» экстремумы (_prior_extremes исключил его как «сегодня»).
            # %-окна (max_/min_) вплетаем daily_last_pct; сырой net (net_*) — dl[1]
            # (дневной net), иначе pct-значением испортили бы контрактную шкалу.
            daily_last_net = dl[1]
            ex = dict(ex)
            for _k in list(ex.keys()):
                _cur = daily_last_net if _k.startswith("net_") else daily_last_pct
                if _cur is None:
                    continue
                _v = ex[_k]
                if _v is None:
                    ex[_k] = _cur
                elif "max" in _k:
                    ex[_k] = max(_v, _cur)
                else:
                    ex[_k] = min(_v, _cur)
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
            "npart": npart,                     # число участников (для подсказки illiquid)
            "ratio": sig["ratio"],
            "direction": sig["direction"],
            "status": sig["status"],
            "record": _record_for(net_pct, ex),       # рекорд ПЕРЕКОСА (net_pct, %)
            "net_record": _net_record_for(net, ex),   # рекорд СЫРОЙ чистой позиции (контракты, all-time)
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
        # Свежайшая ИНТРАДЕЙ-дата (5-мин бар новее дневной свечи). None если интрадея
        # нет. Фронт покажет честно: «дневные за X · интрадей за Y».
        "intraday_date": intraday_date.isoformat() if intraday_date else None,
        "clgroup": clgroup,
        "min_part": min_part,               # порог ликвидности группы (для подсказки)
        "rows": rows,
    }
