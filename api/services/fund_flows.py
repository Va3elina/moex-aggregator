"""Притоки и оттоки фондов — расчёт графика «Деньги в фондах» (api/routers/funds.py, /api/funds/flows).

Один код на сайт и на уведомления о рекордных потоках (signals/funds_flow_notify.py): бот обязан видеть ровно те
столбцы, что читатель видит на графике (Вадим 26.10). Здесь только чистые функции над рядами фондов; SQL выборки
и кэш остаются в роутере.

fund_series — {fund_id: [(trade_date, nav, pay), ...]} по возрастанию даты, nav/pay уже протянуты вперёд по
общей сетке дат выбранных фондов (как в SQL роутера); fill_series строит то же из сырых строк fund_data.
"""
from collections import defaultdict
from datetime import date

from sqlalchemy import text

from api.services.fund_reorg import FLOW_REORG_CORRECTIONS

# Маппинг category → отображаемое имя и индекс-бенчмарк
# min_date — нижняя граница истории. У money_market и gold реально доступная
# история до 2022 содержит шум (мало эмитентов / низкие объёмы / неточные NAV),
# поэтому отрезаем до 2022-01-09 чтобы не вводить пользователей в заблуждение.
CATEGORY_INDEX_MAP = {
    "money_market": {"name": "Денежный рынок", "index": "RUSFAR3M", "min_date": "2022-01-09"},
    "stocks": {"name": "Акции", "index": "IMOEX", "min_date": "1997-05-26"},
    "bonds": {"name": "Облигации", "index": "RGBITR", "min_date": "1997-03-24"},
    "gold": {"name": "Золото", "index": "GLDRUB_TOM", "min_date": "2022-01-09"},
    # Юаневые фонды (раздел «Юань»). Бенчмарк — валютная пара CNYRUB_TOM
    # (курс юаня к рублю, board CETS): NAV юаневых фондов хранится в ₽, поэтому
    # курс — более понятный ориентир, чем юаневая ставка RUSFAR CNY.
    # CNY — в fund_data.nav_cny/pay_cny. min_date=2024-07: до этого юаневая
    # ликвидность на MOEX была мала/нестабильна — часть фондов имеет точки
    # с 2023, но осмысленный ряд начинается с лета-2024.
    "yuan": {"name": "Юань", "index": "CNYRUB_TOM", "min_date": "2024-07-01"},
}


def load_fund_categories() -> dict:
    """Загрузка категорий фондов из БД. Возвращает структуру аналогичную старой FUND_CATEGORIES."""
    from api.database import get_engine   # лениво: чистые функции ниже не тянут подключение к БД
    engine = get_engine()
    with engine.connect() as conn:
        # Фильтр «>=2 точки NAV»: фонд с одной (или нулём) точек СЧА не может
        # показать ни линию динамики, ни приток/отток (нет «изменения») —
        # выглядит как мусор. Скрываем такие фонды из «Денег в фондах», пока
        # фетчер не накопит историю (новый фонд → появится после backfill).
        # subcategory='Авторские' в облигациях (стратегия «Рублёвые перспективы»)
        # скрыта из «Денег в фондах» — по запросу продукта, фонд-стратегия не
        # сопоставим с остальными группами по дюрации. Авторские фонды АКЦИЙ
        # (блогерские фонды Альфа-Капитал и др.) под этот запрет не подпадали
        # изначально — фильтр был написан без учёта category и скрывал их тоже;
        # ограничиваем исключение категорией bonds.
        rows = conn.execute(text(
            "SELECT fund_id, ticker, name, category, subcategory, uk_id FROM funds f "
            "WHERE (SELECT count(*) FROM fund_data fd WHERE fd.fund_id = f.fund_id AND fd.nav IS NOT NULL) >= 2 "
            "AND NOT (category = 'bonds' AND subcategory = 'Авторские') "
            "ORDER BY category, subcategory_order, ticker"
        )).fetchall()

    categories = {}
    for row in rows:
        fund_id, ticker, name, cat, subcat, uk_id = row[0], row[1], row[2], row[3], row[4], row[5]
        if cat not in CATEGORY_INDEX_MAP:
            continue
        if cat not in categories:
            meta = CATEGORY_INDEX_MAP[cat]
            categories[cat] = {
                "name": meta["name"],
                "index": meta["index"],
                "funds": {}
            }
        categories[cat]["funds"][fund_id] = {"ticker": ticker, "name": name, "subcategory": subcat, "uk_id": uk_id}

    return categories


# История «всё время» на графике: роутер берёт period=all от 2020-01-01, но не раньше min_date категории.
ALL_FROM = date(2020, 1, 1)


def _net(nav_prev, pay_prev, nav_curr, pay_curr) -> float:
    """Поток фонда за шаг: ΔСЧА минус переоценка пая от СЧА на начало шага."""
    tf = nav_curr - nav_prev
    if pay_prev > 0 and pay_curr > 0:
        return tf - nav_prev * (pay_curr - pay_prev) / pay_prev
    return tf


def corrections(category: str, cat_funds: dict, fund_series: dict) -> dict:
    """Поправки на технические реорганизации (FLOW_REORG_CORRECTIONS): день скачка — наибольший |поток| фонда в окне
    ±10 дней от даты события (устойчиво к лагу и нерабочим дням); знаковая поправка в рублях по ключу
    (fund_id, день скачка). Фонда нет в fund_series (снят пользователем) — поправка не применяется."""
    ticker_to_fid = {finfo.get("ticker"): fid for fid, finfo in cat_funds.items()}
    out: dict = {}
    for ev_ticker, ev_date_str, impact_bln in FLOW_REORG_CORRECTIONS.get(category, []):
        fid = ticker_to_fid.get(ev_ticker)
        if fid is None or fid not in fund_series:
            continue
        ev_date = date.fromisoformat(ev_date_str)
        series = fund_series[fid]
        best_day = None
        best_mag = -1.0
        for j in range(1, len(series)):
            d_prev, nav_prev, pay_prev = series[j - 1]
            d_curr, nav_curr, pay_curr = series[j]
            if abs((d_curr - ev_date).days) > 10:
                continue
            nf = _net(nav_prev, pay_prev, nav_curr, pay_curr)
            if abs(nf) > best_mag:
                best_mag = abs(nf)
                best_day = d_curr
        if best_day is not None:
            out[(fid, best_day)] = out.get((fid, best_day), 0.0) + impact_bln * 1e9
    return out


def daily_rows(fund_series: dict, corr: dict) -> list:
    """Дневные чистые потоки в рублях — базовая, самая точная дискретизация: поток = ΔСЧА − переоценка, где
    переоценка берётся от СЧА на начало ШАГА. На крупном слоте (неделя/месяц) переоценка считается от СЧА начала
    слота и занижается, если СЧА росла внутри него, поэтому величина НЕ складывается из подпериодов. Отсюда же
    считается скользящая сумма. Строка: (prev_d, curr_d, net, gross_in, gross_out, СЧА на начало шага)."""
    fund_date_map: dict = {fid: {d: (nav, pay) for d, nav, pay in rows} for fid, rows in fund_series.items()}
    all_dates = sorted(set(d for rows in fund_series.values() for d, _, _ in rows))
    out = []
    for i in range(1, len(all_dates)):
        prev_d = all_dates[i - 1]
        curr_d = all_dates[i]
        total_net = 0.0
        gross_in = 0.0
        gross_out = 0.0
        prev_total = 0.0
        for fid, date_map in fund_date_map.items():
            if prev_d not in date_map or curr_d not in date_map:
                continue  # фонд отсутствует в одном из дней — пропускаем
            prev_nav, prev_pay = date_map[prev_d]
            curr_nav, curr_pay = date_map[curr_d]
            prev_total += prev_nav
            net_flow = _net(prev_nav, prev_pay, curr_nav, curr_pay)
            c = corr.get((fid, curr_d))
            if c:
                net_flow -= c
            total_net += net_flow
            if net_flow >= 0:
                gross_in += net_flow
            else:
                gross_out += net_flow
        out.append((prev_d, curr_d, total_net, gross_in, gross_out, prev_total))
    return out


def period_key(d: date, timeframe: str):
    if timeframe == "1w":
        return d.isocalendar()[:2]
    if timeframe == "1m":
        return (d.year, d.month)
    if timeframe == "3m":
        return (d.year, (d.month - 1) // 3)
    if timeframe == "1y":
        return (d.year,)
    return d.isocalendar()[:2]


def _bar(start, end, net, g_in, g_out, base) -> dict:
    return {
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "flow": round(net / 1e9, 4),
        "gross_in": round(g_in / 1e9, 4),
        "gross_out": round(g_out / 1e9, 4),
        "flow_pct": round((net / base) * 100, 2) if base > 0 else 0,
    }


def slot_flows(fund_series: dict, corr: dict, timeframe: str) -> list:
    """Столбцы графика притоков-оттоков (млрд ₽) на таймфрейме: 1d — дневные шаги, иначе календарные слоты
    (ISO-неделя, месяц, квартал, год); последний слот может быть неполным — так он и нарисован на сайте."""
    if timeframe == "1d":
        return [_bar(*r) for r in daily_rows(fund_series, corr)]
    # Группируем каждый фонд по периодам отдельно
    fund_period: dict = {}
    for fid, rows in fund_series.items():
        periods: dict = defaultdict(list)
        for d, nav, pay in rows:
            periods[period_key(d, timeframe)].append((d, nav, pay))
        fund_period[fid] = dict(periods)
    all_period_keys = sorted(set(pk for fp in fund_period.values() for pk in fp.keys()))
    flows = []
    for i in range(1, len(all_period_keys)):
        pk = all_period_keys[i]
        ppk = all_period_keys[i - 1]
        total_net = 0.0
        gross_in = 0.0
        gross_out = 0.0
        prev_total = 0.0
        period_start = None
        period_end = None
        for fid, fp in fund_period.items():
            curr_pts = fp.get(pk)
            if not curr_pts:
                continue
            curr_pts_s = sorted(curr_pts, key=lambda x: x[0])
            start_d = curr_pts_s[0][0]
            end_d, end_nav, end_pay = curr_pts_s[-1]
            if period_start is None or start_d < period_start:
                period_start = start_d
            if period_end is None or end_d > period_end:
                period_end = end_d
            prev_pts = fp.get(ppk)
            if not prev_pts:
                # Фонд появился впервые — пропускаем (структурное изменение, не поток)
                continue
            prev_nav = sorted(prev_pts, key=lambda x: x[0])[-1][1]
            prev_pay = sorted(prev_pts, key=lambda x: x[0])[-1][2]
            prev_total += prev_nav
            net_flow = _net(prev_nav, prev_pay, end_nav, end_pay)
            for (c_fid, c_day), c_val in corr.items():
                if c_fid == fid and start_d <= c_day <= end_d:
                    net_flow -= c_val
            total_net += net_flow
            if net_flow >= 0:
                gross_in += net_flow
            else:
                gross_out += net_flow
        if period_start is not None:
            flows.append(_bar(period_start, period_end, total_net, gross_in, gross_out, prev_total))
    return flows


def fill_series(raw: list, fund_ids, date_from: date) -> dict:
    """То же, что SQL роутера: сетка — даты, где у выбранных фондов есть строка fund_data (от date_from); nav и pay
    фонда на дату — свои или последние непустые ДО неё (в том числе раньше date_from); дни без СЧА выпадают.
    raw — строки (fund_id, trade_date, nav, pay) всех нужных фондов за всю историю."""
    ids = set(fund_ids)
    by_fund: dict = defaultdict(list)
    grid = set()
    for fid, d, nav, pay in raw:
        if fid not in ids:
            continue
        by_fund[fid].append((d, nav, pay))
        if d >= date_from:
            grid.add(d)
    grid = sorted(grid)
    out: dict = {}
    for fid, rows in by_fund.items():
        rows.sort(key=lambda r: r[0])
        own = {d: (nav, pay) for d, nav, pay in rows}
        j = 0
        last_nav = last_pay = None
        series = []
        for d in grid:
            while j < len(rows) and rows[j][0] < d:     # «последнее непустое строго до даты»
                if rows[j][1] is not None:
                    last_nav = rows[j][1]
                if rows[j][2] is not None:
                    last_pay = rows[j][2]
                j += 1
            nav, pay = own.get(d, (None, None))
            nav = nav if nav is not None else last_nav
            pay = pay if pay is not None else last_pay
            if nav is not None:
                series.append((d, float(nav), float(pay or 0)))
        if series:
            out[fid] = series
    return out
