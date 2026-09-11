"""
Цена «на закрытие сессии в 19:00» — единственная цена акций и фьючерсов,
которую сайт отдаёт наружу (требование к раскрытию биржевой информации,
решение владельца 2026-09-10).

Правило:
  • цена торгового дня D — последняя сделка ДО 19:00 МСК этого дня;
  • наружу она выходит только после PUBLISH_AT (19:10) дня D. Десять минут —
    запас на то, чтобы 5-минутный цикл ингеста успел дописать бар 18:55;
  • сделки вечерней сессии (19:05–23:50) не попадают ни в один публичный ряд;
  • выходная сессия, закрывшаяся к 19:00, — обычный день по тем же правилам;
  • открытые позиции (ОИ) правило не затрагивает: они по-прежнему живые.

Админам (role=admin) сайт отдаёт прежнюю незамедленную версию. Чтобы увидеть
сайт глазами пользователя, админ включает на фронте «вид пользователя» — тогда
каждый запрос несёт заголовок X-Frame-View: user (см. frontend services/viewMode).

Откуда берём цену 19:00 (замер на проде 2026-09-11):
  • дневные свечи не годятся — и у акций, и у фьючерсов они включают вечернюю
    сессию (end_time 23:59:59, close = последняя сделка до 23:50);
  • фьючерсы — часовые бары: ISS отдаёт их целиком (бар 18:00 закрыт в
    18:59:59), история с 2011 года;
  • акции — 5-минутные бары. Часовые у акций пишутся один раз в начале часа и
    больше не дописываются (бар 18:00 SBER закрыт в 18:02:38, close не
    совпадает с закрытием), поэтому для цены закрытия они непригодны;
  • где внутридневных баров нет (глубокая история), остаётся дневная свеча.

⚠️ ВРЕМЯ. В БД лежит НАИВНОЕ московское время (контейнер живёт в UTC) — все
границы здесь тоже наивные московские, как в market_delay.
"""
from datetime import date, datetime, time, timedelta

from sqlalchemy import text

from api.services.market_delay import now_msk

# Сделки до этого момента входят в цену дня.
SESSION_CLOSE = time(19, 0)
# С этого момента цена дня видна наружу.
PUBLISH_AT = time(19, 10)

# Заголовок «вид пользователя» у админа.
VIEW_HEADER = "x-frame-view"

# Сколько дней назад от последнего бара искать закрытие предыдущей сессии.
# Покрывает новогодние каникулы (до 11 дней без торгов).
_LOOKBACK_DAYS = 21

# Дневную свечу вместо бара сессии разрешаем только в глубокой истории: у
# свежих дней она несёт вечернюю сессию, и такая подмена вывела бы наружу
# цену позже 19:00.
_DAILY_FALLBACK_MIN_AGE_DAYS = 3


def last_published_date(now: datetime | None = None) -> date:
    """Последний день, чья цена закрытия уже опубликована.

    До 19:10 это вчера (или раньше — ряды сами найдут последнюю сессию), после
    19:10 — сегодня. Торговый ли это день, здесь не важно: запросы берут
    последний бар не позже этой даты.
    """
    now = now or now_msk()
    if now.time() >= PUBLISH_AT:
        return now.date()
    return now.date() - timedelta(days=1)


def published_end(now: datetime | None = None) -> datetime:
    """Потолок begin_time для внутридневных баров опубликованных сессий.

    Бар входит в публичную цену, только если он начался раньше этой границы И
    раньше 19:00 своего дня (второе условие отсекает вечерние сессии прошлых
    дней — его ставят сами запросы).
    """
    return datetime.combine(last_published_date(now), SESSION_CLOSE)


def published_next_day(now: datetime | None = None) -> datetime:
    """Полночь после последнего опубликованного дня — потолок для дневных свечей."""
    return datetime.combine(last_published_date(now) + timedelta(days=1), time.min)


def is_live_viewer(user, request=None) -> bool:
    """Отдавать ли этому зрителю незамедленную цену (прежнюю версию сайта).

    Только роль admin, и только пока админ не переключился в вид пользователя.
    """
    if user is None or (getattr(user, "role", "") or "").lower() != "admin":
        return False
    if request is not None:
        try:
            if (request.headers.get(VIEW_HEADER) or "").strip().lower() == "user":
                return False
        except Exception:
            pass
    return True


def view_tag(live: bool) -> str:
    """Суффикс ключей кэша: версии для админа и для всех не должны смешиваться."""
    return "live" if live else "pub"


def session_interval(inst_type: str | None) -> int:
    """Таймфрейм баров, из которых берём цену 19:00 (см. докстринг модуля)."""
    return 60 if inst_type == "futures" else 5


_SESSION_BARS_SQL = text("""
    SELECT secid, sec_id, begin_time::date AS d,
           (array_agg(open ORDER BY begin_time))[1] AS o,
           max(high) AS h,
           min(low) AS l,
           (array_agg(close ORDER BY begin_time DESC))[1] AS c,
           sum(volume) AS v
    FROM candles
    WHERE sec_id = ANY(:sec_ids) AND interval = :interval
      AND begin_time >= :start AND begin_time < :end
      AND begin_time::time < time '19:00'
      AND close > 0 AND volume > 0
    GROUP BY secid, sec_id, begin_time::date
""")


def session_bars(db, sec_ids, interval: int, date_from: date, date_to: date,
                 now: datetime | None = None, memo: dict | None = None) -> dict:
    """Бары сессий «до 19:00» из внутридневных данных.

    Возвращает {(secid, день): (sec_id, open, high, low, close, volume)} только
    по опубликованным дням (не позже last_published_date). secid — полный код
    контракта ('SRU6'), sec_id — код серии ('SRU'), как в candles.

    memo — словарь на один проход cache_updater: ключи графиков одного актива
    просят одно и то же окно, и без него запрос повторялся бы на каждый ключ.
    """
    if not sec_ids or date_from > date_to:
        return {}
    start = datetime.combine(date_from, time.min)
    end = min(datetime.combine(date_to + timedelta(days=1), time.min), published_end(now))
    if end <= start:
        return {}
    ids = sorted(set(sec_ids))
    mkey = (tuple(ids), interval, start, end)
    if memo is not None and mkey in memo:
        return memo[mkey]
    rows = db.execute(_SESSION_BARS_SQL, {
        "sec_ids": ids, "interval": interval, "start": start, "end": end,
    }).fetchall()
    out = {
        (r[0], r[2]): (r[1], float(r[3]), float(r[4]), float(r[5]), float(r[6]), float(r[7] or 0))
        for r in rows
    }
    if memo is not None:
        memo[mkey] = out
    return out


def day_closes(bars: dict, preferred_by_day: dict | None = None) -> dict:
    """{день: цена 19:00} из session_bars.

    На один день приходится несколько контрактов (фронт и следующий). Берём тот,
    что график выбрал на этот день (preferred_by_day: день → sec_id), иначе —
    самый торгуемый за сессию.
    """
    by_day: dict = {}
    for (_secid, d), (sid, _o, _h, _l, c, v) in bars.items():
        by_day.setdefault(d, []).append((sid, c, v))
    out = {}
    for d, lst in by_day.items():
        want = (preferred_by_day or {}).get(d)
        pick = next((c for sid, c, _v in lst if sid == want), None)
        if pick is None:
            pick = max(lst, key=lambda x: x[2])[1]
        out[d] = pick
    return out


def step_values(times, closes: dict, last_pub: date, prior: float | None = None) -> list:
    """Цена-ступенька для внутридневного графика.

    В момент t действует закрытие последней сессии, чьи 19:00 уже наступили:
    до 19:00 дня D — закрытие предыдущего торгового дня, с 19:00 — закрытие
    самого D. Так на 5-минутном графике у цены две точки опоры — «вчерашний
    вечер» и «сегодняшний вечер», а между ними линия идёт горизонтально.

    times — возрастающие datetime. Возвращает список той же длины; None —
    момент надо выкинуть: вечер сессии, чьё закрытие ещё не опубликовано, или
    цена закрытия до этого момента неизвестна вовсе.
    """
    days = sorted(d for d in closes if d <= last_pub)
    out = []
    i = 0
    carried = prior
    for t in times:
        d = t.date()
        while i < len(days) and days[i] < d:
            carried = closes[days[i]]
            i += 1
        if t.time() >= SESSION_CLOSE:
            if d > last_pub:
                out.append(None)
                continue
            out.append(closes.get(d, carried))
        else:
            out.append(carried)
    return out


def publish_rows(db, rows, sec_ids, inst_type: str | None, interval: int,
                 preferred_by_day: dict | None = None, now: datetime | None = None,
                 memo: dict | None = None) -> list:
    """Переводит свечи графика в публичную цену.

    rows — кортежи (begin_time, open, high, low, close, volume, sec_id, secid)
    в порядке времени, уже по выбранному контракту дня. Возвращает кортежи той
    же формы:
      • дневной ТФ — OHLC сессии до 19:00 вместо дневной свечи; дни, чья цена
        ещё не опубликована, выпадают;
      • 5 минут и час — open=high=low=close = цена-ступенька (step_values),
        объём бара остаётся как есть (это не цена).
    """
    if not rows:
        return []
    last_pub = last_published_date(now)
    si = session_interval(inst_type)
    first_d = rows[0][0].date()
    last_d = min(rows[-1][0].date(), last_pub)

    if interval == 24:
        bars = session_bars(db, sec_ids, si, first_d, last_d, now=now, memo=memo)
        fallback_before = last_pub - timedelta(days=_DAILY_FALLBACK_MIN_AGE_DAYS)
        out = []
        for r in rows:
            d = r[0].date()
            if d > last_pub:
                continue
            bar = bars.get((r[7], d))
            if bar is not None:
                _sid, o, h, l, c, _v = bar
                out.append((r[0], o, h, l, c, r[5], r[6], r[7]))
            elif d < fallback_before:
                out.append(r)
        return out

    bars = session_bars(db, sec_ids, si, first_d - timedelta(days=_LOOKBACK_DAYS), last_d,
                        now=now, memo=memo)
    closes = day_closes(bars, preferred_by_day)
    vals = step_values([r[0] for r in rows], closes, last_pub)
    return [(r[0], v, v, v, v, r[5], r[6], r[7]) for r, v in zip(rows, vals) if v is not None]


def price_at(db, sec_ids, inst_type: str | None, interval: int, moment: datetime,
             now: datetime | None = None, memo: dict | None = None) -> float | None:
    """Публичная цена в момент moment — якорь для растяжки цены в дельте."""
    last_pub = last_published_date(now)
    si = session_interval(inst_type)
    d = moment.date()
    bars = session_bars(db, sec_ids, si, d - timedelta(days=_LOOKBACK_DAYS), min(d, last_pub),
                        now=now, memo=memo)
    closes = day_closes(bars)
    if interval == 24:
        known = [k for k in closes if k <= min(d, last_pub)]
        return closes[max(known)] if known else None
    return step_values([moment], closes, last_pub)[0]


_SECID_CLOSES_SQL = text("""
    SELECT DISTINCT ON (begin_time::date) begin_time::date AS d, close
    FROM candles
    WHERE secid = :secid AND type = :type AND interval = :interval
      AND begin_time >= :start AND begin_time < :end
      AND begin_time::time < time '19:00'
      AND close > 0 AND volume > 0
    ORDER BY begin_time::date, begin_time DESC
""")


def secid_closes(conn, secid: str, inst_type: str, date_from: date,
                 now: datetime | None = None) -> dict:
    """{день: цена 19:00} одной бумаги (secid) с даты date_from.

    Для рядов одной бумаги (сезонность, сделки фондов, экспорт): последние
    недели дневного ряда заменяются ценой 19:00, глубокая история остаётся
    дневной свечой.
    """
    start = datetime.combine(date_from, time.min)
    end = published_end(now)
    if end <= start:
        return {}
    rows = conn.execute(_SECID_CLOSES_SQL, {
        "secid": secid, "type": inst_type, "interval": session_interval(inst_type),
        "start": start, "end": end,
    }).fetchall()
    return {r[0]: float(r[1]) for r in rows}


def publish_daily_closes(rows, closes: dict, now: datetime | None = None) -> list:
    """Дневной ряд [(день, close), ...] → публичный: без неопубликованных дней,
    с ценой 19:00 там, где она известна."""
    last_pub = last_published_date(now)
    return [(d, closes.get(d, c)) for d, c in rows if d <= last_pub]


def publish_daily_ohlc(db, secid: str, rows: list, window_days: int = 60,
                       now: datetime | None = None) -> list:
    """Дневной ряд одной акции (dict с ключами trade_date/open/high/low/close)
    → публичный: без неопубликованных дней, у последних window_days дней OHLC —
    сессия до 19:00 из 5-минуток. Поле change_pct, если есть, пересчитывается
    по новым закрытиям (для выгрузок)."""
    last_pub = last_published_date(now)
    out = [r for r in rows if r["trade_date"] <= last_pub]
    if not out:
        return out
    bars = session_bars(db, [secid], session_interval("stock"),
                        last_pub - timedelta(days=window_days), last_pub, now=now)
    prev = None
    for r in out:
        bar = bars.get((secid, r["trade_date"]))
        if bar is not None:
            _sid, o, h, l, c, _v = bar
            r["open"], r["high"], r["low"], r["close"] = o, h, l, c
        if "change_pct" in r:
            cur = float(r["close"]) if r["close"] is not None else None
            r["change_pct"] = (round((cur - prev) / prev * 100, 4)
                               if cur is not None and prev else None)
            prev = cur
    return out
