#!/usr/bin/env python3
"""
Догрузка внутридневных свечей акций (5м, 60м, 7д) поверх битых баров.

Зачем. До фикса RESAVE_BARS (fetch_candles_spot_realtime.py, 2026-09)
реалтайм записывал последний бар один раз, пока тот ещё формировался, и больше
его не трогал. В БД остались снимки: часовик — первые минуты часа,
5-минутка — случайная доля своих пяти минут, недельная — утро понедельника.
Скрипт находит битые дни и перезаписывает их барами из того же источника, что
и реалтайм (Algopack/apim).

Какой день битый. Дневная свеча (interval=24) пишется из ISS целиком и верна:
день битый, если сумма объёмов баров интервала за день расходится с ней больше
чем на TOL. Неделя битая, если объём недельной свечи расходится с суммой
дневных за неделю. Целые дни не трогаем, поэтому повторный запуск продолжает
с того места, где прервался прошлый.

Когда пишем. День (неделю) перезаписываем, только если данные источника ближе
к дневному объёму, чем то, что лежит в БД. Оборванная выдача (сбой пагинации)
не может сделать хуже.

Источник:
  5м  — 1-минутки apim, агрегированные как в реалтайме (aggregate_to_5min);
  60м — часовики apim;  7д — недельные apim.
ISS отдаёт свечи страницами по 500 строк, параметр limit игнорирует.

⚠️ MOEX молча режет IP за ровный поток запросов (скилл moex-moex-ban). Запускать
вне торгов, темп — --rps. Много сбоев подряд — скрипт останавливается сам.

Использование (в контейнере оркестратора):
  python3 Candles/backfill_stock_intraday.py --from 2025-12-01 --till 2026-09-12           # план, без записи
  python3 Candles/backfill_stock_intraday.py --from 2025-12-01 --till 2026-09-12 --apply   # запись
  python3 Candles/backfill_stock_intraday.py --from 2025-12-01 --till 2026-09-12 --check   # отношение объёмов по месяцам
  ... --intervals 5,60,7   ... --tickers SBER,GAZP   ... --rps 4 --concurrency 2
  ... --only-existing  — только дни, где бары уже есть (не догружать историю
                         тикеров, у которых внутридневных баров в окне ещё не было)
"""

import argparse
import asyncio
import logging
import math
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))
load_dotenv(PROJECT_DIR / ".env")

# Цепочка Минцифры: apim.moex.com живёт на Russian Trusted Root CA, которого
# нет в certifi. См. moex_tls.py.
from moex_tls import moex_ssl_context  # noqa: E402

DB_URL = os.getenv("DB_URL")
ALGOPACK_API_KEY = os.getenv("ALGOPACK_API_KEY", "")
BASE_URL = "https://apim.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities"

TOL = 1e-3            # относительное расхождение объёмов, с которого день битый
PAGE = 500            # ISS отдаёт свечи страницами по 500
MERGE_GAP_DAYS = 4    # битые дни ближе этого берём одним диапазоном (выходные внутри)
MAX_RANGE_DAYS = 31   # длинный диапазон режем — offset пагинации не растёт без края
MAX_FAILS_IN_ROW = 8  # столько запросов подряд не прошли и после повторов — стоп
MINUTES_PER_DAY_CAP = 1000  # потолок 1-минуток в день для оценки плана

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


class Abort(Exception):
    """MOEX не отвечает или отверг ключ — продолжать бессмысленно."""


# ======================================================================
#                         СОСТОЯНИЕ БД
# ======================================================================

DAY_SQL = text("""
    SELECT begin_time::date AS d,
           SUM(volume) FILTER (WHERE interval = 24) AS dv,
           SUM(volume) FILTER (WHERE interval = 60) AS v60,
           COUNT(*)    FILTER (WHERE interval = 60) AS n60,
           SUM(volume) FILTER (WHERE interval = 5)  AS v5,
           COUNT(*)    FILTER (WHERE interval = 5)  AS n5,
           -- графики ищут внутридневные бары по sec_id: бар с пустым sec_id им не виден
           COUNT(*)    FILTER (WHERE interval IN (5, 60) AND sec_id IS NULL) AS nulls
    FROM candles
    WHERE secid = :s AND type = 'stock' AND interval IN (5, 60, 24)
      AND begin_time >= :f AND begin_time < :t
    GROUP BY 1
""")

WEEK_SQL = text("""
    SELECT begin_time, volume FROM candles
    WHERE secid = :s AND type = 'stock' AND interval = 7
      AND begin_time >= :f AND begin_time < :t
""")


class Day:
    __slots__ = ('dv', 'v', 'n')

    def __init__(self, dv: float, v60: float, n60: int, v5: float, n5: int):
        self.dv = dv
        self.v = {60: v60, 5: v5}
        self.n = {60: n60, 5: n5}


def is_off(vol: float, ref: float) -> bool:
    return abs(vol - ref) > ref * TOL


def monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def load_state(engine, secids: List[str], d_from: date, d_till: date, with_weeks: bool):
    """{secid: {день: Day}} по дням с дневным объёмом > 0, {secid: {понедельник: (begin, объём)}}
    и число баров 5м/60м с пустым sec_id в окне."""
    f = datetime.combine(monday(d_from), datetime.min.time())
    t = datetime.combine(d_till + timedelta(days=1), datetime.min.time())
    days: Dict[str, Dict[date, Day]] = {}
    weeks: Dict[str, Dict[date, Tuple[datetime, float]]] = {}
    nulls = 0
    with engine.connect() as conn:
        for s in secids:
            days[s] = {}
            for r in conn.execute(DAY_SQL, {'s': s, 'f': f, 't': t}):
                if r.d >= d_from:
                    nulls += r.nulls
                if r.dv and r.dv > 0:
                    days[s][r.d] = Day(float(r.dv), float(r.v60 or 0), int(r.n60),
                                       float(r.v5 or 0), int(r.n5))
            if with_weeks:
                weeks[s] = {r.begin_time.date(): (r.begin_time, float(r.volume or 0))
                            for r in conn.execute(WEEK_SQL, {'s': s, 'f': f, 't': t})}
    return days, weeks, nulls


def broken_days(st: Dict[date, Day], interval: int, d_from: date,
                only_existing: bool = False) -> List[date]:
    """Битые дни интервала. only_existing — только дни, где бары уже есть (без догрузки истории)."""
    return sorted(d for d, x in st.items()
                  if d >= d_from and is_off(x.v[interval], x.dv)
                  and (x.n[interval] > 0 or not only_existing))


def week_refs(st: Dict[date, Day]) -> Dict[date, float]:
    """{понедельник: сумма дневных объёмов недели}."""
    out: Dict[date, float] = defaultdict(float)
    for d, x in st.items():
        out[monday(d)] += x.dv
    return out


def broken_weeks(st: Dict[date, Day], wk: Dict[date, Tuple[datetime, float]],
                 only_existing: bool = False) -> List[date]:
    return sorted(m for m, ref in week_refs(st).items()
                  if is_off(wk.get(m, (None, 0.0))[1], ref) and (m in wk or not only_existing))


def to_ranges(days: List[date]) -> List[Tuple[date, date]]:
    out: List[Tuple[date, date]] = []
    for d in days:
        if out and (d - out[-1][1]).days <= MERGE_GAP_DAYS and (d - out[-1][0]).days < MAX_RANGE_DAYS:
            out[-1] = (out[-1][0], d)
        else:
            out.append((d, d))
    return out


# ======================================================================
#                         ИСТОЧНИК
# ======================================================================

class Moex:
    """apim с общим темпом запросов на все корутины."""

    def __init__(self, session: aiohttp.ClientSession, rps: float):
        self.session = session
        self.gap = 1.0 / rps
        self.next_at = 0.0
        self.lock = asyncio.Lock()
        self.requests = 0
        self.fails_in_row = 0

    async def _pace(self):
        async with self.lock:
            now = time.monotonic()
            wait = self.next_at - now
            self.next_at = max(now, self.next_at) + self.gap
        if wait > 0:
            await asyncio.sleep(wait)

    async def _page(self, url: str, params: dict) -> Tuple[list, list]:
        last = None
        for attempt in range(4):
            await self._pace()
            self.requests += 1
            try:
                async with self.session.get(
                        url, params=params,
                        headers={'Authorization': f'Bearer {ALGOPACK_API_KEY}'},
                        timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status in (401, 403):
                        raise Abort(f"HTTP {resp.status} от apim — ключ Algopack?")
                    if resp.status == 429:
                        log.warning("429 от apim, пауза 60 с")
                        await asyncio.sleep(60)
                        continue
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)
                c = data['candles']
                self.fails_in_row = 0
                return c['columns'], c['data']
            except Abort:
                raise
            except Exception as e:
                last = e
                if attempt < 3:
                    await asyncio.sleep(5 * 3 ** attempt)  # 5, 15, 45 с
        self.fails_in_row += 1
        if self.fails_in_row >= MAX_FAILS_IN_ROW:
            raise Abort(f"{self.fails_in_row} запросов подряд не прошли (последняя ошибка: {last!r}) "
                        f"— похоже, MOEX режет IP, см. скилл moex-moex-ban")
        raise RuntimeError(f"не прошло и после повторов: {last!r}")

    async def candles(self, ticker: str, interval: int, d1: date, d2: date) -> List[dict]:
        url = f"{BASE_URL}/{ticker}/candles.json"
        out: List[dict] = []
        start = 0
        while True:
            cols, rows = await self._page(url, {
                'interval': interval, 'from': d1.isoformat(), 'till': d2.isoformat(), 'start': start,
            })
            out.extend(dict(zip(cols, r)) for r in rows)
            if len(rows) < PAGE:
                return out
            start += len(rows)


def _ts(s: str) -> datetime:
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')


# Бар: (begin, end, open, high, low, close, value, volume)
Bar = Tuple[datetime, datetime, float, float, float, float, float, float]


def bars_direct(rows: List[dict]) -> List[Bar]:
    return [(_ts(r['begin']), _ts(r['end']), r['open'], r['high'], r['low'], r['close'],
             r['value'] or 0, r['volume'] or 0)
            for r in rows if r['open'] is not None and r['close'] is not None]


def bars_5min(rows_1m: List[dict]) -> List[Bar]:
    """1-минутки → 5-минутки, как aggregate_to_5min в реалтайме."""
    acc: Dict[datetime, list] = {}
    for r in sorted(rows_1m, key=lambda r: r['begin']):
        if r['open'] is None or r['close'] is None:
            continue
        b = _ts(r['begin'])
        k = b.replace(minute=b.minute - b.minute % 5, second=0)
        a = acc.get(k)
        if a is None:
            acc[k] = [r['open'], r['high'], r['low'], r['close'], r['value'] or 0, r['volume'] or 0]
        else:
            a[1] = max(a[1], r['high'])
            a[2] = min(a[2], r['low'])
            a[3] = r['close']
            a[4] += r['value'] or 0
            a[5] += r['volume'] or 0
    return [(k, k + timedelta(minutes=5), *a) for k, a in acc.items()]


# ======================================================================
#                         ЗАПИСЬ
# ======================================================================

UPSERT = """
    INSERT INTO candles (secid, sec_id, type, interval, begin_time, end_time,
                         open, high, low, close, value, volume)
    VALUES {values}
    ON CONFLICT (secid, begin_time, interval, type) DO UPDATE SET
        sec_id = EXCLUDED.sec_id, end_time = EXCLUDED.end_time,
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, value = EXCLUDED.value, volume = EXCLUDED.volume
    WHERE (candles.sec_id, candles.end_time, candles.open, candles.high, candles.low,
           candles.close, candles.value, candles.volume)
          IS DISTINCT FROM
          (EXCLUDED.sec_id, EXCLUDED.end_time, EXCLUDED.open, EXCLUDED.high, EXCLUDED.low,
           EXCLUDED.close, EXCLUDED.value, EXCLUDED.volume)
    RETURNING (xmax = 0)
"""


def save(engine, secid: str, interval: int, bars: List[Bar]) -> Tuple[int, int]:
    """Upsert баров одним коммитом. Возвращает (вставлено, обновлено)."""
    rows = [(secid, secid, 'stock', interval, *b) for b in bars]
    ins = upd = 0
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        for i in range(0, len(rows), PAGE):
            batch = rows[i:i + PAGE]
            values = ','.join(['(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'] * len(batch))
            cur.execute(UPSERT.format(values=values), [v for r in batch for v in r])
            for (fresh,) in cur.fetchall():
                if fresh:
                    ins += 1
                else:
                    upd += 1
        raw.commit()
        cur.close()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()
    return ins, upd


def judge(src: float, db: float, ref: float) -> str:
    if not is_off(src, ref):
        return 'fixed'
    if abs(src - ref) < abs(db - ref):
        return 'improved'
    return 'skipped'


# ======================================================================
#                         ПРОГОН ПО ТИКЕРУ
# ======================================================================

class Stats:
    def __init__(self):
        self.c = defaultdict(int)

    def add(self, key: str, n: int = 1):
        self.c[key] += n


async def fix_days(moex: Moex, engine, secid: str, st: Dict[date, Day], interval: int,
                   d_from: date, apply: bool, stats: Stats, only_existing: bool = False):
    bad = broken_days(st, interval, d_from, only_existing)
    if not bad:
        return
    bad_set = set(bad)
    for d1, d2 in to_ranges(bad):
        try:
            if interval == 5:
                bars = bars_5min(await moex.candles(secid, 1, d1, d2))
            else:
                bars = bars_direct(await moex.candles(secid, interval, d1, d2))
        except RuntimeError as e:
            log.error(f"  {secid} {interval}м {d1}..{d2}: {e}")
            stats.add(f'{interval}:range_errors')
            continue
        by_day: Dict[date, List[Bar]] = defaultdict(list)
        for b in bars:
            by_day[b[0].date()].append(b)
        write: List[Bar] = []
        for d in (x for x in bad if d1 <= x <= d2):
            x = st[d]
            src = sum(b[7] for b in by_day.get(d, []))
            verdict = judge(src, x.v[interval], x.dv)
            stats.add(f'{interval}:{verdict}')
            if verdict != 'skipped':
                write.extend(by_day[d])
        stats.add(f'{interval}:days', sum(1 for x in bad_set if d1 <= x <= d2))
        if apply and write:
            ins, upd = await asyncio.to_thread(save, engine, secid, interval, write)
            stats.add(f'{interval}:inserted', ins)
            stats.add(f'{interval}:updated', upd)
        elif write:
            stats.add(f'{interval}:would_write', len(write))


async def fix_weeks(moex: Moex, engine, secid: str, st: Dict[date, Day],
                    wk: Dict[date, Tuple[datetime, float]], d_from: date, d_till: date,
                    apply: bool, stats: Stats, only_existing: bool = False):
    bad = broken_weeks(st, wk, only_existing)
    if not bad:
        return
    try:
        bars = bars_direct(await moex.candles(secid, 7, monday(d_from), d_till))
    except RuntimeError as e:
        log.error(f"  {secid} 7д: {e}")
        stats.add('7:range_errors')
        return
    refs = week_refs(st)
    src_by_week = {b[0].date(): b for b in bars}
    write: List[Bar] = []
    for m in bad:
        b = src_by_week.get(m)
        db_begin, db_vol = wk.get(m, (None, 0.0))
        if b is None or (db_begin is not None and db_begin != b[0]):
            # Разные begin у одной недели — upsert вставил бы дубль, не рискуем.
            stats.add('7:skipped')
            continue
        verdict = judge(b[7], db_vol, refs[m])
        stats.add(f'7:{verdict}')
        if verdict != 'skipped':
            write.append(b)
    stats.add('7:days', len(bad))
    if apply and write:
        ins, upd = await asyncio.to_thread(save, engine, secid, 7, write)
        stats.add('7:inserted', ins)
        stats.add('7:updated', upd)
    elif write:
        stats.add('7:would_write', len(write))


# ======================================================================
#                         ПЛАН И ПРОВЕРКА
# ======================================================================

def estimate_1m_rows(st: Dict[date, Day], days: List[date]) -> Tuple[int, int]:
    """Вилка числа 1-минуток за дни: от числа 5-мин баров до пятикратного."""
    known = [x.n[5] for x in st.values() if x.n[5] > 0]
    fallback = int(median(known)) if known else 20
    lo = hi = 0
    for d in days:
        x = st.get(d)
        n5 = x.n[5] if x and x.n[5] > 0 else fallback
        lo += n5
        hi += min(5 * n5, MINUTES_PER_DAY_CAP)
    return lo, hi


def print_plan(days_state, weeks_state, intervals, d_from, d_till, rps, only_existing):
    req_lo = req_hi = 0
    for interval in (i for i in intervals if i in (5, 60)):
        n_sec = n_bad = n_zero = n_weekend = n_ranges = rows = 0
        zero_by_sec = defaultdict(int)
        lo_total = hi_total = 0
        for s, st in days_state.items():
            bad = broken_days(st, interval, d_from, only_existing)
            if not bad:
                continue
            n_sec += 1
            n_bad += len(bad)
            for d in bad:
                if st[d].n[interval] == 0:
                    n_zero += 1
                    zero_by_sec[s] += 1
                if d.weekday() >= 5:
                    n_weekend += 1
                rows += st[d].n[interval]
            for d1, d2 in to_ranges(bad):
                n_ranges += 1
                span = [d for d in st if d1 <= d <= d2]
                if interval == 5:
                    lo, hi = estimate_1m_rows(st, span)
                else:
                    lo = hi = 18 * len(span)
                lo_total += math.ceil((lo + 1) / PAGE)
                hi_total += math.ceil((hi + 1) / PAGE)
        req_lo += lo_total
        req_hi += hi_total
        log.info(f"{interval}м: тикеров {n_sec}, битых дней {n_bad} (без единого бара {n_zero}, "
                 f"выходных {n_weekend}), диапазонов {n_ranges}, баров в БД на битых днях {rows}, "
                 f"запросов ~{lo_total}–{hi_total}")
        if zero_by_sec:
            top = sorted(zero_by_sec.items(), key=lambda kv: -kv[1])[:15]
            first = {s: min((d for d, x in days_state[s].items() if x.n[interval] > 0), default=None)
                     for s, _ in top}
            log.info(f"  дни без баров, топ: " + ', '.join(f"{s} {n} (1-й бар {first[s]})" for s, n in top))
    if 7 in intervals:
        n_sec = n_weeks = 0
        for s, st in days_state.items():
            bad = broken_weeks(st, weeks_state.get(s, {}), only_existing)
            if bad:
                n_sec += 1
                n_weeks += len(bad)
        req_lo += n_sec
        req_hi += n_sec
        log.info(f"7д: тикеров {n_sec}, битых недель {n_weeks}, запросов ~{n_sec}")
    log.info(f"ИТОГО запросов ~{req_lo}–{req_hi}; при {rps} запр/с — "
             f"{req_lo / rps / 60:.0f}–{req_hi / rps / 60:.0f} мин")


def print_check(days_state, weeks_state, intervals, d_from):
    def table(secids, label):
        agg = defaultdict(lambda: {i: [] for i in (5, 60)})
        sums = defaultdict(lambda: defaultdict(float))
        for s in secids:
            for d, x in days_state.get(s, {}).items():
                if d < d_from:
                    continue
                k = d.strftime('%Y-%m')
                sums[k]['dv'] += x.dv
                for i in (5, 60):
                    agg[k][i].append(x.v[i] / x.dv)
                    sums[k][i] += x.v[i]
        log.info(f"--- {label}: по месяцам — среднее отношение за день / минимум / целых дней / отношение сумм")
        for k in sorted(agg):
            parts = []
            for i in (60, 5):
                if i not in intervals:
                    continue
                r = agg[k][i]
                ok = sum(1 for v in r if abs(v - 1) <= TOL)
                parts.append(f"{i}м {sum(r) / len(r):.3f} / {min(r):.3f} / {ok}/{len(r)} / "
                             f"{sums[k][i] / sums[k]['dv']:.3f}")
            log.info(f"{k}  " + '  |  '.join(parts))

    table([s for s in ('SBER', 'GAZP', 'LKOH') if s in days_state], 'SBER+GAZP+LKOH')
    table(list(days_state), 'все акции')
    if 7 in intervals:
        per_month = defaultdict(lambda: [0, 0])
        for s, st in days_state.items():
            wk = weeks_state.get(s, {})
            bad = set(broken_weeks(st, wk))
            for m in week_refs(st):
                if m < monday(d_from):
                    continue
                k = m.strftime('%Y-%m')
                per_month[k][1] += 1
                per_month[k][0] += m not in bad
        log.info("--- 7д: целых недель / всего")
        for k in sorted(per_month):
            log.info(f"{k}  {per_month[k][0]}/{per_month[k][1]}")


# ======================================================================
#                         MAIN
# ======================================================================

async def run(args) -> int:
    d_from = date.fromisoformat(args.date_from)
    d_till = date.fromisoformat(args.date_till)
    intervals = sorted({int(x) for x in args.intervals.split(',')})
    if not set(intervals) <= {5, 60, 7}:
        log.error("--intervals: только 5, 60, 7")
        return 1
    if not DB_URL:
        log.error("Не задан DB_URL")
        return 1

    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        secids = [r[0] for r in conn.execute(text(
            "SELECT sec_id FROM instruments WHERE type = 'stock' ORDER BY sec_id"))]
    if args.tickers:
        want = {t.strip().upper() for t in args.tickers.split(',')}
        secids = [s for s in secids if s in want]

    t0 = time.time()
    days_state, weeks_state, nulls = load_state(engine, secids, d_from, d_till, 7 in intervals)
    log.info(f"Окно {d_from}..{d_till}, интервалы {intervals}, тикеров {len(secids)}; "
             f"состояние БД прочитано за {time.time() - t0:.0f} с")
    if nulls:
        log.info(f"Баров 5м/60м с пустым sec_id в окне: {nulls}")

    if args.check:
        print_check(days_state, weeks_state, intervals, d_from)
        return 0

    only = args.only_existing
    print_plan(days_state, weeks_state, intervals, d_from, d_till, args.rps, only)
    if not args.apply:
        log.info("Это план. Запись — с флагом --apply.")
        return 0
    if not ALGOPACK_API_KEY:
        log.error("Не задан ALGOPACK_API_KEY")
        return 1

    stats = Stats()
    todo = [s for s in secids
            if any(broken_days(days_state[s], i, d_from, only) for i in intervals if i != 7)
            or (7 in intervals and broken_weeks(days_state[s], weeks_state.get(s, {}), only))]
    log.info(f"В работу тикеров: {len(todo)}")
    sem = asyncio.Semaphore(args.concurrency)
    done = 0

    connector = aiohttp.TCPConnector(limit=args.concurrency, ssl=moex_ssl_context())
    async with aiohttp.ClientSession(connector=connector) as session:
        moex = Moex(session, args.rps)

        async def one(secid: str):
            nonlocal done
            async with sem:
                st = days_state[secid]
                try:
                    for interval in intervals:
                        if interval == 7:
                            await fix_weeks(moex, engine, secid, st, weeks_state.get(secid, {}),
                                            d_from, d_till, True, stats, only)
                        else:
                            await fix_days(moex, engine, secid, st, interval, d_from, True, stats, only)
                except Abort:
                    raise
                except Exception as e:
                    # Сбой одного тикера (БД, разбор ответа) — не повод бросать остальные.
                    log.exception(f"  {secid}: {e!r}")
                    stats.add('ticker_errors')
                done += 1
                log.info(f"[{done}/{len(todo)}] {secid} готов; запросов {moex.requests}, "
                         f"{(time.time() - t0) / 60:.1f} мин")

        tasks = [asyncio.create_task(one(s)) for s in todo]
        try:
            await asyncio.gather(*tasks)
        except Abort as e:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log.error(f"ОСТАНОВ: {e}. Сделанное записано; повторный запуск продолжит с битых дней.")
            return 2
        finally:
            log.info("ИТОГ: " + ', '.join(f"{k}={v}" for k, v in sorted(stats.c.items())))
            log.info(f"Запросов к apim: {moex.requests}, время {(time.time() - t0) / 60:.1f} мин")
    return 0


def main():
    p = argparse.ArgumentParser(description='Догрузка 5м/60м/7д свечей акций поверх битых баров')
    p.add_argument('--from', dest='date_from', required=True, help='Первый день окна, YYYY-MM-DD')
    p.add_argument('--till', dest='date_till', required=True,
                   help='Последний день окна (включительно). Только закрытые сессии')
    p.add_argument('--intervals', default='5,60', help='Какие интервалы чинить: 5,60,7')
    p.add_argument('--tickers', help='Только эти тикеры, через запятую')
    p.add_argument('--rps', type=float, default=4.0, help='Запросов к apim в секунду (по умолчанию 4)')
    p.add_argument('--concurrency', type=int, default=2, help='Тикеров параллельно (по умолчанию 2)')
    p.add_argument('--only-existing', action='store_true',
                   help='Чинить только дни, где бары уже есть; дни без баров не догружать')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--apply', action='store_true', help='Записывать в БД (без флага — только план)')
    g.add_argument('--check', action='store_true', help='Только отношение объёмов по месяцам')
    sys.exit(asyncio.run(run(p.parse_args())))


if __name__ == '__main__':
    main()
