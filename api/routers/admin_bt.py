"""Стенд — свой бэктест-терминал. /api/admin/bt/*, только админы.

API ничего не считает: кладёт прогон в очередь bt_runs, считает контейнер bt-worker (пакет backtest/), результат
читается из таблиц bt_*. Правило проектирования: в интерфейсе нет действия, которого нет здесь, — всё, что делает
человек мышкой, агент делает вызовом (или `python -m backtest.cli` на сервере).
Все ручки — обычные def (не async): SQLAlchemy синхронный, см. async_endpoints_blocking.
"""
import csv, json, os
from datetime import date, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from api import cache
from api.database import get_db
from api.models import User
from api.routers.auth import require_admin
from backtest import rules as bt_rules, costs as bt_costs, store as bt_store

router = APIRouter(prefix="/api/admin/bt", tags=["admin-backtest"])

NAMES = {'AF': 'Аэрофлот', 'AK': 'АФК Система', 'BR': 'Brent', 'CC': 'Какао', 'CR': 'Юань', 'Eu': 'Евро', 'GK': 'ГМК',
         'GZ': 'Газпром', 'LK': 'ЛУКОЙЛ', 'MN': 'Магнит', 'MX': 'Индекс МосБиржи', 'NM': 'НЛМК', 'PI': 'ПИК',
         'PT': 'Платина', 'RI': 'Индекс РТС', 'SN': 'Сургутнефтегаз', 'SR': 'Сбербанк', 'SS': 'Самолёт',
         'SZ': 'Сегежа', 'Si': 'Доллар', 'TT': 'Татнефть', 'VB': 'ВТБ'}
TF = {5, 15, 60, 1440}
MAX_DAYS = {5: 200, 15: 600, 60: 2500, 1440: 5000}     # потолок диапазона на запрос, чтобы не отдать 300 тыс. свечей


class RunIn(BaseModel):
    rule: Any = Field(..., description="имя пресета или правило целиком (см. backtest/rules.py)")
    name: Optional[str] = None
    exec: str = 'close'
    universe: Optional[list[str]] = None
    since: Optional[date] = None
    until: Optional[date] = None
    tariff: str = 'trader'
    spread: str = 'c3'
    capital: Optional[float] = 1_000_000
    slots: int = Field(6, ge=1, le=30)
    go: str = 'mr1'
    go_limit: float = Field(1.0, gt=0, le=10)
    refresh: bool = False


def _json(v):
    return v if isinstance(v, (dict, list)) or v is None else json.loads(v)


def _run_row(r, full=False):
    out = {'id': r.id, 'name': r.name, 'status': r.status, 'created_at': r.created_at, 'started_at': r.started_at,
           'finished_at': r.finished_at, 'error': r.error, 'spec': _json(r.spec)}
    res = _json(r.result)
    if full:
        out['result'] = res; out['spec_full'] = _json(r.spec_full)
    elif res:
        out['summary'] = {'сделок': res.get('per_trade', {}).get('сделок'),
                          'чистыми_%': res.get('per_trade', {}).get('чистыми_%'),
                          'годовых_%': res.get('account', {}).get('годовых_%'),
                          'просадка_%': res.get('account', {}).get('просадка_%'),
                          'Шарп': res.get('account', {}).get('Шарп')}
    return out


@router.get("/meta")
def meta(_admin: User = Depends(require_admin)):
    """Всё, из чего строится форма прогона: пресеты правил, бумаги, тарифы, режимы."""
    return {'rules': [{'id': k, 'name': r['name'], 'frozen': bool(r.get('frozen')), 'universe': r['universe'],
                       'rule': r} for k, r in bt_rules.PRESETS.items()],
            'instruments': [{'st': s, 'name': NAMES.get(s, s)} for s in bt_store.UNIVERSE_ALL],
            'tariffs': {k: v[-1][1] for k, v in bt_costs.TARIFFS.items()},
            'exec': {'close': 'как в замороженной спецификации: цена закрытия свечи 17:00 / 11:00',
                     'next_open': 'как OsEngine и TradingView: открытие следующей свечи (17:05 / 11:05)',
                     'robot': 'как робот на сервере: вход ~17:15, выход ~11:10'},
            'go': {'mr1': 'историческое: стоимость контракта × ставка риска МосБиржи на дату',
                   'snapshot': 'снимок T-Invest 15.09.2026', 'none': 'не проверять'}}


@router.post("/runs")
def create_run(body: RunIn, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    spec = json.loads(body.model_dump_json())
    try:                                                   # проверка правила до очереди: ошибка сразу, а не из воркера
        bt_rules.load(spec['rule'])
    except Exception as e:
        raise HTTPException(422, f'правило: {type(e).__name__}: {e}')
    if body.exec not in ('close', 'next_open', 'robot') or body.tariff not in bt_costs.TARIFFS \
            or body.go not in ('mr1', 'snapshot', 'none') or body.spread not in ('c3', 'c5', 'none'):
        raise HTTPException(422, 'exec / tariff / go / spread: недопустимое значение')
    rid = db.execute(text("""INSERT INTO bt_runs (name, created_by, spec) VALUES (:n, :u, CAST(:s AS JSONB))
                             RETURNING id"""),
                     {'n': body.name, 'u': admin.id, 's': json.dumps(spec, ensure_ascii=False)}).scalar()
    db.commit()
    return {'id': rid, 'status': 'queued'}


@router.get("/runs")
def list_runs(limit: int = Query(50, le=200), db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    rows = db.execute(text("SELECT * FROM bt_runs ORDER BY id DESC LIMIT :l"), {'l': limit}).fetchall()
    return [_run_row(r) for r in rows]


@router.get("/runs/{run_id}")
def get_run(run_id: int, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    r = db.execute(text("SELECT * FROM bt_runs WHERE id=:r"), {'r': run_id}).fetchone()
    if not r: raise HTTPException(404, 'нет такого прогона')
    return _run_row(r, full=True)


@router.delete("/runs/{run_id}")
def delete_run(run_id: int, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    db.execute(text("DELETE FROM bt_runs WHERE id=:r"), {'r': run_id}); db.commit()
    return {'ok': True}


def _rows(db, sql, p):
    res = db.execute(text(sql), p)
    keys = list(res.keys())
    return [dict(zip(keys, r)) for r in res.fetchall()]


@router.get("/runs/{run_id}/trades")
def run_trades(run_id: int, st: Optional[str] = None, db: Session = Depends(get_db),
               _admin: User = Depends(require_admin)):
    return _rows(db, f"""SELECT st, d, secid, side, move, thr, px_in, d_out, px_out, gross, comm, spread, net, qty,
        notional, go, equity_in, comm_rub, spread_rub, pnl_rub, account_skip
        FROM bt_trades WHERE run_id=:r {'AND st=:st' if st else ''} ORDER BY d, st""", {'r': run_id, 'st': st})


@router.get("/runs/{run_id}/signals")
def run_signals(run_id: int, st: str, date_from: Optional[date] = Query(None, alias='from'),
                date_to: Optional[date] = Query(None, alias='to'), db: Session = Depends(get_db),
                _admin: User = Depends(require_admin)):
    return _rows(db, """SELECT st, d, secid, pa, pb, move, thr_up, thr_dn, straight, side, tradable, skip
        FROM bt_signals WHERE run_id=:r AND st=:st AND d >= COALESCE(:a, DATE '1900-01-01')
          AND d <= COALESCE(:b, DATE '2999-01-01') ORDER BY d""", {'r': run_id, 'st': st, 'a': date_from, 'b': date_to})


@router.get("/runs/{run_id}/equity")
def run_equity(run_id: int, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    return _rows(db, "SELECT d, equity, positions, notional, go_used FROM bt_equity WHERE run_id=:r ORDER BY d",
                 {'r': run_id})


def _candles(db, st, tf, a, b):
    bucket = ("date_trunc('day', c.begin_time)" if tf == 1440 else
              f"date_trunc('day', c.begin_time) + floor((extract(hour from c.begin_time)*60 + "
              f"extract(minute from c.begin_time)) / {tf}) * interval '{tf} minutes'")
    rows = db.execute(text(f"""
        WITH days AS (
          SELECT c.secid, f.lsttrade, c.begin_time::date AS d, count(*) AS n
          FROM candles c JOIN futures_contracts f ON f.secid = c.secid
          WHERE f.sectype = :st AND c.interval = 5 AND c.type = 'futures'
            AND c.begin_time >= :a AND c.begin_time < :b
          GROUP BY 1, 2, 3),
        front AS (
          SELECT DISTINCT ON (d) d, secid FROM days
          WHERE lsttrade >= d AND n >= 60 AND extract(isodow from d) < 6 ORDER BY d, lsttrade)
        SELECT extract(epoch from {bucket})::bigint AS time, min(c.secid) AS secid,
               (array_agg(c.open ORDER BY c.begin_time))[1] AS open, max(c.high) AS high, min(c.low) AS low,
               (array_agg(c.close ORDER BY c.begin_time DESC))[1] AS close, sum(c.volume) AS volume
        FROM candles c JOIN front ON front.secid = c.secid AND front.d = c.begin_time::date
        WHERE c.interval = 5 AND c.type = 'futures' AND c.begin_time >= :a AND c.begin_time < :b
        GROUP BY 1 ORDER BY 1"""), {'st': st, 'a': a, 'b': b + timedelta(days=1)}).fetchall()
    # колонками, а не списком словарей: втрое меньше трафика и быстрее разбор в браузере
    secids, prev = [], None
    for i, r in enumerate(rows):
        if r.secid != prev: secids.append([i, r.secid]); prev = r.secid
    return {'st': st, 'tf': tf, 'from': a.isoformat(), 'to': b.isoformat(),
            't': [r.time for r in rows], 'o': [float(r.open) for r in rows], 'h': [float(r.high) for r in rows],
            'l': [float(r.low) for r in rows], 'c': [float(r.close) for r in rows],
            'v': [float(r.volume or 0) for r in rows], 'secid': secids}


@router.get("/candles")
def candles(st: str, tf: int = 5, date_from: Optional[date] = Query(None, alias='from'),
            date_to: Optional[date] = Query(None, alias='to'), db: Session = Depends(get_db),
            _admin: User = Depends(require_admin)):
    """Склеенный ряд: на каждый день — ближний неистёкший контракт с ≥ 60 свечами (как ряд V3 движка), будни.
    time — секунды «как будто МСК = UTC»: график показывает московское время без пересчёта поясов.
    Страница просит куски по календарным границам (месяц / квартал / год) — поэтому ответ кэшируется в Redis:
    прошлое — на сутки, кусок с сегодняшним днём — на 2 минуты."""
    if tf not in TF or st not in bt_store.UNIVERSE_ALL:
        raise HTTPException(422, 'tf: 5 | 15 | 60 | 1440; st — тип фьючерса из /meta')
    b = date_to or date.today()
    a = max(date_from or (b - timedelta(days=90 if tf == 5 else MAX_DAYS[tf])), b - timedelta(days=MAX_DAYS[tf]))
    ttl = 120 if b >= date.today() else 86400
    return cache.get_or_compute(f'bt:candles:v2:{st}:{tf}:{a}:{b}', lambda: _candles(db, st, tf, a, b), ttl)


# ─── раскладки (рабочие пространства) ────────────────────────────────────────────────────────────
class WorkspaceIn(BaseModel):
    data: dict


@router.get("/workspaces")
def list_workspaces(db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    return _rows(db, "SELECT name, updated_at FROM bt_workspaces ORDER BY updated_at DESC", {})


@router.get("/workspaces/{name}")
def get_workspace(name: str, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    r = db.execute(text("SELECT name, data, updated_at FROM bt_workspaces WHERE name=:n"), {'n': name}).fetchone()
    if not r: raise HTTPException(404, 'нет такой раскладки')
    return {'name': r.name, 'data': _json(r.data), 'updated_at': r.updated_at}


@router.put("/workspaces/{name}")
def put_workspace(name: str, body: WorkspaceIn, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    if not name or len(name) > 80: raise HTTPException(422, 'имя раскладки: 1–80 символов')
    db.execute(text("""INSERT INTO bt_workspaces (name, data) VALUES (:n, CAST(:d AS JSONB))
        ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data, updated_at = now()"""),
               {'n': name, 'd': json.dumps(body.data, ensure_ascii=False)})
    db.commit()
    return {'ok': True}


@router.delete("/workspaces/{name}")
def delete_workspace(name: str, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    db.execute(text("DELETE FROM bt_workspaces WHERE name=:n"), {'n': name}); db.commit()
    return {'ok': True}


# ─── живые сделки робота ─────────────────────────────────────────────────────────────────────────
ROBOT_JOURNAL = os.environ.get('BT_ROBOT_JOURNAL', '/data/hybrid_state/journal.csv')


@router.get("/live/trades")
def live_trades(_admin: User = Depends(require_admin)):
    """Журнал робота гибрида (песочница T-Invest, /opt/hybrid-bot/state/journal.csv, смонтирован только для чтения):
    чтобы рисовать настоящие сделки поверх бэктеста. Файла нет (локальная разработка) — пустой список."""
    if not os.path.exists(ROBOT_JOURNAL):
        return []
    out = []
    with open(ROBOT_JOURNAL, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if r.get('status_in') != 'EXECUTION_REPORT_STATUS_FILL': continue
            num = lambda k: float(r[k]) if r.get(k) not in (None, '', 'nan') else None
            out.append({'st': r['st'], 'secid': r['secid'], 'side': int(float(r['side'])), 'd': r['d_in'],
                        't_in': r.get('t_in') or None, 'px_in': num('exec_in') or num('px_in'), 'qty': num('qty'),
                        'd_out': r.get('d_out') or None, 't_out': r.get('t_out') or None,
                        'px_out': num('exec_out') or num('px_out'), 'pnl_rub': num('pnl_rub'), 'move': num('move'),
                        'note': r.get('note') or None})
    return out
