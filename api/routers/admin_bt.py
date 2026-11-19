"""Стенд — свой бэктест-терминал. /api/admin/bt/*, только админы.

API ничего не считает: кладёт прогон в очередь bt_runs, считает контейнер bt-worker (пакет backtest/), результат
читается из таблиц bt_*. Правило проектирования: в интерфейсе нет действия, которого нет здесь, — всё, что делает
человек мышкой, агент делает вызовом (или `python -m backtest.cli` на сервере).
Все ручки — обычные def (не async): SQLAlchemy синхронный, см. async_endpoints_blocking.
"""
import calendar, csv, glob, json, os, re, urllib.error, urllib.request
from datetime import date, datetime, timedelta
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
         'SZ': 'Сегежа', 'Si': 'Доллар', 'TT': 'Татнефть', 'VB': 'ВТБ', 'CNYRUBF': 'Юань вечный',
         'USDRUBF': 'Доллар вечный', 'EURRUBF': 'Евро вечный', 'GLDRUBF': 'Золото вечное', 'IMOEXF': 'Индекс МосБиржи вечный',
         'SBERF': 'Сбербанк вечный', 'GAZPF': 'Газпром вечный'}
STRATEGIES = {'screener_vb': 'Скринер VB: объёмный пробой лонг + шорт (акции TQBR, дневные свечи)'}
CRYPTO_RE = re.compile(r'[A-Z0-9]{2,20}USDT')        # монета Bybit (бессрочный контракт USDT): свечи — с публичного API Bybit
STOCK_RE = re.compile(r'^[A-Z][A-Z0-9]{0,11}$')         # тикер акции TQBR: график по дневным свечам candles (type='stock')
TF = {5, 15, 60, 1440}
MAX_DAYS = {5: 200, 15: 600, 60: 2500, 1440: 5000}     # потолок диапазона на запрос, чтобы не отдать 300 тыс. свечей


class RunIn(BaseModel):
    rule: Any = Field(None, description="имя пресета или правило целиком (см. backtest/rules.py)")
    code: Optional[str] = Field(None, max_length=60_000, description="стратегия на Python (backtest/pyengine.py); исполняется в bt-sandbox")
    strategy: Optional[str] = Field(None, description="готовая стратегия Стенда: screener_vb (backtest/screener_vb.py)")
    guard: Optional[float] = Field(None, ge=0, le=0.9, description="screener_vb с плечом: просадка от пика, после которой новые сделки ×1")
    params: Optional[dict] = None
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
    leverage: float = Field(1.0, ge=0.1, le=10)
    go_mult: float = Field(1.0, ge=0.5, le=5)
    checks: bool = False
    sweep: Optional[dict] = Field(None, description='{"grid": {"long.q": [0.6, 0.67]}, "oos_from": "2025-01-01"} — перебор параметров')
    refresh: bool = False
    auto: bool = Field(False, description='прогон создан автопересчётом при смене бумаги — интерфейс сам чистит старые')


def _json(v):
    return v if isinstance(v, (dict, list)) or v is None else json.loads(v)


def _run_row(r, full=False):
    out = {'id': r.id, 'name': r.name, 'status': r.status, 'progress': getattr(r, 'progress', None),
           'kind': 'sweep' if (_json(r.spec) or {}).get('sweep') else 'run', 'created_at': r.created_at, 'started_at': r.started_at,
           'finished_at': r.finished_at, 'error': r.error, 'spec': _json(r.spec)}
    res = _json(r.result)
    if full:
        out['result'] = {k: v for k, v in res.items() if k != 'fills'} if isinstance(res, dict) else res   # заявки кампаний — /runs/{id}/fills
        out['spec_full'] = _json(r.spec_full)
    elif res:
        out['summary'] = {'сделок': res.get('per_trade', {}).get('сделок'),
                          'чистыми_%': res.get('per_trade', {}).get('чистыми_%'),
                          'годовых_%': res.get('account', {}).get('годовых_%'),
                          'просадка_%': res.get('account', {}).get('просадка_%'),
                          'Шарп': res.get('account', {}).get('Шарп')}
    return out


def _py_template() -> str:
    from backtest import pyengine
    return pyengine.TEMPLATE


def _py_examples() -> list[dict]:
    from backtest import pyengine
    return [{'name': 'Пример: пересечение двух EMA', 'code': pyengine.TEMPLATE}]


def _inspect(code: str) -> dict:
    """Синтаксис и PARAMS стратегии — через ast, БЕЗ исполнения кода (API-процесс чужой код не запускает никогда)."""
    import ast
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return {'error': f'строка {e.lineno}: {e.msg}', 'params': {}, 'has_on_bar': False}
    params, fns = {}, set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef): fns.add(node.name)
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'PARAMS' for t in node.targets):
            try: params = ast.literal_eval(node.value)
            except Exception: return {'error': 'PARAMS должен быть словарём из чисел и строк', 'params': {}, 'has_on_bar': 'on_bar' in fns}
    from backtest import orders
    mode = 'orders' if orders.is_orders_code(code) else 'signals'
    return {'error': None if 'on_bar' in fns else ('в коде нет функции on_bar(i, b, s, p)' if mode == 'orders' else 'в коде нет функции on_bar(i, b, pos, p)'),
            'params': params if isinstance(params, dict) else {}, 'has_on_bar': 'on_bar' in fns, 'mode': mode}


class CodeIn(BaseModel):
    code: str = Field(..., max_length=60_000)


@router.post("/python/inspect")
def python_inspect(body: CodeIn, _admin: User = Depends(require_admin)):
    return _inspect(body.code)


def _crypto_list():
    """Монеты, по которым боты Kamaz совершали реальные сделки на демо-счетах (по журналам), — в поиске бумаг Стенда.
    Журналы растут каждый день — список кэшируется на 5 минут."""
    def build():
        seen = set()
        for _, d, fmt, _ in ROBOTS:
            if fmt != 'kamaz': continue
            for fn in glob.glob(os.path.join(ROBOTS_DIR, d, 'journal', '*.jsonl')):
                try:
                    with open(fn, encoding='utf-8', errors='replace') as f:
                        for line in f:
                            if '"реал"' in line and '"исполнение"' in line:
                                try: seen.add(json.loads(line)['sym'])
                                except (KeyError, ValueError): pass
                except OSError: pass
        return [{'st': x, 'name': x.replace('USDT', '').replace('1000', '') + ' · Bybit'} for x in sorted(seen)]
    try:
        return cache.get_or_compute('bt:crypto_list:v1', build, 300)
    except Exception:
        return []


@router.get("/meta")
def meta(db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    """Всё, из чего строится форма прогона: пресеты правил, бумаги, тарифы, режимы."""
    stocks = cache.get_or_compute('bt:stocks:v1', lambda: _rows(db, """SELECT sec_id AS st, name FROM instruments
        WHERE type = 'stock' AND name IS NOT NULL ORDER BY sec_id""", {}), 3600)
    return {'strategies': [{'id': k, 'name': v} for k, v in STRATEGIES.items()], 'stocks': stocks, 'crypto': _crypto_list(), 'rules': [{'id': k, 'name': r['name'], 'frozen': bool(r.get('frozen')), 'universe': r['universe'],
                       'rule': r} for k, r in bt_rules.PRESETS.items() if k in bt_rules.SHOWN],
            'instruments': [{'st': s, 'name': NAMES.get(s, s)} for s in bt_store.UNIVERSE_ALL],
            'tariffs': {k: v[-1][1] for k, v in bt_costs.TARIFFS.items()}, 'tariff_labels': bt_costs.TARIFF_LABELS,
            'spread_daily': bool(bt_costs.spread_daily()),
            'python_template': _py_template(), 'python_examples': _py_examples(),
            'exec': {'close': 'как в замороженной спецификации: цена закрытия свечи 17:00 / 11:00',
                     'next_open': 'как OsEngine и TradingView: открытие следующей свечи (17:05 / 11:05)',
                     'robot': 'как робот на сервере: вход ~17:15, выход ~11:10'},
            'go': {'mr1': 'историческое: стоимость контракта × ставка риска МосБиржи на дату',
                   'snapshot': 'снимок T-Invest 15.09.2026', 'tv100': 'как TradingView по умолчанию: 100 % стоимости позиции',
                   'none': 'не проверять'}}


@router.post("/runs")
def create_run(body: RunIn, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    spec = json.loads(body.model_dump_json())
    if body.strategy:                                      # готовая стратегия: правила в коде Стенда, проверять нечего
        if body.strategy not in STRATEGIES: raise HTTPException(422, f'стратегия: {", ".join(STRATEGIES)}')
    elif body.code:                                          # здесь код только РАЗБИРАЕТСЯ (ast), не исполняется
        info = _inspect(body.code)
        if info['error']: raise HTTPException(422, info['error'])
        if body.sweep: raise HTTPException(422, 'перебор параметров для стратегий на Python пока не поддержан')
    else:
        try:                                               # проверка правила до очереди: ошибка сразу, а не из воркера
            bt_rules.load(spec['rule'])
        except Exception as e:
            raise HTTPException(422, f'правило: {type(e).__name__}: {e}')
    if body.exec not in ('close', 'next_open', 'robot') or body.tariff not in bt_costs.TARIFFS \
            or body.go not in ('mr1', 'snapshot', 'tv100', 'none') or body.spread not in ('daily', 'c3', 'c5', 'none'):
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


@router.post("/runs/{run_id}/checks")
def run_checks(run_id: int, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    """Досчитать батарею проверок (плацебо, половины истории, хвосты, соседние параметры…) — прогон встаёт в очередь заново."""
    n = db.execute(text("""UPDATE bt_runs SET spec = spec || '{"checks": true}'::jsonb, status='queued', error=NULL
                           WHERE id=:r AND status IN ('done', 'error')"""), {'r': run_id}).rowcount
    db.commit()
    if not n: raise HTTPException(409, 'прогон не найден или ещё считается')
    return {'id': run_id, 'status': 'queued'}


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
    return _rows(db, f"""SELECT n, st, d, secid, side, move, thr, px_in, d_out, px_out, gross, comm, spread, net, qty,
        notional, go, equity_in, comm_rub, spread_rub, pnl_rub, account_skip, go_cut, m_in, m_out, exit_reason, mfe, mae
        FROM bt_trades WHERE run_id=:r {'AND st=:st' if st else ''} ORDER BY d, m_in, st""", {'r': run_id, 'st': st})


@router.get("/runs/{run_id}/fills")
def run_fills(run_id: int, st: str, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    """Заявки кампаний импортированного прогона (крипта Kamaz): {n: [[время МСК, цена, количество, что], ...]} по бумаге."""
    ns = [str(x[0]) for x in db.execute(text("SELECT n FROM bt_trades WHERE run_id=:r AND st=:st"), {'r': run_id, 'st': st}).fetchall()]
    if not ns: return {}
    f = db.execute(text("SELECT result->'fills' FROM bt_runs WHERE id=:r"), {'r': run_id}).scalar()
    f = _json(f) or {}
    return {n: f[n] for n in ns if n in f}


@router.get("/runs/{run_id}/signals")
def run_signals(run_id: int, st: str, date_from: Optional[date] = Query(None, alias='from'),
                date_to: Optional[date] = Query(None, alias='to'), db: Session = Depends(get_db),
                _admin: User = Depends(require_admin)):
    return _rows(db, """SELECT st, d, secid, pa, pb, move, thr_up, thr_dn, straight, side, tradable, skip
        FROM bt_signals WHERE run_id=:r AND st=:st AND d >= COALESCE(:a, DATE '1900-01-01')
          AND d <= COALESCE(:b, DATE '2999-01-01') ORDER BY d""", {'r': run_id, 'st': st, 'a': date_from, 'b': date_to})


@router.get("/runs/{run_id}/equity")
def run_equity(run_id: int, db: Session = Depends(get_db), _admin: User = Depends(require_admin)):
    return _rows(db, "SELECT d, equity, positions, notional, go_used, margin_call FROM bt_equity WHERE run_id=:r ORDER BY d",
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


BYBIT = 'https://api.bybit.com/v5/market/kline'
MSK = 3 * 3600


def _bybit_get(url):
    """Один запрос к Bybit. Лимит запросов у Bybit — на IP, а с этого сервера работают боты Kamaz: страницы идут с
    паузой, на «Too many visits» (10006 / HTTP 403/429) — до 4 повторов с растущей паузой, потом 503."""
    import time
    for k in range(5):
        time.sleep(0.25 if k == 0 else 1.5 * k)
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'frame-bt'}), timeout=20) as r:
                j = json.load(r)
        except urllib.error.HTTPError as ex:
            if ex.code in (403, 429): continue
            raise HTTPException(502, f'Bybit не ответил: {ex}')
        except (urllib.error.URLError, TimeoutError, ValueError) as ex:
            raise HTTPException(502, f'Bybit не ответил: {ex}')
        if j.get('retCode') == 10006: continue
        if j.get('retCode'): raise HTTPException(422, f"Bybit: {j.get('retMsg')}")
        return j
    raise HTTPException(503, 'Bybit просит реже: лимит запросов, попробуйте через минуту')


def _candles_crypto(st, tf, a, b):
    """Свечи бессрочного контракта Bybit (публичный API, без ключей). Bybit отдаёт до 1000 свечей за запрос, новые
    первыми — листаем назад. Внутри дня время сдвинуто в МСК «как будто UTC» (как свечи МосБиржи в Стенде);
    дневная свеча Bybit начинается в 00:00 UTC — ставим её на её дату."""
    iv = {5: '5', 15: '15', 60: '60', 1440: 'D'}[tf]
    start = (calendar.timegm(a.timetuple()) - MSK) * 1000                     # полночь МСК в мс UTC, без пояса процесса
    end = (calendar.timegm(b.timetuple()) + 86400 - MSK) * 1000 - 1
    rows, cur = {}, end
    for _ in range(80):
        url = f'{BYBIT}?category=linear&symbol={st}&interval={iv}&start={start}&end={cur}&limit=1000'
        j = _bybit_get(url)
        lst = j['result']['list']
        for x in lst: rows[int(x[0])] = x
        if len(lst) < 1000: break
        cur = int(lst[-1][0]) - 1
        if cur < start: break
    ks = sorted(rows)
    shift = 0 if tf == 1440 else MSK
    return {'st': st, 'tf': tf, 'from': a.isoformat(), 'to': b.isoformat(), 't': [k // 1000 + shift for k in ks],
            'o': [float(rows[k][1]) for k in ks], 'h': [float(rows[k][2]) for k in ks], 'l': [float(rows[k][3]) for k in ks],
            'c': [float(rows[k][4]) for k in ks], 'v': [float(rows[k][5]) for k in ks], 'secid': [[0, st]] if ks else [],
            'cur': 'USD'}


def _candles_stock_intraday(db, st, tf, a, b):
    """Акция: 5-минутные свечи TQBR (в БД — у большинства бумаг с 12.2025, у SBER и VTBR с 11.2021), свёрнутые в tf."""
    bucket = (f"date_trunc('day', begin_time) + floor((extract(hour from begin_time)*60 + "
              f"extract(minute from begin_time)) / {tf}) * interval '{tf} minutes'")
    rows = db.execute(text(f"""SELECT extract(epoch from {bucket})::bigint AS time,
               (array_agg(open ORDER BY begin_time))[1] AS open, max(high) AS high, min(low) AS low,
               (array_agg(close ORDER BY begin_time DESC))[1] AS close, sum(volume) AS volume
        FROM candles WHERE sec_id = :st AND secid = :st AND interval = 5 AND begin_time >= :a AND begin_time < :b   -- без type: только покрывающий индекс, см. store._sql_stock
        GROUP BY 1 ORDER BY 1"""), {'st': st, 'a': a, 'b': b + timedelta(days=1)}).fetchall()
    return {'st': st, 'tf': tf, 'from': a.isoformat(), 'to': b.isoformat(), 't': [r.time for r in rows],
            'o': [float(r.open) for r in rows], 'h': [float(r.high) for r in rows], 'l': [float(r.low) for r in rows],
            'c': [float(r.close) for r in rows], 'v': [float(r.volume or 0) for r in rows], 'secid': [[0, st]] if rows else []}


def _candles_stock(db, st, a, b):
    """Акция: дневные свечи TQBR как есть (с выходными сессиями) — бумаги стратегий на акциях (screener_vb)."""
    rows = db.execute(text("""SELECT extract(epoch from date_trunc('day', begin_time))::bigint AS time, open, high, low, close, volume
        FROM candles WHERE secid = :st AND interval = 24 AND type = 'stock' AND begin_time >= :a AND begin_time < :b
        ORDER BY begin_time"""), {'st': st, 'a': a, 'b': b + timedelta(days=1)}).fetchall()
    return {'st': st, 'tf': 1440, 'from': a.isoformat(), 'to': b.isoformat(), 't': [r.time for r in rows],
            'o': [float(r.open) for r in rows], 'h': [float(r.high) for r in rows], 'l': [float(r.low) for r in rows],
            'c': [float(r.close) for r in rows], 'v': [float(r.volume or 0) for r in rows], 'secid': [[0, st]] if rows else []}


@router.get("/candles")
def candles(st: str, tf: int = 5, date_from: Optional[date] = Query(None, alias='from'),
            date_to: Optional[date] = Query(None, alias='to'), db: Session = Depends(get_db),
            _admin: User = Depends(require_admin)):
    """Склеенный ряд: на каждый день — ближний неистёкший контракт с ≥ 60 свечами (как ряд V3 движка), будни.
    time — секунды «как будто МСК = UTC»: график показывает московское время без пересчёта поясов.
    Страница просит куски по календарным границам (месяц / квартал / год) — поэтому ответ кэшируется в Redis:
    прошлое — на сутки, кусок с сегодняшним днём — на 2 минуты."""
    if tf not in TF or (st not in bt_store.UNIVERSE_ALL and not STOCK_RE.match(st) and not CRYPTO_RE.fullmatch(st)):
        raise HTTPException(422, 'tf: 5 | 15 | 60 | 1440; st — тип фьючерса из /meta, тикер акции или монета Bybit (…USDT)')
    b = date_to or date.today()
    if CRYPTO_RE.fullmatch(st):                            # монета: Bybit, время — МСК, как у остальных графиков Стенда
        a = max(date_from or (b - timedelta(days=MAX_DAYS[1440] if tf == 1440 else 90)), b - timedelta(days=MAX_DAYS[tf]))
        ttl = 120 if b >= date.today() else 86400
        return cache.get_or_compute(f'bt:candles:crypto:v1:{st}:{tf}:{a}:{b}', lambda: _candles_crypto(st, tf, a, b), ttl)
    if st not in bt_store.UNIVERSE_ALL:                    # акция: дневные свечи или 5-минутные, свёрнутые в tf
        a = max(date_from or (b - timedelta(days=MAX_DAYS[1440] if tf == 1440 else 90)), b - timedelta(days=MAX_DAYS[tf]))
        ttl = 120 if b >= date.today() else 86400
        if tf != 1440:
            return cache.get_or_compute(f'bt:candles:stock:v2:{st}:{tf}:{a}:{b}', lambda: _candles_stock_intraday(db, st, tf, a, b), ttl)
        return cache.get_or_compute(f'bt:candles:stock:v1:{st}:{a}:{b}', lambda: _candles_stock(db, st, a, b), ttl)
    a = max(date_from or (b - timedelta(days=90 if tf == 5 else MAX_DAYS[tf])), b - timedelta(days=MAX_DAYS[tf]))
    ttl = 120 if b >= date.today() else 86400
    return cache.get_or_compute(f'bt:candles:v2:{st}:{tf}:{a}:{b}', lambda: _candles(db, st, tf, a, b), ttl)


def _realism(st):
    from backtest import account as acc, costs as c
    import pandas as pd
    rr = acc.risk_rates(); a = acc.ASSET.get(st); coef = acc.broker_coef(st)
    go = rr[a].dropna() if a in rr.columns else pd.Series(dtype=float)
    go = go[go.shift() != go]                                          # только даты изменения ставки
    rpp = acc.step_costs().get(st)
    sp = c.spread_daily().get(st) if hasattr(c, 'spread_daily') else None
    pts = lambda s, k=1.0: [[str(i.date()), round(float(v) * k, 6)] for i, v in s.items()]
    return {'st': st, 'asset': a, 'broker_coef': round(coef, 3), 'go_rate': pts(go, 100 * coef),
            'rub_per_point': pts(rpp.resample('W').last().dropna()) if rpp is not None else [],
            'rub_per_point_const': acc.rub_per_point(st), 'spread': pts(sp.resample('W').median().dropna(), 100) if sp is not None else [],
            'spread_const': c.spread_round(st, 'c3') * 100}


@router.get("/realism")
def realism(st: str, _admin: User = Depends(require_admin)):
    """Из чего складывается реализм по бумаге: ставка ГО во времени (биржа × надбавка брокера), рублёвая стоимость
    пункта (для контрактов в валюте — по дням, из данных биржи), спред стакана."""
    if st not in bt_store.UNIVERSE_ALL: raise HTTPException(422, 'st — тип фьючерса из /meta')
    return cache.get_or_compute(f'bt:realism:v1:{st}', lambda: _realism(st), 86400)


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


# ─── живые сделки роботов ────────────────────────────────────────────────────────────────────────
# Журналы роботов на демо-счетах (смонтированы в api только для чтения, см. docker-compose.yml): чтобы рисовать настоящие
# сделки поверх бэктеста. Нет каталога (локальная разработка, робот снят) — робот просто не попадает в ответ.
ROBOTS_DIR = os.environ.get('BT_ROBOTS_DIR', '/data/robots')
ROBOTS = [  # (имя, каталог, формат журнала, что торгует)
    ('Пятёрка', 'five', 'hybrid', 'фьючерсы BR, CC, VB, AK, MX · песочница T-Invest'),
    ('Валюты', 'fx', 'hybrid', 'фьючерсы юань и доллар · песочница T-Invest'),
    ('Скринер VB', 'vb', 'vb', 'акции TQBR · песочница T-Invest'),
    ('Kamaz soft3', 'kamaz_soft3', 'kamaz', 'крипта Bybit · 12 ч → безубыток +0.2 %, ещё 3 ч → по рынку · демо-счёт'),
    ('Kamaz A', 'kamaz_a', 'kamaz', 'крипта Bybit · жёсткий таймер 12 ч · демо-счёт'),
    ('Kamaz hold', 'kamaz_hold', 'kamaz', 'крипта Bybit · 12 ч → безубыток, ждать без предела · демо-счёт'),
]


def _num(v):
    try:
        x = float(v)
        return x if x == x else None
    except (TypeError, ValueError):
        return None


def _live_hybrid(path, robot):
    """hybrid_bot.py: строка = сделка (вход и выход). Индекс MX робот торгует мини-контрактом MM (1/100 цены MX) —
    цены приводим к шкале MX, иначе на графике MX сделка окажется в 100 раз ниже свечей."""
    out = []
    with open(path, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if r.get('status_in') != 'EXECUTION_REPORT_STATUS_FILL': continue
            try: out.append(_hybrid_row(r, robot))
            except (KeyError, TypeError, ValueError): continue   # недописанная строка — пропустить её, а не весь журнал
    return out


def _hybrid_row(r, robot):
    """px_in / px_out — цена исполнения; exec_in / exec_out — стоимость позиции в рублях (не цена!)."""
    k = 100.0 if r['st'] == 'MX' and (r.get('secid') or '').startswith('MM') else 1.0
    pin, pout = _num(r.get('px_in')), _num(r.get('px_out'))
    filled_out = r.get('status_out') == 'EXECUTION_REPORT_STATUS_FILL'
    return {'robot': robot, 'st': r['st'], 'secid': r['secid'], 'side': int(float(r['side'])), 'd': r['d_in'],
            't_in': r.get('t_in') or None, 'px_in': round(pin * k, 6) if pin else None, 'qty': _num(r.get('qty')),
            'd_out': r.get('d_out') or None, 't_out': r.get('t_out') or None,
            'px_out': round(pout * k, 6) if pout and filled_out else None,
            'pnl_rub': _num(r.get('pnl_rub')), 'move': _num(r.get('move')), 'note': r.get('note') or None}


def _live_vb(path, robot):
    """robots/vb_bot.py: строка = событие «вход» / «выход» по ноге (key); склеиваем в сделки.
    Итог в рублях — по ценам входа и выхода, БЕЗ комиссии (в журнале VB её нет)."""
    legs = {}
    with open(path, encoding='utf-8') as f:
        for r in csv.DictReader(f):
            try:
                _vb_event(legs, r, robot)
            except (KeyError, TypeError, ValueError):       # недописанная строка — пропустить её, а не весь журнал
                continue
    return list(legs.values())


def _vb_event(legs, r, robot):
    key, act = r.get('key'), r.get('action')
    if not key: return
    d, t = (r.get('ts') or '')[:10], (r.get('ts') or '')[11:19]           # ISO с +03:00 — это уже МСК
    if act == 'вход':
        side = -1 if 'шорт' in (r.get('leg') or '') else 1
        legs[key] = {'robot': robot, 'st': r['ticker'], 'secid': r['ticker'], 'side': side, 'd': d, 't_in': t,
                     'px_in': _num(r.get('price')), 'qty': _num(r.get('qty')), 'd_out': None, 't_out': None,
                     'px_out': None, 'pnl_rub': None, 'move': None, 'note': r.get('leg') or None}
    elif act == 'выход' and key in legs:
        x = legs[key]; x['d_out'], x['t_out'], x['px_out'] = d, t, _num(r.get('price'))
        if x['px_in'] and x['px_out'] and x['qty']:
            x['pnl_rub'] = round(x['side'] * (x['px_out'] - x['px_in']) * x['qty'], 2)


def _msk(t):
    """Время журнала бота Kamaz (UTC сервера, '2026-11-19 17:03:37.629000') → (дата, чч:мм:сс) по МСК."""
    x = datetime.strptime(t[:19], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=MSK)
    return x.strftime('%Y-%m-%d'), x.strftime('%H:%M:%S')


def _live_kamaz(dirpath, robot):
    """Боты Kamaz (/opt/kamaz-bot/bot/state*/journal/*.jsonl): события реального демо-счёта (src=реал) склеиваются в
    кампании: вход по рынку → ступени лесенки (докупки) → перенос цели / мягкая фаза → выход (цель или таймер).
    Итог — в долларах, как его записал бот (поле result события «кампания закрыта»)."""
    camps, out = {}, []
    for fn in sorted(glob.glob(os.path.join(dirpath, 'journal', '*.jsonl'))):
        with open(fn, encoding='utf-8', errors='replace') as f:
            for line in f:
                try:
                    e = json.loads(line)
                    if e.get('src') == 'реал': _kamaz_event(camps, out, e, robot)
                except (KeyError, TypeError, ValueError):     # недописанная строка — пропустить её, а не весь журнал
                    continue
    return out + list(camps.values())


def _kamaz_event(camps, out, e, robot):
    sym, kind, what = e['sym'], e.get('kind'), e.get('what') or ''
    d, t = _msk(e['t'])
    c = camps.get(sym)
    if kind == 'исполнение' and what == 'вход' and c is not None and not c['_out']:
        c['fills'].append({'d': d, 't': t, 'px': float(e['price']), 'qty': float(e['qty']), 'what': 'вход'})   # вход исполнился кусками
        c['qty'] += float(e['qty'])
    elif kind == 'исполнение' and (what == 'вход' or (c is None and what.startswith('ступень'))):
        # ступень без входа — кампания, подхваченная ботом после перезапуска: начинаем её с этой заявки
        camps[sym] = {'robot': robot, 'st': sym, 'secid': sym, 'side': 1, 'd': d, 't_in': t, 'px_in': float(e['price']),
                      'qty': float(e['qty']), 'd_out': None, 't_out': None, 'px_out': None, 'pnl_rub': None, 'move': None,
                      'note': None if what == 'вход' else 'подхвачена после перезапуска бота', 'cur': 'USD',
                      'fills': [{'d': d, 't': t, 'px': float(e['price']), 'qty': float(e['qty']), 'what': what}],
                      'targets': [], 'soft': None, 'avg': float(e['price']), '_out': []}
    elif c is None:
        return
    elif kind == 'исполнение' and what.startswith('ступень'):
        c['fills'].append({'d': d, 't': t, 'px': float(e['price']), 'qty': float(e['qty']), 'what': what})
        c['qty'] += float(e['qty'])
    elif kind in ('цель', 'цель перенесена'):
        c['targets'].append({'d': d, 't': t, 'px': float(e['price'])})
    elif kind == 'мягкая фаза':
        c['soft'] = {'d': d, 't': t, 'be': float(e['be'])}
    elif kind == 'исполнение':                                 # выход: цель или таймер (бывает несколькими кусками)
        c['_out'].append((float(e['price']), float(e['qty'])))
        c['fills'].append({'d': d, 't': t, 'px': float(e['price']), 'qty': -float(e['qty']), 'what': what})
    elif kind == 'кампания закрыта':
        q = sum(x[1] for x in c['_out'])
        c['d_out'], c['t_out'] = d, t
        c['px_out'] = sum(p * x for p, x in c['_out']) / q if q else (c['targets'][-1]['px'] if c['targets'] else None)
        c['pnl_rub'] = round(float(e['result']), 2) if e.get('result') is not None else None
        n = int(e.get('adds') or 0)
        how = {'цель': 'по цели', 'таймер': 'по таймеру', 'стоп': 'по стопу', 'нет позиции': 'позиция закрыта вне бота'}.get(e.get('how'), e.get('how') or '')
        c['note'] = f"{how} · докупок {n}" + (' · после мягкой фазы' if e.get('мягко') else '') + (f" · {c['note']}" if c.get('note') else '')
        out.append(c); del camps[sym]
    for x in (c,) if c is not None else ():
        if x.get('fills'):
            buys = [f for f in x['fills'] if f['qty'] > 0]
            qb = sum(f['qty'] for f in buys)
            x['avg'] = sum(f['px'] * f['qty'] for f in buys) / qb if qb else None


def _clean(x):
    return {k: v for k, v in x.items() if not k.startswith('_')}


@router.get("/live/robots")
def live_robots(_admin: User = Depends(require_admin)):
    return [{'robot': n, 'about': a, 'on': os.path.isdir(os.path.join(ROBOTS_DIR, d))} for n, d, _, a in ROBOTS]


@router.get("/live/trades")
def live_trades(_admin: User = Depends(require_admin)):
    out = []
    for name, d, fmt, _ in ROBOTS:
        if fmt == 'kamaz':
            if os.path.isdir(os.path.join(ROBOTS_DIR, d, 'journal')):
                try: out += [_clean(x) for x in _live_kamaz(os.path.join(ROBOTS_DIR, d), name)]
                except Exception: pass
            continue
        path = os.path.join(ROBOTS_DIR, d, 'journal.csv')
        if not os.path.exists(path): continue
        try:
            out += (_live_hybrid if fmt == 'hybrid' else _live_vb)(path, name)
        except Exception:                                   # битый журнал одного робота не должен ронять остальных
            continue
    return out
