"""Хранилище 5-минутных свечей Стенда: <BT_DATA>/bars/<тип>.pkl. Источник — таблицы candles × futures_contracts.

Свеча помечена временем НАЧАЛА (МСК). «Цена в 17:00» во всех наших расчётах = закрытие свечи, начавшейся в 17:00.
Доступ к БД: DB_URL (SQLAlchemy, в контейнере) → иначе psql через docker/ssh (ноутбук разработчика).
Формат pickle, а не parquet: в образе сайта нет pyarrow, тянуть его ради кэша незачем.
"""
import io, json, os, pathlib, subprocess
import pandas as pd

PKG = pathlib.Path(__file__).resolve().parent
REF = PKG / 'data'                                   # справочники в репозитории: спред, параметры контрактов, ставки риска
DATA = pathlib.Path(os.environ.get('BT_DATA', PKG.parent / 'bt_data'))
BARS = DATA / 'bars'
UNIVERSE_ALL = ['AF', 'AK', 'BR', 'CC', 'CR', 'Eu', 'GK', 'GZ', 'LK', 'MN', 'MX', 'NM', 'PI', 'PT', 'RI', 'SN', 'SR',
                'SS', 'SZ', 'Si', 'TT', 'VB']
SSH = ["ssh", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "ConnectTimeout=30",
       "-i", str(pathlib.Path.home() / ".ssh/id_ed25519"), "root@103.88.243.232"]
COLS = ['secid', 'lsttrade', 't', 'open', 'close']
FORMAT = 2               # 2: только open/close, secid категорией — движку high/low/volume не нужны (графику свечи отдаёт API)
BARS_LRU = 2             # сколько типов держать в памяти целиком; производные ряды (цены в точках) — маленькие, живут все
_CACHE = {}
_BARS = {}


def _sql(st, since):
    return f"""SELECT c.secid, f.lsttrade, c.begin_time, c.open, c.close
      FROM candles c JOIN futures_contracts f ON c.secid=f.secid
      WHERE c.interval=5 AND c.type='futures' AND f.sectype='{st}' AND c.begin_time>='{since}'
      ORDER BY c.secid, c.begin_time"""


def _fetch(st, since):
    if os.environ.get('DB_URL'):
        from sqlalchemy import create_engine
        if 'engine' not in _CACHE:
            url = os.environ['DB_URL']
            _CACHE['engine'] = create_engine(url, connect_args={'ssl_context': False} if 'pg8000' in url else {})
        # COPY потоком: pg8000 собирает строки на чистом питоне, 5 млн свечей через fetchall — 8 минут, через COPY — ~1
        raw = _CACHE['engine'].raw_connection()
        try:
            buf = io.BytesIO(); cur = raw.cursor()
            cur.execute('COPY (' + ' '.join(_sql(st, since).split()) + ') TO STDOUT WITH CSV', stream=buf)
            raw.commit()
        finally:
            raw.close()
        buf.seek(0)
        return pd.read_csv(buf, header=None, names=COLS)
    inner = f"docker exec frame-db-1 psql -U postgres -d moex_db -c {json.dumps('COPY (' + ' '.join(_sql(st, since).split()) + ') TO STDOUT WITH CSV')}"
    cmd = ["bash", "-c", inner] if pathlib.Path('/opt/frame').exists() else SSH + [inner]
    r = subprocess.run(cmd, capture_output=True, timeout=1800, stdin=subprocess.DEVNULL)
    if r.returncode:
        raise RuntimeError(r.stderr.decode()[:400])
    return pd.read_csv(io.BytesIO(r.stdout), header=None, names=COLS)


def _typed(df):
    df['t'] = pd.to_datetime(df.t); df['lsttrade'] = pd.to_datetime(df.lsttrade)
    for c in ('open', 'close'):
        df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)
    df['d'] = df.t.dt.normalize()
    df['m'] = (df.t.dt.hour * 60 + df.t.dt.minute).astype('int16')
    return df


def refresh(sectypes=None, full=False):
    """Докачать свечи. Инкрементально: последние 5 дней хранилища перечитываются (свечи текущего дня дописываются)."""
    BARS.mkdir(parents=True, exist_ok=True)
    out = status()
    if out.get('_format') != FORMAT: full, out = True, {}
    for st in (sectypes or UNIVERSE_ALL):
        f = BARS / f'{st}.pkl'
        old = pd.read_pickle(f) if f.exists() and not full else None
        since = (old.d.max() - pd.Timedelta(days=5)).date().isoformat() if old is not None and len(old) else '2019-01-01'
        new = _typed(_fetch(st, since))
        if old is not None:
            old['secid'] = old.secid.astype(str)
            new = pd.concat([old[old.t < pd.Timestamp(since)], new], ignore_index=True)
        if not len(new): continue
        new['secid'] = new.secid.astype('category')
        new.to_pickle(f)
        out[st] = [int(len(new)), str(new.d.min().date()), str(new.t.max())]
    out['_format'] = FORMAT
    _CACHE_clear()
    (DATA / 'bars_status.json').write_text(json.dumps(out, ensure_ascii=False, indent=1))
    return out


def _CACHE_clear():
    eng = _CACHE.get('engine'); _CACHE.clear(); _BARS.clear()
    if eng is not None: _CACHE['engine'] = eng


def bars(st):
    """Все свечи типа. В памяти — не больше BARS_LRU типов: движок идёт по бумагам по очереди, а 22 типа разом = 1 ГБ."""
    if st in _BARS:
        _BARS[st] = _BARS.pop(st)
    else:
        df = pd.read_pickle(BARS / f'{st}.pkl'); df['secid'] = df.secid.astype(str)
        _BARS[st] = df
        while len(_BARS) > BARS_LRU: _BARS.pop(next(iter(_BARS)))
    return _BARS[st]


def status():
    f = DATA / 'bars_status.json'
    return json.loads(f.read_text()) if f.exists() else {}


def data_until(universe=None):
    v = [x[2] for k, x in status().items() if not k.startswith('_') and (universe is None or k in universe)]
    return max(v, default=None)


def hm(s):
    """'17:05' → 1025 минут от полуночи."""
    return int(s[:2]) * 60 + int(s[3:5])


def day_counts(st):
    """Число свечей за день по контракту: Series[(secid, d)] (фильтр «живой контракт ≥ 60 баров»)."""
    k = ('cnt', st)
    if k not in _CACHE:
        _CACHE[k] = bars(st).groupby(['secid', 'd']).size()
    return _CACHE[k]


def price(st, kind, minute):
    """Series[(secid, d)] цены.
    close@T      — закрытие свечи, начавшейся ровно в T (так считаны все замороженные спецификации);
    open@T       — открытие свечи, начавшейся ровно в T;
    first_open@T — открытие первой свечи дня с началом ≥ T (рыночная заявка «как только пойдут сделки»)."""
    k = (kind, st, minute)
    if k not in _CACHE:
        B = bars(st)
        if kind == 'close':
            s = B[B.m == minute].set_index(['secid', 'd']).close
        elif kind == 'open':
            s = B[B.m == minute].set_index(['secid', 'd']).open
        elif kind == 'first_open':
            s = B[B.m >= minute].sort_values('m').groupby(['secid', 'd']).open.first()
        else:
            raise ValueError(kind)
        _CACHE[k] = s[~s.index.duplicated()]
    return _CACHE[k]


def contracts(st):
    k = ('contracts', st)
    if k not in _CACHE:
        _CACHE[k] = bars(st).groupby('secid').lsttrade.first()
    return _CACHE[k]
