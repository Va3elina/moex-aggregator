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
                'SS', 'SZ', 'Si', 'TT', 'VB',
                'CNYRUBF', 'USDRUBF', 'EURRUBF', 'GLDRUBF', 'IMOEXF', 'SBERF', 'GAZPF']   # вечные: один контракт, lsttrade 2100-01-01
SSH = ["ssh", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "ConnectTimeout=30",
       "-i", str(pathlib.Path.home() / ".ssh/id_ed25519"), "root@103.88.243.232"]
COLS = ['secid', 'lsttrade', 't', 'open', 'high', 'low', 'close', 'volume']
FORMAT = 3               # 3: open/close float64 (по ним сверяются замороженные спецификации), high/low/volume float32
                         #    (нужны стратегиям на Python), secid категорией
BARS_LRU = 2             # сколько типов держать в памяти целиком; производные ряды (цены в точках) — маленькие, живут все
_CACHE = {}
_BARS = {}


def _sql(st, since):
    return f"""SELECT c.secid, f.lsttrade, c.begin_time, c.open, c.high, c.low, c.close, c.volume
      FROM candles c JOIN futures_contracts f ON c.secid=f.secid
      WHERE c.interval=5 AND c.type='futures' AND f.sectype='{st}' AND c.begin_time>='{since}'
      ORDER BY c.secid, c.begin_time"""


def is_stock(st):
    """Акция TQBR (тикер), а не тип фьючерса: у акции один «контракт» без экспирации. Цены сырые (дивидендные гэпы
    и сплиты не поправлены), объём — в штуках без округления до лота: для стратегий «на ночь» на 1 млн это мелочи."""
    import re
    return isinstance(st, str) and st not in UNIVERSE_ALL and not st.endswith('USDT') and bool(re.fullmatch(r'[A-Z][A-Z0-9]{0,11}', st))


def _sql_stock(st, since):
    # без type='stock': иначе планировщик берёт индекс дневок и читает таблицу (8 с на месяц); так — только покрывающий
    # индекс 5/60-минуток (sec_id, interval, begin_time), 20–30 мс. sec_id = secid = тикер бывает только у акции.
    return f"""SELECT secid, '2100-01-01', begin_time, open, high, low, close, volume FROM candles
      WHERE sec_id='{st}' AND secid='{st}' AND interval=5 AND begin_time>='{since}' ORDER BY begin_time"""


STOCK_TTL = 3600         # свечи акции перечитываются из БД не чаще раза в час (вечером — свежий день для сигнала)


def _stock_file(st):
    """5-минутные свечи акции: лениво, при первом обращении (и раз в час), целиком — это ~60–230 тыс. строк."""
    import time
    f = BARS / f'stock_{st}.pkl'
    if not f.exists() or time.time() - f.stat().st_mtime > STOCK_TTL:
        BARS.mkdir(parents=True, exist_ok=True)
        try:
            df = _typed(_fetch(st, '2019-01-01', sql=_sql_stock))
        except Exception:
            if f.exists(): return f                     # БД на минуту недоступна — считаем по свечам, что уже на диске
            raise
        if not len(df): raise ValueError(f'у {st} нет 5-минутных свечей в базе')
        df['secid'] = df.secid.astype('category'); df.to_pickle(f)
        _BARS.pop(st, None); [_CACHE.pop(k) for k in [k for k in _CACHE if isinstance(k, tuple) and st in k]]
        stt = status(); stt[st] = [int(len(df)), str(df.d.min().date()), str(df.t.max())]
        (DATA / 'bars_status.json').write_text(json.dumps(stt, ensure_ascii=False, indent=1))
    return f


def _fetch(st, since, sql=None, names=COLS):
    sql = sql or _sql
    if os.environ.get('DB_URL'):
        from sqlalchemy import create_engine
        if 'engine' not in _CACHE:
            url = os.environ['DB_URL']
            _CACHE['engine'] = create_engine(url, connect_args={'ssl_context': False} if 'pg8000' in url else {})
        # COPY потоком: pg8000 собирает строки на чистом питоне, 5 млн свечей через fetchall — 8 минут, через COPY — ~1
        raw = _CACHE['engine'].raw_connection()
        try:
            buf = io.BytesIO(); cur = raw.cursor()
            cur.execute('COPY (' + ' '.join(sql(st, since).split()) + ') TO STDOUT WITH CSV', stream=buf)
            raw.commit()
        finally:
            raw.close()
        buf.seek(0)
        return pd.read_csv(buf, header=None, names=names)
    inner = f"docker exec frame-db-1 psql -U postgres -d moex_db -c {json.dumps('COPY (' + ' '.join(sql(st, since).split()) + ') TO STDOUT WITH CSV')}"
    cmd = ["bash", "-c", inner] if pathlib.Path('/opt/frame').exists() else SSH + [inner]
    r = subprocess.run(cmd, capture_output=True, timeout=1800, stdin=subprocess.DEVNULL)
    if r.returncode:
        raise RuntimeError(r.stderr.decode()[:400])
    return pd.read_csv(io.BytesIO(r.stdout), header=None, names=names)


def _typed(df):
    df['t'] = pd.to_datetime(df.t); df['lsttrade'] = pd.to_datetime(df.lsttrade)
    for c in ('open', 'close'):
        df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)
    for c in ('high', 'low', 'volume'):
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('float32')
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
    """Все свечи типа. В памяти — не больше BARS_LRU типов: движок идёт по бумагам по очереди, а 22 типа разом = 1 ГБ.
    Акция — свечи из БД, перечитываются раз в час."""
    if is_stock(st): _stock_file(st)
    if st in _BARS:
        _BARS[st] = _BARS.pop(st)
    else:
        df = pd.read_pickle(_stock_file(st) if is_stock(st) else BARS / f'{st}.pkl'); df['secid'] = df.secid.astype(str)
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


# ── Дневные ряды для стратегий (b.ext): «Сила рынка», ключевая ставка, индексы ─────────────────────────────────────
SERIES = DATA / 'series.pkl'
SERIES_TTL = 3600        # перечитываются воркером не чаще раза в час; песочница только читает файл (сети и БД у неё нет)


def _sql_series(_st, _since):
    return """SELECT 'breadth' || ema_period || CASE WHEN universe = 'imoex' THEN '' ELSE '_all' END, trade_date, percent_above
        FROM breadth_history WHERE universe IN ('imoex', 'all')
      UNION ALL SELECT 'key_rate', period_date, value FROM macro_data WHERE indicator = 'KEY_RATE'
      UNION ALL SELECT secid, trade_date, close FROM index_data WHERE secid IN ('IMOEX', 'MCFTR') AND trade_date >= '2007-01-01'"""


def refresh_series(force=False):
    """Дневные ряды из БД → series.pkl: breadth20/50/100/200 — доля акций IMOEX выше EMA (%, «Сила рынка», значение дня
    готово ВЕЧЕРОМ после закрытия), breadth*_all — то же по всем акциям, key_rate — ключевая ставка ЦБ, IMOEX и MCFTR —
    закрытия индексов. Только у воркера (у него есть DB_URL)."""
    import time
    if SERIES.exists() and not force and time.time() - SERIES.stat().st_mtime < SERIES_TTL:
        return False
    df = _fetch(None, None, sql=_sql_series, names=['name', 'd', 'v'])
    df['d'] = pd.to_datetime(df.d); df['v'] = pd.to_numeric(df.v, errors='coerce').astype(float)
    out = {k: g.set_index('d').v.sort_index() for k, g in df.dropna().groupby('name')}
    DATA.mkdir(parents=True, exist_ok=True)
    tmp = SERIES.with_suffix('.tmp'); pd.to_pickle(out, tmp); tmp.replace(SERIES)
    _CACHE.pop('series', None)
    return True


def series():
    """{имя: Series[дата → значение]} — см. refresh_series. Нет файла — пустой словарь."""
    if 'series' not in _CACHE:
        _CACHE['series'] = pd.read_pickle(SERIES) if SERIES.exists() else {}
    return _CACHE['series']
