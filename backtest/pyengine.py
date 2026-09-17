"""Стратегии кодом на Python — свеча за свечой, как Pine в TradingView.

Код стратегии — обычный модуль с тремя именами:
    PARAMS = {"fast": 20, "slow": 100}          # параметры: форма в «Свойствах» (и перебор)
    def init(b, p): ...                          # один раз на бумагу: посчитать индикаторы на всю историю
    def on_bar(i, b, pos, p): ...                # на каждой свече: вернуть желаемую позицию +1 / −1 / 0 или None

b — свечи одной бумаги (склеенный ряд ближних контрактов, 5 минут, МСК, будни), numpy-массивы:
    b.open b.high b.low b.close b.volume · b.minute (минуты от полуночи) · b.date (день) · b.t (время начала свечи)
    b.new_day (первая свеча дня) · b.day_open (открытие дня) · b.prev_close (закрытие прошлого дня)
    b.sma(x, n) b.ema(x, n) b.rsi(x, n) b.atr(n) b.highest(x, n) b.lowest(x, n) — готовые индикаторы (без заглядывания вперёд)
Заявка исполняется по ОТКРЫТИЮ СЛЕДУЮЩЕЙ свечи. Позиция по бумаге одна. Перед сменой контракта позиция закрывается
по закрытию последней свечи (склейка цен через ролл не торгуется). Объём считает счёт (слоты, ГО) — как у правил.

⚠️ Модуль рассчитан на запуск ТОЛЬКО в изолированном контейнере bt-sandbox (без сети и секретов) или локально
разработчиком: код исполняется как есть, без ограничений языка.
"""
import traceback
import numpy as np, pandas as pd
from . import store

MAX_TRADES = 200_000
_STD = {'open', 'high', 'low', 'close', 'volume', 'minute', 't', 'date', 'secid', 'new_day', 'last_of_contract', 'day_open', 'prev_close'}


class Bars:
    def __init__(self, df):
        self.open = df.open.values.astype(float); self.high = df.high.values.astype(float); self.low = df.low.values.astype(float)
        self.close = df.close.values.astype(float); self.volume = df.volume.values.astype(float)
        self.minute = df.m.values.astype(int); self.t = df.t.values.astype('datetime64[m]'); self.date = df.d.values.astype('datetime64[D]')
        self.secid = df.secid.values
        n = len(df); d = self.date
        self.new_day = np.r_[True, d[1:] != d[:-1]] if n else np.zeros(0, bool)
        self.last_of_contract = np.r_[self.secid[1:] != self.secid[:-1], True] if n else np.zeros(0, bool)
        first = np.where(self.new_day)[0]
        self.day_open = np.repeat(self.open[first], np.diff(np.r_[first, n])) if n else self.open
        last_close = np.r_[np.nan, self.close[first[1:] - 1]] if n else self.close
        self.prev_close = np.repeat(last_close, np.diff(np.r_[first, n])) if n else self.close

    def __len__(self): return len(self.close)

    @staticmethod
    def sma(x, n): return pd.Series(x).rolling(int(n)).mean().values

    @staticmethod
    def ema(x, n): return pd.Series(x).ewm(span=int(n), adjust=False, min_periods=int(n)).mean().values

    @staticmethod
    def highest(x, n): return pd.Series(x).rolling(int(n)).max().values

    @staticmethod
    def lowest(x, n): return pd.Series(x).rolling(int(n)).min().values

    @staticmethod
    def rsi(x, n):
        d = pd.Series(x).diff(); up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
        dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
        return (100 - 100 / (1 + up / dn)).values

    def atr(self, n):
        pc = np.r_[np.nan, self.close[:-1]]
        tr = np.nanmax(np.c_[self.high - self.low, np.abs(self.high - pc), np.abs(self.low - pc)], axis=1)
        return pd.Series(tr).ewm(alpha=1 / n, adjust=False, min_periods=n).mean().values

    def head(self, k):
        """Копия первых k свечей — для проверки на заглядывание вперёд."""
        b = object.__new__(Bars)
        for a, v in self.__dict__.items():
            if a in _STD: setattr(b, a, v[:k].copy())                       # только штатные поля: индикаторы пересчитает init
        b.last_of_contract = b.last_of_contract.copy(); b.last_of_contract[-1] = True
        return b


def chain_bars(st, since=None, until=None):
    """Склеенный ряд: на каждый будний день — ближний неистёкший контракт с ≥ 60 свечами, без возврата к более раннему."""
    B = store.bars(st); B = B[B.d.dt.dayofweek < 5]
    cnt = B.groupby(['secid', 'd']).size().rename('n').reset_index()
    cnt['lsttrade'] = cnt.secid.map(store.contracts(st))
    cnt = cnt[(cnt.n >= 60) & (cnt.lsttrade >= cnt.d)].sort_values(['d', 'lsttrade']).drop_duplicates('d')
    keep, cur = [], pd.Timestamp('1900-01-01')
    for lt in cnt.lsttrade:
        if lt >= cur: keep.append(True); cur = lt
        else: keep.append(False)
    cnt = cnt[keep]
    if since: cnt = cnt[cnt.d >= since]
    if until: cnt = cnt[cnt.d <= until]
    return B.merge(cnt[['secid', 'd']], on=['secid', 'd']).sort_values('t').reset_index(drop=True)


def load(code):
    ns = {'__name__': 'strategy', 'np': np, 'pd': pd}
    exec(compile(code, 'strategy.py', 'exec'), ns)
    if not callable(ns.get('on_bar')): raise ValueError('в коде нет функции on_bar(i, b, pos, p)')
    return ns


def run_one(ns, b, params, st):
    if callable(ns.get('init')): ns['init'](b, params)
    on_bar, n = ns['on_bar'], len(b)
    pos, ent, out = 0, None, []
    o, c, last = b.open, b.close, b.last_of_contract
    for i in range(n):
        end = bool(last[i])
        tgt = 0 if end else on_bar(i, b, pos, params)
        if tgt is None: continue
        tgt = int(np.sign(tgt))
        if tgt == pos: continue
        j, px = (i, c[i]) if end else (i + 1, o[i + 1])
        if pos != 0:
            out.append((st, ent[0], pos, ent[1], j, px, 'смена контракта' if end and i < n - 1 else 'конец данных' if end else 'сигнал'))
        ent = (j, px) if tgt != 0 else None
        pos = tgt
        if len(out) > MAX_TRADES: raise ValueError(f'больше {MAX_TRADES} сделок на бумагу — стратегия торгует на каждой свече?')
    rows, hi, lo = [], b.high, b.low
    for s, a, sd, pi, z, po, why in out:
        zz = max(z, a + 1); up, dn = float(np.nanmax(hi[a:zz])) / pi - 1, float(np.nanmin(lo[a:zz])) / pi - 1
        rows.append({'st': s, 'secid': str(b.secid[a]), 'side': sd, 'd': str(b.date[a]), 'm_in': int(b.minute[a]), 'px_in': float(pi),
                     'd_out': str(b.date[z]), 'm_out': int(b.minute[z]), 'px_out': float(po), 'exit_reason': why,
                     'mfe': max(0.0, up if sd > 0 else -dn), 'mae': min(0.0, dn if sd > 0 else -up)})
    return rows


class _Guard(np.ndarray):
    """Массив, который замечает обращение к свечам правее текущей (в т.ч. b.close[-1] — это конец истории)."""
    now = 0; hits = 0

    def __getitem__(self, key):
        n = self.shape[0] if self.ndim else 0
        if isinstance(key, (int, np.integer)):
            if (key if key >= 0 else n + key) > _Guard.now: _Guard.hits += 1
        elif isinstance(key, slice):
            stop = n if key.stop is None else (key.stop if key.stop >= 0 else n + key.stop)
            if stop > _Guard.now + 1 and (key.start is None or key.start >= 0 or key.stop is None): _Guard.hits += 1
        return np.asarray(self).__getitem__(key) if not isinstance(key, (int, np.integer)) else super().__getitem__(key)


def lookahead_check(code, b, params, st):
    """Две проверки на заглядывание вперёд (на первой бумаге прогона).
    1. Индикаторы из init(), посчитанные на обрезанной истории, обязаны совпасть с посчитанными на полной —
       иначе расчёт использует будущие данные (сдвиг назад, нормировка по всей истории, центрированное окно).
    2. on_bar() на первых 20 тыс. свечей выполняется с «охраняемыми» массивами: обращение к свече правее i — нарушение."""
    out = {'бумага': st, 'индикаторы_из_будущего': [], 'обращений_к_будущим_свечам': 0}
    n = len(b)
    if n < 3000: return None
    std = _STD
    ns = load(code); base = Bars.__new__(Bars); base.__dict__.update({k: v for k, v in b.__dict__.items() if k in std})
    if callable(ns.get('init')): ns['init'](base, params)
    user = {k: v for k, v in base.__dict__.items() if k not in std and isinstance(v, np.ndarray) and v.shape[:1] == (n,)}
    for frac in (0.35, 0.6, 0.85):
        k = int(n * frac); part = b.head(k)
        if callable(ns.get('init')): load(code)['init'](part, params)
        for name, full in user.items():
            pv = part.__dict__.get(name)
            if pv is None or name in out['индикаторы_из_будущего']: continue
            a, c = np.asarray(full[:k - 1], float), np.asarray(pv[:k - 1], float)
            if not np.allclose(a, c, rtol=1e-9, atol=1e-12, equal_nan=True): out['индикаторы_из_будущего'].append(name)
    m = min(n, 20000); g = b.head(m)
    ns2 = load(code)
    if callable(ns2.get('init')): ns2['init'](g, params)
    for name, v in list(g.__dict__.items()):
        if isinstance(v, np.ndarray) and v.shape[:1] == (m,) and v.dtype.kind in 'fiub': setattr(g, name, v.view(_Guard))
    _Guard.hits = 0; pos = 0
    for i in range(m - 1):
        _Guard.now = i
        t = ns2['on_bar'](i, g, pos, params)
        if t is not None: pos = int(np.sign(t))
    out['обращений_к_будущим_свечам'] = int(_Guard.hits); _Guard.now = 10 ** 12
    return out


def run_code(code, params=None, universe=None, since=None, until=None, progress=None):
    """→ {'trades': [...], 'params': {...}, 'lookahead': [...]} либо {'error': текст}."""
    try:
        ns = load(code)
        p = {**(ns.get('PARAMS') or {}), **(params or {})}
        uni = universe or store.UNIVERSE_ALL; trades, look = [], []
        for k, st in enumerate(uni):
            b = Bars(chain_bars(st, since, until))
            if len(b) < 500: continue
            rows = run_one(ns, b, p, st); trades += rows
            if not look:
                lc = lookahead_check(code, b, p, st)
                if lc: look.append(lc)
            if progress: progress(k + 1, len(uni))
        return {'trades': trades, 'params': p, 'lookahead': look}
    except Exception:
        tb = traceback.format_exc().splitlines()
        keep = [l for l in tb if 'strategy.py' in l or not l.startswith('  ')]
        return {'error': '\n'.join(keep[-8:])[:2000]}


TEMPLATE = '''# Стратегия на Python: выполняется по каждой бумаге отдельно, свеча за свечой (5 минут, время московское).
# Заявка исполняется по открытию СЛЕДУЮЩЕЙ свечи. Позиция по бумаге одна: +1 лонг, -1 шорт, 0 нет.

PARAMS = {"fast": 30, "slow": 120, "flat_after": "23:30"}     # появятся в «Свойствах»


def init(b, p):
    """Один раз на бумагу: индикаторы на всю историю. b.open b.high b.low b.close b.volume b.minute b.date"""
    b.fast = b.ema(b.close, p["fast"])
    b.slow = b.ema(b.close, p["slow"])
    h, m = p["flat_after"].split(":")
    b.flat = int(h) * 60 + int(m)


def on_bar(i, b, pos, p):
    """Вернуть желаемую позицию: +1, -1, 0 или None (ничего не менять). Смотреть можно только на свечи 0..i."""
    if b.minute[i] >= b.flat:            # на ночь не остаёмся
        return 0
    if i == 0 or np.isnan(b.slow[i - 1]):
        return None
    if b.fast[i] > b.slow[i] and b.fast[i - 1] <= b.slow[i - 1]:
        return +1
    if b.fast[i] < b.slow[i] and b.fast[i - 1] >= b.slow[i - 1]:
        return -1
    return None
'''
