"""Движок «MOEX Screener ЛОНГ + ШОРТ» (agaev1003-screener-longshort) — построчный перенос JS скринера на Python.

Что перенесено 1:1 (функции JS в скобках): фильтры сигнала (scanTickerFull), индикаторы по будням (avgVol, maVal,
atrPct, avgValueRub), дивгэп (isDivGap), гейт отсечки (divTooClose), skip-forward по скальпу, выходы
(simulateSignalResult), тренд-ноги с гейтом IMOEX (trendGateOk), плата за заём по RUSFAR дня (borrowPct),
приведение даты реестра к ex-дате (recordToExDate), портфель «1 млн на идею, тренд-кластер дня делится»
(drawSignalsEquity). Не перенесено: параллельная версия v2 (с 25.09.2026, вечерний вход 19:05 и выход по слабому
объёму) — основной портфель скринера считается без неё.

Данные — те же, что у скринера (дневные свечи ISS TQBR с выходными сессиями): таблица candles (type='stock') +
<BT_DATA>/stocks_extra.csv.gz — бумаги листинга TQBR, которых нет в БД, включая снятые (разовая выгрузка:
`python -m backtest.cli data stocks-iss`); дивиденды — таблица dividends (ISS-роут дивидендов снят); IMOEX — index_data;
сплиты и RUSFAR O/N — backtest/data/splits.csv.gz, rusfar.csv.gz (разовая выгрузка ISS; свежий RUSFAR — той же командой в <BT_DATA>).
Сверка: таблица скринера за 2026 г. — 171/171 сделка, лонг до рубля (research/screener_vb, 20.10.2026).
Прогон в Стенде: spec {"strategy": "screener_vb", "capital": 10e6, "slots": 10, "since": "2012-01-01"} — идея = капитал ÷ slots,
в позициях не больше капитала (тренд-сделки одного дня и стороны делят одну идею); лишняя идея пропускается.
"""
from __future__ import annotations

import bisect
import csv
import gzip
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import io
import json
import os
import time
import urllib.request

import numpy as np
import pandas as pd

DATA = Path(__file__).parent / 'data'                 # справочники в репозитории: сплиты, RUSFAR

# ── Параметры скринера (P, QUALITY, EXIT, TREND) ──
P = dict(volMin=2.94, sigDays=1, atrMin=1.53, atrMax=5.09, atrWin=20, maPeriod=50, cpMin=0.55, maxDayRet=10,
         vrConfirm=1.63, gapCancel=6, divGapVr=1.63, volWin=60, stopPct=17, divGap=3, divGateDays=5,
         commission=0.0005, slippage=0.001)
QUALITY = {'long': dict(maxVr=5.30, minLiq=22.5e6), 'short': dict(maxVr=999, minLiq=75e6)}
EXIT = {'long': dict(tsAfter=5, tsDist=1.4, maxHold=10), 'short': dict(tsAfter=3, tsDist=5.0, maxHold=20)}
TREND = dict(donchN=20, warmup=3, maxHold=60, decayAfter=30, decayN=10, lockArm=20, lockTrail=8, stopShort=17,
             stopLong=10, flipLong=True, longMomDays=63, longMomPct=10)
SHORT_MARGIN_SPREAD = 3
T1_SETTLE_FROM = '2023-07-31'
TRADE_SIZE_RUB = 1_000_000
# Старые тикеры, чья история целиком лежит под новым (склеенная ISS): TCSG→T (÷10), ISKJ→ABIO, MTSI→MTSS, NOTK→NVTK.
# Без исключения одна и та же сделка считалась бы дважды (T и TCSG шорт 14.01.2022).
DUPES = {'TCSG', 'ISKJ', 'MTSI', 'NOTK'}
# Опыт 20.10.2026 (не правило скринера): трейлинг пропорционален ATR% дня сигнала — {'long': k, 'short': k}; None = как скринер
ATR_TRAIL = None


def wk(d: str) -> bool:
    return date.fromisoformat(d).weekday() >= 5


def cal_days(d1: str, d2: str) -> int:
    return (date.fromisoformat(d2) - date.fromisoformat(d1)).days


# ── Бумага: свечи + индикаторы по будням (точная семантика JS) ──
class Ticker:
    def __init__(self, secid, rows):
        self.secid = secid
        self.d = [r[0] for r in rows]
        self.o = np.array([r[1] for r in rows], float)
        self.h = np.array([r[2] for r in rows], float)
        self.l = np.array([r[3] for r in rows], float)
        self.c = np.array([r[4] for r in rows], float)
        self.v = np.array([r[5] for r in rows], float)
        self.n = len(rows)
        self.weekend = np.array([wk(x) for x in self.d])
        self.W = np.flatnonzero(~self.weekend)            # индексы будних баров
        self.pos = {x: i for i, x in enumerate(self.d)}
        wv, wc = self.v[self.W], self.c[self.W]
        self._pv = np.concatenate([[0], np.cumsum(np.where(wv > 0, wv, 0))])
        self._pvn = np.concatenate([[0], np.cumsum(wv > 0)])
        self._pc = np.concatenate([[0], np.cumsum(wc)])
        val = wv * wc
        self._pval = np.concatenate([[0], np.cumsum(np.where(val > 0, val, 0))])
        self._pvaln = np.concatenate([[0], np.cumsum(val > 0)])
        # TR будних баров к предыдущему буднему закрытию, в % от close бара
        wh, wl = self.h[self.W], self.l[self.W]
        pc = np.concatenate([[np.nan], wc[:-1]])
        tr = np.maximum.reduce([wh - wl, np.abs(wh - pc), np.abs(wl - pc)]) / wc * 100
        tr[0] = 0.0
        self._ptr = np.concatenate([[0], np.cumsum(tr)])
        # p[i] — сколько будних баров строго до i
        self._before = np.searchsorted(self.W, np.arange(self.n), side='left')

    def avg_vol(self, i, win):
        p = self._before[i]
        if p < win:
            return None
        cnt = self._pvn[p] - self._pvn[p - win]
        if cnt < win * 0.7:
            return None
        return (self._pv[p] - self._pv[p - win]) / cnt

    def ma(self, i, period):
        q = self._before[i] + (0 if self.weekend[i] else 1)
        if q < period:
            return None
        return (self._pc[q] - self._pc[q - period]) / period

    def atr_pct(self, i, win):
        p = self._before[i]
        if p < win + 1:
            return None
        # TR для будних W[p-win..p-1], каждый к предыдущему буднему закрытию
        return (self._ptr[p] - self._ptr[p - win]) / win

    def avg_value_rub(self, i, win=200):
        p = self._before[i]
        seen = min(p, win)
        if seen < 60:
            return None
        cnt = self._pvaln[p] - self._pvaln[p - seen]
        return (self._pval[p] - self._pval[p - seen]) / cnt if cnt > 0 else None


# ── Направления (DIR) ──
def cost():
    return P['commission'] + P['slippage']


DIRS = {
    'long': dict(
        trendOk=lambda c, ma: c > ma, barOk=lambda c, o: c > o,
        closePos=lambda h, l, c: 0.5 if h == l else (c - l) / (h - l),
        gapThr=lambda close: close * (1 + P['gapCancel'] / 100), gapCancel=lambda o, thr: o > thr,
        entry=lambda px: px * (1 + cost()), catStop=lambda ep: ep * (1 - P['stopPct'] / 100),
        hitCat=lambda h, l, px: l <= px, stopFill=lambda o, px: min(o, px),
        trailPx=lambda ext, dist: ext * (1 - dist / 100), hitTrail=lambda h, l, px: l <= px,
        updExt=lambda ext, h, l: h if h > ext else ext,
        exitFinal=lambda px: px * (1 - cost()), ret=lambda xf, ep: (xf / ep - 1) * 100),
    'short': dict(
        trendOk=lambda c, ma: c < ma, barOk=lambda c, o: c < o,
        closePos=lambda h, l, c: 0.5 if h == l else (h - c) / (h - l),
        gapThr=lambda close: close * (1 - P['gapCancel'] / 100), gapCancel=lambda o, thr: o < thr,
        entry=lambda px: px * (1 - cost()), catStop=lambda ep: ep * (1 + P['stopPct'] / 100),
        hitCat=lambda h, l, px: h >= px, stopFill=lambda o, px: max(o, px),
        trailPx=lambda ext, dist: ext * (1 + dist / 100), hitTrail=lambda h, l, px: h >= px,
        updExt=lambda ext, h, l: l if l < ext else ext,
        exitFinal=lambda px: px * (1 + cost()), ret=lambda xf, ep: (1 - xf / ep) * 100),
}


# ── Рынок: IMOEX-режим, RUSFAR, отсечки ──
class Market:
    def __init__(self, imoex_rows, rusfar_rows, fallback_borrow=0.168):
        self.imoex_d = [r[0] for r in imoex_rows]
        c = np.array([r[1] for r in imoex_rows], float)
        self.imoex_c = c
        self.idx = {d: i for i, d in enumerate(self.imoex_d)}
        s50 = np.convolve(c, np.ones(50) / 50, 'full')[:len(c)]
        s200 = np.convolve(c, np.ones(200) / 200, 'full')[:len(c)]
        self.fast = {}
        self.struct = {}
        for i, d in enumerate(self.imoex_d):
            if i >= 49:
                self.fast[d] = 'bull' if c[i] > s50[i] else 'bear'
                if i >= 199:
                    self.struct[d] = 'bull' if s50[i] > s200[i] else 'bear'
        self.fallback = fallback_borrow
        self.rf = {}
        if rusfar_rows:
            d0 = date.fromisoformat(rusfar_rows[0][0])
            d1 = date.fromisoformat(rusfar_rows[-1][0]) + timedelta(days=400)
            by = dict(rusfar_rows)
            cur = rusfar_rows[0][1]
            t = d0
            while t <= d1:
                k = t.isoformat()
                if k in by:
                    cur = by[k]
                self.rf[k] = cur
                t += timedelta(days=1)
        self.biz = [d for d in self.imoex_d if not wk(d)]

    def mom(self, d, n):
        i = self.idx.get(d)
        if i is None or i < n or not self.imoex_c[i - n]:
            return None
        return (self.imoex_c[i] / self.imoex_c[i - n] - 1) * 100

    def trend_gate(self, dir_, d):
        r = self.struct.get(d)
        if dir_ == 'short':
            return True if r is None else r == 'bear'
        if r != 'bull':
            return False
        m = self.mom(d, TREND['longMomDays'])
        return m is not None and m >= TREND['longMomPct']

    def borrow_pct(self, d1, d2):
        days = cal_days(d1, d2)
        if days <= 0:
            return 0.0
        t = date.fromisoformat(d1)
        s = 0.0
        for _ in range(days):
            t += timedelta(days=1)
            r = self.rf.get(t.isoformat())
            s += ((r + SHORT_MARGIN_SPREAD) / 100 if r is not None else self.fallback) / 365
        return s * 100

    def record_to_ex(self, tk: Ticker, record: str) -> str:
        biz = self.biz
        n = 2 if record < T1_SETTLE_FROM else 1
        if not biz or record > biz[-1]:
            return record
        i = bisect.bisect_right(biz, record) - 1
        if i - n < 0:
            return record
        k = bisect.bisect_right(tk.d, biz[i - n])
        return tk.d[k] if k < tk.n else record


@dataclass
class Sig:
    ticker: str
    dir: str
    signalDate: str
    entryDate: str
    ep: float
    catStopPx: float
    vr: float
    vrEntry: float
    entryAtOpen: bool = False
    exitMode: str = 'scalp'
    res: dict = field(default_factory=dict)
    atr: float | None = None                            # ATR% дня сигнала
    ts: float | None = None                             # ширина трейлинга этой сделки, %


def is_div_gap(tk: Ticker, i, divset):
    if tk.d[i] in divset:
        return True
    if i < 1:
        return False
    ret = (tk.c[i] - tk.c[i - 1]) / tk.c[i - 1] * 100
    if ret > -P['divGap']:
        return False
    av = tk.avg_vol(i, P['volWin'])
    vr = tk.v[i] / av if av else 99
    if vr >= P['divGapVr']:
        return False
    if tk.o[i] >= tk.c[i - 1]:
        return False
    return True


def div_too_close(divs, signal_date, entry_ref, gate):
    return any(d > signal_date and cal_days(entry_ref, d) <= gate for d in divs)


def scan_full(tk: Ticker, divs: list, dir_: str, mk: Market, start_idx=None, bar_final=lambda d: True,
              trend_on=True, trend_long_on=True):
    D, caps, EX = DIRS[dir_], QUALITY[dir_], EXIT[dir_]
    divset = set(divs)
    out = []
    n = tk.n
    i = start_idx if start_idx is not None else P['volWin'] + P['maPeriod']
    i = max(i, P['volWin'] + P['maPeriod'])
    consec = 0
    while i < n - 1:
        if tk.weekend[i] or is_div_gap(tk, i, divset):
            consec = 0; i += 1; continue
        av = tk.avg_vol(i, P['volWin'])
        if not av:
            consec = 0; i += 1; continue
        vr = tk.v[i] / av
        if vr >= P['volMin']:
            consec += 1
        else:
            consec = 0; i += 1; continue
        if consec < P['sigDays'] or vr > caps['maxVr']:
            i += 1; continue
        atr = tk.atr_pct(i, P['atrWin'])
        if not atr or atr < P['atrMin'] or atr > P['atrMax']:
            i += 1; continue
        ma = tk.ma(i, P['maPeriod'])
        c, o, h, l = tk.c[i], tk.o[i], tk.h[i], tk.l[i]
        if not ma or not D['trendOk'](c, ma) or not D['barOk'](c, o):
            i += 1; continue
        if D['closePos'](h, l, c) < P['cpMin']:
            i += 1; continue
        if i >= 1 and abs((c - tk.c[i - 1]) / tk.c[i - 1]) * 100 > P['maxDayRet']:
            i += 1; continue
        av_val = tk.avg_value_rub(i, 200)
        if caps['minLiq'] > 0 and not (av_val and av_val >= caps['minLiq']):
            i += 1; continue
        ei = i + 1
        while ei < n and tk.weekend[ei]:
            ei += 1
        if ei >= n:
            i += 1; continue
        if D['gapCancel'](tk.o[ei], D['gapThr'](c)):
            i += 1; continue
        avE = tk.avg_vol(ei, P['volWin'])
        if not avE:
            i += 1; continue
        vrE = tk.v[ei] / avE
        at_open = dir_ == 'short'
        confirmed = not (vrE < P['vrConfirm'] and bar_final(tk.d[ei]))
        if not at_open and not confirmed:
            i += 1; continue
        if div_too_close(divs, tk.d[i], tk.d[ei], P['divGateDays']):
            i += 1; continue
        ep = D['entry'](tk.o[ei]) if at_open else D['entry'](tk.c[ei])
        cat = D['catStop'](ep)
        ts = ATR_TRAIL[dir_] * atr if ATR_TRAIL else EX['tsDist']
        sig = Sig(tk.secid, dir_, tk.d[i], tk.d[ei], ep, cat, vr, vrE, entryAtOpen=at_open, atr=atr, ts=ts)
        out.append(sig)
        if confirmed and dir_ == 'short' and trend_on and mk.trend_gate('short', sig.signalDate):
            epT = D['entry'](tk.c[ei])
            out.append(Sig(tk.secid, dir_, sig.signalDate, sig.entryDate, epT, D['catStop'](epT), vr, vrE,
                           entryAtOpen=False, exitMode='trend'))
        if confirmed and dir_ == 'long' and trend_long_on and mk.trend_gate('long', sig.signalDate):
            out.append(Sig(tk.secid, dir_, sig.signalDate, sig.entryDate, ep, cat, vr, vrE, exitMode='trend'))
        # skip-forward за выход скальпа (упрощённые выходы, как в JS)
        extreme, exit_j = ep, -1
        if at_open and D['hitCat'](tk.h[ei], tk.l[ei], cat):
            exit_j = ei
        if exit_j < 0:
            for j in range(ei + 1, n):
                held = j - ei
                if not is_div_gap(tk, j, divset):
                    if D['hitCat'](tk.h[j], tk.l[j], cat):
                        exit_j = j; break
                    if held >= EX['tsAfter'] and D['hitTrail'](tk.h[j], tk.l[j], D['trailPx'](extreme, ts)):
                        exit_j = j; break
                extreme = D['updExt'](extreme, tk.h[j], tk.l[j])
                if j + 1 < n and tk.d[j + 1] in divset:
                    exit_j = j; break
                if held >= EX['maxHold']:
                    exit_j = j; break
        if exit_j < 0:
            break
        i = exit_j + 1
        consec = 0
    return out


def simulate(s: Sig, tk: Ticker, divs: list, div_vals: dict, mk: Market, bar_final=lambda d: True):
    D = DIRS[s.dir]
    ei = tk.pos.get(s.entryDate)
    if ei is None:
        return None
    ep = s.ep
    is_trend = s.exitMode == 'trend'
    cat = ep * (1 - TREND['stopLong'] / 100) if (is_trend and s.dir == 'long') else s.catStopPx
    divset = set(divs)
    extreme = ep
    exit_d = exit_px = reason = None
    held = 0
    if s.entryAtOpen and D['hitCat'](tk.h[ei], tk.l[ei], cat):
        exit_d, exit_px, reason = tk.d[ei], D['stopFill'](tk.o[ei], cat), 'cat_stop'
    div_cash = 0.0
    EX = EXIT[s.dir]
    max_hold = TREND['maxHold'] if is_trend else EX['maxHold']
    j = ei + 1
    while reason is None and j < tk.n:
        d = tk.d[j]
        h, l, o, c = tk.h[j], tk.l[j], tk.o[j], tk.c[j]
        held = j - ei
        dg = is_div_gap(tk, j, divset)
        if div_vals.get(d):
            div_cash += div_vals[d]
        if not dg:
            if D['hitCat'](h, l, cat):
                exit_d, exit_px, reason = d, D['stopFill'](o, cat), 'cat_stop'; break
            if is_trend:
                if s.dir == 'long':
                    armed = held >= TREND['warmup'] and extreme >= ep * (1 + TREND['lockArm'] / 100)
                else:
                    armed = held >= TREND['warmup'] and extreme <= ep * (1 - TREND['lockArm'] / 100)
                lock = (extreme * (1 - TREND['lockTrail'] / 100) if s.dir == 'long'
                        else extreme * (1 + TREND['lockTrail'] / 100)) if armed else None
                dn = TREND['decayN'] if held > TREND['decayAfter'] else TREND['donchN']
                donch = None
                if held >= TREND['warmup'] and j >= dn:
                    donch = tk.l[j - dn:j].min() if s.dir == 'long' else tk.h[j - dn:j].max()
                if lock is not None and D['hitTrail'](h, l, lock):
                    exit_d, exit_px, reason = d, D['stopFill'](o, lock), 'profit_lock'; break
                if donch is not None and D['hitTrail'](h, l, donch):
                    exit_d, exit_px, reason = d, D['stopFill'](o, donch), 'donch_break'; break
                if s.dir == 'long' and TREND['flipLong'] and mk.fast.get(d) == 'bear' and bar_final(d):
                    exit_d, exit_px, reason = d, c, 'regime'; break
            elif held >= EX['tsAfter']:
                ts = D['trailPx'](extreme, s.ts if s.ts is not None else EX['tsDist'])
                if D['hitTrail'](h, l, ts):
                    exit_d, exit_px, reason = d, D['stopFill'](o, ts), 'trailing_stop'; break
        extreme = D['updExt'](extreme, h, l)
        if j + 1 < tk.n:
            div_t1 = tk.d[j + 1] in divset
        else:
            div_t1 = any(dd > d and cal_days(d, dd) <= 1 for dd in divs)
        if div_t1 and bar_final(d):
            exit_d, exit_px, reason = d, c, 'div_exit'; break
        if held >= max_hold and bar_final(d):
            exit_d, exit_px, reason = d, c, 'max_hold'; break
        j += 1
    if reason is None:
        exit_d, exit_px, reason = tk.d[-1], tk.c[-1], 'open'
        held = tk.n - 1 - ei
    xf = D['exitFinal'](exit_px)
    ret = D['ret'](xf, ep)
    if div_cash:
        ret += (-1 if s.dir == 'short' else 1) * div_cash / ep * 100
    if s.dir == 'short':
        ret -= mk.borrow_pct(s.entryDate, exit_d)
    return dict(exitDate=exit_d, exitPrice=xf, ret=ret, reason=reason, held=held)


# ── Данные ──
def _copy(sql):
    """COPY запроса в DataFrame — в контейнере через DB_URL, на ноутбуке через ssh (как store._fetch)."""
    from . import store
    q = 'COPY (' + ' '.join(sql.split()) + ') TO STDOUT WITH CSV HEADER'
    if os.environ.get('DB_URL'):
        from sqlalchemy import create_engine
        if 'engine' not in store._CACHE:
            url = os.environ['DB_URL']
            store._CACHE['engine'] = create_engine(url, connect_args={'ssl_context': False} if 'pg8000' in url else {})
        raw = store._CACHE['engine'].raw_connection()
        try:
            buf = io.BytesIO(); cur = raw.cursor(); cur.execute(q, stream=buf); raw.commit()
        finally:
            raw.close()
        buf.seek(0)
        return pd.read_csv(buf)
    import subprocess
    inner = f"docker exec frame-db-1 psql -U postgres -d moex_db -c {json.dumps(q)}"
    cmd = ["bash", "-c", inner] if Path('/opt/frame').exists() else store.SSH + [inner]
    r = subprocess.run(cmd, capture_output=True, timeout=900, stdin=subprocess.DEVNULL)
    if r.returncode:
        raise RuntimeError(r.stderr.decode()[:400])
    return pd.read_csv(io.BytesIO(r.stdout))


def extra_path():
    from . import store
    return store.DATA / 'stocks_extra.csv.gz'


def rusfar_path():
    """Свежая выгрузка `data stocks-iss` лежит в томе <BT_DATA> (образ только для чтения), иначе — копия из репозитория."""
    from . import store
    f = store.DATA / 'rusfar.csv.gz'
    return f if f.exists() else DATA / 'rusfar.csv.gz'


def load_db(until: str | None = None):
    st = _copy("""SELECT secid, begin_time::date AS d, open, high, low, close, volume FROM candles
                  WHERE interval = 24 AND type = 'stock' AND begin_time >= '2011-01-01' ORDER BY secid, begin_time""")
    frames = [st]
    if extra_path().exists():
        ex = pd.read_csv(extra_path())
        frames.append(ex[~ex.secid.isin(set(st.secid))][['secid', 'd', 'open', 'high', 'low', 'close', 'volume']])
    df = pd.concat(frames, ignore_index=True)
    df['d'] = df.d.astype(str).str[:10]
    if until:
        df = df[df.d <= until]
    df = df[(df.close > 0) & (df.open > 0)].dropna(subset=['open', 'high', 'low', 'close', 'volume'])
    df = df.sort_values(['secid', 'd'])
    tickers = {}
    for t, g in df.groupby('secid', sort=False):
        if len(g) >= 100 and t not in DUPES:
            tickers[t] = Ticker(t, g[['d', 'open', 'high', 'low', 'close', 'volume']].values.tolist())
    ix = _copy("SELECT trade_date AS d, close FROM index_data WHERE secid = 'IMOEX' AND close IS NOT NULL ORDER BY trade_date")
    imoex = [(str(d)[:10], float(c)) for d, c in ix.values if not until or str(d)[:10] <= until]
    rf = pd.read_csv(rusfar_path())
    mk = Market(imoex, [(str(d), float(r)) for d, r in rf.values])
    splits = {}
    for r in pd.read_csv(DATA / 'splits.csv.gz').itertuples():
        splits.setdefault(r.secid, []).append((str(r.d), float(r.before) / float(r.after)))
    dv = _copy("SELECT secid, registry_close_date, value FROM dividends WHERE registry_close_date IS NOT NULL")
    divs, dvals = {}, {}
    for t, reg, v in dv.values:
        if t not in tickers:
            continue
        reg = str(reg)[:10]
        ex = mk.record_to_ex(tickers[t], reg)
        v = float(v or 0)
        for sd, k in splits.get(t, []):
            if sd > reg:
                v *= k
        divs.setdefault(t, []).append(ex)
        dvals.setdefault(t, {})
        dvals[t][ex] = dvals[t].get(ex, 0) + v
    for t in divs:
        divs[t].sort()
    return tickers, mk, divs, dvals


def fetch_iss_extra():
    """Разовая выгрузка ISS по запросу (не крон): свечи бумаг листинга TQBR, которых нет в candles, включая снятые
    с торгов, и свежий RUSFAR O/N — в <BT_DATA>. Несколько минут."""
    ISS = 'https://iss.moex.com/iss'

    def get(url):
        for k in range(5):
            try:
                with urllib.request.urlopen(url, timeout=40) as r:
                    return json.load(r)
            except Exception:
                time.sleep(2 + 4 * k)
        raise RuntimeError(url)

    have = set(_copy("SELECT DISTINCT secid FROM candles WHERE interval = 24 AND type = 'stock'").secid)
    listing, start = [], 0
    while True:
        j = get(f'{ISS}/history/engines/stock/markets/shares/boards/TQBR/listing.json?iss.meta=off&start={start}')
        c, rows = j['securities']['columns'], j['securities']['data']
        listing += [dict(zip(c, r)) for r in rows]
        if len(rows) < 100:
            break
        start += len(rows)
    todo = sorted({x['SECID'] for x in listing if (x.get('history_till') or '') >= '2013-06-01'} - have)
    out = []
    for t in todo:
        s = 0
        while True:
            j = get(f'{ISS}/engines/stock/markets/shares/boards/TQBR/securities/{t}/candles.json?interval=24&from=2011-01-01'
                    f'&iss.meta=off&iss.only=candles&candles.columns=begin,open,high,low,close,volume&start={s}')
            rows = j['candles']['data']
            out += [(t, r[0][:10], *r[1:]) for r in rows]
            if len(rows) < 500:
                break
            s += 500
    extra_path().parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out, columns=['secid', 'd', 'open', 'high', 'low', 'close', 'volume']).to_csv(extra_path(), index=False)
    rows, s = [], 0
    while True:
        j = get(f'{ISS}/engines/stock/markets/index/boards/MMIX/securities/RUSFAR/candles.json?interval=24&from=2015-01-01'
                f'&iss.meta=off&iss.only=candles&candles.columns=begin,close&start={s}')
        data = j['candles']['data']
        rows += [(r[0][:10], r[1]) for r in data if r[1]]
        if len(data) < 500:
            break
        s += 500
    if rows:
        pd.DataFrame(rows, columns=['d', 'r']).to_csv(extra_path().parent / 'rusfar.csv.gz', index=False)
    return {'бумаг_докачано': len(todo), 'свечей': len(out), 'rusfar_до': rows[-1][0] if rows else None}


def run(tickers, mk, divs, dvals, bar_final=lambda d: True):
    sigs = []
    for t, tk in tickers.items():
        for dir_ in ('long', 'short'):
            for s in scan_full(tk, divs.get(t, []), dir_, mk, None, bar_final):
                r = simulate(s, tk, divs.get(t, []), dvals.get(t, {}), mk, bar_final)
                if r:
                    s.res = r
                    sigs.append(s)
    return sigs


def cluster_weights(sigs):
    cl = {}
    for s in sigs:
        if s.exitMode == 'trend':
            cl[(s.signalDate, s.dir)] = cl.get((s.signalDate, s.dir), 0) + 1
    return [1 / cl[(s.signalDate, s.dir)] if s.exitMode == 'trend' else 1.0 for s in sigs]


def account(sigs, w, cap_units):
    """В позициях не больше cap_units идей (тренд-кластер дня — одна идея). Слот свободен со дня после выхода;
    в один день — по убыванию VR (данные самого дня). Возвращает вес исполненной сделки, 0 — пропущена."""
    order = sorted(range(len(sigs)), key=lambda i: (sigs[i].entryDate, -sigs[i].vr, sigs[i].exitMode))
    out, active = [0.0] * len(sigs), []
    for i in order:
        s = sigs[i]
        active = [(x, u) for x, u in active if x >= s.entryDate]
        if sum(u for _, u in active) + w[i] <= cap_units + 1e-9:
            out[i] = w[i]
            active.append((s.res['exitDate'] if s.res['reason'] != 'open' else '9999', w[i]))
    return out


def curve(sigs, weights, tickers, mk, size, since, until):
    """Дневная кривая: реализовано + переоценка открытых по close (шорт — минус набежавший заём). → уровень, позиции, экспозиция"""
    cum, t = {}, date(2011, 1, 1)
    acc = 0.0
    while t <= date.today() + timedelta(days=1):
        cum[t.isoformat()] = acc
        t += timedelta(days=1)
        r = mk.rf.get(t.isoformat())
        acc += ((r + SHORT_MARGIN_SPREAD) / 100 if r is not None else mk.fallback) / 365 * 100
    days = sorted({d for tk in tickers.values() for d in tk.d if since <= d <= until and not wk(d)})
    n = len(days)
    real, mtm, expo, pos = np.zeros(n + 1), np.zeros(n), np.zeros(n), np.zeros(n)
    for s, w in zip(sigs, weights):
        if not w:
            continue
        tk, ex = tickers[s.ticker], s.res['exitDate']
        closed = s.res['reason'] != 'open'
        a = bisect.bisect_left(days, s.entryDate)
        b = bisect.bisect_left(days, ex) if closed else n
        for k in range(a, b):
            d = days[k]
            c = tk.c[bisect.bisect_right(tk.d, d) - 1]
            px = (1 - c / s.ep) * 100 if s.dir == 'short' else (c / s.ep - 1) * 100
            if s.dir == 'short':
                px -= cum[d] - cum[s.entryDate]
            mtm[k] += size * w * px / 100
            expo[k] += size * w
            pos[k] += 1
        if closed:
            k = bisect.bisect_left(days, ex)
            if k < n:
                real[k] += size * w * s.res['ret'] / 100
    return days, np.cumsum(real[:-1]) + mtm, pos, expo


REASON = {'trailing_stop': 'трейлинг', 'cat_stop': 'кат. стоп', 'max_hold': 'время', 'div_exit': 'до отсечки',
          'profit_lock': 'защита прибыли', 'donch_break': 'пробой канала', 'regime': 'IMOEX ниже MA50', 'open': 'открыта'}
LEG = lambda s: ('лонг' if s.dir == 'long' else 'шорт') + ('-тренд' if s.exitMode == 'trend' else '-скальп')


def execute(spec, progress=None):
    """Прогон для воркера Стенда: те же таблицы, что у остальных прогонов (s, res, S, T, A, K, E)."""
    from . import metrics, runner
    capital = float(spec.get('capital') or 10_000_000)
    slots = int(spec.get('slots') or 10)
    since = str(spec.get('since') or '2012-01-01')[:10]
    until = str(spec.get('until') or '')[:10] or None
    today = date.today().isoformat()
    final = lambda d: d < today                              # сегодняшний бар ещё формируется
    tickers, mk, divs, dvals = load_db(until)
    if progress: progress(1, 3)
    sigs = [s for s in run(tickers, mk, divs, dvals, final) if s.signalDate >= since]
    sigs.sort(key=lambda s: (s.entryDate, s.ticker, s.exitMode))
    if progress: progress(2, 3)
    w = cluster_weights(sigs)
    wa = account(sigs, w, slots)
    size = capital / slots
    last = max(tk.d[-1] for tk in tickers.values())
    days, lvl, pos, expo = curve(sigs, wa, tickers, mk, size, since, until or last)
    E = pd.DataFrame({'equity': capital + lvl, 'positions': pos.astype(int), 'notional': expo, 'go_used': 0.0, 'margin_call': 0},
                     index=pd.to_datetime(days)).rename_axis('d')
    c_side = (P['commission'] + P['slippage'])
    T = pd.DataFrame([dict(n=i, st=s.ticker, d=pd.Timestamp(s.entryDate), secid=s.ticker, side=1 if s.dir == 'long' else -1,
                           move=None, thr_up=None, thr_dn=None, px_in=s.ep, d_out=pd.Timestamp(s.res['exitDate']),
                           px_out=s.res['exitPrice'], net=s.res['ret'] / 100, comm=2 * P['commission'], spread=2 * P['slippage'],
                           gross=s.res['ret'] / 100 + 2 * c_side, m_in=None, m_out=None, mfe=None, mae=None,
                           exit_reason=f"{REASON.get(s.res['reason'], s.res['reason'])} · {LEG(s)}")
                      for i, s in enumerate(sigs)])
    ex = [i for i, x in enumerate(wa) if x]
    A = pd.DataFrame([dict(n=i, d=T.d[i], st=sigs[i].ticker, qty=int(size * wa[i] / sigs[i].ep), notional=size * wa[i], go=0.0,
                           equity_in=None, comm_rub=size * wa[i] * 2 * P['commission'], spread_rub=size * wa[i] * 2 * P['slippage'],
                           pnl_rub=size * wa[i] * sigs[i].res['ret'] / 100, go_cut=False) for i in ex],
                     columns=['n', 'd', 'st', 'qty', 'notional', 'go', 'equity_in', 'comm_rub', 'spread_rub', 'pnl_rub', 'go_cut'])
    K = pd.DataFrame([dict(n=i, d=T.d[i], st=sigs[i].ticker, side=T.side[i], reason='нет места на счёте')
                      for i, x in enumerate(wa) if not x], columns=['n', 'd', 'st', 'side', 'reason'])
    legs = {}
    for s, x in zip(sigs, w):
        if s.res['reason'] == 'open':
            continue
        k = LEG(s); n0, r0 = legs.get(k, (0, 0.0))
        legs[k] = (n0 + 1, r0 + 1e6 * x * s.res['ret'] / 100)
    full = {**spec, 'strategy': 'screener_vb', 'capital': capital, 'slots': slots, 'since': since, 'until': until,
            'universe': sorted({s.ticker for s in sigs}), 'exec': 'T+1', 'tariff': '0.05 % + 0.1 % за сторону',
            'spread': 'в проскальзывании', 'go': 'не нужно (акции)', 'leverage': 1.0, 'go_mult': 1.0,
            'rule': {'id': 'screener_vb', 'name': spec.get('name') or 'Скринер VB: объёмный пробой лонг + шорт (TQBR)'}}
    res = {'kind': 'screener', 'period': [since, str(T.d_out.max().date()) if len(T) else since], 'signals': len(sigs),
           'per_trade': metrics.per_trade(T) if len(T) else {'сделок': 0},
           'by_instrument': metrics.by_instrument(T).reset_index().to_dict('records') if len(T) else [],
           'data_until': last, 'бумаг': len(tickers), 'бумаг_с_докачки': extra_path().exists(),
           'screener': {'как_скринер_1млн_на_идею_руб': round(sum(1e6 * x * s.res['ret'] / 100 for s, x in zip(sigs, w)
                                                                 if s.res['reason'] != 'open')),
                        'ноги': {k: {'сделок': n0, 'руб_на_1млн': round(r0)} for k, (n0, r0) in legs.items()}}}
    if len(E):
        res['account'] = runner._account_block(A, K, E, full)
    return full, res, None, T, A, K, E
