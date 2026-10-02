"""Мастер ITEKCrypto Kamaz взял входы, которых не взял наш бот «спокойный Kamaz» (28.09–02.10.2026). Разбор: почему он вошёл,
как заработал, можно ли повторить. Только исследование — живой бот не трогаем.

Источники: сделки мастера — inbox/private/kamaz_master/closed.jsonl (накопитель, 125 ордеров за 06–10.2026) + открытая часть SOL;
наш журнал — inbox/private/kamaz_master/our_journal/*.jsonl (копия /opt/kamaz-bot/bot/state/journal); минутки Bybit linear.

Разделы (всё печатается и пишется в kamaz_missed_entries.md рядом):
  1. Условия нашего входа поминутно вокруг его входа (RSI7 3м по-биржевому, от хая 60м, ход 120м; пороги × масштаб монеты дня),
     «почти-сигнал»: какое условие не прошло и на сколько; RSI «внутри свечи» по цене его ордера.
  2. Контекст: монета (ход, объём, минимум суток), биткоин (фильтр «ниже минимума 12 ч», новые минимумы 6–24 ч), эфир, корзина
     8 альтов, финансирование, открытый интерес.
  3. После входа: путь цены (просадка/лучшая точка), когда достигалась наша цель, как он вышел; все его кампании за 4 мес.
  4. Альтернативные условия входа на всей истории мастера: сколько его входов ловит и сколько лишних даёт.
  5. Бэктест «Kamaz B» (вход внутри минуты по цене срабатывания) и выхода частями на ежедневной десятке против рабочего
     варианта + плацебо (вход наугад, столько же сделок).
Запуск: .venv/bin/python experiments/kamaz_missed_entries.py [--skip-bt]
"""
from __future__ import annotations

import glob
import json
import multiprocessing as mp
import sys
import time
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from factory import market  # noqa: E402
from factory.funding import funding  # noqa: E402
import grid_engine as ge  # noqa: E402

MASTER = ROOT / "inbox/private/kamaz_master"
OUT = Path(__file__).with_suffix(".md")
END = pd.Timestamp("2026-10-02 09:00")
HIST_A = pd.Timestamp("2026-06-01")
BASKET = ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "XRPUSDT", "1000BONKUSDT", "SHIB1000USDT", "ADAUSDT", "WIFUSDT"]
FOCUS = [  # (монета, время ордера мастера UTC, что это)
    ("SUIUSDT", "2026-09-29 01:32:24", "пропуск"),
    ("SOLUSDT", "2026-09-29 18:34:53", "пропуск"),
    ("BCHUSDT", "2026-09-28 02:52:00", "пропуск (нет в десятке)"),
    ("ZECUSDT", "2026-09-29 01:10:26", "совпал (у нас докупка)"),
    ("ZECUSDT", "2026-10-01 19:01:58", "совпал (мы на 22 мин позже)"),
    ("SOLUSDT", "2026-09-30 14:52:23", "совпал (мы на 19 мин раньше)"),
]
MIN = pd.Timedelta(minutes=1)
LINES: list[str] = []


SUMMARY = """## Коротко

| сделка мастера | почему мы не вошли | можно повторить? |
|---|---|---|
| SUI 29.09 01:32 @1.1087, +3.92% | почти-сигнал: от хая −3.6…−4.6% и ход 120 мин −3.4…−4.9% проходили, RSI на закрытиях 3-минуток 15.22 / 15.73 / 15.24 при пороге 15. По его цене внутри 3-минутки RSI = 13.3 | да: RSI по формирующейся 3-минутке (B) вошёл бы в 01:19 @1.1265, докупка −1.4% ≈ его цена, выход +1.6% от средней в 06:31 |
| SOL 29.09 18:34 @118.83 | не пролив: RSI 75, цена на максимуме часа, +0.35% за 2 ч, биткоин и корзина вверх, объём обычный | нет — ни одно из 9 правил и 48 порогов не ловит; ручной/внешний вход |
| BCH 28.09 02:52 @309.4, +0.29% | наш сигнал был (02:47–02:53, RSI 8, −4.7%/−5.5%, обвал −3.5% за минуту при объёме ×372), но BCH не в десятке | только расширением списка монет; он сам вышел почти в ноль через 28 ч |
| ZEC 29.09 01:10 (совпал) | на закрытии RSI 15.20; внутри минуты наш порог сработал в ту же минуту @1429 (он @1432.8) | да, B |
| ZEC 01.10 19:01 (мы на 22 мин позже) | не наш сигнал: пролив −1% за минуту (объём ×19, пробит минимум суток), RSI внутри 21, от хая −2.5% | частично: «минутный пролив ≥1% + RSI внутри ≤ 20» |
| SOL 30.09 14:52 (мы раньше) | доливка в уже открытую позицию, RSI 40; наш сигнал 14:32 на 0.4% ниже | не нужно |

**Главное про вход.** Он, по всей видимости, считает те же условия Kamaz A, но по ТЕКУЩЕЙ цене внутри формирующейся 3-минутки, а наш бот — по закрытым. Вариант B (наши пороги 15/−3/−2, RSI по формирующейся свече, вход лимиткой по цене срабатывания) ловит 26 из 44 его кампаний за 4 мес. (59%) против 8 (18%) у бота; точность 6.0% против 3.2%. Перебор 48 порогов «внутри минуты» с подбором до 16.08 и проверкой после выбрал именно наши 15/−3/−2 — то есть меняется не порог, а момент расчёта. Лишних сигналов больше в 1.7 раза (≈21 против 13 в неделю на его 12 монетах — он берёт лишь малую часть).

**Главное про выход (+3.9% против наших +1.6–2%).** Разница не во входе, а в удержании: по SUI наша цель +1.94% была бы через 21 мин; он держал 6.9 ч и продал на 1.1522 — ровно на 0.0075 (0.65%) ниже пика 1.1597, похоже на скользящий стоп. Таймера у него нет: 43 закрытых кампании — 98% в плюс, в среднем +2.8% (наш выход с его же входов — +1.45%), но ценой многодневных просадок без стопа (ADA −5% 9 дней, SUI −14% месяц, BCH −18% 3.5 мес.) и закрытий «в ноль» через 1–4 дня. Большинство продаж — после отката от более высокого пика (не лимитная цель). ZEC — особый режим: плечо 2, без докупок, ровно +1.30% (4 раза подряд). SOL — частями: 69% продано (первая покупка в среднем +1.9%, часть второй +3.8% 02.10 04:54), 31% держит.

**Бэктест 07.2021–27.09.2026 (десятка, $100k, x1).** A +$105.6k, просадка −4.9% (сходится с прежним). B +$123.6k, −6.7%, 2025 +14% против +9%, 2026 +8.4% против +6.5%; плацебо B $5…52k. Но при худшем исполнении (проход 0.1%, комиссии ×2) B ≈ A (+$67.9k против +$64.4k): прибавка B держится на касаниях хвостов свечей — проверяется только демо. Цель 3.9/3.0 с таймером 12 ч: A +$152k, B +$202k, просадка −6.6/−8.0%, в плюс 87% сделок вместо 95%; соседние цели 3.0 и 4.5 тоже лучше базы (не острый пик), худшее исполнение переживает (A +$110k, B +$149k). Оговорки: прибавка почти вся в 2021–2024, в 2025–2026 около нуля (A: 2025 +9.8% против +8.9%, 2026 +1.2% против +6.5%); часть прибавки — свойство самого выхода (вход наугад с целью 3.9: в среднем +$37k против +$16k). Таймер 48 ч — просадка −17…−19%, не годится. Выборка мастера мала (44 кампании за 4 мес.), поэтому B и длинная цель — гипотезы для параллельной бумажной книги/демо, не замена рабочему варианту.

Данных для сверки с основным счётом ITEK нет: основной и REINVEST сейчас скрывают сделки (openTradeInfoProtection=1), личные выгрузки кончаются 09.2025. Наш журнал пишет только полные сигналы — «почти-сигналов» в нём нет (по SUI и SOL 29.09 пусто).
"""


def say(s: str = ""):
    print(s, flush=True)
    LINES.append(s)


def md_table(df: pd.DataFrame, floatfmt=2) -> str:
    d = df.copy()
    for c in d.columns:
        if d[c].dtype.kind == "f":
            sig = any(w in str(c) for w in ("цена", "средняя"))
            d[c] = d[c].map(lambda x: "" if pd.isna(x) else (f"{x:.6g}" if sig else f"{x:.{floatfmt}f}"))
    head = "| " + " | ".join(map(str, d.columns)) + " |"
    sep = "|" + "|".join("---" for _ in d.columns) + "|"
    rows = ["| " + " | ".join(map(str, r)) + " |" for r in d.astype(str).values]
    return "\n".join([head, sep] + rows)


# ---------------------------------------------------------------- данные

def load1m(sym: str, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
    """Минутки из кэша (data/klines + data/klines_seg), недостающие края докачиваются (хвост — в основной кэш, голова — сегментом)."""
    def read():
        parts = []
        f = ROOT / f"data/klines/{sym}_1.parquet"
        if f.exists():
            parts.append(pd.read_parquet(f))
        for g in glob.glob(str(ROOT / f"data/klines_seg/{sym}_*.parquet")):
            parts.append(pd.read_parquet(g))
        if not parts:
            return pd.DataFrame(columns=list("ohlcv"), dtype=float)
        k = pd.concat(parts)
        return k[~k.index.duplicated(keep="last")].sort_index().astype(float)
    k = read()
    sel = k[(k.index >= a) & (k.index <= b)]
    fetched = False
    if sel.empty or sel.index[-1] < b - 3 * MIN:
        market.klines(sym, "1", (sel.index[-1] if len(sel) else a) - 5 * MIN, b)   # хвост — в основной кэш
        fetched = True
    if sel.empty or sel.index[0] > a + 10 * MIN:
        hi = sel.index[0] if len(sel) else b
        z = market._fetch(sym, "1", a, hi)
        if len(z):
            p = ROOT / f"data/klines_seg/{sym}_{a:%Y-%m-%d}_{hi:%Y-%m-%d}.parquet"
            tmp = p.with_suffix(".tmp"); z.to_parquet(tmp); tmp.replace(p)
        fetched = True
    if fetched:
        k = read()
    k = k[(k.index >= a) & (k.index <= b)]
    gaps = k.index.to_series().diff()
    big = gaps[gaps > pd.Timedelta(hours=3)]
    for t, d in big.items():                      # дыры внутри — докачать
        z = market._fetch(sym, "1", t - d, t)
        if len(z):
            p = ROOT / f"data/klines_seg/{sym}_{(t - d):%Y-%m-%d}_{t:%Y-%m-%d}.parquet"
            tmp = p.with_suffix(".tmp"); z.to_parquet(tmp); tmp.replace(p)
    if len(big):
        k = read(); k = k[(k.index >= a) & (k.index <= b)]
    return k


def oi_series(sym: str, a: pd.Timestamp, b: pd.Timestamp) -> pd.Series:
    """Открытый интерес Bybit, 5 мин (публичный API)."""
    rows, end_ms = [], int(b.timestamp() * 1000)
    for _ in range(40):
        u = (f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={sym}&intervalTime=5min&limit=200"
             f"&startTime={int(a.timestamp() * 1000)}&endTime={end_ms}")
        try:
            r = json.load(urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=30))
            L = r["result"]["list"]
        except Exception:
            L = []
        if not L:
            break
        rows += L
        oldest = int(L[-1]["timestamp"])
        if oldest <= a.timestamp() * 1000 or len(L) < 200:
            break
        end_ms = oldest - 1
        time.sleep(0.1)
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series({pd.to_datetime(int(x["timestamp"]), unit="ms"): float(x["openInterest"]) for x in rows}).sort_index()
    return s


def master_orders() -> pd.DataFrame:
    L = [json.loads(x) for x in open(MASTER / "closed.jsonl")]
    D = pd.DataFrame(L)
    D["t_open"] = pd.to_datetime(D.t_open); D["t_close"] = pd.to_datetime(D.t_close)
    D = D[["sym", "side", "t_open", "t_close", "order_price", "p_open", "p_close", "size", "lev"]].copy()
    D["open_left"] = 0.0
    # открытая часть (снимок 02.10): SOL 30.09 14:52 @119.02, 6.5 шт, осталось 4.6 — закрыто 1.9 (строка в closed)
    op = json.load(open(MASTER / "open_2026-10-02.json"))
    for pos in op["snapshots"][-1]["positions"]:
        D = pd.concat([D, pd.DataFrame([dict(sym=pos["sym"], side=1, t_open=pd.Timestamp(pos["t_open"]), t_close=pd.NaT,
                                             order_price=pos["order_price"], p_open=pos["p_open"], p_close=np.nan,
                                             size=pos["size_left"], lev=pos["lev"], open_left=pos["size_left"])])], ignore_index=True)
    return D.sort_values("t_open").reset_index(drop=True)


def campaigns(D: pd.DataFrame) -> pd.DataFrame:
    """Склейка ордеров в кампании: следующий ордер той же монеты открыт, пока предыдущие не закрыты полностью."""
    D = D.copy(); D["t_end"] = D.t_close.fillna(END + pd.Timedelta(days=30))
    out = []
    for sym, g in D.groupby("sym"):
        g = g.sort_values("t_open")
        cur, cur_end = [], None
        for r in g.itertuples():
            if cur and r.t_open < cur_end:
                cur.append(r); cur_end = max(cur_end, r.t_end)
            else:
                if cur:
                    out.append(cur)
                cur, cur_end = [r], r.t_end
        if cur:
            out.append(cur)
    rows = []
    for cid, c in enumerate(sorted(out, key=lambda c: c[0].t_open)):
        q = np.array([x.size for x in c]); px = np.array([x.order_price for x in c])
        closed = [x for x in c if pd.notna(x.t_close)]
        qc = np.array([x.size for x in closed]); pc = np.array([x.p_close for x in closed])
        avg = (q * px).sum() / q.sum()
        rows.append(dict(cid=cid, sym=c[0].sym, t0=c[0].t_open, p0=c[0].order_price, n=len(c), avg=avg, usd=(q * px).sum(),
                         t_last_buy=max(x.t_open for x in c), t1=max((x.t_close for x in closed), default=pd.NaT),
                         t1_first=min((x.t_close for x in closed), default=pd.NaT),
                         px_out=(qc * pc).sum() / qc.sum() if len(qc) else np.nan,
                         open_left=sum(x.open_left for x in c), lev=c[0].lev,
                         orders=[(x.t_open, x.order_price, x.size) for x in c]))
    C = pd.DataFrame(rows)
    C["res_pct"] = (C.px_out / C.avg - 1) * 100
    return C


# ---------------------------------------------------------------- признаки

A_RSI = 1 / 7


def feats(k: pd.DataFrame) -> pd.DataFrame:
    """Признаки на закрытии каждой минутки + цена срабатывания «внутри минуты» (по данным на открытие минуты)."""
    F = pd.DataFrame(index=k.index)
    c, h, l, o, v = k.c, k.h, k.l, k.o, k.v
    F["o"], F["h"], F["l"], F["c"], F["v"] = o, h, l, c, v
    F["r3"] = ge.rsi3(c, "bybit")
    F["hi60"] = h.rolling(60).max()
    F["from_hi"] = (c / F.hi60 - 1) * 100
    F["ch120"] = (c / c.shift(120) - 1) * 100
    F["ch1440"] = (c / c.shift(1440) - 1) * 100
    for n in (1, 5, 15, 30, 60, 240):
        F[f"ch{n}"] = (c / c.shift(n) - 1) * 100
    F["from_hi240"] = (c / h.rolling(240).max() - 1) * 100
    F["from_hi1440"] = (c / h.rolling(1440).max() - 1) * 100
    for hh in (6, 24, 168):
        lo = l.shift(30).rolling(hh * 60, min_periods=hh * 30).min()
        F[f"vs_lo{hh}h"] = (l.rolling(30).min() / lo - 1) * 100          # <0 — за последние 30 мин пробит минимум N ч
    F["vol_x"] = v / v.rolling(1440, min_periods=300).median()
    F["vol60_x"] = v.rolling(60).sum() / (v.rolling(1440, min_periods=300).sum() / 24)
    # RSI «внутри» формирующейся 3-минутки: состояние после последней закрытой 3-минутки
    c3 = c.resample("3min", label="left", closed="left").last().dropna()
    d3 = c3.diff()
    up = d3.clip(lower=0).ewm(alpha=A_RSI, adjust=False).mean(); dn = (-d3.clip(upper=0)).ewm(alpha=A_RSI, adjust=False).mean()
    prev_lbl = k.index.floor("3min") - pd.Timedelta(minutes=3)
    F["c3p"] = c3.reindex(prev_lbl).values; F["up3"] = up.reindex(prev_lbl).values; F["dn3"] = dn.reindex(prev_lbl).values
    F["H_pre"] = np.maximum(h.shift(1).rolling(59).max(), o)              # максимум часа, известный на открытие минуты
    F["c120"] = c.shift(120); F["c1440"] = c.shift(1440)
    return F


def rsi_live(F: pd.DataFrame, P) -> np.ndarray:
    """RSI7 3м, если формирующаяся 3-минутка сейчас стоит на цене P."""
    a = A_RSI; d = P - F.c3p.values
    upn = (1 - a) * F.up3.values + a * np.clip(d, 0, None); dnn = (1 - a) * F.dn3.values + a * np.clip(-d, 0, None)
    return 100 - 100 / (1 + upn / dnn)


def trigger_price(F: pd.DataFrame, scale, rsi_thr=15.0, drop1h=3.0, drop2h=2.0) -> np.ndarray:
    """Самая высокая цена внутри минуты, при которой выполнены все три условия Kamaz A (RSI по формирующейся 3-минутке)."""
    a = A_RSI; kk = rsi_thr / (100 - rsi_thr)
    up, dn, cp = F.up3.values, F.dn3.values, F.c3p.values
    X = ((1 - a) * up / kk - (1 - a) * dn) / a
    Y = (kk * (1 - a) * dn - (1 - a) * up) / a
    p_rsi = np.where(X >= 0, cp - X, cp + Y)
    s = np.asarray(scale, dtype=float)
    return np.fmin(np.fmin(F.H_pre.values * (1 - drop1h * s / 100), F.c120.values * (1 - drop2h * s / 100)), p_rsi)


def scale_for(sym: str, idx: pd.DatetimeIndex) -> np.ndarray:
    doge = ge.range_series("DOGEUSDT", idx.min(), idx.max())
    return ge.daily_scale(sym, idx, doge)


def our_journal() -> pd.DataFrame:
    rows = []
    for f in sorted(glob.glob(str(MASTER / "our_journal/*.jsonl"))):
        for x in open(f):
            rows.append(json.loads(x))
    J = pd.DataFrame(rows)
    J["t"] = pd.to_datetime(J.t.astype(str).str[:19], errors="coerce")
    return J


# ---------------------------------------------------------------- раздел 1–3: пропущенные входы

def nearest_miss(F: pd.DataFrame, sc: float, t: pd.Timestamp) -> dict:
    """Самая близкая к сигналу минута в [t−60, t+15]: наибольший недобор среди трёх условий (в долях порога) минимален."""
    w = F.loc[t - 60 * MIN: t + 15 * MIN]
    g_r = (w.r3 - 15) / 15; g_h = (w.from_hi + 3 * sc) / (3 * sc); g_c = (w.ch120 + 2 * sc) / (2 * sc)
    G = pd.concat([g_r, g_h, g_c], axis=1); G.columns = ["RSI", "от хая", "ход 120м"]
    worst = G.max(axis=1); m = worst.idxmin()
    fail = [n for n in G.columns if G.loc[m, n] > 0]
    return dict(t=m, worst=worst.loc[m], fail=fail, r3=w.r3[m], from_hi=w.from_hi[m], ch120=w.ch120[m])


def campaign_sim(F: pd.DataFrame, t_in: pd.Timestamp, p_in: float, sc: float, tp1=1.94, tpn=1.6, timer_h=12.0, fill_minute_ladder=False):
    """Наша кампания (лесенка DOGE × масштаб, цель 1.94/1.6, таймер 12 ч), если войти в t_in по p_in. Без комиссий."""
    lv = ge.DOGE_LV * sc / 100; sz = ge.SIZES
    w = F.loc[t_in:]
    Q = sz[0] / p_in; cost = sz[0]; k = 0; last = t_in
    for t, r in (w if fill_minute_ladder else w.iloc[1:]).iterrows():
        while k < len(lv) and r.l <= p_in * (1 - lv[k]):
            px = p_in * (1 - lv[k]); Q += sz[k + 1] / p_in; cost += sz[k + 1] / p_in * px; k += 1; last = t
        avg = cost / Q
        tp = p_in * (1 + tp1 / 100) if k == 0 else avg * (1 + tpn / 100)
        if t > last and r.h >= tp:
            return dict(exit="цель", t_out=t, px_out=tp, adds=k, avg=avg, pct=(tp / avg - 1) * 100)
        if t - last >= pd.Timedelta(hours=timer_h):
            return dict(exit="таймер", t_out=t, px_out=r.c, adds=k, avg=avg, pct=(r.c / avg - 1) * 100)
    return dict(exit="открыта", t_out=w.index[-1], px_out=w.c.iloc[-1], adds=k, avg=cost / Q, pct=(w.c.iloc[-1] / (cost / Q) - 1) * 100)


def section_focus(D: pd.DataFrame, C: pd.DataFrame, J: pd.DataFrame, cache: dict):
    say("## 1. Наши условия входа вокруг его входа\n")
    say("Пороги: RSI7 3м ≤ 15, от хая 60 мин ≤ −3%×масштаб, ход 120 мин ≤ −2%×масштаб (масштаб — размах монеты к DOGE, 0.4…1).")
    say("«на закрытии» — значение на закрытии минуты (так считает бот); «по его цене» — если формирующаяся 3-минутка стоит на цене его ордера.\n")
    rows, near, ctx, after = [], [], [], []
    btc = cache["BTCUSDT"]; eth = cache["ETHUSDT"]
    bask = {s: cache[s] for s in BASKET if s in cache}
    for sym, ts, what in FOCUS:
        t = pd.Timestamp(ts); m = t.floor("min")
        F = cache[sym]
        sc = float(scale_for(sym, pd.DatetimeIndex([m]))[0])
        price = float(D[(D.sym == sym) & (D.t_open.dt.floor("min") == m)].order_price.iloc[0])
        prev = F.loc[m - MIN]; cur = F.loc[m]
        r_live = float(rsi_live(F.loc[[m]], price)[0])
        fh_live = (price / F.H_pre[m] - 1) * 100; c120_live = (price / F.c120[m] - 1) * 100
        trig = trigger_price(F.loc[m - 30 * MIN: m + 30 * MIN], sc)
        tw = F.loc[m - 30 * MIN: m + 30 * MIN]
        hit = tw.index[tw.l.values <= trig]
        first_hit = hit[hit >= m - 30 * MIN][0] if len(hit) else None
        rows.append({"монета": sym.replace("USDT", ""), "его вход": f"{t:%d.%m %H:%M:%S}", "цена": price, "масштаб": round(sc, 2),
                     "RSI зак. пред.": prev.r3, "RSI зак.": cur.r3, "RSI по его цене": r_live,
                     "от хая зак.": cur.from_hi, "от хая по цене": fh_live, "порог хая": -3 * sc,
                     "ход120 зак.": cur.ch120, "ход120 по цене": c120_live, "порог хода": -2 * sc,
                     "срабатывание внутри минуты": (f"{first_hit:%H:%M} @{min(F.o[first_hit], trig[tw.index.get_loc(first_hit)]):.6g}" if first_hit is not None else "нет ±30 мин"),
                     "что": what})
        nm = nearest_miss(F, sc, m)
        js = J[(J.get("sym") == sym) & (J.kind == "сигнал") & (J.t >= m - 120 * MIN) & (J.t <= m + 120 * MIN)]
        jin = J[(J.get("sym") == sym) & (J.kind.isin(["вход"])) & (J.t >= m - 24 * 60 * MIN) & (J.t <= m + 24 * 60 * MIN)]
        near.append({"монета": sym.replace("USDT", ""), "его вход": f"{t:%d.%m %H:%M}", "ближе всего": f"{nm['t']:%H:%M}",
                     "RSI": nm["r3"], "от хая": nm["from_hi"], "ход120": nm["ch120"], "не прошло": ", ".join(nm["fail"]) or "всё прошло",
                     "недобор, доля порога": max(nm["worst"], 0),
                     "наш журнал ±2 ч": (", ".join(sorted({f"{x:%H:%M}" for x in js.t})) or "сигналов нет"),
                     "наши входы ±24 ч": ", ".join(f"{r.t:%d.%m %H:%M} ({r.src})" for r in jin.itertuples()) or "—"})
        # контекст
        B = btc.loc[m - MIN]; E = eth.loc[m - MIN]
        b30 = np.nanmean([bask[s].ch30.get(m - MIN, np.nan) for s in bask]); b60 = np.nanmean([bask[s].ch60.get(m - MIN, np.nan) for s in bask])
        br = int(np.nansum([bask[s].ch15.get(m - MIN, np.nan) <= -1.5 for s in bask]))
        fr = funding(sym, m - pd.Timedelta(days=3), m + pd.Timedelta(hours=1))
        fr_last = float(fr[fr.index <= m].iloc[-1]) * 100 if len(fr[fr.index <= m]) else np.nan
        oi = cache.get(("oi", sym), pd.Series(dtype=float))
        def oich(hh):
            if oi.empty:
                return np.nan
            a_ = oi[oi.index <= m - pd.Timedelta(hours=hh)]; b_ = oi[oi.index <= m]
            return (b_.iloc[-1] / a_.iloc[-1] - 1) * 100 if len(a_) and len(b_) else np.nan
        btc_lo12 = bool(btc.c[m - MIN] <= btc.l.shift(30).rolling(720).min()[m - MIN])
        ctx.append({"монета": sym.replace("USDT", ""), "время": f"{t:%a %H:%M}", "ход монеты 1/15/60/240 мин": f"{cur.ch1:+.1f}/{cur.ch15:+.1f}/{cur.ch60:+.1f}/{cur.ch240:+.1f}",
                    "от хая суток": cur.from_hi1440, "мин 6ч/24ч/7д (<0 пробит)": f"{cur.vs_lo6h:+.1f}/{cur.vs_lo24h:+.1f}/{cur.vs_lo168h:+.1f}",
                    "объём минуты ×": cur.vol_x, "объём часа ×": cur.vol60_x,
                    "BTC 30/60/240": f"{B.ch30:+.2f}/{B.ch60:+.2f}/{B.ch240:+.2f}", "BTC мин 6ч/24ч": f"{B.vs_lo6h:+.1f}/{B.vs_lo24h:+.1f}",
                    "BTC фильтр 12ч": "ниже" if btc_lo12 else "нет", "ETH 60": E.ch60,
                    "корзина 30/60": f"{b30:+.2f}/{b60:+.2f}", "упало ≥1.5% за 15м": f"{br}/{len(bask)}",
                    "фандинг %": fr_last, "ОИ 1ч/4ч/24ч %": f"{oich(1):+.1f}/{oich(4):+.1f}/{oich(24):+.1f}"})
        # после входа
        c_ = C[(C.sym == sym) & (C.orders.map(lambda o: any(abs((x[0] - t).total_seconds()) < 60 for x in o)))].iloc[0]
        t_end = c_.t1 if pd.notna(c_.t1) else END
        w = F.loc[m: t_end]
        mae = (w.l.min() / price - 1) * 100; mfe = (w.h.max() / price - 1) * 100
        hit194 = w.index[w.h >= price * 1.0194]
        nx = F.loc[t_end: t_end + pd.Timedelta(hours=6)]
        ours = campaign_sim(F, m, price, sc)
        exit_ord = ", ".join(f"{x[0]:%d.%m %H:%M} @{x[1]:.6g}×{x[2]:g}" for x in c_.orders)
        after.append({"монета": sym.replace("USDT", ""), "вход": f"{t:%d.%m %H:%M}", "ордера (время @цена×кол-во)": exit_ord,
                      "средняя": c_.avg, "выход": (f"{c_.t1_first:%d.%m %H:%M}" + (f"–{c_.t1:%H:%M}" if c_.t1 != c_.t1_first else "")) if pd.notna(c_.t1) else "открыта",
                      "цена выхода": c_.px_out, "итог %": c_.res_pct, "держал ч": ((t_end - t).total_seconds() / 3600),
                      "просадка %": mae, "лучшая %": mfe,
                      "+1.94% достигнуто через ч": ((hit194[0] - m).total_seconds() / 3600) if len(hit194) else np.nan,
                      "макс. 6ч после выхода %": (nx.h.max() / c_.px_out - 1) * 100 if pd.notna(c_.px_out) else np.nan,
                      "наше правило с его входа": f"{ours['exit']} {ours['t_out']:%d.%m %H:%M} {ours['pct']:+.2f}% (докупок {ours['adds']})"})
    T1 = pd.DataFrame(rows); T2 = pd.DataFrame(near); T3 = pd.DataFrame(ctx); T4 = pd.DataFrame(after)
    say(md_table(T1)); say()
    say("**Ближайший «почти-сигнал»** в окне −60…+15 мин (наибольший недобор среди трёх условий, в долях порога):\n")
    say(md_table(T2)); say()
    say("## 2. Контекст входа\n")
    say(md_table(T3)); say()
    say("## 3. После входа\n")
    say(md_table(T4)); say()
    say("SOL: позиция 14.7 SOL (8.2 @118.83 + 6.5 @119.02). Первая покупка закрыта целиком по средней 121.14 — ниже минимума минуты "
        "последней продажи (04:49, 122.93), значит часть её продана раньше; от второй 1.9 SOL продано 04:54 по 123.41, 4.6 SOL держит "
        "(снимок 02.10 09:53). SUI: продажа 08:23 по 1.1522 = пик 08:22 (1.1597) минус 0.0075 — похоже на скользящий стоп.\n")
    return T1, T2, T3, T4


# ---------------------------------------------------------------- main

def build_cache(syms: list[str], a: pd.Timestamp, b: pd.Timestamp, with_oi=()) -> dict:
    cache = {}
    for s in syms:
        k = load1m(s, a, b)
        cache[s] = feats(k)
        print(f"  {s}: {len(k)} минуток {k.index.min()} … {k.index.max()}", flush=True)
    for s in with_oi:
        cache[("oi", s)] = oi_series(s, a + pd.Timedelta(days=20), b)
    return cache


def section_exits(D: pd.DataFrame, C: pd.DataFrame, cache: dict):
    """Как он выходит: все кампании за 4 мес. + наши варианты выхода с ЕГО входов (лесенка и таймер — наши)."""
    rows = []
    EX = {"наш 1.94/1.6, 12 ч": dict(), "3.9/3.0, 12 ч": dict(tp1=3.9, tpn=3.0), "3.9/3.0, 48 ч": dict(tp1=3.9, tpn=3.0, timer_h=48.0)}
    for c in C.itertuples():
        F = cache.get(c.sym)
        if F is None:
            continue
        m0 = c.t0.floor("min"); t1 = c.t1 if pd.notna(c.t1) else END
        w = F.loc[m0: t1.floor("min")]
        sc = float(scale_for(c.sym, pd.DatetimeIndex([m0]))[0])
        od = D[(D.sym == c.sym) & D.t_open.isin([o[0] for o in c.orders]) & D.t_close.notna()]
        cl = od.groupby(od.t_close.dt.floor("min")).apply(lambda g: (g["size"] * g.p_close).sum() / g["size"].sum(), include_groups=False)
        kinds = []                                                             # пик до продажи выше цены продажи → продажа после отката
        for t, px in cl.items():
            pk = F.h.loc[m0: t - MIN].max() if t > m0 else np.nan
            kinds.append("касание" if not pk >= px * 0.9995 else ("откат" if pk > px * 1.003 else "у пика"))
        kind = "/".join(f"{k} {kinds.count(k)}" for k in ("касание", "у пика", "откат") if kinds.count(k))
        sims = {k: campaign_sim(F, m0, c.p0, sc, **kw) for k, kw in EX.items()}
        hit = w.index[w.h >= c.avg * 1.0194]
        rows.append({"cid": c.cid, "монета": c.sym.replace("USDT", ""), "вход": f"{c.t0:%d.%m %H:%M}", "ордеров": c.n, "плечо": c.lev,
                     "$": round(c.usd), "итог %": c.res_pct, "частей выхода": len(cl), "как продал": kind,
                     "держал ч": (t1 - c.t0).total_seconds() / 3600, "просадка %": (w.l.min() / c.avg - 1) * 100,
                     "лучшая %": (w.h.max() / c.avg - 1) * 100, "до +1.94% ч": ((hit[0] - m0).total_seconds() / 3600) if len(hit) else np.nan,
                     **{f"{k} %": v["pct"] for k, v in sims.items()},
                     **{f"{k} ч": (v["t_out"] - m0).total_seconds() / 3600 for k, v in sims.items()},
                     "½+½ %": (sims["наш 1.94/1.6, 12 ч"]["pct"] + sims["3.9/3.0, 48 ч"]["pct"]) / 2,
                     "открыта": c.open_left > 0})
    X = pd.DataFrame(rows)
    say("## 3б. Как он выходит — все его кампании 08.06–02.10\n")
    say("«как продал» по частям выхода: «касание» — цена впервые дошла до уровня (лимитная цель), «у пика» — в пределах 0.3% от "
        "максимума после входа, «откат» — до продажи цена была выше больше чем на 0.3% (скользящий стоп или ручная продажа). "
        "Справа — что дали бы наши правила выхода с ЕГО первой цены (наша лесенка DOGE×масштаб, без комиссий).\n")
    show = X.drop(columns=[c for c in X.columns if c.endswith(" ч") and c != "держал ч" and c != "до +1.94% ч"] + ["cid"])
    say(md_table(show, 2)); say()
    Y = X[~X.открыта]
    summ = []
    for k in ["итог %"] + [f"{k} %" for k in EX] + ["½+½ %"]:
        v = Y[k]
        summ.append({"выход": "его фактический" if k == "итог %" else k.replace(" %", ""), "средний %": v.mean(), "медиана %": v.median(),
                     "в плюс %": (v > 0).mean() * 100, "худший %": v.min(),
                     "держал ч (медиана)": Y["держал ч"].median() if k == "итог %" else (Y[k.replace(" %", " ч")].median() if k.replace(" %", " ч") in Y else np.nan)})
    import re
    cnt = {"касание": 0, "у пика": 0, "откат": 0}
    for x in X[X.ордеров < 30]["как продал"]:
        for k, n in re.findall(r"(касание|у пика|откат) (\d+)", x):
            cnt[k] += int(n)
    say("Части выхода всех кампаний (кроме BCH-накопления 08.06, 41 ордер): " + ", ".join(f"{k} — {v}" for k, v in cnt.items()) + ".\n")
    say(f"Сводка по {len(Y)} закрытым кампаниям (одна кампания = одна позиция, без весов):\n")
    say(md_table(pd.DataFrame(summ), 2)); say()
    byc = Y.groupby("монета").agg(кампаний=("итог %", "size"), итог_медиана=("итог %", "median"), итог_мин=("итог %", "min"),
                                    итог_макс=("итог %", "max"), держал_ч=("держал ч", "median"), плечо=("плечо", "median"))
    say("По монетам (его фактический итог, % от средней цены):\n")
    say(md_table(byc.reset_index(), 2)); say()
    return X


# ---------------------------------------------------------------- раздел 4: вся история мастера

HIST_SYMS = ["BCHUSDT", "SUIUSDT", "1000PEPEUSDT", "ADAUSDT", "ZECUSDT", "SOLUSDT", "SHIB1000USDT", "DOGEUSDT", "XRPUSDT",
             "AVAXUSDT", "HYPEUSDT", "ETHUSDT"]


def history_table(D: pd.DataFrame, C: pd.DataFrame, cache: dict) -> pd.DataFrame:
    """Признаки на каждом ордере мастера: на закрытии предыдущей минуты и «по его цене» внутри минуты."""
    btc = cache["BTCUSDT"]
    rows = []
    first_ids = {(r.sym, o[0]) for r in C.itertuples() for o in r.orders[:1]}
    for r in D.itertuples():
        F = cache.get(r.sym)
        if F is None:
            continue
        m = r.t_open.floor("min")
        if m - MIN not in F.index or m not in F.index:
            continue
        p = F.loc[m - MIN]; P = r.order_price
        sc = float(scale_for(r.sym, pd.DatetimeIndex([m]))[0])
        trig = trigger_price(F.loc[[m]], sc)[0]
        cid = C[(C.sym == r.sym) & C.orders.map(lambda o: any(x[0] == r.t_open for x in o))]
        c_ = cid.iloc[0]
        rows.append(dict(sym=r.sym.replace("USDT", ""), t=r.t_open, price=P, first=(r.sym, r.t_open) in first_ids, cid=c_.cid,
                         below_avg=(P / c_.p0 - 1) * 100, scale=sc,
                         r3=p.r3, r3_live=float(rsi_live(F.loc[[m]], P)[0]), from_hi=p.from_hi, fh_live=(P / F.H_pre[m] - 1) * 100,
                         ch120=p.ch120, c120_live=(P / F.c120[m] - 1) * 100, ch1=F.ch1[m], ch15=p.ch15, ch60=p.ch60, ch1440=p.ch1440,
                         from_hi1440=p.from_hi1440, vs_lo24h=F.vs_lo24h[m], vs_lo6h=F.vs_lo6h[m], vol_x=F.vol_x[m],
                         trig=trig, trig_ok=bool(F.l[m] <= trig), m_low=F.l[m], m_high=F.h[m],
                         btc60=btc.ch60.get(m - MIN, np.nan), btc_lo6=btc.vs_lo6h.get(m - MIN, np.nan),
                         btc_lo24=btc.vs_lo24h.get(m - MIN, np.nan), hour=m.hour))
    return pd.DataFrame(rows)


def section_history(D, C, cache):
    H = history_table(D, C, cache)
    H.to_parquet(ROOT / "inbox/private/kamaz_master/history_feats.parquet")
    return H


def rule_hits(F: pd.DataFrame, sc: np.ndarray, name: str, bask: pd.DataFrame | None = None) -> np.ndarray:
    """Минуты, где правило срабатывает. Правила «зак.» — на закрытии минуты (вход со следующей), «внутри» — в самой минуте."""
    c, l = F.c.values, F.l.values
    if name == "A зак. (наш)":
        return (F.r3.values <= 15) & (F.from_hi.values <= -3 * sc) & (F.ch120.values <= -2 * sc)
    if name == "A зак., RSI ≤ 20":
        return (F.r3.values <= 20) & (F.from_hi.values <= -3 * sc) & (F.ch120.values <= -2 * sc)
    if name == "B внутри (те же пороги)":
        return l <= trigger_price(F, sc)
    if name == "B внутри, RSI ≤ 20":
        return l <= trigger_price(F, sc, rsi_thr=20)
    if name == "B внутри, мягче (RSI 15, хай −2, ход −1.5)":
        return l <= trigger_price(F, sc, drop1h=2.0, drop2h=1.5)
    if name == "B внутри, RSI ≤ 20, хай −2, ход −1.5":
        return l <= trigger_price(F, sc, rsi_thr=20, drop1h=2.0, drop2h=1.5)
    if name == "минутный пролив ≥1% + RSI внутри ≤ 20":
        return (l / F.c.shift(1).values - 1 <= -0.01) & (rsi_live(F, l) <= 20)
    if name == "пробит минимум суток + RSI внутри ≤ 20":
        lo24 = F.l.shift(1).rolling(1440, min_periods=720).min().values
        return (l < lo24) & (rsi_live(F, np.minimum(l, lo24)) <= 20)
    if name == "корзина −2% за 30 и 60 мин" and bask is not None:
        b = bask.reindex(F.index)
        return ((b.b30 <= -2) & (b.b60 <= -2)).values
    raise KeyError(name)


RULES = ["A зак. (наш)", "A зак., RSI ≤ 20", "B внутри (те же пороги)", "B внутри, RSI ≤ 20",
         "B внутри, мягче (RSI 15, хай −2, ход −1.5)", "B внутри, RSI ≤ 20, хай −2, ход −1.5",
         "минутный пролив ≥1% + RSI внутри ≤ 20", "пробит минимум суток + RSI внутри ≤ 20", "корзина −2% за 30 и 60 мин"]


def onsets(t: pd.DatetimeIndex, quiet=60) -> pd.DatetimeIndex:
    keep, last = [], None
    for x in t:
        if last is None or (x - last) > pd.Timedelta(minutes=quiet):
            keep.append(x)
        last = x
    return pd.DatetimeIndex(keep)


def eval_rule(D, C, cache, hitfun, close_based: bool, mid=pd.Timestamp("2026-08-16")):
    """Полнота по его кампаниям и лишние сигналы для одного правила. hitfun(F, sc, sym) → bool-массив по минутам F."""
    A0 = C.t0.min().floor("D")
    starts = C[C.sym.isin(HIST_SYMS)]
    allord = D[D.sym.isin(HIST_SYMS)][["sym", "t_open"]]
    busy = {s: [(r.t0, r.t1 if pd.notna(r.t1) else END) for r in C[C.sym == s].itertuples()] for s in HIST_SYMS}
    out = dict(rec1=0, n1=0, rec2=0, n2=0, on1=0, m1=0, on2=0, m2=0, lead=[], per={})
    for s in HIST_SYMS:
        F = cache[s].loc[A0 - pd.Timedelta(days=1): END]
        sc = SCALES[s].reindex(F.index).values if s in SCALES else scale_for(s, F.index)
        ht = F.index[np.nan_to_num(hitfun(F, sc, s)).astype(bool)]
        if close_based:
            ht = ht + MIN                      # вход возможен со следующей минуты
        hs = pd.Series(1, index=ht)
        for r in starts[starts.sym == s].itertuples():
            M = r.t0.floor("min")
            w = hs.loc[M - 15 * MIN: M + 1 * MIN] if len(hs) else hs
            h = "1" if r.t0 < mid else "2"
            out["n" + h] += 1; out["rec" + h] += len(w) > 0
            out["per"][r.cid] = (w.index[0] - M).total_seconds() / 60 if len(w) else np.nan
            if len(w):
                out["lead"].append((w.index[0] - M).total_seconds() / 60)
        mo = allord[allord.sym == s].t_open
        for x in onsets(ht[ht >= A0]):
            if any(a <= x <= b for a, b in busy[s]):
                continue
            h = "1" if x < mid else "2"
            out["on" + h] += 1; out["m" + h] += bool(((mo >= x - 30 * MIN) & (mo <= x + 30 * MIN)).any())
    return out


SCALES: dict = {}


def rule_row(name, o, weeks):
    rec, n = o["rec1"] + o["rec2"], o["n1"] + o["n2"]; on, m = o["on1"] + o["on2"], o["m1"] + o["m2"]
    return {"правило": name, "ловит его входов": f"{rec}/{n} ({rec / n * 100:.0f}%)", "до 16.08": f"{o['rec1']}/{o['n1']}",
            "после 16.08": f"{o['rec2']}/{o['n2']}", "раньше него, мин (медиана)": -np.median(o["lead"]) if o["lead"] else np.nan,
            "сигналов в свободное время": on, "рядом его ордер (±30 мин)": m, "точность %": m / on * 100 if on else np.nan,
            "лишних в неделю": (on - m) / weeks}


def f1(rec, n, m, on):
    r = rec / n if n else 0; p = m / on if on else 0
    return 2 * r * p / (r + p) if r + p else 0


def section_rules(D, C, cache, bask):
    """Полнота (сколько его входов ловит правило) и лишние сигналы — на всей истории мастера (его 12 монет, 06–10.2026)."""
    A0 = C.t0.min().floor("D")
    for s in HIST_SYMS:
        F = cache[s].loc[A0 - pd.Timedelta(days=1): END]
        SCALES[s] = pd.Series(scale_for(s, F.index), F.index)
    weeks = (END - A0).days / 7
    rows, PE, objs = [], {}, {}
    for rn in RULES:
        o = eval_rule(D, C, cache, lambda F, sc, s, rn=rn: rule_hits(F, sc, rn, bask), "зак." in rn or "корзина" in rn)
        rows.append(rule_row(rn, o, weeks)); PE[rn] = o["per"]; objs[rn] = o
    R = pd.DataFrame(rows)
    n_st = int(C.sym.isin(HIST_SYMS).sum())
    say("## 4. Альтернативные условия входа на всей истории мастера\n")
    say(f"Его кампаний (первый ордер) на 12 монетах {A0:%d.%m}–{END:%d.%m}: {n_st}. «Ловит» — правило сработало за 15 мин до его ордера "
        "или в ту же минуту (правила «зак.» — с учётом входа на следующей минуте). «Лишние» — начала сигналов (после часа тишины) "
        "в монетах, где у него в этот момент нет открытой позиции, без его ордера в ±30 мин.\n")
    say(md_table(R, 1)); say()
    # небольшой перебор порогов «внутри минуты»: подбор по F1 до 16.08, проверка после
    grid = []
    for rt in (10, 15, 20):
        for dh in (0.0, 1.5, 2.0, 3.0):
            for d2 in (0.0, 1.0, 1.5, 2.0):
                o = eval_rule(D, C, cache, lambda F, sc, s, rt=rt, dh=dh, d2=d2: F.l.values <= trigger_price(F, sc, rsi_thr=rt, drop1h=dh, drop2h=d2), False)
                grid.append(dict(RSI=rt, хай=-dh, ход=-d2, F1_до=f1(o["rec1"], o["n1"], o["m1"], o["on1"]), F1_после=f1(o["rec2"], o["n2"], o["m2"], o["on2"]),
                                 **{k: v for k, v in rule_row("", o, weeks).items() if k != "правило"}))
    G = pd.DataFrame(grid).sort_values("F1_до", ascending=False)
    say("Перебор порогов «внутри минуты» (48 вариантов): лучшие по F1 на 08.06–15.08 и как они же на 16.08–02.10 "
        f"(для сравнения: A зак. — F1 {f1(*[objs[RULES[0]][k] for k in ('rec1', 'n1', 'm1', 'on1')]):.2f} / "
        f"{f1(*[objs[RULES[0]][k] for k in ('rec2', 'n2', 'm2', 'on2')]):.2f}; B внутри с нашими порогами — "
        f"{f1(*[objs[RULES[2]][k] for k in ('rec1', 'n1', 'm1', 'on1')]):.2f} / {f1(*[objs[RULES[2]][k] for k in ('rec2', 'n2', 'm2', 'on2')]):.2f}).\n")
    say(md_table(G.head(8), 2)); say()
    PEd = pd.DataFrame(PE); PEd.index.name = "cid"
    return R, G, PEd


TEN = ["BTCUSDT", "DOGEUSDT", "ENAUSDT", "ETHUSDT", "HYPEUSDT", "NEARUSDT", "SOLUSDT", "SUIUSDT", "XRPUSDT", "ZECUSDT"]


def week_ab(cache: dict, a=pd.Timestamp("2026-09-27 18:00")):
    """Наша десятка с 27.09 18:00 (старт бота): кампании A (как бот) и B (вход внутри минуты), выход наш (1.94/1.6, 12 ч)."""
    rows = []
    for s in TEN:
        F = cache[s] if s in cache else feats(load1m(s, pd.Timestamp("2026-09-20"), END))
        F = F.loc[pd.Timestamp("2026-09-24"):]
        sc = scale_for(s, F.index)
        sigA = (F.r3.values <= 15) & (F.from_hi.values <= -3 * sc) & (F.ch120.values <= -2 * sc) & (F.ch1440.values > -25 * sc)
        trig = trigger_price(F, sc)
        hitB = (F.l.values <= trig) & (F.ch1440.shift(1).values > -25 * sc)
        for name in ("A", "B"):
            t = a
            while True:
                idx = F.index[(F.index >= t)]
                if name == "A":
                    cand = idx[sigA[F.index.get_indexer(idx)]]
                    if not len(cand) or cand[0] + MIN not in F.index:
                        break
                    m = cand[0] + MIN; px = F.o[m]
                else:
                    cand = idx[hitB[F.index.get_indexer(idx)]]
                    if not len(cand):
                        break
                    m = cand[0]; px = min(F.o[m], trig[F.index.get_loc(m)])
                r = campaign_sim(F, m, px, float(sc[F.index.get_loc(m)]))
                rows.append(dict(правило=name, монета=s.replace("USDT", ""), вход=m, цена=px, выход=r["t_out"], как=r["exit"], докупок=r["adds"], итог=r["pct"]))
                t = r["t_out"] + MIN
                if r["exit"] == "открыта":
                    break
    W = pd.DataFrame(rows)
    A_ = W[W.правило == "A"]; B_ = W[W.правило == "B"]
    def twin(r, other):
        o = other[(other.монета == r.монета) & ((other.вход - r.вход).abs() <= pd.Timedelta(minutes=45))]
        return f"{o.вход.iloc[0]:%d.%m %H:%M}" if len(o) else "—"
    B_ = B_.assign(**{"у A": [twin(r, A_) for r in B_.itertuples()]})
    onlyA = A_[[twin(r, B_) == "—" for r in A_.itertuples()]]
    show = B_.assign(вход=B_.вход.dt.strftime("%d.%m %H:%M"), выход=B_.выход.dt.strftime("%d.%m %H:%M")).drop(columns="правило")
    say("### 4б. Неделя бота (27.09 18:00 – 02.10), наша десятка: кампании B и есть ли такая же у A (±45 мин)\n")
    say("Выход у обоих — наш (1.94/1.6, таймер 12 ч), без комиссий, по ячейке, без ограничения числа монет.\n")
    say(md_table(show.reset_index(drop=True), 4)); say()
    say(f"Итого: A — {len(A_)} кампаний, сумма {A_.итог.sum():+.2f}% (на ячейку); B — {len(B_)} кампаний, сумма {B_.итог.sum():+.2f}%. "
        f"Только у A (B вошёл раньше или иначе): {', '.join(f'{r.монета} {r.вход:%d.%m %H:%M}' for r in onlyA.itertuples()) or 'нет'}.\n")
    return W


# ---------------------------------------------------------------- раздел 5: бэктест на ежедневной десятке

BASE = dict(btc_filter=False, L=1.0, cap_to_equity=True, crash_skip=25.0)       # рабочий вариант бота
LONG = dict(tp1=3.9, tpn=3.0)                                                  # «держать дольше»: цель как у него на SUI/XRP
N_PLACEBO = 5


REAL = dict(through=0.1, maker=0.0004, taker=0.0011)                          # лимитки только при проходе 0.1%, комиссии ×2


def bt_variants(kind: str = "main"):
    V = {}
    if kind == "main":
        for e in ("A", "B"):
            V[f"{e}"] = (e, dict(BASE))
            V[f"{e}, цель 3.9/3.0"] = (e, dict(BASE, **LONG))
            V[f"{e}, цель 3.9/3.0, таймер 48 ч"] = (e, dict(BASE, **LONG, timer_h=48.0))
            V[f"{e} ½ обычная"] = (e, dict(BASE, L=0.5))                        # две половины = выход частями
            V[f"{e} ½ цель 3.9/3.0"] = (e, dict(BASE, L=0.5, **LONG))
            for sd in range(1, N_PLACEBO + 1):
                V[f"{e} наугад #{sd}"] = (e, dict(BASE, random_entry=True, random_onsets=True, seed=sd))
    else:                                                                       # проверки на прочность
        for e in ("A", "B"):
            V[f"{e}, цель 3.0/2.5"] = (e, dict(BASE, tp1=3.0, tpn=2.5))
            V[f"{e}, цель 4.5/3.5"] = (e, dict(BASE, tp1=4.5, tpn=3.5))
            V[f"{e}, исполнение хуже (проход 0.1%, комиссии ×2)"] = (e + ("thr" if e == "B" else ""), dict(BASE, **REAL))
            V[f"{e}, цель 3.9/3.0, исполнение хуже"] = (e + ("thr" if e == "B" else ""), dict(BASE, **LONG, **REAL))
            for sd in range(1, N_PLACEBO + 1):
                V[f"{e} наугад, цель 3.9/3.0 #{sd}"] = (e, dict(BASE, **LONG, random_entry=True, random_onsets=True, seed=sd))
    return V


def bt_task(args):
    import grid_universe as gu
    sym, a, b, kind = args
    G = gu.glob_data()
    A_ = a; B_ = min(b + pd.Timedelta(days=3), gu.END)
    k = gu.load_1m(sym, A_ - pd.Timedelta(days=2), B_)
    if k.empty or len(k) < 3000:
        return dict(sym=sym, skipped=True)
    day_ok = G["U"][sym].reindex(k.index.floor("D")).fillna(0).values.astype(bool)
    sel = (k.index >= A_) & (k.index <= B_)
    scale_full = ge.daily_scale(sym, k.index, G["DOGE_RNG"])
    F = feats(k)
    trig = trigger_price(F, scale_full)
    crash_ok = np.nan_to_num(F.ch1440.values, nan=0.0) > -25.0 * scale_full
    t_s, ok_s, al = trig[sel], crash_ok[sel], day_ok[sel]
    l_s = k.l.values[sel]
    coins = {"A": gu.SegCoin(sym, k, A_, B_, day_ok[sel])}
    for name, thr in (("B", 0.0), ("Bthr", 0.001)):
        hit = l_s <= t_s * (1 - thr)
        sig = np.zeros(sel.sum(), bool)
        sig[:-1] = hit[1:] & al[1:] & ok_s[:-1]                                  # срабатывание в минуте i → «сигнал» на i−1
        c_ = gu.SegCoin(sym, k, A_, B_, day_ok[sel])
        kB = c_.k.copy()
        kB["o"] = np.where(np.r_[False, sig[:-1]], np.minimum(kB.o.values, t_s), kB.o.values)   # вход по цене срабатывания
        c_.k = kB
        c_.signal = (lambda p, _s=sig: _s)
        coins[name] = c_
        if name == "B":
            cr = gu.SegCoin(sym, k, A_, B_, day_ok[sel])                          # наугад — вход по открытию минуты, частота начал B
            cr.signal = (lambda p, _s=sig: np.random.default_rng(p.seed).random(len(_s)) < (_s & ~np.r_[False, _s[:-1]]).mean())
            coins["Brand"] = cr
    scale = scale_full[sel]
    res = {}
    for vn, (e, kw) in bt_variants(kind).items():
        coin = coins["Brand"] if (e == "B" and kw.get("random_entry")) else coins[e]
        E, Cm, liq, tr = ge.run(coin, ge.P(side=1, scale=scale, **kw))
        Cm["монета"] = sym.replace("USDT", "")
        d = E.resample("D").last().dropna()
        res[vn] = dict(days=d.diff().fillna(d.iloc[0] - 10_000.0), camps=Cm, liq=liq)
    return dict(sym=sym, skipped=False, res=res)


def section_backtest(kind: str = "main"):
    import grid_universe as gu
    segs = gu.segments()
    t0 = time.time()
    out = []
    with mp.get_context("spawn").Pool(6) as pool:
        for j, r in enumerate(pool.imap_unordered(bt_task, [(*x, kind) for x in segs], chunksize=1), 1):
            out.append(r)
            if j % 40 == 0:
                print(f"  бэктест: {j}/{len(segs)} за {time.time() - t0:.0f} с", flush=True)
    ok = [r for r in out if not r["skipped"]]
    V = bt_variants(kind)
    eqs, camps, liqs = {}, {}, {}
    for vn in V:
        P = pd.concat([r["res"][vn]["days"] for r in ok], axis=1).sum(axis=1).sort_index()
        eqs[vn] = P
        camps[vn] = pd.concat([r["res"][vn]["camps"] for r in ok if len(r["res"][vn]["camps"])])
        liqs[vn] = sum(r["res"][vn]["liq"] is not None for r in ok)
    for e in (("A", "B") if kind == "main" else ()):                             # выход частями = сумма двух половин
        eqs[f"{e}, половина на 1.94/1.6 + половина на 3.9/3.0"] = eqs[f"{e} ½ обычная"].add(eqs[f"{e} ½ цель 3.9/3.0"], fill_value=0)
        camps[f"{e}, половина на 1.94/1.6 + половина на 3.9/3.0"] = pd.concat([camps[f"{e} ½ обычная"], camps[f"{e} ½ цель 3.9/3.0"]])
        liqs[f"{e}, половина на 1.94/1.6 + половина на 3.9/3.0"] = liqs[f"{e} ½ обычная"] + liqs[f"{e} ½ цель 3.9/3.0"]
    rows = []
    for vn, P in eqs.items():
        if "½" in vn:
            continue
        eq = 100_000 + P.cumsum()
        Cm = camps[vn].copy(); Cm["t1"] = pd.to_datetime(Cm.t1)
        yr = {}
        for y in range(2021, 2027):
            e_ = eq[eq.index.year == y]
            if not len(e_):
                continue
            st = eq[eq.index < e_.index[0]].iloc[-1] if (eq.index < e_.index[0]).any() else 100_000
            yr[str(y)] = (e_.iloc[-1] - st) / 1000
        last30 = Cm[Cm.t1 >= pd.Timestamp("2026-06-08")]
        rows.append({"вариант": vn, "итог $k": (eq.iloc[-1] - 100_000) / 1000, "просадка %": (eq / eq.cummax() - 1).min() * 100,
                     **{f"{y} %": v for y, v in yr.items()}, "сделок": len(Cm), "в плюс %": (Cm.pnl > 0).mean() * 100,
                     "на сделку $": Cm.pnl.mean(), "худшая $": Cm.pnl.min(), "ликвид.": liqs[vn],
                     "06–09.2026 $k": last30.pnl.sum() / 1000})
    T = pd.DataFrame(rows)
    T["группа"] = T.вариант.str.replace(r" #\d+$", "", regex=True)
    main = T[~T.вариант.str.contains("наугад")].drop(columns="группа").copy()
    main["разброс $k"] = ""
    for g, G_ in T[T.вариант.str.contains("наугад")].groupby("группа", sort=False):
        row = G_.drop(columns=["вариант", "группа"]).mean(numeric_only=True).to_dict()
        row.update({"вариант": f"{g} ({len(G_)} прогонов, среднее)", "худшая $": G_["худшая $"].min(), "ликвид.": int(G_["ликвид."].sum()),
                    "разброс $k": f"{G_['итог $k'].min():.1f}…{G_['итог $k'].max():.1f}"})
        main = pd.concat([main, pd.DataFrame([row])], ignore_index=True)
    if kind == "main":
        say("## 5. Бэктест: ежедневная десятка, 07.2021–27.09.2026, $100k (10 ячеек), x1, без фильтра, пропуск −25%/сутки\n")
        say("A — рабочий вариант (сигнал на закрытии минуты, вход по открытию следующей). B — те же пороги, но RSI по формирующейся "
            "3-минутке: вход в той же минуте по цене срабатывания (как лимитка, переставляемая раз в минуту), комиссия как у рыночной. "
            "«наугад» — вход в случайные минуты с той же частотой начал сигналов (у B — частота B). Выход частями = две половинные "
            "подсистемы (одна с обычной целью, другая с 3.9/3.0), сумма. Годы — % от $100k; «06–09.2026» — сделки с 08.06 (период мастера).\n")
    else:
        say("### 5б. Проверки на прочность\n")
        say("Соседние цели (3.0/2.5 и 4.5/3.5 вокруг его 3.9), хуже исполнение (лимитки — и вход B — только при проходе цены на 0.1%, "
            "комиссии ×2), плацебо для длинной цели (вход наугад + цель 3.9/3.0: не даёт ли прибавку сам выход на любом входе).\n")
    say(md_table(main, 1)); say()
    print(f"бэктест {time.time() - t0:.0f} с", flush=True)
    if kind == "main":
        for vn in ("A", "B", "A, цель 3.9/3.0", "B, цель 3.9/3.0"):
            camps[vn].to_parquet(ROOT / f"inbox/private/kamaz_master/bt_camps_{vn.replace(', ', '_').replace('/', '-').replace(' ', '_')}.parquet")
    return main, camps


if __name__ == "__main__":
    pd.set_option("display.width", 250)
    t0 = time.time()
    D = master_orders(); C = campaigns(D); J = our_journal()
    say("# Мастер Kamaz: входы, которые наш бот не взял (28.09–02.10.2026)\n")
    say(f"*Скрипт `experiments/kamaz_missed_entries.py`, данные до {END:%d.%m.%Y %H:%M} UTC. Время — UTC.*\n")
    say(SUMMARY)
    focus_syms = sorted({f[0] for f in FOCUS})
    syms = sorted(set(focus_syms + HIST_SYMS + ["BTCUSDT", "ETHUSDT"] + BASKET))
    cache = build_cache(syms, pd.Timestamp("2026-05-25"), END, with_oi=focus_syms)
    section_focus(D, C, J, cache)
    section_exits(D, C, cache)
    bask = pd.DataFrame(dict(b30=pd.concat([cache[s].ch30 for s in BASKET], axis=1).mean(axis=1),
                             b60=pd.concat([cache[s].ch60 for s in BASKET], axis=1).mean(axis=1)))
    section_rules(D, C, cache, bask)
    week_ab(cache)
    if "--skip-bt" not in sys.argv:
        section_backtest("main")
        section_backtest("checks")
    OUT.write_text("\n".join(LINES) + "\n")
    print(f"готово за {time.time() - t0:.0f} с → {OUT}")


