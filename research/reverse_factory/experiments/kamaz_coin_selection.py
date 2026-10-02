"""Kamaz (ITEKCrypto Kamaz, копитрейдинг Bybit): по какому принципу мастер выбирает монеты (02.10.2026).

Вопрос Вадима: если отбор монет автоматический — взять параметры монет и сравнить «его» и «не его».

Источники сделок мастера:
  P0 — посты ITEK в TG «Камаз покатал …» (июль–август 2024), только список монет;
  P1 — личная копия Вадима 12.11.2024–16.01.2025 (inbox/private/work/itekcrypto-kamaz/campaigns.parquet, 198 кампаний);
  P2 — накопитель inbox/private/kamaz_master/closed.jsonl (витрина Bybit 08.06–01.10.2026, 125 ордеров),
       запасной вариант — inbox/top-itekcrypto-kamaz.bybit.txt.
Кандидаты: все USDT-бессрочные Bybit (живые + снятые), признаки на день D только по данным ДО D (свеча D−1 и раньше).

Стадии:  fetch   — докачать дневные свечи до 01.10, инструменты (linear+spot), ОИ и финансирование по пулу,
                   15-мин свечи по пулу на окна P1 и P2 (кэш data/kamaz_sel/);
         analyze — признаки, сравнение, правила, дерево, разбор моментов входа → KAMAZ_COINS.md.
Запуск: .venv/bin/python experiments/kamaz_coin_selection.py fetch analyze bt bt2 report
         (bt/bt2 — бэктест вариантов отбора на grid_universe: UNIV_SET=one + UNIV_FILE; report склеивает KAMAZ_COINS.md
          из data/kamaz_sel/verdict.md — вывод, написан руками — и сгенерированных разделов)
Без процессов (только потоки для сети): fork-пул после чтения parquet виснет.
"""
from __future__ import annotations

import concurrent.futures as cf
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from factory import market  # noqa: E402
from factory.funding import funding  # noqa: E402

DATA = ROOT / "data"
W = DATA / "kamaz_sel"
PRIV = ROOT / "inbox/private"
MD = ROOT / "KAMAZ_COINS.md"
LAST_DAY = pd.Timestamp("2026-10-01")          # последний день с признаками (свеча 30.09 закрыта)

PERIODS = {
    "P0": ("2024-07-10", "2024-08-31", "посты ITEK в TG, июль–август 2024"),
    "P1": ("2024-11-12", "2025-01-16", "копия Вадима, 12.11.2024–16.01.2025"),
    "P2": ("2026-06-08", "2026-10-01", "витрина + накопитель, 08.06–01.10.2026"),
}
EVT_WIN = {"P1": ("2024-11-09", "2025-01-18"), "P2": ("2026-06-05", "2026-10-02 12:00")}

# «Камаз покатал …» — посты канала ITEK ALGO (inbox/private/itek_tg/itek_algo.json), переписано руками.
# 06.08 «вынесло на БСН и битке» — БСН = BCH (кириллицей).
TG_P0 = {
    "2024-07-17": "ENA OP WLD FTM BCH BTC 1000FLOKI SHIB1000 1000PEPE XRP NOT",
    "2024-07-19": "BTC 1000PEPE SOL SHIB1000 1000FLOKI XRP",
    "2024-07-21": "DOGE UMA ENA 1000PEPE NOT 1000FLOKI",
    "2024-07-26": "1000PEPE BTC OP SOL AVAX",
    "2024-08-02": "1000PEPE SOL BTC OP",
    "2024-08-06": "BCH BTC",
    "2024-08-08": "XRP 1000PEPE UMA SOL NOT 1000FLOKI",
    "2024-08-12": "1000PEPE SHIB1000 SOL TON ENA DOGE OP FTM",
    "2024-08-17": "OP SOL 1000PEPE SHIB1000 NOT DOGE",
    "2024-08-30": "AVAX WLD ENA",
}

# не крипта (как в experiments/universe.py) + типы инструментов Bybit
NON_CRYPTO = {"XAU", "XAUT", "PAXG", "XAG", "XPT", "SOXL", "SNDK", "CSCO", "TSLA", "NVDA", "AAPL", "AMZN", "GOOGL", "GOOG", "META", "MSFT",
              "MSTR", "COIN", "HOOD", "SPY", "QQQ", "TQQQ", "SQQQ", "NFLX", "AMD", "INTC", "PLTR", "CRCL", "BABA", "ORCL", "AVGO", "MU", "SMCI",
              "USDC", "USDE", "FDUSD", "TUSD", "DAI", "USD1", "RLUSD", "EUR", "GBP", "JPY"}
MEME = {"DOGE", "1000PEPE", "SHIB1000", "1000BONK", "1000FLOKI", "WIF", "POPCAT", "MEW", "BOME", "BRETT", "10000SATS", "1000RATS", "NOT",
        "TRUMP", "MOODENG", "GOAT", "PNUT", "FARTCOIN", "NEIROETH", "1000NEIRO", "PENGU", "SPX", "1000000MOG", "1000TURBO", "MEME", "PEOPLE",
        "1000CAT", "DOGS", "CHILLGUY", "ACT", "1000000BABYDOGE", "1000000CHEEMS", "10000ELON", "MYRO", "10000WEN", "SLERF", "MANEKI", "DEGEN",
        "PUMPFUN", "USELESS", "1000000PEIPEI", "10000COQ", "10000LADYS", "1000BTT", "LUNC", "1000LUNC", "BANANAS31", "MELANIA", "WLFI",
        "FWOG", "GIGA", "MICHI", "PONKE", "SUNDOG", "HIPPO", "BAN", "LAUNCHCOIN", "TOSHI", "SKYAI", "TUT", "BROCCOLI", "1000000BOB", "ALCH",
        "HMSTR", "CATI", "MAJOR", "1000X", "MUBARAK", "1000CHEEMS", "AIDOGE", "10000000AIDOGE", "1000000VINU", "DOGE2", "PIPPIN", "ZEREBRO",
        "AI16Z", "GRIFFAIN", "ARC", "SWARMS", "VINE", "JELLYJELLY", "KOMA", "BABYDOGE", "PUFFER", "RFC", "PUMPBTC", "AKE", "LAB", "BANK",
        "SIREN", "RAVE", "ASTER", "XPL", "TROLL", "1000TOSHI", "WOJAK", "SPELL", "LADYS"}
UA = {"User-Agent": "Mozilla/5.0"}
GAP_MIN = 15          # старты кампаний ближе 15 мин друг к другу = один момент («пачка»)


def base(sym: str) -> str:
    return sym[:-4] if sym.endswith("USDT") else sym


def nm(sym: str) -> str:
    """Короткое имя для таблиц: 1000PEPEUSDT → PEPE, SHIB1000USDT → SHIB."""
    b = base(sym)
    for p in ("10000000", "1000000", "10000", "1000"):
        if b.startswith(p) and len(b) > len(p):
            return b[len(p):]
    return b.replace("SHIB1000", "SHIB")


def spot_base(sym: str) -> str:
    b = base(sym)
    if b == "SHIB1000":
        return "SHIB"
    for p in ("10000000", "1000000", "10000", "1000"):
        if b.startswith(p) and len(b) > len(p):
            return b[len(p):]
    return b


# ───────────────────────── сеть ─────────────────────────

def get(url: str):
    for a in range(6):
        try:
            r = json.load(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30))
            if r.get("retCode") == 0:
                return r["result"]
            if r.get("retCode") in (10001, 110023) or "symbol" in str(r.get("retMsg", "")).lower():
                return None
        except Exception:
            pass
        time.sleep(1.5 + 2 * a)
    return None


def instruments(cat: str) -> list[dict]:
    out, cur = [], ""
    while True:
        r = get(f"https://api.bybit.com/v5/market/instruments-info?category={cat}&limit=1000" + (f"&cursor={urllib.parse.quote(cur)}" if cur else ""))
        if r is None:
            break
        out += r["list"]
        cur = r.get("nextPageCursor") or ""
        if not cur:
            break
    return out


def oi_daily(sym: str, a: pd.Timestamp, b: pd.Timestamp) -> pd.Series:
    rows, cur = [], ""
    s_ms, e_ms = int(a.timestamp() * 1000), int(b.timestamp() * 1000)
    while True:
        u = (f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={sym}&intervalTime=1d"
             f"&startTime={s_ms}&endTime={e_ms}&limit=200" + (f"&cursor={urllib.parse.quote(cur)}" if cur else ""))
        r = get(u)
        if not r or not r.get("list"):
            break
        rows += r["list"]
        cur = r.get("nextPageCursor") or ""
        if not cur:
            break
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series({pd.to_datetime(int(x["timestamp"]), unit="ms"): float(x["openInterest"]) for x in rows}).sort_index()
    return s[~s.index.duplicated()]


def kl_range(sym: str, interval: str, a: pd.Timestamp, b: pd.Timestamp) -> pd.DataFrame:
    """Свечи за [a, b] без общего кэша (окна далеко друг от друга — общий кэш докачал бы всё между ними)."""
    rows, end_ms, a_ms = [], int(b.timestamp() * 1000), int(a.timestamp() * 1000)
    while True:
        r = get(f"https://api.bybit.com/v5/market/kline?category=linear&symbol={sym}&interval={interval}&start={a_ms}&end={end_ms}&limit=1000")
        if not r or not r.get("list"):
            break
        L = r["list"]
        rows += L
        oldest = int(L[-1][0])
        if oldest <= a_ms or len(L) < 1000:
            break
        end_ms = oldest - 1
    if not rows:
        return pd.DataFrame()
    d = pd.DataFrame(rows, columns=["ts", "o", "h", "l", "c", "v", "t"]).astype(float)
    d["ts"] = pd.to_datetime(d.ts, unit="ms")
    return d.drop_duplicates("ts").set_index("ts").sort_index()[["o", "h", "l", "c", "v", "t"]]


def pmap(fn, items, n=8, label=""):
    out, t0 = {}, time.time()
    with cf.ThreadPoolExecutor(n) as ex:
        futs = {ex.submit(fn, it): it for it in items}
        for j, f in enumerate(cf.as_completed(futs), 1):
            try:
                out[futs[f]] = f.result()
            except Exception as e:                       # noqa: BLE001
                out[futs[f]] = e
            if j % 100 == 0:
                print(f"  {label} {j}/{len(items)} за {time.time() - t0:.0f} с", flush=True)
    return out


# ───────────────────────── сделки мастера ─────────────────────────

def master_orders_p2() -> pd.DataFrame:
    f = PRIV / "kamaz_master/closed.jsonl"
    if f.exists():
        R = pd.DataFrame([json.loads(x) for x in open(f)])
        R["src_file"] = "closed.jsonl"
    else:
        txt = ROOT / "inbox/top-itekcrypto-kamaz.bybit.txt"
        R = pd.read_csv(txt, comment="#")
        R["t_open"] = pd.to_datetime(R.t_open_ms, unit="ms"); R["t_close"] = pd.to_datetime(R.t_close_ms, unit="ms")
        R["src_file"] = txt.name
    R["t_open"] = pd.to_datetime(R.t_open); R["t_close"] = pd.to_datetime(R.t_close)
    R["usd"] = R.order_price * R["size"]
    return R.sort_values("t_open").reset_index(drop=True)


def build_campaigns(orders: pd.DataFrame) -> pd.DataFrame:
    """Кампания = ордера одной монеты, пока позиция не закрылась целиком (новый ордер позже всех закрытий → новая)."""
    out = []
    for s, g in orders.sort_values("t_open").groupby("sym"):
        cur = None
        for r in g.itertuples():
            if cur is None or r.t_open > cur["t_end"]:
                if cur:
                    out.append(cur)
                cur = dict(sym=s, t0=r.t_open, t_end=r.t_close, n=1, usd_first=r.usd, lev=getattr(r, "lev", np.nan), buys=[r.t_open])
            else:
                cur["t_end"] = max(cur["t_end"], r.t_close); cur["n"] += 1; cur["buys"].append(r.t_open)
        out.append(cur)
    return pd.DataFrame(out).sort_values("t0").reset_index(drop=True)


def master_events() -> dict:
    """{period: DataFrame кампаний [sym, t0, buys]} + наборы монет."""
    C1 = pd.read_parquet(PRIV / "work/itekcrypto-kamaz/campaigns.parquet")
    C1 = pd.DataFrame(dict(sym=C1.sym + "USDT", t0=pd.to_datetime(C1.t0), t_end=pd.to_datetime(C1.t_end), n=C1.n_orders, usd_first=C1.usd_first,
                           buys=[list(pd.to_datetime(x)) for x in C1.order_t],
                           grp=np.where(C1.sym.isin(["1000PEPE", "DOGE"]) & (C1.usd_first < 100), "A", "B")))
    O2 = master_orders_p2()
    C2 = build_campaigns(O2)
    tg = pd.DataFrame([dict(sym=s + "USDT", t0=pd.Timestamp(d)) for d, ss in TG_P0.items() for s in ss.split()])
    return dict(P0=tg, P1=C1.sort_values("t0").reset_index(drop=True), P2=C2, O2=O2)


# ───────────────────────── fetch ─────────────────────────

def stage_fetch():
    W.mkdir(parents=True, exist_ok=True)
    print("инструменты…", flush=True)
    lin = instruments("linear"); spot = instruments("spot")
    json.dump(lin, open(W / "instruments_linear.json", "w")); json.dump(spot, open(W / "instruments_spot.json", "w"))
    perps = [x["symbol"] for x in lin if x["contractType"] == "LinearPerpetual" and x["quoteCoin"] == "USDT"]
    print(f"  бессрочных USDT сейчас {len(perps)}, спот {len(spot)}", flush=True)

    def upd(s):
        f = DATA / "klines" / f"{s}_D.parquet"
        a = pd.read_parquet(f).index.min() if f.exists() else pd.Timestamp("2020-01-01")
        return len(market.klines(s, "D", a, LAST_DAY + pd.Timedelta(days=1)))
    pmap(upd, perps, 8, "дневные")

    P = panel_basic()
    pool = pool_symbols(P, rank_max=150, windows=[(PERIODS[p][0], PERIODS[p][1]) for p in PERIODS])
    print(f"пул для ОИ/финансирования: {len(pool)} монет", flush=True)
    (W / "oi").mkdir(exist_ok=True)

    def oi_job(s):
        parts = [oi_daily(s, pd.Timestamp(a) - pd.Timedelta(days=40), pd.Timestamp(b) + pd.Timedelta(days=1)) for a, b, _ in PERIODS.values()]
        parts = [p for p in parts if len(p)]
        if parts:
            ser = pd.concat(parts); ser = ser[~ser.index.duplicated()].sort_index()
            ser.rename("oi").to_frame().to_parquet(W / "oi" / f"{s}.parquet")
        return sum(len(p) for p in parts)
    pmap(oi_job, pool, 8, "ОИ")

    def fund_job(s):
        return len(funding(s, "2024-05-25", LAST_DAY + pd.Timedelta(days=1)))
    pmap(fund_job, pool, 6, "финансирование")

    evt_pool = pool_symbols(P, rank_max=60, windows=list(EVT_WIN.values()))
    ev = master_events()
    evt_pool = sorted(set(evt_pool) | set(ev["P1"].sym) | set(ev["P2"].sym) | {"BTCUSDT", "ETHUSDT"})
    print(f"пул для 15-мин свечей: {len(evt_pool)} монет", flush=True)
    (W / "k15").mkdir(exist_ok=True)

    def k15_job(s):
        n = 0
        for tag, (a, b) in EVT_WIN.items():
            f = W / "k15" / f"{s}_{tag}.parquet"
            need = (pd.Timestamp(b) - pd.Timestamp(a)) / pd.Timedelta(minutes=15)
            if f.exists() and len(pd.read_parquet(f)) >= 0.8 * need:
                continue
            d = kl_range(s, "15", pd.Timestamp(a), pd.Timestamp(b))
            if len(d) < 0.8 * need:                      # снятые с торгов (API отдаёт хвост): из локального кэша 1/5/15 мин
                for iv in ("5", "1", "15"):
                    m = DATA / "klines" / f"{s}_{iv}.parquet"
                    if not m.exists():
                        continue
                    k = pd.read_parquet(m).astype(float)
                    k = k[(k.index >= a) & (k.index <= b)]
                    if len(k):
                        r = k.resample("15min").agg(dict(o="first", h="max", l="min", c="last", v="sum")).dropna()
                        r["t"] = r.v * r.c
                        if len(r) > len(d):
                            d = pd.concat([d, r]); d = d[~d.index.duplicated(keep="first")].sort_index()
            if len(d):
                d.to_parquet(f); n += len(d)
        return n
    pmap(k15_job, evt_pool, 8, "15 мин")
    fetch_m1_moments()
    print("fetch готов", flush=True)


def moments(ev: dict, tag: str, gap_min: int = 30) -> list[list]:
    """Моменты входа = старты кампаний, склеенные в пачки (следующий старт ≤ gap_min после предыдущего)."""
    groups, cur = [], []
    for r in ev[tag].sort_values("t0").itertuples():
        if cur and (r.t0 - cur[-1].t0) > pd.Timedelta(minutes=gap_min):
            groups.append(cur); cur = []
        cur.append(r)
    if cur:
        groups.append(cur)
    return groups


def fetch_m1_moments():
    """Минутки его монет вокруг каждого момента входа ([t−8 ч, t+30 мин]) — для RSI 3-мин и условий Kamaz A."""
    ev = master_events()
    (W / "m1").mkdir(exist_ok=True)
    jobs = []
    for tag in ("P1", "P2"):
        ts = [(g[0].t0, g[-1].t0) for g in moments(ev, tag, GAP_MIN)]
        for s in sorted(set(ev[tag].sym)):
            f = W / "m1" / f"{s}_{tag}.parquet"
            if not f.exists():
                jobs.append((s, tag, tuple(ts)))

    def job(j):
        s, tag, ts = j
        parts = [kl_range(s, "1", t.floor("min") - pd.Timedelta(hours=8), t2.floor("min") + pd.Timedelta(minutes=30)) for t, t2 in ts]
        parts = [x for x in parts if len(x)]
        if parts:
            d = pd.concat(parts); d = d[~d.index.duplicated()].sort_index()
            d.to_parquet(W / "m1" / f"{s}_{tag}.parquet")
            return len(d)
        return 0
    print(f"минутки вокруг моментов: {len(jobs)} пар монета×период", flush=True)
    pmap(job, jobs, 8, "минутки")


# ───────────────────────── панель признаков ─────────────────────────

def universe_syms() -> list[str]:
    s = set()
    if (W / "instruments_linear.json").exists():
        s |= {x["symbol"] for x in json.load(open(W / "instruments_linear.json")) if x["contractType"] == "LinearPerpetual" and x["quoteCoin"] == "USDT"}
    s |= {p[0] for p in json.load(open(DATA / "universe_perps.json"))}
    s |= set(json.load(open(DATA / "universe_delisted.json")))
    return sorted(x for x in s if x.endswith("USDT"))


def is_crypto(sym: str, info: dict) -> bool:
    if base(sym) in NON_CRYPTO:
        return False
    st = info.get(sym, {}).get("symbolType", "")
    return st not in ("stock", "ETF", "commodity", "forex")


_PB: dict = {}


def panel_basic() -> dict:
    """Широкие кадры дата × монета по дневным свечам (только 2024-03-01…01.10.2026) + дата первой свечи."""
    if _PB:
        return _PB
    info = {}
    if (W / "instruments_linear.json").exists():
        info = {x["symbol"]: x for x in json.load(open(W / "instruments_linear.json"))}
    O, H, L, C, V, first = {}, {}, {}, {}, {}, {}
    for s in universe_syms():
        f = DATA / "klines" / f"{s}_D.parquet"
        if not f.exists() or not is_crypto(s, info):
            continue
        d = pd.read_parquet(f).astype(float)
        if d.empty:
            continue
        first[s] = d.index.min()
        d = d[(d.index >= "2024-01-01") & (d.index <= LAST_DAY)]
        if d.empty:
            continue
        O[s], H[s], L[s], C[s], V[s] = d.o, d.h, d.l, d.c, d.v
    days = pd.date_range("2024-01-01", LAST_DAY, freq="D")
    fr = lambda X: pd.DataFrame(X).reindex(days)  # noqa: E731
    _PB.update(O=fr(O), H=fr(H), L=fr(L), C=fr(C), V=fr(V), first=pd.Series(first), info=info)
    T = _PB["C"] * _PB["V"]
    T30 = T.rolling(30, min_periods=20).mean().shift(1)
    alive = _PB["C"].shift(1).notna()
    _PB["T"] = T; _PB["T30"] = T30.where(alive)
    _PB["rank30"] = _PB["T30"].rank(axis=1, ascending=False)
    return _PB


def pool_symbols(P: dict, rank_max: int, windows) -> list[str]:
    R = P["rank30"]
    s = set()
    for a, b in windows:
        r = R.loc[pd.Timestamp(a).normalize():pd.Timestamp(b).normalize()]
        s |= set(r.columns[(r <= rank_max).any()])
    return sorted(s)


def features(P: dict, ev: dict) -> pd.DataFrame:
    """Длинная таблица (период, день, монета) с признаками на день D только по данным до D."""
    O, H, L, C, V, T = (P[k] for k in "O H L C V T".split())
    info = P["info"]
    sh = lambda X: X.shift(1)  # noqa: E731
    F = dict(
        rank30=P["rank30"],
        turn30_m=P["T30"] / 1e6,
        turn7_m=sh(T.rolling(7, min_periods=5).mean()) / 1e6,
        turn1_m=sh(T) / 1e6,
        rank7=sh(T.rolling(7, min_periods=5).mean()).where(C.shift(1).notna()).rank(axis=1, ascending=False),
        rng30=sh(((H - L) / C).rolling(30, min_periods=20).median()) * 100,
        rng1=sh((H - L) / C) * 100,
        atr14=sh((pd.concat([H - L, (H - C.shift(1)).abs(), (L - C.shift(1)).abs()]).groupby(level=0).max().reindex(C.index) / C)
                 .rolling(14, min_periods=10).mean()) * 100,
        r1=sh(C.pct_change(1, fill_method=None)) * 100,
        r7=sh(C.pct_change(7, fill_method=None)) * 100,
        r30=sh(C.pct_change(30, fill_method=None)) * 100,
        dist_hi30=(sh(C) / sh(H.rolling(30, min_periods=20).max()) - 1) * 100,
    )
    R = C.pct_change(fill_method=None)
    F["corr_btc60"] = sh(R.rolling(60, min_periods=40).corr(R["BTCUSDT"]))
    age = pd.DataFrame({s: (C.index - P["first"][s]).days for s in C.columns}, index=C.index)
    F["age_d"] = age.where(C.shift(1).notna())

    days = []
    for p, (a, b, _) in PERIODS.items():
        for d in pd.date_range(a, b, freq="D"):
            days.append((p, d))
    rows = []
    sets = period_sets(ev)
    entry = {p: set(zip(ev[p].sym, ev[p].t0.dt.normalize())) for p in ("P1", "P2")}
    entry["P0"] = set(zip(ev["P0"].sym, ev["P0"].t0.dt.normalize()))
    for p, d in days:
        r = P["rank30"].loc[d]
        syms = list(r.index[r <= 200]) + [s for s in sets[p] if s in r.index and pd.notna(r[s]) and r[s] > 200]
        for s in syms:
            row = dict(period=p, day=d, sym=s)
            for k, X in F.items():
                row[k] = X.at[d, s]
            row["in_set"] = int(s in sets[p]); row["entry_day"] = int((s, d) in entry[p])
            rows.append(row)
    X = pd.DataFrame(rows)

    # инструментные (на сегодня): минимальная заявка, шаг цены, спот, копитрейдинг, «зона инноваций», ST
    spot = set()
    if (W / "instruments_spot.json").exists():
        spot = {x["baseCoin"] for x in json.load(open(W / "instruments_spot.json")) if x.get("quoteCoin") == "USDT" and x.get("status") == "Trading"}
    px = C.shift(1)
    X["px"] = [px.at[d, s] for d, s in zip(X.day, X.sym)]
    X["min_order_usd"] = [max(float(info[s]["lotSizeFilter"]["minOrderQty"]) * p_, float(info[s]["lotSizeFilter"].get("minNotionalValue") or 5))
                          if s in info else np.nan for s, p_ in zip(X.sym, X.px)]
    X["tick_bp"] = [float(info[s]["priceFilter"]["tickSize"]) / p_ * 1e4 if s in info else np.nan for s, p_ in zip(X.sym, X.px)]
    X["spot"] = [int(spot_base(s) in spot) for s in X.sym]
    X["copy_ok"] = [int(info.get(s, {}).get("copyTrading", "none") != "none") if s in info else np.nan for s in X.sym]
    X["innov"] = [int(info.get(s, {}).get("symbolType") == "innovation") if s in info else np.nan for s in X.sym]
    X["st_tag"] = [int("ST" in (info.get(s, {}).get("tags") or [])) if s in info else np.nan for s in X.sym]
    X["live_now"] = [int(s in info) for s in X.sym]
    X["meme"] = [int(base(s) in MEME) for s in X.sym]
    X["max_lev"] = [float(info[s]["leverageFilter"]["maxLeverage"]) if s in info else np.nan for s in X.sym]

    # финансирование: средняя сумма за сутки за 7 дней до D, %; ОИ на 00:00 D (= конец D−1), $
    fund_d, oi_usd = {}, {}
    for s in X.sym.unique():
        f = DATA / "funding" / f"{s}.parquet"
        if f.exists():
            fr = pd.read_parquet(f).rate
            fd = fr.groupby(fr.index.floor("D")).sum().reindex(C.index).fillna(0)
            fund_d[s] = fd.rolling(7, min_periods=5).mean().shift(1) * 100
        g = W / "oi" / f"{s}.parquet"
        if g.exists():
            o = pd.read_parquet(g).oi
            o.index = o.index.floor("D")
            oi_usd[s] = o[~o.index.duplicated()].reindex(C.index) * C[s].shift(1)
    X["fund7"] = [fund_d[s].at[d] if s in fund_d else np.nan for s, d in zip(X.sym, X.day)]
    X["oi_m"] = [oi_usd[s].at[d] / 1e6 if s in oi_usd else np.nan for s, d in zip(X.sym, X.day)]
    X["oi_turn"] = X.oi_m / X.turn30_m
    X["age_y"] = X.age_d / 365.0
    X["ten"] = 0
    U = ten_matrix()
    for i, (d, s) in enumerate(zip(X.day, X.sym)):
        if d in U.index and s in U.columns and U.at[d, s] == 1:
            X.iat[i, X.columns.get_loc("ten")] = 1
    return X


_TEN: dict = {}


def ten_matrix() -> pd.DataFrame:
    """Наша ежедневная десятка (data/universe_daily_top10.parquet до 27.09) + досчёт тем же правилом на 28.09–01.10."""
    if "U" not in _TEN:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import universe  # noqa: E402
        U = pd.read_parquet(DATA / "universe_daily_top10.parquet")
        add = universe.select(10, U.index.max() + pd.Timedelta(days=1), LAST_DAY)
        _TEN["U"] = pd.concat([U, add]).fillna(0).astype(np.int8)
    return _TEN["U"]


def period_sets(ev: dict) -> dict:
    return {p: set(ev[p].sym) for p in ("P0", "P1", "P2")}


# ───────────────────────── анализ ─────────────────────────

FEATS = ["rank30", "turn30_m", "rank7", "rng30", "atr14", "corr_btc60", "age_y", "fund7", "oi_turn", "r7", "r30", "dist_hi30",
         "min_order_usd", "tick_bp", "meme", "spot"]
FEAT_RU = {"rank30": "место по обороту 30 дн.", "turn30_m": "оборот 30 дн., млн $/день", "rank7": "место по обороту 7 дн.",
           "rng30": "типичный размах дня, %", "atr14": "ATR14, %", "corr_btc60": "корреляция с BTC (60 дн.)", "age_y": "возраст на Bybit, лет",
           "fund7": "финансирование, %/сутки", "oi_turn": "ОИ / оборот дня", "r7": "ход за 7 дн., %", "r30": "ход за 30 дн., %",
           "dist_hi30": "от хая 30 дн., %", "min_order_usd": "мин. заявка, $", "tick_bp": "шаг цены, б.п.", "meme": "мем (доля)",
           "spot": "есть спот (доля)", "innov": "зона инноваций (доля)"}


def auc(pos: np.ndarray, neg: np.ndarray) -> float:
    pos, neg = pos[~np.isnan(pos)], neg[~np.isnan(neg)]
    if not len(pos) or not len(neg):
        return np.nan
    from scipy.stats import mannwhitneyu
    return mannwhitneyu(pos, neg).statistic / (len(pos) * len(neg))


def coin_table(X: pd.DataFrame, p: str, pool_rank: int = 100) -> pd.DataFrame:
    """Монета × период: медианы признаков за дни периода; пул = медианное место ≤ pool_rank или монета мастера."""
    x = X[X.period == p]
    g = x.groupby("sym")
    T = g[FEATS + ["innov", "copy_ok", "live_now", "ten"]].median()
    T["ten_share"] = g.ten.mean()
    T["in_set"] = g.in_set.max()
    T["days"] = g.size()
    T["entries"] = g.entry_day.sum()
    T = T[(T.days >= 0.25 * T.days.max()) | (T.in_set == 1)]
    return T[(T.rank30 <= pool_rank) | (T.in_set == 1)].copy()


def prf(pred: set, truth: set) -> tuple:
    tp = len(pred & truth)
    pr = tp / len(pred) if pred else 0.0
    rc = tp / len(truth) if truth else 0.0
    f1 = 2 * pr * rc / (pr + rc) if pr + rc else 0.0
    return pr, rc, f1


def rule_grid():
    for N in (10, 15, 20, 25, 30, 40, 50, 70):
        for A in (0, 0.5, 1, 1.5, 2, 3):
            for rlo in (0, 3, 4, 5):
                for rhi in (6, 8, 10, 99):
                    if rlo >= rhi:
                        continue
                    for cmin in (-1, 0.5, 0.6, 0.7):
                        for spot_req in (0, 1):
                            yield dict(N=N, A=A, rlo=rlo, rhi=rhi, cmin=cmin, spot=spot_req)


def apply_rule(T: pd.DataFrame, r: dict) -> set:
    m = (T.rank30 <= r["N"]) & (T.age_y >= r["A"]) & (T.rng30 >= r["rlo"]) & (T.rng30 <= r["rhi"]) & (T.corr_btc60 >= r["cmin"])
    if r["spot"]:
        m &= T.spot == 1
    return set(T.index[m])


def rule_str(r: dict) -> str:
    s = [f"место по обороту ≤ {r['N']}"]
    if r["A"]:
        s.append(f"возраст ≥ {r['A']:g} г.")
    if r["rlo"]:
        s.append(f"размах ≥ {r['rlo']}%")
    if r["rhi"] < 99:
        s.append(f"размах ≤ {r['rhi']}%")
    if r["cmin"] > -1:
        s.append(f"корр. с BTC ≥ {r['cmin']}")
    if r["spot"]:
        s.append("есть спот")
    return ", ".join(s)


def load_k15(sym: str, tag: str) -> pd.DataFrame:
    f = W / "k15" / f"{sym}_{tag}.parquet"
    return pd.read_parquet(f) if f.exists() else pd.DataFrame()


def trigger_episodes(k: pd.DataFrame, drop60=3.0, drop120=2.0, merge_h=6) -> pd.DatetimeIndex:
    """Моменты «пролива» на 15-мин свечах: от хая последнего часа ≤ −drop60% и за 2 ч ≤ −drop120%; склейка в эпизоды."""
    if k.empty:
        return pd.DatetimeIndex([])
    hi60 = k.h.rolling(4).max()
    lo = k.l
    f60 = (lo / hi60 - 1) * 100
    r120 = (lo / k.c.shift(8) - 1) * 100
    hit = k.index[(f60 <= -drop60) & (r120 <= -drop120)]
    ep, last = [], None
    for t in hit:
        if last is None or (t - last) > pd.Timedelta(hours=merge_h):
            ep.append(t)
        last = t
    return pd.DatetimeIndex(ep)


def md_table(df: pd.DataFrame, floatfmt=1) -> str:
    cols = list(df.columns)
    out = ["| " + " | ".join(str(c) for c in cols) + " |", "|" + "---|" * len(cols)]
    for _, r in df.iterrows():
        cells = []
        for c in cols:
            v = r[c]
            if isinstance(v, (float, np.floating)):
                cells.append("—" if pd.isna(v) else (f"{v:.{floatfmt}f}" if abs(v) < 1e5 else f"{v:,.0f}"))
            else:
                cells.append(str(v))
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)


def stage_analyze():
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 40)
    ev = master_events()
    sets = period_sets(ev)
    P = panel_basic()
    X = features(P, ev)
    X.to_parquet(W / "features.parquet")
    L: list[str] = []

    def say(x: str):
        L.append(x); print(x, flush=True)

    # ── 1. какие монеты и как меняется набор ──
    say("## 1. Монеты мастера по периодам\n")
    allc = sorted(set().union(*sets.values()), key=lambda s: nm(s))
    cnt = {p: ev[p].groupby("sym").size() for p in ("P0", "P1", "P2")}
    first_fresh = ev["P2"][ev["P2"].t0 >= "2026-09-26"]
    rows = []
    for s in allc:
        r = dict(монета=nm(s))
        r["P0 TG (упомин.)"] = int(cnt["P0"].get(s, 0)) or "·"
        r["P1 кампаний"] = int(cnt["P1"].get(s, 0)) or "·"
        r["P2 кампаний"] = int(cnt["P2"].get(s, 0)) or "·"
        r["из них 26.09–01.10"] = int((first_fresh.sym == s).sum()) or "·"
        rows.append(r)
    say(md_table(pd.DataFrame(rows)))
    j = lambda a, b: len(a & b) / len(a | b)  # noqa: E731
    say(f"\nМонет: P0 {len(sets['P0'])}, P1 {len(sets['P1'])}, P2 {len(sets['P2'])}. "
        f"Общие P0∩P1 {len(sets['P0'] & sets['P1'])} (Жаккар {j(sets['P0'], sets['P1']):.2f}), "
        f"P1∩P2 {len(sets['P1'] & sets['P2'])} ({j(sets['P1'], sets['P2']):.2f}), "
        f"P0∩P2 {len(sets['P0'] & sets['P2'])} ({j(sets['P0'], sets['P2']):.2f}); во всех трёх: "
        + ", ".join(sorted(nm(s) for s in sets['P0'] & sets['P1'] & sets['P2'])) + ".")
    # P1 по месяцам: ротация внутри периода
    c1 = ev["P1"].copy(); c1["m"] = c1.t0.dt.to_period("M")
    c2 = ev["P2"].copy(); c2["m"] = c2.t0.dt.to_period("M")
    say("\nПо месяцам (новые монеты месяца относительно всех предыдущих в периоде):\n")
    for c in (c1, c2):
        seen = set()
        for m, g in c.groupby("m"):
            cur = set(g.sym)
            say(f"- {m}: {len(cur)} монет — " + ", ".join(sorted(nm(s) for s in cur)) +
                (f"; новые: {', '.join(sorted(nm(s) for s in cur - seen))}" if seen and cur - seen else ""))
            seen |= cur

    # размер первой заявки и плечо по монетам (P2) — признак ручной настройки по монете
    C2 = ev["P2"]
    t = C2.groupby("sym").agg(кампаний=("t0", "size"), первая_заявка_мед=("usd_first", "median"), плечо=("lev", "median"))
    t.index = [nm(s) for s in t.index]
    say("\nP2: первая заявка кампании и плечо по монетам (у бота с одной настройкой они были бы одинаковыми):\n")
    say(md_table(t.reset_index().rename(columns={"index": "монета", "первая_заявка_мед": "первая заявка, $ (медиана)"}), 0))

    # ── 2. признаки: его монеты против остальных ──
    say("\n## 2. Его монеты против остальных (признаки на день до входа, медианы за период)\n")
    say("Пул — монеты с медианным местом по обороту за 30 дн. ≤ 100 в периоде (+ все монеты мастера). "
        "AUC — насколько признак разделяет «его»/«не его» (0.5 — никак, 1.0 — идеально; <0.5 — у его монет меньше).\n")
    tabs = {p: coin_table(X, p) for p in PERIODS}
    BIN = {"meme", "spot", "innov"}
    FMT = {"fund7": 3, "corr_btc60": 2, "oi_turn": 2, "tick_bp": 2, "age_y": 1, "rank30": 0, "rank7": 0, "turn30_m": 0, "min_order_usd": 0}

    def feat_table(tabs_: dict, feats: list) -> str:
        rows_ = []
        for f in feats:
            r = dict(признак=FEAT_RU.get(f, f))
            nd = FMT.get(f, 1)
            for p in PERIODS:
                T = tabs_[p]
                pos, neg = T[T.in_set == 1][f].values.astype(float), T[T.in_set == 0][f].values.astype(float)
                agg = np.nanmean if f in BIN else np.nanmedian
                fm = (lambda v: "—" if not np.isfinite(v) else (f"{v * 100:.0f}%" if f in BIN else f"{v:.{nd}f}"))
                r[f"{p} его"] = fm(agg(pos)) if len(pos) else "—"
                r[f"{p} прочие"] = fm(agg(neg)) if len(neg) else "—"
                a = auc(pos, neg)
                r[f"{p} AUC"] = "—" if not np.isfinite(a) else f"{a:.2f}"
            rows_.append(r)
        return md_table(pd.DataFrame(rows_))

    say(feat_table(tabs, FEATS + ["innov"]))
    say("\nТо же внутри топ-30 по обороту (вопрос «почему из ликвидных именно эти»):\n")
    tabs30 = {p: t[t.rank30 <= 30] for p, t in tabs.items()}
    say(feat_table(tabs30, FEATS + ["innov"]))
    for p in PERIODS:
        t = tabs30[p]
        say(f"- {p}: в топ-30 его {int(t.in_set.sum())} из {len(t)}; не его: " + ", ".join(
            f"{nm(s)} ({t.at[s, 'age_y']:.1f} г.{', мем' if t.at[s, 'meme'] else ''})" for s in t.index[t.in_set == 0]))

    # места его монет по обороту
    say("\nМесто его монет по обороту за 30 дн. (медиана за период) и сколько монет с местом выше он НЕ торговал:\n")
    rows = []
    for p in PERIODS:
        T = tabs[p].sort_values("rank30")
        mine = T[T.in_set == 1]
        worst = mine.rank30.max()
        skipped = T[(T.in_set == 0) & (T.rank30 <= worst)]
        rows.append(dict(период=p, монет=len(mine), **{"его места": ", ".join(f"{nm(s)} {int(v)}" for s, v in mine.rank30.items())},
                         **{"выше худшего его, но не торговал": f"{len(skipped)}: " + ", ".join(nm(s) for s in skipped.index[:25]) + ("…" if len(skipped) > 25 else "")}))
    for r in rows:
        say(f"- **{r['период']}** ({r['монет']} монет): {r['его места']}.\n  Не торговал при месте выше его худшего — {r['выше худшего его, но не торговал']}")

    # ── 3. простые правила, проверка вне подбора по времени ──
    say("\n## 3. Можно ли описать список правилом (проверка вне подбора по времени)\n")
    best = {}
    for fit in ("P1", "P2"):
        T = tabs[fit]
        truth = set(T.index[T.in_set == 1])
        sc = []
        for r in rule_grid():
            pr, rc, f1 = prf(apply_rule(T, r), truth)
            sc.append((f1, pr, rc, r))
        cx = lambda r: (r["A"] > 0) + (r["rlo"] > 0) + (r["rhi"] < 99) + (r["cmin"] > -1) + r["spot"]  # noqa: E731
        sc.sort(key=lambda z: (-round(z[0], 3), cx(z[3]), z[3]["N"]))
        best[fit] = sc[0]
    rows = []
    for fit, (f1, pr, rc, r) in best.items():
        for p in PERIODS:
            T = tabs[p]; truth = set(T.index[T.in_set == 1])
            pred = apply_rule(T, r)
            a, b, c = prf(pred, truth)
            rows.append({"подбор на": fit, "правило": rule_str(r), "период": p + (" (подбор)" if p == fit else ""),
                         "отобрано": len(pred), "точность": a, "полнота": b, "F1": c,
                         "лишние": ", ".join(sorted(nm(s) for s in pred - truth)[:12]), "пропущены": ", ".join(sorted(nm(s) for s in truth - pred))})
    say(md_table(pd.DataFrame(rows), 2))
    # базовые линии
    say("\nБазовые линии:\n")
    rows = []
    for N in (10, 20, 30):
        for p in PERIODS:
            T = tabs[p]; truth = set(T.index[T.in_set == 1])
            pred = set(T.index[T.rank30 <= N])
            a, b, c = prf(pred, truth)
            rows.append({"способ": f"просто топ-{N} по обороту", "период": p, "точность": a, "полнота": b, "F1": c})
    for prev, nxt in (("P0", "P1"), ("P1", "P2"), ("P0", "P2")):
        a, b, c = prf(sets[prev], sets[nxt])
        rows.append({"способ": f"его же список из {prev}", "период": nxt, "точность": a, "полнота": b, "F1": c})
    for p in PERIODS:
        T = tabs[p]; truth = set(T.index[T.in_set == 1])
        ten = set(T.index[T.ten_share > 0])
        a, b, c = prf(ten, truth)
        rows.append({"способ": "наша ежедневная десятка (хоть день в периоде)", "период": p, "точность": a, "полнота": b, "F1": c})
    say(md_table(pd.DataFrame(rows), 2))

    # дерево решений
    from sklearn.tree import DecisionTreeClassifier, export_text
    tf = ["rank30", "rng30", "atr14", "corr_btc60", "age_y", "fund7", "oi_turn", "r30", "dist_hi30", "min_order_usd", "tick_bp", "meme", "spot"]
    say("\nДерево решений (глубина 2–3, классы уравновешены), обучение на одном периоде → проверка на другом:\n")
    rows, trees = [], []
    for train, tests in ((["P1"], ["P2", "P0"]), (["P0", "P1"], ["P2"]), (["P2"], ["P1", "P0"])):
        Tr = pd.concat([tabs[p] for p in train])
        Xtr = Tr[tf].astype(float).fillna(Tr[tf].astype(float).median())
        for depth in (2, 3):
            m = DecisionTreeClassifier(max_depth=depth, min_samples_leaf=3, class_weight="balanced", random_state=0).fit(Xtr, Tr.in_set)
            if depth == 2:
                trees.append((train, export_text(m, feature_names=[FEAT_RU.get(f, f) for f in tf], decimals=2)))
            for p in train + tests:
                T = tabs[p]
                Xt = T[tf].astype(float).fillna(Xtr.median())
                pred = set(T.index[m.predict(Xt) == 1]); truth = set(T.index[T.in_set == 1])
                a, b, c = prf(pred, truth)
                rows.append({"обучение": "+".join(train), "глубина": depth, "период": p + (" (обучение)" if p in train else ""),
                             "отобрано": len(pred), "точность": a, "полнота": b, "F1": c})
    say(md_table(pd.DataFrame(rows), 2))
    for train, txt in trees:
        say(f"\nДерево глубины 2, обучено на {'+'.join(train)}:\n```\n{txt}```")

    # ── 4. моменты входа: что ещё пролилось одновременно ──
    say("\n## 4. Момент входа: какие монеты пролились одновременно и какую он взял\n")
    say("15-мин свечи, пул = топ-60 по обороту в окне + его монеты. «Пролилась» = от хая предыдущих 2 ч до минимума к концу свечи входа ≤ −3%.\n")
    k15 = {}
    for tag in ("P1", "P2"):
        for f in (W / "k15").glob(f"*_{tag}.parquet"):
            k15[(f.name.rsplit("_", 1)[0], tag)] = pd.read_parquet(f)
    buys = {}
    for tag in ("P1", "P2"):
        for r in ev[tag].itertuples():
            buys.setdefault((r.sym, tag), []).extend(list(r.buys) if isinstance(r.buys, list) else [r.t0])
    buys = {k: pd.DatetimeIndex(sorted(v)) for k, v in buys.items()}
    opened = {}
    for tag in ("P1", "P2"):
        for r in ev[tag].itertuples():
            opened.setdefault((r.sym, tag), []).append((r.t0, r.t_end))

    def in_pos(sym, tag, t):
        """У мастера уже открыта кампания по монете (тогда нового старта быть не может — только ступень лесенки)."""
        return any(a < t - pd.Timedelta(minutes=60) and t <= b for a, b in opened.get((sym, tag), []))

    def dd_at(sym, tag, t):
        k = k15.get((sym, tag))
        if k is None or k.empty:
            return np.nan
        b = t.floor("15min")
        pre = k.loc[b - pd.Timedelta(hours=2): b - pd.Timedelta(minutes=15)]
        cur = k.loc[b - pd.Timedelta(hours=2): b]
        if len(pre) < 4 or b not in k.index:
            return np.nan
        return (cur.l.min() / pre.h.max() - 1) * 100

    mom_rows = []
    for tag in ("P1", "P2"):
        for g in moments(ev, tag, GAP_MIN):
            t = g[0].t0
            t_last = g[-1].t0
            d = t.normalize()
            if d not in P["rank30"].index:
                continue
            rk = P["rank30"].loc[d]
            pool = set(rk.index[rk <= 60]) | sets[tag]
            bought = {r.sym for r in g}
            dd = {s: dd_at(s, tag, t) for s in pool}
            dd = {s: v for s, v in dd.items() if pd.notna(v)}
            if not dd:
                continue
            drop = {s for s, v in dd.items() if v <= -3}
            setdrop = drop & sets[tag]
            set_bought_any = {s for s in setdrop if (s, tag) in buys and ((buys[(s, tag)] >= t - pd.Timedelta(minutes=60)) & (buys[(s, tag)] <= t_last + pd.Timedelta(minutes=60))).any()}
            missed = setdrop - set_bought_any
            missed_inpos = {s for s in missed if in_pos(s, tag, t)}
            for s in bought:
                v = dd.get(s, np.nan)
                deeper_non = [x for x in drop - sets[tag] if dd[x] < v] if pd.notna(v) else []
                mom_rows.append(dict(period=tag, t=t, sym=s, n_bought=len(bought), dd=v, n_drop=len(drop), n_drop_set=len(setdrop),
                                     n_drop_nonset=len(drop - sets[tag]), set_drop_bought=len(set_bought_any),
                                     n_deeper_nonset=len(deeper_non),
                                     rank_dd_all=(sorted(dd.values()).index(v) + 1) if pd.notna(v) else np.nan, n_pool=len(dd),
                                     top_nonset=", ".join(nm(x) for x in sorted(drop - sets[tag], key=lambda x: dd[x])[:6]),
                                     btc_dd=dd.get("BTCUSDT", np.nan), missed=", ".join(f"{nm(x)} {dd[x]:.1f}%" + (" (в позиции)" if x in missed_inpos else "") for x in sorted(missed, key=lambda x: dd[x])),
                                     n_missed=len(missed), n_missed_inpos=len(missed_inpos)))
    M = pd.DataFrame(mom_rows, columns=["period", "t", "sym", "n_bought", "dd", "n_drop", "n_drop_set", "n_drop_nonset", "set_drop_bought",
                                        "n_deeper_nonset", "rank_dd_all", "n_pool", "top_nonset", "btc_dd", "missed", "n_missed", "n_missed_inpos"])
    M.to_parquet(W / "moments.parquet")
    for tag in ("P1", "P2"):
        m = M[M.period == tag]
        if m.empty:
            continue
        mm = m.drop_duplicates("t")
        say(f"**{tag}**: стартов кампаний {len(m)} в {len(mm)} моментах (пачка: старты ближе 15 мин). "
            f"Пролив ≥3% у купленной монеты: {(m.dd <= -3).mean() * 100:.0f}% стартов (медиана хода {m.dd.median():.1f}%). "
            f"В момент входа пролилось монет пула: медиана {mm.n_drop.median():.0f} (из {mm.n_pool.median():.0f}), "
            f"из них его монет {mm.n_drop_set.median():.0f}, чужих {mm.n_drop_nonset.median():.0f}. "
            f"Купленная монета — самая глубокая из всего пула в {(m.rank_dd_all == 1).mean() * 100:.0f}% стартов; "
            f"чужих монет, проливших глубже купленной: медиана {m.n_deeper_nonset.median():.0f}, хоть одна — в {(m.n_deeper_nonset > 0).mean() * 100:.0f}% стартов. "
            f"Из его монет, проливших ≥3% в тот же момент, он купил (±60 мин) {mm.set_drop_bought.sum() / max(mm.n_drop_set.sum(), 1) * 100:.0f}%; "
            f"не купленных {mm.n_missed.sum()}, из них {mm.n_missed_inpos.sum()} уже были в открытой кампании.\n")
    big = M.drop_duplicates("t").sort_values("n_bought", ascending=False).head(6)
    say("Крупнейшие пачки (много монет за минуты) — что купил и что из чужих пролилось сильнее всего:\n")
    for r in big.itertuples():
        got = M[M.t == r.t].sort_values("dd")
        say(f"- {r.t:%d.%m.%Y %H:%M} ({r.period}): купил {r.n_bought} — " + ", ".join(f"{nm(x.sym)} {x.dd:.1f}%" for x in got.itertuples())
            + f"; BTC {r.btc_dd:.1f}%; его монеты с проливом, но без покупки: {r.missed or 'нет'}; чужие с проливом ≥3%: {r.n_drop_nonset} ({r.top_nonset})")

    # ── 4б. внутри его списка: кого из пролившихся он берёт (минутки, условия Kamaz A) ──
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import grid_engine as ge  # noqa: E402
    say("\n### 4б. Внутри его списка: какую из пролившихся монет он берёт\n")
    say("Минутки его монет вокруг каждого момента. Для каждой монеты списка, у которой НЕ было открытой кампании: "
        "в окне [начало пачки − 20 мин, конец пачки + 2 мин] смотрим минимум RSI7 на 3-мин свечах Bybit, ход от хая 60 мин и ход за 120 мин; "
        "«сигнал A» = в одну минуту RSI ≤ 15, от хая 60 мин ≤ −3%, за 120 мин ≤ −2% (правило Kamaz A для DOGE); "
        "«мягкий» = RSI ≤ 25, от хая ≤ −2%, за 120 мин ≤ −1.5%.\n")
    m1 = {}
    for f in (W / "m1").glob("*.parquet"):
        sym_, tag_ = f.stem.rsplit("_", 1)
        k = pd.read_parquet(f).astype(float)
        if len(k):
            m1[(sym_, tag_)] = dict(k=k, r3=pd.Series(ge.rsi3(k.c, "bybit"), index=k.index),
                                    f60=(k.c / k.h.rolling(60).max() - 1) * 100, r120=(k.c / k.c.shift(120) - 1) * 100)
    srow = []
    for tag in ("P1", "P2"):
        for g in moments(ev, tag, GAP_MIN):
            t_a, t_b = g[0].t0, g[-1].t0
            bought = {r.sym for r in g}
            for s_ in sets[tag]:
                if s_ not in bought and in_pos(s_, tag, t_a):
                    continue
                if (s_, tag) not in m1:
                    continue
                D = m1[(s_, tag)]
                lo, hi = t_a.floor("min") - pd.Timedelta(minutes=20), t_b.floor("min") + pd.Timedelta(minutes=2)
                if s_ in bought:                          # для купленной — до её собственной покупки
                    tb = min(r.t0 for r in g if r.sym == s_)
                    hi = tb.floor("min")
                r3 = D["r3"].loc[lo:hi]; f60 = D["f60"].loc[lo:hi]; r120 = D["r120"].loc[lo:hi]
                if len(r3) < 5:
                    continue
                sigA = ((r3 <= 15) & (f60 <= -3) & (r120 <= -2)).any()
                soft = ((r3 <= 25) & (f60 <= -2) & (r120 <= -1.5)).any()
                srow.append(dict(period=tag, t=t_a, sym=s_, bought=int(s_ in bought), rsi_min=r3.min(), f60_min=f60.min(), r120_min=r120.min(),
                                 sigA=int(sigA), soft=int(soft), n_in_pack=len(bought)))
    S = pd.DataFrame(srow)
    S.to_parquet(W / "set_moments.parquet")
    rows = []
    for tag in ("P1", "P2"):
        for b_ in (1, 0):
            x = S[(S.period == tag) & (S.bought == b_)]
            rows.append({"период": tag, "монеты списка": "купил" if b_ else "не купил (без открытой кампании)", "случаев": len(x),
                         "мин. RSI 3м (медиана)": x.rsi_min.median(), "от хая 60 мин, %": x.f60_min.median(), "за 120 мин, %": x.r120_min.median(),
                         "сигнал A, %": x.sigA.mean() * 100, "мягкий, %": x.soft.mean() * 100})
    say(md_table(pd.DataFrame(rows), 1))
    for tag in ("P1", "P2"):
        x = S[S.period == tag]
        if len(x):
            a1 = auc(-x[x.bought == 1].f60_min.values, -x[x.bought == 0].f60_min.values)
            a2 = auc(-x[x.bought == 1].rsi_min.values, -x[x.bought == 0].rsi_min.values)
            sa = x[x.sigA == 1]
            say(f"- {tag}: насколько «глубже пролилась» отличает купленную от некупленной монеты списка в тот же момент: AUC {a1:.2f} по ходу от хая, "
                f"{a2:.2f} по RSI. Среди монет списка с сигналом A он купил {sa.bought.mean() * 100:.0f}% ({int(sa.bought.sum())}/{len(sa)}), "
                f"без сигнала A — {x[x.sigA == 0].bought.mean() * 100:.0f}%.")

    # ── 5. пропущенные проливы: кого он «видит» ──
    say("\n## 5. Проливы, на которые он не вошёл: список или правило?\n")
    say("Эпизод пролива = на 15-мин свечах минимум ≤ −3% от хая последнего часа и ≤ −2% за 2 ч (близко к фильтру Kamaz A), "
        "эпизоды склеены по 6 ч. «Взял» = покупка этой монеты в ±3 ч от эпизода (старт или докупка).\n")
    say("Для P2 считаем с 28.06: витрина отдаёт только позиции, закрытые за 90 дней до выгрузки, — июнь виден неполно.\n")
    rows = []
    for tag in ("P1", "P2"):
        a, b = (pd.Timestamp(x) for x in PERIODS[tag][:2])
        if tag == "P2":
            a = pd.Timestamp("2026-06-28")
        T = tabs[tag]
        cand = [s for s in T.index if T.at[s, "rank30"] <= 40 or T.at[s, "in_set"] == 1]
        for s in cand:
            k = k15.get((s, tag))
            if k is None or k.empty:
                continue
            k = k[(k.index >= a) & (k.index <= b + pd.Timedelta(days=1))]
            ep = trigger_episodes(k)
            bt = buys.get((s, tag), pd.DatetimeIndex([]))
            hit = [((bt >= e - pd.Timedelta(hours=3)) & (bt <= e + pd.Timedelta(hours=3))).any() for e in ep]
            free = [not in_pos(s, tag, e) for e in ep]
            rows.append(dict(период=tag, монета=nm(s), его=int(T.at[s, "in_set"]), место=T.at[s, "rank30"], эпизодов=len(ep), взял=int(sum(hit)),
                             **{"эпизодов без позиции": int(sum(free)), "взял без позиции": int(sum(h and f for h, f in zip(hit, free)))},
                             кампаний=int(T.at[s, "entries"]) if tag != "P0" else 0))
    E = pd.DataFrame(rows, columns=["период", "монета", "его", "место", "эпизодов", "взял", "эпизодов без позиции", "взял без позиции", "кампаний"])
    E.to_parquet(W / "episodes.parquet")
    for tag in ("P1", "P2"):
        e = E[E.период == tag]
        mine, other = e[e.его == 1], e[e.его == 0]
        fe, ft = mine["эпизодов без позиции"].sum(), mine["взял без позиции"].sum()
        say(f"**{tag}**: его монеты — эпизодов {mine.эпизодов.sum()}, взял {mine.взял.sum()} ({mine.взял.sum() / max(mine.эпизодов.sum(), 1) * 100:.0f}%); "
            f"когда по монете не было открытой кампании — {fe} эпизодов, взял {ft} ({ft / max(fe, 1) * 100:.0f}%); "
            f"чужие из топ-40 — эпизодов {other.эпизодов.sum()} у {len(other)} монет, взял 0. "
            f"Чужих монет с ≥ {int(mine.эпизодов.median())} эпизодов (медиана у его монет): "
            f"{(other.эпизодов >= mine.эпизодов.median()).sum()} — "
            + ", ".join(f"{r.монета} ({r.эпизодов}, место {r.место:.0f})" for r in other.sort_values("эпизодов", ascending=False).head(14).itertuples()) + ".\n")
        pr = ft / max(fe, 1)
        p0 = (1 - pr) ** other.эпизодов
        say(f"Если бы чужая монета была в его списке и он брал её проливы с той же частотой ({pr * 100:.0f}%), шанс не взять ни одного "
            f"< 5% у {(p0 < 0.05).sum()} из {len(other)} чужих монет топ-40 (< 1% — у {(p0 < 0.01).sum()}). Значит, их нет в списке, а не «не было сигнала».\n")
        say(md_table(mine.sort_values("эпизодов", ascending=False)[["монета", "место", "эпизодов", "взял", "эпизодов без позиции", "взял без позиции", "кампаний"]], 0))
        say("")

    # ── 6. наша десятка против его списка ──
    say("\n## 6. Наша ежедневная десятка и его монеты\n")
    U = ten_matrix()
    for tag in ("P1", "P2"):
        C = ev[tag]
        inten = [int(d in U.index and s in U.columns and U.at[d, s] == 1) for s, d in zip(C.sym, C.t0.dt.normalize())]
        a, b = (pd.Timestamp(x) for x in PERIODS[tag][:2])
        u = U.loc[a:min(b, U.index.max())]
        ours = set(u.columns[u.sum() > 0])
        say(f"- **{tag}**: его стартов на монетах, бывших в нашей десятке в тот день: {np.mean(inten) * 100:.0f}% ({sum(inten)}/{len(inten)}). "
            f"Монет в десятке за период {len(ours)}, его {len(sets[tag])}, общих {len(ours & sets[tag])}: {', '.join(sorted(nm(s) for s in ours & sets[tag]))}. "
            f"Только у нас: {', '.join(sorted(nm(s) for s in ours - sets[tag]))}. Только у него: {', '.join(sorted(nm(s) for s in sets[tag] - ours))}.")
    X.to_parquet(W / "features.parquet")
    (W / "analysis.md").write_text("\n".join(L))
    return X, M, E


# ───────────────────────── бэктест вариантов отбора (grid_universe, рабочий вариант x1) ─────────────────────────

def variant_universes() -> dict:
    """Варианты нашей десятки по выводам о мастере. Фильтры включаются с 2023 года: раньше «возраст на Bybit» занижен
    (USDT-бессрочные старых монет появились в 2020–2021), и фильтр выкинул бы ETH/SOL/DOGE."""
    U = pd.read_parquet(DATA / "universe_daily_top10.parquet")
    first, C = {}, {}
    for s in set(U.columns) | {"BTCUSDT"}:
        d = pd.read_parquet(DATA / "klines" / f"{s}_D.parquet").astype(float)
        first[s] = d.index.min(); C[s] = d.c
    C = pd.DataFrame(C).reindex(pd.date_range("2020-01-01", U.index.max(), freq="D"))
    R = C.pct_change(fill_method=None)
    corr = R.rolling(60, min_periods=40).corr(R["BTCUSDT"]).shift(1).reindex(U.index)
    age = pd.DataFrame({s: (U.index - first[s]).days for s in U.columns}, index=U.index)
    on = pd.Series(U.index >= "2023-01-01", index=U.index)
    out = {"база (десятка как есть)": U}
    m1 = (age < 365).mul(on, axis=0)
    out["без монет моложе года"] = U.where(~m1, 0)
    m2 = (corr[U.columns] < 0.4).fillna(False).mul(on, axis=0)
    out["без монет с корр. BTC < 0.4"] = U.where(~m2, 0)
    out["без молодых и без корр. < 0.4"] = U.where(~(m1 | m2), 0)
    return out


def stage_bt():
    import multiprocessing as mp
    import os
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    os.environ["UNIV_SET"] = "one"
    import grid_universe as gu  # noqa: E402
    res_rows, by_year, removed = [], {}, {}
    V = variant_universes()
    base = V["база (десятка как есть)"]
    for name, U in V.items():
        f = W / f"univ_{len(res_rows)}.parquet"
        U.astype(np.int8).to_parquet(f)
        os.environ["UNIV_FILE"] = str(f)
        gu._G.clear()
        segs = gu.segments()
        t0 = time.time()
        with mp.get_context("spawn").Pool(6) as pool:
            out = list(pool.imap_unordered(gu.task, segs, chunksize=1))
        ok = [r for r in out if not r["skipped"]]
        Pd = pd.concat([r["res"]["база"]["days"] for r in ok], axis=1).sum(axis=1).sort_index()
        eq = 100_000 + Pd.cumsum()
        Cs = pd.concat([r["res"]["база"]["camps"] for r in ok if len(r["res"]["база"]["camps"])])
        Cs["t1"] = pd.to_datetime(Cs.t1)
        liq = sum(r["res"]["база"]["liq"] is not None for r in ok)
        yr = {}
        for y in range(2021, 2027):
            e = eq[eq.index.year == y]
            if e.empty:
                continue
            st = eq[eq.index < e.index[0]].iloc[-1] if (eq.index < e.index[0]).any() else 100_000
            yr[y] = (e.iloc[-1] - st) / 1000
        by_year[name] = yr
        dd = (eq / eq.cummax() - 1).min() * 100
        e23 = eq[eq.index >= "2023-01-01"]
        st23 = eq[eq.index < "2023-01-01"].iloc[-1]
        dd23 = (e23 / e23.cummax() - 1).min() * 100
        res_rows.append({"вариант": name, "монето-дней": int(U.values.sum()), "итог, $": eq.iloc[-1] - 100_000,
                         "2023–26, $": e23.iloc[-1] - st23, "просадка, %": dd, "просадка 2023–26, %": dd23,
                         "сделок": len(Cs), "в плюс, %": (Cs.pnl > 0).mean() * 100, "ликвидаций": liq,
                         "худшие монеты": ", ".join(f"{nm(m + 'USDT')} {v / 1000:+.1f}k" for m, v in Cs.groupby("монета").pnl.sum().sort_values().head(4).items())})
        if name != "база (десятка как есть)":
            rm = (base.astype(bool) & ~U.astype(bool))
            removed[name] = rm.sum()[rm.sum() > 0].sort_values(ascending=False)
        print(f"{name}: {time.time() - t0:.0f} с, итог {eq.iloc[-1] - 100_000:+,.0f}$", flush=True)
    L = ["## 7. Бэктест: наша десятка с фильтрами «как у мастера» (рабочий вариант x1: без фильтра BTC, пропуск −25%/сутки, лесенка ≤ счёта)\n",
         "Фильтры включены с 01.01.2023 (раньше возраст на Bybit занижен). Выкинутые монеты не заменяются (минуток на замену нет) — в такие дни монет меньше 10.\n",
         md_table(pd.DataFrame(res_rows), 1), "\nПо годам, % на $100k:\n",
         md_table(pd.DataFrame(by_year).T.reset_index().rename(columns={"index": "вариант"}), 1)]
    for k, v in removed.items():
        L.append(f"\n- {k}: выкинуто монето-дней {int(v.sum())} — " + ", ".join(f"{nm(s)} {int(n)}" for s, n in v.head(20).items()))
    (W / "bt.md").write_text("\n".join(L))
    print("\n".join(L))


def fill_minutes(sym: str, a: pd.Timestamp, b: pd.Timestamp) -> int:
    """Докачать недостающие дни минуток в data/klines_seg/<SYM>_kz_<a>_<b>.parquet (их подхватывает grid_universe.load_1m)."""
    import glob
    idx = pd.DatetimeIndex([])
    f = DATA / "klines" / f"{sym}_1.parquet"
    if f.exists():
        idx = idx.union(pd.read_parquet(f).index)
    for g in glob.glob(str(DATA / f"klines_seg/{sym}_*.parquet")):
        idx = idx.union(pd.read_parquet(g).index)
    per_day = pd.Series(1, index=idx).resample("D").size().reindex(pd.date_range(a.normalize(), b.normalize(), freq="D"), fill_value=0)
    miss = per_day.index[per_day < 1300]
    if not len(miss):
        return 0
    runs, cur = [], [miss[0]]
    for d in miss[1:]:
        if (d - cur[-1]).days == 1:
            cur.append(d)
        else:
            runs.append(cur); cur = [d]
    runs.append(cur)
    n = 0
    for r in runs:
        ra, rb = r[0], r[-1] + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
        d = kl_range(sym, "1", ra, rb)
        if len(d):
            d[["o", "h", "l", "c", "v"]].to_parquet(DATA / f"klines_seg/{sym}_kz_{ra:%Y-%m-%d}_{rb:%Y-%m-%d}.parquet"); n += len(d)
    return n


def run_variant(U: pd.DataFrame, tag: str) -> dict:
    """Прогон рабочего варианта по таблице «дата × монета» (1 — монета разрешена для входа). Без заглядывания: таблица задана заранее."""
    import multiprocessing as mp
    import os
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    os.environ["UNIV_SET"] = "one"
    import grid_universe as gu  # noqa: E402
    f = W / f"univ_{tag}.parquet"
    U.astype(np.int8).to_parquet(f)
    os.environ["UNIV_FILE"] = str(f)
    gu._G.clear()
    segs = gu.segments()
    with mp.get_context("spawn").Pool(6) as pool:
        out = list(pool.imap_unordered(gu.task, segs, chunksize=1))
    ok = [r for r in out if not r["skipped"]]
    skipped = [f"{nm(r['sym'])} {r['a']:%d.%m.%y}" for r in out if r["skipped"]]
    Pd = pd.concat([r["res"]["база"]["days"] for r in ok], axis=1).sum(axis=1).sort_index()
    Cs = pd.concat([r["res"]["база"]["camps"] for r in ok if len(r["res"]["база"]["camps"])])
    Cs["t1"] = pd.to_datetime(Cs.t1)
    liq = [f"{nm(r['sym'])} {r['res']['база']['liq']:%d.%m.%y}" for r in ok if r["res"]["база"]["liq"] is not None]
    return dict(P=Pd, C=Cs, liq=liq, skipped=skipped)


def stage_bt2():
    """Честная проверка «его список против нашей десятки»: список мастера на январь 2025 (P1, известен на 16.01.2025)
    торгуем с 17.01.2025 по 27.09.2026 всеми монетами каждый день; десятку — как есть в том же окне. Счёт — по $10k на монету,
    сравнение в % на вложенное (у списка 17 монет, у десятки 10 ячеек)."""
    a, b = pd.Timestamp("2025-01-17"), pd.Timestamp("2026-09-27")
    ev = master_events()
    p1 = sorted(s for s in set(ev["P1"].sym) if s != "FTMUSDT")
    core = ["DOGEUSDT", "1000PEPEUSDT", "SOLUSDT", "SUIUSDT", "ADAUSDT", "AVAXUSDT", "BCHUSDT", "SHIB1000USDT", "ETHUSDT", "XRPUSDT"]
    print("докачка минуток:", flush=True)
    for s_ in sorted(set(p1) | set(core)):
        print(f"  {s_}: +{fill_minutes(s_, a - pd.Timedelta(days=3), b + pd.Timedelta(days=1))} минут", flush=True)
    U10 = pd.read_parquet(DATA / "universe_daily_top10.parquet")
    days = pd.date_range(a, b, freq="D")
    variants = {
        "наша десятка": (U10.loc[a:b], 10),
        "его список на 16.01.2025 (17 монет)": (pd.DataFrame(1, index=days, columns=p1), len(p1)),
        "его «ядро» 2024–26 (10 монет, задним числом)": (pd.DataFrame(1, index=days, columns=core), len(core)),
    }
    rows, yrs = [], {}
    for name, (U, ncells) in variants.items():
        t0 = time.time()
        R = run_variant(U, name.split()[0] + str(ncells))
        P_ = R["P"]; P_ = P_[(P_.index >= a) & (P_.index <= b + pd.Timedelta(days=3))]
        cap = ncells * 10_000
        eq = cap + P_.cumsum()
        dd = (eq / eq.cummax() - 1).min() * 100
        C = R["C"]
        yrs[name] = {y: P_[P_.index.year == y].sum() / cap * 100 for y in (2025, 2026)}
        by = C.groupby("монета").pnl.sum().sort_values()
        rows.append({"вариант": name, "ячеек по $10k": ncells, "итог, % на вложенное": P_.sum() / cap * 100, "итог на $100k, $": P_.sum() / cap * 100_000,
                     "просадка, %": dd, "сделок": len(C), "в плюс, %": (C.pnl > 0).mean() * 100, "ликвидаций": len(R["liq"]),
                     "хуже всего": ", ".join(f"{nm(m + 'USDT')} {v / 1000:+.1f}k" for m, v in by.head(3).items()),
                     "лучше всего": ", ".join(f"{nm(m + 'USDT')} {v / 1000:+.1f}k" for m, v in by.tail(3)[::-1].items())})
        print(f"{name}: {time.time() - t0:.0f} с; пропущено отрезков без минуток: {R['skipped']}", flush=True)
    L = ["## 8. Бэктест: его список против нашей десятки, 17.01.2025–27.09.2026 (вне периода, где список подсмотрен)\n",
         "Рабочий вариант x1 (правила Kamaz A, без фильтра BTC, пропуск −25%/сутки, лесенка ≤ счёта), по $10k на монету. "
         "«Ядро» выбрано задним числом (монеты, которые он держал и в 2024, и в 2026) — это не честная проверка, а ориентир.\n",
         md_table(pd.DataFrame(rows), 1), "\nПо годам, % на вложенное:\n",
         md_table(pd.DataFrame(yrs).T.reset_index().rename(columns={"index": "вариант"}), 1)]
    (W / "bt2.md").write_text("\n".join(L))
    print("\n".join(L))


def stage_report():
    """KAMAZ_COINS.md = вывод руками (data/kamaz_sel/verdict.md) + сгенерированные разделы."""
    parts = [W / "verdict.md", W / "analysis.md", W / "bt.md", W / "bt2.md"]
    MD.write_text("\n\n".join(f.read_text().rstrip() for f in parts if f.exists()) + "\n")
    print("→", MD)


if __name__ == "__main__":
    args = sys.argv[1:] or ["analyze"]
    if "fetch" in args:
        stage_fetch()
    if "analyze" in args:
        stage_analyze()
    if "bt" in args:
        stage_bt()
    if "bt2" in args:
        stage_bt2()
    if "report" in args:
        stage_report()
