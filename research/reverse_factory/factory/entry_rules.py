"""Стадия 5. Правило входа: на каких свечах цель входит и почему.

Работает, когда (1) у входов есть сетка свечей (timing.grid_tf_min) или задан таймфрейм,
(2) позиций ≥ 60, (3) позиции НЕ неттированы. Шаги (обобщение разбора Meridian):
  1. свеча сигнала = последняя закрытая свеча перед входом;
  2. признаки каждой свечи каждой монеты + признаки BTC (режим рынка);
  3. «подпись» — чем свечи входа отличаются от обычных (размер эффекта по медианам);
  4. перебор библиотеки правил (растёт: добавляйте найденные у новых целей семейства);
  5. дерево решений на ориентированных признаках;
  6. честная проверка: лучшее правило выбирается на первых 2/3 входов, оценивается на последней 1/3.
Метрики правила: охват (какую долю входов цели ловит), точность (какая доля срабатываний —
реальные входы; считается только на свечах, где цель вне позиции по монете), F1.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier, export_text

from . import market

MIN_POS = 60


# ─── индикаторы ───────────────────────────────────────────────────────────────
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def rsi(s, n):
    d = s.diff()
    u = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    v = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + u / v)


def atr(k, n):
    tr = pd.concat([k.h - k.l, (k.h - k.c.shift()).abs(), (k.l - k.c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def supertrend(k, n, m):
    a = atr(k, n)
    hl = (k.h + k.l) / 2
    up, dn, c = (hl - m * a).values, (hl + m * a).values, k.c.values
    fu, fd, d = up.copy(), dn.copy(), np.ones(len(c))
    for i in range(1, len(c)):
        fu[i] = max(up[i], fu[i - 1]) if c[i - 1] > fu[i - 1] else up[i]
        fd[i] = min(dn[i], fd[i - 1]) if c[i - 1] < fd[i - 1] else dn[i]
        d[i] = 1 if c[i] > fd[i - 1] else (-1 if c[i] < fu[i - 1] else d[i - 1])
    return pd.Series(d, k.index)


def xup(a, b):
    return (a > b) & (a.shift() <= b.shift())


# ─── признаки свечи ──────────────────────────────────────────────────────────
def features(k: pd.DataFrame) -> pd.DataFrame:
    c = k.c
    f = pd.DataFrame(index=k.index)
    for n in [1, 4, 12, 28]:
        f[f"ход {n} св %"] = 100 * (c / c.shift(n) - 1)
    f["тело %"] = 100 * (c / k.o - 1)
    a = atr(k, 14)
    f["тело/ATR"] = (c - k.o) / a
    f["ATR %"] = 100 * a / c
    f["объём/ср20"] = k.v / k.v.rolling(20).mean()
    f["объём/ср50"] = k.v / k.v.rolling(50).mean()
    for n in [20, 55]:
        lo, hi = k.l.rolling(n).min(), k.h.rolling(n).max()
        f[f"место в канале {n}"] = (c - lo) / (hi - lo)
    for n in [20, 50, 200]:
        e = ema(c, n)
        f[f"от EMA{n} %"] = 100 * (c / e - 1)
        f[f"наклон EMA{n} %"] = 100 * (e / e.shift(4) - 1)
    f["RSI7"] = rsi(c, 7)
    f["RSI14"] = rsi(c, 14)
    f["Supertrend10/2"] = supertrend(k, 10, 2)
    return f


def rules(k: pd.DataFrame) -> dict[str, tuple[pd.Series, pd.Series]]:
    """Библиотека правил: имя → (сигнал лонг, сигнал шорт). ДОПОЛНЯТЬ новыми семействами."""
    c, R = k.c, {}
    for n in [10, 20, 30, 50, 100, 200]:
        e = ema(c, n)
        R[f"цена×EMA{n}"] = (xup(c, e), xup(e, c))
    for f_, s_ in [(5, 20), (9, 21), (12, 26), (20, 50), (50, 200)]:
        a, b = ema(c, f_), ema(c, s_)
        R[f"EMA{f_}×EMA{s_}"] = (xup(a, b), xup(b, a))
    for n in [5, 10, 20, 55, 100]:
        hh, ll = k.h.rolling(n).max().shift(), k.l.rolling(n).min().shift()
        R[f"пробой {n}"] = (xup(c, hh), xup(ll, c))
    for n in [7, 14]:
        r = rsi(c, n)
        for lv in [30, 50, 70]:
            L = pd.Series(lv, k.index)
            R[f"RSI{n}×{lv} (по ходу)"] = (xup(r, L), xup(100 - L, r))
        R[f"RSI{n} выход из зоны 30/70 (против)"] = (xup(r, pd.Series(30, k.index)), xup(pd.Series(70, k.index), r))
    m = ema(c, 12) - ema(c, 26)
    sg = ema(m, 9)
    R["MACD×сигнал"] = (xup(m, sg), xup(sg, m))
    for n, mm in [(7, 2), (10, 2), (10, 3), (14, 2)]:
        d = supertrend(k, n, mm)
        R[f"Supertrend{n}/{mm}"] = ((d == 1) & (d.shift() == -1), (d == -1) & (d.shift() == 1))
    mid, sd = c.rolling(20).mean(), c.rolling(20).std()
    R["Боллинджер 20×2 пробой"] = (xup(c, mid + 2 * sd), xup(mid - 2 * sd, c))
    R["Боллинджер 20×2 возврат"] = (xup(c, mid - 2 * sd), xup(mid + 2 * sd, c))
    # семейство «импульс с объёмом» — найдено у Meridian (26.09.2026)
    body = 100 * (c / k.o - 1)
    vr = k.v / k.v.rolling(20).mean()
    pos20 = (c - k.l.rolling(20).min()) / (k.h.rolling(20).max() - k.l.rolling(20).min())
    for b in [0.8, 1.2, 2.0]:
        for v in [1.2, 1.5]:
            R[f"импульс тело≥{b}% объём≥{v}× у края канала"] = ((body >= b) & (vr >= v) & (pos20 >= 0.6),
                                                                  (body <= -b) & (vr >= v) & (pos20 <= 0.4))
    for n in [20, 30, 50]:
        e = ema(c, n)
        R[f"импульс объём≥1.2× + цена×EMA{n}"] = (xup(c, e) & (vr >= 1.2) & (body > 0), xup(e, c) & (vr >= 1.2) & (body < 0))
    return R


def regimes(btc: pd.DataFrame) -> dict[str, pd.Series]:
    c = btc.c
    return {"—": None, "BTC>EMA200": c > ema(c, 200), "BTC>EMA50": c > ema(c, 50),
            "BTC свеча того же цвета": c > btc.o}


# ─── основная функция ────────────────────────────────────────────────────────
def run(p: pd.DataFrame, fp: dict, meta: dict, tf_min: int | None = None, max_syms: int = 30) -> dict:
    tf_min = tf_min or (fp["timing"] or {}).get("grid_tf_min")
    if meta.get("netted_suspect"):
        return dict(skipped="позиции неттированы (несколько стратегий в одном счёте) — по сделкам правило не восстановить, смотрите стадию «копия по кривой»")
    if not tf_min:
        return dict(skipped="у входов нет сетки свечей — входы лимитками/вручную или по тикам; укажите таймфрейм вручную (--tf)")
    if len(p) < MIN_POS:
        return dict(skipped=f"мало позиций ({len(p)} < {MIN_POS})")
    interval = market.tf_to_interval(tf_min)
    step = pd.Timedelta(minutes=market.MIN[interval])
    p = p.copy()
    p["sig"] = p.t_open.dt.floor(step) - step
    vc = p.sym.value_counts()
    syms = [s for s in vc.index if vc[s] >= 3][:max_syms]
    a, b = p.t_open.min() - step * 300, p.t_open.max() + step
    # не больше ~2 лет истории на 15-минутках и мельче
    if (b - a) / step > 70000:
        a = b - step * 70000
        p = p[p.t_open >= a + step * 300]
    btc = market.klines("BTCUSDT", interval, a, b)
    reg = regimes(btc)
    split_t = p.t_open.sort_values().iloc[int(len(p) * 2 / 3)]
    stats, prof, tree_rows = {}, [], []
    n_true = {"all": 0, "train": 0, "test": 0}
    for sym in syms:
        k = market.klines(sym, interval, a, b)
        if len(k) < 300:
            continue
        t = p[p.sym == sym]
        f = features(k)
        fb = features(btc).add_prefix("BTC ").reindex(k.index)
        # вне позиции по монете на момент закрытия свечи
        close_t = k.index + step
        flat = pd.Series(True, k.index)
        for _, x in t.iterrows():
            t1 = x.t_close if pd.notna(x.t_close) else b
            flat[(close_t > x.t_open) & (close_t < t1)] = False
        lab = pd.Series(0, k.index)
        for _, x in t.iterrows():
            if x.sig in lab.index:
                lab[x.sig] = x.side
        inwin = (k.index >= p.t_open.min() - step) & (k.index <= p.t_open.max())
        base = flat & inwin
        train = k.index < split_t
        for part, m in [("all", base), ("train", base & train), ("test", base & ~train)]:
            n_true[part] += int((lab[m] != 0).sum())
        # подпись
        F = f.join(fb)
        F["lab"] = lab
        prof.append(F[base])
        # правила × режим
        for rname, (L, S) in rules(k).items():
            L, S = L.fillna(False).astype(bool), S.fillna(False).astype(bool)
            for gname, g in reg.items():
                if g is None:                       # без фильтра режима
                    L2, S2 = L, S
                else:                               # лонг — только когда режим «вверх», шорт — когда «вниз»
                    gl = g.reindex(k.index).fillna(False).astype(bool)
                    L2, S2 = L & gl, S & ~gl
                key = f"{rname} | {gname}"
                acc = stats.setdefault(key, {"all": [0, 0], "train": [0, 0], "test": [0, 0]})
                for part, m in [("all", base), ("train", base & train), ("test", base & ~train)]:
                    hit = int(((L2 & m) & (lab == 1)).sum() + ((S2 & m) & (lab == -1)).sum())
                    acc[part][0] += hit
                    acc[part][1] += int((L2 & m).sum() + (S2 & m).sum())
        # для дерева: ориентируем по цвету свечи
        sgn = np.sign(f["тело %"]).replace(0, 1)
        O = f.join(fb).copy()
        for col in O.columns:
            if "канал" in col:
                O[col] = np.where(sgn > 0, O[col], 1 - O[col])
            elif any(s in col for s in ["ход", "тело", "от EMA", "наклон", "Supertrend"]):
                O[col] = O[col] * sgn
            elif "RSI" in col:
                O[col] = np.where(sgn > 0, O[col], 100 - O[col])
        O["y"] = ((lab == sgn) & (lab != 0)).astype(int)
        O["train"] = train
        tree_rows.append(O[base])

    if not stats:
        return dict(skipped="нет свечей по монетам цели")

    def f1(h, n, tot):
        rec, prec = h / max(tot, 1), h / max(n, 1)
        return rec, prec, (2 * rec * prec / (rec + prec) if rec + prec else 0.0)

    rows = []
    for key, acc in stats.items():
        r_all = f1(*acc["all"], n_true["all"])
        r_tr = f1(*acc["train"], n_true["train"])
        r_te = f1(*acc["test"], n_true["test"])
        rows.append(dict(правило=key, совпало=acc["all"][0], сработало=acc["all"][1], охват=round(r_all[0], 2),
                         точность=round(r_all[1], 2), F1=round(r_all[2], 3), F1_подбор=round(r_tr[2], 3), F1_проверка=round(r_te[2], 3)))
    R = pd.DataFrame(rows).sort_values("F1", ascending=False)
    best_tr = R.sort_values("F1_подбор", ascending=False).iloc[0]

    # подпись
    P = pd.concat(prof)
    sig = []
    allm = P[P.lab == 0]
    for col in [c for c in P.columns if c != "lab"]:
        base_med, iqr = allm[col].median(), allm[col].quantile(.75) - allm[col].quantile(.25)
        if not iqr or np.isnan(iqr):
            continue
        for side, name in [(1, "лонг"), (-1, "шорт")]:
            x = P[P.lab == side][col]
            if len(x) >= 8:
                sig.append(dict(признак=col, сторона=name, медиана_входов=round(float(x.median()), 2),
                                p10=round(float(x.quantile(.1)), 2), p90=round(float(x.quantile(.9)), 2),
                                медиана_обычных=round(float(base_med), 2), эффект=round(float((x.median() - base_med) / iqr), 2)))
    SIG = pd.DataFrame(sig)
    if len(SIG):
        SIG = SIG.reindex(SIG.эффект.abs().sort_values(ascending=False).index)

    # дерево
    T = pd.concat(tree_rows).replace([np.inf, -np.inf], np.nan).fillna(0)
    Xc = [c for c in T.columns if c not in ("y", "train")]
    tree_txt, tree_q = "", {}
    if T.y.sum() >= 20:
        tr_, te_ = T[T.train], T[~T.train]
        clf = DecisionTreeClassifier(max_depth=3, min_samples_leaf=max(10, int(T.y.sum() * 0.05)),
                                     class_weight="balanced", random_state=0).fit(tr_[Xc], tr_.y)
        tree_txt = export_text(clf, feature_names=Xc, decimals=2)
        if len(te_) and te_.y.sum():
            pr = clf.predict(te_[Xc])
            hit = int(((pr == 1) & (te_.y == 1)).sum())
            rec, prec = hit / te_.y.sum(), hit / max(pr.sum(), 1)
            tree_q = dict(охват=round(float(rec), 2), точность=round(float(prec), 2), F1=round(float(2 * rec * prec / (rec + prec)), 3) if rec + prec else 0.0)
    best = R.iloc[0]
    verdict = ("правило найдено" if best_tr.F1_проверка >= 0.6 else
               "правило найдено частично" if best_tr.F1_проверка >= 0.3 else "правило не найдено")
    return dict(interval=interval, symbols=len([s for s in syms]), entries=n_true["all"],
                best=dict(best), best_by_train=dict(best_tr), verdict=verdict, tree_test=tree_q,
                _rules=R, _signature=SIG, _tree=tree_txt)
