import numpy as np, pandas as pd


def tstat(x):
    """t-статистика среднего против нуля (без scipy — его нет в образе сайта)."""
    x = np.asarray(x, float); n = len(x)
    sd = x.std(ddof=1) if n > 2 else 0.0
    return float(x.mean() / (sd / np.sqrt(n))) if sd > 0 else None


def per_trade(T, col='net'):
    n = T[col]
    return {'сделок': int(len(T)), 'лонгов': int((T.side > 0).sum()), 'шортов': int((T.side < 0).sum()),
            'валовыми_%': round(100 * T.gross.mean(), 4), 'чистыми_%': round(100 * n.mean(), 4),
            'винрейт_%': round(100 * (n > 0).mean(), 1),
            't': round(tstat(n), 2) if tstat(n) is not None else None,
            'профит_фактор': round(float(n[n > 0].sum() / -n[n < 0].sum()), 2) if (n < 0).any() else None}


def curve(E, capital):
    eq = E.equity.reindex(pd.bdate_range(E.index.min(), E.index.max())).ffill()
    r = eq.pct_change().fillna(0)
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    dd = eq / eq.cummax() - 1
    return {'капитал_старт': capital, 'капитал_конец': round(float(eq.iloc[-1])),
            'годовых_%': round(100 * ((eq.iloc[-1] / capital) ** (1 / yrs) - 1), 2),
            'просадка_%': round(100 * float(dd.min()), 2), 'дата_дна': str(dd.idxmin().date()),
            'Шарп': round(float(r.mean() / r.std() * np.sqrt(252)), 2) if r.std() > 0 else 0.0,
            'лет': round(yrs, 2),
            'ГО_макс_%': round(100 * float((E.go_used / E.equity).max()), 1),
            'загрузка_средняя_%': round(100 * float((E.notional / E.equity).mean()), 1)}


def by_year(A, E, capital):
    eq = E.equity; out = {}
    for y, g in eq.groupby(eq.index.year):
        start = eq[eq.index.year < y].iloc[-1] if (eq.index.year < y).any() else capital
        out[int(y)] = round(100 * (g.iloc[-1] / start - 1), 1)
    return out


def by_instrument(T, col='net'):
    g = T.groupby('st')
    return pd.DataFrame({'сделок': g.size(), 'чистыми_%': (100 * g[col].mean()).round(3),
                         'винрейт_%': (100 * g[col].apply(lambda x: (x > 0).mean())).round(1)}
                        ).sort_values('чистыми_%', ascending=False)
