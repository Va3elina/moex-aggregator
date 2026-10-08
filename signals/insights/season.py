"""Сезонность по всем активам — то же правило, что у витрины /hot (api/services/hot.py: scan_season, season_years,
season_paths, unsplit); завод витрину не импортирует, правило повторено здесь.

Активы: индексы, валюты и золото (SEASON_INDEXES) и акции со связью с фьючерсом (ticker_futures_map.display_name —
ликвидные имена), дневная история от 10 полных лет; сплиты склеены (дневной скачок больше чем в 2,5 раза — не рынок).
Правило: следующие 3 месяца (91 календарный день от той же даты) шли в одну сторону минимум в 75% лет (лет не меньше
10), и медиана по модулю от 3%. Находка — только в день, когда актив ВХОДИТ в условие (накануне не выполнялось), и не
чаще раза в 30 дней на актив: на истории прода это ~35 находок за 4 месяца, около двух в неделю.

Индекс Мосбиржи и доллар здесь не считаются: у них свои детекторы сезонности (detect_seasonality — ход 20/40 торговых
дней, detect_seasonal_curve — сезонные дно и пик), и вторая находка по тем же рядам дала бы повтор темы.
"""
from functools import lru_cache

import numpy as np
import pandas as pd

from signals.insights import data as dbdata

SEASON_INDEXES = (("IMOEX", "Индекс МосБиржи"), ("RTSI", "Индекс РТС"), ("RGBI", "Индекс гособлигаций"),
                  ("USD000UTSTOM", "Доллар"), ("CNYRUB_TOM", "Юань"), ("EUR_RUB__TOM", "Евро"),
                  ("GLDRUB_TOM", "Золото"))
SKIP = {"IMOEX", "USD000UTSTOM"}    # свои детекторы (MIX, Si) — без повтора
DAYS = 91
MIN_YEARS = 10
EDGE = 0.75
MIN_MOVE = 3.0                      # медиана за 3 месяца, %
COOLDOWN = 30                       # дней между находками по одному активу
# слова для поста: «индекс РТС рос», «золото росло»
WORD = {"RTSI": ("индекс РТС", "рос", "падал"), "RGBI": ("индекс гособлигаций", "рос", "падал"),
        "CNYRUB_TOM": ("юань", "рос", "падал"), "EUR_RUB__TOM": ("евро", "рос", "падал"),
        "GLDRUB_TOM": ("золото", "росло", "падало"), "IMOEX": ("индекс Мосбиржи", "рос", "падал"),
        "USD000UTSTOM": ("доллар", "рос", "падал")}


def unsplit(s: pd.Series) -> pd.Series:
    """Склейка сплитов: дневной скачок больше чем в 2,5 раза — не рынок (Норникель 1:100 в 2024); старые цены
    пересчитываем в новый масштаб. Повтор api/services/hot.unsplit."""
    s = s.astype(float)
    k = (s / s.shift(1)).fillna(1.0).values
    jump = np.where((k > 2.5) | (k < 0.4), k, 1.0)
    # цена дня j умножается на произведение скачков ПОСЛЕ него
    after = np.append(np.cumprod(jump[::-1])[::-1][1:], 1.0)
    return pd.Series(s.values * after, index=s.index)


def years_returns(s: pd.Series, t) -> list:
    """Ход окна [та же дата, +91 день] по каждому полному году до года t, % → [(год, ход)]. Повтор hot.season_years."""
    t = pd.Timestamp(t)
    idx = s.index.values
    out = []
    for y in range(s.index[0].year + 1, t.year):
        try:
            d0 = t.replace(year=y)
        except ValueError:          # 29 февраля
            d0 = t.replace(year=y, day=28)
        d1 = d0 + pd.Timedelta(days=DAYS)
        i0 = int(np.searchsorted(idx, np.datetime64(d0), "right")) - 1
        i1 = int(np.searchsorted(idx, np.datetime64(d1), "right")) - 1
        if i0 >= 0 and i1 >= 0 and d0 > s.index[0]:
            out.append((y, round((float(s.iloc[i1]) / float(s.iloc[i0]) - 1) * 100, 1)))
    return out


def condition(s: pd.Series, t) -> dict | None:
    """Выполнено ли правило на дату t → {up, n, med, rising, years} или None."""
    yrs = years_returns(s, t)
    if len(yrs) < MIN_YEARS:
        return None
    vals = [v for _, v in yrs]
    up = sum(v > 0 for v in vals)
    share, med = up / len(vals), float(np.median(vals))
    rising = share >= EDGE and med > 0
    if not (rising or (share <= 1 - EDGE and med < 0)) or abs(med) < MIN_MOVE:
        return None
    return {"up": up, "n": len(vals), "med": med, "rising": rising, "share": share, "years": yrs,
            "hits": up if rising else len(vals) - up}


def entries(s: pd.Series, since, until, cooldown=COOLDOWN) -> list:
    """Дни входа в условие (накануне — торговый день актива — не выполнялось), не чаще раза в cooldown дней.
    Счёт идёт с since: чтобы пауза учитывала находки до окна, since берут раньше окна."""
    since, until = pd.Timestamp(since), pd.Timestamp(until)
    days = s.index[(s.index >= since) & (s.index <= until)]
    out, prev, last = [], None, None
    if len(days):
        j = s.index.get_loc(days[0])
        prev = condition(s, s.index[j - 1]) is not None if j > 0 else False
    for d in days:
        c = condition(s, d)
        if c is not None and not prev and (last is None or (d - last).days >= cooldown):
            out.append((d, c))
            last = d
        prev = c is not None
    return out


GEN = {"RTSI": "индекса РТС", "RGBI": "индекса гособлигаций", "CNYRUB_TOM": "юаня", "EUR_RUB__TOM": "евро",
       "GLDRUB_TOM": "золота", "IMOEX": "индекса Мосбиржи", "USD000UTSTOM": "доллара"}


def of(code: str, name: str) -> str:
    """Родительный падеж для «путь …»: «золота», «акций «Норильский никель»»."""
    return GEN.get(code, f"акций «{name}»")


def word(code: str, name: str) -> tuple:
    """(подлежащее, «рос», «падал»)."""
    return WORD.get(code, (name, "рос", "падал"))


def title(code: str, name: str, c: dict) -> str:
    subj, up, down = word(code, name)
    med = f"{c['med']:+.1f}".replace(".", ",").replace("-", "−")
    return (f"Сезонность: {name} — следующие 3 месяца {up if c['rising'] else down} в {c['hits']} из {c['n']} лет, "
            f"обычно {med}%")


def score(c: dict) -> float:
    """Балл как у прежних находок сезонности (4–9): доля лет и размер хода."""
    return 4 + abs(c["share"] - 0.5) * 6 + min(abs(c["med"]), 10.0) / 100 * 20


@lru_cache(None)
def assets() -> tuple:
    """((код, имя, ряд склеенных закрытий), …): индексы/валюты/золото и акции со связью с фьючерсом."""
    out = []
    ix = dbdata.read("season_index", parse_dates=["d"])
    names = dict(SEASON_INDEXES)
    for code, g in ix.groupby("secid"):
        s = g.drop_duplicates("d", keep="last").set_index("d")["close"].sort_index()
        out.append((code, names.get(code, code), unsplit(s)))
    st = dbdata.read("season_stocks", parse_dates=["d"])
    for code, g in st.groupby("secid"):
        s = g.drop_duplicates("d", keep="last").set_index("d")["close"].sort_index()
        out.append((code, str(g["display_name"].iloc[0]), unsplit(s)))
    return tuple(out)


def series(code: str):
    """(имя, ряд) по коду или (None, None)."""
    for c, name, s in assets():
        if c == code:
            return name, s
    return None, None


def paths(s: pd.Series) -> dict:
    """Путь каждого года по календарю: Series по дню года (0 — 1 января), % от первого закрытия года. По календарю, а не
    по торговым дням: с торгами выходного дня сессий в году стало больше, и счёт по ним съезжал (hot.season_paths)."""
    out = {}
    for y, g in s.groupby(s.index.year):
        doy = (g.index - pd.Timestamp(y, 1, 1)).days
        p = pd.Series((g.values / g.values[0] - 1) * 100, index=doy).groupby(level=0).last()
        out[int(y)] = p.reindex(range(0, int(doy[-1]) + 1)).ffill()
    return out


def median_path(s: pd.Series, year: int) -> pd.Series:
    """Средний путь — медиана по полным годам до year на каждый календарный день (один выдающийся год не тянет путь);
    день берётся, если он есть хотя бы у 70% лет."""
    full = [p for y, p in paths(s[s.index.year < year]).items() if p.index[-1] >= 355]
    if not full:
        return pd.Series(dtype=float)
    m = pd.concat(full, axis=1)
    ok = m.notna().sum(axis=1) >= 0.7 * len(full)
    return m[ok].median(axis=1)


def smooth_path(p: pd.Series, window: int = 7) -> pd.Series:
    """Медианный путь без зубцов: скользящее среднее по 7 календарным дням с центром. Декабрь переходит в январь, как в
    detect.seasonal_curve; путь не очищен от тренда, поэтому хвосты стыкуются по уровню: перед 1 января — конец пути
    минус итог года, после 31 декабря — начало плюс итог."""
    if p.empty:
        return p
    q = p.reindex(range(0, 365)).interpolate(limit_direction="both")
    h, total = window // 2, float(q.iloc[-1])
    ext = np.concatenate([q.values[-h:] - total, q.values, q.values[:h] + total])
    sm = pd.Series(ext).rolling(window, center=True, min_periods=1).mean().values[h:-h]
    return pd.Series(sm, index=q.index)
