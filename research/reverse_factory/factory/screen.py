"""Воронка отбора целей с витрины Bybit по сохранённому скринингу.

Скрининг собирается во вкладке bybit.com (JS по витрине: список всех трейдеров → история
последних ≤100 ордеров → признаки стиля, описания, подключения) и сохраняется формой
в inbox/bybit_screen_<дата>.json. Здесь — только фильтр и ранжирование, без сети.
"""
from __future__ import annotations

import json
import re

import pandas as pd

LINK = re.compile(r"https?://\S+|www\.\S+|\b[\w-]+\.(?:io|net|app|pro|trade|me|com)\b\S*|t\.me/\S+|github|netlify|myfxbook|tradingview|invvo|since 20\d\d", re.I)


def load(path: str) -> pd.DataFrame:
    d = json.loads(open(path).read())
    S = pd.DataFrame([f for f in d["screened"] if not f.get("empty")])
    intros = d.get("intros") or {}
    S["intro"] = S.mark.map(lambda m: (intros.get(m) or {}).get("intro", ""))
    S["apps"] = S.mark.map(lambda m: (intros.get(m) or {}).get("apps", ""))
    S["links"] = S.intro.map(lambda t: " ".join(sorted(set(x[:50] for x in LINK.findall(t or ""))))[:120])
    num = lambda s: pd.to_numeric(pd.Series(s).astype(str).str.replace(r"[+,%]", "", regex=True), errors="coerce")
    S["roi90"], S["dd90"] = num(S.roi90), num(S.dd90)
    S["payoff"] = S.avgW / S.avgL.where(S.avgL > 0)
    S["bot_sign"] = (S.modeMin >= 0.6) | (S.batch >= 0.3) | S.apps.astype(bool) | (S.intro.fillna("") + " " + S.name.fillna("")).str.contains(r"bot\b|robot|algo|quant|автомат|бот|webhook|strategy", case=False, regex=True)
    return S


def pick(S: pd.DataFrame, max_per_week=10.0, min_pos=10, majors=0.6, min_payoff=1.5, style="any",
         no_averaging=True, top=30) -> pd.DataFrame:
    m = (~S.hidden) & (S.nPos >= min_pos) & (S.perWeek <= max_per_week) & (S.maj >= majors) & (S.exp > 0)
    if min_payoff:
        m &= S.payoff >= min_payoff
    if no_averaging:
        m &= S.multi <= 0.2
    if style == "bot":
        m &= S.bot_sign
    elif style == "human":
        m &= ~S.bot_sign
    cols = ["name", "roi90", "dd90", "nPos", "perWeek", "maj", "top", "holdH", "lev", "short", "win", "avgW", "avgL",
            "modeMinV", "modeMin", "batch", "apps", "links", "mark"]
    out = S[m].copy()
    out["score"] = out.exp * out.nPos ** 0.5
    return out.sort_values("score", ascending=False)[cols].head(top)


def leads_with_links(S: pd.DataFrame) -> pd.DataFrame:
    return S[S.links.astype(bool)][["name", "roi90", "dd90", "nPos", "hidden", "apps", "links", "mark"]]


def clone_farms(S: pd.DataFrame, tol=0.02) -> list[list[str]]:
    """Группы аккаунтов с одинаковыми ROI/просадкой/числом сделок — один бот на много аккаунтов."""
    g = S.dropna(subset=["roi90", "dd90"]).assign(k=lambda x: x.dd90.round(2).astype(str) + "|" + x.nPos.astype(str))
    return [list(v.name) for _, v in g.groupby("k") if len(v) >= 3 and v.roi90.std() < max(tol * 100, 0.5)]
