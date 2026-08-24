"""
Asset classification — 4 уровня (group / subtype / tier / mode).

Используется ensemble-детектором: для каждого актива определяет какую
scoring модель применить (mean-reversion vs momentum) и какую калибровочную
таблицу использовать.

Уровни:
  1. `group` — из БД instruments.group (Акции / Сырьё / Валюта / Индексы).
     Высокоуровневая категоризация Алгопака.
  2. `subtype` — мануальное уточнение (Сырьё → metal/energy/soft, Валюта → fx_rub/fx_cross).
     БД не различает эти подтипы, но они принципиально по-разному ведут.
  3. `tier` — 1 (top liquid) → 5 (tail) по avg daily volume. Модифицирует
     пороги — для tier 1 ставим conservative thresholds, для tier 4-5 не публикуем.
  4. `mode` — meanrev / momentum. Главное решение какая scoring логика.

Subtype/tier mappings вручную (basis from research_v2 query + memory):
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


# ───────────────────────────────────────────────────────────────
# Subtype mapping — внутри `group` дополнительно разделяет
# ───────────────────────────────────────────────────────────────
SUBTYPE_MAP: dict[str, str] = {
    # ── Валюта (group="Валюта") разделяется по типу пары ──
    # FX RUB — пары к рублю (momentum, в РФ это устойчивый trend)
    "USDRUBF": "fx_rub", "EURRUBF": "fx_rub", "CNYRUBF": "fx_rub",
    "Si": "fx_rub", "Eu": "fx_rub", "CR": "fx_rub",
    "TY": "fx_rub", "I2": "fx_rub", "BY": "fx_rub", "AR": "fx_rub",
    "UM": "fx_rub", "ER": "fx_rub",
    # FX cross — не-рублевые валютные пары (тоже momentum, но иная динамика)
    "ED": "fx_cross", "AU": "fx_cross", "UC": "fx_cross",
    "MY": "fx_cross",

    # ── Индексы — фондовые индексы (mean-reversion, российский рынок) ──
    "MX": "index", "MM": "index", "RI": "index",
    "IMOEXF": "index", "IB": "index", "VI": "index",
    "SF": "index",  # фьючерс на индекс S&P 500 (зарубежный индекс)
    # Зарубежные индексы/ETF. DX раньше был Dollar Index (fx_cross) —
    # с 2026 MOEX использует код DX для фьючерса на DAX.
    "NA": "index", "QQQF": "index", "SP500F": "index", "DJ": "index",
    "DX": "index", "N2": "index", "HS": "index", "SX": "index",
    "R2": "index", "SQ": "index", "TL": "index", "EM": "index",
    # Страновые MSCI ETF
    "AA": "index", "AG": "index", "BZ": "index", "CI": "index",
    "ND": "index", "KR": "index", "SD": "index",
    # Ставки/облигации
    "RGBIF": "index", "RF": "index",
    # Крипто — как IB/BT (индексные прокси)
    "EH": "index", "ET": "index", "S3": "index", "XR": "index", "TX": "index",

    # ── Сырьё (group="Сырьё") разделяется на металлы/энерго/с.х. ──
    # Metals — драгоценные металлы (momentum, gold-style trends)
    "GD": "metal", "GL": "metal", "GLDRUBF": "metal",
    "SV": "metal", "PT": "metal", "PD": "metal", "HK": "metal",
    "GN": "metal", "S1": "metal", "S2": "metal", "SLVRUBF": "metal",
    "LT": "metal", "LD": "metal",
    # Energy — нефть/газ (mean-reversion, кризисные циклы)
    "BR": "energy", "BM": "energy", "NG": "energy",
    "FF": "energy", "NR": "energy",
    "WT": "energy", "92": "energy", "95": "energy", "DL": "energy",
    # Soft — с.х. товары (mean-reversion, сильные циклы спрос-предложение)
    "CC": "soft", "KC": "soft", "OJ": "soft", "W4": "soft",
    "Su": "soft",  # сахар (Su-серия; SA-серия классифицируется по group)
}


# ───────────────────────────────────────────────────────────────
# Tier by avg daily volume contracts (calculated from БД query)
# 1 = top liquid (>100K), 2 = high (30-100K), 3 = mid (10-30K),
# 4 = low (1-10K), 5 = tail (<1K).
# ───────────────────────────────────────────────────────────────
TIER_MAP: dict[str, int] = {
    # ── Tier 1 — top liquid (>100K avg volume contracts) ──
    "CNYRUBF": 1, "CR": 1, "IMOEXF": 1, "GLDRUBF": 1, "NR": 1,
    "NG": 1, "Si": 1, "CC": 1, "SV": 1, "USDRUBF": 1,
    # ── Tier 2 — high (30-100K) ──
    "GK": 2, "IB": 2, "VB": 2, "MM": 2, "GD": 2, "GZ": 2,
    "SR": 2, "SS": 2, "GL": 2, "BR": 2, "MX": 2, "KC": 2,
    "FF": 2, "ED": 2, "Eu": 2,
    # ── Tier 3 — mid (10-30K) ──
    "RB": 3, "YD": 3, "RI": 3, "UC": 3, "SF": 3,
    "X5": 3, "BM": 3, "MN": 3, "PT": 3, "CE": 3, "SE": 3,
    "SBERF": 3, "PD": 3, "TN": 3,
    # ── Tier 4 — low (1-10K) ──
    "EURRUBF": 4, "RL": 4, "AF": 4, "LK": 4, "AL": 4,
    "HS": 4, "MC": 4, "ME": 4, "NM": 4, "RM": 4, "SP": 4,
    "SN": 4, "RN": 4, "TT": 4, "HY": 4, "MG": 4, "NK": 4,
    "OJ": 4, "NA": 4,
    # ── Tier 5 — tail (<1K) ──
    "DX": 5, "CH": 5, "MY": 5, "VI": 5, "W4": 5,
    "HK": 5, "AU": 5,
}


# ───────────────────────────────────────────────────────────────
# Mode mapping — какая scoring модель применяется
# ───────────────────────────────────────────────────────────────
MEANREV_SUBTYPES = {"equity", "index", "soft", "energy"}
MOMENTUM_SUBTYPES = {"fx_rub", "fx_cross", "metal"}


# Минимальный tier для publishing (tier 4-5 → не публикуем — слишком шумные)
MIN_PUBLISH_TIER = 3


@dataclass(frozen=True)
class AssetClass:
    """Полная классификация одного актива."""
    sectype: str
    name: Optional[str]    # human-readable name (если есть)
    group: str             # из БД (Акции / Сырьё / Валюта / Индексы / Unknown)
    subtype: str           # equity / index / fx_rub / fx_cross / metal / energy / soft / unknown
    tier: int              # 1 (top) → 5 (tail)
    mode: str              # meanrev / momentum / neutral

    @property
    def publishable(self) -> bool:
        """Можно ли публиковать сигналы по этому активу (по tier)."""
        return self.tier <= MIN_PUBLISH_TIER

    @property
    def display_label(self) -> str:
        """Краткая метка для логов: 'SR(equity/tier2/meanrev)'."""
        return f"{self.sectype}({self.subtype}/tier{self.tier}/{self.mode})"


# ───────────────────────────────────────────────────────────────
# Inference helpers
# ───────────────────────────────────────────────────────────────
def _infer_subtype(sectype: str, group: Optional[str]) -> str:
    """Subtype: сначала manual map, потом из group, потом fallback на equity.

    Default = 'equity' потому что в нашей вселенной (ALGOPACK_OI_TICKERS)
    non-equity активы (FX, metals, energy, soft) явно перечислены в SUBTYPE_MAP.
    Всё остальное — российские акции.
    """
    if sectype in SUBTYPE_MAP:
        return SUBTYPE_MAP[sectype]
    if group == "Индексы":
        return "index"
    if group == "Валюта":
        return "fx_rub"
    if group == "Сырьё":
        return "energy"
    # Default fallback (включая group == "Акции" и unknown group)
    return "equity"


def _infer_mode(subtype: str) -> str:
    if subtype in MEANREV_SUBTYPES:
        return "meanrev"
    if subtype in MOMENTUM_SUBTYPES:
        return "momentum"
    return "neutral"


def classify(
    sectype: str,
    *,
    name: Optional[str] = None,
    group: Optional[str] = None,
) -> AssetClass:
    """Build AssetClass для актива. `group` и `name` — из БД instruments.

    Если group не передан, infer'нем по subtype mapping или fallback на 'unknown'.
    """
    subtype = _infer_subtype(sectype, group)
    tier = TIER_MAP.get(sectype, 5)
    mode = _infer_mode(subtype)
    # Если group не задан, попытаемся вывести из subtype
    if not group:
        group_inferred = {
            "equity": "Акции", "index": "Индексы",
            "fx_rub": "Валюта", "fx_cross": "Валюта",
            "metal": "Сырьё", "energy": "Сырьё", "soft": "Сырьё",
        }.get(subtype, "Unknown")
        group = group_inferred
    return AssetClass(
        sectype=sectype,
        name=name,
        group=group,
        subtype=subtype,
        tier=tier,
        mode=mode,
    )
