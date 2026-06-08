"""
Calibration table — bucket score → empirical probability.

Хранится в `signals/data/calibration.json` — это **рабочая** таблица для
production-detector'а. Структура:

  {
    "meanrev": {
      "-3": {"n": 30, "p_rise": 10.0, "p_fall": 30.0, "avg_ret": -4.55},
      "-2": {"n": 124, "p_rise": 17.7, "p_fall": 40.3, "avg_ret": -4.16},
      ...
      "+4": {"n": 45, "p_rise": 57.8, "p_fall": 8.9, "avg_ret": +10.52}
    },
    "momentum": {
      ...
    },
    "metadata": {
      "built_at": "2026-05-19T22:00:00",
      "lookback_days": 730,
      "n_assets": 57,
      "fwd_horizon_days": 20,
      "move_threshold_pct": 5.0
    }
  }

Перестраивать через `signals.calibration.builder.build()` после получения
свежих данных (recommended: раз в месяц).
"""
from __future__ import annotations
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


CALIBRATION_FILE = Path(__file__).parent / "data" / "calibration.json"


@dataclass(frozen=True)
class BucketStats:
    """Эмпирическая статистика для bucket."""
    n: int           # сколько раз наблюдалось
    p_rise: float    # % случаев когда fwd_ret > +5%
    p_fall: float    # % случаев когда fwd_ret < -5%
    avg_ret: float   # средний fwd_ret в %


def _load_table() -> dict:
    """Загрузить JSON калибровочную таблицу. Файл может не существовать."""
    if not CALIBRATION_FILE.exists():
        return {"meanrev": {}, "momentum": {}, "metadata": {}}
    with CALIBRATION_FILE.open() as f:
        return json.load(f)


def lookup(score: float, mode: str) -> Optional[BucketStats]:
    """Найти статистику для score в данной модели.

    Bucket = floor(score). Если данных не было — вернёт None.
    """
    table = _load_table()
    mode_tbl = table.get(mode, {})
    bucket = math.floor(score)
    raw = mode_tbl.get(str(bucket))
    if raw is None:
        return None
    return BucketStats(
        n=int(raw["n"]),
        p_rise=float(raw["p_rise"]),
        p_fall=float(raw["p_fall"]),
        avg_ret=float(raw["avg_ret"]),
    )


def is_publishable(stats: Optional[BucketStats], direction: str,
                   *, min_probability: float = 30.0, min_n: int = 30) -> bool:
    """Стоит ли публиковать сигнал с такими статистиками.

    Args:
        stats: результат lookup() или None
        direction: 'bull' / 'bear' — определяет какую вероятность смотрим
        min_probability: минимальный % успеха (default 30 — выше базового rate ~20%)
        min_n: минимальное количество исторических случаев (для надёжности)
    """
    if stats is None:
        return False
    if stats.n < min_n:
        return False
    if direction == "bull":
        return stats.p_rise >= min_probability
    if direction == "bear":
        return stats.p_fall >= min_probability
    return False


def get_metadata() -> dict:
    """Метаданные текущей калибровки (когда построена, сколько активов, etc.)."""
    return _load_table().get("metadata", {})


# ───────────────────────────────────────────────────────────────
# Builder — пересчитать calibration из rows
# ───────────────────────────────────────────────────────────────
def build_from_rows(
    rows: list[dict],
    *,
    move_threshold_pct: float = 5.0,
    metadata: Optional[dict] = None,
) -> dict:
    """Построить calibration из rows формата `_score`, `_fwd`, `_group`.

    Каждый row должен иметь:
      - _score: float (output compute_score)
      - _fwd: float (fwd_return в %)
      - _group: 'meanrev' / 'momentum'
    """
    from collections import defaultdict
    from statistics import fmean

    by_group_bucket: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        score = r.get("_score")
        fwd = r.get("_fwd")
        group = r.get("_group")
        if score is None or fwd is None or group not in {"meanrev", "momentum"}:
            continue
        bucket = math.floor(score)
        by_group_bucket[group][bucket].append(fwd)

    out: dict = {"meanrev": {}, "momentum": {}}
    for group, buckets in by_group_bucket.items():
        for b, returns in buckets.items():
            n = len(returns)
            rise = sum(1 for r in returns if r > move_threshold_pct) / n if n else 0
            fall = sum(1 for r in returns if r < -move_threshold_pct) / n if n else 0
            out[group][str(b)] = {
                "n": n,
                "p_rise": round(rise * 100, 1),
                "p_fall": round(fall * 100, 1),
                "avg_ret": round(fmean(returns), 2) if n else 0,
            }
    out["metadata"] = metadata or {}
    return out


def save_table(table: dict) -> Path:
    """Сохранить calibration в JSON. Перезаписывает существующий."""
    CALIBRATION_FILE.parent.mkdir(parents=True, exist_ok=True)
    with CALIBRATION_FILE.open("w") as f:
        json.dump(table, f, indent=2, ensure_ascii=False)
    return CALIBRATION_FILE
