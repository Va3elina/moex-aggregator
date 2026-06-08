"""
Build & save calibration JSON для production ensemble-детектора.

Загружает /tmp/research_v2_raw.csv (output explore_v2), применяет
production scoring (signals.scoring) и classification (signals.classification),
строит calibration table per mode (meanrev / momentum), сохраняет в
signals/data/calibration.json.

Должен запускаться после R&D phase (когда новые данные / scoring изменился).
Production detector тогда сразу подхватит новую калибровку.

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.build_calibration
"""
from __future__ import annotations
import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from signals import calibration, classification, scoring


CSV_PATH = "/tmp/research_v2_raw.csv"


def to_float(v):
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def to_bool(v):
    return v == "True"


def parse_features_row(r: dict) -> dict:
    """Конвертирует CSV row в feature dict для scoring."""
    return {
        "is_new_high_30": to_bool(r.get("is_new_high_30")),
        "is_new_low_30": to_bool(r.get("is_new_low_30")),
        "is_new_high_90": to_bool(r.get("is_new_high_90")),
        "is_new_low_90": to_bool(r.get("is_new_low_90")),
        "ema20_above_ema50": to_bool(r.get("ema20_above_ema50")),
        "channel_pos": to_float(r.get("channel_pos")),
        "price_vs_ema20_pct": to_float(r.get("price_vs_ema20_pct")),
        "price_vs_ema50_pct": to_float(r.get("price_vs_ema50_pct")),
        "daily_range_pct": to_float(r.get("daily_range_pct")),
        "atr_pct": to_float(r.get("atr_pct")),
        "oi_price_corr_20d": to_float(r.get("oi_price_corr_20d")),
        "fiz_net_change_5d": to_float(r.get("fiz_net_change_5d")),
        "yur_net_change_5d": to_float(r.get("yur_net_change_5d")),
        "fiz_avg_long_size": to_float(r.get("fiz_avg_long_size")),
        "fiz_long_share": to_float(r.get("fiz_long_share")),
        "yur_part_balance": to_float(r.get("yur_part_balance")),
        "volume_z": to_float(r.get("volume_z")),
    }


def main():
    print(f"Loading {CSV_PATH}...")
    rows = []
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            sectype = r["sectype"]
            cls = classification.classify(sectype)
            if cls.mode not in {"meanrev", "momentum"}:
                continue
            feat = parse_features_row(r)
            score = scoring.compute_score(feat, cls.mode)
            fwd = to_float(r.get("fwd_return"))
            if fwd is None:
                continue
            r["_score"] = score
            r["_group"] = cls.mode
            r["_fwd"] = fwd
            rows.append(r)

    print(f"Processed {len(rows)} rows.")

    # Build calibration table
    from collections import defaultdict
    by_group_count = defaultdict(int)
    for r in rows:
        by_group_count[r["_group"]] += 1
    print(f"  per group: {dict(by_group_count)}")

    metadata = {
        "built_at": datetime.utcnow().isoformat(),
        "csv_source": CSV_PATH,
        "n_total": len(rows),
        "n_per_group": dict(by_group_count),
        "fwd_horizon_days": 20,
        "move_threshold_pct": 5.0,
        "scoring_version": "v3-type-aware",
    }
    table = calibration.build_from_rows(rows, move_threshold_pct=5.0, metadata=metadata)

    # Save
    path = calibration.save_table(table)
    print(f"\n✓ Saved calibration → {path}")

    # Show what's inside
    print("\n--- Sample of calibration ---")
    for group in ["meanrev", "momentum"]:
        print(f"\n{group}:")
        for bucket, stats in sorted(table[group].items(), key=lambda kv: int(kv[0])):
            print(f"  bucket [{bucket}]: n={stats['n']:>4}, "
                  f"P↑{stats['p_rise']:>5.1f}%, P↓{stats['p_fall']:>5.1f}%, "
                  f"avg{stats['avg_ret']:+6.2f}%")


if __name__ == "__main__":
    main()
