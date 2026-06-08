"""
Showcase — публикует 20 пар постов в Frame_Signal для демо новой системы.

Для каждого недавнего сигнала:
  1. Пост с картинкой (chart) и caption — как было бы в реальности
  2. Через 3 секунды — пост с результатом (что произошло через 20 дней)
  3. Пауза 5 секунд перед следующей парой

Использует type-aware scoring v3 (откатные / трендовые) и эмпирическую
калибровку из /tmp/research_v3_calibration.json.

Запуск:
  cd /opt/frame && /opt/frame/signals/.venv/bin/python -m signals.research.showcase
"""
from __future__ import annotations
import csv
import json
import math
import sys
import time
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from signals import config
from signals.render.chart import render_signal_chart, render_result_chart
from signals.publish.telegram import send_signal_post
from signals.research.explore_v3 import (
    TYPE_MAP, MEANREV_TYPES, MOMENTUM_TYPES,
    dispatch_score, to_float, to_bool,
)

import requests

CSV_PATH = "/tmp/research_v2_raw.csv"
CALIBRATION_PATH = "/tmp/research_v3_calibration.json"

# Human-readable asset names (из БД instruments были бы лучше, но для showcase mapping ok)
ASSET_NAMES = {
    "SR": "Сбербанк", "GZ": "Газпром", "LK": "Лукойл", "VB": "ВТБ",
    "GK": "Норильский никель", "AL": "Алроса", "AF": "Аэрофлот",
    "CH": "Северсталь", "HY": "РусГидро", "MC": "Мечел",
    "ME": "Мосбиржа", "MN": "Магнит", "NM": "НЛМК",
    "NK": "Новатэк", "RN": "Роснефть", "SN": "Сургутнефтегаз",
    "SP": "Сбербанк (прив)", "SS": "Самолет", "TN": "Татнефть",
    "TT": "Татнефть (прив)", "X5": "X5 Group", "YD": "Яндекс",
    "RB": "Газпромнефть", "RL": "Россети", "SE": "Транснефть",
    "RM": "Роснефть-РМ", "MG": "Магнит-MG", "HS": "Хедж-S",
    "CE": "Газпромэнергохолдинг", "SF": "Аэрофлот-SF", "NA": "Норникель-NA",
    "SBERF": "Сбербанк (вечн)", "GAZPF": "Газпром (вечн)",
    "MX": "Индекс МосБиржи", "MM": "Индекс МосБиржи (мини)",
    "RI": "Индекс РТС", "IMOEXF": "Индекс МосБиржи (вечн)",
    "IB": "Индекс ОФЗ", "VI": "Индекс волатильности RVI",
    "USDRUBF": "USD/RUB (вечн)", "EURRUBF": "EUR/RUB (вечн)",
    "CNYRUBF": "CNY/RUB (вечн)", "Si": "USD/RUB",
    "Eu": "EUR/RUB", "CR": "CNY/RUB",
    "ED": "EUR/USD", "AU": "AUD/USD", "UC": "USD/CNY",
    "DX": "Индекс доллара DXY", "MY": "USD/CNY (M)",
    "GD": "Золото", "GL": "Золото (руб)", "GLDRUBF": "Золото (руб, вечн)",
    "SV": "Серебро", "PT": "Платина", "PD": "Палладий", "HK": "Hang Seng",
    "BR": "Нефть Brent", "BM": "Нефть Brent (мини)",
    "NG": "Природный газ", "FF": "Газ TTF", "NR": "Нефть Urals",
    "CC": "Какао", "KC": "Кофе", "OJ": "Апельсиновый сок", "W4": "Пшеница",
}

# Порог publish per group (по результатам train/test validation)
PUBLISH_FILTER = {
    "meanrev": lambda s: s >= 3.0 or s <= -2.0,
    "momentum": lambda s: 2.0 <= s < 4.5,  # 4.5+ нестабильно на test
}


def build_calibration_from_rows(rows: list[dict]) -> dict:
    """Compute calibration table per group inline (не зависит от external JSON)."""
    from collections import defaultdict
    from statistics import fmean
    by_group_bucket = defaultdict(lambda: defaultdict(list))
    for r in rows:
        bucket = math.floor(r["_score"])
        by_group_bucket[r["_group"]][bucket].append(r["_fwd"])
    out = {}
    for group, buckets in by_group_bucket.items():
        out[group] = {}
        for b, returns in buckets.items():
            n = len(returns)
            rise = sum(1 for ret in returns if ret > 5) / n
            fall = sum(1 for ret in returns if ret < -5) / n
            out[group][b] = {
                "n": n,
                "p_rise": round(rise * 100, 1),
                "p_fall": round(fall * 100, 1),
                "avg_ret": round(fmean(returns), 2),
            }
    return out


def lookup_probability(score: float, group: str, calibration: dict) -> dict:
    """Lookup bucket и достать стат: P_rise, P_fall, avg_ret, n."""
    bucket = math.floor(score)
    tbl = calibration.get(group, {})
    return tbl.get(bucket) or {
        "n": 0, "p_rise": 50.0, "p_fall": 50.0, "avg_ret": 0.0,
    }


def determine_direction(score: float, group: str) -> str:
    """bull / bear / neutral."""
    if group == "meanrev":
        if score >= 3.0:
            return "bull"
        if score <= -2.0:
            return "bear"
    elif group == "momentum":
        if 2.0 <= score < 4.5:
            return "bull"
    return "neutral"


def get_triggered_features(r: dict, group: str) -> list[str]:
    """Список сработавших индикаторов (для caption)."""
    out = []
    if to_bool(r.get("is_new_low_90")):
        out.append("📉 Новый минимум за 90 дней (классический сигнал отскока)")
    if to_bool(r.get("is_new_high_90")):
        out.append("📈 Новый максимум за 90 дней")
    if to_bool(r.get("is_new_high_30")) and not to_bool(r.get("is_new_high_90")):
        out.append("📈 Новый максимум за 30 дней")
    if to_bool(r.get("is_new_low_30")) and not to_bool(r.get("is_new_low_90")):
        out.append("📉 Новый минимум за 30 дней")

    corr = to_float(r.get("oi_price_corr_20d"))
    if corr is not None:
        if corr > 0.3:
            out.append(f"🔗 Открытый интерес растёт вместе с ценой (корреляция +{corr:.2f}) — тренд подтверждён")
        elif corr < -0.3:
            out.append(f"🔗 Открытый интерес против цены (корреляция {corr:.2f})")

    p_ema = to_float(r.get("price_vs_ema50_pct"))
    if p_ema is not None:
        if group == "meanrev":
            if p_ema > 10:
                out.append(f"📊 Цена выше скользящей на +{p_ema:.1f}% — растянута, ждём откат")
            elif p_ema < -10:
                out.append(f"📊 Цена ниже скользящей на {p_ema:.1f}% — перепродана, ждём отскок")
        elif group == "momentum":
            if p_ema > 5:
                out.append(f"📊 Цена выше тренда на +{p_ema:.1f}% — тренд продолжается")
            elif p_ema < -5:
                out.append(f"📊 Цена ниже тренда на {p_ema:.1f}% — нисходящий тренд")

    cp = to_float(r.get("channel_pos"))
    if cp is not None:
        if group == "meanrev":
            if cp < 0.15:
                out.append(f"📍 Цена внизу диапазона (на {cp*100:.0f}%) — ждём отскок")
            elif cp > 0.85:
                out.append(f"📍 Цена вверху диапазона (на {cp*100:.0f}%) — ждём откат")
        elif group == "momentum":
            if cp > 0.85:
                out.append(f"📍 Цена вверху диапазона (на {cp*100:.0f}%) — пробой подтверждён")
            elif cp < 0.15:
                out.append(f"📍 Цена внизу диапазона (на {cp*100:.0f}%)")

    atr = to_float(r.get("atr_pct"))
    if atr is not None and group == "momentum":
        if atr < 1.0:
            out.append(f"🌊 Очень спокойный диапазон (ATR {atr:.1f}%) — устойчивый тренд")
        elif atr > 3.0:
            out.append(f"🌊 Высокая волатильность (ATR {atr:.1f}%) — нестабильность")

    fiz_5d = to_float(r.get("fiz_net_change_5d"))
    if fiz_5d is not None:
        if fiz_5d > 30:
            out.append(f"👥 Физлица резко нарастили лонг (+{fiz_5d:.0f}% за 5 дней)")
        elif fiz_5d < -30:
            out.append(f"👥 Физлица резко сократили лонг ({fiz_5d:.0f}% за 5 дней)")

    yur_5d = to_float(r.get("yur_net_change_5d"))
    if yur_5d is not None:
        if yur_5d > 20:
            out.append(f"🏢 Юрлица наращивают позицию (+{yur_5d:.0f}% за 5 дней)")
        elif yur_5d < -20:
            out.append(f"🏢 Юрлица сокращают позицию ({yur_5d:.0f}% за 5 дней)")

    return out


def format_date_ru(iso: str) -> str:
    """2026-03-12 → 12.03.2026"""
    return datetime.fromisoformat(iso).strftime("%d.%m.%Y")


def compose_signal_caption(r: dict, group: str, direction: str,
                            probability: dict, signal_num: int, total: int) -> str:
    """Caption первого поста — сам сигнал."""
    sectype = r["sectype"]
    name = ASSET_NAMES.get(sectype, sectype)
    score = r["_score"]
    asset_type = r["_type"]
    date_str = format_date_ru(r["date"])

    type_label = {
        "meanrev": "ОТКАТНЫЙ актив (цена возвращается к норме)",
        "momentum": "ТРЕНДОВЫЙ актив (тренд продолжается)",
    }.get(group, "—")

    if direction == "bull":
        direction_label = "📈 РОСТ"
        prob = probability["p_rise"]
        avg = probability["avg_ret"]
        target_text = f"рост более 5%"
    elif direction == "bear":
        direction_label = "📉 ПАДЕНИЕ"
        prob = probability["p_fall"]
        avg = probability["avg_ret"]
        target_text = f"падение более 5%"
    else:
        direction_label = "⚠️ Неопределённость"
        prob = 50
        avg = 0
        target_text = "—"

    triggered = get_triggered_features(r, group)
    features_block = "\n".join(triggered) if triggered else "—"

    type_emoji = {"meanrev": "🔄", "momentum": "📈"}.get(group, "📊")
    asset_tag = sectype.lower().replace("/", "")

    return (
        f"🎯 Сигнал #{signal_num} / {total} — {name}\n"
        f"Дата сигнала: {date_str}\n\n"
        f"#{asset_tag} #{asset_type} {type_emoji}\n\n"
        f"Тип: {type_label}\n"
        f"Направление: {direction_label}\n\n"
        f"Вероятность {target_text}: **{prob:.0f}%**\n"
        f"Среднее ожидание: {avg:+.2f}% за 20 рабочих дней\n"
        f"Балл системы: {score:+.2f}\n\n"
        f"Сработавшие индикаторы:\n{features_block}\n\n"
        f"База: {probability['n']} аналогичных случаев за 2 года истории.\n\n"
        f"Подробнее: {config.SITE_URL}"
    )


def compose_result_caption(r: dict, signal_num: int, direction: str) -> str:
    """Текст второго поста — результат."""
    sectype = r["sectype"]
    name = ASSET_NAMES.get(sectype, sectype)
    fwd_ret = r["_fwd"]
    date_signal = format_date_ru(r["date"])

    # Дата проверки = дата сигнала + 20 рабочих дней (упрощённо +28 кал. дней)
    try:
        from datetime import timedelta
        d_signal = datetime.fromisoformat(r["date"]).date()
        d_check = d_signal + timedelta(days=28)
        date_check = d_check.strftime("%d.%m.%Y")
    except Exception:
        date_check = "—"

    # Verdict
    if direction == "bull":
        if fwd_ret > 5:
            verdict = f"✅ **СИГНАЛ ПОДТВЕРЖДЁН** — рост превысил 5%"
        elif fwd_ret > 0:
            verdict = f"⚠️ Частично — рост был, но меньше 5%"
        else:
            verdict = f"❌ **СИГНАЛ НЕ СРАБОТАЛ** — цена упала, а ожидали рост"
    elif direction == "bear":
        if fwd_ret < -5:
            verdict = f"✅ **СИГНАЛ ПОДТВЕРЖДЁН** — падение превысило 5%"
        elif fwd_ret < 0:
            verdict = f"⚠️ Частично — падение было, но меньше 5%"
        else:
            verdict = f"❌ **СИГНАЛ НЕ СРАБОТАЛ** — цена выросла, а ожидали падение"
    else:
        verdict = "—"

    return (
        f"📊 Результат Сигнала #{signal_num} — {name}\n\n"
        f"Дата сигнала: {date_signal}\n"
        f"Дата проверки: {date_check} (через 20 рабочих дней)\n\n"
        f"Цена изменилась: **{fwd_ret:+.2f}%**\n\n"
        f"{verdict}\n\n"
        f"Подробнее: {config.SITE_URL}"
    )


def cleanup_channel():
    """Delete previous test posts + truncate signal_log."""
    print("Cleanup: deleting previous posts + truncate signal_log...")
    bot = config.BOT_TOKEN
    chat = config.SIGNALS_CHANNEL_ID

    # Delete all posts from signal_log
    from signals.db import SessionLocal
    from sqlalchemy import text
    with SessionLocal() as s:
        msg_ids = [
            row[0] for row in s.execute(text(
                "SELECT message_id FROM signal_log WHERE message_id IS NOT NULL"
            )).fetchall()
        ]
    deleted = 0
    for mid in msg_ids:
        r = requests.get(f"https://api.telegram.org/bot{bot}/deleteMessage",
                          params={"chat_id": chat, "message_id": mid}, timeout=10)
        if r.json().get("ok"):
            deleted += 1
    print(f"  deleted {deleted} posts")

    # Truncate
    with SessionLocal() as s:
        s.execute(text("TRUNCATE signal_log RESTART IDENTITY"))
        s.commit()
    print("  signal_log truncated")


def send_text_post(text: str) -> Optional[int]:
    """Send plain text message (для результата)."""
    bot = config.BOT_TOKEN
    chat = config.SIGNALS_CHANNEL_ID
    r = requests.post(
        f"https://api.telegram.org/bot{bot}/sendMessage",
        json={"chat_id": chat, "text": text, "parse_mode": "Markdown"},
        timeout=30,
    )
    data = r.json()
    if data.get("ok"):
        return data["result"]["message_id"]
    print(f"  ERROR sendMessage: {data}")
    return None


def main():
    # Load CSV, compute scores via dispatch
    print("Loading data...")
    rows = []
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            sectype = r["sectype"]
            asset_type = TYPE_MAP.get(sectype, "other")
            score, group = dispatch_score(r, asset_type)
            fwd = to_float(r.get("fwd_return"))
            if fwd is None:
                continue
            r["_type"] = asset_type
            r["_group"] = group
            r["_score"] = score
            r["_fwd"] = fwd
            rows.append(r)

    # Build calibration from ALL rows (not just filtered)
    calibration = build_calibration_from_rows(rows)
    print(f"Calibration buckets: {sum(len(v) for v in calibration.values())} total")

    # Filter by publish-worthy threshold
    candidates = []
    for r in rows:
        filt = PUBLISH_FILTER.get(r["_group"])
        if filt and filt(r["_score"]):
            r["_direction"] = determine_direction(r["_score"], r["_group"])
            candidates.append(r)

    print(f"\nTotal candidates passed filter: {len(candidates)}")
    from collections import Counter
    print(f"  by mode: {dict(Counter(r['_group'] for r in candidates))}")
    print(f"  by direction: {dict(Counter(r['_direction'] for r in candidates))}")

    # ── DIVERSIFIED SELECTION ──
    # Цель: 20 показательных сигналов с балансом по mode + direction + assets + dates.
    # Quotas:
    #   5 meanrev bull (страtegically strong scores)
    #   5 meanrev bear (другой класс сигналов)
    #   5 momentum bull (большая часть signals здесь)
    #   5 mix — оставшиеся слоты
    # Constraints:
    #   - не более 2 сигналов на asset
    #   - даты не ближе 5 дней друг к другу в одной группе

    # ── Global state — tracks assets/dates ACROSS quota calls ──
    asset_counts: dict[str, int] = {}
    used_dates: list[date] = []

    def pick(pool: list[dict], quota: int, max_per_asset: int = 2,
             min_date_gap: int = 5) -> list[dict]:
        """Жадный отбор: сильнейшие |score| с GLOBAL constraints."""
        from datetime import datetime, timedelta
        result = []
        pool_sorted = sorted(pool, key=lambda r: -abs(r["_score"]))
        for r in pool_sorted:
            if len(result) >= quota:
                break
            if asset_counts.get(r["sectype"], 0) >= max_per_asset:
                continue
            try:
                d = datetime.fromisoformat(r["date"]).date()
            except Exception:
                continue
            too_close = any(abs((d - ud).days) < min_date_gap for ud in used_dates)
            if too_close:
                continue
            result.append(r)
            asset_counts[r["sectype"]] = asset_counts.get(r["sectype"], 0) + 1
            used_dates.append(d)
        return result

    meanrev_bull = [r for r in candidates if r["_group"] == "meanrev" and r["_direction"] == "bull"]
    meanrev_bear = [r for r in candidates if r["_group"] == "meanrev" and r["_direction"] == "bear"]
    momentum_bull = [r for r in candidates if r["_group"] == "momentum" and r["_direction"] == "bull"]

    selected = []
    selected += pick(meanrev_bull, quota=6)
    selected += pick(meanrev_bear, quota=5)
    selected += pick(momentum_bull, quota=6)

    # Fill remaining slots from overall pool, prioritising highest |score|
    remaining_pool = [r for r in candidates if r not in selected]
    selected += pick(remaining_pool, quota=20 - len(selected), max_per_asset=2, min_date_gap=3)

    selected = selected[:20]

    print(f"\nSelected {len(selected)} signals (from {len(candidates)} candidates):")
    for i, r in enumerate(selected, 1):
        d = determine_direction(r["_score"], r["_group"])
        print(f"  #{i:>2} {r['date']} {r['sectype']:>8} ({r['_group']:>8}) "
              f"score={r['_score']:+.2f} dir={d} fwd_ret={r['_fwd']:+.2f}%")

    # Reverse so we publish chronologically (oldest first → newest last)
    selected.reverse()

    # Dry-run support
    if "--dry-run" in sys.argv:
        print("\n=== DRY-RUN mode: not publishing, just showing selection ===")
        # Print breakdown stats
        from collections import Counter
        modes = Counter(r["_group"] for r in selected)
        dirs = Counter(determine_direction(r["_score"], r["_group"]) for r in selected)
        dates = sorted({r["date"] for r in selected})
        assets = Counter(r["sectype"] for r in selected)
        print(f"  by mode: {dict(modes)}")
        print(f"  by direction: {dict(dirs)}")
        print(f"  unique dates: {len(dates)} (from {dates[0]} to {dates[-1]})")
        print(f"  unique assets: {len(assets)} → {dict(assets)}")
        # Outcomes
        hits = sum(1 for r in selected if
                   (determine_direction(r["_score"], r["_group"]) == "bull" and r["_fwd"] > 5) or
                   (determine_direction(r["_score"], r["_group"]) == "bear" and r["_fwd"] < -5))
        partial = sum(1 for r in selected if
                      (determine_direction(r["_score"], r["_group"]) == "bull" and 0 < r["_fwd"] <= 5) or
                      (determine_direction(r["_score"], r["_group"]) == "bear" and -5 <= r["_fwd"] < 0))
        fails = sum(1 for r in selected if
                    (determine_direction(r["_score"], r["_group"]) == "bull" and r["_fwd"] <= 0) or
                    (determine_direction(r["_score"], r["_group"]) == "bear" and r["_fwd"] >= 0))
        print(f"\nHistorical outcomes of selected (для калибровки expectations):")
        print(f"  ✓ confirmed: {hits}/{len(selected)}")
        print(f"  ⚠ partial:   {partial}/{len(selected)}")
        print(f"  ✗ failed:    {fails}/{len(selected)}")
        return 0

    # Cleanup before showcase
    cleanup_channel()

    # Publish pairs
    print(f"\nPublishing {len(selected)} signal-result pairs...")
    for i, r in enumerate(selected, 1):
        group = r["_group"]
        direction = determine_direction(r["_score"], group)
        prob = lookup_probability(r["_score"], group, calibration)

        # 1. SIGNAL POST (с chart на дату сигнала)
        caption = compose_signal_caption(r, group, direction, prob, i, len(selected))
        print(f"\n[{i:>2}/{len(selected)}] {r['sectype']} {r['date']} score={r['_score']:+.2f} {direction}")

        signal_date = date.fromisoformat(r["date"])
        # result_date ≈ signal_date + 20 trading days ≈ +28 calendar days
        from datetime import timedelta as _td
        result_date = signal_date + _td(days=28)

        # Render signal chart
        sectype = r["sectype"]
        asset_name = ASSET_NAMES.get(sectype, sectype)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_signal = Path(tmp.name)
        try:
            render_signal_chart(sectype, signal_date, direction, tmp_signal,
                                clgroup="FIZ", name=asset_name)
            ok, msg_id, err = send_signal_post(image_path=tmp_signal, caption=caption)
            if ok:
                print(f"   ✓ signal post msg_id={msg_id}")
            else:
                print(f"   ✗ signal post FAILED: {err}")
                tmp_signal.unlink(missing_ok=True)
                continue
        except Exception as e:
            print(f"   ✗ render/send signal failed: {type(e).__name__}: {e}")
            tmp_signal.unlink(missing_ok=True)
            continue
        finally:
            tmp_signal.unlink(missing_ok=True)

        # 2. RESULT POST (с chart на момент закрытия)
        time.sleep(3)
        result_caption = compose_result_caption(r, i, direction)
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_result = Path(tmp.name)
        try:
            render_result_chart(sectype, signal_date, result_date, direction,
                                r["_fwd"], tmp_result, clgroup="FIZ", name=asset_name)
            ok2, msg_id2, err2 = send_signal_post(image_path=tmp_result, caption=result_caption)
            if ok2:
                print(f"   ✓ result post msg_id={msg_id2}")
            else:
                print(f"   ✗ result post FAILED: {err2}")
        except Exception as e:
            print(f"   ✗ render/send result failed: {type(e).__name__}: {e}")
        finally:
            tmp_result.unlink(missing_ok=True)

        # Pause between pairs
        if i < len(selected):
            time.sleep(5)

    print("\n=== SHOWCASE COMPLETE ===")


if __name__ == "__main__":
    main()
