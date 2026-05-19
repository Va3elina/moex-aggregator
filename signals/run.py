"""Signal-engine entry point.

Обычный запуск (cron):
    0 * * * * /opt/frame/signals/run.sh >> /opt/frame/logs/signals.log 2>&1

Backtest (исторический прогон):
    python -m signals.run --backtest 2026-03-19 2026-05-19
    python -m signals.run --backtest 2026-03-19 2026-05-19 --dry-run   # без отправки
    python -m signals.run --backtest 2026-03-19 2026-05-19 --top 20    # только N сильнейших

Flow:
    1. detect_all_oi(as_of_date) — z-score scan по 65 тикерам × {FIZ, YUR}
    2. для каждого candidate:
        - cooldown check (24h) — НЕ применяется в backtest
        - render chart → PNG /tmp
        - sendPhoto в Telegram канал
        - INSERT в signal_log
"""
from __future__ import annotations
import argparse
import logging
import sys
import tempfile
import time
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from signals import config
from signals.db import last_signal_ts, insert_signal_log
from signals.detectors.oi import detect_all_oi, OISignalCandidate
from signals.render.browser import render_chart
from signals.publish.telegram import send_signal_post


def _fmt_int(n: int) -> str:
    """175675 → '175 675'."""
    return f"{n:,}".replace(",", " ")


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s signals: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("signals.run")


# ─────────────────────────────────────────────────────────────────
# Текст поста
# ─────────────────────────────────────────────────────────────────
def _format_caption(
    sig: OISignalCandidate,
    *,
    signal_num: int | None = None,
    total_count: int | None = None,
) -> str:
    """Caption поста: номер + хэштеги + emoji + название + действие + values + reasoning + ссылка."""
    asset_tag = sig.asset.lower()
    if sig.clgroup == "FIZ":
        group_word = "физлица"
        group_hashtag = "#физлица"
    else:
        group_word = "юрлица"
        group_hashtag = "#юрлица"

    if sig.direction == "up":
        emoji = "📈"
        action = (
            "резко нарастили чистый лонг"
            if sig.current_net > 0
            else "сокращают чистый шорт"
        )
    else:
        emoji = "📉"
        action = (
            "резко сократили чистый лонг"
            if sig.current_net > 0
            else "наращивают чистый шорт"
        )

    name = sig.asset_name or sig.asset

    # Header с номером (только в backtest)
    header = ""
    if signal_num is not None:
        if total_count is not None:
            header = f"📊 Сигнал #{signal_num} / {total_count}\n\n"
        else:
            header = f"📊 Сигнал #{signal_num}\n\n"

    # Чистая позиция: переход
    prev_str = _fmt_int(sig.previous_net)
    cur_str = _fmt_int(sig.current_net)
    delta_str = _fmt_int(abs(sig.daily_change))
    delta_sign = "+" if sig.daily_change > 0 else "−"

    # Z-score reasoning — человеческими словами
    z_abs = abs(sig.z_score)
    z_sign = "+" if sig.z_score > 0 else "−"
    reasoning = (
        f"Z-score: {z_sign}{z_abs:.2f} — отклонение от средней дневной "
        f"динамики за {config.LOOKBACK_DAYS} дней в {z_abs:.1f}× раза."
    )

    return (
        f"{header}"
        f"#{asset_tag} #cot {group_hashtag}\n\n"
        f"{emoji} {name}: {group_word} {action}.\n\n"
        f"Чистая позиция: {prev_str} → {cur_str} контрактов ({delta_sign}{delta_str})\n"
        f"{reasoning}\n\n"
        f"Мониторьте позиции на {config.SITE_URL}"
    )


# ─────────────────────────────────────────────────────────────────
# Cooldown
# ─────────────────────────────────────────────────────────────────
def _in_cooldown(sig: OISignalCandidate) -> bool:
    last_ts = last_signal_ts(
        asset=sig.asset,
        indicator="oi",
        signal_type=sig.signal_type,
    )
    if last_ts is None:
        return False
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - last_ts
    return delta.total_seconds() < config.COOLDOWN_HOURS * 3600


# ─────────────────────────────────────────────────────────────────
# Один сигнал: render + publish + log
# ─────────────────────────────────────────────────────────────────
def process_signal(
    sig: OISignalCandidate,
    *,
    dry_run: bool = False,
    skip_cooldown: bool = False,
    signal_num: int | None = None,
    total_count: int | None = None,
) -> bool:
    """Returns True если опубликовано (для счётчика), False иначе."""
    if not skip_cooldown and _in_cooldown(sig):
        log.info("skipped (cooldown): %s/%s z=%+.2f", sig.asset, sig.clgroup, sig.z_score)
        return False

    caption = _format_caption(sig, signal_num=signal_num, total_count=total_count)
    log.info(
        "processing %s/%s z=%+.2f Δ=%+d (%s) on %s",
        sig.asset, sig.clgroup, sig.z_score, sig.daily_change, sig.direction, sig.tradedate,
    )

    if dry_run:
        log.info("DRY-RUN caption:\n%s\n", caption)
        return True

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        render_chart(sig.asset, sig.tradedate, tmp_path, clgroup=sig.clgroup)
        ok, msg_id, err = send_signal_post(image_path=tmp_path, caption=caption)
        if ok:
            insert_signal_log(
                asset=sig.asset,
                indicator="oi",
                signal_type=sig.signal_type,
                direction=sig.direction,
                z_score=sig.z_score,
                raw_value=sig.daily_change,
                channel_id=config.SIGNALS_CHANNEL_ID,
                message_id=msg_id,
                message_text=caption,
            )
            log.info("published: %s/%s message_id=%s", sig.asset, sig.clgroup, msg_id)
            return True
        else:
            log.error("publish failed for %s/%s: %s", sig.asset, sig.clgroup, err)
            return False
    finally:
        tmp_path.unlink(missing_ok=True)


# ─────────────────────────────────────────────────────────────────
# Backtest: прогон по диапазону дат
# ─────────────────────────────────────────────────────────────────
def run_backtest(
    start: date,
    end: date,
    *,
    dry_run: bool,
    top_n: int | None,
    delay_sec: float,
    min_abs_z: float | None = None,
    clgroup_filter: str | None = None,
) -> int:
    """Прогон детектора по каждому дню диапазона [start, end] включительно."""
    log.info("=== BACKTEST %s → %s, dry_run=%s, top=%s ===", start, end, dry_run, top_n)

    all_sigs: list[OISignalCandidate] = []
    d = start
    while d <= end:
        sigs = detect_all_oi(as_of_date=d)
        if sigs:
            log.info("  %s — %d signals", d, len(sigs))
        all_sigs.extend(sigs)
        d += timedelta(days=1)

    log.info("backtest collected %d raw signals over %d days",
             len(all_sigs), (end - start).days + 1)

    # Dedupe (asset, tradedate, clgroup) → keep max |z|.
    # При rolling backtest один и тот же event ловится повторно (выходные/праздники
    # → детектор видит ту же последнюю точку несколько дней подряд).
    seen: dict[tuple[str, date, str], OISignalCandidate] = {}
    for s in all_sigs:
        key = (s.asset, s.tradedate, s.clgroup)
        if key not in seen or abs(s.z_score) > abs(seen[key].z_score):
            seen[key] = s
    all_sigs = list(seen.values())
    log.info("after dedup: %d unique events", len(all_sigs))

    # Фильтры
    if min_abs_z is not None:
        before = len(all_sigs)
        all_sigs = [s for s in all_sigs if abs(s.z_score) >= min_abs_z]
        log.info("after min |z|>=%.2f: %d (was %d)", min_abs_z, len(all_sigs), before)
    if clgroup_filter:
        before = len(all_sigs)
        all_sigs = [s for s in all_sigs if s.clgroup == clgroup_filter]
        log.info("after clgroup=%s: %d (was %d)", clgroup_filter, len(all_sigs), before)

    # Сортировка: хронологически (старые сначала — канал увидит «timeline»)
    all_sigs.sort(key=lambda s: (s.tradedate, -abs(s.z_score)))

    # Filter top-N по |z| если задан
    if top_n is not None and top_n > 0:
        top = sorted(all_sigs, key=lambda s: -abs(s.z_score))[:top_n]
        top_keys = {(s.asset, s.tradedate, s.clgroup) for s in top}
        all_sigs = [s for s in all_sigs if (s.asset, s.tradedate, s.clgroup) in top_keys]
        log.info("filtered to top-%d by |z| → %d signals", top_n, len(all_sigs))

    # Если dry-run — просто покажем summary
    if dry_run:
        log.info("--- DRY-RUN: signals that would be published ---")
        for s in all_sigs:
            log.info(
                "  %s  %s/%-3s  z=%+5.2f  Δ=%+8d  %s",
                s.tradedate, s.asset.ljust(9), s.clgroup,
                s.z_score, s.daily_change, s.asset_name or "?",
            )
        return 0

    # Реальная отправка с rate-limit задержкой и нумерацией постов
    total = len(all_sigs)
    log.info("--- PUBLISHING %d signals (delay %.1fs between) ---", total, delay_sec)
    published = 0
    for i, sig in enumerate(all_sigs):
        ok = process_signal(
            sig, dry_run=False, skip_cooldown=True,
            signal_num=i + 1, total_count=total,
        )
        if ok:
            published += 1
        if i < total - 1:
            time.sleep(delay_sec)
    log.info("backtest done: %d/%d published", published, total)
    return 0


# ─────────────────────────────────────────────────────────────────
# Обычный запуск (cron)
# ─────────────────────────────────────────────────────────────────
def run_live(dry_run: bool) -> int:
    log.info("=== signal-engine LIVE run (dry_run=%s) ===", dry_run)
    candidates = detect_all_oi()
    log.info("detector found %d candidates", len(candidates))
    for sig in candidates:
        try:
            process_signal(sig, dry_run=dry_run)
        except Exception:
            log.exception("failed to process %s/%s", sig.asset, sig.clgroup)
    log.info("=== signal-engine run end ===")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="signals.run")
    parser.add_argument("--dry-run", action="store_true", help="не публиковать, только логи")
    parser.add_argument("--backtest", nargs=2, metavar=("START", "END"),
                        help="прогон по диапазону дат (YYYY-MM-DD YYYY-MM-DD)")
    parser.add_argument("--top", type=int, default=None,
                        help="(только с --backtest) — оставить только N самых сильных по |z|")
    parser.add_argument("--min-z", type=float, default=None,
                        help="(только с --backtest) — фильтр по минимальному |z|")
    parser.add_argument("--clgroup", choices=["FIZ", "YUR"], default=None,
                        help="(только с --backtest) — оставить только одну группу")
    parser.add_argument("--delay", type=float, default=3.0,
                        help="(только с --backtest без --dry-run) — задержка между постами в сек")
    args = parser.parse_args()

    if not args.dry_run:
        if not config.BOT_TOKEN or not config.SIGNALS_CHANNEL_ID:
            log.error("BOT_TOKEN or SIGNALS_CHANNEL_ID not set; aborting")
            return 1

    if args.backtest:
        try:
            start = date.fromisoformat(args.backtest[0])
            end = date.fromisoformat(args.backtest[1])
        except ValueError as e:
            log.error("invalid date in --backtest: %s", e)
            return 1
        if start > end:
            log.error("--backtest START must be <= END")
            return 1
        return run_backtest(
            start, end,
            dry_run=args.dry_run,
            top_n=args.top,
            delay_sec=args.delay,
            min_abs_z=args.min_z,
            clgroup_filter=args.clgroup,
        )

    return run_live(args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
