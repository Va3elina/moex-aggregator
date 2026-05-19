"""Matplotlib chart renderer for signal posts.

Воспроизводит наш editorial-dark стиль настолько близко, насколько возможно
без браузера: тёмный фон, белая close-line цены, pumpkin-line OI net позиций
на dual-axis, красный кружок + стрелка на последней точке, header с тикером,
footer с URL и датой.

Output: PNG 1280×720 (по умолчанию из config).
"""
from __future__ import annotations
import matplotlib
matplotlib.use("Agg")  # headless backend — без X server

import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from matplotlib.ticker import FuncFormatter
from pathlib import Path
from datetime import date

from signals import config
from signals.db import get_oi_daily, get_candles_continuous, get_asset_name


def _human_int(value: float, _pos=None) -> str:
    """Y-axis formatter: 175000 → '175K', 1500000 → '1.5M'."""
    abs_v = abs(value)
    if abs_v >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if abs_v >= 1_000:
        return f"{value/1_000:.0f}K"
    return f"{value:.0f}"


def render_chart(sectype: str, signal_date: date, output_path: Path) -> Path:
    """Сгенерировать chart PNG. Возвращает путь к файлу."""

    candles = get_candles_continuous(sectype, days=config.CHART_PERIOD_DAYS)
    oi = get_oi_daily(sectype, "FIZ", days=config.CHART_PERIOD_DAYS)
    asset_name = get_asset_name(sectype) or sectype

    if not oi:
        raise ValueError(f"No OI data for {sectype}")

    # ─── Figure / axes setup ───
    fig, ax_oi = plt.subplots(
        figsize=(config.CHART_WIDTH / 100, config.CHART_HEIGHT / 100),
        dpi=100,
    )
    fig.patch.set_facecolor(config.COLOR_BG)
    ax_oi.set_facecolor(config.COLOR_BG)

    # OI (левая ось, pumpkin)
    oi_dates = [p.tradedate for p in oi]
    oi_values = [p.net for p in oi]
    ax_oi.plot(oi_dates, oi_values, color=config.COLOR_OI, lw=1.8, label="Net физлица")
    ax_oi.tick_params(axis="y", labelcolor=config.COLOR_OI, labelsize=10, colors=config.COLOR_OI)
    ax_oi.yaxis.set_major_formatter(FuncFormatter(_human_int))

    # Цена (правая ось, белая) — twin axis
    if candles:
        ax_price = ax_oi.twinx()
        ax_price.set_facecolor(config.COLOR_BG)
        price_dates = [c.begin_time for c in candles]
        price_values = [c.close for c in candles]
        ax_price.plot(price_dates, price_values, color=config.COLOR_PRICE, lw=1.0, alpha=0.95, label="Цена")
        ax_price.tick_params(axis="y", labelcolor=config.COLOR_PRICE, labelsize=10, colors=config.COLOR_PRICE)
        ax_price.yaxis.set_major_formatter(FuncFormatter(_human_int))
        for spine_name in ("top", "left"):
            ax_price.spines[spine_name].set_visible(False)
        ax_price.spines["right"].set_color(config.COLOR_GRID)
        ax_price.spines["bottom"].set_color(config.COLOR_GRID)

    # ─── Маркер на последнюю OI точку: красный кружок + стрелка ───
    last_x = oi_dates[-1]
    last_y = oi_values[-1]
    ax_oi.scatter(
        [last_x], [last_y],
        s=260, facecolors="none",
        edgecolors=config.COLOR_MARKER, linewidths=3,
        zorder=10,
    )
    if len(oi_values) > 1:
        prev_y = oi_values[-2]
        arrow_dir = 1 if last_y > prev_y else -1
        y_range = max(oi_values) - min(oi_values)
        offset = y_range * 0.06 * arrow_dir
        ax_oi.annotate(
            "",
            xy=(last_x, last_y + offset),
            xytext=(last_x, last_y),
            arrowprops=dict(
                arrowstyle="-|>",
                color=config.COLOR_MARKER,
                lw=2.5,
                mutation_scale=18,
            ),
            zorder=11,
        )

    # ─── Grid и оси ───
    ax_oi.grid(True, color=config.COLOR_GRID, lw=0.5, alpha=0.6)
    for spine_name in ("top", "right"):
        ax_oi.spines[spine_name].set_visible(False)
    ax_oi.spines["left"].set_color(config.COLOR_GRID)
    ax_oi.spines["bottom"].set_color(config.COLOR_GRID)
    ax_oi.tick_params(axis="x", labelcolor=config.COLOR_MUTED, labelsize=10, colors=config.COLOR_MUTED)

    # X-axis: month formatter
    period_months = max(1, config.CHART_PERIOD_DAYS // 30)
    locator_step = 2 if period_months > 6 else 1
    ax_oi.xaxis.set_major_locator(mdates.MonthLocator(interval=locator_step))
    ax_oi.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

    # ─── Header (тикер + имя актива) ───
    header_dot = "●"
    fig.text(
        0.04, 0.94,
        f"{header_dot} {asset_name} ({sectype})",
        color=config.COLOR_FG, fontsize=16, fontweight="bold",
        ha="left", va="top",
    )

    # ─── Footer (URL + дата) ───
    fig.text(
        0.99, 0.015,
        f"{config.SITE_URL} · {signal_date.strftime('%d.%m.%Y')}",
        color=config.COLOR_MUTED, fontsize=9,
        ha="right", va="bottom",
    )

    plt.tight_layout(rect=[0.02, 0.04, 0.99, 0.91])
    plt.savefig(output_path, facecolor=config.COLOR_BG, dpi=100, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    return output_path
