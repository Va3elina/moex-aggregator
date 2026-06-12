"""Matplotlib chart renderer для signal-постов — editorial-light палитра.

Две функции:
  - `render_signal_chart`: chart на дату сигнала с маркером последней точки + стрелкой направления
  - `render_result_chart`: chart от signal_date-90d до result_date с маркерами обеих точек

Output: 1280×720 PNG.

Palette match для editorial-light темы сайта:
  bg #F5F1E8 (beige paper)
  primary text #1A1A1A
  blue price #2563EB
  orange OI #FF5C2B (pumpkin)
  green arrow / verdict #10B981
  red arrow / verdict #EF4444
"""
from __future__ import annotations
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")  # headless backend

import matplotlib.pyplot as plt
from matplotlib import dates as mdates
from matplotlib.ticker import FuncFormatter

from signals.db import get_candles_continuous, get_oi_daily, get_asset_name


# ───────────────────────────────────────────────────────────────
# Editorial-light палитра (match с UI export сайта)
# ───────────────────────────────────────────────────────────────
BG = "#F5F1E8"
PAPER = "#FFFFFF"
TEXT_PRIMARY = "#1A1A1A"
TEXT_SECONDARY = "#6B6359"
GRID = "#D4CFC2"
PRICE = "#2563EB"
OI = "#FF5C2B"
ARROW_BULL = "#10B981"
ARROW_BEAR = "#EF4444"
SIGNAL_MARK = "#1A1A1A"

FIG_W = 12.8
FIG_H = 7.2
DPI = 100

WINDOW_DAYS_SIGNAL = 180     # signal chart context
WINDOW_PRE_RESULT = 90       # result chart: before signal


# ───────────────────────────────────────────────────────────────
def _format_price(value: float, _pos=None) -> str:
    abs_v = abs(value)
    if abs_v >= 1000:
        return f"{value:,.0f}".replace(",", " ")
    if abs_v >= 10:
        return f"{value:.2f}"
    return f"{value:.4f}"


def _format_int_compact(value: float, _pos=None) -> str:
    abs_v = abs(value)
    if abs_v >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs_v >= 1_000:
        return f"{value / 1_000:.0f}K"
    return f"{value:.0f}"


def _setup_axes(ax_p, ax_o):
    for ax in (ax_p, ax_o):
        ax.set_facecolor(PAPER)
        ax.tick_params(colors=TEXT_SECONDARY, labelsize=10)
        for spine in ax.spines.values():
            spine.set_color(GRID)
            spine.set_linewidth(1)
    ax_p.grid(True, color=GRID, linewidth=0.6, alpha=0.7)
    ax_p.tick_params(axis="y", colors=PRICE)
    ax_o.tick_params(axis="y", colors=OI)
    ax_p.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m.%y"))
    ax_p.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))


def _date_ru(d: date) -> str:
    months = [
        "января", "февраля", "марта", "апреля", "мая", "июня",
        "июля", "августа", "сентября", "октября", "ноября", "декабря",
    ]
    return f"{d.day} {months[d.month - 1]} {d.year} г."


def _header_block(fig, name: str, sectype: str, meta_line: str):
    fig.text(0.04, 0.93, name, fontsize=22, fontweight="bold",
             color=TEXT_PRIMARY, ha="left", va="bottom")
    # Empirically tuned char width для bold 22pt cyrillic — ≈0.0175 figure fraction.
    badge_x = 0.04 + max(len(name), 6) * 0.0175 + 0.012
    fig.text(badge_x, 0.937, f" {sectype} ", fontsize=14, fontweight="bold",
             color="white", ha="left", va="bottom",
             bbox=dict(boxstyle="round,pad=0.4", facecolor=OI, edgecolor="none"))
    fig.text(0.04, 0.90, meta_line, fontsize=11,
             color=TEXT_SECONDARY, ha="left", va="bottom")


def _footer_block(fig, date_str: str):
    fig.text(0.04, 0.03, "таймфрейм.рф", fontsize=11,
             color=TEXT_SECONDARY, ha="left", va="bottom")
    fig.text(0.96, 0.03, date_str, fontsize=11,
             color=TEXT_SECONDARY, ha="right", va="bottom")


def _legend(fig, name: str):
    fig.text(0.45, 0.86, f"● {name}", fontsize=11, color=PRICE,
             ha="right", va="bottom", fontweight="bold")
    fig.text(0.55, 0.86, "● Чистая позиция", fontsize=11, color=OI,
             ha="left", va="bottom", fontweight="bold")


# ───────────────────────────────────────────────────────────────
# Render: signal chart (состояние на дату сигнала + стрелка)
# ───────────────────────────────────────────────────────────────
def render_signal_chart(
    sectype: str,
    signal_date: date,
    direction: str,           # 'bull' / 'bear'
    output_path: Path,
    *,
    clgroup: str = "FIZ",
    name: Optional[str] = None,
) -> Path:
    name = name or get_asset_name(sectype) or sectype

    candles = get_candles_continuous(sectype, WINDOW_DAYS_SIGNAL, end_date=signal_date)
    oi = get_oi_daily(sectype, clgroup, WINDOW_DAYS_SIGNAL, as_of_date=signal_date)

    if not candles:
        raise ValueError(f"no candles for {sectype} as of {signal_date}")

    fig = plt.figure(figsize=(FIG_W, FIG_H), dpi=DPI, facecolor=BG)
    ax_p = fig.add_axes([0.07, 0.10, 0.86, 0.74])
    ax_o = ax_p.twinx()

    dates_p = [c.begin_time for c in candles]
    prices = [c.close for c in candles]
    ax_p.plot(dates_p, prices, color=PRICE, linewidth=1.8)

    if oi:
        dates_oi = [o.tradedate for o in oi]
        nets = [o.net for o in oi]
        ax_o.plot(dates_oi, nets, color=OI, linewidth=1.5)

    # Last point marker + arrow
    last_price = prices[-1]
    last_date = dates_p[-1]
    ax_p.scatter([last_date], [last_price], s=120, color=SIGNAL_MARK,
                 zorder=10, edgecolor=BG, linewidth=2)

    # Extend x-axis с empty space справа — место для стрелки и метки
    first_date = dates_p[0]
    ax_p.set_xlim(first_date, last_date + timedelta(days=25))

    arrow_color = ARROW_BULL if direction == "bull" else ARROW_BEAR
    price_range = max(prices) - min(prices)
    arrow_dy = price_range * 0.18 * (1 if direction == "bull" else -1)
    arrow_dx_days = 15
    ax_p.annotate(
        "",
        xy=(last_date + timedelta(days=arrow_dx_days), last_price + arrow_dy),
        xytext=(last_date, last_price),
        arrowprops=dict(arrowstyle="->", color=arrow_color, lw=3),
        zorder=11,
    )
    label_text = "Прогноз: рост" if direction == "bull" else "Прогноз: падение"
    # Label позиционируем над/под концом стрелки
    label_xy = (last_date + timedelta(days=arrow_dx_days),
                last_price + arrow_dy)
    ax_p.annotate(
        label_text,
        xy=label_xy,
        xytext=(-65, 15 if direction == "bull" else -15),
        textcoords="offset points",
        fontsize=11, fontweight="bold", color=arrow_color,
        ha="center", va="bottom" if direction == "bull" else "top",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                  edgecolor=arrow_color, linewidth=1.5),
    )

    _setup_axes(ax_p, ax_o)
    ax_p.yaxis.set_major_formatter(FuncFormatter(_format_price))
    ax_o.yaxis.set_major_formatter(FuncFormatter(_format_int_compact))

    _legend(fig, name)
    clgroup_ru = "Физлица" if clgroup == "FIZ" else "Юрлица"
    _header_block(fig, name, sectype,
                  f"Открытые позиции · 1Д · 6M · {clgroup_ru} · Состояние на сигнал")
    _footer_block(fig, _date_ru(signal_date))

    fig.savefig(output_path, dpi=DPI, facecolor=BG)
    plt.close(fig)
    return output_path


# ───────────────────────────────────────────────────────────────
# Render: result chart (от signal-90d до result_date с обеими маркерами)
# ───────────────────────────────────────────────────────────────
def render_result_chart(
    sectype: str,
    signal_date: date,
    result_date: date,
    direction: str,
    fwd_return_pct: float,
    output_path: Path,
    *,
    clgroup: str = "FIZ",
    name: Optional[str] = None,
) -> Path:
    name = name or get_asset_name(sectype) or sectype

    total_days = (result_date - signal_date).days + WINDOW_PRE_RESULT + 10
    candles = get_candles_continuous(sectype, total_days, end_date=result_date)
    oi = get_oi_daily(sectype, clgroup, total_days, as_of_date=result_date)

    if not candles:
        raise ValueError(f"no candles for {sectype} as of {result_date}")

    fig = plt.figure(figsize=(FIG_W, FIG_H), dpi=DPI, facecolor=BG)
    ax_p = fig.add_axes([0.07, 0.10, 0.86, 0.74])
    ax_o = ax_p.twinx()

    dates_p = [c.begin_time for c in candles]
    prices = [c.close for c in candles]
    ax_p.plot(dates_p, prices, color=PRICE, linewidth=1.8)

    if oi:
        dates_oi = [o.tradedate for o in oi]
        nets = [o.net for o in oi]
        ax_o.plot(dates_oi, nets, color=OI, linewidth=1.5, alpha=0.7)

    # Find signal point
    signal_point = next((c for c in candles
                         if c.begin_time.date() >= signal_date), None)
    # Result point — last candle in window
    result_point = candles[-1]

    if signal_point:
        ax_p.axvline(signal_point.begin_time, color=SIGNAL_MARK,
                     linestyle="--", linewidth=1.2, alpha=0.6)
        ax_p.scatter([signal_point.begin_time], [signal_point.close], s=120,
                     color=SIGNAL_MARK, zorder=10, edgecolor=BG, linewidth=2)
        ax_p.annotate(
            "Сигнал", xy=(signal_point.begin_time, signal_point.close),
            xytext=(10, 12), textcoords="offset points",
            fontsize=10, fontweight="bold", color=SIGNAL_MARK,
        )

    # Verdict colour
    is_correct = (direction == "bull" and fwd_return_pct > 0) or \
                 (direction == "bear" and fwd_return_pct < 0)
    verdict_color = ARROW_BULL if is_correct else ARROW_BEAR

    ax_p.scatter([result_point.begin_time], [result_point.close], s=150,
                 color=verdict_color, zorder=11, edgecolor=BG, linewidth=2)
    ax_p.annotate(
        f"{fwd_return_pct:+.2f}%",
        xy=(result_point.begin_time, result_point.close),
        xytext=(-50, 20 if fwd_return_pct > 0 else -30),
        textcoords="offset points",
        fontsize=14, fontweight="bold", color="white",
        bbox=dict(boxstyle="round,pad=0.5", facecolor=verdict_color, edgecolor="none"),
    )

    _setup_axes(ax_p, ax_o)
    ax_p.yaxis.set_major_formatter(FuncFormatter(_format_price))
    ax_o.yaxis.set_major_formatter(FuncFormatter(_format_int_compact))

    _legend(fig, name)
    clgroup_ru = "Физлица" if clgroup == "FIZ" else "Юрлица"
    direction_ru = "рост" if direction == "bull" else "падение"
    _header_block(
        fig, name, sectype,
        f"Открытые позиции · {clgroup_ru} · Прогноз был на {direction_ru} от {_date_ru(signal_date)}"
    )
    _footer_block(fig, f"Результат на {_date_ru(result_date)}")

    fig.savefig(output_path, dpi=DPI, facecolor=BG)
    plt.close(fig)
    return output_path


# ───────────────────────────────────────────────────────────────
# Backward-compat shim для существующих вызовов render_chart
# ───────────────────────────────────────────────────────────────
def render_chart(
    sectype: str,
    signal_date: date,
    output_path: Path,
    clgroup: str = "FIZ",
) -> Path:
    """Старая совместимая обёртка — рендерит signal chart (bull arrow по умолчанию)."""
    return render_signal_chart(
        sectype=sectype, signal_date=signal_date,
        direction="bull", output_path=output_path, clgroup=clgroup,
    )
