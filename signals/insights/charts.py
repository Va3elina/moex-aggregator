"""Простые технические графики к черновикам: фонды по месяцам и за n дней, сделки фондов, реакция на макро-новость.

Картинки в боте ревью — для Вадима и Саши, не для канала (Вадим 08.10: «чем проще и понятнее, тем лучше»): белый фон,
стандартный шрифт matplotlib (DejaVu), крупные подписи. Заголовок прямо говорит, что нарисовано, и главную цифру.
Выделено только событие — один оранжевый (#FF5C2B) столбец или точка, остальное серое; на поле не больше одной подписи
(значение у выделенного). Месяцы по-русски. Позиции и сезонность рисует прежний код (cards.draw_chart).

Вход — card["chart"] из signals/insights/cards.py:
    bars     — потоки в фонды по месяцам: x — первое число месяца, y — млрд ₽, current — последний месяц неполный
    flow     — потоки в фонды, сумма за n дней по дням: x, y (млрд ₽)
    hbars    — сделки фондов: labels, values (млрд ₽), покупки сверху, продажи снизу
    intraday — цена фьючерса по 5 минутам: x, y, event — время новости
"""
import textwrap

import numpy as np
import pandas as pd

ACC, GREY, LIGHT, INK, MUTED, GRID = "#FF5C2B", "#9A9A9A", "#C8C8C8", "#1D1D1F", "#6E6E73", "#E6E6E6"
W_PX, H_PX, DPI = 1200, 675, 100
MON = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]
SOURCE = {"funds": "Данные: раскрытие управляющих компаний (СЧА и паи), framedata.ru",
          "fund_trades": "Данные: отчёты фондов о составе (СЧА), страница «Что покупают фонды»",
          "default": "Данные: Мосбиржа"}


def bfmt(v: float) -> str:
    """Миллиарды со знаком: +25; −3,1; 0."""
    if abs(v) < 0.05:
        return "0"
    s = f"{abs(v):.0f}" if abs(v) >= 10 else f"{abs(v):.1f}".replace(".", ",")
    return ("+" if v > 0 else "−") + s


def pfmt(v: float) -> str:
    s = f"{v:,.0f}" if abs(v) >= 1000 else f"{v:,.1f}" if abs(v) >= 10 else f"{v:,.2f}"
    return s.replace(",", " ").replace(".", ",")


def date_ticks(x0, x1, intraday=False) -> list:
    """Подписи оси X: часы внутри дня, «окт 26» по месяцам (на длинном окне — через месяц-два)."""
    x0, x1 = pd.Timestamp(x0), pd.Timestamp(x1)
    span = (x1 - x0).total_seconds() / 86400
    if intraday or span <= 2:
        step = max(1, int(np.ceil(span * 24 / 8)))
        return [(t, f"{t:%H:%M}") for t in pd.date_range(x0.ceil("h"), x1, freq=f"{step}h")]
    starts = pd.date_range(x0.normalize().replace(day=1), x1, freq="MS")
    starts = starts[starts >= x0]
    starts = starts[::max(1, int(np.ceil(len(starts) / 10)))]
    return [(t, f"{MON[t.month - 1]} {t:%y}") for t in starts]


def _fig(left=0.08):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig = plt.figure(figsize=(W_PX / DPI, H_PX / DPI), dpi=DPI, facecolor="white")
    ax = fig.add_axes((left, 0.12, 0.97 - left, 0.70))
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    for sp in ("left", "bottom"):
        ax.spines[sp].set_color(LIGHT)
    ax.tick_params(labelsize=14, colors=INK, length=0)
    ax.set_axisbelow(True)
    return fig, ax


def _title(fig, text):
    fig.text(0.02, 0.97, "\n".join(textwrap.wrap(text, 74)), ha="left", va="top", fontsize=17, color=INK,
             fontweight="bold", linespacing=1.3)


def _xdates(ax, ticks, shift=pd.Timedelta(0)):
    ax.set_xticks([t + shift for t, _ in ticks], [lab for _, lab in ticks])


def _yfmt(ax, fmt):
    import matplotlib.ticker as mt
    ax.yaxis.set_major_locator(mt.MaxNLocator(nbins=7, steps=[1, 2, 5, 10]))   # без «12,5» → «+12»
    ax.yaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: fmt(v)))
    ax.yaxis.grid(True, color=GRID, lw=1)


def _bars(fig, ax, ch):
    x, y = pd.DatetimeIndex(ch["x"]), np.asarray(ch["y"], dtype=float)
    mid = x + pd.Timedelta(days=14)                      # столбец — на своём месяце
    cur = bool(ch.get("current"))
    n = len(y) - (1 if cur else 0)
    ax.bar(mid[:n], y[:n], width=22, color=GREY, lw=0)
    if cur and len(y):
        # неполный текущий месяц — светлее
        ax.bar(mid[-1:], y[-1:], width=22, color=ACC, alpha=0.45, edgecolor=ACC, lw=1.5)
    else:
        ax.bar(mid[-1:], y[-1:], width=22, color=ACC, lw=0)
    ax.annotate(bfmt(y[-1]), (mid[-1], y[-1]), textcoords="offset points", xytext=(0, 8 if y[-1] >= 0 else -20),
                ha="center", fontsize=15, fontweight="bold", color=ACC)
    ax.axhline(0, color=INK, lw=1)
    _yfmt(ax, bfmt)
    _xdates(ax, date_ticks(x[0], x[-1]), pd.Timedelta(days=14))
    ax.set_xlim(x[0] - pd.Timedelta(days=10), x[-1] + pd.Timedelta(days=40))


def _flow(fig, ax, ch):
    x, y = pd.DatetimeIndex(ch["x"]), np.asarray(ch["y"], dtype=float)
    ax.plot(x, y, color=GREY, lw=2)
    ax.axhline(0, color=INK, lw=1)
    ax.axhline(y[-1], color=ACC, lw=1, ls=":", alpha=0.8)      # уровень сейчас: видно, когда было больше
    ax.scatter([x[-1]], [y[-1]], s=90, color=ACC, zorder=5)
    ax.annotate(bfmt(y[-1]), (x[-1], y[-1]), textcoords="offset points", xytext=(-10, 10 if y[-1] >= 0 else -24),
                ha="right", fontsize=15, fontweight="bold", color=ACC)
    _yfmt(ax, bfmt)
    _xdates(ax, date_ticks(x[0], x[-1]))
    ax.set_xlim(x[0], x[-1] + (x[-1] - x[0]) * 0.02)


def _hbars(fig, ax, ch):
    labels, vals = list(ch["labels"]), np.asarray(ch["values"], dtype=float)
    k = np.arange(len(vals))[::-1]                       # первый — сверху
    hl = {0} | ({int(np.flatnonzero(vals < 0)[0])} if (vals < 0).any() else set())
    cols = [ACC if i in hl else GREY for i in range(len(vals))]
    ax.barh(k, vals, height=0.62, color=cols, lw=0)
    ax.axvline(0, color=INK, lw=1)
    lim = float(np.abs(vals).max() or 1.0)
    ax.set_xlim(-lim * 1.3 if (vals < 0).any() else 0, lim * 1.3)
    for kk, v in zip(k, vals):
        ax.text(v + (lim * 0.02 if v >= 0 else -lim * 0.02), kk, bfmt(v), ha="left" if v >= 0 else "right",
                va="center", fontsize=14, color=INK)
    ax.set_yticks(k, labels)
    ax.tick_params(axis="y", labelsize=15)
    ax.spines["left"].set_visible(False)
    import matplotlib.ticker as mt
    ax.xaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: bfmt(v)))
    ax.xaxis.grid(True, color=GRID, lw=1)


def _intraday(fig, ax, ch):
    x, y = pd.DatetimeIndex(ch["x"]), np.asarray(ch["y"], dtype=float)
    ax.plot(x, y, color="#4A4A4F", lw=2)
    _yfmt(ax, pfmt)
    _xdates(ax, date_ticks(x[0], x[-1], intraday=True))
    e = pd.Timestamp(ch["event"]) if ch.get("event") is not None else None
    # ночная новость раньше первой свечи — поле начинается чуть раньше неё, черта видна
    x0 = min(x[0], e) - (x[-1] - x[0]) * 0.01 if e is not None else x[0]
    ax.set_xlim(x0, x[-1])
    if e is not None:
        ax.axvline(e, color=ACC, lw=2)
        ax.text(e, 1.01, f" новость {e:%H:%M}", transform=ax.get_xaxis_transform(), ha="left",
                va="bottom", fontsize=14, fontweight="bold", color=ACC)


def _season3m(fig, ax, ch):
    """Сезонность по активу (правило /hot): одна шкала «% с начала года»; медианный путь прошлых лет — серый на весь
    год, нынешний год — оранжевый до сегодня; следующие 3 месяца — лёгкая заливка без подписей."""
    x, y = pd.DatetimeIndex(ch["x"]), np.asarray(ch["y"], dtype=float)
    ax.plot(x, y, color=GREY, lw=2.2)
    if len(ch.get("x2", [])):
        ax.plot(pd.DatetimeIndex(ch["x2"]), np.asarray(ch["y2"], dtype=float), color=ACC, lw=2.6)
    now = pd.Timestamp(ch["now"])
    ax.axvspan(now, min(now + pd.Timedelta(days=ch.get("days", 91)), x[-1]), color=ACC, alpha=0.08, lw=0)
    ax.axhline(0, color=INK, lw=1)
    _yfmt(ax, lambda v: ("+" if v > 0 else "−" if v < 0 else "") + f"{abs(v):.0f}%")
    ax.set_ylabel("% с начала года", fontsize=14, color=INK)
    months = pd.date_range(x[0], x[-1], freq="MS")
    ax.set_xticks([m + pd.Timedelta(days=14) for m in months], [MON[m.month - 1] for m in months])
    ax.set_xlim(x[0], x[-1])


DRAW = {"bars": _bars, "flow": _flow, "hbars": _hbars, "intraday": _intraday, "season3m": _season3m}


def draw(card: dict, path: str):
    import matplotlib.pyplot as plt
    ch = card["chart"]
    typ = ch["type"]
    fig, ax = _fig(left=0.22 if typ == "hbars" else 0.08)
    try:
        DRAW[typ](fig, ax, ch)
        _title(fig, ch.get("title", ""))
        if ch.get("subtitle"):
            lines = len(textwrap.wrap(ch.get("title", ""), 74))
            fig.text(0.02, 0.97 - 0.055 * lines - 0.01, ch["subtitle"], ha="left", va="top", fontsize=13, color=MUTED)
        fig.text(0.02, 0.02, SOURCE.get(card.get("kind"), SOURCE["default"]), fontsize=11, color=MUTED)
        fig.savefig(path, dpi=DPI, facecolor="white")
    finally:
        plt.close(fig)
