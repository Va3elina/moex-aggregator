"""Streamlit-песочница для индекса страха и жадности (Mosbirzha).

Шкала: 0 = экстремальный страх · 100 = экстремальная жадность.

Запуск:  python -m streamlit run sandbox/app.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# ---------- Persistence: сохранение настроек между перезагрузками --------

SETTINGS_FILE = Path(__file__).parent / "cache" / "ui_state.json"


def _load_saved_state() -> None:
    """Подтянуть сохранённые значения виджетов в session_state.

    Делается один раз за сессию, до создания виджетов.
    """
    if st.session_state.get("_ui_state_loaded"):
        return
    st.session_state["_ui_state_loaded"] = True
    if not SETTINGS_FILE.exists():
        return
    try:
        saved = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        for k, v in saved.items():
            if k not in st.session_state:
                st.session_state[k] = v
    except Exception:
        pass


def _save_state() -> None:
    """Сериализовать текущий session_state в файл."""
    state = {}
    for k, v in st.session_state.items():
        if isinstance(k, str) and k.startswith("_"):
            continue
        if isinstance(v, (int, float, str, bool)):
            state[k] = v
        elif (isinstance(v, list)
              and all(isinstance(x, (int, float, str, bool)) for x in v)):
            state[k] = v
    try:
        SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        SETTINGS_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


_load_saved_state()

sys.path.insert(0, str(Path(__file__).parent))
from fear_lib import (CLASS_BANDS, COMPONENT_KEYS, DEFAULT_TRANSFORMS,
                      DEFAULT_WEIGHTS, FearParams, Transform, apply_transform,
                      build_raw_components, classify, compose,
                      load_raw_from_cache, market_returns, rolling_correlation)

# ---------- русские названия и подсказки -----------------------------------

COMPONENT_RU = {
    "rvi":             "1. RVI (волатильность)",
    "breadth":         "2. Широта рынка (% акций выше EMA-200)",
    "market_momentum": "3. Импульс рынка (IMOEX vs EMA-125)",
}

COMPONENT_DESC = {
    "rvi":
        "Индекс волатильности Мосбиржи (аналог VIX). Высокий RVI = "
        "рынок ждёт сильных движений = страх. Низкий = штиль = жадность.",
    "breadth":
        "Доля акций индекса IMOEX, торгующихся выше скользящей EMA-200. "
        "Чем больше акций в восходящем тренде — тем шире рынок растёт = "
        "жадность.",
    "market_momentum":
        "Положение IMOEX относительно своей 125-дневной EMA. Когда индекс "
        "выше скользящей средней — рынок в восходящем импульсе = жадность. "
        "Ниже — импульс отрицательный = страх.",
}

CLASS_RU = {
    "Extreme Fear":  "Экстремальный страх",
    "Fear":          "Страх",
    "Neutral":       "Нейтрально",
    "Greed":         "Жадность",
    "Extreme Greed": "Экстремальная жадность",
    "n/a":           "—",
}

KIND_RU = {
    "pct_rank":       "перцентильный ранг",
    "thresholds":     "по порогам",
    "level_momentum": "уровень + импульс",
}

SIGNAL_MODE_RU = {
    "both":       "и страх, и жадность",
    "fear_only":  "только страх",
    "greed_only": "только жадность",
}

SIGNAL_MODE_HELP = (
    "Какие сигналы фактор может подавать в итоговый индекс:\n\n"
    "• «и страх, и жадность» — симметрично. Высокое значение тянет в "
    "одну сторону, низкое — в другую (диапазон 0…100).\n\n"
    "• «только страх» — потолок 50. Высокое значение тянет в страх, "
    "низкое = нейтрально (фактор «молчит»). Хорошо для RVI: высокий "
    "RVI = паника, но низкий RVI = «просто спокойно», а не жадность.\n\n"
    "• «только жадность» — пол 50. Высокое значение тянет в жадность, "
    "низкое = нейтрально."
)

KIND_HELP = {
    "pct_rank":
        "Сравниваем текущее значение с историей: «насколько часто за "
        "последние N дней значение было ниже текущего?». 0% = минимум "
        "за период, 100% = максимум. Удобно для показателей без "
        "понятных абсолютных уровней (потоки, спреды).",
    "thresholds":
        "Линейная шкала: задаёшь два сырых значения — одно для "
        "максимальной жадности (=100), другое для максимального страха "
        "(=0). Между ними значение растёт линейно. За пределами — "
        "обрезается. Удобно когда есть «понятные» уровни.",
    "level_momentum":
        "Гибрид: одна половина смотрит абсолютный уровень показателя "
        "через сигмоиду, другая — насколько резко он меняется. "
        "Помогает индикаторам вроде RVI, которые могут долго торчать "
        "на высоких уровнях: чистая шкала «по порогам» залипает в нуле, "
        "а импульсная часть продолжает реагировать на ускорения и "
        "откаты.",
}

# ---------- загрузка --------------------------------------------------------

st.set_page_config(page_title="Индекс страха и жадности — песочница",
                   layout="wide")
st.title("Индекс страха и жадности — песочница")
st.caption("0 = экстремальный страх · 50 = нейтрально · 100 = экстремальная жадность")


@st.cache_data(show_spinner="загружаю данные…")
def _load() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    raw = load_raw_from_cache()
    df  = build_raw_components(raw)
    cols = [c for c in ("IMOEX", "RTSI") if c in raw["indices"].columns]
    market = raw["indices"][cols].copy()
    return df, market, raw


try:
    raw_df, market_df, raw_dict = _load()
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

MARKET_COLORS  = {"IMOEX": "#3b82f6", "RTSI": "#a855f7"}
MARKET_LABELS  = {"IMOEX": "IMOEX (рублёвый)",
                  "RTSI":  "RTS (долларовый)"}


# =========================================================================
# Сайдбар
# =========================================================================
st.sidebar.header("Общие настройки")

final_ema = st.sidebar.slider(
    "сглаживание итогового индекса (дни)", 1, 30, 5,
    key="final_ema",
    help="На сколько дней усреднять итоговую линию индекса. "
         "1 — без сглаживания, видны все дрожания. "
         "5 — лёгкое сглаживание (по умолчанию). "
         "30 — очень плавная линия, но реакция на новости запаздывает.")

rvi_fear_gate = st.sidebar.slider(
    "RVI как фильтр страха", 0.0, 1.0, 0.5, step=0.05,
    key="rvi_fear_gate",
    help="RVI работает не только как обычный фактор, но и как фильтр "
         "над страховой половиной композита.\n\n"
         "Когда RVI спокоен (его оценка > 0), он *сжимает* итоговый "
         "индекс к нейтрали 50, даже если другие факторы кричат страх. "
         "Жадная половина композита не трогается — фильтр действует "
         "только на «страховое плечо».\n\n"
         "• 0.0 — фильтр выключен\n"
         "• 0.5 — мягко: при rvi_score=50 страх сжимается на 25%, при "
         "rvi_score=100 — на 50%\n"
         "• 1.0 — жёстко: при rvi_score=50 страх сжимается на 50%, при "
         "rvi_score=100 индекс пригвождён к нейтрали (страх запрещён)")

period = st.sidebar.selectbox(
    "период на графиках",
    ["1y", "3y", "5y", "all"], index=2,
    key="period",
    help="Сколько истории показывать. На сами вычисления не влияет — "
         "только на масштаб картинки.")

corr_window = st.sidebar.slider(
    "окно скользящей корреляции (дни)", 20, 252, 60,
    key="corr_window",
    help="За сколько последних торговых дней считать корреляцию каждого "
         "фактора с рынком. Меньше — более «нервная» корреляция, "
         "больше — плавная.")

fwd_horizon = st.sidebar.select_slider(
    "горизонт прогноза доходности (дни)",
    options=[1, 5, 10, 20, 60], value=20,
    key="fwd_horizon",
    help="На сколько дней вперёд смотрим доходность рыночного индекса, "
         "чтобы оценить «насколько фактор предсказал движение». "
         "1 = завтра, 20 = через месяц, 60 = через квартал.")

st.sidebar.divider()
st.sidebar.subheader("Рыночные индексы для сравнения")
st.sidebar.caption("Накладываются на главный график; используются в "
                   "корреляциях.")
show_imoex = st.sidebar.checkbox(
    "IMOEX (рублёвый)", value=True, key="show_imoex",
    help="Главный индекс Мосбиржи в рублях. Базовый ориентир для "
         "российского рынка.")
show_rtsi = st.sidebar.checkbox(
    "RTS (долларовый)", value=False, key="show_rtsi",
    help="Тот же набор акций, но пересчитанный в долларах. Падает "
         "сильнее IMOEX когда рубль слабеет; полезен чтобы видеть "
         "реальную долларовую динамику и страх иностранных инвесторов.")
corr_market = st.sidebar.radio(
    "корреляции считать с",
    [c for c in ("IMOEX", "RTSI") if c in market_df.columns],
    format_func=lambda k: MARKET_LABELS[k],
    horizontal=True, key="corr_market",
    help="С каким индексом сравнивать факторы во вкладке «Корреляции».")

st.sidebar.divider()
st.sidebar.subheader("Настройки факторов")
st.sidebar.caption("Каждый фактор раскрывается отдельно. Веса нормируются "
                   "автоматически — если выключить какой-то, остальные "
                   "перераспределяют его долю.")

transforms: dict[str, Transform] = {}
weights:    dict[str, float] = {}
enabled:    dict[str, bool] = {}

for key in COMPONENT_KEYS:
    label = COMPONENT_RU[key]
    d = DEFAULT_TRANSFORMS[key]

    # Чекбокс «включить» — снаружи expander'а: один клик включает/выключает.
    enabled[key] = st.sidebar.checkbox(
        label, value=True, key=f"en_{key}",
        help=COMPONENT_DESC[key])

    with st.sidebar.expander("⚙ настройки", expanded=False):
        st.caption(COMPONENT_DESC[key])

        weights[key] = st.slider(
            "вес фактора (%)", 0, 100,
            int(DEFAULT_WEIGHTS[key]*100), key=f"w_{key}",
            help="Насколько важен этот фактор в итоговом индексе. "
                 "Все включённые веса нормируются в сумму 100 %.")

        modes = ["both", "fear_only", "greed_only"]
        signal_mode = st.radio(
            "вклад фактора в индекс",
            modes,
            format_func=lambda m: SIGNAL_MODE_RU[m],
            index=modes.index(d.signal_mode),
            key=f"sm_{key}", horizontal=True,
            help=SIGNAL_MODE_HELP)

        greed_residual = float(d.greed_residual)
        fear_residual  = float(d.fear_residual)

        kinds = ["pct_rank", "thresholds", "level_momentum"]
        kind = st.radio(
            "способ оценки",
            kinds,
            format_func=lambda k: KIND_RU[k],
            index=kinds.index(d.kind),
            key=f"k_{key}", horizontal=True,
            help="Как превратить сырое значение в шкалу 0–100.")
        st.caption(KIND_HELP[kind])

        ema_span = st.slider(
            "сглаживание этого компонента (дни)",
            1, 30, d.ema_span, key=f"e_{key}",
            help="На сколько дней усреднять оценку именно этого фактора. "
                 "1 — без сглаживания. Большие значения убирают "
                 "краткосрочные дёргания.")

        if kind == "pct_rank":
            window = st.select_slider(
                "окно ранга (дни)",
                options=[60, 120, 252, 504, 756],
                value=d.window, key=f"win_{key}",
                help="По какой истории считать перцентильный ранг. "
                     "252 = один торговый год. Меньше — быстрее "
                     "реагирует, но забывает прошлые экстремумы. "
                     "Больше — стабильнее, но медленнее.")
            higher_is_greed = st.radio(
                "большое значение =",
                ["жадность", "страх"],
                index=(0 if d.higher_is_greed else 1),
                key=f"h_{key}", horizontal=True,
                help="Что означает высокий показатель: жадность "
                     "(приток в акции, рост широты рынка) или страх "
                     "(рост волатильности, отток в кэш)."
            ) == "жадность"
            transforms[key] = Transform(kind="pct_rank", window=window,
                                        higher_is_greed=higher_is_greed,
                                        ema_span=ema_span,
                                        signal_mode=signal_mode,
                                        greed_residual=greed_residual,
                                        fear_residual=fear_residual)

        elif kind == "thresholds":
            raw_min, raw_max = float(raw_df[key].min()), float(raw_df[key].max())
            greed_at = st.number_input(
                "значение для жадности (=100)",
                value=float(d.greed_at if d.greed_at is not None else raw_min),
                key=f"g_{key}",
                help="При каком сыром значении показатель отображается "
                     "как 100 (максимальная жадность).")
            fear_at = st.number_input(
                "значение для страха (=0)",
                value=float(d.fear_at if d.fear_at is not None else raw_max),
                key=f"f_{key}",
                help="При каком сыром значении показатель отображается "
                     "как 0 (максимальный страх).")
            tail_emphasis = float(d.tail_emphasis)
            transforms[key] = Transform(kind="thresholds",
                                        greed_at=greed_at, fear_at=fear_at,
                                        tail_emphasis=tail_emphasis,
                                        ema_span=ema_span,
                                        signal_mode=signal_mode,
                                        greed_residual=greed_residual,
                                        fear_residual=fear_residual)

        else:  # level_momentum
            center_kinds = ["fixed", "ema"]
            center_kind_ru = {"fixed": "фиксированное число",
                              "ema":   "EMA от показателя"}
            lm_center_kind = st.radio(
                "уровень: тип центра",
                center_kinds,
                format_func=lambda k: center_kind_ru[k],
                index=center_kinds.index(d.lm_center_kind),
                key=f"lmck_{key}", horizontal=True,
                help="Что брать за нейтральный уровень (=50 на шкале):\n\n"
                     "• «фиксированное число» — постоянная константа, "
                     "которую ты задаёшь вручную.\n\n"
                     "• «EMA от показателя» — скользящая EMA самого "
                     "показателя. Динамический «свой ноль»: когда "
                     "значение ниже своей EMA — нейтрально, выше — "
                     "отклонение растёт в страх (особенно полезно для "
                     "RVI, который имеет долгие циклы).")
            if lm_center_kind == "fixed":
                lm_center = st.number_input(
                    "уровень: центр (нейтральное значение)",
                    value=float(d.lm_center), key=f"lmc_{key}",
                    help="Сырое значение, которое даёт оценку 50.")
                lm_center_ema_span = int(d.lm_center_ema_span)
            else:
                lm_center_ema_span = st.slider(
                    "уровень: окно EMA (дни)",
                    20, 500, int(d.lm_center_ema_span), step=10,
                    key=f"lmce_{key}",
                    help="Период EMA для динамического центра. "
                         "200 — классическая EMA-200 (≈ долгосрочный "
                         "режим). Меньше — более чувствительный «нуль», "
                         "больше — стабильнее.")
                lm_center = float(d.lm_center)  # не используется
            lm_steepness = st.slider(
                "уровень: крутизна k", 0.02, 0.30, float(d.lm_steepness),
                step=0.01, key=f"lmk_{key}",
                help="Насколько резко индекс реагирует на отклонение "
                     "от центра. 0.04 — пологая кривая, плавный переход. "
                     "0.15 — резкая, любое отклонение быстро уносит "
                     "в крайности.")
            lm_delta_n = st.slider(
                "импульс: окно дельты (дни)", 1, 30, int(d.lm_delta_n),
                key=f"lmn_{key}",
                help="За сколько дней меряем изменение показателя. "
                     "5 = смотрим, насколько вырос/упал за неделю.")
            lm_z_window = st.select_slider(
                "импульс: окно нормализации",
                options=[60, 120, 252, 504, 756],
                value=int(d.lm_z_window), key=f"lmzw_{key}",
                help="На какой истории считать «обычную скорость "
                     "изменения», чтобы понять — текущая дельта это "
                     "много или норма.")
            lm_momentum_scale = st.slider(
                "импульс: сила (пунктов на 1σ)",
                0.0, 40.0, float(d.lm_momentum_scale), step=1.0,
                key=f"lms_{key}",
                help="Если показатель ускорился на 1 стандартное "
                     "отклонение от своей нормы — на сколько пунктов "
                     "сдвигается импульсная составляющая. 15 — "
                     "умеренно, 30 — сильно.")
            lm_level_weight = st.slider(
                "смешивание: вес уровня α", 0.0, 1.0,
                float(d.lm_level_weight), step=0.05, key=f"lmw_{key}",
                help="Сколько веса даём абсолютному уровню vs импульсу. "
                     "1.0 = только уровень (как порог). "
                     "0.0 = только импульс. "
                     "0.6 = баланс с лёгким перевесом в уровень.")
            lm_higher_is_fear = st.radio(
                "большое значение =", ["страх", "жадность"],
                index=(0 if d.lm_higher_is_fear else 1),
                key=f"lmh_{key}", horizontal=True,
                help="Что означает высокий показатель: страх (RVI, "
                     "отток в MM) или жадность."
            ) == "страх"
            transforms[key] = Transform(
                kind="level_momentum",
                lm_center=lm_center,
                lm_center_kind=lm_center_kind,
                lm_center_ema_span=lm_center_ema_span,
                lm_steepness=lm_steepness,
                lm_delta_n=lm_delta_n, lm_z_window=lm_z_window,
                lm_momentum_scale=lm_momentum_scale,
                lm_level_weight=lm_level_weight,
                lm_higher_is_fear=lm_higher_is_fear,
                ema_span=ema_span,
                signal_mode=signal_mode,
                greed_residual=greed_residual,
                fear_residual=fear_residual)


total_w = sum(weights[k] for k in weights if enabled[k])
norm_weights = {k: (weights[k] / total_w if enabled[k] and total_w else 0.0)
                for k in weights}

params = FearParams(transforms=transforms, weights=norm_weights,
                    enabled=enabled, final_ema_span=final_ema,
                    rvi_fear_gate=rvi_fear_gate)
scores = compose(raw_df, params)

if period != "all":
    cutoff = scores.index.max() - pd.DateOffset(years={"1y":1,"3y":3,"5y":5}[period])
    scores      = scores.loc[scores.index >= cutoff]
    raw_show    = raw_df.loc[raw_df.index >= cutoff]
    market_show = market_df.loc[market_df.index >= cutoff]
else:
    raw_show, market_show = raw_df, market_df

# какие рыночные индексы рисовать
selected_markets = []
if show_imoex and "IMOEX" in market_show.columns: selected_markets.append("IMOEX")
if show_rtsi  and "RTSI"  in market_show.columns: selected_markets.append("RTSI")


# =========================================================================
# Сегодняшнее значение
# =========================================================================
last_row = scores.dropna(subset=["fng"]).iloc[-1] if scores["fng"].notna().any() else None
if last_row is not None:
    cls_ru = CLASS_RU[classify(last_row["fng"])]
    color = next((c[3] for c in CLASS_BANDS
                  if c[0] <= last_row["fng"] < c[1]), "#9ca3af")
    st.markdown(f"<h3 style='color:{color}'>"
                f"Сегодня: {last_row['fng']:.1f} — {cls_ru}</h3>",
                unsafe_allow_html=True)
    cols = st.columns(3)
    for i, key in enumerate(COMPONENT_KEYS):
        v  = last_row.get(f"{key}_score",    np.nan)
        st_today = last_row.get(f"{key}_strength", 1.0)
        eff_pct = norm_weights[key] * (st_today if not np.isnan(st_today) else 0) * 100
        # base vs effective weight
        if transforms[key].signal_mode == "both":
            delta_str = f"вес {norm_weights[key]*100:.0f}%"
        else:
            delta_str = (f"вес {norm_weights[key]*100:.0f}% × "
                         f"сила {(st_today*100 if not np.isnan(st_today) else 0):.0f}%"
                         f" = {eff_pct:.0f}%")
        cols[i].metric(
            COMPONENT_RU[key].split(". ", 1)[1],
            f"{v:.0f}" if not np.isnan(v) else "—",
            delta=delta_str,
            help=COMPONENT_DESC[key] + "\n\n"
                 f"Режим: {SIGNAL_MODE_RU[transforms[key].signal_mode]}.")


# =========================================================================
# Вкладки
# =========================================================================
tab_overview, tab_components, tab_corr = st.tabs(
    ["Обзор", "Подробно по факторам", "Корреляции с IMOEX"])


# ---------- Обзор ----------------------------------------------------------
with tab_overview:
    st.caption(
        "Жёлтая линия — итоговая оценка из всех включённых факторов. "
        "Цветные горизонтальные полосы — границы классов: красный = страх, "
        "зелёный = жадность. Пунктирные линии (правая ось) — рыночные "
        "индексы (включай-выключай в сайдбаре): синий — IMOEX, "
        "фиолетовый — RTS.")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=scores.index, y=scores["fng"],
                             name="Индекс страха-жадности",
                             line=dict(color="#fbbf24", width=2.5)),
                  secondary_y=False)
    for mkey in selected_markets:
        fig.add_trace(go.Scatter(
            x=market_show.index, y=market_show[mkey],
            name=MARKET_LABELS[mkey],
            line=dict(color=MARKET_COLORS[mkey], width=1, dash="dot")),
                      secondary_y=True)
    for lo, hi, _, c in CLASS_BANDS:
        fig.add_hrect(y0=lo, y1=hi, fillcolor=c, opacity=0.10, line_width=0,
                      secondary_y=False)
    fig.update_layout(height=460, margin=dict(t=30, b=10, l=10, r=10),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02))
    fig.update_yaxes(title="Индекс (0=страх, 100=жадность)", range=[0, 100],
                     secondary_y=False)
    right_title = " / ".join(selected_markets) if selected_markets else "—"
    fig.update_yaxes(title=right_title, secondary_y=True, showgrid=False)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Шесть факторов по отдельности")
    st.caption("Каждый график — оценка одного фактора по той же шкале "
               "0–100. Пунктир посередине — нейтраль (50).")
    fig2 = make_subplots(rows=1, cols=3, shared_xaxes=True,
                         subplot_titles=[COMPONENT_RU[k]
                                         for k in COMPONENT_KEYS])
    for i, key in enumerate(COMPONENT_KEYS):
        s = scores.get(f"{key}_score")
        fig2.add_trace(go.Scatter(x=s.index, y=s,
                                  line=dict(color="#fbbf24", width=1.6),
                                  showlegend=False), row=1, col=i + 1)
        fig2.add_hline(y=50, line=dict(color="#9ca3af", width=1, dash="dot"),
                       row=1, col=i + 1)
    fig2.update_layout(height=320, margin=dict(t=40, b=10, l=10, r=10))
    for c in (1, 2, 3):
        fig2.update_yaxes(range=[0, 100], row=1, col=c)
    st.plotly_chart(fig2, use_container_width=True)


# ---------- Подробно по факторам -------------------------------------------
with tab_components:
    pick = st.selectbox(
        "выбрать фактор", COMPONENT_KEYS,
        format_func=lambda k: COMPONENT_RU[k],
        key="dd_pick")
    raw_s   = raw_show[pick]
    score_s = scores[f"{pick}_score"]
    t = transforms[pick]

    st.markdown(f"**{COMPONENT_RU[pick]}**")
    st.caption(COMPONENT_DESC[pick])

    cols_info = st.columns([3, 2])
    with cols_info[0]:
        st.markdown(f"**вклад в индекс:** `{SIGNAL_MODE_RU[t.signal_mode]}`")
        st.markdown(f"**способ оценки:** `{KIND_RU[t.kind]}`")
        if t.kind == "thresholds":
            extras = (f" · акцент на края p=`{t.tail_emphasis}`"
                      if t.tail_emphasis != 1.0 else "")
            st.markdown(f"страх (=0) при `{t.fear_at}` · "
                        f"жадность (=100) при `{t.greed_at}`{extras}")
        elif t.kind == "pct_rank":
            higher = "жадность" if t.higher_is_greed else "страх"
            st.markdown(f"окно `{t.window}` дней · большое значение = {higher}")
        else:
            higher = "страх" if t.lm_higher_is_fear else "жадность"
            center_str = (f"EMA-{t.lm_center_ema_span}"
                          if t.lm_center_kind == "ema"
                          else f"const={t.lm_center}")
            st.markdown(
                f"центр=`{center_str}`, k=`{t.lm_steepness}` · "
                f"импульс Δ=`{t.lm_delta_n}`, z-окно=`{t.lm_z_window}`, "
                f"сила=`{t.lm_momentum_scale}` · "
                f"вес уровня α=`{t.lm_level_weight}` · большое = {higher}")
        # signal strength today
        st_today = scores.get(f"{pick}_strength")
        if st_today is not None and not st_today.dropna().empty:
            st_now = float(st_today.dropna().iloc[-1])
        else:
            st_now = 1.0
        eff_w_today = norm_weights[pick] * st_now
        st.markdown(
            f"сглаживание `{t.ema_span}` · базовый вес "
            f"`{norm_weights[pick]*100:.0f}%` · сила голоса сейчас "
            f"`{st_now*100:.0f}%` · эффективный вес "
            f"`{eff_w_today*100:.1f}%` · включён: "
            f"`{'да' if enabled[pick] else 'нет'}`")

    st.markdown("**Сырое значение vs итоговая оценка**")
    st.caption("Синий — сырой показатель (левая ось). Жёлтый — оценка "
               "по шкале страх-жадность (правая ось). Цветные полосы — "
               "классы оценки.")

    fig_dd = make_subplots(specs=[[{"secondary_y": True}]])
    fig_dd.add_trace(go.Scatter(x=raw_s.index, y=raw_s, name="сырое значение",
                                line=dict(color="#3b82f6", width=1)),
                     secondary_y=False)
    if (t.kind == "level_momentum" and t.lm_center_kind == "ema"):
        ema_line = raw_s.ewm(span=t.lm_center_ema_span,
                             adjust=False, min_periods=1).mean()
        fig_dd.add_trace(go.Scatter(
            x=ema_line.index, y=ema_line,
            name=f"центр (EMA-{t.lm_center_ema_span})",
            line=dict(color="#22c55e", width=1.2, dash="dot")),
                         secondary_y=False)
    fig_dd.add_trace(go.Scatter(x=score_s.index, y=score_s, name="оценка (0..100)",
                                line=dict(color="#fbbf24", width=1.6)),
                     secondary_y=True)
    for lo, hi, _, c in CLASS_BANDS:
        fig_dd.add_hrect(y0=lo, y1=hi, fillcolor=c, opacity=0.07, line_width=0,
                         secondary_y=True)
    if t.kind == "thresholds":
        fig_dd.add_hline(y=t.fear_at,  line=dict(color="#dc2626", dash="dash"),
                         secondary_y=False)
        fig_dd.add_hline(y=t.greed_at, line=dict(color="#10b981", dash="dash"),
                         secondary_y=False)
    fig_dd.update_layout(height=420, margin=dict(t=30, b=10, l=10, r=10),
                         legend=dict(orientation="h", yanchor="bottom", y=1.02))
    fig_dd.update_yaxes(title="сырое значение", secondary_y=False)
    fig_dd.update_yaxes(title="оценка (0=страх, 100=жадность)",
                        range=[0, 100], secondary_y=True)
    st.plotly_chart(fig_dd, use_container_width=True)

    if t.signal_mode != "both":
        st.markdown("**Сила голоса по времени**")
        st.caption(
            "Этот фактор в режиме «"
            f"{SIGNAL_MODE_RU[t.signal_mode]}» — он голосует не всегда. "
            "Когда сила = 0, фактор исключается из взвешенного среднего "
            "(другие факторы делят его долю). Когда сила = 1 — голосует "
            "своим базовым весом полностью.")
        strength_s = scores[f"{pick}_strength"]
        fig_str = go.Figure(go.Scatter(
            x=strength_s.index, y=strength_s * 100,
            line=dict(color="#22c55e", width=1.5),
            fill='tozeroy', fillcolor='rgba(34,197,94,0.15)',
            showlegend=False))
        fig_str.update_layout(
            height=200, margin=dict(t=10, b=10, l=10, r=10),
            yaxis_title="сила голоса (%)",
            yaxis_range=[0, 105])
        st.plotly_chart(fig_str, use_container_width=True)

    # ----- Калибратор k для level_momentum -----
    if t.kind == "level_momentum":
        with st.expander("🎚 Сравнение разных k (крутизна сигмоиды)",
                         expanded=False):
            st.caption(
                "Подбираем чувствительность фактора. На графике несколько "
                "вариантов оценки фактора, посчитанных при разных значениях "
                "k. Все остальные параметры (центр, импульс, режим) "
                "берутся из текущей конфигурации. На фоне — вертикальные "
                "линии помечают крупные исторические кризисы — посмотри, "
                "как глубоко в страх каждый k уносит индикатор в эти дни.")

            k_options = [0.04, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12, 0.15]
            ks = st.multiselect(
                "значения k для сравнения",
                k_options,
                default=[0.04, 0.06, 0.08, 0.10],
                key=f"kcal_{pick}",
                help="Какие варианты крутизны сигмоиды наложить на график.")

            if ks:
                # Compute score history for each k using current other params
                k_scores = pd.DataFrame(index=raw_show.index)
                for k_val in ks:
                    t_test = Transform(**{**t.__dict__,
                                           "lm_steepness": float(k_val)})
                    full_score = apply_transform(raw_df[pick], t_test)
                    k_scores[f"k={k_val}"] = full_score.loc[raw_show.index]

                k_palette = ["#dc2626","#f97316","#eab308","#22c55e",
                             "#3b82f6","#a855f7","#ec4899","#14b8a6"]

                fig_k = go.Figure()
                for i, col in enumerate(k_scores.columns):
                    fig_k.add_trace(go.Scatter(
                        x=k_scores.index, y=k_scores[col],
                        name=col,
                        line=dict(color=k_palette[i % len(k_palette)],
                                  width=1.5)))
                for lo, hi, _, c in CLASS_BANDS:
                    fig_k.add_hrect(y0=lo, y1=hi, fillcolor=c,
                                    opacity=0.07, line_width=0)
                # crisis date markers
                crisis_dates = [
                    ("2014-12-16", "Декабрь-2014 (валюта)"),
                    ("2020-03-18", "COVID-крах"),
                    ("2022-02-24", "Старт СВО"),
                    ("2022-09-21", "Мобилизация"),
                ]
                for d_str, label in crisis_dates:
                    d = pd.Timestamp(d_str)
                    if k_scores.index.min() <= d <= k_scores.index.max():
                        # workaround: pass datestring + manual annotation
                        # to avoid plotly+pandas Timestamp arithmetic bug
                        fig_k.add_shape(
                            type="line", x0=d_str, x1=d_str,
                            y0=0, y1=100, yref="y",
                            line=dict(color="#9ca3af", dash="dash", width=1))
                        fig_k.add_annotation(
                            x=d_str, y=100, yref="y",
                            text=label, showarrow=False,
                            yshift=10, font=dict(size=10, color="#9ca3af"))
                fig_k.update_layout(
                    height=440, margin=dict(t=40, b=10, l=10, r=10),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    yaxis_title="оценка фактора (0=страх, 100=жадность)",
                    yaxis_range=[0, 100])
                st.plotly_chart(fig_k, use_container_width=True)

                # Crisis-day comparison table
                st.markdown("**Оценка в кризисные даты при разных k**")
                full_scores_per_k: dict[str, pd.Series] = {}
                for k_val in ks:
                    t_test = Transform(**{**t.__dict__,
                                           "lm_steepness": float(k_val)})
                    full_scores_per_k[f"k={k_val}"] = apply_transform(
                        raw_df[pick], t_test)

                rows = []
                for d_str, label in crisis_dates + [
                    ("2025-01-15", "Янв-2025 (для контекста)"),
                    (str(raw_df.index[-1].date()), "Последняя точка"),
                ]:
                    d = pd.Timestamp(d_str)
                    if d not in raw_df.index:
                        continue
                    raw_v = raw_df.loc[d, pick]
                    row = {"дата": d_str, "событие": label,
                           "raw": f"{raw_v:.1f}" if pd.notna(raw_v) else "—"}
                    for k_val in ks:
                        sc = full_scores_per_k[f"k={k_val}"].get(d, np.nan)
                        row[f"k={k_val}"] = (f"{sc:.1f}"
                                             if pd.notna(sc) else "—")
                    rows.append(row)
                if rows:
                    st.dataframe(pd.DataFrame(rows), hide_index=True,
                                 use_container_width=True)

                # quantile-based "what does each k say at p90/p95/p99 of dev"
                if t.lm_center_kind == "ema":
                    dev = (raw_df[pick]
                           - raw_df[pick].ewm(span=t.lm_center_ema_span,
                                              adjust=False, min_periods=1).mean())
                    st.markdown("**Что даёт каждый k на типичных отклонениях "
                                "от EMA**")
                    qs = [0.50, 0.75, 0.90, 0.95, 0.99]
                    qrows = []
                    for q in qs:
                        dv = dev.quantile(q)
                        row = {"квантиль": f"p{int(q*100):02d}",
                               "отклонение": f"{dv:+.1f}"}
                        for k_val in ks:
                            t_test = Transform(**{**t.__dict__,
                                                   "lm_steepness": float(k_val),
                                                   "lm_level_weight": 1.0,
                                                   "lm_center_kind": "fixed",
                                                   "lm_center": 0.0,
                                                   "ema_span": 1})
                            sc = apply_transform(
                                pd.Series([dv]*5, name=pick), t_test).iloc[-1]
                            row[f"k={k_val}"] = f"{sc:.1f}"
                        qrows.append(row)
                    st.dataframe(pd.DataFrame(qrows), hide_index=True,
                                 use_container_width=True)

    cdf_left, cdf_right = st.columns(2)
    with cdf_left:
        st.markdown("**Распределение сырого значения**")
        st.caption("Гистограмма — как часто встречаются разные значения "
                   "за всю историю. Внизу — квантили: p05 = «нижние 5% "
                   "значений ниже этого числа», p50 = медиана, и т.д.")
        clean = raw_s.dropna()
        fig_h = go.Figure(go.Histogram(x=clean, nbinsx=60, marker_color="#3b82f6"))
        if t.kind == "thresholds":
            fig_h.add_vline(x=t.fear_at,  line=dict(color="#dc2626", dash="dash"))
            fig_h.add_vline(x=t.greed_at, line=dict(color="#10b981", dash="dash"))
        elif t.kind == "level_momentum":
            center_x = (raw_s.ewm(span=t.lm_center_ema_span, adjust=False,
                                   min_periods=1).mean().iloc[-1]
                        if t.lm_center_kind == "ema" else t.lm_center)
            fig_h.add_vline(x=center_x,
                            line=dict(color="#9ca3af", dash="dash"))
        fig_h.update_layout(height=300, margin=dict(t=10, b=10, l=10, r=10),
                            showlegend=False)
        st.plotly_chart(fig_h, use_container_width=True)
        if not clean.empty:
            q = clean.quantile([0.05, 0.25, 0.5, 0.75, 0.95]).round(3)
            st.write({f"p{int(k*100):02d}": v for k, v in q.items()})

    with cdf_right:
        if t.kind == "level_momentum":
            if t.lm_center_kind == "ema":
                ema_now = raw_s.ewm(span=t.lm_center_ema_span,
                                    adjust=False, min_periods=1).mean().iloc[-1]
                st.markdown("**Кривая уровня** (центр = EMA сейчас)")
                st.caption(
                    f"Текущая EMA-{t.lm_center_ema_span} от показателя ≈ "
                    f"`{ema_now:.2f}`. Кривая рисуется относительно этого "
                    "значения; завтра центр сдвинется, и кривая поедет "
                    "вместе с ним. Импульс добавляется поверх.")
            else:
                st.markdown("**Кривая уровня** (импульс добавляется поверх)")
                st.caption("Сигмоида: как сырое значение превращается в "
                           "оценку, если бы импульсной части не было. "
                           "Импульс гнёт линию вверх/вниз ещё.")
        else:
            st.markdown("**Кривая преобразования** (сырое → оценка)")
            st.caption("Какая оценка получается для каждого возможного "
                       "сырого значения.")
        if not raw_s.dropna().empty:
            grid = np.linspace(float(raw_s.min()), float(raw_s.max()), 500)
            if t.kind == "level_momentum":
                # Изоляция уровня: α=1, и при ema-центре фиксируем
                # его последним значением, иначе EMA по синтетической
                # сетке даст бессмыслицу.
                fixed_center = (
                    raw_s.ewm(span=t.lm_center_ema_span, adjust=False,
                              min_periods=1).mean().iloc[-1]
                    if t.lm_center_kind == "ema" else t.lm_center)
                t_level = Transform(**{**t.__dict__,
                                        "lm_level_weight": 1.0,
                                        "lm_center_kind": "fixed",
                                        "lm_center": float(fixed_center),
                                        "ema_span": 1})
                mapping = apply_transform(pd.Series(grid, name=pick), t_level)
            else:
                mapping = apply_transform(
                    pd.Series(grid, name=pick),
                    Transform(**{**t.__dict__, "ema_span": 1}))
            fig_curve = go.Figure(go.Scatter(x=grid, y=mapping,
                                              line=dict(color="#fbbf24",
                                                        width=2.5)))
            for lo, hi, _, c in CLASS_BANDS:
                fig_curve.add_hrect(y0=lo, y1=hi, fillcolor=c, opacity=0.10,
                                    line_width=0)
            fig_curve.update_layout(height=300,
                                    margin=dict(t=10, b=10, l=10, r=10),
                                    yaxis_title="оценка",
                                    xaxis_title="сырое значение",
                                    yaxis_range=[0,100], showlegend=False)
            st.plotly_chart(fig_curve, use_container_width=True)


# ---------- Корреляции -----------------------------------------------------
with tab_corr:
    market_label = MARKET_LABELS.get(corr_market, corr_market)
    horizon_label = f"{fwd_horizon}-дневная будущая доходность {market_label}"
    fwd = market_returns(raw_dict, corr_market, horizon=fwd_horizon)
    fwd_show = fwd.loc[fwd.index >= scores.index.min()]

    st.markdown(f"### Скользящая корреляция фактора с {horizon_label}")
    st.caption(f"Окно = {corr_window} торговых дней. "
               "Корреляция Пирсона. Близко к 0 — фактор не помогает "
               "предсказывать движение рынка. Положительная — фактор "
               "растёт перед ростом рынка. Отрицательная — растёт перед "
               "падением. Меняется со временем — это нормально для "
               "индекса страха.")
    corr_df = pd.DataFrame(index=scores.index)
    for key in COMPONENT_KEYS:
        corr_df[key] = rolling_correlation(scores[f"{key}_score"], fwd, corr_window)

    fig_c = go.Figure()
    palette = ["#dc2626","#22c55e","#3b82f6"]
    for i, key in enumerate(COMPONENT_KEYS):
        fig_c.add_trace(go.Scatter(x=corr_df.index, y=corr_df[key],
                                   name=COMPONENT_RU[key],
                                   line=dict(color=palette[i], width=1.4)))
    fig_c.add_hline(y=0, line=dict(color="#9ca3af", dash="dot"))
    fig_c.update_layout(height=440, margin=dict(t=30, b=10, l=10, r=10),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        yaxis_title="корреляция Пирсона",
                        yaxis_range=[-1,1])
    st.plotly_chart(fig_c, use_container_width=True)

    st.markdown("### Корреляция за весь видимый период")
    st.caption("Слева — корреляция оценки фактора (после преобразования) "
               "с будущей доходностью IMOEX. Справа — корреляция сырого "
               "показателя без преобразований. Если оценка коррелирует "
               "хуже, чем raw — преобразование «съедает» сигнал.")
    static = pd.DataFrame({
        "фактор": [COMPONENT_RU[k] for k in COMPONENT_KEYS],
        f"оценка vs {market_label}":
            [scores[f"{k}_score"].corr(fwd) for k in COMPONENT_KEYS],
        f"сырое vs {market_label}":
            [raw_show[k].corr(fwd) for k in COMPONENT_KEYS],
    })
    st.dataframe(static.style.format(
        {col: "{:+.3f}" for col in static.columns if col != "фактор"}),
        use_container_width=True, hide_index=True)

    st.markdown(f"### Итоговый индекс vs будущая доходность {market_label}")
    fng_corr = scores["fng"].corr(fwd)
    st.markdown(f"**Корреляция Пирсона: {fng_corr:+.3f}**")
    st.caption("Каждая точка — один торговый день: по горизонтали "
               "значение индекса, по вертикали — фактическая доходность "
               f"{market_label} через {fwd_horizon} торговых дня(дней). "
               "Если есть облако с наклоном — индекс предсказывает.")
    df_sc = pd.concat([scores["fng"], fwd], axis=1).dropna()
    fig_s = go.Figure(go.Scatter(x=df_sc["fng"], y=df_sc[fwd.name],
                                 mode="markers",
                                 marker=dict(size=4, color="#3b82f6",
                                             opacity=0.5)))
    fig_s.update_layout(height=380, margin=dict(t=30, b=10, l=10, r=10),
                        xaxis_title="индекс страха-жадности",
                        yaxis_title=horizon_label)
    st.plotly_chart(fig_s, use_container_width=True)


# =========================================================================
# Экспорт
# =========================================================================
with st.expander("скачать / текущая конфигурация"):
    st.caption("Кнопка ниже — выгружает CSV со всеми оценками и "
               "композитом по дням. Сниппет на Python — текущую "
               "конфигурацию параметров; копируешь и потом вставишь в "
               "production-скрипт `Macro/compute_fear_history.py`.")
    st.download_button("scores.csv",
                       scores.to_csv().encode("utf-8"),
                       file_name="fng_scores.csv",
                       mime="text/csv")
    snippet_lines = ["FearParams(",
                     f"    final_ema_span={final_ema},",
                     f"    rvi_fear_gate={rvi_fear_gate},",
                     f"    weights={norm_weights},",
                     f"    enabled={enabled},",
                     "    transforms={"]
    for k, t in transforms.items():
        if t.kind == "level_momentum":
            snippet_lines.append(
                f"        {k!r}: Transform(kind='level_momentum', "
                f"lm_center={t.lm_center}, "
                f"lm_center_kind={t.lm_center_kind!r}, "
                f"lm_center_ema_span={t.lm_center_ema_span}, "
                f"lm_steepness={t.lm_steepness}, "
                f"lm_delta_n={t.lm_delta_n}, lm_z_window={t.lm_z_window}, "
                f"lm_momentum_scale={t.lm_momentum_scale}, "
                f"lm_level_weight={t.lm_level_weight}, "
                f"lm_higher_is_fear={t.lm_higher_is_fear}, "
                f"ema_span={t.ema_span}, signal_mode={t.signal_mode!r}),")
        elif t.kind == "thresholds":
            snippet_lines.append(
                f"        {k!r}: Transform(kind='thresholds', "
                f"greed_at={t.greed_at}, fear_at={t.fear_at}, "
                f"tail_emphasis={t.tail_emphasis}, "
                f"ema_span={t.ema_span}, signal_mode={t.signal_mode!r}),")
        else:
            snippet_lines.append(
                f"        {k!r}: Transform(kind='pct_rank', "
                f"window={t.window}, higher_is_greed={t.higher_is_greed}, "
                f"ema_span={t.ema_span}, signal_mode={t.signal_mode!r}),")
    snippet_lines += ["    },", ")"]
    st.code("\n".join(snippet_lines), language="python")


# Сохранить текущие значения виджетов на диск (выполняется на каждый рендер).
_save_state()
