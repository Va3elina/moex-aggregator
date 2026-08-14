/**
 * EmbedIndicators — движок пользовательских индикаторов на графике (модель ТЗ:
 * как в TradingView, где индикатор добавляется списком, настраивается по строке
 * и живёт либо поверх графика, либо отдельной панелью).
 *
 * pane в модели: 0 — наложение поверх ценового графика (MA, EMA, Боллинджер),
 * 1+ — отдельная панель со своей шкалой (RSI, ATR, объёмы). У последних ценовая
 * ось не годится в принципе: RSI это 0..100 против десятков тысяч у цены, на
 * одной шкале он превратился бы в плоскую линию у края — поэтому «вернуть на
 * график» им недоступно (overlayOk).
 *
 * Профиль объёма стоит особняком: он вообще не временной ряд и не может быть
 * серией, поэтому наружу уходит не через indicatorSeriesByPane, а отдельным
 * описанием для примитива (volumeProfileSpec). Панель у него всегда 0 —
 * гистограмма по цене имеет смысл только рядом с ценой (ownPaneOk).
 *
 * Компонент НЕ трогает сам чарт: индикаторы отдаются наружу массивом серий,
 * сгруппированным по панелям, а embed уже собирает из этого panes для
 * LwChartPanes.
 */
import { Fragment, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { Eye, EyeOff, Settings2, X as XIcon, Trash2, MoreHorizontal, ChevronRight, LineChart } from 'lucide-react';
import type { LwSeries } from '../../components/chart/lwTypes';
import type { VolumeProfileSpec } from '../../components/LwChartPanes';
import { VP_DEFAULTS } from '../../components/chart/volumeProfilePrimitive';
import {
  sma, ema, bollinger, rsi, atr, trueRange, volumeBars, volumeMa, wma, rma, withSource,
  SOURCE_LABELS, VOLUME_UP, VOLUME_DOWN, type IndCandle, type IndPoint, type IndSource,
} from '../../utils/indicators';
import { useEmbedPersist } from './embedPersist';
import { usePortalTheme } from '../../hooks/usePortalTheme';
import { ToolbarMenuButton, CTL_FS, CTL_FW } from './EmbedToolbar';
import { ColorButton, type ElStyle } from './ColorPicker';

/** Имя нарочно НЕ IndKind: так уже называется вид панели в SandboxPage. */
export type IndicatorKind = 'ma' | 'ema' | 'bb' | 'rsi' | 'atr' | 'volume' | 'vp';

/**
 * От какого РЯДА считается индикатор. 'price' — свечи цены (единственный вариант
 * на большинстве графиков), 'oi' — ряд открытого интереса в окне ОИ.
 *
 * Отсутствие поля = 'price': так наборы, сохранённые до появления базиса, читаются
 * без миграции.
 */
export type IndBasis = 'price' | 'oi';

/** Один доступный базис в конкретном окне: id + подпись из самого окна (у цены —
 *  название актива, у ОИ — «Чистая позиция»/«Покупки», зависит от режима и
 *  показателя). */
export interface BasisOption {
  id: IndBasis;
  label: string;
  /** Цвет ряда — тот же, что у линии на графике и у квадратика в строке легенды.
   *  Окно обязано брать его из СВОЕЙ сборки серий, иначе выбор базиса и легенда
   *  разъедутся. Необязательный: у окон с единственным базисом (Сила рынка,
   *  ChartLab) выбора нет и рисовать маркер негде — см. PRICE_ONLY. */
  color?: string;
}

export interface IndicatorInst {
  id: string;
  kind: IndicatorKind;
  length: number;
  /** Множитель отклонения — только у полос Боллинджера. */
  mult?: number;
  /** null = цвет по палитре, из порядкового номера. */
  color: string | null;
  width: 1 | 2 | 3 | 4;
  visible: boolean;
  /** 0 = поверх графика, 1+ — отдельная панель.
   *  ⚠️ У наложений по базису 'oi' номер панели В СОСТОЯНИИ остаётся нулём:
   *  фактическая панель считается из текущего положения ряда ОИ (см.
   *  effectivePane). Иначе перенос ОИ вниз пришлось бы сопровождать миграцией
   *  всех навешенных на него индикаторов. */
  pane: number;
  /** От какого ряда считаем. undefined = 'price' (см. IndBasis). */
  basis?: IndBasis;
  /** К какому краю прижат профиль объёма. Только для kind='vp'. */
  side?: 'left' | 'right';
  /** Границы зон осциллятора (RSI: перекупленность/перепроданность). */
  upper?: number;
  lower?: number;

  // ── вкладка «Аргументы» ───────────────────────────────────────────────
  /** Что скармливаем индикатору: закрытие, максимум, типичная цена и т.д. */
  source?: IndSource;
  /** Сглаживание поверх индикатора — вторая линия (в TradingView «RSI-based MA»). */
  smoothType?: SmoothType;
  smoothLength?: number;
  /** Отклонение полос Боллинджера вокруг сглаживающей — только для smoothType='sma_bb'. */
  bbMult?: number;

  // ── вкладка «Стиль» ───────────────────────────────────────────────────
  /** Показывать саму линию индикатора. Выключают, когда нужна только её MA. */
  lineOn?: boolean;
  /** Цвет сглаживающей. null — жёлтый по умолчанию, как в терминалах. */
  maColor?: string | null;
  /** Пунктиры границ зон. */
  bandsOn?: boolean;
  /** Заливки зон. Отдельно от линий: кому-то мешает фон, а уровни нужны. */
  fillOn?: boolean;
  /** Знаков после запятой в подписях. undefined — авто. */
  precision?: number;
  /** Пилс последнего значения на ценовой шкале («Метки на ценовой шкале» в TV). */
  axisLabel?: boolean;
  /** Показывать значение в строке индикатора («Значения в строке статуса»). */
  statusValue?: boolean;
  /** Показывать параметры в подписи строки: «RSI 14» против «RSI». */
  statusArgs?: boolean;

  // ── объёмы ──────────────────────────────────────────────────────────────
  /** Скользящая по объёму (Volume MA). */
  volMaOn?: boolean;
  volMaLength?: number;
  /** Сглаживание ATR: у Уайлдера это RMA, но в TV на выбор. */
  smooth?: SmoothType;
  /** Стиль КАЖДОГО элемента отдельно: 'line' — сама линия, 'ma' —
   *  сглаживающая, 'upper'/'middle'/'lower' — границы зон, 'band'/'over'/'under'
   *  — заливки. Картой, а не плоскими полями: элементов у одного индикатора уже
   *  восемь, и на каждый нужны цвет, прозрачность, толщина и пунктир. */
  styles?: Record<string, ElStyle>;
}

interface KindDef {
  label: string;
  /** Подпись строки: «EMA 20», «Боллинджер 20×2». */
  title: (i: IndicatorInst) => string;
  defLength: number;
  defMult?: number;
  /** Куда кладём по умолчанию: 0 — поверх графика, 1 — отдельной панелью.
   *  У RSI/ATR/объёма своя шкала (0..100 против десятков тысяч у цены), им
   *  на ценовой оси делать нечего. */
  defaultPane: 0 | 1;
  /** Можно ли класть поверх цены вообще (пункт «Вернуть на график»). */
  overlayOk: boolean;
  /** Можно ли вынести в свою панель. false — у профиля объёма: он рисуется
   *  по ценовой шкале и в пустой панели показывать было бы нечего. */
  ownPaneOk?: boolean;
  /** Нужен объём в свече. Такие индикаторы прячем там, где его нет — иначе
   *  пользователь добавляет строку, а на графике не появляется ничего. */
  needsVolume?: boolean;
  /** Подпись поля «Период», если оно значит не период. */
  lengthLabel?: string;
  /** Дефолтные границы зон. Есть — значит у индикатора есть перекупленность и
   *  перепроданность, и он рисует их разметкой. */
  bands?: { upper: number; lower: number; middle: number | null };
  /** Короткое имя для заголовков секций окна («RSI», а не «RSI (индекс…)»). */
  shortName?: string;
  /** Есть ли выбор источника цены. У объёма и профиля его нет: они считаются от
   *  объёма, а не от цены. */
  hasSource?: boolean;
  /** Можно ли положить сверху сглаживающую (вторую линию). */
  hasSmoothing?: boolean;
  /** Раздельные цвета растущий/нисходящий + Volume MA (вкладка «Стиль» у объёмов). */
  hasVolumeStyle?: boolean;
  /** Выбор сглаживания (ATR: RMA/SMA/EMA/WMA). */
  hasAtrSmoothing?: boolean;
  /** Временно убран из меню добавления ДЛЯ БАЗИСА ЦЕНЫ. Код и рендер живы: уже
   *  добавленные экземпляры продолжают работать и настраиваться, новые не
   *  создать. На других базисах вид может быть открыт (см. oiOk у Боллинджера). */
  hiddenFromMenu?: boolean;
  /** Можно ли считать вид от ряда ОИ. false у объёмов и профиля объёма: у ряда
   *  ОИ объёма нет в принципе, строка появилась бы, а линия — нет. */
  oiOk?: boolean;
}

export const KINDS: Record<IndicatorKind, KindDef> = {
  ma: { label: 'Скользящая средняя (MA)', shortName: 'MA', title: (i) => `MA ${i.length}`, defLength: 20, defaultPane: 0, overlayOk: true, hasSource: true, lengthLabel: 'Длина', oiOk: true },
  ema: { label: 'Экспоненциальная средняя (EMA)', shortName: 'EMA', title: (i) => `EMA ${i.length}`, defLength: 20, defaultPane: 0, overlayOk: true, hasSource: true, lengthLabel: 'Длина', oiOk: true },
  // Боллинджер и профиль объёма временно скрыты из меню (Вадим, 04.08.2026).
  // hiddenFromMenu, а не удаление: у кого они уже добавлены — продолжают
  // рисоваться и настраиваться, вернуть в меню = снять один флаг.
  // ⚠️ Скрыт на ОБОИХ базисах: короткое время Боллинджер был открыт для ряда
  // ОИ, но Вадим 09.08.2026 попросил убрать его из меню совсем. Поэтому у bb
  // нет oiOk — «доступен хотя бы на одном базисе» не выполняется нигде.
  bb: { label: 'Полосы Боллинджера', shortName: 'Боллинджер', title: (i) => `Боллинджер ${i.length}×${i.mult ?? 2}`, defLength: 20, defMult: 2, defaultPane: 0, overlayOk: true, hasSource: true, lengthLabel: 'Длина', hiddenFromMenu: true },
  // 30/70 — канон Уайлдера, тот же дефолт в TradingView и любом терминале.
  // Середина 50 отделяет бычью половину диапазона от медвежьей.
  rsi: {
    label: 'RSI', shortName: 'RSI', title: (i) => `RSI ${i.length}`, defLength: 14, defaultPane: 1, overlayOk: false,
    bands: { upper: 70, lower: 30, middle: 50 },
    hasSource: true, hasSmoothing: true, lengthLabel: 'Длина RSI', oiOk: true,
  },
  atr: {
    label: 'ATR', shortName: 'ATR', title: (i) => (i.statusArgs === false ? 'ATR' : `ATR ${i.length}`),
    defLength: 14, defaultPane: 1, overlayOk: false, lengthLabel: 'Длина', oiOk: true,
    // Сглаживание ATR: канон Уайлдера — RMA, но в терминале это выбор.
    hasAtrSmoothing: true,
  },
  volume: {
    label: 'Объёмы', shortName: 'Объёмы',
    title: (i) => (i.volMaOn && i.statusArgs !== false ? `Объёмы · MA ${i.volMaLength ?? 20}` : 'Объёмы'),
    defLength: 14, defaultPane: 1, overlayOk: false, needsVolume: true, lengthLabel: 'Длина',
    hasVolumeStyle: true,
  },
  vp: {
    label: 'Профиль объёма', title: () => 'Профиль объёма', defLength: VP_DEFAULTS.rows,
    shortName: 'Профиль', defaultPane: 0, overlayOk: true, ownPaneOk: false, needsVolume: true, lengthLabel: 'Уровней',
    hiddenFromMenu: true,
  },
};

/** Базис экземпляра с учётом дефолта. Отдельной функцией, потому что читается он
 *  из десятка мест, и `i.basis ?? 'price'` россыпью легко разъезжается. */
const basisOf = (i: IndicatorInst): IndBasis => i.basis ?? 'price';

/**
 * Виды, доступные для данного базиса: на цене решает hiddenFromMenu, на ОИ —
 * флаг oiOk. Вид без единого доступного базиса (Боллинджер, профиль объёма) не
 * появляется в меню вообще, но уже добавленные экземпляры живут и настраиваются.
 */
function kindAllowedOn(kind: IndicatorKind, basis: IndBasis): boolean {
  const d = KINDS[kind];
  return basis === 'oi' ? !!d.oiOk : !d.hiddenFromMenu;
}

/**
 * ATR по ряду ОИ — это НЕ ATR. У точки ОИ одно значение (high=low=close), и
 * истинный диапазон вырождается в модуль дневного приращения позиции: получается
 * средняя амплитуда изменения позиции, метрика осмысленная, но к «истинному
 * диапазону» отношения не имеющая. Называть её ATR — врать пользователю, поэтому
 * подпись своя.
 *
 * «Ср. изменение», а не «Амплитуда ОИ»: величина — среднее (сглаженное) значение
 * модуля изменения, а «амплитуда» читается как размах за период, то есть другая
 * математика.
 */
const OI_ATR_NAME = 'Ср. изменение';

/** Подпись строки с учётом базиса. */
function indTitle(i: IndicatorInst): string {
  if (i.kind === 'atr' && basisOf(i) === 'oi') {
    return i.statusArgs === false ? OI_ATR_NAME : `${OI_ATR_NAME} ${i.length}`;
  }
  return KINDS[i.kind].title(i);
}

/** Короткое имя для заголовков секций окна настроек — с той же поправкой. */
function indShortName(i: IndicatorInst): string {
  if (i.kind === 'atr' && basisOf(i) === 'oi') return OI_ATR_NAME;
  const d = KINDS[i.kind];
  return d.shortName ?? d.label;
}

/** Виды сглаживающей поверх индикатора (вкладка «Аргументы» → СГЛАЖИВАНИЕ). */
export type SmoothType = 'none' | 'sma' | 'sma_bb' | 'ema' | 'rma' | 'wma';
/** Варианты сглаживания ATR (в TV: RMA/SMA/EMA/WMA). */
export const ATR_SMOOTH: SmoothType[] = ['rma', 'sma', 'ema', 'wma'];
export const SMOOTH_LABELS: Record<SmoothType, string> = {
  none: 'Нет',
  sma: 'Простая скользящая средняя (SMA)',
  sma_bb: 'SMA + полосы Боллинджера',
  ema: 'EMA',
  rma: 'Сглаженная (накатная)',
  wma: 'Взвешенная (WMA)',
};

/** Цвет сглаживающей по умолчанию — янтарный, как в терминалах: он не спорит с
 *  цветом самой линии и читается на тёмном фоне зон. */
const MA_COLOR = 'var(--oi-amber)';

/** Применить выбранное сглаживание. `none` → пусто, вызывающий просто не строит
 *  вторую линию. */
function smoothOf<T>(pts: IndPoint<T>[], type: SmoothType, length: number): IndPoint<T>[] {
  if (type === 'none') return [];
  if (type === 'ema') return ema(pts, length);
  if (type === 'wma') return wma(pts, length);
  if (type === 'rma') return rma(pts, length);
  return sma(pts, length);   // sma и sma_bb — середина одна и та же
}

/** Палитра наложений — та же CC-гамма, что у ⚙-Формата серий. Осталась как
 *  фолбэк для видов без своей гаммы. */
const PALETTE = ['#9B8BF0', '#E0A34E', '#57C7C7', '#5BD49C', '#EF6F6F', '#5DA3E9'];

/**
 * Цвет ПО ВИДУ индикатора (просьба Вадима «присвоить всему цвета»): RSI всегда
 * фиолетовый, ATR — красный, объёмы — синие и так далее. Раньше цвет брался
 * хешем от id, то есть был случайным: две панели RSI на соседних окнах могли
 * оказаться разного цвета, и глазом они не связывались.
 *
 * Внутри вида — три оттенка: MA 20 и MA 50 на одном графике обязаны отличаться,
 * иначе две одинаковые линии не различить. Четвёртый и дальше идут по кругу
 * (столько одинаковых индикаторов сразу — уже экзотика).
 */
const KIND_TINTS: Record<IndicatorKind, string[]> = {
  ma:     ['#E0A34E', '#C4842B', '#F2C078'],   // охра
  ema:    ['#5BD49C', '#37A876', '#8FE5BC'],   // зелёный
  bb:     ['#57C7C7', '#369F9F', '#8BDBDB'],   // бирюза
  rsi:    ['#9B8BF0', '#7361D8', '#BDB2F7'],   // фиолетовый
  atr:    ['#EF6F6F', '#CE4B4B', '#F79A9A'],   // красный
  volume: ['#5DA3E9', '#3B7FC7', '#8DC1F0'],   // синий
  vp:     ['#8E9BB3', '#6C7A94', '#AEB9CC'],   // серо-синий
};

const uid = () => 'ind_' + Date.now().toString(36) + '_' + Math.floor(Math.random() * 1e6).toString(36);

/**
 * Санитизация чужого JSON из localStorage — по образцу parse() в EmbedFormat.
 *
 * ⚠️ Объект собирается ПОЛЯ ЗА ПОЛЕМ, а не спредом распарсенного. Это намеренно
 * (в localStorage может лежать что угодно), но означает, что каждое новое поле
 * IndicatorInst нужно добавлять и сюда — иначе оно работает ровно до F5.
 */
function parseList(raw: string): IndicatorInst[] {
  try {
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return [];
    return arr.flatMap((x): IndicatorInst[] => {
      if (!x || typeof x !== 'object') return [];
      const kind = x.kind as IndicatorKind;
      if (!(kind in KINDS)) return [];
      const length = Number.isFinite(x.length) ? Math.max(1, Math.min(500, Math.round(x.length))) : KINDS[kind].defLength;
      const width = [1, 2, 3, 4].includes(x.width) ? x.width : 2;
      return [{
        id: typeof x.id === 'string' ? x.id : uid(),
        kind,
        length,
        mult: Number.isFinite(x.mult) ? x.mult : KINDS[kind].defMult,
        color: typeof x.color === 'string' ? x.color : null,
        width,
        visible: x.visible !== false,
        pane: Number.isFinite(x.pane) ? x.pane : 0,
        // Базис: только 'oi' пишем явно — undefined и есть 'price', и лишнее поле
        // в каждой строке персиста ни к чему.
        basis: x.basis === 'oi' ? 'oi' : undefined,
        side: x.side === 'left' || x.side === 'right' ? x.side : undefined,
        upper: Number.isFinite(x.upper) ? x.upper : undefined,
        lower: Number.isFinite(x.lower) ? x.lower : undefined,
        source: x.source in SOURCE_LABELS ? x.source : undefined,
        smoothType: x.smoothType in SMOOTH_LABELS ? x.smoothType : undefined,
        smoothLength: Number.isFinite(x.smoothLength) ? Math.max(1, Math.min(500, Math.round(x.smoothLength))) : undefined,
        bbMult: Number.isFinite(x.bbMult) ? x.bbMult : undefined,
        lineOn: typeof x.lineOn === 'boolean' ? x.lineOn : undefined,
        maColor: typeof x.maColor === 'string' ? x.maColor : undefined,
        bandsOn: typeof x.bandsOn === 'boolean' ? x.bandsOn : undefined,
        fillOn: typeof x.fillOn === 'boolean' ? x.fillOn : undefined,
        precision: Number.isFinite(x.precision) ? Math.max(0, Math.min(8, Math.round(x.precision))) : undefined,
        // ⚠️ Каждое новое поле модели ОБЯЗАНО быть здесь, иначе оно не переживёт
        // перезагрузку: персист пишет весь объект, а читает — только этот разбор.
        axisLabel: typeof x.axisLabel === 'boolean' ? x.axisLabel : undefined,
        statusValue: typeof x.statusValue === 'boolean' ? x.statusValue : undefined,
        statusArgs: typeof x.statusArgs === 'boolean' ? x.statusArgs : undefined,
        volMaOn: typeof x.volMaOn === 'boolean' ? x.volMaOn : undefined,
        volMaLength: Number.isFinite(x.volMaLength) ? Math.max(2, Math.min(500, Math.round(x.volMaLength))) : undefined,
        smooth: x.smooth in SMOOTH_LABELS ? x.smooth : undefined,
        styles: parseStyles(x.styles),
      }];
    });
  } catch { return []; }
}

/** Санитизация карты стилей: ключи свои, значения — только знакомые поля. */
function parseStyles(raw: unknown): Record<string, ElStyle> | undefined {
  if (!raw || typeof raw !== 'object') return undefined;
  const out: Record<string, ElStyle> = {};
  for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
    if (!v || typeof v !== 'object') continue;
    const o = v as Record<string, unknown>;
    const st: ElStyle = {};
    if (typeof o.color === 'string') st.color = o.color;
    if (Number.isFinite(o.opacity)) st.opacity = Math.max(0, Math.min(100, Math.round(o.opacity as number)));
    if ([1, 2, 3, 4].includes(o.width as number)) st.width = o.width as 1 | 2 | 3 | 4;
    if (o.dash === 'solid' || o.dash === 'dashed' || o.dash === 'dotted') st.dash = o.dash;
    if (Object.keys(st).length) out[k] = st;
  }
  return Object.keys(out).length ? out : undefined;
}

/** Цвет с прозрачностью → строка для канваса. Прозрачность отдельным полем, а
 *  не восьмизначным hex: слайдер должен двигаться, не трогая тон. */
export function styleColor(st: ElStyle | undefined, fallback: string): string {
  const c = st?.color ?? fallback;
  const op = st?.opacity ?? 100;
  return op >= 100 ? c : `color-mix(in srgb, ${c} ${op}%, transparent)`;
}

export interface IndicatorsApi {
  list: IndicatorInst[];
  /** basis по умолчанию 'price' — окнам с единственным рядом (Сила рынка,
   *  ChartLab) второй аргумент передавать не нужно. */
  add: (kind: IndicatorKind, basis?: IndBasis) => void;
  remove: (id: string) => void;
  patch: (id: string, p: Partial<IndicatorInst>) => void;
  /** Правка стиля ОДНОГО элемента индикатора (линия, сглаживающая, граница…). */
  patchStyle: (id: string, el: string, p: Partial<ElStyle>) => void;
  /** Копия со всеми настройками — чтобы сравнить два периода одного индикатора. */
  duplicate: (id: string) => void;
  /** Поменять панель местами с соседней занятой. dir: -1 выше, +1 ниже. */
  movePane: (id: string, dir: -1 | 1) => void;
  setPane: (id: string, toOwnPane: boolean) => void;
  /** Обмен номерами двух панелей. Публичный, потому что двигать панель умеет не
   *  только индикатор: нативный ряд embed'а (ОИ) занимает панель на равных. */
  swapPanes: (a: number, b: number) => void;
  /** Все занятые панели ≥1, включая зарезервированные под нативные ряды. */
  occupiedPanes: number[];
  /** Свободный номер для новой панели — с учётом зарезервированных. */
  freePane: () => number;
  colorOf: (i: IndicatorInst) => string;
}

/**
 * Стейт + персист + CRUD. Форма как у useSeriesFormats/useDrawTools.
 *
 * ⚠️ Номера панелей — ОБЩЕЕ пространство. Кроме индикаторов панель может занимать
 * нативный ряд embed'а (открытый интерес), и второй раздатчик номеров без
 * согласования посадил бы RSI в чужую панель. Поэтому владелец нативного ряда
 * передаёт свои номера в `reserved`, а перестановку применяет по `onSwapPanes` —
 * ровно ту же, что движок применил к индикаторам.
 */
export function useIndicators(lsKey: string, opts?: {
  reserved?: number[];
  onSwapPanes?: (a: number, b: number) => void;
}): IndicatorsApi {
  const { rd, wr } = useEmbedPersist();
  const [list, setList] = useState<IndicatorInst[]>(() => parseList(rd(lsKey, '')));
  const listRef = useRef(list); listRef.current = list;
  const reservedRef = useRef(opts?.reserved); reservedRef.current = opts?.reserved;
  const onSwapRef = useRef(opts?.onSwapPanes); onSwapRef.current = opts?.onSwapPanes;
  // Пропускаем первую запись (маунт), иначе затрём сохранённое пустым списком —
  // та же ловушка, что в useDrawTools.
  const ready = useRef(false);
  const keyRef = useRef(lsKey); keyRef.current = lsKey;
  useEffect(() => {
    if (!ready.current) { ready.current = true; return; }
    wr(keyRef.current, JSON.stringify(list));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [list]);

  const add = useCallback((kind: IndicatorKind, basis: IndBasis = 'price') => {
    const d = KINDS[kind];
    setList((l) => {
      // Индикаторы со своей шкалой садятся каждый в СВОЮ панель: RSI и ATR на
      // одной оси были бы нечитаемы (0..100 против абсолютных значений цены).
      const pane = d.defaultPane === 0 ? 0 : nextFreePane(l, reservedRef.current);
      return [...l, { id: uid(), kind, length: d.defLength, mult: d.defMult, color: null, width: 2, visible: true, pane, basis: basis === 'oi' ? basis : undefined }];
    });
  }, []);
  /** Перенос строки: на график ⇄ в свою панель. */
  const setPane = useCallback((id: string, toOwnPane: boolean) => {
    setList((l) => l.map((x) => (x.id === id ? { ...x, pane: toOwnPane ? nextFreePane(l.filter((y) => y.id !== id), reservedRef.current) : 0 } : x)));
  }, []);
  const remove = useCallback((id: string) => setList((l) => l.filter((x) => x.id !== id)), []);
  const patch = useCallback((id: string, p: Partial<IndicatorInst>) => {
    setList((l) => l.map((x) => (x.id === id ? { ...x, ...p } : x)));
  }, []);
  const patchStyle = useCallback((id: string, el: string, p: Partial<ElStyle>) => {
    setList((l) => l.map((x) => (x.id === id ? { ...x, styles: { ...x.styles, [el]: { ...x.styles?.[el], ...p } } } : x)));
  }, []);
  const duplicate = useCallback((id: string) => {
    setList((l) => {
      const src = l.find((x) => x.id === id);
      if (!src) return l;
      // Копия садится в СВОЮ панель, если оригинал сидит в отдельной: две линии
      // на одной шкале — это то, ради чего копию и делают, но два RSI в одной
      // панели накладываются друг на друга и читаются хуже, чем рядом.
      const pane = src.pane === 0 ? 0 : nextFreePane(l, reservedRef.current);
      return [...l, { ...src, id: uid(), pane }];
    });
  }, []);

  // Перестановка применяется СРАЗУ к обеим сторонам: к индикаторам здесь, к
  // нативному ряду — его владельцем через onSwapPanes. Обе стороны делают одно
  // и то же преобразование a↔b, поэтому порядок вызовов роли не играет.
  const swapPanes = useCallback((a: number, b: number) => {
    if (a === b) return;
    setList((l) => l.map((x) => (x.pane === a ? { ...x, pane: b } : x.pane === b ? { ...x, pane: a } : x)));
    onSwapRef.current?.(a, b);
  }, []);

  const occupiedPanes = useMemo(
    () => [...new Set([...list.filter((x) => x.pane > 0).map((x) => x.pane), ...(opts?.reserved ?? [])])].sort((a, b) => a - b),
    [list, opts?.reserved],
  );
  const occupiedRef = useRef(occupiedPanes); occupiedRef.current = occupiedPanes;
  const freePane = useCallback(() => nextFreePane(listRef.current, reservedRef.current), []);

  const movePane = useCallback((id: string, dir: -1 | 1) => {
    const cur = listRef.current.find((x) => x.id === id);
    if (!cur || cur.pane === 0) return;
    // Меняемся номерами с ближайшей ЗАНЯТОЙ панелью в нужную сторону: пустые
    // номера пропускаем, иначе «выше» иногда не давало бы видимого эффекта.
    const occupied = occupiedRef.current;
    const target = occupied[occupied.indexOf(cur.pane) + dir];
    if (target != null) swapPanes(cur.pane, target);
  }, [swapPanes]);

  const colorOf = useCallback(
    (i: IndicatorInst) => {
      const own = i.styles?.line?.color ?? i.color;
      if (own) return own;                       // выбранный руками цвет — приоритет
      const tints = KIND_TINTS[i.kind];
      if (!tints) return PALETTE[Math.abs(hash(i.id)) % PALETTE.length];
      // Порядковый номер СРЕДИ СВОЕГО ВИДА — по нему берём оттенок. Читаем ref,
      // а не list: колбэк обязан оставаться стабильным (он в депсах мемо серий).
      const idx = listRef.current.filter((x) => x.kind === i.kind).findIndex((x) => x.id === i.id);
      return tints[(idx < 0 ? 0 : idx) % tints.length];
    },
    [],
  );

  return { list, add, remove, patch, patchStyle, setPane, duplicate, movePane, swapPanes, occupiedPanes, freePane, colorOf };
}

/** Минимальный свободный номер панели ≥1 (после удаления индикатора номера не
 *  переиспользуются автоматически — иначе оставшиеся строки прыгали бы).
 *  `reserved` — панели нативных рядов embed'а, их движок не видит в списке. */
function nextFreePane(list: IndicatorInst[], reserved?: number[]): number {
  const used = new Set([...list.filter((x) => x.pane > 0).map((x) => x.pane), ...(reserved ?? [])]);
  let p = 1;
  while (used.has(p)) p++;
  return p;
}

function hash(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return h;
}

/**
 * Ряды-основания по базисам. Массив — сокращение для окон с ЕДИНСТВЕННЫМ рядом
 * (Сила рынка, ChartLab): им незачем знать про базисы вообще, и их вызов
 * `useIndicatorSeries(list, bars, …)` остался прежним. Окно ОИ передаёт объект.
 *
 * Почему не Map: набор базисов фиксирован типом IndBasis, а объект-литерал
 * проверяется компилятором и легко мемоизируется на стороне вызывающего.
 */
export type IndCandleSet = IndCandle<string>[] | { price: IndCandle<string>[]; oi?: IndCandle<string>[] | null };

const asSet = (c: IndCandleSet): Record<IndBasis, IndCandle<string>[]> =>
  (Array.isArray(c) ? { price: c, oi: [] } : { price: c.price, oi: c.oi ?? [] });

/**
 * Фактическая панель индикатора.
 *
 * ⚠️ Наложение по базису ОИ (pane === 0) живёт ТАМ, ГДЕ СЕЙЧАС РЯД ОИ: на графике
 * цены, если ОИ наверху, и в панели ОИ, если он уехал вниз. Номер вычисляется,
 * а не хранится, — иначе перенос ОИ через ⋯-меню требовал бы миграции состояния
 * всех навешенных на него индикаторов, а любой её пропуск оставлял бы линию в
 * пустой панели.
 *
 * `oiPane` = 0 для окон без ряда ОИ, там функция вырождается в `i.pane`.
 */
function effectivePane(i: IndicatorInst, oiPane = 0): number {
  return i.pane === 0 && basisOf(i) === 'oi' ? oiPane : i.pane;
}

/**
 * Индикаторы → серии, СГРУППИРОВАННЫЕ ПО ПАНЕЛЯМ. Индекс массива = номер панели,
 * 0 — основной график. Пустые панели схлопываются вызывающим.
 *
 * Считаем на клиенте (замер: весь набор на 5000 барах — 2.7 мс против 100 мс на
 * пересоздание серий графика, узкое место не тут).
 *
 * `toSec` передаётся снаружи: у embed'ов время в исходных точках строковое, а
 * правило перевода зависит от таймфрейма.
 */
export function indicatorSeriesByPane(
  list: IndicatorInst[],
  candles: IndCandleSet,
  toSec: (t: string) => number,
  colorOf: (i: IndicatorInst) => string,
  /** Ось ОСНОВНОГО ряда панели 0 — на неё ложатся наложения (MA/EMA/Боллинджер).
   *  ⚠️ Раньше здесь было жёстко 'left', потому что у ОИ там цена. На графиках,
   *  где основной ряд справа (Сила рынка — индекс, Баффетт — коэффициент),
   *  наложения уходили на ПУСТУЮ левую ось: она автомасштабировалась сама по
   *  себе, и линии «прыгали» относительно кривой, поверх которой нарисованы. */
  overlayScale: 'left' | 'right' = 'left',
  /** Где сейчас ряд ОИ (см. effectivePane). Число, а не объект опций, —
   *  мемоизация useIndicatorSeries держится на сравнении по ссылке. */
  oiPane = 0,
): LwSeries[][] {
  const out: LwSeries[][] = [[]];
  const bySeries = asSet(candles);
  if (!bySeries.price.length && !bySeries.oi.length) return out;
  // «Точность» + «Метки на ценовой шкале» — общие для линейных индикаторов
  // (в TV это блок ВЫХОДНЫЕ ЗНАЧЕНИЯ).
  const axisOpts = (i: IndicatorInst): Partial<LwSeries> => {
    const p = i.precision;
    if (p == null) return { lastValueVisible: i.axisLabel !== false };
    const fmt = (v: number) => v.toFixed(p);
    return { lastValueVisible: i.axisLabel !== false, axisFmt: fmt, tipFmt: fmt, minMove: Math.pow(10, -p) };
  };
  const put = (pane: number, sers: LwSeries[]) => {
    while (out.length <= pane) out.push([]);
    out[pane].push(...sers);
  };
  const conv = (pts: { time: string; value: number; color?: string }[]) =>
    pts.map((p) => ({ time: toSec(p.time), value: p.value, ...(p.color ? { color: p.color } : {}) }));

  for (const i of list) {
    if (!i.visible) continue;
    const basis = basisOf(i);
    const cnd = bySeries[basis];
    // Базиса нет или он пуст (цена выключена тумблером, ОИ не пришёл, режим
    // «Покупки + Продажи») — серию не строим вовсе. Строка в легенде остаётся:
    // по ней индикатор и возвращают, когда ряд появится снова.
    if (!cnd.length) continue;
    const color = colorOf(i);
    // Наложение (pane === 0 в состоянии) против собственной панели. Именно это
    // различает ось и пилс, а НЕ фактический номер панели: наложение по ОИ,
    // уехавшее вместе с ним вниз, остаётся наложением.
    const overlay = i.pane === 0;
    const pane = effectivePane(i, oiPane);
    // Источник — общий для всех расчётных индикаторов: RSI по максимумам и RSI
    // по закрытиям это разные ряды, и настройка обязана влиять на математику,
    // а не только на подпись.
    const src = withSource(cnd, i.source ?? 'close');
    // На основном графике индикатор садится на ЦЕНОВУЮ (левую) ось; в своей
    // панели — на правую, там она единственная.
    const elStyle = (el: string): ElStyle | undefined => i.styles?.[el];
    const line = elStyle('line');
    const base = {
      // В своей панели ось всегда правая — она там единственная. Ряд ОИ живёт на
      // ПРАВОЙ оси в любой панели (на графике цены левая занята ценой), поэтому
      // его наложения всегда справа: на левой они масштабировались бы по цене и
      // «плавали» относительно кривой, поверх которой нарисованы.
      scale: (basis === 'oi' ? 'right' : overlay ? overlayScale : 'right') as 'left' | 'right',
      color: styleColor(line, color),
      lineWidth: line?.width ?? i.width,
      dashed: line?.dash === 'dashed' || line?.dash === 'dotted',
      type: 'line' as const,
      lastValueVisible: !overlay,
    };
    if (i.kind === 'ma' || i.kind === 'ema') {
      const pts = (i.kind === 'ma' ? sma : ema)(src, i.length);
      if (pts.length) put(pane, [{ ...base, id: i.id, label: indTitle(i), data: conv(pts) }]);
    } else if (i.kind === 'bb') {
      const { mid, upper, lower } = bollinger(src, i.length, i.mult ?? 2);
      if (mid.length) {
        // Полосы тоньше середины и пунктиром — иначе три линии сливаются.
        put(pane, [
          { ...base, id: i.id + ':u', label: `${indTitle(i)} ↑`, data: conv(upper), dashed: true, lineWidth: 1 },
          { ...base, id: i.id, label: indTitle(i), data: conv(mid) },
          { ...base, id: i.id + ':l', label: `${indTitle(i)} ↓`, data: conv(lower), dashed: true, lineWidth: 1 },
        ]);
      }
    } else if (i.kind === 'rsi') {
      const pts = rsi(src, i.length);
      const b = KINDS.rsi.bands!;
      // minMove по выбранной точности; дефолт 0.01 — иначе ось RSI округлит всё
      // до целых и станет ступенчатой.
      const mm = i.precision != null ? Math.pow(10, -i.precision) : 0.01;
      if (pts.length) {
        const out: LwSeries[] = [];
        if (i.lineOn !== false) {
          out.push({
            ...base, id: i.id, label: indTitle(i), data: conv(pts), minMove: mm,
            ...(i.bandsOn === false ? {} : {
              bands: {
                upper: i.upper ?? b.upper, lower: i.lower ?? b.lower, middle: b.middle,
                fill: i.fillOn !== false,
                upperColor: styleColor(elStyle('upper'), 'var(--text-secondary)'),
                middleColor: styleColor(elStyle('middle'), 'color-mix(in srgb, var(--text-secondary) 60%, transparent)'),
                lowerColor: styleColor(elStyle('lower'), 'var(--text-secondary)'),
                bandFill: elStyle('band') ? styleColor(elStyle('band'), '#888888') : undefined,
                overFill: elStyle('over') ? styleColor(elStyle('over'), 'var(--oi-green)') : undefined,
                underFill: elStyle('under') ? styleColor(elStyle('under'), 'var(--oi-red)') : undefined,
              },
            }),
          });
        }
        // Сглаживающая поверх RSI — вторая линия, как «RSI-based MA» в TradingView.
        const st = i.smoothType ?? 'none';
        if (st !== 'none') {
          const sl = i.smoothLength ?? 14;
          const ma = smoothOf(pts, st, sl);
          if (ma.length) {
            const maSt = elStyle('ma');
            out.push({
              ...base, id: i.id + ':ma', label: `MA ${sl}`, data: conv(ma), minMove: mm,
              color: styleColor(maSt, i.maColor ?? MA_COLOR),
              lineWidth: maSt?.width ?? 1,
              dashed: maSt?.dash === 'dashed' || maSt?.dash === 'dotted',
            });
            // Полосы Боллинджера вокруг сглаживающей — только у sma_bb.
            if (st === 'sma_bb') {
              const bb = bollinger(pts, sl, i.bbMult ?? 2);
              if (bb.upper.length) {
                out.push({ ...base, id: i.id + ':bbu', label: 'BB ↑', color: i.maColor ?? MA_COLOR, data: conv(bb.upper), minMove: mm, lineWidth: 1, dashed: true });
                out.push({ ...base, id: i.id + ':bbl', label: 'BB ↓', color: i.maColor ?? MA_COLOR, data: conv(bb.lower), minMove: mm, lineWidth: 1, dashed: true });
              }
            }
          }
        }
        if (out.length) put(pane, out);
      }
    } else if (i.kind === 'atr') {
      // Сглаживание: RMA — канон Уайлдера и наш дефолт; остальные считаем от
      // ряда истинных диапазонов, как это делает TradingView.
      const sm = i.smooth ?? 'rma';
      const pts = sm === 'rma' ? atr(cnd, i.length) : smoothOf(trueRange(cnd), sm, i.length);
      if (pts.length) put(pane, [{ ...base, id: i.id, label: indTitle(i), data: conv(pts), ...axisOpts(i) }]);
    } else if (i.kind === 'volume') {
      // Цвета столбцов раздельные (TV: «Растущий»/«Нисходящий»); дефолт — наши
      // токены зелёного/красного.
      const upC = styleColor(elStyle('volUp'), VOLUME_UP);
      const dnC = styleColor(elStyle('volDown'), VOLUME_DOWN);
      const volFmt = (v: number) => (
        i.precision != null ? v.toFixed(i.precision)
          : v >= 1e6 ? (v / 1e6).toFixed(1) + 'М' : v >= 1e3 ? Math.round(v / 1e3) + 'т' : String(Math.round(v))
      );
      const pts = volumeBars(cnd, upC, dnC);
      const sers: LwSeries[] = [];
      if (pts.length) {
        sers.push({
          // Пилс последнего бара у гистограммы объёма по умолчанию не нужен —
          // это объём одного дня, а не уровень (в TV так же). Включается
          // «Метки на ценовой шкале».
          ...base, type: 'histogram', id: i.id, label: indTitle(i), data: conv(pts), base: 0,
          lastValueVisible: i.axisLabel === true,
          axisFmt: volFmt, tipFmt: volFmt,
        });
      }
      // Volume MA — вторая линия поверх столбцов.
      if (i.volMaOn) {
        const ma = volumeMa(cnd, i.volMaLength ?? 20) as { time: string; value: number }[];
        if (ma.length) {
          const st = elStyle('volMa');
          sers.push({
            ...base, type: 'line', id: i.id + ':volma', label: `MA ${i.volMaLength ?? 20}`,
            color: styleColor(st, 'var(--chart-line-1)'), lineWidth: st?.width ?? 2,
            dashed: st?.dash === 'dashed' || st?.dash === 'dotted',
            data: conv(ma), lastValueVisible: i.axisLabel === true, axisFmt: volFmt, tipFmt: volFmt,
          });
        }
      }
      if (sers.length) put(pane, sers);
    }
  }
  return out;
}

/**
 * Описание профиля объёма для примитива графика — или null, если профиля в
 * списке нет.
 *
 * Берём ПЕРВЫЙ видимый: два профиля на одной панели легли бы друг на друга и
 * читались как один кривой. Настройки при этом у каждой строки свои, так что
 * пользователь может держать несколько заготовок и переключать их «глазом».
 *
 * Цвета: полосы — те же токены, что у индикатора «Объёмы» (покупки/продажи), а
 * выбранный в палитре цвет уходит на POC. Иначе выбор цвета не значил бы ничего:
 * сам профиль двухцветный по смыслу.
 */
export function volumeProfileSpec(
  list: IndicatorInst[],
  candles: IndCandle<string>[],
  seriesId: string,
  colorOf: (i: IndicatorInst) => string,
): VolumeProfileSpec | null {
  const inst = list.find((i) => i.kind === 'vp' && i.visible);
  if (!inst || !candles.length) return null;
  return {
    seriesId,
    candles,
    rows: Math.max(4, Math.min(400, Math.round(inst.length))),
    side: inst.side ?? VP_DEFAULTS.side,
    widthPct: VP_DEFAULTS.widthPct,
    valueAreaPct: VP_DEFAULTS.valueAreaPct,
    upColor: VOLUME_UP,
    downColor: VOLUME_DOWN,
    pocColor: colorOf(inst),
  };
}

/** Мемо-обёртка: новая ссылка на спеку сама по себе дёшева (серии не трогает),
 *  но лишний вызов applyOptions сбрасывает кэш профиля и заставляет пересчитать
 *  его на ближайшем кадре. */
export function useVolumeProfileSpec(
  list: IndicatorInst[],
  candles: IndCandle<string>[],
  seriesId: string,
  colorOf: (i: IndicatorInst) => string,
): VolumeProfileSpec | null {
  return useMemo(() => volumeProfileSpec(list, candles, seriesId, colorOf), [list, candles, seriesId, colorOf]);
}

/**
 * Последние значения индикаторов — для показа В СТРОКЕ, справа от названия
 * (как «ATR 14 RMA 5,51» в терминалах).
 *
 * Так значение читается там же, где написано, чей оно, — а пилс на оси для
 * объёма вдобавок бессмыслен: это объём одного бара, а не уровень.
 */
export interface IndValue { text: string }

export function indicatorValues(
  seriesByPane: LwSeries[][],
): Record<string, IndValue> {
  const out: Record<string, IndValue> = {};
  for (const arr of seriesByPane) {
    for (const d of arr) {
      const n = d.data.length;
      if (!n) continue;
      const idx = n - 1;
      const pt = d.data[idx];
      if (!pt) continue;
      // Цвета у числа нет: раньше оно красилось по направлению к предыдущему
      // бару (зелёный/красный), но в строке это спорило с квадратиком цвета
      // ряда и с подписью. Число идёт серым, как остальная служебная разметка.
      out[d.id] = { text: d.axisFmt ? d.axisFmt(pt.value) : String(Math.round(pt.value)) };
    }
  }
  return out;
}

// ─────────────────────────────── UI ───────────────────────────────

const SURFACE: CSSProperties = {
  color: 'var(--text-primary)',
  background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 92%, transparent)',
  border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
  backdropFilter: 'blur(3px)',
  boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
};
const ICON_BTN: CSSProperties = {
  width: 20, height: 20, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
  border: 'none', borderRadius: 5, background: 'transparent', color: 'var(--text-secondary)',
  cursor: 'pointer', padding: 0, flexShrink: 0,
};

/** Строка списка для НАТИВНОЙ серии embed'а (цена, ОИ): её нельзя удалить. */
export interface NativeRow {
  id: string;
  label: string;
  color: string;
  visible: boolean;
  onToggle: () => void;
  /** Панель ряда: 0 — основной график, 1+ — своя. */
  pane?: number;
  /** Перенос между панелями. Есть → в ⋯ появляется «Переместить».
   *  Считать соседей embed обязан сам: движок не знает, где его нативный ряд. */
  onMove?: (to: 'up' | 'down' | 'own' | 'main') => void;
  canUp?: boolean;
  canDown?: boolean;
}

/**
 * Список индикаторов — React-оверлей ВНУТРИ области графика.
 *
 * z-index 10: выше слоя рисования (7) и хит-слоя (8), ниже рейла инструментов
 * рисования (14) и тулбара (20) — рейл в невысоких панелях упирается в верх и
 * должен оставаться кликабельным поверх списка.
 * data-export-ignore обязателен — иначе список попадёт в PNG.
 */
export function IndicatorList({ api, native, visible, values, oiPane = 0, bases }: {
  api: IndicatorsApi;
  native: NativeRow[];
  visible: boolean;
  /** id серии → последнее значение. Показывается в строке, справа от названия. */
  values?: Record<string, IndValue>;
  /** Где сейчас ряд ОИ — тем же числом, что ушло в useIndicatorSeries.
   *  ⚠️ Строка обязана считать панель ТАК ЖЕ, как сборка серий, иначе подпись
   *  окажется не в том углу, где линия. */
  oiPane?: number;
  /** Доступные базисы окна. Есть больше одного → в ⋯ строки появляется
   *  «Считать от», а у строк не-ценового базиса — бейдж. */
  bases?: BasisOption[];
}) {
  if (!visible) return null;

  // Своей кнопки «+ Индикатор» здесь НЕТ намеренно: добавление живёт в тулбаре
  // (IndicatorsButton), а вторая точка входа прямо под строками дублировала её
  // и занимала место в углу графика.
  return (
    <div data-export-ignore="true" style={listBoxStyle}>
      {native.filter((r) => (r.pane ?? 0) === 0).map((r) => <NativeRowView key={r.id} row={r} />)}
      {/* Только наложения. Индикаторы своих панелей рисуют строку САМИ, над
          своим графиком — см. PaneIndicatorList. */}
      {api.list.filter((i) => effectivePane(i, oiPane) === 0).map((i) => (
        <IndicatorRow key={i.id} inst={i} api={api} value={values?.[i.id]} bases={bases} />
      ))}
    </div>
  );
}

/** Базисы, на которых вид имеет смысл (см. kindAllowedOn). */
function allowedBases(kind: IndicatorKind, bases: BasisOption[]): BasisOption[] {
  return bases.filter((b) => kindAllowedOn(kind, b.id));
}

/** Базисы окна по умолчанию — только цена: столько их у Силы рынка и ChartLab. */
const PRICE_ONLY: BasisOption[] = [{ id: 'price', label: 'Цена' }];

/** Список видов индикаторов — общий для кнопки в графике и кнопки в тулбаре. */
export function AddIndicatorMenu({ api, hasVolume, bases = PRICE_ONLY, onPickBasis, onDone }: {
  api: IndicatorsApi; hasVolume: boolean;
  bases?: BasisOption[];
  /** Базисов больше одного → вид не добавляется сразу, а уходит наверх, в окно
   *  выбора. Само окно живёт в IndicatorsButton: меню закрывается по клику мимо,
   *  и модалка, отрисованная изнутри него, схлопнулась бы вместе с ним. */
  onPickBasis?: (kind: IndicatorKind) => void;
  onDone: () => void;
}) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 1, minWidth: 222 }}>
      {/* Вид показываем, если он доступен ХОТЯ БЫ на одном базисе окна: так
          объёмы исчезают из меню, когда цена выключена (считать их не от чего),
          а Боллинджер и профиль объёма не показываются нигде — у них не открыт
          ни один базис. */}
      {(Object.keys(KINDS) as IndicatorKind[]).filter((k) => {
        const ok = allowedBases(k, bases);
        return ok.length > 0 && (hasVolume || !KINDS[k].needsVolume);
      }).map((k) => {
        const d = KINDS[k];
        const ok = allowedBases(k, bases);
        return (
          <button
            key={k}
            type="button"
            onClick={() => {
              if (ok.length > 1 && onPickBasis) { onPickBasis(k); return; }
              api.add(k, ok[0].id);
              onDone();
            }}
            style={{
              display: 'block', width: '100%', textAlign: 'left', padding: '5px 8px', borderRadius: 6,
              // Кегль общий со всеми списками тулбара (CTL_FS/CTL_FW). Здесь он
              // задавался в обход них, без веса вовсе — список видов индикаторов
              // был тоньше соседних выпадашек.
              border: 'none', background: 'transparent', fontSize: CTL_FS, fontWeight: CTL_FW,
              color: 'var(--text-primary)', cursor: 'pointer', whiteSpace: 'nowrap',
            }}
          >
            {d.label}
          </button>
        );
      })}
    </div>
  );
}

/** Кнопка «Индикаторы» для тулбара виджета — тот же список, что и в графике. */
export function IndicatorsButton({ api, hasVolume, bases = PRICE_ONLY, compact }: {
  api: IndicatorsApi; hasVolume: boolean; bases?: BasisOption[]; compact?: boolean;
}) {
  // Вид, выбранный в меню и ждущий ответа «к чему применить». Держим ЗДЕСЬ, а не
  // в меню: ToolbarMenuButton закрывает содержимое по клику мимо, и окно выбора
  // умерло бы на первом же нажатии внутри себя.
  const [pick, setPick] = useState<IndicatorKind | null>(null);
  return (
    <>
      <ToolbarMenuButton label="Индикаторы" title="Добавить индикатор" icon={<LineChart size={14} />} compact={compact}>
        {(close) => (
          <AddIndicatorMenu
            api={api} hasVolume={hasVolume} bases={bases}
            onPickBasis={(k) => { setPick(k); close(); }}
            onDone={close}
          />
        )}
      </ToolbarMenuButton>
      {pick && (
        <BasisDialog
          kind={pick}
          bases={allowedBases(pick, bases)}
          onPick={(b) => { api.add(pick, b); setPick(null); }}
          onClose={() => setPick(null)}
        />
      )}
    </>
  );
}

/**
 * Квадратик цвета ряда — тот же маркер, что стоит перед названием в строке
 * легенды. Вынесен общим, чтобы легенда, окно выбора базиса и подменю «Считать
 * от» показывали один и тот же ряд ОДИНАКОВО: пользователь узнаёт ряд по цвету,
 * и три слегка разных квадратика читались бы как три разные сущности.
 *
 * Цвет — сырым значением: это CSS-переменная либо литерал, и внутри поддерева
 * панели переменная разрешится сама (тема живёт на data-theme).
 */
function SeriesSwatch({ color }: { color: string }) {
  return <span style={{ width: 8, height: 8, borderRadius: 2, background: color, flexShrink: 0 }} />;
}

/** Содержимое пункта «ряд окна» для меню: маркер цвета + подпись ряда. */
function basisItemLabel(b: BasisOption): ReactNode {
  return (
    <>
      {b.color && <SeriesSwatch color={b.color} />}
      <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{b.label}</span>
    </>
  );
}

/**
 * «Применить к: …» — окно выбора базиса при добавлении.
 *
 * Показывается только там, где базисов реально больше одного (окно ОИ с живым
 * рядом ОИ и включённой ценой). На остальных графиках выбора нет, и вид, как и
 * раньше, добавляется одним кликом.
 */
function BasisDialog({ kind, bases, onPick, onClose }: {
  kind: IndicatorKind; bases: BasisOption[];
  onPick: (b: IndBasis) => void; onClose: () => void;
}) {
  // Тема панели, а не оболочки: портал уходит в body, вне поддерева .sb-panel —
  // см. тот же приём в SettingsDialog/RowPopMenu.
  const portalTheme = usePortalTheme();
  return createPortal(
    <div {...portalTheme} style={DIALOG_BACKDROP} onPointerDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ ...DIALOG, width: 'min(300px, 92vw)' }} onPointerDown={(e) => e.stopPropagation()}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 12px 6px' }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-primary)' }}>{KINDS[kind].label}</span>
          <button type="button" onClick={onClose} style={ICON_BTN}><XIcon size={13} /></button>
        </div>
        <div style={{ padding: '4px 8px 10px' }}>
          <div style={{ fontSize: 9.5, fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase', color: 'var(--text-secondary)', opacity: 0.75, padding: '4px 4px 7px' }}>
            Применить к
          </div>
          {/* menuItem отдаёт голый <button> без key — в списке его надо обернуть,
              иначе React ругается на отсутствие ключа. */}
          {bases.map((b) => <Fragment key={b.id}>{menuItem(basisItemLabel(b), () => onPick(b.id))}</Fragment>)}
        </div>
      </div>
    </div>,
    document.body,
  );
}

/**
 * Строки индикаторов КОНКРЕТНОЙ панели. Рендерится внутрь этой панели (проп
 * paneOverlay у LwChartPanes), поэтому садится в её левый верхний угол.
 *
 * Why: индикатор в своей панели и его строка в общем списке наверху — это два
 * разных места для одной сущности. Пользователь ищет подпись там, где линия.
 */
export function PaneIndicatorList({ api, pane, values, native, oiPane = 0, bases }: {
  api: IndicatorsApi; pane: number; values?: Record<string, IndValue>;
  /** Нативные ряды embed'а, съехавшие в эту панель (ОИ). */
  native?: NativeRow[];
  /** Где сейчас ряд ОИ — см. одноимённый проп IndicatorList. */
  oiPane?: number;
  bases?: BasisOption[];
}) {
  const rows = api.list.filter((i) => effectivePane(i, oiPane) === pane);
  const nat = (native ?? []).filter((r) => (r.pane ?? 0) === pane);
  if (!rows.length && !nat.length) return null;
  return (
    <div data-export-ignore="true" style={listBoxStyle}>
      {nat.map((r) => <NativeRowView key={r.id} row={r} />)}
      {rows.map((i) => <IndicatorRow key={i.id} inst={i} api={api} value={values?.[i.id]} bases={bases} />)}
    </div>
  );
}

/** Строка нативного ряда. Удалить её нельзя (ряд принадлежит самому виджету),
 *  зато можно скрыть и перенести в свою панель. */
function NativeRowView({ row }: { row: NativeRow }) {
  const hasMenu = !!row.onMove;
  return (
    <Row
      color={row.color} label={row.label} visible={row.visible} onToggle={row.onToggle}
      value={{ text: '' }} valueId={row.id}
      menu={hasMenu ? <NativeRowMenu row={row} /> : undefined}
    />
  );
}

/** Общая посадка списков: отступ слева = ширина ценовой оси (её публикует
 *  LwChartPanes), иначе список ложится на цифры шкалы. z 10 — выше слоя
 *  рисования (7) и хит-слоя (8), ниже рейла рисования (14) и тулбара (20). */
const listBoxStyle: CSSProperties = {
  // Отступ 4px и сверху, и слева (слева — от кромки ценовой оси, не от кромки
  // виджета): было 8, и список висел заметно ниже и правее угла поля.
  position: 'absolute', top: 4, left: 'calc(var(--lw-axis-left, 0px) + 4px)', zIndex: 10,
  display: 'flex', flexDirection: 'column', gap: 1, alignItems: 'flex-start',
};

/** Строка индикатора вместе со своей шестерёнкой: попап живёт рядом со строкой,
 *  а не в корне списка — иначе для панельных строк его пришлось бы отдельно
 *  позиционировать через всю иерархию. */
function IndicatorRow({ inst, api, value, bases }: {
  inst: IndicatorInst; api: IndicatorsApi; value?: IndValue; bases?: BasisOption[];
}) {
  const [open, setOpen] = useState(false);
  const basis = basisOf(inst);
  // Бейдж различает «MA 20» по цене и «MA 20» по ОИ — без него две одинаковые
  // строки в одном углу неотличимы. В строке места нет, поэтому короткий код
  // базиса, а полная подпись ряда («Чистая позиция») уходит в тултип.
  const basisLabel = bases?.find((b) => b.id === basis)?.label;
  const badge = basis === 'price' ? undefined : {
    text: basisBadgeText(basis, basisLabel),
    title: basisLabel ? `Считается от ряда «${basisLabel}»` : undefined,
  };
  // Базиса нет в текущем режиме виджета (например наложение по ОИ, а выбран
  // режим «Покупки + Продажи» — единого ряда ОИ там нет): рисовать нечего.
  // Строку УБИРАЕМ целиком (решение Вадима: «так проще», чем гасить глазиком).
  // Сам индикатор остаётся в состоянии — вернулся режим, вернулась и строка.
  const basisGone = !!bases && bases.length > 0 && !bases.some((b) => b.id === basis);
  // ⚠️ Никакого «закрыть по клику мимо» здесь быть не должно: окно настроек
  // уходит ПОРТАЛОМ в body, то есть лежит вне этой строки, и такой обработчик
  // принимал бы за клик мимо любое нажатие внутри самого окна — оно закрывалось
  // бы на первом же действии. Закрытие живёт в окне: подложка, ✕, Отмена, Ок.
  if (basisGone) return null;

  return (
    <div style={{ position: 'relative' }}>
      <Row
        color={api.colorOf(inst)}
        label={indTitle(inst)}
        badge={badge}
        value={inst.statusValue === false ? undefined : value}
        valueId={inst.id}
        visible={inst.visible}
        onToggle={() => api.patch(inst.id, { visible: !inst.visible })}
        onSettings={() => setOpen((v) => !v)}
        onRemove={() => api.remove(inst.id)}
        menu={<RowMenu inst={inst} api={api} bases={bases} onSettings={() => setOpen(true)} />}
      />
      {open && <SettingsDialog inst={inst} api={api} onClose={() => setOpen(false)} />}
    </div>
  );
}

/** Короткий код базиса в строке легенды. Цена своего бейджа не имеет: она
 *  подразумевается, и метка на каждой строке была бы шумом. */
const BASIS_BADGE: Record<IndBasis, string> = { price: '', oi: 'ОИ' };

/** Бейдж должен называть КОНКРЕТНЫЙ ряд, а не базис вообще: «ОИ» одинаково
 *  стояло и на средней по открытому интересу, и по чистой позиции, и по
 *  покупкам — понять, что именно посчитано, было нельзя (фидбек Вадима).
 *  Показатель называется полной подписью ряда, здесь ужимаем её до бейджа. */
const OI_SHORT: Record<string, string> = {
  'Открытый интерес': 'ОИ',
  'Чистая позиция': 'Чист. поз.',
  'Покупки': 'Покупки',
  'Продажи': 'Продажи',
  'Покупки + Продажи': 'Пок.+Прод.',
};
function basisBadgeText(basis: IndBasis, label?: string): string {
  if (!label) return BASIS_BADGE[basis];
  return OI_SHORT[label] ?? label;
}

function Row({ color, label, badge, value, valueId, visible, title, onToggle, onSettings, onRemove, menu }: {
  color: string; label: string; value?: IndValue; valueId?: string; visible: boolean;
  /** Подсказка на всю строку (например «нет данных в этом режиме»). */
  title?: string;
  /** Метка базиса расчёта — см. BASIS_BADGE. */
  badge?: { text: string; title?: string };
  onToggle: () => void; onSettings?: () => void; onRemove?: () => void;
  /** Меню «⋯» в конце строки. Кнопки переноса между панелями здесь больше нет —
   *  перенос живёт в меню, как в терминалах: на строке и так тесно, а
   *  пользуются им редко. */
  menu?: ReactNode;
}) {
  return (
    <div
      title={title}
      style={{
        display: 'flex', alignItems: 'center', gap: 5, padding: '1px 6px 1px 4px', borderRadius: 6,
        background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 72%, transparent)',
        opacity: visible ? 1 : 0.5,
      }}
    >
      <SeriesSwatch color={color} />
      <span style={{ fontSize: 10.5, fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>{label}</span>
      {/* Бейдж базиса — сразу за названием, до значения: он часть имени строки
          («MA 20 ОИ»), а не её показание. Рамка вместо заливки, чтобы не спорить
          с квадратиком цвета ряда слева. */}
      {badge?.text && (
        <span
          title={badge.title}
          style={{
            fontSize: 8.5, fontWeight: 700, lineHeight: 1.1, letterSpacing: '0.04em',
            padding: '1px 3px', borderRadius: 3, whiteSpace: 'nowrap', flexShrink: 0,
            color: 'var(--text-secondary)',
            border: '1px solid color-mix(in srgb, var(--text-secondary) 45%, transparent)',
          }}
        >
          {badge.text}
        </span>
      )}
      {/* Значение справа от названия: серым и на пункт мельче подписи — цифра
          подчинена названию, а не спорит с ним. Моноширинные цифры, чтобы не
          пляшли при смене бара под курсором. */}
      {value && (
        <span data-ind-value={valueId} style={{ fontSize: 9.5, fontWeight: 600, color: 'var(--text-secondary)', whiteSpace: 'nowrap', fontVariantNumeric: 'tabular-nums' }}>{value.text}</span>
      )}
      <button type="button" title={visible ? 'Скрыть' : 'Показать'} onClick={onToggle} style={ICON_BTN}>
        {visible ? <Eye size={11} /> : <EyeOff size={11} />}
      </button>
      {onSettings && <button type="button" title="Настройки" onClick={onSettings} style={ICON_BTN}><Settings2 size={11} /></button>}
      {/* КОРЗИНА, а не крестик: крестик читается как «закрыть/скрыть», а кнопка
          удаляет индикатор насовсем — рядом с «глазом» это путало. */}
      {onRemove && <button type="button" title="Удалить" onClick={onRemove} style={ICON_BTN}><Trash2 size={11} /></button>}
      {menu}
    </div>
  );
}

/**
 * Меню строки индикатора. Из меню терминала взято только то, что у нас есть за
 * чем стоять: перенос между панелями (раньше был отдельной кнопкой), дубль,
 * скрытие, удаление и настройки.
 *
 * Чего нет и почему: оповещения по индикатору (алерты у нас по цене и ОИ, не по
 * значению индикатора), «добавить индикатор на индикатор», Избранное, порядок
 * слоёв, видимость по интервалам, «о скрипте»/«исходный код»/«дерево объектов» —
 * всё это про пользовательские Pine-скрипты, которых у нас нет.
 */
function RowMenu({ inst, api, bases, onSettings }: {
  inst: IndicatorInst; api: IndicatorsApi; bases?: BasisOption[]; onSettings: () => void;
}) {
  const [sub, setSub] = useState<'move' | 'basis' | null>(null);
  const d = KINDS[inst.kind];
  const own = inst.pane > 0;
  const at = api.occupiedPanes.indexOf(inst.pane);
  const canUp = at > 0;
  const canDown = at >= 0 && at < api.occupiedPanes.length - 1;
  const side = useSubmenuSide(!!sub);
  // Смена базиса уже добавленному индикатору. Пункт есть, только если в окне
  // реально больше одного подходящего ряда: у объёмов базис один (цена), а у
  // скрытых видов вроде Боллинджера — ни одного, и пункт не появится.
  const basisOpts = (bases ?? []).filter((b) => kindAllowedOn(inst.kind, b.id));

  return (
    <RowPopMenu>
      {(close) => {
        const item = (label: string, onClick: () => void, disabled = false): ReactNode =>
          menuItem(label, () => { onClick(); setSub(null); close(); }, disabled);
        return (
          <>
            {item('Настройки…', onSettings)}
            {basisOpts.length > 1 && (
              <div style={{ position: 'relative' }}>
                {subTrigger('Считать от', () => setSub((v) => (v === 'basis' ? null : 'basis')))}
                {sub === 'basis' && (
                  <div ref={side.ref} style={{ ...side.style, ...SURFACE }}>
                    {/* Панель НЕ трогаем: у наложения она и так вычисляемая, а
                        свою панель индикатор сохраняет — менялся базис, а не
                        место на экране. */}
                    {basisOpts.map((b) => (
                      <Fragment key={b.id}>{menuItem(
                        basisItemLabel(b),
                        () => { api.patch(inst.id, { basis: b.id === 'oi' ? 'oi' : undefined }); setSub(null); close(); },
                        false,
                        b.id === basisOf(inst),
                      )}</Fragment>
                    ))}
                  </div>
                )}
              </div>
            )}
            {/* Пустое подменю не показываем: у наложения без своей панели
                (ownPaneOk: false) двигать некуда вообще. */}
            {(own || d.ownPaneOk !== false) && (
            <div style={{ position: 'relative' }}>
              {subTrigger('Переместить', () => setSub((v) => (v === 'move' ? null : 'move')))}
              {sub === 'move' && (
                <div ref={side.ref} style={{ ...side.style, ...SURFACE }}>
                  {/* Только у индикатора в своей панели: на основном графике
                      меняться местами не с чем. Серость считаем по СОСЕДЯМ —
                      без них клик молча ничего не делал. */}
                  {own && item('Выше', () => api.movePane(inst.id, -1), !canUp)}
                  {own && item('Ниже', () => api.movePane(inst.id, 1), !canDown)}
                  {d.ownPaneOk !== false && !own && item('В отдельную панель', () => api.setPane(inst.id, true))}
                  {d.overlayOk && own && item('На основной график', () => api.setPane(inst.id, false))}
                </div>
              )}
            </div>
            )}
            {item('Дублировать', () => api.duplicate(inst.id))}
            {/* «Скрыть» и «Удалить» в меню НЕ дублируем: они уже есть на строке
                отдельными кнопками (глаз и корзина), а два пути к одному действию
                заставляют гадать, чем они отличаются. */}
          </>
        );
      }}
    </RowPopMenu>
  );
}

/**
 * Кнопка «⋯» со своим меню. Меню уходит ПОРТАЛОМ в body и позиционируется по
 * кнопке.
 *
 * Why: строки живут внутри панели графика, а панель режет по своим краям. Меню,
 * висевшее на `position:absolute` внутри строки, срезало справа на узкой панели
 * («В отдельную пан…») и снизу — у короткой нижней панели, где строка стоит
 * почти у самой кромки. Портал снимает вопрос целиком, ценой ручного замера.
 */
function RowPopMenu({ children }: { children: (close: () => void) => ReactNode }) {
  const [open, setOpen] = useState(false);
  // Тема панели, а не оболочки: в песочнице у каждой панели свой data-theme на
  // .sb-panel, а портал уходит в body — вне этого поддерева, и CSS-переменные
  // резолвились бы от <html> (тема оболочки). Вне песочницы тема поддерева и
  // корневая совпадают — хук молчит, и разметка меню остаётся прежней.
  const portalTheme = usePortalTheme();
  const btnRef = useRef<HTMLButtonElement | null>(null);
  const boxRef = useRef<HTMLDivElement | null>(null);
  const [pos, setPos] = useState<{ left: number; top: number } | null>(null);

  // Меряем ПОСЛЕ вставки: до неё габаритов нет. Первый кадр — скрытый (visibility),
  // иначе меню мигнёт в левом верхнем углу и прыгнет на место.
  useLayoutEffect(() => {
    if (!open) { setPos(null); return; }
    const a = btnRef.current, el = boxRef.current;
    if (!a || !el) return;
    const r = a.getBoundingClientRect(), m = el.getBoundingClientRect();
    const left = Math.max(6, Math.min(r.left, window.innerWidth - m.width - 6));
    // Не влезло вниз — раскрываем ВВЕРХ от кнопки.
    const top = r.bottom + 4 + m.height > window.innerHeight - 6
      ? Math.max(6, r.top - m.height - 4)
      : r.bottom + 4;
    setPos({ left, top });
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: PointerEvent) => {
      const t = e.target as Node;
      if (!btnRef.current?.contains(t) && !boxRef.current?.contains(t)) setOpen(false);
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [open]);

  return (
    <>
      <button ref={btnRef} type="button" title="Ещё" onClick={() => setOpen((v) => !v)} style={ICON_BTN}>
        <MoreHorizontal size={11} />
      </button>
      {open && createPortal(
        <div
          ref={boxRef}
          {...portalTheme}
          style={{
            position: 'fixed', left: pos?.left ?? 0, top: pos?.top ?? 0,
            visibility: pos ? 'visible' : 'hidden',
            zIndex: 60, minWidth: 196, padding: 4, borderRadius: 9, ...SURFACE,
            ...portalTheme.style,
          }}
        >
          {children(() => setOpen(false))}
        </div>,
        document.body,
      )}
    </>
  );
}

/** Пункт всплывающего меню строки — общий для индикаторов и нативных рядов.
 *  label — ReactNode, а не строка: пункты выбора ряда несут ещё и квадратик
 *  цвета (basisItemLabel). Для голого текста flex-раскладка неотличима от
 *  прежней block: единственный текстовый ребёнок так же прижат влево. */
function menuItem(label: ReactNode, onClick: () => void, disabled = false, active = false): ReactNode {
  return (
    <button
      type="button" disabled={disabled} onClick={onClick}
      style={{
        display: 'flex', alignItems: 'center', gap: 6, width: '100%', textAlign: 'left', padding: '5px 9px', borderRadius: 6,
        // Кегль общий с тулбаром и его выпадашками (CTL_FS/CTL_FW): списки
        // открываются рядом, и разный шрифт в них сразу бросается в глаза.
        border: 'none', fontSize: CTL_FS, cursor: disabled ? 'default' : 'pointer',
        background: active ? 'color-mix(in srgb, var(--accent) 14%, transparent)' : 'transparent',
        color: disabled ? 'var(--text-secondary)' : active ? 'var(--accent)' : 'var(--text-primary)',
        fontWeight: active ? 800 : CTL_FW,
        opacity: disabled ? 0.45 : 1,
      }}
    >
      {label}
    </button>
  );
}

/** Кнопка подменю («Переместить ›», «Показатель ›»). */
function subTrigger(label: string, onClick: () => void): ReactNode {
  return (
    <button
      type="button" onClick={onClick}
      style={{
        display: 'flex', width: '100%', alignItems: 'center', justifyContent: 'space-between',
        padding: '5px 9px', borderRadius: 6, border: 'none', background: 'transparent',
        fontSize: CTL_FS, fontWeight: CTL_FW, color: 'var(--text-primary)', cursor: 'pointer',
      }}
    >
      {label}<ChevronRight size={12} />
    </button>
  );
}

const SUBMENU: CSSProperties = { position: 'absolute', top: 0, left: '100%', marginLeft: 4, zIndex: 41, minWidth: 186, padding: 4, borderRadius: 9 };
const SUBMENU_FLIPPED: CSSProperties = { ...SUBMENU, left: 'auto', right: '100%', marginLeft: 0, marginRight: 4 };

/**
 * Подменю открывается вправо, но список строк живёт ВНУТРИ панели графика, а у
 * неё overflow:hidden — на узкой панели пункты срезало на полуслове («В отдельную
 * пан…»). Меряем по ближайшему предку, который реально обрезает, а не по окну:
 * места в окне полно, режет именно панель.
 */
function useSubmenuSide(open: boolean) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [flip, setFlip] = useState(false);
  useLayoutEffect(() => {
    if (!open || !ref.current) { setFlip(false); return; }
    const el = ref.current;
    let box: HTMLElement | null = el.parentElement;
    while (box && getComputedStyle(box).overflow === 'visible') box = box.parentElement;
    const limit = box ? box.getBoundingClientRect().right : window.innerWidth;
    setFlip(el.getBoundingClientRect().right > limit - 4);
  }, [open]);
  return { ref, style: (flip ? SUBMENU_FLIPPED : SUBMENU) as CSSProperties };
}

/**
 * ⋯-меню нативной строки — перенос ряда между панелями.
 *
 * Режим и показатель ОИ здесь были с 2026-06, но вернулись в тулбар окна
 * (EmbedOpenInterest, ряд `controls`): срез меняют часто, а в меню линии его
 * не находили. Настройки ВИДА ряда (тип линии, цвет) живут в ⚙ Формат.
 *
 * Подменю «Переместить ›» здесь НЕТ: перенос — единственный пункт этого меню, и
 * лишний уровень заставлял открывать список ради списка. Пункты стоят сразу.
 * «Выше/Ниже» показываем только у ряда в своей панели: на основном графике
 * менять местами нечего, и вечно серые пункты читались как поломка.
 */
function NativeRowMenu({ row }: { row: NativeRow }) {
  const own = (row.pane ?? 0) > 0;

  return (
    <RowPopMenu>
      {(closeMenu) => {
        const pick = (fn: () => void) => { fn(); closeMenu(); };
        return (
          <>
            {row.onMove && (
              <>
                {own && menuItem('Выше', () => pick(() => row.onMove?.('up')), !row.canUp)}
                {own && menuItem('Ниже', () => pick(() => row.onMove?.('down')), !row.canDown)}
                {!own && menuItem('В отдельную панель', () => pick(() => row.onMove?.('own')))}
                {own && menuItem('На основной график', () => pick(() => row.onMove?.('main')))}
              </>
            )}
          </>
        );
      }}
    </RowPopMenu>
  );
}

const numInput = (w: number, dim = false): CSSProperties => ({
  width: w, opacity: dim ? 0.5 : 1, padding: '3px 6px', borderRadius: 6, fontSize: 11.5,
  border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
  background: 'var(--bg-base, transparent)', color: 'var(--text-primary)',
});

/** Границы зон не должны пересекаться, иначе заливка выворачивается наизнанку:
 *  верхняя держится выше середины, нижняя ниже. */
function clampBand(raw: string, min: number, max: number, def: number): number {
  const v = Number(raw);
  if (!Number.isFinite(v)) return def;
  return Math.max(min, Math.min(max, Math.round(v)));
}

/**
 * Окно настроек индикатора — по образцу TradingView (скриншоты Вадима): три
 * вкладки, секции с заголовками, подпись слева / контрол справа, футер
 * «Отмена / Ок».
 *
 * ⚠️ Правки применяются СРАЗУ, а «Ок» просто закрывает: график под окном виден,
 * и настройка, которую видно только после подтверждения, заставляет открывать
 * окно по три раза, чтобы подобрать значение. «Отмена» откатывает к снимку,
 * сделанному на открытии.
 *
 * Чего из оригинала здесь НЕТ и почему: вкладка «Видимость» по таймфреймам (у
 * нас их три, а не полтора десятка от тиков до месяцев), «Интервал/дождаться
 * закрытия» (данные приходят готовыми свечами, внутрибарных обновлений нет) и
 * «Рассчитать отклонение» (дивергенции — отдельная задача, не настройка).
 */
function SettingsDialog({ inst, api, onClose }: { inst: IndicatorInst; api: IndicatorsApi; onClose: () => void }) {
  const d = KINDS[inst.kind];
  const basis = basisOf(inst);
  const [tab, setTab] = useState<'args' | 'style'>('args');
  // Снимок на открытии — для «Отмены». Правки идут в реальном времени.
  const snapshot = useRef<IndicatorInst>(inst);
  const set = (p: Partial<IndicatorInst>) => api.patch(inst.id, p);
  const smooth = inst.smoothType ?? 'none';
  // Тема панели, а не оболочки — см. RowPopMenu: портал в body теряет data-theme
  // с .sb-panel, и светлое окно настроек всплывало над тёмной панелью.
  const portalTheme = usePortalTheme();

  // Порталом в body: строка индикатора живёт внутри своей панели, а та в
  // песочнице сидит в панели с backdrop-filter — предок с фильтром становится
  // точкой отсчёта для fixed, и окно уехало бы вместе с ней.
  return createPortal(
    <div {...portalTheme} style={DIALOG_BACKDROP} onPointerDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={DIALOG} onPointerDown={(e) => e.stopPropagation()}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 12px 6px' }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-primary)' }}>{indTitle(inst)}</span>
          <button type="button" onClick={onClose} style={ICON_BTN}><XIcon size={13} /></button>
        </div>

        <div style={{ display: 'flex', gap: 14, padding: '0 12px', borderBottom: '1px solid var(--border-color, rgba(128,128,128,0.28))' }}>
          {([['args', 'Аргументы'], ['style', 'Стиль']] as const).map(([id, t]) => (
            <button
              key={id} type="button" onClick={() => setTab(id)}
              style={{
                border: 'none', background: 'transparent', cursor: 'pointer', padding: '4px 0 7px',
                fontSize: 12, fontWeight: tab === id ? 700 : 500,
                color: tab === id ? 'var(--text-primary)' : 'var(--text-secondary)',
                borderBottom: tab === id ? '2px solid var(--text-primary)' : '2px solid transparent',
              }}
            >
              {t}
            </button>
          ))}
        </div>

        <div style={{ padding: '10px 12px', overflowY: 'auto', flex: 1 }}>
          {tab === 'args' ? (
            <>
              <Section label={`Настройки ${indShortName(inst)}`}>
                <Field label={d.lengthLabel ?? 'Длина'}>
                  <input
                    type="number" min={2} max={500} value={inst.length}
                    onChange={(e) => set({ length: clampBand(e.target.value, 2, 500, d.defLength) })}
                    style={numInput(78)}
                  />
                </Field>
                {/* Выбор источника только на цене: у точки ОИ одно значение,
                    и open/high/low/close там совпадают — селект предлагал бы
                    восемь вариантов, дающих один и тот же ряд. */}
                {d.hasSource && basis === 'price' && (
                  <Field label="Данные">
                    <Select
                      value={inst.source ?? 'close'}
                      options={(Object.keys(SOURCE_LABELS) as IndSource[]).map((k) => ({ id: k, label: SOURCE_LABELS[k] }))}
                      onChange={(v) => set({ source: v as IndSource })}
                    />
                  </Field>
                )}
                {inst.kind === 'bb' && (
                  <Field label="Отклонение">
                    <input
                      type="number" min={0.5} max={5} step={0.5} value={inst.mult ?? 2}
                      onChange={(e) => set({ mult: Math.max(0.5, Math.min(5, Number(e.target.value) || 2)) })}
                      style={numInput(78)}
                    />
                  </Field>
                )}
                {inst.kind === 'vp' && (
                  <Field label="Сторона">
                    <Select
                      value={inst.side ?? 'right'}
                      options={[{ id: 'right', label: 'Справа' }, { id: 'left', label: 'Слева' }]}
                      onChange={(v) => set({ side: v as 'left' | 'right' })}
                    />
                  </Field>
                )}
                {/* Почему у ATR по ОИ другое имя — объясняем прямо здесь, иначе
                    расхождение с привычным «ATR» выглядит как ошибка. */}
                {inst.kind === 'atr' && basis === 'oi' && (
                  <Note>
                    У ряда позиций нет максимума и минимума внутри бара, поэтому истинный
                    диапазон равен модулю изменения позиции. Показатель — средняя величина,
                    на которую позиция меняется за бар; это не ATR цены.
                  </Note>
                )}
              </Section>

              {d.hasAtrSmoothing && (
                <Section label="Расчёты">
                  {/* Канон Уайлдера — RMA; остальные считаются от того же ряда
                      истинных диапазонов, как в TradingView.
                      ⚠️ «Интервал расчёта» и «Дождаться закрытия интервала» из
                      TV не переносим: это мультитаймфрейм (считать индикатор по
                      часовым барам на дневном графике), у нас его нет. */}
                  <Field label="Сглаживание">
                    <Select
                      value={inst.smooth ?? 'rma'}
                      options={ATR_SMOOTH.map((k) => ({ id: k, label: SMOOTH_LABELS[k] }))}
                      onChange={(v) => set({ smooth: v as SmoothType })}
                    />
                  </Field>
                </Section>
              )}

              {d.hasVolumeStyle && (
                <Section label="Скользящая по объёму">
                  <Field label="Показывать">
                    <Select
                      value={inst.volMaOn ? 'on' : 'off'}
                      options={[{ id: 'off', label: 'Нет' }, { id: 'on', label: 'Да' }]}
                      onChange={(v) => set({ volMaOn: v === 'on' })}
                    />
                  </Field>
                  <Field label="Длина" dim={!inst.volMaOn}>
                    <input
                      type="number" min={2} max={500} value={inst.volMaLength ?? 20} disabled={!inst.volMaOn}
                      onChange={(e) => set({ volMaLength: clampBand(e.target.value, 2, 500, 20) })}
                      style={numInput(78, !inst.volMaOn)}
                    />
                  </Field>
                </Section>
              )}

              {d.hasSmoothing && (
                <Section label="Сглаживание">
                  <Field label="Тип">
                    <Select
                      value={smooth}
                      options={(Object.keys(SMOOTH_LABELS) as SmoothType[]).map((k) => ({ id: k, label: SMOOTH_LABELS[k] }))}
                      onChange={(v) => set({ smoothType: v as SmoothType })}
                    />
                  </Field>
                  <Field label="Длина" dim={smooth === 'none'}>
                    <input
                      type="number" min={2} max={500} value={inst.smoothLength ?? 14} disabled={smooth === 'none'}
                      onChange={(e) => set({ smoothLength: clampBand(e.target.value, 2, 500, 14) })}
                      style={numInput(78, smooth === 'none')}
                    />
                  </Field>
                  {/* Отклонение живёт только у варианта с полосами — в остальных
                      случаях поле показываем погашенным, как в оригинале, чтобы
                      было видно, что оно относится именно к этому выбору. */}
                  <Field label="Боллинджер, откл." dim={smooth !== 'sma_bb'}>
                    <input
                      type="number" min={0.5} max={5} step={0.5} value={inst.bbMult ?? 2} disabled={smooth !== 'sma_bb'}
                      onChange={(e) => set({ bbMult: Math.max(0.5, Math.min(5, Number(e.target.value) || 2)) })}
                      style={numInput(78, smooth !== 'sma_bb')}
                    />
                  </Field>
                </Section>
              )}
            </>
          ) : (
            <>
              {d.hasVolumeStyle ? (
                <>
                  {/* Столбцы объёма двухцветные по смыслу (бар вверх/вниз), одной
                      «линии» у них нет — как в TradingView две строки цвета. */}
                  <StyleRow
                    label="Растущий"
                    on={inst.lineOn !== false}
                    onToggle={() => set({ lineOn: inst.lineOn === false })}
                    style={inst.styles?.volUp ?? { color: '#5BD49C' }}
                    onStyle={(p) => api.patchStyle(inst.id, 'volUp', p)}
                    noLine
                  />
                  <StyleRow
                    label="Нисходящий"
                    on={inst.lineOn !== false}
                    style={inst.styles?.volDown ?? { color: '#EF6F6F' }}
                    onStyle={(p) => api.patchStyle(inst.id, 'volDown', p)}
                    noLine
                  />
                  {inst.volMaOn && (
                    <StyleRow
                      label={`MA ${inst.volMaLength ?? 20}`}
                      on
                      style={inst.styles?.volMa ?? { color: '#5B8DEF', width: 2 }}
                      onStyle={(p) => api.patchStyle(inst.id, 'volMa', p)}
                    />
                  )}
                </>
              ) : (
                <StyleRow
                  label={indShortName(inst)}
                  on={inst.lineOn !== false}
                  onToggle={() => set({ lineOn: inst.lineOn === false })}
                  style={inst.styles?.line ?? { color: api.colorOf(inst), width: inst.width }}
                  onStyle={(p) => api.patchStyle(inst.id, 'line', p)}
                />
              )}
              {d.hasSmoothing && smooth !== 'none' && (
                <StyleRow
                  label="Сглаживающая"
                  on
                  style={inst.styles?.ma ?? { color: '#E0A34E', width: 1 }}
                  onStyle={(p) => api.patchStyle(inst.id, 'ma', p)}
                />
              )}
              {d.bands && (
                <>
                  <StyleRow
                    label="Верхняя граница"
                    on={inst.bandsOn !== false}
                    onToggle={() => set({ bandsOn: inst.bandsOn === false })}
                    style={inst.styles?.upper ?? { color: '#9C9C9C', dash: 'dashed' }}
                    onStyle={(p) => api.patchStyle(inst.id, 'upper', p)}
                    value={inst.upper ?? d.bands.upper}
                    onValue={(v) => set({ upper: clampBand(String(v), 51, 99, d.bands!.upper) })}
                  />
                  <StyleRow
                    label="Средняя"
                    on={inst.bandsOn !== false}
                    style={inst.styles?.middle ?? { color: '#7B7B7B', dash: 'dashed' }}
                    onStyle={(p) => api.patchStyle(inst.id, 'middle', p)}
                    value={d.bands.middle ?? 50}
                  />
                  <StyleRow
                    label="Нижняя граница"
                    on={inst.bandsOn !== false}
                    style={inst.styles?.lower ?? { color: '#9C9C9C', dash: 'dashed' }}
                    onStyle={(p) => api.patchStyle(inst.id, 'lower', p)}
                    value={inst.lower ?? d.bands.lower}
                    onValue={(v) => set({ lower: clampBand(String(v), 1, 49, d.bands!.lower) })}
                  />
                  <StyleRow
                    label="Заливка зоны"
                    on={inst.fillOn !== false}
                    onToggle={() => set({ fillOn: inst.fillOn === false })}
                    style={inst.styles?.band ?? { color: '#9C9C9C', opacity: 8 }}
                    onStyle={(p) => api.patchStyle(inst.id, 'band', p)}
                    noLine
                  />
                  <StyleRow
                    label="Перекупленность"
                    on={inst.fillOn !== false}
                    style={inst.styles?.over ?? { color: '#5BD49C', opacity: 12 }}
                    onStyle={(p) => api.patchStyle(inst.id, 'over', p)}
                    noLine
                  />
                  <StyleRow
                    label="Перепроданность"
                    on={inst.fillOn !== false}
                    style={inst.styles?.under ?? { color: '#EF6F6F', opacity: 12 }}
                    onStyle={(p) => api.patchStyle(inst.id, 'under', p)}
                    noLine
                  />
                </>
              )}
              <Section label="Выходные значения">
                <Field label="Точность">
                  <Select
                    value={inst.precision == null ? 'auto' : String(inst.precision)}
                    options={[{ id: 'auto', label: 'По умолчанию' }, ...[0, 1, 2, 3, 4].map((n) => ({ id: String(n), label: n === 0 ? 'Целые' : `${n} знака` }))]}
                    onChange={(v) => set({ precision: v === 'auto' ? undefined : Number(v) })}
                  />
                </Field>
                <Field label="Метки на ценовой шкале">
                  <Select
                    value={(d.hasVolumeStyle ? inst.axisLabel === true : inst.axisLabel !== false) ? 'on' : 'off'}
                    options={[{ id: 'on', label: 'Да' }, { id: 'off', label: 'Нет' }]}
                    onChange={(v) => set({ axisLabel: v === 'on' })}
                  />
                </Field>
                <Field label="Значения в строке">
                  <Select
                    value={inst.statusValue === false ? 'off' : 'on'}
                    options={[{ id: 'on', label: 'Да' }, { id: 'off', label: 'Нет' }]}
                    onChange={(v) => set({ statusValue: v === 'on' })}
                  />
                </Field>
                <Field label="Параметры в подписи">
                  <Select
                    value={inst.statusArgs === false ? 'off' : 'on'}
                    options={[{ id: 'on', label: 'Да' }, { id: 'off', label: 'Нет' }]}
                    onChange={(v) => set({ statusArgs: v === 'on' })}
                  />
                </Field>
              </Section>
            </>
          )}
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 7, padding: '8px 12px', borderTop: '1px solid var(--border-color, rgba(128,128,128,0.28))' }}>
          <button type="button" onClick={() => { api.patch(inst.id, snapshot.current); onClose(); }} style={btn(false)}>Отмена</button>
          <button type="button" onClick={onClose} style={btn(true)}>Ок</button>
        </div>
      </div>
    </div>,
    document.body,
  );
}

// ⚠️ fixed, а не absolute. Строка индикатора живёт ВНУТРИ своей панели, а та у
// RSI высотой ~180px: absolute-оверлей зажимался в неё, и окно обрезалось по
// «Отмена/Ок». Модалка обязана вставать над всем окном. z 100000 — конвенция
// песочницы для оверлеев (меню и шторки живут там же).
// color: портал уходит в document.body, поэтому наследует цвет текста от
// <body> (тема ОБОЛОЧКИ). data-theme на корне чинит фон и переменные, но не
// унаследованное значение — без явного color светлая панель в тёмной оболочке
// давала бы светлый текст на светлой карточке. То же во всех порталах ниже.
const DIALOG_BACKDROP: CSSProperties = {
  position: 'fixed', inset: 0, zIndex: 100000, display: 'flex', color: 'var(--text-primary)',
  alignItems: 'center', justifyContent: 'center',
  background: 'color-mix(in srgb, #000 45%, transparent)',
};
const DIALOG: CSSProperties = {
  width: 'min(340px, 92vw)', maxHeight: 'min(560px, 86vh)', display: 'flex', flexDirection: 'column',
  borderRadius: 10, ...SURFACE,
};

function Section({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ fontSize: 9.5, fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase', color: 'var(--text-secondary)', opacity: 0.75, marginBottom: 7 }}>
        {label}
      </div>
      {children}
    </div>
  );
}

/** Пояснение под полями секции — там, где настройка требует не подписи, а
 *  объяснения смысла (сейчас это ATR по ряду ОИ). */
function Note({ children }: { children: ReactNode }) {
  return (
    <div style={{ fontSize: 10.5, lineHeight: 1.45, color: 'var(--text-secondary)', marginTop: 2 }}>
      {children}
    </div>
  );
}

function Field({ label, children, dim }: { label: string; children: ReactNode; dim?: boolean }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10, marginBottom: 8, opacity: dim ? 0.45 : 1 }}>
      <span style={{ fontSize: 11.5, color: 'var(--text-primary)' }}>{label}</span>
      {children}
    </div>
  );
}

/** Строка вкладки «Стиль»: галка · подпись · кнопка цвета/линии · значение. */
function StyleRow({ label, on, onToggle, style, onStyle, value, onValue, noLine }: {
  label: string; on: boolean; onToggle?: () => void;
  style?: ElStyle; onStyle?: (p: Partial<ElStyle>) => void;
  value?: number; onValue?: (v: number) => void;
  /** Заливкам линия не нужна — только цвет и прозрачность. */
  noLine?: boolean;
}) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8, opacity: on ? 1 : 0.45 }}>
      <button
        type="button" onClick={onToggle} disabled={!onToggle}
        style={{
          width: 14, height: 14, borderRadius: 3, flexShrink: 0, padding: 0, cursor: onToggle ? 'pointer' : 'default',
          border: '1.5px solid var(--border-color, rgba(128,128,128,0.45))',
          background: on ? 'var(--accent)' : 'transparent',
        }}
      />
      <span style={{ fontSize: 11.5, color: 'var(--text-primary)', flex: 1 }}>{label}</span>
      {style && onStyle && <ColorButton value={style} onChange={onStyle} showLine={!noLine} />}
      {value != null && (
        <input
          type="number" value={value} disabled={!onValue}
          onChange={(e) => onValue?.(Number(e.target.value))}
          style={numInput(52, !onValue)}
        />
      )}
    </div>
  );
}

function Select({ value, options, onChange }: { value: string; options: { id: string; label: string }[]; onChange: (v: string) => void }) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      style={{
        maxWidth: 168, padding: '3px 6px', borderRadius: 6, fontSize: 11.5,
        border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
        background: 'var(--bg-base, transparent)', color: 'var(--text-primary)', cursor: 'pointer',
      }}
    >
      {options.map((o) => <option key={o.id} value={o.id}>{o.label}</option>)}
    </select>
  );
}

const btn = (primary: boolean): CSSProperties => ({
  padding: '4px 14px', borderRadius: 7, fontSize: 11.5, fontWeight: 600, cursor: 'pointer',
  border: primary ? 'none' : '1px solid var(--border-color, rgba(128,128,128,0.45))',
  background: primary ? 'var(--accent)' : 'transparent',
  color: primary ? '#fff' : 'var(--text-primary)',
});

/** Мемо-обёртка: пересчёт дешёвый, но новая ссылка на массив пересоздаёт ВСЕ
 *  серии графика (эффект серий зависит от массива), а это уже сотня миллисекунд. */
export function useIndicatorSeries(
  list: IndicatorInst[],
  /** ⚠️ Объект-набор рядов вызывающий обязан мемоизировать: новый литерал на
   *  каждый рендер обнуляет этот мемо и пересоздаёт все серии графика. */
  candles: IndCandleSet,
  toSec: (t: string) => number,
  colorOf: (i: IndicatorInst) => string,
  overlayScale: 'left' | 'right' = 'left',
  oiPane = 0,
): LwSeries[][] {
  return useMemo(
    () => indicatorSeriesByPane(list, candles, toSec, colorOf, overlayScale, oiPane),
    [list, candles, toSec, colorOf, overlayScale, oiPane],
  );
}
