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
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { Eye, EyeOff, Settings2, X as XIcon, Trash2, Plus, MoreHorizontal, ChevronRight } from 'lucide-react';
import type { LwSeries } from '../../components/chart/lwTypes';
import type { VolumeProfileSpec } from '../../components/LwChartPanes';
import { VP_DEFAULTS } from '../../components/chart/volumeProfilePrimitive';
import {
  sma, ema, bollinger, rsi, atr, volumeBars, wma, rma, withSource,
  SOURCE_LABELS, VOLUME_UP, VOLUME_DOWN, type IndCandle, type IndPoint, type IndSource,
} from '../../utils/indicators';
import { useEmbedPersist } from './embedPersist';
import { ColorButton, type ElStyle } from './ColorPicker';

/** Имя нарочно НЕ IndKind: так уже называется вид панели в SandboxPage. */
export type IndicatorKind = 'ma' | 'ema' | 'bb' | 'rsi' | 'atr' | 'volume' | 'vp';

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
  /** 0 = поверх графика, 1+ — отдельная панель. */
  pane: number;
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
}

export const KINDS: Record<IndicatorKind, KindDef> = {
  ma: { label: 'Скользящая средняя (MA)', shortName: 'MA', title: (i) => `MA ${i.length}`, defLength: 20, defaultPane: 0, overlayOk: true, hasSource: true, lengthLabel: 'Длина' },
  ema: { label: 'Экспоненциальная средняя (EMA)', shortName: 'EMA', title: (i) => `EMA ${i.length}`, defLength: 20, defaultPane: 0, overlayOk: true, hasSource: true, lengthLabel: 'Длина' },
  bb: { label: 'Полосы Боллинджера', shortName: 'Боллинджер', title: (i) => `Боллинджер ${i.length}×${i.mult ?? 2}`, defLength: 20, defMult: 2, defaultPane: 0, overlayOk: true, hasSource: true, lengthLabel: 'Длина' },
  // 30/70 — канон Уайлдера, тот же дефолт в TradingView и любом терминале.
  // Середина 50 отделяет бычью половину диапазона от медвежьей.
  rsi: {
    label: 'RSI', shortName: 'RSI', title: (i) => `RSI ${i.length}`, defLength: 14, defaultPane: 1, overlayOk: false,
    bands: { upper: 70, lower: 30, middle: 50 },
    hasSource: true, hasSmoothing: true, lengthLabel: 'Длина RSI',
  },
  atr: { label: 'ATR', shortName: 'ATR', title: (i) => `ATR ${i.length}`, defLength: 14, defaultPane: 1, overlayOk: false, lengthLabel: 'Длина' },
  volume: { label: 'Объёмы', shortName: 'Объёмы', title: () => 'Объёмы', defLength: 14, defaultPane: 1, overlayOk: false, needsVolume: true, lengthLabel: 'Длина' },
  vp: {
    label: 'Профиль объёма', title: () => 'Профиль объёма', defLength: VP_DEFAULTS.rows,
    shortName: 'Профиль', defaultPane: 0, overlayOk: true, ownPaneOk: false, needsVolume: true, lengthLabel: 'Уровней',
  },
};

/** Виды сглаживающей поверх индикатора (вкладка «Аргументы» → СГЛАЖИВАНИЕ). */
export type SmoothType = 'none' | 'sma' | 'sma_bb' | 'ema' | 'rma' | 'wma';
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

/** Палитра наложений — та же CC-гамма, что у ⚙-Формата серий. */
const PALETTE = ['#9B8BF0', '#E0A34E', '#57C7C7', '#5BD49C', '#EF6F6F', '#5DA3E9'];

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
  add: (kind: IndicatorKind) => void;
  remove: (id: string) => void;
  patch: (id: string, p: Partial<IndicatorInst>) => void;
  /** Правка стиля ОДНОГО элемента индикатора (линия, сглаживающая, граница…). */
  patchStyle: (id: string, el: string, p: Partial<ElStyle>) => void;
  /** Копия со всеми настройками — чтобы сравнить два периода одного индикатора. */
  duplicate: (id: string) => void;
  /** Поменять панель местами с соседней занятой. dir: -1 выше, +1 ниже. */
  movePane: (id: string, dir: -1 | 1) => void;
  setPane: (id: string, toOwnPane: boolean) => void;
  colorOf: (i: IndicatorInst) => string;
}

/** Стейт + персист + CRUD. Форма как у useSeriesFormats/useDrawTools. */
export function useIndicators(lsKey: string): IndicatorsApi {
  const { rd, wr } = useEmbedPersist();
  const [list, setList] = useState<IndicatorInst[]>(() => parseList(rd(lsKey, '')));
  // Пропускаем первую запись (маунт), иначе затрём сохранённое пустым списком —
  // та же ловушка, что в useDrawTools.
  const ready = useRef(false);
  const keyRef = useRef(lsKey); keyRef.current = lsKey;
  useEffect(() => {
    if (!ready.current) { ready.current = true; return; }
    wr(keyRef.current, JSON.stringify(list));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [list]);

  const add = useCallback((kind: IndicatorKind) => {
    const d = KINDS[kind];
    setList((l) => {
      // Индикаторы со своей шкалой садятся каждый в СВОЮ панель: RSI и ATR на
      // одной оси были бы нечитаемы (0..100 против абсолютных значений цены).
      const pane = d.defaultPane === 0 ? 0 : nextFreePane(l);
      return [...l, { id: uid(), kind, length: d.defLength, mult: d.defMult, color: null, width: 2, visible: true, pane }];
    });
  }, []);
  /** Перенос строки: на график ⇄ в свою панель. */
  const setPane = useCallback((id: string, toOwnPane: boolean) => {
    setList((l) => l.map((x) => (x.id === id ? { ...x, pane: toOwnPane ? nextFreePane(l.filter((y) => y.id !== id)) : 0 } : x)));
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
      const pane = src.pane === 0 ? 0 : nextFreePane(l);
      return [...l, { ...src, id: uid(), pane }];
    });
  }, []);

  const movePane = useCallback((id: string, dir: -1 | 1) => {
    setList((l) => {
      const cur = l.find((x) => x.id === id);
      if (!cur || cur.pane === 0) return l;
      // Меняемся номерами с ближайшей ЗАНЯТОЙ панелью в нужную сторону: пустые
      // номера пропускаем, иначе «выше» иногда не давало бы видимого эффекта.
      const occupied = [...new Set(l.filter((x) => x.pane > 0).map((x) => x.pane))].sort((a, b) => a - b);
      const at = occupied.indexOf(cur.pane);
      const target = occupied[at + dir];
      if (target == null) return l;
      return l.map((x) => (x.pane === cur.pane ? { ...x, pane: target } : x.pane === target ? { ...x, pane: cur.pane } : x));
    });
  }, []);

  const colorOf = useCallback(
    (i: IndicatorInst) => i.styles?.line?.color ?? i.color ?? PALETTE[Math.abs(hash(i.id)) % PALETTE.length],
    [],
  );

  return { list, add, remove, patch, patchStyle, setPane, duplicate, movePane, colorOf };
}

/** Минимальный свободный номер панели ≥1 (после удаления индикатора номера не
 *  переиспользуются автоматически — иначе оставшиеся строки прыгали бы). */
function nextFreePane(list: IndicatorInst[]): number {
  const used = new Set(list.filter((x) => x.pane > 0).map((x) => x.pane));
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
  candles: IndCandle<string>[],
  toSec: (t: string) => number,
  colorOf: (i: IndicatorInst) => string,
): LwSeries[][] {
  const out: LwSeries[][] = [[]];
  if (!candles.length) return out;
  const put = (pane: number, sers: LwSeries[]) => {
    while (out.length <= pane) out.push([]);
    out[pane].push(...sers);
  };
  const conv = (pts: { time: string; value: number; color?: string }[]) =>
    pts.map((p) => ({ time: toSec(p.time), value: p.value, ...(p.color ? { color: p.color } : {}) }));

  for (const i of list) {
    if (!i.visible) continue;
    const color = colorOf(i);
    const onMain = i.pane === 0;
    // Источник — общий для всех расчётных индикаторов: RSI по максимумам и RSI
    // по закрытиям это разные ряды, и настройка обязана влиять на математику,
    // а не только на подпись.
    const src = withSource(candles, i.source ?? 'close');
    // На основном графике индикатор садится на ЦЕНОВУЮ (левую) ось; в своей
    // панели — на правую, там она единственная.
    const elStyle = (el: string): ElStyle | undefined => i.styles?.[el];
    const line = elStyle('line');
    const base = {
      scale: (onMain ? 'left' : 'right') as 'left' | 'right',
      color: styleColor(line, color),
      lineWidth: line?.width ?? i.width,
      dashed: line?.dash === 'dashed' || line?.dash === 'dotted',
      type: 'line' as const,
      lastValueVisible: !onMain,
    };
    if (i.kind === 'ma' || i.kind === 'ema') {
      const pts = (i.kind === 'ma' ? sma : ema)(src, i.length);
      if (pts.length) put(i.pane, [{ ...base, id: i.id, label: KINDS[i.kind].title(i), data: conv(pts) }]);
    } else if (i.kind === 'bb') {
      const { mid, upper, lower } = bollinger(src, i.length, i.mult ?? 2);
      if (mid.length) {
        // Полосы тоньше середины и пунктиром — иначе три линии сливаются.
        put(i.pane, [
          { ...base, id: i.id + ':u', label: `${KINDS.bb.title(i)} ↑`, data: conv(upper), dashed: true, lineWidth: 1 },
          { ...base, id: i.id, label: KINDS.bb.title(i), data: conv(mid) },
          { ...base, id: i.id + ':l', label: `${KINDS.bb.title(i)} ↓`, data: conv(lower), dashed: true, lineWidth: 1 },
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
            ...base, id: i.id, label: KINDS.rsi.title(i), data: conv(pts), minMove: mm,
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
        if (out.length) put(i.pane, out);
      }
    } else if (i.kind === 'atr') {
      const pts = atr(candles, i.length);
      if (pts.length) put(i.pane, [{ ...base, id: i.id, label: KINDS.atr.title(i), data: conv(pts), minMove: i.precision != null ? Math.pow(10, -i.precision) : 0.01 }]);
    } else if (i.kind === 'volume') {
      const pts = volumeBars(candles);
      if (pts.length) {
        put(i.pane, [{
          ...base, type: 'histogram', id: i.id, label: 'Объёмы', data: conv(pts), base: 0,
          axisFmt: (v: number) => (v >= 1e6 ? (v / 1e6).toFixed(1) + 'М' : v >= 1e3 ? Math.round(v / 1e3) + 'т' : String(Math.round(v))),
        }]);
      }
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

// ─────────────────────────────── UI ───────────────────────────────

const SURFACE: CSSProperties = {
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
}

/**
 * Список индикаторов — React-оверлей ВНУТРИ области графика.
 *
 * z-index 10: выше слоя рисования (7), хит-слоя (8) и панели слоёв (9), ниже
 * тулбара (20). data-export-ignore обязателен — иначе список попадёт в PNG.
 */
export function IndicatorList({ api, native, visible, hasVolume = false }: {
  api: IndicatorsApi;
  native: NativeRow[];
  visible: boolean;
  /** Есть ли объём в свечах. Без него «Объёмы» и «Профиль объёма» не показываем:
   *  добавились бы строки, за которыми на графике пусто. */
  hasVolume?: boolean;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e: PointerEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setMenuOpen(false);
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [menuOpen]);

  if (!visible) return null;

  return (
    <div ref={rootRef} data-export-ignore="true" style={listBoxStyle}>
      {native.map((r) => (
        <Row key={r.id} color={r.color} label={r.label} visible={r.visible} onToggle={r.onToggle} />
      ))}
      {/* Только наложения. Индикаторы своих панелей рисуют строку САМИ, над
          своим графиком — см. PaneIndicatorList. */}
      {api.list.filter((i) => i.pane === 0).map((i) => (
        <IndicatorRow key={i.id} inst={i} api={api} />
      ))}

      <div style={{ position: 'relative' }}>
        <button
          type="button"
          onClick={() => setMenuOpen((v) => !v)}
          style={{
            display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 7px', marginTop: 2,
            borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: 10.5, fontWeight: 600,
            background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 80%, transparent)',
            color: 'var(--text-secondary)',
          }}
        >
          <Plus size={11} />Индикатор
        </button>
        {menuOpen && (
          <div style={{ position: 'absolute', top: 'calc(100% + 4px)', left: 0, zIndex: 2, minWidth: 232, padding: 5, borderRadius: 9, ...SURFACE }}>
            {(Object.keys(KINDS) as IndicatorKind[]).filter((k) => hasVolume || !KINDS[k].needsVolume).map((k) => {
              const d = KINDS[k];
              return (
                <button
                  key={k}
                  type="button"
                  onClick={() => { api.add(k); setMenuOpen(false); }}
                  style={{
                    display: 'block', width: '100%', textAlign: 'left', padding: '5px 8px', borderRadius: 6,
                    border: 'none', background: 'transparent', fontSize: 11.5,
                    color: 'var(--text-primary)', cursor: 'pointer',
                  }}
                >
                  {d.label}
                  {d.defaultPane > 0 && <span style={{ fontSize: 10, opacity: 0.7 }}> · отдельной панелью</span>}
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * Строки индикаторов КОНКРЕТНОЙ панели. Рендерится внутрь этой панели (проп
 * paneOverlay у LwChartPanes), поэтому садится в её левый верхний угол.
 *
 * Why: индикатор в своей панели и его строка в общем списке наверху — это два
 * разных места для одной сущности. Пользователь ищет подпись там, где линия.
 */
export function PaneIndicatorList({ api, pane }: { api: IndicatorsApi; pane: number }) {
  const rows = api.list.filter((i) => i.pane === pane);
  if (!rows.length) return null;
  return (
    <div data-export-ignore="true" style={listBoxStyle}>
      {rows.map((i) => <IndicatorRow key={i.id} inst={i} api={api} />)}
    </div>
  );
}

/** Общая посадка списков: отступ слева = ширина ценовой оси (её публикует
 *  LwChartPanes), иначе список ложится на цифры шкалы. z 10 — выше слоя
 *  рисования (7) и хит-слоя (8), ниже тулбара (20). */
const listBoxStyle: CSSProperties = {
  position: 'absolute', top: 8, left: 'calc(var(--lw-axis-left, 0px) + 8px)', zIndex: 10,
  display: 'flex', flexDirection: 'column', gap: 1, alignItems: 'flex-start',
};

/** Строка индикатора вместе со своей шестерёнкой: попап живёт рядом со строкой,
 *  а не в корне списка — иначе для панельных строк его пришлось бы отдельно
 *  позиционировать через всю иерархию. */
function IndicatorRow({ inst, api }: { inst: IndicatorInst; api: IndicatorsApi }) {
  const [open, setOpen] = useState(false);
  // ⚠️ Никакого «закрыть по клику мимо» здесь быть не должно: окно настроек
  // уходит ПОРТАЛОМ в body, то есть лежит вне этой строки, и такой обработчик
  // принимал бы за клик мимо любое нажатие внутри самого окна — оно закрывалось
  // бы на первом же действии. Закрытие живёт в окне: подложка, ✕, Отмена, Ок.
  return (
    <div style={{ position: 'relative' }}>
      <Row
        color={api.colorOf(inst)}
        label={KINDS[inst.kind].title(inst)}
        visible={inst.visible}
        onToggle={() => api.patch(inst.id, { visible: !inst.visible })}
        onSettings={() => setOpen((v) => !v)}
        onRemove={() => api.remove(inst.id)}
        menu={<RowMenu inst={inst} api={api} onSettings={() => setOpen(true)} />}
      />
      {open && <SettingsDialog inst={inst} api={api} onClose={() => setOpen(false)} />}
    </div>
  );
}

function Row({ color, label, visible, onToggle, onSettings, onRemove, menu }: {
  color: string; label: string; visible: boolean;
  onToggle: () => void; onSettings?: () => void; onRemove?: () => void;
  /** Меню «⋯» в конце строки. Кнопки переноса между панелями здесь больше нет —
   *  перенос живёт в меню, как в терминалах: на строке и так тесно, а
   *  пользуются им редко. */
  menu?: ReactNode;
}) {
  return (
    <div
      style={{
        display: 'flex', alignItems: 'center', gap: 5, padding: '1px 6px 1px 4px', borderRadius: 6,
        background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 72%, transparent)',
        opacity: visible ? 1 : 0.5,
      }}
    >
      {/* Цвет — сырым значением: это CSS-переменная либо литерал, и внутри
          поддерева панели переменная разрешится сама (тема живёт на data-theme). */}
      <span style={{ width: 8, height: 8, borderRadius: 2, background: color, flexShrink: 0 }} />
      <span style={{ fontSize: 10.5, fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>{label}</span>
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
function RowMenu({ inst, api, onSettings }: { inst: IndicatorInst; api: IndicatorsApi; onSettings: () => void }) {
  const [open, setOpen] = useState(false);
  const [sub, setSub] = useState(false);
  const ref = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: PointerEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) { setOpen(false); setSub(false); }
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [open]);

  const d = KINDS[inst.kind];
  const own = inst.pane > 0;
  const close = () => { setOpen(false); setSub(false); };
  const item = (label: string, onClick: () => void, disabled = false): ReactNode => (
    <button
      type="button" disabled={disabled}
      onClick={() => { onClick(); close(); }}
      style={{
        display: 'block', width: '100%', textAlign: 'left', padding: '5px 9px', borderRadius: 6,
        border: 'none', background: 'transparent', fontSize: 11.5, cursor: disabled ? 'default' : 'pointer',
        color: disabled ? 'var(--text-secondary)' : 'var(--text-primary)', opacity: disabled ? 0.45 : 1,
      }}
    >
      {label}
    </button>
  );

  return (
    <div ref={ref} style={{ position: 'relative', display: 'inline-flex' }}>
      <button type="button" title="Ещё" onClick={() => setOpen((v) => !v)} style={ICON_BTN}>
        <MoreHorizontal size={11} />
      </button>
      {open && (
        <div style={{ position: 'absolute', top: 'calc(100% + 4px)', left: 0, zIndex: 40, minWidth: 196, padding: 4, borderRadius: 9, ...SURFACE }}>
          {item('Настройки…', onSettings)}
          <div style={{ position: 'relative' }}>
            <button
              type="button" onClick={() => setSub((v) => !v)}
              style={{
                display: 'flex', width: '100%', alignItems: 'center', justifyContent: 'space-between',
                padding: '5px 9px', borderRadius: 6, border: 'none', background: 'transparent',
                fontSize: 11.5, color: 'var(--text-primary)', cursor: 'pointer',
              }}
            >
              Переместить<ChevronRight size={12} />
            </button>
            {sub && (
              <div style={{ position: 'absolute', top: 0, left: '100%', marginLeft: 4, zIndex: 41, minWidth: 186, padding: 4, borderRadius: 9, ...SURFACE }}>
                {item('Выше', () => api.movePane(inst.id, -1), !own)}
                {item('Ниже', () => api.movePane(inst.id, 1), !own)}
                {d.ownPaneOk !== false && !own && item('В отдельную панель', () => api.setPane(inst.id, true))}
                {d.overlayOk && own && item('На основной график', () => api.setPane(inst.id, false))}
              </div>
            )}
          </div>
          {item('Дублировать', () => api.duplicate(inst.id))}
          {/* «Скрыть» и «Удалить» в меню НЕ дублируем: они уже есть на строке
              отдельными кнопками (глаз и корзина), а два пути к одному действию
              заставляют гадать, чем они отличаются. */}
        </div>
      )}
    </div>
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
  const [tab, setTab] = useState<'args' | 'style'>('args');
  // Снимок на открытии — для «Отмены». Правки идут в реальном времени.
  const snapshot = useRef<IndicatorInst>(inst);
  const set = (p: Partial<IndicatorInst>) => api.patch(inst.id, p);
  const smooth = inst.smoothType ?? 'none';

  // Порталом в body: строка индикатора живёт внутри своей панели, а та в
  // песочнице сидит в панели с backdrop-filter — предок с фильтром становится
  // точкой отсчёта для fixed, и окно уехало бы вместе с ней.
  return createPortal(
    <div style={DIALOG_BACKDROP} onPointerDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={DIALOG} onPointerDown={(e) => e.stopPropagation()}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 12px 6px' }}>
          <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-primary)' }}>{d.title(inst)}</span>
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
              <Section label={`Настройки ${d.shortName ?? d.label}`}>
                <Field label={d.lengthLabel ?? 'Длина'}>
                  <input
                    type="number" min={2} max={500} value={inst.length}
                    onChange={(e) => set({ length: clampBand(e.target.value, 2, 500, d.defLength) })}
                    style={numInput(78)}
                  />
                </Field>
                {d.hasSource && (
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
              </Section>

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
              <StyleRow
                label={d.shortName ?? d.label}
                on={inst.lineOn !== false}
                onToggle={() => set({ lineOn: inst.lineOn === false })}
                style={inst.styles?.line ?? { color: api.colorOf(inst), width: inst.width }}
                onStyle={(p) => api.patchStyle(inst.id, 'line', p)}
              />
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
                    value={String(inst.precision ?? 2)}
                    options={[0, 1, 2, 3, 4].map((n) => ({ id: String(n), label: n === 0 ? 'Целые' : `${n} знака` }))}
                    onChange={(v) => set({ precision: Number(v) })}
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
const DIALOG_BACKDROP: CSSProperties = {
  position: 'fixed', inset: 0, zIndex: 100000, display: 'flex',
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
  candles: IndCandle<string>[],
  toSec: (t: string) => number,
  colorOf: (i: IndicatorInst) => string,
): LwSeries[][] {
  return useMemo(() => indicatorSeriesByPane(list, candles, toSec, colorOf), [list, candles, toSec, colorOf]);
}
