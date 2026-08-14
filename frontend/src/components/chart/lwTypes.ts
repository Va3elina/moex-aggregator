/**
 * Общие типы и хелперы графиков (lightweight-charts).
 *
 * Жили в LwChart.tsx, пока он был единственным чарт-компонентом. После перехода
 * всех индикаторов на многопанельный LwChartPanes сам LwChart удалён, а это —
 * то, что от него осталось нужного всем: форма серии/точки, типы слоя рисования,
 * контекст пользовательских настроек графика и форматтеры оси времени.
 */
import { createContext } from 'react';

// value ВСЕГДА = close (для OHLC-серий тоже) — чтобы пилс последнего значения,
// сигнатура reveal-анимации и alert-«+» (все читают def.data[].value) работали
// без спец-веток. open/high/low нужны только candlestick/bar при отрисовке.
export interface LwPoint { time: number; value: number; color?: string; open?: number; high?: number; low?: number; close?: number }

export interface LwSeries {
  id: string;
  type: 'line' | 'area' | 'histogram' | 'candlestick' | 'bar';
  data: LwPoint[];
  color: string;
  scale?: 'left' | 'right';
  lineWidth?: number;
  /** Явная толщина, выбранная юзером в пикере формата серии — побеждает и
   *  глобальный chartPrefs.lineWidth песочницы, и дефолтный lineWidth. */
  userLineWidth?: number;
  /** Пунктирная линия — для прогнозного «хвоста» (Баффетт): проекцию не выдаём
   *  за реальные данные. Действует на line и area. */
  dashed?: boolean;
  areaTop?: string;
  areaBottom?: string;
  base?: number;
  label: string;
  axisFmt?: (v: number) => string;
  tipFmt?: (v: number) => string;
  lastValueVisible?: boolean;
  zeroLine?: boolean;
  /** Мин. шаг цены оси/пилюли. Дефолт 1 (целые — ОИ/Баффетт); проценты/breadth → 0.01/0.1. */
  minMove?: number;
  /** Зоны на шкале серии (RSI: 30/70). Рисуются примитивом под линией, цвета
   *  резолвятся из токенов на стороне чарта. См. chart/bandsPrimitive.ts. */
  bands?: LwBands;
}

/** Границы зон индикатора. Цвета — CSS-токены, резолвятся в LwChartPanes. */
export interface LwBands {
  upper: number;
  lower: number;
  /** null — среднюю линию не рисовать. */
  middle?: number | null;
  color?: string;
  /** Заливать зоны. false — только пунктиры уровней (кому мешает фон). */
  fill?: boolean;
  /** Цвета по элементам: каждая граница и каждая заливка настраиваются отдельно
   *  (иначе на одной панели соседние элементы приходится красить одинаково). */
  upperColor?: string;
  middleColor?: string;
  lowerColor?: string;
  bandFill?: string;
  overFill?: string;
  underFill?: string;
}

// ── Рисование (модель TradingView): фигуры живут в координатах {logical, price},
// перепроецируются на зум/пан/ресайз (как метки экспираций) → «ездят» с графиком.
// logical — дробный индекс бара (timeScale.coordinateToLogical) → свободная позиция.
export type LwDrawShape = 'trend' | 'hline' | 'vline' | 'ray' | 'arrow' | 'rect' | 'ellipse' | 'fib' | 'brush' | 'text' | 'ruler';
export type LwDrawTool = 'select' | LwDrawShape;
export interface LwDrawPoint { logical: number; price: number }
export type LwDash = 'solid' | 'dashed' | 'dotted';
export interface LwDrawing {
  id: string; tool: LwDrawShape; pts: LwDrawPoint[]; color: string; width: number;
  /** Панель, В КОТОРОЙ живёт фигура (0 — основной график, 1+ — панели
   *  индикаторов). Координата `price` у точек считается по шкале ИМЕННО этой
   *  панели: у RSI и у цены шкалы разные, и без номера фигура спроецировалась бы
   *  не туда. Поле необязательное — у фигур, нарисованных до появления
   *  рисования на панелях индикаторов, его нет, и они читаются как панель 0. */
  pane?: number;
  text?: string; dash?: LwDash; opacity?: number; hidden?: boolean; locked?: boolean;
  /** ── настройки ТЕКСТА (у текст-фигуры и у подписи на линии) ── */
  textSize?: number;
  /** null/undefined — цветом самой фигуры. */
  textColor?: string | null;
  textBold?: boolean;
  /** Подложка под текстом — читаемо поверх свечей. */
  textBg?: boolean;
  /** ── ЗАЛИВКА (фон) фигуры: прямоугольник/эллипс ──
   *  undefined читается как true: у фигур, нарисованных до появления настройки,
   *  фон был всегда включён, и они должны остаться такими же. */
  fill?: boolean;
  /** null/undefined — фон цветом самой фигуры (прежнее поведение). */
  fillColor?: string | null;
  /** Прозрачность фона 0..1. undefined — исторические 13% от прозрачности фигуры. */
  fillOpacity?: number;
}

/** Пользовательские настройки графика из песочницы (толщина линий, кроссхэйр,
 *  сетка, последнее значение, водяной знак). Пробрасываются контекстом: вне
 *  песочницы контекст null → поведение движка прежнее. */
export interface ChartPrefs { lineWidth?: 1 | 2 | 3; crosshair?: boolean; grid?: boolean; lastValue?: boolean; watermark?: boolean }
export const ChartPrefsCtx = createContext<ChartPrefs | null>(null);

// Логотип TradingView скрываем; атрибуция (по лицензии Lightweight Charts) — строкой
// в футере/«О проекте». Стиль инжектим один раз (id-селектор ловит все инстансы).
let tvLogoHidden = false;
export function hideTvLogo() {
  if (tvLogoHidden || typeof document === 'undefined') return;
  tvLogoHidden = true;
  const st = document.createElement('style');
  st.textContent = 'a#tv-attr-logo{display:none!important}';
  document.head.appendChild(st);
}


// Ось времени по-русски (как в макете дизайнера песочницы): год / месяц / день / время.
// Тип тика (0..3) — РОДНАЯ логика lightweight-charts (её же использует сам
// TradingView): у каждого бара есть вес относительно ПРЕДЫДУЩЕГО бара (пересёк
// границу года/месяца/дня → Year/Month/Day; иначе — самая крупная внутрисуточная
// граница: 12ч/6ч/3ч/1ч/30м/5м/1м). Алгоритм расстановки тиков предпочитает
// показывать САМЫЙ «круглый» доступный бар — поэтому на мультидневном интрадей-
// зуме иногда мелькает одинокая «12:00» среди чисел дней (бар ровно в полдень
// имеет более высокий вес, чем соседние часовые бары) — это не баг, а то же
// самое поведение, что и в настоящем TradingView; тип 3 нам приходит уже
// свёрнутым (Time), различить «12:00» и «13:00» на этом уровне API нельзя.
const MONTHS_RU = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];
export function ruTickMark(time: unknown, type: number): string {
  const t = typeof time === 'number' ? time : 0;
  const d = new Date(t * 1000);
  if (type === 0) return String(d.getUTCFullYear());
  if (type === 1) return MONTHS_RU[d.getUTCMonth()];
  if (type === 2) return String(d.getUTCDate());
  return String(d.getUTCHours()).padStart(2, '0') + ':' + String(d.getUTCMinutes()).padStart(2, '0');
}

/** §5.2 макета: на дневных графиках ось показывает ТОЛЬКО месяцы и годы —
 *  внутримесячные дневные подписи скрыты (мельтешат и встают неровно при
 *  ресайзе). Для интрадея НЕ подходит (там нужно время) — не передавайте. */
export function monthsYearsTickFmt(time: number, type: number): string {
  const d = new Date(time * 1000);
  if (type === 0) return String(d.getUTCFullYear());
  if (type === 1) return MONTHS_RU[d.getUTCMonth()];
  return '';
}
