/**
 * Единая тема графиков — все токены в одном месте.
 *
 * CSS-переменные (padding, fonts, heights) определены в index.css
 * и адаптируются под breakpoints. Здесь — JS-константы для SVG-графиков.
 *
 * Правило: если значение используется в 2+ компонентах — оно должно быть здесь.
 */

// ─── Цвета линий (уникальные для каждого графика, но из единой палитры) ───

export const CHART_COLORS = {
  // Основные линии
  primary: '#6366f1',      // Индиго — IndexChart (IMOEX), SimpleChart primary
  accent: '#C8FF2E',       // Лайм — цена в SeasonalityPrice, текущий год в Yearly
  secondary: '#f59e0b',    // Оранжевый — SimpleChart secondary, Buffett
  tertiary: '#f43f5e',     // Розовый — SimpleChart третья линия

  // Позитив / негатив (гистограммы, хитмап)
  positive: '#2EE59D',
  negative: '#FF4D4D',

  // Вспомогательные
  muted: '#9CA3B8',        // Оси, вторичный текст, средняя линия Yearly
  adjusted: '#22c55e',     // Adjusted price (без дивидендов)

  // Индекс страха (зоны)
  fear: {
    extremeGreed: '#22c55e',
    greed: '#84cc16',
    neutral: '#eab308',
    fear: '#f97316',
    extremeFear: '#ef4444',
  },
} as const;

// ─── Цвета фондов (палитра для множественных линий) ───

export const FUND_PALETTE = [
  '#2EE59D', '#4DA3FF', '#9D4DFF', '#FF4D4D', '#FFB020',
  '#00D9FF', '#FF6B9D', '#FCD34D', '#14B8A6', '#F97316',
] as const;

// ─── Grid и фон ───

export const GRID = {
  minor: 'rgba(255,255,255,0.06)',      // Обычные горизонтальные линии
  major: 'rgba(255,255,255,0.08)',      // Чуть заметнее (SimpleChart)
  zero: 'rgba(255,255,255,0.15)',       // Нулевая линия
  separator: 'rgba(255,255,255,0.06)', // Вертикальные разделители (месяцы)
} as const;

// ─── Crosshair ───

export const CROSSHAIR = {
  color: 'rgba(255,255,255,0.3)',
  strokeWidth: 1,
  dashArray: '4,4',
  // Акцентный crosshair (для графиков с цветовым акцентом)
  accentColor: '#C8FF2E',
  accentDashArray: '4,3',
  accentOpacity: 0.5,
} as const;

// ─── Dot (точка на hover) ───

export const DOT = {
  radius: 4,
  strokeWidth: 2,
  strokeColor: 'var(--bg-secondary)',
} as const;

// ─── SVG viewBox (единый для всех кастомных SVG) ───

export const SVG = {
  viewBoxWidth: 1000,
  viewBoxHeight: 500,
  viewBox: '0 0 1000 500',
} as const;

// ─── Line stroke ───

export const LINE = {
  primaryWidth: 2,
  secondaryWidth: 1.5,
  accentWidth: 2.5,
  dashed: '6,3',          // Пунктир для adjusted / average
  lineCap: 'round' as const,
  lineJoin: 'round' as const,
} as const;

// ─── Анимация ─────────────────────────────────────────────────────
// Единое место для ВСЕХ параметров анимации графиков.
// Импортируй ANIMATION из chartTheme вместо хардкода цифр в компонентах.

export const ANIMATION = {
  // ── Морфинг линейных графиков (SimpleChart, PriceChart, IndexChart) ──
  /** мс — длительность перехода между двумя наборами точек */
  morphDuration: 600,
  /** easeOutCubic — быстрый старт, мягкий финиш */
  easing: (t: number) => 1 - Math.pow(1 - t, 3),

  // ── Волна гистограммы (SeasonalityHistogram, FlowsHistogram) ──
  // Бары вырастают из нуля с каскадом слева направо.
  // waveDuration — общее время от старта первого бара до финиша последнего.
  // waveStagger — разброс задержки между первым и последним баром.
  // Per-bar анимация = waveDuration − waveStagger.
  /** мс — полная длительность волны */
  waveDuration: 1500,
  /** мс — разброс задержки между первым и последним баром */
  waveStagger: 700,
  /** CSS easing для transition/animation баров */
  waveEasing: 'cubic-bezier(0.25, 0.46, 0.45, 0.94)',

  // ── CSS reveal (clip-path left-to-right sweep) ──
  // Используется через класс .chart-reveal в index.css
  revealDuration: 1000,

  // ── Legacy (обратная совместимость) ──
  duration: 600,
  staggerBase: 30,
} as const;

// ─── Tooltip CSS-классы (Tailwind) ───

export const TOOLTIP = {
  containerClass: 'bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl py-1.5 px-3 tabular-nums',
  labelClass: 'text-[11px] text-theme-secondary',
  valueClass: 'text-xs font-semibold tabular-nums',
  dateClass: 'text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded-md border border-theme tabular-nums',
  dotSize: 'w-2 h-2 rounded-full',
} as const;

// ─── Padding (fallback для случаев когда CSS var недоступен) ───
// Основные значения в index.css: --chart-pad-left, --chart-pad-right-dual и т.д.

export const PADDING = {
  top: 10,
  bottom: 50,
  left: 60,     // fallback — CSS var: --chart-pad-left (100px desktop)
  rightDual: 80, // fallback — CSS var: --chart-pad-right-dual (95px desktop)
  rightSingle: 12,
} as const;

/**
 * Читает CSS-переменную с fallback
 */
export function cssVar(name: string, fallback: number): number {
  if (typeof window === 'undefined') return fallback;
  return parseFloat(getComputedStyle(document.documentElement).getPropertyValue(name)) || fallback;
}

/**
 * Получить стандартные padding из CSS-переменных
 */
export function getChartPadding(dual = true) {
  return {
    top: PADDING.top,
    bottom: PADDING.bottom,
    left: cssVar('--chart-pad-left', PADDING.left),
    right: cssVar(dual ? '--chart-pad-right-dual' : '--chart-pad-right-single',
                  dual ? PADDING.rightDual : PADDING.rightSingle),
  };
}
