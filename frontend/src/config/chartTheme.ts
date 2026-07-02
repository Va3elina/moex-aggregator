/**
 * Единая тема графиков — все токены в одном месте.
 *
 * CSS-переменные (padding, fonts, heights) определены в index.css
 * и адаптируются под breakpoints. Здесь — JS-константы для SVG-графиков.
 *
 * Правило: если значение используется в 2+ компонентах — оно должно быть здесь.
 */

// ─── Цвета линий — theme-aware через CSS-переменные ───
//
// Все строки `var(--name)` нативно резолвятся браузером в SVG attributes:
// stroke, fill, stopColor — поэтому JS просто передаёт строку.
//
// Палитра завязана на editorial theme:
// - accent (рыжий pumpkin) — основная линия
// - funds-flow-positive/negative (forest green / clay) — гистограммные бары
// - text-muted — оси, средние линии
//
// Меняешь theme → меняются цвета на ВСЕХ графиках одновременно. Никаких хардкодов.
export const CHART_COLORS = {
  // Основные линии
  primary: 'var(--accent)',                       // Pumpkin — IndexChart (IMOEX), SimpleChart primary
  accent: 'var(--accent)',                        // Pumpkin — цена в SeasonalityPrice, текущий год в Yearly
  secondary: 'var(--funds-flow-positive)',        // Forest green — secondary lines (Buffett, СЧА index)
  tertiary: 'var(--funds-flow-negative)',         // Clay red — третья линия

  // Позитив / негатив — theme-aware (forest/clay в editorial, brighter в dark)
  positive: 'var(--funds-flow-positive)',
  negative: 'var(--funds-flow-negative)',

  // Вспомогательные
  muted: 'var(--text-muted)',                     // Оси, средние линии, baseline
  adjusted: 'var(--funds-flow-positive)',         // Adjusted price (без гэпов) — match секондару
} as const;

// ─── Палитра для множественных линий (compareYears на Сезонности, fund cards) ───
// Editorial-friendly: deep desaturated цвета, не неон. Подходит к paper bg.
// Первый цвет (используется когда выбран ОДНА серия) — тёмный indigo.
export const FUND_PALETTE = [
  '#3B4F7A', // deep indigo
  '#7A4332', // brick brown
  '#3F6B47', // forest dark
  '#6B4F8A', // muted purple
  '#8A5C3F', // warm clay
  '#2D5F6E', // deep teal
  '#7A3F5C', // rosewood
  '#5C5F2D', // olive
  '#3F5C6B', // slate blue
  '#6B5C2D', // dark gold
] as const;

// ─── Grid и фон ───
// Используем CSS-переменные чтобы grid был theme-aware: тёмный на светлой
// бумажной теме, светлый на dark/okx. Значения --chart-grid / --chart-grid-*
// определены в index.css для каждой темы.

export const GRID = {
  minor: 'var(--chart-grid)',                            // Обычные горизонтальные линии
  major: 'var(--chart-grid-major, var(--chart-grid))',   // Чуть заметнее (SimpleChart)
  zero: 'var(--chart-grid-zero, var(--text-muted))',     // Нулевая линия
  separator: 'var(--chart-grid)',                         // Вертикальные разделители (месяцы)
} as const;

// ─── Crosshair ───

export const CROSSHAIR = {
  color: 'var(--chart-crosshair, var(--text-muted))',
  strokeWidth: 1,
  dashArray: '4,4',
  // Акцентный crosshair (для графиков с цветовым акцентом)
  accentColor: 'var(--accent)',
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

// Tooltip — paper-style (без glass). Все размеры через fluid scale (--fs-*, --sp-*).
// Inline-style используется чтобы CSS var действительно резолвились (Tailwind arbitrary
// с var() работают, но clamp resolved корректно только в native CSS контексте).
export const TOOLTIP = {
  containerClass: 'rounded-lg border border-theme shadow-md tabular-nums',
  containerStyle: {
    background: 'var(--bg-primary)',  // paper — match chart bg, без backdrop-blur
    padding: 'var(--sp-1) var(--sp-2)',  // компактно: 2-4px / 4-8px clamp
    fontSize: 'var(--fs-2xs)',  // 8-11px — fluid, было --fs-xs (10-12.5px)
    // whiteSpace: nowrap — критично для multi-row tooltip'ов в narrow viewport.
    // Без него на mobile (375px) при tooltip y правом крае chart карточка с
    // содержимым "С 2007 г. +13.01%" разъезжается на 2 строки. nowrap на
    // контейнере + индивидуальные nowrap'ы на label/value/date гарантируют
    // что текст всегда в одну строку независимо от позиции курсора.
    whiteSpace: 'nowrap' as const,
  },
  labelClass: 'text-theme-secondary whitespace-nowrap',
  labelStyle: { fontSize: 'var(--fs-2xs)' },
  valueClass: 'font-semibold tabular-nums whitespace-nowrap',
  valueStyle: { fontSize: 'var(--fs-2xs)' },  // унифицировано с labelStyle для компактности
  /* Плавающая дата над графиком (crosshair/annotation hover) — без рамки и
     без подложки: только текст. Прозрачный фон убирает саму проблему обрезки
     высоких букв (Д, У, цифры) краем paper-прямоугольника. Безопасно, т.к.
     дата стоит НАД верхней линией графика, а вертикальный crosshair начинается
     ровно от неё — линия не проходит за цифрами. Бывшие dateClass/dateStyle
     (рамка + paper) удалены — последний потребитель (дата внутри hover-карточки
     cbr-flows) переведён на эту же плавающую пилюлю. */
  datePillClass: 'text-theme-secondary tabular-nums whitespace-nowrap',
  datePillStyle: {
    background: 'transparent',
    padding: '0 var(--sp-2)',
    fontSize: 'var(--fs-2xs)',
    whiteSpace: 'nowrap' as const,
  },
  dotClass: 'rounded-full flex-shrink-0',
  dotStyle: { width: 'var(--sp-2)', height: 'var(--sp-2)' },
  /** @deprecated Use `dotClass` + `dotStyle`. Kept for backward compat. */
  dotSize: 'w-2 h-2 rounded-full',
} as const;

// ─── Padding (fallback для случаев когда CSS var недоступен) ───
// Основные значения в index.css: --chart-pad-left, --chart-pad-right-dual и т.д.

export const PADDING = {
  top: 10,
  bottom: 50,
  left: 60,     // fallback — CSS var: --chart-pad-left (100px desktop)
  rightDual: 80, // fallback — CSS var: --chart-pad-right-dual (95px desktop)
  rightSingle: 80, // fallback — unified with rightDual, see --chart-pad-right-single
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
