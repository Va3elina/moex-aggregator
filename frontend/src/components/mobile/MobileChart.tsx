/**
 * MobileChart — edge-to-edge chart для мобильной версии.
 *
 * Дизайн отличается от десктопного SimpleChart:
 *   - Chart-area тянется на ВСЮ ширину frame'а
 *   - Y-axis tick labels рендерятся ВНУТРИ chart-area по краям (с полу-
 *     прозрачным фоном) вместо отдельных боковых отступов
 *   - Current value pill — на правом краю каждой линии (всегда виден)
 *   - X-axis labels снизу с минимальным паддингом
 *   - Crosshair через long-press (300ms hold) вместо hover
 *
 * Поддерживает 1 или 2 серии: primary (price) + optional secondary (OI).
 * Каждая со своей шкалой Y (dual-axis).
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties } from 'react';
import { axisFontSize } from '../chart/chartTypography';
import { niceScale } from '../../utils/niceTicks';

/**
 * Одна точка серии. `gap: true` — заглушка в зоне без реальных данных
 * (см. alignSeriesToPrimary). В таких точках линия разрывается.
 */
export interface ChartPoint {
  time: string;
  value: number;
  displayTime?: string;
  gap?: boolean;
}

export interface MobileChartSeries {
  /** Точки данных. Должно быть >= 2 точек.
   *  - `time` используется для X-позиционирования (alignment, downsampling).
   *  - `displayTime` (опционально) — реальная дата для tooltip/labels.
   *    Используется когда `time` синтетический (например seasonality yearly
   *    где time = синтетическая дата td-mapping'а, а displayTime = реальная
   *    дата торгового дня "2026-05-19").
   *  - `gap` (внутренний, выставляется alignSeriesToPrimary) — точка-заглушка
   *    в зоне, где у серии НЕТ реальных данных (до её первого значения на
   *    общем baseline). Линия в таких точках РАЗРЫВАЕТСЯ, а не держит flat
   *    значение. См. ChartPoint / pathFor. */
  data: ChartPoint[];
  /** Цвет линии (var(--*)). */
  color: string;
  /** Подпись для tooltip'а / legend'а. */
  label: string;
  /** На какой оси: 'left' (primary) или 'right' (secondary). */
  axis?: 'left' | 'right';
  /** Форматтер значений в pill / tooltip. */
  formatValue?: (v: number) => string;
  /** Форматтер подписей ОСИ (короче, чем pill — напр. без юнита «трлн ₽»).
   *  По умолчанию = formatValue. Симметрично desktop formatPrimaryAxis. */
  formatAxis?: (v: number) => string;
  /** Скрыть current-value pill на оси (для секций где pill misleading,
   *  например yearly average которая accumulated за весь год). */
  hidePill?: boolean;
  /** Принудительно сделать эту серию baseline для X-оси (alignSeriesToPrimary).
   *  По умолчанию baseline = самая длинная серия — это верно для seasonality
   *  (median должен занимать всю ширину). Но для ОИ baseline ОБЯЗАН быть цена
   *  (как на десктопе: OI выравнивается к свечам), даже если у инструмента
   *  история OI длиннее цены — иначе цену «растягивают» по таймстемпам OI и она
   *  рисуется плоскими ступенями / разрывами (баг GMKN). Если флаг стоит хотя бы
   *  на одной серии — она и есть baseline; иначе fallback на «самую длинную». */
  isBaseline?: boolean;
}

interface MobileChartProps {
  series: MobileChartSeries[];
  /** Фиксированная высота canvas в px. Если не задана — chart авто-фитится
   *  в контейнер через ResizeObserver (parent должен иметь flex:1 или явную
   *  высоту). */
  height?: number;
  /** Видимость X-axis date labels. */
  showXLabels?: boolean;
  /** Кастомный format для X-axis labels. По умолчанию DD.MM. */
  formatXLabel?: (time: string) => string;
  /** «Круглые» деления (nice ticks) для ЛЕВОЙ / ПРАВОЙ оси отдельно.
   *  Default false — прочие мобильные графики не задеты. */
  niceTicksLeft?: boolean;
  niceTicksRight?: boolean;
  loading?: boolean;
  /**
   * Явный триггер replay'я line-draw анимации. Структурная сигнатура
   * (длина + крайние таймстемпы price-серии) не ловит смену контента, когда
   * меняются только значения при тех же датах — например переключение
   * «что показать» в ОИ (Открытый интерес → Лонг → Нетто). Parent передаёт
   * идентификатор «что показано»; смена → анимация replay'ится. Не передан
   * → fallback на структурную сигнатуру (прежнее поведение).
   */
  animKey?: string;
}

const PAD_TOP = 8;
const PAD_BOTTOM = 34; // воздух снизу для X-labels (дата чуть выше bottom edge)
const PAD_X = 4;

/**
 * Лёгкий haptic tick — срабатывает когда crosshair переходит на новую точку.
 * Имитирует TradingView mobile feel. На iOS Safari тихо не работает (Apple
 * специально не имплементировал Vibration API) — graceful fallback.
 */
function hapticTick() {
  if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
    navigator.vibrate(8);
  }
}

// Y-axis tick positions: 5 равномерных уровней (10/30/50/70/90% от высоты).
// Чаще чем стандартные 3 — приближает к десктоп-чарту, юзеру проще читать
// промежуточные значения OI / цены.
const Y_TICK_POSITIONS = [0.1, 0.3, 0.5, 0.7, 0.9];

/**
 * LTTB (Largest Triangle Three Buckets) — downsampling алгоритм для визуального
 * сохранения формы линии. На годовом графике с 250 точками и 360px viewport
 * без downsampling'а получается "забор" — каждый день рендерится как пик.
 * LTTB выбрасывает монотонные сегменты, сохраняя локальные экстремумы.
 *
 * Сложность O(n). Сохраняет первую и последнюю точку (важно для pill'ов с
 * current value). Stable: одни и те же данные дают одинаковый результат.
 *
 * Reference: Sveinn Steinarsson, MSc thesis 2013 (University of Iceland).
 */
function lttbDownsample<T extends { time: string; value: number; gap?: boolean }>(
  data: T[],
  threshold: number,
): T[] {
  if (threshold >= data.length || threshold <= 2) return data;

  const sampled: T[] = [data[0]];
  const bucketSize = (data.length - 2) / (threshold - 2);
  let a = 0;

  for (let i = 0; i < threshold - 2; i++) {
    // Bounds следующего bucket для расчёта "среднего" anchor'а
    const rangeStart = Math.floor((i + 1) * bucketSize) + 1;
    const rangeEnd = Math.min(Math.floor((i + 2) * bucketSize) + 1, data.length);

    let avgX = 0;
    let avgY = 0;
    const rangeLen = rangeEnd - rangeStart;
    for (let j = rangeStart; j < rangeEnd; j++) {
      avgX += j;
      avgY += data[j].value;
    }
    avgX /= rangeLen;
    avgY /= rangeLen;

    // Текущий bucket — ищем точку формирующую максимальную площадь треугольника
    const currStart = Math.floor(i * bucketSize) + 1;
    const currEnd = Math.floor((i + 1) * bucketSize) + 1;
    const aX = a;
    const aY = data[a].value;

    let maxArea = -1;
    let bestIdx = currStart;
    for (let j = currStart; j < currEnd; j++) {
      const area = Math.abs(
        (aX - avgX) * (data[j].value - aY) - (aX - j) * (avgY - aY),
      );
      if (area > maxArea) {
        maxArea = area;
        bestIdx = j;
      }
    }

    sampled.push(data[bestIdx]);
    a = bestIdx;
  }

  sampled.push(data[data.length - 1]);
  return sampled;
}

/**
 * Симметричное SMA — sliding window [i - w/2, i + w/2]. В отличие от
 * trailing SMA не сдвигает кривую вправо: для каждой точки берётся
 * среднее с её соседями по обе стороны.
 *
 * Применяется ПОСЛЕ LTTB чтобы убрать high-frequency джиттер. LTTB
 * сохраняет swing-точки (high/low) что на волатильных рядах (OI, news
 * spike'и) создаёт эффект "забора". SMA усредняет соседние swing'и в
 * плавный тренд. Тейлдеры именно так смотрят OI — через MA-сглаженный
 * график, а не raw daily ticks.
 */
function smaSmooth<T extends { time: string; value: number; gap?: boolean }>(
  data: T[],
  window: number,
): T[] {
  if (data.length < window || window < 2) return data;
  const half = Math.floor(window / 2);
  const result: T[] = [];
  for (let i = 0; i < data.length; i++) {
    // Gap-точку оставляем как есть — она не рисуется, усреднять её незачем,
    // и она не должна «заражать» соседей своим placeholder-значением.
    if (data[i].gap) {
      result.push(data[i]);
      continue;
    }
    // Окно СИММЕТРИЧНО сжимается у краёв: на расстоянии d от ближайшего края
    // берём по min(half, d) соседей с каждой стороны. На ПОСЛЕДНЕЙ точке d=0 →
    // окно = сама точка → СЫРОЕ значение. Это критично: последняя точка — это
    // «текущая цена» (её подсвечивает pill). Раньше окно у края вырождалось в
    // ТРЕЙЛИНГ-среднее [i-half, i] → на прорежённом длинном периоде соседи слева
    // далеко по времени → «текущая цена» уезжала (1г=5479 vs реальная 1н=5717).
    // Первая точка тоже остаётся сырой (левый край не искажается).
    const eHalf = Math.min(half, i, data.length - 1 - i);
    const start = i - eHalf;
    const end = i + eHalf + 1;
    let sum = 0;
    let count = 0;
    for (let j = start; j < end; j++) {
      if (data[j].gap) continue; // не усредняем с заглушками
      sum += data[j].value;
      count += 1;
    }
    result.push({ ...data[i], value: count > 0 ? sum / count : data[i].value });
  }
  return result;
}

/**
 * Универсальный форматтер даты/времени для X-axis и tooltip.
 *
 * Автоматически определяет intraday vs daily timeframe по timestamp:
 *  - "2026-05-12T08:00:00" → "12.05 08:00" (есть ненулевое время)
 *  - "2026-05-12T00:00:00" → "12.05.26"     (daily — полночь)
 *  - "2026-05-12"          → "12.05.26"     (без time)
 *
 * Раньше использовался toLocaleDateString для всего, поэтому на 5мин/1ч
 * таймфреймах часы пропадали — юзер видел повторяющиеся даты без понимания
 * какой час показан.
 */
function formatTimeForDisplay(time: string): string {
  const d = new Date(time);
  if (isNaN(d.getTime())) return time;
  // Intraday если timestamp содержит время и оно НЕ полночь UTC
  const isIntraday = time.includes('T') && !time.endsWith('T00:00:00');
  if (isIntraday) {
    const dd = String(d.getDate()).padStart(2, '0');
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const HH = String(d.getHours()).padStart(2, '0');
    const MM = String(d.getMinutes()).padStart(2, '0');
    return `${dd}.${mm} ${HH}:${MM}`;
  }
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
}

/**
 * Time-align all series to a common X-axis baseline.
 *
 * Контекст: разные источники данных возвращают ряды РАЗНОЙ длины и/или
 * РАЗНОГО диапазона. Примеры:
 *   - OI: candles 161 точка Nov-May, OI 118 точек Nov-May (одинаковый
 *     диапазон, пропуски внутри) → нужно forward-fill чтобы линии
 *     заканчивались на одной x-координате
 *   - Seasonality yearly: median Jan-Dec (252 точки), current year Jan-May
 *     (100 точек) → median должен занимать всю ширину, current year только
 *     часть слева (не extend'им в будущее искусственно)
 *
 * Алгоритм:
 *   1. Baseline = САМАЯ ДЛИННАЯ серия (по числу точек). Она задаёт x-ось.
 *   2. Для каждой не-baseline серии: для каждого baseline-timestamp ищем
 *      значение этой серии с тем же ключом (дата для дневных или полный
 *      timestamp для intraday). Нет match — carry-forward (внутри диапазона).
 *   3. После последнего timestamp'а secondary — STOP (не extend'им). Это
 *      позволяет короткой серии (current year) визуально закончиться где
 *      её данные кончаются, а длинная — продолжалась до правого края.
 *
 * Зеркало `alignToCandles` из OpenInterestPage.tsx, обобщённое для случая
 * когда baseline не series[0].
 */
function alignSeriesToPrimary(series: MobileChartSeries[]): MobileChartSeries[] {
  if (series.length < 2) return series;

  // Baseline: явный (series.isBaseline) имеет приоритет — это домен-знание
  // страницы. ОИ ставит isBaseline на цену, чтобы x-ось всегда была свечами
  // (как десктоп), даже когда история OI у инструмента длиннее цены.
  //   - OI: baseline = price (явный флаг) — цена не растягивается/не рвётся
  //   - Seasonality yearly: флага нет → fallback на «самую длинную» (median),
  //     текущий год остаётся короче и не достраивается в будущее
  let baselineIdx = series.findIndex((s) => s.isBaseline);
  if (baselineIdx < 0) {
    baselineIdx = 0;
    for (let i = 1; i < series.length; i++) {
      if (series[i].data.length > series[baselineIdx].data.length) baselineIdx = i;
    }
  }
  const baseline = series[baselineIdx];
  if (!baseline || baseline.data.length < 2) return series;

  // intraday vs daily: ключ по полному timestamp если у baseline видна
  // intraday-точность (нечастая ситуация на мобиле, но безопасно).
  const looksIntraday = baseline.data.some((p) => p.time.includes('T') && !p.time.endsWith('T00:00:00'));
  const keyOf = (t: string) => (looksIntraday ? t : t.slice(0, 10));

  return series.map((s, i) => {
    if (i === baselineIdx) return s; // baseline не трогаем
    if (s.data.length === 0) return s;

    // Map значений по date-key + parallel map для displayTime (если есть).
    const valueMap = new Map<string, number>();
    const displayMap = new Map<string, string | undefined>();
    for (const p of s.data) {
      const k = keyOf(p.time);
      valueMap.set(k, p.value);
      displayMap.set(k, p.displayTime);
    }

    // sLastTime ограничивает aligned справа: не extend'им за пределы
    // фактических данных secondary серии.
    const sLastTime = s.data[s.data.length - 1].time;
    const firstValue = s.data[0].value;

    const aligned: ChartPoint[] = [];
    let lastValue: number | null = null;
    let lastDisplay: string | undefined = undefined;
    for (const bp of baseline.data) {
      // STOP когда вышли за пределы данных secondary справа.
      // Это позволяет seasonality yearly: median 252 точки полная ширина,
      // current year aligned обрывается в текущем месяце.
      if (bp.time > sLastTime) break;
      const k = keyOf(bp.time);
      const v = valueMap.get(k);
      if (v !== undefined) {
        lastValue = v;
        lastDisplay = displayMap.get(k);
      }
      if (lastValue === null) {
        // ДО первого реального значения серии: данных нет → РАЗРЫВ линии,
        // а не горизонтальное удержание firstValue. Раньше тут пушился
        // firstValue (carry-forward) — это рисовало плоское плато слева и
        // вертикальный обрыв на стыке с реальными данными (баг: у цены,
        // когда её история короче OI, линия «стояла на месте»). gap=true →
        // pathFor пропускает точку и стартует новый сегмент. Значение всё
        // равно ставим (firstValue) чтобы не ломать fit()/range, но оно не
        // рисуется и не показывается в tooltip/pill.
        aligned.push({ time: bp.time, value: firstValue, gap: true });
        continue;
      }
      // Внутри диапазона серии: carry-forward последнего известного значения
      // (заполняем редкие пропуски дат, чтобы линии заканчивались на одной
      // x-координате).
      aligned.push({
        time: bp.time,
        value: lastValue,
        displayTime: lastDisplay,
      });
    }
    return { ...s, data: aligned };
  });
}

/**
 * Последнее РЕАЛЬНОЕ (не gap) значение ряда — для current-value pill.
 * Берётся из СЫРОГО ряда (prop), МИНУЯ alignSeriesToPrimary/LTTB/smaSmooth:
 * прорежение и сглаживание — для ВИЗУАЛА линии, но «текущая цена» обязана
 * совпадать с реальной последней точкой (и с десктопом — там тот же endpoint,
 * но без обработки). Без этого pill показывал сглаженную/усреднённую цену,
 * уезжавшую на длинных периодах (1г≠1н при одной и той же последней свече).
 */
function lastRealValue(s?: MobileChartSeries): number | null {
  if (!s) return null;
  for (let i = s.data.length - 1; i >= 0; i--) {
    if (!s.data[i].gap) return s.data[i].value;
  }
  return null;
}

export default function MobileChart({
  series: rawSeries,
  height: heightProp,
  showXLabels = true,
  formatXLabel,
  niceTicksLeft = false,
  niceTicksRight = false,
  loading = false,
  animKey,
}: MobileChartProps) {
  // Выравниваем secondary серии к primary по времени (forward-fill).
  // Это перед всеми useMemo, потому что downsampling и range computation
  // должны работать уже на выровненных рядах.
  const series = useMemo(() => alignSeriesToPrimary(rawSeries), [rawSeries]);

  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(360);
  const axisFs = axisFontSize(width);
  const [measuredHeight, setMeasuredHeight] = useState(260);
  // measured — стал ли известен реальный размер контейнера (первый колбэк
  // ResizeObserver). Line-drawing анимация не должна стартовать до этого:
  // getTotalLength() на path'е, нарисованном по placeholder-ширине 360,
  // вернёт неверную длину → strokeDasharray окажется короче реального пути
  // → правый край графика «не дорисовывается» при первом открытии.
  const [measured, setMeasured] = useState(false);
  const [crosshair, setCrosshair] = useState<{ idx: number; pinned: boolean } | null>(null);
  // Refs к каждой SVG <path> для line-drawing animation.
  // Sparse array — index соответствует sIdx в downsampledSeries.map.
  const pathRefs = useRef<(SVGPathElement | null)[]>([]);

  // ResizeObserver с разделёнными threshold'ами:
  //  - ширина: 1px (на iOS address bar НЕ меняет ширину контейнера, threshold
  //    не нужен; убрав 8px фильтр получаем точное соответствие SVG ширины
  //    реальному контейнеру — критично для fullscreen где разница в несколько
  //    пикселей даёт визуальную «обрезку» графика справа).
  //  - высота: 8px (iOS Safari address bar show/hide меняет высоту вьюпорта
  //    постоянно при скролле — без фильтра chart дёргается).
  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const w = e.contentRect.width;
        const h = e.contentRect.height;
        if (w > 0) {
          setWidth((prev) => (Math.abs(prev - w) >= 1 ? Math.max(200, w) : prev));
          setMeasured(true);  // реальная ширина известна — анимация может стартовать
        }
        if (h > 0) {
          setMeasuredHeight((prev) =>
            Math.abs(prev - h) >= 8 ? Math.max(160, h) : prev,
          );
        }
      }
    });
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  // Если задан явно — используем; иначе измеренная высота контейнера.
  const height = heightProp ?? measuredHeight;

  // Правый жёлоб под current-value pill правой оси (как у десктопа): чтобы pill
  // не садился на КОНЕЦ линии (последняя точка у правого края). Только если правая
  // ось есть и её pill показывается — иначе плот занимает всю ширину как раньше.
  // Левый pill оставляем оверлеем: он у левого края, НЕ на точке данных (последнее
  // значение на правом крае), поэтому на линию обычно не наезжает; жёлоб слева
  // удвоил бы потерю ширины на узком экране. PILL_GUTTER_R — тюнится одним числом.
  const hasRightPill = rawSeries.some((s) => s.axis === 'right' && !s.hidePill);
  const PILL_GUTTER_R = hasRightPill ? 52 : PAD_X;
  const innerW = width - PAD_X - PILL_GUTTER_R;
  const innerH = height - PAD_TOP - PAD_BOTTOM;

  // Pipeline двух стадий:
  //   1. LTTB — уменьшение количества точек до ~width*0.4 (≈144 на 360px)
  //   2. SMA — симметричное сглаживание с adaptive окном:
  //        > 80 точек → window 7 (агрессивно)
  //        > 40       → window 5
  //        > 20       → window 3
  //        иначе      → без сглаживания (короткий период)
  // LTTB сохраняет важные swing'и, но на OI и других волатильных рядах
  // эти swing'и И ЕСТЬ шум. SMA усредняет соседние пики в плавный тренд.
  // Cubic-bezier поверх в pathFor добавляет финальную геометрическую
  // плавность.
  const downsampledSeries = useMemo(() => {
    const threshold = Math.max(50, Math.floor(width * 0.4));

    // Baseline (longest) определяет downsampling ratio. Это критично для
    // index-based xAt: если baseline сжимается 252→160 (ratio 0.635), а
    // короткая серия 124 точки остаётся 124 → их индексы не пропорциональны
    // времени. Current year (Jan-Jun) рендерится до 77% width вместо 49%.
    // Решение: пропорциональный downsample. Короткая серия сжимается с
    // тем же ratio (124 → 79), и относительные позиции сохраняются.
    let baselineLength = 0;
    for (const s of series) {
      if (s.data.length > baselineLength) baselineLength = s.data.length;
    }
    const ratio = baselineLength > threshold ? threshold / baselineLength : 1;

    return series.map((s) => {
      // Целевое число точек для этой серии — пропорционально baseline.
      const targetCount = ratio < 1
        ? Math.max(2, Math.round(s.data.length * ratio))
        : s.data.length;
      let data = lttbDownsample(s.data, targetCount);
      const smaWindow =
        data.length > 80 ? 7 : data.length > 40 ? 5 : data.length > 20 ? 3 : 0;
      if (smaWindow > 0) data = smaSmooth(data, smaWindow);
      return { ...s, data };
    });
  }, [series, width]);

  // Line drawing animation — replay'ится при СТРУКТУРНОЙ смене серий
  // (загрузка новых данных, смена актива, смена периода) и один раз —
  // когда ResizeObserver впервые сообщил реальную ширину (measured).
  // Не запускается:
  //   - на изменения width/height (ResizeObserver) — был баг с "обрезкой"
  //     графика в fullscreen, потому что downsampling зависел от width
  //   - на изменения значений в тех же таймстемпах (например, toggle фонда
  //     в Деньгах-в-Фондах — длина и first/last timestamps те же, просто
  //     суммы пересчитались)
  //
  // Сигнатура структуры: длина primary + first/last timestamp. Если оба
  // совпадают с предыдущим — анимацию не перезапускаем.
  //
  // Pattern:
  //  1. dasharray = pathLength, dashoffset = pathLength → линия скрыта
  //  2. Force reflow → applied
  //  3. transition 600ms → dashoffset 0 → линия проявляется
  //  4. По завершении: dasharray='none' → если потом ResizeObserver изменит
  //     geometry path'а, линия остаётся полностью видимой.
  const lastStructureRef = useRef<string>('');
  useEffect(() => {
    // Ждём первого реального замера контейнера (measured): до него path
    // нарисован по placeholder-ширине 360 → getTotalLength() вернул бы
    // неверную длину → strokeDasharray короче пути → правый край не
    // дорисовывается. measured флипается один раз, поэтому при последующих
    // resize (fullscreen) эффект НЕ перезапускается — это сохраняет
    // прежнее поведение «анимация не реагирует на ширину».
    if (!measured) return;
    const primary = rawSeries[0]?.data;
    const structurePart = primary && primary.length > 0
      ? `${primary.length}|${primary[0].time}|${primary[primary.length - 1].time}|${rawSeries.length}`
      : '';
    // animKey (опц., от parent) — явный триггер replay'я. structurePart НЕ
    // ловит смену контента, когда меняются только значения при тех же датах
    // (переключение «что показать» в ОИ: Открытый интерес → Лонг → Нетто).
    // Сигнатура = animKey + структура: смена любой части перезапускает.
    const signature = `${animKey ?? ''}|${structurePart}`;
    // Сигнатура та же (toggle/refresh без новых таймстемпов) — пропускаем
    if (signature === lastStructureRef.current) return;
    lastStructureRef.current = signature;

    const timers: number[] = [];
    pathRefs.current.forEach((p) => {
      if (!p) return;
      const len = p.getTotalLength();
      if (!Number.isFinite(len) || len === 0) return;
      p.style.transition = 'none';
      p.style.strokeDasharray = `${len}`;
      p.style.strokeDashoffset = `${len}`;
      // Force reflow чтобы initial dashoffset применился до transition
      p.getBoundingClientRect();
      p.style.transition = 'stroke-dashoffset 600ms ease-out';
      p.style.strokeDashoffset = '0';
      // Clear dasharray after animation — путь должен оставаться видимым
      // даже если потом ResizeObserver изменит длину path.
      const tid = window.setTimeout(() => {
        if (p && p.isConnected) {
          p.style.strokeDasharray = 'none';
        }
      }, 650);
      timers.push(tid);
    });
    return () => {
      for (const t of timers) window.clearTimeout(t);
    };
  }, [rawSeries, measured, animKey]);

  // Разделяем серии по осям (на downsampled)
  const leftSeries = useMemo(
    () => downsampledSeries.filter((s) => s.axis !== 'right'),
    [downsampledSeries],
  );
  const rightSeries = useMemo(
    () => downsampledSeries.filter((s) => s.axis === 'right'),
    [downsampledSeries],
  );
  const hasLeft = leftSeries.length > 0;
  const hasRight = rightSeries.length > 0;

  // Range fitting для каждой оси (с 5% headroom сверху-снизу)
  const fit = (data: number[]) => {
    if (data.length === 0) return { min: 0, max: 1, span: 1 };
    const min = Math.min(...data);
    const max = Math.max(...data);
    const span = max - min || 1;
    return { min: min - span * 0.05, max: max + span * 0.05, span: span * 1.1 };
  };

  // Range берёт min/max по ВСЕМ series на оси — иначе secondary series
  // могут выйти за visible (если её range шире чем у primary).
  // niceTicks: округляем границы оси до круглых (niceScale) — линия зумится к
  // круглым границам, и тики (с теми же границами) считаются ОДИН раз здесь,
  // чтобы шаг не рассинхронился. ticks=null → прежнее поведение (Y_TICK_POSITIONS).
  const leftRange = useMemo(() => {
    if (!hasLeft) return null;
    const vals = leftSeries.flatMap((s) => s.data.map((d) => d.value));
    const base = fit(vals);
    if (!niceTicksLeft || vals.length === 0) return { ...base, ticks: null as number[] | null };
    const ns = niceScale(Math.min(...vals), Math.max(...vals), 4);
    return { min: ns.min, max: ns.max, span: ns.max - ns.min, ticks: ns.ticks as number[] | null };
  }, [hasLeft, leftSeries, niceTicksLeft]);
  const rightRange = useMemo(() => {
    if (!hasRight) return null;
    const vals = rightSeries.flatMap((s) => s.data.map((d) => d.value));
    const base = fit(vals);
    if (!niceTicksRight || vals.length === 0) return { ...base, ticks: null as number[] | null };
    const ns = niceScale(Math.min(...vals), Math.max(...vals), 4);
    return { min: ns.min, max: ns.max, span: ns.max - ns.min, ticks: ns.ticks as number[] | null };
  }, [hasRight, rightSeries, niceTicksRight]);

  // Сколько точек на оси X — берём из самой длинной серии (после downsampling)
  const N = useMemo(
    () => Math.max(...downsampledSeries.map((s) => s.data.length), 0),
    [downsampledSeries],
  );

  // X-coordinate i-th точки
  const xAt = (i: number) => PAD_X + (i / Math.max(N - 1, 1)) * innerW;

  // Y-coordinate value по конкретной оси
  const yAt = (v: number, range: { min: number; span: number } | null) =>
    range ? PAD_TOP + innerH - ((v - range.min) / range.span) * innerH : 0;

  // SVG path для линии — Catmull-Rom через cubic Bezier. Преобразует
  // ряд точек в плавную кривую через C{c1} {c2} {p2} сегменты, где c1/c2
  // вычисляются как взвешенная разница соседних точек. Tension 0.5 —
  // стандарт для финансовых графиков (no artificial overshoot на swing'ах).
  // На <3 точках падаем к простому M-L pattern.
  //
  // Gap-точки (gap=true — зона без реальных данных, см. alignSeriesToPrimary)
  // РАЗРЫВАЮТ линию: путь строится по непрерывным рунам не-gap точек, каждый
  // рун начинается своим M. Так короткая серия (напр. цена короче OI) рисуется
  // только там, где у неё есть данные, без плоского плато слева.
  const buildSmoothPath = (pts: { x: number; y: number }[]): string => {
    if (pts.length === 0) return '';
    if (pts.length === 1) return ''; // одиночная точка — линию не рисуем
    if (pts.length === 2) {
      return `M${pts[0].x.toFixed(1)},${pts[0].y.toFixed(1)} L${pts[1].x.toFixed(1)},${pts[1].y.toFixed(1)}`;
    }
    const tension = 0.5;
    let path = `M${pts[0].x.toFixed(1)},${pts[0].y.toFixed(1)}`;
    for (let i = 0; i < pts.length - 1; i++) {
      const p0 = pts[i === 0 ? 0 : i - 1];
      const p1 = pts[i];
      const p2 = pts[i + 1];
      const p3 = pts[i + 2 < pts.length ? i + 2 : i + 1];
      const c1x = p1.x + ((p2.x - p0.x) * tension) / 6;
      const c1y = p1.y + ((p2.y - p0.y) * tension) / 6;
      const c2x = p2.x - ((p3.x - p1.x) * tension) / 6;
      const c2y = p2.y - ((p3.y - p1.y) * tension) / 6;
      path += ` C${c1x.toFixed(1)},${c1y.toFixed(1)} ${c2x.toFixed(1)},${c2y.toFixed(1)} ${p2.x.toFixed(1)},${p2.y.toFixed(1)}`;
    }
    return path;
  };

  const pathFor = (data: ChartPoint[], range: { min: number; span: number } | null) => {
    if (data.length < 2) return '';
    // Быстрый путь: нет ни одной gap-точки → один непрерывный сегмент (общий
    // случай — обе серии одинаковой длины, никаких разрывов).
    if (!data.some((d) => d.gap)) {
      const pts = data.map((d, i) => ({ x: xAt(i), y: yAt(d.value, range) }));
      return buildSmoothPath(pts);
    }
    // Разбиваем на непрерывные руны не-gap точек, сохраняя реальный индекс i
    // для корректной x-координаты. Каждый рун — отдельный subpath.
    const segments: string[] = [];
    let run: { x: number; y: number }[] = [];
    const flush = () => {
      const seg = buildSmoothPath(run);
      if (seg) segments.push(seg);
      run = [];
    };
    data.forEach((d, i) => {
      if (d.gap) { flush(); return; }
      run.push({ x: xAt(i), y: yAt(d.value, range) });
    });
    flush();
    return segments.join(' ');
  };

  // Long-press handlers
  // TradingView-style touch pattern:
  //   touchstart → crosshair появляется МГНОВЕННО на позиции пальца
  //   touchmove  → crosshair следует за пальцем (через тот же handler)
  //   touchend   → crosshair исчезает (палец = указатель, lift = clear)
  // Раньше был 300ms long-press hold для "pin" режима — это и было
  // ощущение "ожидания". TradingView не делает pin вообще, crosshair
  // ведёт себя как live cursor пока палец на chart'е.
  // Haptic tick на каждой новой точке имитирует TradingView feel.
  const updateCrosshairFromTouch = (touchX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = touchX - rect.left - PAD_X;
    const idx = Math.round((relX / innerW) * (N - 1));
    const clamped = Math.max(0, Math.min(N - 1, idx));
    setCrosshair((prev) => {
      if (prev?.idx === clamped) return prev;
      hapticTick();
      return { idx: clamped, pinned: false };
    });
  };

  const handleTouchStart = (e: React.TouchEvent) => {
    updateCrosshairFromTouch(e.touches[0].clientX);
  };

  const handleTouchMove = (e: React.TouchEvent) => {
    updateCrosshairFromTouch(e.touches[0].clientX);
  };

  const handleTouchEnd = () => {
    setCrosshair(null);
  };

  const handleClickAway = () => setCrosshair(null);

  // Mouse handlers для desktop тестирования
  const handleMouseMove = (e: React.MouseEvent) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = e.clientX - rect.left - PAD_X;
    const idx = Math.round((relX / innerW) * (N - 1));
    setCrosshair({ idx: Math.max(0, Math.min(N - 1, idx)), pinned: false });
  };

  const handleMouseLeave = () => {
    if (!crosshair?.pinned) setCrosshair(null);
  };

  // X-axis ticks (3-4 для мобиле, чтобы не наезжали)
  const xTicks = useMemo(() => {
    const count = Math.min(4, N);
    return Array.from({ length: count }, (_, i) => Math.round((i / (count - 1)) * (N - 1)));
  }, [N]);

  // Format X label — DD.MM.YY для дневных таймфреймов, DD.MM HH:mm для intraday.
  // Страницы могут override через formatXLabel prop для специфических форматов
  // (например, MM.YY для Buffett на 10-летних периодах).
  const fmtX = (time: string) => {
    if (formatXLabel) return formatXLabel(time);
    return formatTimeForDisplay(time);
  };

  // Format Y (axis pill)
  const fmtY = (v: number, s: MobileChartSeries) =>
    s.formatValue ? s.formatValue(v) : v.toFixed(0);

  if (loading || series.length === 0 || N < 2) {
    if (loading) {
      // Skeleton — placeholder с диагональными линиями имитирующими chart
      return (
        <div ref={wrapRef} style={{ width: '100%', height: heightProp ?? '100%', minHeight: 160, padding: '0 4px' }}>
          <div
            style={{
              width: '100%',
              height,
              borderRadius: 8,
              background: 'color-mix(in srgb, var(--text-primary) 6%, var(--bg-secondary))',
              animation: 'skeleton-pulse 1.5s ease-in-out infinite',
              position: 'relative',
              overflow: 'hidden',
            }}
          >
            {/* SVG-имитация скелета графика для контекста */}
            <svg width="100%" height={height} viewBox={`0 0 360 ${height}`} preserveAspectRatio="none">
              {[0.2, 0.5, 0.8].map((t, i) => (
                <line
                  key={i}
                  x1={0}
                  y1={height * t}
                  x2={360}
                  y2={height * t}
                  stroke="color-mix(in srgb, var(--text-primary) 4%, transparent)"
                  strokeWidth={1}
                />
              ))}
            </svg>
          </div>
        </div>
      );
    }
    return (
      <div
        ref={wrapRef}
        style={{ width: '100%', height: heightProp ?? '100%', minHeight: 160, display: 'grid', placeItems: 'center', color: 'var(--text-muted)' }}
      >
        Нет данных
      </div>
    );
  }

  // Last value pills (на правом краю)
  const leftLastIdx = hasLeft ? leftSeries[0].data.length - 1 : -1;
  const rightLastIdx = hasRight ? rightSeries[0].data.length - 1 : -1;
  // «Текущая цена» для pill — из СЫРОГО ряда (как ПК), а не из обработанного.
  // leftSeries[0]/rightSeries[0] соответствуют первому raw-ряду той же оси
  // (порядок сохраняется через align→downsample), поэтому фильтруем rawSeries
  // тем же предикатом оси.
  const rawLeftLast = lastRealValue(rawSeries.filter((s) => s.axis !== 'right')[0]);
  const rawRightLast = lastRealValue(rawSeries.filter((s) => s.axis === 'right')[0]);

  // Подписи оси: короткий форматтер если задан, иначе как в pill.
  const fmtYAxis = (v: number, s: MobileChartSeries) =>
    s.formatAxis ? s.formatAxis(v) : fmtY(v, s);
  // Значения делений: круглые (nice ticks) если включено для оси, иначе —
  // прежние 5 уровней Y_TICK_POSITIONS (через yAt позиция идентична старой,
  // поэтому графики без флага не меняются).
  const leftTickVals = !leftRange ? [] : (leftRange.ticks
    ?? Y_TICK_POSITIONS.map((t) => leftRange.min + leftRange.span * (1 - t)));
  const rightTickVals = !rightRange ? [] : (rightRange.ticks
    ?? Y_TICK_POSITIONS.map((t) => rightRange.min + rightRange.span * (1 - t)));
  // Сетка следует за основной видимой осью (левая если есть, иначе правая).
  const gridRange = hasLeft ? leftRange : rightRange;
  const gridVals = hasLeft ? leftTickVals : rightTickVals;

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%', height: heightProp ?? '100%', minHeight: 160 }}>
      <svg
        width={width}
        height={height}
        style={{
          display: 'block',
          userSelect: 'none',
          // touch-action: none — chart полностью не реагирует на встроенные
          // browser-жесты (нет scroll rubber-band, нет dragging). Page sсroll
          // вообще запрещён (.fm-app overflow:hidden), поэтому pan-y был
          // не нужен. Pull-to-refresh работает через document listener
          // в hooks/usePullToRefresh — он независим от SVG touch-action.
          touchAction: 'none',
        }}
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onClick={handleClickAway}
      >
        {/* Y-axis grid lines (горизонтальные) — следуют за делениями основной оси. */}
        {gridRange && gridVals.map((v, i) => {
          const gy = yAt(v, gridRange);
          return (
          <line
            key={`grid-y-${i}`}
            x1={PAD_X}
            y1={gy}
            x2={PAD_X + innerW}
            y2={gy}
            stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)"
            strokeWidth={1}
          />
          );
        })}

        {/* X-axis grid lines (вертикальные) — на тех же X-tick позициях
            что и нижние даты. Полная сетка как в десктопном SimpleChart. */}
        {xTicks.map((idx, i) => {
          // Пропускаем первый и последний tick — они на самом краю SVG,
          // линия слилась бы с edge'ами chart-area.
          if (i === 0 || i === xTicks.length - 1) return null;
          const x = xAt(idx);
          return (
            <line
              key={`grid-x-${i}`}
              x1={x}
              y1={PAD_TOP}
              x2={x}
              y2={PAD_TOP + innerH}
              stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)"
              strokeWidth={1}
            />
          );
        })}

        {/* Линии — рисуются из downsampled данных. Каждая получает ref для
            line-drawing animation (см. useEffect выше). */}
        {downsampledSeries.map((s, sIdx) => {
          const range = s.axis === 'right' ? rightRange : leftRange;
          return (
            <path
              key={`line-${sIdx}`}
              ref={(el) => { pathRefs.current[sIdx] = el; }}
              d={pathFor(s.data, range)}
              fill="none"
              stroke={s.color}
              strokeWidth={2}
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          );
        })}

        {/* Y-axis tick labels — INSIDE chart edges. 5 значений на каждой
            оси (10/30/50/70/90% от высоты), match с tick lines выше. */}
        {hasLeft && leftRange && leftTickVals.map((v, i) => (
            <text
              key={`yl-${i}`}
              x={PAD_X + 4}
              y={yAt(v, leftRange) + 3}
              fontSize={axisFs}
              fontWeight={600}
              fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
              style={{ pointerEvents: 'none' }}
            >
              {fmtYAxis(v, leftSeries[0])}
            </text>
        ))}
        {hasRight && rightRange && rightTickVals.map((v, i) => (
            <text
              key={`yr-${i}`}
              x={width - 4}
              y={yAt(v, rightRange) + 3}
              fontSize={axisFs}
              fontWeight={600}
              fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
              textAnchor="end"
              style={{ pointerEvents: 'none' }}
            >
              {fmtYAxis(v, rightSeries[0])}
            </text>
        ))}

        {/* Current value pills — каждая У СВОЕЙ оси (как на десктопе):
            primary (left axis) → pill прижата к ЛЕВОМУ краю chart-area
            secondary (right axis) → pill прижата к ПРАВОМУ краю
            Pill сидит на Y-координате последнего значения серии. Это даёт
            юзеру "current value" анкер на стороне соответствующей оси,
            а не оба значения сжатых в правом верхнем углу. */}
        {hasLeft && leftRange && leftLastIdx >= 0 && !leftSeries[0].hidePill && (() => {
          // Значение — сырое (как ПК); fallback на обработанное, если сырого нет.
          const lastV = rawLeftLast ?? leftSeries[0].data[leftLastIdx].value;
          const y = yAt(lastV, leftRange);
          const text = fmtY(lastV, leftSeries[0]);
          return (
            <PillLabel
              key="pill-left"
              x={PAD_X}
              y={y}
              text={text}
              color={leftSeries[0].color}
              anchor="start"
              fontSize={axisFs}
            />
          );
        })()}
        {hasRight && rightRange && rightLastIdx >= 0 && !rightSeries[0].hidePill && (() => {
          // Значение — сырое (как ПК); fallback на обработанное, если сырого нет.
          const lastV = rawRightLast ?? rightSeries[0].data[rightLastIdx].value;
          const y = yAt(lastV, rightRange);
          const text = fmtY(lastV, rightSeries[0]);
          return (
            <PillLabel
              key="pill-right"
              x={width}
              y={y}
              text={text}
              color={rightSeries[0].color}
              anchor="end"
              fontSize={axisFs}
            />
          );
        })()}

        {/* Crosshair vertical line + tooltip-data dots */}
        {crosshair && (
          <g pointerEvents="none">
            <line
              x1={xAt(crosshair.idx)}
              y1={PAD_TOP}
              x2={xAt(crosshair.idx)}
              y2={PAD_TOP + innerH}
              stroke="var(--text-primary)"
              strokeWidth={1}
              strokeDasharray="3,3"
              opacity={0.6}
            />
            {downsampledSeries.map((s, sIdx) => {
              const range = s.axis === 'right' ? rightRange : leftRange;
              // Skip если idx за пределами этой series — не clamp
              if (crosshair.idx >= s.data.length) return null;
              const point = s.data[crosshair.idx];
              // Gap-точка (зона без данных) — dot не рисуем, реального значения тут нет.
              if (!point || point.gap) return null;
              const x = xAt(crosshair.idx);
              const y = yAt(point.value, range);
              return (
                <circle
                  key={`cross-dot-${sIdx}`}
                  cx={x}
                  cy={y}
                  r={4}
                  fill={s.color}
                  stroke="var(--bg-primary)"
                  strokeWidth={2}
                />
              );
            })}
          </g>
        )}

        {/* X-axis labels */}
        {showXLabels && (
          <g>
            {xTicks.map((idx, ti) => {
              const longest = downsampledSeries.reduce(
                (acc, s) => (s.data.length > acc.data.length ? s : acc),
                downsampledSeries[0],
              );
              const point = longest.data[Math.min(idx, longest.data.length - 1)];
              if (!point) return null;
              const x = xAt(idx);
              const isFirst = ti === 0;
              const isLast = ti === xTicks.length - 1;
              const anchor = isFirst ? 'start' : isLast ? 'end' : 'middle';
              return (
                <text
                  key={`xl-${idx}`}
                  x={x}
                  y={height - 16}
                  fontSize={axisFs}
                  fontWeight={600}
                  fill="var(--text-secondary)"
                  textAnchor={anchor}
                  dominantBaseline="alphabetic"
                >
                  {fmtX(point.time)}
                </text>
              );
            })}
          </g>
        )}
      </svg>

      {/* Tooltip — над линиями. Только когда crosshair активен.
          Использует downsampled данные для consistency с crosshair-dots
          и нарисованными линиями (если бы передали original series — была
          бы рассинхронизация: точка показана на одной y-координате, а
          tooltip показывает другое значение). */}
      {crosshair && (
        <TooltipCard
          x={xAt(crosshair.idx)}
          chartW={width}
          series={downsampledSeries}
          idx={crosshair.idx}
        />
      )}
    </div>
  );
}

// ──────────────────────────────────────────────────────────
// Sub-components
// ──────────────────────────────────────────────────────────

interface PillLabelProps {
  x: number;
  y: number;
  text: string;
  color: string;
  anchor?: 'start' | 'end';
  fontSize?: number;
}

function PillLabel({ x, y, text, color, anchor = 'end', fontSize = 10 }: PillLabelProps) {
  const fontY = fontSize;
  const charW = fontY * 0.55;
  const padX = 4;
  const padY = 2;
  const w = text.length * charW + padX * 2;
  const h = fontY + padY * 2;
  const pillX = anchor === 'end' ? x - w - 2 : x + 2;
  return (
    <g pointerEvents="none">
      <rect x={pillX} y={y - h / 2} width={w} height={h} rx={3} fill={color} />
      <text
        x={pillX + w / 2}
        y={y}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={fontY}
        fontWeight={700}
        fill="#fff"
        style={{ fontFamily: 'IBM Plex Mono, monospace' }}
      >
        {text}
      </text>
    </g>
  );
}

interface TooltipCardProps {
  x: number;
  chartW: number;
  series: MobileChartSeries[];
  idx: number;
}

function TooltipCard({ x, chartW, series, idx }: TooltipCardProps) {
  const W = 140;
  const leftSide = x > chartW / 2;
  const left = leftSide ? Math.max(8, x - W - 8) : Math.min(chartW - W - 8, x + 8);

  // Дата для tooltip:
  //   1. Сначала ищем серию у которой есть точка с этим idx — это «primary
  //      по охвату»: на seasonality yearly cur(2026) покрывает только первую
  //      половину индексов, и её дата (реальная "19.05.26") должна
  //      показываться, а не avg synthetic дата для того же idx ("26.06.26").
  //   2. Если ни одна серия не покрывает (краевой случай) — берём самую
  //      длинную (она всегда имеет какую-то точку).
  // Предпочитаем серию, у которой на этом idx есть РЕАЛЬНАЯ точка (не gap) —
  // её дата корректна. Затем любую покрывающую idx. Иначе самую длинную.
  const refSeries =
    series.find((s) => idx < s.data.length && !s.data[idx]?.gap) ??
    series.find((s) => idx < s.data.length) ??
    series.reduce((acc, s) => (s.data.length > acc.data.length ? s : acc), series[0]);
  const refPoint = refSeries?.data[Math.min(idx, refSeries.data.length - 1)];
  // Формат даты с учётом intraday: для 5мин/1ч timestamps с ненулевым временем
  // (e.g. "2026-05-12T08:00:00") показываем "12.05 08:00". Для daily — "12.05.26".
  // displayTime > time: для seasonality yearly time синтетический (td-mapping),
  // а displayTime — реальная дата ("2026-05-19").
  const refTime = refPoint?.displayTime ?? refPoint?.time;
  const date = refTime ? formatTimeForDisplay(refTime) : '';

  const style: CSSProperties = {
    position: 'absolute',
    top: 8,
    left,
    width: W,
    background: 'var(--bg-primary)',
    border: '1.5px solid var(--text-primary)',
    borderRadius: 8,
    padding: '6px 9px',
    fontSize: 'var(--fs-xs)',
    boxShadow: '3px 3px 0 var(--text-primary)',
    pointerEvents: 'none',
  };

  return (
    <div style={style}>
      <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-2xs)', marginBottom: 4, fontWeight: 600 }}>
        {date}
      </div>
      {series.map((s, i) => {
        // Skip серии где idx за пределами data — не clamp'аем к last point,
        // иначе tooltip "висит" на последнем известном значении когда юзер
        // навёлся на дату для которой данных нет (например yearly current
        // year ещё не дошёл до этого td). Gap-точку тоже скипаем — это зона
        // без реальных данных серии (цена короче OI слева).
        if (idx >= s.data.length) return null;
        const point = s.data[idx];
        if (!point || point.gap) return null;
        const value = s.formatValue ? s.formatValue(point.value) : point.value.toFixed(2);
        return (
          <div
            key={`tr-${i}`}
            style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 6, marginTop: i === 0 ? 0 : 2 }}
          >
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, color: s.color, fontWeight: 600 }}>
              <span style={{ width: 7, height: 7, borderRadius: 99, background: s.color }} />
              {s.label}
            </span>
            <span className="mono" style={{ color: 'var(--text-primary)', fontWeight: 700 }}>{value}</span>
          </div>
        );
      })}
    </div>
  );
}
