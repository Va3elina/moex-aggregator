/**
 * StackedBidirectionalHistogram — стек-бар с двусторонним накоплением.
 *
 * Для каждого периода:
 *   • Категории с value > 0 накапливаются ВВЕРХ от 0
 *   • Категории с value < 0 накапливаются ВНИЗ от 0
 *
 * Архитектура — pattern FlowsHistogram (Притоки/Оттоки):
 *   • Chart-area внутри absolute div с CSS padding (top/bottom/left/right)
 *   • SVG width=100% height=100% preserveAspectRatio="none"
 *   • Bars в процентах от SVG (0-100%)
 *   • Y-axis labels — HTML absolute div справа
 *   • X-axis period labels — HTML absolute div снизу
 *
 * Это даёт chart-area реально расширяющийся до padding'ов, без issue
 * с aspect ratio viewBox vs container, которое раньше центрировало content.
 */

import { forwardRef, useCallback, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react';
import type { CbrFlowsPeriod } from '../../services/api';
import { getCategoryColor } from './cbrPalette';
import { getCategoryShortLabel } from './cbrCategoryInfo';
import { useTheme } from '../../contexts/ThemeContext';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { chartFontScale, axisPadding } from '../chart/chartTypography';
import ChartLegend, { type ChartLegendItem } from '../chart/ChartLegend';
import ChartDatePill from '../chart/ChartDatePill';
import { TOOLTIP } from '../../config/chartTheme';

interface Props {
  periods: CbrFlowsPeriod[];
  categories: string[];
  unit: string;
  height: number;
  loading?: boolean;
  /**
   * Явный триггер re-animation. Меняется когда parent хочет показать wave
   * заново (смена type активов / period selector). НЕ должен меняться
   * при toggle категорий (тогда animation не сбрасывается → нет «пустого
   * графика» при выключении категории по очереди).
   *
   * Если не передан — fallback на periods length+dates (старая логика).
   */
  animTrigger?: string;
  /**
   * Полный (нефильтрованный) список периодов — для расчёта м/м и г/г в
   * тултипе. При узком period-фильтре («1Г») соседи для сравнения лежат
   * за пределами видимых `periods`. Не передан → fallback на `periods`.
   */
  allPeriods?: CbrFlowsPeriod[];
  /** Пилс суммы столбца на ценовой шкале. Уместен там, где значение ОДНО
   *  (чистый поток фондов); для многокатегорийных потоков ЦБ одна цифра
   *  бессмысленна — там подробный тултип. */
  valuePill?: boolean;
  /** Показывать курсорный тултип и В ПЕСОЧНИЦЕ. По умолчанию он там скрыт
   *  правилом .sb-panel .chart-tooltip-root (мелкие панели), но у потоков ЦБ
   *  разбор по категориям — единственный способ прочитать числа. */
  tooltipInSandbox?: boolean;

  // ─── Ниже — обобщение под НЕ-денежные плитки (сезонность). Все дефолты
  //     повторяют прежнее поведение потоков ЦБ/фондов байт-в-байт. ───

  /**
   * Плитка БЕЗ реальных дат: подписи оси и плавающая пилюля берутся из
   * `p.label`, год и сравнения м/м–г/г скрыты. Нужно сезонности, где бар —
   * агрегат за много лет («Январь», «Пн», «10:00»), а `end_date`/`year`
   * заполнить нечем.
   */
  dateless?: boolean;
  /** Суффикс единиц у чисел на Y-оси и в тултипе. По умолчанию «млрд».
   *  Пустая строка — не рисовать суффикс вовсе (его несёт сам формат). */
  unitSuffix?: string;
  /** Формат значения в тултипе и пилсе. По умолчанию — 2 знака после запятой. */
  fmtValue?: (v: number) => string;
  /** Формат подписи Y-оси. По умолчанию — целое. */
  fmtAxis?: (v: number) => string;
  /**
   * Верхняя граница симметричной шкалы из максимума по модулю. Дефолт —
   * округление вверх до десятков с минимумом 10, что зашито под МИЛЛИАРДЫ:
   * ряд в процентах (сезонность даёт ~1–3%) при нём схлопнется в «минимум 10»
   * и все столбцы станут высотой в пиксель.
   * ⚠️ Ссылка должна быть стабильной (useCallback) — уходит в депсы мемо.
   */
  niceMax?: (maxAbs: number) => number;
  /** Цвет столбца по ЗНАКУ значения (зелёный/красный), а не по палитре
   *  категорий ЦБ. Для одиночного знакопеременного ряда категория одна и
   *  цветом кодируется направление, а не «кто из участников». */
  colorBySign?: boolean;
  /** Своя легенда вместо списка категорий (у сезонности это «Рост/Падение»,
   *  а не имена категорий). */
  legendOverride?: ChartLegendItem[];
  /** Линия поверх столбцов — у сезонности медиана «Без выбросов». Длина
   *  values должна совпадать с periods; null = разрыв. */
  overlay?: { label: string; color: string; values: (number | null)[] } | null;
}

/**
 * Императивный handle — по образцу LwChartHandle.syncBeforeCapture: PNG-экспорт
 * (html2canvas) может снять скриншот ДО того, как reveal-анимация (animProgress)
 * и/или ResizeObserver-замер контейнера (containerW) устаканились — напр. если
 * export кликнули сразу после «Развернуть» в fullscreen. В файл попадают бары
 * высотой ~0% и/или X-подписи дат, посчитанные под старую (узкую) ширину панели
 * и потому налезающие друг на друга в НОВОЙ (широкой) fullscreen-области.
 * ExportModal вызывает settleForCapture() в beforeCapture ПЕРЕД captureChart.
 */
export interface StackedBidirectionalHistogramHandle {
  settleForCapture: () => void;
}

interface HoverState {
  periodIdx: number;
  mouseX: number;
  mouseY: number;
}

// Padding chart-area + font-size через CSS variables (fluid clamp в index.css).
// Это даёт автоматическую адаптацию на mobile: --chart-pad-left/right на
// узких экранах меньше (~34-58px), на desktop ~95-100px. Font axis тоже
// scales дискретно (10→11→13→15→17px на breakpoints 320/375/425/768/1024/1440).
// Symmetric padding-left ≈ padding-right даёт chart-area по центру paper-card.
// Match с pattern FlowsHistogram/SimpleChart.
const MIN_BAR_H = 0.6;  // % минимум высоты сегмента

const StackedBidirectionalHistogram = forwardRef<StackedBidirectionalHistogramHandle, Props>(function StackedBidirectionalHistogram({
  periods,
  categories,
  height,
  loading,
  animTrigger,
  valuePill,
  tooltipInSandbox,
  allPeriods,
  dateless,
  unitSuffix,
  fmtValue,
  fmtAxis,
  niceMax,
  colorBySign,
  legendOverride,
  overlay,
}, ref) {
  const { theme } = useTheme();
  const axisSuffix = unitSuffix ?? 'млрд';
  const valueOf = fmtValue ?? ((v: number) => v.toFixed(2));
  const axisOf = fmtAxis ?? ((v: number) => String(Math.round(v)));
  // Цвет столбца: по знаку (одиночный ряд) либо по палитре категорий ЦБ.
  const barColor = (cat: string, v: number) =>
    colorBySign ? (v >= 0 ? 'var(--oi-green)' : 'var(--oi-red)') : getCategoryColor(cat, theme);
  const containerRef = useRef<HTMLDivElement>(null);
  // Chart-контейнер (зона ПОД легендой) — для клампа тултипа в plot-коридоре.
  const chartWrapRef = useRef<HTMLDivElement>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  const vw = useViewportWidth();
  const isMobile = vw < 768;

  // Ширина СВОЕГО контейнера (не window.innerWidth) — на full-page /cbr-flows
  // это одно и то же (chart занимает почти весь viewport), но в узкой панели
  // песочницы container << viewport: без этого легенда/число X-меток берутся
  // по РАЗМЕРУ ОКНА браузера, а не панели → максимальный шрифт легенды +
  // 6 дат-меток набиваются в 300px и налезают друг на друга. containerW=0 до
  // первого измерения → используем vw как fallback (без ResizeObserver flash).
  const [containerW, setContainerW] = useState(0);
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w && w > 0) setContainerW(Math.round(w));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const fontScale = chartFontScale(containerW || vw);
  // Отступы под Y-ось по КОНТЕЙНЕРУ (не viewport) — см. axisPadding: в узкой
  // embed-панели песочницы containerW << vw реального окна, а --chart-pad-*
  // CSS-переменные завязаны на media queries по viewport и не ужимаются →
  // широкий gutter/крупный шрифт наезжают на бары. pad/font ниже подменяют
  // CSS-var значения на всех местах их использования в этом компоненте.
  const pad = axisPadding(containerW || vw);
  const axisFontPx = fontScale.axis;

  // Entrance animation: grow-from-zero wave.
  // animProgress[i] ∈ [0, 1] — прогресс bar'а #i (height + stack scale).
  //
  // animKey — комбинируем ДВА сигнала:
  //   - animTrigger (от parent) — меняется при смене type/period, явный
  //     сигнал «перезапусти wave».
  //   - data-signature (длина + крайние даты) — меняется когда данные впервые
  //     приезжают. На первом открытии компонент рендерится в loading-состоянии
  //     с пустыми данными; эффект делает early-return, а animTrigger потом не
  //     меняется → entrance-анимация НЕ играла при первом открытии страницы.
  //     Signature ловит переход [] → загружено.
  //   Toggle категорий не меняет ни animTrigger, ни signature → wave корректно
  //   НЕ перезапускается.
  //
  // ВАЖНО: signature берём из allPeriods (ПОЛНЫЙ набор), а не из periods (срез
  // нижнего таймлайна-навигатора через navRange). Иначе драг навигатора менял
  // длину/крайние даты periods → wave переигрывалась на каждое движение окна.
  // Теперь — как в «Деньги в фондах»: волна привязана к fetched-данным, а
  // навигатор лишь слайсит уже отрисованные бары. Fallback на periods для
  // вызовов без allPeriods (мобилка без навигатора) — поведение не меняется.
  //   ⚠️ У плитки без дат (dateless) end_date пустой — сигнатуру берём из
  //   label, иначе она вырождается в «длина|", и смена среза (дни недели →
  //   месяцы) с тем же числом баров не перезапускала бы волну.
  const sigPeriods = allPeriods ?? periods;
  const sigOf = (p: CbrFlowsPeriod | undefined) => p?.end_date || p?.label || '';
  const dataSig =
    `${sigPeriods.length}|${sigOf(sigPeriods[0])}` +
    `|${sigOf(sigPeriods[sigPeriods.length - 1])}`;
  const animKey = `${animTrigger ?? ''}|${dataSig}`;
  const [animProgress, setAnimProgress] = useState<number[]>(() =>
    new Array(periods.length).fill(0),
  );
  // Сброс прогресса СИНХРОННО при смене animKey (React-паттерн «adjust state
  // during render»). Без него один кадр бары рендерятся со старым animProgress:
  // при переключении на более длинный период правые бары (индексы за пределами
  // старого массива) попадают на `animProgress[i] ?? 1` = full — и видны на
  // полную высоту до того, как до них дойдёт wave.
  const [animatedKey, setAnimatedKey] = useState(animKey);
  if (animKey !== animatedKey) {
    setAnimatedKey(animKey);
    setAnimProgress(new Array(periods.length).fill(0));
  }
  // rafRef хранит ID текущего кадра СНАРУЖИ эффекта — нужен settleForCapture
  // (imperative handle ниже), чтобы отменить цикл и не дать следующему tick()
  // перезаписать принудительно выставленный progress=1 значением помельче.
  const rafRef = useRef(0);
  useEffect(() => {
    if (periods.length === 0) return;
    setAnimProgress(new Array(periods.length).fill(0));
    const start = performance.now();
    // Slower wave (match с Притоки/Оттоки feel):
    const totalStagger = Math.min(800, periods.length * 70);
    const perBarDuration = 900;
    const totalDuration = totalStagger + perBarDuration;
    const tick = (now: number) => {
      const elapsed = now - start;
      if (elapsed >= totalDuration) {
        setAnimProgress(new Array(periods.length).fill(1));
        rafRef.current = 0;
        return;
      }
      setAnimProgress(
        periods.map((_, i) => {
          const delay = periods.length > 1 ? (i / (periods.length - 1)) * totalStagger : 0;
          const localElapsed = Math.max(0, elapsed - delay);
          const t = Math.min(1, localElapsed / perBarDuration);
          return 1 - Math.pow(1 - t, 4);
        }),
      );
      rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafRef.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [animKey]);

  // Экспорт-хук: обрывает reveal-wave и досрочно доводит все бары до progress=1,
  // а заодно пере-читает РЕАЛЬНУЮ ширину контейнера в обход ResizeObserver —
  // после fullscreen-ресайза колбэк ResizeObserver может не успеть отработать
  // до html2canvas, и pad/шрифт/число X-меток посчитаются под старую (узкую)
  // ширину, а бары ещё не докрашены. Мгновенный jump не виден пользователю —
  // ExportModal вызывает это ДО показа canvas, живой экран не подглядывает.
  useImperativeHandle(ref, () => ({
    settleForCapture: () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      rafRef.current = 0;
      setAnimProgress(new Array(periods.length).fill(1));
      const el = containerRef.current;
      const w = el?.getBoundingClientRect().width;
      if (w && w > 0) setContainerW(Math.round(w));
    },
  }), [periods.length]);
  // Y-axis симметричный max
  const yMax = useMemo(() => {
    if (!periods.length) return niceMax ? niceMax(0) : 10;
    let maxAbs = 0;
    for (const p of periods) {
      let pos = 0, neg = 0;
      for (const cat of categories) {
        const v = p.values[cat] ?? 0;
        if (v >= 0) pos += v;
        else neg += -v;
      }
      maxAbs = Math.max(maxAbs, pos, neg);
    }
    // Линия-оверлей тоже должна помещаться в шкалу — иначе медиана уходит за
    // верхнюю грид-линию там, где она больше самого столбца.
    if (overlay) {
      for (const v of overlay.values) {
        if (v != null) maxAbs = Math.max(maxAbs, Math.abs(v));
      }
    }
    // Дефолт — округление до десятков с минимумом 10: зашито под МИЛЛИАРДЫ.
    return niceMax ? niceMax(maxAbs) : Math.max(10, Math.ceil((maxAbs * 1.12) / 10) * 10);
  }, [periods, categories, overlay, niceMax]);

  // 5 уровней Y-axis: [-max, -max/2, 0, max/2, max]
  const yTicks = useMemo(
    () => [-yMax, -yMax / 2, 0, yMax / 2, yMax],
    [yMax],
  );

  const legendItems = useMemo<ChartLegendItem[]>(
    () => legendOverride
      ?? categories.map((cat) => ({ color: getCategoryColor(cat, theme), label: getCategoryShortLabel(cat) })),
    [categories, theme, legendOverride],
  );

  // ─── Hover handler ───────────────────────────────────────────────────────
  // Bars лежат внутри chart-area, которая отступает от краёв container на
  // var(--chart-pad-left) и var(--chart-pad-right-single). Считаем xRatio
  // относительно chart-area, не container — иначе у краёв курсор активирует
  // wrong bar (раньше чем визуально подошёл).
  const handleMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    const target = e.currentTarget;
    const rect = target.getBoundingClientRect();
    const chartAreaWidth = rect.width - pad.left - pad.right;
    if (chartAreaWidth <= 0 || !periods.length) {
      setHover(null);
      return;
    }
    const xWithinChart = e.clientX - rect.left - pad.left;
    const xRatio = xWithinChart / chartAreaWidth;
    if (xRatio < 0 || xRatio > 1) {
      setHover(null);
      return;
    }
    const nearestIdx = Math.min(
      periods.length - 1,
      Math.max(0, Math.floor(xRatio * periods.length)),
    );
    const outer = containerRef.current;
    const outerRect = outer ? outer.getBoundingClientRect() : rect;
    setHover({
      periodIdx: nearestIdx,
      mouseX: e.clientX - outerRect.left,
      mouseY: e.clientY - outerRect.top,
    });
  }, [periods.length, pad.left, pad.right]);

  const handleLeave = useCallback(() => setHover(null), []);

  // ─── States ──────────────────────────────────────────────────────────────
  if (loading) {
    return (
      <div className="rounded-2xl" style={{ height: `${height}px`, background: 'var(--bg-secondary)', opacity: 0.6 }} />
    );
  }
  if (!periods.length) {
    return (
      <div className="flex items-center justify-center"
        style={{ height: `${height}px`, color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>
        Нет данных
      </div>
    );
  }

  const barSlot = 100 / periods.length;  // % одного period-слота
  const barW = barSlot * 0.65;            // 65% слота — сам бар
  const barOffset = (barSlot - barW) / 2;

  // Толщина окантовки (--bar-outline: светлая на тёмной теме, тёмная на светлой).
  // Считаем по РЕАЛЬНОЙ ширине бара в пикселях, а не по числу периодов: число
  // ничего не говорит о толщине столбца, пока не известна ширина поля. Из-за
  // этого у потоков фондов рамки фактически не было — дневной срез за год это
  // ~250 столбцов, порог «>100 → 0.25» срабатывал всегда, независимо от того,
  // насколько широкая панель. И наоборот: 30 столбцов в узкой панели песочницы
  // такие же тонкие, как 100 на полной странице.
  // Пороги подобраны так, чтобы рамка нигде не съедала больше ~8% ширины бара:
  // на 12px бар — 1px, на 7px — 0.7px и т.д. Иначе на средних ширинах столбец
  // читается как «сплошной контур», а не как заливка.
  const barPx = Math.max(0, ((containerW || vw) - pad.left - pad.right) * (barW / 100));
  const outlineWidth = barPx >= 12 ? 1 : barPx >= 7 ? 0.7 : barPx >= 3.5 ? 0.4 : 0.25;

  return (
    <div
      ref={containerRef}
      className="relative flex flex-col"
      style={{ height: `${height}px` }}
    >
      {/* ═══ Зона легенды — auto height, не сжимается при wrap (7 категорий
              могут перейти на 2 строки на узком viewport). flex-shrink: 0
              чтобы legend никогда не уходила за верхний край.
              Отступы — единые токены геометрии графиков: сверху
              --chart-legend-top-gap (зазор от верха paper-card), снизу
              --chart-legend-mb (зазор легенда → chart-area). ═══ */}
      <div
        style={{
          flexShrink: 0,
          paddingTop: 'var(--chart-legend-top-gap, 8px)',
          paddingBottom: 'var(--chart-legend-mb, 2px)',
        }}
      >
        <ChartLegend items={legendItems} fontSize={fontScale.legend} dotSize={fontScale.legendDot} />
      </div>

      {/* ═══ Контейнер chart — flex: 1 (берёт оставшуюся высоту) ═══ */}
      <div
        ref={chartWrapRef}
        className="relative"
        style={{ flex: 1, minHeight: 0 }}
        onMouseMove={handleMove}
        onMouseLeave={handleLeave}
      >
        {/* === Y-axis labels (HTML absolute справа) ===
            bottom такой же как chart-area — labels выравниваются с grid lines. */}
        <div
          className="absolute"
          style={{
            top: 'var(--chart-pad-top, 19px)',
            bottom: 'var(--chart-pad-bottom, 50px)',
            right: 0,
            width: `${pad.right}px`,
            pointerEvents: 'none',
          }}
        >
          {yTicks.map((v) => {
            const yPct = 50 - (v / yMax) * 50;  // 0% top → 100% bottom
            return (
              <div
                key={v}
                className="absolute font-bold"
                style={{
                  top: `${yPct}%`,
                  left: 'var(--sp-2)',
                  right: 0,
                  transform: 'translateY(-50%)',
                  fontSize: `${axisFontPx}px`,
                  color: 'var(--axis-color, var(--text-primary))',
                  fontVariantNumeric: 'tabular-nums',
                  whiteSpace: 'nowrap',
                }}
              >
                {axisOf(v)}
                {axisSuffix && (
                  <span className="font-bold" style={{ fontSize: '0.7em', opacity: 0.85, marginLeft: 3 }}>
                    {axisSuffix}
                  </span>
                )}
              </div>
            );
          })}
        </div>

        {/* === Chart-area (SVG inside absolute div) ===
            bottom = pad-bottom + (font-x + 8) — оставляем место для ДВУХ строк
            подписей (период + год) под chart-area. Иначе year обрезался за
            paper-card. */}
        <div
          className="absolute"
          style={{
            top: 'var(--chart-pad-top, 19px)',
            bottom: 'var(--chart-pad-bottom, 50px)',
            left: `${pad.left}px`,
            right: `${pad.right}px`,
          }}
        >
          <svg
            width="100%"
            height="100%"
            preserveAspectRatio="none"
            style={{ overflow: 'visible', display: 'block' }}
          >
            {/* Горизонтальные grid линии (solid, 5 уровней) */}
            {yTicks.map((v) => {
              const yPct = 50 - (v / yMax) * 50;
              const isZero = v === 0;
              return (
                <line
                  key={v}
                  x1="0" x2="100%"
                  y1={`${yPct}%`} y2={`${yPct}%`}
                  stroke={isZero ? 'var(--text-primary)' : 'var(--text-muted)'}
                  strokeWidth={isZero ? 1.5 : 1}
                  opacity={isZero ? 0.55 : 0.3}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })}

            {/* Bars с grow-from-zero wave animation:
                progress ∈ [0,1] — height и position стека масштабируются
                пропорционально. На progress=0 — bar высотой 0 у zero line.
                На progress=1 — полная высота. Stack maintained correctly во
                все промежуточные моменты (т.к. stackUp/Down тоже scale-ются). */}
            {periods.map((p, i) => {
              const isHovered = hover?.periodIdx === i;
              const hoverOpacity = hover && !isHovered ? 0.5 : 1;
              // progress: animProgress[i] либо 1 если animation завершена / state cleared
              const progress = animProgress[i] ?? 1;
              let stackUp = 0;
              let stackDown = 0;
              const x = i * barSlot + barOffset;
              return (
                <g
                  key={i}
                  opacity={hoverOpacity}
                  style={{ transition: 'opacity 150ms' }}
                >
                  {categories.map((cat) => {
                    const v = p.values[cat] ?? 0;
                    if (v === 0) return null;
                    // Final height в % (без scaling)
                    const hPctFinal = Math.max((Math.abs(v) / yMax) * 50, MIN_BAR_H);
                    // Scale by progress
                    const hPct = hPctFinal * progress;
                    if (v > 0) {
                      const stackUpPctFinal = (stackUp / yMax) * 50;
                      const stackUpPct = stackUpPctFinal * progress;
                      stackUp += v;
                      return (
                        <rect
                          key={`${i}-${cat}`}
                          x={`${x}%`}
                          y={`${50 - stackUpPct - hPct}%`}
                          width={`${barW}%`}
                          height={`${hPct}%`}
                          fill={barColor(cat, v)}
                          stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                          vectorEffect="non-scaling-stroke"
                        />
                      );
                    } else {
                      const stackDownPctFinal = (stackDown / yMax) * 50;
                      const stackDownPct = stackDownPctFinal * progress;
                      stackDown += -v;
                      return (
                        <rect
                          key={`${i}-${cat}`}
                          x={`${x}%`}
                          y={`${50 + stackDownPct}%`}
                          width={`${barW}%`}
                          height={`${hPct}%`}
                          fill={barColor(cat, v)}
                          stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                          vectorEffect="non-scaling-stroke"
                        />
                      );
                    }
                  })}
                </g>
              );
            })}

            {/* Линия-оверлей (медиана «Без выбросов» у сезонности).
                ⚠️ Отдельный вложенный <svg> с viewBox 0 0 100 100: у родителя
                viewBox'а нет, значит его пользовательские единицы — ПИКСЕЛИ, и
                polyline пришлось бы считать через реальный размер бокса (а он
                задан CSS-отступами и меняется с ресайзом). Внутри viewBox'а
                координаты нормированы 0..100, как проценты у баров.
                Растёт вместе с волной: точка i умножается на свой progress. */}
            {overlay && overlay.values.length > 0 && (
              <svg
                x="0" y="0" width="100%" height="100%"
                viewBox="0 0 100 100" preserveAspectRatio="none"
                style={{ overflow: 'visible' }}
              >
                <polyline
                  points={periods.map((_, i) => {
                    const v = overlay.values[i];
                    if (v == null) return '';
                    const prog = animProgress[i] ?? 1;
                    const x = i * barSlot + barSlot / 2;
                    const y = 50 - (v / yMax) * 50 * prog;
                    return `${x},${y}`;
                  }).filter(Boolean).join(' ')}
                  fill="none"
                  stroke={overlay.color}
                  strokeWidth={2}
                  strokeLinejoin="round"
                  strokeLinecap="round"
                  vectorEffect="non-scaling-stroke"
                  pointerEvents="none"
                />
              </svg>
            )}

            {/* Crosshair при hover */}
            {hover && periods[hover.periodIdx] && (
              <line
                x1={`${hover.periodIdx * barSlot + barSlot / 2}%`}
                x2={`${hover.periodIdx * barSlot + barSlot / 2}%`}
                y1="0" y2="100%"
                stroke="var(--text-primary)"
                strokeWidth={1}
                opacity={0.25}
                strokeDasharray="2 3"
                vectorEffect="non-scaling-stroke"
                pointerEvents="none"
              />
            )}
          </svg>
        </div>

        {/* === X-axis labels (DD.MM.YY format) — match с FlowsHistogram.
            bottom: var(--chart-xlabel-bottom) — same CSS-var как у Притоки/Оттоки.
            Gap до bottom grid line ~13px (vs прежние 4px) — labels чуть ниже. */}
        <div
          className={`absolute font-semibold${dateless ? '' : ' flex justify-between px-2'}`}
          style={{
            left: `${pad.left}px`,
            right: `${pad.right}px`,
            bottom: 'var(--chart-xlabel-bottom, 20px)',
            fontSize: `${axisFontPx}px`,
            color: 'var(--axis-color, #9CA3B8)',
            fontVariantNumeric: 'tabular-nums',
            pointerEvents: 'none',
          }}
        >
          {/* dateless — подпись ПОД СВОИМ столбцом, по центру слота.
              Даты можно раскидывать justify-between (важен ход времени, а не
              привязка к конкретному бару), но срез читается только вместе со
              своим столбцом: «какой месяц красный» по разъехавшимся подписям
              не понять. Прореживаем, только если подписи физически не влезают. */}
          {dateless && (() => {
            const chartW = Math.max(1, (containerW || vw) - pad.left - pad.right);
            const slotPx = chartW / periods.length;
            const maxLen = periods.reduce((m, p) => Math.max(m, p.label.length), 1);
            // ~0.62em на символ у полужирного шрифта + 6px воздуха по бокам.
            const needPx = maxLen * axisFontPx * 0.62 + 6;
            const step = Math.max(1, Math.ceil(needPx / slotPx));
            return periods.map((p, i) => (
              i % step === 0 ? (
                <span
                  key={`xlab-${i}`}
                  style={{
                    position: 'absolute',
                    left: `${i * barSlot + barSlot / 2}%`,
                    transform: 'translateX(-50%)',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {p.label}
                </span>
              ) : null
            ));
          })()}
          {!dateless && (() => {
            // Число X-меток по РЕАЛЬНОЙ ширине контейнера, не isMobile (viewport) —
            // на узкой панели песочницы (containerW ~300-500) 6 дат друг на друга
            // налезают; используем vw-fallback пока containerW не измерен.
            const cw = containerW || vw;
            const maxTicks = cw < 420 ? 3 : cw < 640 ? 4 : cw < 900 ? 5 : 6;
            const tickCount = Math.min(maxTicks, periods.length);
            const labelOf = (p: CbrFlowsPeriod) =>
              new Date(p.end_date).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
            if (tickCount < 2) {
              // Edge case: 1 period — single centered label
              return <span style={{ margin: '0 auto' }}>{labelOf(periods[0])}</span>;
            }
            return Array.from({ length: tickCount }, (_, i) => {
              const idx = Math.min(
                Math.round((i * (periods.length - 1)) / (tickCount - 1)),
                periods.length - 1,
              );
              return <span key={`xlab-${i}`}>{labelOf(periods[idx])}</span>;
            });
          })()}
        </div>


        {/* Вотермарк намеренно убран (Вадим) — на плотном stacked-графике с
            категориями у нулевой линии он перекрывал данные; на остальных
            графиках сайта ChartWatermark остаётся штатно. */}

        {/* Пилс значения на ЦЕНОВОЙ ШКАЛЕ — сумма столбца под курсором.
            Горизонтальной линии намеренно НЕТ: столбец и так подсвечен, а линия
            через весь график в плитке периодов только мешает. Пилс закрывает
            дыру «видно форму, не видно чисел»: курсорный тултип в песочнице
            скрыт по правилу .sb-panel .chart-tooltip-root. */}
        {valuePill && hover && periods[hover.periodIdx] && (() => {
          const p = periods[hover.periodIdx];
          const total = categories.reduce((acc, c) => acc + (p.values[c] ?? 0), 0);
          if (!Number.isFinite(total)) return null;
          const cs = getComputedStyle(document.documentElement);
          const padTop = parseFloat(cs.getPropertyValue('--chart-pad-top')) || 14;
          const h = (containerRef.current?.clientHeight ?? height) - padTop;
          // Та же геометрия, что у столбцов: ноль по центру поля, полуразмах = yMax.
          const y = padTop + h / 2 - (total / yMax) * (h / 2);
          const up = total >= 0;
          return (
            <div
              data-export-ignore="true"
              style={{
                // ⚠️ Прижимаем к КРОМКЕ ПОЛЯ (ширина оси = pad.right), иначе пилс
                // уезжает за цифры шкалы и висит у самого края панели.
                position: 'absolute', right: pad.right, top: Math.max(padTop, Math.min(padTop + h, y)),
                transform: 'translateY(-50%)', zIndex: 6, pointerEvents: 'none',
                padding: '1px 6px', borderRadius: 4, whiteSpace: 'nowrap',
                fontSize: 11, fontWeight: 700, fontVariantNumeric: 'tabular-nums',
                background: up ? 'var(--oi-green)' : 'var(--oi-red)', color: '#fff',
              }}
            >
              {fmtValue
                ? fmtValue(total)
                : `${up ? '+' : '−'}${Math.abs(total).toFixed(Math.abs(total) >= 10 ? 0 : 1)}`}
            </div>
          );
        })()}

        {/* Плавающая дата над графиком — единый стиль/позиция со всеми чартами
            (ChartDatePill: прозрачный текст, низ прижат к верхней грид-линии,
            кламп в границах chart-area чтобы не наезжать на Y-шкалу). */}
        {hover && periods[hover.periodIdx] && (() => {
          const cs = getComputedStyle(document.documentElement);
          const padTop = parseFloat(cs.getPropertyValue('--chart-pad-top')) || 14;
          const w = containerRef.current?.clientWidth ?? 800;
          const slotW = (w - pad.left - pad.right) / periods.length;
          const p = periods[hover.periodIdx];
          return (
            <ChartDatePill
              date={dateless ? p.label : `${p.label} ${p.year}`}
              x={pad.left + (hover.periodIdx + 0.5) * slotW}
              topLineY={padTop}
              minX={pad.left}
              maxX={w - pad.right}
            />
          );
        })()}
      </div>

      {/* === Tooltip === */}
      {hover && periods[hover.periodIdx] && (() => {
        const p = periods[hover.periodIdx];
        const entries = categories
          .map((cat) => ({ cat, val: p.values[cat] ?? 0 }))
          .filter(e => e.val !== 0);
        const sortedPositive = entries.filter(e => e.val > 0).sort((a, b) => b.val - a.val);
        const sortedNegative = entries.filter(e => e.val < 0).sort((a, b) => a.val - b.val);
        const containerW = containerRef.current?.clientWidth ?? 800;
        // Adaptive tooltip width: 260px default, но не больше container-32
        // (gap 16px по бокам). На mobile 360-375px tooltip станет ~330-340px.
        const tooltipW = Math.min(260, Math.max(180, containerW - 32));
        // Переключаем сторону с СЕРЕДИНЫ chart-area.
        const placeLeft = hover.mouseX > containerW / 2;
        const left = placeLeft ? hover.mouseX - tooltipW - 16 : hover.mouseX + 16;
        // Tooltip clamp: карточка в plot-коридоре (между крайними грид-линиями
        // chart-области, с запасом 6px) — не накрывает легенду и дату-пилюлю
        // сверху, не заезжает на подписи оси снизу. Координаты hover.mouseY —
        // от OUTER контейнера (включая зону легенды), поэтому границы коридора
        // считаем через offsetTop chart-контейнера.
        const tooltipMaxH = isMobile ? 180 : 240;
        const csDoc = getComputedStyle(document.documentElement);
        const padTopPx = parseFloat(csDoc.getPropertyValue('--chart-pad-top')) || 14;
        const padBottomPx = parseFloat(csDoc.getPropertyValue('--chart-pad-bottom')) || 50;
        const wrapTop = chartWrapRef.current?.offsetTop ?? 40;
        const wrapH = chartWrapRef.current?.clientHeight ?? (height - wrapTop);
        const minTop = wrapTop + padTopPx + 6;
        const maxTop = wrapTop + wrapH - padBottomPx - 6 - tooltipMaxH;
        const top = Math.max(minTop, Math.min(hover.mouseY - 20, maxTop));

        return (
          <div
            data-export-ignore="true"
            className={`${TOOLTIP.containerClass} absolute z-20${tooltipInSandbox ? '' : ' chart-tooltip-root'}`}
            style={{
              ...TOOLTIP.containerStyle,
              // Подложка — «приподнятая поверхность», а не --bg-primary из общего
              // TOOLTIP: тот на тёмной теме почти чёрный и карточка читается как
              // дыра в графике. --bg-elevated светлее фона на тёмной теме и
              // светлее бумаги на светлой — тултип везде выглядит как слой НАД
              // полотном, а не как вырез в нём. Рамка — от text-primary с малой
              // альфой вместо сплошного --border-color (в editorial-dark он
              // кремово-белый и бил в глаза сильнее самих данных).
              background: 'var(--bg-elevated)',
              border: '1px solid color-mix(in srgb, var(--text-primary) 16%, transparent)',
              left: `${left}px`,
              top: `${top}px`,
              width: `${tooltipW}px`,
              pointerEvents: 'none',
              whiteSpace: 'normal',
            }}
          >
            {sortedPositive.map((e) => (
              <div key={e.cat} className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: barColor(e.cat, e.val) }} />
                  <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{e.cat}</span>
                </div>
                <span className={TOOLTIP.valueClass} style={{ ...TOOLTIP.valueStyle, color: 'var(--funds-flow-positive)' }}>
                  {fmtValue ? fmtValue(e.val) : `+${e.val.toFixed(2)}`}
                </span>
              </div>
            ))}
            {sortedPositive.length > 0 && sortedNegative.length > 0 && (
              <div style={{ height: 1, background: 'var(--text-muted)', opacity: 0.25, margin: '4px 0' }} />
            )}
            {sortedNegative.map((e) => (
              <div key={e.cat} className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: barColor(e.cat, e.val) }} />
                  <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{e.cat}</span>
                </div>
                <span className={TOOLTIP.valueClass} style={{ ...TOOLTIP.valueStyle, color: 'var(--funds-flow-negative)' }}>
                  {valueOf(e.val)}
                </span>
              </div>
            ))}
            {/* Линия-оверлей отдельной строкой — её значения в стек не входят,
                но в тултипе нужны (сезонность: медиана «Без выбросов»). */}
            {overlay && overlay.values[hover.periodIdx] != null && (
              <div className="flex items-center justify-between py-0.5" style={{ gap: 'var(--sp-2)' }}>
                <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-1)' }}>
                  <span className={TOOLTIP.dotClass} style={{ ...TOOLTIP.dotStyle, backgroundColor: overlay.color }} />
                  <span className={`${TOOLTIP.labelClass} truncate`} style={TOOLTIP.labelStyle}>{overlay.label}</span>
                </div>
                <span className={TOOLTIP.valueClass} style={TOOLTIP.valueStyle}>
                  {valueOf(overlay.values[hover.periodIdx] as number)}
                </span>
              </div>
            )}
          </div>
        );
      })()}
    </div>
  );
});

export default StackedBidirectionalHistogram;
