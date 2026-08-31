import { useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { SeasonalityResponse } from '../../services/api';
import { CHART_COLORS, CROSSHAIR, TOOLTIP, ANIMATION, cssVar } from '../../config/chartTheme';
import { resampleVals } from '../../utils/chartAnimation';
import { useNoChartAnim } from '../chart/chartAnim';
import { ChartGrid, ChartCrosshair, ChartTooltip, TooltipRow } from '../chart';
import ChartLegend from '../chart/ChartLegend';
import ChartWatermark from '../ChartWatermark';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { axisFontSize, xAxisTickCount } from '../chart/chartTypography';

interface TooltipState {
  x: number;
  y: number;
  bar?: SeasonalityResponse['bars'][0];
}

interface SeriesMeta {
  key: string;
  label: string;
  color: string;
}

interface SeasonalityHistogramProps {
  bars: SeasonalityResponse['bars'];
  /** Legacy prop (не используется — анимацию ведёт внутренний rAF-морф) */
  animatedHeights?: number[];
  maxAbs: number;
  tooltip: TooltipState | null;
  setTooltip: (t: TooltipState | null) => void;
  /** Мульти-серии (2..3). null = одиночный бар. */
  monthlySeries?: SeasonalityResponse[] | null;
  /** Метаданные серий для мульти-режима (label + color). */
  seriesMeta?: SeriesMeta[];
  /** Компактный режим (test-dashboard): прореживание X-меток даже на desktop. */
  compact?: boolean;
  /** Описание выбранного периода для single-mode легенды (например «С 2008 г.»).
   *  В multi-mode игнорируется (там периоды видны как метки серий seriesMeta). */
  periodLabel?: string;
  /** Числовая ось X (дни месяца): подписи по КРУГЛЫМ значениям (5,10,15…/10,20,30)
   *  + первый день, а не по равномерному индексу (давал 1,6,11,16…). Включать
   *  ТОЛЬКО для mode='monthday' — у остальных режимов метки нечисловые (часы/дни
   *  недели/месяцы), там дефолтная индекс-логика. */
  niceXLabels?: boolean;
}

// Параметры волны/морфа из единого конфига — совпадают с FlowsHistogram.
const easeOutCubic = ANIMATION.easing;

export default function SeasonalityHistogram({
  bars,
  maxAbs,
  tooltip,
  setTooltip,
  monthlySeries,
  seriesMeta,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  compact: _compact = false,
  periodLabel,
  niceXLabels = false,
}: SeasonalityHistogramProps) {
  const vw = useViewportWidth();
  const axisFs = axisFontSize(vw);

  // ── Анимация баров — эталонная схема «Деньги в фондах» (FundsMoneyPage). ──
  // Волна с нуля (каскад слева направо) один раз — на первом рендере с
  // данными. Дальнейшие смены (режим месяцы/дни/часы, период, серии) морфят
  // из текущих отображаемых значений: ресемпл к новой длине числа баров.
  // Морф идёт в НОРМАЛИЗОВАННОМ пространстве (val / effectiveMaxAbs ∈ [-1..1]),
  // т.е. в визуальном — смена шкалы не дёргает бары. Серии спариваются по
  // ключам seriesMeta: новая серия растёт с нуля, исчезнувшая пропадает.
  // useLayoutEffect + синхронный стартовый кадр — без вспышки «25-го кадра».
  const targetKeys = useMemo<string[]>(() => {
    const count = monthlySeries && seriesMeta
      ? Math.min(monthlySeries.length, seriesMeta.length)
      : 0;
    if (count >= 2) return seriesMeta!.slice(0, count).map(m => m.key);
    return ['single'];
  }, [monthlySeries, seriesMeta]);
  const targetSeries = useMemo<number[][]>(() => {
    const count = monthlySeries && seriesMeta
      ? Math.min(monthlySeries.length, seriesMeta.length)
      : 0;
    if (count >= 2) {
      const series = monthlySeries!.slice(0, count);
      const em = Math.max(
        ...series.flatMap(s => s.bars.map(b => Math.abs(b.avg_change))),
        0.01,
      );
      return series.map(s => bars.map((_, i) => (s.bars[i]?.avg_change ?? 0) / em));
    }
    const em = maxAbs || 0.01;
    return [bars.map(b => b.avg_change / em)];
  }, [bars, monthlySeries, seriesMeta, maxAbs]);
  const [dispSeries, setDispSeries] = useState<number[][]>([]);
  const dispRef = useRef<number[][]>([]);
  const dispKeysRef = useRef<string[]>([]);
  const wavedRef = useRef(false);
  const rafRef = useRef<number | null>(null);
  // Песочница: волна появления выключена (см. chart/chartAnim.ts).
  const noAnim = useNoChartAnim();
  useLayoutEffect(() => {
    if (bars.length === 0) {
      dispRef.current = [];
      dispKeysRef.current = [];
      setDispSeries([]);
      return;
    }
    if (rafRef.current) cancelAnimationFrame(rafRef.current);
    const targets = targetSeries;
    const wave = !wavedRef.current || dispRef.current.length === 0;
    wavedRef.current = true;
    if (wave && noAnim) {
      dispRef.current = targets;
      dispKeysRef.current = targetKeys;
      setDispSeries(targets);
      return;
    }
    const from = targets.map((t, s) => {
      if (wave) return new Array<number>(t.length).fill(0);
      const oldIdx = dispKeysRef.current.indexOf(targetKeys[s]);
      if (oldIdx === -1) return new Array<number>(t.length).fill(0);
      return resampleVals(dispRef.current[oldIdx], t.length);
    });
    // Стартовый кадр — до первого paint, чтобы не мигнуть старыми барами.
    dispRef.current = from;
    dispKeysRef.current = targetKeys;
    setDispSeries(from);
    const totalDuration = wave ? ANIMATION.waveDuration : ANIMATION.morphDuration;
    const staggerDelay = wave ? ANIMATION.waveStagger : 0;
    const n = targets[0]?.length ?? 0;
    let start: number | null = null;
    const tick = (ts: number) => {
      if (start == null) start = ts;
      const elapsed = ts - start;
      const next = targets.map((t, s) => t.map((v, i) => {
        const barDelay = n > 0 ? (i / n) * staggerDelay : 0;
        const barElapsed = Math.max(0, elapsed - barDelay);
        const tt = Math.min(barElapsed / (totalDuration - staggerDelay), 1);
        return from[s][i] + (v - from[s][i]) * easeOutCubic(tt);
      }));
      dispRef.current = next;
      setDispSeries(next);
      if (elapsed < totalDuration) rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, [targetSeries, targetKeys, bars.length, noAnim]);

  // Кэш горизонтального padding для onMouseMove — читаем CSS-токен один раз
  // на маунт и при resize, а не на каждое движение мыши (getComputedStyle
  // внутри hot-path триггерит style recalculation в браузере).
  const padXRef = useRef(70);
  useLayoutEffect(() => {
    const read = () => {
      const v = parseFloat(
        getComputedStyle(document.documentElement).getPropertyValue('--seasonality-hist-pad-x')
      );
      if (!Number.isNaN(v) && v > 0) padXRef.current = v;
    };
    read();
    window.addEventListener('resize', read);
    return () => window.removeEventListener('resize', read);
  }, []);

  if (bars.length === 0) {
    return (
      <div className="flex items-center justify-center" style={{ aspectRatio: '16/9', color: 'var(--text-muted)' }}>Нет данных</div>
    );
  }

  // Защита от race condition: seriesMeta и monthlySeries могут в кадре
  // рассинхронизироваться по длине — берём минимум.
  const safeCount = monthlySeries && seriesMeta
    ? Math.min(monthlySeries.length, seriesMeta.length)
    : 0;
  const isMulti = safeCount >= 2;
  const nSeries = isMulti ? safeCount : 1;
  const safeMeta = isMulti ? seriesMeta!.slice(0, safeCount) : [];
  const safeSeries = isMulti ? monthlySeries!.slice(0, safeCount) : [];

  const effectiveMaxAbs = isMulti
    ? Math.max(
        ...safeSeries.flatMap(s => s.bars.map(b => Math.abs(b.avg_change))),
        0.01,
      )
    : maxAbs;

  // Легенда — только периоды/года (название актива убрано: оно есть в
  // заголовке карточки и дублирование выглядело перегружено).
  //   multi-mode → seriesMeta (у каждой серии свой цвет-кружок)
  //   single-mode → один период (цвет positive — базовый цвет гистограммы)
  const periodItems: { color: string; label: string }[] = isMulti
    ? safeMeta.map(s => ({ color: s.color, label: s.label }))
    : (periodLabel ? [{ color: CHART_COLORS.positive, label: periodLabel }] : []);

  // Легенда снова однострочная → стандартный верхний padding.
  const padTop = 'var(--seasonality-hist-pad-top, 28px)';

  const W = 1000, H = 500;
  const midY = H / 2;
  // halfH = H*0.47 — match FlowsHistogram (там halfH=47%, midY=50%).
  // 3% inset сверху и снизу: gridlines от 3% до 97% SVG.
  // Раньше было H*0.38 (12% inset) — gap до X-labels был ~33px (vs Flows 16px).
  // Сейчас H*0.47 (3% inset) — gap matches Flows.
  const halfH = H * 0.47;

  // Unified pointer handler — mouse + touch одинаково.
  const handlePointerMove = (clientX: number, clientY: number, el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    const x = clientX - rect.left;
    const PAD = padXRef.current;
    const barAreaWidth = rect.width - 2 * PAD;
    const xInBars = x - PAD;
    if (xInBars < 0 || xInBars > barAreaWidth) { setTooltip(null); return; }
    const slotW_px = barAreaWidth / bars.length;
    const idx = Math.floor(xInBars / slotW_px);
    if (idx >= 0 && idx < bars.length) {
      const slotCenterPx = PAD + idx * slotW_px + slotW_px / 2;
      setTooltip({ x: slotCenterPx, y: clientY - rect.top, bar: bars[idx] });
    } else {
      setTooltip(null);
    }
  };

  return (
    <div
      // Без chart-reveal на обёртке: бары и так растут «волной» слева направо
      // (dispSeries + rAF), а обёртка уводила вместе с ними оси и
      // подписи — обрамление видно с первого кадра.
      className="relative overflow-hidden pb-2 cursor-crosshair"
      style={{
        aspectRatio: 'var(--seasonality-aspect-ratio, 2.4)',
        minHeight: 'var(--seasonality-hist-min-height, 240px)',
        maxHeight: 'var(--seasonality-hist-max-height, 450px)',
        // КРИТИЧНО: max-width: 100% защищает от расширения за parent.
        // aspect-ratio + min-height по CSS-спеке IGNORE родительские
        // constraints — на mobile (375px viewport) chart раздувался
        // до 432px (=180×2.4) и выезжал за card на 122px вправо.
        // С max-width: 100% chart shrinks по ширине, height clamps к min-height.
        maxWidth: '100%',
        // margin auto центрирует block-элемент когда aspect-ratio + maxHeight
        // ограничивают ширину меньше parent (на широких desktop'ах: при
        // maxHeight≈500 ширина ≈1200, а контейнер ≈1274 — без auto-margin
        // график прижимался к левому краю).
        marginLeft: 'auto',
        marginRight: 'auto',
        // touchAction: none — нужно для mobile чтобы водение пальцем по чарту
        // не триггерило scroll страницы.
        touchAction: 'none',
      }}
      onMouseMove={(e) => handlePointerMove(e.clientX, e.clientY, e.currentTarget)}
      onMouseLeave={() => setTooltip(null)}
      onTouchStart={(e) => {
        if (e.touches[0]) handlePointerMove(e.touches[0].clientX, e.touches[0].clientY, e.currentTarget);
      }}
      onTouchMove={(e) => {
        if (e.touches[0]) handlePointerMove(e.touches[0].clientX, e.touches[0].clientY, e.currentTarget);
      }}
      onTouchEnd={() => setTooltip(null)}
    >
      {/* Легенда: в single-mode — один кружок + название актива (символьно
          совпадает по структуре с multi-mode, где у каждой серии свой кружок).
          Размер шрифта берётся ChartLegend'ом из useViewportWidth + legendFontSize
          (breakpoint-aware: 11→12→13→15→17→19px), dominant-baseline=central даёт
          pixel-perfect центрирование dot↔text без CSS hacks.
          Раньше показывали bold-span с тикером слева + ChartLegend «Рост/Падение»
          справа — но это дублировало информацию: цвет баров уже несёт смысл
          (зелёные ≥ 0, красные < 0), а тикер дублировался с заголовком карточки.
          Теперь весь identifier графика — один flex-item (кружок + label). */}
      {/* Легенда — периоды/года, каждый со своим цветом-кружком.
          top-0: карточка сверху уже даёт 8px (md:pt-2 / p-2) — итоговый зазор
          от верхней границы = --chart-legend-top-gap, как в OI. */}
      {periodItems.length > 0 && (
        <div className="absolute top-0 left-1/2 -translate-x-1/2 z-20 pointer-events-none">
          <ChartLegend
            items={periodItems}
            fontWeight={600}
            gap={16}
            style={{ color: 'var(--text-primary)' }}
          />
        </div>
      )}

      <div className="absolute" style={{
        top: padTop,
        bottom: 'var(--seasonality-hist-pad-bottom, 24px)',
        left: 'var(--seasonality-hist-pad-x, 70px)',
        right: 'var(--seasonality-hist-pad-x, 70px)',
      }}>
        <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
          {/* Высоты баров — из dispSeries (анимированные нормализованные
              значения, rAF ведёт волну/морф — см. эффект выше).
              Outline толщина плавно уменьшается с ростом count — раньше при
              bars > 20 отключали совсем (тонкий бар становился чёрным), теперь
              0.25-1px gradient даёт hint контура без visual dominance. */}
          {bars.map((bar, i) => {
            const outlineWidth = bars.length <= 20 ? 1
                              : bars.length <= 50 ? 0.7
                              : bars.length <= 100 ? 0.4
                              : 0.25;
            const slotW = W / bars.length;
            const slotPadding = slotW * 0.1;
            const groupW = slotW - slotPadding * 2;
            const subBarW = isMulti ? groupW / nSeries : slotW * (bars.length > 12 ? 0.6 : 0.5);

            return (
              <g key={bar.key}
                opacity={tooltip?.bar ? (tooltip.bar.key === bar.key ? 1 : 0.5) : 1}
                className="transition-opacity duration-150">
                {isMulti ? (
                  safeMeta.map((style, s) => {
                    const seriesBar = safeSeries[s]?.bars?.[i];
                    if (!seriesBar) return null;
                    const nv = dispSeries[s]?.[i] ?? 0;
                    const h = Math.max(Math.abs(nv) * halfH, H * 0.005);
                    const bx = i * slotW + slotPadding + s * subBarW;
                    const y = nv >= 0 ? midY - h : midY;
                    return (
                      <rect
                        key={style.key}
                        x={bx + 1} y={y}
                        width={subBarW - 2} height={h}
                        fill={style.color} rx="2"
                        stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                        vectorEffect="non-scaling-stroke"
                      />
                    );
                  })
                ) : (() => {
                  const nv = dispSeries[0]?.[i] ?? 0;
                  const h = Math.max(Math.abs(nv) * halfH, H * 0.005);
                  const bx = i * slotW + (slotW - subBarW) / 2;
                  const y = nv >= 0 ? midY - h : midY;
                  const color = nv >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
                  return (
                    <rect
                      key="single"
                      x={bx} y={y}
                      width={subBarW} height={h}
                      fill={color} rx="3"
                      stroke="var(--bar-outline)" strokeWidth={outlineWidth}
                      vectorEffect="non-scaling-stroke"
                    />
                  );
                })()}
              </g>
            );
          })}

          <ChartGrid
            yTicks={[-effectiveMaxAbs, -effectiveMaxAbs / 2, effectiveMaxAbs / 2, effectiveMaxAbs].map(val => ({
              // pct формула обновлена: 190 → halfH (250). Gridlines теперь на 0%, 25%, 50%, 75%, 100%
              // SVG height вместо 12%, 31%, 50%, 69%, 88%.
              pct: (midY - (val / effectiveMaxAbs) * halfH) / H * 100,
            }))}
            zeroPct={(midY / H) * 100}
          />

          {tooltip?.bar && (() => {
            const idx = bars.indexOf(tooltip.bar!);
            if (idx === -1) return null;
            const slotW = 1000 / bars.length;
            const cx = idx * slotW + slotW / 2;
            // y1/y2 = 0..H (full SVG height) — раньше 60..440 с 12% inset
            return (
              <ChartCrosshair x={cx} color={CROSSHAIR.accentColor}
                dashArray={CROSSHAIR.accentDashArray} opacity={CROSSHAIR.accentOpacity}
                y1={midY - halfH} y2={midY + halfH} />
            );
          })()}
        </svg>
        {/* Watermark — внутри bar-area div (он absolute → containing block для absolute потомков).
            Gridlines/bars идут от 12% до 88% высоты SVG (halfH=H*0.38, запас 12% по краям).
            Позиция:
              left = 20px от левого края bar-area (== 20px после Y-axis labels)
              bottom = 12% от высоты bar-area (доходит до нижней gridline) + 5px зазор
            Чисто CSS, адаптивно к любому размеру (aspectRatio + media query). */}
        {/* bottom=calc(3%+5px): 3% — внутренний inset gridlines (halfH=0.47*H,
            match Flows), 5px — зазор над bottom gridline. Watermark сидит над
            нижней gridline на 5px, как и было задумано в editorial-стиле. */}
        <ChartWatermark left={5} bottom="calc(3% + 5px)" />
      </div>

      {/* Tooltip — используем ChartTooltip (автоматический flip по центру) */}
      {tooltip?.bar && (() => {
        const idx = bars.indexOf(tooltip.bar!);
        if (idx === -1) return null;
        return (
          <ChartTooltip x={tooltip.x} y={tooltip.y} clampTop={cssVar('--seasonality-hist-pad-top', 28)} clampBottom={cssVar('--seasonality-hist-pad-bottom', 24)}>
            {isMulti ? (
              <>
                {/* Label header — bigger/bolder для лучшей читаемости. Особенно
                    важно в monthday/monthly modes где X-axis labels прорежены. */}
                <div className="text-sm text-theme-primary mb-1 font-bold">{tooltip.bar!.label}</div>
                {safeMeta.map((style, s) => {
                  const seriesBar = safeSeries[s]?.bars?.[idx];
                  if (!seriesBar) return null;
                  const val = seriesBar.avg_change;
                  const valColor = val >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
                  const valStr = `${val > 0 ? '+' : ''}${val.toFixed(2)}%`;
                  // Точка = цвет серии, значение = зелёный/красный от знака
                  return (
                    <div key={style.key} className="flex items-center justify-between gap-3">
                      <div className="flex items-center gap-1.5 min-w-0">
                        <span className={`${TOOLTIP.dotSize} flex-shrink-0`} style={{ backgroundColor: style.color }} />
                        <span className={`${TOOLTIP.labelClass} truncate`}>{style.label}</span>
                      </div>
                      <span className={`${TOOLTIP.valueClass} whitespace-nowrap`} style={{ color: valColor }}>
                        {valStr}
                      </span>
                    </div>
                  );
                })}
              </>
            ) : (() => {
              const bar = tooltip.bar!;
              const color = bar.avg_change >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
              const valStr = `${bar.avg_change > 0 ? '+' : ''}${Math.abs(bar.avg_change) >= 0.01 ? bar.avg_change.toFixed(3) : bar.avg_change.toFixed(4)}%`;
              return (
                <>
                  {/* Label header (день/месяц/час) — prominent даже когда X-axis
                      label прорежены adaptive thinning'ом. */}
                  <div className="text-sm text-theme-primary mb-1 font-bold">{bar.label}</div>
                  <TooltipRow color={color} label={bar.avg_change >= 0 ? 'Рост' : 'Падение'} value={valStr} />
                  <div className="text-2xs text-theme-secondary mt-0.5">{bar.count} наблюдений</div>
                </>
              );
            })()}
          </ChartTooltip>
        );
      })()}

      {/* Y labels — синхронно с bar-area по высоте (тот же padTop) */}
      <div className="absolute pointer-events-none" style={{
        top: padTop,
        bottom: 'var(--seasonality-hist-pad-bottom, 24px)',
        right: 0,
        width: 'var(--seasonality-hist-pad-x, 70px)',
      }}>
        {[-effectiveMaxAbs, -effectiveMaxAbs / 2, 0, effectiveMaxAbs / 2, effectiveMaxAbs].map((val, i) => {
          // 38 → 50 (halfH/H * 100): Y-labels теперь на 0%, 25%, 50%, 75%, 100% SVG.
          // Раньше формула давала 12%, 31%, 50%, 69%, 88% — соответствовало старым gridlines.
          const yPct = 50 - (val / effectiveMaxAbs) * 50;
          const label = val === 0 ? '0' : `${val > 0 ? '+' : ''}${val.toFixed(2)}%`;
          return (
            <div key={i} className="absolute"
              style={{
                top: `${yPct}%`,
                // right 4px mobile / 12px desktop — больше зазор на desktop для эстетики,
                // меньше на mobile чтобы label'у "+0.02%" хватало места
                right: 'var(--seasonality-ylabel-right, 12px)',
                transform: 'translateY(-50%)',
              }}>
              {/* background + padding создают "halo" цветом фона карточки —
                  перекрывает горизонтальную grid-линию в зоне, где text заходит на
                  bar-area (~2px на desktop). Стандартный приём для axis labels,
                  аналог `paint-order: stroke fill` в SVG (Strength/SimpleChart). */}
              <span className="font-bold" style={{
                  fontSize: 'var(--chart-font-y, 16px)',
                  color: 'var(--axis-color, #9CA3B8)',
                  backgroundColor: 'var(--bg-primary)',
                  padding: '1px 4px',
                  borderRadius: 2,
              }}>{label}</span>
            </div>
          );
        })}
      </div>

      {/* X labels — bottom: var(--chart-xlabel-bottom) (20px) — same offset как
          в FlowsHistogram. Adaptive thinning по chart width и font size:
          считаем сколько labels влезает = chartWidth / labelWidth. step = ceil(N/fitting).
          Mobile + desktop работают по одной формуле. */}
      <div className="absolute flex justify-between font-bold px-2" style={{ bottom: 'var(--chart-xlabel-bottom, 20px)', left: 'var(--seasonality-hist-pad-x, 70px)', right: 'var(--seasonality-hist-pad-x, 70px)', fontSize: 'var(--chart-font-x, 14px)', color: 'var(--axis-color, #9CA3B8)' }}>
        {bars.map((bar, i) => {
          // Adaptive thinning: barAreaWidth ≈ vw × 0.85 - 200 (rough estimate
          // — page padding + seasonality-hist-pad-x). Сколько labels влезает =
          // chartWidth/labelWidth (короткие "31" или "янв" → ~4 chars).
          const estBarArea = Math.max(200, vw * 0.85 - 200);
          const fittingCount = xAxisTickCount(estBarArea, axisFs, 4);
          const step = Math.max(1, Math.ceil(bars.length / fittingCount));
          // Раньше был "|| i === bars.length - 1" force-last-bar — но при
          // step=2 на 12 месяцах это давало Nov(10) AND Dec(11) рядом
          // (overlap). Теперь только step-aligned, без force.
          //
          // niceXLabels (mode='monthday'): подписи по КРУГЛЫМ значениям дня —
          // step снапается к ближайшему «приятному» 5/10 (узкий экран → 10), и
          // метка показывается, когда сам день кратен niceStep (5,10,15… либо
          // 10,20,30) + первый день. Даёт 1,5,10,15,20,25,30 вместо 1,6,11,16…
          // Дефолт (все прочие оси: даты/часы/месяцы) — без изменений.
          const niceStep = step <= 5 ? 5 : 10;
          const showLabel = niceXLabels
            ? (i === 0 || Number(bar.label) % niceStep === 0)
            : (step === 1 || i % step === 0);
          return (
            <span key={bar.key} className="text-center" style={{ width: `${100 / bars.length}%` }}>
              {showLabel ? bar.label : ''}
            </span>
          );
        })}
      </div>

      {/* Watermark перенесён внутрь bar-area div выше (для точной привязки
          к gridlines на 12-88% SVG). Здесь больше не рендерим. */}
    </div>
  );
}
