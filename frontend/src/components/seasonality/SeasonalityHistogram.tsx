import { useLayoutEffect, useRef, useState } from 'react';
import type { SeasonalityResponse } from '../../services/api';
import { CHART_COLORS, CROSSHAIR, TOOLTIP, ANIMATION } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair, ChartTooltip, TooltipRow } from '../chart';
import ChartLegend from '../chart/ChartLegend';
import ChartWatermark from '../ChartWatermark';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { axisFontSize, xAxisTickCount, legendFontSize } from '../chart/chartTypography';

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
  /** Legacy prop (не используется — анимация переехала на CSS transition) */
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
  /** Название выбранного актива (например «IMOEX», «SBER»). Рендерится bold перед legend. */
  assetLabel?: string;
}

// Параметры волны из единого конфига — совпадают с FlowsHistogram.
// Per-bar длительность = waveDuration − waveStagger.
const ANIM_DURATION_MS = ANIMATION.waveDuration - ANIMATION.waveStagger;
const STAGGER_TOTAL_MS = ANIMATION.waveStagger;
const ANIM_EASING = ANIMATION.waveEasing;

export default function SeasonalityHistogram({
  bars,
  maxAbs,
  tooltip,
  setTooltip,
  monthlySeries,
  seriesMeta,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  compact: _compact = false,
  assetLabel,
}: SeasonalityHistogramProps) {
  const vw = useViewportWidth();
  const axisFs = axisFontSize(vw);
  // reveal — CSS clip-path слева направо на первом рендере
  const [revealed, setRevealed] = useState(false);
  useLayoutEffect(() => {
    if (bars.length > 0 && !revealed) setRevealed(true);
  }, [bars.length, revealed]);

  // grown — флаг «волны слева направо» при каждом mount'е.
  // Компонент remount'ится через key={fetchId} в родителе при каждом новом
  // fetch'е — значит grown всегда начинает с false, rAF переключает в true,
  // CSS transition с staggered delay рисует волну. Единая логика для всех сценариев.
  const [grown, setGrown] = useState(false);
  useLayoutEffect(() => {
    if (bars.length > 0 && !grown) {
      const id = requestAnimationFrame(() => setGrown(true));
      return () => cancelAnimationFrame(id);
    }
  }, [bars.length, grown]);

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
      className={`relative overflow-hidden pb-2 cursor-crosshair ${revealed ? 'chart-reveal' : ''}`}
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
      {/* Легенда — SVG-based через <ChartLegend>. dominant-baseline=central
          даёт pixel-perfect центрирование dot↔text без CSS hacks.
          Перед легендой — bold-label с тикером выбранного актива (если задан).
          fontSize = legendFontSize(vw) — точное совпадение размера с легендой
          (breakpoint-aware: 11→12→13→15→17→19px). */}
      <div className="absolute top-1 left-1/2 -translate-x-1/2 z-20 pointer-events-none flex items-center gap-3">
        {assetLabel && (
          <span
            style={{
              color: 'var(--text-primary)',
              fontSize: legendFontSize(vw),
              fontWeight: 600,
              letterSpacing: '-0.01em',
              lineHeight: 1,
            }}
          >
            {assetLabel}
          </span>
        )}
        <ChartLegend
          items={isMulti
            ? safeMeta.map(s => ({ color: s.color, label: s.label }))
            : [
                { color: CHART_COLORS.positive, label: 'Рост' },
                { color: CHART_COLORS.negative, label: 'Падение' },
              ]
          }
          fontWeight={600}
          gap={16}
          style={{ color: 'var(--text-primary)' }}
        />
      </div>

      <div className="absolute" style={{
        top: 'var(--seasonality-hist-pad-top, 28px)',
        bottom: 'var(--seasonality-hist-pad-bottom, 24px)',
        left: 'var(--seasonality-hist-pad-x, 70px)',
        right: 'var(--seasonality-hist-pad-x, 70px)',
      }}>
        <svg viewBox="0 0 1000 500" preserveAspectRatio="none" width="100%" height="100%">
          {/* Единый путь: волна слева направо через transition-delay = (i / n) * stagger.
              Каждый бар стартует с задержкой пропорциональной позиции → эффект «волны».
              Outline видим только когда баров мало — иначе stroke съедает interior
              color (бар выглядит чёрным на mobile / monthday-31). */}
          {bars.map((bar, i) => {
            const showBarOutline = bars.length <= 20;
            const slotW = W / bars.length;
            const slotPadding = slotW * 0.1;
            const groupW = slotW - slotPadding * 2;
            const subBarW = isMulti ? groupW / nSeries : slotW * (bars.length > 12 ? 0.6 : 0.5);
            // Волновая задержка: от 0 до STAGGER_TOTAL_MS равномерно по барам
            const staggerDelay = bars.length > 1 ? (i / (bars.length - 1)) * STAGGER_TOTAL_MS : 0;
            const transitionStyle = {
              transition: `y ${ANIM_DURATION_MS}ms ${ANIM_EASING} ${staggerDelay}ms, height ${ANIM_DURATION_MS}ms ${ANIM_EASING} ${staggerDelay}ms`,
            };

            return (
              <g key={bar.key}
                opacity={tooltip?.bar ? (tooltip.bar.key === bar.key ? 1 : 0.5) : 1}
                className="transition-opacity duration-150">
                {isMulti ? (
                  safeMeta.map((style, s) => {
                    const seriesBar = safeSeries[s]?.bars?.[i];
                    if (!seriesBar) return null;
                    const val = seriesBar.avg_change;
                    const normalized = val / effectiveMaxAbs;
                    const h = grown ? Math.max(Math.abs(normalized) * halfH, H * 0.005) : 0;
                    const bx = i * slotW + slotPadding + s * subBarW;
                    const y = val >= 0 ? midY - h : midY;
                    return (
                      <rect
                        key={style.key}
                        x={bx + 1} y={y}
                        width={subBarW - 2} height={h}
                        fill={style.color} rx="2"
                        {...(showBarOutline ? { stroke: 'var(--bar-outline)', strokeWidth: 1 } : {})}
                        style={transitionStyle}
                      />
                    );
                  })
                ) : (() => {
                  const val = bar.avg_change;
                  const normalized = val / (effectiveMaxAbs || 0.01);
                  const h = grown ? Math.max(Math.abs(normalized) * halfH, H * 0.005) : 0;
                  const bx = i * slotW + (slotW - subBarW) / 2;
                  const y = val >= 0 ? midY - h : midY;
                  const color = val >= 0 ? CHART_COLORS.positive : CHART_COLORS.negative;
                  return (
                    <rect
                      key="single"
                      x={bx} y={y}
                      width={subBarW} height={h}
                      fill={color} rx="3"
                      {...(showBarOutline ? { stroke: 'var(--bar-outline)', strokeWidth: 1 } : {})}
                      style={transitionStyle}
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
          <ChartTooltip x={tooltip.x} y={tooltip.y}>
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

      {/* Y labels — синхронно с bar-area по высоте */}
      <div className="absolute pointer-events-none" style={{
        top: 'var(--seasonality-hist-pad-top, 28px)',
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
          const showLabel = step === 1 || i % step === 0;
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
