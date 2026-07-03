import { useRef, useMemo, useState, useLayoutEffect } from 'react';
import ChartNavigator from '../ChartNavigator';
import type { PriceChartResponse } from '../../services/api';
import { CHART_COLORS, CROSSHAIR, PADDING, cssVar, ANIMATION } from '../../config/chartTheme';
import { ChartGrid, ChartCrosshair, ChartDot, ChartDateLabel, ChartTooltip, TooltipRow, ChartYAxis, ChartXAxis, ChartMarker } from '../chart';
import ChartLegend from '../chart/ChartLegend';
import { easeOutCubic, morphPts, ptsToPath } from '../../utils/chartAnimation';
import ChartWatermark from '../ChartWatermark';

interface TooltipState {
  x: number;
  y: number;
  priceDate?: string;
  priceClose?: number;
  priceAdj?: number;
}

interface SeasonalityPriceChartProps {
  priceData: PriceChartResponse;
  priceNavRange: [number, number] | null;
  setPriceNavRange: (range: [number, number] | null) => void;
  tooltip: TooltipState | null;
  setTooltip: (t: TooltipState | null) => void;
  chartHeight: number;
}

export default function SeasonalityPriceChart({
  priceData,
  priceNavRange,
  setPriceNavRange,
  tooltip,
  setTooltip,
  chartHeight,
}: SeasonalityPriceChartProps) {
  const divHoverRef = useRef(false);

  // Анимация путей: reveal на первом рендере, морф при смене периода,
  // мгновенное обновление при drag'е навигатора (priceData === prev).
  const [revealed, setRevealed] = useState(false);
  const [animRawPath, setAnimRawPath] = useState('');
  const [animAdjPath, setAnimAdjPath] = useState('');
  const prevRawRef = useRef<{ x: number; y: number }[]>([]);
  const prevAdjRef = useRef<{ x: number; y: number }[]>([]);
  const currRawRef = useRef<{ x: number; y: number }[]>([]);
  const currAdjRef = useRef<{ x: number; y: number }[]>([]);
  const animRef = useRef<number | null>(null);
  const isFirstRef = useRef(true);
  const prevPriceDataRef = useRef(priceData);

  const allPricePoints = priceData.data;
  const priceDividends = priceData.dividends;

  // Navigator data format
  const priceNavData = useMemo(() =>
    allPricePoints.map(p => ({ time: p.date, value: p.close })),
    [allPricePoints]);

  // Display points (filtered by navigator)
  const pricePoints = useMemo(() => {
    if (!priceNavRange) return allPricePoints;
    return allPricePoints.slice(priceNavRange[0], priceNavRange[1] + 1);
  }, [allPricePoints, priceNavRange]);

  const priceMinMax = useMemo(() => {
    if (pricePoints.length === 0) return { min: 0, max: 1 };
    const allVals = pricePoints.flatMap(p => [p.close, p.adjusted]);
    const min = Math.min(...allVals);
    const max = Math.max(...allVals);
    const range = max - min || 1;
    return { min: min - range * 0.05, max: max + range * 0.05 };
  }, [pricePoints]);

  // Padding from CSS tokens (with fallback). PT — из --chart-pad-top:
  // единый зазор легенда→верхняя линия со всеми графиками.
  const PL = cssVar('--chart-pad-left', PADDING.left), PR = cssVar('--chart-pad-right-single', PADDING.rightSingle), PT = cssVar('--chart-pad-top', PADDING.top), PB = 60;
  const hasAdj = pricePoints.some(p => p.close !== p.adjusted);
  const scX = (i: number) => (i / Math.max(pricePoints.length - 1, 1));
  const scY = (v: number) => 1 - (v - priceMinMax.min) / (priceMinMax.max - priceMinMax.min);

  // Таргет в координатах viewBox (0..1000, 0..500) для обоих линий.
  const targetRaw = useMemo(
    () => pricePoints.map((p, i) => ({ x: scX(i) * 1000, y: scY(p.close) * 500 })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [pricePoints, priceMinMax]
  );
  const targetAdj = useMemo(
    () => pricePoints.map((p, i) => ({ x: scX(i) * 1000, y: scY(p.adjusted) * 500 })),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [pricePoints, priceMinMax]
  );

  // Анимация линий — морф на ЛЮБЫЕ изменения (период, тикер, навигатор):
  // 1) Первый рендер → CSS-reveal + instant set paths
  // 2) Смена priceData (период / тикер) → длинный морф 700ms (заметная смена)
  // 3) Только диапазон навигатора изменился → короткий морф 220ms (drag-friendly)
  useLayoutEffect(() => {
    if (!targetRaw.length) { setAnimRawPath(''); setAnimAdjPath(''); return; }
    if (animRef.current) cancelAnimationFrame(animRef.current);

    const priceDataChanged = prevPriceDataRef.current !== priceData;
    prevPriceDataRef.current = priceData;

    // Первый рендер: мгновенно + запуск CSS-reveal
    if (isFirstRef.current || prevRawRef.current.length === 0) {
      isFirstRef.current = false;
      prevRawRef.current = targetRaw;
      prevAdjRef.current = targetAdj;
      currRawRef.current = [];
      currAdjRef.current = [];
      setAnimRawPath(ptsToPath(targetRaw));
      setAnimAdjPath(ptsToPath(targetAdj));
      if (!revealed) setRevealed(true);
      return;
    }

    // Морф: длинная анимация для смены priceData (используем общую
    // ANIMATION.duration как в SimpleChart / OI / Buffett — единый «ритм»
    // через все графики проекта), короткая для drag навигатора.
    const fromRaw = currRawRef.current.length > 0 ? currRawRef.current : prevRawRef.current;
    const fromAdj = currAdjRef.current.length > 0 ? currAdjRef.current : prevAdjRef.current;
    const DURATION = priceDataChanged ? ANIMATION.morphDuration : 220;
    let start: number | null = null;

    const animate = (ts: number) => {
      if (!start) start = ts;
      const t = easeOutCubic(Math.min((ts - start) / DURATION, 1));
      const interpRaw = morphPts(fromRaw, targetRaw, t);
      const interpAdj = morphPts(fromAdj, targetAdj, t);
      currRawRef.current = interpRaw;
      currAdjRef.current = interpAdj;
      setAnimRawPath(ptsToPath(interpRaw));
      setAnimAdjPath(ptsToPath(interpAdj));
      if (t < 1) {
        animRef.current = requestAnimationFrame(animate);
      } else {
        prevRawRef.current = targetRaw;
        prevAdjRef.current = targetAdj;
        currRawRef.current = [];
        currAdjRef.current = [];
      }
    };
    animRef.current = requestAnimationFrame(animate);

    return () => { if (animRef.current) cancelAnimationFrame(animRef.current); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [targetRaw, targetAdj, priceData]);

  // Ранний выход ПОСЛЕ всех хуков (targetRaw/targetAdj/useLayoutEffect выше) —
  // иначе при пустых данных число хуков между рендерами менялось и React падал
  // (rules-of-hooks). Все хуки выше безопасны на пустом массиве: useMemo→[],
  // эффект гейтит `if (!targetRaw.length)`.
  if (pricePoints.length === 0) {
    return (
      <div className="flex items-center justify-center" style={{ height: chartHeight, color: 'var(--text-muted)' }}>Нет данных</div>
    );
  }

  const yTicks = Array.from({ length: 5 }, (_, i) => {
    const val = priceMinMax.min + ((priceMinMax.max - priceMinMax.min) * i) / 4;
    return { value: val, pct: scY(val) * 100 };
  });
  const xTicks = (() => {
    const count = Math.min(7, pricePoints.length);
    return Array.from({ length: count }, (_, i) => {
      const idx = Math.floor((i / Math.max(count - 1, 1)) * (pricePoints.length - 1));
      return { label: pricePoints[idx].date.slice(5), pct: scX(idx) * 100 };
    });
  })();
  // Visible dividends
  const visibleDivs = priceDividends.filter(d => {
    return pricePoints.some(p => p.date === d.date);
  }).map(d => {
    const idx = pricePoints.findIndex(p => p.date === d.date);
    return { ...d, idx, pct: scX(idx) * 100 };
  });

  return (
    <div>
      {/* Legend centered — SVG-based через <ChartLegend> для pixel-perfect alignment.
          margin-bottom — единый токен --chart-legend-mb (зазор легенда → chart). */}
      <div style={{ marginBottom: 'var(--chart-legend-mb, 2px)' }}>
        <ChartLegend
          items={[
            { color: CHART_COLORS.accent, label: 'Цена' },
            ...(hasAdj ? [{ color: CHART_COLORS.adjusted, label: 'Без дивидендных гэпов' }] : []),
          ]}
          fontWeight={600}
          gap={20}
          style={{ color: 'var(--text-primary)' }}
        />
      </div>

      {/* Chart area — chart-reveal класс активируется после первого рендера.
          touchAction: none + onTouch* handlers → поддержка водения пальцем на mobile. */}
      <div
        className={`relative cursor-crosshair ${revealed ? 'chart-reveal' : ''}`}
        style={{ aspectRatio: '2.4', minHeight: 280, maxHeight: 550, maxWidth: '100%', touchAction: 'none' }}
        onMouseMove={(e) => {
          if (divHoverRef.current) return;
          const rect = e.currentTarget.getBoundingClientRect();
          if (pricePoints.length === 0) return;
          const chartAreaW = rect.width - PL - PR;
          const mx = e.clientX - rect.left - PL;
          if (mx < 0 || mx > chartAreaW) { setTooltip(null); return; }
          const idx = Math.round((mx / chartAreaW) * (pricePoints.length - 1));
          const ci = Math.max(0, Math.min(idx, pricePoints.length - 1));
          const p = pricePoints[ci];
          setTooltip({
            x: PL + (ci / Math.max(pricePoints.length - 1, 1)) * chartAreaW,
            y: e.clientY - rect.top,
            priceDate: p.date, priceClose: p.close, priceAdj: p.adjusted,
          });
        }}
        onMouseLeave={() => { divHoverRef.current = false; setTooltip(null); }}
        onTouchStart={(e) => {
          if (!e.touches[0] || divHoverRef.current) return;
          const t = e.touches[0];
          const rect = e.currentTarget.getBoundingClientRect();
          if (pricePoints.length === 0) return;
          const chartAreaW = rect.width - PL - PR;
          const mx = t.clientX - rect.left - PL;
          if (mx < 0 || mx > chartAreaW) return;
          const idx = Math.round((mx / chartAreaW) * (pricePoints.length - 1));
          const ci = Math.max(0, Math.min(idx, pricePoints.length - 1));
          const p = pricePoints[ci];
          setTooltip({
            x: PL + (ci / Math.max(pricePoints.length - 1, 1)) * chartAreaW,
            y: t.clientY - rect.top,
            priceDate: p.date, priceClose: p.close, priceAdj: p.adjusted,
          });
        }}
        onTouchMove={(e) => {
          if (!e.touches[0] || divHoverRef.current) return;
          const t = e.touches[0];
          const rect = e.currentTarget.getBoundingClientRect();
          if (pricePoints.length === 0) return;
          const chartAreaW = rect.width - PL - PR;
          const mx = t.clientX - rect.left - PL;
          if (mx < 0 || mx > chartAreaW) return;
          const idx = Math.round((mx / chartAreaW) * (pricePoints.length - 1));
          const ci = Math.max(0, Math.min(idx, pricePoints.length - 1));
          const p = pricePoints[ci];
          setTooltip({
            x: PL + (ci / Math.max(pricePoints.length - 1, 1)) * chartAreaW,
            y: t.clientY - rect.top,
            priceDate: p.date, priceClose: p.close, priceAdj: p.adjusted,
          });
        }}
        onTouchEnd={() => { divHoverRef.current = false; setTooltip(null); }}
      >
        {/* Floating date — absolute над верхней грид-линией (y=PT), как в
            SimpleChart/Yearly: низ бокса (~17px) на 3px ниже линии
            (= PILL_GAP_ABOVE_LINE) → top = PT - 14. Формат как в OI
            («7 апр. 2026 г.») — в стейте ISO остаётся (ключ поиска точки). */}
        {tooltip?.priceDate && (
          <div className="absolute pointer-events-none" style={{ top: PT - 14, left: 0, right: 0, zIndex: 5 }}>
            <ChartDateLabel
              date={new Date(tooltip.priceDate).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' })}
              x={tooltip.x}
              padLeft={PL}
              padRight={PR}
            />
          </div>
        )}
        {/* SVG */}
        <div className="absolute" style={{ left: PL, right: PR, top: PT, bottom: PB }}>
          <svg viewBox={`0 0 1000 500`} preserveAspectRatio="none" width="100%" height="100%">
            {/* Grid */}
            <ChartGrid yTicks={yTicks} />
            {/* Raw price — путь анимируется через animRawPath */}
            <path d={animRawPath} fill="none" stroke={CHART_COLORS.accent} strokeWidth="2" vectorEffect="non-scaling-stroke" strokeLinecap="round" strokeLinejoin="round" />
            {/* Adjusted — dashed линия */}
            {hasAdj && (
              <path d={animAdjPath} fill="none" stroke={CHART_COLORS.adjusted} strokeWidth="2" vectorEffect="non-scaling-stroke" strokeLinecap="round" strokeLinejoin="round" strokeDasharray="6,3" />
            )}
            {/* Crosshair + Dot */}
            {tooltip?.priceDate && (() => {
              const idx = pricePoints.findIndex(p => p.date === tooltip.priceDate);
              if (idx < 0) return null;
              const cx = scX(idx) * 1000;
              const cy = scY(pricePoints[idx].close) * 500;
              return (
                <>
                  <ChartCrosshair x={cx} color={CROSSHAIR.accentColor} dashArray={CROSSHAIR.accentDashArray} opacity={CROSSHAIR.accentOpacity} />
                  <ChartDot x={cx} y={cy} color={CHART_COLORS.accent} />
                </>
              );
            })()}
          </svg>
          {/* Watermark — привязан к data area.
              Gridlines от 0% до 100% SVG, нижняя на низ data area.
              PB=60 хардкод (не --chart-pad-bottom!) → bottom=65.
              Left адаптивно через CSS-вар. */}
          <ChartWatermark
            left="calc(var(--chart-pad-left, 100px) + 5px)"
            bottom={65}
          />

          {/* Asset name перенесён в legend row выше. */}
        </div>

        {/* Y labels */}
        {/* padRight=PR — иначе на mobile labels overlap data area (см. YearlySeasonalityChart) */}
        <ChartYAxis ticks={yTicks} side="right" format={(v) => v.toFixed(0)} padTop={PT} padBottom={PB} padRight={PR} />

        {/* X labels */}
        <ChartXAxis labels={xTicks.map(t => t.label)} padLeft={PL} padRight={PR} />

        {/* Dividend circles at bottom */}
        <div className="absolute" style={{ left: PL, right: PR, bottom: PB - 14 }}>
          {visibleDivs.map((d, i) => (
            <ChartMarker
              key={i}
              label="Д"
              xPct={d.pct}
              guideHeight={`calc(100% - ${PB + 14}px)`}
              onHover={() => { divHoverRef.current = true; setTooltip(null); }}
              onLeave={() => { divHoverRef.current = false; }}
            >
              <div className="text-xs font-medium text-theme-primary">{d.value} ₽ — {d.date}</div>
            </ChartMarker>
          ))}
        </div>

        {/* Value tooltip */}
        {tooltip?.priceDate && (
          <ChartTooltip x={tooltip.x} y={tooltip.y} clampTop={PT} clampBottom={PB}>
            <TooltipRow color={CHART_COLORS.accent} label="Цена" value={`${tooltip.priceClose?.toFixed(2)} ₽`} />
            {tooltip.priceAdj !== tooltip.priceClose && (
              <TooltipRow color={CHART_COLORS.adjusted} label="Без гэпов" value={`${tooltip.priceAdj?.toFixed(2)} ₽`} />
            )}
          </ChartTooltip>
        )}
      </div>

      {/* Navigator — скрыт в html2canvas snapshot через data-export-ignore */}
      <div data-export-ignore="true">
        <ChartNavigator
          data={priceNavData}
          onChange={(s, e) => setPriceNavRange([s, e])}
          color={CHART_COLORS.accent}
          insetLeft="var(--chart-pad-left)"
          insetRight="var(--chart-pad-right-single)"
        />
      </div>
    </div>
  );
}
