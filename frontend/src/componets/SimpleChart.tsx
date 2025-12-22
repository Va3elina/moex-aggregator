import { useMemo, useState, useRef, useEffect } from 'react';

interface DataPoint {
  time: string;
  value: number;
}

interface SimpleChartProps {
  data: DataPoint[];
  secondaryData?: DataPoint[];
  thirdData?: DataPoint[];
  height?: number;
  primaryColor?: string;
  secondaryColor?: string;
  thirdColor?: string;
  showSecondary?: boolean;
  showThird?: boolean;
  formatValue?: (value: number) => string;
  formatTime?: (time: string) => string;
  loading?: boolean;
  primaryLabel?: string;
  secondaryLabel?: string;
  thirdLabel?: string;
}

export default function SimpleChart({
  data,
  secondaryData,
  thirdData,
  height = 450,
  primaryColor = '#6366f1',
  secondaryColor = '#f59e0b',
  thirdColor = '#f43f5e',
  showSecondary = false,
  showThird = false,
  formatValue = (v) => v.toLocaleString('ru-RU'),
  formatTime = (t) => {
    const date = new Date(t);
    return date.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short' });
  },
  loading = false,
  primaryLabel = 'Цена',
  secondaryLabel = 'OI',
  thirdLabel = '',
}: SimpleChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(800);
  const [tooltip, setTooltip] = useState<{
    x: number;
    primaryY: number;
    secondaryY: number | null;
    thirdY: number | null;
    value: number;
    secondaryValue?: number;
    thirdValue?: number;
    time: string;
    visible: boolean;
  }>({ x: 0, primaryY: 0, secondaryY: null, thirdY: null, value: 0, time: '', visible: false });

  const [animated, setAnimated] = useState(false);

  useEffect(() => {
    const updateWidth = () => {
      if (containerRef.current) {
        setWidth(containerRef.current.clientWidth);
      }
    };
    updateWidth();
    window.addEventListener('resize', updateWidth);
    return () => window.removeEventListener('resize', updateWidth);
  }, []);

  useEffect(() => {
    if (data.length > 0 && !loading) {
      setAnimated(false);
      const timer = setTimeout(() => setAnimated(true), 50);
      return () => clearTimeout(timer);
    }
  }, [data, loading]);

  const padding = { top: 40, right: 30, bottom: 50, left: 80 };
  const chartWidth = Math.max(width - padding.left - padding.right, 100);
  const chartHeight = height - padding.top - padding.bottom;

  const chartCalc = useMemo(() => {
    if (data.length === 0) {
      return {
        path: '', areaPath: '', points: [], yTicks: [], xTicks: [],
        secondaryPath: '', secondaryPoints: [],
        thirdPath: '', thirdPoints: [],
        scaleSecondaryY: () => 0
      };
    }

    // Primary scale (price)
    const values = data.map((d) => d.value);
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    const range = maxVal - minVal || 1;
    const yPadding = range * 0.1;
    const yMinVal = minVal - yPadding;
    const yMaxVal = maxVal + yPadding;

    // Secondary scale (OI) - общая для secondary и third
    let secYMin = 0;
    let secYMax = 1;
    const allSecondaryValues: number[] = [];
    if (showSecondary && secondaryData && secondaryData.length > 0) {
      allSecondaryValues.push(...secondaryData.map((d) => d.value));
    }
    if (showThird && thirdData && thirdData.length > 0) {
      allSecondaryValues.push(...thirdData.map((d) => d.value));
    }
    if (allSecondaryValues.length > 0) {
      const secMin = Math.min(...allSecondaryValues);
      const secMax = Math.max(...allSecondaryValues);
      const secRange = secMax - secMin || 1;
      secYMin = secMin - secRange * 0.1;
      secYMax = secMax + secRange * 0.1;
    }

    const scaleX = (index: number, total: number) => (index / Math.max(total - 1, 1)) * chartWidth;
    const scaleY = (value: number) => chartHeight - ((value - yMinVal) / (yMaxVal - yMinVal)) * chartHeight;
    const scaleSecondaryY = (value: number) =>
      chartHeight - ((value - secYMin) / (secYMax - secYMin)) * chartHeight;

    // Primary points
    const points = data.map((d, i) => ({
      x: scaleX(i, data.length),
      y: scaleY(d.value),
      value: d.value,
      time: d.time,
      timestamp: new Date(d.time).getTime(),
    }));

    const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
    const areaPathD = points.length > 0
      ? `${pathD} L ${points[points.length - 1].x} ${chartHeight} L ${points[0].x} ${chartHeight} Z`
      : '';

    // Secondary points (OI) - с собственными координатами
    let secondaryPoints: typeof points = [];
    let secondaryPathD = '';
    if (showSecondary && secondaryData && secondaryData.length > 0) {
      secondaryPoints = secondaryData.map((d, i) => ({
        x: scaleX(i, secondaryData.length),
        y: scaleSecondaryY(d.value),
        value: d.value,
        time: d.time,
        timestamp: new Date(d.time).getTime(),
      }));
      secondaryPathD = secondaryPoints.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
    }

    // Third points
    let thirdPoints: typeof points = [];
    let thirdPathD = '';
    if (showThird && thirdData && thirdData.length > 0) {
      thirdPoints = thirdData.map((d, i) => ({
        x: scaleX(i, thirdData.length),
        y: scaleSecondaryY(d.value),
        value: d.value,
        time: d.time,
        timestamp: new Date(d.time).getTime(),
      }));
      thirdPathD = thirdPoints.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
    }

    // Y ticks
    const yTickCount = 5;
    const yTicks = Array.from({ length: yTickCount }, (_, i) => {
      const value = yMinVal + ((yMaxVal - yMinVal) * i) / (yTickCount - 1);
      return { value, y: scaleY(value) };
    });

    // X ticks
    const xTickCount = Math.min(7, data.length);
    const xTicks = Array.from({ length: xTickCount }, (_, i) => {
      const index = Math.floor((i / Math.max(xTickCount - 1, 1)) * (data.length - 1));
      return { time: data[index].time, x: scaleX(index, data.length) };
    });

    return {
      path: pathD, areaPath: areaPathD, points, yTicks, xTicks,
      secondaryPath: secondaryPathD, secondaryPoints,
      thirdPath: thirdPathD, thirdPoints,
      scaleSecondaryY
    };
  }, [data, secondaryData, thirdData, chartWidth, chartHeight, showSecondary, showThird]);

  // Поиск ближайшей точки по X координате
  const findClosestPoint = (points: typeof chartCalc.points, mouseX: number) => {
    if (points.length === 0) return null;
    let closest = points[0];
    let closestDist = Math.abs(points[0].x - mouseX);
    for (const p of points) {
      const dist = Math.abs(p.x - mouseX);
      if (dist < closestDist) {
        closestDist = dist;
        closest = p;
      }
    }
    return closest;
  };

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    if (chartCalc.points.length === 0) return;

    const svg = e.currentTarget;
    const rect = svg.getBoundingClientRect();
    const mouseX = e.clientX - rect.left - padding.left;

    // Ограничиваем область
    if (mouseX < 0 || mouseX > chartWidth) {
      setTooltip(prev => ({ ...prev, visible: false }));
      return;
    }

    // Находим ближайшие точки для каждой линии
    const primaryPoint = findClosestPoint(chartCalc.points, mouseX);
    const secondaryPoint = showSecondary ? findClosestPoint(chartCalc.secondaryPoints, mouseX) : null;
    const thirdPoint = showThird ? findClosestPoint(chartCalc.thirdPoints, mouseX) : null;

    if (!primaryPoint) return;

    setTooltip({
      x: primaryPoint.x + padding.left,
      primaryY: primaryPoint.y + padding.top,
      secondaryY: secondaryPoint ? secondaryPoint.y + padding.top : null,
      thirdY: thirdPoint ? thirdPoint.y + padding.top : null,
      value: primaryPoint.value,
      secondaryValue: secondaryPoint?.value,
      thirdValue: thirdPoint?.value,
      time: primaryPoint.time,
      visible: true,
    });
  };

  const handleMouseLeave = () => {
    setTooltip((prev) => ({ ...prev, visible: false }));
  };

  if (loading) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-gradient-to-br from-slate-900 to-slate-800" style={{ height }}>
        <div className="flex items-center gap-3 text-slate-400">
          <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
          <span className="text-lg">Загрузка...</span>
        </div>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-gradient-to-br from-slate-900 to-slate-800" style={{ height }}>
        <p className="text-slate-500 text-lg">Нет данных для отображения</p>
      </div>
    );
  }

  const currentValue = data[data.length - 1]?.value || 0;
  const firstValue = data[0]?.value || currentValue;
  const change = currentValue - firstValue;
  const changePercent = firstValue !== 0 ? (change / firstValue) * 100 : 0;
  const isPositive = change >= 0;

  // Ограничение позиции тултипа
  const tooltipX = Math.min(Math.max(tooltip.x, padding.left + 10), width - 180);
  const tooltipY = Math.max(30, Math.min(tooltip.primaryY - 70, height - 140));

  return (
    <div ref={containerRef} className="rounded-2xl p-5 bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 border border-slate-700/50 shadow-2xl">
      {/* Заголовок с ценой */}
      <div className="mb-2 flex items-baseline gap-4">
        <span className="text-4xl font-bold text-white tracking-tight">
          {formatValue(currentValue)}
        </span>
        <span className={`text-base font-semibold px-2 py-0.5 rounded-lg ${
          isPositive 
            ? 'text-emerald-400 bg-emerald-500/10' 
            : 'text-rose-400 bg-rose-500/10'
        }`}>
          {isPositive ? '↑' : '↓'} {Math.abs(changePercent).toFixed(2)}%
        </span>
      </div>

      {/* SVG График */}
      <svg
        width={width}
        height={height}
        className="cursor-crosshair select-none"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
      >
        <defs>
          <linearGradient id="primaryGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={primaryColor} stopOpacity="0.4" />
            <stop offset="50%" stopColor={primaryColor} stopOpacity="0.1" />
            <stop offset="100%" stopColor={primaryColor} stopOpacity="0" />
          </linearGradient>
          <clipPath id="chartClip">
            <rect x={0} y={0} width={chartWidth} height={chartHeight} />
          </clipPath>
        </defs>

        <g transform={`translate(${padding.left}, ${padding.top})`}>
          {/* Горизонтальные линии сетки */}
          {chartCalc.yTicks.map((tick, i) => (
            <g key={i}>
              <line
                x1={0}
                y1={tick.y}
                x2={chartWidth}
                y2={tick.y}
                stroke="#334155"
                strokeWidth="1"
                strokeDasharray="4,6"
                opacity="0.5"
              />
              <text
                x={-12}
                y={tick.y + 4}
                textAnchor="end"
                fill="#64748b"
                fontSize="12"
                fontWeight="500"
              >
                {formatValue(tick.value)}
              </text>
            </g>
          ))}

          {/* X метки */}
          {chartCalc.xTicks.map((tick, i) => (
            <text
              key={i}
              x={Math.min(tick.x, chartWidth - 20)}
              y={chartHeight + 30}
              textAnchor="middle"
              fill="#64748b"
              fontSize="12"
              fontWeight="500"
            >
              {formatTime(tick.time)}
            </text>
          ))}

          {/* Область графика с клиппингом */}
          <g clipPath="url(#chartClip)">
            {/* Область под основной линией */}
            <path
              d={chartCalc.areaPath}
              fill="url(#primaryGradient)"
              className={`transition-opacity duration-700 ${animated ? 'opacity-100' : 'opacity-0'}`}
            />

            {/* Third линия (продажи) */}
            {showThird && chartCalc.thirdPath && (
              <path
                d={chartCalc.thirdPath}
                fill="none"
                stroke={thirdColor}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={`transition-opacity duration-700 ${animated ? 'opacity-100' : 'opacity-0'}`}
              />
            )}

            {/* Secondary линия (OI) */}
            {showSecondary && chartCalc.secondaryPath && (
              <path
                d={chartCalc.secondaryPath}
                fill="none"
                stroke={secondaryColor}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={`transition-opacity duration-700 ${animated ? 'opacity-100' : 'opacity-0'}`}
              />
            )}

            {/* Основная линия */}
            <path
              d={chartCalc.path}
              fill="none"
              stroke={primaryColor}
              strokeWidth="3"
              strokeLinecap="round"
              strokeLinejoin="round"
              className={`transition-opacity duration-700 ${animated ? 'opacity-100' : 'opacity-0'}`}
            />

            {/* Затемнение области после курсора */}
            {tooltip.visible && (
              <rect
                x={tooltip.x - padding.left}
                y={0}
                width={Math.max(0, chartWidth - (tooltip.x - padding.left))}
                height={chartHeight}
                fill="#0f172a"
                opacity="0.5"
                className="transition-opacity duration-150"
              />
            )}
          </g>

          {/* Вертикальная линия и точки курсора */}
          {tooltip.visible && (
            <>
              <line
                x1={tooltip.x - padding.left}
                y1={0}
                x2={tooltip.x - padding.left}
                y2={chartHeight}
                stroke="#6366f1"
                strokeWidth="1.5"
                strokeDasharray="4,4"
              />
              {/* Точка на основной линии */}
              <circle
                cx={tooltip.x - padding.left}
                cy={tooltip.primaryY - padding.top}
                r="7"
                fill={primaryColor}
                stroke="#fff"
                strokeWidth="3"
                className="drop-shadow-lg"
              />
              {/* Точка на secondary линии */}
              {showSecondary && tooltip.secondaryY !== null && (
                <circle
                  cx={tooltip.x - padding.left}
                  cy={tooltip.secondaryY - padding.top}
                  r="6"
                  fill={secondaryColor}
                  stroke="#fff"
                  strokeWidth="2"
                />
              )}
              {/* Точка на third линии */}
              {showThird && tooltip.thirdY !== null && (
                <circle
                  cx={tooltip.x - padding.left}
                  cy={tooltip.thirdY - padding.top}
                  r="6"
                  fill={thirdColor}
                  stroke="#fff"
                  strokeWidth="2"
                />
              )}
            </>
          )}
        </g>

        {/* Тултип */}
        {tooltip.visible && (
          <foreignObject
            x={tooltipX}
            y={tooltipY}
            width="170"
            height="120"
          >
            <div className="bg-slate-800/95 backdrop-blur-sm rounded-xl p-3 border border-slate-600/50 shadow-xl">
              <p className="text-slate-400 text-xs mb-1">
                {new Date(tooltip.time).toLocaleDateString('ru-RU', {
                  day: '2-digit', month: 'short', year: 'numeric'
                })}
              </p>
              <p className="text-white font-bold text-lg">
                {formatValue(tooltip.value)}
              </p>
              {showSecondary && tooltip.secondaryValue !== undefined && (
                <p className="text-sm mt-1" style={{ color: secondaryColor }}>
                  {secondaryLabel}: {tooltip.secondaryValue.toLocaleString('ru-RU')}
                </p>
              )}
              {showThird && tooltip.thirdValue !== undefined && (
                <p className="text-sm" style={{ color: thirdColor }}>
                  {thirdLabel}: {tooltip.thirdValue.toLocaleString('ru-RU')}
                </p>
              )}
            </div>
          </foreignObject>
        )}
      </svg>

      {/* Легенда */}
      <div className="flex gap-6 mt-4 text-sm flex-wrap">
        <span className="flex items-center gap-2">
          <span className="w-5 h-1 rounded-full" style={{ backgroundColor: primaryColor }} />
          <span className="text-slate-400">{primaryLabel}</span>
        </span>
        {showSecondary && (
          <span className="flex items-center gap-2">
            <span className="w-5 h-1 rounded-full" style={{ backgroundColor: secondaryColor }} />
            <span className="text-slate-400">{secondaryLabel}</span>
          </span>
        )}
        {showThird && (
          <span className="flex items-center gap-2">
            <span className="w-5 h-1 rounded-full" style={{ backgroundColor: thirdColor }} />
            <span className="text-slate-400">{thirdLabel}</span>
          </span>
        )}
      </div>
    </div>
  );
}