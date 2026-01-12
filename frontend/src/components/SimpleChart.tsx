import { useMemo, useState, useRef, useEffect, useCallback } from 'react';
import { Download } from 'lucide-react';

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

// Интерполяция между двумя значениями
const lerp = (start: number, end: number, t: number) => start + (end - start) * t;

// Easing функция для плавности (ease-out cubic)
const easeOutCubic = (t: number) => 1 - Math.pow(1 - t, 3);

// Ресемплинг массива точек до нужной длины
const resamplePoints = (points: { x: number; y: number }[], targetLength: number) => {
  if (points.length === 0) return [];
  if (points.length === targetLength) return points;

  const result: { x: number; y: number }[] = [];
  for (let i = 0; i < targetLength; i++) {
    const t = i / (targetLength - 1);
    const sourceIndex = t * (points.length - 1);
    const lowerIndex = Math.floor(sourceIndex);
    const upperIndex = Math.min(lowerIndex + 1, points.length - 1);
    const localT = sourceIndex - lowerIndex;

    result.push({
      x: lerp(points[lowerIndex].x, points[upperIndex].x, localT),
      y: lerp(points[lowerIndex].y, points[upperIndex].y, localT),
    });
  }
  return result;
};

// Интерполяция между двумя массивами точек
const interpolatePoints = (
  from: { x: number; y: number }[],
  to: { x: number; y: number }[],
  t: number
) => {
  const maxLen = Math.max(from.length, to.length);
  const fromResampled = resamplePoints(from, maxLen);
  const toResampled = resamplePoints(to, maxLen);

  return fromResampled.map((p, i) => ({
    x: lerp(p.x, toResampled[i].x, t),
    y: lerp(p.y, toResampled[i].y, t),
  }));
};

// Генерация SVG path из точек
const pointsToPath = (points: { x: number; y: number }[]) => {
  if (points.length === 0) return '';
  return points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
};

// Генерация area path
const pointsToAreaPath = (points: { x: number; y: number }[], chartHeight: number) => {
  if (points.length === 0) return '';
  const linePath = pointsToPath(points);
  return `${linePath} L ${points[points.length - 1].x} ${chartHeight} L ${points[0].x} ${chartHeight} Z`;
};

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
  const svgRef = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(800);
  const animationRef = useRef<number | null>(null);
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

  // Анимированные пути
  const [animatedPaths, setAnimatedPaths] = useState({
    primary: '',
    area: '',
    secondary: '',
    third: '',
  });

  // Предыдущие точки для интерполяции
  const prevPointsRef = useRef<{
    primary: { x: number; y: number }[];
    secondary: { x: number; y: number }[];
    third: { x: number; y: number }[];
  }>({ primary: [], secondary: [], third: [] });

  // Флаг первой загрузки
  const isFirstRender = useRef(true);

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

  const padding = { top: 40, right: 30, bottom: 50, left: 80 };
  const chartWidth = Math.max(width - padding.left - padding.right, 100);
  const chartHeight = height - padding.top - padding.bottom;

  // Вычисление целевых точек
  const targetCalc = useMemo(() => {
    if (data.length === 0) {
      return {
        points: [],
        secondaryPoints: [],
        thirdPoints: [],
        yTicks: [],
        xTicks: [],
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
    }));

    // Secondary points
    let secondaryPoints: typeof points = [];
    if (showSecondary && secondaryData && secondaryData.length > 0) {
      secondaryPoints = secondaryData.map((d, i) => ({
        x: scaleX(i, secondaryData.length),
        y: scaleSecondaryY(d.value),
        value: d.value,
        time: d.time,
      }));
    }

    // Third points
    let thirdPoints: typeof points = [];
    if (showThird && thirdData && thirdData.length > 0) {
      thirdPoints = thirdData.map((d, i) => ({
        x: scaleX(i, thirdData.length),
        y: scaleSecondaryY(d.value),
        value: d.value,
        time: d.time,
      }));
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

    return { points, secondaryPoints, thirdPoints, yTicks, xTicks };
  }, [data, secondaryData, thirdData, chartWidth, chartHeight, showSecondary, showThird]);

  // Анимация морфинга
  const animateMorph = useCallback(() => {
    if (loading || data.length === 0) return;

    const targetPrimary = targetCalc.points.map(p => ({ x: p.x, y: p.y }));
    const targetSecondary = targetCalc.secondaryPoints.map(p => ({ x: p.x, y: p.y }));
    const targetThird = targetCalc.thirdPoints.map(p => ({ x: p.x, y: p.y }));

    // Если первый рендер или нет предыдущих данных - показываем сразу с fade
    if (isFirstRender.current || prevPointsRef.current.primary.length === 0) {
      isFirstRender.current = false;
      prevPointsRef.current = {
        primary: targetPrimary,
        secondary: targetSecondary,
        third: targetThird,
      };

      // Анимация появления
      let startTime: number | null = null;
      const fadeIn = (timestamp: number) => {
        if (!startTime) startTime = timestamp;
        const elapsed = timestamp - startTime;
        const duration = 500;
        const t = Math.min(elapsed / duration, 1);
        const eased = easeOutCubic(t);

        // Анимируем от нижней границы графика
        const animatedPrimary = targetPrimary.map(p => ({
          x: p.x,
          y: chartHeight - (chartHeight - p.y) * eased,
        }));
        const animatedSecondary = targetSecondary.map(p => ({
          x: p.x,
          y: chartHeight - (chartHeight - p.y) * eased,
        }));
        const animatedThird = targetThird.map(p => ({
          x: p.x,
          y: chartHeight - (chartHeight - p.y) * eased,
        }));

        setAnimatedPaths({
          primary: pointsToPath(animatedPrimary),
          area: pointsToAreaPath(animatedPrimary, chartHeight),
          secondary: pointsToPath(animatedSecondary),
          third: pointsToPath(animatedThird),
        });

        if (t < 1) {
          animationRef.current = requestAnimationFrame(fadeIn);
        }
      };

      animationRef.current = requestAnimationFrame(fadeIn);
      return;
    }

    // Морфинг от предыдущих данных к новым
    const fromPrimary = prevPointsRef.current.primary;
    const fromSecondary = prevPointsRef.current.secondary;
    const fromThird = prevPointsRef.current.third;

    let startTime: number | null = null;
    const duration = 600; // мс

    const animate = (timestamp: number) => {
      if (!startTime) startTime = timestamp;
      const elapsed = timestamp - startTime;
      const t = Math.min(elapsed / duration, 1);
      const eased = easeOutCubic(t);

      // Интерполяция всех линий
      const interpolatedPrimary = interpolatePoints(fromPrimary, targetPrimary, eased);
      const interpolatedSecondary = fromSecondary.length > 0 || targetSecondary.length > 0
        ? interpolatePoints(
            fromSecondary.length > 0 ? fromSecondary : targetSecondary.map(p => ({ ...p, y: chartHeight })),
            targetSecondary.length > 0 ? targetSecondary : fromSecondary.map(p => ({ ...p, y: chartHeight })),
            eased
          )
        : [];
      const interpolatedThird = fromThird.length > 0 || targetThird.length > 0
        ? interpolatePoints(
            fromThird.length > 0 ? fromThird : targetThird.map(p => ({ ...p, y: chartHeight })),
            targetThird.length > 0 ? targetThird : fromThird.map(p => ({ ...p, y: chartHeight })),
            eased
          )
        : [];

      setAnimatedPaths({
        primary: pointsToPath(interpolatedPrimary),
        area: pointsToAreaPath(interpolatedPrimary, chartHeight),
        secondary: pointsToPath(interpolatedSecondary),
        third: pointsToPath(interpolatedThird),
      });

      if (t < 1) {
        animationRef.current = requestAnimationFrame(animate);
      } else {
        // Сохраняем конечные точки как предыдущие
        prevPointsRef.current = {
          primary: targetPrimary,
          secondary: targetSecondary,
          third: targetThird,
        };
      }
    };

    // Отменяем предыдущую анимацию
    if (animationRef.current) {
      cancelAnimationFrame(animationRef.current);
    }

    animationRef.current = requestAnimationFrame(animate);
  }, [loading, data, targetCalc, chartHeight]);

  // Запуск анимации при изменении данных
  useEffect(() => {
    animateMorph();

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [animateMorph]);

  // Функция скачивания графика как PNG
  const downloadChart = useCallback(() => {
    if (!svgRef.current) return;

    const svg = svgRef.current;

    // Клонируем SVG для модификации
    const clonedSvg = svg.cloneNode(true) as SVGSVGElement;

    // Добавляем xmlns если нет
    clonedSvg.setAttribute('xmlns', 'http://www.w3.org/2000/svg');

    // Добавляем фон в SVG
    const bgRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    bgRect.setAttribute('width', '100%');
    bgRect.setAttribute('height', '100%');
    bgRect.setAttribute('fill', '#121523');
    clonedSvg.insertBefore(bgRect, clonedSvg.firstChild);

    // Добавляем inline стили для текста
    const texts = clonedSvg.querySelectorAll('text');
    texts.forEach(text => {
      text.style.fontFamily = 'system-ui, -apple-system, sans-serif';
    });

    const svgData = new XMLSerializer().serializeToString(clonedSvg);

    // Создаём canvas с учётом devicePixelRatio для чёткости
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const scale = 2; // Для ретина-дисплеев
    canvas.width = width * scale;
    canvas.height = height * scale;
    ctx.scale(scale, scale);

    // Создаём изображение из SVG
    const img = new Image();
    const svgBlob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' });
    const url = URL.createObjectURL(svgBlob);

    img.onload = () => {
      ctx.drawImage(img, 0, 0, width, height);
      URL.revokeObjectURL(url);

      // Скачиваем PNG
      const link = document.createElement('a');
      link.download = `chart-${new Date().toISOString().slice(0, 10)}.png`;
      link.href = canvas.toDataURL('image/png');
      link.click();
    };

    img.onerror = () => {
      console.error('Ошибка при создании изображения');
      URL.revokeObjectURL(url);
    };

    img.src = url;
  }, [width, height]);

  // Поиск ближайшей точки по X координате
  const findClosestPoint = (points: typeof targetCalc.points, mouseX: number) => {
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
    if (targetCalc.points.length === 0) return;

    const svg = e.currentTarget;
    const rect = svg.getBoundingClientRect();
    const mouseX = e.clientX - rect.left - padding.left;

    if (mouseX < 0 || mouseX > chartWidth) {
      setTooltip(prev => ({ ...prev, visible: false }));
      return;
    }

    const primaryPoint = findClosestPoint(targetCalc.points, mouseX);
    const secondaryPoint = showSecondary ? findClosestPoint(targetCalc.secondaryPoints, mouseX) : null;
    const thirdPoint = showThird ? findClosestPoint(targetCalc.thirdPoints, mouseX) : null;

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

  // Показываем полный лоадер только если нет данных вообще
  if (data.length === 0 && loading) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-[#121523]" style={{ height }}>
        <div className="flex items-center gap-3 text-[#A7ADBC]">
          <div className="w-6 h-6 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
          <span className="text-lg">Загрузка...</span>
        </div>
      </div>
    );
  }

  if (data.length === 0 && !loading) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-[#121523]" style={{ height }}>
        <p className="text-[#A7ADBC] text-lg">Нет данных для отображения</p>
      </div>
    );
  }

  const currentValue = data[data.length - 1]?.value || 0;
  const firstValue = data[0]?.value || currentValue;
  const change = currentValue - firstValue;
  const changePercent = firstValue !== 0 ? (change / firstValue) * 100 : 0;
  const isPositive = change >= 0;

  const tooltipX = Math.min(Math.max(tooltip.x, padding.left + 10), width - 180);
  const tooltipY = Math.max(30, Math.min(tooltip.primaryY - 70, height - 140));

  return (
    <div ref={containerRef} className="rounded-2xl p-5 bg-[#121523] border border-white/10 relative">
      {/* Кнопка скачивания */}
      <button
        onClick={downloadChart}
        className="absolute top-4 right-4 z-10 flex items-center justify-center w-9 h-9 bg-[#1A1F2E]/90 backdrop-blur-sm rounded-lg border border-white/10 text-[#A7ADBC] hover:text-[#C8FF2E] hover:border-[#C8FF2E]/30 hover:scale-110 active:scale-95 active:bg-[#0B0D12] transition-all duration-150 ease-out"
        title="Скачать график как PNG"
      >
        <Download size={18} />
      </button>

      {/* Маленький индикатор загрузки поверх графика */}
      {loading && (
        <div className="absolute top-4 right-16 z-10 flex items-center gap-2 bg-[#1A1F2E]/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-white/10">
          <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
          <span className="text-xs text-[#A7ADBC]">Обновление...</span>
        </div>
      )}

      {/* Заголовок с ценой */}
      <div className="mb-2 flex items-baseline gap-4">
        <span className="text-4xl font-bold text-[#F4F6FA] tracking-tight transition-all duration-500">
          {formatValue(currentValue)}
        </span>
        <span className={`text-base font-semibold px-2 py-0.5 rounded-lg transition-all duration-500 ${
          isPositive 
            ? 'text-[#2EE59D] bg-[#2EE59D]/10' 
            : 'text-[#FF4D4D] bg-[#FF4D4D]/10'
        }`}>
          {isPositive ? '↑' : '↓'} {Math.abs(changePercent).toFixed(2)}%
        </span>
      </div>

      {/* SVG График */}
      <svg
        ref={svgRef}
        width={width}
        height={height}
        className="cursor-crosshair select-none"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
      >
        <defs>
          <linearGradient id="primaryGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={primaryColor} stopOpacity="0.3" />
            <stop offset="50%" stopColor={primaryColor} stopOpacity="0.1" />
            <stop offset="100%" stopColor={primaryColor} stopOpacity="0" />
          </linearGradient>
          <clipPath id="chartClip">
            <rect x={0} y={0} width={chartWidth} height={chartHeight} />
          </clipPath>
        </defs>

        <g transform={`translate(${padding.left}, ${padding.top})`}>
          {/* Горизонтальные линии сетки */}
          {targetCalc.yTicks.map((tick, i) => (
            <g key={i}>
              <line
                x1={0}
                y1={tick.y}
                x2={chartWidth}
                y2={tick.y}
                stroke="#2A2F3E"
                strokeWidth="1"
                strokeDasharray="4,6"
                opacity="0.5"
              />
              <text
                x={-12}
                y={tick.y + 4}
                textAnchor="end"
                fill="#5E6576"
                fontSize="12"
                fontWeight="500"
              >
                {formatValue(tick.value)}
              </text>
            </g>
          ))}

          {/* X метки */}
          {targetCalc.xTicks.map((tick, i) => (
            <text
              key={i}
              x={Math.min(tick.x, chartWidth - 20)}
              y={chartHeight + 30}
              textAnchor="middle"
              fill="#5E6576"
              fontSize="12"
              fontWeight="500"
            >
              {formatTime(tick.time)}
            </text>
          ))}

          {/* Область графика с клиппингом */}
          <g clipPath="url(#chartClip)">
            {/* Область под основной линией */}
            {animatedPaths.area && (
              <path
                d={animatedPaths.area}
                fill="url(#primaryGradient)"
              />
            )}

            {/* Third линия (продажи) */}
            {showThird && animatedPaths.third && (
              <path
                d={animatedPaths.third}
                fill="none"
                stroke={thirdColor}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )}

            {/* Secondary линия (покупки) */}
            {showSecondary && animatedPaths.secondary && (
              <path
                d={animatedPaths.secondary}
                fill="none"
                stroke={secondaryColor}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )}

            {/* Основная линия */}
            {animatedPaths.primary && (
              <path
                d={animatedPaths.primary}
                fill="none"
                stroke={primaryColor}
                strokeWidth="3"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            )}

            {/* Затемнение области после курсора */}
            {tooltip.visible && (
              <rect
                x={tooltip.x - padding.left}
                y={0}
                width={Math.max(0, chartWidth - (tooltip.x - padding.left))}
                height={chartHeight}
                fill="#0B0D12"
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
                stroke="#C8FF2E"
                strokeWidth="1"
                strokeDasharray="4,4"
                opacity="0.5"
              />
              {/* Точка на основной линии */}
              <circle
                cx={tooltip.x - padding.left}
                cy={tooltip.primaryY - padding.top}
                r="6"
                fill={primaryColor}
                stroke="#0B0D12"
                strokeWidth="2"
                className="drop-shadow-lg"
              />
              {/* Точка на secondary линии */}
              {showSecondary && tooltip.secondaryY !== null && (
                <circle
                  cx={tooltip.x - padding.left}
                  cy={tooltip.secondaryY - padding.top}
                  r="5"
                  fill={secondaryColor}
                  stroke="#0B0D12"
                  strokeWidth="2"
                />
              )}
              {/* Точка на third линии */}
              {showThird && tooltip.thirdY !== null && (
                <circle
                  cx={tooltip.x - padding.left}
                  cy={tooltip.thirdY - padding.top}
                  r="5"
                  fill={thirdColor}
                  stroke="#0B0D12"
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
            height="130"
          >
            <div className="bg-[#1A1F2E]/95 backdrop-blur-sm rounded-xl p-3 border border-white/10 shadow-xl">
              <p className="text-[#A7ADBC] text-xs mb-1">
                {new Date(tooltip.time).toLocaleDateString('ru-RU', {
                  day: '2-digit', month: 'short', year: 'numeric'
                })}
              </p>
              <p className="text-[#F4F6FA] font-bold text-lg">
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
      <div className="flex gap-6 mt-4 text-sm flex-wrap justify-center">
        <span className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-full" style={{ backgroundColor: primaryColor }} />
          <span className="text-[#A7ADBC]">{primaryLabel}</span>
        </span>
        {showSecondary && (
          <span className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full" style={{ backgroundColor: secondaryColor }} />
            <span className="text-[#A7ADBC]">{secondaryLabel}</span>
          </span>
        )}
        {showThird && (
          <span className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full" style={{ backgroundColor: thirdColor }} />
            <span className="text-[#A7ADBC]">{thirdLabel}</span>
          </span>
        )}
      </div>
    </div>
  );
}