import { useMemo, useState, useRef, useEffect, useCallback } from 'react';
import { Download, BarChart2, TrendingUp } from 'lucide-react';

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
  formatSecondaryValue?: (value: number) => string;
  formatThirdValue?: (value: number) => string;
  formatTime?: (time: string) => string;
  loading?: boolean;
  primaryLabel?: string;
  secondaryLabel?: string;
  thirdLabel?: string;
  allowHistogram?: boolean;
  histogramDisabled?: boolean;
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
  formatSecondaryValue,
  formatThirdValue,
  formatTime = (t) => {
    const date = new Date(t);
    return date.toLocaleDateString('ru-RU', { day: '2-digit', month: 'short', year: '2-digit' });
  },
  loading = false,
  primaryLabel = 'Цена',
  secondaryLabel = 'OI',
  thirdLabel = '',
  allowHistogram = false,
  histogramDisabled = false,
}: SimpleChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const [width, setWidth] = useState(800);
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768);
  const [chartMode, setChartMode] = useState<'line' | 'histogram'>('line');

  useEffect(() => {
    if (histogramDisabled && chartMode === 'histogram') {
      setChartMode('line');
    }
  }, [histogramDisabled, chartMode]);

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

  // Opacity для OI линий (для fade-in эффекта)
  const [oiOpacity, setOiOpacity] = useState(1);

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
      setIsMobile(window.innerWidth < 768);
    };
    updateWidth();
    window.addEventListener('resize', updateWidth);
    return () => window.removeEventListener('resize', updateWidth);
  }, []);

  // Адаптивные отступы: на мобиле меньше
  const padding = isMobile
    ? { top: 30, right: 10, bottom: 40, left: 45 }
    : { top: 40, right: 90, bottom: 50, left: 80 };

  // На мобиле ограничиваем высоту графика
  const effectiveHeight = isMobile ? Math.min(height, 300) : height;
  const chartWidth = Math.max(width - padding.left - padding.right, 100);
  const chartHeight = effectiveHeight - padding.top - padding.bottom;

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

    // Y ticks (primary - price)
    const yTickCount = 5;
    const yTicks = Array.from({ length: yTickCount }, (_, i) => {
      const value = yMinVal + ((yMaxVal - yMinVal) * i) / (yTickCount - 1);
      return { value, y: scaleY(value) };
    });

    // Secondary Y ticks (OI - right axis)
    const secYTicks = allSecondaryValues.length > 0
      ? Array.from({ length: yTickCount }, (_, i) => {
        const value = secYMin + ((secYMax - secYMin) * i) / (yTickCount - 1);
        return { value, y: scaleSecondaryY(value) };
      })
      : [];

    // X ticks
    const xTickCount = Math.min(7, data.length);
    const xTicks = Array.from({ length: xTickCount }, (_, i) => {
      const index = Math.floor((i / Math.max(xTickCount - 1, 1)) * (data.length - 1));
      return { time: data[index].time, x: scaleX(index, data.length) };
    });

    return { points, secondaryPoints, thirdPoints, yTicks, secYTicks, xTicks };
  }, [data, secondaryData, thirdData, chartWidth, chartHeight, showSecondary, showThird]);

  // Анимация морфинга
  const animateMorph = useCallback(() => {
    if (loading || data.length === 0) return;

    const targetPrimary = targetCalc.points.map(p => ({ x: p.x, y: p.y }));
    const targetSecondary = targetCalc.secondaryPoints.map(p => ({ x: p.x, y: p.y }));
    const targetThird = targetCalc.thirdPoints.map(p => ({ x: p.x, y: p.y }));

    // Если первый рендер или нет предыдущих данных - показываем с fade
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

        // Анимируем primary от нижней границы графика
        const animatedPrimary = targetPrimary.map(p => ({
          x: p.x,
          y: chartHeight - (chartHeight - p.y) * eased,
        }));

        // OI линии показываем сразу, но с fade opacity
        setOiOpacity(eased);

        setAnimatedPaths({
          primary: pointsToPath(animatedPrimary),
          area: pointsToAreaPath(animatedPrimary, chartHeight),
          secondary: pointsToPath(targetSecondary),
          third: pointsToPath(targetThird),
        });

        if (t < 1) {
          animationRef.current = requestAnimationFrame(fadeIn);
        } else {
          setOiOpacity(1);
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

      // Интерполяция primary линии
      const interpolatedPrimary = interpolatePoints(fromPrimary, targetPrimary, eased);

      // Интерполяция secondary - только если есть откуда и куда
      let interpolatedSecondary: { x: number; y: number }[] = [];
      if (targetSecondary.length > 0) {
        if (fromSecondary.length > 0) {
          interpolatedSecondary = interpolatePoints(fromSecondary, targetSecondary, eased);
        } else {
          // Появление с fade
          interpolatedSecondary = targetSecondary;
          setOiOpacity(eased);
        }
      } else if (fromSecondary.length > 0) {
        // Исчезновение с fade
        interpolatedSecondary = fromSecondary;
        setOiOpacity(1 - eased);
      }

      // Интерполяция third - аналогично
      let interpolatedThird: { x: number; y: number }[] = [];
      if (targetThird.length > 0) {
        if (fromThird.length > 0) {
          interpolatedThird = interpolatePoints(fromThird, targetThird, eased);
        } else {
          interpolatedThird = targetThird;
        }
      } else if (fromThird.length > 0) {
        interpolatedThird = fromThird;
      }

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
        setOiOpacity(1);
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

  // Интерполяция точки на линии по X координате (ОПТИМИЗИРОВАНО: бинарный поиск O(log n))
  const interpolatePointOnLine = (points: typeof targetCalc.points, mouseX: number) => {
    if (points.length === 0) return null;
    if (points.length === 1) return points[0];

    // Бинарный поиск вместо линейного (O(log n) вместо O(n))
    let left = 0;
    let right = points.length - 1;

    // Если mouseX за пределами — вернуть ближайшую крайнюю точку
    if (mouseX <= points[0].x) return points[0];
    if (mouseX >= points[right].x) return points[right];

    while (left < right - 1) {
      const mid = Math.floor((left + right) / 2);
      if (points[mid].x <= mouseX) {
        left = mid;
      } else {
        right = mid;
      }
    }

    const p1 = points[left];
    const p2 = points[right];

    // Линейная интерполяция между найденными точками
    const t = (mouseX - p1.x) / (p2.x - p1.x);
    return {
      x: mouseX,
      y: p1.y + (p2.y - p1.y) * t,
      value: p1.value + (p2.value - p1.value) * t,
      time: p1.time, // Используем время ближайшей точки
    };
  };

  // Общая логика обновления tooltip по X-координате
  const updateTooltipAtX = useCallback((clientX: number, svgElement: SVGSVGElement) => {
    if (targetCalc.points.length === 0) return;

    const rect = svgElement.getBoundingClientRect();
    const mouseX = clientX - rect.left - padding.left;

    if (mouseX < 0 || mouseX > chartWidth) {
      setTooltip(prev => ({ ...prev, visible: false }));
      return;
    }

    const primaryPoint = interpolatePointOnLine(targetCalc.points, mouseX);
    const secondaryPoint = showSecondary ? interpolatePointOnLine(targetCalc.secondaryPoints, mouseX) : null;
    const thirdPoint = showThird ? interpolatePointOnLine(targetCalc.thirdPoints, mouseX) : null;

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
  }, [targetCalc, chartWidth, padding, showSecondary, showThird]);

  // Mouse events
  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    updateTooltipAtX(e.clientX, e.currentTarget);
  };

  const handleMouseLeave = () => {
    setTooltip((prev) => ({ ...prev, visible: false }));
  };

  // Touch events — для мобильных
  const handleTouchStart = (e: React.TouchEvent<SVGSVGElement>) => {
    if (e.touches.length === 1) {
      e.preventDefault(); // Предотвращаем скролл
      updateTooltipAtX(e.touches[0].clientX, e.currentTarget);
    }
  };

  const handleTouchMove = (e: React.TouchEvent<SVGSVGElement>) => {
    if (e.touches.length === 1) {
      e.preventDefault();
      updateTooltipAtX(e.touches[0].clientX, e.currentTarget);
    }
  };

  const handleTouchEnd = () => {
    // Скрываем tooltip через 1.5 сек после отпускания пальца
    setTimeout(() => {
      setTooltip((prev) => ({ ...prev, visible: false }));
    }, 1500);
  };

  // Показываем полный лоадер только если нет данных вообще
  if (data.length === 0 && loading) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-theme-secondary" style={{ height: effectiveHeight }}>
        <div className="flex items-center gap-3 text-theme-secondary">
          <div className="w-6 h-6 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
          <span className="text-lg">Загрузка...</span>
        </div>
      </div>
    );
  }

  if (data.length === 0 && !loading) {
    return (
      <div ref={containerRef} className="rounded-2xl flex items-center justify-center bg-theme-secondary" style={{ height: effectiveHeight }}>
        <p className="text-theme-secondary text-lg">Нет данных для отображения</p>
      </div>
    );
  }

  const currentValue = data[data.length - 1]?.value || 0;
  const firstValue = data[0]?.value || currentValue;
  const change = currentValue - firstValue;
  const changePercent = firstValue !== 0 ? (change / firstValue) * 100 : 0;
  const isPositive = change >= 0;

  return (
    <div ref={containerRef} className="rounded-2xl p-5 bg-theme-secondary border border-theme relative">
      {/* Кнопка переключения линия/гистограмма */}
      {allowHistogram && (
        <button
          onClick={() => !histogramDisabled && setChartMode(m => m === 'line' ? 'histogram' : 'line')}
          disabled={histogramDisabled}
          className={`absolute top-4 right-14 z-10 flex items-center justify-center w-9 h-9 bg-theme-tertiary/90 backdrop-blur-sm rounded-lg border border-theme transition-all duration-150 ease-out ${histogramDisabled
            ? 'text-theme-muted/40 cursor-not-allowed opacity-40'
            : 'text-theme-secondary hover:text-[#C8FF2E] hover:border-[#C8FF2E]/30 hover:scale-110 active:scale-95 active:bg-[#0B0D12]'
            }`}
          title={histogramDisabled ? 'Гистограмма недоступна в этом режиме' : chartMode === 'line' ? 'Переключить на гистограмму' : 'Переключить на линию'}
        >
          {chartMode === 'line' ? <BarChart2 size={18} /> : <TrendingUp size={18} />}
        </button>
      )}

      {/* Кнопка скачивания */}
      <button
        onClick={downloadChart}
        className="absolute top-4 right-4 z-10 flex items-center justify-center w-9 h-9 bg-theme-tertiary/90 backdrop-blur-sm rounded-lg border border-theme text-theme-secondary hover:text-[#C8FF2E] hover:border-[#C8FF2E]/30 hover:scale-110 active:scale-95 active:bg-[#0B0D12] transition-all duration-150 ease-out"
        title="Скачать график как PNG"
      >
        <Download size={18} />
      </button>

      {/* Маленький индикатор загрузки поверх графика */}
      {loading && (
        <div className={`absolute top-4 ${allowHistogram ? 'right-[6.5rem]' : 'right-16'} z-10 flex items-center gap-2 bg-theme-tertiary/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-theme`}>
          <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
          <span className="text-xs text-theme-secondary">Обновление...</span>
        </div>
      )}

      {/* Заголовок с текущим значением */}
      <div className="mb-2 flex items-baseline gap-4">
        <span className="text-4xl font-bold text-theme-primary tracking-tight">
          {formatValue(currentValue)}
        </span>
        <span className={`text-base font-semibold px-2 py-0.5 rounded-lg ${isPositive
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
        height={effectiveHeight}
        className="cursor-crosshair select-none"
        style={{ touchAction: 'none' }}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
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

          {/* Правая ось Y (OI) — скрыта на мобиле */}
          {!isMobile && showSecondary && targetCalc.secYTicks && targetCalc.secYTicks.map((tick, i) => (
            <text
              key={`sec-${i}`}
              x={chartWidth + 12}
              y={tick.y + 4}
              textAnchor="start"
              fill={secondaryColor}
              fontSize="11"
              fontWeight="500"
              opacity="0.8"
            >
              {tick.value.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
            </text>
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

            {/* Third данные — гистограмма или линия */}
            {showThird && chartMode === 'histogram' && targetCalc.thirdPoints.length > 0 && (
              <g opacity={oiOpacity} className="transition-opacity duration-300">
                {targetCalc.thirdPoints.map((p, i) => {
                  const barWidth = Math.max((chartWidth / targetCalc.thirdPoints.length) * 0.35, 1);
                  const barHeight = Math.max(chartHeight - p.y, 0);
                  return (
                    <rect
                      key={`third-bar-${i}`}
                      x={p.x - barWidth / 2 + barWidth * 0.5}
                      y={p.y}
                      width={barWidth}
                      height={barHeight}
                      fill={thirdColor}
                      fillOpacity={0.5}
                      stroke={thirdColor}
                      strokeWidth={0.5}
                    />
                  );
                })}
              </g>
            )}
            {showThird && chartMode === 'line' && animatedPaths.third && (
              <path
                d={animatedPaths.third}
                fill="none"
                stroke={thirdColor}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                opacity={oiOpacity}
              />
            )}

            {/* Secondary данные — гистограмма или линия */}
            {showSecondary && chartMode === 'histogram' && targetCalc.secondaryPoints.length > 0 && (
              <g opacity={oiOpacity} className="transition-opacity duration-300">
                {targetCalc.secondaryPoints.map((p, i) => {
                  const totalSeries = showThird ? 2 : 1;
                  const barWidth = Math.max((chartWidth / targetCalc.secondaryPoints.length) * 0.35, 1);
                  const barHeight = Math.max(chartHeight - p.y, 0);
                  return (
                    <rect
                      key={`sec-bar-${i}`}
                      x={p.x - barWidth / 2 - (totalSeries > 1 ? barWidth * 0.5 : 0)}
                      y={p.y}
                      width={barWidth}
                      height={barHeight}
                      fill={secondaryColor}
                      fillOpacity={0.5}
                      stroke={secondaryColor}
                      strokeWidth={0.5}
                    />
                  );
                })}
              </g>
            )}
            {showSecondary && chartMode === 'line' && animatedPaths.secondary && (
              <path
                d={animatedPaths.secondary}
                fill="none"
                stroke={secondaryColor}
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                opacity={oiOpacity}
              />
            )}

            {/* Основная линия (всегда линия) */}
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

        {/* Тултип: дата вверху вертикальной линии + карточка значений */}
        {tooltip.visible && (() => {
          const d = new Date(tooltip.time);
          const dateStr = d.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' });
          const hours = d.getHours();
          const minutes = d.getMinutes();
          const dateLabel = (hours !== 0 || minutes !== 0)
            ? `${dateStr} ${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`
            : dateStr;

          const cardWidth = isMobile ? 150 : 200;
          const isRightHalf = tooltip.x > padding.left + chartWidth / 2;
          const cardX = isRightHalf
            ? tooltip.x - cardWidth - 8
            : tooltip.x + 8;

          const fmtSecondary = formatSecondaryValue || formatValue;
          const fmtThird = formatThirdValue || formatValue;

          const lines: { color: string; label: string; value: string }[] = [
            { color: primaryColor, label: primaryLabel, value: formatValue(tooltip.value) },
          ];
          if (showSecondary && tooltip.secondaryValue !== undefined) {
            lines.push({ color: secondaryColor, label: secondaryLabel, value: fmtSecondary(tooltip.secondaryValue) });
          }
          if (showThird && tooltip.thirdValue !== undefined) {
            lines.push({ color: thirdColor, label: thirdLabel, value: fmtThird(tooltip.thirdValue) });
          }

          const cardHeight = 8 + lines.length * 26 + 8;

          return (
            <>
              {/* Дата — вверху вертикальной линии */}
              <foreignObject
                x={Math.min(Math.max(tooltip.x - 55, padding.left), width - padding.right - 110)}
                y={padding.top - 24}
                width="110"
                height="22"
              >
                <div className="flex justify-center pointer-events-none">
                  <span className="text-[11px] text-theme-secondary bg-theme-tertiary/90 backdrop-blur-sm px-2 py-0.5 rounded border border-theme whitespace-nowrap">
                    {dateLabel}
                  </span>
                </div>
              </foreignObject>

              {/* Карточка значений рядом с курсором */}
              <foreignObject
                x={cardX}
                y={Math.min(Math.max(tooltip.primaryY - cardHeight / 2, padding.top), padding.top + chartHeight - cardHeight)}
                width={cardWidth}
                height={cardHeight}
              >
                <div className="bg-theme-tertiary/95 backdrop-blur-sm rounded-lg border border-theme shadow-xl pointer-events-none py-1.5 px-3">
                  {lines.map((line, i) => (
                    <div key={i} className="flex items-center justify-between gap-2 py-0.5">
                      <div className="flex items-center gap-1.5 min-w-0">
                        <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: line.color }} />
                        <span className="text-[11px] text-theme-secondary truncate">{line.label}</span>
                      </div>
                      <span className="text-xs font-semibold text-theme-primary whitespace-nowrap">{line.value}</span>
                    </div>
                  ))}
                </div>
              </foreignObject>
            </>
          );
        })()}
      </svg>

      {/* Легенда */}
      <div className="flex gap-6 mt-4 text-sm flex-wrap justify-center">
        <span className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-full" style={{ backgroundColor: primaryColor }} />
          <span className="text-theme-secondary">{primaryLabel}</span>
        </span>
        {showSecondary && (
          <span className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full" style={{ backgroundColor: secondaryColor }} />
            <span className="text-theme-secondary">{secondaryLabel}</span>
          </span>
        )}
        {showThird && (
          <span className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full" style={{ backgroundColor: thirdColor }} />
            <span className="text-theme-secondary">{thirdLabel}</span>
          </span>
        )}
      </div>
    </div >
  );
}