import { useState, useEffect, useRef, useCallback } from 'react';
import { Grid3X3 } from 'lucide-react';
import { getHeatmapData } from '../services/api';
import { useRealtimeData } from '../hooks/useRealtimeData';
import type { HeatmapStock, HeatmapSector } from '../services/api';

// Опции для фильтров
const COLOR_OPTIONS = [
  { value: 'change_1d', label: 'Изменение 1Д' },
  { value: 'change_1w', label: 'Изменение 1Н' },
  { value: 'change_1m', label: 'Изменение 1М' },
];

const GROUP_OPTIONS = [
  { value: 'sector', label: 'По секторам' },
  { value: 'none', label: 'Без группировки' },
];

// Squarify алгоритм для treemap
function squarify(
  items: { id: string; value: number; data: HeatmapStock }[],
  x: number,
  y: number,
  width: number,
  height: number
): { id: string; x: number; y: number; width: number; height: number; data: HeatmapStock }[] {
  if (items.length === 0 || width <= 0 || height <= 0) return [];

  const total = items.reduce((sum, item) => sum + item.value, 0);
  if (total === 0) return [];

  const sortedItems = [...items].sort((a, b) => b.value - a.value);
  const result: { id: string; x: number; y: number; width: number; height: number; data: HeatmapStock }[] = [];

  let currentX = x;
  let currentY = y;
  let remainingWidth = width;
  let remainingHeight = height;
  let remainingItems = [...sortedItems];
  let remainingTotal = total;

  while (remainingItems.length > 0) {
    const isHorizontal = remainingWidth >= remainingHeight;
    const side = isHorizontal ? remainingHeight : remainingWidth;

    let row: typeof items = [];
    let rowValue = 0;
    let bestRatio = Infinity;

    for (let i = 0; i < remainingItems.length; i++) {
      const testRow = remainingItems.slice(0, i + 1);
      const testValue = testRow.reduce((s, item) => s + item.value, 0);
      const rowLength = (testValue / remainingTotal) * (isHorizontal ? remainingWidth : remainingHeight);

      let worstRatio = 0;
      for (const item of testRow) {
        const itemSize = (item.value / testValue) * side;
        const ratio = Math.max(rowLength / itemSize, itemSize / rowLength);
        worstRatio = Math.max(worstRatio, ratio);
      }

      if (worstRatio <= bestRatio) {
        bestRatio = worstRatio;
        row = testRow;
        rowValue = testValue;
      } else {
        break;
      }
    }

    if (row.length === 0) break;

    const rowSize = (rowValue / remainingTotal) * (isHorizontal ? remainingWidth : remainingHeight);
    let offset = 0;

    for (const item of row) {
      const itemSize = (item.value / rowValue) * side;

      if (isHorizontal) {
        result.push({
          id: item.id,
          x: currentX,
          y: currentY + offset,
          width: rowSize,
          height: itemSize,
          data: item.data
        });
      } else {
        result.push({
          id: item.id,
          x: currentX + offset,
          y: currentY,
          width: itemSize,
          height: rowSize,
          data: item.data
        });
      }
      offset += itemSize;
    }

    if (isHorizontal) {
      currentX += rowSize;
      remainingWidth -= rowSize;
    } else {
      currentY += rowSize;
      remainingHeight -= rowSize;
    }

    remainingItems = remainingItems.slice(row.length);
    remainingTotal -= rowValue;
  }

  return result;
}

export default function HeatmapPage() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [sectors, setSectors] = useState<HeatmapSector[]>([]);
  const [allStocks, setAllStocks] = useState<HeatmapStock[]>([]);
  const [loading, setLoading] = useState(true);
  const [containerSize, setContainerSize] = useState({ width: 1200, height: 700 });
  const [lastUpdate, setLastUpdate] = useState<string>('');

  // Фильтры
  const sizeBy = 'market_cap';
  const [colorBy, setColorBy] = useState('change_1d');
  const [groupBy, setGroupBy] = useState('none');

  // Тултип
  const [tooltip, setTooltip] = useState<{
    visible: boolean;
    x: number;
    y: number;
    stock: HeatmapStock | null;
  }>({ visible: false, x: 0, y: 0, stock: null });

  // Измерение контейнера
  useEffect(() => {
    const updateSize = () => {
      if (containerRef.current) {
        const rect = containerRef.current.getBoundingClientRect();
        setContainerSize({
          width: rect.width || 1200,
          height: Math.max(600, window.innerHeight - 180)
        });
      }
    };

    updateSize();
    const timer = setTimeout(updateSize, 100);
    window.addEventListener('resize', updateSize);
    return () => {
      window.removeEventListener('resize', updateSize);
      clearTimeout(timer);
    };
  }, []);

  // Загрузка данных
  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getHeatmapData(sizeBy, colorBy, groupBy);
      setSectors(data.sectors);
      setAllStocks(data.stocks);

      const now = new Date();
      setLastUpdate(now.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }));
    } catch (error) {
      console.error('Error loading heatmap:', error);
    }
    setLoading(false);
  }, [sizeBy, colorBy, groupBy]);

  useEffect(() => { loadData(); }, [loadData]);

  // SSE: автоматическое обновление хитмапа
  useRealtimeData(['5min', 'mv_refresh'], loadData);

  // Получение значения для размера
  const getSizeValue = (stock: HeatmapStock): number => {
    const key = sizeBy as keyof HeatmapStock;
    return Math.max((stock[key] as number) || 1, 1);
  };

  // Получение значения для цвета
  const getColorValue = (stock: HeatmapStock): number => {
    const key = colorBy as keyof HeatmapStock;
    return (stock[key] as number) || 0;
  };

  // Цвета как на референсе
  const getColor = (change: number): string => {
    if (change >= 3) return '#16a34a';
    if (change >= 2) return '#22c55e';
    if (change >= 1) return '#4ade80';
    if (change >= 0.5) return '#86efac';
    if (change > 0) return '#bbf7d0';
    if (change === 0) return '#374151';
    if (change > -0.5) return '#fecaca';
    if (change > -1) return '#fca5a5';
    if (change > -2) return '#f87171';
    if (change > -3) return '#ef4444';
    return '#dc2626';
  };

  // Форматирование процента (с запятой)
  const formatPercent = (value: number): string => {
    return `${value.toFixed(2).replace('.', ',')}%`;
  };

  // Размер шрифта — максимально адаптивный под размер блока
  const getFontSize = (width: number, height: number): { ticker: number; percent: number } => {
    // Тикер занимает ~60% ширины блока (учитывая что обычно 4-5 символов)
    const tickerByWidth = Math.floor(width / 4.5);
    // Тикер не больше 40% высоты блока
    const tickerByHeight = Math.floor(height * 0.35);
    // Берём минимум, но не меньше 10 и не больше 48
    const ticker = Math.min(Math.max(Math.min(tickerByWidth, tickerByHeight), 10), 48);

    // Процент — примерно 70% от тикера
    const percent = Math.floor(ticker * 0.7);

    return { ticker, percent };
  };

  // Построение treemap
  const buildTreemap = () => {
    const gap = 3;

    if (groupBy === 'none') {
      const items = allStocks.map(stock => ({
        id: stock.secId,
        value: getSizeValue(stock),
        data: stock
      }));
      return { type: 'flat' as const, rects: squarify(items, gap, gap, containerSize.width - gap * 2, containerSize.height - gap * 2) };
    }

    // По секторам
    const sectorItems = sectors.map(sector => ({
      id: sector.name,
      value: sector.stocks.reduce((sum, s) => sum + getSizeValue(s), 0),
      stocks: sector.stocks
    }));

    const sectorRects = squarify(
      sectorItems.map(s => ({ id: s.id, value: s.value, data: null as unknown as HeatmapStock })),
      0, 0, containerSize.width, containerSize.height
    );

    const stockRects: { id: string; x: number; y: number; width: number; height: number; data: HeatmapStock; sector: string }[] = [];
    const sectorLabels: { name: string; x: number; y: number }[] = [];

    sectorRects.forEach((sectorRect, idx) => {
      const sector = sectorItems[idx];
      if (!sector) return;

      sectorLabels.push({
        name: sector.id,
        x: sectorRect.x + 6,
        y: sectorRect.y + 16
      });

      const stockItems = sector.stocks.map(stock => ({
        id: stock.secId,
        value: getSizeValue(stock),
        data: stock
      }));

      const rects = squarify(
        stockItems,
        sectorRect.x + gap,
        sectorRect.y + gap,
        sectorRect.width - gap * 2,
        sectorRect.height - gap * 2
      );

      rects.forEach(rect => {
        stockRects.push({ ...rect, sector: sector.id });
      });
    });

    return { type: 'grouped' as const, sectorRects, stockRects, sectorLabels };
  };

  const treemapData = !loading ? buildTreemap() : null;

  const renderStock = (rect: { id: string; x: number; y: number; width: number; height: number; data: HeatmapStock }, key: string) => {
    const change = getColorValue(rect.data);
    const fonts = getFontSize(rect.width, rect.height);
    const showTicker = rect.width > 25 && rect.height > 20;
    const showPercent = rect.width > 40 && rect.height > 35;
    const gap = 2;
    const radius = 6;

    return (
      <g
        key={key}
        style={{ cursor: 'pointer' }}
        onMouseEnter={(e) => {
          const svgRect = (e.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
          setTooltip({
            visible: true,
            x: svgRect.left + rect.x + rect.width / 2,
            y: svgRect.top + rect.y,
            stock: rect.data
          });
        }}
        onMouseLeave={() => setTooltip(prev => ({ ...prev, visible: false }))}
      >
        <rect
          x={rect.x + gap / 2}
          y={rect.y + gap / 2}
          width={Math.max(0, rect.width - gap)}
          height={Math.max(0, rect.height - gap)}
          rx={radius}
          ry={radius}
          fill={getColor(change)}
          className="transition-all duration-150 hover:brightness-110"
        />
        {showTicker && (
          <text
            x={rect.x + rect.width / 2}
            y={rect.y + rect.height / 2 - (showPercent ? fonts.percent * 0.8 : 0)}
            textAnchor="middle"
            dominantBaseline="middle"
            fill="white"
            fontSize={fonts.ticker}
            fontWeight="800"
            style={{
              textShadow: '0 2px 8px rgba(0,0,0,0.8), 0 1px 3px rgba(0,0,0,0.9)',
              letterSpacing: '-0.02em'
            }}
          >
            {rect.id}
          </text>
        )}
        {showPercent && (
          <text
            x={rect.x + rect.width / 2}
            y={rect.y + rect.height / 2 + fonts.ticker * 0.55}
            textAnchor="middle"
            dominantBaseline="middle"
            fill="white"
            fontSize={fonts.percent}
            fontWeight="700"
            style={{
              textShadow: '0 2px 6px rgba(0,0,0,0.8), 0 1px 2px rgba(0,0,0,0.9)'
            }}
          >
            {formatPercent(change)}
          </text>
        )}
      </g>
    );
  };

  return (
    <div className="max-w-full mx-auto px-2 md:px-4 py-3 md:py-4">
      {/* Заголовок и фильтры */}
      <div className="flex flex-wrap items-center justify-between gap-4 mb-4">
        <div className="flex items-center gap-3">
          <div className="p-3 bg-gradient-to-br from-[#22c55e] to-[#14b8a6] rounded-xl">
            <Grid3X3 className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-theme-primary">Карта рынка</h1>
            <p className="text-theme-secondary text-sm">Обновлено в {lastUpdate || '--:--'}</p>
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <select
            value={colorBy}
            onChange={(e) => setColorBy(e.target.value)}
            className="bg-theme-secondary border border-theme text-theme-primary px-3 py-2 rounded-lg text-sm cursor-pointer hover:border-[var(--border-hover)] focus:border-[var(--accent)] focus:outline-none"
          >
            {COLOR_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>

          <select
            value={groupBy}
            onChange={(e) => setGroupBy(e.target.value)}
            className="bg-theme-secondary border border-theme text-theme-primary px-3 py-2 rounded-lg text-sm cursor-pointer hover:border-[var(--border-hover)] focus:border-[var(--accent)] focus:outline-none"
          >
            {GROUP_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Карта */}
      <div
        ref={containerRef}
        className="relative rounded-lg overflow-hidden"
        style={{ height: containerSize.height }}
      >
        {loading ? (
          <div className="absolute inset-0 flex items-center justify-center text-slate-400">
            <div className="animate-spin w-8 h-8 border-2 border-[#C8FF2E] border-t-transparent rounded-full" />
          </div>
        ) : treemapData && treemapData.type === 'grouped' ? (
          <svg width={containerSize.width} height={containerSize.height}>
            {treemapData.stockRects.map((rect) => renderStock(rect, `${rect.sector}-${rect.id}`))}

            {/* Названия секторов */}
            {treemapData.sectorLabels.map((label) => (
              <text
                key={label.name}
                x={label.x}
                y={label.y}
                fill="rgba(255,255,255,0.9)"
                fontSize="12"
                fontWeight="600"
                style={{ textShadow: '0 1px 4px rgba(0,0,0,0.9), 0 0 8px rgba(0,0,0,0.8)' }}
              >
                {label.name}
              </text>
            ))}
          </svg>
        ) : treemapData && treemapData.type === 'flat' ? (
          <svg width={containerSize.width} height={containerSize.height}>
            {treemapData.rects.map((rect) => renderStock(rect, rect.id))}
          </svg>
        ) : null}
      </div>

      {/* Тултип */}
      {tooltip.visible && tooltip.stock && (
        <div
          className="fixed z-50 bg-theme-secondary border border-theme rounded-lg p-3 shadow-xl pointer-events-none"
          style={{
            left: Math.min(tooltip.x, window.innerWidth - 160),
            top: tooltip.y < 200 ? tooltip.y + 30 : tooltip.y - 10,
            transform: tooltip.y < 200 ? 'translate(-50%, 0)' : 'translate(-50%, -100%)'
          }}
        >
          <div className="font-bold text-white text-base">{tooltip.stock.name}</div>
          <div className="text-slate-400 text-sm mb-2">{tooltip.stock.secId} • {tooltip.stock.sector}</div>
          <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
            <span className="text-slate-400">Цена:</span>
            <span className="text-white font-medium">{tooltip.stock.price.toFixed(2)} ₽</span>
            <span className="text-slate-400">День:</span>
            <span className={`font-medium ${tooltip.stock.change_1d >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {formatPercent(tooltip.stock.change_1d)}
            </span>
            <span className="text-slate-400">Неделя:</span>
            <span className={`font-medium ${tooltip.stock.change_1w >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {formatPercent(tooltip.stock.change_1w)}
            </span>
            <span className="text-slate-400">Месяц:</span>
            <span className={`font-medium ${tooltip.stock.change_1m >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {formatPercent(tooltip.stock.change_1m)}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}