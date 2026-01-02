import { useState, useEffect, useRef } from 'react';
import { getHeatmapData } from '../services/api';
import type { HeatmapStock, HeatmapSector } from '../services/api';

// Опции для фильтров
const SIZE_OPTIONS = [
  { value: 'value_1d', label: 'Оборот 1Д' },
  { value: 'value_1w', label: 'Оборот 1Н' },
  { value: 'value_1m', label: 'Оборот 1М' },
  { value: 'volume_1d', label: 'Объём 1Д' },
  { value: 'volume_1w', label: 'Объём 1Н' },
  { value: 'volume_1m', label: 'Объём 1М' },
];

const COLOR_OPTIONS = [
  { value: 'change_1d', label: 'Изменение 1Д' },
  { value: 'change_1w', label: 'Изменение 1Н' },
  { value: 'change_1m', label: 'Изменение 1М' },
];

const GROUP_OPTIONS = [
  { value: 'sector', label: 'По секторам' },
  { value: 'none', label: 'Без групп' },
];

// Улучшенный Treemap алгоритм
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

  // Сортируем по убыванию для лучшего распределения
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

    // Находим оптимальную строку
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

    // Размещаем строку
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

    // Обновляем оставшееся пространство
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

  // Фильтры
  const [sizeBy, setSizeBy] = useState('value_1d');
  const [colorBy, setColorBy] = useState('change_1d');
  const [groupBy, setGroupBy] = useState('sector');

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
  useEffect(() => {
    loadData();
  }, [sizeBy, colorBy, groupBy]);

  const loadData = async () => {
    setLoading(true);
    try {
      const data = await getHeatmapData(sizeBy, colorBy, groupBy);
      setSectors(data.sectors);
      setAllStocks(data.stocks);
    } catch (error) {
      console.error('Error loading heatmap:', error);
    }
    setLoading(false);
  };

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

  // Цвета как у TradingView
  const getColor = (change: number): string => {
    if (change >= 3) return '#00873c';
    if (change >= 2) return '#00a344';
    if (change >= 1) return '#00c853';
    if (change >= 0.5) return '#26a69a';
    if (change > 0) return '#4db6ac';
    if (change === 0) return '#546e7a';
    if (change > -0.5) return '#ef9a9a';
    if (change > -1) return '#e57373';
    if (change > -2) return '#ef5350';
    if (change > -3) return '#e53935';
    return '#c62828';
  };

  // Форматирование
  const formatPercent = (value: number): string => {
    return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
  };

  // Расчёт размера шрифта на основе размера блока
  const getFontSize = (width: number, height: number): { ticker: number; percent: number; price: number } => {
    const minDim = Math.min(width, height);
    const area = width * height;

    if (area > 40000) return { ticker: 18, percent: 15, price: 12 };
    if (area > 20000) return { ticker: 16, percent: 13, price: 11 };
    if (area > 10000) return { ticker: 14, percent: 12, price: 10 };
    if (area > 5000) return { ticker: 13, percent: 11, price: 9 };
    if (minDim > 50) return { ticker: 12, percent: 10, price: 8 };
    if (minDim > 35) return { ticker: 10, percent: 9, price: 0 };
    return { ticker: 9, percent: 8, price: 0 };
  };

  // Построение treemap
  const buildTreemap = () => {
    const gap = 1; // Минимальный зазор между блоками

    if (groupBy === 'none') {
      const items = allStocks.map(stock => ({
        id: stock.secId,
        value: getSizeValue(stock),
        data: stock
      }));
      return { type: 'flat' as const, rects: squarify(items, gap, gap, containerSize.width - gap * 2, containerSize.height - gap * 2) };
    }

    // Распределяем место между секторами
    const sectorItems = sectors.map(sector => ({
      id: sector.name,
      value: sector.stocks.reduce((sum, s) => sum + getSizeValue(s), 0),
      stocks: sector.stocks
    }));

    const sectorRects = squarify(
      sectorItems.map(s => ({ id: s.id, value: s.value, data: null as unknown as HeatmapStock })),
      0, 0, containerSize.width, containerSize.height
    );

    // Внутри каждого сектора распределяем акции
    const stockRects: { id: string; x: number; y: number; width: number; height: number; data: HeatmapStock; sector: string }[] = [];
    const sectorLabels: { name: string; x: number; y: number }[] = [];

    sectorRects.forEach((sectorRect, idx) => {
      const sector = sectorItems[idx];
      if (!sector) return;

      // Добавляем label сектора
      sectorLabels.push({
        name: sector.id,
        x: sectorRect.x + 4,
        y: sectorRect.y + 14
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

  return (
    <div className="max-w-full mx-auto px-4 py-4">
      {/* Заголовок и фильтры */}
      <div className="flex flex-wrap items-center justify-between gap-4 mb-4">
        <h1 className="text-2xl font-bold text-white">Карта рынка</h1>

        <div className="flex flex-wrap gap-2">
          <select
            value={sizeBy}
            onChange={(e) => setSizeBy(e.target.value)}
            className="bg-slate-800 border border-slate-600 text-white px-3 py-2 rounded-lg text-sm cursor-pointer hover:border-slate-500"
          >
            {SIZE_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>

          <select
            value={colorBy}
            onChange={(e) => setColorBy(e.target.value)}
            className="bg-slate-800 border border-slate-600 text-white px-3 py-2 rounded-lg text-sm cursor-pointer hover:border-slate-500"
          >
            {COLOR_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>

          <select
            value={groupBy}
            onChange={(e) => setGroupBy(e.target.value)}
            className="bg-slate-800 border border-slate-600 text-white px-3 py-2 rounded-lg text-sm cursor-pointer hover:border-slate-500"
          >
            {GROUP_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Легенда */}
      <div className="flex items-center justify-center gap-2 mb-3 text-xs">
        <span className="text-red-400 font-medium">-3%</span>
        <div className="w-52 h-2.5 rounded-sm" style={{
          background: 'linear-gradient(to right, #c62828, #e53935, #ef5350, #546e7a, #4db6ac, #00c853, #00873c)'
        }} />
        <span className="text-green-400 font-medium">+3%</span>
      </div>

      {/* Карта */}
      <div
        ref={containerRef}
        className="relative bg-[#1a1a2e] rounded-lg overflow-hidden"
        style={{ height: containerSize.height }}
      >
        {loading ? (
          <div className="absolute inset-0 flex items-center justify-center text-slate-400">
            <div className="animate-spin w-8 h-8 border-2 border-emerald-500 border-t-transparent rounded-full" />
          </div>
        ) : treemapData && treemapData.type === 'grouped' ? (
          <svg width={containerSize.width} height={containerSize.height}>
            {/* Акции */}
            {treemapData.stockRects.map((rect) => {
              const change = getColorValue(rect.data);
              const fonts = getFontSize(rect.width, rect.height);
              const showTicker = rect.width > 30 && rect.height > 25;
              const showPercent = rect.width > 45 && rect.height > 35;
              const showPrice = fonts.price > 0 && rect.width > 60 && rect.height > 50;

              return (
                <g
                  key={`${rect.sector}-${rect.id}`}
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
                    x={rect.x + 0.5}
                    y={rect.y + 0.5}
                    width={Math.max(0, rect.width - 1)}
                    height={Math.max(0, rect.height - 1)}
                    fill={getColor(change)}
                    className="transition-all duration-150 hover:brightness-110"
                  />
                  {showTicker && (
                    <text
                      x={rect.x + rect.width / 2}
                      y={rect.y + rect.height / 2 - (showPercent ? fonts.percent * 0.6 : 0)}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="white"
                      fontSize={fonts.ticker}
                      fontWeight="700"
                      style={{ textShadow: '0 1px 3px rgba(0,0,0,0.7)' }}
                    >
                      {rect.id}
                    </text>
                  )}
                  {showPercent && (
                    <text
                      x={rect.x + rect.width / 2}
                      y={rect.y + rect.height / 2 + fonts.ticker * 0.5}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="white"
                      fontSize={fonts.percent}
                      fontWeight="500"
                      style={{ textShadow: '0 1px 3px rgba(0,0,0,0.7)' }}
                    >
                      {formatPercent(change)}
                    </text>
                  )}
                  {showPrice && (
                    <text
                      x={rect.x + rect.width / 2}
                      y={rect.y + rect.height / 2 + fonts.ticker * 0.5 + fonts.percent + 2}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="rgba(255,255,255,0.75)"
                      fontSize={fonts.price}
                      style={{ textShadow: '0 1px 2px rgba(0,0,0,0.5)' }}
                    >
                      {rect.data.price.toFixed(2)}₽
                    </text>
                  )}
                </g>
              );
            })}

            {/* Названия секторов поверх */}
            {treemapData.sectorLabels.map((label) => (
              <text
                key={label.name}
                x={label.x}
                y={label.y}
                fill="rgba(255,255,255,0.9)"
                fontSize="11"
                fontWeight="600"
                style={{ textShadow: '0 1px 4px rgba(0,0,0,0.9), 0 0 8px rgba(0,0,0,0.8)' }}
              >
                {label.name}
              </text>
            ))}
          </svg>
        ) : treemapData && treemapData.type === 'flat' ? (
          <svg width={containerSize.width} height={containerSize.height}>
            {treemapData.rects.map((rect) => {
              const change = getColorValue(rect.data);
              const fonts = getFontSize(rect.width, rect.height);
              const showTicker = rect.width > 30 && rect.height > 25;
              const showPercent = rect.width > 45 && rect.height > 35;
              const showPrice = fonts.price > 0 && rect.width > 60 && rect.height > 50;

              return (
                <g
                  key={rect.id}
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
                    x={rect.x + 0.5}
                    y={rect.y + 0.5}
                    width={Math.max(0, rect.width - 1)}
                    height={Math.max(0, rect.height - 1)}
                    fill={getColor(change)}
                    className="transition-all duration-150 hover:brightness-110"
                  />
                  {showTicker && (
                    <text
                      x={rect.x + rect.width / 2}
                      y={rect.y + rect.height / 2 - (showPercent ? fonts.percent * 0.6 : 0)}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="white"
                      fontSize={fonts.ticker}
                      fontWeight="700"
                      style={{ textShadow: '0 1px 3px rgba(0,0,0,0.7)' }}
                    >
                      {rect.id}
                    </text>
                  )}
                  {showPercent && (
                    <text
                      x={rect.x + rect.width / 2}
                      y={rect.y + rect.height / 2 + fonts.ticker * 0.5}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="white"
                      fontSize={fonts.percent}
                      fontWeight="500"
                      style={{ textShadow: '0 1px 3px rgba(0,0,0,0.7)' }}
                    >
                      {formatPercent(change)}
                    </text>
                  )}
                  {showPrice && (
                    <text
                      x={rect.x + rect.width / 2}
                      y={rect.y + rect.height / 2 + fonts.ticker * 0.5 + fonts.percent + 2}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fill="rgba(255,255,255,0.75)"
                      fontSize={fonts.price}
                    >
                      {rect.data.price.toFixed(2)}₽
                    </text>
                  )}
                </g>
              );
            })}
          </svg>
        ) : null}
      </div>

      {/* Тултип */}
      {tooltip.visible && tooltip.stock && (
        <div
          className="fixed z-50 bg-slate-800/95 backdrop-blur border border-slate-600 rounded-lg p-3 shadow-2xl pointer-events-none"
          style={{
            left: tooltip.x,
            top: tooltip.y - 8,
            transform: 'translate(-50%, -100%)'
          }}
        >
          <div className="font-bold text-white text-base">{tooltip.stock.name}</div>
          <div className="text-slate-400 text-sm mb-2">{tooltip.stock.secId} • {tooltip.stock.sector}</div>
          <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
            <span className="text-slate-400">Цена:</span>
            <span className="text-white font-medium">{tooltip.stock.price.toFixed(2)} ₽</span>
            <span className="text-slate-400">1Д:</span>
            <span className={`font-medium ${tooltip.stock.change_1d >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {formatPercent(tooltip.stock.change_1d)}
            </span>
            <span className="text-slate-400">1Н:</span>
            <span className={`font-medium ${tooltip.stock.change_1w >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {formatPercent(tooltip.stock.change_1w)}
            </span>
            <span className="text-slate-400">1М:</span>
            <span className={`font-medium ${tooltip.stock.change_1m >= 0 ? 'text-green-400' : 'text-red-400'}`}>
              {formatPercent(tooltip.stock.change_1m)}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}