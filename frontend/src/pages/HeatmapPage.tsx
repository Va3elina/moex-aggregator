import { useState, useEffect, useRef, useCallback } from 'react';
import { Grid3X3 } from 'lucide-react';
import { getHeatmapData, getHeatmapImoex } from '../services/api';
import { useRealtimeData } from '../hooks/useRealtimeData';
import type { HeatmapStock, HeatmapSector } from '../services/api';

// Опции для фильтров
const COLOR_OPTIONS = [
  { value: 'change_1d', label: 'Изменение 1Д' },
  { value: 'change_1w', label: 'Изменение 1Н' },
  { value: 'change_1m', label: 'Изменение 1М' },
  { value: 'change_1y', label: 'Изменение 1Г' },
];

const SIZE_OPTIONS = [
  { value: 'market_cap', label: 'Капитализация' },
  { value: 'value_1d', label: 'Оборот 1Д' },
  { value: 'value_1w', label: 'Оборот 1Н' },
  { value: 'value_1m', label: 'Оборот 1М' },
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
  const [mapMode, setMapMode] = useState<'imoex' | 'all'>('imoex');
  const [sizeBy, setSizeBy] = useState<string>('market_cap');
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
          height: Math.max(window.innerWidth < 768 ? 400 : 600, window.innerHeight - (window.innerWidth < 768 ? 120 : 180))
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

  // Загрузка данных — не зависит от colorBy/sizeBy (все метрики приходят в одном ответе)
  const hasDataRef = useRef(false);
  const loadData = useCallback(async () => {
    if (!hasDataRef.current) setLoading(true);
    try {
      const data = mapMode === 'imoex'
        ? await getHeatmapImoex('change_1d', groupBy)
        : await getHeatmapData('market_cap', 'change_1d', groupBy);
      setSectors(data.sectors);
      setAllStocks(data.stocks);
      setLastUpdate(data.updated_at || new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }));
      hasDataRef.current = true;
    } catch (error) {
      console.error('Error loading heatmap:', error);
    }
    setLoading(false);
  }, [mapMode, groupBy]);

  // Первая загрузка + при смене mapMode/groupBy
  useEffect(() => { loadData(); }, [loadData]);

  // При смене colorBy НЕ перезагружаем данные — просто перерисовка через React
  // (все change_1d/1w/1m/1y уже есть в allStocks)

  // SSE: автоматическое обновление хитмапа
  useRealtimeData(['5min', 'mv_refresh'], loadData);

  // Получение значения для размера (экспонента усиливает разницу крупных/мелких)
  const getSizeValue = (stock: HeatmapStock): number => {
    const key = sizeBy as keyof HeatmapStock;
    const raw = Math.max((stock[key] as number) || 1, 1);
    return Math.pow(raw, 0.65);
  };

  // Получение значения для цвета
  const getColorValue = (stock: HeatmapStock): number => {
    const key = colorBy as keyof HeatmapStock;
    return (stock[key] as number) || 0;
  };

  // Цвета в стиле Finviz: тёмный центр → насыщенный (но не неоновый) на краях
  const getColor = (change: number): string => {
    const maxChange = colorBy === 'change_1y' ? 20 : colorBy === 'change_1m' ? 5 : colorBy === 'change_1w' ? 2 : 0.8;
    const abs = Math.abs(change);
    const t = Math.min(abs / maxChange, 1); // 0..1

    if (change > 0) {
      // #2a2a2a → #245528 → #2d8c2d → #30a830 (Finviz green)
      const r = Math.round(42 - t * 18);          // 42 → 24
      const g = Math.round(42 + t * (168 - 42));   // 42 → 168
      const b = Math.round(42 - t * 18);           // 42 → 24
      return `rgb(${r},${g},${b})`;
    }
    if (change < 0) {
      // #2a2a2a → #6b2828 → #b82020 → #dc2626 (Finviz red)
      const r = Math.round(42 + t * (220 - 42));   // 42 → 220
      const g = Math.round(42 - t * 4);            // 42 → 38
      const b = Math.round(42 - t * 4);            // 42 → 38
      return `rgb(${r},${g},${b})`;
    }
    return '#2a2a2a';
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
      const items = allStocks
        .filter(stock => stock.change_1d !== 0 || stock.change_1w !== 0 || stock.change_1m !== 0)
        .map(stock => ({
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
    const sectorLabels: { name: string; x: number; y: number; width: number; height: number }[] = [];

    sectorRects.forEach((sectorRect, idx) => {
      const sector = sectorItems[idx];
      if (!sector) return;

      const headerH = 18;
      sectorLabels.push({
        name: sector.id,
        x: sectorRect.x,
        y: sectorRect.y,
        width: sectorRect.width,
        height: headerH,
      });

      const stockItems = sector.stocks
        .filter(stock => stock.change_1d !== 0 || stock.change_1w !== 0 || stock.change_1m !== 0)
        .map(stock => ({
          id: stock.secId,
          value: getSizeValue(stock),
          data: stock
        }));

      const rects = squarify(
        stockItems,
        sectorRect.x + gap,
        sectorRect.y + headerH,
        sectorRect.width - gap * 2,
        sectorRect.height - headerH - gap
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
    const showTicker = rect.width > 18 && rect.height > 14;
    const showPercent = rect.width > 30 && rect.height > 25;
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
          style={{ transition: 'fill 0.6s ease, x 0.5s ease, y 0.5s ease, width 0.5s ease, height 0.5s ease' }}
          className="hover:brightness-110"
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
              letterSpacing: '-0.02em',
              transition: 'x 0.5s ease, y 0.5s ease, font-size 0.5s ease',
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
          <div className="flex rounded-lg overflow-hidden border border-theme">
            <button
              onClick={() => setMapMode('imoex')}
              className={`px-3 py-2 text-sm font-medium transition-colors ${
                mapMode === 'imoex'
                  ? 'bg-[var(--accent)] text-black'
                  : 'bg-theme-secondary text-theme-primary hover:bg-theme-tertiary'
              }`}
            >
              Индекс IMOEX
            </button>
            <button
              onClick={() => setMapMode('all')}
              className={`px-3 py-2 text-sm font-medium transition-colors ${
                mapMode === 'all'
                  ? 'bg-[var(--accent)] text-black'
                  : 'bg-theme-secondary text-theme-primary hover:bg-theme-tertiary'
              }`}
            >
              Все акции
            </button>
          </div>

          <select
            value={sizeBy}
            onChange={(e) => setSizeBy(e.target.value)}
            className="bg-theme-secondary border border-theme text-theme-primary px-3 py-2 rounded-lg text-sm cursor-pointer hover:border-[var(--border-hover)] focus:border-[var(--accent)] focus:outline-none"
          >
            {SIZE_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>

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
            <div className="flex flex-col items-center gap-3">
              <div className="animate-spin w-8 h-8 border-2 border-[#C8FF2E] border-t-transparent rounded-full" />
              <span className="text-sm text-theme-secondary">Загрузка карты...</span>
            </div>
          </div>
        ) : treemapData && treemapData.type === 'grouped' ? (
          <svg width={containerSize.width} height={containerSize.height}
            className="animate-in fade-in duration-500">
            {treemapData.stockRects.map((rect) => renderStock(rect, `${rect.sector}-${rect.id}`))}

            {/* Заголовки секторов — отдельная полоска */}
            {treemapData.sectorLabels.map((label, i) => (
              <g key={label.name}>
                <rect
                  x={label.x + 3} y={label.y}
                  width={label.width - 6} height={label.height}
                  rx={3}
                  fill="var(--bg-secondary)"
                />
                <clipPath id={`sector-clip-${i}`}>
                  <rect x={label.x + 3} y={label.y} width={label.width - 6} height={label.height} />
                </clipPath>
                <text
                  x={label.x + 9}
                  y={label.y + label.height / 2}
                  dominantBaseline="central"
                  fill="rgba(255,255,255,0.7)"
                  fontSize="11"
                  fontWeight="500"
                  clipPath={`url(#sector-clip-${i})`}
                >
                  {label.name}
                </text>
              </g>
            ))}
          </svg>
        ) : treemapData && treemapData.type === 'flat' ? (
          <svg width={containerSize.width} height={containerSize.height}
            className="animate-in fade-in duration-500">
            {treemapData.rects.map((rect) => renderStock(rect, rect.id))}
          </svg>
        ) : null}
      </div>

      {/* Тултип */}
      {tooltip.visible && tooltip.stock && (
        <div
          className="fixed z-50 bg-[#1A1F2E]/95 backdrop-blur-sm border border-white/10 rounded-xl py-3 px-5 shadow-2xl pointer-events-none"
          style={{
            left: Math.min(Math.max(tooltip.x, 180), window.innerWidth - 180),
            top: tooltip.y < 200 ? tooltip.y + 25 : tooltip.y - 10,
            transform: tooltip.y < 200 ? 'translate(-50%, 0)' : 'translate(-50%, -100%)'
          }}
        >
          <div className="flex items-center gap-3 mb-2">
            <span className="font-bold text-white text-[15px]">{tooltip.stock.secId}</span>
            <span className="text-[13px] text-slate-400">{tooltip.stock.name}</span>
            <span className="text-[13px] text-white font-semibold ml-auto">{tooltip.stock.price.toFixed(2)} ₽</span>
          </div>
          <div className="flex items-center gap-4 text-[13px]">
            {[
              { label: 'Д', value: tooltip.stock.change_1d },
              { label: 'Н', value: tooltip.stock.change_1w },
              { label: 'М', value: tooltip.stock.change_1m },
              { label: 'Г', value: tooltip.stock.change_1y },
            ].map(({ label, value }) => (
              <span key={label} className="flex items-center gap-1.5">
                <span className="text-slate-500">{label}</span>
                <span className={`font-semibold ${value >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {formatPercent(value)}
                </span>
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}