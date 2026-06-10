import { useState, useEffect, useRef, useCallback } from 'react';
import { Grid3X3 } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import Dropdown, { type DropdownOption } from '../components/Dropdown';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import CsvExportButton from '../components/export/CsvExportButton';
import ChartWatermark from '../components/ChartWatermark';
import { METHODOLOGY } from '../data/methodology';
import { getHeatmapData, getHeatmapImoex } from '../services/api';
import { useRealtimeData } from '../hooks/useRealtimeData';
import type { HeatmapStock, HeatmapSector } from '../services/api';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { heatmapTourSteps } from '../data/tours/heatmap';
import { useTierAccess } from '../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';
import { handleTierError } from '../utils/tierError';
import { usePersistedState } from '../hooks/usePersistedState';

// Опции для фильтров
const PERIOD_OPTIONS = [
  { value: '1d', label: '1Д', color: 'change_1d', volume: 'value_1d' },
  { value: '1w', label: '1Н', color: 'change_1w', volume: 'value_1w' },
  { value: '1m', label: '1М', color: 'change_1m', volume: 'value_1m' },
  { value: '1y', label: '1Г', color: 'change_1y', volume: 'value_1m' },
];

const SIZE_OPTIONS = [
  { value: 'market_cap', label: 'Капитализация' },
  { value: 'volume', label: 'Оборот' },
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
  // Outer paper-card ref — used for capture (включает watermark в snapshot)
  const captureRef = useRef<HTMLDivElement>(null);
  const [sectors, setSectors] = useState<HeatmapSector[]>([]);
  const [allStocks, setAllStocks] = useState<HeatmapStock[]>([]);
  const [loading, setLoading] = useState(true);
  const [containerSize, setContainerSize] = useState({ width: 1200, height: 700 });
  const [lastUpdate, setLastUpdate] = useState<string>('');

  // Кэш prev_close для real-time пересчёта change_1d
  const prevCloseMap = useRef<Record<string, number>>({});

  // Tier-gating: Free → только IMOEX, Basic/Pro → + Все акции
  const heatAccess = useTierAccess('heatmap');
  const { showUpgrade } = useUpgradePrompt();

  // Фильтры. mapMode/groupBy персистим в localStorage — чтобы режим карты
  // («Индекс IMOEX» / «Все акции») и группировка не сбрасывались на новой сессии.
  // sizeBy/period остаются session-only (по умолчанию market_cap / 1Д).
  const [mapMode, setMapMode] = usePersistedState<'imoex' | 'all'>('frame:heatmap:mapMode', 'imoex');
  const [sizeBy, setSizeBy] = useState<string>('market_cap');
  const [period, setPeriod] = useState('1d');
  const [groupBy, setGroupBy] = usePersistedState<string>('frame:heatmap:groupBy', 'sector');

  // Onboarding tour
  const tour = useOnboardingTour('heatmap');

  // Вычисляемые значения из периода
  const periodConfig = PERIOD_OPTIONS.find(p => p.value === period) || PERIOD_OPTIONS[0];
  const colorBy = periodConfig.color;
  const volumeKey = periodConfig.volume;

  // Тултип
  const [tooltip, setTooltip] = useState<{
    visible: boolean;
    x: number;
    y: number;
    stock: HeatmapStock | null;
  }>({ visible: false, x: 0, y: 0, stock: null });

  // Измерение контейнера.
  //
  // Bug history: раньше слушали `window.resize`, который на мобиле срабатывает
  // при сворачивании/разворачивании toolbar браузера на скролле — это меняло
  // window.innerHeight и пересчитывало layout, плитки прыгали при скролле.
  //
  // Fix: слушаем только реальные изменения размера контейнера через
  // ResizeObserver (он не срабатывает на mobile chrome resize, только на
  // настоящие layout-changes) + orientationchange для поворотов телефона.
  // Высота пересчитывается ТОЛЬКО при смене ширины (orientation/breakpoint),
  // на скролле остаётся стабильной.
  useEffect(() => {
    const node = containerRef.current;
    if (!node) return;

    const updateSize = () => {
      if (!containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const newWidth = rect.width || 1200;
      setContainerSize(prev => {
        const widthChanged = Math.abs(newWidth - prev.width) > 1;
        const newHeight = widthChanged
          ? Math.max(
              window.innerWidth < 768 ? 400 : 600,
              window.innerHeight - (window.innerWidth < 768 ? 120 : 180)
            )
          : prev.height;
        return { width: newWidth, height: newHeight };
      });
    };

    updateSize();
    const timer = setTimeout(updateSize, 100);
    const ro = new ResizeObserver(updateSize);
    ro.observe(node);
    window.addEventListener('orientationchange', updateSize);
    return () => {
      ro.disconnect();
      window.removeEventListener('orientationchange', updateSize);
      clearTimeout(timer);
    };
  }, []);

  // Загрузка данных — не зависит от colorBy/sizeBy (все метрики приходят в одном ответе)
  const hasDataRef = useRef(false);
  // Stale-guard: при быстром переключении mapMode/groupBy медленный ранний ответ
  // мог перезаписать свежий. reqId фиксирует «последний» запрос.
  const reqIdRef = useRef(0);
  const loadData = useCallback(async () => {
    const reqId = ++reqIdRef.current;
    const isStale = () => reqId !== reqIdRef.current;
    if (!hasDataRef.current) setLoading(true);
    try {
      const data = mapMode === 'imoex'
        ? await getHeatmapImoex('change_1d', groupBy)
        : await getHeatmapData('market_cap', 'change_1d', groupBy);
      if (isStale()) return;
      setSectors(data.sectors);
      setAllStocks(data.stocks);
      // Кэшируем prev_close для real-time пересчёта
      data.stocks.forEach((s: HeatmapStock) => {
        if (s.prev_close > 0) prevCloseMap.current[s.secId] = s.prev_close;
      });
      setLastUpdate(data.updated_at || new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }));
      hasDataRef.current = true;
    } catch (error) {
      if (isStale()) return;
      console.error('Error loading heatmap:', error);
      handleTierError(error, {
        showUpgrade,
        indicator: 'heatmap',
        featureName: 'режим «Все акции»',
      });
    }
    if (!isStale()) setLoading(false);
  }, [mapMode, groupBy, showUpgrade]);

  // Tier-guard для восстановленного режима: если из localStorage пришёл
  // mapMode='all', но тариф его не разрешает (Free → только IMOEX) — откатываем
  // на 'imoex'. Иначе Free-юзер залип бы на недоступном 'all' (пустая карта +
  // 403). Ждём загрузки матрицы тарифа (isLoading), чтобы не сбросить раньше
  // времени. Срабатывает один раз после гидрации tier-данных.
  useEffect(() => {
    if (heatAccess.isLoading) return;
    if (mapMode === 'all' && !heatAccess.canUseMode('all')) {
      setMapMode('imoex');
    }
  }, [heatAccess.isLoading, mapMode, heatAccess, setMapMode]);

  // Первая загрузка + при смене mapMode/groupBy
  useEffect(() => { loadData(); }, [loadData]);

  // SSE: при обновлении MV — загружаем свежие цены (lightweight) + полный reload раз в 15 мин
  const fullReloadCounter = useRef(0);
  useRealtimeData(['5min', 'mv_refresh'], async () => {
    fullReloadCounter.current += 1;
    // Каждый 3й раз (раз в ~15 мин) — полный reload для change_1w/1m/1y
    if (fullReloadCounter.current % 3 === 0) {
      await loadData();
      return;
    }
    // Остальное время — только цены (lightweight)
    try {
      const resp = await fetch('/api/heatmap/prices');
      if (!resp.ok) return;
      const prices: Record<string, number> = await resp.json();
      setAllStocks(prev => prev.map(stock => {
        const newPrice = prices[stock.secId];
        const prevClose = prevCloseMap.current[stock.secId];
        if (!newPrice || !prevClose || prevClose <= 0) return stock;
        return {
          ...stock,
          price: newPrice,
          change_1d: Math.round((newPrice - prevClose) / prevClose * 10000) / 100,
        };
      }));
      setLastUpdate(new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Moscow' }));
    } catch { /* ignore */ }
  }, 5000);

  // Получение сырого значения для размера
  const getRawSizeValue = (stock: HeatmapStock): number => {
    const key = (sizeBy === 'volume' ? volumeKey : sizeBy) as keyof HeatmapStock;
    return Math.max((stock[key] as number) || 1, 1);
  };

  // Для СЕКТОРОВ: слабая компрессия (0.85) — площади примерно пропорциональны
  // реальной капитализации. Финсектор >> недвижка.
  const getSectorSizeValue = (stock: HeatmapStock): number => {
    return Math.pow(getRawSizeValue(stock), 0.85);
  };

  // Для КОМПАНИЙ внутри сектора: сильная компрессия (0.55) — чтобы мелкие
  // тикеры не схлопывались в невидимые полоски внутри своего сектора.
  const getSizeValue = (stock: HeatmapStock): number => {
    return Math.pow(getRawSizeValue(stock), 0.55);
  };

  // Получение значения для цвета
  const getColorValue = (stock: HeatmapStock): number => {
    const key = colorBy as keyof HeatmapStock;
    return (stock[key] as number) || 0;
  };

  // Earth-tone палитра (как в референсе): brick red ↔ forest green, без неона.
  // Тёмный центр (#2a2a2a) → насыщенный землистый цвет на максимуме.
  // Целевые точки:
  //   negative max: #7a3528 (deep brick / clay)
  //   positive max: #2d6b3f (deep forest green)
  const getColor = (change: number): string => {
    const maxChange = colorBy === 'change_1y' ? 20 : colorBy === 'change_1m' ? 5 : colorBy === 'change_1w' ? 2 : 0.8;
    const abs = Math.abs(change);
    const t = Math.min(abs / maxChange, 1); // 0..1

    if (change > 0) {
      // #2a2a2a → #2d6b3f (forest green)
      const r = Math.round(42 + t * (45 - 42));    // 42 → 45
      const g = Math.round(42 + t * (107 - 42));   // 42 → 107
      const b = Math.round(42 + t * (63 - 42));    // 42 → 63
      return `rgb(${r},${g},${b})`;
    }
    if (change < 0) {
      // #2a2a2a → #7a3528 (brick / clay)
      const r = Math.round(42 + t * (122 - 42));   // 42 → 122
      const g = Math.round(42 + t * (53 - 42));    // 42 → 53
      const b = Math.round(42 + t * (40 - 42));    // 42 → 40
      return `rgb(${r},${g},${b})`;
    }
    return '#2a2a2a';
  };

  // Форматирование процента (с запятой)
  const formatPercent = (value: number): string => {
    return `${value.toFixed(2)}%`;
  };

  // Размер шрифта — гранулярное пропорциональное масштабирование.
  //
  // ИТЕРАЦИЯ 3 (2026-05-15): возвращаемся к length-independent формуле `w/4.5`
  // (как был дизайн до мая 2026). Length-based variant (v605-v609) делал
  // медиум-плитки слишком агрессивными и они упирались в cap → SBER, ROSN,
  // NVTK выглядели одинаково большими. С `w/4.5` только реально огромные
  // плитки достигают cap, остальные масштабируются плавно — больше «шагов».
  //
  // ПАРАМЕТРЫ:
  //   - tickerByWidth = width / 4.5 (старая формула, length-independent).
  //     Длинные тикеры (SNGSP) сжимаются через SVG textLength compression.
  //   - tickerByHeight = height * 0.35 (с percent), * 0.55 (без — compact tile).
  //   - cap = 64 (vs original 48 → +33% headroom для огромных плиток).
  //   - percent = ticker * 0.65 (slightly меньше ticker'а — match Finviz-style).
  const getFontSize = (
    width: number,
    height: number,
    _ticker: string,
    hasPercent: boolean,
  ): { ticker: number; percent: number } => {
    const tickerByWidth = Math.floor(width / 4.5);
    const tickerByHeight = Math.floor(height * (hasPercent ? 0.35 : 0.55));
    const tickerSize = Math.min(Math.max(Math.min(tickerByWidth, tickerByHeight), 9), 64);
    const percent = Math.floor(tickerSize * 0.65);
    return { ticker: tickerSize, percent };
  };

  // Truncate text to fit width (approx char-width = fontSize * 0.55 для regular weight).
  // Возвращает текст с многоточием если не помещается.
  const fitText = (text: string, maxWidth: number, fontSize: number): string => {
    const charWidth = fontSize * 0.55;
    const maxChars = Math.floor(maxWidth / charWidth);
    if (maxChars >= text.length) return text;
    if (maxChars <= 1) return text.slice(0, 1);
    return text.slice(0, maxChars - 1) + '…';
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

    // По секторам: площадь сектора ~ суммарная капитализация (слабая компрессия)
    const sectorItems = sectors.map(sector => ({
      id: sector.name,
      value: sector.stocks.reduce((sum, s) => sum + getSectorSizeValue(s), 0),
      stocks: sector.stocks
    }));

    const sectorRects = squarify(
      sectorItems.map(s => ({ id: s.id, value: s.value, data: null as unknown as HeatmapStock })),
      0, 0, containerSize.width, containerSize.height
    );

    const stockRects: { id: string; x: number; y: number; width: number; height: number; data: HeatmapStock; sector: string }[] = [];
    const sectorLabels: { name: string; x: number; y: number; width: number; height: number }[] = [];

    // Header height — компактнее на мобиле (15px) и шире на десктопе (18px),
    // чтобы маленьким секторам оставалось место для самих плиток.
    const headerH = containerSize.width < 600 ? 15 : 18;
    const minTileArea = 20; // если меньше — не показываем пустой header

    sectorRects.forEach((sectorRect, idx) => {
      const sector = sectorItems[idx];
      if (!sector) return;

      const tileAreaH = sectorRect.height - headerH - gap;
      // Если в секторе вообще не помещаются плитки — пропускаем header,
      // чтобы не было "обрезанное название сектора и пусто под ним".
      if (tileAreaH < minTileArea) return;

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
        tileAreaH
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
    const showTicker = rect.width > 18 && rect.height > 14;
    const showPercent = rect.width > 30 && rect.height > 25;
    // hasPercent передаём в getFontSize чтобы корректно учесть height budget
    // (с процентом ticker занимает меньше высоты, оставляя место под percent).
    const fonts = getFontSize(rect.width, rect.height, rect.id, showPercent);
    // textLength применяем ТОЛЬКО при overflow — иначе короткие тикеры (SBER)
    // растягиваются на всю ширину блока. Estimate: char-width ≈ fontSize * 0.58
    // для bold sans-serif. Если расчётная ширина > доступной — компрессим.
    const tickerMaxWidth = Math.max(0, rect.width - 10);
    const percentMaxWidth = Math.max(0, rect.width - 8);
    // Estimate использует 0.7 — консервативная оценка реального char-width
    // (с учётом kerning/hinting). Триггерим компрессию на 90% от maxWidth
    // вместо 100% — даёт margin на edge-cases когда реальный рендер чуть
    // шире оценки.
    const tickerEstWidth = rect.id.length * fonts.ticker * 0.7;
    const percentEstWidth = formatPercent(change).length * fonts.percent * 0.6;
    const tickerNeedsCompress = tickerEstWidth >= tickerMaxWidth * 0.9;
    const percentNeedsCompress = percentEstWidth >= percentMaxWidth * 0.9;
    const gap = 2;
    const radius = 6;

    // ClipPath — hard-cap geometry, чтобы текст физически не вылезал за rect
    // независимо от того угадал ли getFontSize верный размер. Это страховка
    // на случай если ratio оценки немного ошибается (Cyrillic, custom fonts,
    // letter-spacing edge cases). ID составной чтобы не пересекался между
    // секторами (key уже включает sector-prefix).
    const clipId = `tile-${key}`;
    const clipX = rect.x + gap / 2;
    const clipY = rect.y + gap / 2;
    const clipW = Math.max(0, rect.width - gap);
    const clipH = Math.max(0, rect.height - gap);

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
        <defs>
          <clipPath id={clipId}>
            <rect x={clipX} y={clipY} width={clipW} height={clipH} rx={radius} ry={radius} />
          </clipPath>
        </defs>
        <rect
          x={clipX}
          y={clipY}
          width={clipW}
          height={clipH}
          rx={radius}
          ry={radius}
          fill={getColor(change)}
          className="hover:brightness-110"
        />
        {/* fill="#fff" hardcoded by design: текст рендерится поверх цветного tile
            (red/green/orange color from change %), не на bg-primary. Всегда нужен
            белый для контраста независимо от темы. var(--text-inverse) тут НЕ подходит —
            он темный в dark theme (≈ bg-primary inverse), что делает текст нечитаемым
            на красных/зелёных tiles. */}
        {showTicker && (
          <text
            x={rect.x + rect.width / 2}
            y={rect.y + rect.height / 2 - (showPercent ? fonts.percent * 0.8 : 0)}
            textAnchor="middle"
            dominantBaseline="central"
            fill="#fff"
            fontSize={fonts.ticker}
            fontWeight="800"
            clipPath={`url(#${clipId})`}
            {...(tickerNeedsCompress ? { textLength: tickerMaxWidth, lengthAdjust: 'spacingAndGlyphs' as const } : {})}
            style={{
              letterSpacing: '-0.02em',
              transition: 'font-size 0.3s ease',
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
            dominantBaseline="central"
            fill="#fff"
            fontSize={fonts.percent}
            fontWeight="700"
            clipPath={`url(#${clipId})`}
            {...(percentNeedsCompress ? { textLength: percentMaxWidth, lengthAdjust: 'spacingAndGlyphs' as const } : {})}
          >
            {formatPercent(change)}
          </text>
        )}
      </g>
    );
  };

  return (
    <div className="max-w-full mx-auto px-2 md:px-4 py-3 md:py-4">
      <PageHeader
        icon={Grid3X3}
        title="Карта рынка"
        subtitle={`Обновлено в ${lastUpdate || '--:--'}`}
        help={METHODOLOGY.heatmap}
        helpLink="/methodology/heatmap"
      />

      {/* Editorial frame — обнимает controls + treemap в один контейнер с
          1.5px outline + hard-shadow (как на OI page). */}
      <div className="editorial-frame">

      {/* Контролы — Dropdown'ы. Camera button — в конце через ml-auto. */}
      <div className="flex flex-wrap mb-4 md:mb-6 items-center" style={{ gap: 'var(--sp-2)' }}>
        <div data-tour="heatmap-map-mode">
        <Dropdown<'imoex' | 'all'>
          options={[
            { key: 'imoex', label: 'Индекс IMOEX' },
            {
              key: 'all',
              label: 'Все акции',
              locked: !heatAccess.isLoading && !heatAccess.canUseMode('all'),
            },
          ]}
          value={mapMode}
          onChange={setMapMode}
          onLockedClick={() => {
            const tier = heatAccess.requiredTierFor({ mode: 'all' });
            if (tier) {
              showUpgrade({
                tier,
                featureName: 'режим «Все акции»',
                indicator: 'heatmap',
              });
            }
          }}
        />
        </div>

        <div data-tour="heatmap-size" className="flex" style={{ gap: 'var(--sp-2)' }}>
        <Dropdown<string>
          options={SIZE_OPTIONS.map((o): DropdownOption<string> => ({ key: o.value, label: o.label }))}
          value={sizeBy}
          onChange={setSizeBy}
        />

        <Dropdown<string>
          options={GROUP_OPTIONS.map((o): DropdownOption<string> => ({ key: o.value, label: o.label }))}
          value={groupBy}
          onChange={setGroupBy}
        />
        </div>

        <div data-tour="heatmap-period">
        <Dropdown<string>
          options={PERIOD_OPTIONS.map((o): DropdownOption<string> => ({ key: o.value, label: o.label }))}
          value={period}
          onChange={setPeriod}
        />
        </div>

        {/* Camera button inline, прижат к правому краю.
            captureRef = outer paper-card → snapshot включает watermark. */}
        <div data-tour="heatmap-export" className="ml-auto flex items-center gap-2">
        <CsvExportButton
          indicator="heatmap"
          config={() => ({
            indicator: 'heatmap',
            title: 'Экспорт: Карта рынка',
            layers: [{
              id: 'current',
              label: 'Снапшот всех акций',
              description: 'Текущая цена + изменения 1д/1н/1м/1г + market cap + объём',
              defaultSelected: true,
            }],
            selectors: [],
            params: [
              { label: 'Режим (UI)', value: mapMode === 'imoex' ? 'IMOEX' : 'Все акции' },
              { label: 'Размер (UI)', value: SIZE_OPTIONS.find(o => o.value === sizeBy)?.label ?? sizeBy },
              { label: 'Период (UI)', value: PERIOD_OPTIONS.find(o => o.value === period)?.label ?? period },
            ],
            buildUrl: () => '/api/export/heatmap.csv',
            buildFilename: () => 'heatmap.csv',
          })}
        />
        <ChartCaptureButton
          getTargetElement={() => captureRef.current}
          filename={`frame-heatmap-${mapMode}-${period}-${groupBy}`}
          metadata={{
            // Главный заголовок скриншота — всегда «Карта рынка». Режим
            // (Индекс IMOEX / Все акции) уходит первым тегом в подзаголовок.
            title: 'Карта рынка',
            details: [
              mapMode === 'imoex' ? 'Индекс IMOEX' : 'Все акции',
              SIZE_OPTIONS.find(o => o.value === sizeBy)?.label ?? sizeBy,
              PERIOD_OPTIONS.find(o => o.value === period)?.label ?? period,
              GROUP_OPTIONS.find(o => o.value === groupBy)?.label ?? groupBy,
            ].filter(Boolean),
          }}
        />
        </div>
      </div>

      {/* Карта — paper-card как у OI/Funds-Money: 2px inkstroke + paper bg.
          Editorial-стандарт: chart cards 2px, chips/buttons 1.5px.
          Treemap SVG внутри. Watermark — absolute bottom-left как у других charts. */}
      <div
        ref={captureRef}
        data-tour="heatmap-chart"
        className="relative rounded-2xl overflow-hidden bg-theme-primary"
        style={{ border: '2px solid var(--text-primary)' }}
      >
      <div
        ref={containerRef}
        className="relative"
        style={{ height: containerSize.height }}
      >
        {loading ? (
          <div className="absolute inset-0 flex items-center justify-center text-slate-400">
            <div className="flex flex-col items-center gap-3">
              <div className="animate-spin w-8 h-8 border-2 border-t-transparent rounded-full" style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }} />
              <span className="text-sm text-theme-secondary">Загрузка карты...</span>
            </div>
          </div>
        ) : treemapData && treemapData.type === 'grouped' ? (
          <svg width={containerSize.width} height={containerSize.height}>
            {treemapData.stockRects.map((rect) => renderStock(rect, `${rect.sector}-${rect.id}`))}

            {/* Заголовки секторов — text на inner paper card без плашки.
                Шрифт адаптивный (10-12px) + truncate с ellipsis если не лезет.
                Раньше был <rect fill="var(--bg-secondary)"> подложкой —
                убрана по запросу: текст напрямую на paper bg обеих тем. */}
            {treemapData.sectorLabels.map((label) => {
              // FontSize: ограничен высотой полоски (height - 4) и не больше 12.
              const labelFs = Math.min(12, Math.max(9, label.height - 4));
              const padX = 9;
              const availableW = label.width - padX * 2;
              const displayName = fitText(label.name, availableW, labelFs);
              return (
                <text
                  key={label.name}
                  x={label.x + padX}
                  y={label.y + label.height / 2}
                  dominantBaseline="central"
                  fill="var(--text-primary)"
                  opacity={0.85}
                  fontSize={labelFs}
                  fontWeight="600"
                >
                  {displayName}
                </text>
              );
            })}
          </svg>
        ) : treemapData && treemapData.type === 'flat' ? (
          <svg width={containerSize.width} height={containerSize.height}>
            {treemapData.rects.map((rect) => renderStock(rect, rect.id))}
          </svg>
        ) : null}
      </div>
      {/* Watermark — bottom-left, как на других chart-card'ах */}
      <ChartWatermark left={10} bottom={10} />
      </div>{/* /paper card */}

      </div>{/* /editorial-frame */}


      {/* Тултип — paper-style без glass, в цвет фона графика.
          Mobile-aware: max-width clamp + smaller padding/fonts + truncate
          на длинном stock.name (раньше hardcoded 180px clamp + var(--sp-5)
          padding раздували tooltip до 50%+ ширины 375vw экрана). */}
      {tooltip.visible && tooltip.stock && (
        <div
          className="fixed z-50 border border-theme rounded-xl shadow-md pointer-events-none"
          style={{
            // Clamp 90px вместо 180 — tooltip может ближе подходить к краям
            // на mobile где viewport узкий.
            left: Math.min(Math.max(tooltip.x, 90), window.innerWidth - 90),
            top: tooltip.y < 200 ? tooltip.y + 25 : tooltip.y - 10,
            transform: tooltip.y < 200 ? 'translate(-50%, 0)' : 'translate(-50%, -100%)',
            backgroundColor: 'var(--bg-primary)',
            // Padding fluid sp-2/sp-3 (4-8px / 6-12px) вместо sp-3/sp-5 — экономит
            // ~20px ширины на mobile.
            padding: 'var(--sp-2) var(--sp-3)',
            fontSize: 'var(--fs-2xs)',
            // max-width: не шире viewport - 32px gap, потолок 360px на desktop.
            maxWidth: 'min(calc(100vw - 32px), 360px)',
          }}
        >
          <div className="flex items-center mb-1" style={{ gap: 'var(--sp-2)' }}>
            <span className="font-bold text-theme-primary whitespace-nowrap" style={{ fontSize: 'var(--fs-xs)' }}>{tooltip.stock.secId}</span>
            <span className="text-theme-secondary truncate min-w-0" style={{ fontSize: 'var(--fs-2xs)' }}>{tooltip.stock.name}</span>
            <span className="text-theme-primary font-semibold ml-auto whitespace-nowrap flex-shrink-0" style={{ fontSize: 'var(--fs-2xs)' }}>{tooltip.stock.price.toFixed(2)} ₽</span>
          </div>
          <div className="flex items-center" style={{ fontSize: 'var(--fs-2xs)', gap: 'var(--sp-3)' }}>
            {[
              { label: 'Д', value: tooltip.stock.change_1d },
              { label: 'Н', value: tooltip.stock.change_1w },
              { label: 'М', value: tooltip.stock.change_1m },
              { label: 'Г', value: tooltip.stock.change_1y },
            ].map(({ label, value }) => (
              <span key={label} className="flex items-center" style={{ gap: 'var(--sp-1)' }}>
                <span className="text-theme-muted">{label}</span>
                <span className={`font-semibold whitespace-nowrap ${value >= 0 ? 'text-theme-success' : 'text-theme-danger'}`}>
                  {formatPercent(value)}
                </span>
              </span>
            ))}
          </div>
        </div>
      )}

      <OnboardingTour
        steps={heatmapTourSteps}
        open={tour.open}
        onClose={tour.close}
      />
    </div>
  );
}