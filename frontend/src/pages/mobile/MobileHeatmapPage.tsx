/**
 * MobileHeatmapPage — мобильная версия «Карты рынка».
 *
 * Дизайн: full-bleed treemap, минимум chrome.
 *   - TopBar + PageHeader (компактнее десктопа)
 *   - Period chips (1Д/1Н/1М/1Г) — выбор за какой период считать цвет
 *   - SVG treemap занимает почти весь экран, edge-to-edge
 *   - Тап по плитке → bottom-sheet с детальной информацией
 *   - Группировка по секторам всегда включена (на мобиле без неё хаос)
 *
 * Размер плитки = market_cap (всегда), цвет = change_<period>.
 * Phase 3 — только индекс IMOEX (~50 акций). Все 200+ акций — Phase 4.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { Grid3X3 } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileSheet from '../../components/mobile/MobileSheet';
import { getHeatmapImoex } from '../../services/api';
import type { HeatmapSector, HeatmapStock } from '../../services/api';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { squarify } from '../../utils/squarify';
import { formatNumber } from '../../utils/formatNumber';

type Period = '1d' | '1w' | '1m' | '1y';

const PERIOD_OPTIONS: Array<{ key: Period; label: string; colorKey: keyof HeatmapStock }> = [
  { key: '1d', label: '1Д', colorKey: 'change_1d' },
  { key: '1w', label: '1Н', colorKey: 'change_1w' },
  { key: '1m', label: '1М', colorKey: 'change_1m' },
  { key: '1y', label: '1Г', colorKey: 'change_1y' },
];

// Color scale: red (-X) → dark → green (+X), интенсивность ~ |change|
function getColor(change: number, maxChange: number): string {
  if (!Number.isFinite(change)) return '#2a2a2a';
  const t = Math.min(Math.abs(change) / maxChange, 1);
  if (change > 0) {
    // Зелёный
    const r = Math.round(42 + t * (16 - 42));
    const g = Math.round(40 + t * (160 - 40));
    const b = Math.round(42 + t * (40 - 42));
    return `rgb(${r},${g},${b})`;
  }
  if (change < 0) {
    // Красный
    const r = Math.round(42 + t * (192 - 42));
    const g = Math.round(40 + t * (32 - 40));
    const b = Math.round(42 + t * (40 - 42));
    return `rgb(${r},${g},${b})`;
  }
  return '#2a2a2a';
}

function formatPercent(v: number): string {
  if (!Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
}

export default function MobileHeatmapPage() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [sectors, setSectors] = useState<HeatmapSector[]>([]);
  const [loading, setLoading] = useState(true);
  const [containerSize, setContainerSize] = useState({ w: 360, h: 500 });
  const [period, setPeriod] = useState<Period>('1d');
  const [selectedStock, setSelectedStock] = useState<HeatmapStock | null>(null);

  const periodConfig = PERIOD_OPTIONS.find((p) => p.key === period) ?? PERIOD_OPTIONS[0];
  const colorKey = periodConfig.colorKey;
  // Maximum для color scaling — зависит от периода (1Д макс 0.8%, 1Г 20%)
  const maxChange = period === '1y' ? 20 : period === '1m' ? 5 : period === '1w' ? 2 : 0.8;

  // ResizeObserver — обновляем размер контейнера
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (r.width > 0 && r.height > 0) {
          setContainerSize({ w: r.width, h: r.height });
        }
      }
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  // Загрузка данных
  const load = useMemo(
    () => async () => {
      try {
        setLoading(true);
        const data = await getHeatmapImoex('change_1d', 'sector');
        setSectors(data.sectors || []);
      } catch (err) {
        console.error('Ошибка загрузки heatmap:', err);
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  useEffect(() => { void load(); }, [load]);
  useRealtimeData(['5min', 'mv_refresh'], () => { void load(); });

  // Построение treemap'а: сначала размер сектора (по market_cap), потом
  // внутри каждого сектора — отдельные акции через squarify.
  const treemapData = useMemo(() => {
    if (sectors.length === 0 || containerSize.w === 0) return null;

    const gap = 2;
    const headerH = 12;
    const W = containerSize.w;
    const H = containerSize.h;

    // Размер сектора = total market_cap его акций
    const sectorItems = sectors.map((s) => ({
      id: s.name,
      value: s.stocks.reduce((sum, st) => sum + (st.market_cap || 0), 0),
      data: s,
    }));

    const sectorRects = squarify(sectorItems, 0, 0, W, H);

    // Внутри каждого сектора — акции
    const stockRects: Array<{
      id: string;
      x: number;
      y: number;
      width: number;
      height: number;
      stock: HeatmapStock;
      sector: string;
    }> = [];

    const sectorLabels: Array<{ name: string; x: number; y: number; w: number }> = [];

    sectorRects.forEach((rect) => {
      const sectorData = rect.data;
      const tileAreaH = rect.height - headerH - gap;
      if (tileAreaH < 30) return; // слишком мелкий сектор

      sectorLabels.push({ name: sectorData.name, x: rect.x, y: rect.y, w: rect.width });

      const stockItems = sectorData.stocks
        .filter((s) => s.change_1d !== 0 || s.change_1w !== 0 || s.market_cap > 0)
        .map((s) => ({ id: s.secId, value: s.market_cap || 0, data: s }));

      const childRects = squarify(
        stockItems,
        rect.x + gap / 2,
        rect.y + headerH,
        rect.width - gap,
        tileAreaH,
      );

      childRects.forEach((cr) => {
        stockRects.push({
          id: cr.id,
          x: cr.x,
          y: cr.y,
          width: cr.width,
          height: cr.height,
          stock: cr.data,
          sector: sectorData.name,
        });
      });
    });

    return { stockRects, sectorLabels };
  }, [sectors, containerSize]);

  // Размер шрифта тикера в зависимости от плитки
  const getFontSize = (w: number, h: number): { tk: number; pct: number } => {
    const tickerByWidth = Math.floor(w / 4.5);
    const tickerByHeight = Math.floor(h * 0.38);
    const tk = Math.min(Math.max(Math.min(tickerByWidth, tickerByHeight), 9), 36);
    const pct = Math.floor(tk * 0.65);
    return { tk, pct };
  };

  return (
    <MobileLayout>
      <MobilePageHeader
        Icon={Grid3X3}
        title="Карта рынка"
        subtitle="IMOEX · По секторам"
        helpLink="/methodology/heatmap"
      />

      {/* Period chips */}
      <div
        style={{
          display: 'flex',
          gap: 6,
          padding: '4px 12px 8px',
          flexShrink: 0,
        }}
      >
        {PERIOD_OPTIONS.map((opt) => (
          <button
            key={opt.key}
            className={`fm-chip ${period === opt.key ? 'active' : ''}`}
            onClick={() => setPeriod(opt.key)}
            style={{ flex: 1, justifyContent: 'center' }}
          >
            {opt.label}
          </button>
        ))}
      </div>

      {/* Treemap container — full-bleed, занимает оставшееся место */}
      <div
        ref={containerRef}
        style={{
          margin: '0 8px',
          border: '2px solid var(--text-primary)',
          borderRadius: 12,
          overflow: 'hidden',
          background: 'var(--bg-secondary)',
          flex: 1,
          minHeight: 400,
          position: 'relative',
        }}
      >
        {loading && (
          <div style={{ display: 'grid', placeItems: 'center', height: '100%', color: 'var(--text-muted)' }}>
            Загрузка...
          </div>
        )}
        {!loading && treemapData && (
          <svg
            width={containerSize.w}
            height={containerSize.h}
            viewBox={`0 0 ${containerSize.w} ${containerSize.h}`}
            style={{ display: 'block' }}
          >
            {/* Сначала плитки акций */}
            {treemapData.stockRects.map((r) => {
              const change = (r.stock[colorKey] as number) || 0;
              const fill = getColor(change, maxChange);
              const { tk, pct } = getFontSize(r.width, r.height);
              const showTicker = r.width > 28 && r.height > 18;
              const showPct = r.width > 38 && r.height > 30;
              return (
                <g
                  key={`${r.sector}-${r.id}`}
                  onClick={() => setSelectedStock(r.stock)}
                  style={{ cursor: 'pointer' }}
                >
                  <rect
                    x={r.x + 1}
                    y={r.y + 1}
                    width={Math.max(0, r.width - 2)}
                    height={Math.max(0, r.height - 2)}
                    fill={fill}
                    rx={3}
                  />
                  {showTicker && (
                    <text
                      x={r.x + r.width / 2}
                      y={r.y + r.height / 2 - (showPct ? pct * 0.5 : 0)}
                      textAnchor="middle"
                      dominantBaseline="central"
                      fill="#fff"
                      fontSize={tk}
                      fontWeight={800}
                      style={{ textShadow: '0 1px 2px rgba(0,0,0,0.7)', letterSpacing: '-0.02em' }}
                    >
                      {r.id}
                    </text>
                  )}
                  {showPct && (
                    <text
                      x={r.x + r.width / 2}
                      y={r.y + r.height / 2 + tk * 0.55}
                      textAnchor="middle"
                      dominantBaseline="central"
                      fill="#fff"
                      fontSize={pct}
                      fontWeight={600}
                      opacity={0.92}
                      style={{ textShadow: '0 1px 2px rgba(0,0,0,0.7)' }}
                    >
                      {formatPercent(change)}
                    </text>
                  )}
                </g>
              );
            })}

            {/* Поверх — sector labels */}
            {treemapData.sectorLabels.map((s, i) => (
              <text
                key={`sl-${i}`}
                x={s.x + 4}
                y={s.y + 9}
                fontSize={8}
                fontWeight={700}
                fill="var(--text-secondary)"
                letterSpacing={0.4}
                style={{ textTransform: 'uppercase' }}
                pointerEvents="none"
              >
                {s.name}
              </text>
            ))}
          </svg>
        )}
      </div>

      {/* Bottom sheet с деталями выбранной плитки */}
      <MobileSheet
        open={selectedStock !== null}
        onClose={() => setSelectedStock(null)}
        title={selectedStock?.name ?? ''}
      >
        {selectedStock && (
          <div style={{ padding: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
              <span className="mono" style={{ fontSize: 13, color: 'var(--text-secondary)', letterSpacing: '0.04em', fontWeight: 600 }}>
                {selectedStock.secId} · {selectedStock.sector}
              </span>
              <span style={{ fontSize: 18, fontWeight: 800 }}>
                {formatNumber(selectedStock.price, 2)} ₽
              </span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
              {(['change_1d', 'change_1w', 'change_1m', 'change_1y'] as const).map((key) => {
                const v = selectedStock[key] as number;
                const label = key === 'change_1d' ? '1 день' : key === 'change_1w' ? '1 неделя' : key === 'change_1m' ? '1 месяц' : '1 год';
                return (
                  <div
                    key={key}
                    style={{
                      padding: '8px 10px',
                      background: 'var(--bg-secondary)',
                      border: '1.5px solid var(--text-primary)',
                      borderRadius: 8,
                    }}
                  >
                    <div style={{ fontSize: 10, color: 'var(--text-secondary)', fontWeight: 600, marginBottom: 2 }}>{label}</div>
                    <div
                      style={{
                        fontSize: 14,
                        fontWeight: 800,
                        color: v >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)',
                      }}
                    >
                      {formatPercent(v)}
                    </div>
                  </div>
                );
              })}
            </div>
            <div style={{ marginTop: 12, fontSize: 12, color: 'var(--text-muted)' }}>
              Капитализация: {formatNumber(selectedStock.market_cap / 1e9, 1)} млрд ₽
              {selectedStock.value_1d > 0 && (
                <> · Объём за день: {formatNumber(selectedStock.value_1d / 1e9, 2)} млрд ₽</>
              )}
            </div>
          </div>
        )}
      </MobileSheet>
    </MobileLayout>
  );
}
