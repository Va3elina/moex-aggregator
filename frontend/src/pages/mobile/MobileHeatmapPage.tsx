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
import { Grid3X3, Lock } from 'lucide-react';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import { handleTierError } from '../../utils/tierError';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileSheet from '../../components/mobile/MobileSheet';
import { getHeatmapImoex, getHeatmapData } from '../../services/api';
import type { HeatmapSector, HeatmapStock } from '../../services/api';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { squarify } from '../../utils/squarify';
import { formatNumber } from '../../utils/formatNumber';
import MobileSkeleton from '../../components/mobile/MobileSkeleton';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour from '../../components/onboarding/OnboardingTour';
import type { TourStep } from '../../components/onboarding/OnboardingTour';

type Period = '1d' | '1w' | '1m' | '1y';
type SizeBy = 'market_cap' | 'volume';

const PERIOD_OPTIONS: Array<{
  key: Period;
  label: string;
  colorKey: keyof HeatmapStock;
  volumeKey: keyof HeatmapStock;
}> = [
  { key: '1d', label: '1Д', colorKey: 'change_1d', volumeKey: 'value_1d' },
  { key: '1w', label: '1Н', colorKey: 'change_1w', volumeKey: 'value_1w' },
  { key: '1m', label: '1М', colorKey: 'change_1m', volumeKey: 'value_1m' },
  // 1Г: годового оборота нет в API → используем месячный как ближайший прокси
  { key: '1y', label: '1Г', colorKey: 'change_1y', volumeKey: 'value_1m' },
];

// Earth-tone палитра — match desktop HeatmapPage:
//   centre #2a2a2a (dark grey) → pos max #2d6b3f (deep forest green)
//                              → neg max #7a3528 (deep brick/clay)
// Без неона: землистые тона на максимуме, как в финансовых референсах.
function getColor(change: number, maxChange: number): string {
  if (!Number.isFinite(change)) return '#2a2a2a';
  const t = Math.min(Math.abs(change) / maxChange, 1);
  if (change > 0) {
    // #2a2a2a → #2d6b3f (forest green)
    const r = Math.round(42 + t * (45 - 42));
    const g = Math.round(42 + t * (107 - 42));
    const b = Math.round(42 + t * (63 - 42));
    return `rgb(${r},${g},${b})`;
  }
  if (change < 0) {
    // #2a2a2a → #7a3528 (brick / clay)
    const r = Math.round(42 + t * (122 - 42));
    const g = Math.round(42 + t * (53 - 42));
    const b = Math.round(42 + t * (40 - 42));
    return `rgb(${r},${g},${b})`;
  }
  return '#2a2a2a';
}

function formatPercent(v: number): string {
  if (!Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
}

type Universe = 'imoex' | 'all';
type GroupBy = 'sector' | 'none';

export default function MobileHeatmapPage() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [sectors, setSectors] = useState<HeatmapSector[]>([]);
  const [loading, setLoading] = useState(true);
  const [containerSize, setContainerSize] = useState({ w: 360, h: 500 });
  const [period, setPeriod] = useState<Period>('1d');
  const [universe, setUniverse] = useState<Universe>('imoex');
  const heatAccess = useTierAccess('heatmap');
  const { showUpgrade } = useUpgradePrompt();
  const [groupBy, setGroupBy] = useState<GroupBy>('sector');
  const [sizeBy, setSizeBy] = useState<SizeBy>('market_cap');
  const [selectedStock, setSelectedStock] = useState<HeatmapStock | null>(null);

  const [timeSheetOpen, setTimeSheetOpen] = useState(false);
  const [optionsSheetOpen, setOptionsSheetOpen] = useState(false);

  const tour = useOnboardingTour('heatmap');
  // Единый паттерн тура (как у OI/Buffett/Funds/Seasonality):
  // Intro → Кнопки → Время → Опции (с демо режима) → Чтение → End
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Карта рынка',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Все акции IMOEX на одной плитке. Размер плитки —
            капитализация (или оборот), цвет — изменение цены за период.
          </p>
          <p>Разберём управление и научимся читать карту.</p>
        </>
      ),
      onEnter: () => { setTimeSheetOpen(false); setOptionsSheetOpen(false); },
    },
    {
      selector: '.fm-page-actions',
      title: 'Кнопки управления',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>Снизу — 3 кнопки:</p>
          <p style={{ marginBottom: 4 }}>
            <strong>Время</strong> — период расчёта изменения цены
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Опции</strong> — что определяет размер плитки
          </p>
          <p>
            <strong>Экран</strong> — развернуть карту на весь экран
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
    },
    {
      selector: null,
      title: 'Период изменения цены',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл для тебя кнопку <strong>«Время»</strong>:
          </p>
          <p>
            <strong>1Д / 1Н / 1М / 1Г</strong> — за какой период считается
            рост/падение цены, и соответственно — каким цветом окрашивается
            плитка.
          </p>
        </>
      ),
      onEnter: () => { setTimeSheetOpen(true); setOptionsSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Размер плитки',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл кнопку <strong>«Опции»</strong>:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Капитализация</strong> — крупные компании занимают
            больше места (Сбер, Лукойл, Газпром доминируют)
          </p>
          <p>
            <strong>Оборот</strong> — больше места там, где сегодня больше
            торгов. Полезно когда хочешь видеть «где сейчас движуха».
          </p>
        </>
      ),
      onEnter: () => { setOptionsSheetOpen(true); setTimeSheetOpen(false); },
    },
    {
      selector: '[data-tour="heatmap-chart"]',
      title: 'Чтение карты',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            <strong>Тёмно-зелёные</strong> плитки — сильный рост, <strong>тёмно-красные</strong> —
            сильное падение. Серые — около нуля.
          </p>
          <p>
            <strong>Тап по плитке</strong> — детали компании: цена, изменения
            за все периоды, капитализация, оборот.
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
      onEnter: () => { setOptionsSheetOpen(false); setTimeSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Готово!',
      body: (
        <p>
          Нажми <strong>?</strong> рядом с заголовком — методология
          цветовых порогов и источники данных.
        </p>
      ),
    },
  ];

  const periodConfig = PERIOD_OPTIONS.find((p) => p.key === period) ?? PERIOD_OPTIONS[0];
  const colorKey = periodConfig.colorKey;
  // Maximum для color scaling — зависит от периода (1Д макс 0.8%, 1Г 20%)
  const maxChange = period === '1y' ? 20 : period === '1m' ? 5 : period === '1w' ? 2 : 0.8;

  // ResizeObserver с threshold-фильтром: микро-изменения <8px игнорируются.
  // Treemap re-layouts на каждое изменение размера (squarify O(n)), частые
  // вызовы из-за iOS address bar show/hide создают визуальное дёрганье.
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (r.width > 0 && r.height > 0) {
          setContainerSize((prev) => {
            if (Math.abs(prev.w - r.width) < 8 && Math.abs(prev.h - r.height) < 8) {
              return prev; // микро-изменение — игнорируем
            }
            return { w: r.width, h: r.height };
          });
        }
      }
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  // Загрузка данных — учитывает universe (IMOEX vs все) + groupBy
  const load = useMemo(
    () => async () => {
      try {
        setLoading(true);
        const data =
          universe === 'imoex'
            ? await getHeatmapImoex('change_1d', groupBy)
            : await getHeatmapData('market_cap', 'change_1d', groupBy);
        setSectors(data.sectors || []);
      } catch (err) {
        console.error('Ошибка загрузки heatmap:', err);
        handleTierError(err, {
          showUpgrade,
          indicator: 'heatmap',
          featureName: 'режим «Все акции»',
        });
      } finally {
        setLoading(false);
      }
    },
    [universe, groupBy, showUpgrade],
  );

  useEffect(() => { void load(); }, [load]);
  useRealtimeData(['5min', 'mv_refresh'], () => { void load(); });

  // Построение treemap'а:
  //   - groupBy='sector': сначала размер сектора, потом акции внутри
  //   - groupBy='none': плоский squarify по всем акциям сразу (без header'ов)
  //   - sizeBy='market_cap': площадь = капитализация
  //   - sizeBy='volume':     площадь = оборот за выбранный период (value_*)
  // Компрессия `pow(value, 0.55)` на уровне акций — чтобы мелкие тикеры
  // не схлопывались в невидимые полоски (паттерн из desktop heatmap).
  const treemapData = useMemo(() => {
    if (sectors.length === 0 || containerSize.w === 0) return null;

    const gap = 2;
    const headerH = 12;
    const W = containerSize.w;
    const H = containerSize.h;

    const volumeKey = periodConfig.volumeKey;

    // Сырое значение размера для одной акции
    const rawSize = (s: HeatmapStock): number => {
      const key: keyof HeatmapStock = sizeBy === 'volume' ? volumeKey : 'market_cap';
      const v = (s[key] as number) || 0;
      return Math.max(v, 1);
    };
    // Компрессированное значение для акций (внутри сектора и в плоском режиме)
    const stockSize = (s: HeatmapStock): number => Math.pow(rawSize(s), 0.55);
    // Компрессированное значение для секторов (сумма по их акциям)
    const sectorSize = (s: HeatmapStock): number => Math.pow(rawSize(s), 0.85);

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

    if (groupBy === 'none') {
      // Плоский режим: все акции в одном squarify, без header'ов секторов
      const allStocks = sectors
        .flatMap((s) => s.stocks.map((st) => ({ ...st, sectorName: s.name })))
        .filter((s) => rawSize(s) > 1)
        .map((s) => ({ id: s.secId, value: stockSize(s), data: s }));

      const rects = squarify(allStocks, 0, 0, W, H);
      rects.forEach((r) => {
        stockRects.push({
          id: r.id,
          x: r.x,
          y: r.y,
          width: r.width,
          height: r.height,
          stock: r.data,
          sector: r.data.sectorName,
        });
      });

      return { stockRects, sectorLabels };
    }

    // Иерархический режим: секторы → акции
    const sectorItems = sectors.map((s) => ({
      id: s.name,
      value: s.stocks.reduce((sum, st) => sum + sectorSize(st), 0),
      data: s,
    }));

    const sectorRects = squarify(sectorItems, 0, 0, W, H);

    sectorRects.forEach((rect) => {
      const sectorData = rect.data;
      const tileAreaH = rect.height - headerH - gap;
      if (tileAreaH < 30) return; // слишком мелкий сектор

      sectorLabels.push({ name: sectorData.name, x: rect.x, y: rect.y, w: rect.width });

      const stockItems = sectorData.stocks
        .filter((s) => rawSize(s) > 1)
        .map((s) => ({ id: s.secId, value: stockSize(s), data: s }));

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
  }, [sectors, containerSize, groupBy, sizeBy, periodConfig]);

  // Размер шрифта тикера в зависимости от плитки.
  // На очень мелких плитках (< 20×14) опускаем minimum до 7px чтобы хоть
  // что-то было видно — лучше мелкий тикер, чем пустая цветная плашка.
  const getFontSize = (w: number, h: number): { tk: number; pct: number } => {
    // Подбираем размер по самой узкой оси с учётом длины тикера (~4 символа)
    const tickerByWidth = Math.floor(w / 3.6);
    const tickerByHeight = Math.floor(h * 0.42);
    const tk = Math.min(Math.max(Math.min(tickerByWidth, tickerByHeight), 7), 36);
    const pct = Math.floor(tk * 0.65);
    return { tk, pct };
  };

  return (
    <MobileLayout
      onTimeClick={() => setTimeSheetOpen(true)}
      timeSummary={periodConfig.label}
      timeTourId="heatmap-period"
      onSettingsClick={() => setOptionsSheetOpen(true)}
      settingsSummary={`${universe === 'imoex' ? 'IMOEX' : 'Все'} · ${sizeBy === 'volume' ? 'Оборот' : 'Кап'}`}
      settingsTourId="heatmap-options"
      onRefresh={load}
      loading={loading}
    >
      <MobilePageHeader
        Icon={Grid3X3}
        title="Карта рынка"
        helpLink="/methodology/heatmap"
      />

      {/* Full-bleed treemap (как OI chart): без рамки, без margin, без bg.
          position:relative + minHeight:0 — те же приёмы что в OI, чтобы
          treemap не выпрыгивал за main и не вызывал page-scroll. */}
      <div
        ref={containerRef}
        data-tour="heatmap-chart"
        style={{
          flex: 1,
          minHeight: 0,
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        {loading && (
          <div style={{ padding: 4, height: '100%' }}>
            <MobileSkeleton variant="chart" height="100%" />
          </div>
        )}
        {!loading && treemapData && (
          <svg
            width={containerSize.w}
            height={containerSize.h}
            viewBox={`0 0 ${containerSize.w} ${containerSize.h}`}
            style={{ display: 'block', touchAction: 'none' }}
          >
            {/* Сначала плитки акций */}
            {treemapData.stockRects.map((r) => {
              const change = (r.stock[colorKey] as number) || 0;
              const fill = getColor(change, maxChange);
              const { tk, pct } = getFontSize(r.width, r.height);
              // Снижено: показываем ticker почти всегда (до 18×12) — лучше
              // мелкий ticker чем пустая плитка. % скрываем чуть раньше.
              const showTicker = r.width > 18 && r.height > 12;
              const showPct = r.width > 38 && r.height > 28;
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
                      style={{ letterSpacing: '-0.02em' }}
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
                    >
                      {formatPercent(change)}
                    </text>
                  )}
                </g>
              );
            })}

            {/* Sector labels поверх плиток с truncation:
                - sector width < 36px → label не рисуется (слишком тесно)
                - иначе обрезаем по char-budget = (width - 8 padding) / 7 (px per char) */}
            {treemapData.sectorLabels.map((s, i) => {
              if (s.w < 36) return null;
              const budget = Math.floor((s.w - 8) / 7);
              const display =
                s.name.length <= budget
                  ? s.name
                  : s.name.slice(0, Math.max(3, budget - 1)) + '…';
              return (
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
                  {display}
                </text>
              );
            })}
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
              {/* API quirk: market_cap уже в МИЛЛИАРДАХ ₽ (113.0 = 113 млрд),
                  а value_1d — в рублях. Не делим первое, делим второе. */}
              Капитализация: {formatNumber(selectedStock.market_cap, 1)} млрд ₽
              {selectedStock.value_1d > 0 && (
                <> · Объём за день: {formatNumber(selectedStock.value_1d / 1e9, 2)} млрд ₽</>
              )}
            </div>
          </div>
        )}
      </MobileSheet>

      {/* Time sheet — выбор периода для расчёта цвета */}
      <MobileSheet
        open={timeSheetOpen}
        onClose={() => setTimeSheetOpen(false)}
        title="Период расчёта"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
          {PERIOD_OPTIONS.map((opt) => (
            <button
              key={opt.key}
              className={`fm-chip ${period === opt.key ? 'active' : ''}`}
              onClick={() => {
                setPeriod(opt.key);
                setTimeSheetOpen(false);
              }}
              style={{ justifyContent: 'flex-start', padding: '14px 16px' }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </MobileSheet>

      {/* Options sheet — universe + groupBy */}
      <MobileSheet
        open={optionsSheetOpen}
        onClose={() => setOptionsSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
          {/* Universe: IMOEX vs все акции */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Что показать
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {([
                { key: 'imoex' as const, label: 'IMOEX', hint: '~50 акций индекса' },
                { key: 'all' as const, label: 'Все акции', hint: '200+ MOEX' },
              ]).map((opt) => {
                const allowed = heatAccess.isLoading || heatAccess.canUseMode(opt.key);
                return (
                  <button
                    key={opt.key}
                    onClick={() => {
                      if (!allowed) {
                        const tier = heatAccess.requiredTierFor({ mode: opt.key });
                        if (tier) {
                          showUpgrade({
                            tier,
                            featureName: `режим «${opt.label}»`,
                            indicator: 'heatmap',
                          });
                        }
                        return;
                      }
                      setUniverse(opt.key);
                    }}
                    style={{
                      flex: 1,
                      padding: '12px 10px',
                      background: universe === opt.key ? 'var(--accent)' : 'var(--bg-secondary)',
                      color: universe === opt.key ? 'var(--text-inverse)' : 'var(--text-primary)',
                      border: '1.5px solid var(--text-primary)',
                      borderRadius: 10,
                      cursor: allowed ? 'pointer' : 'not-allowed',
                      font: 'inherit',
                      textAlign: 'center',
                      opacity: allowed ? 1 : 0.5,
                      position: 'relative',
                    }}
                    aria-disabled={!allowed}
                  >
                    <div style={{ fontSize: 13, fontWeight: 700, display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                      {opt.label}
                      {!allowed && <Lock size={11} strokeWidth={2.2} />}
                    </div>
                    <div style={{ fontSize: 10, opacity: 0.7, marginTop: 2 }}>{opt.hint}</div>
                  </button>
                );
              })}
            </div>
          </div>

          {/* SizeBy: market_cap / volume */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Размер плиток
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {([
                { key: 'market_cap' as const, label: 'Капитализация', hint: 'Кто стоит дороже' },
                { key: 'volume' as const, label: 'Оборот', hint: 'Кого торгуют активнее' },
              ]).map((opt) => (
                <button
                  key={opt.key}
                  onClick={() => setSizeBy(opt.key)}
                  style={{
                    flex: 1,
                    padding: '12px 10px',
                    background: sizeBy === opt.key ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: sizeBy === opt.key ? 'var(--text-inverse)' : 'var(--text-primary)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 10,
                    cursor: 'pointer',
                    font: 'inherit',
                    textAlign: 'center',
                  }}
                >
                  <div style={{ fontSize: 12, fontWeight: 700 }}>{opt.label}</div>
                  <div style={{ fontSize: 9, opacity: 0.7, marginTop: 2 }}>{opt.hint}</div>
                </button>
              ))}
            </div>
          </div>

          {/* GroupBy: sector / none */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Группировка
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {([
                { key: 'sector' as const, label: 'По секторам', hint: 'Группы IT, Нефть и т.д.' },
                { key: 'none' as const, label: 'Без группировки', hint: 'Только по размеру' },
              ]).map((opt) => (
                <button
                  key={opt.key}
                  onClick={() => setGroupBy(opt.key)}
                  style={{
                    flex: 1,
                    padding: '12px 10px',
                    background: groupBy === opt.key ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: groupBy === opt.key ? 'var(--text-inverse)' : 'var(--text-primary)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 10,
                    cursor: 'pointer',
                    font: 'inherit',
                    textAlign: 'center',
                  }}
                >
                  <div style={{ fontSize: 12, fontWeight: 700 }}>{opt.label}</div>
                  <div style={{ fontSize: 9, opacity: 0.7, marginTop: 2 }}>{opt.hint}</div>
                </button>
              ))}
            </div>
          </div>
        </div>
      </MobileSheet>

      <OnboardingTour
        steps={tourSteps}
        open={tour.open}
        onClose={tour.close}
      />
    </MobileLayout>
  );
}
