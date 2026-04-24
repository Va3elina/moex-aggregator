// Экспериментальный Seasonax-style dashboard для режима "Тест":
// - Yearly chart сверху (кумулятивная траектория)
// - 2×2 grid из 4 гистограмм снизу (intraday / weekday / monthday / monthly)
// - Все 5 чартов реагируют на общие фильтры (compareYears, noOutliers, exactYear)
//
// Отдельный компонент, чтобы не раздувать SeasonalityPage.tsx.

import { useState, useEffect } from 'react';
import { Maximize2, X, ChevronDown } from 'lucide-react';
import type { SeasonalityResponse, SeasonalityMode, YearlySeasonalityResponse } from '../../services/api';
import SeasonalityHistogram from './SeasonalityHistogram';
import YearlySeasonalityChart from './YearlySeasonalityChart';
import InstrumentSearchModal from '../InstrumentSearchModal';

const TEST_MODES: SeasonalityMode[] = ['intraday', 'weekday', 'monthday', 'monthly'];

const MODE_LABELS: Record<SeasonalityMode, string> = {
  intraday: 'Внутри дня',
  weekday: 'По дням недели',
  monthday: 'Внутри месяца',
  monthly: 'По месяцам',
};

interface HistTooltip {
  x: number; y: number;
  bar?: SeasonalityResponse['bars'][0];
}

interface ChartTooltip {
  x: number; y: number;
  bar?: SeasonalityResponse['bars'][0];
  barAdj?: SeasonalityResponse['bars'][0];
  priceDate?: string; priceClose?: number; priceAdj?: number;
  yearlyAvgPct?: number; yearlyCurPct?: number; yearlyCurDate?: string;
}

interface SeriesMeta { key: string; label: string; color: string }

interface TestDashboardProps {
  compareYears: number[];
  selectedStock: string;
  selectedName: string;
  onSelectInstrument: (sectype: string, name: string) => void;
  renderFilters: () => React.ReactNode;
  yearlyData: YearlySeasonalityResponse | null;
  yearlySeries: YearlySeasonalityResponse[] | null;
  seriesMeta: SeriesMeta[];
  testHistData: Record<SeasonalityMode, SeasonalityResponse | null>;
  testHistSeries: Record<SeasonalityMode, SeasonalityResponse[] | null>;
  testHistTooltips: Record<SeasonalityMode, HistTooltip | null>;
  setTestHistTooltips: React.Dispatch<React.SetStateAction<Record<SeasonalityMode, HistTooltip | null>>>;
  chartTooltip: ChartTooltip | null;
  setChartTooltip: (t: ChartTooltip | null) => void;
  /** Per-chart fetch IDs — у каждого чарта свой счётчик для независимой wave-анимации. */
  testChartFetchIds: { yearly: number; intraday: number; weekday: number; monthday: number; monthly: number };
  loading: boolean;
  error: string | null;
}

type ModalView = 'yearly' | SeasonalityMode | null;

// В test-режиме container расширен до 1800px — пользуемся всей шириной.
// Yearly chart компактный сверху (240px) — главный акцент на гистограммах,
// yearly нужен как "контекст тренда", не сам по себе.
const YEARLY_HEIGHT = 240;

export default function TestDashboard({
  compareYears,
  selectedStock,
  selectedName,
  onSelectInstrument,
  renderFilters,
  yearlyData,
  yearlySeries,
  seriesMeta,
  testHistData,
  testHistSeries,
  testHistTooltips,
  setTestHistTooltips,
  chartTooltip,
  setChartTooltip,
  testChartFetchIds,
  loading,
  error,
}: TestDashboardProps) {
  // Модалка "развернуть": показывает выбранный чарт на 90vw/90vh с возможностью
  // переключаться между yearly и 4 режимами гистограмм + сменить актив.
  const [modalView, setModalView] = useState<ModalView>(null);
  const [modalStockPickerOpen, setModalStockPickerOpen] = useState(false);

  // ESC закрывает модалку
  useEffect(() => {
    if (!modalView) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setModalView(null); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [modalView]);
  const hasAny = yearlyData !== null || Object.values(testHistData).some(d => d !== null);

  // Empty states
  if (compareYears.length === 0) {
    return (
      <div className="rounded-2xl border p-4 flex items-center justify-center text-center"
        style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', aspectRatio: '16/9', color: 'var(--text-muted)' }}>
        Выберите хотя бы один "Период с" в меню выше
      </div>
    );
  }
  if (loading && !hasAny) {
    return (
      <div className="rounded-2xl border p-4 flex items-center justify-center"
        style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', aspectRatio: '16/9' }}>
        <div className="flex flex-col items-center gap-3">
          <div className="w-8 h-8 border-2 border-[#06b6d4] border-t-transparent rounded-full animate-spin" />
          <span className="text-theme-secondary">Загрузка...</span>
        </div>
      </div>
    );
  }
  if (error && !hasAny) {
    return (
      <div className="rounded-2xl border p-4 flex items-center justify-center"
        style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', aspectRatio: '16/9', color: 'var(--text-muted)' }}>
        {error}
      </div>
    );
  }

  const yearsRangeLabel = yearlyData?.years_range ? `· ${yearlyData.years_range}` : '';

  return (
    <div className="space-y-4 relative">
      {/* Спиннер обновления (общий, при фетче в фоне) */}
      {loading && hasAny && (
        <div className="absolute top-2 right-2 z-30 flex items-center gap-2 bg-theme-tertiary/90 backdrop-blur-sm px-3 py-1.5 rounded-lg border border-theme">
          <div className="w-4 h-4 border-2 border-[#C8FF2E] border-t-transparent rounded-full animate-spin" />
          <span className="text-xs text-theme-secondary">Обновление...</span>
        </div>
      )}

      {/* ===== TOP: Yearly chart =====
          container-type: inline-size → все clamp(...cqi...) масштабируются от
          ширины этого container'а. Никаких ручных размеров.
          aspect 5.5 + max 340 → намного жирнее чарт, меньше мёртвого воздуха. */}
      <div className="rounded-2xl border px-4 pt-1 pb-2 relative"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          borderColor: 'var(--border-color)',
          containerType: 'inline-size',
          // После возврата placeholder'а к 22px урезал max-height на ~20px,
          // чтобы общая высота card'а осталась примерно как было (~280-300).
          ['--seasonality-aspect-ratio' as string]: '5.5',
          ['--seasonality-min-height' as string]: '240px',
          ['--seasonality-max-height' as string]: '320px',
          ['--chart-font-y' as string]: 'clamp(9px, 0.8cqi, 13px)',
          ['--chart-font-x' as string]: 'clamp(9px, 0.75cqi, 12px)',
          // Title/legend/placeholder ужимаем до минимума — чарту больше места.
          // date-placeholder держим дефолтным 22px (ChartDateLabel тоже 22px)
          // — иначе при hover'е контейнер прыгал бы вверх-вниз (CLS).
          ['--seasonality-legend-font-size' as string]: 'clamp(9px, 0.7cqi, 12px)',
          ['--seasonality-legend-mb' as string]: '2px',
        } as React.CSSProperties}>
        <ExpandButton onClick={() => setModalView('yearly')} />
        <h3 className="text-center font-medium mb-0"
          style={{
            color: 'var(--text-secondary)',
            fontSize: 'clamp(10px, 0.7cqi, 13px)',
            lineHeight: 1.3,
          }}>
          Сезонный тренд {selectedName}
          {yearsRangeLabel && (
            <span className="text-theme-muted font-normal"> {yearsRangeLabel}</span>
          )}
        </h3>
        {yearlyData ? (
          <YearlySeasonalityChart
            key={`yearly-${testChartFetchIds.yearly}`}
            yearlyData={yearlyData}
            seriesData={yearlySeries}
            seriesMeta={seriesMeta}
            tooltip={chartTooltip}
            setTooltip={setChartTooltip}
            chartHeight={YEARLY_HEIGHT}
          />
        ) : (
          <div className="flex items-center justify-center"
            style={{ height: YEARLY_HEIGHT, color: 'var(--text-muted)' }}>
            Нет данных
          </div>
        )}
      </div>

      {/* ===== BOTTOM: 4 histograms в один ряд =====
          На xl (1280+) → 4 колонки в ряд (~430px каждая при 1800px container).
          На md (768+) → 2 колонки (fallback для tablet).
          На мобиле → 1 колонка стеком. */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3 xl:gap-4">
        {TEST_MODES.map(m => {
          const bars = testHistData[m]?.bars || [];
          const maxAbs = Math.max(...bars.map(b => Math.abs(b.avg_change)), 0.01);
          return (
            <div
              key={m}
              className="rounded-2xl border px-3 pt-1 pb-1.5 relative"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                borderColor: 'var(--border-color)',
                containerType: 'inline-size',
                // Row-режим: cell ~430px. aspect 1.5, все шрифты по cqi.
                ['--seasonality-hist-pad-x' as string]: '32px',
                ['--seasonality-hist-min-height' as string]: '240px',
                ['--seasonality-hist-max-height' as string]: '320px',
                ['--seasonality-aspect-ratio' as string]: '1.5',
                ['--chart-font-y' as string]: 'clamp(9px, 2.4cqi, 13px)',
                ['--chart-font-x' as string]: 'clamp(8px, 2cqi, 11px)',
              } as React.CSSProperties}
            >
              <ExpandButton onClick={() => setModalView(m)} />
              <h4 className="text-center font-medium mb-0"
                style={{
                  color: 'var(--text-secondary)',
                  fontSize: 'clamp(9px, 2.3cqi, 12px)',
                  lineHeight: 1.3,
                }}>
                {MODE_LABELS[m]}
              </h4>
              {bars.length === 0 ? (
                <div className="flex items-center justify-center"
                  style={{ minHeight: 240, color: 'var(--text-muted)' }}>
                  Нет данных
                </div>
              ) : (
                <SeasonalityHistogram
                  key={`${m}-${testChartFetchIds[m]}`}
                  bars={bars}
                  maxAbs={maxAbs}
                  tooltip={testHistTooltips[m]}
                  setTooltip={(t) => setTestHistTooltips(prev => ({ ...prev, [m]: t }))}
                  monthlySeries={testHistSeries[m]}
                  seriesMeta={seriesMeta}
                  compact
                />
              )}
            </div>
          );
        })}
      </div>

      {/* ===== MODAL: развёрнутый вид графика =====
          Одна модалка на 5 чартов. Переключение через табы yearly + 4 mode'а.
          Стейт (tooltip, data) переиспользуется с dashboard'а — hover в
          модалке не конфликтует т.к. гистограммы в dashboard'е скрыты overlay'ем. */}
      {modalView && (
        <ExpandedChartModal
          view={modalView}
          setView={setModalView}
          selectedStock={selectedStock}
          selectedName={selectedName}
          onOpenStockPicker={() => setModalStockPickerOpen(true)}
          renderFilters={renderFilters}
          yearlyData={yearlyData}
          yearlySeries={yearlySeries}
          seriesMeta={seriesMeta}
          histData={testHistData}
          histSeries={testHistSeries}
          chartTooltip={chartTooltip}
          setChartTooltip={setChartTooltip}
          histTooltips={testHistTooltips}
          setHistTooltips={setTestHistTooltips}
          testChartFetchIds={testChartFetchIds}
        />
      )}

      {/* Nested stock picker — над модалкой */}
      {modalStockPickerOpen && (
        <InstrumentSearchModal
          onSelect={(sectype, name) => {
            onSelectInstrument(sectype, name);
            setModalStockPickerOpen(false);
          }}
          onClose={() => setModalStockPickerOpen(false)}
          excludeType="futures"
        />
      )}
    </div>
  );
}

/** Кнопка «развернуть» в углу чарт-карточки. Две диагональные стрелки (Maximize2). */
function ExpandButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      title="Развернуть"
      aria-label="Развернуть график"
      className="absolute top-2 right-2 z-10 p-1.5 rounded-md opacity-50 hover:opacity-100 hover:bg-white/5 transition-all"
      style={{ color: 'var(--text-secondary)' }}
    >
      <Maximize2 size={14} />
    </button>
  );
}

/** Полноэкранная модалка с одним чартом (yearly | histogram-mode).
    Таб-бар сверху позволяет переключаться между всеми 5 видами без закрытия. */
interface ExpandedChartModalProps {
  view: 'yearly' | SeasonalityMode;
  setView: (v: ModalView) => void;
  selectedStock: string;
  selectedName: string;
  onOpenStockPicker: () => void;
  renderFilters: () => React.ReactNode;
  yearlyData: YearlySeasonalityResponse | null;
  yearlySeries: YearlySeasonalityResponse[] | null;
  seriesMeta: SeriesMeta[];
  histData: Record<SeasonalityMode, SeasonalityResponse | null>;
  histSeries: Record<SeasonalityMode, SeasonalityResponse[] | null>;
  chartTooltip: ChartTooltip | null;
  setChartTooltip: (t: ChartTooltip | null) => void;
  histTooltips: Record<SeasonalityMode, HistTooltip | null>;
  setHistTooltips: React.Dispatch<React.SetStateAction<Record<SeasonalityMode, HistTooltip | null>>>;
  testChartFetchIds: { yearly: number; intraday: number; weekday: number; monthday: number; monthly: number };
}

function ExpandedChartModal({
  view, setView, selectedStock, selectedName, onOpenStockPicker, renderFilters,
  yearlyData, yearlySeries, seriesMeta,
  histData, histSeries,
  chartTooltip, setChartTooltip, histTooltips, setHistTooltips,
  testChartFetchIds,
}: ExpandedChartModalProps) {
  const isYearly = view === 'yearly';
  const currentHist = !isYearly ? histData[view] : null;
  const histBars = currentHist?.bars || [];
  const histMaxAbs = Math.max(...histBars.map(b => Math.abs(b.avg_change)), 0.01);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ backgroundColor: 'rgba(0, 0, 0, 0.75)', backdropFilter: 'blur(4px)' }}
      onClick={(e) => { if (e.target === e.currentTarget) setView(null); }}
    >
      <div
        className="rounded-2xl border overflow-hidden flex flex-col"
        style={{
          backgroundColor: 'var(--bg-primary)',
          borderColor: 'var(--border-color)',
          width: '90vw',
          maxWidth: '1600px',
          height: '90vh',
          maxHeight: '900px',
          containerType: 'inline-size',
          // Динамические шрифты от ширины модалки (1440-1600px).
          ['--chart-font-y' as string]: 'clamp(11px, 0.9cqi, 16px)',
          ['--chart-font-x' as string]: 'clamp(10px, 0.85cqi, 14px)',
          ['--seasonality-legend-font-size' as string]: 'clamp(11px, 0.9cqi, 14px)',
          ['--seasonality-legend-mb' as string]: '8px',
          ['--chart-date-placeholder-height' as string]: '22px',
        } as React.CSSProperties}
      >
        {/* Header: stock picker + mode tabs + close */}
        <div className="flex items-center gap-3 px-4 py-3 border-b" style={{ borderColor: 'var(--border-color)' }}>
          <button
            onClick={onOpenStockPicker}
            className="flex items-center gap-2 px-3 py-2 rounded-xl border text-sm font-medium transition-colors hover:opacity-90"
            style={{
              backgroundColor: 'var(--bg-secondary)',
              borderColor: 'var(--border-color)',
              color: 'var(--text-primary)',
            }}
          >
            <div className="w-6 h-6 rounded-full flex-shrink-0 flex items-center justify-center text-[9px] font-bold text-white/70"
              style={{ backgroundColor: `hsl(${selectedStock.split('').reduce((a, c) => a + c.charCodeAt(0), 0) % 360}, 50%, 40%)` }}>
              {selectedStock.slice(0, 2)}
            </div>
            <div className="flex flex-col items-start leading-tight">
              <span className="text-[13px]">{selectedName}</span>
              <span className="text-[10px] text-theme-secondary">{selectedStock}</span>
            </div>
            <ChevronDown size={14} className="text-theme-secondary" />
          </button>

          {/* Tabs: yearly + 4 histogram modes — pill-group */}
          <div className="btn-group-scroll gap-1 p-1 rounded-xl border"
            style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}>
            <ModalTab active={view === 'yearly'} onClick={() => setView('yearly')}>Годовая</ModalTab>
            {TEST_MODES.map(m => (
              <ModalTab key={m} active={view === m} onClick={() => setView(m)}>
                {MODE_LABELS[m]}
              </ModalTab>
            ))}
          </div>

          <div className="flex-1" />
          <button
            onClick={() => setView(null)}
            className="p-2 rounded-lg hover:bg-white/5 transition-colors"
            style={{ color: 'var(--text-secondary)' }}
            title="Закрыть (ESC)"
            aria-label="Закрыть"
          >
            <X size={18} />
          </button>
        </div>

        {/* Filters row — те же что на главной, через renderFilters callback.
            State живёт в SeasonalityPage — любое изменение обновит и модалку,
            и dashboard позади (под overlay'ем). */}
        <div className="flex flex-wrap items-center gap-2 md:gap-3 px-4 py-2.5 border-b"
          style={{ borderColor: 'var(--border-color)' }}>
          {renderFilters()}
        </div>

        {/* Chart area — занимает весь остаток */}
        <div className="flex-1 p-4 overflow-hidden"
          style={{
            // Внутри модалки yearly занимает всю высоту (aspect не нужен, flex-1).
            ['--seasonality-aspect-ratio' as string]: isYearly ? '3.2' : '2.2',
            ['--seasonality-min-height' as string]: '300px',
            ['--seasonality-max-height' as string]: '800px',
            ['--seasonality-hist-min-height' as string]: '300px',
            ['--seasonality-hist-max-height' as string]: '800px',
            ['--seasonality-hist-pad-x' as string]: '80px',
          } as React.CSSProperties}>
          {isYearly ? (
            yearlyData ? (
              <YearlySeasonalityChart
                key={`modal-yearly-${testChartFetchIds.yearly}`}
                yearlyData={yearlyData}
                seriesData={yearlySeries}
                seriesMeta={seriesMeta}
                tooltip={chartTooltip}
                setTooltip={setChartTooltip}
                chartHeight={600}
              />
            ) : (
              <div className="h-full flex items-center justify-center text-theme-muted">Нет данных</div>
            )
          ) : (
            histBars.length > 0 ? (
              <SeasonalityHistogram
                key={`modal-${view}-${testChartFetchIds[view]}`}
                bars={histBars}
                maxAbs={histMaxAbs}
                tooltip={histTooltips[view]}
                setTooltip={(t) => setHistTooltips(prev => ({ ...prev, [view]: t }))}
                monthlySeries={histSeries[view]}
                seriesMeta={seriesMeta}
              />
            ) : (
              <div className="h-full flex items-center justify-center text-theme-muted">Нет данных</div>
            )
          )}
        </div>
      </div>
    </div>
  );
}

function ModalTab({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      className="px-3 py-1.5 text-xs md:text-sm font-medium rounded-lg transition-all whitespace-nowrap"
      style={{
        backgroundColor: active ? 'var(--accent)' : 'transparent',
        color: active ? 'var(--bg-primary)' : 'var(--text-secondary)',
      }}
    >
      {children}
    </button>
  );
}
