/**
 * /cbr-flows — Потоки участников биржевых торгов по данным ЦБ (ОРФР).
 *
 * Источник: ежемесячный обзор финансовой стабильности Банка России.
 * Бэкенд парсит XLSX с cbr.ru/analytics/finstab/orfr/ и upsert'ит в БД
 * (см. CBR/fetch_orfr_flows.py).
 *
 * Структура:
 *   • Header (title + help link)
 *   • Editorial frame
 *     • Chip-row из 3 типов (Акции / ОФЗ / Валюты) — переключатель
 *     • Stacked bidirectional histogram (positive вверх, negative вниз)
 *     • Подпись года под осью X
 *   • Footer (источник + дата обновления)
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import { LineChart, Landmark, DollarSign, Building2, ChevronDown, Users } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import { METHODOLOGY } from '../data/methodology';
import {
  getCbrFlows,
  type CbrFlowsResponse,
  type CbrInstrumentType,
} from '../services/api';
import { useFitToViewport } from '../hooks/useFitToViewport';
import StackedBidirectionalHistogram from '../components/cbr/StackedBidirectionalHistogram';
import { getCategoryColor } from '../components/cbr/cbrPalette';
import { getCategoryInfo } from '../components/cbr/cbrCategoryInfo';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { cbrFlowsTourSteps } from '../data/tours/cbr-flows';
import { useTheme } from '../contexts/ThemeContext';

const INSTRUMENT_TABS: Array<{
  key: CbrInstrumentType;
  label: string;
  Icon: typeof LineChart;
}> = [
  { key: 'stocks', label: 'Акции',  Icon: LineChart },
  { key: 'ofz',    label: 'ОФЗ',    Icon: Landmark },
  { key: 'fx',     label: 'Валюты', Icon: DollarSign },
];

type PeriodFilter = '1y' | '2y' | 'all';
const PERIOD_OPTIONS: { key: PeriodFilter; label: string; months: number | null }[] = [
  { key: '1y', label: '1Г', months: 12 },
  { key: '2y', label: '2Г', months: 24 },
  { key: 'all', label: 'Всё', months: null },
];

export default function CbrFlowsPage() {
  const { theme } = useTheme();
  const [type, setType] = useState<CbrInstrumentType>('stocks');
  const [data, setData] = useState<CbrFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Категории-фильтр: какие категории скрыты из графика.
  // При смене type — сбрасываем (категории различаются для stocks/ofz/fx).
  const [hiddenCategories, setHiddenCategories] = useState<Set<string>>(new Set());

  // Период: 1г / 2г / Всё (default 1г)
  const [period, setPeriod] = useState<PeriodFilter>('1y');

  // Popover-dropdown с выбором категорий (открывается при клике на кнопку)
  const [categoriesOpen, setCategoriesOpen] = useState(false);

  // Onboarding tour
  const tour = useOnboardingTour('cbr-flows');
  const categoriesBtnRef = useRef<HTMLDivElement>(null);

  // Click-outside handler: закрывает popover при клике вне его контейнера
  useEffect(() => {
    if (!categoriesOpen) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as Node;
      if (categoriesBtnRef.current && !categoriesBtnRef.current.contains(target)) {
        setCategoriesOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [categoriesOpen]);

  // Esc-key closes popover
  useEffect(() => {
    if (!categoriesOpen) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setCategoriesOpen(false);
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [categoriesOpen]);

  const chartAnchorRef = useRef<HTMLDivElement>(null);
  const chartHeight = useFitToViewport(chartAnchorRef, {
    min: 360,
    max: 640,
    bottomBuffer: 96,
  });

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    getCbrFlows(type)
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setLoading(false);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(e?.message ?? 'Не удалось загрузить данные');
        setLoading(false);
      });
    return () => { cancelled = true; };
  }, [type]);

  // Reset hidden при смене типа актива
  useEffect(() => {
    setHiddenCategories(new Set());
  }, [type]);

  // Видимые категории (для передачи в график)
  const visibleCategories = useMemo(() => {
    if (!data) return [];
    return data.categories.filter((c) => !hiddenCategories.has(c));
  }, [data, hiddenCategories]);

  // Фильтрованные periods по выбранному period filter (последние N месяцев / всё)
  const visiblePeriods = useMemo(() => {
    if (!data) return [];
    const opt = PERIOD_OPTIONS.find((o) => o.key === period);
    if (!opt || opt.months === null) return data.periods;
    return data.periods.slice(-opt.months);
  }, [data, period]);

  const toggleCategory = (cat: string) => {
    setHiddenCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) {
        next.delete(cat);
      } else {
        // Нельзя скрыть последнюю видимую категорию
        if (visibleCategories.length <= 1) return prev;
        next.add(cat);
      }
      return next;
    });
  };

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
      <PageHeader
        icon={Building2}
        title="Потоки участников биржи"
        subtitle="Кто покупает и кто продаёт по типам активов — по данным Банка России"
        help={METHODOLOGY.cbrFlows}
        helpLink="/methodology/cbr-flows"
        sourceNote="Источник: Банк России (ОРФР)"
      />

      <div className="editorial-frame">
        {/* === Row 1: 3 chip'а типа активов (grid-cols-3, равные ширины) === */}
        <div
          data-tour="cbr-type"
          className="grid grid-cols-3 mb-3 md:mb-4"
          style={{ gap: 'var(--sp-2)' }}
        >
          {INSTRUMENT_TABS.map((t) => {
            const isActive = type === t.key;
            const Icon = t.Icon;
            return (
              <button
                key={t.key}
                onClick={() => setType(t.key)}
                className="editorial-press flex items-center justify-center font-semibold rounded-full min-w-0"
                style={{
                  gap: 'var(--sp-2)',
                  padding: 'var(--sp-2) var(--sp-3)',
                  fontSize: 'var(--fs-sm)',
                  backgroundColor: isActive ? 'var(--accent)' : 'var(--bg-secondary)',
                  color: isActive ? 'var(--text-inverse)' : 'var(--text-primary)',
                  border: '2px solid var(--text-primary)',
                  boxShadow: isActive ? 'var(--shadow-hard-chip)' : undefined,
                }}
              >
                <Icon
                  className="shrink-0"
                  style={{ width: 'var(--ico-sm)', height: 'var(--ico-sm)' }}
                />
                <span className="truncate">{t.label}</span>
              </button>
            );
          })}
        </div>

        {/* === Row 2: Категории + Период chips слева, camera button справа === */}
        <div
          className="flex flex-wrap items-center mb-4 md:mb-6"
          style={{ gap: 'var(--sp-2)' }}
        >

          {/* === Категории — popover-dropdown с rich items (слева в Row 2).
              Кнопка показывает счётчик «N/M» + chevron. Клик → раскрывается
              панель ниже с карточками каждой категории (dot + name + desc + ✓).
              Click-outside и Esc закрывают. */}
          <div ref={categoriesBtnRef} data-tour="cbr-categories" className="relative shrink-0">
            <button
              onClick={() => setCategoriesOpen((o) => !o)}
              className="editorial-press flex items-center font-semibold rounded-full"
              style={{
                gap: 'var(--sp-2)',
                padding: 'var(--sp-2) var(--sp-3)',
                fontSize: 'var(--fs-sm)',
                backgroundColor: categoriesOpen ? 'var(--accent)' : 'var(--bg-secondary)',
                color: categoriesOpen ? 'var(--text-inverse)' : 'var(--text-primary)',
                border: '2px solid var(--text-primary)',
                boxShadow: categoriesOpen ? 'var(--shadow-hard-chip)' : undefined,
              }}
            >
              <Users style={{ width: 'var(--ico-sm)', height: 'var(--ico-sm)' }} />
              <span className="whitespace-nowrap">
                Категории {data ? `${visibleCategories.length}/${data.categories.length}` : ''}
              </span>
              <ChevronDown
                style={{
                  width: 'var(--ico-sm)',
                  height: 'var(--ico-sm)',
                  transition: 'transform 200ms',
                  transform: categoriesOpen ? 'rotate(180deg)' : 'rotate(0deg)',
                }}
              />
            </button>

            {/* === Popover — absolute с правым краем выровненным к кнопке === */}
            {categoriesOpen && data && (
              <div
                className="absolute z-30 rounded-2xl"
                style={{
                  top: 'calc(100% + var(--sp-2))',
                  left: 0,
                  width: 'min(420px, calc(100vw - 32px))',
                  maxHeight: '70vh',
                  overflowY: 'auto',
                  background: 'var(--bg-primary)',
                  border: '2px solid var(--text-primary)',
                  boxShadow: 'var(--shadow-hard-card, 6px 6px 0 var(--text-primary))',
                }}
              >
                {/* Header popover */}
                <div
                  className="flex items-center justify-between border-b border-theme"
                  style={{ padding: 'var(--sp-3) var(--sp-4)' }}
                >
                  <span
                    className="font-bold"
                    style={{ fontSize: 'var(--fs-base)', color: 'var(--text-primary)' }}
                  >
                    Участники биржи
                  </span>
                  <span
                    className="font-mono font-bold"
                    style={{ fontSize: 'var(--fs-xs)', color: 'var(--accent)' }}
                  >
                    {visibleCategories.length}/{data.categories.length}
                  </span>
                </div>

                {/* Items list */}
                <div style={{ padding: 'var(--sp-2)' }}>
                  {data.categories.map((cat) => {
                    const isHidden = hiddenCategories.has(cat);
                    const isLastVisible = !isHidden && visibleCategories.length === 1;
                    const color = getCategoryColor(cat, theme);
                    const info = getCategoryInfo(cat);
                    return (
                      <button
                        key={cat}
                        onClick={() => {
                          if (!isLastVisible) toggleCategory(cat);
                        }}
                        disabled={isLastVisible}
                        className="w-full text-left rounded-xl transition-all duration-150"
                        style={{
                          padding: 'var(--sp-3)',
                          marginBottom: 'var(--sp-1)',
                          background: isHidden ? 'transparent' : 'color-mix(in srgb, var(--text-primary) 4%, transparent)',
                          opacity: isHidden ? 0.4 : 1,
                          cursor: isLastVisible ? 'not-allowed' : 'pointer',
                          border: '1.5px solid transparent',
                          borderColor: isHidden ? 'transparent' : 'color-mix(in srgb, var(--text-primary) 12%, transparent)',
                        }}
                        title={isLastVisible
                          ? 'Нельзя скрыть последнюю видимую категорию'
                          : isHidden ? 'Показать на графике' : 'Скрыть с графика'}
                      >
                        <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
                          {/* Checkbox-style indicator */}
                          <div
                            className="flex items-center justify-center flex-shrink-0 rounded-md"
                            style={{
                              width: 22, height: 22,
                              marginTop: 2,
                              background: isHidden ? 'transparent' : color,
                              border: `2px solid ${isHidden ? 'var(--text-muted)' : color}`,
                              transition: 'all 150ms',
                            }}
                          >
                            {!isHidden && (
                              <svg viewBox="0 0 14 14" width="12" height="12" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                <polyline points="2 7 6 11 12 3" />
                              </svg>
                            )}
                          </div>
                          <div className="flex-1 min-w-0">
                            <div
                              className="font-bold"
                              style={{
                                fontSize: 'var(--fs-sm)',
                                color: 'var(--text-primary)',
                                marginBottom: 'var(--sp-1)',
                              }}
                            >
                              {cat}
                            </div>
                            {info && (
                              <div
                                style={{
                                  fontSize: 'var(--fs-xs)',
                                  color: 'var(--text-secondary)',
                                  lineHeight: 1.4,
                                }}
                              >
                                {info}
                              </div>
                            )}
                          </div>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </div>
            )}
          </div>

          {/* === Период chips (6М / 2Г / Всё) === */}
          <div data-tour="cbr-period" className="flex items-center" style={{ gap: 'var(--sp-1)' }}>
            {PERIOD_OPTIONS.map((opt) => {
              const isActive = period === opt.key;
              return (
                <button
                  key={opt.key}
                  onClick={() => setPeriod(opt.key)}
                  className="editorial-press font-semibold rounded-full"
                  style={{
                    padding: 'var(--sp-2) var(--sp-3)',
                    fontSize: 'var(--fs-sm)',
                    minWidth: '48px',
                    backgroundColor: isActive ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: isActive ? 'var(--text-inverse)' : 'var(--text-primary)',
                    border: '2px solid var(--text-primary)',
                    boxShadow: isActive ? 'var(--shadow-hard-chip)' : undefined,
                  }}
                >
                  {opt.label}
                </button>
              );
            })}
          </div>

          {/* Camera button — экспорт графика в PNG */}
          <div data-tour="cbr-export" className="shrink-0 ml-auto">
          <ChartCaptureButton
            getTargetElement={() => chartAnchorRef.current}
            filename={`frame-cbr-flows-${type}-${period}`}
            metadata={{
              title: 'Потоки участников биржи',
              asset: data?.instrument_label ?? INSTRUMENT_TABS.find(t => t.key === type)?.label ?? '',
              details: [
                `Период: ${PERIOD_OPTIONS.find(o => o.key === period)?.label}`,
                'Источник: Банк России · ОРФР',
                // `data.source` (имя XLSX-файла) намеренно убрано — не информативно
                // для пользователя в подписи экспорта.
              ].filter(Boolean) as string[],
            }}
          />
          </div>
        </div>

        {/* === Inner paper-card вокруг графика ===
            bg-primary = главный фон сайта (paper light / чёрный dark) —
            графики лежат на «странице», а не на secondary card-фоне
            frame'а. 1.5px outline + rounded-2xl как у Heatmap. */}
        <div
          ref={chartAnchorRef}
          data-tour="cbr-chart"
          className="rounded-2xl"
          style={{
            background: 'var(--bg-primary)',
            border: '1.5px solid var(--text-primary)',
            // padding-bottom 12 — gap между year labels и rounded paper-card edge.
            // Без него html2canvas при export обрезал year (близко к радиусной зоне).
            // overflow убран — content может вылезти за rounded corners, но
            // axis labels ОТКЛ paper-card edges (через CSS-var paddings), не
            // достают углов.
            padding: '0 0 12px 0',
          }}
        >
          {error ? (
            <div
              className="flex items-center justify-center"
              style={{
                height: `${chartHeight}px`,
                color: 'var(--text-secondary)',
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-4)',
                textAlign: 'center',
              }}
            >
              <div>
                <div className="font-bold mb-2">Не удалось загрузить данные</div>
                <div style={{ fontSize: 'var(--fs-xs)', opacity: 0.8 }}>{error}</div>
              </div>
            </div>
          ) : (
            <StackedBidirectionalHistogram
              periods={visiblePeriods}
              categories={visibleCategories}
              allPeriods={data?.periods}
              unit={data?.unit ?? 'млрд руб.'}
              height={chartHeight}
              loading={loading}
              animTrigger={`${type}|${period}`}
            />
          )}
        </div>
      </div>{/* /editorial-frame */}

      <OnboardingTour
        steps={cbrFlowsTourSteps}
        open={tour.open}
        onClose={tour.close}
      />
    </div>
  );
}
