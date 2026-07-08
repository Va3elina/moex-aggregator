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
import { LineChart, Landmark, DollarSign, Building2, ChevronDown, Users, Lock } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import { METHODOLOGY } from '../data/methodology';
import {
  getCbrFlows,
  type CbrFlowsResponse,
  type CbrInstrumentType,
} from '../services/api';
import { useFitToViewport } from '../hooks/useFitToViewport';
import StackedBidirectionalHistogram from '../components/cbr/StackedBidirectionalHistogram';
import ChartNavigator from '../components/ChartNavigator';
import { getCategoryColor } from '../components/cbr/cbrPalette';
import { getCategoryInfo, getCategoryShortLabel } from '../components/cbr/cbrCategoryInfo';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import CsvExportButton from '../components/export/CsvExportButton';
import ChartActionsMenu from '../components/ChartActionsMenu';
import ChartSettings from '../components/chart/ChartSettings';
import ChartTabs from '../components/ChartTabs';
import SegmentedControl from '../components/SegmentedControl';
import { periodToQuery } from '../utils/csvPeriod';
import { useOnboardingTour } from '../hooks/useFirstVisit';
import { usePersistedState, usePersistedSet } from '../hooks/usePersistedState';
import { useIndicatorData } from '../hooks/useIndicatorData';
import OnboardingTour from '../components/onboarding/OnboardingTour';
import { cbrFlowsTourSteps } from '../data/tours/cbr-flows';
import { useTheme } from '../contexts/ThemeContext';
import { useTierAccess } from '../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../components/tier/UpgradeModal';

const INSTRUMENT_TABS: Array<{
  key: CbrInstrumentType;
  label: string;
  Icon: typeof LineChart;
}> = [
  { key: 'stocks', label: 'Акции',  Icon: LineChart },
  { key: 'ofz',    label: 'ОФЗ',    Icon: Landmark },
  { key: 'fx',     label: 'Валюты', Icon: DollarSign },
];

type PeriodFilter = '1y' | '3y' | 'all';
// Данные ОРФР помесячные (история с 2021 г., ~5 лет). Периоды открыты на всех
// тарифах, включая free (matrix cbr_flows free max_history_days=None) — замок с
// периодов снят, гейтинг теперь только на «институциональных» категориях.
const PERIOD_OPTIONS: { key: PeriodFilter; label: string; months: number | null }[] = [
  { key: '1y', label: '1Г', months: 12 },
  { key: '3y', label: '3Г', months: 36 },
  { key: 'all', label: 'Всё', months: null },
];

export default function CbrFlowsPage() {
  const { theme } = useTheme();
  // Тип актива (Акции/ОФЗ/Валюты) персистится в localStorage — не сбрасывается на новой сессии.
  const [type, setType] = usePersistedState<CbrInstrumentType>('frame:cbr:type', 'stocks');
  // Данные грузятся через useIndicatorData (reqId-guard от гонки при быстрой смене
  // типа теперь встроен в хук безусловно). errorMessage воспроизводит прежний raw
  // `e?.message ?? …`. Без SSE и без tier (период гейтится в onClick).
  const { data, loading, error } = useIndicatorData<CbrFlowsResponse>({
    fetcher: () => getCbrFlows(type),
    deps: [type],
    errorMessage: (e) => (e as { message?: string } | null)?.message ?? 'Не удалось загрузить данные',
  });

  // Категории-фильтр: какие категории скрыты из графика. Персистим в localStorage
  // с ключом по типу актива (frame:cbr:hidden:<type>) — выбор не сбрасывается на
  // новой сессии и хранится отдельно для stocks/ofz/fx (категории разные). Смена
  // type перечитывает набор под новый ключ — это заменяет прежний reset в пустой Set.
  const [hiddenCategories, setHiddenCategories] = usePersistedSet<string>(`frame:cbr:hidden:${type}`);

  // Период: 1г / 3г / Всё (default 1г) — персистится в localStorage
  const [period, setPeriod] = usePersistedState<PeriodFilter>('frame:cbr:period', '1y');
  // Tier-gating периодов (зеркало мобилки): «Всё» (безлимит истории) под
  // замком для free — matrix cbr_flows free = 365 дней. canUsePeriod читает
  // ту же матрицу что и backend-enforcement.
  const cbrAccess = useTierAccess('cbr_flows');
  const { showUpgrade } = useUpgradePrompt();

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

  // Категории под замком тарифа — источник истины бэкенд (locked_categories):
  // набор per-type, значения таких категорий на free не отдаются. Они остаются
  // в data.categories, чтобы показать их в пикере с замком (апселл на Базовый).
  const lockedCategories = useMemo(
    () => new Set(data?.locked_categories ?? []),
    [data],
  );

  // Видимые категории (для передачи в график): исключаем и скрытые вручную,
  // и залоченные тарифом.
  const visibleCategories = useMemo(() => {
    if (!data) return [];
    return data.categories.filter((c) => !hiddenCategories.has(c) && !lockedCategories.has(c));
  }, [data, hiddenCategories, lockedCategories]);

  // Фильтрованные periods по выбранному period filter (последние N месяцев / всё)
  const visiblePeriods = useMemo(() => {
    if (!data) return [];
    const opt = PERIOD_OPTIONS.find((o) => o.key === period);
    if (!opt || opt.months === null) return data.periods;
    return data.periods.slice(-opt.months);
  }, [data, period]);

  // Окно rail-навигатора по видимым периодам (таймлайн под графиком).
  const [navRange, setNavRange] = useState<[number, number]>([0, 0]);

  // Сброс окна при смене НАБОРА периодов (тип / период-пресет / догрузка данных) —
  // СИНХРОННО во время рендера (паттерн React «adjust state during render»), а не в
  // эффекте. Эффект сбрасывал navRange ПОСЛЕ кадра, поэтому на первом кадре после
  // смены периода окно оставалось старым → entrance-волна стартовала по неполному
  // срезу displayPeriods, а после досброса уже не перезапускалась (animKey привязан
  // к allPeriods, а не к срезу) → анимировался только «недостающий» промежуток, а
  // старый период оставался статичным. Сравнение по ИДЕНТИЧНОСТИ visiblePeriods
  // (новый массив при смене data/period, та же ссылка при драге таймлайна): драг
  // окно не сбрасывает → волна не переигрывается, смена периода — переигрывает целиком.
  const [prevVisible, setPrevVisible] = useState(visiblePeriods);
  if (prevVisible !== visiblePeriods) {
    setPrevVisible(visiblePeriods);
    setNavRange([0, Math.max(0, visiblePeriods.length - 1)]);
  }

  // Срез периодов по окну навигатора — его и рисует гистограмма.
  const displayPeriods = useMemo(() => {
    if (visiblePeriods.length === 0) return [];
    // navRange ещё не инициализирован эффектом → показываем всё.
    if (navRange[0] === 0 && navRange[1] === 0 && visiblePeriods.length > 1) return visiblePeriods;
    const s = Math.max(0, Math.min(navRange[0], visiblePeriods.length - 1));
    const e = Math.max(s, Math.min(navRange[1], visiblePeriods.length - 1));
    return visiblePeriods.slice(s, e + 1);
  }, [visiblePeriods, navRange]);

  // Данные для rail-навигатора: важна только длина (= число периодов).
  // Стабильная ссылка через useMemo — иначе новый массив каждый рендер сбрасывал
  // бы выделение навигатора в бесконечном цикле.
  const cbrNavData = useMemo(
    () => visiblePeriods.map((_, i) => ({ time: String(i), value: 0 })),
    [visiblePeriods]
  );

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
        title="Поток капитала"
        subtitle="Кто покупает и кто продаёт по типам активов — по данным Банка России"
        help={METHODOLOGY.cbrFlows}
        helpLink="/methodology/cbr-flows"
        sourceNote="Источник: Банк России (ОРФР)"
      />

      {/* Карточка с вкладками: обёртка несёт единую editorial-тень. */}
      <div className="tabbed-card">

      {/* Вкладки типа актива — приклеены к верхней кромке панели. Активная
          сливается с панелью, неактивные затемнены. Категории/период — внутри. */}
      <ChartTabs<CbrInstrumentType>
        tourId="cbr-type"
        value={type}
        onChange={setType}
        items={INSTRUMENT_TABS.map((t) => ({ key: t.key, label: t.label, Icon: t.Icon }))}
      />

      <div className="editorial-frame has-tabs">

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
                    // Локнутая тарифом категория — клик ведёт на апгрейд, а не toggle.
                    const locked = lockedCategories.has(cat);
                    const isHidden = hiddenCategories.has(cat);
                    const isLastVisible = !isHidden && !locked && visibleCategories.length === 1;
                    const color = getCategoryColor(cat, theme);
                    const info = getCategoryInfo(cat);
                    // Визуально «выключенной» считаем и скрытую вручную, и локнутую.
                    const dimmed = isHidden || locked;
                    return (
                      <button
                        key={cat}
                        onClick={() => {
                          if (locked) {
                            showUpgrade({ tier: 'basic', featureName: `категория «${cat}»`, indicator: 'cbr_flows' });
                            return;
                          }
                          if (!isLastVisible) toggleCategory(cat);
                        }}
                        disabled={isLastVisible}
                        className="w-full text-left rounded-xl transition-all duration-150"
                        style={{
                          padding: 'var(--sp-3)',
                          marginBottom: 'var(--sp-1)',
                          background: dimmed ? 'transparent' : 'color-mix(in srgb, var(--text-primary) 4%, transparent)',
                          opacity: dimmed ? 0.4 : 1,
                          cursor: isLastVisible ? 'not-allowed' : 'pointer',
                          border: '1.5px solid transparent',
                          borderColor: dimmed ? 'transparent' : 'color-mix(in srgb, var(--text-primary) 12%, transparent)',
                        }}
                        title={locked
                          ? 'Доступно на тарифе Базовый'
                          : isLastVisible
                          ? 'Нельзя скрыть последнюю видимую категорию'
                          : isHidden ? 'Показать на графике' : 'Скрыть с графика'}
                      >
                        <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
                          {/* Checkbox-style indicator (локнутая → замок) */}
                          <div
                            className="flex items-center justify-center flex-shrink-0 rounded-md"
                            style={{
                              width: 22, height: 22,
                              marginTop: 2,
                              background: dimmed ? 'transparent' : color,
                              border: `2px solid ${dimmed ? 'var(--text-muted)' : color}`,
                              transition: 'all 150ms',
                            }}
                          >
                            {locked ? (
                              <Lock size={12} strokeWidth={2.4} style={{ color: 'var(--text-muted)' }} />
                            ) : !isHidden && (
                              <svg viewBox="0 0 14 14" width="12" height="12" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                                <polyline points="2 7 6 11 12 3" />
                              </svg>
                            )}
                          </div>
                          <div className="flex-1 min-w-0">
                            <div
                              className="font-bold flex items-center"
                              style={{
                                gap: 'var(--sp-2)',
                                fontSize: 'var(--fs-sm)',
                                color: 'var(--text-primary)',
                                marginBottom: 'var(--sp-1)',
                              }}
                            >
                              {cat}
                              {locked && (
                                <span
                                  className="font-bold uppercase rounded-full"
                                  style={{
                                    fontSize: '0.6rem',
                                    letterSpacing: '0.04em',
                                    padding: '1px 6px',
                                    color: 'var(--accent)',
                                    border: '1px solid var(--accent)',
                                  }}
                                >
                                  Базовый
                                </span>
                              )}
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

          {/* === Период (1Г / Всё) — горизонтальный ряд, «Всё» под замком для free === */}
          <div data-tour="cbr-period">
          <SegmentedControl<PeriodFilter>
            options={PERIOD_OPTIONS.map((opt) => ({
              key: opt.key,
              label: opt.label,
              locked: !(cbrAccess.isLoading || cbrAccess.canUsePeriod(opt.key)),
            }))}
            value={period}
            onChange={setPeriod}
            onLockedClick={(p) => {
              const tier = cbrAccess.requiredTierFor({ period: p });
              if (tier) {
                showUpgrade({ tier, featureName: `период «${PERIOD_OPTIONS.find((o) => o.key === p)?.label ?? p}»`, indicator: 'cbr_flows' });
              }
            }}
          />
          </div>

          {/* Действия (Скриншот/CSV) свёрнуты в kebab «⋮» в углу графика (паттерн OI).
              Через portal монтируется в обёртку графика (containerRef=chartAnchorRef). */}
          <ChartActionsMenu containerRef={chartAnchorRef} tourId="cbr-export">
          <CsvExportButton
            indicator="cbr_flows"
            config={() => ({
              indicator: 'cbr_flows',
              title: 'Экспорт: Поток капитала',
              layers: [{
                id: 'flows',
                label: 'Потоки ОРФР',
                description: 'period_year, label, category, value (млрд ₽)',
                defaultSelected: true,
              }],
              // Unified порядок: тип инструмента (актив-эквивалент) →
              // категории (mode-эквивалент) → период.
              selectors: [
                {
                  kind: 'multiselect',
                  id: 'instruments',
                  label: 'Тип инструмента',
                  default: [type],
                  hint: 'Несколько → ZIP с CSV per тип',
                  options: INSTRUMENT_TABS.map(t => ({ value: t.key, label: t.label })),
                },
                {
                  kind: 'multiselect',
                  id: 'categories',
                  label: 'Категории участников',
                  default: [], // пустой = все категории (backend не фильтрует)
                  hint: 'Пусто = все категории',
                  // Подписи берём из getCategoryShortLabel — единый источник
                  // правды с легендой графика, чтобы они не разъезжались.
                  options: [
                    'Физические лица',
                    'СЗКО',
                    'Прочие Банки',
                    'Нерезиденты',
                    'НФО',
                    'Нефинансовые организации',
                    'Доверительное управление',
                    'Банк России',
                    'Российские кредитные организации',
                    'Клиенты российских кредитных организаций',
                  ].map((value) => ({ value, label: getCategoryShortLabel(value) })),
                },
                {
                  kind: 'period',
                  id: 'period',
                  label: 'Период',
                  default: { type: 'preset', value: period },
                  presets: [
                    { value: '1y', label: '1Г', days: 365 },
                    { value: '3y', label: '3Г', days: 1095 },
                    { value: '5y', label: '5Л', days: 1825 },
                    { value: 'all', label: 'Всё', days: 11000 },
                  ],
                },
              ],
              params: [],
              buildUrl: (_layers, vals) => {
                const insts = (vals.instruments as string[] ?? [type]).join(',');
                const cats = (vals.categories as string[] ?? []);
                const catsParam = cats.length > 0 ? `&categories=${encodeURIComponent(cats.join(','))}` : '';
                // ORFR хранит данные по годам, так что period конвертируется в `years`.
                let yearsParam = '';
                const pv = vals.period;
                if (pv && typeof pv === 'object' && (pv as { type?: string }).type === 'range') {
                  // Range — берём years = diff в годах.
                  const r = pv as { type: 'range'; from: string; to: string };
                  const fromY = parseInt(r.from.slice(0, 4), 10);
                  const toY = parseInt(r.to.slice(0, 4), 10);
                  yearsParam = `&years=${Math.max(1, toY - fromY + 1)}`;
                } else {
                  // Preset → days → years.
                  const days = parseInt(periodToQuery(pv, 365).replace('days=', ''), 10);
                  yearsParam = `&years=${Math.max(1, Math.round(days / 365))}`;
                }
                return `/api/export/cbr-flows.csv?instrument=${insts}${yearsParam}${catsParam}`;
              },
              buildFilename: () => `cbr_flows_${Date.now()}.zip`,
            })}
          />
          <ChartCaptureButton
            getTargetElement={() => chartAnchorRef.current}
            filename={`frame-cbr-flows-${type}-${period}`}
            metadata={{
              // asset намеренно НЕ задаём: в шапке экспорта primary = asset ?? title,
              // и для «Потока капитала» главным заголовком должен быть сам индикатор,
              // а тип актива (Акции/ОФЗ/Валюта) уходит в подзаголовок.
              title: 'Поток капитала',
              details: [
                data?.instrument_label ?? INSTRUMENT_TABS.find(t => t.key === type)?.label ?? '',
                `Период: ${PERIOD_OPTIONS.find(o => o.key === period)?.label}`,
                'Источник: Банк России · ОРФР',
                // `data.source` (имя XLSX-файла) намеренно убрано — не информативно
                // для пользователя в подписи экспорта.
              ].filter(Boolean) as string[],
            }}
          />
          <ChartSettings showType={false} />
          </ChartActionsMenu>
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
            // position:relative — точка монтирования для portal kebab'а действий.
            position: 'relative',
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
              periods={displayPeriods}
              categories={visibleCategories}
              allPeriods={data?.periods}
              unit={data?.unit ?? 'млрд руб.'}
              height={chartHeight}
              loading={loading}
              animTrigger={`${type}|${period}`}
            />
          )}

          {/* Rail-таймлайн — единый вид со всеми графиками. Окно по visiblePeriods,
              срез уходит в гистограмму как displayPeriods. */}
          {!error && cbrNavData.length > 1 && (
            <div className="cbr-nav-wrap" data-export-ignore="true">
              <ChartNavigator
                data={cbrNavData}
                color="var(--accent)"
                previewMode="line"
                onChange={(s, e) => setNavRange([s, e])}
                insetLeft="var(--chart-pad-left)"
                insetRight="var(--chart-pad-right-single)"
              />
            </div>
          )}
        </div>
      </div>{/* /editorial-frame */}

      </div>{/* /tabbed-card */}

      <OnboardingTour
        steps={cbrFlowsTourSteps}
        open={tour.open}
        onClose={tour.close}
      />
    </div>
  );
}
