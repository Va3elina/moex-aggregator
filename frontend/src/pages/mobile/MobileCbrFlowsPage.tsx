/**
 * MobileCbrFlowsPage — мобильная версия «Поток капитала».
 *
 * Phase 3: упрощённая версия:
 *   - Type tabs (Акции / ОФЗ / Валюта) — chip row
 *   - Period chips (6м / 1г / Всё)
 *   - Existing StackedBidirectionalHistogram переиспользуется (адаптивен)
 *   - Hidden categories через sheet — Phase 4
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { Banknote, Landmark, DollarSign, Building2, Lock } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileSheet from '../../components/mobile/MobileSheet';
import MobileSkeleton from '../../components/mobile/MobileSkeleton';
import { axisFontSize } from '../../components/chart/chartTypography';
import { getCategoryColor } from '../../components/cbr/cbrPalette';
import { getDefaultHiddenCategories } from '../../components/cbr/cbrDefaultVisibility';
import { useTheme } from '../../contexts/ThemeContext';
import {
  getCbrFlows,
  type CbrInstrumentType,
  type CbrFlowsResponse,
  type CbrFlowsPeriod,
} from '../../services/api';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import { usePersistedState, usePersistedSet } from '../../hooks/usePersistedState';
import OnboardingTour from '../../components/onboarding/OnboardingTour';
import type { TourStep } from '../../components/onboarding/OnboardingTour';

type PeriodFilter = '1y' | '3y' | 'all';

const TYPE_TABS: Array<{ key: CbrInstrumentType; label: string; Icon: typeof Banknote }> = [
  { key: 'stocks', label: 'Акции', Icon: Building2 },
  { key: 'ofz', label: 'ОФЗ', Icon: Landmark },
  { key: 'fx', label: 'Валюта', Icon: DollarSign },
];

const PERIOD_OPTIONS: Array<{ key: PeriodFilter; label: string; months: number | null }> = [
  // Данные помесячные (история с 2021 г., ~5 лет). Периоды открыты на всех
  // тарифах, включая free (matrix cbr_flows free max_history_days=None) — замки
  // с периодов сняты, гейтинг теперь на «институциональных» категориях.
  { key: '1y', label: '1Г', months: 12 },
  { key: '3y', label: '3Г', months: 36 },
  { key: 'all', label: 'Всё', months: null },
];

export default function MobileCbrFlowsPage() {
  // Шарим desktop-ключи: enum'ы CbrInstrumentType и PeriodFilter побайтово
  // идентичны desktop (CbrFlowsPage), дефолты совпадают ('stocks'/'1y'), а
  // onClick-гейт периода не даёт free-юзеру записать платное 'all'. → выбор
  // переживает reload и консистентен с desktop в том же браузере.
  const [type, setType] = usePersistedState<CbrInstrumentType>('frame:cbr:type', 'stocks');
  const [period, setPeriod] = usePersistedState<PeriodFilter>('frame:cbr:period', '1y');
  const [data, setData] = useState<CbrFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [timeSheetOpen, setTimeSheetOpen] = useState(false);
  const [optionsSheetOpen, setOptionsSheetOpen] = useState(false);
  // Hidden categories — Set, исключаются из стека (как на десктопе). Персистим
  // по типу актива (frame:cbr:hidden:<type>, общий ключ с десктопом) — выбор не
  // сбрасывается на новой сессии и хранится отдельно для stocks/ofz/fx. Первый
  // визит — дефолт сужен до базовых категорий (см. cbrDefaultVisibility): до 7
  // участников (stocks/ofz) занимают слишком много места на мобильном экране.
  const [hiddenCategories, setHiddenCategories] = usePersistedSet<string>(
    `frame:cbr:hidden:${type}`,
    getDefaultHiddenCategories(type),
  );

  // Tier-gating периодов: free = до 365 дней (matrix cbr_flows), «Всё»
  // (безлимит) под замком. canUsePeriod читает ту же матрицу что и backend;
  // зеркалит десктоп и паттерн других индикаторов (Buffett).
  const cbrAccess = useTierAccess('cbr_flows');
  const { showUpgrade } = useUpgradePrompt();

  const tour = useOnboardingTour('cbr-flows');
  // Единый паттерн тура: Intro → Кнопки → Время → Опции (с демо) → Чтение → End
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Поток капитала',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Кто покупает и кто продаёт на российском рынке: банки, СЗКО,
            физлица, нерезиденты. Данные раскрываются Банком России
            ежемесячно — структурный взгляд на рынок.
          </p>
          <p>Разберём управление и научимся читать гистограмму.</p>
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
            <strong>Время</strong> — глубина истории (1Г/Всё)
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Опции</strong> — тип инструмента (Акции/ОФЗ/Валюта)
            + скрытие отдельных категорий участников
          </p>
          <p>
            <strong>Экран</strong> — развернуть гистограмму
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
    },
    {
      selector: null,
      title: 'Период',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл для тебя кнопку <strong>«Время»</strong>:
          </p>
          <p>
            <strong>1Г / Всё</strong> — глубина истории помесячно.
            На длинных периодах виден исторический контекст потоков.
          </p>
        </>
      ),
      onEnter: () => { setTimeSheetOpen(true); setOptionsSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Инструменты и категории',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 4 }}>
            Открыл кнопку <strong>«Опции»</strong>:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Тип инструмента:</strong> Акции, ОФЗ (облигации фед.займа),
            Валюта. У каждого свой список категорий участников.
          </p>
          <p>
            <strong>Категории участников</strong> можно скрывать/показывать
            индивидуально — изолируешь именно тот сегмент, что важен
            (например, только «Физлица» или «СЗКО»).
          </p>
        </>
      ),
      onEnter: () => { setOptionsSheetOpen(true); setTimeSheetOpen(false); },
    },
    {
      selector: '[data-tour="cbr-chart"]',
      title: 'Чтение гистограммы',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Стек-бары по месяцам. <strong>Сверху от нуля</strong> — нетто-покупки
            категории, <strong>снизу</strong> — нетто-продажи. Цвет = категория.
          </p>
          <p>
            Зажми палец — появится тултип с конкретными значениями за месяц
            по каждому участнику.
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
          Нажми <strong>?</strong> рядом с заголовком — методология и
          источники данных ЦБ (обзоры финансового рынка).
        </p>
      ),
    },
  ];

  // Загрузка
  // Stale-guard: reqIdRef на УРОВНЕ компонента — переживает пересоздание loadData
  // по [type]. При быстром переключении типа (или дубле onRefresh + эффект)
  // медленный ранний ответ не должен перезаписать свежий. myId фиксирует
  // «последний» запрос; устаревший ответ не пишет state и не гасит спиннер свежего.
  const reqIdRef = useRef(0);
  const loadData = useMemo(
    () => async () => {
      const myId = ++reqIdRef.current;
      const isStale = () => myId !== reqIdRef.current;
      try {
        setLoading(true);
        setError(null);
        const result = await getCbrFlows(type);
        if (isStale()) return;
        setData(result);
      } catch (err) {
        if (isStale()) return;
        setError(err instanceof Error ? err.message : 'Ошибка загрузки');
      } finally {
        if (!isStale()) setLoading(false);
      }
    },
    [type],
  );

  useEffect(() => {
    void loadData();
  }, [loadData]);

  // Применение фильтра периода
  const visiblePeriods = useMemo(() => {
    if (!data) return [];
    const opt = PERIOD_OPTIONS.find((o) => o.key === period);
    if (!opt || opt.months === null) return data.periods;
    return data.periods.slice(-opt.months);
  }, [data, period]);

  // Категории под замком тарифа — источник истины бэкенд (locked_categories,
  // per-type). Их значения на free не отдаются; в списке показываем с замком.
  const lockedCategories = useMemo(
    () => new Set(data?.locked_categories ?? []),
    [data],
  );

  // Hidden categories filtering. Исключаем и скрытые вручную, и залоченные тарифом.
  const visibleCategories = useMemo(
    () => (data?.categories ?? []).filter(
      (c) => !hiddenCategories.has(c) && !lockedCategories.has(c),
    ),
    [data, hiddenCategories, lockedCategories],
  );

  const toggleCategory = (cat: string) => {
    setHiddenCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  const typeLabel = TYPE_TABS.find((t) => t.key === type)?.label ?? '';
  const periodLabel = PERIOD_OPTIONS.find((p) => p.key === period)?.label ?? '';

  return (
    <MobileLayout
      onTimeClick={() => setTimeSheetOpen(true)}
      timeSummary={periodLabel}
      timeTourId="cbr-time"
      onSettingsClick={() => setOptionsSheetOpen(true)}
      settingsSummary={typeLabel}
      settingsTourId="cbr-type"
      onRefresh={loadData}
      loading={loading}
    >
      <MobilePageHeader
        Icon={Banknote}
        title="Поток капитала"
        subtitle={`${typeLabel} · ${periodLabel}`}
        helpLink="/methodology/cbr-flows"
        sourceNote="Источник: Банк России (ОРФР)"
      />

      {/* Mobile-optimized stacked histogram — pixel coords, touch handlers,
          vibrate tick, compact tooltip, year в датах. */}
      <div
        data-tour="cbr-chart"
        style={{ flex: 1, minHeight: 0, position: 'relative' }}
      >
        <div style={{ position: 'absolute', inset: 0 }}>
          {error ? (
            <div style={{ padding: 24, color: 'var(--text-muted)', textAlign: 'center' }}>{error}</div>
          ) : loading ? (
            <MobileSkeleton variant="chart" height="100%" />
          ) : data && visiblePeriods.length > 0 ? (
            <MobileCbrHistogram
              periods={visiblePeriods}
              categories={visibleCategories}
              animKey={`${type}|${period}|${Array.from(hiddenCategories).sort().join(',')}`}
            />
          ) : (
            <div style={{ padding: 24, color: 'var(--text-muted)', textAlign: 'center' }}>Нет данных</div>
          )}
        </div>
      </div>

      {/* Time sheet — period */}
      <MobileSheet
        open={timeSheetOpen}
        onClose={() => setTimeSheetOpen(false)}
        title="Период"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
          {PERIOD_OPTIONS.map((opt) => {
            // «Всё» (безлимит истории) под замком для free/guest — backend
            // усёк бы данные. Тап показывает upgrade вместо тихого no-op.
            const allowed = cbrAccess.isLoading || cbrAccess.canUsePeriod(opt.key);
            return (
              <button
                key={opt.key}
                className={`fm-chip ${period === opt.key ? 'active' : ''}`}
                onClick={() => {
                  if (!allowed) {
                    const tier = cbrAccess.requiredTierFor({ period: opt.key });
                    if (tier) {
                      showUpgrade({
                        tier,
                        featureName: `период «${opt.label}»`,
                        indicator: 'cbr_flows',
                      });
                    }
                    setTimeSheetOpen(false);
                    return;
                  }
                  setPeriod(opt.key);
                  setTimeSheetOpen(false);
                }}
                style={{
                  justifyContent: 'flex-start',
                  padding: '14px 16px',
                  opacity: allowed ? 1 : 0.5,
                }}
                aria-disabled={!allowed}
              >
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                  {opt.label}
                  {!allowed && <Lock size={12} strokeWidth={2.2} />}
                </span>
              </button>
            );
          })}
        </div>
      </MobileSheet>

      {/* Settings sheet — instrument type + категории toggle */}
      <MobileSheet
        open={optionsSheetOpen}
        onClose={() => setOptionsSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 18 }}>
          {/* Тип инструмента */}
          <div>
            <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Тип инструмента
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {TYPE_TABS.map((tab) => {
                const Icon = tab.Icon;
                const isActive = type === tab.key;
                return (
                  <button
                    key={tab.key}
                    className={`fm-chip ${isActive ? 'active' : ''}`}
                    onClick={() => setType(tab.key)}
                    style={{ justifyContent: 'flex-start', gap: 10, padding: '12px 16px' }}
                  >
                    <Icon size={16} strokeWidth={2.2} />
                    <span>{tab.label}</span>
                  </button>
                );
              })}
            </div>
          </div>

          {/* Категории toggle */}
          {data && data.categories.length > 0 && (
            <div>
              <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 8 }}>
                <div style={{ fontSize: 'var(--fs-xs)', fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                  Категории
                </div>
                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-muted)', fontWeight: 600 }}>
                  {visibleCategories.length}/{data.categories.length}
                </div>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {data.categories.map((cat) => {
                  const locked = lockedCategories.has(cat);
                  const visible = !hiddenCategories.has(cat);
                  return (
                    <CategoryToggleRow
                      key={cat}
                      label={cat}
                      visible={visible}
                      locked={locked}
                      onToggle={() => toggleCategory(cat)}
                      onUpgrade={() => {
                        showUpgrade({ tier: 'basic', featureName: `категория «${cat}»`, indicator: 'cbr_flows' });
                        setOptionsSheetOpen(false);
                      }}
                    />
                  );
                })}
              </div>
            </div>
          )}
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

// ──────────────────────────────────────────────────────────
// CategoryToggleRow — строка с цветным dot + label + checkbox для toggle.
// ──────────────────────────────────────────────────────────

function CategoryToggleRow({
  label,
  visible,
  locked = false,
  onToggle,
  onUpgrade,
}: {
  label: string;
  visible: boolean;
  locked?: boolean;
  onToggle: () => void;
  onUpgrade?: () => void;
}) {
  const { theme } = useTheme();
  const color = getCategoryColor(label, theme);
  // Локнутая тарифом категория: клик ведёт на апгрейд, а не toggle; вид — dimmed.
  const dimmed = locked || !visible;
  return (
    <button
      type="button"
      onClick={locked ? onUpgrade : onToggle}
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 10,
        padding: '10px 12px',
        background: dimmed ? 'transparent' : 'var(--bg-secondary)',
        border: '1.5px solid var(--text-primary)',
        borderRadius: 8,
        font: 'inherit',
        color: 'var(--text-primary)',
        textAlign: 'left',
        cursor: 'pointer',
        opacity: dimmed ? 0.5 : 1,
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0, flex: 1 }}>
        <span
          style={{
            width: 12,
            height: 12,
            borderRadius: 3,
            background: locked ? 'transparent' : color,
            border: '1px solid var(--text-primary)',
            flexShrink: 0,
          }}
        />
        <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {label}
        </span>
        {locked && (
          <span
            style={{
              fontSize: '0.6rem',
              fontWeight: 800,
              textTransform: 'uppercase',
              letterSpacing: '0.04em',
              padding: '1px 6px',
              borderRadius: 99,
              color: 'var(--accent)',
              border: '1px solid var(--accent)',
              flexShrink: 0,
            }}
          >
            Базовый
          </span>
        )}
      </div>
      {locked ? (
        <Lock size={13} strokeWidth={2.2} style={{ color: 'var(--text-muted)', flexShrink: 0 }} />
      ) : (
        <span style={{ fontSize: 'var(--fs-xs)', fontWeight: 700, color: visible ? 'var(--accent)' : 'var(--text-muted)' }}>
          {visible ? '●' : '○'}
        </span>
      )}
    </button>
  );
}

// ──────────────────────────────────────────────────────────
// MobileCbrHistogram — stacked bidirectional bars. Mobile-optimized:
//   - Pixel coords через ResizeObserver
//   - Touch handlers + vibrate
//   - Grow-up wave animation (stagger)
//   - Compact tooltip с values для visible categories
//   - Smart X-labels с годом
// ──────────────────────────────────────────────────────────

function MobileCbrHistogram({
  periods,
  categories,
  animKey,
}: {
  periods: CbrFlowsPeriod[];
  categories: string[];
  animKey: string;
}) {
  const { theme } = useTheme();
  const wrapRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 360, h: 320 });
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [touchX, setTouchX] = useState<number | null>(null);
  const [animated, setAnimated] = useState(false);

  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (r.width > 0 && r.height > 0) {
          setSize((prev) => {
            if (Math.abs(prev.w - r.width) < 8 && Math.abs(prev.h - r.height) < 8) return prev;
            return { w: Math.max(200, r.width), h: Math.max(200, r.height) };
          });
        }
      }
    });
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  // Replay animation на смену animKey (type/period/hiddenCategories).
  useEffect(() => {
    setAnimated(false);
    const t = setTimeout(() => setAnimated(true), 30);
    return () => clearTimeout(t);
  }, [animKey]);

  const { w: W, h: H } = size;
  // Размер шрифта осей — общий с «Открытыми позициями» (MobileChart).
  const axisFs = axisFontSize(W);

  // Y-max: симметрично для + и − стэков по абсолютной сумме.
  const yMax = useMemo(() => {
    let max = 0.1;
    for (const p of periods) {
      let sumPos = 0, sumNeg = 0;
      for (const cat of categories) {
        const v = p.values[cat] ?? 0;
        if (v > 0) sumPos += v;
        else sumNeg += -v;
      }
      max = Math.max(max, sumPos, sumNeg);
    }
    return max;
  }, [periods, categories]);

  const padX = 8;
  const padTop = 14;
  const padBottom = 22;
  // Правый жёлоб = ширина самой длинной подписи шкалы + зазор, чтобы стэки
  // доходили почти до цифр (без лишней пустоты справа), не наезжая на шкалу.
  const yTicks = [yMax, yMax / 2, 0, -yMax / 2, -yMax];
  const fmtYTick = (val: number) =>
    val === 0 ? '0' : `${val > 0 ? '+' : ''}${Math.abs(val) >= 10 ? val.toFixed(0) : val.toFixed(1)}`;
  const maxTickLen = Math.max(...yTicks.map((v) => fmtYTick(v).length));
  const padRight = Math.ceil(maxTickLen * axisFs * 0.62) + 7;
  const innerW = W - padX - padRight;
  const innerH = H - padTop - padBottom;
  const midY = padTop + innerH / 2;
  const halfH = innerH / 2;

  const slotW = periods.length > 0 ? innerW / periods.length : 0;
  // slotPadding 0.2 → barW = slotW * 0.6. Тоньше столбцы, больше воздуха
  // между периодами (consistent с FlowsHistogram + SeasonalityBars).
  const slotPadding = slotW * 0.2;
  const barW = slotW - slotPadding * 2;

  // X-axis ticks — равномерно распределённые ПО ВСЕЙ ширине (как в десктопном
  // StackedBidirectionalHistogram и MobileChart.xTicks). Берём ≤5 индексов
  // через round(i*(N-1)/(count-1)): первый = 0, последний = N-1 → крайние
  // метки на левом и правом краю, остальные равномерно между ними.
  // Раньше брались каждые labelStep индексов (0, step, 2*step …) и каждая
  // позиционировалась по ЦЕНТРУ своего слота — последний индекс не доходил
  // до правого края, поэтому метки «съезжали» влево, а справа зиял пробел.
  const xTickIndices = useMemo(() => {
    const count = Math.min(5, periods.length);
    if (count < 2) return periods.length > 0 ? [0] : [];
    return Array.from({ length: count }, (_, i) =>
      Math.min(Math.round((i * (periods.length - 1)) / (count - 1)), periods.length - 1),
    );
  }, [periods.length]);

  // Unified формат: всегда месяц-год (через end_date period'а).
  // Quarters автоматически конвертятся: Q1=март, Q2=июнь, Q3=сент, Q4=дек.
  const MONTH_SHORT = ['Янв','Фев','Мар','Апр','Май','Июн','Июл','Авг','Сен','Окт','Ноя','Дек'];
  const fmtPeriodLabel = (p: CbrFlowsPeriod): string => {
    const d = new Date(p.end_date);
    if (isNaN(d.getTime())) return `${p.label} ${p.year}`;
    return `${MONTH_SHORT[d.getMonth()]} '${String(d.getFullYear()).slice(-2)}`;
  };

  const updateHover = (clientX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = clientX - rect.left;
    setTouchX(relX);
    const idx = Math.floor((relX - padX) / slotW);
    if (idx < 0 || idx >= periods.length) {
      setHoverIdx(null);
      return;
    }
    setHoverIdx((prev) => {
      if (prev === idx) return prev;
      if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
        navigator.vibrate(8);
      }
      return idx;
    });
  };

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%', height: '100%', minHeight: 200 }}>
      <svg
        width={W}
        height={H}
        style={{ display: 'block', touchAction: 'none' }}
        onTouchStart={(e) => updateHover(e.touches[0].clientX)}
        onTouchMove={(e) => updateHover(e.touches[0].clientX)}
        onTouchEnd={() => { setHoverIdx(null); setTouchX(null); }}
      >
        {/* Y-grid lines (3 уровня + zero) */}
        {[0.25, 0.5, 0.75].map((t) => {
          const yUp = midY - halfH * t;
          const yDown = midY + halfH * t;
          return (
            <g key={t}>
              <line x1={padX} y1={yUp} x2={padX + innerW} y2={yUp}
                stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)" strokeWidth={1} />
              <line x1={padX} y1={yDown} x2={padX + innerW} y2={yDown}
                stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)" strokeWidth={1} />
            </g>
          );
        })}
        {/* Zero line */}
        <line x1={padX} y1={midY} x2={padX + innerW} y2={midY}
          stroke="var(--text-primary)" strokeWidth={1.2} opacity={0.6} />

        {/* Y-axis шкала (млрд ₽) в правом жёлобе: [+max, +max/2, 0, -max/2,
            -max]. Крайние с hanging/alphabetic, чтобы не обрезались по краям. */}
        {yTicks.map((val, i, arr) => {
          const ty = midY - (val / yMax) * halfH;
          const baseline = i === 0 ? 'hanging' : i === arr.length - 1 ? 'alphabetic' : 'central';
          const label = fmtYTick(val);
          return (
            <text
              key={`ys-${i}`}
              x={W - 4}
              y={ty}
              fontSize={axisFs}
              fontWeight={600}
              fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
              textAnchor="end"
              dominantBaseline={baseline}
              pointerEvents="none"
            >
              {label}
            </text>
          );
        })}

        {/* Stacked bars per period */}
        {periods.map((p, i) => {
          const x = padX + i * slotW + slotPadding;
          const isHovered = hoverIdx === i;
          const delay = Math.min(i * 4, 200);
          const transition = `transform 500ms cubic-bezier(0.34, 1.56, 0.64, 1) ${delay}ms, opacity 150ms ease`;
          let stackUp = 0;
          let stackDown = 0;
          return (
            <g
              key={i}
              opacity={hoverIdx === null || isHovered ? 1 : 0.45}
              style={{ transition: 'opacity 150ms ease' }}
            >
              {categories.map((cat) => {
                const v = p.values[cat] ?? 0;
                if (v === 0) return null;
                const hFinal = (Math.abs(v) / yMax) * halfH;
                const h = animated ? hFinal : 0;
                const color = getCategoryColor(cat, theme);
                if (v > 0) {
                  const stackUpFinal = (stackUp / yMax) * halfH;
                  const stackUpAnim = animated ? stackUpFinal : 0;
                  stackUp += v;
                  return (
                    <rect
                      key={cat}
                      x={x}
                      y={midY - stackUpAnim - h}
                      width={barW}
                      height={h}
                      fill={color}
                      style={{ transition }}
                    />
                  );
                } else {
                  const stackDownFinal = (stackDown / yMax) * halfH;
                  const stackDownAnim = animated ? stackDownFinal : 0;
                  stackDown += -v;
                  return (
                    <rect
                      key={cat}
                      x={x}
                      y={midY + stackDownAnim}
                      width={barW}
                      height={h}
                      fill={color}
                      style={{ transition }}
                    />
                  );
                }
              })}
            </g>
          );
        })}

        {/* Crosshair */}
        {hoverIdx !== null && (() => {
          const cx = padX + hoverIdx * slotW + slotW / 2;
          return (
            <line x1={cx} y1={padTop} x2={cx} y2={padTop + innerH}
              stroke="var(--text-primary)" strokeWidth={1} strokeDasharray="3,3"
              opacity={0.6} pointerEvents="none" />
          );
        })()}

        {/* X-axis labels: равномерно распределены по всей ширине (xTickIndices).
            Позиция = доля индекса в полном диапазоне * innerW, поэтому первый
            тик прижат к левому краю (anchor 'start'), последний — к правому
            (anchor 'end'), остальные центрированы. Это убирает «съезд» меток
            влево с пустотой справа. */}
        {xTickIndices.map((idx, ti) => {
          const p = periods[idx];
          if (!p) return null;
          const isFirst = ti === 0;
          const isLast = ti === xTickIndices.length - 1;
          const anchor: 'start' | 'middle' | 'end' = isFirst ? 'start' : isLast ? 'end' : 'middle';
          const ratio = periods.length > 1 ? idx / (periods.length - 1) : 0;
          const x = padX + ratio * innerW;
          return (
            <text
              key={`xl-${idx}`}
              x={x}
              y={H - 6}
              fontSize={axisFs}
              fontWeight={600}
              fill="var(--text-secondary)"
              textAnchor={anchor}
              dominantBaseline="alphabetic"
            >
              {fmtPeriodLabel(p)}
            </text>
          );
        })}
      </svg>

      {/* Tooltip — список visible categories со значениями */}
      {hoverIdx !== null && touchX !== null && periods[hoverIdx] && (() => {
        const tooltipW = 240;
        const margin = 8;
        const left = Math.max(margin, Math.min(touchX - tooltipW / 2, W - tooltipW - margin));
        const p = periods[hoverIdx];
        // Сортируем категории: сначала positive (>0), потом negative
        const entries = categories
          .map((c) => ({ cat: c, v: p.values[c] ?? 0 }))
          .filter((e) => e.v !== 0);
        return (
          <div
            style={{
              position: 'absolute',
              top: 4,
              left,
              width: tooltipW,
              padding: '6px 10px',
              background: 'var(--bg-primary)',
              border: '1.5px solid var(--text-primary)',
              borderRadius: 8,
              fontSize: 'var(--fs-xs)',
              pointerEvents: 'none',
              display: 'flex',
              flexDirection: 'column',
              gap: 2,
            }}
          >
            {(() => {
              // Header: полный месяц + год через end_date (quarter → месяц)
              const ed = new Date(p.end_date);
              const dateStr = isNaN(ed.getTime())
                ? `${p.label} ${p.year}`
                : ed.toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' });
              return (
                <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', fontWeight: 700, marginBottom: 2, textTransform: 'capitalize' }}>
                  {dateStr}
                </div>
              );
            })()}
            {entries.map(({ cat, v }) => (
              <div key={cat} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 6 }}>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, minWidth: 0, flex: 1 }}>
                  <span style={{ width: 7, height: 7, borderRadius: 99, background: getCategoryColor(cat, theme), flexShrink: 0 }} />
                  <span style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {cat}
                  </span>
                </span>
                <span
                  className="mono"
                  style={{
                    color: v >= 0 ? 'var(--funds-flow-positive, #5BD49C)' : 'var(--funds-flow-negative, #FF7A5C)',
                    fontWeight: 800,
                    fontSize: 'var(--fs-xs)',
                    flexShrink: 0,
                  }}
                >
                  {v >= 0 ? '+' : ''}{v.toFixed(1)} млрд ₽
                </span>
              </div>
            ))}
          </div>
        );
      })()}
    </div>
  );
}
