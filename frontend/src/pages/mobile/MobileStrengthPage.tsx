/**
 * MobileStrengthPage — мобильная версия «Сила рынка».
 *
 * Split-layout как на десктопе:
 *   - Верхний chart: breadth (% акций выше EMA)
 *   - Нижний chart: IMOEX (RUB) / RTS (USD) для контекста
 * Опции: period (6m/1y/2y/5y/all), EMA (50/100/200), universe (all/imoex),
 *        currency (rub/usd).
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { Activity, Lock } from 'lucide-react';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import { handleTierError } from '../../utils/tierError';
import { usePersistedState } from '../../hooks/usePersistedState';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileSheet from '../../components/mobile/MobileSheet';
import {
  getBreadthCurrent,
  getBreadthHistory,
  type BreadthCurrentResponse,
  type BreadthHistoryResponse,
  type BreadthUniverse,
} from '../../services/api';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour from '../../components/onboarding/OnboardingTour';
import type { TourStep } from '../../components/onboarding/OnboardingTour';
import DollarStaleHint from '../../components/strength/DollarStaleHint';

const EMA_PERIODS = [20, 50, 100, 200] as const;

type Period = '1y' | '5y' | 'all';
type UniverseBase = 'all' | 'imoex';
type Currency = 'rub' | 'usd';

const PERIOD_LABELS: Record<Period, string> = {
  '1y': '1 год',
  '5y': '5 лет',
  'all': 'Вся история',
};

const PERIOD_DAYS: Record<Period, number> = {
  '1y': 365,
  '5y': 1825,
  'all': 3650,
};

const CLASSIFICATION_LABELS: Record<string, { label: string; color: string }> = {
  overbought: { label: 'Перегрев', color: 'var(--funds-flow-positive, #5BD49C)' },
  bullish: { label: 'Бычий', color: 'var(--accent)' },
  neutral: { label: 'Нейтрально', color: 'var(--text-secondary)' },
  oversold: { label: 'Перепродано', color: 'var(--funds-flow-negative, #FF7A5C)' },
};

export default function MobileStrengthPage() {
  // Шарим desktop-ключи strength: enum'ы (Period/emaPeriod/UniverseBase/Currency)
  // побайтово идентичны desktop, дефолты совпадают, onClick-гейты не дают free
  // записать платные значения ('usd'/'all'/'5y'). chartMode/showPrice у mobile
  // нет — их desktop-ключи не трогаем.
  const [emaPeriod, setEmaPeriod] = usePersistedState<20 | 50 | 100 | 200>('frame:strength:emaPeriod', 200);
  const [period, setPeriod] = usePersistedState<Period>('frame:strength:period', '1y');
  const [universeBase, setUniverseBase] = usePersistedState<UniverseBase>('frame:strength:universeBase', 'imoex');
  const [currency, setCurrency] = usePersistedState<Currency>('frame:strength:currency', 'rub');
  const strengthAccess = useTierAccess('strength');
  const { showUpgrade } = useUpgradePrompt();
  const [current, setCurrent] = useState<BreadthCurrentResponse | null>(null);
  const [history, setHistory] = useState<BreadthHistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  // reqId stale-guard: переживает пересоздание loadData (useMemo по deps).
  const reqIdRef = useRef(0);
  const [timeSheetOpen, setTimeSheetOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);

  // Итоговый universe: добавляем _usd при долларовом режиме
  const universe: BreadthUniverse =
    currency === 'usd'
      ? (`${universeBase}_usd` as BreadthUniverse)
      : (universeBase as BreadthUniverse);

  const priceLabel = currency === 'usd' ? 'Индекс RTS' : 'Индекс IMOEX';

  const tour = useOnboardingTour('strength');
  // Единый паттерн тура: Intro → Кнопки → Время → Опции (с демо) → Чтение → End
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Сила рынка',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Сколько акций сейчас торгуется выше своей средней (EMA-50/100/200).
            Когда большинство — bull-настрой, когда меньшинство — bear.
          </p>
          <p>Разберём управление и научимся читать график.</p>
        </>
      ),
      onEnter: () => { setTimeSheetOpen(false); setSettingsOpen(false); },
    },
    {
      selector: '.fm-page-actions',
      title: 'Кнопки управления',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>Снизу — 3 кнопки:</p>
          <p style={{ marginBottom: 4 }}>
            <strong>Время</strong> — глубина истории (1Г/5Л/Всё)
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Опции</strong> — EMA-период, вселенная (Все/IMOEX), валюта
          </p>
          <p>
            <strong>Экран</strong> — развернуть график
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
            <strong>1Г / 5Л / Всё</strong> — глубина истории. На
            длинных периодах виден контекст (где был рынок раньше), на коротких —
            актуальная динамика.
          </p>
        </>
      ),
      onEnter: () => { setTimeSheetOpen(true); setSettingsOpen(false); },
    },
    {
      selector: null,
      title: 'EMA, вселенная, валюта',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 4 }}>
            Открыл кнопку <strong>«Опции»</strong>:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>EMA-период:</strong> 50/100/200 дней. Короткая EMA — быстрее
            сигнализирует, длинная — стабильнее.
          </p>
          <p>
            <strong>Вселенная:</strong> Все (~95 акций) или только IMOEX-индекс
            (топ ликвидных).
          </p>
        </>
      ),
      onEnter: () => { setSettingsOpen(true); setTimeSheetOpen(false); },
    },
    {
      selector: '[data-tour="strength-chart"]',
      title: 'Чтение графика',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            <strong>Сверху</strong> — линия индекса (IMOEX или RTS).
          </p>
          <p style={{ marginBottom: 6 }}>
            <strong>Снизу</strong> — гистограмма % акций выше EMA. Зелёные
            столбики ≥ 50% (большинство в тренде роста), красные &lt; 50%.
          </p>
          <p>Зажми палец — синхронный crosshair для обеих зон.</p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
      onEnter: () => { setSettingsOpen(false); setTimeSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Готово!',
      body: (
        <p>
          Нажми <strong>?</strong> рядом с заголовком — методология
          расчёта силы рынка и интерпретация уровней.
        </p>
      ),
    },
  ];

  const loadData = useMemo(
    () => async () => {
      // Захватываем id СИНХРОННО, до первого await — конкурентные вызовы
      // (быстрый перебор EMA/вселенной/валюты) получают разные id.
      const myId = ++reqIdRef.current;
      const isStale = () => myId !== reqIdRef.current;
      try {
        setLoading(true);
        const [cur, hist] = await Promise.all([
          getBreadthCurrent(emaPeriod, universe),
          getBreadthHistory(emaPeriod, PERIOD_DAYS[period], universe),
        ]);
        if (isStale()) return;
        setCurrent(cur);
        setHistory(hist);
      } catch (err) {
        if (isStale()) return;
        console.error('Ошибка strength:', err);
        handleTierError(err, {
          showUpgrade,
          indicator: 'strength',
          featureName: universeBase === 'all' ? 'вселенная «100 акций»' :
            currency === 'usd' ? 'долларовый режим' : 'индикатор «Сила рынка»',
        });
      } finally {
        // Устаревший ответ не гасит спиннер свежего запроса.
        if (!isStale()) setLoading(false);
      }
    },
    [emaPeriod, period, universe, universeBase, currency, showUpgrade],
  );

  useEffect(() => {
    void loadData();
  }, [loadData]);

  const classInfo = current ? CLASSIFICATION_LABELS[current.classification] : null;

  // Долларовый ряд отстаёт от рублёвого (РТС и курс не торгуются на выходных и в
  // нерабочие дни) → показываем маркеры «доллар не обновляется». Только в USD-режиме.
  const dollarStale = currency === 'usd' && !!history?.dollar_stale;

  // Sync'нутые данные: для каждой даты — price + breadth. Только пересечение
  // дат (не все breadth дни имеют price snapshot и наоборот).
  const syncedData = useMemo(() => {
    if (!history?.data?.length || !history?.imoex?.length) return [];
    const priceMap = new Map(history.imoex.map((d) => [d.date, d.close]));
    return history.data
      .filter((d) => priceMap.has(d.date))
      .map((d) => ({
        time: d.date,
        breadth: d.percent_above,
        price: priceMap.get(d.date) as number,
      }));
  }, [history]);

  // Краткая подпись для кнопки шестеренки: текущее значение + EMA
  const settingsSummary = current
    ? `${current.percent_above.toFixed(0)}% · EMA${emaPeriod}`
    : `EMA${emaPeriod}`;

  return (
    <MobileLayout
      onTimeClick={() => setTimeSheetOpen(true)}
      timeSummary={PERIOD_LABELS[period]}
      timeTourId="strength-time"
      onSettingsClick={() => setSettingsOpen(true)}
      settingsSummary={settingsSummary}
      settingsTourId="strength-settings"
      fullscreenTourId="strength-fullscreen"
      onRefresh={loadData}
      loading={loading}
      tightActionGap
    >
      <MobilePageHeader
        Icon={Activity}
        title="Сила рынка"
        helpLink="/methodology/strength"
        sourceNote="Индекс IMOEX/RTSI: ПАО Московская Биржа"
      />

      {/* Dual chart: один SVG, top=price line, bottom=breadth bars (histogram).
          Shared crosshair + tooltip — touch position маппится в общий idx,
          оба графика обновляются одновременно (как на десктопе). */}
      <div
        data-tour="strength-chart"
        style={{ flex: 1, minHeight: 0, position: 'relative' }}
      >
        {dollarStale && (
          <DollarStaleHint
            style={{ position: 'absolute', top: 8, left: 8, zIndex: 5 }}
          />
        )}
        <div style={{ position: 'absolute', inset: 0 }}>
          <StrengthDualChart
            data={syncedData}
            priceLabel={priceLabel}
            loading={loading}
          />
        </div>
      </div>

      {/* Time sheet — выбор периода */}
      <MobileSheet
        open={timeSheetOpen}
        onClose={() => setTimeSheetOpen(false)}
        title="Период"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
          {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => {
            // Глубокие периоды (5Л/Всё) для гостя/Free под замком —
            // backend ответит 403. canUsePeriod читает матрицу тарифов.
            const allowed = strengthAccess.isLoading || strengthAccess.canUsePeriod(p);
            return (
              <button
                key={p}
                className={`fm-chip ${period === p ? 'active' : ''}`}
                onClick={() => {
                  if (!allowed) {
                    const tier = strengthAccess.requiredTierFor({ period: p });
                    if (tier) {
                      showUpgrade({
                        tier,
                        featureName: `период «${PERIOD_LABELS[p]}»`,
                        indicator: 'strength',
                      });
                    }
                    setTimeSheetOpen(false);
                    return;
                  }
                  setPeriod(p);
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
                  {PERIOD_LABELS[p]}
                  {!allowed && <Lock size={12} strokeWidth={2.2} />}
                </span>
              </button>
            );
          })}
        </div>
      </MobileSheet>

      {/* Settings sheet — EMA period + кнопка «Сектора» */}
      <MobileSheet
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        title="Настройки"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 18 }}>
          {/* Текущее значение — мини-карточка в самом sheet'е */}
          {current && (
            <div
              style={{
                padding: '12px 14px',
                background: 'var(--bg-secondary)',
                border: '1.5px solid var(--text-primary)',
                borderRadius: 10,
                boxShadow: '3px 3px 0 var(--text-primary)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 8,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
                <span style={{ fontSize: 'var(--fs-3xl)', fontWeight: 800, color: 'var(--accent)', lineHeight: 1 }}>
                  {current.percent_above.toFixed(0)}%
                </span>
                <span style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600 }}>
                  {current.count_above} из {current.count_total}
                </span>
              </div>
              {classInfo && (
                <span
                  style={{
                    padding: '4px 10px',
                    background: classInfo.color,
                    color: 'var(--text-inverse)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 999,
                    fontSize: 10,
                    fontWeight: 800,
                    letterSpacing: '0.04em',
                    textTransform: 'uppercase',
                  }}
                >
                  {classInfo.label}
                </span>
              )}
            </div>
          )}

          {/* EMA period chips */}
          <div>
            <div
              style={{
                fontSize: 'var(--fs-xs)',
                fontWeight: 800,
                color: 'var(--text-secondary)',
                letterSpacing: '0.06em',
                textTransform: 'uppercase',
                marginBottom: 8,
              }}
            >
              Период EMA
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {EMA_PERIODS.map((p) => (
                <button
                  key={p}
                  className={`fm-chip ${emaPeriod === p ? 'active' : ''}`}
                  onClick={() => setEmaPeriod(p)}
                  style={{ flex: 1, justifyContent: 'center' }}
                >
                  EMA {p}
                </button>
              ))}
            </div>
          </div>

          {/* Universe: imoex / all */}
          <div>
            <div
              style={{
                fontSize: 'var(--fs-xs)',
                fontWeight: 800,
                color: 'var(--text-secondary)',
                letterSpacing: '0.06em',
                textTransform: 'uppercase',
                marginBottom: 8,
              }}
            >
              Набор акций
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {([
                { key: 'imoex' as const, label: 'IMOEX' },
                { key: 'all' as const, label: 'Все акции' },
              ]).map((opt) => {
                const allowed = strengthAccess.isLoading || strengthAccess.canUseUniverse(opt.key);
                return (
                  <button
                    key={opt.key}
                    className={`fm-chip ${universeBase === opt.key ? 'active' : ''}`}
                    onClick={() => {
                      if (!allowed) {
                        const tier = strengthAccess.requiredTierFor({ universe: opt.key });
                        if (tier) {
                          showUpgrade({
                            tier,
                            featureName: `вселенная «${opt.label}»`,
                            indicator: 'strength',
                          });
                        }
                        return;
                      }
                      setUniverseBase(opt.key);
                    }}
                    style={{
                      flex: 1,
                      justifyContent: 'center',
                      opacity: allowed ? 1 : 0.5,
                      cursor: allowed ? 'pointer' : 'not-allowed',
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 4,
                    }}
                    aria-disabled={!allowed}
                  >
                    {opt.label}
                    {!allowed && <Lock size={11} strokeWidth={2.2} />}
                  </button>
                );
              })}
            </div>
          </div>

          {/* Currency: rub / usd */}
          <div>
            <div
              style={{
                fontSize: 'var(--fs-xs)',
                fontWeight: 800,
                color: 'var(--text-secondary)',
                letterSpacing: '0.06em',
                textTransform: 'uppercase',
                marginBottom: 8,
              }}
            >
              Валюта
            </div>
            <div style={{ display: 'flex', gap: 6, position: 'relative' }}>
              {dollarStale && (
                <DollarStaleHint style={{ position: 'absolute', top: -8, right: -8, zIndex: 5 }} />
              )}
              {([
                { key: 'rub' as const, label: 'Рубли (IMOEX)' },
                { key: 'usd' as const, label: 'Доллары (RTS)' },
              ]).map((opt) => {
                // USD доступен только если можно использовать любую _usd вселенную
                const allowed = opt.key === 'rub' ||
                  strengthAccess.isLoading ||
                  strengthAccess.canUseUniverse('imoex_usd');
                return (
                  <button
                    key={opt.key}
                    className={`fm-chip ${currency === opt.key ? 'active' : ''}`}
                    onClick={() => {
                      if (!allowed) {
                        const tier = strengthAccess.requiredTierFor({ universe: 'imoex_usd' });
                        if (tier) {
                          showUpgrade({
                            tier,
                            featureName: 'долларовый режим',
                            indicator: 'strength',
                          });
                        }
                        return;
                      }
                      setCurrency(opt.key);
                    }}
                    style={{
                      flex: 1,
                      justifyContent: 'center',
                      opacity: allowed ? 1 : 0.5,
                      cursor: allowed ? 'pointer' : 'not-allowed',
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 4,
                    }}
                    aria-disabled={!allowed}
                  >
                    {opt.label}
                    {!allowed && <Lock size={11} strokeWidth={2.2} />}
                  </button>
                );
              })}
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

// ──────────────────────────────────────────────────────────
// StrengthDualChart — один SVG с двумя зонами (price line сверху, breadth
// bars снизу). Shared crosshair + tooltip как на десктопе. Year в датах.
// ──────────────────────────────────────────────────────────

interface DualChartPoint {
  time: string;
  breadth: number;
  price: number;
}

// Амбер-pivot для среднего звена. Не CSS-var: --warning меняется по темам
// (bordeaux в light, amber в dark) → дал бы разный gradient. Хардкод даёт
// одинаковый hue-переход через жёлтый в обеих темах.
const BREADTH_AMBER = '#F5B935';

function breadthBarColor(v: number): string {
  // 5-ступенчатый gradient через амбер: зелёный → жёлто-зелёный → амбер →
  // жёлто-красный → красный. Без transparent (он съедал насыщенность на
  // dark theme — bars выглядели тусклыми, особенно в зоне 30-70%).
  if (v >= 80) return 'var(--funds-flow-positive, #5BD49C)';
  if (v >= 60) return `color-mix(in srgb, var(--funds-flow-positive, #5BD49C) 60%, ${BREADTH_AMBER} 40%)`;
  if (v >= 40) return BREADTH_AMBER;
  if (v >= 20) return `color-mix(in srgb, var(--funds-flow-negative, #FF7A5C) 60%, ${BREADTH_AMBER} 40%)`;
  return 'var(--funds-flow-negative, #FF7A5C)';
}

function fmtXLabel(t: string): string {
  const d = new Date(t);
  if (isNaN(d.getTime())) return t;
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
}

function StrengthDualChart({
  data,
  priceLabel,
  loading,
}: {
  data: DualChartPoint[];
  priceLabel: string;
  loading: boolean;
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 360, h: 500 });
  const [crosshair, setCrosshair] = useState<number | null>(null);

  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (r.width > 0 && r.height > 0) {
          // Width: threshold 1px чтобы при fullscreen subpixel changes сразу
          // подхватились (см. MobileChart — тот же урок).
          // Height: 8px чтобы iOS Safari address bar не дёргал чарт постоянно.
          setSize((prev) => {
            const widthChanged = Math.abs(prev.w - r.width) >= 1;
            const heightChanged = Math.abs(prev.h - r.height) >= 8;
            if (!widthChanged && !heightChanged) return prev;
            return {
              w: widthChanged ? Math.max(200, r.width) : prev.w,
              h: heightChanged ? Math.max(200, r.height) : prev.h,
            };
          });
        }
      }
    });
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  const W = size.w;
  const H = size.h;
  const PAD_X = 4;
  const PAD_TOP = 8;
  const PAD_BOTTOM = 22; // X-axis labels снизу
  const MID_GAP = 18; // зона X-labels между двумя графиками
  // Правый жёлоб под Y-шкалу + pill последнего значения (как PILL_GUTTER_R в
  // MobileChart «Открытых позиций»): график рисуется только до W - PAD_RIGHT,
  // а цифры шкалы стоят в зарезервированной колонке справа и НЕ перекрываются
  // линией/барами.
  const PAD_RIGHT = 46;
  const innerW = W - PAD_X - PAD_RIGHT;
  const totalInnerH = H - PAD_TOP - PAD_BOTTOM - MID_GAP;
  // Top zone (price) = 50%, bottom zone (breadth) = 50%
  const topH = totalInnerH * 0.5;
  const botH = totalInnerH * 0.5;
  const topY0 = PAD_TOP;
  const topY1 = topY0 + topH;
  const botY0 = topY1 + MID_GAP;
  const botY1 = botY0 + botH;

  const N = data.length;
  const xAt = (i: number) => PAD_X + (i / Math.max(N - 1, 1)) * innerW;

  // Price range
  const priceRange = useMemo(() => {
    if (N === 0) return { min: 0, max: 1, span: 1 };
    const vals = data.map((d) => d.price);
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const span = max - min || 1;
    return { min: min - span * 0.05, max: max + span * 0.05, span: span * 1.1 };
  }, [data, N]);

  // Breadth fixed range 0-100% (бары всегда от 0 до текущего значения)
  const yBreadth = (v: number) => botY1 - (v / 100) * botH;
  const yPrice = (v: number) =>
    topY1 - ((v - priceRange.min) / priceRange.span) * topH;

  // Path для price line (Catmull-Rom простой M-L для performance).
  // ВАЖНО: deps включают и W, и H — yPrice захватывает topY1 в closure,
  // который зависит от H. Без H в deps в fullscreen path остаётся с
  // старыми Y-координатами, а crosshair circle (yPrice вызывается прямо
  // в render) использует новые — линия и круг расходятся визуально.
  const pricePath = useMemo(() => {
    if (N < 2) return '';
    return data
      .map((d, i) => {
        const x = xAt(i);
        const y = yPrice(d.price);
        return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, W, H, priceRange]);

  // Bar width для histogram
  const barW = N > 0 ? (innerW / N) * 0.85 : 0;

  // X-axis ticks — 4 равномерных
  const xTicks = useMemo(() => {
    if (N < 2) return [];
    const count = Math.min(4, N);
    return Array.from({ length: count }, (_, i) =>
      Math.round((i / (count - 1)) * (N - 1)),
    );
  }, [N]);

  // Touch handlers
  const updateFromTouch = (clientX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;
    const relX = clientX - rect.left - PAD_X;
    const idx = Math.round((relX / innerW) * (N - 1));
    const clamped = Math.max(0, Math.min(N - 1, idx));
    setCrosshair((prev) => {
      if (prev === clamped) return prev;
      if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
        navigator.vibrate(8);
      }
      return clamped;
    });
  };

  if (loading || N < 2) {
    return (
      <div ref={wrapRef} style={{ width: '100%', height: '100%', display: 'grid', placeItems: 'center', color: 'var(--text-muted)' }}>
        {loading ? '' : 'Нет данных'}
      </div>
    );
  }

  const hoveredPoint = crosshair !== null ? data[crosshair] : null;

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%', height: '100%' }}>
      <svg
        width={W}
        height={H}
        style={{ display: 'block', userSelect: 'none', touchAction: 'none' }}
        onTouchStart={(e) => updateFromTouch(e.touches[0].clientX)}
        onTouchMove={(e) => updateFromTouch(e.touches[0].clientX)}
        onTouchEnd={() => setCrosshair(null)}
      >
        {/* Top zone grid (5 levels) */}
        {[0.2, 0.5, 0.8].map((t, i) => (
          <line
            key={`gt-${i}`}
            x1={PAD_X}
            y1={topY0 + topH * t}
            x2={PAD_X + innerW}
            y2={topY0 + topH * t}
            stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)"
            strokeWidth={1}
          />
        ))}

        {/* Bottom zone reference lines (30, 50, 70 — zones) */}
        {[30, 50, 70].map((v) => (
          <line
            key={`gb-${v}`}
            x1={PAD_X}
            y1={yBreadth(v)}
            x2={PAD_X + innerW}
            y2={yBreadth(v)}
            stroke="color-mix(in srgb, var(--text-primary) 8%, transparent)"
            strokeWidth={1}
          />
        ))}

        {/* Price line (top) */}
        <path
          d={pricePath}
          fill="none"
          stroke="var(--chart-line-1, #5DA3E9)"
          strokeWidth={2}
          strokeLinejoin="round"
          strokeLinecap="round"
        />

        {/* Breadth bars (bottom) — color по классификации */}
        {data.map((d, i) => {
          const x = xAt(i) - barW / 2;
          const y = yBreadth(d.breadth);
          const h = botY1 - y;
          if (h <= 0) return null;
          return (
            <rect
              key={i}
              x={x}
              y={y}
              width={barW}
              height={h}
              fill={breadthBarColor(d.breadth)}
              opacity={crosshair === i ? 1 : 0.75}
              rx={0.5}
            />
          );
        })}

        {/* Y-axis labels — price (top, right edge — как в «Открытых позициях») */}
        {[0.2, 0.5, 0.8].map((t, i) => {
          const v = priceRange.min + priceRange.span * (1 - t);
          return (
            <text
              key={`yp-${i}`}
              x={W - PAD_X - 4}
              y={topY0 + topH * t + 3}
              fontSize={9}
              fontWeight={600}
              fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
              textAnchor="end"
              pointerEvents="none"
            >
              {v.toFixed(0)}
            </text>
          );
        })}
        {/* Y-axis labels — breadth zones (right edge) */}
        {[30, 50, 70].map((v) => (
          <text
            key={`yb-${v}`}
            x={W - PAD_X - 4}
            y={yBreadth(v) + 3}
            fontSize={9}
            fontWeight={600}
            fill="color-mix(in srgb, var(--text-primary) 55%, transparent)"
            textAnchor="end"
            pointerEvents="none"
          >
            {v}%
          </text>
        ))}

        {/* Pills с последним значением — у правого края, под шкалой (как в ОИ) */}
        {(() => {
          const last = data[N - 1];
          return (
            <>
              <PillSmall x={W - PAD_X - 2} y={yPrice(last.price)} text={last.price.toFixed(0)} color="var(--chart-line-1, #5DA3E9)" align="right" />
              <PillSmall
                x={W - PAD_X - 2}
                y={yBreadth(last.breadth)}
                text={`${last.breadth.toFixed(0)}%`}
                color={breadthBarColor(last.breadth)}
                align="right"
              />
            </>
          );
        })()}

        {/* X-axis labels (между зонами, на дате) */}
        {xTicks.map((idx, i) => {
          const point = data[idx];
          if (!point) return null;
          const x = xAt(idx);
          const isFirst = i === 0;
          const isLast = i === xTicks.length - 1;
          const anchor = isFirst ? 'start' : isLast ? 'end' : 'middle';
          return (
            <text
              key={`xl-${idx}`}
              x={x}
              y={topY1 + MID_GAP - 4}
              fontSize={9.5}
              fontWeight={600}
              fill="var(--text-secondary)"
              textAnchor={anchor}
            >
              {fmtXLabel(point.time)}
            </text>
          );
        })}

        {/* Bottom-edge x-labels (год для long периодов) */}
        {xTicks.map((idx, i) => {
          const point = data[idx];
          if (!point) return null;
          const x = xAt(idx);
          const isFirst = i === 0;
          const isLast = i === xTicks.length - 1;
          const anchor = isFirst ? 'start' : isLast ? 'end' : 'middle';
          return (
            <text
              key={`xb-${idx}`}
              x={x}
              y={H - 6}
              fontSize={9.5}
              fontWeight={600}
              fill="var(--text-secondary)"
              textAnchor={anchor}
              dominantBaseline="alphabetic"
            >
              {fmtXLabel(point.time)}
            </text>
          );
        })}

        {/* Crosshair vertical line */}
        {crosshair !== null && (
          <>
            <line
              x1={xAt(crosshair)}
              y1={topY0}
              x2={xAt(crosshair)}
              y2={botY1}
              stroke="var(--text-primary)"
              strokeWidth={1}
              strokeDasharray="3,3"
              opacity={0.6}
              pointerEvents="none"
            />
            <circle
              cx={xAt(crosshair)}
              cy={yPrice(data[crosshair].price)}
              r={4}
              fill="var(--chart-line-1, #5DA3E9)"
              stroke="var(--bg-primary)"
              strokeWidth={2}
              pointerEvents="none"
            />
          </>
        )}
      </svg>

      {/* Shared tooltip (один для обоих графиков) */}
      {hoveredPoint && (() => {
        const tooltipW = 200;
        const margin = 8;
        const cx = xAt(crosshair as number);
        const left = Math.max(margin, Math.min(cx - tooltipW / 2, W - tooltipW - margin));
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
              fontSize: 11,
              pointerEvents: 'none',
              display: 'flex',
              flexDirection: 'column',
              gap: 3,
            }}
          >
            <div style={{ fontSize: 9.5, color: 'var(--text-secondary)', fontWeight: 600 }}>
              {fmtXLabel(hoveredPoint.time)}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--chart-line-1, #5DA3E9)', fontWeight: 700 }}>
                {priceLabel}
              </span>
              <span className="mono" style={{ fontWeight: 800 }}>
                {hoveredPoint.price.toFixed(0)}
              </span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: breadthBarColor(hoveredPoint.breadth), fontWeight: 700 }}>
                Сила
              </span>
              <span className="mono" style={{ fontWeight: 800 }}>
                {hoveredPoint.breadth.toFixed(0)}%
              </span>
            </div>
          </div>
        );
      })()}
    </div>
  );
}

function PillSmall({
  x,
  y,
  text,
  color,
  align = 'left',
}: {
  x: number;
  y: number;
  text: string;
  color: string;
  align?: 'left' | 'right';
}) {
  // Размер таблетки увеличен в 1.5× (текущее значение графика подсвечивается
  // заметнее): 5.5→8.25, 8→12, h 14→21, шрифт 10→15, rx 3→4.5.
  const w = text.length * 8.25 + 12;
  const h = 21;
  // align='right' → x это ПРАВЫЙ край pill'а (растём влево). Так последнее
  // значение садится у правой шкалы, как в «Открытых позициях».
  const rectX = align === 'right' ? x - w : x;
  return (
    <g pointerEvents="none">
      <rect x={rectX} y={y - h / 2} width={w} height={h} fill={color} rx={4.5} />
      <text
        x={rectX + w / 2}
        y={y}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={15}
        fontWeight={700}
        fill="#fff"
        style={{ fontFamily: 'IBM Plex Mono, monospace' }}
      >
        {text}
      </text>
    </g>
  );
}

