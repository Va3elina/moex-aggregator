/**
 * MobileBuffettPage — мобильная версия индикатора Баффетта.
 *
 * Простая структура: chart (cap + ratio) + viewMode toggle + period sheet.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { Scale, Lock } from 'lucide-react';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileChart from '../../components/mobile/MobileChart';
import MobileSheet from '../../components/mobile/MobileSheet';
import {
  getBuffettCapGdp,
  getBuffettCapM2,
  type BuffettCapGdpResponse,
  type BuffettRatioResponse,
  type BuffettPeriod,
} from '../../services/api';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour, { type TourStep } from '../../components/onboarding/OnboardingTour';

type ViewMode = 'cap-gdp' | 'cap-m2';

const PERIOD_LABELS: Partial<Record<BuffettPeriod, string>> = {
  '5y': '5 лет',
  '10y': '10 лет',
  '20y': '20 лет',
  'all': 'Вся история',
};

export default function MobileBuffettPage() {
  const [viewMode, setViewMode] = useState<ViewMode>('cap-gdp');
  const buffAccess = useTierAccess('buffett');
  const { showUpgrade } = useUpgradePrompt();
  const [period, setPeriod] = useState<BuffettPeriod>('10y');
  // Последний период, по которому данные успешно загрузились. «Вся история»
  // (all) недоступна Free/Гостю → backend отдаёт 403 → откатываемся сюда.
  const lastGoodPeriod = useRef<BuffettPeriod>('10y');
  // Таймфрейм аггрегации: день/неделя/месяц. По умолчанию месяц.
  const [timeframe, setTimeframe] = useState<'1d' | '1w' | '1m'>('1m');
  const [capGdpData, setCapGdpData] = useState<BuffettCapGdpResponse | null>(null);
  const [capM2Data, setCapM2Data] = useState<BuffettRatioResponse | null>(null);
  const [loading, setLoading] = useState(true);

  const [periodSheetOpen, setPeriodSheetOpen] = useState(false);
  const [modeSheetOpen, setModeSheetOpen] = useState(false);

  const tour = useOnboardingTour('buffett');
  // Единый паттерн: Intro → Кнопки → Время → Опции (с демо режима) → Чтение → End
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Индикатор Баффета',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Сравнивает капитализацию рынка с экономикой (ВВП) или с
            денежной массой (M2). Классический способ оценить «перегрет
            ли рынок».
          </p>
          <p>Разберём управление и научимся читать график.</p>
        </>
      ),
      onEnter: () => { setModeSheetOpen(false); setPeriodSheetOpen(false); },
    },
    {
      selector: '.fm-page-actions',
      title: 'Кнопки управления',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>Снизу — 3 кнопки:</p>
          <p style={{ marginBottom: 4 }}>
            <strong>Время</strong> — период и шаг агрегации
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Опции</strong> — версия индикатора (Cap/GDP или Cap/M2)
          </p>
          <p>
            <strong>Экран</strong> — развернуть график на весь экран
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
    },
    {
      selector: null,
      title: 'Период и таймфрейм',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл для тебя кнопку <strong>«Время»</strong>. Внутри:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Таймфрейм</strong> — шаг агрегации: день / неделя / месяц
          </p>
          <p>
            <strong>Период</strong> — глубина истории: 5 / 10 / 20 лет или Вся история
          </p>
        </>
      ),
      onEnter: () => { setPeriodSheetOpen(true); setModeSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Версии индикатора',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл кнопку <strong>«Опции»</strong>. Две версии индикатора:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Cap / ВВП</strong> — классика, сравнение с экономикой.
            Растущий ratio значит рынок «дорогой» относительно реальной
            экономики.
          </p>
          <p>
            <strong>Cap / M2</strong> — альтернатива, сравнение с денежной
            массой. Полезна при QE/дефляции. Сейчас переключу на Cap/M2 —
            посмотри как меняется график.
          </p>
        </>
      ),
      onEnter: () => { setViewMode('cap-m2'); setModeSheetOpen(true); setPeriodSheetOpen(false); },
    },
    {
      selector: '[data-tour="buffett-chart"]',
      title: 'Чтение графика',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Голубая линия — капитализация (трлн ₽, левая ось). Оранжевая —
            сам индикатор Кап/ВВП или Кап/M2 (%, правая ось).
          </p>
          <p>
            Зажми палец — появится курсор с точными значениями. Возвращаю
            в Cap/GDP по умолчанию.
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
      onEnter: () => { setViewMode('cap-gdp'); setModeSheetOpen(false); setPeriodSheetOpen(false); },
    },
    {
      selector: null,
      title: 'Готово!',
      body: (
        <p>
          Нажми <strong>?</strong> рядом с заголовком — методология расчёта
          и пороги для интерпретации (overvalued/undervalued).
        </p>
      ),
    },
  ];

  // Загрузка данных
  const loadData = useMemo(
    () => async () => {
      try {
        setLoading(true);
        if (viewMode === 'cap-gdp') {
          const result = await getBuffettCapGdp(period, false, timeframe);
          setCapGdpData(result);
        } else {
          const result = await getBuffettCapM2(period, false, timeframe);
          setCapM2Data(result);
        }
        // Период успешно загрузился — запоминаем как «безопасный» для отката.
        lastGoodPeriod.current = period;
      } catch (err) {
        console.error('Ошибка Buffett:', err);
        const msg = err instanceof Error ? err.message : String(err);
        // Ограничение глубины истории: «Вся история» (all) недоступна на
        // Free/Гостю — backend шлёт «Период all недоступен на тарифе …».
        // Откатываем период на последний рабочий, иначе график завис бы с
        // пустыми данными, а кнопка «Время» показывала бы «Вся история».
        if (msg.includes('Период') && msg.includes('недоступ')) {
          setPeriod(lastGoodPeriod.current);
          showUpgrade({
            tier: 'pro',
            featureName: 'полная история',
            indicator: 'buffett',
          });
          return;
        }
        // Ограничение режима (Кап / M2) или иной tier-отказ.
        if (msg.includes('тарифе') || msg.includes('недоступ')) {
          const requiredTier: 'basic' | 'pro' = msg.includes('Pro') ? 'pro' : 'basic';
          showUpgrade({
            tier: requiredTier,
            featureName: viewMode === 'cap-m2' ? 'режим «Кап / M2»' : 'индикатор Баффетта',
            indicator: 'buffett',
          });
        }
      } finally {
        setLoading(false);
      }
    },
    [viewMode, period, timeframe, showUpgrade],
  );

  useEffect(() => {
    void loadData();
  }, [loadData]);

  // Series для chart. Dual-axis как на десктопе:
  //   primary (left axis, accent) — Buffett ratio %
  //   secondary (right axis, blue) — капитализация в трлн ₽
  const chartSeries = useMemo(() => {
    if (viewMode === 'cap-gdp') {
      if (!capGdpData?.data?.length) return [];
      const ratioData = capGdpData.data.map((d) => ({ time: d.date, value: d.buffett }));
      // cap приходит уже в ТРЛН ₽ (31.05 = 31 трлн) — используем как есть
      const capData = capGdpData.data.map((d) => ({ time: d.date, value: d.cap }));

      // Axis convention: левая ось — абсолютные значения (капитализация),
      // правая ось — индикатор-ratio. Это match с desktop SimpleChart pattern.
      return [
        {
          data: capData,
          color: 'var(--chart-line-1, #5DA3E9)',
          label: 'Капитализация',
          axis: 'left' as const,
          formatValue: (v: number) => `${v.toFixed(1)} трлн`,
        },
        {
          data: ratioData,
          color: 'var(--accent)',
          label: 'Кап / ВВП',
          axis: 'right' as const,
          formatValue: (v: number) => `${v.toFixed(0)}%`,
        },
      ];
    }
    if (!capM2Data?.data?.length) return [];
    // Cap/M2 — ratio в decimal, умножаем на 100 для процентов
    const ratioData = capM2Data.data.map((d) => ({ time: d.date, value: d.ratio * 100 }));
    // cap опциональный для cap-m2; если есть — добавляем primary (cap слева)
    const hasCap = capM2Data.data.some((d) => d.cap !== undefined && d.cap !== null);
    const capData = hasCap
      ? capM2Data.data
          .filter((d) => d.cap !== undefined && d.cap !== null)
          .map((d) => ({ time: d.date, value: d.cap as number }))
      : [];
    // Если есть cap — он на левой, ratio на правой. Иначе ratio на левой.
    if (capData.length > 0) {
      return [
        {
          data: capData,
          color: 'var(--chart-line-1, #5DA3E9)',
          label: 'Капитализация',
          axis: 'left' as const,
          formatValue: (v: number) => `${v.toFixed(1)} трлн`,
        },
        {
          data: ratioData,
          color: 'var(--accent)',
          label: 'Кап / M2',
          axis: 'right' as const,
          formatValue: (v: number) => `${v.toFixed(0)}%`,
        },
      ];
    }
    return [{
      data: ratioData,
      color: 'var(--accent)',
      label: 'Кап / M2',
      axis: 'left' as const,
      formatValue: (v: number) => `${v.toFixed(0)}%`,
    }];
  }, [viewMode, capGdpData, capM2Data]);

  const settingsSummary = viewMode === 'cap-gdp' ? 'Кап / ВВП' : 'Кап / M2';

  return (
    <MobileLayout
      onTimeClick={() => setPeriodSheetOpen(true)}
      timeSummary={`${PERIOD_LABELS[period]} · ${timeframe === '1d' ? '1д' : timeframe === '1w' ? '1н' : '1м'}`}
      timeTourId="buffett-period"
      onSettingsClick={() => setModeSheetOpen(true)}
      settingsSummary={settingsSummary}
      settingsTourId="buffett-mode"
      onRefresh={loadData}
      loading={loading}
    >
      <MobilePageHeader
        Icon={Scale}
        title="Индикатор Баффетта"
        helpLink="/methodology/buffett"
      />

      {/* Full-bleed chart — без рамки, как в OI. */}
      <div
        data-tour="buffett-chart"
        style={{ flex: 1, minHeight: 0, position: 'relative' }}
      >
        <div style={{ position: 'absolute', inset: 0 }}>
          <MobileChart
            series={chartSeries}
            loading={loading}
            formatXLabel={(t) => {
              const d = new Date(t);
              return isNaN(d.getTime()) ? t : d.toLocaleDateString('ru-RU', { month: '2-digit', year: '2-digit' });
            }}
          />
        </div>
      </div>

      <MobileSheet
        open={modeSheetOpen}
        onClose={() => setModeSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 20 }}>
          {/* Версия индикатора */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Версия индикатора
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {(['cap-gdp', 'cap-m2'] as const).map((m) => {
                const allowed = buffAccess.isLoading || buffAccess.canUseMode(m);
                const label = m === 'cap-gdp' ? 'Капитализация / ВВП' : 'Капитализация / M2';
                return (
                  <button
                    key={m}
                    onClick={() => {
                      if (!allowed) {
                        const tier = buffAccess.requiredTierFor({ mode: m });
                        if (tier) {
                          showUpgrade({
                            tier,
                            featureName: `режим «${label}»`,
                            indicator: 'buffett',
                          });
                        }
                        return;
                      }
                      setViewMode(m);
                    }}
                    style={{
                      display: 'block',
                      width: '100%',
                      textAlign: 'left',
                      padding: '12px 14px',
                      background: viewMode === m ? 'var(--accent)' : 'var(--bg-secondary)',
                      color: viewMode === m ? 'var(--text-inverse)' : 'var(--text-primary)',
                      border: '1.5px solid var(--text-primary)',
                      borderRadius: 10,
                      fontWeight: 700,
                      fontSize: 13,
                      cursor: allowed ? 'pointer' : 'not-allowed',
                      opacity: allowed ? 1 : 0.5,
                      position: 'relative',
                    }}
                    aria-disabled={!allowed}
                  >
                    <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                      {label}
                      {!allowed && <Lock size={12} strokeWidth={2.2} />}
                    </div>
                    <div style={{ fontSize: 10, fontWeight: 500, opacity: 0.8, marginTop: 3, lineHeight: 1.3 }}>
                      {m === 'cap-gdp' ? 'Классика. Сравнение с экономикой.' : 'Альтернатива. Сравнение с денежной массой.'}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>

        </div>
      </MobileSheet>

      <MobileSheet
        open={periodSheetOpen}
        onClose={() => setPeriodSheetOpen(false)}
        title="Время"
      >
        <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 18 }}>
          {/* Таймфрейм аггрегации — день / неделя / месяц */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Таймфрейм
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {(['1d', '1w', '1m'] as const).map((tf) => (
                <button
                  key={tf}
                  className={`fm-chip ${timeframe === tf ? 'active' : ''}`}
                  onClick={() => setTimeframe(tf)}
                  style={{ flex: 1, justifyContent: 'center' }}
                >
                  {tf === '1d' ? '1 день' : tf === '1w' ? '1 неделя' : '1 месяц'}
                </button>
              ))}
            </div>
          </div>

          {/* Период истории */}
          <div>
            <div style={{ fontSize: 11, fontWeight: 800, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Период
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {(Object.keys(PERIOD_LABELS) as BuffettPeriod[]).map((p) => (
                <button
                  key={p}
                  className={`fm-chip ${period === p ? 'active' : ''}`}
                  onClick={() => {
                    setPeriod(p);
                    setPeriodSheetOpen(false);
                  }}
                  style={{ justifyContent: 'flex-start', padding: '14px 16px' }}
                >
                  {PERIOD_LABELS[p]}
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
