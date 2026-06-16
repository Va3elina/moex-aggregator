/**
 * MobileOpenInterestPage — мобильная версия индикатора «Открытые позиции».
 *
 * Архитектура (отличается от десктопа):
 *   - TopBar + PageHeader + Editorial Frame + BottomRail (MobileLayout)
 *   - В frame'е: MobileChart (edge-to-edge) + MobileQuickActions
 *   - Asset/Period/Options открывают slide-up Sheet'ы
 *
 * Phase 2 — упрощённая версия:
 *   - Тип данных «Объём позиций» / «Число трейдеров», 5 вариантов ОИ
 *   - Период через sheet (1д/1н/1м/3м/6м/1г/2г/5л/Всё)
 *   - Один график: цена + net OI
 *   - Экспирации, толкование тура — Phase 4
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { BarChart3 } from 'lucide-react';
import MobileLayout from '../../components/mobile/MobileLayout';
import MobilePageHeader from '../../components/mobile/MobilePageHeader';
import MobileChart from '../../components/mobile/MobileChart';
import MobileSheet from '../../components/mobile/MobileSheet';
import MobileAssetSearch from '../../components/mobile/MobileAssetSearch';
import { getChartData } from '../../services/api';
import { useUpgradePrompt } from '../../components/tier/UpgradeModal';
import { handleTierError, oiTierResolver } from '../../utils/tierError';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { Lock } from 'lucide-react';
import type { ChartResponse } from '../../types';
import { useAuth } from '../../contexts/AuthContext';
import { useRealtimeData } from '../../hooks/useRealtimeData';
import { getDefaultPeriod } from '../../config/accessControl';
import { useOnboardingTour } from '../../hooks/useFirstVisit';
import OnboardingTour, { type TourStep } from '../../components/onboarding/OnboardingTour';
import { usePersistedState } from '../../hooks/usePersistedState';

type Period = '1d' | '1w' | '1m' | '1y' | '5y' | 'all';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';
type DisplayMode = 'positions' | 'participants';

const OI_VARIANTS: OIVariant[] = ['oi', 'long', 'short', 'both', 'net'];

// Подписи вариантов зависят от типа данных: объём контрактов («Покупки»)
// либо количество участников («Покупатели»).
function variantLabel(v: OIVariant, mode: DisplayMode): string {
  const pos = mode === 'positions';
  switch (v) {
    case 'oi':    return 'Открытый интерес';
    case 'long':  return pos ? 'Покупки' : 'Покупатели';
    case 'short': return pos ? 'Продажи' : 'Продавцы';
    case 'both':  return pos ? 'Покупки + Продажи' : 'Покупатели + Продавцы';
    case 'net':   return 'Чистая позиция';
  }
}

const VARIANT_COLORS: Record<OIVariant, string> = {
  oi:    'var(--oi-amber)',
  long:  'var(--oi-green)',
  short: 'var(--oi-red)',
  both:  'var(--oi-purple)',
  net:   'var(--accent)',
};

const PERIOD_LABELS: Record<Period, string> = {
  '1d': '1 день',
  '1w': '1 неделя',
  '1m': '1 месяц',
  '1y': '1 год',
  '5y': '5 лет',
  'all': 'Вся история',
};

const INTERVAL_LABELS: Record<number, string> = {
  5: '5 мин',
  60: '1 час',
  24: '1 день',
};

// Ограничения периодов для каждого таймфрейма (производительность + смысл).
// Зеркалит desktop OpenInterestPage logic.
//   - 5мин: только короткие периоды (1д/1н/1м)
//   - 1час: до полугода
//   - 1д:   все периоды (1д не имеет смысла — переключаем на 1н)
const MAX_PERIODS_BY_INTERVAL: Record<number, Period[]> = {
  5: ['1d', '1w', '1m'],
  60: ['1d', '1w', '1m'],
  24: ['1w', '1m', '1y', '5y', 'all'],
};

export default function MobileOpenInterestPage() {
  const { isAuthenticated } = useAuth();
  const { showUpgrade } = useUpgradePrompt();
  const oiAccess = useTierAccess('open_interest');

  // State
  const [selectedInstrument, setSelectedInstrument] = useState('SR');
  const [instrumentName, setInstrumentName] = useState('Сбербанк');
  // Шарим desktop-ключи OI (enum'ы побайтово идентичны, формат JSON совместим;
  // instrument — ИСКЛЮЧЕНИЕ, см. ниже). displayMode → отдельный mobile-ключ
  // (у desktop 3-е значение 'price', которого нет в mobile-UI).
  const [period, setPeriod] = usePersistedState<Period>('frame:oi:period', getDefaultPeriod('1y', isAuthenticated) as Period);
  const [intervalValue, setIntervalValue] = usePersistedState('frame:oi:interval', 24);
  const [clgroup, setClgroup] = usePersistedState<'FIZ' | 'YUR'>('frame:oi:clgroup', 'YUR');
  const [oiVariant, setOiVariant] = usePersistedState<OIVariant>('frame:oi:oiVariant', 'net');
  const [displayMode, setDisplayMode] = usePersistedState<DisplayMode>('frame:oi:mobileDisplayMode', 'positions');
  const [data, setData] = useState<ChartResponse | null>(null);
  const [loading, setLoading] = useState(true);
  // Stale-guard против out-of-order-гонки: при быстром переборе актива/интервала
  // медленный ранний ответ не должен перезаписать свежий. reqIdRef на уровне
  // компонента — переживает пересоздание loadData (useMemo по deps).
  const reqIdRef = useRef(0);

  // Onboarding tour — shared storage key с десктопом (juser видит онбординг
  // один раз на любом устройстве). Контент адаптирован под мобильный layout.
  const tour = useOnboardingTour('oi');
  // Sheets state — объявляю до tourSteps чтобы onEnter мог их открывать
  const [assetSearchOpen, setAssetSearchOpen] = useState(false);
  const [periodSheetOpen, setPeriodSheetOpen] = useState(false);
  const [optionsSheetOpen, setOptionsSheetOpen] = useState(false);

  // === Единый паттерн тура для всех индикаторов ===
  // 1. Intro — что это за индикатор
  // 2. Кнопки управления — обзор всех action-кнопок (Актив/Время/Опции/Экран)
  // 3. Время — открывает sheet с периодами и таймфреймами
  // 4. Опции/Режимы — открывает sheet, описывает все внутренние настройки + демо смены режима
  // 5. Чтение графика — закрывает sheets, объясняет crosshair и оси
  // 6. End — ссылка на методологию
  const tourSteps: TourStep[] = [
    {
      selector: null,
      title: 'Открытые позиции',
      body: (
        <>
          <p style={{ marginBottom: 8 }}>
            Сколько контрактов открыто у участников рынка по фьючерсам Мосбиржи.
            И в какую сторону они ставят деньги.
          </p>
          <p>Разберём управление и научимся читать график.</p>
        </>
      ),
      onEnter: () => { setAssetSearchOpen(false); setPeriodSheetOpen(false); setOptionsSheetOpen(false); },
    },
    {
      selector: '.fm-page-actions',
      title: 'Кнопки управления',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>Снизу — 4 кнопки:</p>
          <p style={{ marginBottom: 4 }}>
            <strong>Актив</strong> — выбор фьючерса (Сбер, Газпром, Si, BR…)
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Время</strong> — период и интервал
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Опции</strong> — вариант позиций и категория участников
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
      title: 'Период и интервал',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл для тебя кнопку <strong>«Время»</strong>. Внутри:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Интервал</strong> — шаг данных: 5 мин (только до 1 месяца) /
            1 час (до 6 месяцев) / 1 день (вся история)
          </p>
          <p>
            <strong>Период</strong> — глубина истории: 1М / 1Г / Всё
          </p>
        </>
      ),
      onEnter: () => { setPeriodSheetOpen(true); setOptionsSheetOpen(false); setAssetSearchOpen(false); },
    },
    {
      selector: null,
      title: 'Варианты позиций и категории',
      align: 'top',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Открыл кнопку <strong>«Опции»</strong>. Внутри:
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Тип данных:</strong> «Объём позиций» — сколько контрактов
            куплено или продано, «Число трейдеров» — сколько человек держит позиции.
          </p>
          <p style={{ marginBottom: 4 }}>
            <strong>Вариант позиций:</strong> общий (long+short), покупки, продажи,
            чистая позиция (long−short), обе линии вместе.
          </p>
          <p>
            <strong>Физ/Юр:</strong> биржа считает позиции отдельно для физлиц
            и юрлиц. Их поведение часто различается — физики «на эмоциях»,
            юрики стратегически. Сравнение групп = ключевой инсайт.
          </p>
        </>
      ),
      onEnter: () => { setOptionsSheetOpen(true); setPeriodSheetOpen(false); setAssetSearchOpen(false); },
    },
    {
      selector: '[data-tour="oi-chart"]',
      title: 'Чтение графика',
      body: (
        <>
          <p style={{ marginBottom: 6 }}>
            Голубая линия — цена фьючерса (левая ось). Цветная — выбранный
            вариант позиций (правая ось, в тысячах контрактов).
          </p>
          <p>
            <strong>Коснись и удерживай палец</strong> — появится перекрестье
            с точными значениями на эту дату.
          </p>
        </>
      ),
      position: 'top',
      spotlightPadding: 4,
      onEnter: () => { setOptionsSheetOpen(false); setPeriodSheetOpen(false); setAssetSearchOpen(false); },
    },
    {
      selector: null,
      title: 'Готово!',
      body: (
        <p>
          Нажми <strong>?</strong> рядом с заголовком — подробная методология
          и сценарии трактовки открытых позиций.
        </p>
      ),
    },
  ];

  // Какие периоды доступны для текущего interval'а
  const allowedPeriods = MAX_PERIODS_BY_INTERVAL[intervalValue] ?? MAX_PERIODS_BY_INTERVAL[24];

  // Какие интервалы реально есть у актива (ISS-only → только дневка [24]).
  const availableIntervals: number[] = data?.available_intervals ?? [24];
  const hasInterval = (int: number) => availableIntervals.includes(int);
  const isPeriodAvailable = (p: Period) => allowedPeriods.includes(p);

  // Смена interval'а: авто-перенастройка period'а если стал недоступен
  function handleIntervalChange(newInterval: number) {
    const allowed = MAX_PERIODS_BY_INTERVAL[newInterval] ?? MAX_PERIODS_BY_INTERVAL[24];
    setIntervalValue(newInterval);
    if (!allowed.includes(period)) {
      // Берём максимальный доступный (последний в массиве)
      setPeriod(allowed[allowed.length - 1]);
    }
  }

  // Tier-коррекция дефолтного периода (как desktop): гость дефолтит на '1y', но
  // open_interest free-лимит 180д → 403 на первой загрузке. Опускаем до макс.
  // периода, доступного и по тарифу, и по интервалу → данные вместо модалки.
  useEffect(() => {
    if (oiAccess.isLoading) return;
    if (oiAccess.canUsePeriod(period)) return;
    const allowed = allowedPeriods.filter(p => oiAccess.canUsePeriod(p));
    if (allowed.length) setPeriod(allowed[allowed.length - 1]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [oiAccess.isLoading, period, intervalValue]);

  // Смена period'а: авто-фикс interval'а если выбранный не поддерживает
  function handlePeriodChange(newPeriod: Period) {
    setPeriod(newPeriod);
    const currentAllowed = MAX_PERIODS_BY_INTERVAL[intervalValue] ?? MAX_PERIODS_BY_INTERVAL[24];
    if (!currentAllowed.includes(newPeriod)) {
      // Найти минимальный interval который поддерживает этот период
      const candidate = [24, 60, 5].find((int) =>
        (MAX_PERIODS_BY_INTERVAL[int] ?? []).includes(newPeriod),
      );
      if (candidate) setIntervalValue(candidate);
    }
  }

  // Имя инструмента приходит из MobileAssetSearch (callback ставит и тикер,
  // и название атомарно). Запрашивать getInstrument отдельно не надо —
  // на мобиле нет URL deep-links для assets, синхронизация гарантирована.
  // (Раньше тут был useEffect → /api/instruments/SR, но 'SR' это futures
  // code, а endpoint работает только со stocks → 404 в консоли.)

  // Загрузка chart data
  const loadData = useMemo(() => async () => {
    // Захватываем id СИНХРОННО, до первого await — конкурентные вызовы
    // получают разные id; устаревший (не последний) ответ не пишет state.
    const myId = ++reqIdRef.current;
    const isStale = () => myId !== reqIdRef.current;
    try {
      setLoading(true);
      const result = await getChartData(
        selectedInstrument,
        selectedInstrument,
        'futures',
        intervalValue,
        clgroup,
        true, // show_oi
        period,
      );
      if (isStale()) return;
      setData(result);
      // Новый актив не поддерживает текущий интервал (ISS-only → только дневка) —
      // сбрасываем на максимальный доступный (как на десктопе).
      if (result.available_intervals?.length && !result.available_intervals.includes(intervalValue)) {
        setIntervalValue(Math.max(...result.available_intervals));
      }
    } catch (err) {
      if (isStale()) return;
      // Tier 403 → upgrade prompt вместо silent console error
      if (!handleTierError(err, {
        showUpgrade,
        indicator: 'open_interest',
        featureName: (msg) => msg.replace(/^.*?: /, ''),
        tierResolver: oiTierResolver,
      })) {
        console.error('Ошибка загрузки OI:', err);
      }
    } finally {
      // Устаревший ответ не гасит спиннер свежего запроса.
      if (!isStale()) setLoading(false);
    }
  }, [selectedInstrument, intervalValue, clgroup, period, showUpgrade]);

  useEffect(() => { void loadData(); }, [loadData]);
  useRealtimeData(['5min', 'hourly'], () => { void loadData(); });

  // Преобразуем data в series для MobileChart с учётом выбранного варианта OI
  const chartSeries = useMemo(() => {
    if (!data || !data.candles || data.candles.length === 0) return [];

    const priceData = data.candles.map((c) => ({ time: c.time, value: c.close }));
    const oiPoints = data.open_interest || [];
    const fmtOI = (v: number) =>
      Math.abs(v) >= 1000 ? (v / 1000).toFixed(1) + 'K' : v.toFixed(0);

    // Тип данных: позиции (объём контрактов) или участники (их количество).
    const isPos = displayMode === 'positions';
    const variantValue = (oi: typeof oiPoints[number]): number => {
      switch (oiVariant) {
        case 'oi':    return isPos
          ? (oi.pos_long || 0) + Math.abs(oi.pos_short || 0)
          : (oi.pos_long_num || 0) + (oi.pos_short_num || 0);
        case 'long':  return isPos ? (oi.pos_long || 0) : (oi.pos_long_num || 0);
        case 'short': return isPos ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0);
        case 'net':   return isPos
          ? (oi.net_position ?? (oi.pos_long || 0) + (oi.pos_short || 0))
          : (oi.pos_long_num || 0) - (oi.pos_short_num || 0);
        case 'both':  return isPos ? (oi.pos_long || 0) : (oi.pos_long_num || 0); // primary серия — long
      }
    };

    const oiData = oiPoints.map((oi) => ({ time: oi.time, value: variantValue(oi) }));
    // Для 'both' — добавляем дополнительную серию short
    const shortData =
      oiVariant === 'both'
        ? oiPoints.map((oi) => ({ time: oi.time, value: isPos ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0) }))
        : [];

    const baseSeries = [
      {
        data: priceData,
        color: 'var(--chart-line-1, #5DA3E9)',
        label: instrumentName,
        axis: 'left' as const,
        // Цена = baseline x-оси (как десктоп: OI выравнивается к свечам).
        // Без этого у инструментов с длинной историей OI (GMKN) цена
        // растягивалась по таймстемпам OI → плоские ступени / разрыв линии.
        isBaseline: true,
        formatValue: (v: number) => v.toFixed(0),
      },
      ...(oiData.length > 0
        ? [{
            data: oiData,
            color: VARIANT_COLORS[oiVariant],
            label: oiVariant === 'both'
              ? variantLabel('long', displayMode)
              : variantLabel(oiVariant, displayMode),
            axis: 'right' as const,
            formatValue: fmtOI,
          }]
        : []),
      ...(shortData.length > 0
        ? [{
            data: shortData,
            color: VARIANT_COLORS.short,
            label: variantLabel('short', displayMode),
            axis: 'right' as const,
            formatValue: fmtOI,
          }]
        : []),
    ];

    return baseSeries;
  }, [data, instrumentName, oiVariant, displayMode]);

  const timeLabel = `${PERIOD_LABELS[period]} · ${INTERVAL_LABELS[intervalValue] ?? intervalValue + 'ч'}`;
  // Summary в кнопке «Опции» (aria-label/title) — три chunks, чтобы юзер
  // понимал текущий displayMode (Объём позиций/Число трейдеров), variant и категорию
  // участников без открытия sheet'а. Раньше displayMode был только косвенно
  // (через variantLabel: «Покупки» vs «Покупатели»), что не помогало
  // юзерам понять «где же переключатель Позиции/Участники».
  const modeShort = displayMode === 'positions' ? 'Объём' : 'Трейдеры';
  const optionsLabel = `${modeShort} · ${variantLabel(oiVariant, displayMode)} · ${clgroup === 'YUR' ? 'Юр' : 'Физ'}`;

  return (
    <MobileLayout
      onAssetClick={() => setAssetSearchOpen(true)}
      assetLabel={instrumentName}
      assetTicker={selectedInstrument}
      assetTourId="oi-asset"
      onTimeClick={() => setPeriodSheetOpen(true)}
      timeSummary={timeLabel}
      timeTourId="oi-time"
      onSettingsClick={() => setOptionsSheetOpen(true)}
      settingsSummary={optionsLabel}
      settingsTourId="oi-settings"
      fullscreenTourId="oi-fullscreen"
      onRefresh={loadData}
      loading={loading}
    >
      <MobilePageHeader
        Icon={BarChart3}
        title="Открытые позиции"
        subtitle={`${instrumentName} · ${timeLabel}`}
        helpLink="/methodology/oi"
      />

      {/* Full-bleed chart — без рамки/padding'а, как в TradingView mobile.
          Absolute positioning гарантирует что chart заполнит весь parent —
          height:100% на MobileChart wrapRef плохо resolves в flex-context
          на iOS WebKit, а с position:absolute + inset:0 — всегда работает. */}
      <div
        data-tour="oi-chart"
        style={{
          flex: 1,
          minHeight: 0,
          position: 'relative',
        }}
      >
        <div style={{ position: 'absolute', inset: 0 }}>
          <MobileChart
            series={chartSeries}
            niceTicksLeft={true}
            niceTicksRight={true}
            loading={loading}
            animKey={`${selectedInstrument}|${period}|${intervalValue}|${clgroup}|${displayMode}|${oiVariant}`}
            formatXLabel={(t) => {
              // Явный форматтер вместо default'а MobileChart: default'у нельзя
              // надёжно отличить daily-свечу с timestamp T07:00:00 (MOEX-open)
              // от 1h-тика с T07:00:00 — обе имеют minutes=0. Поэтому решение
              // на уровне страницы: интервал известен → формат detrminистичный.
              const d = new Date(t);
              if (isNaN(d.getTime())) return t;
              if (intervalValue !== 24) {
                const dd = String(d.getDate()).padStart(2, '0');
                const mm = String(d.getMonth() + 1).padStart(2, '0');
                const HH = String(d.getHours()).padStart(2, '0');
                const MM = String(d.getMinutes()).padStart(2, '0');
                return `${dd}.${mm} ${HH}:${MM}`;
              }
              return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
            }}
          />
        </div>
      </div>

      {/* Sheet: выбор актива. Открытые позиции существуют только по
          фьючерсам — filterType="futures" ограничивает список фьючерсами
          FORTS (паритет с десктопным InstrumentSearchModal). */}
      <MobileAssetSearch
        open={assetSearchOpen}
        onClose={() => setAssetSearchOpen(false)}
        onSelect={(sectype, name) => {
          setSelectedInstrument(sectype);
          setInstrumentName(name);
        }}
        filterType="futures"
        indicator="open_interest"
      />

      {/* Sheet: период + интервал */}
      <MobileSheet
        open={periodSheetOpen}
        onClose={() => setPeriodSheetOpen(false)}
        title="Время"
      >
        <div style={{ padding: 16 }}>
          <div style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Интервал
          </div>
          <div style={{ display: 'flex', gap: 6, marginBottom: 16 }}>
            {[24, 60, 5].filter((int) => int === 24 || hasInterval(int)).map((int) => {
              const allowed = oiAccess.isLoading || oiAccess.canUseInterval(int);
              return (
                <button
                  key={int}
                  className={`fm-chip ${intervalValue === int ? 'active' : ''}`}
                  onClick={() => {
                    if (!allowed) {
                      const tier = oiAccess.requiredTierFor({ interval: int });
                      if (tier) {
                        showUpgrade({
                          tier,
                          featureName: `${int === 5 ? '5-минутный' : `${int}-часовой`} таймфрейм`,
                          indicator: 'open_interest',
                        });
                      }
                      return;
                    }
                    handleIntervalChange(int);
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
                  title={!allowed ? 'Доступно на повышенном тарифе' : undefined}
                >
                  {INTERVAL_LABELS[int]}
                  {!allowed && <Lock size={12} strokeWidth={2.2} />}
                </button>
              );
            })}
          </div>

          <div style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Период
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {(Object.keys(PERIOD_LABELS) as Period[]).map((p) => {
              // Два независимых ограничения:
              //   available  — производительность (5м-интервал ≠ длинная история)
              //   tierAllowed — подписка (глубокая история на Basic/Pro)
              const available = isPeriodAvailable(p);
              const tierAllowed = oiAccess.isLoading || oiAccess.canUsePeriod(p);
              return (
                <button
                  key={p}
                  className={`fm-chip ${period === p ? 'active' : ''}`}
                  onClick={() => {
                    if (!available) return;
                    if (!tierAllowed) {
                      const tier = oiAccess.requiredTierFor({ period: p });
                      if (tier) {
                        showUpgrade({
                          tier,
                          featureName: `период «${PERIOD_LABELS[p]}»`,
                          indicator: 'open_interest',
                        });
                      }
                      return;
                    }
                    handlePeriodChange(p);
                    setPeriodSheetOpen(false);
                  }}
                  disabled={!available}
                  style={{
                    opacity: !available ? 0.35 : tierAllowed ? 1 : 0.5,
                    cursor: available && tierAllowed ? 'pointer' : 'not-allowed',
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 4,
                  }}
                  aria-disabled={!available || !tierAllowed}
                  title={!available ? `Недоступно для ${INTERVAL_LABELS[intervalValue]}` : undefined}
                >
                  {PERIOD_LABELS[p]}
                  {available && !tierAllowed && <Lock size={11} strokeWidth={2.2} />}
                </button>
              );
            })}
          </div>
        </div>
      </MobileSheet>

      {/* Sheet: опции (вариант OI + категория участников) */}
      <MobileSheet
        open={optionsSheetOpen}
        onClose={() => setOptionsSheetOpen(false)}
        title="Опции"
      >
        <div style={{ padding: 16 }}>
          <div style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Тип данных
          </div>
          <div style={{ display: 'flex', gap: 6, marginBottom: 20 }}>
            {([['positions', 'Объём позиций'], ['participants', 'Число трейдеров']] as const).map(([m, label]) => (
              <button
                key={m}
                className={`fm-chip ${displayMode === m ? 'active' : ''}`}
                onClick={() => {
                  setDisplayMode(m);
                  setOptionsSheetOpen(false);
                }}
                style={{ flex: 1, justifyContent: 'center' }}
              >
                {label}
              </button>
            ))}
          </div>

          <div style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Что показать на графике
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 20 }}>
            {OI_VARIANTS.map((v) => (
              <button
                key={v}
                onClick={() => {
                  setOiVariant(v);
                  setOptionsSheetOpen(false);
                }}
                className={`fm-chip ${oiVariant === v ? 'active' : ''}`}
                style={{ gap: 6 }}
              >
                <span
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: 99,
                    background: VARIANT_COLORS[v],
                    flexShrink: 0,
                  }}
                />
                {variantLabel(v, displayMode)}
              </button>
            ))}
          </div>

          <div style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
            Категория участников
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            {(['YUR', 'FIZ'] as const).map((g) => (
              <button
                key={g}
                className={`fm-chip ${clgroup === g ? 'active' : ''}`}
                onClick={() => {
                  setClgroup(g);
                  setOptionsSheetOpen(false);
                }}
                style={{ flex: 1, justifyContent: 'center' }}
              >
                {g === 'YUR' ? 'Юрлица' : 'Физлица'}
              </button>
            ))}
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
