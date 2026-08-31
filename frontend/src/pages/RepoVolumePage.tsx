/**
 * /repo — Репо в акциях (экспериментальная вкладка, тест гипотезы).
 *
 * Гипотеза: объём сделок РЕПО с ЦК по бумаге — прокси шортов (бумагу берут
 * в репо, чтобы продать в короткую). Бумага — любая наша акция (пикер тот же,
 * что на Сезонности), история с июля 2013. На одном графике: спот-котировка
 * (левая ось) + правая ось в трёх режимах:
 *   «Объём» — дневной объём репо EQRP+PSRP, млрд ₽;
 *   «Ставка» — ставка стакана EQRP + бенчмарк RUSFAR (цена денег), % годовых;
 *   «Спред» — их разница в п.п. с нулевой отметкой (сам сигнал).
 * Ставка сильно ниже RUSFAR = бумага «special», её берут ради шорта, а не
 * ради денег; ставка ≈ RUSFAR = обычное фондирование под залог. Две линии
 * рядом («Ставка») показывают уровни, но разницу глаз не вычитает — для этого
 * отдельный режим «Спред».
 *
 * Источник репо — ISS MOEX (рынок ccp), бэкенд тянет историю on-demand:
 * первая загрузка тикера занимает несколько секунд, дальше из кэша.
 *
 * Admin-only (обкатка перед возможным публичным релизом): не-админ
 * редиректится на главную, nav-таб скрыт (adminOnly в Layout), API под
 * require_admin.
 *
 * Структура (упрощённый вариант Buffett-страницы):
 *   • PageHeader
 *   • Editorial frame: чипы бумаг + режим + сглаживание + период, SimpleChart
 */

import { useMemo, useRef, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { Repeat, ChevronDown } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import SimpleChart from '../components/SimpleChart';
import SegmentedControl from '../components/SegmentedControl';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import InstrumentIcon from '../components/InstrumentIcon';
import { getRepoVolume, type RepoVolumeResponse } from '../services/api';
import { useAuth } from '../contexts/AuthContext';
import { useFitToViewport } from '../hooks/useFitToViewport';
import { usePersistedState } from '../hooks/usePersistedState';
import { useIndicatorData } from '../hooks/useIndicatorData';

// Бумага любая из наших акций — выбор через общий InstrumentSearchModal
// (тот же пикер, что на Сезонности). Дефолт — исходный кейс гипотезы.
const DEFAULT_TICKER = 'SFIN';
const DEFAULT_NAME = 'ЭсЭфАй';

// История репо с ЦК начинается в июле 2013 (раньше рынка не было), RUSFAR —
// с 2018, поэтому спред на длинных периодах короче объёма.
type PeriodFilter = '1y' | '3y' | '5y' | 'all';
const PERIOD_OPTIONS: { key: PeriodFilter; label: string; days: number | null }[] = [
  { key: '1y', label: '1Г', days: 365 },
  { key: '3y', label: '3Г', days: 1095 },
  { key: '5y', label: '5Л', days: 1825 },
  { key: 'all', label: 'Всё', days: null },
];

// Дневной объём репо очень шумный (пики под дивиденды/экспирации) — без
// сглаживания тренд не читается. МА считается по торговым дням.
type SmoothMode = 'raw' | 'ma5' | 'ma20';
const SMOOTH_OPTIONS: { key: SmoothMode; label: string; window: number }[] = [
  { key: 'raw', label: 'Дни', window: 1 },
  { key: 'ma5', label: 'МА 5', window: 5 },
  { key: 'ma20', label: 'МА 20', window: 20 },
];

// Правая ось: объём репо, две ставки рядом, или их разница.
// «Ставка» отвечает на вопрос «сколько стоит», «Спред» — «дороже или дешевле
// денег», то есть собственно сигнал: две линии рядом глазом не вычитаются.
type ViewMode = 'volume' | 'rate' | 'spread';
const MODE_OPTIONS: { key: ViewMode; label: string }[] = [
  { key: 'volume', label: 'Объём' },
  { key: 'rate', label: 'Ставка' },
  { key: 'spread', label: 'Спред' },
];

const fmtBillions = (v: number) =>
  `${(v / 1e9).toLocaleString('ru-RU', { maximumFractionDigits: v >= 1e10 ? 1 : 2 })} млрд ₽`;

const fmtPercent = (v: number) =>
  `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })}%`;

// Спред всегда со знаком: «-8,4 п.п.» читается как «на 8,4 дешевле денег».
const fmtSpread = (v: number) =>
  `${v > 0 ? '+' : ''}${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} п.п.`;

/** Скользящее среднее по имеющимся точкам (без выравнивания по календарю).
 *  Первые win-1 точек считаются по накопленному окну — иначе начало пустует. */
function movingAverage(points: { time: string; value: number }[], win: number) {
  if (win <= 1) return points;
  let sum = 0;
  const queue: number[] = [];
  return points.map((p) => {
    queue.push(p.value);
    sum += p.value;
    if (queue.length > win) sum -= queue.shift() as number;
    return { time: p.time, value: sum / queue.length };
  });
}

export default function RepoVolumePage() {
  const { user, loading: authLoading } = useAuth();
  const [ticker, setTicker] = usePersistedState<string>('frame:repo:ticker', DEFAULT_TICKER);
  // Имя держим отдельно, чтобы кнопка пикера была подписана до ответа API.
  const [tickerName, setTickerName] = usePersistedState<string>('frame:repo:name', DEFAULT_NAME);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [period, setPeriod] = usePersistedState<PeriodFilter>('frame:repo:period', '3y');
  const [smooth, setSmooth] = usePersistedState<SmoothMode>('frame:repo:smooth', 'ma5');
  const [mode, setMode] = usePersistedState<ViewMode>('frame:repo:mode', 'volume');

  const { data, loading, error } = useIndicatorData<RepoVolumeResponse>({
    fetcher: () => getRepoVolume(ticker),
    deps: [ticker],
    errorMessage: (e) => (e as { message?: string } | null)?.message ?? 'Не удалось загрузить данные',
  });

  const chartAnchorRef = useRef<HTMLDivElement>(null);
  const chartHeight = useFitToViewport(chartAnchorRef, {
    min: 360,
    max: 720,
    bottomBuffer: 64,
  });

  // Срез по периоду — от последней даты данных, не от «сегодня».
  const visiblePoints = useMemo(() => {
    if (!data?.data.length) return [];
    const opt = PERIOD_OPTIONS.find((o) => o.key === period);
    if (!opt || opt.days === null) return data.data;
    const last = new Date(data.data[data.data.length - 1].date);
    const cutoff = new Date(last.getTime() - opt.days * 86400_000).toISOString().slice(0, 10);
    return data.data.filter((p) => p.date >= cutoff);
  }, [data, period]);

  const priceData = useMemo(
    () => visiblePoints.map((p) => ({ time: p.date, value: p.close })),
    [visiblePoints],
  );

  const smoothWindow = SMOOTH_OPTIONS.find((o) => o.key === smooth)?.window ?? 1;

  const repoData = useMemo(
    () => movingAverage(visiblePoints.map((p) => ({ time: p.date, value: p.repo })), smoothWindow),
    [visiblePoints, smoothWindow],
  );

  // Ставки: дни без сделок в стакане EQRP — пропуск точки, НЕ ноль (ноль
  // означал бы «бумага стоит как деньги» и врал бы на неликвиде).
  const rateData = useMemo(
    () =>
      visiblePoints
        .filter((p) => p.rate !== null)
        .map((p) => ({ time: p.date, value: p.rate as number })),
    [visiblePoints],
  );
  const rusfarData = useMemo(
    () =>
      visiblePoints
        .filter((p) => p.rusfar !== null)
        .map((p) => ({ time: p.date, value: p.rusfar as number })),
    [visiblePoints],
  );

  // Спред = ставка бумаги минус RUSFAR, в процентных пунктах. Считается только
  // там, где есть обе ставки. Ниже нуля — за бумагу платят премию к деньгам,
  // то есть её берут ради бумаги (шорт), а не ради денег.
  const spreadData = useMemo(
    () =>
      movingAverage(
        visiblePoints
          .filter((p) => p.rate !== null && p.rusfar !== null)
          .map((p) => ({ time: p.date, value: (p.rate as number) - (p.rusfar as number) })),
        smoothWindow,
      ),
    [visiblePoints, smoothWindow],
  );

  const assetName = data?.name ?? tickerName;
  const isRateMode = mode === 'rate';
  const isSpreadMode = mode === 'spread';
  // Нулевая линия — та самая «цена денег»: всё что под ней, дешевле фондирования.
  const zeroLine = useMemo(
    () => (isSpreadMode ? [{ value: 0, color: 'var(--text-muted)', axis: 'secondary' as const }] : undefined),
    [isSpreadMode],
  );

  // Admin-only: гость/не-админ — на главную. Проверка ПОСЛЕ всех хуков
  // (React hooks rule). Пока auth грузится — ничего не рендерим.
  if (authLoading) return null;
  if (user?.role !== 'admin') return <Navigate to="/" replace />;

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
      <PageHeader
        icon={Repeat}
        title="Репо в акциях"
        subtitle="Объём и ставка РЕПО с ЦК против котировки — тест гипотезы «репо как прокси шортов»"
        sourceNote="Источник: Московская биржа (ISS, рынок РЕПО с ЦК) · экспериментальный индикатор, история с июля 2013"
      />

      <div className="editorial-frame">
        {/* Контролы: бумага + сглаживание + период */}
        <div className="flex flex-wrap items-center mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
          {/* Пикер бумаги — общий InstrumentSearchModal (как на Сезонности):
              акций больше сотни, чипами такой список не показать. */}
          <div className="relative">
            <button
              onClick={() => setPickerOpen(true)}
              title={assetName}
              className="widget-flat font-medium transition-colors flex items-center hover:opacity-90"
              style={{
                color: 'var(--text-primary)',
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-2) var(--sp-4)',
                gap: 'var(--sp-3)',
                minWidth: 'clamp(140px, 22vw, 170px)',
                maxWidth: 220,
              }}
            >
              <InstrumentIcon sectype={ticker} size={28} rounded="full" eager />
              <div className="flex-1 text-left" style={{ minWidth: 0 }}>
                <div
                  className="font-medium"
                  style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                >
                  {assetName}
                </div>
                <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>
                  {ticker}
                </div>
              </div>
              <ChevronDown size={14} className="text-theme-secondary" />
            </button>

            {pickerOpen && (
              <InstrumentSearchModal
                filterType="stock"
                showIntradayBadge={false}
                onSelect={(sectype, name) => {
                  setTicker(sectype);
                  setTickerName(name);
                  setPickerOpen(false);
                }}
                onClose={() => setPickerOpen(false)}
              />
            )}
          </div>
          <SegmentedControl<ViewMode>
            options={MODE_OPTIONS.map((o) => ({ key: o.key, label: o.label }))}
            value={mode}
            onChange={setMode}
          />
          {/* Сглаживание — для объёма и спреда. В режиме «Ставка» линии
              показываем сырыми: там важен сам уровень относительно RUSFAR. */}
          {!isRateMode && (
            <SegmentedControl<SmoothMode>
              options={SMOOTH_OPTIONS.map((o) => ({ key: o.key, label: o.label }))}
              value={smooth}
              onChange={setSmooth}
            />
          )}
          <SegmentedControl<PeriodFilter>
            options={PERIOD_OPTIONS.map((o) => ({ key: o.key, label: o.label }))}
            value={period}
            onChange={setPeriod}
          />
        </div>

        <div
          ref={chartAnchorRef}
          className="rounded-2xl"
          style={{
            position: 'relative',
            background: 'var(--bg-primary)',
            border: '1.5px solid var(--text-primary)',
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
          ) : data && !loading && !data.has_repo ? (
            // Репо есть далеко не у всех бумаг: у неликвида сделок может не быть
            // вовсе. Честно говорим об этом, а не рисуем линию по нулям.
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
                <div className="font-bold mb-2">По этой бумаге репо не торгуется</div>
                <div style={{ fontSize: 'var(--fs-xs)', opacity: 0.8 }}>
                  За всю историю с 2013 года по {assetName} нет ни одной сделки РЕПО с ЦК
                </div>
              </div>
            </div>
          ) : (
            <SimpleChart
              data={priceData}
              secondaryData={isRateMode ? rateData : isSpreadMode ? spreadData : repoData}
              thirdData={isRateMode ? rusfarData : undefined}
              showSecondary={true}
              showThird={isRateMode}
              // RUSFAR — приглушённый ориентир, ставка бумаги — главная линия.
              thirdColor="var(--text-muted)"
              height={chartHeight}
              loading={loading}
              formatValue={(v) => `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽`}
              formatSecondaryValue={
                isSpreadMode ? fmtSpread : isRateMode ? fmtPercent : fmtBillions
              }
              formatThirdValue={fmtPercent}
              // Короткий формат для оси — без юнита, юнит в легенде/тултипе.
              formatSecondaryAxis={(v) =>
                isRateMode || isSpreadMode
                  ? v.toLocaleString('ru-RU', { maximumFractionDigits: 1 })
                  : (v / 1e9).toLocaleString('ru-RU', { maximumFractionDigits: 1 })
              }
              // Нулевая отметка спреда — граница «дороже/дешевле денег».
              horizontalLines={zeroLine}
              niceTicks={true}
              niceTicksSecondary={true}
              primaryLabel={assetName}
              secondaryLabel={
                isRateMode
                  ? 'Ставка репо (EQRP), %'
                  : isSpreadMode
                    ? 'Спред к RUSFAR, п.п.'
                    : 'Объём репо, млрд ₽'
              }
              thirdLabel="RUSFAR (цена денег), %"
              showValueHeader={false}
              legendPosition="top"
              showDownloadButton={false}
              showNavigator={true}
              chartPadding={{ left: 120, right: 120 }}
            />
          )}
        </div>
      </div>{/* /editorial-frame */}
    </div>
  );
}
