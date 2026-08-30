/**
 * /repo — Репо в акциях (экспериментальная вкладка, тест гипотезы).
 *
 * Гипотеза: объём сделок РЕПО с ЦК по бумаге — прокси шортов (бумагу берут
 * в репо, чтобы продать в короткую). На одном графике: спот-котировка
 * (левая ось) + правая ось в двух режимах:
 *   «Объём» — дневной объём репо EQRP+PSRP, млрд ₽;
 *   «Ставка» — ставка стакана EQRP + бенчмарк RUSFAR (цена денег), % годовых.
 * Ставка сильно ниже RUSFAR = бумага «special», её берут ради шорта, а не
 * ради денег; ставка ≈ RUSFAR = обычное фондирование под залог.
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

import { useMemo, useRef } from 'react';
import { Navigate } from 'react-router-dom';
import { Repeat } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import SimpleChart from '../components/SimpleChart';
import SegmentedControl from '../components/SegmentedControl';
import { getRepoVolume, type RepoVolumeResponse } from '../services/api';
import { useAuth } from '../contexts/AuthContext';
import { useFitToViewport } from '../hooks/useFitToViewport';
import { usePersistedState } from '../hooks/usePersistedState';
import { useIndicatorData } from '../hooks/useIndicatorData';

// Держать в синхроне с REPO_ASSETS в api/routers/repo_volume.py.
const ASSETS = [
  { key: 'SFIN', label: 'ЭсЭфАй' },
  { key: 'SBER', label: 'Сбербанк' },
  { key: 'GAZP', label: 'Газпром' },
  { key: 'MGNT', label: 'Магнит' },
  { key: 'MVID', label: 'М.Видео' },
] as const;
type AssetKey = (typeof ASSETS)[number]['key'];

type PeriodFilter = '1y' | '3y' | 'all';
const PERIOD_OPTIONS: { key: PeriodFilter; label: string; days: number | null }[] = [
  { key: '1y', label: '1Г', days: 365 },
  { key: '3y', label: '3Г', days: 1095 },
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

// Правая ось: объём репо или ставки (репо EQRP + RUSFAR).
type ViewMode = 'volume' | 'rate';
const MODE_OPTIONS: { key: ViewMode; label: string }[] = [
  { key: 'volume', label: 'Объём' },
  { key: 'rate', label: 'Ставка' },
];

const fmtBillions = (v: number) =>
  `${(v / 1e9).toLocaleString('ru-RU', { maximumFractionDigits: v >= 1e10 ? 1 : 2 })} млрд ₽`;

const fmtPercent = (v: number) =>
  `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })}%`;

export default function RepoVolumePage() {
  const { user, loading: authLoading } = useAuth();
  const [ticker, setTicker] = usePersistedState<AssetKey>('frame:repo:ticker', 'SFIN');
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

  const repoData = useMemo(() => {
    const win = SMOOTH_OPTIONS.find((o) => o.key === smooth)?.window ?? 1;
    if (win <= 1) return visiblePoints.map((p) => ({ time: p.date, value: p.repo }));
    // Скользящее среднее по торговым дням; первые win-1 точек — по факту
    // накопленного окна (иначе начало графика пустует).
    let sum = 0;
    const queue: number[] = [];
    return visiblePoints.map((p) => {
      queue.push(p.repo);
      sum += p.repo;
      if (queue.length > win) sum -= queue.shift() as number;
      return { time: p.date, value: sum / queue.length };
    });
  }, [visiblePoints, smooth]);

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

  const assetName = ASSETS.find((a) => a.key === ticker)?.label ?? ticker;
  const isRateMode = mode === 'rate';

  // Admin-only: гость/не-админ — на главную. Проверка ПОСЛЕ всех хуков
  // (React hooks rule). Пока auth грузится — ничего не рендерим.
  if (authLoading) return null;
  if (user?.role !== 'admin') return <Navigate to="/" replace />;

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
      <PageHeader
        icon={Repeat}
        title="Репо в акциях"
        subtitle="Объём сделок РЕПО с ЦК против котировки — тест гипотезы «репо как прокси шортов»"
        sourceNote="Источник: Московская биржа (ISS, рынок РЕПО с ЦК) · экспериментальный индикатор, 5 тестовых бумаг"
      />

      <div className="editorial-frame">
        {/* Контролы: бумага + сглаживание + период */}
        <div className="flex flex-wrap items-center mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
          <SegmentedControl<AssetKey>
            options={ASSETS.map((a) => ({ key: a.key, label: a.label }))}
            value={ticker}
            onChange={setTicker}
          />
          <SegmentedControl<ViewMode>
            options={MODE_OPTIONS.map((o) => ({ key: o.key, label: o.label }))}
            value={mode}
            onChange={setMode}
          />
          {/* Сглаживание применимо только к объёму (ставки не шумят так). */}
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
          ) : (
            <SimpleChart
              data={priceData}
              secondaryData={isRateMode ? rateData : repoData}
              thirdData={isRateMode ? rusfarData : undefined}
              showSecondary={true}
              showThird={isRateMode}
              // RUSFAR — приглушённый ориентир, ставка бумаги — главная линия.
              thirdColor="var(--text-muted)"
              height={chartHeight}
              loading={loading}
              formatValue={(v) => `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽`}
              formatSecondaryValue={isRateMode ? fmtPercent : fmtBillions}
              formatThirdValue={fmtPercent}
              // Короткий формат для оси — без юнита, юнит в легенде/тултипе.
              formatSecondaryAxis={(v) =>
                isRateMode
                  ? v.toLocaleString('ru-RU', { maximumFractionDigits: 1 })
                  : (v / 1e9).toLocaleString('ru-RU', { maximumFractionDigits: 1 })
              }
              niceTicks={true}
              niceTicksSecondary={true}
              primaryLabel={assetName}
              secondaryLabel={isRateMode ? 'Ставка репо (EQRP), %' : 'Объём репо, млрд ₽'}
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
