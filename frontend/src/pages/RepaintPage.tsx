/**
 * RepaintPage — экспериментальный admin-only индикатор «Перекраска» (/admin/repaint).
 *
 * Идея (Вадим, 2026-08-30): сколько % от free float бумаги сменило руки за
 * последний месяц. CDV (кумулятивный дельта-объём) аппроксимируется по свечам
 * (в БД нет биржевого разреза buy/sell): дельты часовиков складываются в бакеты
 * выбранного ТФ, изменение CDV за месяц делится на количество акций в свободном
 * обращении. Вторая метрика — отклонение CDV от его среднего за месяц
 * (насколько напокупали/напродавали относительно накопленной базы,
 * спекулятивный спрос).
 *
 * Source endpoints (оба role=admin, api/routers/repaint.py):
 *   GET /api/admin/repaint/screener                — метрики по всем акциям (таблица)
 *   GET /api/admin/repaint/series/{secid}?days&tf  — ряд ТФ: цена + CDV + метрики
 *
 * Каркас как у остальных индикаторов: PageHeader + editorial-frame, в нём ряд
 * контролов (актив / таймфрейм / период / CDV в % от free float) и графики.
 *
 * Пока индикатор экспериментальный: ссылка на него — только в admin-вкладке
 * навигации, обычным пользователям не показывается.
 */
import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { ChevronDown, Repeat2, Search } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import PageHeader from '../components/PageHeader';
import SimpleChart from '../components/SimpleChart';
import SegmentedControl from '../components/SegmentedControl';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import InstrumentIcon from '../components/InstrumentIcon';
import HelpTooltip from '../components/HelpTooltip';
import { useAuth } from '../contexts/AuthContext';
import { usePersistedState } from '../hooks/usePersistedState';
import { useIndicatorData } from '../hooks/useIndicatorData';
import { PERIOD_LABELS } from '../config/chartConfig';
import { getRepaintScreener, getRepaintSeries } from '../services/api';
import type { RepaintSeries, RepaintTf } from '../services/api';

const HINTS = {
  repaint:
    'Изменение CDV за последние 30 дней, делённое на количество акций в свободном обращении (free float). Показывает, какая доля free float нетто сменила руки за месяц: кто-то продал — кто-то новый купил. Знак — в какую сторону: + напокупали, − напродавали. Вторая линия — отклонение текущего CDV от его среднего за 30 дней, тоже в % от free float: насколько напокупали или напродавали относительно накопленной базы (спекулятивный спрос). Окно — 30 календарных дней на любом таймфрейме. CDV — аппроксимация по свечам (формула из OHLCV, как CDV в TradingView), биржевого разреза покупок/продаж в данных нет.',
  cdv:
    'Кумулятивный дельта-объём в штуках акций: сумма дельт свечей с начала выбранного периода (смысл несут изменения, не уровень). Дельта свечи = sign(close−open) × тело/(тело+тени) × объём; считается по часовым свечам и складывается в бакеты таймфрейма.',
  cdvFf:
    'CDV в процентах от количества акций в свободном обращении: какая доля free float нетто сменила руки с начала выбранного периода. Дельта каждой свечи делится на free float на её дату — официальная FF-капитализация МосБиржи (cap_total × ff_factor из корзины MOEXBMI, помесячно) / цена закрытия на дату среза.',
} as const;

const DEFAULT_TICKER = 'SBER';
const DEFAULT_NAME = 'Сбербанк';

// Период — от сегодня назад. «Всё» упирается в начало часовой истории (SBER с 2011).
type Period = '1m' | '6m' | '1y' | '3y' | '5y' | 'all';
const PERIODS: Period[] = ['1m', '6m', '1y', '3y', '5y', 'all'];
const PERIOD_DAYS: Record<Period, number> = {
  '1m': 30, '6m': 182, '1y': 365, '3y': 1095, '5y': 1825, 'all': 9000,
};

// ТФ — бакеты из часовых свечей (складывает бэкенд). Как на ОИ, у мелкого ТФ
// есть потолок периода (1ч за 5 лет — десятки тысяч точек), у недельного — пол
// (месяц в неделях — 4 точки). Период вне диапазона сам переключает ТФ.
const TF_OPTIONS: { key: RepaintTf; label: string; periods: Period[] }[] = [
  { key: '1h', label: '1ч', periods: ['1m', '6m', '1y'] },
  { key: '4h', label: '4ч', periods: ['1m', '6m', '1y', '3y', '5y'] },
  { key: '1d', label: '1д', periods: PERIODS },
  { key: '1w', label: '1н', periods: ['6m', '1y', '3y', '5y', 'all'] },
];
const tfPeriods = (tf: RepaintTf) => TF_OPTIONS.find((o) => o.key === tf)?.periods ?? PERIODS;

const CHART_HEIGHT = 380;
const METRIC_CHART_HEIGHT = 300;

/** Штуки акций → компактно: 1.23 млрд / 45.6 млн / 789 тыс. */
function fmtShares(v: number): string {
  const a = Math.abs(v);
  if (a >= 1e9) return `${(v / 1e9).toFixed(2)} млрд`;
  if (a >= 1e6) return `${(v / 1e6).toFixed(1)} млн`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(0)} тыс`;
  return v.toFixed(0);
}

function fmtPct(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
}

/** Подпись оси в %: без хвостовых нулей (0.05% / 1.5% / 12%). */
const fmtAxisPct = (v: number) => `${Number(v.toFixed(2))}%`;
const fmtRub = (v: number) => `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽`;
const fmtSignedShares = (v: number) => `${v >= 0 ? '+' : ''}${fmtShares(v)} шт`;

function pctColor(v: number | null | undefined): string {
  if (v == null) return 'var(--text-muted)';
  return v >= 0 ? 'var(--success)' : 'var(--danger)';
}

export default function RepaintPage() {
  const { user, loading: authLoading } = useAuth();
  const isAdmin = user?.role === 'admin';

  const [ticker, setTicker] = usePersistedState<string>('frame:repaint:ticker', DEFAULT_TICKER);
  // Имя держим отдельно, чтобы кнопка актива была подписана до ответа API.
  const [tickerName, setTickerName] = usePersistedState<string>('frame:repaint:name', DEFAULT_NAME);
  const [tf, setTf] = usePersistedState<RepaintTf>('frame:repaint:tf', '4h');
  const [period, setPeriod] = usePersistedState<Period>('frame:repaint:period', '1y');
  const [cdvInFf, setCdvInFf] = usePersistedState<boolean>('frame:repaint:cdv-ff', false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [query, setQuery] = useState('');

  // Прежний ряд не сбрасываем на время загрузки — SimpleChart приглушает его сам.
  const { data: series, loading, error } = useIndicatorData<RepaintSeries>({
    fetcher: () => getRepaintSeries(ticker, PERIOD_DAYS[period], tf),
    deps: [ticker, period, tf, isAdmin],
    enabled: isAdmin,
    errorMessage: (e) => (e as { message?: string } | null)?.message ?? 'Не удалось загрузить данные',
  });
  const { data: screener, error: rowsError } = useIndicatorData({
    fetcher: getRepaintScreener,
    deps: [isAdmin],
    enabled: isAdmin,
    errorMessage: (e) => (e as { message?: string } | null)?.message ?? 'Не удалось загрузить скринер',
  });

  const rows = useMemo(() => screener?.rows ?? [], [screener]);
  // В пикере — только бумаги из скринера (есть часовые свечи и free float),
  // иначе выбор упирается в «нет данных». Пока скринер не пришёл — все акции.
  const pickerIds = useMemo(() => (rows.length > 0 ? rows.map((r) => r.sec_id) : undefined), [rows]);

  const filteredRows = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(
      r => r.sec_id.toLowerCase().includes(q) || r.name.toLowerCase().includes(q),
    );
  }, [rows, query]);

  const priceData = useMemo(
    () => (series?.points ?? []).map((p) => ({
      time: p.time, value: p.close, open: p.open, high: p.high, low: p.low,
    })),
    [series],
  );
  const cdvData = useMemo(
    () => (series?.points ?? []).map((p) => ({ time: p.time, value: cdvInFf ? p.cdv_ff_pct : p.cdv })),
    [series, cdvInFf],
  );

  // Точки с готовыми метриками (первый месяц истории — прогрев окна, метрик нет).
  const metricPoints = useMemo(
    () => (series?.points ?? []).filter((p) => p.repaint_pct != null),
    [series],
  );
  const repaintData = useMemo(
    () => metricPoints.map((p) => ({ time: p.time, value: p.repaint_pct as number })),
    [metricPoints],
  );
  const devData = useMemo(
    () => metricPoints.map((p) => ({ time: p.time, value: p.dev_pct as number })),
    [metricPoints],
  );

  // Период вне диапазона текущего ТФ → самый детальный ТФ, который его держит (как на ОИ).
  const changePeriod = (p: Period) => {
    if (!tfPeriods(tf).includes(p)) {
      setTf(TF_OPTIONS.find((o) => o.periods.includes(p))?.key ?? '1d');
    }
    setPeriod(p);
  };
  // ТФ, которому текущий период не по размеру → ближайший допустимый период.
  const changeTf = (next: RepaintTf) => {
    const allowed = tfPeriods(next);
    if (!allowed.includes(period)) {
      const idx = PERIODS.indexOf(period);
      const dist = (p: Period) => Math.abs(PERIODS.indexOf(p) - idx);
      setPeriod(allowed.reduce((best, p) => (dist(p) < dist(best) ? p : best)));
    }
    setTf(next);
  };

  const selectTicker = (secId: string, name: string) => {
    setTicker(secId);
    setTickerName(name);
  };

  // Admin-only: гость/не-админ — на главную. Проверка ПОСЛЕ всех хуков
  // (React hooks rule). Пока auth грузится — ничего не рендерим.
  if (authLoading) return null;
  if (!isAdmin) return <Navigate to="/" replace />;

  // Пока грузится новый актив, series ещё от прежнего — подпись берём из state.
  const assetName = series && series.sec_id === ticker ? series.name : tickerName;

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
      <PageHeader
        icon={Repeat2}
        title="Перекраска"
        subtitle="Сколько % free float сменило руки за месяц · эксперимент, только для администратора"
      />

      <div className="editorial-frame">
        {/* Контролы как на остальных индикаторах: актив + таймфрейм + период,
            последним — тумблер «CDV в % от free float» для верхнего графика. */}
        <div className="flex flex-wrap items-center mb-4 md:mb-6" style={{ gap: 'var(--sp-2)' }}>
          {/* Пикер бумаги — общий InstrumentSearchModal (как на ОИ и Сезонности). */}
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
            <InstrumentIcon sectype={ticker} size={24} rounded="full" eager />
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
            <ChevronDown size={14} className="text-theme-secondary flex-shrink-0" />
          </button>
          <SegmentedControl<RepaintTf>
            options={TF_OPTIONS.map((o) => ({ key: o.key, label: o.label }))}
            value={tf}
            onChange={changeTf}
          />
          <SegmentedControl<Period>
            options={PERIODS.map((p) => ({ key: p, label: PERIOD_LABELS[p] }))}
            value={period}
            onChange={changePeriod}
          />
          <TogglePill active={cdvInFf} onClick={() => setCdvInFf(!cdvInFf)} title={HINTS.cdvFf}>
            CDV в % от free float
          </TogglePill>
        </div>

        {error ? (
          <div
            className="flex items-center justify-center"
            style={{
              height: CHART_HEIGHT,
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
          <>
            {/* Цена + CDV (в штуках или в % от free float) */}
            <SectionTitle
              title={cdvInFf ? 'Цена и CDV, % от free float' : 'Цена и CDV'}
              hint={cdvInFf ? HINTS.cdvFf : HINTS.cdv}
            />
            <div className="mb-6 md:mb-8">
              {series ? (
                <SimpleChart
                  data={priceData}
                  secondaryData={cdvData}
                  showSecondary={true}
                  primaryColor="var(--accent)"
                  secondaryColor="var(--accent-secondary)"
                  primaryLabel="Цена"
                  secondaryLabel={cdvInFf ? 'CDV, % от free float' : 'CDV, шт'}
                  formatValue={fmtRub}
                  formatSecondaryValue={cdvInFf ? fmtPct : fmtSignedShares}
                  formatSecondaryAxis={cdvInFf ? fmtAxisPct : fmtShares}
                  loading={loading}
                  showValueHeader={false}
                  legendPosition="top"
                  showDownloadButton={false}
                  showNavigator={false}
                  height={CHART_HEIGHT}
                />
              ) : (
                <Skeleton height={CHART_HEIGHT} rounded="lg" />
              )}
            </div>

            {/* Метрики перекраски */}
            <SectionTitle title="Перекраска и отклонение от среднего, % от free float" hint={HINTS.repaint} />
            {!series ? (
              <Skeleton height={METRIC_CHART_HEIGHT} rounded="lg" />
            ) : metricPoints.length > 0 ? (
              <SimpleChart
                data={repaintData}
                secondaryData={devData}
                showSecondary={true}
                primaryColor="var(--accent)"
                secondaryColor="var(--accent-secondary)"
                primaryLabel="Перекраска за 30д"
                secondaryLabel="Отклонение от среднего 30д"
                formatValue={fmtPct}
                formatPrimaryAxis={fmtAxisPct}
                formatSecondaryValue={fmtPct}
                formatSecondaryAxis={fmtAxisPct}
                loading={loading}
                showValueHeader={false}
                legendPosition="top"
                showDownloadButton={false}
                showNavigator={false}
                height={METRIC_CHART_HEIGHT}
              />
            ) : (
              <p className="text-sm py-8 text-center" style={{ color: 'var(--text-muted)' }}>
                Недостаточно истории для месячного окна — метрики появятся, когда часовых свечей
                будет больше 30 дней.
              </p>
            )}
          </>
        )}
      </div>{/* /editorial-frame */}

      {/* Скринер по всем акциям */}
      <div className="mt-6 md:mt-8">
        <SectionTitle title="Все акции — текущая перекраска" hint={HINTS.repaint} />
        {rowsError && (
          <Card padding="md" className="mb-6">
            <p style={{ color: 'var(--danger)' }}>Ошибка скринера: {rowsError}</p>
          </Card>
        )}
        <Card padding="md" className="md:p-5">
          <div className="relative mb-3" style={{ maxWidth: 320 }}>
            <Search
              size={15}
              className="absolute left-3 top-1/2 -translate-y-1/2"
              style={{ color: 'var(--text-muted)' }}
            />
            <input
              type="text"
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder="Тикер или название…"
              className="w-full pl-9 pr-3 py-2 text-sm outline-none border"
              style={{
                backgroundColor: 'var(--bg-primary)',
                borderColor: 'var(--border-color)',
                borderRadius: 'var(--radius-md, 8px)',
                color: 'var(--text-primary)',
              }}
            />
          </div>
          {!screener && !rowsError ? (
            <Skeleton height={300} rounded="lg" />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm" style={{ color: 'var(--text-primary)' }}>
                <thead>
                  <tr
                    className="text-xs uppercase text-left"
                    style={{ color: 'var(--text-muted)', letterSpacing: '0.06em' }}
                  >
                    <th className="py-2 pr-3 font-semibold">Бумага</th>
                    <th className="py-2 px-3 font-semibold text-right">Перекраска 30д</th>
                    <th className="py-2 px-3 font-semibold text-right">Отклонение 30д</th>
                    <th className="py-2 px-3 font-semibold text-right">Free float</th>
                    <th className="py-2 pl-3 font-semibold text-right">Цена</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.map(r => (
                    <tr
                      key={r.sec_id}
                      onClick={() => { selectTicker(r.sec_id, r.name); window.scrollTo({ top: 0, behavior: 'smooth' }); }}
                      className="cursor-pointer transition-colors"
                      style={{
                        borderTop: '1px solid var(--border-color)',
                        backgroundColor: r.sec_id === ticker
                          ? 'color-mix(in srgb, var(--accent) 8%, transparent)'
                          : undefined,
                      }}
                    >
                      <td className="py-2 pr-3">
                        <span className="font-medium">{r.name}</span>
                        <span className="ml-2 text-xs" style={{ color: 'var(--text-muted)' }}>{r.sec_id}</span>
                      </td>
                      <td className="py-2 px-3 text-right font-medium" style={{ color: pctColor(r.repaint_pct) }}>
                        {fmtPct(r.repaint_pct)}
                      </td>
                      <td className="py-2 px-3 text-right" style={{ color: pctColor(r.dev_pct) }}>
                        {fmtPct(r.dev_pct)}
                      </td>
                      <td className="py-2 px-3 text-right" style={{ color: 'var(--text-muted)' }}>
                        {fmtShares(r.ff_shares)} шт
                      </td>
                      <td className="py-2 pl-3 text-right">
                        {r.close.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {filteredRows.length === 0 && (
                <p className="text-sm py-6 text-center" style={{ color: 'var(--text-muted)' }}>
                  Ничего не найдено
                </p>
              )}
            </div>
          )}
        </Card>
      </div>

      {pickerOpen && (
        <InstrumentSearchModal
          filterType="stock"
          showIntradayBadge={false}
          onlySectypes={pickerIds}
          onSelect={(sectype, name) => {
            selectTicker(sectype, name);
            setPickerOpen(false);
          }}
          onClose={() => setPickerOpen(false)}
        />
      )}
    </div>
  );
}

function SectionTitle({ title, hint }: { title: string; hint: string }) {
  return (
    <div className="flex items-center gap-2 mb-3">
      <p
        className="text-xs uppercase"
        style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
      >
        {title}
      </p>
      <HelpTooltip content={hint} icon="info" />
      <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
    </div>
  );
}

/** Тумблер в той же пилюле, что SegmentedControl: включён — accent-заливка. */
function TogglePill({ active, onClick, title, children }: {
  active: boolean;
  onClick: () => void;
  title?: string;
  children: ReactNode;
}) {
  const [hovered, setHovered] = useState(false);
  return (
    <div
      className="frame-segmented rounded-full overflow-hidden"
      style={{
        display: 'inline-grid',
        backgroundColor: 'var(--bg-secondary)',
        border: '2px solid var(--text-primary)',
      }}
    >
      <button
        type="button"
        aria-pressed={active}
        title={title}
        onClick={onClick}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
        className="frame-segmented-item font-semibold inline-flex items-center justify-center"
        style={{
          fontSize: 'var(--fs-sm)',
          padding: 'var(--sp-2) var(--sp-3)',
          backgroundColor: active
            ? 'var(--accent)'
            : hovered
              ? 'color-mix(in srgb, var(--accent) 18%, transparent)'
              : 'transparent',
          color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
          cursor: 'pointer',
          whiteSpace: 'nowrap',
          transition: 'background-color 0.12s ease, color 0.12s ease',
        }}
      >
        {children}
      </button>
    </div>
  );
}
