/**
 * RepaintPage — экспериментальный admin-only индикатор «Перекраска» (/admin/repaint).
 *
 * Идея (Вадим, 2026-08-30): сколько % от free float бумаги сменило руки за
 * последний месяц. CDV (кумулятивный дельта-объём) аппроксимируется по свечам
 * выбранного ТФ, как в TradingView (в БД нет биржевого разреза buy/sell): 1ч/4ч
 * из часовиков, 1д/1н из дневных свечей; изменение CDV за месяц делится на
 * количество акций в свободном обращении. Вторая метрика — отклонение CDV от его среднего за месяц
 * (насколько напокупали/напродавали относительно накопленной базы,
 * спекулятивный спрос).
 *
 * Source endpoints (оба role=admin, api/routers/repaint.py):
 *   GET /api/admin/repaint/screener?tf             — бумаги с данными (список для пикера)
 *   GET /api/admin/repaint/series/{secid}?days&tf  — ряд ТФ: цена + CDV + метрики
 *
 * Каркас как у остальных индикаторов: PageHeader, папки-вкладки (как категории
 * фондов в «Деньгах в фондах») и editorial-frame с рядом контролов (актив /
 * таймфрейм / период / CDV в % от free float) и одним графиком на вкладку:
 * «Цена и CDV» и «Перекраска».
 *
 * Пока индикатор экспериментальный: ссылка на него — только в admin-вкладке
 * навигации, обычным пользователям не показывается.
 */
import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { ChevronDown, LineChart, Repeat2 } from 'lucide-react';
import Skeleton from '../components/Skeleton';
import PageHeader from '../components/PageHeader';
import SimpleChart from '../components/SimpleChart';
import SegmentedControl from '../components/SegmentedControl';
import ChartTabs from '../components/ChartTabs';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import InstrumentIcon from '../components/InstrumentIcon';
import { useAuth } from '../contexts/AuthContext';
import { usePersistedState } from '../hooks/usePersistedState';
import { useIndicatorData } from '../hooks/useIndicatorData';
import { PERIOD_LABELS } from '../config/chartConfig';
import { getRepaintScreener, getRepaintSeries } from '../services/api';
import type { RepaintSeries, RepaintTf } from '../services/api';

const HINTS = {
  dev:
    'Отклонение текущего CDV от его среднего за последние 30 дней, в % от количества акций в свободном обращении (free float). Показывает, насколько сейчас напокупали или напродавали относительно обычного уровня месяца: если покупки шли ровно весь месяц, отклонение около нуля; если всё пришло за последние дни, отклонение большое (спекулятивный перегрев). Знак: + напокупали, − напродавали. CDV — аппроксимация по свечам выбранного таймфрейма (формула скрипта CDV в TradingView), биржевого разреза покупок/продаж в данных нет.',
  cdv:
    'Кумулятивный дельта-объём в штуках акций: сумма дельт свечей выбранного таймфрейма с начала периода (смысл несут изменения, не уровень). Дельта свечи = объём × (тело + тени/2)/(тело + тени) со знаком закрытия: тело целиком идёт в сторону закрытия, тени делятся пополам (формула скрипта CDV в TradingView). Как в TradingView, считается по самой свече таймфрейма, поэтому CDV меняется вместе с ТФ, на крупном ТФ оценка грубее. 1ч и 4ч строятся из часовых свечей, 1д и 1н — из дневных.',
  cdvFf:
    'CDV в процентах от количества акций в свободном обращении: какая доля free float нетто сменила руки с начала выбранного периода. Дельта каждой свечи делится на free float на её дату — официальная FF-капитализация МосБиржи (cap_total × ff_factor из корзины MOEXBMI, помесячно) / цена закрытия на дату среза.',
} as const;

const DEFAULT_TICKER = 'SBER';
const DEFAULT_NAME = 'Сбербанк';

// Период — от сегодня назад. «Всё» — вся история дневных свечей (SBER с 2007).
type Period = '1m' | '6m' | '1y' | '3y' | '5y' | 'all';
const PERIODS: Period[] = ['1m', '6m', '1y', '3y', '5y', 'all'];
const PERIOD_DAYS: Record<Period, number> = {
  '1m': 30, '6m': 182, '1y': 365, '3y': 1095, '5y': 1825, 'all': 9000,
};

// ТФ — свечи, по которым бэкенд считает дельту (1ч/4ч из часовиков, 1д/1н из
// дневных). Как на ОИ, у мелкого ТФ есть потолок периода (1ч за 5 лет — десятки
// тысяч точек), у недельного — пол (месяц в неделях — 4 точки). Период вне
// диапазона сам переключает ТФ.
const TF_OPTIONS: { key: RepaintTf; label: string; periods: Period[] }[] = [
  { key: '1h', label: '1ч', periods: ['1m', '6m', '1y'] },
  { key: '4h', label: '4ч', periods: ['1m', '6m', '1y', '3y', '5y'] },
  { key: '1d', label: '1д', periods: PERIODS },
  { key: '1w', label: '1н', periods: ['6m', '1y', '3y', '5y', 'all'] },
];
const tfPeriods = (tf: RepaintTf) => TF_OPTIONS.find((o) => o.key === tf)?.periods ?? PERIODS;

const CHART_HEIGHT = 380;

type Tab = 'cdv' | 'repaint';

/** Штуки акций → компактно: 1.23 млрд / 45.6 млн / 789 тыс. */
function fmtShares(v: number): string {
  const a = Math.abs(v);
  if (a >= 1e9) return `${(v / 1e9).toFixed(2)} млрд`;
  if (a >= 1e6) return `${(v / 1e6).toFixed(1)} млн`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(0)} тыс`;
  return v.toFixed(0);
}

// Проценты на графиках: у малых значений (CDV в % FF за месяц — сотые доли
// процента) третий знак, иначе подписи оси и бейдж слипаются в «0.01%».
const pctDigits = (v: number) => (Math.abs(v) < 0.1 ? 3 : 2);
/** Подпись оси в %: без хвостовых нулей (0.005% / 0.05% / 1.5%). */
const fmtAxisPct = (v: number) => `${Number(v.toFixed(pctDigits(v)))}%`;
/** Значение в тултипе и бейдже: со знаком, точность по величине. */
const fmtChartPct = (v: number) => `${v > 0 ? '+' : ''}${v.toFixed(pctDigits(v))}%`;
const fmtRub = (v: number) => `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽`;
const fmtSignedShares = (v: number) => `${v >= 0 ? '+' : ''}${fmtShares(v)} шт`;

export default function RepaintPage() {
  const { user, loading: authLoading } = useAuth();
  // Админ или ранний доступ по email (early_access с бэка, см. api/services/early_access.py).
  const isAdmin = user?.role === 'admin' || !!user?.early_access?.includes('repaint');

  const [ticker, setTicker] = usePersistedState<string>('frame:repaint:ticker', DEFAULT_TICKER);
  // Имя держим отдельно, чтобы кнопка актива была подписана до ответа API.
  const [tickerName, setTickerName] = usePersistedState<string>('frame:repaint:name', DEFAULT_NAME);
  const [tf, setTf] = usePersistedState<RepaintTf>('frame:repaint:tf', '4h');
  const [period, setPeriod] = usePersistedState<Period>('frame:repaint:period', '1y');
  const [cdvInFf, setCdvInFf] = usePersistedState<boolean>('frame:repaint:cdv-ff', false);
  const [devInFf, setDevInFf] = usePersistedState<boolean>('frame:repaint:dev-ff', false);
  const [tab, setTab] = usePersistedState<Tab>('frame:repaint:tab', 'cdv');
  const [pickerOpen, setPickerOpen] = useState(false);

  // Прежний ряд не сбрасываем на время загрузки — SimpleChart приглушает его сам.
  const { data: series, loading, error } = useIndicatorData<RepaintSeries>({
    fetcher: () => getRepaintSeries(ticker, PERIOD_DAYS[period], tf),
    deps: [ticker, period, tf, isAdmin],
    enabled: isAdmin,
    errorMessage: (e) => (e as { message?: string } | null)?.message ?? 'Не удалось загрузить данные',
  });
  // Скринер считается на свечах того же ТФ, что и график, — иначе цифры в
  // таблице расходились бы с последней точкой метрик.
  const { data: screener } = useIndicatorData({
    fetcher: () => getRepaintScreener(tf),
    deps: [isAdmin, tf],
    enabled: isAdmin,
    errorMessage: (e) => (e as { message?: string } | null)?.message ?? 'Не удалось загрузить скринер',
  });

  const rows = useMemo(() => screener?.rows ?? [], [screener]);
  // Скринер здесь нужен только пикеру: в нём — бумаги, по которым есть свечи и
  // free float (таблица под графиком убрана 2026-09-26 по просьбе юзера),
  // иначе выбор упирается в «нет данных». Пока скринер не пришёл — все акции.
  const pickerIds = useMemo(() => (rows.length > 0 ? rows.map((r) => r.sec_id) : undefined), [rows]);

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
  // Цена только на точках с готовой метрикой — обе линии на одном ряду дат.
  const metricPriceData = useMemo(
    () => metricPoints.map((p) => ({ time: p.time, value: p.close, open: p.open, high: p.high, low: p.low })),
    [metricPoints],
  );
  const devData = useMemo(
    () => metricPoints.map((p) => ({ time: p.time, value: (devInFf ? p.dev_pct : p.dev_shares) as number })),
    [metricPoints, devInFf],
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

      {/* Карточка с вкладками: обёртка несёт единую editorial-тень на
          [вкладки + панель], как на «Деньгах в фондах». */}
      <div className="tabbed-card">
      <ChartTabs<Tab>
        value={tab}
        onChange={setTab}
        items={[
          { key: 'cdv', label: 'Цена и CDV', Icon: LineChart },
          { key: 'repaint', label: 'Отклонение от среднего', Icon: Repeat2 },
        ]}
      />

      <div className="editorial-frame has-tabs">
        {/* Контролы как на остальных индикаторах: актив + таймфрейм + период,
            на вкладке «Цена и CDV» ещё тумблер «CDV в % от free float». */}
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
          {tab === 'cdv' && (
            <TogglePill active={cdvInFf} onClick={() => setCdvInFf(!cdvInFf)} title={HINTS.cdvFf}>
              CDV в % от free float
            </TogglePill>
          )}
          {tab === 'repaint' && (
            <TogglePill active={devInFf} onClick={() => setDevInFf(!devInFf)} title={HINTS.dev}>
              В % от free float
            </TogglePill>
          )}
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
        ) : !series ? (
          <Skeleton height={CHART_HEIGHT} rounded="lg" />
        ) : tab === 'cdv' ? (
          /* Цена + CDV (в штуках или в % от free float) */
          <SimpleChart
            data={priceData}
            secondaryData={cdvData}
            showSecondary={true}
            primaryColor="var(--accent)"
            secondaryColor="var(--accent-secondary)"
            primaryLabel="Цена"
            secondaryLabel={cdvInFf ? 'CDV, % от free float' : 'CDV, шт'}
            formatValue={fmtRub}
            formatSecondaryValue={cdvInFf ? fmtChartPct : fmtSignedShares}
            formatSecondaryAxis={cdvInFf ? fmtAxisPct : fmtShares}
            niceTicks={true}
            niceTicksSecondary={true}
            loading={loading}
            showValueHeader={false}
            legendPosition="top"
            showDownloadButton={false}
            showNavigator={true}
            chartPadding={{ left: 120, right: 120 }}
            height={CHART_HEIGHT}
          />
        ) : metricPoints.length > 0 ? (
          /* Цена + отклонение CDV от среднего за 30 дней: в штуках акций,
             по тумблеру — в % от free float. Отклонение — акцентная (оранжевая)
             линия, цена — вторым цветом. */
          <SimpleChart
            data={metricPriceData}
            secondaryData={devData}
            showSecondary={true}
            primaryColor="var(--accent-secondary)"
            secondaryColor="var(--accent)"
            primaryLabel="Цена"
            secondaryLabel={devInFf ? 'Отклонение от среднего 30д, % FF' : 'Отклонение от среднего 30д, шт'}
            formatValue={fmtRub}
            formatSecondaryValue={devInFf ? fmtChartPct : fmtSignedShares}
            formatSecondaryAxis={devInFf ? fmtAxisPct : fmtShares}
            niceTicks={true}
            niceTicksSecondary={true}
            loading={loading}
            showValueHeader={false}
            legendPosition="top"
            showDownloadButton={false}
            showNavigator={true}
            chartPadding={{ left: 120, right: 120 }}
            height={CHART_HEIGHT}
          />
        ) : (
          <p className="text-sm py-8 text-center" style={{ color: 'var(--text-muted)' }}>
            Недостаточно истории для месячного окна — отклонение появится, когда истории
            будет больше 30 дней.
          </p>
        )}
      </div>{/* /editorial-frame */}
      </div>{/* /tabbed-card */}

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
