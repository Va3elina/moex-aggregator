/**
 * EmbedSeasonality — виджет сезонности (per-ticker). Движок — общий LwChart
 * (как ОИ/Баффетт/Фонды), без SVG. Два режима:
 *   - «Календарь» — категориальная гистограмма ср. % изменения по срезу
 *     (Внутри дня / По дням недели / Внутри месяца / По месяцам). Ось X —
 *     КАТЕГОРИИ, не время: бары кладутся на синтетические timestamps с шагом
 *     сутки, а tickFmt мапит время назад в индекс бара → label среза.
 *   - «Годовая» — кумулятивная траектория среднего года (линия) + текущий год +
 *     опц. медиана. td (0..max_trading_days) растягивается на ПОЛНЫЙ
 *     синтетический год, чтобы ось показывала янв..дек (252 торговых дня иначе
 *     сожмутся в ~8 месяцев).
 *
 * ⚠️ Категориальная ось = приём дизайнера: кроссхэйр-пилюля на оси дат покажет
 * синтетическую дату — приемлемо (паритет с макетом), тултип значений верный.
 *
 * Контролы: тип графика + разрез в тулбаре; дивиденды/медиана/текущий год — в ⚙.
 * Виджет целиком под PRO-токеном → тир-гейтинга/онбординга/экспорта нет.
 */
import { useCallback, useContext, useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { CalendarDays, TrendingUp, Clock, Calendar, CalendarRange, Layers, X } from 'lucide-react';
import type { LwSeries } from '../../components/chart/lwTypes';
import LwChartPanes from '../../components/LwChartPanes';
import StackedBidirectionalHistogram from '../../components/cbr/StackedBidirectionalHistogram';
import { useTheme } from '../../contexts/ThemeContext';
import { FUND_PALETTE } from '../../config/chartTheme';
import { type PeriodConfig, makePeriodId } from '../../components/seasonality/periodConfig';
import {
  getSeasonality,
  getSeasonalityYears,
  getSeasonalityYearly,
  getSeasonalityIntradayUnsupported,
  type CbrFlowsPeriod,
  type SeasonalityResponse,
  type SeasonalityMode,
  type YearlySeasonalityResponse,
} from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { EmbedFrame, AssetButton, PillGroup, Dropdown, ToolbarMenuButton, SandboxWindowCtx } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';

type ChartType = 'histogram' | 'yearly';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';

// Размер панели под конкретный срез (§6.11 — «минимально необходимый размер под
// таймфрейм»): категориальным гистограммам нужна разная ширина под число баров
// (5 дней недели vs 31 день месяца vs 8 часовых баров intraday), годовой
// траектории — больше и ширины (252 торг. дня), и высоты (непрерывная линия,
// не бары). Работает только внутри песочницы — onResize там, дальше не задан.
// ⚠️ Применяется ОДИН РАЗ, при спавне панели (см. sizedRef ниже): дальше размер
// окна принадлежит пользователю.
const SIZE_BY_VIEW: Record<string, { w: number; h: number }> = {
  'histogram|intraday': { w: 480, h: 340 },
  'histogram|weekday': { w: 420, h: 340 },
  'histogram|monthday': { w: 680, h: 380 },
  'histogram|monthly': { w: 540, h: 360 },
  yearly: { w: 640, h: 420 },
};

const CHART_TYPES: { id: ChartType; label: string; icon: ReactNode }[] = [
  { id: 'histogram', label: 'Календарь', icon: <CalendarDays size={14} /> },
  { id: 'yearly', label: 'Годовая', icon: <TrendingUp size={14} /> },
];

// Точные строки MODE_LABELS со страницы Сезонности (SeasonalityPage.tsx).
const MODES: { id: SeasonalityMode; label: string }[] = [
  { id: 'intraday', label: 'Внутри дня' },
  { id: 'weekday', label: 'По дням недели' },
  { id: 'monthday', label: 'Внутри месяца' },
  { id: 'monthly', label: 'По месяцам' },
];
// Иконка «Разрез» зависит от выбранного значения (см. Dropdown icon как функция).
const MODE_ICONS: Record<SeasonalityMode, ReactNode> = {
  intraday: <Clock size={14} />,
  weekday: <CalendarDays size={14} />,
  monthday: <Calendar size={14} />,
  monthly: <CalendarRange size={14} />,
};

// Инструменты без дивидендов: индексы, валюты, сырьё, вечные фьючерсы.
// Тоггл «Без дивидендных гэпов» для них бесполезен → прячем (копия со страницы).
const NON_DIVIDEND_TICKERS = new Set([
  'IMOEX', 'RTSI', 'RGBI', 'RVI', 'MCFTR', 'RGBITR', 'RUSFAR3M',
  'GLDRUB_TOM', 'USD000UTSTOM', 'EUR_RUB__TOM', 'CNYRUB_TOM',
  'USDRUBF', 'EURRUBF', 'CNYRUBF', 'IMOEXF',
]);

// Полная история — как FULL_HISTORY_ITERS на странице.
const FULL_HISTORY_ITERS = 9999;

// Синтетическая временная база категориальной оси и годовой траектории.
const DAY = 86400;
const T0 = Math.floor(Date.UTC(2001, 0, 1) / 1000); // база вне реальных дат
const MONTHS_RU = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];

// Единственная «категория» плитки Календаря: значение одно на бар, цвет несёт
// направление (движок идёт с colorBySign), а не принадлежность к категории.
const SEASON_CAT = 'Ср. изменение';

// Формат процентов: точность по величине ряда (<2 → 2 знака, <10 → 1, иначе 0).
function pctDigits(maxAbs: number): number {
  return maxAbs < 2 ? 2 : maxAbs < 10 ? 1 : 0;
}
function fmtPct(v: number, digits: number, signed = true): string {
  const s = Math.abs(v).toFixed(digits).replace('.', ',');
  return (v < 0 ? '−' : signed && v > 0 ? '+' : '') + s + '%';
}


/** Максимум серий — как на сайте (MAX_COMPARE_SERIES). Больше пяти линий на
 *  одной плитке не читается, особенно в узкой панели. */
const MAX_PERIODS = 5;

const plural = (n: number, one: string, few: string, many: string): string => {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return one;
  if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return few;
  return many;
};

/** Содержимое поповера «Периоды»: строка на серию (цвет, подпись, свои тогглы
 *  медианы и дивидендов, удаление) + «добавить период». */
function PeriodsMenu({ periods, meta, availableYears, hasDividends, onChange }: {
  periods: PeriodConfig[];
  meta: { id: string; label: string; color: string }[];
  availableYears: number[];
  hasDividends: boolean;
  onChange: React.Dispatch<React.SetStateAction<PeriodConfig[]>>;
}) {
  const patch = (id: string, upd: Partial<PeriodConfig>) =>
    onChange((prev) => prev.map((p) => (p.id === id ? { ...p, ...upd } : p)));
  const rowStyle: CSSProperties = {
    display: 'flex', alignItems: 'center', gap: 6, padding: '4px 0',
  };
  const chipStyle = (on: boolean): CSSProperties => ({
    padding: '2px 7px', borderRadius: 6, cursor: 'pointer', whiteSpace: 'nowrap',
    fontSize: 'var(--fs-2xs)', fontWeight: 600, lineHeight: 1.6,
    border: `1px solid ${on ? 'var(--accent)' : 'color-mix(in srgb, var(--text-primary) 20%, transparent)'}`,
    background: on ? 'color-mix(in srgb, var(--accent) 16%, transparent)' : 'transparent',
    color: on ? 'var(--accent)' : 'var(--text-secondary)',
  });
  // Год, которого ещё нет среди серий — чтобы «+ Период» не плодил дубли
  // первым же кликом (дубли разрешены, но осмысленны только вручную).
  const usedYears = new Set(periods.map((p) => p.sinceYear));
  const freeYears = availableYears.filter((y) => !usedYears.has(y));

  return (
    <div style={{ minWidth: 230, maxWidth: 300, display: 'flex', flexDirection: 'column', gap: 2 }}>
      {periods.map((p) => {
        const m = meta.find((x) => x.id === p.id);
        return (
          <div key={p.id} style={{ borderBottom: '1px solid color-mix(in srgb, var(--text-primary) 10%, transparent)', paddingBottom: 4 }}>
            <div style={rowStyle}>
              <span style={{ width: 8, height: 8, borderRadius: 999, background: m?.color ?? 'var(--text-muted)', flexShrink: 0 }} />
              <select
                value={p.sinceYear}
                onChange={(e) => patch(p.id, { sinceYear: Number(e.target.value) })}
                style={{
                  flex: 1, minWidth: 0, background: 'transparent', color: 'var(--text-primary)',
                  border: 'none', fontSize: 'var(--fs-xs)', fontWeight: 700, cursor: 'pointer', padding: 0,
                }}
              >
                {availableYears.map((y) => <option key={y} value={y}>С {y} г.</option>)}
              </select>
              {periods.length > 1 && (
                <button
                  type="button"
                  onClick={() => onChange((prev) => prev.filter((x) => x.id !== p.id))}
                  title="Убрать период"
                  style={{ border: 'none', background: 'transparent', color: 'var(--text-secondary)', cursor: 'pointer', padding: 2, display: 'inline-flex' }}
                >
                  <X size={13} />
                </button>
              )}
            </div>
            <div style={{ display: 'flex', gap: 4, paddingBottom: 2 }}>
              <button type="button" style={chipStyle(p.median)} onClick={() => patch(p.id, { median: !p.median })}>
                Без выбросов
              </button>
              {hasDividends && (
                <button type="button" style={chipStyle(p.excludeDividends)} onClick={() => patch(p.id, { excludeDividends: !p.excludeDividends })}>
                  Без див.
                </button>
              )}
            </div>
          </div>
        );
      })}
      <button
        type="button"
        disabled={periods.length >= MAX_PERIODS || freeYears.length === 0}
        onClick={() => onChange((prev) => [...prev, {
          id: makePeriodId(),
          sinceYear: freeYears[Math.floor(freeYears.length / 2)] ?? availableYears[0],
          median: false,
          excludeDividends: false,
        }])}
        style={{
          marginTop: 4, padding: '5px 8px', borderRadius: 7, cursor: 'pointer',
          border: '1px solid color-mix(in srgb, var(--text-primary) 20%, transparent)',
          background: 'transparent', color: 'var(--text-primary)',
          fontSize: 'var(--fs-xs)', fontWeight: 700,
          opacity: periods.length >= MAX_PERIODS || freeYears.length === 0 ? 0.4 : 1,
        }}
      >
        + Период
      </button>
    </div>
  );
}

/** `initialInstrument` — стартовый актив от песочницы (см. EmbedOpenInterest). */
export default function EmbedSeasonality({ initialInstrument }: { initialInstrument?: string } = {}) {
  const { rd, wr, rdBool } = useEmbedPersist();
  const [params] = useSearchParams();
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const [stock, setStock] = useState<string>(() => initialInstrument || params.get('instrument') || rd('frame:embed:seasonality:stock', 'SBER'));
  const [chartType, setChartType] = useState<ChartType>(() => rd('frame:embed:seasonality:chartType', 'histogram') as ChartType);
  const [mode, setMode] = useState<SeasonalityMode>(() => rd('frame:embed:seasonality:mode', 'weekday') as SeasonalityMode);
  const [showCurrentYear, setShowCurrentYear] = useState<boolean>(() => rdBool('frame:embed:seasonality:showCurrentYear', true));
  // Серии «Период с YYYY» — как на сайте (components/seasonality/periodConfig.ts):
  // у КАЖДОЙ свои «медиана» и «без дивидендов», поэтому «С 2000» и «С 2000 ·
  // медиана» могут жить рядом. Раньше в виджете были два ГЛОБАЛЬНЫХ тоггла и
  // всегда одна серия по всей истории — самого смысла сезонности («а как оно
  // было без 2008 и 2022») виджет не умел.
  const [periods, setPeriods] = useState<PeriodConfig[]>([]);
  const [availableYears, setAvailableYears] = useState<number[]>([]);

  // Активы без физических часовых данных (см. api/routers/seasonality.py
  // INDICES_WITH_INTRADAY) — грузим один раз с бэка, список маленький и
  // меняется только с деплоем (раньше был захардкожен здесь же — уходил из
  // синхронизации при каждом расширении бэкенда, см. git history).
  const [intradayUnsupported, setIntradayUnsupported] = useState<string[]>([]);
  useEffect(() => {
    getSeasonalityIntradayUnsupported().then(setIntradayUnsupported).catch(() => {});
  }, []);

  // Доступные годы + сброс серий при смене тикера. Ровно как на сайте: у
  // каждого инструмента своя глубина истории, перенос выбора «от прошлого
  // актива» сбивает с толку → всегда одна серия с min_year нового актива.
  useEffect(() => {
    if (!stock) return;
    let cancelled = false;
    getSeasonalityYears(stock).then((resp) => {
      if (cancelled) return;
      setAvailableYears(resp.years);
      setPeriods(resp.years.length > 0
        ? [{ id: makePeriodId(), sinceYear: resp.years[0], median: false, excludeDividends: false }]
        : []);
    }).catch(() => {
      if (cancelled) return;
      setAvailableYears([]);
      setPeriods([]);
    });
    return () => { cancelled = true; };
  }, [stock]);

  // Клэмп: если текущий актив не поддерживает intraday (напр. persisted с
  // прошлой сессии, или список только что подгрузился) — тихо переключаем на
  // дефолт вместо доёма до 404 «Нет интрадей данных».
  useEffect(() => {
    if (mode === 'intraday' && intradayUnsupported.includes(stock)) {
      setMode('weekday');
    }
  }, [mode, stock, intradayUnsupported]);

  // §6.11: панель песочницы принимает размер под срез — но ТОЛЬКО при спавне.
  // ⚠️ Раньше эффект висел на [chartType, mode] и бил по размеру при каждом
  // переключении, а resizePanel не «расширяет до», а ЗАДАЁТ размер жёстко:
  // растянутое пользователем окно схлопывалось обратно после любого клика по
  // тулбару. Сезонность была единственным виджетом, который вообще трогает
  // onResize, поэтому так вело себя только это окно.
  // Читаемость широких срезов при этом не страдает: подписи оси прореживаются
  // по фактической ширине панели (см. dateless в StackedBidirectionalHistogram).
  const windowCtx = useContext(SandboxWindowCtx);
  const sizedRef = useRef(false);
  useEffect(() => {
    if (sizedRef.current) return;
    sizedRef.current = true;
    const sz = SIZE_BY_VIEW[chartType === 'histogram' ? `histogram|${mode}` : 'yearly'];
    if (sz) windowCtx?.onResize?.(sz.w, sz.h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Данные: по ОДНОМУ ответу на серию периода (порядок ответов = порядок periods).
  const [histSeries, setHistSeries] = useState<SeasonalityResponse[]>([]);
  const [yearSeries, setYearSeries] = useState<YearlySeasonalityResponse[]>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Стале-гард для отбрасывания устаревших ответов при быстром переключении.
  const reqIdRef = useRef(0);

  useEffect(() => { wr('frame:embed:seasonality:stock', stock); }, [stock]);
  useEffect(() => { wr('frame:embed:seasonality:chartType', chartType); }, [chartType]);
  useEffect(() => { wr('frame:embed:seasonality:mode', mode); }, [mode]);
  useEffect(() => { wr('frame:embed:seasonality:showCurrentYear', String(showCurrentYear)); }, [showCurrentYear]);

  // Инструменты без дивидендов: тоггл серии прячем И всегда шлём false.
  const hasDividends = !NON_DIVIDEND_TICKERS.has(stock);

  // Инструменты без интрадей (index_data) — «Внутри дня» не предлагаем в
  // дропдауне (не даём выбрать заведомо нерабочий срез).
  const supportsIntraday = !intradayUnsupported.includes(stock);
  const modeOptions = supportsIntraday ? MODES : MODES.filter((m) => m.id !== 'intraday');

  // Ключ серий для депсов эффекта: объект periods пересоздаётся на любой
  // toggle, а грузить надо только когда реально изменился НАБОР запросов.
  const periodsKey = useMemo(
    () => periods.map((p) => `${p.sinceYear}|${p.median ? 'm' : ''}|${p.excludeDividends ? 'd' : ''}`).join(';'),
    [periods],
  );

  // Фетч: по запросу на серию, параллельно. Стале-ответы отбрасываем по reqId.
  useEffect(() => {
    if (!stock) { setStatus('empty'); return; }
    if (periods.length === 0) return;  // ждём available-years
    const reqId = ++reqIdRef.current;
    let cancelled = false;
    setStatus('loading');
    const done = () => cancelled || reqId !== reqIdRef.current;

    if (chartType === 'histogram') {
      Promise.all(periods.map((p) => getSeasonality(
        stock, mode, FULL_HISTORY_ITERS, hasDividends && p.excludeDividends,
        { sinceYear: p.sinceYear, aggType: p.median ? 'median' : undefined },
      )))
        .then((res) => {
          if (done()) return;
          setHistSeries(res);
          setStatus((res[0]?.bars?.length ?? 0) > 0 ? 'ok' : 'empty');
        })
        .catch((err) => {
          if (done()) return;
          console.error('embed/seasonality histogram load failed:', err);
          setStatus('error');
        });
    } else {
      Promise.all(periods.map((p) => getSeasonalityYearly(
        stock, hasDividends && p.excludeDividends,
        { sinceYear: p.sinceYear, aggType: p.median ? 'median' : undefined },
      )))
        .then((res) => {
          if (done()) return;
          setYearSeries(res);
          setStatus((res[0]?.average?.length ?? 0) > 0 ? 'ok' : 'empty');
        })
        .catch((err) => {
          if (done()) return;
          console.error('embed/seasonality yearly load failed:', err);
          setStatus('error');
        });
    }
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stock, chartType, mode, periodsKey, hasDividends]);

  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();

  // Высоту не считаем: LwChartPanes всегда занимает 100% родителя, а родитель —
  // <div position:absolute;inset:0>. Прежний ResizeObserver существовал только
  // ради пропа height у LwChart.
  const boxRef = useRef<HTMLDivElement>(null);

  const isHist = chartType === 'histogram';
  // Первая серия рисуется СТОЛБЦАМИ, остальные — линиями поверх (движок
  // складывает категории в стек, см. overlays в StackedBidirectionalHistogram).
  const bars = useMemo(() => histSeries[0]?.bars ?? [], [histSeries]);

  // Подписи и цвета серий — как на сайте (SeasonalityPage.seriesMeta):
  // модификаторы в подписи дизамбигуируют дубли года («С 2000» vs
  // «С 2000 (медиана)»).
  const seriesMeta = useMemo(() => periods.map((p, idx) => {
    const mods = [p.median && 'медиана', p.excludeDividends && 'без див.'].filter(Boolean);
    return {
      id: p.id,
      label: `С ${p.sinceYear} г.${mods.length ? ` (${mods.join(', ')})` : ''}`,
      color: FUND_PALETTE[idx % FUND_PALETTE.length],
    };
  }), [periods]);

  // Ключ единственной «категории» плитки = подпись первой серии: он же идёт
  // в тултип. Иначе строки тултипа асимметричны — «Ср. изменение» у столбцов
  // и «С 2017 г.» у линии, и непонятно, к какому периоду относится первая.
  const baseCat = seriesMeta[0]?.label ?? SEASON_CAT;
  const baseCats = useMemo(() => [baseCat], [baseCat]);

  // ── Календарь: плитка срезов на движке потоков ЦБ ──
  // Тот же визуальный язык, что у потоков ЦБ и фондов: столбцы вверх/вниз от
  // нуля, окантовка, волна появления. Дат у среза нет (бар — агрегат за много
  // лет), поэтому движок идёт в режиме dateless.
  const histPeriods = useMemo<CbrFlowsPeriod[]>(
    () => bars.map((b) => ({
      year: 0,
      label: b.label,
      kind: 'month' as const,
      end_date: '',
      values: { [baseCat]: b.avg_change },
    })),
    [bars, baseCat],
  );

  // Точность процентов — по размаху ВСЕХ серий, иначе вторая серия с большим
  // размахом печаталась бы с точностью первой.
  const histDigits = useMemo(() => {
    let maxAbs = 0.01;
    for (const r of histSeries) for (const b of r.bars) maxAbs = Math.max(maxAbs, Math.abs(b.avg_change));
    return pctDigits(maxAbs);
  }, [histSeries]);

  // Дополнительные серии периодов — линиями поверх столбцов первой.
  const histOverlays = useMemo(() => histSeries.slice(1).flatMap((r, i) => {
    const meta = seriesMeta[i + 1];
    if (!meta || r.bars.length !== bars.length || !bars.length) return [];
    return [{ label: meta.label, color: meta.color, values: r.bars.map((b) => b.avg_change) }];
  }), [histSeries, seriesMeta, bars]);

  // Число наблюдений в срезе — без выборки «+3,2%» ничего не говорит
  // (на сайте эта строка есть в тултипе бара).
  const histTooltipNote = useCallback((i: number) => {
    const n = bars[i]?.count;
    return n == null ? null : `${n} ${plural(n, 'наблюдение', 'наблюдения', 'наблюдений')}`;
  }, [bars]);

  // Шкала под ПРОЦЕНТЫ: дефолт движка округляет до десятков с минимумом 10 —
  // он рассчитан на миллиарды, и ряд в 1-3% схлопнулся бы в столбцы высотой
  // в пиксель. Округляем вверх до «красивого» шага своего порядка величины.
  const histNiceMax = useCallback((maxAbs: number) => {
    if (!(maxAbs > 0)) return 1;
    const target = maxAbs * 1.12;
    const pow = Math.pow(10, Math.floor(Math.log10(target)));
    const step = [1, 1.5, 2, 2.5, 3, 4, 5, 10].find((s) => s * pow >= target) ?? 10;
    return step * pow;
  }, []);

  // Легенда: направление столбцов + подпись периода первой серии + остальные
  // серии своими цветами (на сайте период тоже подписан, у нас его не было).
  const histLegend = useMemo(() => [
    { label: 'Рост', color: 'var(--oi-green)' },
    { label: 'Падение', color: 'var(--oi-red)' },
    ...(seriesMeta[0] ? [{ label: seriesMeta[0].label, color: 'transparent', marker: 'none' as const }] : []),
    // (подпись первой серии — текстом без маркера: её столбцы уже раскрашены
    //  по знаку, отдельный цветной кружок был бы третьим смыслом цвета)
    ...histOverlays.map((o) => ({ label: o.label, color: o.color })),
  ], [seriesMeta, histOverlays]);

  const histFmtValue = useCallback((v: number) => fmtPct(v, Math.max(histDigits, 1)), [histDigits]);
  const histFmtAxis = useCallback((v: number) => fmtPct(v, histDigits, false), [histDigits]);

  // ── Годовая: кумулятивные линии на синтетическом годе ──
  // По линии на каждую серию периода + опционально текущий год.
  const yearlyLwSeries = useMemo<LwSeries[]>(() => {
    if (isHist || !yearSeries[0]?.average?.length) return [];
    const base = yearSeries[0];
    const maxTD = Math.max(1, base.max_trading_days - 1);
    // td → день синтетического года (растяжка на 364 дня, чтобы ось = янв..дек).
    const tOf = (td: number) => T0 + Math.round((td / maxTD) * 364) * DAY;
    const tip = (v: number) => fmtPct(v, 1);
    const axis = (v: number) => fmtPct(v, 0, false);
    const out: LwSeries[] = yearSeries.flatMap((r, i) => {
      const meta = seriesMeta[i];
      if (!meta || !r.average?.length) return [];
      return [{
        id: meta.id, type: 'line' as const, scale: 'right' as const, color: meta.color, lineWidth: 2,
        label: meta.label, zeroLine: i === 0,
        data: r.average.map((p) => ({ time: tOf(p.td), value: p.avg_pct })),
        tipFmt: tip, axisFmt: axis, minMove: 0.01,
      }];
    });
    if (showCurrentYear && base.current?.length) {
      out.push({
        id: 'cur', type: 'line', scale: 'right', color: 'var(--accent)', lineWidth: 2,
        label: String(base.current_year),
        data: base.current.map((p) => ({ time: tOf(p.td), value: p.pct })),
        tipFmt: tip, axisFmt: axis, minMove: 0.01,
      });
    }
    return out;
  }, [isHist, yearSeries, seriesMeta, showCurrentYear]);

  // Ось годовой: только месяцы (год синтетический — прячем «2001»).
  const yearlyTickFmt = useMemo(() => {
    if (isHist) return undefined;
    return (time: number, type: number) => {
      if (type > 1) return '';
      const d = new Date(time * 1000);
      return type === 0 ? '' : MONTHS_RU[d.getUTCMonth()];
    };
  }, [isHist]);

  // Лейбл нативного кроссхэйра «Годовой»: время синтетическое (T0 + индекс),
  // родной форматтер lightweight-charts показал бы случайную «реальную» дату
  // 2001 года — подставляем день+месяц без фиктивного года.
  const crosshairTimeFmt = useMemo(() => (time: number) => {
    const d = new Date(time * 1000);
    return `${d.getUTCDate()} ${MONTHS_RU[d.getUTCMonth()]}`;
  }, []);

  // Движку ЦБ нужна ЯВНАЯ высота в пикселях (он рисует SVG в фиксированный
  // бокс, а не тянется по родителю) — меряем контейнер, как у потоков фондов.
  const [chartH, setChartH] = useState(300);
  useEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setChartH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return (
    <EmbedFrame
      toolbarUnified
      lead={
        <AssetButton
          ticker={displayTicker(stock)}
          excludeType="futures"
          showIntradayBadge={false}
          current={stock}
          onSelect={(secid) => {
            setStock(secid);
            // Переключились на тикер без интрадей, пока стоял «Внутри дня» —
            // откатываем сразу в том же батче, до первого фетча (иначе 404).
            if (mode === 'intraday' && intradayUnsupported.includes(secid)) setMode('weekday');
          }}
        />
      }
      toolbar={
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — см. useToolbarCompact.ts: всегда полные лейблы. */}
          <div ref={toolbarMeasureRef} aria-hidden style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
            <PillGroup<ChartType> value={chartType} options={CHART_TYPES} onChange={setChartType} />
            <Dropdown<SeasonalityMode> value={mode} options={modeOptions} onChange={setMode} title="Разрез" icon={MODE_ICONS[mode]} />
            <ToolbarMenuButton label="Периоды" icon={<Layers size={14} />}>{() => null}</ToolbarMenuButton>
          </div>
          <PillGroup<ChartType> value={chartType} options={CHART_TYPES} onChange={setChartType} compact={toolbarCompact} />
          {isHist && <Dropdown<SeasonalityMode> value={mode} options={modeOptions} onChange={setMode} title="Разрез" icon={MODE_ICONS[mode]} compact={toolbarCompact} />}
          {/* Периоды — поповер, а не чипы в тулбаре: один чип «С 2008 г.
              (медиана, без див.)» ~200px, пять таких не влезут ни в какую
              панель песочницы. */}
          <ToolbarMenuButton label="Периоды" title="Периоды сравнения" icon={<Layers size={14} />} compact={toolbarCompact}>
            {() => (
              <PeriodsMenu
                periods={periods}
                meta={seriesMeta}
                availableYears={availableYears}
                hasDividends={hasDividends}
                onChange={setPeriods}
              />
            )}
          </ToolbarMenuButton>
        </div>
      }
      more={
        <>
          {!isHist && (
            <DrawerSection label="Текущий год">
              <ToggleRow label="Линия текущего года" checked={showCurrentYear} onChange={setShowCurrentYear} />
            </DrawerSection>
          )}
        </>
      }
    >
      <div ref={boxRef} style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}>
        {status === 'ok' && isHist && histPeriods.length > 0 && (
          <StackedBidirectionalHistogram
            periods={histPeriods}
            categories={baseCats}
            unit="%"
            height={chartH}
            animTrigger={`${stock}|${mode}|${periodsKey}`}
            // Срез — не датированный период: подписи оси и плавающая пилюля
            // берутся из label, блок «Объём торгов / м-м / г-г» скрыт.
            dateless
            colorBySign
            unitSuffix=""
            fmtValue={histFmtValue}
            fmtAxis={histFmtAxis}
            niceMax={histNiceMax}
            legendOverride={histLegend}
            overlays={histOverlays}
            tooltipNote={histTooltipNote}
            tooltipInSandbox
          />
        )}
        {status === 'ok' && !isHist && yearlyLwSeries.length > 0 && (
          <LwChartPanes
            panes={[{ series: yearlyLwSeries }]}
            // Статичный вид (как у потоков капитала): пан и зум выключены,
            // график всегда показывает всю историю и подстраивается под панель.
            // Здесь смысл в картине целиком, а не в разглядывании участка.
            staticView
            dark={dark}
            fitKey={`${chartType}|${stock}|${periodsKey}|${showCurrentYear}`}
            tickFmt={yearlyTickFmt}
            crosshairTimeFmt={crosshairTimeFmt}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text={stock ? 'Нет данных' : 'Акция не выбрана'} />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedFrame>
  );
}
