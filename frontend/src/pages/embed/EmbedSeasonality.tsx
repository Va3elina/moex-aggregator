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
import { useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { CalendarDays, TrendingUp, Clock, Calendar, CalendarRange } from 'lucide-react';
import type { LwSeries } from '../../components/chart/lwTypes';
import LwChartPanes from '../../components/LwChartPanes';
import { useTheme } from '../../contexts/ThemeContext';
import {
  getSeasonality,
  getSeasonalityYearly,
  getSeasonalityIntradayUnsupported,
  type SeasonalityResponse,
  type SeasonalityMode,
  type YearlySeasonalityResponse,
} from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { EmbedMsg } from './embedUi';
import { DrawerSection, ToggleRow } from './EmbedSettings';
import { EmbedFrame, AssetButton, PillGroup, Dropdown, SandboxWindowCtx } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';

type ChartType = 'histogram' | 'yearly';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';

// Размер панели под конкретный срез (§6.11 — «минимально необходимый размер под
// таймфрейм»): категориальным гистограммам нужна разная ширина под число баров
// (5 дней недели vs 31 день месяца vs 8 часовых баров intraday), годовой
// траектории — больше и ширины (252 торг. дня), и высоты (непрерывная линия,
// не бары). Работает только внутри песочницы — onResize там, дальше не задан.
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

// Формат процентов: точность по величине ряда (<2 → 2 знака, <10 → 1, иначе 0).
function pctDigits(maxAbs: number): number {
  return maxAbs < 2 ? 2 : maxAbs < 10 ? 1 : 0;
}
function fmtPct(v: number, digits: number, signed = true): string {
  const s = Math.abs(v).toFixed(digits).replace('.', ',');
  return (v < 0 ? '−' : signed && v > 0 ? '+' : '') + s + '%';
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
  const [excludeDividends, setExcludeDividends] = useState<boolean>(() => rdBool('frame:embed:seasonality:excludeDividends', false));
  const [showNoOutliers, setShowNoOutliers] = useState<boolean>(() => rdBool('frame:embed:seasonality:showNoOutliers', false));
  const [showCurrentYear, setShowCurrentYear] = useState<boolean>(() => rdBool('frame:embed:seasonality:showCurrentYear', true));

  // Активы без физических часовых данных (см. api/routers/seasonality.py
  // INDICES_WITH_INTRADAY) — грузим один раз с бэка, список маленький и
  // меняется только с деплоем (раньше был захардкожен здесь же — уходил из
  // синхронизации при каждом расширении бэкенда, см. git history).
  const [intradayUnsupported, setIntradayUnsupported] = useState<string[]>([]);
  useEffect(() => {
    getSeasonalityIntradayUnsupported().then(setIntradayUnsupported).catch(() => {});
  }, []);

  // Клэмп: если текущий актив не поддерживает intraday (напр. persisted с
  // прошлой сессии, или список только что подгрузился) — тихо переключаем на
  // дефолт вместо доёма до 404 «Нет интрадей данных».
  useEffect(() => {
    if (mode === 'intraday' && intradayUnsupported.includes(stock)) {
      setMode('weekday');
    }
  }, [mode, stock, intradayUnsupported]);

  // §6.11: панель песочницы принимает размер под текущий срез при каждой смене
  // типа графика/режима (не только при спавне) — 31 бар «внутри месяца» не
  // помещается в размер под 5 баров «по дням недели», и наоборот жаль места.
  const windowCtx = useContext(SandboxWindowCtx);
  useEffect(() => {
    const sz = SIZE_BY_VIEW[chartType === 'histogram' ? `histogram|${mode}` : 'yearly'];
    if (sz) windowCtx?.onResize?.(sz.w, sz.h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chartType, mode]);

  // Данные: база + опциональная медиана, отдельно для histogram и yearly.
  const [histBase, setHistBase] = useState<SeasonalityResponse | null>(null);
  const [histMedian, setHistMedian] = useState<SeasonalityResponse | null>(null);
  const [yearBase, setYearBase] = useState<YearlySeasonalityResponse | null>(null);
  const [yearMedian, setYearMedian] = useState<YearlySeasonalityResponse | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Стале-гард для отбрасывания устаревших ответов при быстром переключении.
  const reqIdRef = useRef(0);

  useEffect(() => { wr('frame:embed:seasonality:stock', stock); }, [stock]);
  useEffect(() => { wr('frame:embed:seasonality:chartType', chartType); }, [chartType]);
  useEffect(() => { wr('frame:embed:seasonality:mode', mode); }, [mode]);
  useEffect(() => { wr('frame:embed:seasonality:excludeDividends', String(excludeDividends)); }, [excludeDividends]);
  useEffect(() => { wr('frame:embed:seasonality:showNoOutliers', String(showNoOutliers)); }, [showNoOutliers]);
  useEffect(() => { wr('frame:embed:seasonality:showCurrentYear', String(showCurrentYear)); }, [showCurrentYear]);

  // Инструменты без дивидендов: тоггл прячем И всегда шлём excludeDividends=false.
  const hasDividends = !NON_DIVIDEND_TICKERS.has(stock);
  const effExcludeDividends = hasDividends && excludeDividends;

  // Инструменты без интрадей (index_data) — «Внутри дня» не предлагаем в
  // дропдауне (не даём выбрать заведомо нерабочий срез).
  const supportsIntraday = !intradayUnsupported.includes(stock);
  const modeOptions = supportsIntraday ? MODES : MODES.filter((m) => m.id !== 'intraday');

  // Фетч: база + (опционально) медиана параллельно. По chartType — гистограмма
  // или годовая. Стале-ответы отбрасываем по reqId.
  useEffect(() => {
    if (!stock) { setStatus('empty'); return; }
    const reqId = ++reqIdRef.current;
    let cancelled = false;
    setStatus('loading');

    if (chartType === 'histogram') {
      const basePromise = getSeasonality(stock, mode, FULL_HISTORY_ITERS, effExcludeDividends);
      const medianPromise = showNoOutliers
        ? getSeasonality(stock, mode, FULL_HISTORY_ITERS, effExcludeDividends, { aggType: 'median' })
        : Promise.resolve<SeasonalityResponse | null>(null);
      Promise.all([basePromise, medianPromise])
        .then(([base, median]) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          setHistBase(base);
          setHistMedian(median);
          setStatus((base?.bars?.length ?? 0) > 0 ? 'ok' : 'empty');
        })
        .catch((err) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          console.error('embed/seasonality histogram load failed:', err);
          setStatus('error');
        });
    } else {
      const basePromise = getSeasonalityYearly(stock, effExcludeDividends);
      const medianPromise = showNoOutliers
        ? getSeasonalityYearly(stock, effExcludeDividends, { aggType: 'median' })
        : Promise.resolve<YearlySeasonalityResponse | null>(null);
      Promise.all([basePromise, medianPromise])
        .then(([base, median]) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          setYearBase(base);
          setYearMedian(median);
          setStatus((base?.average?.length ?? 0) > 0 ? 'ok' : 'empty');
        })
        .catch((err) => {
          if (cancelled || reqId !== reqIdRef.current) return;
          console.error('embed/seasonality yearly load failed:', err);
          setStatus('error');
        });
    }
    return () => { cancelled = true; };
  }, [stock, chartType, mode, effExcludeDividends, showNoOutliers]);

  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();

  // Высоту не считаем: LwChartPanes всегда занимает 100% родителя, а родитель —
  // <div position:absolute;inset:0>. Прежний ResizeObserver существовал только
  // ради пропа height у LwChart.
  const boxRef = useRef<HTMLDivElement>(null);

  const isHist = chartType === 'histogram';
  const bars = histBase?.bars ?? [];

  // ── Календарь: категориальная гистограмма ──
  const histSeries = useMemo<LwSeries[]>(() => {
    if (!isHist || bars.length === 0) return [];
    const maxAbs = Math.max(...bars.map((b) => Math.abs(b.avg_change)), 0.01);
    const digits = pctDigits(maxAbs);
    const out: LwSeries[] = [{
      id: 'season', type: 'histogram', scale: 'right', base: 0,
      color: 'var(--oi-green)', label: 'Ср. изменение',
      // Ось категориальная (срез, не время) — «последний бар» ничего не значит,
      // пилюля last-value на оси только путает (не зависит от тумблера песочницы).
      lastValueVisible: false,
      data: bars.map((b, i) => ({
        time: T0 + i * DAY,
        value: b.avg_change,
        color: b.avg_change >= 0 ? 'var(--oi-green)' : 'var(--oi-red)',
      })),
      axisFmt: (v) => fmtPct(v, digits, false),
      tipFmt: (v) => fmtPct(v, Math.max(digits, 1)),
      minMove: 0.01,
    }];
    const med = histMedian?.bars;
    if (showNoOutliers && med?.length === bars.length) {
      out.push({
        id: 'median', type: 'line', scale: 'right', color: 'var(--chart-line-1)', lineWidth: 2,
        label: 'Без выбросов', lastValueVisible: false,
        data: med.map((b, i) => ({ time: T0 + i * DAY, value: b.avg_change })),
        axisFmt: (v) => fmtPct(v, digits, false),
        tipFmt: (v) => fmtPct(v, Math.max(digits, 1)),
        minMove: 0.01,
      });
    }
    return out;
  }, [isHist, bars, histMedian, showNoOutliers]);

  // tickFmt категориальной оси: синтетическое время → индекс бара → label среза.
  const histTickFmt = useMemo(() => {
    if (!isHist) return undefined;
    const labels = bars.map((b) => b.label);
    return (time: number) => {
      const idx = Math.round((time - T0) / DAY);
      return labels[idx] ?? '';
    };
  }, [isHist, bars]);

  // ── Годовая: кумулятивные линии на синтетическом годе ──
  const yearlySeries = useMemo<LwSeries[]>(() => {
    if (isHist || !yearBase?.average?.length) return [];
    const maxTD = Math.max(1, yearBase.max_trading_days - 1);
    // td → день синтетического года (растяжка на 364 дня, чтобы ось = янв..дек).
    const tOf = (td: number) => T0 + Math.round((td / maxTD) * 364) * DAY;
    const tip = (v: number) => fmtPct(v, 1);
    const axis = (v: number) => fmtPct(v, 0, false);
    const out: LwSeries[] = [{
      id: 'avg', type: 'line', scale: 'right', color: 'var(--chart-line-1)', lineWidth: 2,
      label: 'Средний год', zeroLine: true,
      data: yearBase.average.map((p) => ({ time: tOf(p.td), value: p.avg_pct })),
      tipFmt: tip, axisFmt: axis, minMove: 0.01,
    }];
    if (showCurrentYear && yearBase.current?.length) {
      out.push({
        id: 'cur', type: 'line', scale: 'right', color: 'var(--accent)', lineWidth: 2,
        label: String(yearBase.current_year),
        data: yearBase.current.map((p) => ({ time: tOf(p.td), value: p.pct })),
        tipFmt: tip, axisFmt: axis, minMove: 0.01,
      });
    }
    if (showNoOutliers && yearMedian?.average?.length) {
      out.push({
        id: 'median', type: 'line', scale: 'right', color: 'var(--chart-line-3)', lineWidth: 2,
        label: 'Без выбросов', lastValueVisible: false,
        data: yearMedian.average.map((p) => ({ time: tOf(p.td), value: p.avg_pct })),
        tipFmt: tip, axisFmt: axis, minMove: 0.01,
      });
    }
    return out;
  }, [isHist, yearBase, yearMedian, showNoOutliers, showCurrentYear]);

  // Ось годовой: только месяцы (год синтетический — прячем «2001»).
  const yearlyTickFmt = useMemo(() => {
    if (isHist) return undefined;
    return (time: number, type: number) => {
      if (type > 1) return '';
      const d = new Date(time * 1000);
      return type === 0 ? '' : MONTHS_RU[d.getUTCMonth()];
    };
  }, [isHist]);

  // Лейбл нативного кроссхэйра (полоска у оси времени под курсором): время у нас
  // синтетическое (T0 + индекс), родной форматтер lightweight-charts показал бы
  // случайную «реальную» дату 2001 года — вместо этого подставляем ту же
  // категориальную подпись, что и на оси (Календарь), либо день+месяц без
  // фиктивного года (Годовая).
  const crosshairTimeFmt = useMemo(() => {
    if (isHist) {
      const labels = bars.map((b) => b.label);
      return (time: number) => {
        const idx = Math.round((time - T0) / DAY);
        return labels[idx] ?? '';
      };
    }
    return (time: number) => {
      const d = new Date(time * 1000);
      return `${d.getUTCDate()} ${MONTHS_RU[d.getUTCMonth()]}`;
    };
  }, [isHist, bars]);

  const lwSeries = isHist ? histSeries : yearlySeries;

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
          </div>
          <PillGroup<ChartType> value={chartType} options={CHART_TYPES} onChange={setChartType} compact={toolbarCompact} />
          {isHist && <Dropdown<SeasonalityMode> value={mode} options={modeOptions} onChange={setMode} title="Разрез" icon={MODE_ICONS[mode]} compact={toolbarCompact} />}
        </div>
      }
      more={
        <>
          {hasDividends && (
            <DrawerSection label="Дивиденды">
              <ToggleRow label="Без дивидендных гэпов" checked={excludeDividends} onChange={setExcludeDividends} />
            </DrawerSection>
          )}
          <DrawerSection label="Серии">
            <ToggleRow label="Без выбросов" checked={showNoOutliers} onChange={setShowNoOutliers} hint="Вторая серия — медиана" />
          </DrawerSection>
          {!isHist && (
            <DrawerSection label="Текущий год">
              <ToggleRow label="Линия текущего года" checked={showCurrentYear} onChange={setShowCurrentYear} />
            </DrawerSection>
          )}
        </>
      }
    >
      <div ref={boxRef} style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}>
        {status === 'ok' && lwSeries.length > 0 && (
          <LwChartPanes
            panes={[{ series: lwSeries }]}
            // Статичный вид (как у потоков капитала): пан и зум выключены,
            // график всегда показывает всю историю и подстраивается под панель.
            // Здесь смысл в картине целиком, а не в разглядывании участка.
            staticView
            dark={dark}
            fitKey={`${chartType}|${stock}|${mode}|${showNoOutliers}|${effExcludeDividends}|${showCurrentYear}`}
            tickFmt={isHist ? histTickFmt : yearlyTickFmt}
            crosshairTimeFmt={crosshairTimeFmt}
            legendItems={isHist
              ? [
                  { label: 'Рост', color: 'var(--oi-green)' },
                  { label: 'Падение', color: 'var(--oi-red)' },
                  ...(showNoOutliers && histMedian ? [{ label: 'Без выбросов', color: 'var(--chart-line-1)' }] : []),
                ]
              : undefined}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text={stock ? 'Нет данных' : 'Акция не выбрана'} />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedFrame>
  );
}
