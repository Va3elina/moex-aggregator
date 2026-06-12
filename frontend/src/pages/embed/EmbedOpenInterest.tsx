/**
 * EmbedOpenInterest — самодостаточный виджет ОИ для iframe.
 *
 * Контролы (актив-фьючерс / таймфрейм / период / группа / режим / вариант ОИ /
 * цена / экспирации) — в drawer'е настроек (шестерёнка в заголовке панели).
 * Шапка виджета — только название + «Открытые позиции», чтобы график получал
 * максимум места в узкой панели.
 *
 * Это chromeless full-PRO зеркало OpenInterestPage: серии ОИ
 * (displayMode × oiVariant), alignToCandles, цвета/лейблы и аннотации экспираций
 * портированы VERBATIM по семантике со страницы.
 *
 * ВАЖНО: ОИ живёт на ФЬЮЧЕРСАХ — instrument это код фьючерса (SR), не акции (SBER).
 * Состояние шарится по ключам frame:embed:oi:* (в extension-iframe storage
 * партиционирован → там состояние своё).
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart, { type ChartAnnotation } from '../../components/SimpleChart';
import { getChartData, getInstrument } from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { formatNumber, formatPrice } from '../../utils/formatNumber';
import { EmbedMsg } from './embedUi';
import {
  useEmbedSettings,
  EmbedShell,
  DrawerSection,
  SegGroup,
  ToggleRow,
  AssetPickerInline,
} from './EmbedSettings';

type ChartData = Awaited<ReturnType<typeof getChartData>>;
type OiPoint = ChartData['open_interest'][number];
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type ClGroup = 'FIZ' | 'YUR';
type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '5y' | 'all';
// Режим/вариант ОИ — зеркалит OpenInterestPage, но без 'price' (embed всегда
// показывает серию ОИ; price-only режим заменён тумблером «Цена»).
type DisplayMode = 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';

type Series = { time: string; value: number }[];

const TF_OPTS: { id: number; label: string }[] = [
  { id: 5, label: '5 мин' },
  { id: 60, label: '1 час' },
  { id: 24, label: '1 день' },
];

const P_LABEL: Record<Period, string> = {
  '1d': '1Д', '1w': '1Н', '1m': '1М', '3m': '3М', '6m': '6М',
  '1y': '1Г', '2y': '2Г', '5y': '5Л', 'all': 'Всё',
};

// Допустимые периоды для интервала (зеркалит OpenInterestPage
// MAX_PERIODS_BY_INTERVAL: 5мин→макс 1М, 1час→макс 6М, 1день→всё включая 1Д).
const ALLOWED: Record<number, Period[]> = {
  5: ['1d', '1w', '1m'],
  60: ['1d', '1w', '1m', '3m', '6m'],
  24: ['1d', '1w', '1m', '3m', '6m', '1y', '2y', '5y', 'all'],
};

// Цвета ОИ — все через CSS-var (адаптируются к теме внутри iframe).
const OI_COLORS = {
  primary: 'var(--chart-line-1)',
  amber: 'var(--oi-amber)',
  green: 'var(--oi-green)',
  red: 'var(--oi-red)',
  cyan: 'var(--oi-cyan)',
};

function periodOpts(interval: number) {
  return (ALLOWED[interval] || ALLOWED[24]).map((id) => ({ id, label: P_LABEL[id] }));
}

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

const num = (v: number | null): number => v ?? 0;

export default function EmbedOpenInterest() {
  const [params] = useSearchParams();
  const settings = useEmbedSettings();

  const [instrument, setInstrument] = useState<string>(() =>
    params.get('instrument') || readLS('frame:embed:oi:instrument', 'SR'),
  );
  const [instrumentName, setInstrumentName] = useState<string>(params.get('name') || '');
  const [clgroup, setClgroup] = useState<ClGroup>(() => readLS('frame:embed:oi:clgroup', 'YUR') as ClGroup);
  const [interval, setIntervalValue] = useState<number>(() => Number(readLS('frame:embed:oi:interval', '24')) || 24);
  const [period, setPeriod] = useState<Period>(() => (params.get('period') || readLS('frame:embed:oi:period', '6m')) as Period);
  const [displayMode, setDisplayMode] = useState<DisplayMode>(() => readLS('frame:embed:oi:displayMode', 'positions') as DisplayMode);
  const [oiVariant, setOiVariant] = useState<OIVariant>(() => readLS('frame:embed:oi:oiVariant', 'net') as OIVariant);
  const [showPrice, setShowPrice] = useState<boolean>(() => readLS('frame:embed:oi:showPrice', 'true') === 'true');
  const [showExpirations, setShowExpirations] = useState<boolean>(() => readLS('frame:embed:oi:showExpirations', 'false') === 'true');

  const [data, setData] = useState<ChartData | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Persist выбор.
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:instrument', instrument); } catch { /* quota */ } }, [instrument]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:clgroup', clgroup); } catch { /* quota */ } }, [clgroup]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:interval', String(interval)); } catch { /* quota */ } }, [interval]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:period', period); } catch { /* quota */ } }, [period]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:displayMode', displayMode); } catch { /* quota */ } }, [displayMode]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:oiVariant', oiVariant); } catch { /* quota */ } }, [oiVariant]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:showPrice', String(showPrice)); } catch { /* quota */ } }, [showPrice]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:showExpirations', String(showExpirations)); } catch { /* quota */ } }, [showExpirations]);

  // При смене таймфрейма скорректировать период, если он стал недоступен.
  const changeInterval = (next: number) => {
    setIntervalValue(next);
    const allowed = ALLOWED[next] || ALLOWED[24];
    if (!allowed.includes(period)) setPeriod(allowed[allowed.length - 1]);
  };

  // Смена периода: 1Д на дневном ТФ не имеет смысла → бампим интервал до 60
  // (зеркалит OpenInterestPage onChange периода).
  const changePeriod = (next: Period) => {
    if (next === '1d' && interval === 24) setIntervalValue(60);
    setPeriod(next);
  };

  // Резолв имени, если пришёл только sec_id.
  useEffect(() => {
    if (instrumentName) return;
    let cancelled = false;
    getInstrument(instrument)
      .then((inst) => { if (!cancelled && inst?.name) setInstrumentName(inst.name); })
      .catch(() => { /* имя не критично */ });
    return () => { cancelled = true; };
  }, [instrument, instrumentName]);

  // Загрузка данных графика. show_oi=true всегда (в embed всегда есть серия ОИ).
  useEffect(() => {
    if (!instrument) { setStatus('empty'); return; }
    let cancelled = false;
    setStatus('loading');
    getChartData(instrument, instrument, 'futures', interval, clgroup, true, period)
      .then((res) => {
        if (cancelled) return;
        const hasData = (res?.candles?.length ?? 0) > 0;
        setData(res);
        setStatus(hasData ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/oi load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [instrument, clgroup, interval, period]);

  // Резиновая высота графика.
  const chartBoxRef = useRef<HTMLDivElement>(null);
  const [chartH, setChartH] = useState(280);
  useEffect(() => {
    const el = chartBoxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setChartH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const chartData = useMemo<Series>(
    () => (data?.candles ?? []).map((c) => ({ time: c.time, value: c.close })),
    [data],
  );

  // Выравнивание OI данных по временным меткам свечей (порт alignToCandles из
  // OpenInterestPage). OI имеет меньше точек, чем свечи → index-based X-mapping
  // в SimpleChart сдвигал бы серию во времени. Для каждой свечи берём последнее
  // известное значение OI (forward-fill). Дневной ключ — по дате (свечи
  // T00:00:00, OI T23:50:00); интрадей — по полному timestamp.
  const oiSeries = useMemo(() => {
    if (!data?.open_interest) {
      return { secondary: undefined as Series | undefined, third: undefined as Series | undefined };
    }
    const isPositions = displayMode === 'positions';
    const oi = data.open_interest;

    let secondary: Series | undefined;
    let third: Series | undefined;

    switch (oiVariant) {
      case 'oi':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions
            ? num(o.pos_long) + Math.abs(num(o.pos_short))
            : num(o.pos_long_num) + num(o.pos_short_num),
        }));
        break;
      case 'long':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? num(o.pos_long) : num(o.pos_long_num),
        }));
        break;
      case 'short':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? Math.abs(num(o.pos_short)) : num(o.pos_short_num),
        }));
        break;
      case 'both':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? num(o.pos_long) : num(o.pos_long_num),
        }));
        third = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions ? Math.abs(num(o.pos_short)) : num(o.pos_short_num),
        }));
        break;
      case 'net':
        secondary = oi.map((o: OiPoint) => ({
          time: o.time,
          value: isPositions
            ? (o.net_position ?? (num(o.pos_long) + num(o.pos_short)))
            : num(o.pos_long_num) - num(o.pos_short_num),
        }));
        break;
    }

    const isIntraday = interval !== 24;
    const align = (series: Series | undefined): Series | undefined => {
      if (!series || series.length === 0 || chartData.length === 0) return series;
      const map = new Map<string, number>();
      for (const p of series) {
        const key = isIntraday ? p.time : p.time.slice(0, 10);
        map.set(key, p.value);
      }
      const aligned: Series = [];
      let last: number | null = null;
      for (const candle of chartData) {
        const key = isIntraday ? candle.time : candle.time.slice(0, 10);
        const val = map.get(key);
        if (val !== undefined) last = val;
        if (last !== null) aligned.push({ time: candle.time, value: last });
      }
      return aligned.length > 0 ? aligned : series;
    };

    return { secondary: align(secondary), third: align(third) };
  }, [data, displayMode, oiVariant, interval, chartData]);

  // Цвета серии ОИ (зеркалит getColors). oi=amber, long=green, short=red,
  // net=cyan, both=green/red.
  const colors = useMemo(() => {
    switch (oiVariant) {
      case 'oi': return { secondary: OI_COLORS.amber, third: '' };
      case 'long': return { secondary: OI_COLORS.green, third: '' };
      case 'short': return { secondary: OI_COLORS.red, third: '' };
      case 'both': return { secondary: OI_COLORS.green, third: OI_COLORS.red };
      case 'net': return { secondary: OI_COLORS.cyan, third: '' };
      default: return { secondary: OI_COLORS.amber, third: '' };
    }
  }, [oiVariant]);

  // Лейблы серии ОИ (зеркалит getLabels — зависят от displayMode).
  const labels = useMemo(() => {
    const isPositions = displayMode === 'positions';
    switch (oiVariant) {
      case 'oi': return { secondary: 'Открытые позиции', third: '' };
      case 'long': return { secondary: isPositions ? 'Покупки' : 'Покупатели', third: '' };
      case 'short': return { secondary: isPositions ? 'Продажи' : 'Продавцы', third: '' };
      case 'both': return {
        secondary: isPositions ? 'Покупки' : 'Покупатели',
        third: isPositions ? 'Продажи' : 'Продавцы',
      };
      case 'net': return { secondary: 'Чистая позиция', third: '' };
      default: return { secondary: '', third: '' };
    }
  }, [displayMode, oiVariant]);

  // Лейблы вариантов для drawer-сегментов (зависят от displayMode — выводим,
  // а не хардкодим, чтобы совпадало с labels выше).
  const variantOpts = useMemo(() => {
    const isPositions = displayMode === 'positions';
    return [
      { id: 'oi' as OIVariant, label: 'Открытые позиции' },
      { id: 'long' as OIVariant, label: isPositions ? 'Покупки' : 'Покупатели' },
      { id: 'short' as OIVariant, label: isPositions ? 'Продажи' : 'Продавцы' },
      { id: 'both' as OIVariant, label: isPositions ? 'Покупки + Продажи' : 'Покупатели + Продавцы' },
      { id: 'net' as OIVariant, label: 'Чистая позиция' },
    ];
  }, [displayMode]);

  // Аннотации экспираций (порт со страницы): из contract_switches.slice(1).
  const annotations = useMemo<ChartAnnotation[] | undefined>(() => {
    if (!showExpirations) return undefined;
    const switches = data?.contract_switches;
    if (!switches || switches.length <= 1) return undefined;
    return switches.slice(1).map((sw): ChartAnnotation => ({
      time: sw.date,
      label: sw.to,
      description: `${sw.from} → ${sw.to}`,
      color: '#3a3f4f',
      textColor: '#9CA3B8',
    }));
  }, [data, showExpirations]);

  const displayName = instrumentName || displayTicker(instrument);

  return (
    <EmbedShell
      settings={settings}
      title={displayName}
      subtitle="Открытые позиции"
      drawer={
        <>
          <DrawerSection label="Актив (фьючерс)">
            <AssetPickerInline
              filterType="futures"
              current={instrument}
              active={settings.open}
              onSelect={(secid, name) => { setInstrument(secid); setInstrumentName(name); }}
            />
          </DrawerSection>
          <DrawerSection label="Таймфрейм">
            <SegGroup value={interval} options={TF_OPTS} onChange={changeInterval} />
          </DrawerSection>
          <DrawerSection label="Период">
            <SegGroup value={period} options={periodOpts(interval)} onChange={changePeriod} />
          </DrawerSection>
          <DrawerSection label="Группа участников">
            <SegGroup
              value={clgroup}
              options={[{ id: 'FIZ', label: 'Физлица' }, { id: 'YUR', label: 'Юрлица' }]}
              onChange={(v) => setClgroup(v)}
            />
          </DrawerSection>
          <DrawerSection label="Режим">
            <SegGroup<DisplayMode>
              value={displayMode}
              options={[{ id: 'positions', label: 'Объём позиций' }, { id: 'participants', label: 'Число трейдеров' }]}
              onChange={setDisplayMode}
            />
          </DrawerSection>
          <DrawerSection label="Показатель">
            <SegGroup<OIVariant> value={oiVariant} options={variantOpts} onChange={setOiVariant} />
          </DrawerSection>
          <DrawerSection label="Слои">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <ToggleRow label="Цена" checked={showPrice} onChange={setShowPrice} hint="Линия цены фьючерса" />
              <ToggleRow label="Экспирации" checked={showExpirations} onChange={setShowExpirations} hint="Метки смены контракта" />
            </div>
          </DrawerSection>
        </>
      }
    >
      <div ref={chartBoxRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {status === 'ok' && data && (
          <SimpleChart
            data={chartData}
            secondaryData={oiSeries.secondary}
            thirdData={oiSeries.third}
            showPrimary={showPrice}
            showSecondary={!!oiSeries.secondary}
            showThird={oiVariant === 'both' && !!oiSeries.third}
            primaryLabel={displayName}
            secondaryLabel={labels.secondary}
            thirdLabel={labels.third}
            primaryColor={OI_COLORS.primary}
            secondaryColor={colors.secondary}
            thirdColor={colors.third}
            annotations={annotations}
            height={chartH}
            showDownloadButton={false}
            showNavigator={false}
            showValueHeader={false}
            legendPosition="top"
            formatValue={formatPrice}
            formatSecondaryValue={(v) => formatNumber(v, 0)}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && (
          <EmbedMsg text={instrument ? 'Нет данных по этому инструменту' : 'Инструмент не выбран'} />
        )}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedShell>
  );
}
