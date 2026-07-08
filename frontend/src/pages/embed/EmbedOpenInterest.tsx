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
import LwChart, { type LwSeries, type LwMarker } from '../../components/LwChart';
import { useTheme } from '../../contexts/ThemeContext';
import { getChartData, getInstrument } from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { formatNumber, formatPrice } from '../../utils/formatNumber';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup, ToggleRow } from './EmbedSettings';
import { EmbedFrame, AssetButton, PillGroup, Dropdown, WheelHint } from './EmbedToolbar';
import { readLS, writeLS } from './embedPersist';

// Компактные лейблы таймфрейма для инлайн-пилюль тулбара.
const TF_COMPACT: { id: number; label: string }[] = [
  { id: 5, label: '5м' },
  { id: 60, label: '1ч' },
  { id: 24, label: '1д' },
];

type ChartData = Awaited<ReturnType<typeof getChartData>>;
type OiPoint = ChartData['open_interest'][number];
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type ClGroup = 'FIZ' | 'YUR';
// Режим/вариант ОИ — зеркалит OpenInterestPage, но без 'price' (embed всегда
// показывает серию ОИ; price-only режим заменён тумблером «Цена»).
type DisplayMode = 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';

type Series = { time: string; value: number }[];

// Единый монолитный график: грузим МАКС историю (дневной — всю; интрадей — месяц),
// а по времени юзер зумит колесом (осевой зум SimpleChart). Дискретных периодов нет.
const loadPeriodFor = (interval: number): string => (interval === 24 ? 'all' : '1m');
// Время → UNIX-секунды для LwChart. Дневной ТФ: UTC-полночь по дате (чтобы не было
// сдвига даты из-за таймзоны); интрадей — полный timestamp.
const toSec = (t: string, intraday: boolean): number => {
  if (!intraday) {
    const [y, m, d] = t.slice(0, 10).split('-').map(Number);
    return Math.floor(Date.UTC(y, m - 1, d) / 1000);
  }
  return Math.floor(new Date(t).getTime() / 1000);
};

// Цвета ОИ — все через CSS-var (адаптируются к теме внутри iframe).
const OI_COLORS = {
  primary: 'var(--chart-line-1)',
  amber: 'var(--oi-amber)',
  green: 'var(--oi-green)',
  red: 'var(--oi-red)',
  cyan: 'var(--oi-cyan)',
};

const num = (v: number | null): number => v ?? 0;

export default function EmbedOpenInterest() {
  const [params] = useSearchParams();
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const [instrument, setInstrument] = useState<string>(() =>
    params.get('instrument') || readLS('frame:embed:oi:instrument', 'SR'),
  );
  const [instrumentName, setInstrumentName] = useState<string>(params.get('name') || '');
  const [clgroup, setClgroup] = useState<ClGroup>(() => readLS('frame:embed:oi:clgroup', 'FIZ') as ClGroup);
  const [interval, setIntervalValue] = useState<number>(() => Number(readLS('frame:embed:oi:interval', '24')) || 24);
  const [displayMode, setDisplayMode] = useState<DisplayMode>(() => readLS('frame:embed:oi:displayMode', 'positions') as DisplayMode);
  const [oiVariant, setOiVariant] = useState<OIVariant>(() => readLS('frame:embed:oi:oiVariant', 'net') as OIVariant);
  const [showPrice, setShowPrice] = useState<boolean>(() => readLS('frame:embed:oi:showPrice', 'true') === 'true');
  const [showExpirations, setShowExpirations] = useState<boolean>(() => readLS('frame:embed:oi:showExpirations', 'false') === 'true');

  const [data, setData] = useState<ChartData | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Persist выбор.
  useEffect(() => { writeLS('frame:embed:oi:instrument', instrument); }, [instrument]);
  useEffect(() => { writeLS('frame:embed:oi:clgroup', clgroup); }, [clgroup]);
  useEffect(() => { writeLS('frame:embed:oi:interval', String(interval)); }, [interval]);
  useEffect(() => { writeLS('frame:embed:oi:displayMode', displayMode); }, [displayMode]);
  useEffect(() => { writeLS('frame:embed:oi:oiVariant', oiVariant); }, [oiVariant]);
  useEffect(() => { writeLS('frame:embed:oi:showPrice', String(showPrice)); }, [showPrice]);
  useEffect(() => { writeLS('frame:embed:oi:showExpirations', String(showExpirations)); }, [showExpirations]);

  const changeInterval = (next: number) => setIntervalValue(next);

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
    getChartData(instrument, instrument, 'futures', interval, clgroup, true, loadPeriodFor(interval))
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
  }, [instrument, clgroup, interval]);

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
      case 'oi': return { secondary: 'Открытый интерес', third: '' };
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
      { id: 'oi' as OIVariant, label: 'Открытый интерес' },
      { id: 'long' as OIVariant, label: isPositions ? 'Покупки' : 'Покупатели' },
      { id: 'short' as OIVariant, label: isPositions ? 'Продажи' : 'Продавцы' },
      { id: 'both' as OIVariant, label: isPositions ? 'Покупки + Продажи' : 'Покупатели + Продавцы' },
      { id: 'net' as OIVariant, label: 'Чистая позиция' },
    ];
  }, [displayMode]);

  // Метки экспираций (смена контракта) — маркеры LwChart на серии.
  const lwMarkers = useMemo<LwMarker[]>(() => {
    if (!showExpirations) return [];
    const switches = data?.contract_switches;
    if (!switches || switches.length <= 1) return [];
    const intraday = interval !== 24;
    return switches.slice(1).map((sw) => ({ time: toSec(sw.date, intraday), text: sw.to, color: '#9CA3B8', position: 'aboveBar' as const }));
  }, [data, showExpirations, interval]);

  const displayName = instrumentName || displayTicker(instrument);

  // Серии для LwChart: цена (линия, левая ось) + показатель ОИ (area/линии, правая ось).
  const lwSeries = useMemo<LwSeries[]>(() => {
    const intraday = interval !== 24;
    const out: LwSeries[] = [];
    if (showPrice && chartData.length > 0) {
      out.push({
        id: 'price', type: 'line', scale: 'left', color: OI_COLORS.primary, lineWidth: 2, label: displayName,
        data: chartData.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
        tipFmt: (v) => formatPrice(v), axisFmt: (v) => formatPrice(v),
      });
    }
    if (oiSeries.secondary && oiSeries.secondary.length > 0) {
      if (oiVariant === 'both') {
        out.push({
          id: 'oi-long', type: 'line', scale: 'right', color: colors.secondary, lineWidth: 2, label: labels.secondary,
          data: oiSeries.secondary.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
          tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
        });
        if (oiSeries.third) {
          out.push({
            id: 'oi-short', type: 'line', scale: 'right', color: colors.third, lineWidth: 2, label: labels.third,
            data: oiSeries.third.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
            tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
          });
        }
      } else {
        // Линия, а не area: Вадим — «должны быть просто две линии». Для net (пересекает
        // ноль) area заливалась от базы и выглядела сломанной → линия + пунктир zeroLine.
        out.push({
          id: 'oi', type: 'line', scale: 'right', color: colors.secondary, lineWidth: 2, label: labels.secondary,
          zeroLine: oiVariant === 'net',
          data: oiSeries.secondary.map((p) => ({ time: toSec(p.time, intraday), value: p.value })),
          tipFmt: (v) => formatNumber(v, 0), axisFmt: (v) => formatNumber(v, 0),
        });
      }
    }
    return out;
  }, [chartData, oiSeries, oiVariant, colors, labels, showPrice, displayName, interval]);

  return (
    <EmbedFrame
      lead={
        <AssetButton
          ticker={displayTicker(instrument)}
          filterType="futures"
          hideLowActivity
          current={instrument}
          onSelect={(secid, name) => { setInstrument(secid); setInstrumentName(name); }}
        />
      }
      toolbar={
        <>
          <PillGroup value={interval} options={TF_COMPACT} onChange={changeInterval} />
          <Dropdown value={oiVariant} options={variantOpts} onChange={setOiVariant} title="Показатель ОИ" />
        </>
      }
      more={
        <>
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
          <DrawerSection label="Слои">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              <ToggleRow label="Цена" checked={showPrice} onChange={setShowPrice} hint="Линия цены фьючерса" />
              <ToggleRow label="Экспирации" checked={showExpirations} onChange={setShowExpirations} hint="Метки смены контракта" />
            </div>
          </DrawerSection>
          <WheelHint>
            Колесо над графиком — <b>зум времени</b>; зажать и тащить — панорама;
            <b> Shift+колесо</b> или колесо над осью цифр — вертикальный масштаб. Наведи — тултип со значениями.
          </WheelHint>
        </>
      }
    >
      <div ref={chartBoxRef} style={{ position: 'absolute', inset: 0 }}>
        {status === 'ok' && data && lwSeries.length > 0 && (
          <LwChart
            series={lwSeries}
            markers={lwMarkers}
            height={chartH}
            dark={dark}
            fitKey={`${instrument}|${interval}`}
            initialBars={interval === 24 ? 252 : 220}
          />
        )}
        {/* Цена выключена + у контракта нет OI-данных → серий нет. Без этого был
            пустой холст без объяснения (аудит). */}
        {status === 'ok' && data && lwSeries.length === 0 && (
          <EmbedMsg text="Нет данных для отображения" />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && (
          <EmbedMsg text={instrument ? 'Нет данных по этому инструменту' : 'Инструмент не выбран'} />
        )}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedFrame>
  );
}
