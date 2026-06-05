/**
 * EmbedOpenInterest — самодостаточный виджет ОИ для iframe.
 *
 * Контролы (актив-фьючерс / таймфрейм / период / группа) — в drawer'е настроек
 * (шестерёнка в заголовке панели). Шапка виджета — только название + «Открытый
 * интерес», чтобы график получал максимум места в узкой панели.
 *
 * ВАЖНО: ОИ живёт на ФЬЮЧЕРСАХ — instrument это код фьючерса (SR), не акции (SBER).
 * Состояние шарится по ключам frame:embed:oi:* (в extension-iframe storage
 * партиционирован → там состояние своё).
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import { getChartData, getInstrument } from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { formatNumber, formatPrice } from '../../utils/formatNumber';
import { EmbedMsg } from './embedUi';
import {
  useEmbedSettings,
  EmbedShell,
  DrawerSection,
  SegGroup,
  AssetPickerInline,
} from './EmbedSettings';

type ChartData = Awaited<ReturnType<typeof getChartData>>;
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type ClGroup = 'FIZ' | 'YUR';
type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '5y' | 'all';

const TF_OPTS: { id: number; label: string }[] = [
  { id: 5, label: '5 мин' },
  { id: 60, label: '1 час' },
  { id: 24, label: '1 день' },
];

const P_LABEL: Record<Period, string> = {
  '1d': '1Д', '1w': '1Н', '1m': '1М', '3m': '3М', '6m': '6М',
  '1y': '1Г', '2y': '2Г', '5y': '5Л', 'all': 'Всё',
};

// Допустимые периоды для интервала (зеркалит OpenInterestPage: 5мин→макс 1М,
// 1час→макс 6М, 1день→всё; для дневного 1Д не имеет смысла).
const ALLOWED: Record<number, Period[]> = {
  5: ['1d', '1w', '1m'],
  60: ['1d', '1w', '1m', '3m', '6m'],
  24: ['1w', '1m', '3m', '6m', '1y', '2y', '5y', 'all'],
};

function periodOpts(interval: number) {
  return (ALLOWED[interval] || ALLOWED[24]).map((id) => ({ id, label: P_LABEL[id] }));
}

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

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

  const [data, setData] = useState<ChartData | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Persist выбор.
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:instrument', instrument); } catch { /* quota */ } }, [instrument]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:clgroup', clgroup); } catch { /* quota */ } }, [clgroup]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:interval', String(interval)); } catch { /* quota */ } }, [interval]);
  useEffect(() => { try { localStorage.setItem('frame:embed:oi:period', period); } catch { /* quota */ } }, [period]);

  // При смене таймфрейма скорректировать период, если он стал недоступен.
  const changeInterval = (next: number) => {
    setIntervalValue(next);
    const allowed = ALLOWED[next] || ALLOWED[24];
    if (!allowed.includes(period)) setPeriod(allowed[allowed.length - 1]);
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

  // Загрузка данных графика.
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

  const chartData = useMemo(
    () => (data?.candles ?? []).map((c: { time: string; close: number }) => ({ time: c.time, value: c.close })),
    [data],
  );
  const oiData = useMemo(
    () => (data?.open_interest ?? [])
      .filter((o: { net_position: number | null }) => o.net_position !== null)
      .map((o: { time: string; net_position: number | null }) => ({ time: o.time, value: o.net_position as number })),
    [data],
  );

  const displayName = instrumentName || displayTicker(instrument);

  return (
    <EmbedShell
      settings={settings}
      title={displayName}
      subtitle="Открытый интерес"
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
            <SegGroup value={period} options={periodOpts(interval)} onChange={(v) => setPeriod(v)} />
          </DrawerSection>
          <DrawerSection label="Группа участников">
            <SegGroup
              value={clgroup}
              options={[{ id: 'FIZ', label: 'Физлица' }, { id: 'YUR', label: 'Юрлица' }]}
              onChange={(v) => setClgroup(v)}
            />
          </DrawerSection>
        </>
      }
    >
      <div ref={chartBoxRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {status === 'ok' && data && (
          <SimpleChart
            data={chartData}
            secondaryData={oiData}
            showPrimary
            showSecondary
            primaryLabel={displayName}
            secondaryLabel="Чистая позиция"
            primaryColor="var(--chart-line-1)"
            secondaryColor="var(--accent)"
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
          <EmbedMsg text={instrument ? 'Нет данных ОИ по этому инструменту' : 'Инструмент не выбран'} />
        )}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedShell>
  );
}
