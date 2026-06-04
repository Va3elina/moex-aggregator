/**
 * EmbedOpenInterest — самодостаточный виджет ОИ для iframe.
 *
 * v1 БЕЗ синка с терминалом: инструмент выбирается ВНУТРИ виджета через
 * InstrumentSearchModal (фьючерсы). Состояние шарится с сайтом по ключам
 * frame:oi:* (в extension-iframe storage партиционирован — там состояние своё).
 *
 * Параметры (query, опц.): instrument (стартовый sec_id фьючерса), period, name.
 *
 * ВАЖНО: ОИ живёт на ФЬЮЧЕРСАХ — instrument это код фьючерса (SR), не акции (SBER).
 *
 * Размер резиновый (ResizeObserver) — перерисовка при ресайзе плавающего окна.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import InstrumentSearchModal from '../../components/InstrumentSearchModal';
import { getChartData, getInstrument } from '../../services/api';
import { displayTicker } from '../../utils/displayTicker';
import { formatNumber, formatPrice } from '../../utils/formatNumber';
import { EmbedMsg, embedColumn, embedHeader, pickerBtn, segBtn } from './embedUi';

type ChartData = Awaited<ReturnType<typeof getChartData>>;
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type ClGroup = 'FIZ' | 'YUR';

function readInitialInstrument(param: string | null): string {
  if (param) return param;
  try {
    return localStorage.getItem('frame:embed:oi:instrument') || 'SR';
  } catch {
    return 'SR';
  }
}

function readInitialClgroup(): ClGroup {
  try {
    return (localStorage.getItem('frame:embed:oi:clgroup') as ClGroup) || 'YUR';
  } catch {
    return 'YUR';
  }
}

export default function EmbedOpenInterest() {
  const [params] = useSearchParams();
  const period = params.get('period') || '6m';

  const [instrument, setInstrument] = useState<string>(() =>
    readInitialInstrument(params.get('instrument')),
  );
  const [instrumentName, setInstrumentName] = useState<string>(params.get('name') || '');
  const [clgroup, setClgroup] = useState<ClGroup>(readInitialClgroup);
  const [pickerOpen, setPickerOpen] = useState(false);

  const [data, setData] = useState<ChartData | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  // Persist выбор (зеркалит сайт вне extension).
  useEffect(() => {
    try {
      localStorage.setItem('frame:embed:oi:instrument', instrument);
    } catch {
      /* quota / private */
    }
  }, [instrument]);
  useEffect(() => {
    try {
      localStorage.setItem('frame:embed:oi:clgroup', clgroup);
    } catch {
      /* quota / private */
    }
  }, [clgroup]);

  // Резолв имени, если пришёл только sec_id.
  useEffect(() => {
    if (instrumentName) return;
    let cancelled = false;
    getInstrument(instrument)
      .then((inst) => {
        if (!cancelled && inst?.name) setInstrumentName(inst.name);
      })
      .catch(() => {
        /* имя не критично — покажем тикер */
      });
    return () => {
      cancelled = true;
    };
  }, [instrument, instrumentName]);

  // Загрузка данных графика.
  useEffect(() => {
    if (!instrument) {
      setStatus('empty');
      return;
    }
    let cancelled = false;
    setStatus('loading');
    getChartData(instrument, instrument, 'futures', 24, clgroup, true, period)
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
    return () => {
      cancelled = true;
    };
  }, [instrument, clgroup, period]);

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
    () =>
      (data?.candles ?? []).map((c: { time: string; close: number }) => ({
        time: c.time,
        value: c.close,
      })),
    [data],
  );

  const oiData = useMemo(
    () =>
      (data?.open_interest ?? [])
        .filter((o: { net_position: number | null }) => o.net_position !== null)
        .map((o: { time: string; net_position: number | null }) => ({
          time: o.time,
          value: o.net_position as number,
        })),
    [data],
  );

  const displayName = instrumentName || displayTicker(instrument);

  return (
    <div style={embedColumn}>
      <div style={embedHeader}>
        <button style={pickerBtn} onClick={() => setPickerOpen(true)} title="Выбрать инструмент">
          <span style={{ fontWeight: 700, fontSize: 14 }}>{displayName}</span>
          <span style={{ opacity: 0.5, fontSize: 11 }}>▾</span>
        </button>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>Открытый интерес</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
          {(['FIZ', 'YUR'] as const).map((g) => (
            <button key={g} style={segBtn(clgroup === g)} onClick={() => setClgroup(g)}>
              {g === 'FIZ' ? 'Физлица' : 'Юрлица'}
            </button>
          ))}
        </div>
      </div>

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
          <EmbedMsg
            text={instrument ? 'Нет данных ОИ по этому инструменту' : 'Инструмент не выбран'}
          />
        )}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>

      {pickerOpen && (
        <InstrumentSearchModal
          filterType="futures"
          indicator="oi"
          onSelect={(secid, name) => {
            setInstrument(secid);
            setInstrumentName(name);
            setPickerOpen(false);
          }}
          onClose={() => setPickerOpen(false)}
        />
      )}
    </div>
  );
}
