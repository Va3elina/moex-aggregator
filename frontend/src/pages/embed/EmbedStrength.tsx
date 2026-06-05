/**
 * EmbedStrength — виджет «Сила рынка» (% акций выше EMA). EMA и период — в
 * drawer'е настроек. Переиспользует BreadthChart; universe=imoex (free).
 */
import { useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import BreadthChart from '../../components/strength/BreadthChart';
import { getBreadthHistory } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { useEmbedSettings, EmbedShell, DrawerSection, SegGroup } from './EmbedSettings';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Synced = { time: string; breadth: number; imoex: number }[];
type Period = '6m' | '1y' | '2y' | '5y' | 'all';

const EMAS: { id: number; label: string }[] = [
  { id: 50, label: 'EMA 50' },
  { id: 100, label: 'EMA 100' },
  { id: 200, label: 'EMA 200' },
];
const PERIODS: { id: Period; label: string }[] = [
  { id: '6m', label: '6М' },
  { id: '1y', label: '1Г' },
  { id: '2y', label: '2Г' },
  { id: '5y', label: '5Л' },
  { id: 'all', label: 'Всё' },
];

function daysForPeriod(period: Period): number {
  switch (period) {
    case '6m': return 182;
    case '2y': return 730;
    case '5y': return 1825;
    case 'all': return 3650;
    default: return 365;
  }
}

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

export default function EmbedStrength() {
  const [params] = useSearchParams();
  const settings = useEmbedSettings();

  const [ema, setEma] = useState<number>(() => Number(readLS('frame:strength:ema', '200')) || 200);
  const [period, setPeriod] = useState<Period>(() => (params.get('period') || readLS('frame:embed:strength:period', '1y')) as Period);
  const [synced, setSynced] = useState<Synced>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { try { localStorage.setItem('frame:strength:ema', String(ema)); } catch { /* quota */ } }, [ema]);
  useEffect(() => { try { localStorage.setItem('frame:embed:strength:period', period); } catch { /* quota */ } }, [period]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getBreadthHistory(ema, daysForPeriod(period), 'imoex')
      .then((res) => {
        if (cancelled) return;
        const imoexMap = new Map((res?.imoex ?? []).map((d) => [d.date, d.close]));
        const s: Synced = (res?.data ?? [])
          .filter((d) => imoexMap.has(d.date))
          .map((d) => ({ time: d.date, breadth: d.percent_above, imoex: imoexMap.get(d.date)! }));
        setSynced(s);
        setStatus(s.length ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/strength load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [ema, period]);

  const boxRef = useRef<HTMLDivElement>(null);
  const [chartH, setChartH] = useState(280);
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
    <EmbedShell
      settings={settings}
      title="Сила рынка"
      subtitle={`% выше EMA ${ema}`}
      drawer={
        <>
          <DrawerSection label="Скользящая (EMA)">
            <SegGroup value={ema} options={EMAS} onChange={(v) => setEma(v)} />
          </DrawerSection>
          <DrawerSection label="Период">
            <SegGroup value={period} options={PERIODS} onChange={(v) => setPeriod(v)} />
          </DrawerSection>
        </>
      }
    >
      <div ref={boxRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {status === 'ok' && synced.length > 0 && (
          <BreadthChart
            syncedData={synced}
            hoverIndex={null}
            height={chartH}
            mode="histogram"
            padding={{ left: 48, right: 16, top: 8, bottom: 22 }}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedShell>
  );
}
