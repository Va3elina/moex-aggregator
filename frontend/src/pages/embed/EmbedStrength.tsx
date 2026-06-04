/**
 * EmbedStrength — виджет «Сила рынка» (рыночный, % акций выше EMA). Переиспользует
 * BreadthChart (гистограмма breadth). Контрол EMA внутри; universe=imoex (free).
 * IndexChart (верхний график IMOEX) для компактного виджета опускаем.
 */
import { useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import BreadthChart from '../../components/strength/BreadthChart';
import { getBreadthHistory } from '../../services/api';
import { EmbedMsg, embedColumn, embedHeader, segBtn } from './embedUi';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Synced = { time: string; breadth: number; imoex: number }[];

const EMAS = [50, 100, 200];

function daysForPeriod(period: string): number {
  switch (period) {
    case '6m': return 182;
    case '2y': return 730;
    case '5y': return 1825;
    case 'all': return 3650;
    default: return 365;
  }
}

export default function EmbedStrength() {
  const [params] = useSearchParams();
  const days = daysForPeriod(params.get('period') || '1y');
  const [ema, setEma] = useState<number>(() => {
    try { return Number(localStorage.getItem('frame:strength:ema')) || 200; } catch { return 200; }
  });
  const [synced, setSynced] = useState<Synced>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => {
    try { localStorage.setItem('frame:strength:ema', String(ema)); } catch { /* quota */ }
  }, [ema]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getBreadthHistory(ema, days, 'imoex')
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
  }, [ema, days]);

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
    <div style={embedColumn}>
      <div style={embedHeader}>
        <span style={{ fontWeight: 700, fontSize: 14 }}>Сила рынка</span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>% выше EMA</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
          {EMAS.map((e) => (
            <button key={e} style={segBtn(ema === e)} onClick={() => setEma(e)}>EMA{e}</button>
          ))}
        </div>
      </div>
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
    </div>
  );
}
