/**
 * EmbedCbrFlows — виджет «Потоки ЦБ» (рыночный, тикер не нужен).
 * Переиспользует StackedBidirectionalHistogram. Контрол типа инструмента внутри.
 */
import { useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import StackedBidirectionalHistogram from '../../components/cbr/StackedBidirectionalHistogram';
import { getCbrFlows } from '../../services/api';
import { EmbedMsg, embedColumn, embedHeader, segBtn } from './embedUi';

type CbrType = 'stocks' | 'ofz' | 'fx';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type CbrResp = Awaited<ReturnType<typeof getCbrFlows>>;

const TYPES: { id: CbrType; label: string }[] = [
  { id: 'stocks', label: 'Акции' },
  { id: 'ofz', label: 'ОФЗ' },
  { id: 'fx', label: 'Валюта' },
];

export default function EmbedCbrFlows() {
  const [params] = useSearchParams();
  const [type, setType] = useState<CbrType>((params.get('type') as CbrType) || 'stocks');
  const [data, setData] = useState<CbrResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getCbrFlows(type)
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setStatus((res?.periods?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/cbr-flows load failed:', err);
        setStatus('error');
      });
    return () => {
      cancelled = true;
    };
  }, [type]);

  const boxRef = useRef<HTMLDivElement>(null);
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
    <div style={embedColumn}>
      <div style={embedHeader}>
        <span style={{ fontWeight: 700, fontSize: 14 }}>Потоки ЦБ</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 4 }}>
          {TYPES.map((t) => (
            <button key={t.id} style={segBtn(type === t.id)} onClick={() => setType(t.id)}>
              {t.label}
            </button>
          ))}
        </div>
      </div>
      <div ref={boxRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {status === 'ok' && data && (
          <StackedBidirectionalHistogram
            periods={data.periods}
            categories={data.categories}
            allPeriods={data.periods}
            unit="млрд руб."
            height={chartH}
            animTrigger={type}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </div>
  );
}
