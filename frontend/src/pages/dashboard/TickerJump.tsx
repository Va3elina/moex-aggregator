/**
 * Меню переходов по тикеру — один и тот же список везде: в срезах базы, в графе
 * связей, в разборе поста. Это и есть «перепрыгивать между всем, потому что всё
 * связано»: из любой ячейки с тикером — в свечи, показатели, акционеров,
 * дивиденды, вес в индексе, новости, кандидаты и связи по нему же.
 */
import { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { ChevronDown } from 'lucide-react';

/** Куда можно прыгнуть по тикеру. Список — не «все срезы», а те, где тикер осмыслен. */
export const ПЕРЕХОДЫ: Array<{ имя: string; куда: (t: string) => string }> = [
  { имя: 'Свечи', куда: (t) => `/admin/dashboard/db/candles?secid=${t}` },
  { имя: 'Показатели отчётности', куда: (t) => `/admin/dashboard/db/metrics?secid=${t}` },
  { имя: 'Акционеры', куда: (t) => `/admin/dashboard/db/shareholders?ticker=${t}` },
  { имя: 'Дивиденды', куда: (t) => `/admin/dashboard/db/dividends?secid=${t}&с=2015-01-01` },
  { имя: 'Вес в индексе', куда: (t) => `/admin/dashboard/db/index_composition?ticker=${t}` },
  { имя: 'Новости', куда: (t) => `/admin/dashboard/db/news?ticker=${t}&с=2026-01-01` },
  { имя: 'Кандидаты в посты', куда: (t) => `/admin/dashboard/posts?ticker=${t}` },
  { имя: 'Связи владения', куда: (t) => `/admin/dashboard/graph/${t}` },
  { имя: 'Факты о мире', куда: (t) => `/admin/dashboard/db/facts?entity=${t}` },
];

export default function TickerJump({ t, кроме }: { t: string; кроме?: string }) {
  const [открыт, setОткрыт] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    if (!открыт) return;
    const закрыть = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setОткрыт(false);
    };
    document.addEventListener('mousedown', закрыть);
    return () => document.removeEventListener('mousedown', закрыть);
  }, [открыт]);
  return (
    <span ref={ref} style={{ position: 'relative', display: 'inline-block' }}>
      <button
        onClick={() => setОткрыт((o) => !o)}
        className="mono"
        style={{
          fontSize: 11, padding: '1px 7px', borderRadius: 999, cursor: 'pointer', border: 'none',
          background: 'rgba(93,163,233,0.16)', color: 'var(--d-cold)', fontWeight: 600,
        }}
        title="перейти по тикеру"
      >
        {t}<ChevronDown size={9} style={{ display: 'inline', marginLeft: 2, verticalAlign: -1 }} />
      </button>
      {открыт && (
        <div className="dash-jump">
          <div className="mono" style={{ fontSize: 10, color: 'var(--d-dim)', padding: '2px 9px 5px' }}>{t} →</div>
          {ПЕРЕХОДЫ.filter((п) => п.имя !== кроме).map((п) => (
            <Link key={п.имя} to={п.куда(t)} onClick={() => setОткрыт(false)}>{п.имя}</Link>
          ))}
        </div>
      )}
    </span>
  );
}
