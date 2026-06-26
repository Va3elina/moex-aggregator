/**
 * AnomalyBell — колокол в шапке (десктоп): бейдж непросмотренных + дропдаун с
 * лентой «Новости и сигналы». Тело ленты — общий AnomalyFeedPanel (тот же на
 * мобильном bottom-sheet MobileAnomalyBell). Открытие колокола гасит бейдж
 * (markAllSeen). Внизу панели — тумблер всплывающих тостов.
 */
import { useRef, useEffect } from 'react';
import { Bell } from 'lucide-react';
import { useAnomalies } from '../../contexts/AnomalyContext';
import { AnomalyFeedPanel } from './AnomalyFeedPanel';

export function AnomalyBell() {
  const { unseenCount, markAllSeen, bellOpen, setBellOpen } = useAnomalies();
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!bellOpen) return;
    const onDoc = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setBellOpen(false);
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [bellOpen, setBellOpen]);

  const toggle = () => {
    const next = !bellOpen;
    setBellOpen(next);
    if (next) markAllSeen();   // открыли — гасим бейдж
  };

  return (
    <div ref={ref} style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
      <button onClick={toggle} aria-label="Аномалии рынка"
        className="editorial-press grid place-items-center w-5 h-5 xl:w-8 xl:h-8 rounded-full"
        style={{ position: 'relative', color: 'var(--accent)', border: '1.5px solid var(--text-primary)',
          backgroundColor: 'transparent', cursor: 'pointer' }}>
        <Bell strokeWidth={2}
          style={{ width: 'clamp(13px, 1vw + 0.3rem, 17px)', height: 'clamp(13px, 1vw + 0.3rem, 17px)' }} />
        {unseenCount > 0 && (
          <span style={{ position: 'absolute', top: -4, right: -4, minWidth: 15, height: 15, padding: '0 3px',
            background: 'var(--accent-orange, #FF9100)', color: '#1a1206', fontSize: 10, fontWeight: 600,
            borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center', lineHeight: 1 }}>
            {unseenCount > 9 ? '9+' : unseenCount}
          </span>
        )}
      </button>

      {bellOpen && (
        <div style={{ position: 'absolute', right: 0, top: 'calc(100% + 10px)', width: 'min(340px, calc(100vw - 24px))', maxHeight: 440,
          overflowY: 'auto', background: 'var(--bg-secondary, #15181C)',
          border: '0.5px solid var(--border-color, rgba(255,255,255,0.12))', borderRadius: 12,
          boxShadow: '0 12px 36px rgba(0,0,0,0.45)', zIndex: 1300 }}>
          <div style={{ padding: '12px 14px', borderBottom: '0.5px solid var(--border-color)' }}>
            <span style={{ color: 'var(--text-primary)', fontSize: 14, fontWeight: 600 }}>Новости и сигналы</span>
          </div>
          <AnomalyFeedPanel onClose={() => setBellOpen(false)} />
        </div>
      )}
    </div>
  );
}
