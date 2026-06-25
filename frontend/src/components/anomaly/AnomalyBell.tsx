/**
 * AnomalyBell — колокол в шапке: бейдж непросмотренных + дропдаун-лента последних
 * аномалий (это «всё» для тех, кто хочет просмотреть; тост — только свежее).
 * Внизу — тумблер всплывающих тостов (кнопка «выключить» для всех; обратно
 * включает тут же). Открытие колокола гасит бейдж (markAllSeen).
 */
import { useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bell, TrendingUp, TrendingDown, Send } from 'lucide-react';
import { useAnomalies } from '../../contexts/AnomalyContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { openAnomaly } from './anomalyActions';
import type { AnomalyItem } from '../../services/api';

function relTime(iso?: string | null): string {
  if (!iso) return '';
  const m = Math.floor((Date.now() - new Date(iso).getTime()) / 60_000);
  if (m < 1) return 'только что';
  if (m < 60) return `${m} мин`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} ч`;
  const d = Math.floor(h / 24);
  return d === 1 ? 'вчера' : `${d} дн`;
}

export function AnomalyBell() {
  const {
    items, unseenCount, markAllSeen, toastsEnabled, setToastsEnabled, bellOpen, setBellOpen,
    channelPosts,
  } = useAnomalies();
  const { showUpgrade } = useUpgradePrompt();
  const navigate = useNavigate();
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

  const onItem = (item: AnomalyItem) => {
    openAnomaly(item, navigate, showUpgrade);
    setBellOpen(false);
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
            <span style={{ color: 'var(--text-primary)', fontSize: 14, fontWeight: 600 }}>Аномалии рынка</span>
          </div>

          {items.length === 0 ? (
            <div style={{ padding: '28px 14px', textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 }}>
              Пока тихо — заметных аномалий нет
            </div>
          ) : (
            items.slice(0, 20).map((item) => {
              const up = item.direction === 'up';
              const Dir = up ? TrendingUp : TrendingDown;
              const c = up ? 'var(--success, #00E676)' : 'var(--danger, #FF5252)';
              return (
                <button key={item.id} onClick={() => onItem(item)}
                  style={{ display: 'block', width: '100%', textAlign: 'left', background: 'transparent',
                    border: 'none', borderBottom: '0.5px solid var(--border-color)', padding: '10px 14px', cursor: 'pointer' }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 2 }}>
                    <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>
                      {item.asset_name || item.asset_id}{item.mine ? ' · ваш сигнал' : ''}
                    </span>
                    <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{relTime(item.created_at)}</span>
                  </div>
                  <div style={{ color: 'var(--text-primary)', fontSize: 13, marginBottom: 3 }}>{item.headline}</div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                    <Dir size={14} color={c} />
                    {item.severity_value != null && (
                      <span style={{ color: c, fontSize: 12, fontWeight: 500 }}>×{item.severity_value.toFixed(1)}</span>
                    )}
                  </div>
                </button>
              );
            })
          )}

          {channelPosts.length > 0 && (
            <>
              <div style={{ padding: '10px 14px 6px', borderTop: '0.5px solid var(--border-color)' }}>
                <span style={{ color: 'var(--text-secondary)', fontSize: 12, fontWeight: 600 }}>Новости каналов</span>
              </div>
              {channelPosts.map((p) => (
                <button key={`cp-${p.id}`}
                  onClick={() => window.open(p.link, '_blank', 'noopener,noreferrer')}
                  style={{ display: 'block', width: '100%', textAlign: 'left', background: 'transparent',
                    border: 'none', borderBottom: '0.5px solid var(--border-color)', padding: '8px 14px', cursor: 'pointer' }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 2 }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 5, color: 'var(--accent-cyan, #22D3EE)', fontSize: 11 }}>
                      <Send size={11} /> {p.channel_name || p.channel}
                    </span>
                    <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{relTime(p.posted_at)}</span>
                  </div>
                  {p.text && (
                    <div style={{ color: 'var(--text-primary)', fontSize: 12.5, lineHeight: 1.35 }}>
                      {p.text.length > 110 ? `${p.text.slice(0, 110)}…` : p.text}
                    </div>
                  )}
                </button>
              ))}
            </>
          )}

          <div style={{ padding: '10px 14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>Всплывающие уведомления</span>
            <button onClick={() => setToastsEnabled(!toastsEnabled)} role="switch" aria-checked={toastsEnabled}
              aria-label="Показывать всплывающие аномалии"
              style={{ width: 36, height: 20, borderRadius: 10, border: 'none', cursor: 'pointer', flexShrink: 0,
                background: toastsEnabled ? 'var(--success, #00E676)' : 'var(--bg-tertiary, #333)', position: 'relative' }}>
              <span style={{ position: 'absolute', top: 2, left: toastsEnabled ? 18 : 2, width: 16, height: 16,
                borderRadius: '50%', background: '#fff', transition: 'left 0.15s' }} />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
