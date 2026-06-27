/**
 * AnomalyFeedPanel — общее ТЕЛО ленты «Новости и сигналы»: объединённый список
 * (аномалии + посты каналов по времени, новые сверху) + тумблер всплывающих
 * тостов внизу. Шапку рисует обёртка (десктоп-дропдаун AnomalyBell или
 * мобильный bottom-sheet MobileAnomalyBell) — тело одно на оба, чтобы не
 * дублировать рендер ~30 строк и логику диплинков.
 *
 * onClose вызывается после перехода по аномалии (закрыть дропдаун/шит). Посты
 * каналов открываются в новой вкладке и панель не закрывают.
 */
import { useNavigate } from 'react-router-dom';
import { TrendingUp, TrendingDown, Send } from 'lucide-react';
import { useAnomalies } from '../../contexts/AnomalyContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { openAnomaly, tradeDateLabel } from './anomalyActions';
import type { AnomalyItem } from '../../services/api';

export function relTime(iso?: string | null): string {
  if (!iso) return '';
  const m = Math.floor((Date.now() - new Date(iso).getTime()) / 60_000);
  if (m < 1) return 'только что';
  if (m < 60) return `${m} мин`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} ч`;
  const d = Math.floor(h / 24);
  return d === 1 ? 'вчера' : `${d} дн`;
}

export function AnomalyFeedPanel({ onClose }: { onClose: () => void }) {
  const { items, channelPosts, toastsEnabled, setToastsEnabled } = useAnomalies();
  const { showUpgrade } = useUpgradePrompt();
  const navigate = useNavigate();

  // Единый список: аномалии + посты каналов, по времени (новые сверху).
  const merged = [
    ...items.map((a) => ({ kind: 'anomaly' as const, key: `a-${a.id}`,
      ts: a.created_at ? Date.parse(a.created_at) : 0, a })),
    ...channelPosts.map((p) => ({ kind: 'post' as const, key: `p-${p.id}`,
      ts: p.posted_at ? Date.parse(p.posted_at) : 0, p })),
  ].sort((x, y) => y.ts - x.ts);

  const onItem = (item: AnomalyItem) => {
    openAnomaly(item, navigate, showUpgrade);
    onClose();
  };

  return (
    <>
      {merged.length === 0 ? (
        <div style={{ padding: '28px 14px', textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 }}>
          Пока тихо — ничего нового
        </div>
      ) : (
        merged.slice(0, 30).map((row) => {
          if (row.kind === 'post') {
            const p = row.p;
            return (
              <button key={row.key}
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
            );
          }
          const item = row.a;
          const up = item.direction === 'up';
          const Dir = up ? TrendingUp : TrendingDown;
          const c = up ? 'var(--success, #00E676)' : 'var(--danger, #FF5252)';
          const dateLbl = tradeDateLabel(item.signal_date);
          return (
            <button key={row.key} onClick={() => onItem(item)}
              style={{ display: 'block', width: '100%', textAlign: 'left', background: 'transparent',
                border: 'none', borderBottom: '0.5px solid var(--border-color)', padding: '10px 14px', cursor: 'pointer' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 2 }}>
                <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>
                  {item.asset_name || item.asset_id}{item.mine ? ' · ваш сигнал' : ''}
                </span>
                <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{relTime(item.created_at)}</span>
              </div>
              <div style={{ color: 'var(--text-primary)', fontSize: 13, marginBottom: 3 }}>{item.headline}</div>
              {item.type === 'promo' ? (
                item.context && (
                  <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{item.context}</div>
                )
              ) : (
                <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                  <Dir size={14} color={c} />
                  {item.severity_value != null && (
                    <span style={{ color: c, fontSize: 12, fontWeight: 500 }}>×{item.severity_value.toFixed(1)}</span>
                  )}
                  {dateLbl && (
                    <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>· за {dateLbl}</span>
                  )}
                </div>
              )}
            </button>
          );
        })
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
    </>
  );
}
