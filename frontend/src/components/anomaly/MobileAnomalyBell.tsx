/**
 * MobileAnomalyBell — колокол для мобильного TopBar: круглая иконка с бейджем
 * непросмотренных + bottom-sheet с лентой «Новости и сигналы». Тело ленты —
 * тот же AnomalyFeedPanel, что и в десктоп-дропдауне.
 *
 * Управляется ГЛОБАЛЬНЫМ bellOpen из AnomalyContext (не локальным стейтом),
 * поэтому кнопка «Посмотреть» в дайджест-тосте (setBellOpen(true)) открывает
 * именно этот лист на мобиле. Шит рендерится в портале на body, чтобы не
 * клиппиться sticky-топбаром, и переиспользует .fm-sheet* из mobile.css.
 */
import { createPortal } from 'react-dom';
import { Bell, X } from 'lucide-react';
import { useAnomalies } from '../../contexts/AnomalyContext';
import { AnomalyFeedPanel } from './AnomalyFeedPanel';

export default function MobileAnomalyBell() {
  const { unseenCount, markAllSeen, bellOpen, setBellOpen } = useAnomalies();

  const toggle = () => {
    const next = !bellOpen;
    setBellOpen(next);
    if (next) markAllSeen();   // открыли — гасим бейдж
  };
  const close = () => setBellOpen(false);

  return (
    <>
      <button className="fm-icon-btn" onClick={toggle} aria-label="Новости и сигналы"
        style={{ color: 'var(--accent)' }}>
        <Bell size={16} strokeWidth={2.2} />
        {unseenCount > 0 && (
          <span style={{ position: 'absolute', top: -3, right: -3, minWidth: 15, height: 15, padding: '0 3px',
            background: 'var(--accent-orange, #FF9100)', color: '#1a1206', fontSize: 10, fontWeight: 700,
            borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center', lineHeight: 1 }}>
            {unseenCount > 9 ? '9+' : unseenCount}
          </span>
        )}
      </button>

      {bellOpen && createPortal(
        <>
          <div className="fm-sheet-backdrop" onClick={close} />
          <div className="fm-sheet" role="dialog" aria-label="Новости и сигналы" style={{ maxHeight: '80dvh' }}>
            <div className="fm-sheet-handle" />
            <div className="fm-sheet-header">
              <span className="fm-sheet-title">Новости и сигналы</span>
              <button className="fm-icon-btn" onClick={close} aria-label="Закрыть">
                <X size={16} strokeWidth={2.2} />
              </button>
            </div>
            <div className="fm-sheet-body">
              <AnomalyFeedPanel onClose={close} />
            </div>
          </div>
        </>,
        document.body,
      )}
    </>
  );
}
