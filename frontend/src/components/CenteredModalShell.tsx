/**
 * CenteredModalShell — переиспользуемая оболочка центрированной модалки
 * в editorial-стиле (бэкдроп + панель с outline и hard shadow).
 *
 * До неё каждая модалка (Upgrade/InstrumentSearch/CreateAlert) носила свой
 * bespoke-бэкдроп; для маленьких настроечных панелей (например «Слои графика»)
 * нужен общий шелл. Закрытие: клик по бэкдропу, ESC, крестик. Скролл body
 * блокируется на время показа. zIndex 9999 — как у UpgradeModal/CreateAlertModal.
 */
import { useEffect, type ReactNode } from 'react';
import { X } from 'lucide-react';

interface CenteredModalShellProps {
  open: boolean;
  onClose: () => void;
  title?: string;
  children: ReactNode;
  /** Максимальная ширина панели, px */
  maxWidth?: number;
}

export default function CenteredModalShell({
  open,
  onClose,
  title,
  children,
  maxWidth = 380,
}: CenteredModalShellProps) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.removeEventListener('keydown', onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 flex items-center justify-center p-4"
      style={{ zIndex: 9999, background: 'rgba(0,0,0,0.5)' }}
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(e) => e.stopPropagation()}
        className="w-full rounded-2xl"
        style={{
          maxWidth,
          backgroundColor: 'var(--bg-primary)',
          border: '2px solid var(--text-primary)',
          boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
          padding: 'var(--sp-5)',
          maxHeight: '85vh',
          overflowY: 'auto',
        }}
      >
        <div className="flex items-center justify-between" style={{ marginBottom: 'var(--sp-4)' }}>
          <span className="font-bold" style={{ fontSize: 'var(--fs-base)', color: 'var(--text-primary)' }}>
            {title}
          </span>
          <button
            type="button"
            onClick={onClose}
            aria-label="Закрыть"
            className="inline-flex items-center justify-center rounded-full"
            style={{ color: 'var(--text-primary)', width: 28, height: 28 }}
          >
            <X size={18} />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}
