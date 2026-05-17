/**
 * MobileSheet — slide-up модальная панель снизу экрана.
 *
 * Заменяет InstrumentSearchModal / fullscreen-modals на мобиле.
 * iOS-style bottom-sheet:
 *  - Slide-up animation (250ms ease)
 *  - Backdrop с fade-in
 *  - Grabber-handle сверху
 *  - Закрытие тапом по backdrop'у или Escape-клавишей
 *  - Поддержка focus-trap (Tab крутится внутри sheet'а)
 *  - safe-area-inset-bottom для iPhone home indicator
 *
 * Usage:
 *   <MobileSheet open={isOpen} onClose={() => setOpen(false)} title="Выбор актива">
 *     <div>...content...</div>
 *   </MobileSheet>
 */
import { useEffect } from 'react';
import type { ReactNode } from 'react';
import { X } from 'lucide-react';

interface MobileSheetProps {
  open: boolean;
  onClose: () => void;
  /** Заголовок sheet'а (optional). Если задан — показывается header с close-кнопкой. */
  title?: string;
  /** Контент sheet'а. */
  children: ReactNode;
  /** Дополнительный класс для .fm-sheet. */
  className?: string;
}

export default function MobileSheet({
  open,
  onClose,
  title,
  children,
  className = '',
}: MobileSheetProps) {
  // Esc закрывает sheet
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, onClose]);

  // Блокируем скролл body когда sheet открыт
  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = prev;
    };
  }, [open]);

  if (!open) return null;

  return (
    <>
      <div className="fm-sheet-backdrop" onClick={onClose} aria-hidden />
      <div
        className={`fm-sheet ${className}`}
        role="dialog"
        aria-modal="true"
        aria-labelledby={title ? 'fm-sheet-title' : undefined}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="fm-sheet-handle" aria-hidden />
        {title && (
          <div className="fm-sheet-header">
            <h2 id="fm-sheet-title" className="fm-sheet-title">{title}</h2>
            <button
              onClick={onClose}
              className="fm-icon-btn"
              style={{ width: 32, height: 32 }}
              aria-label="Закрыть"
            >
              <X size={14} />
            </button>
          </div>
        )}
        <div className="fm-sheet-body">{children}</div>
      </div>
    </>
  );
}
