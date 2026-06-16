/**
 * OptionHelp — иконка «?» с поясняющим поповером для опций выбора.
 *
 * Используется в desktop-дропдауне OI (components/Dropdown) и в мобильных
 * чипах OI (pages/mobile/MobileOpenInterestPage) — единый компонент, единое
 * поведение.
 *
 * Поповер монтируется через portal в document.body с position:fixed — иначе
 * его обрезает overflow:auto у меню дропдауна или нижний шит на мобиле. Hover
 * на desktop, tap на mobile. Клик по «?» не выбирает опцию (stopPropagation),
 * а mousedown по поповеру не закрывает родительский дропдаун/шит.
 *
 * Позиционирование: под иконкой, с клампом по горизонтали и переворотом ВВЕРХ,
 * если снизу не помещается (актуально для чипа у нижней кромки мобильного шита).
 * Замер реальной высоты в useLayoutEffect (до paint) — без мигания.
 */
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { HelpCircle, X } from 'lucide-react';

interface OptionHelpProps {
  /** Заголовок поповера (опционально). */
  title?: string;
  /** Текст пояснения. */
  content: string;
}

// Выше мобильного шита (.fm-sheet z-index:101, во время тура 10000) и десктопного
// меню дропдауна (z-50) — чтобы поповер всегда был поверх своего контейнера.
const POPOVER_Z = 10002;

export default function OptionHelp({ title, content }: OptionHelpProps) {
  const [open, setOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);
  const iconRef = useRef<HTMLSpanElement>(null);
  const popRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const check = () => setIsMobile(window.matchMedia('(hover: none)').matches);
    check();
    window.addEventListener('resize', check);
    return () => window.removeEventListener('resize', check);
  }, []);

  // Позиционируем ПОСЛЕ рендера поповера: знаем его реальную высоту → корректный
  // переворот вверх. useLayoutEffect срабатывает до paint, поэтому скрытый кадр
  // (visibility:hidden пока pos===null) пользователь не видит.
  useLayoutEffect(() => {
    if (!open) { setPos(null); return; }
    const icon = iconRef.current?.getBoundingClientRect();
    const pop = popRef.current;
    if (!icon || !pop) return;
    const M = 8, GAP = 6;
    const w = pop.offsetWidth;
    const h = pop.offsetHeight;
    let left = icon.left;
    if (left + w > window.innerWidth - M) left = window.innerWidth - M - w;
    if (left < M) left = M;
    let top = icon.bottom + GAP;
    if (top + h > window.innerHeight - M && icon.top - GAP - h >= M) {
      top = icon.top - GAP - h; // снизу не влезает, но влезает сверху
    }
    setPos({ top, left });
  }, [open, isMobile]);

  useEffect(() => {
    if (!open) return;
    const close = () => setOpen(false);
    const onDown = (e: MouseEvent) => {
      if (popRef.current?.contains(e.target as Node)) return;
      if (iconRef.current?.contains(e.target as Node)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey);
    window.addEventListener('scroll', close, true);
    window.addEventListener('resize', close);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey);
      window.removeEventListener('scroll', close, true);
      window.removeEventListener('resize', close);
    };
  }, [open]);

  const hoverOpen = () => { if (!isMobile) setOpen(true); };
  const hoverClose = () => { if (!isMobile) setOpen(false); };

  return (
    <span
      ref={iconRef}
      role="button"
      tabIndex={0}
      aria-label="Что это и как считается"
      onClick={(e) => { e.stopPropagation(); setOpen(o => !o); }}
      onMouseEnter={hoverOpen}
      onMouseLeave={hoverClose}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); setOpen(o => !o); }
      }}
      className="inline-flex items-center justify-center flex-shrink-0"
      style={{ cursor: 'help', color: 'currentColor', opacity: open ? 1 : 0.65 }}
    >
      <HelpCircle size={15} strokeWidth={1.8} />
      {open && createPortal(
        <div
          ref={popRef}
          role="tooltip"
          onMouseEnter={hoverOpen}
          onMouseLeave={hoverClose}
          onMouseDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
          style={{
            position: 'fixed',
            top: pos?.top ?? 0,
            left: pos?.left ?? 0,
            visibility: pos ? 'visible' : 'hidden',
            zIndex: POPOVER_Z,
            width: 'min(300px, calc(100vw - 32px))',
            padding: 'var(--sp-4) var(--sp-5)',
            borderRadius: 'var(--radius-md, 8px)',
            backgroundColor: 'var(--bg-secondary)',
            border: '1px solid var(--border-color)',
            boxShadow: 'var(--shadow-hard, 0 10px 40px -10px rgba(0,0,0,0.5), 0 4px 12px rgba(0,0,0,0.3))',
            color: 'var(--text-secondary)',
            fontSize: 'var(--fs-xs)',
            lineHeight: 1.5,
            cursor: 'default',
          }}
        >
          {isMobile && (
            <button
              type="button"
              onClick={(e) => { e.stopPropagation(); setOpen(false); }}
              aria-label="Закрыть"
              style={{
                position: 'absolute', top: 8, right: 8, padding: 4,
                color: 'var(--text-muted)', background: 'transparent', border: 'none', cursor: 'pointer',
              }}
            >
              <X size={16} />
            </button>
          )}
          {title && (
            <p style={{
              color: 'var(--text-primary)', fontWeight: 600, fontSize: 'var(--fs-xs)',
              marginBottom: 'var(--sp-2)', paddingRight: isMobile ? 24 : 0,
            }}>
              {title}
            </p>
          )}
          <p style={{ margin: 0 }}>{content}</p>
        </div>,
        document.body
      )}
    </span>
  );
}
