/**
 * HelpTooltip — знак "?" возле заголовка индикатора.
 *
 * Два режима:
 *   1. `linkTo` указан → иконка становится ссылкой на отдельную
 *      страницу методологии (`/methodology/oi` и т.д.). Hover показывает
 *      короткую превью-подсказку, клик — переход на страницу.
 *   2. `linkTo` не указан → обычный popover с текстом из `entry`:
 *      hover на desktop, tap + modal на mobile.
 *
 * Приоритет — `linkTo`. По мере создания страниц методологии каждый
 * индикатор перейдёт на `linkTo` и popover с текстом станет просто
 * коротким preview-сниппетом.
 */
import { useState, useEffect, useRef } from 'react';
import { HelpCircle, X, ArrowUpRight } from 'lucide-react';
import { Link } from 'react-router-dom';
import type { MethodologyEntry } from '../data/methodology';

interface HelpTooltipProps {
  /** Готовая запись из METHODOLOGY. */
  entry?: MethodologyEntry;
  /** Или указать вручную заголовок + текст. */
  title?: string;
  content?: string;
  /** Размер иконки. Default 16. */
  size?: number;
  /** Ссылка на страницу методологии. Если указана — иконка становится
   *  ссылкой, а hover показывает preview с кнопкой «Подробнее». */
  linkTo?: string;
}

export default function HelpTooltip({ entry, title, content, size = 16, linkTo }: HelpTooltipProps) {
  const [open, setOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const popoverRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const check = () => setIsMobile(window.matchMedia('(hover: none)').matches);
    check();
    window.addEventListener('resize', check);
    return () => window.removeEventListener('resize', check);
  }, []);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (
        popoverRef.current &&
        !popoverRef.current.contains(e.target as Node) &&
        triggerRef.current &&
        !triggerRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    };
    const escHandler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    document.addEventListener('keydown', escHandler);
    return () => {
      document.removeEventListener('mousedown', handler);
      document.removeEventListener('keydown', escHandler);
    };
  }, [open]);

  const shortText = entry?.short ?? title ?? '';
  const fullText = entry?.full ?? content ?? '';

  const hoverOpen = () => { if (!isMobile) setOpen(true); };
  const hoverClose = () => { if (!isMobile) setOpen(false); };

  const iconStyle: React.CSSProperties = {
    width: size + 8,
    height: size + 8,
    borderRadius: '50%',
    color: open ? 'var(--accent)' : 'var(--text-muted)',
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    transition: 'color 0.15s',
  };

  const popover = open && (
    <div
      ref={popoverRef}
      role="tooltip"
      onMouseEnter={hoverOpen}
      onMouseLeave={hoverClose}
      style={{
        position: 'absolute',
        top: 'calc(100% + 6px)',
        left: 0,
        zIndex: 60,
        width: 'min(360px, calc(100vw - 32px))',
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
          onClick={() => setOpen(false)}
          style={{
            position: 'absolute',
            top: 8,
            right: 8,
            padding: 4,
            color: 'var(--text-muted)',
            background: 'transparent',
            border: 'none',
            cursor: 'pointer',
          }}
          aria-label="Закрыть"
        >
          <X size={16} />
        </button>
      )}
      {shortText && (
        <p
          style={{
            color: 'var(--text-primary)',
            fontWeight: 600,
            fontSize: 'var(--fs-xs)',
            marginBottom: 'var(--sp-2)',
            paddingRight: isMobile ? 24 : 0,
          }}
        >
          {shortText}
        </p>
      )}
      {fullText && <p style={{ marginBottom: linkTo ? 10 : 0 }}>{fullText}</p>}
      {linkTo && (
        <Link
          to={linkTo}
          onClick={() => setOpen(false)}
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 4,
            marginTop: 6,
            color: 'var(--accent)',
            fontSize: 'var(--fs-2xs)',
            fontWeight: 600,
            textDecoration: 'none',
          }}
        >
          Подробнее о методологии <ArrowUpRight size={12} />
        </Link>
      )}
    </div>
  );

  // Mode 1: linkTo → иконка = чистая ссылка (без hover-preview).
  if (linkTo) {
    return (
      <span className="relative inline-flex items-center">
        <Link
          ref={triggerRef as React.RefObject<HTMLAnchorElement>}
          to={linkTo}
          style={iconStyle}
          aria-label="Методология индикатора"
          title="Методология индикатора"
        >
          <HelpCircle size={size} strokeWidth={1.8} />
        </Link>
      </span>
    );
  }

  // Mode 2: без ссылки — только popover
  return (
    <span className="relative inline-flex items-center">
      <button
        ref={triggerRef as React.RefObject<HTMLButtonElement>}
        type="button"
        onClick={() => setOpen(o => !o)}
        onMouseEnter={hoverOpen}
        onMouseLeave={hoverClose}
        style={{ ...iconStyle, cursor: 'help', background: 'transparent', border: 'none', padding: 0 }}
        aria-label="Подсказка о методологии"
        aria-expanded={open}
      >
        <HelpCircle size={size} strokeWidth={1.8} />
      </button>
      {popover}
    </span>
  );
}
