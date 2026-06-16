/**
 * Dropdown — универсальный pill-стиля selector для editorial-редизайна.
 *
 * Trigger: outline pill с current value + chevron, при клике открывается popup
 * со списком options. Выбор через onChange. Закрывается по клику вне или ESC.
 *
 * Optional: каждый option может иметь colorDot (цветной квадрат слева, ширина 14)
 * — используется для OI-вариантов где у каждого режима свой цвет линии.
 *
 * Стилизация editorial — outline+hard-shadow задаётся через CSS-override
 * `[data-theme^="editorial"] .frame-dropdown`. На OKX/dark — обычный flat pill.
 */
import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { ChevronDown, HelpCircle, Lock, X } from 'lucide-react';

export interface DropdownOption<T extends string> {
  key: T;
  label: string;
  /** CSS color для маркера слева (палочка/dot). Опционально. */
  color?: string;
  /** Аватар-кружок слева (логотип УК): картинка либо буква на цветном bg.
   *  Аддитивно — рендерится только если задан, не влияет на legacy-вызовы. */
  avatar?: { img?: string; bg?: string; color?: string; letter?: string };
  /** Disabled state — option не кликабелен (premium-only и т.п.) */
  locked?: boolean;
  /** Скрыть из списка (но оставить в типе) */
  hidden?: boolean;
  /** Подсказка «?» справа от лейбла: что показывает вариант и как считается.
   *  Рендерится только если задано. Клик по «?» не выбирает опцию. */
  help?: { title?: string; content: string };
}

/**
 * OptionHelp — «?» возле опции дропдауна с поясняющим поповером.
 *
 * Поповер монтируется через portal в document.body с position:fixed —
 * иначе его обрезает overflow:auto у меню. Hover на desktop, tap на mobile.
 * Клик по иконке не выбирает опцию (stopPropagation), а mousedown по поповеру
 * не закрывает сам дропдаун (его outside-handler слушает document.mousedown).
 */
function OptionHelp({ title, content }: { title?: string; content: string }) {
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

  useEffect(() => {
    if (!open) return;
    const place = () => {
      const r = iconRef.current?.getBoundingClientRect();
      if (!r) return;
      const width = Math.min(300, window.innerWidth - 32);
      let left = r.left;
      if (left + width > window.innerWidth - 8) left = window.innerWidth - 8 - width;
      if (left < 8) left = 8;
      setPos({ top: r.bottom + 6, left });
    };
    place();
    const onScroll = () => setOpen(false);
    const onDown = (e: MouseEvent) => {
      if (popRef.current?.contains(e.target as Node)) return;
      if (iconRef.current?.contains(e.target as Node)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey);
    window.addEventListener('scroll', onScroll, true);
    window.addEventListener('resize', place);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey);
      window.removeEventListener('scroll', onScroll, true);
      window.removeEventListener('resize', place);
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
      {open && pos && createPortal(
        <div
          ref={popRef}
          role="tooltip"
          onMouseEnter={hoverOpen}
          onMouseLeave={hoverClose}
          onMouseDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
          style={{
            position: 'fixed',
            top: pos.top,
            left: pos.left,
            zIndex: 80,
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

// Кружок-аватар ~20px для dropdown-опций (логотип УК). Картинка перекрывает букву.
function DropdownAvatar({ a, size = 20 }: { a: NonNullable<DropdownOption<string>['avatar']>; size?: number }) {
  return (
    <span
      className="inline-flex items-center justify-center flex-shrink-0 rounded-full overflow-hidden"
      style={{
        width: size,
        height: size,
        backgroundColor: a.img ? undefined : (a.bg ?? 'var(--bg-secondary)'),
        color: a.color ?? 'var(--text-secondary)',
        fontWeight: 900,
        fontSize: Math.round(size * 0.5),
        lineHeight: 1,
      }}
    >
      {a.img
        ? <img src={a.img} alt="" style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
        : (a.letter ?? '')}
    </span>
  );
}

interface DropdownProps<T extends string> {
  /** Подпись над dropdown (опционально, eyebrow style) */
  label?: string;
  options: DropdownOption<T>[];
  value: T;
  onChange: (key: T) => void;
  /** Что показать в trigger когда value не в options (fallback) */
  placeholder?: string;
  /** Минимальная ширина trigger в px — чтобы dropdowns в ряду были выравнены */
  minWidth?: number;
  /** maxWidth выпадающего меню в px (default 280) — шире для длинных лейблов. */
  menuMaxWidth?: number;
  /** className для wrapper'а */
  className?: string;
  /** Фолбэк-handler для клика по locked-опции. Если задан — закрывает popup
   *  и вызывается с key. Используется для показа UpgradeModal вместо тихого
   *  отказа. Если не задан — locked click игнорируется (legacy behavior). */
  onLockedClick?: (key: T) => void;
}

export default function Dropdown<T extends string>({
  label,
  options,
  value,
  onChange,
  placeholder = 'Выбрать',
  minWidth,
  menuMaxWidth = 280,
  className = '',
  onLockedClick,
}: DropdownProps<T>) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);

  // Закрытие по клику вне + ESC
  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', onClick);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onClick);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  const current = options.find(o => o.key === value);
  const visible = options.filter(o => !o.hidden);

  return (
    <div ref={wrapRef} className={`frame-dropdown relative inline-block ${className}`}>
      {label && (
        <p
          className="uppercase mb-1"
          style={{
            color: 'var(--text-muted)',
            letterSpacing: '0.1em',
            fontWeight: 600,
            fontSize: 'var(--fs-2xs)',
          }}
        >
          {label}
        </p>
      )}
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="frame-dropdown-trigger flex items-center font-semibold rounded-full"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          color: 'var(--text-primary)',
          border: '2px solid var(--text-primary)',
          minWidth: minWidth,
          fontSize: 'var(--fs-sm)',
          padding: 'var(--sp-2) var(--sp-4)',
          gap: 'var(--sp-2)',
          transition: 'transform 0.15s ease, box-shadow 0.15s ease',
        }}
      >
        {current?.avatar && <DropdownAvatar a={current.avatar} />}
        {current?.color && (
          <span
            className="inline-block flex-shrink-0 rounded-full"
            style={{ width: 14, height: 3, backgroundColor: current.color }}
          />
        )}
        <span className="flex-1 text-left whitespace-nowrap">
          {current?.label ?? placeholder}
        </span>
        <ChevronDown
          size={16}
          style={{
            transition: 'transform 0.2s ease',
            transform: open ? 'rotate(180deg)' : 'rotate(0)',
            flexShrink: 0,
          }}
        />
      </button>

      {open && (
        <div
          className="frame-dropdown-menu absolute z-50 mt-1 py-1 rounded-xl"
          style={{
            backgroundColor: 'var(--bg-secondary)',
            border: '2px solid var(--text-primary)',
            boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
            minWidth: '100%',
            maxWidth: menuMaxWidth,
            maxHeight: '60vh',
            overflowY: 'auto',
          }}
        >
          {visible.map(opt => {
            const active = opt.key === value;
            return (
              <button
                key={opt.key}
                type="button"
                // Не disabled если onLockedClick задан — иначе native disabled
                // блокирует pointer events и колбэк не сработает
                disabled={opt.locked && !onLockedClick}
                data-active={active}
                onClick={() => {
                  if (opt.locked) {
                    if (onLockedClick) {
                      onLockedClick(opt.key);
                      setOpen(false);
                    }
                    return;
                  }
                  onChange(opt.key);
                  setOpen(false);
                }}
                className="frame-dropdown-item w-full text-left text-sm flex items-center gap-2"
                style={{
                  // Active = filled accent pill с hard shadow (как btn-control.active в editorial).
                  // Inactive = plain text, hover подсвечивает item.
                  margin: '4px 6px',
                  padding: '8px 12px',
                  width: 'calc(100% - 12px)',
                  borderRadius: 999,
                  fontWeight: active ? 800 : 600,
                  color: opt.locked
                    ? 'var(--text-muted)'
                    : active
                      ? 'var(--text-inverse)'
                      : 'var(--text-primary)',
                  backgroundColor: active ? 'var(--accent)' : 'transparent',
                  border: active ? '2px solid var(--text-primary)' : '2px solid transparent',
                  boxShadow: active ? 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))' : 'none',
                  cursor: opt.locked ? 'not-allowed' : 'pointer',
                  opacity: opt.locked ? 0.5 : 1,
                  transition: 'background-color 0.12s ease, color 0.12s ease',
                }}
              >
                {opt.avatar && <DropdownAvatar a={opt.avatar} />}
                {opt.color && (
                  <span
                    className="inline-block flex-shrink-0 rounded-full"
                    style={{ width: 14, height: 3, backgroundColor: opt.color }}
                  />
                )}
                <span className="flex-1 whitespace-nowrap">{opt.label}</span>
                {opt.help && !opt.locked && (
                  <OptionHelp title={opt.help.title} content={opt.help.content} />
                )}
                {opt.locked && <Lock size={12} className="flex-shrink-0" />}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
