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
import { createPortal } from 'react-dom';
import { HelpCircle, Info, X, ArrowUpRight } from 'lucide-react';
import { Link } from 'react-router-dom';
import type { MethodologyEntry } from '../data/methodology';
import { useTheme } from '../contexts/ThemeContext';

interface HelpTooltipProps {
  /** Готовая запись из METHODOLOGY. */
  entry?: MethodologyEntry;
  /** Или указать вручную заголовок + текст. */
  title?: string;
  content?: string;
  /** Несколько визуально разделённых блоков (например режимы графика).
   *  Каждый блок: жирный подзаголовок + абзац. Если задано — рендерится
   *  вместо `content`. */
  sections?: { heading: string; body: string }[];
  /** Размер иконки. Default 16. */
  size?: number;
  /** Ссылка на страницу методологии. Если указана — иконка становится
   *  ссылкой, а hover показывает preview с кнопкой «Подробнее». */
  linkTo?: string;
  /** Сторона раскрытия поповера относительно иконки. 'left' (default) —
   *  левый край поповера у иконки, растёт вправо. 'right' — правый край у
   *  иконки, растёт ВЛЕВО (для «?» у правой границы блока, чтобы не вылезал). */
  align?: 'left' | 'right';
  /** Иконка триггера: 'help' (default, «?» — методология индикаторов) или
   *  'info' («i» в кружке — пояснения метрик, admin-stats). */
  icon?: 'help' | 'info';
  /** Поповер поверх всего, а не внутри потока: position:fixed по координатам
   *  иконки + прижим к вьюпорту. Нужен там, где «?» живёт внутри контейнера с
   *  overflow:hidden (модалки) — иначе подсказка обрезается его краем. */
  float?: boolean;
}

export default function HelpTooltip({ entry, title, content, sections, size = 16, linkTo, align = 'left', icon = 'help', float = false }: HelpTooltipProps) {
  const IconCmp = icon === 'info' ? Info : HelpCircle;
  const [open, setOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const popoverRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLElement>(null);
  // В float-режиме поповер уезжает порталом в body и теряет data-theme панели
  // песочницы (у каждой панели она своя) — фон/рамка резолвились бы от <html>,
  // то есть темой оболочки. Ставим атрибут всегда: в обычном (не портальном)
  // режиме и вне песочницы значение совпадает с тем, что и так наследуется.
  const { theme } = useTheme();

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

  // float-режим: координаты считаем от иконки и держим поповер в fixed-слое.
  // Пересчитываем на скролле (capture — ловим и скролл внутренних контейнеров)
  // и на ресайзе, иначе подсказка «отклеится» от «?» при прокрутке списка.
  const [floatPos, setFloatPos] = useState<{ top: number; left: number } | null>(null);
  useEffect(() => {
    if (!float || !open) { setFloatPos(null); return; }
    const place = () => {
      const el = triggerRef.current;
      if (!el) return;
      const r = el.getBoundingClientRect();
      const w = Math.min(360, window.innerWidth - 32);
      const raw = align === 'right' ? r.right - w : r.left;
      const left = Math.max(16, Math.min(raw, window.innerWidth - 16 - w));
      setFloatPos({ top: r.bottom + 6, left });
    };
    place();
    window.addEventListener('scroll', place, true);
    window.addEventListener('resize', place);
    return () => {
      window.removeEventListener('scroll', place, true);
      window.removeEventListener('resize', place);
    };
  }, [float, open, align]);

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
      data-theme={theme}
      role="tooltip"
      onMouseEnter={hoverOpen}
      onMouseLeave={hoverClose}
      style={{
        // fixed не режется overflow:hidden предков (у модалок он есть), пока
        // никто из них не создаёт containing block через transform/filter.
        ...(float
            ? { position: 'fixed', top: floatPos?.top ?? -9999, left: floatPos?.left ?? -9999 }
            : { position: 'absolute', top: 'calc(100% + 6px)', ...(align === 'right' ? { right: 0 } : { left: 0 }) }),
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
      {sections ? (
        sections.map((s, i) => (
          <div key={i} style={{ marginTop: i === 0 ? 0 : 'var(--sp-3)' }}>
            <p style={{ color: 'var(--text-primary)', fontWeight: 600, marginBottom: 'var(--sp-1)' }}>{s.heading}</p>
            <p style={{ margin: 0 }}>{s.body}</p>
          </div>
        ))
      ) : (
        fullText && <p style={{ marginBottom: linkTo ? 10 : 0 }}>{fullText}</p>
      )}
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
          <IconCmp size={size} strokeWidth={1.8} />
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
        aria-label={icon === 'info' ? 'Как считается метрика' : 'Подсказка о методологии'}
        aria-expanded={open}
      >
        <IconCmp size={size} strokeWidth={1.8} />
      </button>
      {/* ⚠️ В float-режиме поповер уходит ПОРТАЛОМ в body. Одного
          position:fixed мало: панель песочницы использует backdrop-filter, а он
          создаёт containing block — fixed становится относительным панели и
          режется её overflow:hidden. Подсказка «открывалась», её текст был в
          DOM, но на экране не появлялась. Тот же капкан ловили с палитрой
          цветов (см. ColorPicker). */}
      {float ? (popover ? createPortal(popover, document.body) : null) : popover}
    </span>
  );
}
