/**
 * ChartActionsMenu — kebab «⋮» в правом верхнем углу графика, сворачивающий
 * действия над графиком (Слои, Скриншот, CSV, Алерт) в одно меню.
 *
 * Зачем: контролы данных (актив/ТФ/период/режим) разрослись и выталкивали
 * иконки действий на вторую строку. Решение 2026-06-13: действия — иконками
 * внутри kebab-поповера в углу самого графика (паттерн TradingView), строка
 * контролов остаётся только под выбор данных.
 *
 * Монтирование через portal в `containerRef` (обёртка графика, position:relative):
 * JSX-вызов остаётся в строке контролов (рядом со state/handlers страницы), а
 * DOM рендерится в углу графика. Так не пришлось физически переносить большой
 * блок кнопок (с их конфигами) в другое место страницы.
 *
 * data-export-ignore: меню живёт ВНУТРИ снимаемой области графика, поэтому
 * контейнер/кнопка/поповер помечены, чтобы html2canvas не втянул их в PNG.
 *
 * Закрытие: клик-вне (mousedown) + ESC. Поповер прячем через display, НЕ
 * размонтируя children — у действия с НЕ-портируемой модалкой (CreateAlertModal —
 * DOM-потомок меню) она иначе пропала бы вместе с поповером. Клик внутри такой
 * модалки contains() считает «внутри ref» → меню не закроется. Модалки в body
 * (ExportModal, слои) — вне ref, клик по ним закроет меню, но сама модалка цела.
 */
import { useEffect, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { MoreVertical } from 'lucide-react';

interface ChartActionsMenuProps {
  children: ReactNode;
  /** Обёртка графика (position:relative), куда монтируется меню. */
  containerRef: React.RefObject<HTMLElement | null>;
  /** data-tour id на контейнере (для онбординг-тура) */
  tourId?: string;
}

export default function ChartActionsMenu({ children, containerRef, tourId }: ChartActionsMenuProps) {
  const [open, setOpen] = useState(false);
  const [host, setHost] = useState<HTMLElement | null>(null);
  const ref = useRef<HTMLDivElement>(null);

  // Хост-узел доступен только после маунта графика — берём его в эффекте.
  useEffect(() => { setHost(containerRef.current); }, [containerRef]);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', onDown);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('mousedown', onDown);
      document.removeEventListener('keydown', onKey);
    };
  }, [open]);

  if (!host) return null;

  return createPortal(
    <div
      ref={ref}
      data-tour={tourId}
      data-export-ignore="true"
      // Гасим всплытие мыши: на графиках с hover-перекрестьем (Сила рынка) меню
      // живёт ВНУТРИ контейнера графика → без этого наведение на kebab дёргало бы
      // crosshair. Outside-click это не ломает: клики ВНЕ меню до document доходят.
      onMouseDown={(e) => e.stopPropagation()}
      onMouseMove={(e) => e.stopPropagation()}
      style={{ position: 'absolute', top: 'var(--sp-2)', right: 'var(--sp-2)', zIndex: 20 }}
    >
      <button
        type="button"
        data-export-ignore="true"
        onClick={() => setOpen((o) => !o)}
        aria-label="Действия с графиком"
        aria-expanded={open}
        title="Действия с графиком"
        className="editorial-press rounded-full inline-flex items-center justify-center"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          color: 'var(--text-primary)',
          width: 40,
          height: 40,
        }}
      >
        <MoreVertical size={20} />
      </button>
      <div
        data-export-ignore="true"
        role="menu"
        style={{
          display: open ? 'flex' : 'none',
          flexDirection: 'column',
          alignItems: 'center',
          gap: 'var(--sp-2)',
          position: 'absolute',
          top: 'calc(100% + var(--sp-2))',
          right: 0,
          padding: 'var(--sp-2)',
          backgroundColor: 'var(--bg-primary)',
          border: '2px solid var(--text-primary)',
          borderRadius: 16,
          boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
        }}
      >
        {children}
      </div>
    </div>,
    host,
  );
}
