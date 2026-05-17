/**
 * OnboardingTour — spotlight-стиль гайд для индикаторов.
 *
 * Подсвечивает реальные UI-элементы на странице через SVG-маску (cutout)
 * + плавающий tooltip с пояснением рядом с целевым элементом.
 *
 * Использование:
 *   <OnboardingTour
 *     steps={oiTourSteps}
 *     open={shouldShowTour}
 *     onClose={() => markAsSeen()}
 *   />
 *
 * Step описывает:
 *   - selector: CSS-селектор подсвечиваемого элемента (или null = generic step)
 *   - title: заголовок tooltip'а
 *   - body: текст / JSX
 *   - position: где tooltip относительно target ('bottom' | 'top' | 'left' | 'right')
 *
 * Если элемент не найден или скрыт — step показывается как центрированная
 * модалка без spotlight'а (fallback на «общий» режим).
 *
 * Стиль: editorial-press (2px border + hard shadow + accent кнопки).
 */
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { ChevronLeft, ChevronRight, X } from 'lucide-react';

export interface TourStep {
  /** CSS-селектор элемента для spotlight. null = центрированная generic-модалка. */
  selector: string | null;
  /** Заголовок tooltip'а. */
  title: string;
  /** Тело — текст или React-нода. */
  body: ReactNode;
  /** Где разместить tooltip относительно target. По умолчанию 'bottom'. */
  position?: 'top' | 'bottom' | 'left' | 'right';
  /** Дополнительный padding вокруг target в spotlight cutout (px). По умолчанию 8. */
  spotlightPadding?: number;
}

export interface OnboardingTourProps {
  steps: TourStep[];
  open: boolean;
  onClose: () => void;
  /** Callback на «Закрыть» (X) — отличается от «Готово» на последнем шаге.
   *  По умолчанию = onClose. */
  onSkip?: () => void;
}

const TOOLTIP_WIDTH = 360;
const TOOLTIP_MOBILE_PADDING = 16;
const VIEWPORT_PADDING = 16;

export default function OnboardingTour({
  steps,
  open,
  onClose,
  onSkip,
}: OnboardingTourProps) {
  const [stepIndex, setStepIndex] = useState(0);
  const [targetRect, setTargetRect] = useState<DOMRect | null>(null);
  const [viewport, setViewport] = useState({ w: typeof window !== 'undefined' ? window.innerWidth : 1024, h: typeof window !== 'undefined' ? window.innerHeight : 768 });
  const overlayRef = useRef<HTMLDivElement>(null);

  const isMobile = viewport.w < 768;
  const step = steps[stepIndex];
  const isLastStep = stepIndex === steps.length - 1;
  const isFirstStep = stepIndex === 0;

  // Track viewport size
  useEffect(() => {
    if (!open) return;
    const handler = () => setViewport({ w: window.innerWidth, h: window.innerHeight });
    window.addEventListener('resize', handler);
    return () => window.removeEventListener('resize', handler);
  }, [open]);

  // Reset step index when tour reopens
  useEffect(() => {
    if (open) setStepIndex(0);
  }, [open]);

  // Find target element + scroll into view + measure rect
  useLayoutEffect(() => {
    if (!open || !step?.selector) {
      setTargetRect(null);
      return;
    }

    const updateRect = () => {
      const el = document.querySelector(step.selector!) as HTMLElement | null;
      if (!el) {
        setTargetRect(null);
        return;
      }
      const rect = el.getBoundingClientRect();
      setTargetRect(rect);
    };

    // Scroll target into view first
    const el = document.querySelector(step.selector) as HTMLElement | null;
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      // Wait for scroll to settle, then measure
      setTimeout(updateRect, 300);
    } else {
      updateRect();
    }

    // Re-measure on resize/scroll
    window.addEventListener('resize', updateRect);
    window.addEventListener('scroll', updateRect, true);
    return () => {
      window.removeEventListener('resize', updateRect);
      window.removeEventListener('scroll', updateRect, true);
    };
  }, [open, step?.selector, stepIndex]);

  // Keyboard navigation
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        (onSkip || onClose)();
      } else if (e.key === 'ArrowRight' || e.key === 'Enter') {
        e.preventDefault();
        if (isLastStep) onClose();
        else setStepIndex(i => i + 1);
      } else if (e.key === 'ArrowLeft') {
        e.preventDefault();
        if (!isFirstStep) setStepIndex(i => i - 1);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open, isLastStep, isFirstStep, onClose, onSkip]);

  const handleNext = useCallback(() => {
    if (isLastStep) onClose();
    else setStepIndex(i => i + 1);
  }, [isLastStep, onClose]);

  const handlePrev = useCallback(() => {
    if (!isFirstStep) setStepIndex(i => i - 1);
  }, [isFirstStep]);

  if (!open || !step) return null;

  // Tooltip position calculation
  const tooltipPos = calculateTooltipPosition({
    rect: targetRect,
    viewport,
    preferred: step.position,
    isMobile,
  });

  // Spotlight rect with padding
  const spotlightPad = step.spotlightPadding ?? 8;
  const spotlight = targetRect
    ? {
        x: targetRect.left - spotlightPad,
        y: targetRect.top - spotlightPad,
        w: targetRect.width + spotlightPad * 2,
        h: targetRect.height + spotlightPad * 2,
      }
    : null;

  return (
    <div
      ref={overlayRef}
      className="fixed inset-0 z-[9999]"
      style={{ pointerEvents: 'auto' }}
    >
      {/* SVG backdrop with cutout around target element */}
      <svg
        width={viewport.w}
        height={viewport.h}
        className="absolute inset-0"
        style={{ pointerEvents: 'none' }}
      >
        <defs>
          <mask id="tour-spotlight-mask">
            {/* White = visible (dark), Black = transparent (cutout) */}
            <rect x={0} y={0} width="100%" height="100%" fill="white" />
            {spotlight && (
              <rect
                x={spotlight.x}
                y={spotlight.y}
                width={spotlight.w}
                height={spotlight.h}
                rx={12}
                ry={12}
                fill="black"
              />
            )}
          </mask>
        </defs>
        <rect
          x={0}
          y={0}
          width="100%"
          height="100%"
          fill="rgba(0, 0, 0, 0.65)"
          mask="url(#tour-spotlight-mask)"
        />
        {/* Subtle accent glow around spotlight rect */}
        {spotlight && (
          <rect
            x={spotlight.x}
            y={spotlight.y}
            width={spotlight.w}
            height={spotlight.h}
            rx={12}
            ry={12}
            fill="none"
            stroke="var(--accent)"
            strokeWidth={2}
            opacity={0.9}
          />
        )}
      </svg>

      {/* Tooltip card */}
      <div
        className="absolute"
        style={{
          left: tooltipPos.x,
          top: tooltipPos.y,
          width: isMobile ? `calc(100vw - ${TOOLTIP_MOBILE_PADDING * 2}px)` : TOOLTIP_WIDTH,
          maxWidth: TOOLTIP_WIDTH,
          background: 'var(--bg-primary)',
          border: '2px solid var(--text-primary)',
          boxShadow: '5px 5px 0 0 var(--text-primary)',
          borderRadius: 'var(--sp-3)',
          padding: 'var(--sp-5)',
          pointerEvents: 'auto',
        }}
      >
        {/* Header: progress + close button */}
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center" style={{ gap: 4 }}>
            {steps.map((_, i) => (
              <span
                key={i}
                style={{
                  width: i === stepIndex ? 24 : 8,
                  height: 8,
                  borderRadius: 4,
                  background: i <= stepIndex ? 'var(--accent)' : 'var(--text-secondary)',
                  opacity: i <= stepIndex ? 1 : 0.4,
                  transition: 'all 200ms ease',
                }}
              />
            ))}
            <span
              className="text-theme-secondary ml-2"
              style={{ fontSize: 'var(--fs-2xs)' }}
            >
              {stepIndex + 1} / {steps.length}
            </span>
          </div>
          <button
            onClick={onSkip || onClose}
            className="text-theme-secondary hover:text-theme-primary transition-colors"
            aria-label="Закрыть тур"
            style={{ padding: 4 }}
          >
            <X size={16} />
          </button>
        </div>

        {/* Title */}
        <h3
          className="font-bold text-theme-primary mb-2"
          style={{ fontSize: 'var(--fs-lg)' }}
        >
          {step.title}
        </h3>

        {/* Body */}
        <div
          className="text-theme-secondary mb-4"
          style={{ fontSize: 'var(--fs-sm)', lineHeight: 1.5 }}
        >
          {step.body}
        </div>

        {/* Controls */}
        <div className="flex items-center justify-between" style={{ gap: 'var(--sp-2)' }}>
          <button
            onClick={onSkip || onClose}
            className="text-theme-secondary hover:text-theme-primary transition-colors"
            style={{ fontSize: 'var(--fs-xs)', padding: 'var(--sp-2) var(--sp-3)' }}
          >
            Пропустить
          </button>

          <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
            {!isFirstStep && (
              <button
                onClick={handlePrev}
                className="editorial-press font-semibold rounded-full flex items-center"
                style={{
                  background: 'var(--bg-secondary)',
                  color: 'var(--text-primary)',
                  border: '2px solid var(--text-primary)',
                  fontSize: 'var(--fs-sm)',
                  padding: 'var(--sp-2) var(--sp-3)',
                  gap: 4,
                }}
              >
                <ChevronLeft size={14} />
                Назад
              </button>
            )}
            <button
              onClick={handleNext}
              className="editorial-press font-semibold rounded-full flex items-center"
              style={{
                background: 'var(--accent)',
                color: 'var(--text-inverse)',
                border: '2px solid var(--text-primary)',
                boxShadow: 'var(--shadow-hard-chip)',
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-2) var(--sp-3)',
                gap: 4,
              }}
            >
              {isLastStep ? 'Готово' : 'Далее'}
              {!isLastStep && <ChevronRight size={14} />}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────
// Helper: tooltip position calculation
// ─────────────────────────────────────────────────────────────────────

function calculateTooltipPosition({
  rect,
  viewport,
  preferred = 'bottom',
  isMobile,
}: {
  rect: DOMRect | null;
  viewport: { w: number; h: number };
  preferred?: 'top' | 'bottom' | 'left' | 'right';
  isMobile: boolean;
}): { x: number; y: number } {
  const w = isMobile ? viewport.w - TOOLTIP_MOBILE_PADDING * 2 : TOOLTIP_WIDTH;
  const estimatedHeight = 280; // rough estimate, ok для positioning
  const gap = 16;

  // Generic mode (no target) — center
  if (!rect) {
    return {
      x: (viewport.w - w) / 2,
      y: (viewport.h - estimatedHeight) / 2,
    };
  }

  // На mobile всегда показываем снизу target'а — проще читается
  if (isMobile) {
    const x = TOOLTIP_MOBILE_PADDING;
    let y = rect.bottom + gap;
    if (y + estimatedHeight > viewport.h - VIEWPORT_PADDING) {
      // Если не помещается снизу — над target'ом
      y = rect.top - estimatedHeight - gap;
      if (y < VIEWPORT_PADDING) y = VIEWPORT_PADDING;
    }
    return { x, y };
  }

  // Desktop: try preferred position, fallback by available space
  const spaceBelow = viewport.h - rect.bottom - gap;
  const spaceAbove = rect.top - gap;
  const spaceRight = viewport.w - rect.right - gap;
  const spaceLeft = rect.left - gap;

  // Choose actual position based on space
  const positions: Array<'bottom' | 'top' | 'right' | 'left'> = [preferred, 'bottom', 'top', 'right', 'left'] as const as never;
  const fits = {
    bottom: spaceBelow >= estimatedHeight,
    top: spaceAbove >= estimatedHeight,
    right: spaceRight >= w,
    left: spaceLeft >= w,
  };

  let actual: 'top' | 'bottom' | 'left' | 'right' = preferred;
  for (const p of positions) {
    if (fits[p]) { actual = p; break; }
  }

  let x: number, y: number;
  if (actual === 'bottom') {
    x = rect.left + rect.width / 2 - w / 2;
    y = rect.bottom + gap;
  } else if (actual === 'top') {
    x = rect.left + rect.width / 2 - w / 2;
    y = rect.top - estimatedHeight - gap;
  } else if (actual === 'right') {
    x = rect.right + gap;
    y = rect.top + rect.height / 2 - estimatedHeight / 2;
  } else {
    x = rect.left - w - gap;
    y = rect.top + rect.height / 2 - estimatedHeight / 2;
  }

  // Clamp to viewport
  x = Math.max(VIEWPORT_PADDING, Math.min(x, viewport.w - w - VIEWPORT_PADDING));
  y = Math.max(VIEWPORT_PADDING, Math.min(y, viewport.h - estimatedHeight - VIEWPORT_PADDING));

  return { x, y };
}
