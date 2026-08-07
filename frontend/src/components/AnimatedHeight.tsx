/**
 * AnimatedHeight — обёртка, сглаживающая ЛЮБОЕ изменение высоты контента
 * (смена вкладки, фильтры, догрузка данных): вместо мгновенного прыжка —
 * CSS-переход высоты. Появилась для /oi (переход график ⇄ скринер дёргал
 * низ страницы на тысячи пикселей), но нарочно без привязки к странице.
 *
 * Механика «auto → px → auto»: в покое высота auto (ничего не клипается,
 * тултипы и дропдауны живут как обычно). ResizeObserver ловит изменение
 * высоты контента, на время перехода наружный див получает явную высоту
 * (старая → новая) + overflow hidden, по завершении возвращается auto.
 * Прерывание анимации новым изменением стартует с текущей промежуточной
 * высоты — без снапа.
 *
 * ⚠️ RO не стреляет в фоновой вкладке (колбэки привязаны к рендер-кадрам) —
 * это не баг: пока вкладку не видно, анимировать нечего; при показе браузер
 * доставит отложенное наблюдение и высота догонит контент.
 */
import { useLayoutEffect, useRef } from 'react';
import type { ReactNode } from 'react';

interface Props {
  children: ReactNode;
  /** Длительность перехода, мс. */
  duration?: number;
}

export default function AnimatedHeight({ children, duration = 280 }: Props) {
  const outerRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const outer = outerRef.current;
    const inner = innerRef.current;
    if (!outer || !inner) return;

    let prevH: number | null = null;
    let animating = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const settle = () => {
      animating = false;
      outer.style.transition = '';
      outer.style.height = 'auto';
      outer.style.overflow = '';
    };

    const ro = new ResizeObserver(() => {
      const target = inner.offsetHeight;
      // Старт: в покое — высота прошлого наблюдения (outer уже сравнялся с
      // контентом, из DOM старую не достать); в полёте — текущая
      // промежуточная высота outer, чтобы прерывание не снапало.
      const start = animating ? outer.offsetHeight : prevH;
      prevH = target;
      if (start == null || start === target) return;
      outer.style.transition = 'none';
      outer.style.height = `${start}px`;
      outer.style.overflow = 'hidden';
      void outer.offsetHeight; // reflow: зафиксировать старт до перехода
      animating = true;
      outer.style.transition = `height ${duration}ms ease`;
      outer.style.height = `${target}px`;
      clearTimeout(timer);
      timer = setTimeout(settle, duration + 30);
    });
    ro.observe(inner);
    return () => { ro.disconnect(); clearTimeout(timer); };
  }, [duration]);

  return (
    <div ref={outerRef}>
      <div ref={innerRef}>{children}</div>
    </div>
  );
}
