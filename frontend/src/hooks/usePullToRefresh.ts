/**
 * usePullToRefresh — нативный жест «потянуть вниз чтобы обновить» для мобилки.
 *
 * Принцип:
 *   - Listener регистрируется на document (touchstart/move/end)
 *   - Активируется только если scrollTop === 0 и palec лёг в верхних 100px
 *     viewport — это позволяет long-press на chart работать как раньше
 *     (chart начинается ниже header'а)
 *   - При pull > REFRESH_TRIGGER (70px) и release → вызывается onRefresh()
 *   - Indicator-distance отдаётся наружу через return — MobileLayout
 *     анимирует floating spinner
 *
 * Resistance: фактическая pullDistance = touchDiff * 0.5 (т.е. палец движется
 * быстрее чем индикатор). Создаёт ощущение «упругости», как в iOS Safari.
 *
 * Usage:
 *   const { pullDistance, isRefreshing } = usePullToRefresh(() => loadData());
 *   <Indicator distance={pullDistance} refreshing={isRefreshing} />
 */
import { useEffect, useRef, useState } from 'react';

const REFRESH_TRIGGER = 70;
const MAX_PULL = 110;
const PULL_AREA_HEIGHT = 100; // только верхние 100px viewport — pull зона
const RESISTANCE = 0.5;

export function usePullToRefresh(onRefresh: (() => Promise<void> | void) | undefined) {
  const [pullDistance, setPullDistance] = useState(0);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const startYRef = useRef(0);
  const pullingRef = useRef(false);
  const pullDistanceRef = useRef(0);

  useEffect(() => {
    if (!onRefresh) return;

    function onTouchStart(e: TouchEvent) {
      const scrollTop =
        window.scrollY ||
        document.documentElement.scrollTop ||
        document.body.scrollTop ||
        0;
      if (scrollTop > 0) return;

      const y = e.touches[0].clientY;
      if (y > PULL_AREA_HEIGHT) return;

      startYRef.current = y;
      pullingRef.current = true;
    }

    function onTouchMove(e: TouchEvent) {
      if (!pullingRef.current || isRefreshing) return;

      const diff = e.touches[0].clientY - startYRef.current;
      if (diff <= 0) {
        if (pullDistanceRef.current !== 0) {
          pullDistanceRef.current = 0;
          setPullDistance(0);
        }
        return;
      }

      const resisted = Math.min(diff * RESISTANCE, MAX_PULL);
      pullDistanceRef.current = resisted;
      setPullDistance(resisted);
    }

    async function onTouchEnd() {
      if (!pullingRef.current) return;
      pullingRef.current = false;

      const finalDistance = pullDistanceRef.current;
      if (finalDistance >= REFRESH_TRIGGER && !isRefreshing) {
        setIsRefreshing(true);
        try {
          await onRefresh?.();
        } catch (err) {
          console.error('Pull-to-refresh error:', err);
        } finally {
          setIsRefreshing(false);
          pullDistanceRef.current = 0;
          setPullDistance(0);
        }
      } else {
        pullDistanceRef.current = 0;
        setPullDistance(0);
      }
    }

    document.addEventListener('touchstart', onTouchStart, { passive: true });
    document.addEventListener('touchmove', onTouchMove, { passive: true });
    document.addEventListener('touchend', onTouchEnd, { passive: true });
    document.addEventListener('touchcancel', onTouchEnd, { passive: true });

    return () => {
      document.removeEventListener('touchstart', onTouchStart);
      document.removeEventListener('touchmove', onTouchMove);
      document.removeEventListener('touchend', onTouchEnd);
      document.removeEventListener('touchcancel', onTouchEnd);
    };
  }, [onRefresh, isRefreshing]);

  return { pullDistance, isRefreshing, trigger: REFRESH_TRIGGER };
}
