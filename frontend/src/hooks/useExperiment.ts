/**
 * useExperiment — клиентский accessor для A/B variant.
 *
 * Использование:
 *   const { variant, label, loading } = useExperiment('homepage_v2');
 *   if (variant === 'b') return <NewHomepage />;
 *   return <OldHomepage />;
 *
 * Особенности:
 *  - Кеш в sessionStorage чтобы не делать fetch на каждый mount
 *  - Deterministic split на backend → один user всегда видит ту же variant
 *  - Если эксперимент не найден / не active → variant='a' (default fallback, safe)
 *  - Loading state — показывайте старую версию или skeleton (не B без assignment)
 */
import { useEffect, useState } from 'react';
import { getABAssignment, type AssignResponse } from '../services/api';

const CACHE_PREFIX = 'frame_ab_';

interface UseExperimentResult {
  variant: 'a' | 'b';
  label: string;
  active: boolean;
  loading: boolean;
}

export function useExperiment(name: string): UseExperimentResult {
  // Initial state — пробуем cache, fallback на 'a' default
  const [state, setState] = useState<UseExperimentResult>(() => {
    try {
      const cached = sessionStorage.getItem(CACHE_PREFIX + name);
      if (cached) {
        const parsed = JSON.parse(cached) as AssignResponse;
        return {
          variant: parsed.variant,
          label: parsed.label,
          active: parsed.active,
          loading: false,
        };
      }
    } catch { /* ignore */ }
    return { variant: 'a', label: 'default', active: false, loading: true };
  });

  useEffect(() => {
    let cancelled = false;
    getABAssignment(name)
      .then(r => {
        if (cancelled) return;
        try {
          sessionStorage.setItem(CACHE_PREFIX + name, JSON.stringify(r));
        } catch { /* ignore */ }
        setState({ variant: r.variant, label: r.label, active: r.active, loading: false });
      })
      .catch(() => {
        // Network error → fallback на 'a'. Loading=false чтобы UI не залип.
        if (cancelled) return;
        setState(s => ({ ...s, loading: false }));
      });
    return () => { cancelled = true; };
  }, [name]);

  return state;
}
