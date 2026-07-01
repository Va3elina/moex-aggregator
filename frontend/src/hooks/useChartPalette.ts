import { useCallback, useState } from 'react';

/**
 * Глобальная палитра графиков (accessibility). 'colorblind' = colorblind-safe
 * схема Okabe-Ito (сине-оранжевая вместо красно-зелёной) — перекрывает цветовые
 * CSS-переменные графиков через `:root[data-palette="colorblind"]` (index.css),
 * поверх ЛЮБОЙ темы. Настройка глобальная (не пер-график), живёт в localStorage.
 */
export type ChartPalette = 'default' | 'colorblind';

const KEY = 'frame:chartPalette';

export function readPalette(): ChartPalette {
  try {
    return localStorage.getItem(KEY) === 'colorblind' ? 'colorblind' : 'default';
  } catch {
    return 'default';
  }
}

/** Выставляет data-palette на <html> (или снимает). Идемпотентно. */
export function applyPalette(p: ChartPalette): void {
  const root = document.documentElement;
  if (p === 'colorblind') root.setAttribute('data-palette', 'colorblind');
  else root.removeAttribute('data-palette');
}

export function useChartPalette(): [ChartPalette, (p: ChartPalette) => void] {
  const [palette, setState] = useState<ChartPalette>(readPalette);
  const setPalette = useCallback((p: ChartPalette) => {
    setState(p);
    try { localStorage.setItem(KEY, p); } catch { /* private mode — не критично */ }
    applyPalette(p);
  }, []);
  return [palette, setPalette];
}
