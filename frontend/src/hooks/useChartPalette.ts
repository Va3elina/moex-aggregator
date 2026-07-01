import { useCallback, useState } from 'react';

/**
 * Глобальная палитра графиков (accessibility). Покрывает ВСЕ типы нарушения
 * цветовосприятия, а не только красно-зелёный (одна палитра физически не может —
 * сине-оранжевая слепа для тританопов). Перекрывает цветовые CSS-переменные
 * графиков через `:root[data-palette="…"]` (index.css), поверх ЛЮБОЙ темы.
 * Настройка глобальная (не пер-график), живёт в localStorage.
 *
 *  - 'default'    — цвета темы (красно-зелёные пары).
 *  - 'redgreen'   — красно-зелёный дальтонизм (дейтер/протан, ~8% муж.): сине-янтарная.
 *  - 'blueyellow' — сине-жёлтый (тританопия): бирюза-роза.
 *  - 'contrast'   — полная цветовая слепота / макс. контраст: по ЯРКОСТИ, не по тону.
 */
export type ChartPalette = 'default' | 'redgreen' | 'blueyellow' | 'contrast';

const KEY = 'frame:chartPalette';
const NON_DEFAULT: ChartPalette[] = ['redgreen', 'blueyellow', 'contrast'];

export function readPalette(): ChartPalette {
  try {
    const v = localStorage.getItem(KEY) as ChartPalette | null;
    return v && NON_DEFAULT.includes(v) ? v : 'default';
  } catch {
    return 'default';
  }
}

/** Выставляет data-palette на <html> (или снимает для 'default'). Идемпотентно. */
export function applyPalette(p: ChartPalette): void {
  const root = document.documentElement;
  if (p === 'default') root.removeAttribute('data-palette');
  else root.setAttribute('data-palette', p);
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
