/**
 * niceTickValues — «круглые» значения делений оси в стиле TradingView.
 *
 * Возвращает значения с круглым шагом (1/2/5×10ⁿ, алгоритм D3 tickStep),
 * лежащие ВНУТРИ [min,max]. Домен/масштаб вызывающий код НЕ меняет — деления
 * просто встают на круглые значения, линия и пилюля остаются на месте.
 *
 * Защита от вырожденных диапазонов: min==max → [min]; шаг ≤0 или странный
 * результат → [min,max]; жёсткий cap на число делений (50), чтобы не зациклить.
 *
 *   niceTickValues(-48765, 468591) → [0, 100000, 200000, 300000, 400000]
 *   niceTickValues(42.3, 118.7)    → [60, 80, 100]
 */
export function niceTickValues(min: number, max: number, count = 5): number[] {
  if (!Number.isFinite(min) || !Number.isFinite(max) || min === max) return [min];
  const lo = Math.min(min, max);
  const hi = Math.max(min, max);
  const rawStep = (hi - lo) / Math.max(1, count);
  const pow = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const err = rawStep / pow;
  const step = (err >= 7.07 ? 10 : err >= 3.16 ? 5 : err >= 1.41 ? 2 : 1) * pow;
  if (!Number.isFinite(step) || step <= 0) return [lo, hi];
  const start = Math.ceil(lo / step);
  const stop = Math.floor(hi / step);
  const n = stop - start;
  if (n < 0 || n > 50) return [lo, hi];
  const out: number[] = [];
  for (let i = 0; i <= n; i++) {
    // округляем, чтобы убить float-мусор (0.30000004 → 0.3)
    out.push(Math.round((start + i) * step * 1e6) / 1e6);
  }
  return out;
}
