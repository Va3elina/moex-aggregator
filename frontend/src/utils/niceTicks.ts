/**
 * niceScale — «круглая» шкала оси в стиле TradingView.
 *
 * По диапазону данных [min,max] подбирает круглый шаг (1/2/5×10ⁿ, D3 tickStep)
 * и ОКРУГЛЯЕТ ГРАНИЦЫ: низ вниз, верх вверх до кратного шагу. Возвращает
 * { min, max, ticks } — округлённые границы домена + значения делений от низа
 * до верха включительно. Так на оси всегда есть и верхнее, и нижнее число
 * (в отличие от «деления внутри сырого домена», где верх данных мог оставаться
 * без подписи).
 *
 *   niceScale(33, 66)  → { min: 30, max: 70, ticks: [30,40,50,60,70] }
 *   niceScale(12, 65)  → { min: 10, max: 70, ticks: [10,20,30,40,50,60,70] }
 *
 * Защита от вырожденных диапазонов: min==max → одна точка; странный шаг →
 * исходные границы; cap на число делений (50).
 */
function niceStep(rawStep: number): number {
  const pow = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const err = rawStep / pow;
  return (err >= 7.07 ? 10 : err >= 3.16 ? 5 : err >= 1.41 ? 2 : 1) * pow;
}

export function niceScale(
  min: number,
  max: number,
  count = 5,
): { min: number; max: number; ticks: number[] } {
  if (!Number.isFinite(min) || !Number.isFinite(max) || min === max) {
    return { min, max, ticks: [min] };
  }
  const lo = Math.min(min, max);
  const hi = Math.max(min, max);
  const step = niceStep((hi - lo) / Math.max(1, count));
  if (!Number.isFinite(step) || step <= 0) return { min: lo, max: hi, ticks: [lo, hi] };
  const niceMin = Math.floor(lo / step) * step;
  const niceMax = Math.ceil(hi / step) * step;
  const n = Math.round((niceMax - niceMin) / step);
  if (n < 0 || n > 50) return { min: lo, max: hi, ticks: [lo, hi] };
  const round = (v: number) => Math.round(v * 1e6) / 1e6; // убираем float-мусор
  const ticks: number[] = [];
  for (let i = 0; i <= n; i++) ticks.push(round(niceMin + i * step));
  return { min: round(niceMin), max: round(niceMax), ticks };
}
