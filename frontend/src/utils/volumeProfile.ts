/**
 * Профиль объёма — ЧИСТАЯ математика, без DOM и React (как indicators.ts).
 *
 * Принципиальное отличие от остальных индикаторов: это НЕ временной ряд. MA, RSI
 * и прочие дают значение на каждый бар, а профиль отвечает на другой вопрос —
 * сколько объёма наторговано на каждом ЦЕНОВОМ уровне. Поэтому он не может быть
 * серией lightweight-charts (у той одна точка на момент времени) и рисуется
 * примитивом поверх пейна, а считается по ВИДИМОМУ диапазону баров: сдвинули
 * график — профиль другой. Это ровно поведение VPVR в TradingView.
 *
 * РАСПРЕДЕЛЕНИЕ ОБЪЁМА. Свеча не торгуется по одной цене — она покрывает
 * диапазон low..high, и её объём размазывается по всем уровням, которые этот
 * диапазон пересекает, пропорционально длине пересечения. Альтернатива «весь
 * объём свечи на цену закрытия» проще, но даёт гребёнку из пиков на дневках,
 * где свеча перекрывает десяток уровней.
 *
 * ЗОНА СТОИМОСТИ (value area) считается канонически, парами уровней: от POC
 * шагаем наружу, каждый раз сравнивая сумму ДВУХ уровней сверху с суммой ДВУХ
 * снизу и забирая большую пару, пока не наберём заданную долю объёма. Наивный
 * вариант «по одному уровню за шаг» даёт другие границы на асимметричных
 * профилях, и цифры разойдутся с терминалом.
 */

/** Свеча профиля. Поля опциональны: ряд может быть не свечным (тогда профиля нет). */
export interface VpCandle {
  value: number;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
}

/** Ценовой уровень профиля. up/down — объём растущих и падающих свечей. */
export interface VpBucket {
  lo: number;
  hi: number;
  up: number;
  down: number;
  total: number;
}

export interface VpProfile {
  buckets: VpBucket[];
  /** Объём самого «толстого» уровня — им нормируется длина полос при отрисовке. */
  maxTotal: number;
  /** Point of Control — уровень с максимальным объёмом. */
  pocIndex: number;
  /** Границы зоны стоимости, ВКЛЮЧИТЕЛЬНО. */
  vaFrom: number;
  vaTo: number;
}

const clamp = (v: number, a: number, b: number) => (v < a ? a : v > b ? b : v);

/**
 * Профиль по срезу свечей [from..to] (индексы включительно, дробные округляются
 * наружу — так делает видимый логический диапазон графика).
 *
 * Возвращает null, если считать нечего: нет объёма, нет валидных цен или срез
 * пуст. Null — это «не рисовать», а не ошибка.
 */
export function volumeProfile(
  candles: VpCandle[],
  from: number,
  to: number,
  rows: number,
  valueAreaPct = 0.7,
): VpProfile | null {
  const n = candles.length;
  if (!n) return null;
  const i0 = clamp(Math.floor(from), 0, n - 1);
  const i1 = clamp(Math.ceil(to), 0, n - 1);
  if (i1 < i0) return null;

  let lo = Infinity, hi = -Infinity, anyVolume = false;
  for (let i = i0; i <= i1; i++) {
    const c = candles[i];
    const h = c.high ?? c.close ?? c.value;
    const l = c.low ?? c.close ?? c.value;
    if (!Number.isFinite(h) || !Number.isFinite(l)) continue;
    if (l < lo) lo = l;
    if (h > hi) hi = h;
    if (c.volume) anyVolume = true;
  }
  if (!anyVolume || !Number.isFinite(lo) || !Number.isFinite(hi)) return null;

  const R = clamp(Math.floor(rows), 4, 400);
  const span = hi - lo;
  // Вырожденный случай (вся видимая история по одной цене): один уровень нулевой
  // толщины сломал бы деление, поэтому даём ему искусственный шаг.
  const step = span > 0 ? span / R : 1;
  const buckets: VpBucket[] = [];
  for (let k = 0; k < R; k++) buckets.push({ lo: lo + k * step, hi: lo + (k + 1) * step, up: 0, down: 0, total: 0 });

  const put = (k: number, vol: number, up: boolean) => {
    const b = buckets[k];
    if (up) b.up += vol; else b.down += vol;
    b.total += vol;
  };

  for (let i = i0; i <= i1; i++) {
    const c = candles[i];
    const vol = c.volume;
    if (!vol || !Number.isFinite(vol) || vol <= 0) continue;
    const close = c.close ?? c.value;
    const up = close >= (c.open ?? close);
    const h = c.high ?? close;
    const l = c.low ?? close;
    const range = h - l;
    if (!(range > 0) || span === 0) {
      put(clamp(Math.floor((close - lo) / step), 0, R - 1), vol, up);
      continue;
    }
    const kFrom = clamp(Math.floor((l - lo) / step), 0, R - 1);
    const kTo = clamp(Math.floor((h - lo) / step), 0, R - 1);
    if (kFrom === kTo) { put(kFrom, vol, up); continue; }
    for (let k = kFrom; k <= kTo; k++) {
      const overlap = Math.min(h, buckets[k].hi) - Math.max(l, buckets[k].lo);
      if (overlap > 0) put(k, vol * (overlap / range), up);
    }
  }

  let pocIndex = 0, maxTotal = 0, grand = 0;
  for (let k = 0; k < R; k++) {
    grand += buckets[k].total;
    if (buckets[k].total > maxTotal) { maxTotal = buckets[k].total; pocIndex = k; }
  }
  if (maxTotal <= 0) return null;

  const { vaFrom, vaTo } = valueArea(buckets, pocIndex, grand, valueAreaPct);
  return { buckets, maxTotal, pocIndex, vaFrom, vaTo };
}

/** Расширение от POC парами уровней, пока не наберём долю объёма (см. шапку). */
function valueArea(buckets: VpBucket[], poc: number, grand: number, pct: number): { vaFrom: number; vaTo: number } {
  const target = grand * clamp(pct, 0.1, 1);
  let acc = buckets[poc].total;
  let above = poc + 1, below = poc - 1;
  const R = buckets.length;
  const pairAt = (i: number, dir: 1 | -1): number => {
    const a = buckets[i]?.total ?? 0;
    const b = buckets[i + dir]?.total ?? 0;
    return a + b;
  };
  while (acc < target && (above < R || below >= 0)) {
    const upAvail = above < R;
    const downAvail = below >= 0;
    // При равенстве идём вверх — лишь бы правило было детерминированным.
    const goUp = upAvail && (!downAvail || pairAt(above, 1) >= pairAt(below, -1));
    if (goUp) {
      acc += buckets[above].total;
      if (above + 1 < R) acc += buckets[above + 1].total;
      above += 2;
    } else {
      acc += buckets[below].total;
      if (below - 1 >= 0) acc += buckets[below - 1].total;
      below -= 2;
    }
  }
  return { vaFrom: clamp(below + 1, 0, R - 1), vaTo: clamp(above - 1, 0, R - 1) };
}
