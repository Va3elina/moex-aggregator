/**
 * Технические индикаторы — ЧИСТЫЕ функции, без DOM и без React.
 *
 * Считаем на клиенте, не на сервере. Замер (5000 баров, весь набор из шести
 * индикаторов) — 2.7 мс, тогда как пересоздание шести серий на графике с заливкой
 * данных — 100 мс. То есть математика в ~47 раз дешевле отрисовки: предрасчёт на
 * бэкенде оптимизировал бы 2% времени, принеся новые ручки API, инвалидацию кэша
 * и тарифные гейты на них. Все функции — один проход, O(n).
 *
 * СГЛАЖИВАНИЕ УАЙЛДЕРА в RSI и ATR — принципиально. Если взять простое скользящее
 * среднее, значения разойдутся с TradingView и любым терминалом, и пользователь
 * будет видеть «не тот RSI». Уайлдер: prev*(n-1)/n + cur/n, затравка — простое
 * среднее первых n приращений.
 *
 * ПРОГРЕВ ОБРЕЗАЕМ, а не заполняем нулями/NaN: точки до выхода индикатора на режим
 * просто не выдаются. Нули растянули бы ценовую шкалу панели до нуля и превратили
 * бы индикатор в плоскую полоску у края.
 *
 * Время параметризовано типом: у нас точки живут и с числовым временем (LwPoint),
 * и со строковым (локальный тип Series в ОИ) — конвертация в секунды происходит
 * позже, при сборке LwSeries.
 */

/** Точка ряда: время + значение. `value` для свечей = close (инвариант LwPoint). */
export interface IndPoint<T = number> { time: T; value: number }

/** Свеча: нужна ATR (high/low/close) и объёму (volume, close/open для цвета). */
export interface IndCandle<T = number> extends IndPoint<T> {
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
}

/** Простое скользящее среднее. Бегущая сумма → O(n) независимо от периода. */
export function sma<T>(pts: IndPoint<T>[], length: number): IndPoint<T>[] {
  const n = Math.max(1, Math.floor(length));
  if (pts.length < n) return [];
  const out: IndPoint<T>[] = [];
  let sum = 0;
  for (let i = 0; i < pts.length; i++) {
    sum += pts[i].value;
    if (i >= n) sum -= pts[i - n].value;
    if (i >= n - 1) out.push({ time: pts[i].time, value: sum / n });
  }
  return out;
}

/**
 * Экспоненциальное скользящее среднее. Затравка — SMA первых n значений (так же
 * делает ta.ema в Pine Script): если стартовать с первой цены, первые десятки
 * баров заметно расходятся с терминалом.
 */
export function ema<T>(pts: IndPoint<T>[], length: number): IndPoint<T>[] {
  const n = Math.max(1, Math.floor(length));
  if (pts.length < n) return [];
  const k = 2 / (n + 1);
  const out: IndPoint<T>[] = [];
  let acc = 0;
  for (let i = 0; i < n; i++) acc += pts[i].value;
  let e = acc / n;
  out.push({ time: pts[n - 1].time, value: e });
  for (let i = n; i < pts.length; i++) {
    e = pts[i].value * k + e * (1 - k);
    out.push({ time: pts[i].time, value: e });
  }
  return out;
}

/**
 * Полосы Боллинджера. Стандартное отклонение — ГЕНЕРАЛЬНОЕ (делим на n, не на
 * n-1): так считает ta.stdev в Pine, иначе полосы будут чуть шире, чем в терминале.
 * Считаем через бегущие суммы value и value² — один проход.
 */
export function bollinger<T>(
  pts: IndPoint<T>[],
  length: number,
  mult: number,
): { mid: IndPoint<T>[]; upper: IndPoint<T>[]; lower: IndPoint<T>[] } {
  const n = Math.max(1, Math.floor(length));
  const m = mult;
  const mid: IndPoint<T>[] = [], upper: IndPoint<T>[] = [], lower: IndPoint<T>[] = [];
  if (pts.length < n) return { mid, upper, lower };
  let s = 0, s2 = 0;
  for (let i = 0; i < pts.length; i++) {
    const v = pts[i].value;
    s += v; s2 += v * v;
    if (i >= n) { const o = pts[i - n].value; s -= o; s2 -= o * o; }
    if (i >= n - 1) {
      const mu = s / n;
      // max(0, …) — защита от отрицательного результата из-за потери точности
      // на длинных рядах с большими значениями (у ОИ это сотни тысяч).
      const sd = Math.sqrt(Math.max(0, s2 / n - mu * mu));
      const t = pts[i].time;
      mid.push({ time: t, value: mu });
      upper.push({ time: t, value: mu + m * sd });
      lower.push({ time: t, value: mu - m * sd });
    }
  }
  return { mid, upper, lower };
}

/**
 * RSI по Уайлдеру. Первое значение — на баре с индексом n (нужно n приращений).
 * Крайние случаи: нет падений → 100, нет ростов → 0 (иначе деление на ноль).
 */
export function rsi<T>(pts: IndPoint<T>[], length: number): IndPoint<T>[] {
  const n = Math.max(1, Math.floor(length));
  if (pts.length <= n) return [];
  const out: IndPoint<T>[] = [];
  let avgGain = 0, avgLoss = 0;
  for (let i = 1; i <= n; i++) {
    const ch = pts[i].value - pts[i - 1].value;
    avgGain += (ch > 0 ? ch : 0) / n;
    avgLoss += (ch < 0 ? -ch : 0) / n;
  }
  const val = (g: number, l: number) => (l === 0 ? (g === 0 ? 50 : 100) : 100 - 100 / (1 + g / l));
  out.push({ time: pts[n].time, value: val(avgGain, avgLoss) });
  for (let i = n + 1; i < pts.length; i++) {
    const ch = pts[i].value - pts[i - 1].value;
    avgGain = (avgGain * (n - 1) + (ch > 0 ? ch : 0)) / n;
    avgLoss = (avgLoss * (n - 1) + (ch < 0 ? -ch : 0)) / n;
    out.push({ time: pts[i].time, value: val(avgGain, avgLoss) });
  }
  return out;
}

/**
 * ATR по Уайлдеру. True range = max(high-low, |high-prevClose|, |low-prevClose|).
 * Если high/low не пришли (ряд не свечной — например ОИ), деградируем до
 * |Δvalue|: это перестаёт быть каноническим ATR, но остаётся осмысленной мерой
 * волатильности ряда и не роняет расчёт.
 */
export function atr<T>(candles: IndCandle<T>[], length: number): IndPoint<T>[] {
  const n = Math.max(1, Math.floor(length));
  if (candles.length <= n) return [];
  const tr = (i: number): number => {
    const c = candles[i], p = candles[i - 1];
    const pc = p.close ?? p.value;
    if (c.high == null || c.low == null) return Math.abs(c.value - p.value);
    return Math.max(c.high - c.low, Math.abs(c.high - pc), Math.abs(c.low - pc));
  };
  const out: IndPoint<T>[] = [];
  let acc = 0;
  for (let i = 1; i <= n; i++) acc += tr(i);
  let a = acc / n;
  out.push({ time: candles[n].time, value: a });
  for (let i = n + 1; i < candles.length; i++) {
    a = (a * (n - 1) + tr(i)) / n;
    out.push({ time: candles[i].time, value: a });
  }
  return out;
}

/**
 * Источник цены — то, ЧТО именно скармливается индикатору. В терминалах это
 * отдельная настройка, потому что RSI по максимумам и RSI по закрытиям — разные
 * ряды, и сигналы у них расходятся. hlc3 («типичная цена») и ohlc4 сглаживают
 * шум внутри бара, hlcc4 даёт закрытию двойной вес.
 */
export type IndSource = 'close' | 'open' | 'high' | 'low' | 'hl2' | 'hlc3' | 'ohlc4' | 'hlcc4';

export const SOURCE_LABELS: Record<IndSource, string> = {
  close: 'Цена закрытия', open: 'Цена открытия', high: 'Максимум', low: 'Минимум',
  hl2: '(макс+мин)/2', hlc3: '(макс+мин+закр)/3', ohlc4: '(откр+макс+мин+закр)/4',
  hlcc4: '(макс+мин+закр+закр)/4',
};

/** Пересобрать ряд под выбранный источник. Если OHLC не пришли (ряд не свечной),
 *  всё вырождается в value — расчёт не падает, просто источник ни на что не влияет. */
export function withSource<T>(candles: IndCandle<T>[], src: IndSource = 'close'): IndPoint<T>[] {
  if (src === 'close') return candles.map((c) => ({ time: c.time, value: c.close ?? c.value }));
  return candles.map((c) => {
    const o = c.open ?? c.value, h = c.high ?? c.value, l = c.low ?? c.value, cl = c.close ?? c.value;
    const v = src === 'open' ? o
      : src === 'high' ? h
      : src === 'low' ? l
      : src === 'hl2' ? (h + l) / 2
      : src === 'hlc3' ? (h + l + cl) / 3
      : src === 'ohlc4' ? (o + h + l + cl) / 4
      : src === 'hlcc4' ? (h + l + cl + cl) / 4
      : cl;
    return { time: c.time, value: v };
  });
}

/** Взвешенное скользящее среднее: вес линейно растёт к свежим барам. */
export function wma<T>(pts: IndPoint<T>[], length: number): IndPoint<T>[] {
  const n = Math.max(1, Math.floor(length));
  if (pts.length < n) return [];
  const denom = (n * (n + 1)) / 2;
  const out: IndPoint<T>[] = [];
  for (let i = n - 1; i < pts.length; i++) {
    let acc = 0;
    for (let k = 0; k < n; k++) acc += pts[i - n + 1 + k].value * (k + 1);
    out.push({ time: pts[i].time, value: acc / denom });
  }
  return out;
}

/**
 * Сглаживание Уайлдера (в Pine — ta.rma, в терминалах «сглаженная/накатная»).
 * Тот же рекуррент, что внутри RSI и ATR: prev*(n-1)/n + cur/n.
 */
export function rma<T>(pts: IndPoint<T>[], length: number): IndPoint<T>[] {
  const n = Math.max(1, Math.floor(length));
  if (pts.length < n) return [];
  let acc = 0;
  for (let i = 0; i < n; i++) acc += pts[i].value;
  let a = acc / n;
  const out: IndPoint<T>[] = [{ time: pts[n - 1].time, value: a }];
  for (let i = n; i < pts.length; i++) {
    a = (a * (n - 1) + pts[i].value) / n;
    out.push({ time: pts[i].time, value: a });
  }
  return out;
}

/** Цвета баров объёма: растущая свеча — зелёный токен, падающая — красный. */
export const VOLUME_UP = 'var(--oi-green)';
export const VOLUME_DOWN = 'var(--oi-red)';

/**
 * Объём как гистограмма. Не расчёт, а раскраска: значение берём как есть, цвет
 * бара — по направлению свечи. Бары без объёма пропускаем (у ОИ на некоторых
 * контрактах он не приходит), иначе получим ложные нули.
 */
export function volumeBars<T>(candles: IndCandle<T>[]): (IndPoint<T> & { color: string })[] {
  const out: (IndPoint<T> & { color: string })[] = [];
  for (const c of candles) {
    if (c.volume == null) continue;
    const open = c.open ?? c.value;
    const close = c.close ?? c.value;
    out.push({ time: c.time, value: c.volume, color: close >= open ? VOLUME_UP : VOLUME_DOWN });
  }
  return out;
}
