/**
 * MobileCompanyChart — график раздела «По бумаге» («Сделки фондов») на
 * мобилке. Мобильный аналог пары CompanyFlowsPriceMap / CompanyShareChart:
 * те же данные и формулы, но оформление — как у остальных мобильных
 * индикаторов (StrengthDualChart, MobileFlowsHistogram, MobileChart):
 *   - один SVG на всю площадь, размер через ResizeObserver;
 *   - правая шкала ПОВЕРХ графика (не в жёлобе), pill'ы последних значений;
 *   - touch → crosshair сквозь панели + плавающий tooltip с разбивкой по фондам;
 *   - без навигатора/легенды/водяного знака — контекст даёт шапка страницы.
 *
 * Режимы (общий ряд режимов CompanyFlowsTab):
 *   map      — недельная линия цены + кругляши месячных чистых сделок фондов
 *              (зелёный = нетто-покупка, красный = нетто-продажа, площадь ∝
 *              |нетто|, нормировка по 95-му процентилю окна, «зашкал» — кольцо);
 *   rub      — сверху линия цены, снизу бары позиции фондов в ₽ (Σ СЧА×доля);
 *   cap      — то же, бары в % от free-float капы (ffcap);
 *   overhang — то же, бары в днях оборота (позиция / медианный дневной оборот).
 * Нет истории цены (облигация/ОФЗ) → у share-режимов гистограмма во всю
 * высоту, у «Сделок» — empty-state (кругляши без линии цены бессмысленны).
 *
 * Ось X — МЕСЯЦЫ (слоты). Недели цены раскладываются внутри слота своего
 * месяца дробно ((k+0.5)/K): линия гладкая, бар/кругляш месяца совпадает со
 * своим участком линии — как на десктопе.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { FUND_PALETTE } from '../../config/chartTheme';
import { axisFontSize } from '../chart/chartTypography';
import type { CompanyFlowsSeries } from '../fundtrades/CompanyFlowsHistogram';
import type { CompanyShareFundSeries, ShareMode } from '../fundtrades/CompanyShareChart';

export type MobileCompanyMode = 'map' | ShareMode;

export interface MobileCompanyChartProps {
  mode: MobileCompanyMode;
  /** "YYYY-MM" — ось месяцев потоков (/company-flows), уже обрезана периодом. */
  flowMonths: string[];
  /** Серии фондов (₽ по месяцам, выровнено с flowMonths), уже отфильтрованы. */
  flowSeries: CompanyFlowsSeries[];
  /** "YYYY-MM" — ось месяцев позиции (/company-weights), уже обрезана периодом. */
  shareMonths: string[];
  /** Серии фондов (доля % и СЧА по месяцам, выровнено с shareMonths). */
  shareFunds: CompanyShareFundSeries[];
  /** Free-float капа (₽) по shareMonths — знаменатель «% в обращении». */
  ffcap?: (number | null)[];
  /** Медианный дневной оборот (₽) по shareMonths — знаменатель «Навеса». */
  turnover?: (number | null)[];
  /** ISO-даты понедельников недель, ASC — вся история цены. */
  weeks: string[];
  /** Недельные закрытия, выровнено с weeks. */
  closes: number[];
  loading?: boolean;
  /** Все фонды сняты пользователем — empty-state. */
  noFundsSelected?: boolean;
  /** Нет истории цены (не акция): share-режимы без верхней панели, map — заглушка. */
  priceMissing?: boolean;
}

// Порог обрезки пустого левого хвоста в «Сделках» — как в десктопных чартах.
const MIN_VISIBLE_FLOW_MLN = 1;

// Кругляши сделок: площадь ∝ |нетто| (r ∝ sqrt), потолок зависит от плотности
// окна — на мобиле слоты уже, поэтому потолок ниже десктопного (24 → 18).
const R_MAX = 18;
const R_DOT = 2;
const REF_MARKERS = 24;
const R_MAX_DENSE = 9;
const NORM_QUANTILE = 0.95;
const RING_OUTSET = 3.5;

// Цвета — те же, что на десктопе: линия цены в «Сделках» — индиго (не спорит с
// красными кругляшами продаж), в share-режимах — акцент (цена главная), бары
// позиции — индиго (доля без знака: зелёный/красный тут были бы ложью).
const MAP_LINE_COLOR = FUND_PALETTE[0];
const SHARE_LINE_COLOR = 'var(--accent)';
const BAR_COLOR = FUND_PALETTE[0];
const COLOR_BUY = 'var(--funds-flow-positive, #5BD49C)';
const COLOR_SELL = 'var(--funds-flow-negative, #FF7A5C)';
const AXIS_FILL = 'color-mix(in srgb, var(--text-primary) 55%, transparent)';
const GRID_STROKE = 'color-mix(in srgb, var(--text-primary) 8%, transparent)';

// ── Форматтеры (1-в-1 с десктопными чартами) ──
function fmtMlnNumber(abs: number): string {
  return abs >= 10 ? Math.round(abs).toLocaleString('ru-RU') : abs.toFixed(1);
}
function fmtFlow(v: number): string {
  const sign = v > 0 ? '+' : v < 0 ? '−' : '';
  return `${sign}${fmtMlnNumber(Math.abs(v))} млн ₽`;
}
function fmtPrice(v: number): string {
  if (v >= 1000) return Math.round(v).toLocaleString('ru-RU');
  if (v >= 100) return v.toFixed(1);
  if (v >= 1) return v.toFixed(2);
  return v.toFixed(4);
}
function fmtPct(v: number): string {
  const d = v >= 10 ? 1 : v >= 0.1 ? 2 : 3;
  return `${v.toLocaleString('ru-RU', { maximumFractionDigits: d })}%`;
}
function fmtRub(v: number, suffix = true): string {
  const sfx = suffix ? ' ₽' : '';
  if (v === 0) return `0${sfx}`;
  if (v >= 1e9) {
    const b = v / 1e9;
    const d = b >= 100 ? 0 : b >= 10 ? 1 : 2;
    return `${b.toLocaleString('ru-RU', { maximumFractionDigits: d })} млрд${sfx}`;
  }
  if (v >= 1e6) {
    const m = v / 1e6;
    return `${m.toLocaleString('ru-RU', { maximumFractionDigits: m >= 10 ? 0 : 1 })} млн${sfx}`;
  }
  return `${(v / 1e3).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} тыс${sfx}`;
}
function fmtDays(v: number): string {
  const d = v >= 10 ? 0 : v >= 1 ? 1 : 2;
  return `${v.toLocaleString('ru-RU', { maximumFractionDigits: d })} дн`;
}
const MONTH_SHORT = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'];
function monthShort(m: string): string {
  const mm = parseInt(m.slice(5, 7), 10);
  return `${MONTH_SHORT[(mm - 1 + 12) % 12]} '${m.slice(2, 4)}`;
}
function monthLong(m: string): string {
  return new Date(`${m}-01T00:00:00`).toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' });
}

/** Pill последнего значения у правого края (правый край = x, растём влево). */
function Pill({ x, y, text, color, fontSize }: { x: number; y: number; text: string; color: string; fontSize: number }) {
  const charW = fontSize * 0.55;
  const padX = 4;
  const padY = 2;
  const w = text.length * charW + padX * 2;
  const h = fontSize + padY * 2;
  const pillX = x - w - 2;
  return (
    <g pointerEvents="none">
      <rect x={pillX} y={y - h / 2} width={w} height={h} rx={3} fill={color} />
      <text
        x={pillX + w / 2}
        y={y}
        textAnchor="middle"
        dominantBaseline="central"
        fontSize={fontSize}
        fontWeight={700}
        fill="#fff"
        style={{ fontFamily: 'IBM Plex Mono, monospace' }}
      >
        {text}
      </text>
    </g>
  );
}

function EmptyState({ text }: { text: string }) {
  return (
    <div
      style={{
        position: 'absolute',
        inset: 0,
        display: 'grid',
        placeItems: 'center',
        padding: 24,
        textAlign: 'center',
        color: 'var(--text-muted)',
        fontSize: 'var(--fs-sm)',
        lineHeight: 1.5,
      }}
    >
      {text}
    </div>
  );
}

export default function MobileCompanyChart({
  mode,
  flowMonths,
  flowSeries,
  shareMonths,
  shareFunds,
  ffcap,
  turnover,
  weeks: weeksAll,
  closes: closesAll,
  loading = false,
  noFundsSelected = false,
  priceMissing = false,
}: MobileCompanyChartProps) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 360, h: 420 });
  const [hoverMi, setHoverMi] = useState<number | null>(null);
  const [touchX, setTouchX] = useState<number | null>(null);

  useEffect(() => {
    if (!wrapRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        const r = e.contentRect;
        if (r.width > 0 && r.height > 0) {
          // Width: threshold 1px (fullscreen subpixel); height: 8px, чтобы
          // iOS Safari address bar не дёргал чарт (урок MobileChart).
          setSize((prev) => {
            const widthChanged = Math.abs(prev.w - r.width) >= 1;
            const heightChanged = Math.abs(prev.h - r.height) >= 8;
            if (!widthChanged && !heightChanged) return prev;
            return {
              w: widthChanged ? Math.max(200, r.width) : prev.w,
              h: heightChanged ? Math.max(200, r.height) : prev.h,
            };
          });
        }
      }
    });
    ro.observe(wrapRef.current);
    return () => ro.disconnect();
  }, []);

  const isMap = mode === 'map';
  const shareMode: ShareMode = isMap ? 'rub' : mode;

  // ── Значения месяцев ──
  // map: нетто сделок (млн ₽) = сумма по переданным (уже отфильтрованным) фондам.
  const netMlnAll = useMemo(() => {
    const n = flowMonths.length;
    const out = new Array<number | null>(n).fill(null);
    for (const s of flowSeries) {
      for (let i = 0; i < n; i++) {
        const v = s.values[i];
        if (v == null || Number.isNaN(v)) continue;
        out[i] = (out[i] ?? 0) + v / 1e6;
      }
    }
    return out;
  }, [flowMonths, flowSeries]);

  // share: rub — Σ(СЧА×доля); cap — / ffcap ×100; overhang — / оборот (дни).
  // Месяц без единого полного снапшота или без СЧА — null (дыра, не ноль).
  const shareValsAll = useMemo(() => {
    return shareMonths.map((_, i) => {
      let rub = 0;
      let hasNav = false;
      let cnt = 0;
      for (const f of shareFunds) {
        const w = f.weights[i];
        if (w == null) continue;
        cnt++;
        const nav = f.navs[i];
        if (nav != null && nav > 0) { rub += nav * (w / 100); hasNav = true; }
      }
      if (cnt === 0 || !hasNav) return null;
      if (shareMode === 'rub') return rub;
      if (shareMode === 'overhang') {
        const t = turnover?.[i];
        return t != null && t > 0 ? rub / t : null;
      }
      const cap = ffcap?.[i];
      return cap != null && cap > 0 ? (rub / cap) * 100 : null;
    });
  }, [shareMonths, shareFunds, ffcap, turnover, shareMode]);

  // ── Обрезка пустого левого хвоста (как на десктопе) ──
  const trimStart = useMemo(() => {
    if (isMap) {
      for (let i = 0; i < flowMonths.length; i++) {
        const v = netMlnAll[i];
        if (v != null && Math.abs(v) >= MIN_VISIBLE_FLOW_MLN) return i;
      }
      return 0;
    }
    for (let i = 0; i < shareMonths.length; i++) {
      const v = shareValsAll[i];
      if (v != null && v > 0) return i;
    }
    return 0;
  }, [isMap, flowMonths, netMlnAll, shareMonths, shareValsAll]);

  const months = useMemo(
    () => (isMap ? flowMonths : shareMonths).slice(trimStart),
    [isMap, flowMonths, shareMonths, trimStart],
  );
  const netMln = useMemo(() => netMlnAll.slice(trimStart), [netMlnAll, trimStart]);
  const shareVals = useMemo(() => shareValsAll.slice(trimStart), [shareValsAll, trimStart]);
  const M = months.length;

  // ── Недели цены по слотам месяцев (только внутри окна месяцев) ──
  const weeksByMonth = useMemo(() => {
    const map = new Map<string, number[]>();
    if (!M) return map;
    const lo = months[0];
    const hi = months[M - 1];
    for (let i = 0; i < weeksAll.length; i++) {
      const m = weeksAll[i].slice(0, 7);
      if (m < lo || m > hi) continue;
      const v = closesAll[i];
      if (v == null || !(v > 0)) continue;
      const arr = map.get(m);
      if (arr) arr.push(i);
      else map.set(m, [i]);
    }
    return map;
  }, [months, M, weeksAll, closesAll]);
  const priceCount = useMemo(() => {
    let n = 0;
    weeksByMonth.forEach((arr) => { n += arr.length; });
    return n;
  }, [weeksByMonth]);
  const hasPrice = !priceMissing && priceCount > 1;

  // ── Геометрия панелей ──
  const W = size.w;
  const H = size.h;
  const axisFs = axisFontSize(W);
  const PAD_X = 6;
  const PAD_TOP = 16;
  const PAD_BOTTOM = 22;
  const MID_GAP = 18;
  // map: одна панель (цена + кругляши). share: две панели при наличии цены,
  // иначе одна гистограмма во всю высоту.
  const twoPane = !isMap && hasPrice;
  const totalInnerH = H - PAD_TOP - PAD_BOTTOM - (twoPane ? MID_GAP : 0);
  const topH = isMap ? totalInnerH : twoPane ? totalInnerH * 0.5 : 0;
  const botH = totalInnerH - topH;
  const topY0 = PAD_TOP;
  const topY1 = topY0 + topH;
  const botY0 = topY1 + (twoPane ? MID_GAP : 0);
  const botY1 = botY0 + botH;
  const innerW = W - PAD_X * 2;
  const slotW = M > 0 ? innerW / M : 0;
  const slotX = (mi: number, frac = 0.5) => PAD_X + (mi + frac) * slotW;

  // Шкала цены — по всем неделям окна, поля 6%.
  const priceRange = useMemo(() => {
    if (!hasPrice) return { min: 0, span: 1 };
    let lo = Infinity;
    let hi = -Infinity;
    weeksByMonth.forEach((arr) => {
      for (const wi of arr) {
        const v = closesAll[wi];
        if (v < lo) lo = v;
        if (v > hi) hi = v;
      }
    });
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) return { min: 0, span: 1 };
    if (lo === hi) { lo *= 0.97; hi *= 1.03; }
    const pad = (hi - lo) * 0.06;
    return { min: lo - pad, span: (hi - lo) + pad * 2 };
  }, [hasPrice, weeksByMonth, closesAll]);
  const yPrice = (v: number) => topY1 - ((v - priceRange.min) / priceRange.span) * topH;

  // Линия цены: недели по слотам, дробно внутри слота.
  const linePath = useMemo(() => {
    if (!hasPrice || !M) return '';
    const parts: string[] = [];
    for (let mi = 0; mi < M; mi++) {
      const wk = weeksByMonth.get(months[mi]);
      if (!wk || !wk.length) continue;
      for (let k = 0; k < wk.length; k++) {
        const x = PAD_X + (mi + (k + 0.5) / wk.length) * slotW;
        const y = topY1 - ((closesAll[wk[k]] - priceRange.min) / priceRange.span) * topH;
        parts.push(`${parts.length === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`);
      }
    }
    return parts.join(' ');
  }, [hasPrice, M, months, weeksByMonth, closesAll, slotW, topY1, topH, priceRange]);

  // Закрытие месяца (последняя неделя) и его точка на линии.
  const monthClose = (mi: number): number | null => {
    const wk = weeksByMonth.get(months[mi]);
    return wk && wk.length ? closesAll[wk[wk.length - 1]] : null;
  };
  const monthDot = (mi: number): { x: number; y: number } | null => {
    const wk = weeksByMonth.get(months[mi]);
    if (!wk || !wk.length) return null;
    const K = wk.length;
    return { x: slotX(mi, (K - 0.5) / K), y: yPrice(closesAll[wk[K - 1]]) };
  };
  const lastPrice = useMemo(() => {
    if (!hasPrice) return null;
    for (let mi = M - 1; mi >= 0; mi--) {
      const wk = weeksByMonth.get(months[mi]);
      if (wk && wk.length) return closesAll[wk[wk.length - 1]];
    }
    return null;
  }, [hasPrice, M, months, weeksByMonth, closesAll]);

  // ── Кругляши сделок (map): месяц с нетто ≠ 0 и неделей цены → якорь на
  // последней неделе месяца. Нормировка размера — 95-й процентиль |нетто|.
  const markers = useMemo(() => {
    if (!isMap || !hasPrice) return [] as { mi: number; net: number }[];
    const out: { mi: number; net: number }[] = [];
    for (let mi = 0; mi < M; mi++) {
      const net = netMln[mi];
      if (net == null || net === 0) continue;
      const wk = weeksByMonth.get(months[mi]);
      if (!wk || !wk.length) continue;
      out.push({ mi, net });
    }
    return out;
  }, [isMap, hasPrice, M, netMln, weeksByMonth, months]);
  const normAbsNet = useMemo(() => {
    const sorted = markers.map((m) => Math.abs(m.net)).sort((a, b) => a - b);
    if (!sorted.length) return 0.001;
    const pos = NORM_QUANTILE * (sorted.length - 1);
    const lo = Math.floor(pos);
    const hi = Math.ceil(pos);
    const q = sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
    return Math.max(q, 0.001);
  }, [markers]);
  const rMax = markers.length <= REF_MARKERS
    ? R_MAX
    : Math.max(R_MAX_DENSE, R_MAX * Math.sqrt(REF_MARKERS / markers.length));
  const rFor = (net: number) =>
    Math.max(rMax * Math.sqrt(Math.min(Math.abs(net) / normAbsNet, 1)), R_DOT);
  const isOverflow = (net: number) => Math.abs(net) > normAbsNet;

  // ── Шкала гистограммы (share): 0..max ×1.12, чтобы бар максимума не
  // упирался в верхнюю грид-линию.
  const shareMax = useMemo(() => {
    let mx = 0;
    for (const v of shareVals) if (v != null && v > mx) mx = v;
    return (mx || 0.0001) * 1.12;
  }, [shareVals]);
  const yShare = (v: number) => botY1 - (v / shareMax) * botH;
  const lastShare = useMemo(() => {
    for (let mi = M - 1; mi >= 0; mi--) {
      const v = shareVals[mi];
      if (v != null) return v;
    }
    return null;
  }, [M, shareVals]);
  const fmtVal = (v: number) => (shareMode === 'rub' ? fmtRub(v)
    : shareMode === 'overhang' ? fmtDays(v) : fmtPct(v));
  const fmtAxis = (v: number) => (shareMode === 'rub' ? fmtRub(v, false)
    : shareMode === 'overhang' ? fmtDays(v) : fmtPct(v));
  // Ширина бара: 66% слота, но не шире 22px.
  const barW = Math.min(slotW * 0.66, 22);

  // Бары растут из нуля при смене режима/данных (как MobileFlowsHistogram).
  const [animated, setAnimated] = useState(false);
  useEffect(() => {
    setAnimated(false);
    const t = setTimeout(() => setAnimated(true), 30);
    return () => clearTimeout(t);
  }, [shareVals, mode]);

  // ── X-подписи: 4 равномерных месяца ──
  const xTicks = useMemo(() => {
    if (M < 2) return M === 1 ? [0] : [];
    const count = Math.min(4, M);
    return Array.from({ length: count }, (_, i) => Math.round((i / (count - 1)) * (M - 1)));
  }, [M]);
  const xLabelAt = (mi: number, ti: number): { x: number; anchor: 'start' | 'middle' | 'end' } => {
    if (ti === 0) return { x: PAD_X, anchor: 'start' };
    if (ti === xTicks.length - 1) return { x: PAD_X + innerW, anchor: 'end' };
    return { x: slotX(mi), anchor: 'middle' };
  };

  // ── Touch → слот месяца ──
  const updateHover = (clientX: number) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect || slotW <= 0) return;
    const relX = clientX - rect.left;
    setTouchX(relX);
    const mi = Math.max(0, Math.min(M - 1, Math.floor((relX - PAD_X) / slotW)));
    setHoverMi((prev) => {
      if (prev === mi) return prev;
      if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
        navigator.vibrate(8);
      }
      return mi;
    });
  };
  const clearHover = () => { setHoverMi(null); setTouchX(null); };

  // Разбивка тултипа по фондам (top-4): map — млн ₽ по |вкладу|; share — в
  // единицах режима (rub ₽ / cap % / overhang дни, знаменатель общий).
  const hoverRows = useMemo(() => {
    if (hoverMi == null) return [] as { label: string; color: string; text: string }[];
    const ai = hoverMi + trimStart;
    if (isMap) {
      return flowSeries
        .map((s) => {
          const raw = s.values[ai];
          return raw == null || Number.isNaN(raw) || raw === 0
            ? null
            : { label: s.label, color: s.color, mln: raw / 1e6 };
        })
        .filter((r): r is { label: string; color: string; mln: number } => r != null)
        .sort((a, b) => Math.abs(b.mln) - Math.abs(a.mln))
        .slice(0, 4)
        .map((r) => ({ label: r.label, color: r.color, text: fmtFlow(r.mln) }));
    }
    const cap = ffcap?.[ai];
    const turn = turnover?.[ai];
    return shareFunds
      .map((f) => {
        const w = f.weights[ai];
        if (w == null || w === 0) return null;
        const nav = f.navs[ai];
        if (nav == null || nav <= 0) return null;
        const rub = nav * (w / 100);
        if (shareMode === 'rub') return { label: f.label, color: f.color, v: rub };
        if (shareMode === 'overhang') return turn != null && turn > 0 ? { label: f.label, color: f.color, v: rub / turn } : null;
        return cap != null && cap > 0 ? { label: f.label, color: f.color, v: (rub / cap) * 100 } : null;
      })
      .filter((r): r is { label: string; color: string; v: number } => r != null)
      .sort((a, b) => b.v - a.v)
      .slice(0, 4)
      .map((r) => ({ label: r.label, color: r.color, text: fmtVal(r.v) }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hoverMi, trimStart, isMap, flowSeries, shareFunds, ffcap, turnover, shareMode]);

  // ── Empty-states ──
  let empty: string | null = null;
  if (noFundsSelected) empty = 'Не выбрано ни одного фонда. Включите фонды в «Опциях».';
  else if (isMap && priceMissing) empty = 'Нет истории цены по этой бумаге — «Сделки» на графике цены недоступны. Выберите режим «Позиция» в «Опциях».';
  else if (M === 0) empty = loading ? '' : 'Нет данных по этой бумаге.';
  // «Сделки» без линии цены (цена ещё едет / нет недель в окне) — пусто, а не
  // голые оси: кругляши сажать не на что.
  else if (isMap && !hasPrice) empty = loading ? '' : 'Нет истории цены за выбранный период.';

  // Подписи шкал — оверлей поверх графика с подложкой цвета фона.
  const axisTextProps = {
    x: W - 4,
    fontSize: axisFs,
    fontWeight: 600,
    fill: AXIS_FILL,
    stroke: 'var(--bg-primary)',
    strokeWidth: 3,
    strokeLinejoin: 'round' as const,
    paintOrder: 'stroke' as const,
    textAnchor: 'end' as const,
    pointerEvents: 'none' as const,
  };
  const lineColor = isMap ? MAP_LINE_COLOR : SHARE_LINE_COLOR;
  const hovered = hoverMi != null && hoverMi < M ? hoverMi : null;

  return (
    <div ref={wrapRef} style={{ position: 'relative', width: '100%', height: '100%' }}>
      {empty != null ? (
        <EmptyState text={empty} />
      ) : (
        <svg
          width={W}
          height={H}
          style={{ display: 'block', userSelect: 'none', touchAction: 'none' }}
          onTouchStart={(e) => updateHover(e.touches[0].clientX)}
          onTouchMove={(e) => updateHover(e.touches[0].clientX)}
          onTouchEnd={clearHover}
          onTouchCancel={clearHover}
        >
          {/* ── Верхняя панель: цена (map: единственная панель) ── */}
          {hasPrice && (
            <>
              {[0.2, 0.5, 0.8].map((t) => (
                <line
                  key={`gt-${t}`}
                  x1={PAD_X}
                  y1={topY0 + topH * t}
                  x2={PAD_X + innerW}
                  y2={topY0 + topH * t}
                  stroke={GRID_STROKE}
                  strokeWidth={1}
                />
              ))}
              <path
                d={linePath}
                fill="none"
                stroke={lineColor}
                strokeWidth={2}
                strokeLinejoin="round"
                strokeLinecap="round"
              />
              {/* Кругляши месячных сделок — на линии цены */}
              {isMap && markers.map((m) => {
                const d = monthDot(m.mi);
                if (!d) return null;
                const r = rFor(m.net);
                const color = m.net > 0 ? COLOR_BUY : COLOR_SELL;
                const dim = hovered != null && hovered !== m.mi;
                return (
                  <g key={`mk-${m.mi}`} opacity={dim ? 0.45 : 1} style={{ transition: 'opacity 150ms ease' }}>
                    <circle cx={d.x} cy={d.y} r={r} fill={color} fillOpacity={0.82} stroke="var(--bg-primary)" strokeWidth={1} />
                    {isOverflow(m.net) && (
                      <circle cx={d.x} cy={d.y} r={r + RING_OUTSET} fill="none" stroke={color} strokeWidth={1.5} opacity={0.7} />
                    )}
                  </g>
                );
              })}
              {[0.2, 0.5, 0.8].map((t) => {
                const v = priceRange.min + priceRange.span * (1 - t);
                return (
                  <text key={`yp-${t}`} {...axisTextProps} y={topY0 + topH * t} dominantBaseline="central">
                    {fmtPrice(v)}
                  </text>
                );
              })}
              {lastPrice != null && (
                <Pill x={W} y={yPrice(lastPrice)} text={fmtPrice(lastPrice)} color={lineColor} fontSize={axisFs} />
              )}
              {/* X-подписи между панелями (две панели), как в «Силе рынка» */}
              {twoPane && xTicks.map((mi, ti) => {
                const { x, anchor } = xLabelAt(mi, ti);
                return (
                  <text
                    key={`xm-${mi}`}
                    x={x}
                    y={topY1 + MID_GAP - 4}
                    fontSize={axisFs}
                    fontWeight={600}
                    fill="var(--text-secondary)"
                    textAnchor={anchor}
                  >
                    {monthShort(months[mi])}
                  </text>
                );
              })}
            </>
          )}

          {/* ── Нижняя панель: бары позиции (share-режимы) ── */}
          {!isMap && (
            <>
              {[0.2, 0.5, 0.8].map((t) => (
                <line
                  key={`gb-${t}`}
                  x1={PAD_X}
                  y1={botY0 + botH * t}
                  x2={PAD_X + innerW}
                  y2={botY0 + botH * t}
                  stroke={GRID_STROKE}
                  strokeWidth={1}
                />
              ))}
              <line
                x1={PAD_X}
                y1={botY1}
                x2={PAD_X + innerW}
                y2={botY1}
                stroke="color-mix(in srgb, var(--text-primary) 25%, transparent)"
                strokeWidth={1}
              />
              {shareVals.map((v, mi) => {
                if (v == null || v <= 0) return null;
                const finalH = (v / shareMax) * botH;
                const h = animated ? finalH : 0;
                const delay = Math.min(mi * 4, 200);
                return (
                  <rect
                    key={`bar-${mi}`}
                    x={slotX(mi) - barW / 2}
                    y={botY1 - h}
                    width={barW}
                    height={h}
                    fill={BAR_COLOR}
                    opacity={hovered == null || hovered === mi ? 0.9 : 0.45}
                    rx={1.5}
                    style={{
                      transition: `y 500ms cubic-bezier(0.34, 1.56, 0.64, 1) ${delay}ms, height 500ms cubic-bezier(0.34, 1.56, 0.64, 1) ${delay}ms, opacity 150ms ease`,
                    }}
                  />
                );
              })}
              {[0.2, 0.5, 0.8].map((t) => {
                const v = shareMax * (1 - t);
                return (
                  <text key={`ys-${t}`} {...axisTextProps} y={botY0 + botH * t} dominantBaseline="central">
                    {fmtAxis(v)}
                  </text>
                );
              })}
              {lastShare != null && (
                <Pill x={W} y={yShare(lastShare)} text={fmtAxis(lastShare)} color={BAR_COLOR} fontSize={axisFs} />
              )}
            </>
          )}

          {/* Crosshair сквозь панели + точка на линии цены */}
          {hovered != null && (
            <>
              <line
                x1={slotX(hovered)}
                y1={hasPrice ? topY0 : botY0}
                x2={slotX(hovered)}
                y2={isMap ? topY1 : botY1}
                stroke="var(--text-primary)"
                strokeWidth={1}
                strokeDasharray="3,3"
                opacity={0.6}
                pointerEvents="none"
              />
              {hasPrice && (() => {
                const d = monthDot(hovered);
                if (!d) return null;
                return (
                  <circle cx={d.x} cy={d.y} r={4} fill={lineColor} stroke="var(--bg-primary)" strokeWidth={2} pointerEvents="none" />
                );
              })()}
            </>
          )}

          {/* X-подписи снизу */}
          {xTicks.map((mi, ti) => {
            const { x, anchor } = xLabelAt(mi, ti);
            return (
              <text
                key={`xb-${mi}`}
                x={x}
                y={H - 6}
                fontSize={axisFs}
                fontWeight={600}
                fill="var(--text-secondary)"
                textAnchor={anchor}
                dominantBaseline="alphabetic"
              >
                {monthShort(months[mi])}
              </text>
            );
          })}
        </svg>
      )}

      {/* Плавающий tooltip — следует за пальцем, clamped по ширине */}
      {empty == null && hovered != null && touchX !== null && (() => {
        const tooltipW = 210;
        const margin = 8;
        const left = Math.max(margin, Math.min(touchX - tooltipW / 2, W - tooltipW - margin));
        const close = hasPrice ? monthClose(hovered) : null;
        const net = netMln[hovered];
        const share = shareVals[hovered];
        return (
          <div
            style={{
              position: 'absolute',
              top: 4,
              left,
              width: tooltipW,
              padding: '6px 10px',
              background: 'var(--bg-primary)',
              border: '1.5px solid var(--text-primary)',
              borderRadius: 8,
              fontSize: 'var(--fs-xs)',
              pointerEvents: 'none',
              display: 'flex',
              flexDirection: 'column',
              gap: 3,
            }}
          >
            <div style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)', fontWeight: 600, letterSpacing: '0.02em' }}>
              {monthLong(months[hovered])}
            </div>
            {close != null && (
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                <span style={{ color: lineColor, fontWeight: 700 }}>Цена</span>
                <span className="mono" style={{ fontWeight: 800 }}>{fmtPrice(close)}</span>
              </div>
            )}
            {isMap ? (
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                <span style={{ color: (net ?? 0) >= 0 ? COLOR_BUY : COLOR_SELL, fontWeight: 700 }}>
                  {(net ?? 0) >= 0 ? 'Чистая покупка' : 'Чистая продажа'}
                </span>
                <span className="mono" style={{ fontWeight: 800 }}>{net != null ? fmtFlow(net) : '—'}</span>
              </div>
            ) : (
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
                <span style={{ color: BAR_COLOR, fontWeight: 700 }}>
                  {shareMode === 'rub' ? 'Позиция' : shareMode === 'overhang' ? 'Навес' : '% в обращении'}
                </span>
                <span className="mono" style={{ fontWeight: 800 }}>{share != null ? fmtVal(share) : '—'}</span>
              </div>
            )}
            {hoverRows.map((r) => (
              <div key={r.label} style={{ display: 'flex', justifyContent: 'space-between', gap: 8, fontSize: 'var(--fs-2xs)' }}>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5, minWidth: 0 }}>
                  <span style={{ width: 7, height: 7, borderRadius: '50%', background: r.color, flexShrink: 0 }} />
                  <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)' }}>{r.label}</span>
                </span>
                <span className="mono" style={{ fontWeight: 700, flexShrink: 0 }}>{r.text}</span>
              </div>
            ))}
          </div>
        );
      })()}
    </div>
  );
}
