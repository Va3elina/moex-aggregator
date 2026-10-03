/**
 * HotCharts — компактные графики карточек витрины «Главное» (/hot).
 *
 * Оформление — как у графиков сайта (цена — var(--chart-line-1), позиция —
 * var(--oi-cyan) как «Чистая позиция» на «Открытых позициях», потоки —
 * --funds-flow-*, моно-подписи осей, плашка последнего значения), но движок
 * свой и под маленькую карточку: без навигатора и легенды, подсказка не
 * выходит за карточку, подписи не наезжают друг на друга. Событие видно
 * глазами: цветная зона (окно рекорда, две недели, день), кружки прошлых
 * пиков, пунктир прежнего максимума, подсвеченный столбик, скобка серии.
 *
 * Общие графики сайта (SimpleChart, FlowsHistogram…) не трогаем — этот файл
 * используется только витриной.
 */
import { useCallback, useMemo, useRef, useState } from 'react';
import type { MouseEvent as RMouseEvent, ReactNode } from 'react';
import { monthShort } from '../../i18n';
import type { HotBarsChart, HotLineChart, HotSeasonChart } from '../../services/api';

const H = 200;
const FONT = 'var(--font-mono)';
const POS = 'var(--oi-cyan, var(--chart-line-2))';
const PRICE = 'var(--chart-line-1)';
const UP = 'var(--funds-flow-positive)';
const DN = 'var(--funds-flow-negative)';
const ACC = 'var(--accent)';
const GRID = 'var(--chart-grid)';
const MUTED = 'var(--text-muted)';
const INK = 'var(--text-primary)';
const PANEL = 'var(--bg-secondary)';

const ts = (d: string) => Date.parse((d.length === 7 ? `${d}-15` : d) + 'T00:00:00Z');
const monY = (d: string) => `${monthShort(Number(d.slice(5, 7)) - 1)} ${d.slice(2, 4)}`;
const dayMon = (d: string) => `${Number(d.slice(8, 10))} ${monthShort(Number(d.slice(5, 7)) - 1)} ${d.slice(2, 4)}`;
const pct = (v: number) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(Math.round(v))}%`;
const num = (v: number, d = 0) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toLocaleString('ru-RU', { maximumFractionDigits: d })}`;
const fmtPrice = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: Math.abs(v) < 100 ? 1 : 0 });

function niceTicks(lo: number, hi: number, n: number): number[] {
  const raw = (hi - lo) / Math.max(n, 1);
  const p = Math.pow(10, Math.floor(Math.log10(raw || 1)));
  const f = raw / p;
  const step = (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * p;
  const out: number[] = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-9; v += step) out.push(+v.toFixed(6));
  return out;
}

const path = (pts: [number, number][]) => pts.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)}`).join('');

// Ширина контейнера: рисуем в реальных пикселях, шрифт не сжимается вместе с viewBox.
function useWidth(): [(el: HTMLDivElement | null) => void, number] {
  const [w, setW] = useState(0);
  const ro = useRef<ResizeObserver | null>(null);
  const ref = useCallback((el: HTMLDivElement | null) => {
    ro.current?.disconnect();
    ro.current = null;
    if (!el) return;
    setW(el.clientWidth);
    if (typeof ResizeObserver !== 'undefined') {
      ro.current = new ResizeObserver(() => setW(el.clientWidth));
      ro.current.observe(el);
    }
  }, []);
  return [ref, w];
}

// Подписи оси X без наложения: не ближе minGap пикселей.
function spaced<T extends { x: number }>(items: T[], minGap: number): T[] {
  const out: T[] = [];
  for (const it of items) if (!out.length || it.x - out[out.length - 1].x >= minGap) out.push(it);
  return out;
}

// Подсказка внутри карточки: прижимается к краям, не вылезает наружу.
function Tip({ x, y, w, rows }: { x: number; y: number; w: number; rows: ReactNode[] }) {
  const boxW = 178;
  const left = Math.min(Math.max(x + 10, 4), w - boxW - 4);
  const flip = x + 10 + boxW > w;
  return (
    <div style={{
      position: 'absolute', left: flip ? Math.max(4, x - boxW - 10) : left, top: Math.max(4, y - 10), width: boxW,
      pointerEvents: 'none', background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: 8,
      padding: '6px 8px', fontSize: 11, lineHeight: 1.45, color: INK, boxShadow: 'var(--shadow-sm, none)', zIndex: 5,
    }}>
      {rows.map((r, i) => <div key={i}>{r}</div>)}
    </div>
  );
}

/** Позиция физлиц (перекос, %) + цена: зона события, прошлые пики, прежний уровень. */
export function LineChartCard({ chart, priceLabel }: { chart: HotLineChart; priceLabel: string }) {
  const [ref, w] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const m = { l: 42, r: 46, t: 26, b: 22 };
  const data = chart.series;
  const n = data.length;
  const geo = useMemo(() => {
    if (!w || n < 2) return null;
    const x0 = ts(data[0][0]), x1 = ts(data[n - 1][0]);
    const X = (t: number) => m.l + ((t - x0) / Math.max(x1 - x0, 1)) * (w - m.l - m.r);
    const vals = data.map(d => d[1]);
    const extra = [chart.level?.value, chart.now.value].filter((v): v is number => v != null);
    let lo = Math.min(...vals, ...extra), hi = Math.max(...vals, ...extra);
    const pad = (hi - lo) * 0.12 || 5;
    lo = Math.max(-100, lo - pad); hi = Math.min(100, hi + pad);
    const Y = (v: number) => H - m.b - ((v - lo) / (hi - lo)) * (H - m.t - m.b);
    const pr = (chart.price ?? []).filter((v): v is number => v != null);
    let plo = Math.min(...pr), phi = Math.max(...pr);
    const ppad = (phi - plo) * 0.1 || 1;
    plo -= ppad; phi += ppad;
    const P = (v: number) => H - m.b - ((v - plo) / (phi - plo)) * (H - m.t - m.b);
    return { X, Y, P, lo, hi, plo, phi, x0, x1, hasPrice: pr.length > 1 };
  }, [w, data, n, chart.level?.value, chart.now.value, chart.price]);

  const onMove = (e: RMouseEvent<SVGRectElement>) => {
    if (!geo) return;
    const rect = (e.currentTarget as SVGRectElement).getBoundingClientRect();
    const px = e.clientX - rect.left + m.l;
    let best = 0, bd = Infinity;
    for (let i = 0; i < n; i++) {
      const d = Math.abs(geo.X(ts(data[i][0])) - px);
      if (d < bd) { bd = d; best = i; }
    }
    setHover(best);
  };

  if (!geo) return <div ref={ref} style={{ height: H }} />;
  const { X, Y, P } = geo;
  const pts = data.map(d => [X(ts(d[0])), Y(d[1])] as [number, number]);
  const span = (geo.x1 - geo.x0) / 864e5;
  // ось X: годы на длинном ряду, месяцы на коротком
  const xt: { x: number; label: string }[] = [];
  if (span > 700) {
    for (let y = new Date(geo.x0).getUTCFullYear() + 1; y <= new Date(geo.x1).getUTCFullYear(); y++)
      xt.push({ x: X(Date.UTC(y, 0, 1)), label: String(y) });
  } else {
    const d0 = new Date(geo.x0);
    for (let k = 1; k < 30; k++) {
      const t = Date.UTC(d0.getUTCFullYear(), d0.getUTCMonth() + k, 1);
      if (t > geo.x1) break;
      xt.push({ x: X(t), label: monthShort(new Date(t).getUTCMonth()) });
    }
  }
  const yt = niceTicks(geo.lo, geo.hi, 3);
  const ptk = geo.hasPrice ? niceTicks(geo.plo, geo.phi, 3) : [];
  const zx0 = chart.zone ? X(ts(chart.zone.from)) : null;
  const zx = zx0 == null ? null : Math.min(zx0, w - m.r - 16);
  const zoneW = zx == null ? 0 : w - m.r - zx;
  const zoneInside = zoneW >= 76;
  const nowX = X(ts(chart.now.date)), nowY = Y(chart.now.value);
  const iStart = chart.start ? data.findIndex(d => d[0] >= chart.start!.date) : -1;
  // подписи пиков: месяц и год, без наложения
  const peakLabels = spaced((chart.peaks ?? []).map(p => ({ x: X(ts(p[0])), y: Y(p[1]), d: p[0], v: p[1] })), 44);
  // подпись месяца у пика не кладём на подпись прежнего уровня (она слева, ~150 px)
  const lvY = chart.level?.value != null ? Y(chart.level.value) + (chart.level.value > chart.now.value ? -5 : 13) : null;
  const clashes = (p: { x: number; y: number; v: number }) => {
    if (lvY == null) return false;
    const ly = p.y + (p.v >= chart.now.value ? -9 : 15);
    return p.x < m.l + 170 && Math.abs(ly - lvY) < 14;
  };
  const hv = hover != null ? data[hover] : null;

  return (
    <div ref={ref} style={{ position: 'relative', height: H }} onMouseLeave={() => setHover(null)}>
      <svg width={w} height={H} style={{ display: 'block' }}>
        {zx != null && (
          <g>
            <rect x={zx} y={m.t - 8} width={zoneW} height={H - m.b - m.t + 8} fill={ACC} opacity={0.12} />
            <text x={zoneInside ? zx + 4 : zx - 4} y={m.t - 6} fontSize={10} fontWeight={700} fill={ACC}
              textAnchor={zoneInside ? 'start' : 'end'} dominantBaseline="hanging">{chart.zone!.label}</text>
          </g>
        )}
        {yt.map(v => (
          <g key={`y${v}`}>
            <line x1={m.l} x2={w - m.r} y1={Y(v)} y2={Y(v)} stroke={v === 0 ? MUTED : GRID} strokeWidth={1} />
            {Math.abs(Y(v) - nowY) > 13 && (
              <text x={w - m.r + 6} y={Y(v)} fontSize={10} fontFamily={FONT} fontWeight={700} fill={POS} dominantBaseline="central">{pct(v)}</text>
            )}
          </g>
        ))}
        {ptk.map(v => (
          <text key={`p${v}`} x={m.l - 6} y={P(v)} fontSize={10} fontFamily={FONT} fontWeight={700} fill={PRICE} textAnchor="end" dominantBaseline="central">{fmtPrice(v)}</text>
        ))}
        <g fontSize={10} fontWeight={600}>
          <circle cx={m.l + 4} cy={7} r={3.5} fill={POS} />
          <text x={m.l + 11} y={7} fill={MUTED} dominantBaseline="central">позиция физлиц</text>
          {geo.hasPrice && <>
            <circle cx={m.l + 112} cy={7} r={3.5} fill={PRICE} />
            <text x={m.l + 119} y={7} fill={MUTED} dominantBaseline="central">цена</text>
          </>}
        </g>
        {spaced(xt, 40).map(t => (
          <text key={`x${t.label}${t.x}`} x={t.x} y={H - 6} fontSize={10} fontFamily={FONT} fill={MUTED} textAnchor="middle">{t.label}</text>
        ))}
        {geo.hasPrice && (
          <path d={path((chart.price ?? []).map((v, i) => v == null ? null : [X(ts(data[i][0])), P(v)] as [number, number]).filter((p): p is [number, number] => !!p))}
            fill="none" stroke={PRICE} strokeWidth={1.3} opacity={0.55} />
        )}
        {chart.level?.value != null && (
          <g>
            <line x1={m.l} x2={w - m.r} y1={Y(chart.level.value)} y2={Y(chart.level.value)} stroke={ACC} strokeWidth={1.2} strokeDasharray="5 4" />
            <text x={m.l + 4} y={Y(chart.level.value) + (chart.level.value > chart.now.value ? -5 : 13)} fontSize={10} fill={ACC} fontWeight={600}
              paintOrder="stroke" stroke={PANEL} strokeWidth={3}>
              {chart.level.label} {pct(chart.level.value)}
            </text>
          </g>
        )}
        <path d={path(pts)} fill="none" stroke={POS} strokeWidth={1.9} strokeLinejoin="round" />
        {iStart >= 0 && <path d={path(pts.slice(iStart))} fill="none" stroke={ACC} strokeWidth={3.4} strokeLinejoin="round" strokeLinecap="round" />}
        {peakLabels.map(p => (
          <g key={`pk${p.d}`}>
            <circle cx={p.x} cy={p.y} r={4.5} fill={PANEL} stroke={INK} strokeWidth={1.6} />
            {!clashes(p) && <text x={p.x} y={p.y + (p.v >= chart.now.value ? -9 : 15)} fontSize={9.5} fill={MUTED} textAnchor="middle">{monY(p.d)}</text>}
          </g>
        ))}
        {iStart >= 0 && <circle cx={pts[iStart][0]} cy={pts[iStart][1]} r={4} fill={PANEL} stroke={ACC} strokeWidth={2} />}
        <circle cx={nowX} cy={nowY} r={5.5} fill={ACC} stroke={INK} strokeWidth={1.6} />
        <rect x={w - m.r + 2} y={nowY - 9} width={42} height={18} rx={4} fill={POS} />
        <text x={w - m.r + 23} y={nowY} fontSize={10.5} fontFamily={FONT} fontWeight={800} fill="var(--text-inverse)" textAnchor="middle" dominantBaseline="central">{pct(chart.now.value)}</text>
        {hv && <line x1={X(ts(hv[0]))} x2={X(ts(hv[0]))} y1={m.t - 8} y2={H - m.b} stroke={MUTED} strokeDasharray="2 3" />}
        <rect x={m.l} y={0} width={Math.max(0, w - m.l - m.r)} height={H - m.b} fill="transparent" onMouseMove={onMove} />
      </svg>
      {hv && hover != null && (
        <Tip x={X(ts(hv[0]))} y={Y(hv[1])} w={w} rows={[
          <span style={{ color: MUTED }}>{dayMon(hv[0])}</span>,
          <span><span style={{ color: POS, fontWeight: 700 }}>●</span> Позиция физлиц {pct(hv[1])}</span>,
          ...(chart.price?.[hover] != null ? [<span><span style={{ color: PRICE, fontWeight: 700 }}>●</span> {priceLabel} {fmtPrice(chart.price[hover]!)}</span>] : []),
        ]} />
      )}
    </div>
  );
}

/** Столбики потоков: событие оранжевым, прежний рекорд обведён и продлён пунктиром, серия — зоной. */
export function BarsChartCard({ chart }: { chart: HotBarsChart }) {
  const [ref, w] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const m = { l: 8, r: 8, t: 22, b: 22 };
  const bars = chart.bars;
  const n = bars.length;
  if (!w || !n) return <div ref={ref} style={{ height: H }} />;
  const bw = (w - m.l - m.r) / n;
  const vals = bars.map(b => b[1]);
  let lo = Math.min(0, ...vals, chart.level ?? 0), hi = Math.max(0, ...vals, chart.level ?? 0);
  const sp = hi - lo || 1;
  hi += sp * 0.16; lo -= sp * 0.16;
  const Y = (v: number) => H - m.b - ((v - lo) / (hi - lo)) * (H - m.t - m.b);
  const key = (b: [string, number]) => (chart.weekly ? b[0] : b[0].slice(0, 7));
  const inRun = (k: string) => !!chart.run && k >= chart.run.from && k <= chart.run.to;
  const runIdx = bars.map((b, i) => (inRun(key(b)) ? i : -1)).filter(i => i >= 0);
  const hlI = bars.findIndex(b => key(b) === chart.hl);
  const prevI = chart.prev ? bars.findIndex(b => key(b) === chart.prev) : -1;
  // подписи годов на границах
  const years = spaced(bars.map((b, i) => ({ x: m.l + i * bw, label: b[0].slice(0, 4), i }))
    .filter((t, i, arr) => i > 0 && t.label !== arr[i - 1].label), 36);
  const fmt = (v: number) => num(v, Math.abs(v) >= 10 ? 0 : 2);
  const label = (i: number, color: string) => {
    const v = bars[i][1], x = m.l + i * bw + bw / 2;
    const anchor = x > w - 30 ? 'end' : x < 30 ? 'start' : 'middle';
    return <text x={anchor === 'end' ? x + bw / 2 : x} y={v >= 0 ? Y(v) - 5 : Y(v) + 13} fontSize={10.5} fontFamily={FONT} fontWeight={800} fill={color} textAnchor={anchor}>{fmt(v)}</text>;
  };
  const hv = hover != null ? bars[hover] : null;
  return (
    <div ref={ref} style={{ position: 'relative', height: H }} onMouseLeave={() => setHover(null)}>
      <svg width={w} height={H} style={{ display: 'block' }}>
        {runIdx.length > 0 && (() => {
          const xa = m.l + runIdx[0] * bw, xb = m.l + (runIdx[runIdx.length - 1] + 1) * bw;
          return (
            <g>
              <rect x={xa} y={m.t - 10} width={xb - xa} height={H - m.b - m.t + 10} fill={ACC} opacity={0.1} />
              <text x={Math.min(xa + 4, w - m.r - 80)} y={m.t - 8} fontSize={10} fontWeight={700} fill={ACC} dominantBaseline="hanging">{chart.run!.label}</text>
            </g>
          );
        })()}
        <line x1={m.l} x2={w - m.r} y1={Y(0)} y2={Y(0)} stroke={MUTED} strokeWidth={1} />
        {bars.map((b, i) => {
          const v = b[1], x = m.l + i * bw;
          const isHl = i === hlI, isPrev = i === prevI;
          return (
            <rect key={b[0]} x={x + bw * 0.14} width={Math.max(1, bw * 0.72)} y={Math.min(Y(0), Y(v))}
              height={Math.max(1, Math.abs(Y(v) - Y(0)))}
              fill={isHl ? ACC : v >= 0 ? UP : DN} opacity={isHl || isPrev || inRun(key(b)) ? 1 : 0.45}
              stroke={isHl || isPrev ? INK : 'none'} strokeWidth={isHl || isPrev ? 1.3 : 0} />
          );
        })}
        {chart.level != null && (
          <line x1={m.l} x2={w - m.r} y1={Y(chart.level)} y2={Y(chart.level)} stroke={INK} strokeWidth={1} strokeDasharray="4 3" opacity={0.6} />
        )}
        {prevI >= 0 && label(prevI, INK)}
        {hlI >= 0 && label(hlI, ACC)}
        {years.map(t => <text key={`y${t.i}`} x={t.x + 2} y={H - 6} fontSize={10} fontFamily={FONT} fill={MUTED}>{t.label}</text>)}
        <rect x={m.l} y={0} width={w - m.l - m.r} height={H - m.b} fill="transparent"
          onMouseMove={e => {
            const rect = (e.currentTarget as SVGRectElement).getBoundingClientRect();
            setHover(Math.min(n - 1, Math.max(0, Math.floor((e.clientX - rect.left) / bw))));
          }} />
        {hv && <rect x={m.l + hover! * bw} y={m.t - 10} width={bw} height={H - m.b - m.t + 10} fill={INK} opacity={0.06} pointerEvents="none" />}
      </svg>
      {hv && (
        <Tip x={m.l + hover! * bw + bw / 2} y={Y(hv[1])} w={w} rows={[
          <span style={{ color: MUTED }}>{chart.weekly ? `неделя с ${dayMon(hv[0])}` : monY(hv[0])}</span>,
          <span style={{ fontWeight: 700, color: hv[1] >= 0 ? UP : DN }}>{fmt(hv[1])} {chart.unit}</span>,
        ]} />
      )}
    </div>
  );
}

/** Сезонность года: средняя кривая, текущий год, «сегодня» и зона следующих трёх месяцев. */
export function SeasonChartCard({ chart, label }: { chart: HotSeasonChart; label: string }) {
  const [ref, w] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const m = { l: 8, r: 42, t: 20, b: 22 };
  const avg = chart.avg, cur = chart.cur;
  if (!w || !avg.length) return <div ref={ref} style={{ height: H }} />;
  const tdMax = avg[avg.length - 1][0];
  const X = (td: number) => m.l + (td / Math.max(tdMax, 1)) * (w - m.l - m.r);
  const all = [...avg.map(a => a[1]), ...cur.map(c => c[1]), 0];
  let lo = Math.min(...all), hi = Math.max(...all);
  const pad = (hi - lo) * 0.12 || 2;
  lo -= pad; hi += pad;
  const Y = (v: number) => H - m.b - ((v - lo) / (hi - lo)) * (H - m.t - m.b);
  // месяцы — по первой точке каждого, не ближе 30 px
  const months = spaced(avg.filter((a, i) => i === 0 || a[2] !== avg[i - 1][2]).map(a => ({ x: X(a[0]), label: monthShort(a[2] - 1) })), 30);
  const yt = niceTicks(lo, hi, 3);
  const nowCur = cur[cur.length - 1];
  const hv = hover != null ? avg.find(a => a[0] === hover) : null;
  const hc = hover != null ? cur.find(c => c[0] === hover) : null;
  return (
    <div ref={ref} style={{ position: 'relative', height: H }} onMouseLeave={() => setHover(null)}>
      <svg width={w} height={H} style={{ display: 'block' }}>
        <rect x={X(chart.today)} y={m.t - 8} width={Math.max(3, X(chart.zone_to) - X(chart.today))} height={H - m.b - m.t + 8} fill={ACC} opacity={0.1} />
        <text x={X(chart.today) + 4} y={m.t - 8} fontSize={10} fontWeight={700} fill={ACC} dominantBaseline="hanging">3 месяца</text>
        {yt.map(v => (
          <g key={`y${v}`}>
            <line x1={m.l} x2={w - m.r} y1={Y(v)} y2={Y(v)} stroke={v === 0 ? MUTED : GRID} />
            <text x={w - m.r + 6} y={Y(v)} fontSize={10} fontFamily={FONT} fontWeight={700} fill={MUTED} dominantBaseline="central">{pct(v)}</text>
          </g>
        ))}
        {months.map(t => <text key={`m${t.x}`} x={t.x} y={H - 6} fontSize={10} fontFamily={FONT} fill={MUTED}>{t.label}</text>)}
        <path d={path(avg.map(a => [X(a[0]), Y(a[1])] as [number, number]))} fill="none" stroke={MUTED} strokeWidth={1.8} />
        <path d={path(cur.map(c => [X(c[0]), Y(c[1])] as [number, number]))} fill="none" stroke={ACC} strokeWidth={2} />
        <line x1={X(chart.today)} x2={X(chart.today)} y1={m.t - 8} y2={H - m.b} stroke={INK} strokeDasharray="3 3" opacity={0.5} />
        {nowCur && <circle cx={X(nowCur[0])} cy={Y(nowCur[1])} r={5} fill={ACC} stroke={INK} strokeWidth={1.5} />}
        <rect x={m.l} y={0} width={w - m.l - m.r} height={H - m.b} fill="transparent"
          onMouseMove={e => {
            const rect = (e.currentTarget as SVGRectElement).getBoundingClientRect();
            const td = Math.round(((e.clientX - rect.left) / (w - m.l - m.r)) * tdMax);
            setHover(Math.min(tdMax, Math.max(0, td)));
          }} />
        {hover != null && <line x1={X(hover)} x2={X(hover)} y1={m.t - 8} y2={H - m.b} stroke={MUTED} strokeDasharray="2 3" />}
      </svg>
      {hover != null && hv && (
        <Tip x={X(hover)} y={Y(hv[1])} w={w} rows={[
          <span style={{ color: MUTED }}>{monthShort(hv[2] - 1)}</span>,
          <span><span style={{ color: MUTED, fontWeight: 700 }}>●</span> В среднем {pct(hv[1])}</span>,
          ...(hc ? [<span><span style={{ color: ACC, fontWeight: 700 }}>●</span> {label} {pct(hc[1])}</span>] : []),
        ]} />
      )}
    </div>
  );
}
