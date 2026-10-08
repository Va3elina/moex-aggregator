/**
 * HotCharts — компактные графики карточек витрины «Главное» (/hot).
 *
 * Оформление — как у графиков сайта (цена — var(--chart-line-1), лонги и шорты —
 * зелёный и красный, как покупки и продажи на «Открытых позициях», потоки —
 * --funds-flow-*, моно-подписи осей, плашка последнего значения), но движок
 * свой и под маленькую карточку: без навигатора, подсказка не выходит за
 * карточку. Подписи только в строке над графиком (легенда слева, период
 * события справа) и на осях — на сами линии текст не ложится. Событие видно
 * глазами: цветная зона со скобкой сверху (две недели, день, серия), у позиций —
 * одна линия (нога сигнала), прошлый случай — точкой по наведению в списке под
 * графиком, плашка значения — за правым краем поля.
 *
 * Общие графики сайта (SimpleChart, FlowsHistogram…) не трогаем — этот файл
 * используется только витриной.
 */
import { useCallback, useMemo, useRef, useState } from 'react';
import type { MouseEvent as RMouseEvent, ReactNode } from 'react';
import { monthShort } from '../../i18n';
import type { HotBarsChart, HotLegsChart, HotPastCase, HotSeasonChart } from '../../services/api';

const H = 200;
const FONT = 'var(--font-mono)';
const LONG = 'var(--funds-flow-positive)';
const LEG_NAME = { long: 'Лонги', short: 'Шорты', nl: 'Людей в лонге', ns: 'Людей в шорте' } as const;
const SHORT = 'var(--funds-flow-negative)';
const PRICE = 'var(--chart-line-1)';
const UP = 'var(--funds-flow-positive)';
const DN = 'var(--funds-flow-negative)';
const ACC = 'var(--accent)';
const GRID = 'var(--chart-grid)';
const MUTED = 'var(--text-muted)';
const INK = 'var(--text-primary)';
const PANEL = 'var(--bg-secondary)';

const HEAD_Y = 8;        // строка подписей над графиком
const ZONE_TOP = 18;     // верх зоны события (скобка), поле графика начинается ниже
const END_GAP = 10;      // последняя точка линии не упирается в плашку значения
const LINE_M = { l: 42, r: 54, t: 26, b: 22 };

const ts = (d: string) => Date.parse((d.length === 7 ? `${d}-15` : d) + 'T00:00:00Z');
const monY = (d: string) => `${monthShort(Number(d.slice(5, 7)) - 1)} ${d.slice(2, 4)}`;
const dayMon = (d: string) => `${Number(d.slice(8, 10))} ${monthShort(Number(d.slice(5, 7)) - 1)} ${d.slice(2, 4)}`;
// Объём ноги: 32 тыс, 1,2 тыс, 950
const kfmt = (v: number) => (Math.abs(v) >= 10000 ? `${Math.round(v / 1000)} тыс` : Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(1).replace('.', ',')} тыс` : String(Math.round(v)));
const pct = (v: number) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(Math.round(v))}%`;
const num = (v: number, d = 0) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toLocaleString('ru-RU', { maximumFractionDigits: d })}`;
const fmtPrice = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: Math.abs(v) < 100 ? 1 : 0 });
// Примерная ширина подписи 10px: раскладываем строку над графиком без наложений.
const tw = (s: string) => s.length * 6;

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

type LegendItem = { color: string; text: string };
type Head = { legend: { x: number; it: LegendItem }[]; zone: { x: number; anchor: 'start' | 'end' } | null };

// Строка над графиком: легенда слева от x=0, подпись зоны — над её правым концом (не влезает — над левым).
// Вместе не помещаются — легенда теряет хвост; не помещается и первый пункт — пропадает подпись зоны.
function headRow(items: LegendItem[], zone: { text: string; a: number; b: number } | null, width: number): Head {
  const place = (list: LegendItem[]) => {
    let x = 0;
    return list.map(it => { const p = { x, it }; x += 21 + tw(it.text); return p; });
  };
  const right = (list: LegendItem[]) => (list.length ? list.reduce((s, it) => s + 21 + tw(it.text), 0) + 2 : 0);
  if (!zone) return { legend: place(items), zone: null };
  const zw = tw(zone.text), b = Math.min(zone.b, width);
  for (let k = items.length; k >= Math.min(1, items.length); k--) {
    const list = items.slice(0, k), r = right(list);
    if (b - zw >= r) return { legend: place(list), zone: { x: b, anchor: 'end' } };
    if (zone.a >= r && zone.a + zw <= width) return { legend: place(list), zone: { x: zone.a, anchor: 'start' } };
  }
  return { legend: place(items.slice(0, 1)), zone: null };
}

function HeadRow({ head, zoneText }: { head: Head; zoneText?: string }) {
  return (
    <g fontSize={10} fontWeight={600}>
      {head.legend.map(({ x, it }) => (
        <g key={it.text}>
          <circle cx={x + 4} cy={HEAD_Y} r={3.5} fill={it.color} />
          <text x={x + 11} y={HEAD_Y} fill={MUTED} dominantBaseline="central">{it.text}</text>
        </g>
      ))}
      {head.zone && zoneText && (
        <text x={head.zone.x} y={HEAD_Y} fill={ACC} fontWeight={700} textAnchor={head.zone.anchor} dominantBaseline="central">{zoneText}</text>
      )}
    </g>
  );
}

// Зона события: заливка от скобки до оси X, скобка — сверху.
function Zone({ a, b, bottom }: { a: number; b: number; bottom: number }) {
  return (
    <g pointerEvents="none">
      <rect x={a} y={ZONE_TOP} width={Math.max(0, b - a)} height={bottom - ZONE_TOP} fill={ACC} opacity={0.12} />
      <line x1={a} x2={b} y1={ZONE_TOP} y2={ZONE_TOP} stroke={ACC} strokeWidth={2} />
    </g>
  );
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

/** Нога сигнала (лонги или шорты физлиц) + цена: зона события и начало сдвига. Наводят на прошлый случай в списке
 *  под графиком — нынешний сигнал гаснет, а прошлый рисуется так же: его зона, начало и точка. */
export function LegsChartCard({ chart, priceLabel, highlight }: { chart: HotLegsChart; priceLabel: string; highlight?: HotPastCase | null }) {
  const [ref, w] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const m = LINE_M;
  // прошлый случай под курсором в списке — вместо нынешнего, и график увеличен на него:
  // от 6 недель до начала сдвига до 3 недель после сигнала
  const past = highlight && highlight.date >= (chart.series[0]?.[0] ?? '') ? highlight : null;
  let i0 = 0, i1 = chart.series.length - 1;
  if (past) {
    const lo = new Date(ts(past.from ?? past.date) - 42 * 864e5).toISOString().slice(0, 10);
    const hi = new Date(ts(past.date) + 21 * 864e5).toISOString().slice(0, 10);
    i0 = Math.max(0, chart.series.findIndex(d => d[0] >= lo));
    const j = chart.series.findIndex(d => d[0] > hi);
    i1 = j < 0 ? chart.series.length - 1 : Math.max(i0 + 1, j - 1);
  }
  const data = useMemo(() => chart.series.slice(i0, i1 + 1), [chart.series, i0, i1]);
  const price = useMemo(() => chart.price?.slice(i0, i1 + 1), [chart.price, i0, i1]);
  const n = data.length;
  const ev = 1;
  const xr = w - m.r;
  const geo = useMemo(() => {
    if (!w || n < 2) return null;
    const x0 = ts(data[0][0]), x1 = ts(data[n - 1][0]);
    const X = (t: number) => m.l + ((t - x0) / Math.max(x1 - x0, 1)) * (w - m.l - m.r - END_GAP);
    const vals = data.map(d => d[ev]);
    let lo = Math.min(...vals), hi = Math.max(...vals);
    const pad = (hi - lo) * 0.12 || 1;
    lo = Math.max(0, lo - pad); hi += pad;
    const Y = (v: number) => H - m.b - ((v - lo) / (hi - lo)) * (H - m.t - m.b);
    const pr = (price ?? []).filter((v): v is number => v != null);
    let plo = Math.min(...pr), phi = Math.max(...pr);
    const ppad = (phi - plo) * 0.1 || 1;
    plo -= ppad; phi += ppad;
    const P = (v: number) => H - m.b - ((v - plo) / (phi - plo)) * (H - m.t - m.b);
    return { X, Y, P, lo, hi, plo, phi, x0, x1, hasPrice: pr.length > 1 };
  }, [w, data, n, price, m, ev]);

  const iEnd = past ? data.findIndex(d => d[0] >= past.date) : n - 1;
  const startDate = past ? past.from ?? past.date : chart.start?.date;
  const iStart = startDate ? data.findIndex(d => d[0] >= startDate) : -1;
  const zoneFrom = past ? startDate : chart.zone?.from;
  const zoneLabel = past ? `${past.zone ?? ''} · ${dayMon(past.date)}`.replace(/^ · /, '') : chart.zone?.label;

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
  const color = chart.leg === 'long' || chart.leg === 'nl' ? LONG : SHORT;
  const legName = LEG_NAME[chart.leg];
  const pts = data.map(d => [X(ts(d[0])), Y(d[ev])] as [number, number]);
  const span = (geo.x1 - geo.x0) / 864e5;
  const xt: { x: number; label: string }[] = [];
  if (span > 700) {
    for (let y = new Date(geo.x0).getUTCFullYear() + 1; y <= new Date(geo.x1).getUTCFullYear(); y++)
      xt.push({ x: X(Date.UTC(y, 0, 1)), label: String(y) });
  } else {
    const d0 = new Date(geo.x0);
    for (let k = 1; k < 40; k++) {
      const t = Date.UTC(d0.getUTCFullYear(), d0.getUTCMonth() + k, 1);
      if (t > geo.x1) break;
      xt.push({ x: X(t), label: monthShort(new Date(t).getUTCMonth()) });
    }
  }
  const yt = niceTicks(geo.lo, geo.hi, 3);
  const ptk = geo.hasPrice ? niceTicks(geo.plo, geo.phi, 3) : [];
  const zx = zoneFrom ? Math.min(X(ts(zoneFrom)), xr - 8) : null;
  const zEnd = past && iEnd >= 0 ? Math.min(pts[iEnd][0] + 3, xr) : xr;
  const nowV = chart.now.value;
  const shownV = past && iEnd >= 0 ? data[iEnd][ev] : nowV;      // при показе прошлого — его значение
  const nowY = Y(shownV);
  const pillY = Math.min(Math.max(nowY, m.t), H - m.b);
  const head = headRow(
    [{ color, text: legName.toLowerCase() }, ...(geo.hasPrice ? [{ color: PRICE, text: 'цена' }] : [])],
    zx != null && zoneLabel ? { text: zoneLabel, a: zx, b: zEnd } : null, xr);
  const hv = hover != null ? data[hover] : null;

  return (
    <div ref={ref} style={{ position: 'relative', height: H }} onMouseLeave={() => setHover(null)}>
      <svg width={w} height={H} style={{ display: 'block' }}>
        <HeadRow head={head} zoneText={zoneLabel} />
        {zx != null && <Zone a={zx} b={zEnd} bottom={H - m.b} />}
        {yt.map(v => (
          <g key={`y${v}`}>
            <line x1={m.l} x2={xr} y1={Y(v)} y2={Y(v)} stroke={GRID} strokeWidth={1} />
            {Math.abs(Y(v) - pillY) > 13 && (
              <text x={xr + 6} y={Y(v)} fontSize={10} fontFamily={FONT} fontWeight={700} fill={MUTED} dominantBaseline="central">{kfmt(v)}</text>
            )}
          </g>
        ))}
        {ptk.map(v => (
          <text key={`p${v}`} x={m.l - 6} y={P(v)} fontSize={10} fontFamily={FONT} fontWeight={700} fill={PRICE} textAnchor="end" dominantBaseline="central">{fmtPrice(v)}</text>
        ))}
        {spaced(xt, 40).map(t => (
          <text key={`x${t.label}${t.x}`} x={t.x} y={H - 6} fontSize={10} fontFamily={FONT} fill={MUTED} textAnchor="middle">{t.label}</text>
        ))}
        {geo.hasPrice && (
          <path d={path((price ?? []).map((v, i) => v == null ? null : [X(ts(data[i][0])), P(v)] as [number, number]).filter((p): p is [number, number] => !!p))}
            fill="none" stroke={PRICE} strokeWidth={1.2} opacity={0.45} />
        )}
        <path d={path(pts)} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" />
        {iStart >= 0 && iEnd >= iStart && <path d={path(pts.slice(iStart, iEnd + 1))} fill="none" stroke={color} strokeWidth={3.6} strokeLinejoin="round" strokeLinecap="round" />}
        {iStart >= 0 && <circle cx={pts[iStart][0]} cy={pts[iStart][1]} r={4} fill={PANEL} stroke={color} strokeWidth={2} />}
        {iEnd >= 0 && <circle cx={pts[iEnd][0]} cy={pts[iEnd][1]} r={5.5} fill={color} stroke={INK} strokeWidth={1.6} />}
        <rect x={xr + 4} y={pillY - 9} width={46} height={18} rx={4} fill={color} />
        <text x={xr + 27} y={pillY} fontSize={10} fontFamily={FONT} fontWeight={800} fill="var(--text-inverse)" textAnchor="middle" dominantBaseline="central">{kfmt(shownV)}</text>
        {hv && <line x1={X(ts(hv[0]))} x2={X(ts(hv[0]))} y1={m.t} y2={H - m.b} stroke={MUTED} strokeDasharray="2 3" />}
        <rect x={m.l} y={0} width={Math.max(0, xr - m.l)} height={H - m.b} fill="transparent" onMouseMove={onMove} />
      </svg>
      {hv && hover != null && (
        <Tip x={X(ts(hv[0]))} y={Y(hv[ev])} w={w} rows={[
          <span style={{ color: MUTED }}>{dayMon(hv[0])}</span>,
          <span><span style={{ color, fontWeight: 700 }}>●</span> {legName} {kfmt(hv[ev])}</span>,
          ...(price?.[hover] != null ? [<span><span style={{ color: PRICE, fontWeight: 700 }}>●</span> {priceLabel} {fmtPrice(price[hover]!)}</span>] : []),
        ]} />
      )}
    </div>
  );
}

/** Столбики потоков: событие оранжевым с цифрой, прежний рекорд обведён и отмечен точкой с пунктиром
 *  вправо, серия — зоной со скобкой и подписью над графиком. */
export function BarsChartCard({ chart: base, highlight }: { chart: HotBarsChart; highlight?: HotPastCase | null }) {
  // прошлый случай из «Истории»: его столбик и серия вместо нынешних (если столбик есть на графике)
  const key0 = (b: [string, number]) => (base.weekly ? b[0] : b[0].slice(0, 7));
  const hp = highlight?.hl && base.bars.some(b => key0(b) === highlight.hl) ? highlight : null;
  const chart: HotBarsChart = hp ? { ...base, hl: hp.hl!, run: hp.run ?? undefined, prev: undefined, level: undefined } : base;
  const [ref, w] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const m = { l: 8, r: 8, t: 26, b: 22 };
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
  const cx = (i: number) => m.l + i * bw + bw / 2;
  // подписи годов на границах
  const years = spaced(bars.map((b, i) => ({ x: m.l + i * bw, label: b[0].slice(0, 4), i }))
    .filter((t, i, arr) => i > 0 && t.label !== arr[i - 1].label), 36);
  const fmt = (v: number) => num(v, Math.abs(v) >= 10 ? 0 : 2);
  const run = runIdx.length > 0 && chart.run
    ? { a: m.l + runIdx[0] * bw, b: m.l + (runIdx[runIdx.length - 1] + 1) * bw, label: chart.run.label } : null;
  const head = headRow([], run ? { text: run.label, a: run.a, b: run.b } : null, w - m.r);
  const hv = hover != null ? bars[hover] : null;
  const hlV = hlI >= 0 ? bars[hlI][1] : 0, hlX = hlI >= 0 ? cx(hlI) : 0;
  const hlAnchor = hlX > w - 30 ? 'end' : hlX < 30 ? 'start' : 'middle';
  return (
    <div ref={ref} style={{ position: 'relative', height: H }} onMouseLeave={() => setHover(null)}>
      <svg width={w} height={H} style={{ display: 'block' }}>
        <HeadRow head={head} zoneText={run?.label} />
        {run && <Zone a={run.a} b={run.b} bottom={H - m.b} />}
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
        {prevI >= 0 && chart.level != null && <>
          <line x1={cx(prevI)} x2={w - m.r} y1={Y(chart.level)} y2={Y(chart.level)} stroke={INK} strokeWidth={1} strokeDasharray="3 3" opacity={0.55} />
          <circle cx={cx(prevI)} cy={Y(chart.level)} r={4} fill={INK} stroke={PANEL} strokeWidth={1.5} />
        </>}
        {hlI >= 0 && (
          <text x={hlAnchor === 'end' ? hlX + bw / 2 : hlX} y={hlV >= 0 ? Y(hlV) - 5 : Y(hlV) + 13} fontSize={10.5} fontFamily={FONT}
            fontWeight={800} fill={ACC} textAnchor={hlAnchor} paintOrder="stroke" stroke={PANEL} strokeWidth={3}>{fmt(hlV)}</text>
        )}
        {years.map(t => <text key={`y${t.i}`} x={t.x + 2} y={H - 6} fontSize={10} fontFamily={FONT} fill={MUTED}>{t.label}</text>)}
        <rect x={m.l} y={0} width={w - m.l - m.r} height={H - m.b} fill="transparent"
          onMouseMove={e => {
            const rect = (e.currentTarget as SVGRectElement).getBoundingClientRect();
            setHover(Math.min(n - 1, Math.max(0, Math.floor((e.clientX - rect.left) / bw))));
          }} />
        {hv && <rect x={m.l + hover! * bw} y={m.t} width={bw} height={H - m.b - m.t} fill={INK} opacity={0.06} pointerEvents="none" />}
      </svg>
      {hv && (
        <Tip x={cx(hover!)} y={Y(hv[1])} w={w} rows={[
          <span style={{ color: MUTED }}>{chart.weekly ? `неделя с ${dayMon(hv[0])}` : monY(hv[0])}{hover === prevI ? ' · прежний рекорд' : ''}</span>,
          <span style={{ fontWeight: 700, color: hv[1] >= 0 ? UP : DN }}>{fmt(hv[1])} {chart.unit}</span>,
        ]} />
      )}
    </div>
  );
}

/** Сезонность года: средняя кривая, текущий год, «сегодня» и зона следующих трёх месяцев. */
export function SeasonChartCard({ chart, label: curLabel, highlight }: { chart: HotSeasonChart; label: string; highlight?: HotPastCase | null }) {
  // год из «Истории» — его путь вместо текущего года
  const [ref, w] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const m = { l: 8, r: 42, t: 26, b: 22 };
  const avg = chart.avg, cur = highlight?.curve ?? chart.cur;
  const label = highlight?.curve ? highlight.label : curLabel;
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
  const nowCur = highlight?.curve
    ? cur.reduce((b, p) => (Math.abs(p[0] - chart.today) < Math.abs(b[0] - chart.today) ? p : b), cur[0])
    : cur[cur.length - 1];
  const za = X(chart.today), zb = Math.max(za + 3, X(chart.zone_to));
  const zoneText = '3 месяца';
  const head = headRow([{ color: MUTED, text: 'в среднем' }, { color: ACC, text: label.toLowerCase() }],
    { text: zoneText, a: za, b: zb }, w - m.r);
  const hv = hover != null ? avg.find(a => a[0] === hover) : null;
  const hc = hover != null ? cur.find(c => c[0] === hover) : null;
  return (
    <div ref={ref} style={{ position: 'relative', height: H }} onMouseLeave={() => setHover(null)}>
      <svg width={w} height={H} style={{ display: 'block' }}>
        <HeadRow head={head} zoneText={zoneText} />
        <Zone a={za} b={zb} bottom={H - m.b} />
        {yt.map(v => (
          <g key={`y${v}`}>
            <line x1={m.l} x2={w - m.r} y1={Y(v)} y2={Y(v)} stroke={v === 0 ? MUTED : GRID} />
            <text x={w - m.r + 6} y={Y(v)} fontSize={10} fontFamily={FONT} fontWeight={700} fill={MUTED} dominantBaseline="central">{pct(v)}</text>
          </g>
        ))}
        {months.map(t => <text key={`m${t.x}`} x={t.x} y={H - 6} fontSize={10} fontFamily={FONT} fill={MUTED}>{t.label}</text>)}
        <path d={path(avg.map(a => [X(a[0]), Y(a[1])] as [number, number]))} fill="none" stroke={MUTED} strokeWidth={1.8} />
        <path d={path(cur.map(c => [X(c[0]), Y(c[1])] as [number, number]))} fill="none" stroke={ACC} strokeWidth={2} />
        <line x1={za} x2={za} y1={ZONE_TOP} y2={H - m.b} stroke={INK} strokeDasharray="3 3" opacity={0.5} />
        {nowCur && <circle cx={X(nowCur[0])} cy={Y(nowCur[1])} r={5} fill={ACC} stroke={INK} strokeWidth={1.5} />}
        <rect x={m.l} y={0} width={w - m.l - m.r} height={H - m.b} fill="transparent"
          onMouseMove={e => {
            const rect = (e.currentTarget as SVGRectElement).getBoundingClientRect();
            const td = Math.round(((e.clientX - rect.left) / (w - m.l - m.r)) * tdMax);
            setHover(Math.min(tdMax, Math.max(0, td)));
          }} />
        {hover != null && <line x1={X(hover)} x2={X(hover)} y1={m.t} y2={H - m.b} stroke={MUTED} strokeDasharray="2 3" />}
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
