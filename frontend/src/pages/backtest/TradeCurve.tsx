// Стенд: «Динамика» как в тестере TradingView — накопленный результат по сделкам (точка = сделка), под ним столбики
// благоприятного и неблагоприятного отклонения каждой сделки, внизу лента периодов роста и спада. График неподвижный:
// без прокрутки и масштаба. Наведение — подсказка по сделке или по периоду, клик по сделке — открыть её на графике цены.
import { useEffect, useMemo, useRef, useState } from 'react';
import type { BtTrade } from './api';
import { fmtDate, mmToStr, num, pct } from './lib';
import { pnlOf } from './stats';

interface P { t: BtTrade; cum: number; pnl: number; fav: number; adv: number }
interface Period { from: number; to: number; up: boolean; delta: number }
const UP = '#26a69a', DOWN = '#ef5350', PAD = { l: 10, r: 74, t: 14, b: 40 }, STRIP = 7;

export default function TradeCurve({ trades, money, capital, names, onOpen, height = 320 }: {
  trades: BtTrade[]; money: boolean; capital: number; names: Map<string, string>; onOpen: (t: BtTrade) => void; height?: number;
}) {
  const box = useRef<HTMLDivElement>(null); const cv = useRef<HTMLCanvasElement>(null);
  const [w, setW] = useState(900); const [hover, setHover] = useState<{ i: number; x: number; y: number } | { period: Period; x: number; y: number } | null>(null);

  const pts = useMemo<P[]>(() => {
    const done = trades.filter(t => !(money && t.account_skip)).sort((a, b) => a.d_out < b.d_out ? -1 : a.d_out > b.d_out ? 1 : (a.m_out ?? 0) - (b.m_out ?? 0));
    let cum = 0;
    return done.map(t => { const pnl = pnlOf(t, money); cum += pnl; const k = money ? (t.notional ?? 0) : 1; return { t, cum, pnl, fav: (t.mfe ?? 0) * k, adv: (t.mae ?? 0) * k }; });
  }, [trades, money]);
  const periods = useMemo<Period[]>(() => {
    const out: Period[] = []; let peak = 0, start = 0, inDd = false, base = 0;
    pts.forEach((p, i) => {
      if (!inDd && p.cum < peak) { if (i > start) out.push({ from: start, to: i, up: true, delta: peak - base }); inDd = true; start = i; base = peak; }
      else if (inDd && p.cum >= peak) { out.push({ from: start, to: i, up: false, delta: Math.min(...pts.slice(start, i + 1).map(x => x.cum)) - base }); inDd = false; start = i; base = pts[i - 1]?.cum ?? 0; }
      peak = Math.max(peak, p.cum);
    });
    if (pts.length) out.push(inDd ? { from: start, to: pts.length, up: false, delta: Math.min(...pts.slice(start).map(x => x.cum)) - base } : { from: start, to: pts.length, up: true, delta: pts[pts.length - 1].cum - base });
    return out.filter(p => p.to > p.from);
  }, [pts]);

  useEffect(() => { const el = box.current; if (!el) return; const ro = new ResizeObserver(() => setW(el.clientWidth)); ro.observe(el); setW(el.clientWidth); return () => ro.disconnect(); }, []);

  const geo = useMemo(() => {
    const n = pts.length, plotW = Math.max(10, w - PAD.l - PAD.r), plotH = height - PAD.t - PAD.b;
    const xs = (i: number) => PAD.l + (n <= 1 ? plotW / 2 : (i + 1) / (n + 0.5) * plotW);         // i = -1 — стартовая точка (ноль)
    const lo = Math.min(0, ...pts.map(p => p.cum)), hi = Math.max(0, ...pts.map(p => p.cum)); const span = hi - lo || 1;
    const curveH = plotH * 0.66; const y = (v: number) => PAD.t + (hi - v) / span * curveH;
    const barMax = Math.max(1e-12, ...pts.map(p => Math.max(p.fav, -p.adv))); const barBase = PAD.t + curveH + (plotH - curveH) * 0.55; const barH = (plotH - curveH) * 0.5;
    return { n, plotW, plotH, xs, y, lo, hi, barMax, barBase, barH, step: plotW / (n + 0.5) };
  }, [pts, w, height]);

  useEffect(() => {
    const c = cv.current; if (!c) return; const dpr = window.devicePixelRatio || 1;
    c.width = w * dpr; c.height = height * dpr; const g = c.getContext('2d')!; g.setTransform(dpr, 0, 0, dpr, 0, 0); g.clearRect(0, 0, w, height);
    const { n, xs, y, lo, hi, barMax, barBase, barH, step, plotW } = geo; if (!n) return;
    g.font = '11px -apple-system, Inter, sans-serif'; g.textBaseline = 'middle';
    // сетка и шкала
    const ticks = 4; g.strokeStyle = 'rgba(255,255,255,.06)'; g.fillStyle = '#8a8f98'; g.lineWidth = 1;
    for (let k = 0; k <= ticks; k++) { const v = lo + (hi - lo) * k / ticks, yy = Math.round(y(v)) + .5; g.beginPath(); g.moveTo(PAD.l, yy); g.lineTo(PAD.l + plotW, yy); g.stroke(); g.fillText(money ? (Math.abs(v) >= 1e6 ? num(v / 1e6, 2) + ' млн' : num(v)) : num(100 * v, 1) + '%', PAD.l + plotW + 8, yy); }
    const hp = hover && 'period' in hover ? hover.period : null;
    if (hp) { g.fillStyle = hp.up ? 'rgba(38,166,154,.10)' : 'rgba(239,83,80,.10)'; g.fillRect(xs(hp.from - 1), PAD.t, xs(hp.to - 1) - xs(hp.from - 1), height - PAD.t - PAD.b + 6); }
    // столбики отклонений
    const bw = Math.max(1, Math.min(46, step * 0.86));
    pts.forEach((p, i) => {
      const x = xs(i) - bw / 2;
      if (p.fav > 0) { g.fillStyle = 'rgba(38,166,154,.30)'; const h = p.fav / barMax * barH; g.fillRect(x, barBase - h, bw, h); }
      if (p.pnl > 0) { g.fillStyle = 'rgba(38,166,154,.55)'; const h = Math.min(p.pnl, p.fav || p.pnl) / barMax * barH; g.fillRect(x, barBase - h, bw, h); }
      if (p.adv < 0) { g.fillStyle = 'rgba(239,83,80,.38)'; const h = -p.adv / barMax * barH; g.fillRect(x, barBase, bw, h); }
    });
    g.strokeStyle = '#3a3f48'; g.beginPath(); g.moveTo(PAD.l, Math.round(barBase) + .5); g.lineTo(PAD.l + plotW, Math.round(barBase) + .5); g.stroke();
    // кривая
    const grad = g.createLinearGradient(0, PAD.t, 0, y(lo)); grad.addColorStop(0, 'rgba(38,166,154,.22)'); grad.addColorStop(1, 'rgba(38,166,154,0)');
    g.beginPath(); g.moveTo(xs(-1), y(0)); pts.forEach((p, i) => g.lineTo(xs(i), y(p.cum))); g.lineTo(xs(n - 1), y(Math.min(0, lo) < 0 ? 0 : 0)); g.lineTo(xs(-1), y(0)); g.closePath(); g.fillStyle = grad; g.fill();
    g.beginPath(); g.moveTo(xs(-1), y(0)); pts.forEach((p, i) => g.lineTo(xs(i), y(p.cum))); g.strokeStyle = UP; g.lineWidth = 2; g.lineJoin = 'round'; g.stroke();
    if (n <= 320) { g.fillStyle = UP; [[-1, 0] as const, ...pts.map((p, i) => [i, p.cum] as const)].forEach(([i, v]) => { g.beginPath(); g.arc(xs(i), y(v), n <= 80 ? 4 : 2.5, 0, 6.3); g.fill(); }); }
    // лента периодов
    const sy = height - PAD.b + 10;
    periods.forEach(p => { g.fillStyle = p.up ? UP : DOWN; g.globalAlpha = hp === p ? 1 : .75; g.fillRect(xs(p.from - 1), sy, Math.max(1, xs(p.to - 1) - xs(p.from - 1)), STRIP); }); g.globalAlpha = 1;
    // подписи дат
    g.fillStyle = '#8a8f98'; g.textAlign = 'center'; const labels = Math.max(2, Math.min(8, Math.floor(plotW / 140)));
    for (let k = 0; k < labels; k++) { const i = Math.round(k * (n - 1) / Math.max(1, labels - 1)); const d = pts[i].t.d_out; g.fillText(`${d.slice(8, 10)}.${d.slice(5, 7)}.${d.slice(2, 4)}`, Math.min(PAD.l + plotW - 24, Math.max(PAD.l + 24, xs(i))), height - 12); }
    g.textAlign = 'left';
    // курсор
    if (hover && 'i' in hover) { const p = pts[hover.i]; const x = Math.round(xs(hover.i)) + .5; g.strokeStyle = '#6b7280'; g.lineWidth = 1; g.beginPath(); g.moveTo(x, PAD.t); g.lineTo(x, height - PAD.b + 6); g.stroke(); g.fillStyle = UP; g.strokeStyle = '#0b0b0c'; g.lineWidth = 3; g.beginPath(); g.arc(xs(hover.i), y(p.cum), 6, 0, 6.3); g.stroke(); g.fill(); }
  }, [geo, pts, periods, hover, w, height, money]);

  const move = (e: React.MouseEvent) => {
    if (!pts.length) return; const r = cv.current!.getBoundingClientRect(); const x = e.clientX - r.left, y = e.clientY - r.top; const { xs, n } = geo;
    if (y > height - PAD.b + 4) { const pr = periods.find(p => x >= xs(p.from - 1) && x <= xs(p.to - 1)); setHover(pr ? { period: pr, x, y } : null); return; }
    let i = Math.round((x - PAD.l) / geo.plotW * (n + 0.5) - 1); i = Math.max(0, Math.min(n - 1, i)); setHover({ i, x: xs(i), y });
  };
  const v = (x: number) => money ? `${num(x, 0)} ₽` : pct(x);
  const tip = hover && ('i' in hover ? (() => { const p = pts[hover.i]; const t = p.t; return (
    <div className="bt-tip" style={{ left: hover.x > w / 2 ? hover.x - 320 : hover.x + 16, top: Math.max(6, Math.min(height - 200, hover.y - 60)) }}>
      <div className="h">Сделка {hover.i + 1} · {t.side > 0 ? 'длинная' : 'короткая'} · {names.get(t.st) ?? t.st}</div>
      <div><i style={{ background: UP }} />Совокупные ПР/УБ<b>{v(p.cum)}</b></div>
      <div><i style={{ background: p.pnl >= 0 ? UP : DOWN }} />ПР/УБ сделки<b className={p.pnl >= 0 ? 'bt-up' : 'bt-down'}>{v(p.pnl)}</b></div>
      <div><i style={{ background: 'rgba(38,166,154,.45)' }} />Благоприятное отклонение<b>{v(p.fav)}</b></div>
      <div><i style={{ background: 'rgba(239,83,80,.55)' }} />Неблагоприятное отклонение<b>{v(p.adv)}</b></div>
      <div className="f">{fmtDate(t.d_out)}{t.m_out != null ? `, ${mmToStr(t.m_out)}` : ''}<br />клик — открыть на графике</div>
    </div>); })() : (() => { const p = hover.period; const a = pts[Math.max(0, p.from)]?.t, b = pts[Math.min(pts.length - 1, p.to - 1)]?.t; return (
    <div className="bt-tip" style={{ left: hover.x > w / 2 ? hover.x - 320 : hover.x + 16, top: height - 150 }}>
      <div className="row"><span>{p.up ? 'Рост' : 'Спад'}</span><b className={p.up ? 'bt-up' : 'bt-down'}>{v(p.delta)}{money ? <small>{pct(p.delta / capital)}</small> : null}</b></div>
      <div className="f">{a ? fmtDate(a.d) : ''} — {b ? fmtDate(b.d_out) : ''} · сделок {p.to - p.from}</div>
    </div>); })());
  return (
    <div ref={box} className="bt-curve" style={{ height }}>
      {!pts.length && <div className="bt-nodata" style={{ position: 'absolute', inset: 0 }}>Недостаточно данных</div>}
      <canvas ref={cv} style={{ width: '100%', height }} onMouseMove={move} onMouseLeave={() => setHover(null)} onClick={() => { if (hover && 'i' in hover) onOpen(pts[hover.i].t); }} />
      {tip}
    </div>
  );
}
