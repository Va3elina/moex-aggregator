/**
 * Разметка сигнала поверх графика по правилам канала (research/channel_charts/ANNOTATION_RULES.md): крупная подпись
 * цветом сигнала в свободном месте, короткая стрелка к цели мимо линий, кружки/рамки, подписи меток с одной стороны.
 * Работает в координатах итогового SVG: занятость поля — сетка 4 px по точкам линий, столбикам и уже поставленным меткам.
 */
export interface Rect { x: number; y: number; w: number; h: number }

export class Annotator {
  private G = 4;
  private GW: number; private GH: number;
  private grid: Float32Array;
  readonly bounds: Rect;
  private bg: string;
  private measure: (t: string, s: number, w: number) => number;
  out: string[] = [];
  under: string[] = [];

  constructor(bounds: Rect, bg: string, measure: (t: string, s: number, w: number) => number, total: { w: number; h: number }) {
    this.bounds = bounds; this.bg = bg; this.measure = measure;
    this.GW = Math.ceil(total.w / this.G) + 1; this.GH = Math.ceil(total.h / this.G) + 1;
    this.grid = new Float32Array(this.GW * this.GH);
  }

  mark(x: number, y: number, v = 1) {
    const c = Math.floor(x / this.G), r = Math.floor(y / this.G);
    if (c >= 0 && r >= 0 && c < this.GW && r < this.GH) this.grid[r * this.GW + c] += v;
  }
  /** Ломаная (линия графика) с вертикальными перепадами. */
  trace(pts: { x: number; y: number }[], v = 1) {
    pts.forEach((p, i) => {
      this.mark(p.x, p.y, v);
      if (i > 0) {
        const q = pts[i - 1], n = Math.ceil(Math.max(Math.abs(p.y - q.y), Math.abs(p.x - q.x)) / this.G);
        for (let k = 1; k < n; k++) this.mark(q.x + ((p.x - q.x) * k) / n, q.y + ((p.y - q.y) * k) / n, v);
      }
    });
  }
  block(r: Rect, v = 50) { for (let y = r.y; y < r.y + r.h; y += this.G) for (let x = r.x; x < r.x + r.w; x += this.G) this.mark(x, y, v); }
  sum(r: Rect) {
    let t = 0;
    const c0 = Math.max(0, Math.floor(r.x / this.G)), c1 = Math.min(this.GW - 1, Math.floor((r.x + r.w) / this.G));
    const r0 = Math.max(0, Math.floor(r.y / this.G)), r1 = Math.min(this.GH - 1, Math.floor((r.y + r.h) / this.G));
    for (let rr = r0; rr <= r1; rr++) for (let c = c0; c <= c1; c++) t += this.grid[rr * this.GW + c];
    return t;
  }
  private pathCost(sx: number, sy: number, tx: number, ty: number) {
    const len = Math.hypot(tx - sx, ty - sy); let c = 0;
    for (let d = 0; d < len - 20; d += 4) c += this.sum({ x: sx + ((tx - sx) * d) / len - 8, y: sy + ((ty - sy) * d) / len - 8, w: 16, h: 16 });
    return c;
  }
  halo(inner: string, sw: number) {
    return inner.replace(/stroke="[^"]+"/, `stroke="${this.bg}"`).replace(/fill="(?!none)[^"]+"/, `fill="${this.bg}"`)
      .replace(/stroke-width="([\d.]+)"/, (_m, v) => `stroke-width="${Number(v) + sw}"`);
  }
  haloText(w: number) { return `stroke="${this.bg}" stroke-width="${w}" stroke-linejoin="round" paint-order="stroke"`; }

  ring(cx: number, cy: number, r: number, color: string) {
    const s = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${color}" stroke-width="3.5"/>`;
    this.out.push(this.halo(s, 4) + s);
    this.block({ x: cx - r - 4, y: cy - r - 4, w: 2 * r + 8, h: 2 * r + 8 });
  }
  frame(r: Rect, color: string) {
    const s = `<rect x="${r.x}" y="${r.y}" width="${r.w}" height="${r.h}" rx="8" fill="none" stroke="${color}" stroke-width="3.5"/>`;
    this.out.push(this.halo(s, 4) + s);
    this.block({ x: r.x - 4, y: r.y - 4, w: r.w + 8, h: 6 }); this.block({ x: r.x - 4, y: r.y + r.h - 2, w: r.w + 8, h: 6 });
    this.block({ x: r.x - 4, y: r.y, w: 6, h: r.h }); this.block({ x: r.x + r.w - 2, y: r.y, w: 6, h: r.h });
  }
  label(text: string, cx: number, y: number, color: string, size = 18, right?: number) {
    const w = this.measure(text, size, 700) + 8;
    if (right != null) cx = right - w / 2;                     // прижать правым краем
    const lx = Math.min(Math.max(cx, this.bounds.x + w / 2 + 2), this.bounds.x + this.bounds.w - w / 2 - 2);
    this.out.push(`<text x="${lx}" y="${y + size}" text-anchor="middle" fill="${color}" font-size="${size}" font-weight="700" ${this.haloText(5)}>${text}</text>`);
    this.block({ x: lx - w / 2, y, w, h: size + 8 });
  }

  /** Подпись метки над или под точкой (cy ± r), где свободнее; prefer — сторона по умолчанию. */
  tag(text: string, cx: number, cy: number, r: number, color: string, size = 18, prefer: 'above' | 'below' = 'above', right?: number) {
    const w = this.measure(text, size, 700) + 8, h = size + 8;
    const x = right != null ? right - w : cx - w / 2;
    const up = cy - r - 6 - h, dn = cy + r + 6;
    const cost = (y: number) => this.sum({ x, y, w, h }) + (y < this.bounds.y - 40 || y + h > this.bounds.y + this.bounds.h + 30 ? 1e6 : 0);
    const a = cost(up) + (prefer === 'above' ? 0 : 5), b = cost(dn) + (prefer === 'below' ? 0 : 5);
    this.label(text, x + w / 2, a <= b ? up : dn, color, size);
  }

  /** Крупная подпись в свободном месте и стрелка к цели (tx, ty). avoid — направление, вдоль которого стрелке нельзя. */
  headline(text: string, tx: number, ty: number, color: string, opts: { gap?: number; avoidAngle?: number; minLen?: number; maxLen?: number; above?: number; rect?: Rect; noArrow?: boolean } = {}) {
    const words = text.split(' ');
    // перенос — перед предлогом («рекордный отток / за год», «приток / после 27 недель оттока»), иначе пополам
    const pi = words.findIndex((w, i) => i > 0 && (w === 'за' || w === 'после'));
    const cut = pi > 0 ? pi : Math.ceil(words.length / 2);
    const split = words.length > 1 ? [words.slice(0, cut).join(' '), words.slice(cut).join(' ')] : null;
    // варианты подписи: крупно в одну или две строки; мельче — только если крупной нет чистого места (штраф)
    const variants: { SZ: number; lines: string[]; pen: number }[] = [];
    for (const [SZ, pen] of [[42, 0], [34, 2500]] as const) {
      variants.push({ SZ, lines: [text], pen: pen + (this.measure(text, SZ, 800) > this.bounds.w * 0.45 ? 300 : 0) });
      if (split) variants.push({ SZ, lines: split, pen });
    }
    const gap = opts.gap ?? 28, minL = opts.minLen ?? 45, maxL = opts.maxLen ?? 340;
    const top = this.bounds.y - (opts.above ?? 0);
    let best = { x: this.bounds.x + 10, y: this.bounds.y + 6, s: Infinity, sx: 0, sy: 0, tx, ty, v: variants[0] };
    for (const V of variants) {
    const LH = Math.round(V.SZ * 1.15);
    const w = Math.max(...V.lines.map((l) => this.measure(l, V.SZ, 800))), h = LH * V.lines.length;
    for (let x = this.bounds.x + 12; x + w <= this.bounds.x + this.bounds.w - 12; x += 12) {
      for (let y = top + 2; y + h <= this.bounds.y + this.bounds.h - 6; y += 8) {
        // цель-рамка: стрелка смотрит в ближайшую к подписи точку её края, а не в заранее выбранную
        const R = opts.rect;
        if (R) {
          if (x < R.x + R.w + 20 && x + w > R.x - 20 && y < R.y + R.h + 20 && y + h > R.y - 20) continue;
          const cx = x + w / 2, cy = y + h / 2;
          tx = Math.min(Math.max(cx, R.x + 12), R.x + R.w - 12); ty = Math.min(Math.max(cy, R.y), R.y + R.h);
          if (cx > R.x && cx < R.x + R.w) ty = cy < R.y ? R.y : R.y + R.h;
          else if (cy > R.y && cy < R.y + R.h) tx = cx < R.x ? R.x : R.x + R.w;
        } else if (x < tx + 30 && x + w > tx - 30 && y < ty + 30 && y + h > ty - 30) continue;
        const sx = tx > x + w ? x + w + 8 : tx < x ? x - 8 : x + w / 2;
        const sy = tx > x + w || tx < x ? y + h / 2 : ty < y ? y - 8 : y + h + 8;
        const len = Math.hypot(tx - sx, ty - sy);
        if (len < minL || len > maxL) continue;
        let along = 0;
        if (opts.avoidAngle != null) {
          let d = Math.abs(Math.atan2(ty - sy, tx - sx) - opts.avoidAngle); if (d > Math.PI) d = 2 * Math.PI - d;
          along = d < 0.5 ? 1e7 : d < 0.8 ? (0.8 - d) * 200000 : 0;
        }
        const side = (y + h > ty ? 400 : 0) + V.pen;                      // выше цели — как на канале
        const sc = this.sum({ x: x - 8, y: y - 6, w: w + 16, h: h + 12 }) * 80 + this.pathCost(sx, sy, tx, ty) * 15 + len * 2 + along + side;
        if (sc < best.s) best = { x, y, s: sc, sx, sy, tx, ty, v: V };
      }
    }
    }
    const { SZ, lines } = best.v, LH = Math.round(SZ * 1.15);
    const w = Math.max(...lines.map((l) => this.measure(l, SZ, 800))), h = LH * lines.length;
    this.out.push(lines.map((l, k) => `<text x="${best.x}" y="${best.y + SZ + k * LH - 6}" fill="${color}" font-size="${SZ}" font-weight="800" letter-spacing="-0.5" ${this.haloText(8)}>${l}</text>`).join(''));
    this.block({ x: best.x, y: best.y, w, h });
    tx = best.tx; ty = best.ty;
    if (!opts.noArrow) this.arrow(best.sx, best.sy, tx, ty, color, gap);
  }

  /** Стрелка-указатель от (sx, sy) к (tx, ty), с зазором gap перед целью и обводкой цветом фона. */
  arrow(sx: number, sy: number, tx: number, ty: number, color: string, gap = 0, width = 5, L = 15) {
    const a = Math.atan2(ty - sy, tx - sx), ex = tx - Math.cos(a) * gap, ey = ty - Math.sin(a) * gap;
    if (Math.hypot(ex - sx, ey - sy) <= 30) return;
    const hx = ex - Math.cos(a) * L * 0.8, hy = ey - Math.sin(a) * L * 0.8;
    const wing = (s: number) => `${ex - Math.cos(a) * L + Math.cos(a + s * Math.PI / 2) * L * 0.6},${ey - Math.sin(a) * L + Math.sin(a + s * Math.PI / 2) * L * 0.6}`;
    const shaft = `<line x1="${sx}" y1="${sy}" x2="${hx}" y2="${hy}" stroke="${color}" stroke-width="${width}" stroke-linecap="round"/>`;
    const head = `<polygon points="${ex},${ey} ${wing(1)} ${wing(-1)}" fill="${color}" stroke="${color}" stroke-width="1" stroke-linejoin="round"/>`;
    this.out.push(this.halo(shaft, 6) + this.halo(head, 6) + shaft + head);
    // стрелка — тоже занятое место: подписи, поставленные после, на неё не лезут
    const n = Math.ceil(Math.hypot(ex - sx, ey - sy) / 4);
    for (let k = 0; k <= n; k++) this.block({ x: sx + ((ex - sx) * k) / n - 6, y: sy + ((ey - sy) * k) / n - 6, w: 12, h: 12 });
  }

  /** Крупный знак (например «?») в точке, с обводкой цветом фона. */
  glyph(text: string, x: number, y: number, color: string, size = 60) {
    const w = this.measure(text, size, 800);
    this.out.push(`<text x="${x}" y="${y}" text-anchor="middle" fill="${color}" font-size="${size}" font-weight="800" ${this.haloText(8)}>${text}</text>`);
    this.block({ x: x - w / 2 - 4, y: y - size, w: w + 8, h: size + 8 });
  }

}
