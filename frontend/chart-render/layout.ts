/**
 * Раскладка HTML-обвязки графиков сайта в один SVG — без браузера.
 *
 * Графики «Денег в фондах» (FlowsHistogram) и «Сезонности» (YearlySeasonalityChart) — это SVG поля плюс HTML-блоки,
 * расставленные стилями: подписи осей, легенды, точки, панели друг под другом. В jsdom раскладки нет, поэтому здесь —
 * ровно то подмножество CSS, которым пользуются эти графики: поток блоков сверху вниз, position:absolute с px/%/calc/
 * var/clamp, высота/aspect-ratio, отступы (классы Tailwind p-N, px-N, pb-N и style), flex по центру и justify-between,
 * transform translate(-50%), текст span/div, кружки-точки (div с background и border-radius). Навигатор, ховер-слои и
 * водяной знак пропускаются, как при экспорте 📷.
 */

export interface Box { x: number; y: number; w: number; h: number }
type Theme = Record<string, string>;

const SKIP_CLASS = ['chart-navigator', 'chart-hover-ui', 'chart-tooltip-root'];
const VW = 1440;

export class Layout {
  private theme: Theme;
  private measure: (text: string, size: number, weight: number) => number;
  private ser: { serializeToString(n: Node): string };
  out: string[] = [];
  /** Где оказались SVG графика: исходный элемент и его прямоугольник в итоговом SVG. */
  placed: { el: Element; x: number; y: number; w: number; h: number }[] = [];
  /** Прямоугольники подписей (оси, заголовки) — разметке сигнала туда нельзя. */
  texts: { x: number; y: number; w: number; h: number }[] = [];

  constructor(theme: Theme, measure: (t: string, s: number, w: number) => number, ser: { serializeToString(n: Node): string }) {
    this.theme = theme; this.measure = measure; this.ser = ser;
  }

  // ── значения CSS ──────────────────────────────────────────────────────────
  /** var() → значение темы или запасное; рекурсивно. */
  vars(s: string): string {
    let prev = '';
    while (prev !== s) {
      prev = s;
      s = s.replace(/var\((--[\w-]+)(?:\s*,\s*((?:[^()]|\([^()]*\))*))?\)/g, (_m, n: string, fb?: string) => this.theme[n] ?? (fb ?? '0px').trim());
    }
    return s;
  }

  /** Длина в px: px, %, vw, calc/clamp/min/max/var. base — от чего считать проценты. */
  len(raw: string | null | undefined, base: number): number | null {
    if (raw == null || raw === '' || raw === 'auto') return null;
    const s = this.vars(String(raw)).trim();
    try { return this.expr(s, base); } catch { return null; }
  }

  private expr(s: string, base: number): number {
    s = s.trim();
    const fn = s.match(/^(calc|clamp|min|max)\((.*)\)$/s);
    if (fn) {
      const args = this.splitArgs(fn[2]).map((a) => this.expr(a, base));
      if (fn[1] === 'calc') return args[0];
      if (fn[1] === 'clamp') return Math.min(Math.max(args[1], args[0]), args[2]);
      return fn[1] === 'min' ? Math.min(...args) : Math.max(...args);
    }
    // арифметика: + − * / со скобками, единицы px % vw rem
    const toks = s.match(/\d*\.?\d+(?:e-?\d+)?(?:px|%|vw|rem|em)?|[-+*/()]|calc|clamp|min|max/g) ?? [];
    let i = 0;
    const num = (t: string) => {
      const v = parseFloat(t);
      if (t.endsWith('%')) return (v / 100) * base;
      if (t.endsWith('vw')) return (v / 100) * VW;
      if (t.endsWith('rem') || t.endsWith('em')) return v * 16;
      return v;
    };
    const atom = (): number => {
      const t = toks[i++];
      if (t === '(') { const v = sum(); i++; return v; }
      if (t === '-') return -atom();
      if (t === 'calc' || t === 'clamp' || t === 'min' || t === 'max') {
        // вложенная функция: собрать до парной скобки и посчитать отдельно
        let depth = 0, j = i; const start = i;
        for (; j < toks.length; j++) { if (toks[j] === '(') depth++; else if (toks[j] === ')') { depth--; if (depth === 0) break; } }
        const inner = toks.slice(start + 1, j).join(' ');
        i = j + 1;
        return this.expr(`${t}(${inner})`, base);
      }
      return num(t);
    };
    const prod = (): number => { let v = atom(); while (toks[i] === '*' || toks[i] === '/') { const op = toks[i++]; const r = atom(); v = op === '*' ? v * r : v / r; } return v; };
    const sum = (): number => { let v = prod(); while (toks[i] === '+' || toks[i] === '-') { const op = toks[i++]; const r = prod(); v = op === '+' ? v + r : v - r; } return v; };
    return sum();
  }

  private splitArgs(s: string): string[] {
    const out: string[] = []; let depth = 0, cur = '';
    for (const ch of s) {
      if (ch === '(') depth++;
      if (ch === ')') depth--;
      if (ch === ',' && depth === 0) { out.push(cur); cur = ''; } else cur += ch;
    }
    out.push(cur);
    return out;
  }

  private tw(el: Element, prefix: string): number | null {
    // Tailwind: p-5, px-2, pb-1 … (×4 px)
    for (const c of Array.from(el.classList)) {
      const m = c.match(new RegExp(`^${prefix}-(\\d+(?:\\.5)?)$`));
      if (m) return parseFloat(m[1]) * 4;
    }
    return null;
  }

  private pad(el: Element, w: number) {
    const st = (el as HTMLElement).style;
    const all = this.tw(el, 'p') ?? 0;
    const px = this.tw(el, 'px'), py = this.tw(el, 'py');
    const side = (n: 'top' | 'bottom' | 'left' | 'right', twk: string, axis: number | null) =>
      this.len(st.getPropertyValue(`padding-${n}`), w) ?? this.tw(el, twk) ?? axis ?? all;
    return { t: side('top', 'pt', py), b: side('bottom', 'pb', py), l: side('left', 'pl', px), r: side('right', 'pr', px) };
  }

  private skip(el: Element): boolean {
    const cls = el.getAttribute('class') ?? '';
    if (SKIP_CLASS.some((c) => cls.includes(c))) return true;
    if (el.querySelector(':scope > span > svg[aria-label="Frame"], :scope > svg[aria-label="Frame"]') && (el as HTMLElement).style.position === 'absolute') return true;
    const st = (el as HTMLElement).style;
    return st.display === 'none' || st.visibility === 'hidden' && false;
  }

  private color(raw: string | null | undefined, fallback = this.theme['--text-primary']): string {
    return raw ? this.vars(raw) : fallback;
  }

  // ── раскладка ─────────────────────────────────────────────────────────────
  /** Положить элемент в ширину w с левым верхним углом (x, y); вернуть занятую высоту. cb — содержащий блок для absolute. */
  place(el: Element, x: number, y: number, w: number, cb: Box): number {
    if (this.skip(el)) return 0;
    const tag = el.tagName.toLowerCase();
    const st = (el as HTMLElement).style;
    if (tag === 'svg') return this.svg(el, x, y, w, cb.h || null);   // height="100%" — от содержащего блока

    const mt = this.len(st.marginTop, w) ?? 0, mb = this.len(st.marginBottom, w) ?? 0;
    y += mt;
    const p = this.pad(el, w);
    let h = this.len(st.height, cb.h);
    const ar = st.getPropertyValue('aspect-ratio');
    if (h == null && ar) {
      const r = this.len(ar, 0) ?? 2;
      h = w / r;
      const mn = this.len(st.minHeight, cb.h), mx = this.len(st.maxHeight, cb.h);
      if (mn != null) h = Math.max(h, mn);
      if (mx != null) h = Math.min(h, mx);
    }
    const inner = { x: x + p.l, y: y + p.t, w: w - p.l - p.r };
    // точка/кружок: div с фоном и скруглением
    const bg = st.background || st.backgroundColor;
    if (bg && !el.childNodes.length) {
      const bw = this.len(st.width, w) ?? 0, bh = this.len(st.height, w) ?? 0;
      const rad = st.borderRadius === '50%' ? Math.min(bw, bh) / 2 : this.len(st.borderRadius, bw) ?? 0;
      this.out.push(`<rect x="${x}" y="${y}" width="${bw}" height="${bh}" rx="${rad}" fill="${this.color(bg)}"/>`);
      return bh;
    }
    // текст
    const textOnly = Array.from(el.childNodes).every((n) => n.nodeType === 3 || (n as Element).tagName === 'BR');
    if (textOnly && (el.textContent ?? '').trim()) return this.text(el, x, y, w);

    const isRow = st.display === 'flex' || el.classList.contains('flex');
    const justify = st.justifyContent || (el.classList.contains('justify-center') ? 'center' : el.classList.contains('justify-between') ? 'space-between' : '');
    const kids = Array.from(el.children).filter((k) => !this.skip(k));
    const flow = kids.filter((k) => !this.isAbs(k));
    let used = 0;
    if (isRow && !el.classList.contains('flex-col')) {
      const gap = this.len(st.gap, w) ?? 0;
      const ws = flow.map((k) => this.natW(k, inner.w));
      const total = ws.reduce((a, b) => a + b, 0) + gap * Math.max(0, flow.length - 1);
      let cx = inner.x, step = gap;
      if (justify === 'center') cx = inner.x + (inner.w - total) / 2;
      if (justify === 'space-between' && flow.length > 1) step = (inner.w - ws.reduce((a, b) => a + b, 0)) / (flow.length - 1);
      let rowH = 0;
      const box: Box = { x: inner.x, y: inner.y, w: inner.w, h: h ?? 0 };
      flow.forEach((k, i) => { rowH = Math.max(rowH, this.place(k, cx, inner.y, ws[i], box)); cx += ws[i] + step; });
      used = rowH;
    } else {
      let cy = inner.y;
      const box: Box = { x: inner.x, y: inner.y, w: inner.w, h: (h ?? 0) - p.t - p.b };
      for (const k of flow) cy += this.place(k, inner.x, cy, inner.w, box);
      used = cy - inner.y;
    }
    const height = h ?? used + p.t + p.b;
    // absolute-дети: содержащий блок — этот элемент, если он relative/absolute, иначе переданный
    const positioned = st.position === 'relative' || st.position === 'absolute' || el.classList.contains('relative') || el.classList.contains('absolute');
    const myBox: Box = positioned ? { x, y, w, h: height } : cb;
    for (const k of kids.filter((k) => this.isAbs(k))) this.abs(k, myBox);
    // разделитель панелей
    if (el.classList.contains('border-b')) this.out.push(`<line x1="${x}" x2="${x + w}" y1="${y + height}" y2="${y + height}" stroke="${this.theme['--border-color']}" stroke-opacity="0.25" stroke-width="1"/>`);
    return height + mt + mb;
  }

  private isAbs(el: Element) {
    return (el as HTMLElement).style.position === 'absolute' || el.classList.contains('absolute');
  }

  /** Абсолютный элемент в содержащем блоке cb. */
  abs(el: Element, cb: Box) {
    if (this.skip(el)) return;
    const st = (el as HTMLElement).style;
    const L = this.len(st.left, cb.w), R = this.len(st.right, cb.w), T = this.len(st.top, cb.h), B = this.len(st.bottom, cb.h);
    let w = this.len(st.width, cb.w), h = this.len(st.height, cb.h);
    if (w == null && L != null && R != null) w = cb.w - L - R;
    if (h == null && T != null && B != null) h = cb.h - T - B;
    const tag = el.tagName.toLowerCase();
    if (w == null) w = tag === 'svg' ? Number(el.getAttribute('width')) || 0 : this.natW(el, cb.w);
    let x = L != null ? cb.x + L : R != null ? cb.x + cb.w - R - w : cb.x;
    // высота содержимого нужна для bottom/translateY
    const content = h ?? this.measureH(el, w, cb);
    let y = T != null ? cb.y + T : B != null ? cb.y + cb.h - B - content : cb.y;
    const tr = st.transform || '';
    if (/translateX\(-50%\)|translate\(-50%/.test(tr)) x -= w / 2;
    if (/translateY\(-50%\)|translate\(-50%, -50%\)/.test(tr)) y -= content / 2;
    const opacity = st.opacity ? Number(st.opacity) : 1;
    const start = this.out.length;
    if (tag === 'svg') this.svg(el, x, y, w, h);
    else {
      // свой блок для потомков: размер известен
      const box: Box = { x, y, w, h: h ?? content };
      const p = this.pad(el, w);
      const bg = st.background || st.backgroundColor;
      if (bg && !el.childNodes.length) this.place(el, x, y, w, box);
      else {
        const textOnly = Array.from(el.childNodes).every((n) => n.nodeType === 3);
        if (textOnly && (el.textContent ?? '').trim()) this.text(el, x, y, w, st.textAlign);
        else {
          const isRow = el.classList.contains('flex') || st.display === 'flex';
          if (isRow) this.place(el, x, y, w, box);
          else {
            let cy = y + p.t;
            for (const k of Array.from(el.children)) {
              if (this.isAbs(k)) this.abs(k, box);
              else cy += this.place(k, x + p.l, cy, w - p.l - p.r, box);
            }
          }
        }
      }
    }
    if (opacity < 1 && this.out.length > start) {
      const part = this.out.splice(start).join('');
      this.out.push(`<g opacity="${opacity}">${part}</g>`);
    }
  }

  /** Естественная ширина элемента в строке. */
  private natW(el: Element, avail: number): number {
    const tag = el.tagName.toLowerCase();
    const st = (el as HTMLElement).style;
    if (tag === 'svg') return Number(el.getAttribute('width')) || avail;
    const w = this.len(st.width, avail);
    if (w != null) return w;
    if (Array.from(el.childNodes).every((n) => n.nodeType === 3)) {
      const { size, weight } = this.font(el);
      return this.measure(el.textContent ?? '', size, weight);
    }
    const kids = Array.from(el.children);
    if (kids.length === 1) return this.natW(kids[0], avail);
    return avail;
  }

  private measureH(el: Element, w: number, cb: Box): number {
    const st = (el as HTMLElement).style;
    const h = this.len(st.height, cb.h);
    if (h != null) return h;
    const tag = el.tagName.toLowerCase();
    if (tag === 'svg') return Number(el.getAttribute('height')) || 0;
    if ((el.textContent ?? '').trim() && Array.from(el.querySelectorAll('*')).every((k) => k.tagName === 'SPAN' || k.tagName === 'BR')) {
      return this.font(el).size * 1.2;
    }
    const save = this.out.length;
    const used = this.place(el, 0, 0, w, cb);
    this.out.splice(save);
    return used;
  }

  private font(el: Element) {
    let cur: Element | null = el, size: number | null = null, weight: number | null = null, color: string | null = null;
    while (cur && (size == null || weight == null || color == null)) {
      const st = (cur as HTMLElement).style;
      if (size == null && st.fontSize) size = this.len(st.fontSize, 16);
      if (weight == null) {
        if (st.fontWeight) weight = Number(st.fontWeight) || 400;
        else if (cur.classList.contains('font-bold')) weight = 700;
        else if (cur.classList.contains('font-semibold')) weight = 600;
        else if (cur.classList.contains('font-medium')) weight = 500;
      }
      if (color == null && st.color) color = st.color;
      cur = cur.parentElement;
    }
    return { size: size ?? 14, weight: weight ?? 400, color: this.color(color) };
  }

  private text(el: Element, x: number, y: number, w: number, align?: string): number {
    // текст может сидеть во вложенном span со своим стилем
    const leaf = el.children.length === 1 && el.children[0].tagName === 'SPAN' ? el.children[0] : el;
    const { size, weight, color } = this.font(leaf);
    const t = (el.textContent ?? '').trim();
    const tw = this.measure(t, size, weight);
    const tx = align === 'center' ? x + w / 2 : align === 'right' ? x + w : x;
    const anchor = align === 'center' ? 'middle' : align === 'right' ? 'end' : 'start';
    const esc = t.replace(/&/g, '&amp;').replace(/</g, '&lt;');
    this.texts.push({ x: anchor === 'middle' ? tx - tw / 2 : anchor === 'end' ? tx - tw : tx, y, w: tw, h: size * 1.2 });
    this.out.push(`<text x="${tx}" y="${y + size * 0.95}" font-size="${size}" font-weight="${weight}" fill="${color}" text-anchor="${anchor}">${esc}</text>`);
    return size * 1.2;
  }

  private svg(el: Element, x: number, y: number, w: number, h: number | null): number {
    const aw = el.getAttribute('width'), ah = el.getAttribute('height');
    const W = aw && !aw.endsWith('%') ? Number(aw) : w;
    const H = ah && !ah.endsWith('%') ? Number(ah) : h ?? 0;
    const clone = el.cloneNode(true) as Element;
    clone.setAttribute('x', String(x)); clone.setAttribute('y', String(y));
    clone.setAttribute('width', String(W)); clone.setAttribute('height', String(H));
    clone.setAttribute('overflow', 'visible');
    clone.removeAttribute('style');
    this.out.push(this.ser.serializeToString(clone));
    this.placed.push({ el, x, y, w: W, h: H });
    return H;
  }
}
