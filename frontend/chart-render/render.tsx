/**
 * Тестовый движок картинок сигналов: график сайта (SimpleChart — тот же компонент, что на /oi) рисуется без браузера.
 * Компонент запускается как есть в эмуляции страницы (jsdom): отрабатывают его эффекты (ширина, раскрытие, шрифты), из
 * готовой разметки берётся SVG, переменные темы подставляются значениями editorial-light/dark (сняты с сайта), SVG → PNG
 * (resvg), рамка — функцией экспорта сайта composeFramedCanvas на серверном canvas. Код сайта не меняется.
 *
 *   node dist/render.mjs <card.json> <out.png>
 */
import fs from 'node:fs';
import { JSDOM } from 'jsdom';
import { createCanvas, loadImage, GlobalFonts } from '@napi-rs/canvas';
import { Resvg } from '@resvg/resvg-js';
import { Layout } from './layout';
import { Annotator } from './annotate';

const [, , cardPath, outPath] = process.argv;
const card = JSON.parse(fs.readFileSync(cardPath, 'utf8'));
const THEME_NAME = card.theme ?? 'editorial-light';          // светлая тема по умолчанию (Вадим 18.11)
const THEME: Record<string, string> = JSON.parse(fs.readFileSync(new URL(`./theme-${THEME_NAME}.json`, import.meta.url), 'utf8'));
// шрифт сайта — Inter (fonts/*.ttf рядом со сборкой): и для подписей графика, и для шапки рамки
const FONTS = ['Regular', 'Medium', 'SemiBold', 'Bold', 'ExtraBold'].map((w) => new URL(`./fonts/Inter-${w}.ttf`, import.meta.url).pathname);
for (const f of FONTS) GlobalFonts.registerFromPath(f, 'Inter');
const VW = 1440;                      // ширина «экрана» — от неё у сайта кегль осей
const W = card.width ?? 1000;         // ширина графика
const H = card.height ?? 420;

// ── эмуляция страницы ───────────────────────────────────────────────────────
const dom = new JSDOM(`<!doctype html><html data-theme="${THEME_NAME}" data-accent="pumpkin"><body><div id="root"></div></body></html>`, { pretendToBeVisual: true, url: 'https://framedata.ru/signal-card' });
const win = dom.window as unknown as Window & typeof globalThis;
const g = globalThis as Record<string, unknown>;
for (const k of ['window', 'document', 'HTMLElement', 'Element', 'Node', 'SVGElement', 'SVGSVGElement', 'getComputedStyle', 'requestAnimationFrame', 'cancelAnimationFrame', 'localStorage', 'matchMedia', 'CustomEvent', 'Event', 'MutationObserver']) {
  try { Object.defineProperty(g, k, { value: (win as unknown as Record<string, unknown>)[k], configurable: true, writable: true }); } catch { /* только для чтения */ }
}
Object.defineProperty(win.navigator, 'language', { value: 'ru-RU', configurable: true });
Object.defineProperty(g, 'navigator', { value: win.navigator, configurable: true });
Object.defineProperty(g, 'localStorage', { value: win.localStorage, configurable: true, writable: true });
win.localStorage.setItem('theme', THEME_NAME);
win.localStorage.setItem('frame:lang', 'ru');
Object.defineProperty(win, 'innerWidth', { value: VW, configurable: true });
Object.defineProperty(win, 'innerHeight', { value: 900, configurable: true });
win.matchMedia = ((q: string) => ({ matches: false, media: q, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} })) as never;
g.matchMedia = win.matchMedia;
win.requestAnimationFrame = ((cb: FrameRequestCallback) => setTimeout(() => cb(Date.now()), 0)) as never;
g.requestAnimationFrame = win.requestAnimationFrame;
class RO { cb: ResizeObserverCallback; constructor(cb: ResizeObserverCallback) { this.cb = cb; } observe(el: Element) { setTimeout(() => this.cb([{ target: el, contentRect: { width: W, height: H } } as never], this as never), 0); } unobserve() {} disconnect() {} }
g.ResizeObserver = RO; (win as unknown as Record<string, unknown>).ResizeObserver = RO;
// раскладки в jsdom нет: всем блокам — ширина графика
Object.defineProperty(win.HTMLElement.prototype, 'clientWidth', { get() { return W; }, configurable: true });
Object.defineProperty(win.HTMLElement.prototype, 'offsetWidth', { get() { return W; }, configurable: true });
win.HTMLElement.prototype.getBoundingClientRect = function () { return { x: 0, y: 0, top: 0, left: 0, right: W, bottom: H, width: W, height: H, toJSON() {} } as DOMRect; };
// переменные темы: getComputedStyle(...).getPropertyValue('--x') → значение editorial-dark
const origGCS = win.getComputedStyle.bind(win);
const gcs = (el: Element) => { const cs = origGCS(el); return new Proxy(cs, { get(t, p) { if (p === 'getPropertyValue') return (n: string) => THEME[n] ?? t.getPropertyValue(n); const v = Reflect.get(t, p); return typeof v === 'function' ? v.bind(t) : v; } }); };
win.getComputedStyle = gcs as never; g.getComputedStyle = gcs;
// ширина текста — серверный canvas (подписи осей, пилюли, легенда)
const origCreate = win.document.createElement.bind(win.document);
win.document.createElement = ((tag: string, o?: ElementCreationOptions) => (tag === 'canvas' ? createCanvas(10, 10) : origCreate(tag, o))) as never;

// замеры текста и рамок SVG (у jsdom их нет): ширина — по шрифту Inter на серверном canvas, кегль и вес — из атрибутов
const textCtx = createCanvas(10, 10).getContext('2d');
const svgFont = (el: Element) => {
  const size = parseFloat(el.getAttribute('font-size') ?? (el as HTMLElement).style?.fontSize ?? '') || 12;
  const weight = el.getAttribute('font-weight') ?? (el as HTMLElement).style?.fontWeight ?? '400';
  return { size, font: `${weight} ${size}px Inter` };
};
const SVGEl = (win as unknown as { SVGElement: { prototype: Record<string, unknown> } }).SVGElement.prototype;
SVGEl.getComputedTextLength = function (this: Element) { textCtx.font = svgFont(this).font; return textCtx.measureText(this.textContent ?? '').width; };
SVGEl.getBBox = function (this: Element) {
  const { size, font } = svgFont(this); textCtx.font = font;
  const x = parseFloat(this.getAttribute('x') ?? '0') || 0, y = parseFloat(this.getAttribute('y') ?? '0') || 0;
  return { x, y: y - size, width: textCtx.measureText(this.textContent ?? '').width, height: size * 1.2 };
};

// ── рендер компонента сайта ─────────────────────────────────────────────────
const React = await import('react');
const { createRoot } = await import('react-dom/client');
const { default: SimpleChart } = await import('/src/components/SimpleChart');
const { NoChartAnimCtx } = await import('/src/components/chart/chartAnim');
const { ThemeProvider } = await import('/src/contexts/ThemeContext');
const { formatPrice } = await import('/src/utils/formatNumber');
const { composeFramedCanvas } = await import('/src/components/export/composeFramedCanvas');
// модуль языка сборщик поднимает раньше эмуляции страницы — язык ставим явно
const { setLang } = await import('/src/i18n');
setLang('ru');

const root = createRoot(win.document.getElementById('root')!);
// в боевой сборке React нет act(): рендерим и ждём, пока отработают эффекты графика (ширина, раскрытие)
// вид графика — как на соответствующей странице сайта: /oi — SimpleChart, «Деньги в фондах» — FlowsHistogram,
// «Сезонность» (годовая) — YearlySeasonalityChart
const KIND = card.kind ?? 'oi';
let chartEl;
if (KIND === 'flows') {
  const { default: FlowsHistogram } = await import('/src/components/funds/FlowsHistogram');
  chartEl = React.createElement(FlowsHistogram, {
    flowsData: card.flows, loading: false, flowTitle: card.flowTitle, indexData: card.index ?? undefined,
    indexLabel: card.indexLabel, showIndex: !!card.index, height: H,
  });
} else if (KIND === 'season') {
  const { default: YearlySeasonalityChart } = await import('/src/components/seasonality/YearlySeasonalityChart');
  chartEl = React.createElement(YearlySeasonalityChart, {
    yearlyData: card.yearly, tooltip: null, setTooltip: () => {}, chartHeight: H, showCurrentYear: true,
  });
} else {
  chartEl = React.createElement(SimpleChart, {
    data: card.price, secondaryData: card.line, showPrimary: true, showSecondary: true,
    primaryColor: 'var(--chart-line-1)', secondaryColor: card.lineColor ?? 'var(--oi-cyan)', height: H,
    formatValue: formatPrice, niceTicks: true, niceTicksSecondary: true, gridAxis: 'secondary',
    primaryLabel: card.asset, secondaryLabel: card.lineLabel, showValueHeader: false, legendPosition: 'top',
    showDownloadButton: false, showNavigator: false, chartPadding: { left: 120, right: 120 },
    annotations: [], showWatermark: false,   // без меток экспираций и водяного знака (Вадим 18.11)
  });
}
root.render(React.createElement(ThemeProvider, null, React.createElement(NoChartAnimCtx.Provider, { value: true }, chartEl)));
await new Promise((r) => setTimeout(r, 700));
const host = win.document.getElementById('root')!;
if (process.env.CARD_DUMP) fs.writeFileSync(process.env.CARD_DUMP, host.innerHTML);

// ── SVG → PNG ───────────────────────────────────────────────────────────────
const resolveVars = (s: string): string => {
  let prev = '';
  while (prev !== s) {
    prev = s;
    s = s.replace(/var\((--[\w-]+)(?:,\s*([^()]*(?:\([^()]*\))?[^()]*))?\)/g, (_m, n: string, fb?: string) => THEME[n] ?? (fb ?? '').trim() ?? '');
  }
  return s;
};
const ser = new win.XMLSerializer();
if (KIND !== 'oi') {
  // графики из SVG + HTML-обвязки: раскладываем обвязку сами (layout.ts), получаем один SVG
  const L = new Layout(THEME, (t, size, weight) => { textCtx.font = `${weight} ${size}px Inter`; return textCtx.measureText(t).width; }, ser);
  const PADK = 0;
  const top = host.firstElementChild!;
  const h = L.place(top, PADK, PADK, W, { x: 0, y: 0, w: W, h: H });
  const bw = parseFloat(THEME['--card-border-width']), r = parseFloat(THEME['--card-radius']);
  const BW = W, BH = h;
  const marks = card.signal ? annotateKind(L, BW, BH) : '';
  const svgText = resolveVars(`<svg xmlns="http://www.w3.org/2000/svg" width="${BW}" height="${BH}" viewBox="0 0 ${BW} ${BH}" font-family="Inter" color="${THEME['--text-primary']}">`
    + `<rect x="${bw / 2}" y="${bw / 2}" width="${BW - bw}" height="${BH - bw}" rx="${r}" fill="${THEME['--bg-primary']}" stroke="${THEME['--card-border-color']}" stroke-width="${bw}"/>`
    + L.out.join('') + marks + '</svg>');
  if (process.env.CARD_SVG) fs.writeFileSync(process.env.CARD_SVG, svgText);
  await finish(svgText);
}
const svgs = [...host.querySelectorAll('svg')];
const chartSvg = svgs.find((s) => Number(s.getAttribute('width')) >= W - 2)!;
const legendSvgs = svgs.filter((s) => s !== chartSvg && s.closest('.flex.justify-center'));
const S = 2;                                       // плотность картинки
const LEG_H = 34, PAD = 20, BOX_W = W + PAD * 2, BOX_H = H + LEG_H + PAD * 2;
// легенда — по центру, как flex justify-center у сайта
let lx = 0; const gap = 16;
const legW = legendSvgs.reduce((a, s) => a + Number(s.getAttribute('width')) + gap, -gap);
lx = (BOX_W - legW) / 2;
const legend = legendSvgs.map((s) => { const w = Number(s.getAttribute('width')); const out = `<g transform="translate(${lx},${PAD})">${ser.serializeToString(s)}</g>`; lx += w + gap; return out; }).join('');
// HTML-слои поверх графика (водяной знак FRAME, метки смены контракта) — у сайта это блоки с абсолютной позицией
// и своим SVG внутри: переносим их SVG в те же координаты, с той же прозрачностью
const wrap = chartSvg.parentElement!;
const svgH = Number(chartSvg.getAttribute('height'));
const layers = [...wrap.querySelectorAll('div')].filter((d) => {
  const st = (d as HTMLElement).style;
  return (st.position === 'absolute' || d.classList.contains('absolute')) && !d.className.includes('hidden') && d.querySelector('svg');
}).map((d) => {
  const st = (d as HTMLElement).style;
  const inner = d.querySelector('svg')!;
  const ih = Number(inner.getAttribute('height')) || 0;
  const x = parseFloat(st.left) || 0;
  const y = st.top ? parseFloat(st.top) : svgH - (parseFloat(st.bottom) || 0) - ih;
  const op = st.opacity ? Number(st.opacity) : d.classList.contains('opacity-50') ? 0.5 : 1;
  const color = st.color ? ` color="${st.color}"` : '';
  return `<g transform="translate(${x},${y})" opacity="${op}"${color}>${ser.serializeToString(inner)}</g>`;
}).join('');
// ── слои сигнала: зона окна, пунктир прежнего уровня, отметка «сейчас» ─────────
// Геометрия — та же, что у графика сайта: поле начинается со сдвига <g translate(left, top)>, ширина и высота поля —
// из clipPath, X точки i — left + i/(n−1)·(ширина − 10) (CHART_END_GAP сайта), Y правой оси — по её же делениям.
const plotG = chartSvg.querySelector('g[transform^="translate("]')!;
const [padL, padT] = plotG.getAttribute('transform')!.match(/[-\d.]+/g)!.map(Number);
const clip = chartSvg.querySelector('clipPath rect')!;
const plotW = Number(clip.getAttribute('width')), plotH = Number(clip.getAttribute('height'));
const n = card.price.length;
const xOf = (i: number) => padL + (i / Math.max(n - 1, 1)) * (plotW - 10);
const idxOf = (d: string) => { const i = card.price.findIndex((p: { time: string }) => p.time.slice(0, 10) >= d); return i < 0 ? n - 1 : i; };
const secTicks = [...plotG.querySelectorAll('text[text-anchor="start"]')]
  .map((t) => ({ v: Number((t.textContent ?? '').replace(/\s/g, '').replace('−', '-').replace(',', '.')), y: Number(t.getAttribute('y')) }))
  .filter((t) => Number.isFinite(t.v) && Number.isFinite(t.y));
const t0 = secTicks[0], t1 = secTicks[secTicks.length - 1];
const yOf = (v: number) => padT + t0.y + ((v - t0.v) * (t1.y - t0.y)) / (t1.v - t0.v);
// Слои сигнала в стиле разметки графиков на канале (Вадим 18.11): крупная подпись цветом линии в свободном месте,
// стрелка от неё к точке — по пустому полю и с обводкой цветом фона, кружки на прежних пиках с подписью строго над
// или под кружком, тонкий пунктир уровня главного прежнего пика. Заливок и скобок нет.
const priTicks = [...plotG.querySelectorAll('text[text-anchor="end"]')]
  .map((t) => ({ v: Number((t.textContent ?? '').replace(/\s/g, '').replace('−', '-').replace(',', '.')), y: Number(t.getAttribute('y')) }))
  .filter((t) => Number.isFinite(t.v) && Number.isFinite(t.y));
const p0 = priTicks[0], p1 = priTicks[priTicks.length - 1];
const yPri = (v: number) => padT + p0.y + ((v - p0.v) * (p1.y - p0.y)) / (p1.v - p0.v);
const sig = card.signal ?? {};
const COLOR = THEME[(sig.colorVar as string) ?? '--oi-cyan'] ?? THEME['--accent'];
const BG = THEME['--bg-primary'];
const plotR = padL + plotW - 10, plotB = padT + plotH;

// Карта занятости поля сеткой 4 px: линии обеих серий (с вертикальными перепадами между точками) и подписи.
const G = 4, GW = Math.ceil((padL + plotW + 40) / G), GH = Math.ceil((plotB + 40) / G);
const grid = new Float32Array(GW * GH);
const mark = (x: number, y: number, v = 1) => { const c = Math.floor(x / G), r = Math.floor(y / G); if (c >= 0 && r >= 0 && c < GW && r < GH) grid[r * GW + c] += v; };
const trace = (ys: number[]) => ys.forEach((y, i) => {
  const x = xOf(i); mark(x, y);
  if (i > 0) { const y0 = ys[i - 1], x0 = xOf(i - 1), n = Math.ceil(Math.abs(y - y0) / G); for (let k = 1; k < n; k++) mark(x0 + ((x - x0) * k) / n, y0 + ((y - y0) * k) / n); }
});
trace(card.price.map((p: { value: number }) => yPri(p.value)));
trace(card.line.map((p: { value: number }) => yOf(p.value)));
const block = (x: number, y: number, w: number, h: number, v = 50) => { for (let yy = y; yy < y + h; yy += G) for (let xx = x; xx < x + w; xx += G) mark(xx, yy, v); };
const sum = (x: number, y: number, w: number, h: number) => {
  let t = 0; const c0 = Math.max(0, Math.floor(x / G)), c1 = Math.min(GW - 1, Math.floor((x + w) / G));
  const r0 = Math.max(0, Math.floor(y / G)), r1 = Math.min(GH - 1, Math.floor((y + h) / G));
  for (let r = r0; r <= r1; r++) for (let c = c0; c <= c1; c++) t += grid[r * GW + c];
  return t;
};
// цена пути стрелки: линии рядом с отрезком (±8 px) и подписи на пути; конец у самой точки не считаем
const pathCost = (sx: number, sy: number, tx: number, ty: number) => {
  const len = Math.hypot(tx - sx, ty - sy); let c = 0;
  for (let d = 0; d < len - 20; d += 4) { const px = sx + ((tx - sx) * d) / len, py = sy + ((ty - sy) * d) / len; c += sum(px - 8, py - 8, 16, 16); }
  return c;
};

const mc = createCanvas(10, 10).getContext('2d');
const measure = (text: string, size: number, weight: number) => { mc.font = `${weight} ${size}px Inter`; return mc.measureText(text).width; };
// обводка цветом фона под элементом: стрелка и кружки не сливаются с линиями графика
const halo = (inner: string, sw: number) => inner.replace(/stroke="[^"]+"/, `stroke="${BG}"`).replace(/fill="(?!none)[^"]+"/, `fill="${BG}"`)
  .replace(/stroke-width="([\d.]+)"/, (_m, v) => `stroke-width="${Number(v) + sw}"`);
let under = '', over = '';
// обводка текста цветом фона: подпись читается поверх линий, а линии вокруг неё не закрыты
const HALO = (w: number) => `stroke="${BG}" stroke-width="${w}" stroke-linejoin="round" paint-order="stroke"`;

// прежние пики: кружок на точке, подпись (месяц и год) — над кружком, если там свободно, иначе под ним
const lastI = card.line.length - 1;
const tgt = { x: xOf(lastI), y: yOf(card.line[lastI].value) };
// пики не толпятся: не ближе 110 px к точке сигнала и друг к другу (главный — первый в списке)
const peaks: { date: string; value: number; label: string }[] = [];
for (const pk of (sig.peaks ?? []) as { date: string; value: number; label: string }[]) {
  const x = xOf(idxOf(pk.date));
  // главный прежний рекорд (первый) остаётся всегда, кроме совсем вплотную к точке
  const near = peaks.length === 0 ? 40 : 110;
  if (Math.abs(x - tgt.x) < near || peaks.some((q) => Math.abs(xOf(idxOf(q.date)) - x) < 110)) continue;
  peaks.push(pk);
}
// Все отметки одинаковые (Вадим 18.11: «нет порядка, каша»): один размер кружка, один кегль, подпись — месяц и год,
// у всех с одной стороны — над кружками у максимумов, под кружками у минимумов; не влезает хоть одна — все на другую.
// Главный прежний рекорд отличается только пунктиром своего уровня.
const R_PK = 18, SZ_PK = 18, H_PK = SZ_PK + 8;
const low = sig.dir === 'low';
const fits = (above: boolean) => peaks.every((pk) => {
  const cy = yOf(pk.value);
  // над полем до легенды есть ~30 px свободного места — подписи максимумов у самого верха помещаются туда
  return above ? cy - R_PK - 8 - H_PK >= padT - 28 : cy + R_PK + 8 + H_PK <= plotB;
});
const above = low ? !fits(false) && fits(true) : fits(true) || !fits(false);
peaks.forEach((pk, k) => {
  const cx = xOf(idxOf(pk.date)), cy = yOf(pk.value);
  if (k === 0) under += `<line x1="${padL}" x2="${plotR}" y1="${cy}" y2="${cy}" stroke="${COLOR}" stroke-opacity="0.55" stroke-width="1.5" stroke-dasharray="6 6"/>`;
  const ring = `<circle cx="${cx}" cy="${cy}" r="${R_PK}" fill="none" stroke="${COLOR}" stroke-width="3.5"/>`;
  over += halo(ring, 4) + ring;
  block(cx - R_PK - 4, cy - R_PK - 4, 2 * R_PK + 8, 2 * R_PK + 8);
  const w = measure(pk.label, SZ_PK, 700) + 8;
  const lx = Math.min(Math.max(cx, padL + w / 2 + 2), plotR - w / 2 - 2);
  const ly = above ? cy - R_PK - 8 - H_PK : cy + R_PK + 8;
  over += `<text x="${lx}" y="${ly + SZ_PK}" text-anchor="middle" fill="${COLOR}" font-size="${SZ_PK}" font-weight="700" ${HALO(5)}>${pk.label}</text>`;
  block(lx - w / 2, ly, w, H_PK);
});

// крупная подпись и стрелка к точке сигнала
// откуда линия приходит в точку сигнала: направление от точки ~25 px левее к самой точке
const backI = Math.max(0, lastI - Math.max(3, Math.round(25 / Math.max((plotW - 10) / Math.max(n - 1, 1), 0.01))));
const lineIn = Math.atan2(tgt.y - yOf(card.line[backI].value), tgt.x - xOf(backI));
// Правила канала (research/channel_charts/ANNOTATION_RULES.md): текущая точка обведена кружком; подпись выше и левее
// неё в ближайшем пустом месте, может уйти в полосу легенды над полем; стрелка-указатель короткая (≈4–15 % ширины),
// по диагонали, останавливается перед кружком; линии подпись не закрывает.
const ring0 = `<circle cx="${tgt.x}" cy="${tgt.y}" r="18" fill="none" stroke="${COLOR}" stroke-width="3.5"/>`;
over += halo(ring0, 4) + ring0;
block(tgt.x - 24, tgt.y - 24, 48, 48);
// легенда сайта над полем — занята (в координатах графика она выше нуля на LEG_H)
block((BOX_W - legW) / 2 - PAD - 10, -LEG_H - 4, legW + 20, 28);
if (sig.headline) {
  const SZ = 42, LH = 48;
  const words = String(sig.headline).split(' ');
  const za = words.indexOf('за');
  const cut = za > 0 ? za : Math.ceil(words.length / 2);
  const lines = measure(sig.headline, SZ, 800) > plotW * 0.45 && words.length > 1
    ? [words.slice(0, cut).join(' '), words.slice(cut).join(' ')] : [sig.headline];
  const w = Math.max(...lines.map((l: string) => measure(l, SZ, 800))), h = LH * lines.length;
  let best = { x: padL + 10, y: padT + 6, s: Infinity, sx: 0, sy: 0 };
  for (let x = padL + 12; x + w <= plotR - 12; x += 12) {
    for (let y = -LEG_H + 2; y + h <= plotB - 6; y += 8) {
      if (x < tgt.x + 30 && x + w > tgt.x - 30 && y < tgt.y + 30 && y + h > tgt.y - 30) continue;
      // начало стрелки — середина ближней к точке стороны блока
      const sx = tgt.x > x + w ? x + w + 8 : tgt.x < x ? x - 8 : x + w / 2;
      const sy = tgt.x > x + w || tgt.x < x ? y + h / 2 : tgt.y < y ? y - 8 : y + h + 8;
      const len = Math.hypot(tgt.x - sx, tgt.y - sy);
      if (len < 45 || len > 340) continue;                   // указатель короткий, насколько позволяет свободное место
      // стрелка не ложится на линию: её направление не должно совпадать с тем, откуда линия пришла в точку
      const aIn = Math.atan2(tgt.y - sy, tgt.x - sx);
      let dAng = Math.abs(aIn - lineIn); if (dAng > Math.PI) dAng = 2 * Math.PI - dAng;
      const along = dAng < 0.5 ? 1e7 : dAng < 0.8 ? (0.8 - dAng) * 200000 : 0;   // вдоль линии — запрещено
      // выше и левее точки — как обычно на канале; ниже точки — дороже
      const side = y + h > tgt.y ? 400 : 0;
      const sc = sum(x - 8, y - 6, w + 16, h + 12) * 80 + pathCost(sx, sy, tgt.x, tgt.y) * 15 + len * 2 + along + side;
      if (sc < best.s) best = { x, y, s: sc, sx, sy };
    }
  }
  over += lines.map((l: string, k: number) => `<text x="${best.x}" y="${best.y + SZ + k * LH - 6}" fill="${COLOR}" font-size="${SZ}" font-weight="800" letter-spacing="-0.5" ${HALO(8)}>${l}</text>`).join('');
  const ang = Math.atan2(tgt.y - best.sy, tgt.x - best.sx), ex = tgt.x - Math.cos(ang) * 28, ey = tgt.y - Math.sin(ang) * 28;
  const a = ang, L = 15;
  const hx = ex - Math.cos(a) * L * 0.8, hy = ey - Math.sin(a) * L * 0.8;
  const wing = (sgn: number) => `${ex - Math.cos(a) * L + Math.cos(a + sgn * Math.PI / 2) * L * 0.6},${ey - Math.sin(a) * L + Math.sin(a + sgn * Math.PI / 2) * L * 0.6}`;
  const shaft = `<line x1="${best.sx}" y1="${best.sy}" x2="${hx}" y2="${hy}" stroke="${COLOR}" stroke-width="5" stroke-linecap="round"/>`;
  const head = `<polygon points="${ex},${ey} ${wing(1)} ${wing(-1)}" fill="${COLOR}" stroke="${COLOR}" stroke-width="1" stroke-linejoin="round"/>`;
  over += halo(shaft, 6) + halo(head, 6) + shaft + head;
}
const chart = `<g transform="translate(${PAD},${PAD + LEG_H})">${under}${ser.serializeToString(chartSvg)}${layers}${over}</g>`;
const r = parseFloat(THEME['--card-radius']), bw = parseFloat(THEME['--card-border-width']);
const box = `<svg xmlns="http://www.w3.org/2000/svg" width="${BOX_W}" height="${BOX_H}" viewBox="0 0 ${BOX_W} ${BOX_H}" font-family="Inter" color="${THEME['--text-primary']}">`
  + `<rect x="${bw / 2}" y="${bw / 2}" width="${BOX_W - bw}" height="${BOX_H - bw}" rx="${r}" fill="${THEME['--bg-primary']}" stroke="${THEME['--card-border-color']}" stroke-width="${bw}"/>`
  + legend + chart + '</svg>';
const svgText = resolveVars(box).replace(/visibility:\s*hidden;?/g, '');
await finish(svgText);

// ── PNG и рамка экспорта сайта (шапка, подвал) — общий финал всех видов графиков ─────────────────────────────────
async function finish(svgText: string) {
  const png = new Resvg(svgText, { fitTo: { mode: 'zoom', value: 2 }, font: { loadSystemFonts: false, fontFiles: FONTS, defaultFontFamily: 'Inter' } }).render().asPng();
  const raw = await loadImage(png);
  const rawCanvas = createCanvas(raw.width, raw.height); rawCanvas.getContext('2d').drawImage(raw, 0, 0);
  const framed = composeFramedCanvas(rawCanvas as unknown as HTMLCanvasElement, {
    background: THEME['--bg-primary'], textColor: THEME['--text-primary'], textSecondaryColor: THEME['--text-secondary'],
    accentColor: THEME['--accent'], dpr: 2, metadata: card.meta,
  }) as unknown as ReturnType<typeof createCanvas>;
  fs.writeFileSync(outPath, framed.toBuffer('image/png'));
  root.unmount();
  console.log('ok', outPath);
  process.exit(0);
}

// ── разметка сигнала для графиков с раскладкой (фонды, сезонность) ───────────────────────────────────────────────
function nums(s: string | null) { return (s ?? '').match(/-?\d*\.?\d+(?:e-?\d+)?/g)?.map(Number) ?? []; }
/** Точки ломаной из d="M x y L x y …" в координатах итогового SVG. */
function pathPts(d: string, ox: number, oy: number, sx = 1, sy = 1) {
  const v = nums(d); const out: { x: number; y: number }[] = [];
  for (let i = 0; i + 1 < v.length; i += 2) out.push({ x: ox + v[i] * sx, y: oy + v[i + 1] * sy });
  return out;
}
function annotateKind(L: Layout, BW: number, BH: number): string {
  const sig = card.signal;
  const measure = (t: string, size: number, weight: number) => { textCtx.font = `${weight} ${size}px Inter`; return textCtx.measureText(t).width; };
  const big = L.placed.filter((p) => p.w > 300 && p.h > 80);
  if (!big.length) return '';
  const bounds = big.reduce((a, p) => ({ x: Math.min(a.x, p.x), y: Math.min(a.y, p.y), w: Math.max(a.x + a.w, p.x + p.w) - Math.min(a.x, p.x), h: Math.max(a.y + a.h, p.y + p.h) - Math.min(a.y, p.y) }),
    { x: big[0].x, y: big[0].y, w: big[0].w, h: big[0].h });
  const A = new Annotator(bounds, THEME['--bg-primary'], measure, { w: BW, h: BH });
  const color = THEME[sig.colorVar ?? '--accent'] ?? THEME['--accent'];
  // занятость: подписи осей и заголовки, легенды, линии и столбики всех графиков
  for (const t of L.texts) A.block({ x: t.x - 4, y: t.y - 2, w: t.w + 8, h: t.h + 4 });
  for (const p of L.placed) if (!(p.w > 300 && p.h > 80)) A.block({ x: p.x, y: p.y, w: p.w, h: p.h });
  for (const p of big) {
    const vb = nums(p.el.getAttribute('viewBox'));
    const sx = vb.length === 4 ? p.w / vb[2] : 1, sy = vb.length === 4 ? p.h / vb[3] : 1;
    for (const path of Array.from(p.el.querySelectorAll('path'))) {
      const st = path.getAttribute('stroke') ?? '';
      if (!st || st === 'none') continue;
      const pts = pathPts(path.getAttribute('d') ?? '', p.x, p.y, sx, sy);
      // линии сетки и нуля (прямые по горизонтали или вертикали) — не занятость: подпись поверх них читается
      if (pts.every((q) => Math.abs(q.y - pts[0].y) < 1) || pts.every((q) => Math.abs(q.x - pts[0].x) < 1)) continue;
      A.trace(pts, 6);
    }
    for (const rc of Array.from(p.el.querySelectorAll('rect'))) {
      const f = rc.getAttribute('fill') ?? '';
      if (!f.includes('funds-flow')) continue;
      const [x, y, w, h] = ['x', 'y', 'width', 'height'].map((k) => Number(rc.getAttribute(k)) || 0);
      A.block({ x: p.x + x * sx, y: p.y + y * sy, w: Math.max(w * sx, 2), h: h * sy }, 20);
    }
  }
  if (KIND === 'flows') {
    // гистограмма — SVG со столбиками притоков/оттоков; слоты баров — равные доли ширины (ось X «Денег в фондах»)
    const hist = big.find((p) => p.el.querySelector('rect[fill*="funds-flow"]'));
    if (!hist) return '';
    const n = card.flows.flows.length, slot = hist.w / n;
    // у гистограммы свой viewBox — столбики переводим в пиксели итогового SVG
    const vbh = nums(hist.el.getAttribute('viewBox'));
    const hx = vbh.length === 4 ? hist.w / vbh[2] : 1, hy = vbh.length === 4 ? hist.h / vbh[3] : 1;
    const rects = Array.from(hist.el.querySelectorAll('rect[fill*="funds-flow"]')).map((rc) => ({
      x: hist.x + (Number(rc.getAttribute('x')) || 0) * hx, y: hist.y + (Number(rc.getAttribute('y')) || 0) * hy,
      w: (Number(rc.getAttribute('width')) || 0) * hx, h: (Number(rc.getAttribute('height')) || 0) * hy }));
    if (!rects.length) return '';
    const barOf = (i: number) => {
      const cx = hist.x + (i + 0.5) * slot;
      return rects.reduce((b, r) => (Math.abs(r.x + r.w / 2 - cx) < Math.abs(b.x + b.w / 2 - cx) ? r : b), rects[0]);
    };
    const [i0, i1] = sig.hl;
    const bs = Array.from({ length: i1 - i0 + 1 }, (_, k) => barOf(i0 + k));
    const fx0 = hist.x + i0 * slot - 4, fx1 = hist.x + (i1 + 1) * slot + 4;
    const fy0 = Math.min(...bs.map((b) => b.y)) - 6, fy1 = Math.max(...bs.map((b) => b.y + b.h)) + 6;
    // один узкий столбик — кружок на его конце, несколько — рамка вокруг (как на канале)
    const single = i0 === i1 && fx1 - fx0 < 24;
    const endY = sig.dir === 'low' ? bs[0].y + bs[0].h : bs[0].y;
    if (single) A.ring(bs[0].x + bs[0].w / 2, endY, 16, color);
    else A.frame({ x: fx0, y: fy0, w: fx1 - fx0, h: fy1 - fy0 }, color);
    // прежний рекорд — кружок на его столбике, подпись снаружи (у оттока — под, у притока — над)
    if (sig.prev != null) {
      const b = barOf(sig.prev), out = sig.dir === 'low';
      const cy = out ? b.y + b.h : b.y;
      A.ring(b.x + b.w / 2, cy, 14, color);
      if (sig.prevLabel) A.tag(sig.prevLabel, b.x + b.w / 2, cy, 14, color, 18, out ? 'below' : 'above');
    }
    // сумма — с пустой стороны от нуля: у оттока над рамкой, у притока под ней
    const tx = single ? bs[0].x + bs[0].w / 2 : fx0 - 4, ty = single ? endY : (fy0 + fy1) / 2;
    A.headline(sig.headline, tx, ty, color, single ? { gap: 24, above: 40, maxLen: 420 }
      : { gap: 8, above: 40, maxLen: 480, rect: { x: fx0, y: fy0, w: fx1 - fx0, h: fy1 - fy0 } });
    // сумма — после стрелки, со свободной стороны
    if (sig.amount) {
      if (single) A.tag(sig.amount, tx, endY, 16, color, 20, sig.dir === 'low' ? 'below' : 'above', tx + 18);
      // у правого края рамки: середину края оставляем стрелке
      else A.tag(sig.amount, 0, sig.dir === 'low' ? fy0 : fy1, 6, color, 20, sig.dir === 'low' ? 'above' : 'below', fx1);
    }
  } else if (KIND === 'season') {
    // поле сезонности — SVG с viewBox 0..1000 по X (доля года); период — от сегодня на 3 месяца вперёд
    const plot = big.find((p) => p.el.getAttribute('viewBox'));
    if (!plot) return '';
    const x0 = plot.x + sig.from * plot.w, x1 = plot.x + Math.min(sig.to, 1) * plot.w;
    A.frame({ x: x0, y: plot.y + 4, w: Math.max(x1 - x0, 20), h: plot.h - 8 }, color);
    const R = { x: x0, y: plot.y + 4, w: Math.max(x1 - x0, 20), h: plot.h - 8 };
    // окно переходит через Новый год — его начало года тоже в рамке (без подписи)
    if (sig.to > 1) A.frame({ x: plot.x, y: R.y, w: Math.max((sig.to - 1) * plot.w, 20), h: R.h }, color);
    A.headline(sig.headline, x0 - 4, plot.y + plot.h * 0.35, color, { gap: 8, above: 40, maxLen: 420, rect: R });
  }
  return A.out.join('');
}
