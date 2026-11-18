/**
 * Карточки рубрик канала (посты @FrameTool «Сделки фондов» t.me/FrameTool/220 и «Потоки капитала» /205) для
 * @framesignalbot — без браузера, на серверном canvas. Раскладка и цвета — как у генераторов постов
 * (research/content_cards/posts/2026-08_fund_trades/cards.py): 1080×1200, бумага, белые строки, Inter.
 * Имена и логотипы бумаг — функциями сайта (fundConfig.fundAssetName / resolveFundIconTicker, InstrumentIcon).
 *
 *   kind: 'cover' — обложка (фон assets/cover_bg.jpg, заголовок, пилюля месяца)
 *         'list'  — заголовок, подзаголовок, строки: [логотип] название … значение [мелкая подпись]
 *         'tiles' — «Портфель фондов»: плитки по долям, цвет бумаги как на сайте
 */
import fs from 'node:fs';
import { createCanvas, loadImage, type SKRSContext2D } from '@napi-rs/canvas';
import { fundAssetName, resolveFundIconTicker, fundAssetColor } from '/src/config/fundConfig';
import { STOCK_LOGO_OVERRIDE, FUT_BASE_TO_LOGO } from '/src/components/InstrumentIcon';

const W = 1080, H = 1200;
const INK = '#0A0A0A', PAPER = '#F4F1EA', WHITE = '#FFFFFF', GREY = '#8C8676', UP = '#0A7A45', DOWN = '#C8361D';
const COLORS: Record<string, string> = { up: UP, down: DOWN, ink: INK, grey: GREY };
const here = (p: string) => new URL(p, import.meta.url).pathname;

type Row = { name?: string; asset?: string; isin?: string; ticker?: string; value: string; unit?: string; color?: string; note?: string };
type Tile = { name?: string; asset?: string; isin?: string; ticker?: string; weight: number; color?: string };
export type Card = { kind: 'cover' | 'list' | 'tiles'; title: string | string[]; sub?: string; pill?: string; rows?: Row[]; tiles?: Tile[]; logos?: boolean };

function font(ctx: SKRSContext2D, size: number, weight: number, spacing = 0) {
  ctx.font = `${weight} ${size}px Inter`;
  try { (ctx as unknown as { letterSpacing: string }).letterSpacing = `${spacing}px`; } catch { /* старый canvas */ }
}
function rrect(ctx: SKRSContext2D, x: number, y: number, w: number, h: number, r: number) {
  ctx.beginPath(); ctx.moveTo(x + r, y); ctx.arcTo(x + w, y, x + w, y + h, r); ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r); ctx.arcTo(x, y, x + w, y, r); ctx.closePath();
}
/** Перенос по словам в ширину w — строки подзаголовка. */
function wrap(ctx: SKRSContext2D, text: string, w: number): string[] {
  const out: string[] = []; let line = '';
  for (const word of text.split(' ')) {
    const t = line ? `${line} ${word}` : word;
    if (ctx.measureText(t).width > w && line) { out.push(line); line = word; } else line = t;
  }
  if (line) out.push(line);
  return out;
}

function who(r: { name?: string; asset?: string; isin?: string; ticker?: string }) {
  const ticker = r.ticker ?? resolveFundIconTicker(r.asset, r.isin);
  const name = r.name ?? fundAssetName(r.asset, r.isin);
  return { ticker, name };
}
/** Файл логотипа как у TickerLogo сайта: свой знак (Полюс) → STOCK_LOGO_OVERRIDE → /logos/sm/<тикер>.png. */
function logoFile(ticker?: string): string | null {
  if (!ticker) return null;
  const dir = process.env.LOGO_DIR ?? here('./logos');
  const indiv = `${dir}/indiv/${ticker}.png`;
  if (fs.existsSync(indiv)) return indiv;
  // как InstrumentIcon: акция → свой знак; гособлигация ('RB') и база фьючерса → FUT_BASE_TO_LOGO (герб и т. п.)
  const t = (STOCK_LOGO_OVERRIDE as Record<string, string>)[ticker] ?? (FUT_BASE_TO_LOGO as Record<string, string>)[ticker] ?? ticker;
  for (const f of [`${dir}/sm/${t}.png`, `${dir}/sm/${ticker}.png`]) if (fs.existsSync(f)) return f;
  return null;
}

async function head(ctx: SKRSContext2D, c: Card, top: number): Promise<number> {
  ctx.fillStyle = INK; font(ctx, 96, 700, -3.4); ctx.textBaseline = 'alphabetic';
  const title = Array.isArray(c.title) ? c.title.join(' ') : c.title;
  ctx.fillText(title, 64, top + 82);
  let y = top + 96;
  if (c.sub) {
    font(ctx, 40, 500); ctx.fillStyle = GREY;
    for (const l of wrap(ctx, c.sub, W - 128)) { y += 50; ctx.fillText(l, 64, y + 10); }
    y += 22;
  }
  return y + 28;
}

async function list(ctx: SKRSContext2D, c: Card) {
  const rows = c.rows ?? [];
  let y = await head(ctx, c, 84);
  const bottom = 84 + 1032, gap = 14;
  const rh = Math.min(150, (bottom - y - gap * (rows.length - 1)) / Math.max(rows.length, 1));
  const logos = c.logos ?? rows.some((r) => r.asset || r.isin || r.ticker);
  for (const r of rows) {
    ctx.fillStyle = WHITE; rrect(ctx, 64, y, W - 128, rh, 32); ctx.fill();
    let x = 94;
    const { ticker, name } = who(r);
    if (logos) {
      const ls = Math.min(104, rh - 20), f = logoFile(ticker);
      ctx.save(); ctx.beginPath(); ctx.arc(x + ls / 2, y + rh / 2, ls / 2, 0, Math.PI * 2); ctx.closePath(); ctx.clip();
      ctx.fillStyle = '#fff'; ctx.fill();
      if (f) ctx.drawImage(await loadImage(fs.readFileSync(f)), x, y + (rh - ls) / 2, ls, ls);
      else { ctx.fillStyle = fundAssetColor(r.asset, r.isin) ?? GREY; ctx.fill(); ctx.fillStyle = WHITE; font(ctx, ls * 0.45, 700); ctx.textAlign = 'center'; ctx.fillText((name || '?')[0].toUpperCase(), x + ls / 2, y + rh / 2 + ls * 0.16); ctx.textAlign = 'left'; }
      ctx.restore();
      x += ls + 26;
    }
    // значение справа: крупно число, мелко единицы; под ним — мелкая подпись (было в прошлом месяце)
    const color = COLORS[r.color ?? 'ink'] ?? r.color ?? INK;
    const right = W - 64 - 38;
    font(ctx, 36, 600, -0.4); const uw = r.unit ? ctx.measureText(r.unit).width + 10 : 0;
    const big = r.note ? 50 : 58;
    font(ctx, big, 700, -1.7); const vw = ctx.measureText(r.value).width;
    const vy = r.note ? y + rh / 2 + 4 : y + rh / 2 + big * 0.36;
    ctx.fillStyle = color; ctx.fillText(r.value, right - uw - vw, vy);
    if (r.unit) { font(ctx, 36, 600, -0.4); ctx.fillText(r.unit, right - uw + 10, vy); }
    if (r.note) { font(ctx, 26, 500); ctx.fillStyle = GREY; const nw = ctx.measureText(r.note).width; ctx.fillText(r.note, right - nw, vy + 38); }
    // название: ужимаем кегль, пока не влезет до значения
    let ns = 50; const room = right - uw - vw - 30 - x;
    font(ctx, ns, 600, -1.4);
    while (ns > 30 && ctx.measureText(name).width > room) { ns -= 2; font(ctx, ns, 600, -1.4 * ns / 50); }
    ctx.fillStyle = INK; ctx.fillText(name, x, y + rh / 2 + ns * 0.36);
    y += rh + gap;
  }
}

async function tiles(ctx: SKRSContext2D, c: Card) {
  const ts = c.tiles ?? [];
  const y0 = await head(ctx, c, 84), bottom = 84 + 1032, gap = 12;
  // ряды 3 / 3 / 4 (как пост сентября): ширина плитки в ряду — по доле
  const rowsN = ts.length > 6 ? [3, 3, ts.length - 6] : [Math.ceil(ts.length / 2), Math.floor(ts.length / 2)];
  const rowW = rowsN.map((n, k) => ts.slice(rowsN.slice(0, k).reduce((a, b) => a + b, 0), rowsN.slice(0, k).reduce((a, b) => a + b, 0) + n).reduce((a, t) => a + t.weight, 0));
  const totalW = rowW.reduce((a, b) => a + b, 0);
  const avail = bottom - y0 - gap * (rowsN.length - 1);
  let y = y0, i = 0;
  for (let k = 0; k < rowsN.length; k++) {
    const rh = Math.max(150, avail * (rowW[k] / totalW));
    const items = ts.slice(i, i + rowsN[k]); i += rowsN[k];
    const sum = items.reduce((a, t) => a + t.weight, 0);
    let x = 64; const wAll = W - 128 - gap * (items.length - 1);
    for (const t of items) {
      const w = wAll * (t.weight / sum);
      const { ticker, name } = who(t);
      ctx.fillStyle = t.color ?? fundAssetColor(t.asset, t.isin) ?? '#6B6B6B'; rrect(ctx, x, y, w, Math.min(rh, bottom - y), 22); ctx.fill();
      const f = logoFile(ticker);
      if (f) {
        ctx.save(); ctx.beginPath(); ctx.arc(x + 46, y + 46, 26, 0, Math.PI * 2); ctx.clip();
        ctx.fillStyle = '#fff'; ctx.fill(); ctx.drawImage(await loadImage(fs.readFileSync(f)), x + 20, y + 20, 52, 52); ctx.restore();
      }
      const ns = w < 200 ? 26 : 32;
      font(ctx, ns, 600, -0.6); ctx.fillStyle = WHITE;
      let nm = name; while (ctx.measureText(nm).width > w - 36 && nm.length > 3) nm = nm.slice(0, -2) + '…';
      const ph = Math.min(rh, bottom - y);
      ctx.fillText(nm, x + 20, y + ph - 70);
      font(ctx, w < 200 ? 44 : 56, 800, -1.6);
      ctx.fillText(`${t.weight.toFixed(1).replace('.', ',')}%`, x + 18, y + ph - 22);
      x += w + gap;
    }
    y += rh + gap;
  }
}

async function cover(ctx: SKRSContext2D, c: Card) {
  ctx.drawImage(await loadImage(fs.readFileSync(here('./assets/cover_bg.jpg'))), 0, 0, W, H);
  ctx.fillStyle = INK; font(ctx, 200, 800, -8.5);
  const lines = Array.isArray(c.title) ? c.title : [c.title];
  lines.forEach((l, k) => ctx.fillText(l, 94, 90 + 160 + k * 180));
  if (c.pill) {
    font(ctx, 58, 600, -1); const pw = ctx.measureText(c.pill).width + 92, py = 50 + 450 + (lines.length - 2) * 180;
    ctx.fillStyle = INK; rrect(ctx, 100, py, pw, 104, 52); ctx.fill();
    ctx.fillStyle = PAPER; ctx.fillText(c.pill, 146, py + 72);
  }
}

export async function renderCard(c: Card, out: string) {
  const cv = createCanvas(W, H), ctx = cv.getContext('2d');
  ctx.fillStyle = PAPER; ctx.fillRect(0, 0, W, H);
  if (c.kind === 'cover') await cover(ctx, c);
  else if (c.kind === 'tiles') await tiles(ctx, c);
  else await list(ctx, c);
  fs.writeFileSync(out, cv.toBuffer('image/png'));
}
