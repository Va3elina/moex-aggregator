/**
 * Бейдж уровня уведомления на графике: колокольчик + крестик (удалить), как в
 * TradingView. Живёт в DOM рядом с пунктиром алерта и держит ФИКСИРОВАННЫЙ
 * отступ от шкалы цены, а не от края панели.
 *
 * Почему DOM, а не канвас: иконки и ховер в вёрстке проще и чётче, а таких
 * штук на графике единицы. Но координата Y уровня меняется на каждый
 * вертикальный зум/автомасштаб, а события на это у движка нет. Поэтому рядом
 * есть RenderTickPrimitive — «пустой» примитив на серии-носителе: движок зовёт
 * его draw() ровно тогда, когда перерисовывает пейн, и в этот момент бейдж
 * переставляется по свежему priceToCoordinate. Сам он ничего не рисует.
 */
import type {
  IPrimitivePaneRenderer, IPrimitivePaneView, ISeriesPrimitive, PrimitivePaneViewZOrder, Time,
} from 'lightweight-charts';

/** Зазор между бейджем и кромкой шкалы. Фиксированный по ТЗ. */
const AXIS_GAP = 6;

/** Единый серый уведомления: фон бейджа, пунктир уровня и подпись на оси.
 *  Серый, чтобы не путать с пилсом текущего значения (тот в цвете серии). */
export function alertTone(dark: boolean) {
  return {
    bg: dark ? '#4A4A52' : '#BDB8AD',
    fg: dark ? '#E7E2D6' : '#26262B',
  };
}

const BELL_SVG = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" '
  + 'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
  + '<path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10.3 21a1.94 1.94 0 0 0 3.4 0"/></svg>';
const X_SVG = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" '
  + 'stroke-linecap="round" aria-hidden="true"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>';

export interface PriceLineBadge {
  el: HTMLDivElement;
  /** Переставить на уровень `y` (px от верха панели); `axisW` — ширина шкалы
   *  СВОЕЙ стороны: бейдж ставится с её внутренней кромки, отступ AXIS_GAP. */
  place(y: number | null, axisW: number, paneHeight: number): void;
  setTheme(dark: boolean): void;
  destroy(): void;
}

export interface PriceLineBadgeOptions {
  side: 'left' | 'right';
  titleBell: string;
  titleRemove: string;
  onRemove: () => void;
}

export function createPriceLineBadge(o: PriceLineBadgeOptions): PriceLineBadge {
  const el = document.createElement('div');
  el.dataset.exportIgnore = 'true';
  el.style.cssText = 'position:absolute;display:none;align-items:stretch;z-index:6;height:20px;'
    + 'border-radius:4px;overflow:hidden;transform:translateY(-50%);user-select:none;'
    + 'box-shadow:0 1px 2px rgba(0,0,0,0.18);';
  const cell = (html: string, title: string) => {
    const c = document.createElement('div');
    c.style.cssText = 'display:flex;align-items:center;justify-content:center;width:22px;';
    c.innerHTML = html;
    c.title = title;
    return c;
  };
  const bell = cell(BELL_SVG, o.titleBell);
  const x = cell(X_SVG, o.titleRemove);
  x.style.cursor = 'pointer';
  x.setAttribute('role', 'button');
  x.setAttribute('aria-label', o.titleRemove);
  // Клик по крестику не должен уходить в канвас: там его подхватит хит-тест
  // «плюса»/фигур и пан.
  const stop = (e: Event) => { e.stopPropagation(); };
  x.addEventListener('mousedown', stop);
  x.addEventListener('pointerdown', stop);
  x.addEventListener('click', (e) => { e.stopPropagation(); e.preventDefault(); o.onRemove(); });
  el.appendChild(bell);
  el.appendChild(x);

  let dark = true;
  const paint = (hover: boolean) => {
    const { bg, fg } = alertTone(dark);
    el.style.background = bg;
    bell.style.color = fg;
    // Разделитель ячеек — тонкая линия цвета иконок.
    x.style.borderLeft = `1px solid ${dark ? 'rgba(231,226,214,0.18)' : 'rgba(38,38,43,0.18)'}`;
    x.style.color = hover ? 'var(--accent, #FF5C2B)' : fg;
    x.style.background = hover ? (dark ? 'rgba(231,226,214,0.10)' : 'rgba(38,38,43,0.08)') : 'transparent';
  };
  x.addEventListener('mouseenter', () => paint(true));
  x.addEventListener('mouseleave', () => paint(false));
  paint(false);

  return {
    el,
    place(y, axisW, paneHeight) {
      if (y == null || !Number.isFinite(y) || y < 0 || y > paneHeight) { el.style.display = 'none'; return; }
      if (o.side === 'left') { el.style.left = `${axisW + AXIS_GAP}px`; el.style.right = 'auto'; }
      else { el.style.right = `${axisW + AXIS_GAP}px`; el.style.left = 'auto'; }
      el.style.top = `${y}px`;
      el.style.display = 'flex';
    },
    setTheme(d) { dark = d; paint(false); },
    destroy() { el.parentNode?.removeChild(el); },
  };
}

/** Примитив-«тикер» перерисовки: ничего не рисует, только сообщает, что пейн
 *  перерисован и координаты по оси Y свежие. */
export class RenderTickPrimitive implements ISeriesPrimitive<Time> {
  private readonly _views: IPrimitivePaneView[];
  constructor(onDraw: () => void) {
    this._views = [new TickPaneView(onDraw)];
  }
  paneViews(): readonly IPrimitivePaneView[] { return this._views; }
}

class TickPaneView implements IPrimitivePaneView {
  private readonly _r: IPrimitivePaneRenderer;
  constructor(onDraw: () => void) {
    this._r = { draw: () => { onDraw(); } };
  }
  zOrder(): PrimitivePaneViewZOrder { return 'bottom'; }
  renderer(): IPrimitivePaneRenderer | null { return this._r; }
}
