/**
 * Слой меток экспираций (смена фьючерсного контракта) — DOM-оверлей над канвасом
 * графика: кружок с двумя буквами кода контракта у оси дат + пунктирная
 * вертикаль-направляющая по наведению.
 *
 * Вынесен из LwChart в общий модуль, потому что его понадобилось показывать и в
 * многопанельном LwChartPanes. Копировать было нельзя: слой рисования, тултип и
 * легенда УЖЕ существуют в проекте в двух экземплярах, и это регулярно приводит
 * к расхождениям — третья копия сделала бы поддержку неуправляемой.
 *
 * Позиции считаются через timeToCoordinate и перерисовываются на зум/пан/ресайз
 * (вызывающий дёргает draw()). Цвета — CSS-переменные: слой DOM, а не канвас,
 * поэтому он следует за темой панели сам.
 */
import type { IChartApi, UTCTimestamp } from 'lightweight-charts';

const SVGNS = 'http://www.w3.org/2000/svg';

export interface ExpirationMark { time: number; label: string; description: string }

export interface ExpirationsLayerOptions {
  /** Контейнер панели (position:relative), в который вешаем слой. */
  box: HTMLElement;
  /** Чарт ЭТОЙ панели — из него берутся timeToCoordinate и ширины ценовых шкал. */
  getChart: () => IChartApi | null;
  getMarks: () => ExpirationMark[] | undefined;
  /**
   * Высота оси дат ПОД слоем. Отдельным колбэком, а не из timeScale() своей
   * панели: в вертикальном стеке ось времени есть только у НИЖНЕЙ панели, а
   * кружки живут на панели цены. Взяли бы высоту у себя — получили бы 0, и
   * кружки встали бы по нижней кромке ценовой панели, оторванные от дат,
   * которые они подписывают.
   */
  getAxisHeight: () => number;
}

export interface ExpirationsLayer {
  /** Перерисовать метки под текущий зум/размер. Идемпотентно. */
  draw: () => void;
  /** Снять слой с DOM. */
  destroy: () => void;
}

export function createExpirationsLayer(o: ExpirationsLayerOptions): ExpirationsLayer {
  const { box, getChart, getMarks, getAxisHeight } = o;

  const layer = document.createElement('div');
  layer.style.cssText = 'position:absolute;left:0;right:0;height:0;pointer-events:none;z-index:4';
  box.appendChild(layer);

  const guide = document.createElement('div');
  guide.style.cssText = 'position:absolute;top:0;width:0;display:none;pointer-events:none;z-index:3;border-left:1px dashed var(--text-secondary,#9A958C);opacity:0.45';
  box.appendChild(guide);

  const plotHeight = () => Math.max(0, box.clientHeight - getAxisHeight());

  const draw = () => {
    const ch = getChart();
    if (!ch) return;
    while (layer.firstChild) layer.removeChild(layer.firstChild);
    guide.style.display = 'none';
    const marks = getMarks();
    if (!marks || marks.length === 0) return;
    const ts = ch.timeScale();
    const axisH = getAxisHeight();
    layer.style.bottom = axisH + 'px';
    // §R2-23: timeToCoordinate меряет от ЛЕВОГО КРАЯ ПОЛЯ (после левой ценовой
    // оси), а слой — от края бокса → переводим (+paneL). Кружок, пересёкший
    // ценовую ось (левую или правую), не рисуем вовсе — иначе при ресайзе/пане
    // он наезжает на числа оси.
    let paneL = 0, rightW = 0;
    try { paneL = ch.priceScale('left').width() || 0; } catch { /* шкала скрыта */ }
    try { rightW = ch.priceScale('right').width() || 0; } catch { /* шкала скрыта */ }
    const paneW = ts.width() || Math.max(0, box.clientWidth - paneL - rightW);
    const R = 17 / 2;   // половина кружка
    for (const ex of marks) {
      const x = ts.timeToCoordinate(ex.time as UTCTimestamp);
      if (x == null || x < R || x > paneW - R) continue;
      const bx = paneL + x;
      // §R2-27: текст в flex-центрированном div съезжал вниз в PNG-экспорте
      // (html2canvas не воспроизводит flex/line-height вертикальное
      // центрирование текста — см. фикс #704 на SimpleChart). Кружок+буквы
      // рисуем инлайн-SVG с dominantBaseline="central" — геометрическая
      // инструкция позиционирования, рендерится одинаково и в браузере, и в
      // html2canvas. opacity — на ВНЕШНЕМ div, не на svg (см. #705: opacity на
      // svg html2canvas применяет дважды — бейкает в растр + ещё раз поверх).
      const circle = document.createElement('div');
      circle.style.cssText = [
        'position:absolute', 'bottom:2px', 'left:' + bx + 'px', 'transform:translateX(-50%)',
        'width:17px', 'height:17px', 'cursor:default',
        'pointer-events:auto', 'opacity:0.5', 'transition:opacity 0.12s', 'box-sizing:border-box',
      ].join(';');
      const circleSvg = document.createElementNS(SVGNS, 'svg');
      circleSvg.setAttribute('width', '17'); circleSvg.setAttribute('height', '17');
      circleSvg.style.cssText = 'position:absolute;inset:0;display:block';
      const ring = document.createElementNS(SVGNS, 'circle');
      ring.setAttribute('cx', '8.5'); ring.setAttribute('cy', '8.5'); ring.setAttribute('r', '8');
      ring.setAttribute('fill', 'var(--bg-secondary,#26262B)');
      ring.setAttribute('stroke', 'var(--border-color,rgba(245,241,232,0.18))');
      ring.setAttribute('stroke-width', '1');
      const label = document.createElementNS(SVGNS, 'text');
      label.setAttribute('x', '8.5'); label.setAttribute('y', '9');
      label.setAttribute('text-anchor', 'middle'); label.setAttribute('dominant-baseline', 'central');
      label.setAttribute('font-size', '8.5'); label.setAttribute('font-weight', '700');
      label.setAttribute('fill', 'var(--text-secondary,#9A958C)');
      label.textContent = (ex.label || '').slice(0, 2);
      circleSvg.appendChild(ring); circleSvg.appendChild(label);
      circle.appendChild(circleSvg);

      const tipEl = document.createElement('div');
      tipEl.style.cssText = [
        'position:absolute', 'bottom:calc(100% + 6px)', 'left:50%', 'transform:translateX(-50%)',
        'display:none', 'white-space:nowrap', 'pointer-events:none', 'z-index:8',
        'background:var(--bg-secondary,#17161A)', 'color:var(--text-primary,#F5F1E8)',
        'border:1px solid var(--border-color,rgba(245,241,232,0.18))', 'border-radius:7px',
        'padding:4px 8px', 'font-size:10.5px', 'font-weight:600', 'box-shadow:0 8px 22px rgba(0,0,0,0.45)',
      ].join(';');
      tipEl.textContent = ex.description || '';
      circle.appendChild(tipEl);

      circle.addEventListener('mouseenter', () => {
        circle.style.opacity = '1';
        tipEl.style.display = 'block';
        guide.style.left = bx + 'px';
        guide.style.height = plotHeight() + 'px';
        guide.style.display = 'block';
      });
      circle.addEventListener('mouseleave', () => {
        circle.style.opacity = '0.5';
        tipEl.style.display = 'none';
        guide.style.display = 'none';
      });
      layer.appendChild(circle);
    }
  };

  return {
    draw,
    destroy: () => {
      layer.parentNode?.removeChild(layer);
      guide.parentNode?.removeChild(guide);
    },
  };
}
