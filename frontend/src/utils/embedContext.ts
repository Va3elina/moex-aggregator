/**
 * Контекст embed'а — общий «мост» между индикатором внутри iframe и оболочкой,
 * которая этим iframe владеет (widget.js расширения для терминала Т-Инвестиций).
 *
 * Зачем: в расширении пользователь видит ТОЛЬКО наши плавающие окна, сайта там
 * нет. Значит любой переход «сигнал → график» обязан открывать нашу ПАНЕЛЬ, а не
 * навигировать iframe на страницу сайта (иначе внутри окошка терминала внезапно
 * разворачивается полный сайт с шапкой и футером — ровно этот баг и ловили).
 *
 * Контракт postMessage уже слушает widget.js (`message`-listener сверяет origin
 * И contentWindow, поэтому посторонняя страница терминала спавнить панели не
 * может): {source:'frame-embed', type:'open-signal', deepLink}.
 */
import type { AnomalyDeepLink } from '../services/api';

/** Мы внутри /embed/* — то есть индикатор рендерится как окно расширения либо
 *  как shareable-ссылка на «голый» график, но не как страница сайта. */
export function isEmbedRoute(): boolean {
  try {
    return window.location.pathname.startsWith('/embed');
  } catch {
    return false;
  }
}

/** Есть оболочка-владелец (мы в iframe): ей можно отдать открытие панели. */
export function hasEmbedShell(): boolean {
  try {
    return window.parent !== window;
  } catch {
    return false;
  }
}

/**
 * Отдать диплинк оболочке. `true` — оболочка забрала, вызывающему делать больше
 * нечего; `false` — оболочки нет (standalone pop-out), решай сам (открыть сайт).
 */
export function emitEmbedSignal(deepLink: AnomalyDeepLink | null | undefined): boolean {
  if (!deepLink || !hasEmbedShell()) return false;
  try {
    window.parent.postMessage({ source: 'frame-embed', type: 'open-signal', deepLink }, '*');
    return true;
  } catch {
    return false;
  }
}
