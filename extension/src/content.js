/* Фрейм content script — монтирует плавающее окно на странице терминала.
 * Тонкий: вся логика в widget.js (грузится первым в content_scripts.js).
 * v1 БЕЗ синка с терминалом — окно самодостаточно, инструмент выбирается внутри.
 */
(function () {
  'use strict';
  function go() {
    if (window.FrameWidget) window.FrameWidget.mount(document.body);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', go);
  } else {
    go();
  }
})();
