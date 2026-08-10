/* Фрейм content script — вживляет кнопку «F» в тулбар терминала рядом с «Виджеты»
 * (как «T+» у Trading Tools). Клик по «F» → выпадающий список индикаторов (логика
 * меню/панелей — в widget.js).
 *
 * Терминал — React-SPA: тулбар появляется асинхронно и перерисовывается, выбрасывая
 * чужеродные узлы. Поэтому: (1) ищем кнопку по ТЕКСТУ (классы минифицированы и
 * нестабильны), (2) повторяем попытки + наблюдаем MutationObserver'ом и ре-инжектим.
 */
(function () {
  'use strict';

  var BTN_ID = 'frame-term-launch';
  var api = null;

  function makeTermButton(onClick) {
    var btn = document.createElement('button');
    btn.id = BTN_ID;
    btn.type = 'button';
    btn.title = 'FRAME — индикаторы';
    // Минимал: квадрат с буквой «F» под сетку тулбара терминала (как «T+»).
    // Живёт в ИХ DOM, не в Shadow — стилизуем инлайном.
    var BORDER = 'rgba(245,241,232,0.28)';
    btn.style.cssText = [
      'display:inline-flex', 'align-items:center', 'justify-content:center',
      'width:28px', 'height:28px', 'padding:0', 'margin:0 4px',
      'background:transparent', 'color:#FF5C2B',
      'border:1px solid ' + BORDER, 'border-radius:6px',
      'font:800 15px/1 Georgia,"Times New Roman",serif',
      'cursor:pointer', 'box-sizing:border-box', 'flex:0 0 auto'
    ].join(';');
    btn.textContent = 'F';
    btn.addEventListener('mouseenter', function () { btn.style.borderColor = 'rgba(245,241,232,0.6)'; });
    btn.addEventListener('mouseleave', function () { btn.style.borderColor = BORDER; });
    btn.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();
      onClick();
    });
    return btn;
  }

  function findWidgetsButton() {
    var btns = document.querySelectorAll('button');
    for (var i = 0; i < btns.length; i++) {
      if ((btns[i].textContent || '').trim() === 'Виджеты') return btns[i];
    }
    return null;
  }

  function injectTermButton() {
    if (document.getElementById(BTN_ID)) return true; // уже стоит
    var wbtn = findWidgetsButton();
    if (!wbtn || !wbtn.parentElement) return false;
    var btn = makeTermButton(function () {
      // клик по «F» → выпадающий список индикаторов, заякоренный под кнопкой
      if (api) api.openMenuAt(btn.getBoundingClientRect());
    });
    wbtn.parentElement.insertBefore(btn, wbtn); // слева от «Виджеты»
    return true;
  }

  function start() {
    if (!window.FrameWidget) return;
    api = window.FrameWidget.mount(document.body);

    injectTermButton();

    // Тулбар появляется/перерисовывается асинхронно — добиваем попытками ~20с.
    var tries = 0;
    var iv = setInterval(function () {
      tries++;
      if (injectTermButton() || tries > 40) clearInterval(iv);
    }, 500);

    // Ре-инжект при ре-рендере (дебаунс, чтобы не дёргаться на каждую мутацию).
    var pending = false;
    var mo = new MutationObserver(function () {
      if (pending || document.getElementById(BTN_ID)) return;
      pending = true;
      setTimeout(function () { pending = false; injectTermButton(); }, 400);
    });
    try { mo.observe(document.body, { childList: true, subtree: true }); } catch (e) { /* ignore */ }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
