/* Фрейм content script — монтирует окно (свёрнутым) и вживляет кнопку-вызов
 * в тулбар терминала рядом с «Виджеты» (как «T+» у Trading Tools).
 *
 * Терминал — React-SPA: тулбар появляется асинхронно и перерисовывается, выбрасывая
 * чужеродные узлы. Поэтому: (1) ищем кнопку по ТЕКСТУ (классы минифицированы и
 * нестабильны), (2) повторяем попытки + наблюдаем MutationObserver'ом и ре-инжектим.
 * Если «Виджеты» не нашлась — окно всё равно доступно через плавающую пилюлю (fallback).
 */
(function () {
  'use strict';

  var BTN_ID = 'frame-term-launch';
  var api = null;

  function makeTermButton(onClick) {
    var btn = document.createElement('button');
    btn.id = BTN_ID;
    btn.type = 'button';
    btn.title = 'Фрейм — индикаторы';
    // Стиль под тёмный тулбар терминала (кнопка живёт в ИХ DOM, не в Shadow).
    btn.style.cssText = [
      'display:inline-flex', 'align-items:center', 'gap:6px',
      'height:28px', 'padding:0 10px', 'margin:0 4px',
      'background:transparent', 'color:#F5F1E8',
      'border:1px solid rgba(245,241,232,0.28)', 'border-radius:6px',
      'font:600 12px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,system-ui,sans-serif',
      'cursor:pointer', 'white-space:nowrap', 'box-sizing:border-box'
    ].join(';');
    var dot = document.createElement('span');
    dot.style.cssText = 'width:8px;height:8px;border-radius:50%;background:#FF5C2B;flex:0 0 auto';
    btn.appendChild(dot);
    btn.appendChild(document.createTextNode('Фрейм'));
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
    var btn = makeTermButton(function () { if (api) api.toggle(); });
    wbtn.parentElement.insertBefore(btn, wbtn); // слева от «Виджеты»
    if (api) api.setPillEnabled(false); // кнопка есть → плавающую пилюлю прячем
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
