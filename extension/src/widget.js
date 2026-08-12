/* Фрейм widget — выпадающее меню индикаторов + независимые плавающие панели.
 *
 * Модель (как Trading Tools): клик по кнопке «F» в тулбаре терминала → выпадающий
 * СПИСОК индикаторов (2 группы) → выбрал → открылась отдельная панель. Панелей
 * может быть несколько одновременно; их набор + позиции persist'ятся.
 *
 * Скин — editorial Фрейма. Содержимое панели — iframe на framedata.ru/embed/<id>
 * с ?token= (PRO-токен из popup; нет токена → внутри iframe замок). Vanilla JS,
 * Shadow DOM, без innerHTML.
 */
(function () {
  'use strict';
  if (window.__frameWidgetLoaded) return;
  window.__frameWidgetLoaded = true;

  var EMBED_BASE = window.FRAME_WIDGET_EMBED_BASE || 'https://framedata.ru';

  var INDICATORS = [
    { id: 'signals', label: 'Сигналы', group: 'signals' },
    { id: 'oi', label: 'Открытые позиции', group: 'instrument' },
    { id: 'seasonality', label: 'Сезонность', group: 'instrument' },
    { id: 'screener', label: 'Скринер сигналов', group: 'instrument' },
    { id: 'buffett', label: 'Индикатор Баффетта', group: 'market' },
    { id: 'strength', label: 'Сила рынка', group: 'market' },
    { id: 'funds-money', label: 'Деньги в фондах', group: 'market' },
    { id: 'fund-trades', label: 'Сделки фондов', group: 'market' },
    { id: 'cbr-flows', label: 'Поток капитала', group: 'market' },
    { id: 'heatmap', label: 'Карта рынка', group: 'market' }
  ];

  // deep_link.route (из AnomalyItem) → id embed-индикатора. Клик по сигналу в
  // ленте /embed/signals шлёт postMessage наверх → открываем нужную панель.
  var ROUTE_TO_ID = {
    '/oi': 'oi', '/funds-money': 'funds-money', '/seasonality': 'seasonality',
    '/strength': 'strength', '/buffett': 'buffett', '/cbr-flows': 'cbr-flows',
    '/fund-trades': 'fund-trades', '/fund-movers': 'fund-movers',
    '/heatmap': 'heatmap', '/screener': 'screener',
    // Дайджест «Рынок штормит» в тосте: колокола в панели нет → открываем ленту.
    '/signals': 'signals'
  };

  var DEFAULT_THEME = 'editorial-dark';
  var KEY_PANELS = 'framePanels'; // [{id,x,y,w,h,theme}]
  var KEY_PREFS = 'framePrefs';   // общие настройки графиков (попап расширения)

  // Оконный режим панели: embed рисует ЕДИНУЮ шапку (грип + контролы индикатора +
  // ⤢ + ×) прямо в тулбаре графика — ровно как панель нашего терминала /sandbox.
  // Своя шапка оболочки (.fw-head) при этом не нужна: она была вторым заголовком.
  var WIN_SHELL = true;

  // Дефолты графиков — те же, что в «Настройках песочницы».
  var DEF_PREFS = { lineW: 2, crosshair: true, grid: true, lastValue: true, theme: DEFAULT_THEME };

  // Стартовый размер панели по индикатору — чтобы график+оси влезали сразу,
  // без ручного ресайза (фидбек Вадима «нужно развернуть чтобы было видно всё»).
  var SIZES = {
    'signals':     { w: 560, h: 620 }, // лента сигналов — узкая и высокая
    'oi':          { w: 640, h: 580 },
    'seasonality': { w: 600, h: 520 },
    'screener':    { w: 560, h: 620 }, // таблица-лента — узкая и высокая
    'buffett':     { w: 660, h: 560 },
    'strength':    { w: 600, h: 620 }, // два графика (IMOEX + breadth) — выше
    'funds-money': { w: 660, h: 560 },
    'cbr-flows':   { w: 660, h: 580 },
    'fund-trades': { w: 560, h: 560 },
    'fund-movers': { w: 560, h: 560 },
    'heatmap':     { w: 680, h: 560 }  // плитки — шире, чем график
  };
  var DEFAULT_SIZE = { w: 620, h: 560 };

  // Пол ширины ПО ИНДИКАТОРУ — первая линия защиты от наложения кнопок тулбара:
  // слева контролы индикатора, справа хром окна (рисование/экспорт/⚙/тема/⤢/×), и
  // на узкой панели они наезжают друг на друга. Значения — те же, что у панелей
  // нашего терминала (SandboxPage.MINW_BY_TYPE), замерены по реальным тулбарам.
  // Добавили контрол — подними число здесь И там.
  var MINW_BY_ID = {
    'signals': 340, 'oi': 540, 'seasonality': 470, 'funds-money': 580,
    'strength': 440, 'screener': 460, 'heatmap': 320, 'buffett': 380,
    'fund-trades': 620, 'cbr-flows': 560
  };
  function minW(id) { return MINW_BY_ID[id] || 300; }

  function lsGet(key) {
    return new Promise(function (res) {
      try {
        if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
          chrome.storage.local.get([key], function (o) { res(o ? o[key] : undefined); });
          return;
        }
      } catch (e) { /* нет chrome API */ }
      try { res(JSON.parse(localStorage.getItem('fw:' + key))); } catch (e) { res(undefined); }
    });
  }
  function lsSet(key, v) {
    try {
      if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
        var o = {}; o[key] = v; chrome.storage.local.set(o); return;
      }
    } catch (e) { /* нет chrome API */ }
    try { localStorage.setItem('fw:' + key, JSON.stringify(v)); } catch (e) { /* quota */ }
  }
  function getExtToken() {
    return new Promise(function (res) {
      try {
        if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
          chrome.storage.local.get(['frameToken'], function (o) { res((o && o.frameToken) || null); });
          return;
        }
      } catch (e) { /* нет chrome API */ }
      try { res(localStorage.getItem('fw:frameToken')); } catch (e) { res(null); }
    });
  }

  var CSS = [
    ':host{all:initial}',
    '.fw *,.fw{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,system-ui,sans-serif}',
    // theme tokens
    '[data-theme="editorial-dark"]{--w-bg:#0E0E10;--w-panel:#17161A;--w-text:#F5F1E8;--w-dim:#9A958C;--w-accent:#FF5C2B;--w-border:#F5F1E8;--w-soft:rgba(245,241,232,0.14);--w-shadow:rgba(0,0,0,0.55)}',
    '[data-theme="editorial-light"]{--w-bg:#F4F1EA;--w-panel:#FFFFFF;--w-text:#0A0A0A;--w-dim:#6B6760;--w-accent:#FF5C2B;--w-border:#0A0A0A;--w-soft:rgba(10,10,10,0.14);--w-shadow:rgba(10,10,10,0.85)}',
    // dropdown menu
    '.fw-menu{position:fixed;z-index:2147483646;display:none;flex-direction:column;min-width:210px;background:var(--w-panel);color:var(--w-text);border:2px solid var(--w-border);box-shadow:5px 5px 0 var(--w-shadow);border-radius:4px;padding:6px;overflow:hidden}',
    '.fw-menu.open{display:flex}',
    '.fw-grp{font-size:9px;text-transform:uppercase;letter-spacing:0.08em;color:var(--w-dim);padding:6px 8px 3px}',
    '.fw-item{display:flex;align-items:center;gap:8px;padding:7px 9px;border:0;background:transparent;color:var(--w-text);font-size:13px;font-weight:600;text-align:left;border-radius:4px;cursor:pointer;white-space:nowrap}',
    '.fw-item:hover{background:var(--w-accent);color:#fff}',
    '.fw-item .fw-d{width:7px;height:7px;border-radius:50%;background:var(--w-accent);flex:0 0 auto}',
    '.fw-item:hover .fw-d{background:#fff}',
    // panel — ПЛОСКИЙ, без «браузерного окна»: тонкий край + мягкая тень (не жёсткая
    // офсетная карточка). Фон = фон графика, чтобы шапка+тулбар+график читались одной
    // поверхностью (фидбэк Вадима «убрать окно вокруг графика, кнопки — продолжение графика»).
    '.fw-panel{position:fixed;display:flex;flex-direction:column;background:var(--w-bg);color:var(--w-text);border:1px solid var(--w-soft);box-shadow:0 12px 34px rgba(0,0,0,0.5);border-radius:8px;overflow:hidden}',
    // Полоса-хват сливается с поверхностью графика (тот же фон, без разделителя) —
    // это НЕ «титлбар окна», а верхняя кромка того же контейнера. Тулбар (актив/
    // контролы/⚙) живёт ВНУТРИ iframe прямо под ней, на том же фоне.
    '.fw-head{display:flex;align-items:center;gap:6px;padding:4px 6px 4px 9px;background:var(--w-bg);cursor:move;user-select:none;flex:0 0 auto}',
    '.fw-dot{width:7px;height:7px;border-radius:50%;background:var(--w-accent);flex:0 0 auto}',
    '.fw-title{font-weight:700;font-size:11.5px;letter-spacing:-0.01em;color:var(--w-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}',
    '.fw-beta{font-size:8px;font-weight:800;letter-spacing:0.06em;line-height:1;color:var(--w-dim);border:1px solid var(--w-soft);border-radius:3px;padding:2px 3px;flex:0 0 auto}',
    '.fw-ctrls{margin-left:auto;display:flex;gap:4px;flex:0 0 auto}',
    '.fw-btn{width:22px;height:22px;display:flex;align-items:center;justify-content:center;border:1px solid var(--w-soft);background:transparent;color:var(--w-text);border-radius:3px;cursor:pointer;font-size:14px;line-height:1;padding:0}',
    '.fw-btn:hover{border-color:var(--w-border)}',
    '.fw-body{flex:1 1 auto;position:relative;background:var(--w-bg);min-height:0}',
    '.fw-iframe{position:absolute;inset:0;width:100%;height:100%;border:0;display:block}',
    // Ресайз со ВСЕХ сторон и углов (фидбек Вадима: тянулся только правый нижний
    // угол). Ручки — прозрачные полосы внутри панели (у неё overflow:hidden, поэтому
    // никаких отрицательных офсетов), углы шире и лежат поверх сторон.
    '.fw-rz{position:absolute;z-index:3;touch-action:none}',
    '.fw-rz-n{top:0;left:12px;right:12px;height:6px;cursor:ns-resize}',
    '.fw-rz-s{bottom:0;left:12px;right:12px;height:6px;cursor:ns-resize}',
    '.fw-rz-w{left:0;top:12px;bottom:12px;width:6px;cursor:ew-resize}',
    '.fw-rz-e{right:0;top:12px;bottom:12px;width:6px;cursor:ew-resize}',
    '.fw-rz-nw{left:0;top:0;width:14px;height:14px;cursor:nwse-resize}',
    '.fw-rz-ne{right:0;top:0;width:14px;height:14px;cursor:nesw-resize}',
    '.fw-rz-sw{left:0;bottom:0;width:14px;height:14px;cursor:nesw-resize}',
    '.fw-rz-se{right:0;bottom:0;width:16px;height:16px;cursor:nwse-resize}',
    // развёрнутая на всё поле терминала панель не тянется и не таскается
    '.fw-panel[data-max="1"] .fw-rz{display:none}',
    '.fw-panel[data-max="1"] .fw-head{cursor:default}'
  ].join('');

  function groupLabel(g) { return g === 'signals' ? 'Центр сигналов' : g === 'instrument' ? 'По инструменту' : 'По рынку'; }

  function h(tag, attrs, kids) {
    var e = document.createElement(tag);
    if (attrs) Object.keys(attrs).forEach(function (k) {
      if (k === 'class') e.className = attrs[k];
      else if (k === 'text') e.textContent = attrs[k];
      else e.setAttribute(k, attrs[k]);
    });
    if (kids) kids.forEach(function (c) { e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
    return e;
  }

  function mount(pageRoot) {
    pageRoot = pageRoot || document.body;
    if (window.__frameApi) return window.__frameApi;

    var host = document.createElement('div');
    host.id = 'frame-widget-host';
    var shadow = host.attachShadow({ mode: 'open' });
    var style = document.createElement('style');
    style.textContent = CSS;
    shadow.appendChild(style);

    // ── Dropdown меню ──
    var menu = h('div', { class: 'fw menu fw-menu', 'data-theme': DEFAULT_THEME });
    var lastG = null;
    INDICATORS.forEach(function (ind) {
      if (ind.group !== lastG) { menu.appendChild(h('div', { class: 'fw-grp', text: groupLabel(ind.group) })); lastG = ind.group; }
      var item = h('button', { class: 'fw-item', 'data-id': ind.id }, [h('span', { class: 'fw-d' }), ind.label]);
      menu.appendChild(item);
    });
    // Тема — НЕ в меню: у каждого окна своя кнопка ◐ в тулбаре (фидбек Вадима —
    // «это не наш терминал, тема нужна на шапке окна»).
    shadow.appendChild(menu);
    pageRoot.appendChild(host);

    var extToken = null;
    var prefs = { lineW: DEF_PREFS.lineW, crosshair: DEF_PREFS.crosshair, grid: DEF_PREFS.grid, lastValue: DEF_PREFS.lastValue };
    var panels = []; // {id, el, state, reload}
    var zTop = 2147483600;

    function persist() { lsSet(KEY_PANELS, panels.map(function (p) { return p.state; })); }
    function embedUrl(id, theme, pid, extra) {
      // Токен — во fragment (#token=), НЕ в query: fragment не уходит на сервер
      // (нет в access-логах framedata.ru) и не попадает в Referer. embed читает
      // его из location.hash (EmbedPage.tsx).
      // pid — стабильный id панели: embed неймспейсит по нему настройки, чтобы
      // каждое окно (в т.ч. два одного индикатора) держало свою конфигурацию.
      // extra — доп. query-параметры (напр. instrument из сигнала); передаём ТОЛЬКО
      // на первый рендер панели, дальше embed сам персистит выбор в pid-LS (см.
      // reload() — на смену темы/токена extra не шлём, чтобы не перебивать выбор юзера).
      var q = '?theme=' + theme + (pid ? '&pid=' + encodeURIComponent(pid) : '');
      // Оконный режим + общие настройки графиков (попап). Только скаляры в query —
      // embed читает их как строки, JSON тут когда-то уже стрелял в ногу (кавычки
      // уехали в API-параметр → 422).
      if (WIN_SHELL) {
        q += '&shell=win';
        q += '&lw=' + (prefs.lineW || DEF_PREFS.lineW);
        q += '&ch=' + (prefs.crosshair ? 1 : 0);
        q += '&gr=' + (prefs.grid ? 1 : 0);
        q += '&lv=' + (prefs.lastValue ? 1 : 0);
      }
      if (extra) Object.keys(extra).forEach(function (k) {
        var v = extra[k];
        if (v != null && v !== '') q += '&' + encodeURIComponent(k) + '=' + encodeURIComponent(v);
      });
      return EMBED_BASE + '/embed/' + id + q +
        (extToken ? '#token=' + encodeURIComponent(extToken) : '');
    }
    function clampPanel(st) {
      var vw = window.innerWidth, vh = window.innerHeight;
      st.w = Math.max(minW(st.id), Math.min(st.w, vw - 16));
      st.h = Math.max(200, Math.min(st.h, vh - 16));
      if (st.x == null) st.x = Math.max(8, vw - st.w - 28 - (panels.length * 26) % 180);
      if (st.y == null) st.y = 80 + (panels.length * 26) % 180;
      st.x = Math.max(6, Math.min(st.x, vw - st.w - 6));
      st.y = Math.max(6, Math.min(st.y, vh - 44));
    }

    var SNAP = 18, MARGIN = 6;

    // Снап-окружение терминала: «внутреннее поле» (контейнер виджетов, БЕЗ верхних
    // тулбаров и нижнего статус-бара) + прямоугольники чужих виджетов терминала.
    // Наши панели в shadow DOM → .react-draggable = только виджеты терминала.
    // Классы хешированы (`...widgetsWrap-1ff99`) → матчим по подстроке.
    function getSnapEnv() {
      var field = null, widgets = [];
      try {
        var fEl = document.querySelector('[class*="widgetsWrap"]') ||
                  document.querySelector('[class*="Space-styles-cut"]');
        if (fEl) {
          var fr = fEl.getBoundingClientRect();
          field = {
            left: Math.max(0, fr.left), right: Math.min(window.innerWidth, fr.right),
            top: Math.max(0, fr.top), bottom: Math.min(window.innerHeight, fr.bottom)
          };
        }
        // Виджеты терминала. `.react-draggable` — исходный признак, но вёрстка
        // Т-Банка меняется (класс мог уехать на внутренний узел или исчезнуть),
        // а без целей ВЕРТИКАЛЬНЫЙ магнит просто переставал существовать —
        // горизонталь при этом жила за счёт краёв поля. Поэтому селекторов
        // несколько, и дубликаты (вложенные совпадения) схлопываем по rect'у.
        var nodes = document.querySelectorAll('.react-draggable, [data-widget-id], [class*="widgetWrap"]');
        var seen = {};
        for (var i = 0; i < nodes.length; i++) {
          var r = nodes[i].getBoundingClientRect();
          // Порог только отсекает мусор (иконки/кнопки), а не «узкие» виджеты:
          // прежние 120×80 выбрасывали, например, низкие ленты.
          if (r.width < 80 || r.height < 40) continue;
          var key = Math.round(r.left) + ':' + Math.round(r.top) + ':' + Math.round(r.width) + ':' + Math.round(r.height);
          if (seen[key]) continue;
          seen[key] = 1;
          widgets.push({ left: r.left, right: r.right, top: r.top, bottom: r.bottom });
        }
      } catch (e) { /* терминал перерисовался — fallback на окно */ }
      if (!field) field = { left: MARGIN, right: window.innerWidth - MARGIN, top: MARGIN, bottom: window.innerHeight - MARGIN };
      return { field: field, widgets: widgets };
    }

    // Магнит при перетаскивании. Цели — края «поля» терминала (внутренняя рабочая
    // область БЕЗ тулбаров и статус-бара), края виджетов терминала и края соседних
    // наших панелей. Из всех целей в радиусе SNAP берём БЛИЖАЙШУЮ; env кешируется
    // на старте drag.
    //
    // Раньше `ys` намеренно начинался пустым (верх/низ поля исключались, чтобы
    // панель не липла выше шапки виджета). Побочный эффект: когда виджеты не
    // находились селектором, вертикального магнита не оставалось вообще — по бокам
    // липло, вверх/вниз нет (фидбек Вадима). Теперь верх/низ поля — такие же цели:
    // это именно рабочая область (widgetsWrap), а не тулбар.
    function snapMove(st, self, env) {
      env = env || getSnapEnv();
      var f = env.field;
      var xs = [f.left, f.right]; // цели для левого/правого края панели
      var ys = [f.top, f.bottom]; // цели для верхнего/нижнего края
      function add(L, R, T, B) { xs.push(L, R); ys.push(T, B); }
      env.widgets.forEach(function (w) { add(w.left, w.right, w.top, w.bottom); });
      panels.forEach(function (p) { if (p.state !== self) { var s = p.state; add(s.x, s.x + s.w, s.y, s.y + s.h); } });
      var bx = null, bdx = SNAP + 1;
      xs.forEach(function (v) {
        var dl = Math.abs(st.x - v);          if (dl <= SNAP && dl < bdx) { bx = v; bdx = dl; }
        var dr = Math.abs((st.x + st.w) - v); if (dr <= SNAP && dr < bdx) { bx = v - st.w; bdx = dr; }
      });
      if (bx !== null) st.x = bx;
      var by = null, bdy = SNAP + 1;
      ys.forEach(function (v) {
        var dt = Math.abs(st.y - v);          if (dt <= SNAP && dt < bdy) { by = v; bdy = dt; }
        var db = Math.abs((st.y + st.h) - v); if (db <= SNAP && db < bdy) { by = v - st.h; bdy = db; }
      });
      if (by !== null) st.y = by;
    }

    // Магнит при ресайзе — липнут ТОЛЬКО тянущиеся края (dir: n/s/e/w и их пары).
    // Набор целей тот же, что у snapMove (включая верх/низ поля — см. коммент там).
    function snapResize(st, env, dir) {
      env = env || getSnapEnv();
      var f = env.field;
      var xs = [f.left, f.right], ys = [f.top, f.bottom];
      env.widgets.forEach(function (w) { xs.push(w.left, w.right); ys.push(w.top, w.bottom); });
      panels.forEach(function (p) { if (p.state !== st) { var s = p.state; xs.push(s.x, s.x + s.w); ys.push(s.y, s.y + s.h); } });
      function nearest(list, val) {
        var best = null, bd = SNAP + 1;
        list.forEach(function (v) { var d = Math.abs(val - v); if (d <= SNAP && d < bd) { best = v; bd = d; } });
        return best;
      }
      var v;
      if (dir.indexOf('e') >= 0) { v = nearest(xs, st.x + st.w); if (v !== null) st.w = v - st.x; }
      if (dir.indexOf('w') >= 0) { v = nearest(xs, st.x); if (v !== null) { var right = st.x + st.w; st.x = v; st.w = right - v; } }
      if (dir.indexOf('s') >= 0) { v = nearest(ys, st.y + st.h); if (v !== null) st.h = v - st.y; }
      if (dir.indexOf('n') >= 0) { v = nearest(ys, st.y); if (v !== null) { var bottom = st.y + st.h; st.y = v; st.h = bottom - v; } }
    }

    // Границы при ресайзе: минимальный размер и вьюпорт, но с УДЕРЖАНИЕМ
    // противоположного (неподвижного) края — иначе тяга за левый/верхний край
    // «уползает» вместе с панелью.
    var MIN_W = 300, MIN_H = 200;
    function clampResize(st, dir, o) {
      var vw = window.innerWidth, vh = window.innerHeight;
      // Пол ширины — по индикатору (см. MINW_BY_ID): на общих 300px кнопки тулбара
      // наезжали друг на друга.
      var MIN_W = minW(st.id);
      if (dir.indexOf('w') >= 0) {
        var right = o.x + o.w;
        if (st.x < MARGIN) st.x = MARGIN;
        if (right - st.x < MIN_W) st.x = right - MIN_W;
        st.w = right - st.x;
      } else {
        if (st.w < MIN_W) st.w = MIN_W;
        if (st.x + st.w > vw - MARGIN) st.w = vw - MARGIN - st.x;
      }
      if (dir.indexOf('n') >= 0) {
        var bottom = o.y + o.h;
        if (st.y < MARGIN) st.y = MARGIN;
        if (bottom - st.y < MIN_H) st.y = bottom - MIN_H;
        st.h = bottom - st.y;
      } else {
        if (st.h < MIN_H) st.h = MIN_H;
        if (st.y + st.h > vh - MARGIN) st.h = vh - MARGIN - st.y;
      }
    }

    // Прямоугольник «поля» терминала (контейнер виджетов) — цель для разворота
    // панели на весь терминал.
    function fieldRect() {
      var f = getSnapEnv().field;
      return {
        x: Math.max(0, f.left), y: Math.max(0, f.top),
        w: Math.max(MIN_W, f.right - f.left), h: Math.max(MIN_H, f.bottom - f.top)
      };
    }

    function spawnPanel(id, saved, opts) {
      var ind = INDICATORS.find(function (x) { return x.id === id; });
      if (!ind) return;
      var sz = SIZES[id] || DEFAULT_SIZE;
      var st = saved || { id: id, x: null, y: null, w: sz.w, h: sz.h, theme: prefs.theme || DEFAULT_THEME };
      st.id = id;
      // Стабильный id панели: у новых — генерим, у восстановленных без pid (старые
      // сохранения) — тоже, тогда окно засидится глобальными настройками индикатора
      // (наследует «последнее использованное»). Часть st → persist'ится сам.
      if (!st.pid) st.pid = 'p' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
      clampPanel(st);

      var dot = h('span', { class: 'fw-dot' });
      var title = h('span', { class: 'fw-title', text: ind.label });
      var beta = h('span', { class: 'fw-beta', title: 'Расширение в бете', text: 'BETA' });
      // Настройки (⚙) переехали в тулбар ВНУТРИ iframe — в шапке только кнопки окна.
      var bMax = h('button', { class: 'fw-btn', 'data-a': 'max', title: 'Развернуть на весь терминал', text: '⤢' });
      var bTheme = h('button', { class: 'fw-btn', 'data-a': 'theme', title: 'Тема', text: '◐' });
      var bClose = h('button', { class: 'fw-btn', 'data-a': 'close', title: 'Закрыть', text: '×' });
      var ctrls = h('span', { class: 'fw-ctrls' }, [bMax, bTheme, bClose]);
      var head = h('div', { class: 'fw-head' }, [dot, title, beta, ctrls]);
      var iframe = h('iframe', { class: 'fw-iframe', title: 'FRAME · ' + ind.label });
      var body = h('div', { class: 'fw-body' }, [iframe]);
      // В оконном режиме шапки оболочки нет вовсе — грип/⤢/× рисует сам embed в
      // строке тулбара (та же единая шапка, что у панели /sandbox).
      var el = h('div', { class: 'fw panel fw-panel', 'data-theme': st.theme },
        WIN_SHELL ? [body] : [head, body]);
      var DIRS = ['n', 's', 'w', 'e', 'nw', 'ne', 'sw', 'se'];
      DIRS.forEach(function (d) { el.appendChild(h('div', { class: 'fw-rz fw-rz-' + d, 'data-dir': d })); });

      function applyLayout() { el.style.left = st.x + 'px'; el.style.top = st.y + 'px'; el.style.width = st.w + 'px'; el.style.height = st.h + 'px'; }
      // extra шлём только на ПЕРВЫЙ рендер (opts из сигнала); reload() на смене
      // темы/токена — без extra, чтобы не перебивать последующий выбор актива юзером.
      function reload(extra) { iframe.src = embedUrl(st.id, st.theme, st.pid, extra); }
      function toFront() { zTop += 1; el.style.zIndex = zTop; }

      // Разворот НА ВЕСЬ ТЕРМИНАЛ (не новая вкладка): панель растягивается на поле
      // виджетов терминала, повторный клик возвращает прежнюю геометрию.
      function applyMax() { var r = fieldRect(); st.x = r.x; st.y = r.y; st.w = r.w; st.h = r.h; applyLayout(); }
      // Иконка ⤢/⤡ живёт ВНУТРИ iframe (единая шапка) — состояние окна знает только
      // оболочка, поэтому сообщаем его вниз. Свой .fw-head в этом режиме не рисуется,
      // но кнопки на нём держим синхронными: старые сборки/фолбэк.
      function notifyWinState() {
        try {
          if (iframe.contentWindow) {
            iframe.contentWindow.postMessage(
              { source: 'frame-ext', type: 'win-state', maximized: !!st.max }, EMBED_BASE);
          }
        } catch (e) { /* iframe ещё не загружен */ }
      }
      iframe.addEventListener('load', notifyWinState);
      function syncMaxBtn() {
        el.setAttribute('data-max', st.max ? '1' : '0');
        bMax.textContent = st.max ? '⤡' : '⤢';
        bMax.title = st.max ? 'Свернуть к прежнему размеру' : 'Развернуть на весь терминал';
        notifyWinState();
      }
      // Тема окна — кнопка ◐ в тулбаре embed'а (в оконном режиме своей шапки нет).
      function toggleTheme() {
        st.theme = st.theme === 'editorial-dark' ? 'editorial-light' : 'editorial-dark';
        el.setAttribute('data-theme', st.theme);
        reload();
        persist();
      }
      function toggleMax() {
        if (st.max) {
          st.max = false;
          var p = st.prev;
          if (p) { st.x = p.x; st.y = p.y; st.w = p.w; st.h = p.h; }
          st.prev = null;
          clampPanel(st); applyLayout();
        } else {
          st.prev = { x: st.x, y: st.y, w: st.w, h: st.h };
          st.max = true;
          applyMax(); toFront();
        }
        syncMaxBtn(); persist();
      }

      applyLayout(); el.style.zIndex = ++zTop; reload(opts);
      if (st.max) applyMax();
      syncMaxBtn();
      el.addEventListener('pointerdown', toFront, true);

      ctrls.addEventListener('click', function (e) {
        var b = e.target.closest('.fw-btn'); if (!b) return;
        var a = b.getAttribute('data-a');
        if (a === 'close') { removePanel(panel); }
        else if (a === 'theme') { st.theme = st.theme === 'editorial-dark' ? 'editorial-light' : 'editorial-dark'; el.setAttribute('data-theme', st.theme); reload(); persist(); }
        else if (a === 'max') { toggleMax(); }
      });
      // двойной клик по шапке — тот же разворот/сворот, как у окон терминала
      head.addEventListener('dblclick', function (e) { if (e.target.closest('.fw-btn')) return; toggleMax(); });

      head.addEventListener('pointerdown', function (e) {
        if (e.target.closest('.fw-btn') || st.max) return;
        var sx = e.clientX, sy = e.clientY, ox = st.x, oy = st.y;
        var env = getSnapEnv(); // снимок виджетов/поля терминала на старте drag
        try { head.setPointerCapture(e.pointerId); } catch (er) {}
        function mv(ev) { st.x = ox + (ev.clientX - sx); st.y = oy + (ev.clientY - sy); clampPanel(st); snapMove(st, st, env); el.style.left = st.x + 'px'; el.style.top = st.y + 'px'; }
        function up() { head.removeEventListener('pointermove', mv); head.removeEventListener('pointerup', up); head.removeEventListener('pointercancel', up); persist(); }
        head.addEventListener('pointermove', mv); head.addEventListener('pointerup', up); head.addEventListener('pointercancel', up);
      });
      // Ресайз за любую сторону/угол. Тянущиеся края берутся из data-dir ручки;
      // противоположный край стоит на месте (см. clampResize).
      el.addEventListener('pointerdown', function (e) {
        var hEl = e.target.closest ? e.target.closest('.fw-rz') : null;
        if (!hEl || st.max) return;
        e.preventDefault(); e.stopPropagation();
        var dir = hEl.getAttribute('data-dir');
        var sx = e.clientX, sy = e.clientY;
        var o = { x: st.x, y: st.y, w: st.w, h: st.h };
        var env = getSnapEnv(); // снимок виджетов/поля терминала на старте resize
        try { hEl.setPointerCapture(e.pointerId); } catch (er) {}
        function mv(ev) {
          var dx = ev.clientX - sx, dy = ev.clientY - sy;
          if (dir.indexOf('e') >= 0) st.w = o.w + dx;
          if (dir.indexOf('s') >= 0) st.h = o.h + dy;
          if (dir.indexOf('w') >= 0) { st.x = o.x + dx; st.w = o.w - dx; }
          if (dir.indexOf('n') >= 0) { st.y = o.y + dy; st.h = o.h - dy; }
          clampResize(st, dir, o);
          snapResize(st, env, dir);
          clampResize(st, dir, o); // магнит мог вылезти за минимум/вьюпорт
          applyLayout();
        }
        function up() { hEl.removeEventListener('pointermove', mv); hEl.removeEventListener('pointerup', up); hEl.removeEventListener('pointercancel', up); persist(); }
        hEl.addEventListener('pointermove', mv); hEl.addEventListener('pointerup', up); hEl.addEventListener('pointercancel', up);
      });

      // ── Перетаскивание за грип, который живёт ВНУТРИ iframe ──
      // Мышь после pointerdown обрабатывает вложенный документ, и родитель не видит
      // ни одного pointermove. Приём: на время drag'а гасим у iframe pointer-events —
      // хит-тест начинает проваливаться мимо него, и события снова приходят нам.
      // Координаты берём ЭКРАННЫЕ (screenX/Y): они одинаковы в обоих документах,
      // в отличие от clientX/Y, сдвинутых на положение iframe.
      function dragFromEmbed(sx, sy) {
        if (st.max) return;
        var ox = st.x, oy = st.y;
        var env = getSnapEnv();
        iframe.style.pointerEvents = 'none';
        function mv(ev) {
          st.x = ox + (ev.screenX - sx); st.y = oy + (ev.screenY - sy);
          clampPanel(st); snapMove(st, st, env);
          el.style.left = st.x + 'px'; el.style.top = st.y + 'px';
        }
        function up() {
          window.removeEventListener('pointermove', mv, true);
          window.removeEventListener('pointerup', up, true);
          window.removeEventListener('pointercancel', up, true);
          iframe.style.pointerEvents = '';
          persist();
        }
        window.addEventListener('pointermove', mv, true);
        window.addEventListener('pointerup', up, true);
        window.addEventListener('pointercancel', up, true);
      }

      shadow.appendChild(el);
      var panel = {
        id: id, el: el, state: st, reload: reload, applyLayout: applyLayout,
        applyMax: applyMax, iframe: iframe, toggleMax: toggleMax, toFront: toFront,
        dragFromEmbed: dragFromEmbed, notifyWinState: notifyWinState, toggleTheme: toggleTheme,
      };
      panels.push(panel);
      return panel;
    }

    function removePanel(panel) {
      var i = panels.indexOf(panel);
      if (i >= 0) panels.splice(i, 1);
      if (panel.el.parentNode) panel.el.parentNode.removeChild(panel.el);
      persist();
    }

    // ── Меню: открыть/закрыть, выбор индикатора ──
    function openMenuAt(rect) {
      if (menu.classList.contains('open')) { menu.classList.remove('open'); return; }
      var top = (rect ? rect.bottom : 70) + 6;
      var right = window.innerWidth - (rect ? rect.right : window.innerWidth - 20);
      menu.style.top = top + 'px';
      menu.style.right = Math.max(6, right) + 'px';
      menu.style.left = 'auto';
      menu.classList.add('open');
    }
    function closeMenu() { menu.classList.remove('open'); }
    menu.addEventListener('click', function (e) {
      var it = e.target.closest('.fw-item'); if (!it) return;
      spawnPanel(it.getAttribute('data-id'));
      persist();
      closeMenu();
    });
    // клик вне меню — закрыть
    document.addEventListener('pointerdown', function (e) {
      if (!menu.classList.contains('open')) return;
      var path = e.composedPath ? e.composedPath() : [];
      if (path.indexOf(menu) === -1 && !(e.target && e.target.id === 'frame-term-launch')) closeMenu();
    }, true);

    function reloadAll() { panels.forEach(function (p) { p.reload(); }); }

    // Resize окна (свернул терминал / поворот) → вернуть уехавшие за вьюпорт
    // панели обратно. Дебаунс, чтобы не дёргать на каждый промежуточный пиксель.
    var resizeTimer = null;
    window.addEventListener('resize', function () {
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(function () {
        resizeTimer = null;
        // развёрнутые панели пересчитываем по новому полю терминала, остальные
        // просто возвращаем во вьюпорт
        panels.forEach(function (p) {
          if (p.state.max) { p.applyMax(); return; }
          clampPanel(p.state); p.applyLayout();
        });
      }, 150);
    });

    // ── Приём сигнала из ленты /embed/signals → открыть панель индикатора ──
    // Клик по сигналу в панели «Сигналы» шлёт postMessage наверх. Источник сверяем
    // по origin И по contentWindow (identity — подделать нельзя), чтобы посторонняя
    // страница терминала не могла спавнить наши панели. deep_link.secid → первый
    // рендер панели на этом активе (дальше embed персистит сам).
    window.addEventListener('message', function (e) {
      var d = e.data;
      if (!d || d.source !== 'frame-embed') return;
      if (e.origin !== EMBED_BASE) return;
      var from = null;
      panels.forEach(function (p) { if (p.iframe && p.iframe.contentWindow === e.source) from = p; });
      if (!from) return;

      // Оконные команды единой шапки (кнопки нарисованы внутри iframe).
      if (d.type === 'win') {
        if (d.action === 'close') removePanel(from);
        else if (d.action === 'expand') from.toggleMax();
        else if (d.action === 'theme') from.toggleTheme();
        else if (d.action === 'drag-start') { from.toFront(); from.dragFromEmbed(d.x, d.y); }
        else if (d.action === 'resize' && !from.state.max) {
          if (typeof d.w === 'number') from.state.w = d.w;
          if (typeof d.h === 'number') from.state.h = d.h;
          clampPanel(from.state); from.applyLayout(); persist();
        }
        return;
      }
      if (d.type !== 'open-signal') return;
      var dl = d.deepLink || {};
      var id = ROUTE_TO_ID[dl.route];
      if (!id) return;
      var extra = {};
      if (dl.secid) extra.instrument = dl.secid;
      if (dl.category) extra.category = dl.category;
      if (dl.clgroup) extra.clgroup = dl.clgroup;
      if (dl.interval != null) extra.interval = dl.interval;
      if (dl.mode) extra.mode = dl.mode;
      if (dl.variant) extra.variant = dl.variant;
      if (dl.period) extra.period = dl.period;
      if (spawnPanel(id, null, extra)) persist();
    });

    // Загрузка токена + восстановление панелей.
    Promise.all([getExtToken(), lsGet(KEY_PANELS), lsGet(KEY_PREFS)]).then(function (arr) {
      extToken = arr[0];
      var saved = arr[2];
      if (saved) Object.keys(prefs).forEach(function (k) { if (saved[k] != null) prefs[k] = saved[k]; });
      if (saved && saved.theme) { prefs.theme = saved.theme; menu.setAttribute('data-theme', saved.theme); }
      (arr[1] || []).forEach(function (s) { spawnPanel(s.id, s); });
    });
    // Смена токена в popup → перезагрузить все панели.
    try {
      if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.onChanged) {
        chrome.storage.onChanged.addListener(function (ch, area) {
          if (area !== 'local') return;
          if (ch.frameToken) { extToken = ch.frameToken.newValue || null; reloadAll(); }
          // Настройки графиков меняются в попапе → доезжают во все открытые панели
          // (они попадают в URL embed'а, поэтому нужен reload iframe).
          if (ch.framePrefs) {
            var v = ch.framePrefs.newValue || {};
            Object.keys(prefs).forEach(function (k) { if (v[k] != null) prefs[k] = v[k]; });
            reloadAll();
          }
        });
      }
    } catch (e) { /* нет chrome API */ }

    var api = { openMenuAt: openMenuAt, closeMenu: closeMenu, open: function (id) { spawnPanel(id); persist(); } };
    window.__frameApi = api;
    return api;
  }

  window.FrameWidget = { mount: mount, INDICATORS: INDICATORS };
})();
