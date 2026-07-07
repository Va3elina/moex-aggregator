/* Фрейм widget — выпадающее меню индикаторов + независимые плавающие панели.
 *
 * Модель (как Trading Tools): клик по кнопке «F» в тулбаре терминала → выпадающий
 * СПИСОК индикаторов (2 группы) → выбрал → открылась отдельная панель. Панелей
 * может быть несколько одновременно; их набор + позиции persist'ятся.
 *
 * Скин — editorial Фрейма. Содержимое панели — iframe на таймфрейм.рф/embed/<id>
 * с ?token= (PRO-токен из popup; нет токена → внутри iframe замок). Vanilla JS,
 * Shadow DOM, без innerHTML.
 */
(function () {
  'use strict';
  if (window.__frameWidgetLoaded) return;
  window.__frameWidgetLoaded = true;

  var EMBED_BASE = window.FRAME_WIDGET_EMBED_BASE || 'https://xn--80aklbnczmv.xn--p1ai';

  var INDICATORS = [
    { id: 'oi', label: 'Открытые позиции', group: 'instrument' },
    { id: 'seasonality', label: 'Сезонность', group: 'instrument' },
    { id: 'screener', label: 'Скринер сигналов', group: 'instrument' },
    { id: 'buffett', label: 'Индикатор Баффетта', group: 'market' },
    { id: 'strength', label: 'Сила рынка', group: 'market' },
    { id: 'funds-money', label: 'Фонды', group: 'market' },
    { id: 'fund-trades', label: 'Сделки фондов', group: 'market' },
    { id: 'fund-movers', label: 'Покупки фондов', group: 'market' },
    { id: 'cbr-flows', label: 'Потоки ЦБ', group: 'market' }
  ];

  var DEFAULT_THEME = 'editorial-dark';
  var KEY_PANELS = 'framePanels'; // [{id,x,y,w,h,theme}]

  // Стартовый размер панели по индикатору — чтобы график+оси влезали сразу,
  // без ручного ресайза (фидбек Вадима «нужно развернуть чтобы было видно всё»).
  var SIZES = {
    'oi':          { w: 640, h: 580 },
    'seasonality': { w: 600, h: 520 },
    'screener':    { w: 560, h: 620 }, // таблица-лента — узкая и высокая
    'buffett':     { w: 660, h: 560 },
    'strength':    { w: 600, h: 620 }, // два графика (IMOEX + breadth) — выше
    'funds-money': { w: 660, h: 560 },
    'cbr-flows':   { w: 660, h: 580 },
    'fund-trades': { w: 560, h: 560 },
    'fund-movers': { w: 600, h: 560 }  // две колонки Покупают/Продают
  };
  var DEFAULT_SIZE = { w: 620, h: 560 };

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
    // panel
    '.fw-panel{position:fixed;display:flex;flex-direction:column;background:var(--w-bg);color:var(--w-text);border:2px solid var(--w-border);box-shadow:6px 6px 0 var(--w-shadow);border-radius:3px;overflow:hidden}',
    // Тонкая полоса-хват: только зона перетаскивания + кнопки окна. Функциональный
    // тулбар (актив/контролы/⚙) живёт ВНУТРИ iframe, прямо над графиком.
    '.fw-head{display:flex;align-items:center;gap:6px;padding:4px 6px 4px 9px;background:var(--w-panel);border-bottom:1px solid var(--w-soft);cursor:move;user-select:none;flex:0 0 auto}',
    '.fw-dot{width:7px;height:7px;border-radius:50%;background:var(--w-accent);flex:0 0 auto}',
    '.fw-title{font-weight:700;font-size:11.5px;letter-spacing:-0.01em;color:var(--w-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}',
    '.fw-beta{font-size:8px;font-weight:800;letter-spacing:0.06em;line-height:1;color:var(--w-dim);border:1px solid var(--w-soft);border-radius:3px;padding:2px 3px;flex:0 0 auto}',
    '.fw-ctrls{margin-left:auto;display:flex;gap:4px;flex:0 0 auto}',
    '.fw-btn{width:22px;height:22px;display:flex;align-items:center;justify-content:center;border:1px solid var(--w-soft);background:transparent;color:var(--w-text);border-radius:3px;cursor:pointer;font-size:14px;line-height:1;padding:0}',
    '.fw-btn:hover{border-color:var(--w-border)}',
    '.fw-body{flex:1 1 auto;position:relative;background:var(--w-bg);min-height:0}',
    '.fw-iframe{position:absolute;inset:0;width:100%;height:100%;border:0;display:block}',
    '.fw-resize{position:absolute;right:0;bottom:0;width:16px;height:16px;cursor:nwse-resize;z-index:2}',
    '.fw-resize::after{content:"";position:absolute;right:3px;bottom:3px;width:7px;height:7px;border-right:2px solid var(--w-dim);border-bottom:2px solid var(--w-dim)}'
  ].join('');

  function groupLabel(g) { return g === 'instrument' ? 'По инструменту' : 'По рынку'; }

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
    shadow.appendChild(menu);
    pageRoot.appendChild(host);

    var extToken = null;
    var panels = []; // {id, el, state, reload}
    var zTop = 2147483600;

    function persist() { lsSet(KEY_PANELS, panels.map(function (p) { return p.state; })); }
    var persistT = null;
    function persistDebounced() { if (persistT) clearTimeout(persistT); persistT = setTimeout(function () { persistT = null; persist(); }, 300); }
    function embedUrl(id, theme, pid) {
      // Токен — во fragment (#token=), НЕ в query: fragment не уходит на сервер
      // (нет в access-логах таймфрейм.рф) и не попадает в Referer. embed читает
      // его из location.hash (EmbedPage.tsx).
      // pid — стабильный id панели: embed неймспейсит по нему настройки, чтобы
      // каждое окно (в т.ч. два одного индикатора) держало свою конфигурацию.
      return EMBED_BASE + '/embed/' + id + '?theme=' + theme +
        (pid ? '&pid=' + encodeURIComponent(pid) : '') +
        (extToken ? '#token=' + encodeURIComponent(extToken) : '');
    }
    function clampPanel(st) {
      var vw = window.innerWidth, vh = window.innerHeight;
      st.w = Math.max(300, Math.min(st.w, vw - 16));
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
        var nodes = document.querySelectorAll('.react-draggable');
        for (var i = 0; i < nodes.length; i++) {
          var r = nodes[i].getBoundingClientRect();
          if (r.width > 120 && r.height > 80) widgets.push({ left: r.left, right: r.right, top: r.top, bottom: r.bottom });
        }
      } catch (e) { /* терминал перерисовался — fallback на окно */ }
      if (!field) field = { left: MARGIN, right: window.innerWidth - MARGIN, top: MARGIN, bottom: window.innerHeight - MARGIN };
      return { field: field, widgets: widgets };
    }

    // Магнит при перетаскивании. ВЕРТИКАЛЬ (верх/низ) — ТОЛЬКО к краям виджетов
    // терминала и соседних панелей. Верх/низ «поля» (widgetsWrap) — это тулбар-
    // вкладки сверху и статус-бар снизу; виджеты часто стоят НИЖЕ верха поля, и
    // тогда панель липла на ~20px выше шапки виджета (фидбек Вадима «не та шапка»).
    // ГОРИЗОНТАЛЬ — стороны поля (стенки экрана) + виджеты/панели. Из всех целей в
    // радиусе SNAP берём БЛИЖАЙШУЮ. env кешируется на старте drag.
    function snapMove(st, self, env) {
      env = env || getSnapEnv();
      var f = env.field;
      var xs = [f.left, f.right]; // цели для левого/правого края панели
      var ys = [];                // цели для верхнего/нижнего края (без краёв поля)
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

    // Магнит при ресайзе: правый край → стенка поля + края виджетов/панелей;
    // нижний край → только виджеты/панели (низ поля = статус-бар, к нему не липнем).
    function snapResize(st, env) {
      env = env || getSnapEnv();
      var f = env.field;
      var xs = [f.right], ys = [];
      env.widgets.forEach(function (w) { xs.push(w.left, w.right); ys.push(w.top, w.bottom); });
      panels.forEach(function (p) { if (p.state !== st) { var s = p.state; xs.push(s.x, s.x + s.w); ys.push(s.y, s.y + s.h); } });
      var br = null, bdr = SNAP + 1;
      xs.forEach(function (v) { var d = Math.abs((st.x + st.w) - v); if (d <= SNAP && d < bdr) { br = v; bdr = d; } });
      if (br !== null) st.w = br - st.x;
      var bb = null, bdb = SNAP + 1;
      ys.forEach(function (v) { var d = Math.abs((st.y + st.h) - v); if (d <= SNAP && d < bdb) { bb = v; bdb = d; } });
      if (bb !== null) st.h = bb - st.y;
    }

    function spawnPanel(id, saved) {
      var ind = INDICATORS.find(function (x) { return x.id === id; });
      if (!ind) return;
      var sz = SIZES[id] || DEFAULT_SIZE;
      var st = saved || { id: id, x: null, y: null, w: sz.w, h: sz.h, theme: DEFAULT_THEME };
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
      var bPop = h('button', { class: 'fw-btn', 'data-a': 'pop', title: 'Открыть в новом окне', text: '⤢' });
      var bTheme = h('button', { class: 'fw-btn', 'data-a': 'theme', title: 'Тема', text: '◐' });
      var bClose = h('button', { class: 'fw-btn', 'data-a': 'close', title: 'Закрыть', text: '×' });
      var ctrls = h('span', { class: 'fw-ctrls' }, [bPop, bTheme, bClose]);
      var head = h('div', { class: 'fw-head' }, [dot, title, beta, ctrls]);
      var iframe = h('iframe', { class: 'fw-iframe', title: 'Фрейм · ' + ind.label });
      var body = h('div', { class: 'fw-body' }, [iframe]);
      var resize = h('div', { class: 'fw-resize' });
      var el = h('div', { class: 'fw panel fw-panel', 'data-theme': st.theme }, [head, body, resize]);

      function applyLayout() { el.style.left = st.x + 'px'; el.style.top = st.y + 'px'; el.style.width = st.w + 'px'; el.style.height = st.h + 'px'; }
      function reload() { iframe.src = embedUrl(st.id, st.theme, st.pid); }
      function toFront() { zTop += 1; el.style.zIndex = zTop; }

      applyLayout(); el.style.zIndex = ++zTop; reload();
      el.addEventListener('pointerdown', toFront, true);

      ctrls.addEventListener('click', function (e) {
        var b = e.target.closest('.fw-btn'); if (!b) return;
        var a = b.getAttribute('data-a');
        if (a === 'close') { removePanel(panel); }
        else if (a === 'theme') { st.theme = st.theme === 'editorial-dark' ? 'editorial-light' : 'editorial-dark'; el.setAttribute('data-theme', st.theme); reload(); persist(); }
        else if (a === 'pop') { window.open(embedUrl(st.id, st.theme, st.pid), '_blank', 'width=560,height=460'); }
      });

      head.addEventListener('pointerdown', function (e) {
        if (e.target.closest('.fw-btn')) return;
        var sx = e.clientX, sy = e.clientY, ox = st.x, oy = st.y;
        var env = getSnapEnv(); // снимок виджетов/поля терминала на старте drag
        try { head.setPointerCapture(e.pointerId); } catch (er) {}
        function mv(ev) { st.x = ox + (ev.clientX - sx); st.y = oy + (ev.clientY - sy); clampPanel(st); snapMove(st, st, env); el.style.left = st.x + 'px'; el.style.top = st.y + 'px'; }
        function up() { head.removeEventListener('pointermove', mv); head.removeEventListener('pointerup', up); head.removeEventListener('pointercancel', up); persist(); }
        head.addEventListener('pointermove', mv); head.addEventListener('pointerup', up); head.addEventListener('pointercancel', up);
      });
      resize.addEventListener('pointerdown', function (e) {
        e.preventDefault();
        var sx = e.clientX, sy = e.clientY, ow = st.w, oh = st.h;
        var env = getSnapEnv(); // снимок виджетов/поля терминала на старте resize
        try { resize.setPointerCapture(e.pointerId); } catch (er) {}
        function mv(ev) { st.w = ow + (ev.clientX - sx); st.h = oh + (ev.clientY - sy); clampPanel(st); snapResize(st, env); el.style.width = st.w + 'px'; el.style.height = st.h + 'px'; }
        function up() { resize.removeEventListener('pointermove', mv); resize.removeEventListener('pointerup', up); resize.removeEventListener('pointercancel', up); persist(); }
        resize.addEventListener('pointermove', mv); resize.addEventListener('pointerup', up); resize.addEventListener('pointercancel', up);
      });

      shadow.appendChild(el);
      var panel = { id: id, el: el, state: st, reload: reload, applyLayout: applyLayout, iframe: iframe };
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

    // Вертикальный ресайз из iframe: обычное колесо над графиком → embed шлёт
    // {source:'frame-embed', type:'resize-v', dh}. Находим панель по contentWindow
    // (e.source подделать нельзя — защита от чужих постов) и растим ВВЕРХ: низ на
    // месте, верхний край едет (фидбэк Вадима «график удлинялся вверх»).
    window.addEventListener('message', function (e) {
      var d = e.data;
      if (!d || d.source !== 'frame-embed' || d.type !== 'resize-v') return;
      var p = null;
      for (var i = 0; i < panels.length; i++) {
        if (panels[i].iframe && panels[i].iframe.contentWindow === e.source) { p = panels[i]; break; }
      }
      if (!p) return;
      var dh = Number(d.dh) || 0;
      if (!dh) return;
      var st = p.state;
      var bottom = st.y + st.h;
      var maxH = Math.min(window.innerHeight - 12, bottom - 6);
      var newH = Math.max(200, Math.min(st.h + dh, maxH));
      st.h = newH;
      st.y = bottom - newH;
      if (st.y < 6) { st.y = 6; st.h = bottom - 6; } // упёрлись в верх — дальше не растём
      p.applyLayout();
      persistDebounced();
    });

    // Resize окна (свернул терминал / поворот) → вернуть уехавшие за вьюпорт
    // панели обратно. Дебаунс, чтобы не дёргать на каждый промежуточный пиксель.
    var resizeTimer = null;
    window.addEventListener('resize', function () {
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(function () {
        resizeTimer = null;
        panels.forEach(function (p) { clampPanel(p.state); p.applyLayout(); });
      }, 150);
    });

    // Загрузка токена + восстановление панелей.
    Promise.all([getExtToken(), lsGet(KEY_PANELS)]).then(function (arr) {
      extToken = arr[0];
      (arr[1] || []).forEach(function (s) { spawnPanel(s.id, s); });
    });
    // Смена токена в popup → перезагрузить все панели.
    try {
      if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.onChanged) {
        chrome.storage.onChanged.addListener(function (ch, area) {
          if (area === 'local' && ch.frameToken) { extToken = ch.frameToken.newValue || null; reloadAll(); }
        });
      }
    } catch (e) { /* нет chrome API */ }

    var api = { openMenuAt: openMenuAt, closeMenu: closeMenu, open: function (id) { spawnPanel(id); persist(); } };
    window.__frameApi = api;
    return api;
  }

  window.FrameWidget = { mount: mount, INDICATORS: INDICATORS };
})();
