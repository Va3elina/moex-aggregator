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
    { id: 'oi', label: 'Открытый интерес', group: 'instrument' },
    { id: 'seasonality', label: 'Сезонность', group: 'instrument' },
    { id: 'buffett', label: 'Индикатор Баффетта', group: 'market' },
    { id: 'strength', label: 'Сила рынка', group: 'market' },
    { id: 'funds-money', label: 'Фонды', group: 'market' },
    { id: 'cbr-flows', label: 'Потоки ЦБ', group: 'market' },
    { id: 'fund-trades', label: 'Сделки фондов', group: 'market' }
  ];

  var DEFAULT_THEME = 'editorial-dark';
  var KEY_PANELS = 'framePanels'; // [{id,x,y,w,h,theme}]

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
    '.fw-head{display:flex;align-items:center;gap:8px;padding:7px 9px;background:var(--w-panel);border-bottom:1.5px solid var(--w-border);cursor:move;user-select:none;flex:0 0 auto}',
    '.fw-dot{width:9px;height:9px;border-radius:50%;background:var(--w-accent);flex:0 0 auto}',
    '.fw-title{font-weight:800;font-size:13px;letter-spacing:-0.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}',
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
    function embedUrl(id, theme) {
      return EMBED_BASE + '/embed/' + id + '?theme=' + theme + (extToken ? '&token=' + encodeURIComponent(extToken) : '');
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

    function spawnPanel(id, saved) {
      var ind = INDICATORS.find(function (x) { return x.id === id; });
      if (!ind) return;
      var st = saved || { id: id, x: null, y: null, w: 470, h: 400, theme: DEFAULT_THEME };
      st.id = id;
      clampPanel(st);

      var dot = h('span', { class: 'fw-dot' });
      var title = h('span', { class: 'fw-title', text: 'Фрейм · ' + ind.label });
      var bPop = h('button', { class: 'fw-btn', 'data-a': 'pop', title: 'Открыть в новом окне', text: '⤢' });
      var bTheme = h('button', { class: 'fw-btn', 'data-a': 'theme', title: 'Тема', text: '◐' });
      var bClose = h('button', { class: 'fw-btn', 'data-a': 'close', title: 'Закрыть', text: '×' });
      var ctrls = h('span', { class: 'fw-ctrls' }, [bPop, bTheme, bClose]);
      var head = h('div', { class: 'fw-head' }, [dot, title, ctrls]);
      var iframe = h('iframe', { class: 'fw-iframe', title: 'Фрейм · ' + ind.label });
      var body = h('div', { class: 'fw-body' }, [iframe]);
      var resize = h('div', { class: 'fw-resize' });
      var el = h('div', { class: 'fw panel fw-panel', 'data-theme': st.theme }, [head, body, resize]);

      function applyLayout() { el.style.left = st.x + 'px'; el.style.top = st.y + 'px'; el.style.width = st.w + 'px'; el.style.height = st.h + 'px'; }
      function reload() { iframe.src = embedUrl(st.id, st.theme); }
      function toFront() { zTop += 1; el.style.zIndex = zTop; }

      applyLayout(); el.style.zIndex = ++zTop; reload();
      el.addEventListener('pointerdown', toFront, true);

      ctrls.addEventListener('click', function (e) {
        var b = e.target.closest('.fw-btn'); if (!b) return;
        var a = b.getAttribute('data-a');
        if (a === 'close') { removePanel(panel); }
        else if (a === 'theme') { st.theme = st.theme === 'editorial-dark' ? 'editorial-light' : 'editorial-dark'; el.setAttribute('data-theme', st.theme); reload(); persist(); }
        else if (a === 'pop') { window.open(embedUrl(st.id, st.theme), '_blank', 'width=560,height=460'); }
      });

      head.addEventListener('pointerdown', function (e) {
        if (e.target.closest('.fw-btn')) return;
        var sx = e.clientX, sy = e.clientY, ox = st.x, oy = st.y;
        try { head.setPointerCapture(e.pointerId); } catch (er) {}
        function mv(ev) { st.x = ox + (ev.clientX - sx); st.y = oy + (ev.clientY - sy); clampPanel(st); el.style.left = st.x + 'px'; el.style.top = st.y + 'px'; }
        function up() { head.removeEventListener('pointermove', mv); head.removeEventListener('pointerup', up); persist(); }
        head.addEventListener('pointermove', mv); head.addEventListener('pointerup', up);
      });
      resize.addEventListener('pointerdown', function (e) {
        e.preventDefault();
        var sx = e.clientX, sy = e.clientY, ow = st.w, oh = st.h;
        try { resize.setPointerCapture(e.pointerId); } catch (er) {}
        function mv(ev) { st.w = ow + (ev.clientX - sx); st.h = oh + (ev.clientY - sy); clampPanel(st); el.style.width = st.w + 'px'; el.style.height = st.h + 'px'; }
        function up() { resize.removeEventListener('pointermove', mv); resize.removeEventListener('pointerup', up); persist(); }
        resize.addEventListener('pointermove', mv); resize.addEventListener('pointerup', up);
      });

      shadow.appendChild(el);
      var panel = { id: id, el: el, state: st, reload: reload };
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
