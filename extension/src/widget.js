/* Фрейм widget — плавающее окно с индикаторами поверх терминала Т-Инвестиций.
 *
 * Клон формы Trading Tools (перетаскиваемая панель + header со статус-точкой и
 * кластером контролов + навигация), но в editorial-дизайне Фрейма.
 *
 * Vanilla JS, без сборки (дружелюбно к load-unpacked). Полностью изолировано в
 * Shadow DOM — стили терминала не текут внутрь и наоборот. DOM строится через
 * createElement/textContent (без innerHTML).
 *
 * Содержимое каждой вкладки — iframe на таймфрейм.рф/embed/<indicator>.
 */
(function () {
  'use strict';
  if (window.__frameWidgetLoaded) return;
  window.__frameWidgetLoaded = true;

  // Прод по умолчанию (таймфрейм.рф в punycode). dev-preview переопределяет на localhost.
  var EMBED_BASE = window.FRAME_WIDGET_EMBED_BASE || 'https://xn--80aklbnczmv.xn--p1ai';

  // Две группы: «по инструменту» (следуют тикеру) и «по рынку».
  var TABS = [
    { id: 'oi',           label: 'ОИ',         group: 'instrument' },
    { id: 'seasonality',  label: 'Сезонность', group: 'instrument' },
    { id: 'buffett',      label: 'Баффетт',    group: 'market' },
    { id: 'strength',     label: 'Сила',       group: 'market' },
    { id: 'funds-money',  label: 'Фонды',      group: 'market' },
    { id: 'cbr-flows',    label: 'Потоки ЦБ',  group: 'market' },
    { id: 'fund-trades',  label: 'Сделки',     group: 'market' }
  ];

  var DEFAULTS = { x: null, y: null, w: 520, h: 430, tab: 'oi', theme: 'editorial-dark', collapsed: false, closed: true };
  var KEY = 'frameWidgetState';

  // Хранилище: chrome.storage.local (в расширении) или localStorage (в dev-preview).
  var store = {
    get: function () {
      return new Promise(function (res) {
        try {
          if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
            chrome.storage.local.get([KEY], function (o) { res((o && o[KEY]) || null); });
            return;
          }
        } catch (e) { /* нет chrome API */ }
        try { res(JSON.parse(localStorage.getItem('fw:' + KEY))); } catch (e) { res(null); }
      });
    },
    set: function (v) {
      try {
        if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
          var o = {}; o[KEY] = v; chrome.storage.local.set(o); return;
        }
      } catch (e) { /* нет chrome API */ }
      try { localStorage.setItem('fw:' + KEY, JSON.stringify(v)); } catch (e) { /* quota */ }
    }
  };

  // Глобальный ext-токен (его кладёт popup расширения в chrome.storage.local.frameToken).
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
    '.fw-root,.fw-root *,.fw-launcher{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,system-ui,sans-serif}',
    '.fw-root{position:fixed;z-index:2147483600;display:flex;flex-direction:column;background:var(--w-bg);color:var(--w-text);border:2px solid var(--w-border);box-shadow:6px 6px 0 var(--w-shadow);border-radius:3px;overflow:hidden}',
    '.fw-root[data-theme="editorial-dark"]{--w-bg:#0E0E10;--w-panel:#17161A;--w-text:#F5F1E8;--w-dim:#9A958C;--w-accent:#FF5C2B;--w-border:#F5F1E8;--w-soft:rgba(245,241,232,0.14);--w-shadow:rgba(0,0,0,0.55)}',
    '.fw-root[data-theme="editorial-light"]{--w-bg:#F4F1EA;--w-panel:#FFFFFF;--w-text:#0A0A0A;--w-dim:#6B6760;--w-accent:#FF5C2B;--w-border:#0A0A0A;--w-soft:rgba(10,10,10,0.14);--w-shadow:rgba(10,10,10,0.85)}',
    '.fw-head{display:flex;align-items:center;gap:8px;padding:7px 9px;background:var(--w-panel);border-bottom:1.5px solid var(--w-border);cursor:move;user-select:none;flex:0 0 auto}',
    '.fw-dot{width:9px;height:9px;border-radius:50%;background:var(--w-accent);flex:0 0 auto}',
    '.fw-title{font-weight:800;font-size:13px;letter-spacing:-0.01em;white-space:nowrap}',
    '.fw-sub{font-weight:500;color:var(--w-dim);margin-left:6px}',
    '.fw-ctrls{margin-left:auto;display:flex;gap:4px}',
    '.fw-btn{width:22px;height:22px;display:flex;align-items:center;justify-content:center;border:1px solid var(--w-soft);background:transparent;color:var(--w-text);border-radius:3px;cursor:pointer;font-size:14px;line-height:1;padding:0}',
    '.fw-btn:hover{border-color:var(--w-border)}',
    '.fw-tabs{display:flex;flex-wrap:wrap;align-items:center;gap:4px;padding:6px 9px;background:var(--w-panel);border-bottom:1.5px solid var(--w-border);flex:0 0 auto}',
    '.fw-grp{font-size:9px;text-transform:uppercase;letter-spacing:0.08em;color:var(--w-dim);margin:0 2px 0 4px}',
    '.fw-grp:first-child{margin-left:0}',
    '.fw-tab{font-size:11px;font-weight:600;padding:3px 8px;border:1px solid var(--w-soft);background:transparent;color:var(--w-dim);border-radius:3px;cursor:pointer;white-space:nowrap}',
    '.fw-tab:hover{border-color:var(--w-border);color:var(--w-text)}',
    '.fw-tab.active{background:var(--w-accent);color:#fff;border-color:var(--w-accent)}',
    '.fw-body{flex:1 1 auto;position:relative;background:var(--w-bg);min-height:0}',
    '.fw-iframe{position:absolute;inset:0;width:100%;height:100%;border:0;display:block}',
    '.fw-resize{position:absolute;right:0;bottom:0;width:16px;height:16px;cursor:nwse-resize;z-index:2}',
    '.fw-resize::after{content:"";position:absolute;right:3px;bottom:3px;width:7px;height:7px;border-right:2px solid var(--w-dim);border-bottom:2px solid var(--w-dim)}',
    '.fw-collapsed .fw-tabs,.fw-collapsed .fw-body,.fw-collapsed .fw-resize{display:none}',
    '.fw-collapsed{height:auto!important}',
    '.fw-launcher{position:fixed;z-index:2147483600;right:18px;bottom:18px;display:none;align-items:center;gap:7px;padding:9px 13px;background:var(--w-accent);color:#fff;border:2px solid var(--w-border);box-shadow:4px 4px 0 var(--w-shadow);border-radius:3px;cursor:pointer;font-weight:800;font-size:13px}',
    '.fw-launcher[data-theme="editorial-dark"]{--w-border:#F5F1E8;--w-shadow:rgba(0,0,0,0.55)}',
    '.fw-launcher[data-theme="editorial-light"]{--w-border:#0A0A0A;--w-shadow:rgba(10,10,10,0.85)}'
  ].join('');

  function groupLabel(g) { return g === 'instrument' ? 'По инструменту' : 'По рынку'; }

  // Маленький DOM-хелпер (вместо innerHTML).
  function h(tag, attrs, kids) {
    var e = document.createElement(tag);
    if (attrs) Object.keys(attrs).forEach(function (k) {
      if (k === 'class') e.className = attrs[k];
      else if (k === 'text') e.textContent = attrs[k];
      else e.setAttribute(k, attrs[k]);
    });
    if (kids) kids.forEach(function (c) {
      e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    });
    return e;
  }

  function mount(pageRoot) {
    pageRoot = pageRoot || document.body;
    if (document.getElementById('frame-widget-host')) return;

    var host = document.createElement('div');
    host.id = 'frame-widget-host';
    var shadow = host.attachShadow({ mode: 'open' });
    var style = document.createElement('style');
    style.textContent = CSS;
    shadow.appendChild(style);

    // ── header ──
    var sub = h('span', { class: 'fw-sub' });
    var title = h('span', { class: 'fw-title' }, ['Фрейм', sub]);
    var dot = h('span', { class: 'fw-dot' });
    var btnPop = h('button', { class: 'fw-btn', 'data-act': 'popout', title: 'Открыть в новом окне', text: '⤢' });
    var btnTheme = h('button', { class: 'fw-btn', 'data-act': 'theme', title: 'Сменить тему', text: '◐' });
    var btnCollapse = h('button', { class: 'fw-btn', 'data-act': 'collapse', title: 'Свернуть', text: '–' });
    var btnClose = h('button', { class: 'fw-btn', 'data-act': 'close', title: 'Закрыть', text: '×' });
    var ctrls = h('span', { class: 'fw-ctrls' }, [btnPop, btnTheme, btnCollapse, btnClose]);
    var head = h('div', { class: 'fw-head' }, [dot, title, ctrls]);

    // ── tabs ──
    var tabsWrap = h('div', { class: 'fw-tabs' });
    var lastG = null;
    TABS.forEach(function (t) {
      if (t.group !== lastG) { tabsWrap.appendChild(h('span', { class: 'fw-grp', text: groupLabel(t.group) })); lastG = t.group; }
      tabsWrap.appendChild(h('button', { class: 'fw-tab', 'data-tab': t.id, text: t.label }));
    });

    // ── body / resize ──
    var iframe = h('iframe', { class: 'fw-iframe', title: 'Фрейм индикатор' });
    var body = h('div', { class: 'fw-body' }, [iframe]);
    var resize = h('div', { class: 'fw-resize' });

    var root = h('div', { class: 'fw-root', 'data-theme': 'editorial-dark' }, [head, tabsWrap, body, resize]);

    var lDot = h('span', { class: 'fw-dot' });
    lDot.style.background = '#fff';
    var launcher = h('div', { class: 'fw-launcher', 'data-theme': 'editorial-dark' }, [lDot, 'Фрейм']);

    shadow.appendChild(root);
    shadow.appendChild(launcher);
    pageRoot.appendChild(host);

    var state = Object.assign({}, DEFAULTS);
    var pillEnabled = true; // плавающая пилюля-лаунчер; гасим, если есть кнопка в тулбаре терминала
    var extToken = null; // ext-токен из popup расширения (прокидываем в iframe)

    function clamp() {
      var vw = window.innerWidth, vh = window.innerHeight;
      state.w = Math.max(320, Math.min(state.w, vw - 16));
      state.h = Math.max(220, Math.min(state.h, vh - 16));
      if (state.x == null) state.x = Math.max(10, vw - state.w - 24);
      if (state.y == null) state.y = 84;
      state.x = Math.max(6, Math.min(state.x, vw - state.w - 6));
      state.y = Math.max(6, Math.min(state.y, vh - 44));
    }
    function persist() { store.set(state); }
    // Прилипание к краям вьюпорта при отпускании drag/resize.
    function snapToEdges() {
      var T = 24, vw = window.innerWidth, vh = window.innerHeight;
      if (state.x <= T) state.x = 6;
      else if (state.x + state.w >= vw - T) state.x = Math.max(6, vw - state.w - 6);
      if (state.y <= T) state.y = 6;
      else if (state.y + state.h >= vh - T) state.y = Math.max(6, vh - state.h - 6);
      root.style.left = state.x + 'px';
      root.style.top = state.y + 'px';
    }
    function embedUrl(tab) {
      return EMBED_BASE + '/embed/' + tab + '?theme=' + state.theme +
        (extToken ? '&token=' + encodeURIComponent(extToken) : '');
    }
    function loadIframe() {
      iframe.src = embedUrl(state.tab);
      var t = TABS.find(function (x) { return x.id === state.tab; });
      sub.textContent = t ? ('· ' + t.label) : '';
    }
    function applyTheme() { root.setAttribute('data-theme', state.theme); launcher.setAttribute('data-theme', state.theme); }
    function applyTabs() {
      tabsWrap.querySelectorAll('.fw-tab').forEach(function (b) {
        b.classList.toggle('active', b.getAttribute('data-tab') === state.tab);
      });
    }
    function applyLayout() {
      root.style.left = state.x + 'px'; root.style.top = state.y + 'px';
      root.style.width = state.w + 'px'; root.style.height = state.h + 'px';
      root.classList.toggle('fw-collapsed', state.collapsed);
      btnCollapse.textContent = state.collapsed ? '▢' : '–';
      btnCollapse.title = state.collapsed ? 'Развернуть' : 'Свернуть';
    }
    function applyClosed() {
      root.style.display = state.closed ? 'none' : 'flex';
      launcher.style.display = (state.closed && pillEnabled) ? 'flex' : 'none';
    }
    function applyAll() { clamp(); applyTheme(); applyTabs(); applyLayout(); applyClosed(); loadIframe(); }

    tabsWrap.addEventListener('click', function (e) {
      var b = e.target.closest('.fw-tab'); if (!b) return;
      state.tab = b.getAttribute('data-tab'); applyTabs(); loadIframe(); persist();
    });
    ctrls.addEventListener('click', function (e) {
      var b = e.target.closest('.fw-btn'); if (!b) return;
      var act = b.getAttribute('data-act');
      if (act === 'popout') { window.open(embedUrl(state.tab), '_blank', 'width=560,height=460'); }
      else if (act === 'theme') { state.theme = state.theme === 'editorial-dark' ? 'editorial-light' : 'editorial-dark'; applyTheme(); loadIframe(); persist(); }
      else if (act === 'collapse') { state.collapsed = !state.collapsed; applyLayout(); persist(); }
      else if (act === 'close') { state.closed = true; applyClosed(); persist(); }
    });
    launcher.addEventListener('click', function () { state.closed = false; applyClosed(); persist(); });

    // Перетаскивание за шапку.
    head.addEventListener('pointerdown', function (e) {
      if (e.target.closest('.fw-btn')) return;
      var sx = e.clientX, sy = e.clientY, ox = state.x, oy = state.y;
      try { head.setPointerCapture(e.pointerId); } catch (er) {}
      function mv(ev) { state.x = ox + (ev.clientX - sx); state.y = oy + (ev.clientY - sy); clamp(); root.style.left = state.x + 'px'; root.style.top = state.y + 'px'; }
      function up() { head.removeEventListener('pointermove', mv); head.removeEventListener('pointerup', up); snapToEdges(); persist(); }
      head.addEventListener('pointermove', mv); head.addEventListener('pointerup', up);
    });

    // Ресайз за уголок.
    resize.addEventListener('pointerdown', function (e) {
      e.preventDefault();
      var sx = e.clientX, sy = e.clientY, ow = state.w, oh = state.h;
      try { resize.setPointerCapture(e.pointerId); } catch (er) {}
      function mv(ev) { state.w = ow + (ev.clientX - sx); state.h = oh + (ev.clientY - sy); clamp(); root.style.width = state.w + 'px'; root.style.height = state.h + 'px'; }
      function up() { resize.removeEventListener('pointermove', mv); resize.removeEventListener('pointerup', up); snapToEdges(); persist(); }
      resize.addEventListener('pointermove', mv); resize.addEventListener('pointerup', up);
    });

    window.addEventListener('resize', function () { clamp(); applyLayout(); });

    var api = {
      open: function () { state.closed = false; applyClosed(); persist(); },
      close: function () { state.closed = true; applyClosed(); persist(); },
      toggle: function () { state.closed = !state.closed; applyClosed(); persist(); },
      isOpen: function () { return !state.closed; },
      setPillEnabled: function (v) { pillEnabled = !!v; applyClosed(); }
    };
    Promise.all([store.get(), getExtToken()]).then(function (arr) {
      if (arr[0]) state = Object.assign(state, arr[0]);
      extToken = arr[1];
      applyAll();
    });
    // Реакция на смену токена в popup — на лету перезагрузить активный iframe.
    try {
      if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.onChanged) {
        chrome.storage.onChanged.addListener(function (changes, area) {
          if (area === 'local' && changes.frameToken) {
            extToken = changes.frameToken.newValue || null;
            loadIframe();
          }
        });
      }
    } catch (e) { /* нет chrome API */ }
    return api;
  }

  window.FrameWidget = { mount: mount, TABS: TABS };
})();
