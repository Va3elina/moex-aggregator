/* Фрейм popup — ввод PRO-токена. Кладём в chrome.storage.local.frameToken;
 * плавающее окно слушает onChanged и разблокирует индикаторы на лету. */
(function () {
  'use strict';

  // Адрес API (host_permission в manifest разрешает cross-origin fetch отсюда).
  var API_BASE = 'https://framedata.ru';

  var input = document.getElementById('tok');
  var statusEl = document.getElementById('status');

  function setStatus(text, ok) {
    statusEl.textContent = text;
    statusEl.style.color = ok ? 'var(--ok)' : 'var(--dim)';
  }

  function load() {
    try {
      chrome.storage.local.get(['frameToken'], function (o) {
        var t = (o && o.frameToken) || '';
        input.value = t;
        setStatus(t ? '✓ Токен сохранён' : 'Токен не задан', !!t);
      });
    } catch (e) {
      setStatus('Хранилище недоступно', false);
    }
  }

  function save() {
    var t = (input.value || '').trim();
    try {
      chrome.storage.local.set({ frameToken: t }, function () {
        setStatus(t ? '✓ Сохранено — индикаторы разблокированы' : 'Токен очищен', !!t);
      });
    } catch (e) {
      setStatus('Не удалось сохранить', false);
    }
  }

  function clear() {
    var t = (input.value || '').trim();
    input.value = '';
    function removeLocal(msg) {
      try {
        chrome.storage.local.set({ frameToken: '' }, function () { setStatus(msg, false); });
      } catch (e) { setStatus(msg, false); }
    }
    if (!t) { removeLocal('Токен не задан'); return; }
    // Отзываем на СЕРВЕРЕ (не только локально) — иначе утёкший токен оставался бы
    // рабочим у того, у кого есть копия. Затем чистим из этого браузера.
    // best-effort: сеть недоступна → всё равно удаляем локально + предупреждаем.
    setStatus('Отзываю токен…', false);
    fetch(API_BASE + '/api/extension/revoke', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: t })
    })
      .then(function (r) { removeLocal(r && r.ok ? 'Токен отозван и удалён' : 'Удалён из браузера (отозвать не удалось — проверьте в кабинете)'); })
      .catch(function () { removeLocal('Удалён из браузера (отозвать не удалось — проверьте в кабинете)'); });
  }

  // ── Общие настройки графиков ──
  // Живут в chrome.storage.local.framePrefs; widget.js слушает onChanged и
  // перезагружает панели (значения уходят в URL embed'а). Дефолты совпадают с
  // «Настройками» терминала Фрейма, чтобы вид не разъезжался между продуктами.
  var PREF_DEF = { lineW: 2, crosshair: true, grid: true, lastValue: true };
  var prefEls = {
    lineW: document.getElementById('lineW'),
    crosshair: document.getElementById('crosshair'),
    grid: document.getElementById('grid'),
    lastValue: document.getElementById('lastValue')
  };

  function readPrefs() {
    return {
      lineW: Number(prefEls.lineW.value) || PREF_DEF.lineW,
      crosshair: prefEls.crosshair.checked,
      grid: prefEls.grid.checked,
      lastValue: prefEls.lastValue.checked
    };
  }

  function savePrefs() {
    try {
      // Тему панелей держит widget.js в том же объекте — не затираем её.
      chrome.storage.local.get(['framePrefs'], function (o) {
        var cur = (o && o.framePrefs) || {};
        var next = readPrefs();
        if (cur.theme) next.theme = cur.theme;
        chrome.storage.local.set({ framePrefs: next });
      });
    } catch (e) { /* хранилище недоступно — настройки просто не сохранятся */ }
  }

  function loadPrefs() {
    try {
      chrome.storage.local.get(['framePrefs'], function (o) {
        var p = (o && o.framePrefs) || {};
        prefEls.lineW.value = String(p.lineW != null ? p.lineW : PREF_DEF.lineW);
        prefEls.crosshair.checked = p.crosshair != null ? !!p.crosshair : PREF_DEF.crosshair;
        prefEls.grid.checked = p.grid != null ? !!p.grid : PREF_DEF.grid;
        prefEls.lastValue.checked = p.lastValue != null ? !!p.lastValue : PREF_DEF.lastValue;
      });
    } catch (e) { /* дефолты уже в разметке */ }
  }

  Object.keys(prefEls).forEach(function (k) {
    prefEls[k].addEventListener('change', savePrefs);
  });

  document.getElementById('save').addEventListener('click', save);
  document.getElementById('clear').addEventListener('click', clear);
  input.addEventListener('keydown', function (e) { if (e.key === 'Enter') save(); });

  // Версия — из manifest (единый источник правды, без рассинхрона в футере).
  try {
    var verEl = document.getElementById('ver');
    if (verEl) verEl.textContent = 'v' + chrome.runtime.getManifest().version + ' ·';
  } catch (e) { /* нет chrome API */ }

  load();
  loadPrefs();
})();
