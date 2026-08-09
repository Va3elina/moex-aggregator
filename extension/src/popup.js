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

  document.getElementById('save').addEventListener('click', save);
  document.getElementById('clear').addEventListener('click', clear);
  input.addEventListener('keydown', function (e) { if (e.key === 'Enter') save(); });

  // Версия — из manifest (единый источник правды, без рассинхрона в футере).
  try {
    var verEl = document.getElementById('ver');
    if (verEl) verEl.textContent = 'v' + chrome.runtime.getManifest().version + ' ·';
  } catch (e) { /* нет chrome API */ }

  load();
})();
