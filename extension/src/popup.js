/* Фрейм popup — ввод PRO-токена. Кладём в chrome.storage.local.frameToken;
 * плавающее окно слушает onChanged и разблокирует индикаторы на лету. */
(function () {
  'use strict';

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
    input.value = '';
    try {
      chrome.storage.local.set({ frameToken: '' }, function () { setStatus('Токен очищен', false); });
    } catch (e) { /* ignore */ }
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
