/**
 * embedPersist — per-panel-instance персист настроек embed-виджетов.
 *
 * widget.js даёт каждой панели стабильный pid и прокидывает его в iframe (?pid=).
 * Ключи неймспейсятся по pid, поэтому ДВА окна одного индикатора (напр. ОИ по
 * SBER и по GAZP) держат независимые настройки, а восстановленный воркспейс
 * точно повторяет расположение + настройки КАЖДОГО окна.
 *
 * - Fallback-чтение из глобального ключа `frame:embed:<ind>:<key>` = «шаблон»:
 *   НОВОЕ окно наследует последние настройки этого индикатора.
 * - Запись дублируется в глобальный ключ, чтобы «последнее использованное»
 *   оставалось свежим для следующего нового окна.
 * - Без pid (pop-out / прямая ссылка) — работают только глобальные ключи, как раньше.
 *
 * Только строки (никакого JSON) — иначе кавычки утекали в API-параметры
 * (урок [[terminal_extension]]: сырое чтение JSON-значения ломало clgroup → 422).
 */
function pid(): string {
  try {
    return new URLSearchParams(window.location.search).get('pid') || '';
  } catch {
    return '';
  }
}

// frame:embed:oi:instrument → frame:embed:oi:p:<pid>:instrument
function instKey(fullKey: string, p: string): string {
  return fullKey.replace(/^(frame:embed:[^:]+:)/, `$1p:${p}:`);
}

export function readLS(key: string, fallback: string): string {
  try {
    const p = pid();
    if (p) {
      const v = localStorage.getItem(instKey(key, p));
      if (v !== null) return v;
    }
    const g = localStorage.getItem(key);
    return g !== null ? g : fallback;
  } catch {
    return fallback;
  }
}

export function writeLS(key: string, value: string): void {
  try {
    const p = pid();
    if (p) localStorage.setItem(instKey(key, p), value);
    localStorage.setItem(key, value); // глобальный «последнее использованное» — свежий
  } catch {
    /* quota / partitioned storage */
  }
}

export function readBoolLS(key: string, fallback: boolean): boolean {
  return readLS(key, fallback ? 'true' : 'false') === 'true';
}
