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
import { createContext, useCallback, useContext } from 'react';

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

/* ────────────────────── per-panel неймспейс ПЕСОЧНИЦЫ ──────────────────────
 * В расширении pid приходит через ?pid= (каждая панель — свой iframe). Песочница
 * рендерит embed'ы НАПРЯМУЮ (без iframe, URL один) → pid() пуст и панели делили
 * глобальные ключи. EmbedPidCtx даёт pid вторым источником: SandboxPage оборачивает
 * панель провайдером с её id, useEmbedPersist читает/пишет инстансные ключи с
 * фолбэком на глобальный «шаблон» (та же семантика, что у ?pid=).
 * Вне песочницы контекст пуст → rd/wr эквивалентны readLS/writeLS. */

export const EmbedPidCtx = createContext('');

export function useEmbedPersist() {
  const ns = useContext(EmbedPidCtx);
  // useCallback — чтобы rd/wr были стабильными и безопасными в deps эффектов.
  const rd = useCallback((key: string, fallback: string): string => {
    try {
      if (ns) {
        const v = localStorage.getItem(instKey(key, ns));
        if (v !== null) return v;
      }
    } catch { /* partitioned storage */ }
    return readLS(key, fallback);
  }, [ns]);
  const wr = useCallback((key: string, value: string): void => {
    try { if (ns) localStorage.setItem(instKey(key, ns), value); } catch { /* quota */ }
    writeLS(key, value);
  }, [ns]);
  const rdBool = useCallback((key: string, fallback: boolean): boolean => rd(key, fallback ? 'true' : 'false') === 'true', [rd]);
  return { rd, wr, rdBool };
}
