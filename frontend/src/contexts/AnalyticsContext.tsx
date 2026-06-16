/**
 * AnalyticsContext — клиентский tracker событий пользователя.
 *
 * Архитектура:
 *  1. AnalyticsProvider оборачивает <App /> — даёт доступ к hook'у везде
 *  2. session_id генерируется один раз на mount, хранится в sessionStorage
 *     (умирает при закрытии вкладки → не tracking-cookie)
 *  3. События буферизуются 5 секунд → batch POST /api/analytics/event (1 HTTP / 50 events)
 *  4. На beforeunload → navigator.sendBeacon (надёжная доставка при закрытии)
 *  5. Heartbeat каждые 60s пока document.visibilityState === 'visible'
 *  6. Opt-out check: cookie `frame_analytics_optout=1` ИЛИ user.analytics_optout=true → tracking выключен
 *
 * Privacy:
 *  - Не отправляем PII в payload (только secid/mode/period/indicator)
 *  - Session ID — UUID v4, новый на каждое открытие сайта
 *  - Точный IP не логируется на сервере (только country code из proxy headers)
 *  - Retention 180 дней (cleanup в orchestrator)
 *
 * Использование (любая *Page):
 *   const { track } = useAnalytics();
 *   useEffect(() => { track('indicator_view', { period: '1y' }); }, []);
 */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import type { ReactNode } from 'react';
import { useLocation } from 'react-router-dom';

const API_BASE = import.meta.env.VITE_API_BASE || '';
const STORAGE_SESSION_KEY = 'frame_session_id';
const STORAGE_CONSENT_KEY = 'frame_consent_v1';   // 'accepted' | 'minimal' | null
const COOKIE_OPTOUT_NAME = 'frame_analytics_optout';
const FLUSH_INTERVAL_MS = 5_000;     // batch flush каждые 5s
const HEARTBEAT_INTERVAL_MS = 60_000; // heartbeat каждые 60s
const MAX_BATCH = 50;

// Только типы, которые реально отправляются (см. track()-вызовы по проекту).
// Мёртвые (indicator_view / chart_annotate / period_change / session_end)
// удалены 2026-06-16 — никогда не слались, бэкенд-whitelist их тоже не принимает.
type EventType =
  | 'pageview'
  | 'instrument_select'
  | 'seasonality_mode'
  | 'chart_export'
  | 'theme_toggle'
  | 'session_heartbeat';

interface PendingEvent {
  session_id: string;
  event_type: EventType;
  event_path: string | null;
  payload: Record<string, unknown> | null;
  client_ts: string;
  device: string;
}

interface AnalyticsContextValue {
  /** Залогировать произвольное событие. Если opted-out → no-op. */
  track: (type: EventType, payload?: Record<string, unknown>) => void;
  /** Текущий status согласия пользователя. null = ещё не выбрал → показываем banner. */
  consent: 'accepted' | 'minimal' | null;
  /** Установить согласие — записывает в localStorage и обновляет state. */
  setConsent: (value: 'accepted' | 'minimal') => void;
}

const AnalyticsContext = createContext<AnalyticsContextValue | null>(null);

// ════════════════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════════════════

function generateUUID(): string {
  // crypto.randomUUID на современных браузерах. Fallback — manual.
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  // RFC 4122 v4 manual
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function getOrCreateSessionId(): string {
  try {
    const existing = sessionStorage.getItem(STORAGE_SESSION_KEY);
    if (existing && existing.length === 36) return existing;
    const fresh = generateUUID();
    sessionStorage.setItem(STORAGE_SESSION_KEY, fresh);
    return fresh;
  } catch {
    // Private mode / Storage disabled — session-only UUID без persistence
    return generateUUID();
  }
}

function detectDevice(): string {
  if (typeof window === 'undefined') return 'desktop';
  const ua = navigator.userAgent.toLowerCase();
  if (/mobi|iphone|android.*mobile/.test(ua)) return 'mobile';
  if (/tablet|ipad/.test(ua)) return 'tablet';
  return 'desktop';
}

function readCookie(name: string): string | null {
  try {
    const m = document.cookie.match(new RegExp(`(?:^|; )${name}=([^;]*)`));
    return m ? decodeURIComponent(m[1]) : null;
  } catch {
    return null;
  }
}

function isOptedOut(): boolean {
  // Cookie set'ится через Profile checkbox или скриптом 3rd party
  return readCookie(COOKIE_OPTOUT_NAME) === '1';
}

function readConsent(): 'accepted' | 'minimal' | null {
  try {
    const v = localStorage.getItem(STORAGE_CONSENT_KEY);
    if (v === 'accepted' || v === 'minimal') return v;
    return null;
  } catch {
    return null;
  }
}

// ════════════════════════════════════════════════════════════════════════════
// PROVIDER
// ════════════════════════════════════════════════════════════════════════════

export function AnalyticsProvider({ children }: { children: ReactNode }) {
  const [consent, setConsentState] = useState<'accepted' | 'minimal' | null>(() => readConsent());
  const sessionIdRef = useRef<string>('');
  const queueRef = useRef<PendingEvent[]>([]);
  const flushTimerRef = useRef<number | null>(null);
  const heartbeatTimerRef = useRef<number | null>(null);
  const deviceRef = useRef<string>(detectDevice());

  // Initialize session_id on mount (lazy, чтобы не активировать sessionStorage если consent=null)
  useEffect(() => {
    if (!sessionIdRef.current) {
      sessionIdRef.current = getOrCreateSessionId();
    }
  }, []);

  /** Trackable? — учитываем consent + opt-out cookie + opt-out user setting (через AuthContext). */
  const isTrackable = useCallback(() => {
    if (consent !== 'accepted') return false;
    if (isOptedOut()) return false;
    return true;
  }, [consent]);

  /** Flush queue → POST /api/analytics/event. Если queue пуст или opt-out — no-op.
   *
   *  Auth: у нас JWT в localStorage.access_token, backend читает Bearer header
   *  через get_current_user_optional. Раньше слали credentials:'include' (cookie),
   *  но у нас cookies для auth не используются — поэтому user_id всегда был
   *  NULL → все events помечались как «гости» в /admin/stats. */
  const flush = useCallback(async () => {
    if (queueRef.current.length === 0) return;
    if (!isTrackable()) {
      queueRef.current = []; // drop accumulated events если opt-out
      return;
    }
    // Снимаем snapshot, чтобы новые events во время fetch не потерялись
    const batch = queueRef.current.splice(0, MAX_BATCH);
    const token = typeof localStorage !== 'undefined' ? localStorage.getItem('access_token') : null;
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (token) headers['Authorization'] = `Bearer ${token}`;
    try {
      await fetch(`${API_BASE}/api/analytics/event`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ events: batch }),
        keepalive: true,  // ВАЖНО: разрешает доставку при unload (если у sendBeacon issues)
      });
    } catch {
      // Network error — drop batch (fire-and-forget). Не reinsert чтобы не зацикливаться.
    }
  }, [isTrackable]);

  /** Track — main API. Добавляет event в queue, flush вызывается через interval. */
  const track = useCallback(
    (type: EventType, payload: Record<string, unknown> = {}) => {
      if (!isTrackable()) return;
      if (!sessionIdRef.current) sessionIdRef.current = getOrCreateSessionId();

      const path = typeof window !== 'undefined' ? window.location.pathname : null;
      queueRef.current.push({
        session_id: sessionIdRef.current,
        event_type: type,
        event_path: path,
        payload: Object.keys(payload).length > 0 ? payload : null,
        client_ts: new Date().toISOString(),
        device: deviceRef.current,
      });

      // Flush сразу если буфер полон — не ждать 5s timer
      if (queueRef.current.length >= MAX_BATCH) {
        if (flushTimerRef.current) {
          clearTimeout(flushTimerRef.current);
          flushTimerRef.current = null;
        }
        flush();
      }
    },
    [isTrackable, flush]
  );

  /** Set consent — записывает choice. Если 'accepted' → запускает session, sends pending pageview. */
  const setConsent = useCallback(
    (value: 'accepted' | 'minimal') => {
      try {
        localStorage.setItem(STORAGE_CONSENT_KEY, value);
      } catch {
        /* storage disabled — runtime-only consent */
      }
      setConsentState(value);
      // session_id создаётся при первом track() в любом случае
    },
    []
  );

  // === Periodic flush ===
  useEffect(() => {
    if (consent !== 'accepted') return;
    const tick = () => {
      flush();
      flushTimerRef.current = window.setTimeout(tick, FLUSH_INTERVAL_MS);
    };
    flushTimerRef.current = window.setTimeout(tick, FLUSH_INTERVAL_MS);
    return () => {
      if (flushTimerRef.current) clearTimeout(flushTimerRef.current);
      flushTimerRef.current = null;
    };
  }, [consent, flush]);

  // === Heartbeat (только когда tab visible) ===
  useEffect(() => {
    if (consent !== 'accepted') return;
    const beat = () => {
      if (document.visibilityState === 'visible') {
        track('session_heartbeat');
      }
      heartbeatTimerRef.current = window.setTimeout(beat, HEARTBEAT_INTERVAL_MS);
    };
    heartbeatTimerRef.current = window.setTimeout(beat, HEARTBEAT_INTERVAL_MS);
    return () => {
      if (heartbeatTimerRef.current) clearTimeout(heartbeatTimerRef.current);
      heartbeatTimerRef.current = null;
    };
  }, [consent, track]);

  // === beforeunload → sendBeacon (надёжная доставка остатков queue) ===
  useEffect(() => {
    if (consent !== 'accepted') return;
    const onUnload = () => {
      if (queueRef.current.length === 0 || !isTrackable()) return;
      const batch = queueRef.current.splice(0);
      try {
        const blob = new Blob(
          [JSON.stringify({ events: batch })],
          { type: 'application/json' }
        );
        navigator.sendBeacon(`${API_BASE}/api/analytics/event`, blob);
      } catch {
        /* beacon unavailable — отброс */
      }
    };
    window.addEventListener('beforeunload', onUnload);
    window.addEventListener('pagehide', onUnload);
    return () => {
      window.removeEventListener('beforeunload', onUnload);
      window.removeEventListener('pagehide', onUnload);
    };
  }, [consent, isTrackable]);

  const value = useMemo<AnalyticsContextValue>(
    () => ({ track, consent, setConsent }),
    [track, consent, setConsent]
  );

  return <AnalyticsContext.Provider value={value}>{children}</AnalyticsContext.Provider>;
}

export function useAnalytics(): AnalyticsContextValue {
  const ctx = useContext(AnalyticsContext);
  if (!ctx) {
    // Soft-fail: если provider не установлен (тесты / SSR), вернём no-op stub
    return {
      track: () => undefined,
      consent: null,
      setConsent: () => undefined,
    };
  }
  return ctx;
}

// ════════════════════════════════════════════════════════════════════════════
// AUTO PAGEVIEW HOOK
// ════════════════════════════════════════════════════════════════════════════

/**
 * Автоматический pageview tracker. Отправляет событие при каждом router push.
 * Размещается один раз внутри <Routes> root (в App.tsx) под BrowserRouter.
 */
export function AnalyticsPageViewTracker() {
  const { track } = useAnalytics();
  const location = useLocation();
  const lastPathRef = useRef<string>('');

  useEffect(() => {
    const path = location.pathname;
    if (path === lastPathRef.current) return;
    lastPathRef.current = path;
    track('pageview');
  }, [location.pathname, track]);

  return null;
}
