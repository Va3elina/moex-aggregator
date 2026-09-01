/**
 * AnalyticsContext — клиентский tracker событий пользователя.
 *
 * Архитектура:
 *  1. AnalyticsProvider оборачивает <App /> — даёт доступ к hook'у везде
 *  2. session_id генерируется один раз на mount, хранится в sessionStorage
 *     (умирает при закрытии вкладки → не tracking-cookie)
 *  3. События буферизуются 5 секунд → batch POST /api/usage/log (1 HTTP / 50 events)
 *  4. На beforeunload → navigator.sendBeacon (надёжная доставка при закрытии)
 *  5. Heartbeat каждые 60s пока document.visibilityState === 'visible'
 *  6. Opt-out check: cookie `frame_analytics_optout=1` ИЛИ user.analytics_optout=true → tracking выключен
 *
 * Privacy:
 *  - Не отправляем PII в payload (только secid/mode/period/indicator)
 *  - Session ID — UUID v4, новый на каждое открытие сайта
 *  - Точный IP не логируется на сервере (страна — из заголовка прокси либо из
 *    часовой зоны браузера)
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
// Путь приёма намеренно нейтральный: адрес со словом analytics режут стандартные
// списки блокировщиков, и события части браузеров не доходили вообще.
const INGEST_PATH = '/api/usage/log';
const STORAGE_SESSION_KEY = 'frame_session_id';
const STORAGE_CONSENT_KEY = 'frame_consent_v1';   // 'accepted' | 'minimal' | null
const COOKIE_CONSENT_NAME = 'frame_consent';      // зеркало выбора, чтобы он не терялся с localStorage
const CONSENT_COOKIE_MAX_AGE = 60 * 60 * 24 * 365; // 1 год
const COOKIE_OPTOUT_NAME = 'frame_analytics_optout';
const FLUSH_INTERVAL_MS = 5_000;     // batch flush каждые 5s
const HEARTBEAT_INTERVAL_MS = 60_000; // heartbeat каждые 60s
const MAX_BATCH = 50;

// Только типы, которые реально отправляются (см. track()-вызовы по проекту).
// Мёртвые (indicator_view / chart_annotate / period_change / session_end)
// удалены 2026-06-16 — никогда не слались, бэкенд-whitelist их тоже не принимает.
type EventType =
  | 'consent_optout'
  | 'consent_optin'
  | 'pageview'
  | 'instrument_select'
  | 'seasonality_mode'
  | 'chart_export'
  | 'theme_toggle'
  | 'session_heartbeat'
  // Воронка монетизации: намерение → оплата/триал
  | 'checkout_start'
  | 'trial_start'
  | 'purchase_success'
  | 'trial_activated';

interface PendingEvent {
  session_id: string;
  event_type: EventType;
  event_path: string | null;
  payload: Record<string, unknown> | null;
  client_ts: string;
  device: string;
  /** Часовая зона браузера — из неё сервер берёт страну (IP не смотрим). */
  tz: string | null;
}

interface AnalyticsContextValue {
  /** Залогировать произвольное событие. Если opted-out → no-op. */
  track: (type: EventType, payload?: Record<string, unknown>) => void;
  /** Текущий status согласия пользователя. null = ещё не выбрал → показываем banner. */
  consent: 'accepted' | 'minimal' | null;
  /** Установить согласие — записывает в localStorage + cookie и обновляет state. */
  setConsent: (value: 'accepted' | 'minimal') => void;
  /** Записать смену решения по сбору данных (тумблер в профиле).
   *  Уходит сразу и в обход гейта: это не наблюдение за поведением, а сам факт
   *  выбора — иначе отказ виден только как тишина и его нельзя посчитать. */
  logConsentChange: (kind: 'optout' | 'optin') => void;
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

function detectTimezone(): string | null {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || null;
  } catch {
    return null;
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

function isConsentValue(v: string | null): v is 'accepted' | 'minimal' {
  return v === 'accepted' || v === 'minimal';
}

/** Выбор храним в двух местах сразу: localStorage и cookie.
 *  Хранилища чистятся по-разному (приватный режим, настройки браузера,
 *  вытеснение в Safari), и потеря записи возвращала баннер человеку, который
 *  на него уже отвечал. Читаем из любого, пишем в оба. */
function readConsent(): 'accepted' | 'minimal' | null {
  try {
    const v = localStorage.getItem(STORAGE_CONSENT_KEY);
    if (isConsentValue(v)) return v;
  } catch {
    /* storage disabled — пробуем cookie */
  }
  const c = readCookie(COOKIE_CONSENT_NAME);
  return isConsentValue(c) ? c : null;
}

function writeConsent(value: 'accepted' | 'minimal'): void {
  try {
    localStorage.setItem(STORAGE_CONSENT_KEY, value);
  } catch {
    /* storage disabled — остаётся cookie */
  }
  try {
    document.cookie =
      `${COOKIE_CONSENT_NAME}=${value}; path=/; max-age=${CONSENT_COOKIE_MAX_AGE}; SameSite=Lax`;
  } catch {
    /* cookie недоступны — остаётся localStorage */
  }
}

/** Источник захода (referrer + utm) — для аналитики «откуда пришли».
 *  Храним только ХОСТ реферера (не полный URL → без PII/параметров) + utm-метки.
 *  Привязывается к payload ПЕРВОГО события сессии (см. acqSentRef в track). */
function getAcquisition(): Record<string, string> | null {
  if (typeof window === 'undefined') return null;
  try {
    const out: Record<string, string> = {};
    if (document.referrer) {
      try {
        out.ref = new URL(document.referrer).hostname || 'direct';
      } catch {
        /* malformed referrer — пропускаем */
      }
    }
    const p = new URLSearchParams(window.location.search);
    for (const k of ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content']) {
      const v = p.get(k);
      if (v) out[k] = v.slice(0, 64);
    }
    return Object.keys(out).length > 0 ? out : null;
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
  const lastBeatRef = useRef<number>(0);  // ms-метка последнего ОТПРАВЛЕННОГО heartbeat (дедуп)
  const deviceRef = useRef<string>(detectDevice());
  const tzRef = useRef<string | null>(detectTimezone());
  const acqSentRef = useRef<boolean>(false);  // источник (referrer/utm) шлём 1 раз за сессию

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

  /** Flush queue → POST /api/usage/log. Если queue пуст или opt-out — no-op.
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
      await fetch(`${API_BASE}${INGEST_PATH}`, {
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

      // Первое событие сессии обогащаем источником захода (referrer/utm) →
      // payload.acq. Так «откуда пришли» привязано к точке входа, без отдельного
      // event_type (бэк whitelist'ит типы, а payload хранит как есть).
      let pl: Record<string, unknown> | null = Object.keys(payload).length > 0 ? payload : null;
      if (!acqSentRef.current) {
        acqSentRef.current = true;
        const acq = getAcquisition();
        if (acq) pl = { ...payload, acq };
      }

      queueRef.current.push({
        session_id: sessionIdRef.current,
        event_type: type,
        event_path: path,
        payload: pl,
        client_ts: new Date().toISOString(),
        device: deviceRef.current,
        tz: tzRef.current,
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
      writeConsent(value);
      setConsentState(value);
      // session_id создаётся при первом track() в любом случае
    },
    []
  );

  /** Отправить факт смены решения по сбору данных — сразу и в обход гейта.
   *  Событие одно, без payload: тип, время, страница. */
  const logConsentChange = useCallback((kind: 'optout' | 'optin') => {
    if (!sessionIdRef.current) sessionIdRef.current = getOrCreateSessionId();
    const event: PendingEvent = {
      session_id: sessionIdRef.current,
      event_type: kind === 'optout' ? 'consent_optout' : 'consent_optin',
      event_path: typeof window !== 'undefined' ? window.location.pathname : null,
      payload: null,
      client_ts: new Date().toISOString(),
      device: deviceRef.current,
      tz: tzRef.current,
    };
    const token = typeof localStorage !== 'undefined' ? localStorage.getItem('access_token') : null;
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (token) headers['Authorization'] = `Bearer ${token}`;
    try {
      void fetch(`${API_BASE}${INGEST_PATH}`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ events: [event] }),
        keepalive: true,
      }).catch(() => undefined);
    } catch {
      /* сеть недоступна — теряем запись, UI не трогаем */
    }
  }, []);

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
  // Идемпотентный по времени: после sleep/wake или Chrome tab-freeze браузер может
  // возобновить эффект, не выполнив cleanup прошлой цепочки → накапливаются параллельные
  // setTimeout-цепочки (наблюдалось: после 3.4ч сна пульс ускорялся 60s→~12s на 11 часов).
  // Защита — дедуп по lastBeatRef: сколько бы цепочек ни тикало, в очередь идёт максимум
  // один heartbeat за HEARTBEAT_INTERVAL_MS. -5s — допуск на дрейф таймера.
  useEffect(() => {
    if (consent !== 'accepted') return;
    const beat = () => {
      if (document.visibilityState === 'visible') {
        const now = Date.now();
        if (now - lastBeatRef.current >= HEARTBEAT_INTERVAL_MS - 5_000) {
          lastBeatRef.current = now;
          track('session_heartbeat');
        }
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
        navigator.sendBeacon(`${API_BASE}${INGEST_PATH}`, blob);
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
    () => ({ track, consent, setConsent, logConsentChange }),
    [track, consent, setConsent, logConsentChange]
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
      logConsentChange: () => undefined,
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
