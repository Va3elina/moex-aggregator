/**
 * AnomalyContext — единый источник ленты аномалий для ВСЕГО UI (тосты + колокол).
 *
 * Почему провайдер, а не хук в каждом компоненте: колокол и ToastHost должны
 * делить ОДИН поток (один fetch/poll/SSE), иначе двойные запросы и рассинхрон
 * счётчика непросмотренных. Монтируется один раз в App.tsx под
 * UpgradePromptProvider (тосты зовут апселл) и BrowserRouter (open-handler
 * навигирует).
 *
 * Состояние «просмотрено» (lastSeenId, монотонный): сервер для залогиненных
 * (feed.last_seen_id + POST /seen), localStorage для гостя. Колокол при открытии
 * двигает lastSeenId до максимума → бейдж гаснет. Дедуп самих ТОСТОВ (что уже
 * выстрелило) живёт в ToastHost — это презентационная забота.
 *
 * Real-time: SSE-нудж (source:'anomaly') и возврат фокуса вкладки → refetch;
 * фолбэк-поллинг раз в 90с.
 */
import {
  createContext, useContext, useState, useEffect, useRef, useCallback, useMemo,
  type ReactNode,
} from 'react';
import { useAuth } from './AuthContext';
import { useSSE } from '../hooks/useSSE';
import {
  getAnomalyFeed, markAnomaliesSeen, setAnomalyToasts, type AnomalyItem,
} from '../services/api';

const LS_SEEN = 'anomaly_last_seen_id';
const LS_TOASTS = 'anomaly_toasts_enabled';
const POLL_MS = 90_000;
const FEED_WINDOW_HOURS = 168;   // колокол показывает до 7д; тост-свежесть режет ToastHost

interface AnomalyCtx {
  items: AnomalyItem[];          // лента (новые сверху) — для колокола
  lastSeenId: number;            // монотонный маркер «всё ≤ него просмотрено»
  unseenCount: number;
  toastsEnabled: boolean;
  markAllSeen: () => void;       // открытие колокола — гасит бейдж
  setToastsEnabled: (v: boolean) => void;
  refetch: () => void;
  bellOpen: boolean;             // общий стейт: дайджест-тост открывает колокол
  setBellOpen: (v: boolean) => void;
}

const Ctx = createContext<AnomalyCtx | null>(null);

function lsNum(key: string, dflt = 0): number {
  const v = Number(localStorage.getItem(key));
  return Number.isFinite(v) ? v : dflt;
}

export function AnomalyProvider({ children }: { children: ReactNode }) {
  const { isAuthenticated } = useAuth();
  const { lastEvent } = useSSE();

  const [items, setItems] = useState<AnomalyItem[]>([]);
  const [bellOpen, setBellOpen] = useState(false);
  const [lastSeenId, setLastSeenId] = useState<number>(() => lsNum(LS_SEEN));
  const [toastsEnabled, setEnabledState] = useState<boolean>(
    () => localStorage.getItem(LS_TOASTS) !== '0',   // дефолт — включено
  );

  // refetch без устаревших замыканий (poll/SSE дёргают одну ссылку).
  const fetchRef = useRef<() => void>(() => {});
  const refetch = useCallback(async () => {
    try {
      const feed = await getAnomalyFeed({ maxAgeHours: FEED_WINDOW_HOURS, limit: 50 });
      setItems(feed.items);
      // Источник истины «просмотрено»: сервер для залогиненных, localStorage для гостя.
      if (isAuthenticated && feed.last_seen_id != null) {
        setLastSeenId((cur) => Math.max(cur, feed.last_seen_id as number));
      }
      if (isAuthenticated) setEnabledState(feed.toasts_enabled);
    } catch {
      /* лента не критична — тихо игнорируем сетевой сбой */
    }
  }, [isAuthenticated]);
  fetchRef.current = refetch;

  // Первичная загрузка + поллинг.
  useEffect(() => {
    fetchRef.current();
    const t = setInterval(() => fetchRef.current(), POLL_MS);
    return () => clearInterval(t);
  }, [isAuthenticated]);

  // SSE-нудж: новая аномалия → подтянуть ленту (сам объект REST'ом, как везде).
  useEffect(() => {
    if (lastEvent && (lastEvent as { source?: string }).source === 'anomaly') {
      fetchRef.current();
    }
  }, [lastEvent]);

  // Возврат фокуса вкладки → освежить (могли накопиться, пока был в фоне).
  useEffect(() => {
    const onVis = () => { if (document.visibilityState === 'visible') fetchRef.current(); };
    document.addEventListener('visibilitychange', onVis);
    return () => document.removeEventListener('visibilitychange', onVis);
  }, []);

  const markAllSeen = useCallback(() => {
    const maxId = items.reduce((m, a) => Math.max(m, a.id), lastSeenId);
    if (maxId <= lastSeenId) return;
    setLastSeenId(maxId);
    if (isAuthenticated) {
      markAnomaliesSeen(maxId).catch(() => {});
    } else {
      localStorage.setItem(LS_SEEN, String(maxId));
    }
  }, [items, lastSeenId, isAuthenticated]);

  const setToastsEnabled = useCallback((v: boolean) => {
    setEnabledState(v);
    localStorage.setItem(LS_TOASTS, v ? '1' : '0');   // зеркалим всегда (гость + быстрый старт)
    if (isAuthenticated) setAnomalyToasts(v).catch(() => {});
  }, [isAuthenticated]);

  const unseenCount = useMemo(
    () => items.reduce((n, a) => (a.id > lastSeenId ? n + 1 : n), 0),
    [items, lastSeenId],
  );

  const value = useMemo<AnomalyCtx>(() => ({
    items, lastSeenId, unseenCount, toastsEnabled, markAllSeen, setToastsEnabled, refetch,
    bellOpen, setBellOpen,
  }), [items, lastSeenId, unseenCount, toastsEnabled, markAllSeen, setToastsEnabled, refetch, bellOpen]);

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAnomalies(): AnomalyCtx {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error('useAnomalies must be used within AnomalyProvider');
  return ctx;
}
