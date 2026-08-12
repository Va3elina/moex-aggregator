/**
 * ToastHost — очередь/троттлинг/таймеры всплывающих аномалий. Рендерится один раз
 * в App.tsx. Берёт поток из AnomalyContext, решает ЧТО и КОГДА показать тостом:
 *
 *  • максимум 1–2 видимых (1 на мобиле), остальное копится → колокол;
 *  • потолок за сессию (SESSION_CAP) — дальше только бейдж, не мельтешим;
 *  • спайк (>DIGEST_THRESHOLD свежих разом) → ОДИН дайджест «N аномалий», не серия;
 *  • автоскрытие 9с с паузой на ховере + полоска прогресса;
 *  • фон-вкладка (Page Visibility) → не выстреливаем, копим;
 *  • дедуп между визитами через localStorage last_toasted_id (+ серверный lastSeenId).
 *
 * Клик «Открыть график»: если сервер пометил link_required_tier (цель закрыта
 * тарифом) → апселл + переход на ДЕФОЛТ страницы; иначе — полный диплинк.
 * «Получать»: гость → апселл; иначе создаём сигнал, квота-403 → handleTierError.
 *
 * Интервал читает рефы (не стейт) — чтобы не ловить устаревшие замыкания.
 */
import { useEffect, useRef, useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAnomalies } from '../../contexts/AnomalyContext';
import { useAuth } from '../../contexts/AuthContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import { type AnomalyItem } from '../../services/api';
import { AnomalyToast } from './AnomalyToast';
import { openAnomaly, subscribeAnomalyAction } from './anomalyActions';
import { emitEmbedSignal, isEmbedRoute } from '../../utils/embedContext';
import { Activity, X } from 'lucide-react';

const SESSION_CAP = 5;
const DIGEST_THRESHOLD = 4;       // >этого свежих разом → дайджест вместо серии
const AUTOHIDE_MS = 9_000;
const TOAST_MAX_AGE_MS = 48 * 3_600_000;   // тостом только свежее (старое — лишь в колокол)
const LS_LAST_TOASTED = 'anomaly_last_toasted_id';

interface TimerState { start: number; pausedTotal: number; pausedAt: number | null }

export function ToastHost() {
  const { items, lastSeenId, toastsEnabled, setBellOpen } = useAnomalies();
  const { isAuthenticated } = useAuth();
  const { showUpgrade } = useUpgradePrompt();
  const navigate = useNavigate();

  const [visible, setVisible] = useState<AnomalyItem[]>([]);
  const [progress, setProgress] = useState<Record<number, number>>({});
  const [digest, setDigest] = useState<{ count: number } | null>(null);
  const [subscribing, setSubscribing] = useState<number | null>(null);
  const [subscribed, setSubscribed] = useState<Set<number>>(new Set());
  const [mobile, setMobile] = useState(() => window.innerWidth < 768);
  // Колокола/навигации в embed'е нет — там всё уходит в оболочку расширения.
  const inEmbed = isEmbedRoute();

  // Рефы для интервала (свежие значения без переусанной подписки).
  const itemsRef = useRef(items); itemsRef.current = items;
  const visibleRef = useRef(visible); visibleRef.current = visible;
  const enabledRef = useRef(toastsEnabled); enabledRef.current = toastsEnabled;
  const seenRef = useRef(lastSeenId); seenRef.current = lastSeenId;
  const digestRef = useRef(digest); digestRef.current = digest;
  const mobileRef = useRef(mobile); mobileRef.current = mobile;
  const timers = useRef<Record<number, TimerState>>({});
  const toasted = useRef<Set<number>>(new Set());
  const sessionShown = useRef(0);

  useEffect(() => {
    const onResize = () => setMobile(window.innerWidth < 768);
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  // Главный тик: прогресс + истечение + долив очереди. Один интервал на рефах.
  useEffect(() => {
    const tick = () => {
      const now = Date.now();
      // 1. прогресс + истёкшие
      const prog: Record<number, number> = {};
      const expired: number[] = [];
      for (const it of visibleRef.current) {
        const t = timers.current[it.id];
        if (!t) continue;
        const pausedExtra = t.pausedAt ? now - t.pausedAt : 0;
        const elapsed = now - t.start - t.pausedTotal - pausedExtra;
        const p = Math.min(1, elapsed / AUTOHIDE_MS);
        prog[it.id] = p;
        if (p >= 1) expired.push(it.id);
      }
      if (expired.length) {
        setVisible((v) => v.filter((x) => !expired.includes(x.id)));
      }
      setProgress(prog);

      // 2. долив (pump) — только видимая вкладка, включено, нет активного дайджеста
      if (!enabledRef.current || document.hidden || digestRef.current) return;
      const maxVisible = mobileRef.current ? 1 : 2;
      const curVisible = visibleRef.current.filter((x) => !expired.includes(x.id));
      if (curVisible.length >= maxVisible || sessionShown.current >= SESSION_CAP) return;

      const lastToasted = Number(localStorage.getItem(LS_LAST_TOASTED)) || 0;
      const baseline = Math.max(seenRef.current, lastToasted);
      const fresh = (a: AnomalyItem) =>
        !a.created_at || now - new Date(a.created_at).getTime() < TOAST_MAX_AGE_MS;
      const candidates = itemsRef.current.filter(
        (a) => a.id > baseline && !toasted.current.has(a.id) && fresh(a),
      );
      if (candidates.length === 0) return;

      const advanceToasted = (id: number) => {
        toasted.current.add(id);
        const prev = Number(localStorage.getItem(LS_LAST_TOASTED)) || 0;
        if (id > prev) localStorage.setItem(LS_LAST_TOASTED, String(id));
      };

      // Спайк → дайджест (вместо серии). Все свежие помечаем показанными.
      if (candidates.length > DIGEST_THRESHOLD) {
        candidates.forEach((a) => advanceToasted(a.id));
        sessionShown.current += 1;
        setDigest({ count: candidates.length });
        return;
      }

      // Обычный долив до свободных слотов (новые сверху).
      const slots = maxVisible - curVisible.length;
      const toShow = candidates.slice(0, slots);
      toShow.forEach((a) => {
        timers.current[a.id] = { start: now, pausedTotal: 0, pausedAt: null };
        advanceToasted(a.id);
        sessionShown.current += 1;
      });
      if (toShow.length) setVisible((v) => [...toShow, ...v].slice(0, maxVisible));
    };
    const iv = setInterval(tick, 150);
    return () => clearInterval(iv);
  }, []);

  const pause = useCallback((id: number) => {
    const t = timers.current[id];
    if (t && t.pausedAt == null) t.pausedAt = Date.now();
  }, []);
  const resume = useCallback((id: number) => {
    const t = timers.current[id];
    if (t && t.pausedAt != null) { t.pausedTotal += Date.now() - t.pausedAt; t.pausedAt = null; }
  }, []);
  const dismiss = useCallback((id: number) => {
    setVisible((v) => v.filter((x) => x.id !== id));
  }, []);

  const open = useCallback((item: AnomalyItem) => {
    // В embed'е (окно расширения в терминале) сайта нет — отдаём диплинк оболочке,
    // она откроет НАШУ панель индикатора на активе сигнала. navigate() здесь
    // развернул бы полную страницу сайта внутри окошка терминала.
    if (!(inEmbed && emitEmbedSignal(item.deep_link))) {
      openAnomaly(item, navigate, showUpgrade);
    }
    dismiss(item.id);
  }, [inEmbed, navigate, showUpgrade, dismiss]);

  const subscribe = useCallback(async (item: AnomalyItem) => {
    setSubscribing(item.id);
    const r = await subscribeAnomalyAction(item, isAuthenticated, showUpgrade);
    if (r === 'ok') setSubscribed((s) => new Set(s).add(item.id));
    setSubscribing(null);
  }, [isAuthenticated, showUpgrade]);

  if (!toastsEnabled) return null;
  if (!visible.length && !digest) return null;

  const containerStyle: React.CSSProperties = mobile
    ? { position: 'fixed', left: 12, right: 12, bottom: 72, zIndex: 1200,
        display: 'flex', flexDirection: 'column', gap: 10, pointerEvents: 'none' }
    : { position: 'fixed', right: 20, bottom: 20, width: 340, zIndex: 1200,
        display: 'flex', flexDirection: 'column', gap: 10, pointerEvents: 'none' };

  return (
    <div style={containerStyle} aria-live="polite">
      {digest && (
        <div style={{ pointerEvents: 'auto', background: 'var(--bg-secondary, #15181C)',
          border: '0.5px solid var(--border-color, rgba(255,255,255,0.12))', borderRadius: 12,
          padding: '13px 14px', boxShadow: '0 8px 28px rgba(0,0,0,0.40)',
          display: 'flex', alignItems: 'center', gap: 12 }}>
          <Activity size={20} color="var(--accent-orange, #FF9100)" />
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-primary)', fontSize: 14, fontWeight: 500 }}>Рынок штормит</div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{digest.count} заметных аномалий</div>
          </div>
          <button onClick={() => {
            // В embed'е колокола нет: «Посмотреть» открывает нашу панель «Сигналы»
            // (лента /embed/signals) поверх терминала — тем же мостом, что и тост.
            if (inEmbed) emitEmbedSignal({ route: '/signals' } as AnomalyItem['deep_link']);
            else setBellOpen(true);
            setDigest(null);
          }}
            style={{ background: 'transparent', border: '0.5px solid var(--border-color, rgba(255,255,255,0.14))',
              color: 'var(--text-primary)', fontSize: 13, fontWeight: 500, padding: '6px 12px',
              borderRadius: 8, cursor: 'pointer' }}>Посмотреть</button>
          <button onClick={() => setDigest(null)} aria-label="Закрыть"
            style={{ background: 'transparent', border: 'none', padding: 0, cursor: 'pointer', display: 'flex' }}>
            <X size={16} color="var(--text-muted)" />
          </button>
        </div>
      )}
      {visible.map((item) => (
        <div key={item.id} style={{ pointerEvents: 'auto' }}>
          <AnomalyToast
            item={item}
            progress={progress[item.id] ?? 0}
            subscribing={subscribing === item.id}
            subscribed={subscribed.has(item.id)}
            onOpen={() => open(item)}
            onSubscribe={() => subscribe(item)}
            onClose={() => dismiss(item.id)}
            onPause={() => pause(item.id)}
            onResume={() => resume(item.id)}
          />
        </div>
      ))}
    </div>
  );
}
