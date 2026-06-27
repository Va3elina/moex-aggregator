/**
 * TrialSuccessPage — возвращаемся сюда после привязки карты (T-Bank AddCard)
 * для пробного периода (success_url в trial.start_trial).
 *
 * Flow:
 *  1. На mount дёргаем POST /api/billing/trial/complete — сервер спросит у
 *     T-Bank GetAddCardState, заберёт RebillId и активирует триал.
 *  2. Если ok+active → показываем «пробный период активирован».
 *  3. Иначе короткий polling (привязка могла ещё не финализироваться).
 *  4. По таймауту — кнопка «Проверить ещё раз».
 */
import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { CheckCircle2, Clock, AlertCircle, RotateCw } from 'lucide-react';
import { apiFetch } from '../services/api';
import { useAnalytics } from '../contexts/AnalyticsContext';
import { trackEvent } from '../hooks/useYandexMetrica';

interface CompleteResult {
  ok: boolean;
  status?: string;
  subscription_id?: number | null;
  expires_at?: string | null;
  tier?: string;
  reason?: string;
}

export default function TrialSuccessPage() {
  const [state, setState] = useState<'pending' | 'active' | 'timeout' | 'error'>('pending');
  const [expiresAt, setExpiresAt] = useState<string | null>(null);
  const [reason, setReason] = useState<string | null>(null);
  const [retrying, setRetrying] = useState(false);
  const { track } = useAnalytics();

  // Воронка: фиксируем успешную активацию триала ровно один раз.
  useEffect(() => {
    if (state === 'active') {
      track('trial_activated', {});
      trackEvent('trial_activated');
    }
  }, [state, track]);

  const runComplete = useCallback(async (): Promise<CompleteResult | null> => {
    try {
      const r = await apiFetch('/api/billing/trial/complete', { method: 'POST' });
      const b: CompleteResult = await r.json().catch(() => ({ ok: false }));
      return b;
    } catch {
      return null;
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    let attempts = 0;
    const maxAttempts = 8; // 8 × 2с = 16с

    (async () => {
      const first = await runComplete();
      if (cancelled) return;
      if (first?.ok && first.status === 'active') {
        setExpiresAt(first.expires_at ?? null);
        setState('active');
        return;
      }
      const tick = async () => {
        if (cancelled) return;
        attempts += 1;
        const res = await runComplete();
        if (cancelled) return;
        if (res?.ok && res.status === 'active') {
          setExpiresAt(res.expires_at ?? null);
          setState('active');
          return;
        }
        if (attempts >= maxAttempts) {
          setReason(res?.reason ?? null);
          setState('timeout');
          return;
        }
        setTimeout(tick, 2000);
      };
      setTimeout(tick, 2000);
    })();

    return () => { cancelled = true; };
  }, [runComplete]);

  const retry = async () => {
    setRetrying(true);
    const res = await runComplete();
    setRetrying(false);
    if (res?.ok && res.status === 'active') {
      setExpiresAt(res.expires_at ?? null);
      setState('active');
    } else {
      setReason(res?.reason ?? null);
    }
  };

  const fmtDate = (iso: string | null) =>
    iso ? new Date(iso).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', year: 'numeric' }) : '';

  return (
    <div style={{
      maxWidth: 520, margin: '0 auto', padding: '48px 20px',
      textAlign: 'center', color: 'var(--text-primary, #1a1a1a)',
    }}>
      {state === 'active' && (
        <>
          <CheckCircle2 size={56} color="var(--accent, #FF5C2B)" style={{ display: 'block', margin: '0 auto 16px' }} />
          <h1 style={{ fontSize: 'var(--fs-2xl)', marginBottom: 12 }}>Успешно! Пробный период активирован</h1>
          <p style={{ color: 'var(--text-secondary, #666)', marginBottom: 8, lineHeight: 1.6 }}>
            Доступ открыт{expiresAt ? <> до <b>{fmtDate(expiresAt)}</b></> : ''}. Списание произойдёт
            только по окончании, если вы не отмените. Отменить и отвязать карту можно в профиле.
          </p>
          <Link to="/" className="editorial-press" style={{
            display: 'inline-flex', alignItems: 'center', justifyContent: 'center', minHeight: 44,
            marginTop: 20, padding: '12px 24px', borderRadius: 8,
            background: 'var(--accent, #FF5C2B)', color: '#fff', textDecoration: 'none', fontWeight: 600,
          }}>
            К аналитике →
          </Link>
        </>
      )}

      {state === 'pending' && (
        <>
          <Clock size={56} color="var(--text-muted, #999)" style={{ display: 'block', margin: '0 auto 16px' }} />
          <h1 style={{ fontSize: 'var(--fs-xl)', marginBottom: 12 }}>Активируем пробный период…</h1>
          <p style={{ color: 'var(--text-secondary, #666)' }}>Подтверждаем привязку карты, секунду.</p>
        </>
      )}

      {(state === 'timeout' || state === 'error') && (
        <>
          <AlertCircle size={56} color="var(--text-muted, #999)" style={{ display: 'block', margin: '0 auto 16px' }} />
          <h1 style={{ fontSize: 'var(--fs-xl)', marginBottom: 12 }}>Почти готово</h1>
          <p style={{ color: 'var(--text-secondary, #666)', marginBottom: 8 }}>
            {reason || 'Не удалось подтвердить привязку карты автоматически.'}
          </p>
          <button onClick={retry} disabled={retrying} className="editorial-press" style={{
            display: 'inline-flex', alignItems: 'center', gap: 8, marginTop: 16,
            padding: '12px 24px', borderRadius: 8, border: '1px solid var(--border-color, #ddd)',
            background: 'transparent', color: 'var(--text-primary, #1a1a1a)', cursor: 'pointer', fontWeight: 600,
          }}>
            <RotateCw size={16} /> {retrying ? 'Проверяем…' : 'Проверить ещё раз'}
          </button>
          <div style={{ marginTop: 16 }}>
            <Link to="/pricing" style={{ color: 'var(--accent, #FF5C2B)' }}>Вернуться к тарифам</Link>
          </div>
        </>
      )}
    </div>
  );
}
