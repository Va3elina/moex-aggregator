/**
 * BillingSuccessPage — возвращаемся сюда после успешной оплаты у провайдера.
 *
 * Flow:
 *  1. Сразу при mount'е дёргаем POST /api/billing/sync — он на сервере
 *     спросит у T-Bank GetState актуальный статус всех pending платежей
 *     текущего user'а и активирует те что CONFIRMED. Это страховка на случай
 *     задержки/отсутствия webhook'а.
 *  2. Если /sync уже вернул is_active=true → готово.
 *  3. Иначе polling /api/billing/status каждые 2 сек до 30 сек (вдруг
 *     webhook всё-таки дойдёт).
 *  4. По таймауту — кнопка «Проверить статус» делает повторный /sync.
 */
import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { CheckCircle2, Clock, AlertCircle, RotateCw } from 'lucide-react';
import { apiFetch } from '../services/api';

interface Status {
  tier: string;
  is_active: boolean;
  plan_id?: string | null;
  expires_at?: string | null;
  subscription_id?: number | null;
}

export default function BillingSuccessPage() {
  const [status, setStatus] = useState<Status | null>(null);
  const [state, setState] = useState<'pending' | 'active' | 'timeout'>('pending');
  const [syncing, setSyncing] = useState(false);
  // true если сюда привёл AddCard-триал (а не обычная оплата) → текст про триал.
  const [trialActivated, setTrialActivated] = useState(false);

  /** Принудительный sync через GetState у провайдера. */
  const runSync = useCallback(async (): Promise<Status | null> => {
    try {
      const r = await apiFetch('/api/billing/sync', { method: 'POST' });
      if (!r.ok) return null;
      const s: Status = await r.json();
      setStatus(s);
      return s;
    } catch {
      return null;
    }
  }, []);

  /** Добивание триала: если T-Bank после AddCard-привязки вернул на эту (общую)
   *  success-страницу, а не на /billing/trial-success — всё равно активируем
   *  триал. No-op если ожидающего триала нет. */
  const runTrialComplete = useCallback(async (): Promise<boolean> => {
    try {
      const r = await apiFetch('/api/billing/trial/complete', { method: 'POST' });
      if (!r.ok) return false;
      const b = await r.json();
      return b?.ok === true && b?.status === 'active';
    } catch {
      return false;
    }
  }, []);

  /** Лёгкий polling статуса (без вызовов к провайдеру). */
  const fetchStatus = useCallback(async (): Promise<Status | null> => {
    try {
      const r = await apiFetch('/api/billing/status');
      if (!r.ok) return null;
      const s: Status = await r.json();
      setStatus(s);
      return s;
    } catch {
      return null;
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    let attempts = 0;
    const maxAttempts = 15; // 15 × 2 сек = 30 сек

    (async () => {
      // 0. Триал-привязка (AddCard) могла вести сюда вместо /billing/trial-success —
      //    добиваем триал. No-op для обычных платежей.
      if (await runTrialComplete()) {
        if (cancelled) return;
        await fetchStatus();
        setTrialActivated(true);
        setState('active');
        return;
      }
      // 1. Сразу sync — спросим T-Bank GetState не дожидаясь webhook
      const synced = await runSync();
      if (cancelled) return;
      if (synced?.is_active) {
        setState('active');
        return;
      }
      // 2. Polling каждые 2 сек — вдруг webhook дойдёт за это время
      const tick = async () => {
        if (cancelled) return;
        if (await runTrialComplete()) {
          if (cancelled) return;
          await fetchStatus();
          setTrialActivated(true);
          setState('active');
          return;
        }
        const s = await fetchStatus();
        if (s?.is_active) {
          setState('active');
          return;
        }
        attempts++;
        if (attempts >= maxAttempts) {
          setState('timeout');
        } else {
          setTimeout(tick, 2000);
        }
      };
      setTimeout(tick, 2000);
    })();

    return () => {
      cancelled = true;
    };
  }, [runSync, fetchStatus, runTrialComplete]);

  /** Кнопка «Проверить статус» в timeout-state. */
  const handleManualSync = async () => {
    setSyncing(true);
    const s = await runSync();
    setSyncing(false);
    if (s?.is_active) setState('active');
  };

  return (
    <div className="max-w-xl mx-auto px-6 py-12 text-center">
      {state === 'pending' && (
        <>
          <Clock className="w-16 h-16 mx-auto mb-4 text-blue-400 animate-pulse" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Обрабатываем оплату...</h1>
          <p className="text-theme-secondary">Обычно занимает несколько секунд. Не закрывай страницу.</p>
        </>
      )}
      {state === 'active' && status && (
        <>
          <CheckCircle2 className="w-16 h-16 mx-auto mb-4 text-green-400" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">
            {trialActivated ? 'Успешно! Пробный период активирован' : 'Оплата прошла!'}
          </h1>
          <p className="text-theme-secondary mb-2">
            Тариф: <strong className="text-theme-primary">{status.tier.toUpperCase()}</strong>
          </p>
          {status.expires_at && (
            <p className="text-theme-secondary text-sm mb-6">
              Действует до {new Date(status.expires_at).toLocaleDateString('ru-RU')}
            </p>
          )}
          <div className="flex gap-3 justify-center">
            <Link
              to="/"
              className="px-5 py-2 rounded-xl text-sm font-medium"
              style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
            >
              На главную
            </Link>
            <Link
              to="/profile"
              className="px-5 py-2 rounded-xl text-sm font-medium border"
              style={{ borderColor: 'var(--border-color)', color: 'var(--text-primary)' }}
            >
              Мой профиль
            </Link>
          </div>
        </>
      )}
      {state === 'timeout' && (
        <>
          <AlertCircle className="w-16 h-16 mx-auto mb-4 text-amber-400" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Подтверждение задерживается</h1>
          <p className="text-theme-secondary mb-6">
            Платёж возможно прошёл, но webhook ещё не дошёл до нас.
            Нажми «Проверить статус» — мы сами спросим у Т-Банка.
          </p>
          <div className="flex flex-wrap gap-3 justify-center">
            <button
              onClick={handleManualSync}
              disabled={syncing}
              className="inline-flex items-center gap-2 px-5 py-2 rounded-xl text-sm font-medium transition-opacity hover:opacity-90 disabled:opacity-50"
              style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
            >
              <RotateCw size={16} className={syncing ? 'animate-spin' : ''} />
              {syncing ? 'Проверяем…' : 'Проверить статус'}
            </button>
            <Link
              to="/profile"
              className="px-5 py-2 rounded-xl text-sm font-medium border"
              style={{ borderColor: 'var(--border-color)', color: 'var(--text-primary)' }}
            >
              В профиль
            </Link>
          </div>
          <p className="mt-6 text-xs" style={{ color: 'var(--text-muted)' }}>
            Если статус так и не подтвердится — напиши на{' '}
            <a href="mailto:frameinfo@mail.ru" style={{ color: 'var(--accent)' }} className="hover:underline">
              frameinfo@mail.ru
            </a>
          </p>
        </>
      )}
    </div>
  );
}
