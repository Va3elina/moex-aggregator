/**
 * BillingSuccessPage — возвращаемся сюда после успешной оплаты в ЮKassa.
 *
 * На момент редиректа webhook от ЮKassa может ещё не дойти до нашего сервера,
 * поэтому подписка может быть ещё в статусе 'pending'. Показываем спиннер +
 * проверяем статус каждые 2 сек (до 30 сек). Как только активна — показываем
 * успех и кнопку "В личный кабинет".
 */
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { CheckCircle2, Clock, AlertCircle } from 'lucide-react';
import { apiFetch } from '../services/api';

interface Status {
  tier: string;
  is_active: boolean;
  plan_id: string | null;
  expires_at: string | null;
}

export default function BillingSuccessPage() {
  const [status, setStatus] = useState<Status | null>(null);
  const [state, setState] = useState<'pending' | 'active' | 'timeout'>('pending');

  useEffect(() => {
    let cancelled = false;
    let attempts = 0;
    const maxAttempts = 15; // 15 × 2 сек = 30 сек

    const check = async () => {
      if (cancelled) return;
      try {
        const r = await apiFetch('/api/billing/status');
        if (!r.ok) throw new Error('status fetch failed');
        const s: Status = await r.json();
        setStatus(s);
        if (s.is_active) {
          setState('active');
          return;
        }
      } catch {
        // ignore, продолжаем polling
      }
      attempts++;
      if (attempts >= maxAttempts) {
        setState('timeout');
      } else {
        setTimeout(check, 2000);
      }
    };

    check();
    return () => { cancelled = true; };
  }, []);

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
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Оплата прошла!</h1>
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
            Платёж возможно прошёл, но webhook ещё не дошёл до нас.<br />
            Зайди в{' '}
            <Link to="/profile" className="text-theme-primary underline">профиль</Link>{' '}
            через пару минут — если не подтвердится, напиши в поддержку.
          </p>
        </>
      )}
    </div>
  );
}
