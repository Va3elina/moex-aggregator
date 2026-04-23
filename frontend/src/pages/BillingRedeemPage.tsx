/**
 * BillingRedeemPage — /billing/redeem?token=X
 *
 * Flow:
 * 1. Пользователь кликает invite-ссылку → попадает сюда
 * 2. Если НЕ залогинен → сохраняем token в sessionStorage, редиректим на /login
 *    После успешного входа (в LoginPage добавить redirect back) → возвращаемся сюда
 * 3. Если залогинен → POST /api/billing/redeem {token} → активация
 * 4. Показываем результат (успех или ошибка)
 */
import { useEffect, useState } from 'react';
import { useSearchParams, useNavigate, Link } from 'react-router-dom';
import { CheckCircle2, XCircle, Gift, LogIn } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { apiFetch } from '../services/api';

type State =
  | { kind: 'loading' }
  | { kind: 'need-login' }
  | { kind: 'success'; tier: string; expires_at: string | null }
  | { kind: 'error'; message: string };

const REDEEM_TOKEN_KEY = 'pending_redeem_token';

export default function BillingRedeemPage() {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const { isAuthenticated } = useAuth();

  const tokenFromUrl = params.get('token') || '';
  // Если после login пришли без token — читаем из sessionStorage
  const [token] = useState<string>(() => {
    if (tokenFromUrl) return tokenFromUrl;
    return sessionStorage.getItem(REDEEM_TOKEN_KEY) || '';
  });
  const [state, setState] = useState<State>({ kind: 'loading' });

  useEffect(() => {
    if (!token) {
      setState({ kind: 'error', message: 'Ссылка не содержит токен' });
      return;
    }

    if (!isAuthenticated) {
      // Сохраняем токен чтобы продолжить после login
      sessionStorage.setItem(REDEEM_TOKEN_KEY, token);
      setState({ kind: 'need-login' });
      return;
    }

    // Залогинены — применяем токен
    let cancelled = false;
    apiFetch('/api/billing/redeem', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    })
      .then(async (r) => {
        if (cancelled) return;
        if (!r.ok) {
          const err = await r.json().catch(() => ({}));
          setState({ kind: 'error', message: err.detail || 'Ошибка активации' });
          return;
        }
        const data = await r.json();
        sessionStorage.removeItem(REDEEM_TOKEN_KEY); // очищаем
        setState({
          kind: 'success',
          tier: data.tier,
          expires_at: data.expires_at,
        });
      })
      .catch((e) => {
        if (!cancelled) {
          setState({ kind: 'error', message: e.message || 'Сетевая ошибка' });
        }
      });
    return () => { cancelled = true; };
  }, [token, isAuthenticated]);

  // Если нужен login — автоматически редирект через 2 сек (чтобы пользователь прочёл что происходит)
  useEffect(() => {
    if (state.kind !== 'need-login') return;
    const t = setTimeout(() => navigate(`/login?next=/billing/redeem`), 2000);
    return () => clearTimeout(t);
  }, [state, navigate]);

  return (
    <div className="max-w-xl mx-auto px-6 py-12 text-center">
      {state.kind === 'loading' && (
        <>
          <Gift className="w-16 h-16 mx-auto mb-4 text-indigo-400 animate-pulse" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Активируем подписку...</h1>
          <p className="text-theme-secondary">Секунду...</p>
        </>
      )}

      {state.kind === 'need-login' && (
        <>
          <LogIn className="w-16 h-16 mx-auto mb-4 text-blue-400" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Вход в аккаунт</h1>
          <p className="text-theme-secondary mb-4">
            Чтобы применить ссылку, войди в свой аккаунт.<br />
            После входа подписка активируется автоматически.
          </p>
          <Link
            to="/login?next=/billing/redeem"
            className="inline-block px-6 py-3 rounded-xl font-medium"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            Войти
          </Link>
          <div className="text-xs text-theme-muted mt-4">Перенаправляем через 2 секунды...</div>
        </>
      )}

      {state.kind === 'success' && (
        <>
          <CheckCircle2 className="w-16 h-16 mx-auto mb-4 text-green-400" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Подписка активирована!</h1>
          <p className="text-theme-secondary mb-2">
            Тариф: <strong className="text-theme-primary uppercase">{state.tier}</strong>
          </p>
          {state.expires_at && (
            <p className="text-sm text-theme-secondary mb-6">
              Действует до {new Date(state.expires_at).toLocaleDateString('ru-RU', {
                day: 'numeric', month: 'long', year: 'numeric',
              })}
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
              Профиль
            </Link>
          </div>
        </>
      )}

      {state.kind === 'error' && (
        <>
          <XCircle className="w-16 h-16 mx-auto mb-4 text-red-400" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Не удалось применить</h1>
          <p className="text-red-300 mb-6">{state.message}</p>
          <Link
            to="/pricing"
            className="inline-block px-5 py-2 rounded-xl text-sm font-medium border"
            style={{ borderColor: 'var(--border-color)', color: 'var(--text-primary)' }}
          >
            Перейти к тарифам
          </Link>
        </>
      )}
    </div>
  );
}
