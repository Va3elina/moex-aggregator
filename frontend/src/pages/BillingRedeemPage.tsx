/**
 * BillingRedeemPage — /billing/redeem?token=X
 *
 * Flow:
 * 1. Пользователь кликает invite-ссылку → попадает сюда
 * 2. Если НЕ залогинен → запоминаем token и ведём на регистрацию
 * 3. Если залогинен → POST /api/billing/redeem {token} → активация
 * 4. Показываем результат (успех или ошибка)
 *
 * Возврат после регистрации держится НЕ на `?next=` (он терялся на пути
 * регистрация → /verify-email → главная, и человек оставался без подписки).
 * Токен применяет PendingRedeemApplier по факту авторизации, на любой странице.
 */
import { useEffect, useState } from 'react';
import { useSearchParams, useNavigate, Link } from 'react-router-dom';
import { CheckCircle2, XCircle, Gift, UserPlus } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { apiFetch, parseApiError } from '../services/api';
import {
  clearPendingRedeemToken,
  getPendingRedeemToken,
  popRedeemResult,
  setPendingRedeemToken,
} from '../utils/pendingRedeem';

type State =
  | { kind: 'loading' }
  | { kind: 'need-login' }
  | { kind: 'success'; tier: string; expires_at: string | null }
  | { kind: 'error'; message: string };

const SIGNUP_URL = '/login?mode=register&next=/billing/redeem';

export default function BillingRedeemPage() {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const { isAuthenticated, user } = useAuth();
  // Почта не подтверждена и при этом настоящая: у Telegram/VK без email адрес
  // синтетический, им сначала /add-email, и вести их на ввод кода бессмысленно.
  const needsEmailVerify = Boolean(user && !user.is_verified && !user.requires_email_setup);

  const tokenFromUrl = params.get('token') || '';
  // Пришли без token (вернулись после регистрации) — берём отложенный
  const [token] = useState<string>(() => tokenFromUrl || getPendingRedeemToken() || '');
  // Токена нет, потому что PendingRedeemApplier уже успел его применить и
  // стереть (авторизация происходит ещё на /login). Тогда он оставил исход —
  // забираем его при первом рендере, ДО того как решим, что ссылка пустая.
  const [appliedResult] = useState(() => (tokenFromUrl ? null : popRedeemResult()));
  const [state, setState] = useState<State>({ kind: 'loading' });

  useEffect(() => {
    if (!token) {
      if (appliedResult) {
        setState(appliedResult.ok
          ? { kind: 'success', tier: appliedResult.tier, expires_at: appliedResult.expires_at }
          : { kind: 'error', message: appliedResult.message });
        return;
      }
      setState({ kind: 'error', message: 'Ссылка не содержит токен' });
      return;
    }

    if (!isAuthenticated) {
      // Запоминаем токен, чтобы применить сразу после регистрации или входа
      setPendingRedeemToken(token);
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
          // Токен мёртв (истёк / отозван / исчерпан) — выкидываем, чтобы он не
          // висел в хранилище и не дёргал redeem на каждой навигации.
          if (r.status < 500) clearPendingRedeemToken();
          // parseApiError, а не err.detail: бэкенд заворачивает ошибки в
          // {success:false,error:{message}}, и «Срок действия ссылки истёк»
          // схлопывалось в безликое «Ошибка активации».
          setState({ kind: 'error', message: await parseApiError(r, 'Ошибка активации') });
          return;
        }
        const data = await r.json();
        clearPendingRedeemToken();
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
  }, [token, isAuthenticated, appliedResult]);

  // Гостю даём 2 секунды прочитать, что происходит, и уводим на регистрацию
  useEffect(() => {
    if (state.kind !== 'need-login') return;
    const t = setTimeout(() => navigate(SIGNUP_URL), 2000);
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
          <UserPlus className="w-16 h-16 mx-auto mb-4 text-blue-400" />
          <h1 className="text-2xl font-bold text-theme-primary mb-2">Нужен аккаунт</h1>
          <p className="text-theme-secondary mb-4">
            Создай аккаунт или войди в существующий.<br />
            Подписка активируется сразу после этого, возвращаться по ссылке не нужно.
          </p>
          <Link
            to={SIGNUP_URL}
            className="inline-block px-6 py-3 rounded-xl font-medium"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
          >
            Создать аккаунт
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
          {/* Подтверждение почты. Регистрация по инвайту ведёт СЮДА, а не на
              /verify-email (иначе терялась подписка), поэтому шаг с кодом из
              письма надо предложить здесь — иначе про него узнать неоткуда,
              кроме баннера в профиле. OAuth-юзерам не показываем: у них
              is_verified=true от провайдера. */}
          {needsEmailVerify && (
            <div
              className="rounded-xl border p-4 mb-6 text-sm"
              style={{ borderColor: 'var(--border-color)', color: 'var(--text-secondary)' }}
            >
              Остался один шаг: подтверди почту. Код уже отправлен на{' '}
              <strong className="text-theme-primary">{user?.email}</strong>.
            </div>
          )}
          <div className="flex gap-3 justify-center flex-wrap">
            {needsEmailVerify && (
              <Link
                to="/verify-email"
                className="px-5 py-2 rounded-xl text-sm font-medium"
                style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
              >
                Подтвердить почту
              </Link>
            )}
            <Link
              to="/"
              className={needsEmailVerify
                ? 'px-5 py-2 rounded-xl text-sm font-medium border'
                : 'px-5 py-2 rounded-xl text-sm font-medium'}
              style={needsEmailVerify
                ? { borderColor: 'var(--border-color)', color: 'var(--text-primary)' }
                : { backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
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
