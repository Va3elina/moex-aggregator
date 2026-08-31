/**
 * AddEmailPage — обязательная форма привязки реального email для OAuth-юзеров
 * с synthetic email (Telegram by design, VK часто).
 *
 * Зачем: T-Bank по 54-ФЗ должен выдать чек на реальный email/phone. Если у
 * юзера email вроде `telegram_847239@oauth.local` — чек никуда не дойдёт,
 * что формально нарушает закон. Plus юзер не получит verification и
 * password-reset когда мы их добавим в Phase 2.
 *
 * UX: страница НЕ dismissible — пока юзер не привяжет email, его редиректит
 * сюда из App.tsx гейта при любой навигации (кроме legal-страниц и logout).
 * Если юзер закрыл вкладку — при следующем визите снова попадёт сюда.
 *
 * Verification email отправлять не нужно (пока — Phase 2 после Yandex 360):
 * `is_verified=False` остаётся, но email уже не synthetic — биллинг сможет
 * слать чек на реальный адрес.
 */
import { useState, type FormEvent } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Mail, AlertCircle } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { addEmail, ApiError } from '../services/api';
import { safeInternalPath } from '../utils/postLoginRedirect';
import { emailStepUrl } from '../utils/checkoutIntent';

export default function AddEmailPage() {
  const { user, refreshUser } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  // ?next= — цель, ради которой юзера сюда прислали (обычно оформление
  // подписки). Передаём её дальше на /verify-email, иначе после ввода кода
  // человек окажется на главной и начнёт покупку заново.
  const next = safeInternalPath(searchParams.get('next'));
  const verifyUrl = next ? emailStepUrl('/verify-email', next) : '/verify-email';
  const [email, setEmail] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [errorIsConflict, setErrorIsConflict] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  // Защита: если юзер открыл /add-email напрямую, но email уже не synthetic —
  // редирект на главную. Иначе можно зациклиться.
  if (user && !user.requires_email_setup) {
    // email уже реальный: если не подтверждён — на ввод кода, иначе на главную
    navigate(user.is_verified ? (next || '/') : verifyUrl, { replace: true });
    return null;
  }

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setErrorIsConflict(false);
    const trimmed = email.trim().toLowerCase();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(trimmed)) {
      setError('Введите корректный email (например, user@example.com)');
      return;
    }
    setSubmitting(true);
    try {
      await addEmail(trimmed);
      await refreshUser();
      navigate(verifyUrl, { replace: true });  // дальше — ввод кода из письма
    } catch (err) {
      const isConflict = err instanceof ApiError && err.status === 409;
      setErrorIsConflict(isConflict);
      setError(err instanceof Error ? err.message : 'Не удалось привязать email');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div
      className="min-h-screen flex items-center justify-center px-4"
      style={{ backgroundColor: 'var(--bg-primary)' }}
    >
      <div
        className="w-full max-w-md p-8 rounded-lg"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
        }}
      >
        <div className="flex items-center gap-3 mb-6">
          <div
            className="w-10 h-10 rounded-full flex items-center justify-center"
            style={{ backgroundColor: 'color-mix(in srgb, var(--accent) 14%, transparent)' }}
          >
            <Mail size={20} style={{ color: 'var(--accent)' }} />
          </div>
          <h1 className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>
            Укажите ваш email
          </h1>
        </div>

        <p className="text-sm mb-6" style={{ color: 'var(--text-secondary)' }}>
          {user?.oauth_providers[0] === 'telegram' ? 'Через Telegram' : 'Через VK'} мы
          email не получаем. Оставьте свой — на него придёт чек при оплате и
          ссылка для восстановления доступа.
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label
              htmlFor="email"
              className="block text-sm font-medium mb-2"
              style={{ color: 'var(--text-primary)' }}
            >
              Email
            </label>
            <input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="ivan@example.com"
              required
              autoFocus
              autoComplete="email"
              disabled={submitting}
              className="w-full px-3 py-2 rounded border outline-none focus:ring-2"
              style={{
                backgroundColor: 'var(--bg-primary)',
                color: 'var(--text-primary)',
                borderColor: 'var(--border-color)',
              }}
            />
          </div>

          {error && (
            <div
              className="flex items-start gap-2 p-3 rounded text-sm"
              style={{
                backgroundColor: 'rgba(220, 38, 38, 0.1)',
                color: 'var(--danger, #dc2626)',
              }}
            >
              <AlertCircle size={16} className="mt-0.5 flex-shrink-0" />
              <div>
                <div>{error}</div>
                {errorIsConflict && (
                  <div className="mt-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
                    Если у вас уже есть аккаунт с этим email — выйдите и войдите
                    через провайдера, которым вы регистрировались раньше
                    (Yandex/Google). Мы откроем существующий аккаунт.
                  </div>
                )}
              </div>
            </div>
          )}

          <button
            type="submit"
            disabled={submitting || !email}
            className="w-full py-2.5 rounded font-medium transition-opacity disabled:opacity-50"
            style={{
              backgroundColor: 'var(--accent)',
              color: 'var(--accent-text, #fff)',
            }}
          >
            {submitting ? 'Сохраняем…' : 'Привязать email'}
          </button>
        </form>

        <button
          type="button"
          onClick={() => navigate('/', { replace: true })}
          className="w-full mt-4 py-2 text-sm transition-opacity hover:opacity-70"
          style={{ color: 'var(--text-muted)' }}
        >
          Позже
        </button>
      </div>
    </div>
  );
}
