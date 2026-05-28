/**
 * VerifyEmailPage — подтверждение email 6-значным кодом из письма.
 *
 * Кому показывается: email+password юзерам с is_verified=false (после
 * регистрации редиректим сюда). OAuth-юзеры сюда не попадают — у них
 * is_verified=true от провайдера. Synthetic-email юзеров отправляем сначала
 * на /add-email.
 *
 * Вход НЕ блокируется — это удобная страница ввода кода + ресенд. Баннер в
 * профиле тоже ведёт сюда. Бэкенд: POST /api/auth/verify-email, /resend-verification.
 */
import { useState, useEffect, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { MailCheck, AlertCircle, CheckCircle2 } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { verifyEmail, resendVerification, ApiError } from '../services/api';

export default function VerifyEmailPage() {
  const { user, refreshUser } = useAuth();
  const navigate = useNavigate();
  const [code, setCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [resending, setResending] = useState(false);
  const [cooldown, setCooldown] = useState(0);

  // Обратный отсчёт кулдауна ресенда
  useEffect(() => {
    if (cooldown <= 0) return;
    const t = setInterval(() => setCooldown((c) => (c <= 1 ? 0 : c - 1)), 1000);
    return () => clearInterval(t);
  }, [cooldown]);

  // Гварды — навигация в эффекте (нельзя делать early-return до хуков)
  useEffect(() => {
    if (!user) navigate('/login', { replace: true });
    else if (user.is_verified) navigate('/', { replace: true });
    else if (user.requires_email_setup) navigate('/add-email', { replace: true });
  }, [user, navigate]);

  if (!user || user.is_verified || user.requires_email_setup) return null;

  const handleVerify = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setInfo(null);
    const trimmed = code.trim();
    if (!/^\d{6}$/.test(trimmed)) {
      setError('Код состоит из 6 цифр');
      return;
    }
    setSubmitting(true);
    try {
      await verifyEmail(trimmed);
      await refreshUser();
      navigate('/', { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Не удалось подтвердить email');
    } finally {
      setSubmitting(false);
    }
  };

  const handleResend = async () => {
    if (cooldown > 0 || resending) return;
    setError(null);
    setInfo(null);
    setResending(true);
    try {
      await resendVerification();
      setInfo(`Новый код отправлен на ${user.email}`);
      setCooldown(60);
    } catch (err) {
      // 429 — кулдаун ещё не вышел; вытащим число секунд из сообщения
      if (err instanceof ApiError && err.status === 429) {
        const m = err.message.match(/(\d+)/);
        setCooldown(m ? parseInt(m[1], 10) : 60);
      }
      setError(err instanceof Error ? err.message : 'Не удалось отправить код');
    } finally {
      setResending(false);
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
            style={{ backgroundColor: 'var(--accent-bg)' }}
          >
            <MailCheck size={20} style={{ color: 'var(--accent)' }} />
          </div>
          <h1 className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>
            Подтвердите email
          </h1>
        </div>

        <p className="text-sm mb-6" style={{ color: 'var(--text-secondary)' }}>
          Мы отправили 6-значный код на <b style={{ color: 'var(--text-primary)' }}>{user.email}</b>.
          Введите его ниже. Код действует 30&nbsp;минут.
        </p>

        <form onSubmit={handleVerify} className="space-y-4">
          <div>
            <label
              htmlFor="code"
              className="block text-sm font-medium mb-2"
              style={{ color: 'var(--text-primary)' }}
            >
              Код из письма
            </label>
            <input
              id="code"
              type="text"
              inputMode="numeric"
              autoComplete="one-time-code"
              value={code}
              onChange={(e) => setCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
              placeholder="000000"
              required
              autoFocus
              maxLength={6}
              disabled={submitting}
              className="w-full px-3 py-3 rounded border outline-none focus:ring-2 text-center"
              style={{
                backgroundColor: 'var(--bg-primary)',
                color: 'var(--text-primary)',
                borderColor: 'var(--border-color)',
                fontFamily: 'var(--font-mono, monospace)',
                fontSize: '24px',
                letterSpacing: '0.4em',
                fontWeight: 600,
              }}
            />
          </div>

          {error && (
            <div
              className="flex items-start gap-2 p-3 rounded text-sm"
              style={{ backgroundColor: 'rgba(220, 38, 38, 0.1)', color: 'var(--danger, #dc2626)' }}
            >
              <AlertCircle size={16} className="mt-0.5 flex-shrink-0" />
              <div>{error}</div>
            </div>
          )}

          {info && (
            <div
              className="flex items-start gap-2 p-3 rounded text-sm"
              style={{ backgroundColor: 'rgba(34, 197, 94, 0.1)', color: 'var(--success, #16a34a)' }}
            >
              <CheckCircle2 size={16} className="mt-0.5 flex-shrink-0" />
              <div>{info}</div>
            </div>
          )}

          <button
            type="submit"
            disabled={submitting || code.length !== 6}
            className="w-full py-2.5 rounded font-medium transition-opacity disabled:opacity-50"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--accent-text, #fff)' }}
          >
            {submitting ? 'Проверяем…' : 'Подтвердить'}
          </button>
        </form>

        <button
          type="button"
          onClick={handleResend}
          disabled={cooldown > 0 || resending}
          className="w-full mt-3 py-2 text-sm transition-opacity hover:opacity-70 disabled:opacity-50"
          style={{ color: 'var(--accent)' }}
        >
          {cooldown > 0
            ? `Отправить код повторно (${cooldown}с)`
            : resending
            ? 'Отправляем…'
            : 'Отправить код повторно'}
        </button>

        <button
          type="button"
          onClick={() => navigate('/', { replace: true })}
          className="w-full mt-1 py-2 text-sm transition-opacity hover:opacity-70"
          style={{ color: 'var(--text-muted)' }}
        >
          Позже
        </button>
      </div>
    </div>
  );
}
