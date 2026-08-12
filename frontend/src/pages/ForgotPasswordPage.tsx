/**
 * ForgotPasswordPage — восстановление забытого пароля по коду из письма.
 *
 * Два шага на одной странице: ввод email → ввод кода и нового пароля. Отдельного
 * роута под второй шаг нет намеренно — код живёт 30 минут в БД, а не в URL, так
 * что «ссылка из письма» здесь не нужна и лишний адрес только даёт куда потерять
 * состояние.
 *
 * ⚠️ Бэкенд отвечает 204 даже если такого аккаунта нет (защита от перебора
 * адресов), поэтому шаг 2 показываем ВСЕГДА, а формулировки держим в
 * сослагательном: «если аккаунт существует». Иначе UI выдал бы то, что бэкенд
 * специально скрывает.
 *
 * Бэкенд: POST /api/auth/password-reset/{request,confirm}.
 */
import { useState, useEffect, type FormEvent } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { KeyRound, AlertCircle, CheckCircle2, ArrowLeft } from 'lucide-react';
import { requestPasswordReset, confirmPasswordReset, ApiError } from '../services/api';

export default function ForgotPasswordPage() {
  const navigate = useNavigate();
  const [step, setStep] = useState<'email' | 'code'>('email');
  const [email, setEmail] = useState('');
  const [code, setCode] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [cooldown, setCooldown] = useState(0);

  useEffect(() => {
    if (cooldown <= 0) return;
    const t = setInterval(() => setCooldown((c) => (c <= 1 ? 0 : c - 1)), 1000);
    return () => clearInterval(t);
  }, [cooldown]);

  const handleRequest = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setInfo(null);
    setSubmitting(true);
    try {
      await requestPasswordReset(email.trim().toLowerCase());
      setStep('code');
      setInfo(`Если аккаунт с адресом ${email.trim()} существует, код отправлен на почту.`);
      setCooldown(60);
    } catch (err) {
      if (err instanceof ApiError && err.status === 429) {
        setError('Слишком много запросов. Попробуйте через несколько минут.');
      } else {
        setError(err instanceof Error ? err.message : 'Не удалось отправить код');
      }
    } finally {
      setSubmitting(false);
    }
  };

  const handleConfirm = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setInfo(null);
    if (!/^\d{6}$/.test(code.trim())) {
      setError('Код состоит из 6 цифр');
      return;
    }
    if (password.length < 8) {
      setError('Пароль должен быть не короче 8 символов');
      return;
    }
    setSubmitting(true);
    try {
      await confirmPasswordReset(email.trim().toLowerCase(), code.trim(), password);
      navigate('/login?reset=ok', { replace: true });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Не удалось сменить пароль');
    } finally {
      setSubmitting(false);
    }
  };

  const handleResend = async () => {
    if (cooldown > 0 || submitting) return;
    setError(null);
    setInfo(null);
    try {
      await requestPasswordReset(email.trim().toLowerCase());
      setInfo('Код отправлен повторно.');
      setCooldown(60);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Не удалось отправить код');
    }
  };

  const inputStyle = {
    backgroundColor: 'var(--bg-primary)',
    color: 'var(--text-primary)',
    borderColor: 'var(--border-color)',
  };

  return (
    <div
      className="min-h-screen flex items-center justify-center px-4 py-10"
      style={{ backgroundColor: 'var(--bg-primary)' }}
    >
      <div
        className="w-full max-w-md p-6 sm:p-8 rounded-lg"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
        }}
      >
        <div className="flex items-center gap-3 mb-6">
          <div
            className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0"
            style={{ backgroundColor: 'var(--accent-bg)' }}
          >
            <KeyRound size={20} style={{ color: 'var(--accent)' }} />
          </div>
          <h1 className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>
            {step === 'email' ? 'Восстановление пароля' : 'Новый пароль'}
          </h1>
        </div>

        {step === 'email' ? (
          <>
            <p className="text-sm mb-6" style={{ color: 'var(--text-secondary)' }}>
              Введите адрес, на который зарегистрирован аккаунт — пришлём код для смены пароля.
            </p>
            <form onSubmit={handleRequest} className="space-y-4">
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
                  autoComplete="email"
                  value={email}
                  onChange={(ev) => setEmail(ev.target.value)}
                  placeholder="you@example.com"
                  required
                  autoFocus
                  disabled={submitting}
                  className="w-full px-3 py-3 rounded border outline-none focus:ring-2"
                  style={inputStyle}
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

              <button
                type="submit"
                disabled={submitting || !email.trim()}
                className="w-full py-2.5 rounded font-medium transition-opacity disabled:opacity-50"
                style={{ backgroundColor: 'var(--accent)', color: 'var(--accent-text, #fff)' }}
              >
                {submitting ? 'Отправляем…' : 'Прислать код'}
              </button>
            </form>
          </>
        ) : (
          <>
            <p className="text-sm mb-6" style={{ color: 'var(--text-secondary)' }}>
              Введите код из письма и придумайте новый пароль. Код действует 30&nbsp;минут.
            </p>
            <form onSubmit={handleConfirm} className="space-y-4">
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
                  onChange={(ev) => setCode(ev.target.value.replace(/\D/g, '').slice(0, 6))}
                  placeholder="000000"
                  required
                  autoFocus
                  maxLength={6}
                  disabled={submitting}
                  className="w-full px-3 py-3 rounded border outline-none focus:ring-2 text-center"
                  style={{
                    ...inputStyle,
                    fontFamily: 'var(--font-mono, monospace)',
                    fontSize: '24px',
                    letterSpacing: '0.4em',
                    fontWeight: 600,
                  }}
                />
              </div>

              <div>
                <label
                  htmlFor="new-password"
                  className="block text-sm font-medium mb-2"
                  style={{ color: 'var(--text-primary)' }}
                >
                  Новый пароль
                </label>
                <input
                  id="new-password"
                  type="password"
                  autoComplete="new-password"
                  value={password}
                  onChange={(ev) => setPassword(ev.target.value)}
                  placeholder="минимум 8 символов"
                  required
                  minLength={8}
                  disabled={submitting}
                  className="w-full px-3 py-3 rounded border outline-none focus:ring-2"
                  style={inputStyle}
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
                disabled={submitting || code.length !== 6 || password.length < 8}
                className="w-full py-2.5 rounded font-medium transition-opacity disabled:opacity-50"
                style={{ backgroundColor: 'var(--accent)', color: 'var(--accent-text, #fff)' }}
              >
                {submitting ? 'Меняем пароль…' : 'Сменить пароль'}
              </button>
            </form>

            <button
              type="button"
              onClick={handleResend}
              disabled={cooldown > 0 || submitting}
              className="w-full mt-3 py-2 text-sm transition-opacity hover:opacity-70 disabled:opacity-50"
              style={{ color: 'var(--accent)' }}
            >
              {cooldown > 0 ? `Отправить код повторно (${cooldown}с)` : 'Отправить код повторно'}
            </button>
          </>
        )}

        <Link
          to="/login"
          className="flex items-center justify-center gap-1.5 w-full mt-3 py-2 text-sm transition-opacity hover:opacity-70"
          style={{ color: 'var(--text-muted)' }}
        >
          <ArrowLeft size={14} />
          Вернуться ко входу
        </Link>
      </div>
    </div>
  );
}
