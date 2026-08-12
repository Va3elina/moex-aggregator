/**
 * ForgotPasswordPage — восстановление забытого пароля по коду из письма.
 *
 * ⚠️ Оформлена МОДАЛКОЙ поверх сайта, точно как LoginPage, и роут живёт внутри
 * Layout. Первая версия была fullscreen-страницей — и переход «Забыли пароль?»
 * из модалки входа выбрасывал человека на голый экран без шапки. Меняя вёрстку
 * здесь, держите её в паре с LoginPage.
 *
 * Два шага в одном окне: email → код + новый пароль. Отдельного роута под
 * второй шаг нет намеренно: код живёт 30 минут в БД, а не в URL, так что
 * «ссылка из письма» здесь не нужна и лишний адрес только даёт куда потерять
 * состояние.
 *
 * ⚠️ Бэкенд отвечает 204 даже если аккаунта нет (защита от перебора адресов),
 * поэтому шаг 2 показываем ВСЕГДА, а формулировки держим в сослагательном:
 * «если аккаунт существует». Иначе UI выдаст то, что бэкенд прячет.
 *
 * Бэкенд: POST /api/auth/password-reset/{request,confirm}.
 */
import { useState, useEffect, type FormEvent } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { Mail, Lock, KeyRound, AlertCircle, CheckCircle2, ArrowRight, X } from 'lucide-react';
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

  const fieldWrap = 'relative';
  const fieldIcon = 'absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none';
  const fieldInput =
    'w-full pl-10 pr-3 py-2.5 rounded-xl border outline-none focus:ring-2 text-sm transition-colors';
  const fieldStyle = {
    backgroundColor: 'var(--bg-primary)',
    color: 'var(--text-primary)',
    borderColor: 'var(--border-color)',
  };

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center px-4"
      style={{ backgroundColor: 'rgba(0, 0, 0, 0.6)' }}
    >
      {/* Backdrop — клик уводит на главную, как в модалке входа */}
      <div className="absolute inset-0" onClick={() => navigate('/')} />

      <div
        className="relative w-full max-w-md border rounded-2xl p-6 md:p-8 shadow-2xl max-h-[90vh] overflow-y-auto"
        style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}
      >
        <button
          onClick={() => navigate('/')}
          className="absolute top-4 right-4 p-2.5 -m-1 rounded-lg transition-all hover:bg-[color-mix(in_srgb,var(--text-primary)_10%,transparent)]"
          style={{ color: 'var(--text-muted)' }}
          aria-label="Закрыть"
        >
          <X size={20} />
        </button>

        <div className="text-center mb-6">
          <div
            className="w-11 h-11 rounded-full flex items-center justify-center mx-auto mb-3"
            style={{ backgroundColor: 'color-mix(in srgb, var(--accent) 14%, transparent)' }}
          >
            <KeyRound size={20} style={{ color: 'var(--accent)' }} />
          </div>
          <h2 className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>
            {step === 'email' ? 'Восстановление пароля' : 'Новый пароль'}
          </h2>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            {step === 'email'
              ? 'Пришлём код на почту аккаунта'
              : 'Введите код из письма и придумайте пароль'}
          </p>
        </div>

        {step === 'email' ? (
          <form onSubmit={handleRequest} className="space-y-4">
            <div>
              <label
                htmlFor="reset-email"
                className="block text-sm font-medium mb-1.5"
                style={{ color: 'var(--text-primary)' }}
              >
                Email
              </label>
              <div className={fieldWrap}>
                <Mail size={16} className={fieldIcon} style={{ color: 'var(--text-muted)' }} />
                <input
                  id="reset-email"
                  type="email"
                  autoComplete="email"
                  value={email}
                  onChange={(ev) => setEmail(ev.target.value)}
                  placeholder="you@example.com"
                  required
                  autoFocus
                  disabled={submitting}
                  className={fieldInput}
                  style={fieldStyle}
                />
              </div>
            </div>

            {error && (
              <div
                className="flex items-start gap-2 p-3 rounded-xl text-sm"
                style={{ backgroundColor: 'rgba(220, 38, 38, 0.1)', color: 'var(--danger, #dc2626)' }}
              >
                <AlertCircle size={16} className="mt-0.5 flex-shrink-0" />
                <div>{error}</div>
              </div>
            )}

            <button
              type="submit"
              disabled={submitting || !email.trim()}
              className="w-full flex items-center justify-center gap-2 py-2.5 px-4 rounded-xl font-bold text-sm transition-opacity disabled:opacity-50 hover:opacity-90"
              style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
            >
              {submitting ? (
                <div className="w-5 h-5 border-2 border-current border-t-transparent rounded-full animate-spin" />
              ) : (
                <>
                  Прислать код
                  <ArrowRight size={16} />
                </>
              )}
            </button>
          </form>
        ) : (
          <>
            <form onSubmit={handleConfirm} className="space-y-4">
              <div>
                <label
                  htmlFor="reset-code"
                  className="block text-sm font-medium mb-1.5"
                  style={{ color: 'var(--text-primary)' }}
                >
                  Код из письма
                </label>
                <input
                  id="reset-code"
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
                  className="w-full px-3 py-3 rounded-xl border outline-none focus:ring-2 text-center"
                  style={{
                    ...fieldStyle,
                    fontFamily: 'var(--font-mono, monospace)',
                    fontSize: '24px',
                    letterSpacing: '0.4em',
                    fontWeight: 600,
                  }}
                />
              </div>

              <div>
                <label
                  htmlFor="reset-password"
                  className="block text-sm font-medium mb-1.5"
                  style={{ color: 'var(--text-primary)' }}
                >
                  Новый пароль
                </label>
                <div className={fieldWrap}>
                  <Lock size={16} className={fieldIcon} style={{ color: 'var(--text-muted)' }} />
                  <input
                    id="reset-password"
                    type="password"
                    autoComplete="new-password"
                    value={password}
                    onChange={(ev) => setPassword(ev.target.value)}
                    placeholder="минимум 8 символов"
                    required
                    minLength={8}
                    disabled={submitting}
                    className={fieldInput}
                    style={fieldStyle}
                  />
                </div>
              </div>

              {error && (
                <div
                  className="flex items-start gap-2 p-3 rounded-xl text-sm"
                  style={{ backgroundColor: 'rgba(220, 38, 38, 0.1)', color: 'var(--danger, #dc2626)' }}
                >
                  <AlertCircle size={16} className="mt-0.5 flex-shrink-0" />
                  <div>{error}</div>
                </div>
              )}

              {info && (
                <div
                  className="flex items-start gap-2 p-3 rounded-xl text-sm"
                  style={{ backgroundColor: 'rgba(34, 197, 94, 0.1)', color: 'var(--success, #16a34a)' }}
                >
                  <CheckCircle2 size={16} className="mt-0.5 flex-shrink-0" />
                  <div>{info}</div>
                </div>
              )}

              <button
                type="submit"
                disabled={submitting || code.length !== 6 || password.length < 8}
                className="w-full flex items-center justify-center gap-2 py-2.5 px-4 rounded-xl font-bold text-sm transition-opacity disabled:opacity-50 hover:opacity-90"
                style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
              >
                {submitting ? (
                  <div className="w-5 h-5 border-2 border-current border-t-transparent rounded-full animate-spin" />
                ) : (
                  <>
                    Сменить пароль
                    <ArrowRight size={16} />
                  </>
                )}
              </button>
            </form>

            <button
              type="button"
              onClick={handleResend}
              disabled={cooldown > 0 || submitting}
              className="w-full mt-3 py-2 text-sm font-medium transition-opacity hover:opacity-70 disabled:opacity-50"
              style={{ color: 'var(--accent)' }}
            >
              {cooldown > 0 ? `Отправить код повторно (${cooldown}с)` : 'Отправить код повторно'}
            </button>
          </>
        )}

        <div className="mt-5 text-center text-sm" style={{ color: 'var(--text-muted)' }}>
          Вспомнили пароль?{' '}
          <Link to="/login" className="font-medium hover:underline" style={{ color: 'var(--accent)' }}>
            Войти
          </Link>
        </div>
      </div>
    </div>
  );
}
