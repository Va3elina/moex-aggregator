import { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Mail, Lock, Eye, EyeOff, ArrowRight, X } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { safeInternalPath, setPostLoginNext } from '../utils/postLoginRedirect';
import { apiErrorFromBody } from '../services/api';

// SVG иконки провайдеров — официальные стили (Yandex 2021 rebrand, VK ID 2021)
const VKIcon = () => (
    <svg viewBox="0 0 24 24" width="20" height="20" aria-label="ВКонтакте">
        <rect width="24" height="24" rx="5" fill="#0077FF" />
        <path
            d="M12.79 16.5c-4.74 0-7.45-3.25-7.56-8.66h2.37c.08 3.97 1.82 5.65 3.2 5.99V7.84h2.24v3.42c1.36-.15 2.79-1.71 3.28-3.42h2.24c-.37 2.11-1.93 3.67-3.04 4.31 1.11.53 2.88 1.89 3.55 4.35h-2.46c-.54-1.66-1.85-2.94-3.57-3.11v3.11h-.25z"
            fill="#fff"
        />
    </svg>
);

const YandexIcon = () => (
    <svg viewBox="0 0 24 24" width="20" height="20" aria-label="Яндекс">
        <circle cx="12" cy="12" r="12" fill="#FC3F1D" />
        <path
            d="M13.27 18.4h2.04V5.6h-2.96c-2.98 0-4.55 1.53-4.55 3.79 0 1.8.86 2.86 2.4 3.96L7.55 18.4h2.2l2.95-5.2-1.04-.7c-1.25-.85-1.86-1.51-1.86-2.92 0-1.24.87-2.08 2.51-2.08h.96V18.4z"
            fill="#fff"
        />
    </svg>
);

const TelegramIcon = () => (
    <svg viewBox="0 0 24 24" width="20" height="20" fill="#26A5E4" aria-label="Telegram">
        <path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.479.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z" />
    </svg>
);

interface OAuthProvider {
    id: string;
    name: string;
    configured: boolean;
}

const MIN_PASSWORD_LENGTH = 8;

/**
 * Чего не хватает паролю. Зеркалит check_password_strength
 * (api/security/password.py): единственное обязательное требование — длина.
 * Регистры, цифры и спецсимволы там влияют только на score, отказом не служат.
 */
function passwordProblems(pwd: string): string[] {
    if (pwd.length < MIN_PASSWORD_LENGTH) return [`Минимум ${MIN_PASSWORD_LENGTH} символов`];
    return [];
}

const PASSWORD_HINT = `Минимум ${MIN_PASSWORD_LENGTH} символов`;

/**
 * Тело ответа как JSON, но без падения на не-JSON. Отбойники стоят ДО
 * приложения (лимитер nginx на зоне auth, 502/504 при рестарте контейнера) и
 * отдают HTML-страницу — resp.json() на ней бросал «Unexpected token '<'»
 * прямо в лицо пользователю вместо человеческого текста.
 */
interface AuthTokens {
    access_token: string;
    refresh_token: string;
}

async function readJsonSafe(resp: Response): Promise<Partial<AuthTokens> | null> {
    try {
        return JSON.parse(await resp.text());
    } catch {
        return null;
    }
}

/** Токены из ответа. Ответ без них — сломанный, ловим здесь, а не падением ниже. */
function requireTokens(body: Partial<AuthTokens> | null): AuthTokens {
    if (!body?.access_token || !body?.refresh_token) {
        throw new Error('Сервис вернул неожиданный ответ, попробуйте ещё раз');
    }
    return { access_token: body.access_token, refresh_token: body.refresh_token };
}

/** Осмысленный текст по одному лишь статусу — когда тела нет или оно не JSON. */
function statusFallback(status: number): string {
    if (status === 429) return 'Слишком много попыток. Подождите минуту и попробуйте снова.';
    if (status === 502 || status === 503 || status === 504) return 'Сервис недоступен, попробуйте через минуту';
    return 'Ошибка';
}

export default function LoginPage() {
    const navigate = useNavigate();
    const auth = useAuth();
    const [searchParams] = useSearchParams();
    // Куда вернуть после входа (?next=). Валидируем (только внутренние пути).
    const urlNext = safeInternalPath(searchParams.get('next'));
    // ?mode=register — открыть сразу форму регистрации. Нужно тем, кто приходит
    // по инвайт-ссылке: у них аккаунта нет, и форма входа их только тормозит.
    const [mode, setMode] = useState<'login' | 'register'>(
        searchParams.get('mode') === 'register' ? 'register' : 'login',
    );

    // Если уже залогинен — редирект на next (или главную)
    useEffect(() => {
        if (auth.isAuthenticated) navigate(urlNext || '/');
    }, [auth.isAuthenticated, navigate, urlNext]);

    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [showPassword, setShowPassword] = useState(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [providers, setProviders] = useState<OAuthProvider[]>([]);
    // Согласие на обработку персональных данных (ФЗ-152). Active consent —
    // не pre-checked, submit заблокирован пока юзер не согласится.
    const [agreedToTerms, setAgreedToTerms] = useState(false);

    // Загрузка списка провайдеров
    useEffect(() => {
        fetch('/api/auth/oauth/providers')
            .then(r => r.json())
            .then(data => setProviders(data.providers || []))
            .catch(() => { });
    }, []);

    // Обработка email+password
    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setError('');
        setLoading(true);

        try {
            // Проверяем пароль ДО запроса: бэкенд отвечает на слабый пароль 422,
            // а у 422 текст всегда «Ошибка валидации данных» — по нему нельзя
            // понять, чего не хватает. Правила те же, что в
            // check_password_strength (api/security/password.py).
            if (mode === 'register') {
                const problems = passwordProblems(password);
                if (problems.length > 0) {
                    throw new Error(`Пароль не подходит: ${problems.join(', ').toLowerCase()}`);
                }
            }

            const url = mode === 'login' ? '/api/auth/login' : '/api/auth/register';
            const body = { email, password };

            const resp = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });

            const data = await readJsonSafe(resp);

            if (!resp.ok) {
                throw new Error(apiErrorFromBody(data, statusFallback(resp.status)));
            }

            if (mode === 'register') {
                // /register возвращает UserResponse без токенов — логинимся отдельно,
                // затем ведём на next (если пришли откуда-то по ссылке) либо на
                // подтверждение email (код уже отправлен письмом при регистрации).
                const loginResp = await fetch('/api/auth/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                });
                const loginData = await readJsonSafe(loginResp);
                if (!loginResp.ok) {
                    // Аккаунт уже создан — что бы ни случилось со входом, это не
                    // повод пугать «ошибкой регистрации». Частый случай: пара
                    // register+login летит в один лимитер nginx (зона auth) и
                    // второй запрос ловит 429.
                    setMode('login');   // форма уже готова к входу, вводить заново не надо
                    throw new Error(loginResp.status === 429
                        ? 'Аккаунт создан. Слишком много попыток подряд — подождите минуту и войдите.'
                        : 'Аккаунт создан. Войдите, чтобы продолжить.');
                }
                await auth.login(requireTokens(loginData));
                // Раньше здесь безусловно стоял '/verify-email' — и next терялся:
                // человек, пришедший по инвайт-ссылке, регистрировался и оставался
                // без подписки. Подтвердить почту он сможет из баннера в профиле.
                navigate(urlNext || '/verify-email');
                return;
            }

            // Сохраняем токены через AuthContext (login mode)
            await auth.login(requireTokens(data));
            navigate(urlNext || '/');
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Произошла ошибка');
        } finally {
            setLoading(false);
        }
    };

    // Обработка OAuth
    const handleOAuth = async (providerId: string) => {
        // next переживёт round-trip провайдера через localStorage → AuthCallback.
        setPostLoginNext(urlNext);
        if (providerId === 'telegram') {
            // Прямой редирект на Telegram OAuth (без popup-виджета)
            const botId = '8604817597';
            const origin = encodeURIComponent(window.location.origin);
            const returnTo = encodeURIComponent(window.location.origin + '/auth/callback/telegram');
            window.location.href = `https://oauth.telegram.org/auth?bot_id=${botId}&origin=${origin}&embed=0&request_access=write&return_to=${returnTo}`;
            return;
        }

        // VK, Yandex — стандартный redirect
        try {
            const resp = await fetch(`/api/auth/oauth/${providerId}/url`);
            const data = await resp.json();

            if (resp.ok && data.url) {
                // VK ID PKCE: сохраняем code_verifier и device_id для callback.
                // localStorage (а не sessionStorage) — потому что на iPhone Safari
                // VK ID open auth в НОВОЙ tab/window, а sessionStorage НЕ shared
                // между tabs (даже same-origin). Verifier терялся → 'verifier is
                // missing or invalid' при обмене кода на токен.
                if (data.code_verifier) {
                    localStorage.setItem('vk_code_verifier', data.code_verifier);
                }
                if (data.device_id) {
                    localStorage.setItem('vk_device_id', data.device_id);
                }
                // Yandex CSRF: сохраняем выданный state. На callback сверим с тем,
                // что Yandex вернёт в redirect — привязка к этому браузеру (защита
                // от login-CSRF). localStorage, а не sessionStorage — на случай
                // открытия auth в новой вкладке (как у VK).
                if (data.state) {
                    localStorage.setItem('yandex_oauth_state', data.state);
                }
                window.location.href = data.url;
            } else {
                setError(data.detail || 'OAuth не настроен');
            }
        } catch {
            setError('OAuth не настроен. Ключи будут добавлены позже.');
        }
    };

    const providerIcons: Record<string, React.ReactNode> = {
        vk: <VKIcon />,
        yandex: <YandexIcon />,
        telegram: <TelegramIcon />,
    };

    const providerColors: Record<string, string> = {
        vk: 'hover:border-[#0077FF]/50 hover:bg-[#0077FF]/10',
        yandex: 'hover:border-[#FC3F1D]/50 hover:bg-[#FC3F1D]/10',
        telegram: 'hover:border-[#26A5E4]/50 hover:bg-[#26A5E4]/10',
    };

    const handleClose = () => {
        navigate(urlNext || '/');
    };

    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center px-4"
            style={{ backgroundColor: 'rgba(0, 0, 0, 0.6)' }}
        >
            {/* Backdrop — клик закрывает */}
            <div className="absolute inset-0" onClick={handleClose} />

            {/* Модальное окно */}
            <div
                className="relative w-full max-w-md border rounded-2xl p-6 md:p-8 shadow-2xl max-h-[90vh] overflow-y-auto"
                style={{
                    backgroundColor: 'var(--bg-secondary)',
                    borderColor: 'var(--border-color)',
                }}
            >
                {/* Кнопка закрытия */}
                <button
                    onClick={handleClose}
                    className="absolute top-4 right-4 p-2.5 -m-1 rounded-lg transition-all hover:bg-[color-mix(in_srgb,var(--text-primary)_10%,transparent)]"
                    style={{ color: 'var(--text-muted)' }}
                >
                    <X size={20} />
                </button>

                {/* Заголовок */}
                <div className="text-center mb-6">
                    <h2 className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>
                        {mode === 'login' ? 'Войти в аккаунт' : 'Создать аккаунт'}
                    </h2>
                    <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
                        {mode === 'login' ? 'Добро пожаловать в Фрейм' : 'Регистрация в Фрейм'}
                    </p>
                </div>

                {/* OAuth buttons.
                    Telegram скрыт в режиме регистрации: новых юзеров через Telegram
                    больше не заводим (бэкенд отвечает 403 на новый oauth_id). В режиме
                    входа кнопка остаётся — уже привязанные Telegram-аккаунты логинятся. */}
                <div className="space-y-2.5 mb-5">
                    {providers
                        .filter(p => !(p.id === 'telegram' && mode === 'register'))
                        .map(p => (
                        <button
                            key={p.id}
                            onClick={() => handleOAuth(p.id)}
                            className={`relative w-full flex items-center justify-center gap-2 py-2.5 px-4 rounded-xl border font-medium transition-all duration-200 ${providerColors[p.id] || ''} ${!p.configured ? 'opacity-50' : ''}`}
                            style={{
                                borderColor: 'var(--border-color)',
                                color: 'var(--text-primary)',
                                backgroundColor: 'color-mix(in srgb, var(--text-primary) 5%, transparent)',
                            }}
                            disabled={!p.configured}
                        >
                            {providerIcons[p.id]}
                            <span className="text-sm">
                                {mode === 'login' ? 'Войти' : 'Регистрация'} через {p.name}
                            </span>
                            {!p.configured && (
                                <span className="absolute right-4 top-1/2 -translate-y-1/2 text-xs" style={{ color: 'var(--text-muted)' }}>скоро</span>
                            )}
                        </button>
                    ))}
                </div>

                {/* Divider */}
                <div className="flex items-center gap-4 mb-5">
                    <div className="flex-1 h-px" style={{ backgroundColor: 'var(--border-color)' }} />
                    <span className="text-xs" style={{ color: 'var(--text-muted)' }}>или по email</span>
                    <div className="flex-1 h-px" style={{ backgroundColor: 'var(--border-color)' }} />
                </div>

                {/* Error */}
                {error && (
                    <div className="mb-4 p-3 rounded-xl text-sm"
                        style={{
                            backgroundColor: 'color-mix(in srgb, var(--danger) 15%, transparent)',
                            color: 'var(--danger)',
                            border: '1px solid color-mix(in srgb, var(--danger) 30%, transparent)',
                        }}
                    >
                        {error}
                    </div>
                )}

                {/* Form */}
                <form onSubmit={handleSubmit} className="space-y-3.5">
                    {/* Email */}
                    <div>
                        <label className="block text-sm font-medium mb-1.5" style={{ color: 'var(--text-secondary)' }}>Email</label>
                        <div className="relative">
                            <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4" style={{ color: 'var(--text-muted)' }} />
                            <input
                                type="email"
                                value={email}
                                onChange={e => setEmail(e.target.value)}
                                placeholder="you@example.com"
                                required
                                className="w-full pl-10 pr-4 py-2.5 rounded-xl border outline-none transition-all text-sm"
                                style={{
                                    backgroundColor: 'color-mix(in srgb, var(--text-primary) 5%, transparent)',
                                    borderColor: 'var(--border-color)',
                                    color: 'var(--text-primary)',
                                }}
                            />
                        </div>
                    </div>

                    {/* Password */}
                    <div>
                        <label className="block text-sm font-medium mb-1.5" style={{ color: 'var(--text-secondary)' }}>Пароль</label>
                        <div className="relative">
                            <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4" style={{ color: 'var(--text-muted)' }} />
                            <input
                                type={showPassword ? 'text' : 'password'}
                                value={password}
                                onChange={e => setPassword(e.target.value)}
                                placeholder="••••••••"
                                required
                                minLength={mode === 'register' ? 8 : 1}
                                className="w-full pl-10 pr-11 py-2.5 rounded-xl border outline-none transition-all text-sm"
                                style={{
                                    backgroundColor: 'color-mix(in srgb, var(--text-primary) 5%, transparent)',
                                    borderColor: 'var(--border-color)',
                                    color: 'var(--text-primary)',
                                }}
                            />
                            <button
                                type="button"
                                onClick={() => setShowPassword(!showPassword)}
                                className="absolute right-0.5 top-1/2 -translate-y-1/2 p-3.5 transition-colors hover:opacity-80"
                                style={{ color: 'var(--text-muted)' }}
                            >
                                {showPassword ? <EyeOff size={16} /> : <Eye size={16} />}
                            </button>
                        </div>
                        {/* Требования показываем заранее: раньше о них узнавали
                            только по отказу с бэкенда, причём без текста. */}
                        {mode === 'register' && (
                            <div className="text-xs mt-1.5" style={{ color: 'var(--text-muted)' }}>
                                {PASSWORD_HINT}
                            </div>
                        )}
                    </div>

                    {/* Consent checkbox — только при регистрации.
                        ФЗ-152: active consent (не pre-checked) + явная ссылка
                        на политику обработки персональных данных. */}
                    {mode === 'register' && (
                        <label
                            className="flex items-start gap-2 cursor-pointer select-none"
                            style={{ color: 'var(--text-secondary)' }}
                        >
                            <input
                                type="checkbox"
                                checked={agreedToTerms}
                                onChange={(e) => setAgreedToTerms(e.target.checked)}
                                required
                                style={{
                                    width: 16,
                                    height: 16,
                                    marginTop: 2,
                                    accentColor: 'var(--accent)',
                                    flexShrink: 0,
                                    cursor: 'pointer',
                                }}
                            />
                            <span className="text-xs leading-relaxed">
                                Я даю согласие на обработку персональных данных в соответствии с{' '}
                                <a
                                    href="/privacy"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    style={{ color: 'var(--accent)', textDecoration: 'underline' }}
                                >
                                    Политикой конфиденциальности
                                </a>
                            </span>
                        </label>
                    )}

                    {/* Submit */}
                    <button
                        type="submit"
                        disabled={loading || (mode === 'register' && !agreedToTerms)}
                        className="w-full flex items-center justify-center gap-2 py-2.5 px-4 rounded-xl font-bold text-sm transition-opacity disabled:opacity-50 disabled:cursor-not-allowed hover:opacity-90"
                        style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}
                    >
                        {loading ? (
                            <div className="w-5 h-5 border-2 border-current border-t-transparent rounded-full animate-spin" />
                        ) : (
                            <>
                                {mode === 'login' ? 'Войти' : 'Зарегистрироваться'}
                                <ArrowRight size={16} />
                            </>
                        )}
                    </button>
                </form>

                {/* Toggle mode */}
                <div className="mt-5 text-center text-sm" style={{ color: 'var(--text-muted)' }}>
                    {mode === 'login' ? (
                        <>
                            Нет аккаунта?{' '}
                            <button
                                onClick={() => { setMode('register'); setError(''); }}
                                className="font-medium hover:underline"
                                style={{ color: 'var(--accent)' }}
                            >
                                Зарегистрироваться
                            </button>
                        </>
                    ) : (
                        <>
                            Уже есть аккаунт?{' '}
                            <button
                                onClick={() => { setMode('login'); setError(''); }}
                                className="font-medium hover:underline"
                                style={{ color: 'var(--accent)' }}
                            >
                                Войти
                            </button>
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}
