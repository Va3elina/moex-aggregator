import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Mail, Lock, Eye, EyeOff, ArrowRight, X } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';

// SVG иконки провайдеров
const GoogleIcon = () => (
    <svg viewBox="0 0 24 24" width="20" height="20">
        <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z" fill="#4285F4" />
        <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853" />
        <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05" />
        <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335" />
    </svg>
);

const VKIcon = () => (
    <svg viewBox="0 0 24 24" width="20" height="20" fill="#0077FF">
        <path d="M21.545 17.073c-.092-.18-.176-.34-.256-.484-.396-.708-.886-1.377-1.465-1.993l-.055-.058c-.403-.408-.664-.687-.783-.831-.216-.27-.265-.54-.145-.811.088-.187.423-.69 1.004-1.51.316-.443.569-.793.752-1.047 1.266-1.768 1.826-2.91 1.671-3.417l-.065-.12c-.047-.07-.186-.134-.417-.194a4.17 4.17 0 0 0-.66-.047l-2.927.02c-.063-.012-.167.003-.313.046l-.19.073-.075.04-.073.061c-.065.06-.14.15-.217.27a11.21 11.21 0 0 1-.69 1.215c-.176.265-.342.5-.498.707-.157.206-.303.375-.44.504-.135.127-.245.192-.33.192-.037 0-.09-.024-.16-.072a.65.65 0 0 1-.175-.2 1.082 1.082 0 0 1-.13-.35 3.04 3.04 0 0 1-.06-.516l.003-.56c.006-.35.01-.727.01-1.127a12.35 12.35 0 0 0-.047-.982 3.06 3.06 0 0 0-.16-.678.844.844 0 0 0-.363-.4l-.146-.08a1.965 1.965 0 0 0-.805-.122l-3.143.02c-.25 0-.47.04-.656.118a.796.796 0 0 0-.38.295l-.035.063c-.093.148-.05.222.133.222.35.06.594.218.731.465l.025.077c.09.216.145.442.17.677.06.637.05 1.242-.025 1.816-.075.573-.131.983-.164 1.228-.033.244-.075.442-.127.594-.052.152-.072.225-.06.217-.108.175-.227.258-.356.247-.086-.006-.186-.067-.298-.183a3.24 3.24 0 0 1-.356-.45 12.5 12.5 0 0 1-.415-.685c-.144-.251-.3-.56-.464-.925l-.12-.253-.09-.214-.145-.333-.12-.328-.093-.314a.694.694 0 0 0-.093-.183.488.488 0 0 0-.218-.145l-.16-.068a.86.86 0 0 0-.28-.035l-2.792.02a.885.885 0 0 0-.558.136l-.05.06c-.082.11-.098.243-.047.395l.017.058c.823 1.826 1.723 3.457 2.7 4.891.194.277.372.52.534.73.162.21.36.433.595.666.235.234.49.449.763.644.273.196.607.358 1.002.488.395.13.816.186 1.264.164h.988c.195-.013.37-.074.52-.183l.042-.04a.647.647 0 0 0 .138-.248c.036-.12.05-.254.044-.399a4.236 4.236 0 0 1 .01-.54 4.03 4.03 0 0 1 .079-.506 2.35 2.35 0 0 1 .16-.475.56.56 0 0 1 .246-.27.264.264 0 0 1 .114-.02c.126 0 .28.067.465.192.184.126.36.275.528.45.168.173.35.37.548.592.197.221.37.41.519.567l.147.164c.155.172.392.343.713.512.32.17.6.236.836.2l2.526-.037c.238 0 .43-.028.574-.082a.456.456 0 0 0 .282-.222z" />
    </svg>
);

const YandexIcon = () => (
    <svg viewBox="0 0 24 24" width="20" height="20">
        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1.75 15h-1.72V11.6L9.8 17H7.97l2.5-5.02L8.5 7h1.82l2.05 4.34V7h1.38v10z" fill="#FC3F1D" />
    </svg>
);

const TelegramIcon = () => (
    <svg viewBox="0 0 24 24" width="20" height="20" fill="#26A5E4">
        <path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.479.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z" />
    </svg>
);

interface OAuthProvider {
    id: string;
    name: string;
    configured: boolean;
}

export default function LoginPage() {
    const navigate = useNavigate();
    const auth = useAuth();
    const [mode, setMode] = useState<'login' | 'register'>('login');

    // Если уже залогинен — редирект на главную
    useEffect(() => {
        if (auth.isAuthenticated) navigate('/');
    }, [auth.isAuthenticated, navigate]);

    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [showPassword, setShowPassword] = useState(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [providers, setProviders] = useState<OAuthProvider[]>([]);

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
            const url = mode === 'login' ? '/api/auth/login' : '/api/auth/register';
            const body = { email, password };

            const resp = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });

            const data = await resp.json();

            if (!resp.ok) {
                throw new Error(data.error?.message || data.detail || 'Ошибка');
            }

            // Сохраняем токены через AuthContext
            await auth.login({ access_token: data.access_token, refresh_token: data.refresh_token });
            navigate('/');
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Произошла ошибка');
        } finally {
            setLoading(false);
        }
    };

    // Обработка OAuth
    const handleOAuth = async (providerId: string) => {
        if (providerId === 'telegram') {
            // Прямой редирект на Telegram OAuth (без popup-виджета)
            const botId = '8604817597';
            const origin = encodeURIComponent(window.location.origin);
            const returnTo = encodeURIComponent(window.location.origin + '/auth/callback/telegram');
            window.location.href = `https://oauth.telegram.org/auth?bot_id=${botId}&origin=${origin}&embed=0&request_access=write&return_to=${returnTo}`;
            return;
        }

        // Google, VK, Yandex — стандартный redirect
        try {
            const resp = await fetch(`/api/auth/oauth/${providerId}/url`);
            const data = await resp.json();

            if (resp.ok && data.url) {
                // VK ID PKCE: сохраняем code_verifier и device_id для callback
                if (data.code_verifier) {
                    sessionStorage.setItem('vk_code_verifier', data.code_verifier);
                }
                if (data.device_id) {
                    sessionStorage.setItem('vk_device_id', data.device_id);
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
        google: <GoogleIcon />,
        vk: <VKIcon />,
        yandex: <YandexIcon />,
        telegram: <TelegramIcon />,
    };

    const providerColors: Record<string, string> = {
        google: 'hover:border-[#4285F4]/50 hover:bg-[#4285F4]/10',
        vk: 'hover:border-[#0077FF]/50 hover:bg-[#0077FF]/10',
        yandex: 'hover:border-[#FC3F1D]/50 hover:bg-[#FC3F1D]/10',
        telegram: 'hover:border-[#26A5E4]/50 hover:bg-[#26A5E4]/10',
    };

    const handleClose = () => {
        navigate('/');
    };

    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center px-4"
            style={{ backgroundColor: 'rgba(0, 0, 0, 0.6)' }}
        >
            {/* Backdrop — клик закрывает */}
            <div className="absolute inset-0" onClick={handleClose} />

            {/* Модальное окно */}
            <div
                className="relative w-full max-w-md backdrop-blur-2xl border rounded-2xl p-6 md:p-8 shadow-2xl max-h-[90vh] overflow-y-auto"
                style={{
                    backgroundColor: 'var(--glass-bg)',
                    borderColor: 'var(--border-color)',
                }}
            >
                {/* Кнопка закрытия */}
                <button
                    onClick={handleClose}
                    className="absolute top-4 right-4 p-1.5 rounded-lg transition-all hover:bg-white/10"
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

                {/* OAuth buttons */}
                <div className="space-y-2.5 mb-5">
                    {providers.map(p => (
                        <button
                            key={p.id}
                            onClick={() => handleOAuth(p.id)}
                            className={`w-full flex items-center justify-center gap-3 py-2.5 px-4 rounded-xl border font-medium transition-all duration-200 ${providerColors[p.id] || ''} ${!p.configured ? 'opacity-50' : ''}`}
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
                                <span className="text-xs ml-auto" style={{ color: 'var(--text-muted)' }}>скоро</span>
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
                                className="absolute right-3 top-1/2 -translate-y-1/2 transition-colors hover:opacity-80"
                                style={{ color: 'var(--text-muted)' }}
                            >
                                {showPassword ? <EyeOff size={16} /> : <Eye size={16} />}
                            </button>
                        </div>
                    </div>

                    {/* Submit */}
                    <button
                        type="submit"
                        disabled={loading}
                        className="w-full flex items-center justify-center gap-2 py-2.5 px-4 rounded-xl font-bold text-sm transition-opacity disabled:opacity-50 hover:opacity-90"
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
