import { useEffect, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Loader2, CheckCircle, XCircle } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';

/**
 * OAuth callback handler.
 *
 * После редиректа от провайдера (Google, VK) URL будет:
 *   /auth/callback/google?code=...
 *   /auth/callback/vk?code=...
 *
 * Этот компонент:
 * 1. Читает code из URL
 * 2. Отправляет его на backend
 * 3. Сохраняет токены и редиректит на главную
 */

export default function AuthCallback() {
    const navigate = useNavigate();
    const auth = useAuth();
    const [searchParams] = useSearchParams();
    const [status, setStatus] = useState<'loading' | 'success' | 'error'>('loading');
    const [errorMsg, setErrorMsg] = useState('');

    useEffect(() => {
        const code = searchParams.get('code');

        // Определяем провайдера по URL path
        const path = window.location.pathname;
        let provider = '';
        if (path.includes('google')) provider = 'google';
        else if (path.includes('vk')) provider = 'vk';

        if (!code || !provider) {
            setStatus('error');
            setErrorMsg('Не удалось получить код авторизации');
            return;
        }

        // Отправляем code на backend
        fetch(`/api/auth/oauth/${provider}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code }),
        })
            .then(async (resp) => {
                const data = await resp.json();

                if (!resp.ok) {
                    throw new Error(data.error?.message || data.detail || 'Ошибка авторизации');
                }

                // Сохраняем токены через AuthContext
                await auth.login({ access_token: data.access_token, refresh_token: data.refresh_token });

                setStatus('success');

                // Редирект на главную через секунду
                setTimeout(() => navigate('/'), 1000);
            })
            .catch((err) => {
                setStatus('error');
                setErrorMsg(err.message || 'Произошла ошибка');
            });
    }, [searchParams, navigate, auth]);

    return (
        <div
            className="min-h-screen flex items-center justify-center"
            style={{ backgroundColor: 'var(--bg-primary)' }}
        >
            <div className="text-center">
                {status === 'loading' && (
                    <>
                        <Loader2 className="w-12 h-12 animate-spin mx-auto mb-4" style={{ color: 'var(--accent)' }} />
                        <p className="text-lg" style={{ color: 'var(--text-primary)' }}>Авторизация...</p>
                        <p className="text-sm mt-2" style={{ color: 'var(--text-muted)' }}>Подождите, проверяем данные</p>
                    </>
                )}

                {status === 'success' && (
                    <>
                        <CheckCircle className="w-12 h-12 text-emerald-400 mx-auto mb-4" />
                        <p className="text-lg" style={{ color: 'var(--text-primary)' }}>Успешно!</p>
                        <p className="text-sm mt-2" style={{ color: 'var(--text-muted)' }}>Перенаправляем...</p>
                    </>
                )}

                {status === 'error' && (
                    <>
                        <XCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
                        <p className="text-lg" style={{ color: 'var(--text-primary)' }}>Ошибка авторизации</p>
                        <p className="text-sm mt-2 text-red-400">{errorMsg}</p>
                        <button
                            onClick={() => navigate('/login')}
                            className="mt-4 px-6 py-2 rounded-xl transition-colors text-sm"
                            style={{
                                backgroundColor: 'color-mix(in srgb, var(--text-primary) 10%, transparent)',
                                color: 'var(--text-primary)',
                            }}
                        >
                            Попробовать снова
                        </button>
                    </>
                )}
            </div>
        </div>
    );
}
