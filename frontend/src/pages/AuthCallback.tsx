import { useEffect, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Loader2, CheckCircle, XCircle } from 'lucide-react';

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

                // Сохраняем токены
                localStorage.setItem('access_token', data.access_token);
                localStorage.setItem('refresh_token', data.refresh_token);

                setStatus('success');

                // Редирект на главную через секунду
                setTimeout(() => navigate('/'), 1000);
            })
            .catch((err) => {
                setStatus('error');
                setErrorMsg(err.message || 'Произошла ошибка');
            });
    }, [searchParams, navigate]);

    return (
        <div
            className="min-h-screen flex items-center justify-center"
            style={{
                background: 'radial-gradient(ellipse at top, #1a1f2e 0%, #0f1117 50%, #0a0c10 100%)',
            }}
        >
            <div className="text-center">
                {status === 'loading' && (
                    <>
                        <Loader2 className="w-12 h-12 text-[#C8FF2E] animate-spin mx-auto mb-4" />
                        <p className="text-white text-lg">Авторизация...</p>
                        <p className="text-gray-500 text-sm mt-2">Подождите, проверяем данные</p>
                    </>
                )}

                {status === 'success' && (
                    <>
                        <CheckCircle className="w-12 h-12 text-[#22c55e] mx-auto mb-4" />
                        <p className="text-white text-lg">Успешно!</p>
                        <p className="text-gray-500 text-sm mt-2">Перенаправляем...</p>
                    </>
                )}

                {status === 'error' && (
                    <>
                        <XCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
                        <p className="text-white text-lg">Ошибка авторизации</p>
                        <p className="text-red-400 text-sm mt-2">{errorMsg}</p>
                        <button
                            onClick={() => navigate('/login')}
                            className="mt-4 px-6 py-2 rounded-xl bg-white/10 text-white hover:bg-white/20 transition-colors"
                        >
                            Попробовать снова
                        </button>
                    </>
                )}
            </div>
        </div>
    );
}
