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
    const [providerName, setProviderName] = useState('');

    useEffect(() => {
        // Определяем провайдера по URL path
        const path = window.location.pathname;
        let provider = '';
        if (path.includes('google')) provider = 'google';
        else if (path.includes('vk')) provider = 'vk';
        else if (path.includes('yandex')) provider = 'yandex';
        else if (path.includes('telegram')) provider = 'telegram';

        const names: Record<string, string> = { google: 'Google', vk: 'VK', yandex: 'Яндекс', telegram: 'Telegram' };
        setProviderName(names[provider] || provider);

        if (!provider) {
            setStatus('error');
            setErrorMsg('Неизвестный провайдер');
            return;
        }

        // Telegram: данные приходят в hash (#tgAuthResult=base64) или query params
        if (provider === 'telegram') {
            let tgData: Record<string, string> = {};

            // Вариант 1: hash fragment (#tgAuthResult=base64json)
            const hash = window.location.hash;
            if (hash.includes('tgAuthResult=')) {
                try {
                    const b64 = hash.split('tgAuthResult=')[1];
                    const json = atob(b64);
                    tgData = JSON.parse(json);
                } catch {
                    // fallback to query params
                }
            }

            // Вариант 2: query params (?id=...&hash=...&auth_date=...)
            if (!tgData.id) {
                for (const [key, value] of searchParams.entries()) {
                    tgData[key] = value;
                }
            }

            if (!tgData.id || !tgData.hash) {
                setStatus('error');
                setErrorMsg('Не удалось получить данные от Telegram. Убедитесь, что вы вошли в Telegram и разрешили доступ.');
                return;
            }

            fetch('/api/auth/oauth/telegram', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(tgData),
            })
                .then(async (resp) => {
                    const data = await resp.json();
                    if (!resp.ok) throw new Error(data.error?.message || data.detail || 'Ошибка авторизации');
                    await auth.login({ access_token: data.access_token, refresh_token: data.refresh_token });
                    setStatus('success');
                    // Telegram всегда без email → synthetic → ведём подтвердить почту
                    const tgSynthetic = (data.user?.email || '').endsWith('@oauth.local');
                    setTimeout(() => navigate(tgSynthetic ? '/add-email' : '/'), 1000);
                })
                .catch((err) => {
                    setStatus('error');
                    setErrorMsg(err.message || 'Ошибка авторизации через Telegram');
                });
            return;
        }

        // Google, VK: данные приходят как code
        const code = searchParams.get('code');
        if (!code) {
            setStatus('error');
            setErrorMsg('Не удалось получить код авторизации');
            return;
        }

        // Дедупликация: Google code одноразовый, нельзя отправить дважды
        const storageKey = `oauth_code_${code.slice(0, 20)}`;
        if (sessionStorage.getItem(storageKey)) {
            if (auth.isAuthenticated) {
                setStatus('success');
                setTimeout(() => navigate('/'), 500);
            }
            return;
        }
        sessionStorage.setItem(storageKey, '1');

        // Собираем payload — VK ID PKCE: code_verifier и device_id.
        // Читаем из localStorage (а не sessionStorage) потому что VK на iPhone
        // открывает auth в новой tab, и sessionStorage между ними isolated.
        // Fallback на sessionStorage — для users со старой версией кода в кэше.
        const payload: Record<string, string> = { code };
        const deviceId = searchParams.get('device_id')
            || localStorage.getItem('vk_device_id')
            || sessionStorage.getItem('vk_device_id')
            || '';
        if (deviceId) payload.device_id = deviceId;
        const codeVerifier = localStorage.getItem('vk_code_verifier')
            || sessionStorage.getItem('vk_code_verifier')
            || '';
        if (codeVerifier) payload.code_verifier = codeVerifier;
        // Очищаем оба storage чтобы не накапливать stale verifier'ы
        localStorage.removeItem('vk_code_verifier');
        localStorage.removeItem('vk_device_id');
        sessionStorage.removeItem('vk_code_verifier');
        sessionStorage.removeItem('vk_device_id');

        // Отправляем code на backend
        fetch(`/api/auth/oauth/${provider}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        })
            .then(async (resp) => {
                const data = await resp.json();

                if (!resp.ok) {
                    throw new Error(data.error?.message || data.detail || 'Ошибка авторизации');
                }

                // Сохраняем токены через AuthContext
                await auth.login({ access_token: data.access_token, refresh_token: data.refresh_token });

                setStatus('success');

                // Synthetic email (VK без email) → /add-email подтвердить почту; иначе на главную
                const synthetic = (data.user?.email || '').endsWith('@oauth.local');
                setTimeout(() => navigate(synthetic ? '/add-email' : '/'), 1000);
            })
            .catch((err) => {
                setStatus('error');
                setErrorMsg(err.message || 'Произошла ошибка');
                // Очищаем флаг чтобы можно было повторить
                sessionStorage.removeItem(storageKey);
            });
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return (
        <div
            className="min-h-screen flex items-center justify-center"
            style={{ backgroundColor: 'var(--bg-primary)' }}
        >
            <div className="text-center">
                {status === 'loading' && (
                    <>
                        <Loader2 className="w-12 h-12 animate-spin mx-auto mb-4" style={{ color: 'var(--accent)' }} />
                        <p className="text-lg" style={{ color: 'var(--text-primary)' }}>
                            {providerName ? `Входим через ${providerName}...` : 'Авторизация...'}
                        </p>
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
