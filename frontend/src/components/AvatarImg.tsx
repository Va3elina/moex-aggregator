/**
 * AvatarImg — рендерит <img> аватарки OAuth-юзера с graceful fallback
 * на текст-инициал при load-failure.
 *
 * Зачем: avatar_url сохраняется в БД при OAuth-signup'е (Telegram/VK/Google
 * CDN URL), но потом юзер может сменить или удалить аватар на стороне
 * провайдера — наш URL станет 404'м. Без onError-обработки браузер показывает
 * сломанный image-icon, плюс «Node cannot be found» warnings от React когда
 * row перерендеривается во время loading.
 *
 * Использование внутри обёрточного div'а (с background-color/border/size):
 *   <div className="...wrapper styles...">
 *     <AvatarImg url={user.avatar_url} fallback={(user.email[0] || '?').toUpperCase()} />
 *   </div>
 */

import { useState, useEffect } from 'react';

interface Props {
    url?: string | null;
    /** Текст-fallback (обычно initial letter email/имени). */
    fallback: string;
}

export default function AvatarImg({ url, fallback }: Props) {
    const [failed, setFailed] = useState(false);

    // Сброс failed-state при смене url — например когда таблица пере-фетчит
    // данные и тот же row получает другую аватарку (или fix'нутый URL).
    useEffect(() => {
        setFailed(false);
    }, [url]);

    if (!url || failed) {
        return <>{fallback}</>;
    }

    return (
        <img
            src={url}
            alt=""
            style={{ width: '100%', height: '100%', objectFit: 'cover' }}
            onError={() => setFailed(true)}
            // Браузер сам не перезапрашивает 404 — onError firing один раз достаточно
            loading="lazy"
        />
    );
}
