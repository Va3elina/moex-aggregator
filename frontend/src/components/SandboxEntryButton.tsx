/**
 * SandboxEntryButton — вход в песочницу (роут /sandbox, вне навигации).
 *
 * Песочница уже живёт на сервере, но попасть в неё можно было только по прямому
 * URL. Этот компонент — единственная точка входа, две формы:
 *
 *   variant="pill" — шапка сайта (Layout, правая зона). Глиф + подпись
 *     «Терминал». На узких экранах (<xl) подпись прячется и остаётся кружок,
 *     ровно как у кнопки «Войти» рядом.
 *   variant="icon" — панель контролов страницы индикатора. Кружок 32px только
 *     с глифом, закреплён крайним справа (marginLeft:auto внутри flex-строки).
 *
 * Стиль общий с остальным правым кластером шапки: обводка 1.5px text-primary,
 * прозрачный фон, акцентный глиф. Заливки нет — единственное залитое пятно
 * в шапке остаётся за Plus.
 */
import { useNavigate } from 'react-router-dom';
import FrameLogo from './FrameLogo';

interface SandboxEntryButtonProps {
    /** Форма кнопки. По умолчанию кружок с глифом. */
    variant?: 'pill' | 'icon';
}

const TITLE = 'Терминал: рабочий стол с индикаторами';

export default function SandboxEntryButton({ variant = 'icon' }: SandboxEntryButtonProps) {
    const navigate = useNavigate();

    if (variant === 'pill') {
        return (
            <button
                type="button"
                onClick={() => navigate('/sandbox')}
                title={TITLE}
                aria-label={TITLE}
                // Размеры — классами, НЕ инлайном: инлайн-style бьёт любой
                // xl:-класс по специфичности, и подпись не раскрывалась бы
                // (кнопка оставалась кружком на любой ширине).
                className="editorial-press grid place-items-center rounded-full w-[clamp(22px,1.6vw+0.3rem,32px)] h-[clamp(22px,1.6vw+0.3rem,32px)] xl:w-auto xl:h-8 xl:flex xl:items-center xl:gap-2 xl:pl-2.5 xl:pr-3.5 text-xs font-bold"
                style={{
                    color: 'var(--text-primary)',
                    border: '1.5px solid var(--text-primary)',
                    backgroundColor: 'transparent',
                }}
            >
                <FrameLogo size={16} showWordmark={false} color="var(--accent)" />
                <span className="hidden xl:inline">Терминал</span>
            </button>
        );
    }

    return (
        <button
            type="button"
            onClick={() => navigate('/sandbox')}
            title={TITLE}
            aria-label={TITLE}
            className="editorial-press grid place-items-center rounded-full flex-shrink-0"
            style={{
                color: 'var(--accent)',
                border: '1.5px solid var(--text-primary)',
                backgroundColor: 'transparent',
                width: 32,
                height: 32,
            }}
        >
            <FrameLogo size={16} showWordmark={false} />
        </button>
    );
}
