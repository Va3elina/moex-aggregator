/**
 * SandboxEntryButton — вход в песочницу (роут /sandbox, вне навигации).
 *
 * Песочница уже живёт на сервере, но попасть в неё можно было только по прямому
 * URL. Этот компонент — единственная точка входа, две формы:
 *
 *   variant="pill" — шапка сайта (Layout, правая зона). Глиф + подпись
 *     «Терминал». На узких экранах (<xl) подпись прячется и остаётся кружок,
 *     ровно как у кнопки «Войти» рядом.
 *   variant="icon" — панель контролов страницы индикатора. Кружок 40px только
 *     с глифом, закреплён крайним справа (marginLeft:auto внутри flex-строки).
 *     40px — это размер соседей по строке: дропдауны среза 40, плитки ТФ 37,
 *     kebab «⋮» над графиком 40. Кружок меньше 40 читался бы как второстепенная
 *     служебная иконка.
 *
 * Гейт: песочница — фича тарифа Pro. Не-Pro кнопку ВИДИТ (это витрина, а не
 * секрет) в полном контрасте, но по клику вместо перехода открывается модалка
 * «Доступно на тарифе Pro» (UpgradeModal, общий для сайта механизм). Замочек
 * подмешан к глифу, чтобы клик не выглядел обманом.
 *
 * Стиль общий с остальным правым кластером шапки: обводка 1.5px text-primary,
 * прозрачный фон, акцентный глиф. Заливки нет — единственное залитое пятно
 * в шапке остаётся за Plus.
 */
import { useNavigate } from 'react-router-dom';
import { Lock } from 'lucide-react';
import FrameLogo from './FrameLogo';
import { useAuth } from '../contexts/AuthContext';
import { useUpgradePrompt } from './tier/UpgradeModal';

interface SandboxEntryButtonProps {
    /** Форма кнопки. По умолчанию кружок с глифом. */
    variant?: 'pill' | 'icon';
}

const TITLE_OPEN = 'Терминал: рабочий стол с индикаторами';
const TITLE_LOCKED = 'Терминал: рабочий стол с индикаторами — тариф Pro';

/** Размер кружка на панели контролов индикатора — в размер соседей по строке. */
const ICON_SIZE = 40;

export default function SandboxEntryButton({ variant = 'icon' }: SandboxEntryButtonProps) {
    const navigate = useNavigate();
    const { user } = useAuth();
    const { showUpgrade } = useUpgradePrompt();

    // Тир берём из роли пользователя (как ExtensionTokenSection): матрица
    // features.py про песочницу ничего не знает — это не индикатор.
    const isPro = user?.role === 'pro' || user?.role === 'admin';
    const title = isPro ? TITLE_OPEN : TITLE_LOCKED;

    const onClick = () => {
        if (isPro) {
            navigate('/sandbox');
            return;
        }
        showUpgrade({ tier: 'pro', featureName: 'Терминал — рабочий стол с индикаторами' });
    };

    if (variant === 'pill') {
        return (
            <button
                type="button"
                onClick={onClick}
                title={title}
                aria-label={title}
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
                {!isPro && <Lock className="hidden xl:inline" size={12} style={{ color: 'var(--text-secondary)' }} />}
            </button>
        );
    }

    return (
        <button
            type="button"
            onClick={onClick}
            title={title}
            aria-label={title}
            className="editorial-press grid place-items-center rounded-full flex-shrink-0"
            style={{
                position: 'relative',
                color: 'var(--accent)',
                border: '1.5px solid var(--text-primary)',
                backgroundColor: 'transparent',
                width: ICON_SIZE,
                height: ICON_SIZE,
            }}
        >
            <FrameLogo size={20} showWordmark={false} />
            {!isPro && (
                // Замочек в углу — не перекрывает глиф, но снимает вопрос
                // «почему клик не открыл». Обводка фоном рамки, чтобы значок
                // читался поверх бордера кнопки.
                <span
                    className="grid place-items-center rounded-full"
                    style={{
                        position: 'absolute',
                        right: -3,
                        bottom: -3,
                        width: 16,
                        height: 16,
                        backgroundColor: 'var(--bg-secondary)',
                        border: '1.5px solid var(--text-primary)',
                        color: 'var(--text-primary)',
                    }}
                >
                    <Lock size={8} strokeWidth={2.5} />
                </span>
            )}
        </button>
    );
}
