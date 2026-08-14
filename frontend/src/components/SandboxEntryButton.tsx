/**
 * SandboxEntryButton — вход в песочницу (роут /sandbox, вне навигации).
 *
 * Песочница уже живёт на сервере, но попасть в неё можно было только по прямому
 * URL. Этот компонент — единственная точка входа, две формы. Клик открывает
 * Терминал НОВОЙ ВКЛАДКОЙ (см. openTerminalWindow), сайт остаётся на месте:
 *
 *   variant="pill" — шапка сайта (Layout, правая зона). Глиф + подпись
 *     «Терминал». Подпись раскрывается только с 2xl: на xl список индикаторов
 *     (nav, overflow-hidden) обрезался ровно под кнопкой и это читалось так,
 *     будто кнопка наезжает на список. До 2xl остаётся кружок, ровно как
 *     у кнопки «Войти» рядом.
 *   variant="icon" — панель контролов страницы индикатора. Кружок 40px только
 *     с глифом, закреплён крайним справа (marginLeft:auto внутри flex-строки).
 *     40px — это размер соседей по строке: дропдауны среза 40, плитки ТФ 37,
 *     kebab «⋮» над графиком 40. Кружок меньше 40 читался бы как второстепенная
 *     служебная иконка.
 *
 * Гейт: песочница — фича тарифа Pro. Не-Pro кнопку ВИДИТ (это витрина, а не
 * секрет) в полном контрасте, но по клику вместо перехода открывается модалка
 * «Доступно на тарифе Pro» (UpgradeModal, общий для сайта механизм). Замочка
 * на кнопке нет — про тариф говорит title и сама модалка.
 *
 * Стиль общий с остальным правым кластером шапки: обводка 1.5px text-primary,
 * прозрачный фон, акцентный глиф. Заливки нет — единственное залитое пятно
 * в шапке остаётся за Plus.
 */
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
/** Глиф внутри кружка. 20px терялся в 40px круге — заметный воздух по краям. */
const GLYPH_SIZE = 26;

/**
 * Имя вкладки Терминала. Повторный клик не плодит вкладки, а переиспользует
 * уже открытую.
 */
const WINDOW_NAME = 'frame-terminal';

/**
 * Терминал открывается новой вкладкой текущего окна, а не в этой же: это
 * рабочий стол, его держат рядом с сайтом, а не вместо него. Отдельным окном
 * (popup=…) не открываем намеренно — браузер выносит его из окна пользователя,
 * и это ощущается чужеродно. Без третьего аргумента window.open даёт именно
 * вкладку.
 */
function openTerminalWindow() {
    const win = window.open('/sandbox', WINDOW_NAME);
    win?.focus();
}

export default function SandboxEntryButton({ variant = 'icon' }: SandboxEntryButtonProps) {
    const { user } = useAuth();
    const { showUpgrade } = useUpgradePrompt();

    // Тир берём из роли пользователя (как ExtensionTokenSection): матрица
    // features.py про песочницу ничего не знает — это не индикатор.
    const isPro = user?.role === 'pro' || user?.role === 'admin';
    const title = isPro ? TITLE_OPEN : TITLE_LOCKED;

    const onClick = () => {
        if (isPro) {
            openTerminalWindow();
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
                // Якорь анонс-тура «Новое: Терминал» (data/tours/terminal-intro).
                data-tour="header-terminal"
                // Размеры — классами, НЕ инлайном: инлайн-style бьёт любой
                // xl:-класс по специфичности, и подпись не раскрывалась бы
                // (кнопка оставалась кружком на любой ширине).
                className="editorial-press grid place-items-center rounded-full w-[clamp(22px,1.6vw+0.3rem,32px)] h-[clamp(22px,1.6vw+0.3rem,32px)] 2xl:w-auto 2xl:h-8 2xl:flex 2xl:items-center 2xl:gap-2 2xl:pl-2.5 2xl:pr-3.5 text-xs font-bold"
                style={{
                    color: 'var(--text-primary)',
                    border: '1.5px solid var(--text-primary)',
                    backgroundColor: 'transparent',
                }}
            >
                <FrameLogo size={16} showWordmark={false} color="var(--accent)" />
                <span className="hidden 2xl:inline">Терминал</span>
            </button>
        );
    }

    return (
        <button
            type="button"
            onClick={onClick}
            title={title}
            aria-label={title}
            // Якорь анонс-тура: шаг «И прямо с графика» (data/tours/terminal-intro).
            data-tour="chart-terminal"
            className="editorial-press grid place-items-center rounded-full flex-shrink-0"
            style={{
                color: 'var(--accent)',
                border: '1.5px solid var(--text-primary)',
                backgroundColor: 'transparent',
                width: ICON_SIZE,
                height: ICON_SIZE,
            }}
        >
            <FrameLogo size={GLYPH_SIZE} showWordmark={false} />
        </button>
    );
}
