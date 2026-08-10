/**
 * TierLock — универсальный wrapper для затемнения недоступных по tier'у UI-элементов.
 *
 * Использование:
 *
 *   // Asset (например в InstrumentSearchModal):
 *   <TierLock indicator="open_interest" check={{ asset: "CC" }} featureName="актив CC">
 *     <button>CC — Какао</button>
 *   </TierLock>
 *
 *   // Interval (5min):
 *   <TierLock indicator="open_interest" check={{ interval: 5 }} featureName="5-минутный таймфрейм">
 *     <button>5min</button>
 *   </TierLock>
 *
 *   // Mode (cap-m2 в Buffett):
 *   <TierLock indicator="buffett" check={{ mode: "cap-m2" }} featureName="Капитализация / M2">
 *     <button>Cap/M2</button>
 *   </TierLock>
 *
 *   // Universe (strength):
 *   <TierLock indicator="strength" check={{ universe: "all" }} featureName="вселенная Все акции">
 *     <button>Все акции</button>
 *   </TierLock>
 *
 * Что делает:
 *   - Если у tier'а юзера есть доступ → render children как есть (passthrough)
 *   - Если нет → overlay с lock icon (без затемнения), click → showUpgrade('basic'|'pro')
 *
 * Хорошо работает с любыми buttons/divs/cards. Wrapper не меняет layout —
 * pointer-events перенаправляются на overlay.
 */
import { Lock } from 'lucide-react';
import { useTierAccess, type IndicatorLimits } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from './UpgradeModal';
import type { ReactNode } from 'react';


export interface TierLockProps {
    indicator: string;
    check: {
        asset?: string;
        interval?: number;
        timeframe?: string;
        mode?: string;
        universe?: string;
        period?: string;
        flag?: keyof IndicatorLimits;
    };
    featureName?: string;       // отображается в UpgradeModal: "5-минутный таймфрейм", "актив CC"
    children: ReactNode;
    showLockIcon?: boolean;     // показывать ли иконку поверх (default true)
    iconSize?: number;          // px (default 16)
    style?: React.CSSProperties; // доп. стили для wrapper
    className?: string;
    /** Если true — overlay click не перехватывается, dropdown items могут пропустить.
     *  Полезно для случаев когда parent уже handles click (вариант 'passthrough'). */
    passthroughClick?: boolean;
}


export function TierLock({
    indicator,
    check,
    featureName,
    children,
    showLockIcon = true,
    iconSize = 16,
    style,
    className,
    passthroughClick = false,
}: TierLockProps) {
    const access = useTierAccess(indicator);
    const { showUpgrade } = useUpgradePrompt();

    // Loading state — показываем как есть пока матрица не загружена
    if (access.isLoading) {
        return <>{children}</>;
    }

    // Проверка доступности по типу check
    const accessible =
        check.asset != null ? access.canAccessAsset(check.asset) :
        check.interval != null ? access.canUseInterval(check.interval) :
        check.timeframe != null ? access.canUseTimeframe(check.timeframe) :
        check.mode != null ? access.canUseMode(check.mode) :
        check.universe != null ? access.canUseUniverse(check.universe) :
        check.period != null ? access.canUsePeriod(check.period) :
        check.flag != null ? access.canUseFlag(check.flag) :
        true;

    if (accessible) {
        return <>{children}</>;
    }

    const requiredTier = access.requiredTierFor(check);

    const handleClick = (e: React.MouseEvent) => {
        if (passthroughClick) return;
        e.preventDefault();
        e.stopPropagation();
        if (requiredTier) {
            showUpgrade({ tier: requiredTier, featureName, indicator });
        }
    };

    return (
        <div
            className={className}
            style={{
                position: 'relative',
                display: 'inline-block',
                ...style,
            }}
            title={requiredTier
                ? `Доступно на тарифе ${requiredTier === 'basic' ? 'Basic' : 'Pro'}`
                : undefined}
        >
            {/* Children без затемнения (2026-08-10): элемент выглядит обычным,
                платность выдаёт только замочек; клик по оверлею → апселл.
                Затемнение отпугивало от клика, а клик и есть путь к апгрейду. */}
            <div style={{
                pointerEvents: 'none',
                userSelect: 'none',
            }}>
                {children}
            </div>

            {/* Overlay для перехвата клика + lock icon */}
            <div
                onClick={handleClick}
                style={{
                    position: 'absolute',
                    inset: 0,
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'flex-end',
                    paddingRight: showLockIcon ? 6 : 0,
                    pointerEvents: 'auto',
                    zIndex: 1,
                }}
            >
                {showLockIcon && (
                    <Lock
                        size={iconSize}
                        strokeWidth={2.2}
                        color="var(--text-secondary, #6b6359)"
                        style={{
                            filter: 'drop-shadow(0 1px 2px rgba(0,0,0,0.2))',
                            background: 'rgba(255,255,255,0.85)',
                            borderRadius: '50%',
                            padding: 2,
                        }}
                    />
                )}
            </div>
        </div>
    );
}


/**
 * useTierLockClick — для случаев когда не можем wrap'ить (например, native <select>),
 * возвращает onClick handler который интерсептит и открывает upgrade prompt.
 *
 *   const lockClick = useTierLockClick('open_interest', { interval: 5 });
 *   <select onChange={(e) => {
 *     if (lockClick.shouldBlock(e.target.value)) return lockClick.handleBlock();
 *     // ...
 *   }}>
 */
export function useTierLockClick(indicator: string) {
    const access = useTierAccess(indicator);
    const { showUpgrade } = useUpgradePrompt();

    return {
        /** Проверить заблокирован ли. */
        check: (params: TierLockProps['check']) => {
            const accessible =
                params.asset != null ? access.canAccessAsset(params.asset) :
                params.interval != null ? access.canUseInterval(params.interval) :
                params.mode != null ? access.canUseMode(params.mode) :
                params.universe != null ? access.canUseUniverse(params.universe) :
                params.period != null ? access.canUsePeriod(params.period) :
                params.flag != null ? access.canUseFlag(params.flag) :
                true;
            if (accessible) return null;
            return access.requiredTierFor(params);
        },
        /** Открыть upgrade prompt. */
        prompt: (params: TierLockProps['check'] & { featureName?: string }) => {
            const required = access.requiredTierFor(params);
            if (required) {
                showUpgrade({ tier: required, featureName: params.featureName, indicator });
            }
        },
    };
}
