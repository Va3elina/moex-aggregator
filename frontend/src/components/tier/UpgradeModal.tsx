/**
 * UpgradeModal — модалка «Доступно на тарифе X», открывается при попытке
 * взаимодействия с заблокированной фичей.
 *
 * Контролируется через хук useUpgradePrompt():
 *
 *   const { showUpgrade } = useUpgradePrompt();
 *   onClick={() => showUpgrade({ tier: 'basic', featureName: 'актив CC' })}
 */
import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Lock } from 'lucide-react';
import { API_CSV_ENABLED } from '../../config/features';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import TrialOfferCard from './TrialOfferCard';


interface UpgradePromptProps {
    tier: 'basic' | 'pro';
    featureName?: string;        // "5-минутный таймфрейм", "актив CC", etc.
    indicator?: string;          // 'open_interest', 'seasonality', etc. — для контекста
}

interface UpgradeContextValue {
    showUpgrade: (props: UpgradePromptProps) => void;
}

const UpgradeContext = createContext<UpgradeContextValue | null>(null);


const TIER_LABELS: Record<string, { ru: string; price: string; desc: string }> = {
    basic: {
        ru: 'Basic',
        price: '2 900 ₽/мес',
        desc: 'Realtime данные, все инструменты, расширенная история (10 лет).',
    },
    pro: {
        ru: 'Pro',
        price: '5 900 ₽/мес',
        // API + экспорт CSV скрыты до запуска (см. config/features.ts) — не упоминаем в тексте замочка
        desc: API_CSV_ENABLED
            ? 'Всё из Basic + API, экспорт CSV, TradingView, индикаторы Т-терминала, безлимитные алерты.'
            : 'Всё из Basic + TradingView, индикаторы Т-терминала, безлимитные алерты.',
    },
};


export function UpgradePromptProvider({ children }: { children: ReactNode }) {
    const [prompt, setPrompt] = useState<UpgradePromptProps | null>(null);
    const location = useLocation();

    const showUpgrade = useCallback((props: UpgradePromptProps) => {
        setPrompt(props);
    }, []);

    const close = useCallback(() => setPrompt(null), []);

    // Закрывать модалку при смене роута — иначе при переходе из неё (напр.
    // «Зарегистрироваться и попробовать» → /login) она висела бы поверх страницы.
    useEffect(() => {
        setPrompt(null);
    }, [location.pathname]);

    return (
        <UpgradeContext.Provider value={{ showUpgrade }}>
            {children}
            {prompt && <UpgradeDialog {...prompt} onClose={close} />}
        </UpgradeContext.Provider>
    );
}


export function useUpgradePrompt(): UpgradeContextValue {
    const ctx = useContext(UpgradeContext);
    if (!ctx) {
        // Fallback — никогда не должен сработать если provider в App.tsx
        return { showUpgrade: () => console.warn('UpgradeContext missing') };
    }
    return ctx;
}


function UpgradeDialog({ tier, featureName, onClose }: UpgradePromptProps & { onClose: () => void }) {
    const label = TIER_LABELS[tier];
    const width = useViewportWidth();
    const isMobile = width < 768;

    // Mobile = bottom-sheet (slide up, 100% width, rounded top).
    // Desktop = centered modal.
    return (
        <div
            onClick={onClose}
            style={{
                position: 'fixed',
                inset: 0,
                background: 'rgba(0,0,0,0.5)',
                display: 'flex',
                alignItems: isMobile ? 'flex-end' : 'center',
                justifyContent: 'center',
                zIndex: 9999,
                animation: 'fadeIn 0.2s ease-out',
            }}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    // bg-secondary — panel/card в editorial: белый light / тёмный dark.
                    // text-primary автоматически contrast: чёрный light / cream dark.
                    background: 'var(--bg-secondary, #fff)',
                    color: 'var(--text-primary, #1a1a1a)',
                    borderRadius: isMobile ? '20px 20px 0 0' : 16,
                    padding: isMobile ? 24 : 32,
                    maxWidth: isMobile ? '100%' : 480,
                    width: isMobile ? '100%' : '90%',
                    border: '1px solid var(--border-color, #ddd)',
                    boxShadow: '0 20px 60px rgba(0,0,0,0.3)',
                    paddingBottom: isMobile ? 'max(24px, env(safe-area-inset-bottom))' : 32,
                    maxHeight: '90vh',
                    overflowY: 'auto',
                }}
            >
                {/* Mobile drag handle */}
                {isMobile && (
                    <div
                        style={{
                            width: 40, height: 4, borderRadius: 2,
                            background: 'var(--text-muted, #ddd)',
                            opacity: 0.4,
                            margin: '0 auto 16px',
                        }}
                    />
                )}

                <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 16 }}>
                    {/* Editorial-стиль: solid accent square с outline + hard shadow
                        + белая иконка. Тот же визуал что у .page-header-icon в
                        editorial-light/dark, но через inline styles (модалка
                        живёт в portal — class может не сматчиться). */}
                    <div
                        style={{
                            width: isMobile ? 44 : 48,
                            height: isMobile ? 44 : 48,
                            borderRadius: 12,
                            background: 'var(--accent, #FF5C2B)',
                            border: '2px solid var(--text-primary, #0A0A0A)',
                            boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary, #0A0A0A))',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            flexShrink: 0,
                        }}
                    >
                        <Lock
                            size={isMobile ? 22 : 26}
                            strokeWidth={2.4}
                            color="#FFFFFF"
                        />
                    </div>
                    <h2 style={{
                        fontSize: 'var(--fs-xl)',
                        fontWeight: 700, margin: 0,
                    }}>
                        Доступно на тарифе {label.ru}
                    </h2>
                </div>

                {featureName && (
                    <p style={{
                        color: 'var(--text-secondary, #666)',
                        marginBottom: 12,
                        fontSize: 'var(--fs-base)',
                    }}>
                        Чтобы использовать «{featureName}», нужен тариф {label.ru} или выше.
                    </p>
                )}

                {/* Trial-оффер: TrialOfferCard сам гейтит по trial_eligible. */}
                <TrialOfferCard lockedTier={tier} />

                <div
                    style={{
                        // Inner panel — отличается от main bg, в editorial
                        // light/dark получает соответствующий контраст.
                        background: 'var(--bg-tertiary, #f5f1e8)',
                        border: '1px solid var(--border-color, #e5e5e5)',
                        borderRadius: 10,
                        padding: 16,
                        marginBottom: 20,
                    }}
                >
                    <div style={{
                        fontWeight: 700, fontSize: 'var(--fs-xl)',
                        color: 'var(--accent, #FF5C2B)', marginBottom: 4,
                    }}>
                        {label.price}
                    </div>
                    <div style={{
                        color: 'var(--text-secondary, #666)',
                        fontSize: 'var(--fs-base)', lineHeight: 1.5,
                    }}>
                        {label.desc}
                    </div>
                </div>

                <div style={{
                    display: 'flex',
                    flexDirection: isMobile ? 'column-reverse' : 'row',
                    gap: 12,
                    justifyContent: 'flex-end',
                }}>
                    <button
                        onClick={onClose}
                        className="editorial-press"
                        style={{
                            padding: '12px 20px',
                            borderRadius: 8,
                            border: '1px solid var(--border-color, #ddd)',
                            background: 'transparent',
                            color: 'var(--text-primary, #1a1a1a)',
                            cursor: 'pointer',
                            fontSize: 'var(--fs-base)',
                            fontWeight: 600,
                            minHeight: 44,  // mobile touch target
                        }}
                    >
                        Закрыть
                    </button>
                    <Link
                        to="/pricing"
                        onClick={onClose}
                        className="editorial-press"
                        style={{
                            padding: '12px 20px',
                            borderRadius: 8,
                            background: 'var(--accent, #FF5C2B)',
                            color: 'white',
                            textDecoration: 'none',
                            fontSize: 'var(--fs-base)',
                            fontWeight: 600,
                            display: 'inline-flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            minHeight: 44,
                        }}
                    >
                        Перейти на {label.ru} →
                    </Link>
                </div>
            </div>
        </div>
    );
}
