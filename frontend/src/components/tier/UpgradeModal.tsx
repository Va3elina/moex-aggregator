/**
 * UpgradeModal — модалка «Доступно на тарифе X», открывается при попытке
 * взаимодействия с заблокированной фичей.
 *
 * Контролируется через хук useUpgradePrompt():
 *
 *   const { showUpgrade } = useUpgradePrompt();
 *   onClick={() => showUpgrade({ tier: 'basic', featureName: 'актив CC' })}
 */
import { createContext, useCallback, useContext, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';

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
        desc: 'Всё из Basic + API, экспорт CSV, TradingView, индикаторы Т-терминала, безлимитные алерты.',
    },
};


export function UpgradePromptProvider({ children }: { children: ReactNode }) {
    const [prompt, setPrompt] = useState<UpgradePromptProps | null>(null);

    const showUpgrade = useCallback((props: UpgradePromptProps) => {
        setPrompt(props);
    }, []);

    const close = useCallback(() => setPrompt(null), []);

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

    return (
        <div
            onClick={onClose}
            style={{
                position: 'fixed',
                inset: 0,
                background: 'rgba(0,0,0,0.5)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                zIndex: 9999,
            }}
        >
            <div
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: 'var(--bg-base, #fff)',
                    color: 'var(--text-primary, #1a1a1a)',
                    borderRadius: 16,
                    padding: 32,
                    maxWidth: 480,
                    width: '90%',
                    border: '1px solid var(--border, #ddd)',
                    boxShadow: '0 20px 60px rgba(0,0,0,0.3)',
                }}
            >
                <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
                    <span style={{ fontSize: 32 }}>🔒</span>
                    <h2 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>
                        Доступно на тарифе {label.ru}
                    </h2>
                </div>

                {featureName && (
                    <p style={{ color: 'var(--text-secondary, #666)', marginBottom: 12, fontSize: 15 }}>
                        Чтобы использовать «{featureName}», нужен тариф {label.ru} или выше.
                    </p>
                )}

                <div
                    style={{
                        background: 'var(--bg-secondary, #f5f1e8)',
                        borderRadius: 10,
                        padding: 16,
                        marginBottom: 20,
                    }}
                >
                    <div style={{ fontWeight: 700, fontSize: 18, color: 'var(--accent, #FF5C2B)', marginBottom: 4 }}>
                        {label.price}
                    </div>
                    <div style={{ color: 'var(--text-secondary, #666)', fontSize: 14, lineHeight: 1.5 }}>
                        {label.desc}
                    </div>
                </div>

                <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
                    <button
                        onClick={onClose}
                        style={{
                            padding: '10px 20px',
                            borderRadius: 8,
                            border: '1px solid var(--border, #ddd)',
                            background: 'transparent',
                            color: 'var(--text-primary, #1a1a1a)',
                            cursor: 'pointer',
                            fontSize: 14,
                            fontWeight: 500,
                        }}
                    >
                        Закрыть
                    </button>
                    <Link
                        to="/pricing"
                        onClick={onClose}
                        style={{
                            padding: '10px 20px',
                            borderRadius: 8,
                            background: 'var(--accent, #FF5C2B)',
                            color: 'white',
                            textDecoration: 'none',
                            fontSize: 14,
                            fontWeight: 600,
                            display: 'inline-flex',
                            alignItems: 'center',
                        }}
                    >
                        Перейти на {label.ru} →
                    </Link>
                </div>
            </div>
        </div>
    );
}
