/**
 * UpgradeModal — окно апселла, открывается при попытке взаимодействия с
 * заблокированной фичей.
 *
 * Контролируется через хук useUpgradePrompt():
 *
 *   const { showUpgrade } = useUpgradePrompt();
 *   onClick={() => showUpgrade({ tier: 'basic', featureName: 'актив CC', indicator: 'open_interest' })}
 *
 * Вид окна (2026-11, по образцу TradingView): сверху короткая анимация самой
 * функции (PreviewScene), ниже — что это, одна фраза о пользе, спокойное
 * сравнение «ваш тариф ✕ → нужный ✓» без цены и кнопка-решение. Сцену и тексты
 * подбирает resolvePreview() по indicator + featureName, поэтому места вызова
 * менять не нужно — достаточно передавать indicator и featureName.
 */
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Check, Pause, Play, X } from 'lucide-react';
import { useAuth } from '../../contexts/AuthContext';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { MODAL_LAYER_Z } from '../../utils/modalHost';
import { pricingUrlFrom } from '../../utils/purchaseSource';
import { useAnalytics } from '../../contexts/AnalyticsContext';
import TrialOfferCard from './TrialOfferCard';
import PreviewScene from './PreviewScene';
import { resolvePreview, type FeaturePreviewKey } from './featurePreviews';
import { useTranslation } from 'react-i18next';


interface UpgradePromptProps {
    tier: 'basic' | 'pro';
    featureName?: string;        // "5-минутный таймфрейм", "актив CC", etc.
    indicator?: string;          // 'open_interest', 'seasonality', etc. — по нему выбирается сцена
    /** Явная сцена, когда по indicator её не угадать (Терминал — не индикатор). */
    preview?: FeaturePreviewKey;
}

interface UpgradeContextValue {
    showUpgrade: (props: UpgradePromptProps) => void;
}

const UpgradeContext = createContext<UpgradeContextValue | null>(null);

const TIER_LABEL: Record<UpgradePromptProps['tier'], string> = { basic: 'Basic', pro: 'Pro' };


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


/**
 * Закрытие — крестик на превью, клик по фону, Esc (отдельной кнопки «Закрыть» нет).
 */
function UpgradeDialog({ tier, featureName, indicator, preview, onClose }:
    UpgradePromptProps & { onClose: () => void }) {
    const { t } = useTranslation();
    const { track } = useAnalytics();
    const width = useViewportWidth();
    const isMobile = width < 768;
    const [paused, setPaused] = useState(false);
    const { user } = useAuth();
    // Что именно упёрлось в замок: название функции понятнее ключа индикатора.
    const feature = featureName || indicator || preview || null;
    const cfg = useMemo(
        () => resolvePreview({ featureName, indicator, preview }, t),
        [featureName, indicator, preview, t],
    );
    const currentLabel = !user ? t('Без входа')
        : user.role === 'basic' ? 'Basic'
        : user.role === 'pro' || user.role === 'admin' ? 'Pro'
        : 'Free';

    // Показ окна — знаменатель конверсии «показали → перешли → купили».
    useEffect(() => {
        track('paywall_show', { tier, feature, indicator, preview: cfg.spec.kind });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [onClose]);

    const heroBtn = {
        position: 'absolute' as const,
        width: 32, height: 32, borderRadius: 8,
        border: 'none', cursor: 'pointer',
        background: 'rgba(18,20,25,0.55)',
        color: '#fff',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        backdropFilter: 'blur(4px)',
    };

    // Mobile = bottom-sheet (slide up, 100% width, rounded top). Desktop = centered modal.
    return (
        <div
            onClick={onClose}
            style={{
                position: 'fixed', inset: 0,
                background: 'rgba(0,0,0,0.5)',
                display: 'flex',
                alignItems: isMobile ? 'flex-end' : 'center',
                justifyContent: 'center',
                // Выше ВСЕХ модалок-пикеров: апселл открывается ИЗ них по
                // попытке действия и обязан лечь поверх (пикеры «Сделок фондов»
                // и поиск активов живут на MODAL_LAYER_Z).
                zIndex: MODAL_LAYER_Z + 1,
                animation: 'fadeIn 0.2s ease-out',
            }}
        >
            <div
                role="dialog"
                aria-modal="true"
                aria-label={cfg.title}
                onClick={(e) => e.stopPropagation()}
                style={{
                    background: 'var(--bg-secondary, #fff)',
                    color: 'var(--text-primary, #1a1a1a)',
                    borderRadius: isMobile ? '20px 20px 0 0' : 18,
                    padding: isMobile ? 12 : 14,
                    maxWidth: isMobile ? '100%' : 520,
                    width: isMobile ? '100%' : '92%',
                    border: '1px solid var(--border-color, #ddd)',
                    boxShadow: '0 24px 70px rgba(0,0,0,0.32)',
                    paddingBottom: isMobile ? 'max(16px, env(safe-area-inset-bottom))' : 22,
                    maxHeight: '92vh',
                    overflowY: 'auto',
                }}
            >
                {/* Превью: подложка — мягкое свечение акцента, «экран» уходит за край. */}
                <div
                    className={paused ? 'fp-paused' : undefined}
                    style={{
                        position: 'relative',
                        borderRadius: 12,
                        overflow: 'hidden',
                        background: 'radial-gradient(110% 130% at 0% 0%, color-mix(in srgb, var(--accent, #FF5C2B) 55%, transparent) 0%, transparent 62%), radial-gradient(120% 140% at 100% 100%, rgba(107,92,255,0.45) 0%, transparent 65%), var(--bg-tertiary, #f5f1e8)',
                    }}
                >
                    <PreviewScene spec={cfg.spec} />
                    <button type="button" onClick={onClose} aria-label={t('Закрыть')}
                        style={{ ...heroBtn, top: 10, right: 10 }}>
                        <X size={18} strokeWidth={2.4} />
                    </button>
                    <button type="button" onClick={() => setPaused((p) => !p)}
                        aria-label={paused ? t('Продолжить') : t('Пауза')}
                        style={{ ...heroBtn, bottom: 10, right: 10, width: 28, height: 28 }}>
                        {paused ? <Play size={14} /> : <Pause size={14} />}
                    </button>
                </div>

                <div style={{ padding: isMobile ? '18px 8px 0' : '22px 12px 0' }}>
                    <h2 style={{ fontSize: 'var(--fs-xl)', fontWeight: 700, margin: '0 0 8px', lineHeight: 1.25 }}>
                        {cfg.title}
                    </h2>
                    <p style={{ color: 'var(--text-secondary, #666)', fontSize: 'var(--fs-base)', lineHeight: 1.5, margin: '0 0 16px' }}>
                        {cfg.text}
                    </p>

                    {/* Сравнение «сейчас ✕ → нужен ✓», как у TradingView: статус
                        юзера серой плашкой, нужный тариф — обводкой акцента. Без цены. */}
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 18 }}>
                        <div style={{
                            background: 'var(--bg-tertiary, #f2f2f2)',
                            borderRadius: 12, padding: '12px 14px',
                            display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8,
                        }}>
                            <div style={{ minWidth: 0 }}>
                                <div style={{ color: 'var(--text-muted, #999)', fontSize: 'var(--fs-xs)', marginBottom: 2 }}>{t('Ваш тариф')}</div>
                                <div style={{ fontWeight: 700, fontSize: 'var(--fs-base)' }}>{currentLabel}</div>
                            </div>
                            <X size={20} strokeWidth={2.2} color="var(--text-muted, #999)" style={{ flexShrink: 0 }} />
                        </div>
                        <div style={{
                            border: '1.5px solid var(--accent, #FF5C2B)',
                            borderRadius: 12, padding: '12px 14px',
                            display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8,
                        }}>
                            <div style={{ minWidth: 0 }}>
                                <div style={{ color: 'var(--text-muted, #999)', fontSize: 'var(--fs-xs)', marginBottom: 2 }}>{t('Нужен тариф')}</div>
                                <div style={{ fontWeight: 700, fontSize: 'var(--fs-base)' }}>
                                    {tier === 'basic' ? t('Basic или Pro') : TIER_LABEL[tier]}
                                </div>
                            </div>
                            <Check size={20} strokeWidth={2.6} color="var(--accent, #FF5C2B)" style={{ flexShrink: 0 }} />
                        </div>
                    </div>

                    {/* Trial-оффер: TrialOfferCard сам гейтит по trial_eligible. */}
                    <TrialOfferCard lockedTier={tier} feature={feature} />

                    <Link
                        to={pricingUrlFrom('paywall', feature)}
                        onClick={onClose}
                        className="editorial-press"
                        style={{
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            width: '100%', minHeight: 52,
                            borderRadius: 12,
                            background: 'var(--accent, #FF5C2B)',
                            color: '#fff',
                            textDecoration: 'none',
                            fontSize: 'var(--fs-base)', fontWeight: 700,
                            padding: '0 16px', textAlign: 'center',
                        }}
                    >
                        {cfg.cta}
                    </Link>
                </div>
            </div>
        </div>
    );
}
