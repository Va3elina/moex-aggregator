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
import { Link } from 'react-router-dom';
import { Lock } from 'lucide-react';
import { API_CSV_ENABLED } from '../../config/features';
import { useViewportWidth } from '../../hooks/useViewportWidth';
import { apiFetch } from '../../services/api';

// Длительность пробного периода по тарифам (зеркало backend plans.TRIAL_DAYS).
const TRIAL_DAYS_BY_TIER: Record<string, number> = { basic: 14, pro: 7 };
// «Показать оффер триала 1 раз на браузер» (жёсткий барьер — backend trial_eligible).
const TRIAL_OFFER_SEEN_LS = 'frame_trial_offer_seen_v1';

function fmtRub(n: number | undefined): string {
    return n == null ? '' : n.toLocaleString('ru-RU') + ' ₽';
}

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


/**
 * TrialOffer — блок «Попробуйте бесплатно N дней» внутри модалки замка.
 * Рендерится ТОЛЬКО если backend сказал trial_eligible=true И оффер ещё не
 * показывали в этом браузере (показ 1 раз). Цена/период тянутся из API
 * (не хардкод — требование 54-ФЗ/ЗоЗПП). Гейт по trial_eligible означает: пока
 * бэкенд триала не задеплоен (поле отсутствует) — блок не показывается вообще.
 */
function TrialOffer({ tier, isMobile }: { tier: 'basic' | 'pro'; isMobile: boolean }) {
    const [eligible, setEligible] = useState<boolean | null>(null);
    const [prices, setPrices] = useState<{ monthly?: number; yearly?: number }>({});
    const [period, setPeriod] = useState<'monthly' | 'yearly' | null>(null);
    const [consent, setConsent] = useState(false);
    const [submitting, setSubmitting] = useState(false);
    const [err, setErr] = useState<string | null>(null);

    const days = TRIAL_DAYS_BY_TIER[tier] ?? 7;
    const tierRu = tier === 'pro' ? 'Pro' : 'Basic';

    useEffect(() => {
        // Уже показывали в этом браузере → не навязываем повторно (1 раз на юзера).
        if (typeof window !== 'undefined' && localStorage.getItem(TRIAL_OFFER_SEEN_LS) === '1') {
            setEligible(false);
            return;
        }
        let cancelled = false;
        (async () => {
            try {
                const [sRes, pRes] = await Promise.all([
                    apiFetch('/api/billing/status'),
                    fetch('/api/billing/plans'),
                ]);
                if (cancelled) return;
                const s = sRes.ok ? await sRes.json() : {};
                const p = pRes.ok ? await pRes.json() : {};
                const elig = s.trial_eligible === true;
                setEligible(elig);
                if (elig) {
                    const t = (p.tiers || []).find((x: { tier: string }) => x.tier === tier) as
                        | { monthly?: { amount: number }; yearly?: { amount: number } }
                        | undefined;
                    setPrices({ monthly: t?.monthly?.amount, yearly: t?.yearly?.amount });
                    localStorage.setItem(TRIAL_OFFER_SEEN_LS, '1');  // показ один раз
                }
            } catch {
                if (!cancelled) setEligible(false);
            }
        })();
        return () => { cancelled = true; };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    if (eligible !== true) return null;

    const price = period ? prices[period] : undefined;
    const chargeDate = new Date(Date.now() + days * 86400000)
        .toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });

    async function startTrial() {
        if (!period || !consent || submitting) return;
        setSubmitting(true);
        setErr(null);
        try {
            const r = await apiFetch('/api/billing/trial/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tier, period, consent: true }),
            });
            const b = await r.json().catch(() => ({}));
            if (!r.ok || !b.payment_url) {
                throw new Error(b.detail || b.error?.message || 'Не удалось начать пробный период');
            }
            window.location.href = b.payment_url;  // → T-Bank AddCard форма
        } catch (e) {
            setErr(e instanceof Error ? e.message : 'Ошибка');
            setSubmitting(false);
        }
    }

    const periodBtn = (p: 'monthly' | 'yearly', labelTop: string) => (
        <button
            type="button"
            onClick={() => setPeriod(p)}
            className="editorial-press"
            style={{
                flex: 1,
                padding: '10px 12px',
                borderRadius: 8,
                border: `2px solid ${period === p ? 'var(--accent, #FF5C2B)' : 'var(--border-color, #ddd)'}`,
                background: period === p ? 'var(--accent, #FF5C2B)' : 'transparent',
                color: period === p ? '#fff' : 'var(--text-primary, #1a1a1a)',
                cursor: 'pointer',
                fontWeight: 600,
                fontSize: 'var(--fs-sm)',
                minHeight: 44,
                lineHeight: 1.3,
            }}
        >
            {labelTop}<br />
            <span style={{ fontSize: 'var(--fs-base)', fontWeight: 700 }}>
                {p === 'monthly' ? fmtRub(prices.monthly) + '/мес' : fmtRub(prices.yearly) + '/год'}
            </span>
        </button>
    );

    return (
        <div
            style={{
                background: 'var(--bg-tertiary, #f5f1e8)',
                border: '2px solid var(--accent, #FF5C2B)',
                borderRadius: 10,
                padding: 16,
                marginBottom: 20,
            }}
        >
            <div style={{ fontWeight: 700, fontSize: 'var(--fs-lg)', marginBottom: 4 }}>
                🎁 Попробуйте {tierRu} бесплатно — {days} дней
            </div>
            <div style={{ color: 'var(--text-secondary, #666)', fontSize: 'var(--fs-sm)', marginBottom: 12 }}>
                Карта не спишется сейчас. Выберите план — спишем только по окончании, если не отмените.
            </div>

            <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
                {periodBtn('monthly', 'Помесячно')}
                {periodBtn('yearly', 'За год')}
            </div>

            {period && (
                <div style={{
                    fontSize: 'var(--fs-sm)', color: 'var(--text-secondary, #666)',
                    marginBottom: 12, lineHeight: 1.5,
                }}>
                    Бесплатно {days} дней, затем <b>{fmtRub(price)}</b> за {tierRu}{' '}
                    ({period === 'monthly' ? 'в месяц' : 'в год'}). Первое списание <b>{chargeDate}</b>.
                    Отменить и отвязать карту можно в любой момент до этой даты.
                </div>
            )}

            <label style={{
                display: 'flex', gap: 8, alignItems: 'flex-start',
                fontSize: 'var(--fs-sm)', color: 'var(--text-secondary, #666)',
                marginBottom: 12, cursor: 'pointer', lineHeight: 1.4,
            }}>
                <input
                    type="checkbox"
                    checked={consent}
                    onChange={(e) => setConsent(e.target.checked)}
                    style={{ marginTop: 3, width: 18, height: 18, flexShrink: 0, accentColor: 'var(--accent, #FF5C2B)' }}
                />
                <span>
                    Согласен(на), что по окончании пробного периода спишется выбранная сумма и
                    подписка будет продлеваться автоматически до моей отмены.{' '}
                    <Link to="/offer" target="_blank" style={{ color: 'var(--accent, #FF5C2B)' }}>
                        Условия
                    </Link>.
                </span>
            </label>

            {err && (
                <div style={{ color: '#d33', fontSize: 'var(--fs-sm)', marginBottom: 10 }}>{err}</div>
            )}

            <button
                type="button"
                onClick={startTrial}
                disabled={!period || !consent || submitting}
                className="editorial-press"
                style={{
                    width: '100%',
                    padding: '12px 20px',
                    borderRadius: 8,
                    border: 'none',
                    background: (!period || !consent || submitting)
                        ? 'var(--text-muted, #bbb)' : 'var(--accent, #FF5C2B)',
                    color: '#fff',
                    fontWeight: 700,
                    fontSize: 'var(--fs-base)',
                    cursor: (!period || !consent || submitting) ? 'not-allowed' : 'pointer',
                    minHeight: 48,
                    opacity: (!period || !consent || submitting) ? 0.7 : 1,
                }}
            >
                {submitting ? 'Открываем…' : `Начать бесплатно — ${days} дней`}
            </button>

            <div style={{
                textAlign: 'center', fontSize: 'var(--fs-xs)',
                color: 'var(--text-muted, #999)', marginTop: 10,
            }}>
                или оформите подписку сразу ↓
            </div>
            {/* отступ перед обычным блоком тарифа ниже; isMobile зарезервирован для будущей адаптации */}
            <div style={{ height: isMobile ? 4 : 4 }} />
        </div>
    );
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

                {/* Trial-оффер: показывается только eligible-юзерам, 1 раз. */}
                <TrialOffer tier={tier} isMobile={isMobile} />

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
