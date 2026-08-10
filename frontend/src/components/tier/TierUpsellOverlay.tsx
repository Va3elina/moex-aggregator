/**
 * TierUpsellOverlay — апселл поверх слегка заблюренного контента.
 *
 * Паттерн «виджет открывается, но платная возможность за блюром»: родитель
 * рендерит список как обычно, вешает на него лёгкий blur (см. BLURRED_STYLE)
 * и кладёт рядом этот оверлей (position:absolute поверх). Так юзер видит, ЧТО
 * именно он получит на платном тарифе, вместо голого замка.
 *
 * Используется в пикерах фондов «Деньги в фондах» (FundPickerModal) и «Сделки
 * фондов» (PortfolioFundPicker). Цена — годовая в пересчёте на месяц
 * (useTierPrices), как на карточках PricingPage.
 *
 * Родитель обязан иметь position: relative (или другой containing block).
 */
import { Link } from 'react-router-dom';
import { Lock } from 'lucide-react';
import { useTierPrices, fmtRubMonthly } from '../../hooks/useTierPrices';

/** Стиль для контента под оверлеем: блюр «совсем немного» + без интерактива. */
export const BLURRED_STYLE: React.CSSProperties = {
    filter: 'blur(2.5px)',
    pointerEvents: 'none',
    userSelect: 'none',
};

export default function TierUpsellOverlay({
    tier = 'basic',
    featureName,
    description,
}: {
    tier?: 'basic' | 'pro';
    /** Что именно заперто, напр. «Выбор фондов». */
    featureName: string;
    description?: string;
}) {
    const prices = useTierPrices();
    const tierRu = tier === 'pro' ? 'Pro' : 'Basic';

    return (
        <div
            style={{
                position: 'absolute',
                inset: 0,
                zIndex: 5,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: 20,
                // Лёгкая вуаль поверх блюра — карточка читается на любом списке.
                background: 'color-mix(in srgb, var(--bg-secondary) 35%, transparent)',
            }}
        >
            <div
                style={{
                    background: 'var(--bg-secondary)',
                    border: '2px solid var(--text-primary)',
                    boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
                    borderRadius: 14,
                    padding: '20px 22px',
                    maxWidth: 380,
                    width: '100%',
                    textAlign: 'center',
                }}
            >
                <div
                    style={{
                        width: 44,
                        height: 44,
                        borderRadius: 12,
                        background: 'var(--accent, #FF5C2B)',
                        border: '2px solid var(--text-primary)',
                        boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        margin: '0 auto 12px',
                    }}
                >
                    <Lock size={22} strokeWidth={2.4} color="#FFFFFF" />
                </div>
                <div style={{ fontWeight: 700, fontSize: 'var(--fs-lg)', marginBottom: 6 }}>
                    {featureName} — на тарифе {tierRu}
                </div>
                {description && (
                    <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', lineHeight: 1.5, marginBottom: 10 }}>
                        {description}
                    </div>
                )}
                <div style={{ fontWeight: 700, fontSize: 'var(--fs-xl)', color: 'var(--accent, #FF5C2B)' }}>
                    {fmtRubMonthly(prices[tier].monthlyEquivalent)}
                </div>
                <div style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-xs)', marginBottom: 14 }}>
                    при оплате за год
                </div>
                <Link
                    to="/pricing"
                    className="editorial-press"
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        width: '100%',
                        padding: '11px 18px',
                        background: 'var(--accent)',
                        color: 'var(--text-inverse, #fff)',
                        border: '2px solid var(--text-primary)',
                        borderRadius: 12,
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 700,
                        textDecoration: 'none',
                        boxShadow: '3px 3px 0 var(--text-primary)',
                    }}
                >
                    Перейти на {tierRu} →
                </Link>
            </div>
        </div>
    );
}
