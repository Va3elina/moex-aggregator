/**
 * TrialOfferCard — переиспользуемый блок «Попробуйте бесплатно».
 *
 * Рендерится ТОЛЬКО если backend вернул trial_eligible=true (admin / активная
 * подписка / уже брал триал → false; пока бэк триала не задеплоен — поля нет →
 * скрыт). Цена/период тянутся из /api/billing/plans (не хардкод — 54-ФЗ/ЗоЗПП).
 *
 * Используется:
 *   - в UpgradeModal (lockedTier = тариф замка → выбор тарифа скрыт);
 *   - на PricingPage (без lockedTier → юзер выбирает Basic/Pro сам).
 *
 * Флоу: выбор тарифа+периода + неотмеченная галочка согласия (ГК 438) →
 * POST /api/billing/trial/start → редирект на привязку карты (T-Bank AddCard).
 */
import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { apiFetch } from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';

type Tier = 'basic' | 'pro';
type Period = 'monthly' | 'yearly';

const TRIAL_DAYS: Record<Tier, number> = { basic: 14, pro: 7 };

function fmtRub(n?: number): string {
    return n == null ? '' : n.toLocaleString('ru-RU') + ' ₽';
}

interface Prices {
    basic: { monthly?: number; yearly?: number };
    pro: { monthly?: number; yearly?: number };
}

export default function TrialOfferCard({ lockedTier }: { lockedTier?: Tier }) {
    const { isAuthenticated } = useAuth();
    const navigate = useNavigate();
    const [eligible, setEligible] = useState<boolean | null>(null);
    const [trialEnabled, setTrialEnabled] = useState(false);
    const [prices, setPrices] = useState<Prices>({ basic: {}, pro: {} });
    const [tier, setTier] = useState<Tier | null>(lockedTier ?? null);
    const [period, setPeriod] = useState<Period | null>(null);
    const [consent, setConsent] = useState(false);
    const [submitting, setSubmitting] = useState(false);
    const [err, setErr] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            try {
                // /plans — публичный (доступен гостю): даёт trial_enabled + цены.
                const pRes = await fetch('/api/billing/plans');
                const p = pRes.ok ? await pRes.json() : {};
                if (cancelled) return;
                setTrialEnabled(p.trial_enabled === true);
                const find = (t: string) =>
                    ((p.tiers || []).find((x: { tier: string }) => x.tier === t) || {}) as
                        { monthly?: { amount: number }; yearly?: { amount: number } };
                const b = find('basic');
                const pr = find('pro');
                setPrices({
                    basic: { monthly: b.monthly?.amount, yearly: b.yearly?.amount },
                    pro: { monthly: pr.monthly?.amount, yearly: pr.yearly?.amount },
                });
                // trial_eligible считается только для залогиненных (/status под auth).
                if (isAuthenticated) {
                    const sRes = await apiFetch('/api/billing/status');
                    const s = sRes.ok ? await sRes.json() : {};
                    if (cancelled) return;
                    setEligible(s.trial_eligible === true);
                } else {
                    setEligible(false);
                }
            } catch {
                if (!cancelled) { setTrialEnabled(false); setEligible(false); }
            }
        })();
        return () => { cancelled = true; };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isAuthenticated]);

    // Гость (не зарегистрирован) видит оффер, но действие — регистрация: триал
    // требует аккаунт (eligibility + верифицированный email для чека 54-ФЗ).
    // Показываем только если фича включена (публичный trial_enabled из /plans).
    if (!isAuthenticated) {
        if (!trialEnabled) return null;
        const lead = lockedTier
            ? `Попробуйте ${lockedTier === 'pro' ? 'Pro' : 'Basic'} бесплатно — ${TRIAL_DAYS[lockedTier]} дней`
            : 'Бесплатный период: Basic — 14 дней, Pro — 7 дней';
        return (
            <div style={{
                background: 'var(--bg-tertiary, #f5f1e8)',
                border: '2px solid var(--accent, #FF5C2B)',
                borderRadius: 10, padding: 16, marginBottom: 20,
            }}>
                <div style={{ fontWeight: 700, fontSize: 'var(--fs-lg)', marginBottom: 4 }}>
                    🎁 {lead}
                </div>
                <div style={{ color: 'var(--text-secondary, #666)', fontSize: 'var(--fs-sm)', marginBottom: 14, lineHeight: 1.5 }}>
                    Зарегистрируйтесь и пользуйтесь бесплатно. Карта спишется только по
                    окончании пробного периода, если не отмените.
                </div>
                <button
                    type="button"
                    onClick={() => navigate('/login')}
                    className="editorial-press"
                    style={{
                        width: '100%', padding: '12px 20px', borderRadius: 8, border: 'none',
                        background: 'var(--accent, #FF5C2B)', color: '#fff', fontWeight: 700,
                        fontSize: 'var(--fs-base)', cursor: 'pointer', minHeight: 48,
                    }}
                >
                    Зарегистрироваться и попробовать
                </button>
            </div>
        );
    }

    if (eligible !== true) return null;

    const days = tier ? TRIAL_DAYS[tier] : null;
    const tierRu = tier === 'pro' ? 'Pro' : tier === 'basic' ? 'Basic' : '';
    const price = tier && period ? prices[tier][period] : undefined;
    const chargeDate = days
        ? new Date(Date.now() + days * 86400000).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' })
        : '';
    const ready = !!tier && !!period && consent && !submitting;

    async function startTrial() {
        if (!ready) return;
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
            window.location.href = b.payment_url; // → T-Bank AddCard
        } catch (e) {
            setErr(e instanceof Error ? e.message : 'Ошибка');
            setSubmitting(false);
        }
    }

    const choiceBtn = (
        active: boolean,
        onClick: () => void,
        top: string,
        bottom: string,
    ) => (
        <button
            type="button"
            onClick={onClick}
            className="editorial-press"
            style={{
                flex: 1,
                padding: '10px 12px',
                borderRadius: 8,
                border: `2px solid ${active ? 'var(--accent, #FF5C2B)' : 'var(--border-color, #ddd)'}`,
                background: active ? 'var(--accent, #FF5C2B)' : 'transparent',
                color: active ? '#fff' : 'var(--text-primary, #1a1a1a)',
                cursor: 'pointer',
                fontWeight: 600,
                fontSize: 'var(--fs-sm)',
                minHeight: 44,
                lineHeight: 1.3,
            }}
        >
            {top}
            <br />
            <span style={{ fontSize: 'var(--fs-base)', fontWeight: 700 }}>{bottom}</span>
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
                🎁 Бесплатный пробный период
            </div>
            <div style={{ color: 'var(--text-secondary, #666)', fontSize: 'var(--fs-sm)', marginBottom: 12 }}>
                Карта не спишется сейчас. Спишем только по окончании, если не отмените.
            </div>

            {/* Выбор тарифа — только когда не из замка (на странице тарифов) */}
            {!lockedTier && (
                <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
                    {choiceBtn(tier === 'basic', () => setTier('basic'), 'Basic', '14 дней')}
                    {choiceBtn(tier === 'pro', () => setTier('pro'), 'Pro', '7 дней')}
                </div>
            )}

            {/* Выбор периода списания после триала */}
            <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
                {choiceBtn(period === 'monthly', () => setPeriod('monthly'), 'Помесячно',
                    tier ? fmtRub(prices[tier].monthly) + '/мес' : '—')}
                {choiceBtn(period === 'yearly', () => setPeriod('yearly'), 'За год',
                    tier ? fmtRub(prices[tier].yearly) + '/год' : '—')}
            </div>

            {tier && period && (
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
                disabled={!ready}
                className="editorial-press"
                style={{
                    width: '100%',
                    padding: '12px 20px',
                    borderRadius: 8,
                    border: 'none',
                    background: ready ? 'var(--accent, #FF5C2B)' : 'var(--text-muted, #bbb)',
                    color: '#fff',
                    fontWeight: 700,
                    fontSize: 'var(--fs-base)',
                    cursor: ready ? 'pointer' : 'not-allowed',
                    minHeight: 48,
                    opacity: ready ? 1 : 0.7,
                }}
            >
                {submitting
                    ? 'Открываем…'
                    : days
                        ? `Начать бесплатно — ${days} дней`
                        : 'Начать бесплатно'}
            </button>
        </div>
    );
}
