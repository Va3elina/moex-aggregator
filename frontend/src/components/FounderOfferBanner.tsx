/**
 * FounderOfferBanner — персональный подарочный оффер для whitelisted-юзеров.
 *
 * Виден ТОЛЬКО тем, кому backend вернул `founder_offer` в /api/billing/status
 * (whitelist через env FOUNDER_OFFER_USER_IDS). У всех остальных — null → баннер
 * не рендерится. Появляется на любой странице приложения (mount в App.tsx).
 *
 * Кейс: первый подписчик, у кого подписка истекла, т.к. при покупке автопродление
 * ещё не было настроено. Дарим месяц Pro: привязка карты (0₽ AddCard), бесплатный
 * период, затем штатное автосписание полной цены (как обычный триал).
 *
 * Флоу: «Активировать» → раскрытие условий + неотмеченная галочка согласия
 * (ГК 438, ЗоЗПП 10: цена + дата первого списания) → POST /api/billing/trial/start
 * → редирект на привязку карты T-Bank → /billing/trial-success (complete).
 *
 * Текст вынесен в константы COPY — правится одним местом.
 */
import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { Heart, X } from 'lucide-react';
import { apiFetch } from '../services/api';
import { useAuth } from '../contexts/AuthContext';
import { useIsMobile } from '../hooks/useIsMobile';

const DISMISS_KEY = 'frame_founder_offer_dismissed_v1';

// Текст оффера — единственное место для правок копирайта.
const COPY = {
  title: 'Спасибо, что были первым',
  body:
    'Вы — самый первый подписчик Фрейма. Жаль, что не вышло связаться лично — ' +
    'познакомиться и услышать обратную связь. Сегодня ваша подписка закончилась: ' +
    'когда вы её оформляли, автопродление ещё не было настроено, поэтому она просто ' +
    'истекла. В благодарность хотим подарить вам месяц Pro.',
  cta: 'Активировать месяц Pro',
  // Контакт для обратной связи (если захочет поделиться опытом).
  contactLead: 'Будем рады услышать ваш опыт — пишите',
  contactHandle: '@TorSasha',
  contactUrl: 'https://t.me/TorSasha',
  // Шаг согласия (раскрытие условий автосписания).
  consentLead: (days: number, amountStr: string, dateStr: string) =>
    `Бесплатно ${days} дней Pro. Затем ${dateStr} спишется ${amountStr} и далее ` +
    `${amountStr} ежемесячно, пока не отмените (в профиле — в любой момент).`,
  consentCard: 'Для привязки карты спишется 1 ₽ и сразу вернётся на неё в полном объёме.',
  consentCheckbox: 'Я согласен(на) с автосписанием по окончании бесплатного периода',
  confirm: 'Привязать карту и активировать',
  back: 'Назад',
};

// Маршруты, где баннер мешает (headless-рендер, fullscreen-формы, страницы оплаты).
const HIDDEN_PREFIXES = [
  '/embed', '/signal-export', '/auth/callback', '/add-email',
  '/verify-email', '/login', '/billing/', '/style-preview',
];

interface FounderOffer {
  tier: string;
  period: string;
  days: number;
  amount: number | null;
}

function fmtRub(n: number | null): string {
  return n == null ? '' : n.toLocaleString('ru-RU') + ' ₽';
}

function firstChargeDate(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });
}

export default function FounderOfferBanner() {
  const { isAuthenticated } = useAuth();
  const isMobile = useIsMobile();
  const loc = useLocation();
  const [offer, setOffer] = useState<FounderOffer | null>(null);
  const [dismissed, setDismissed] = useState(
    () => localStorage.getItem(DISMISS_KEY) === '1'
  );
  const [step, setStep] = useState<'intro' | 'consent'>('intro');
  const [consent, setConsent] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!isAuthenticated || dismissed) { setOffer(null); return; }
    let cancelled = false;
    (async () => {
      try {
        const r = await apiFetch('/api/billing/status');
        const s = r.ok ? await r.json() : {};
        if (!cancelled) setOffer(s.founder_offer || null);
      } catch {
        if (!cancelled) setOffer(null);
      }
    })();
    return () => { cancelled = true; };
  }, [isAuthenticated, dismissed]);

  const hidden = HIDDEN_PREFIXES.some((p) => loc.pathname.startsWith(p));
  if (!offer || dismissed || hidden) return null;

  const dismiss = () => {
    localStorage.setItem(DISMISS_KEY, '1');
    setDismissed(true);
  };

  const activate = async () => {
    setSubmitting(true);
    setErr(null);
    try {
      const resp = await apiFetch('/api/billing/trial/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tier: offer.tier, period: offer.period, consent: true }),
      });
      const body = await resp.json().catch(() => ({}));
      if (!resp.ok || !body.payment_url) {
        throw new Error(body.detail || body.error?.message || 'Не удалось активировать');
      }
      window.location.href = body.payment_url; // → привязка карты (T-Bank AddCard)
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Ошибка');
      setSubmitting(false);
    }
  };

  const amountStr = fmtRub(offer.amount);
  const dateStr = firstChargeDate(offer.days);

  return (
    <div
      role="dialog"
      aria-label="Подарок первому подписчику"
      style={{
        position: 'fixed',
        // Mobile: ставим НАД фикс. навигацией (.fm-bottomrail z=20, bottom=0) и
        // page-actions — те же значения, что у проверенного MobilePWABanner,
        // иначе баннер накрывал бы нижнее меню. Desktop: карточка снизу по центру.
        zIndex: 50,
        ...(isMobile
          ? {
              left: 'clamp(12px, 4vw, 20px)',
              right: 'clamp(12px, 4vw, 20px)',
              bottom: 'clamp(120px, 30vw, 145px)',
            }
          : {
              left: '50%',
              transform: 'translateX(-50%)',
              bottom: 'max(16px, env(safe-area-inset-bottom))',
              width: 'min(440px, calc(100vw - 24px))',
            }),
        background: 'var(--bg-primary, #FBF7EF)',
        border: '2px solid var(--text-primary, #0A0A0A)',
        borderRadius: 14,
        boxShadow: '4px 4px 0 var(--text-primary, #0A0A0A)',
        padding: 'var(--sp-4, 18px)',
      }}
    >
      <button
        type="button"
        onClick={dismiss}
        aria-label="Закрыть"
        style={{
          position: 'absolute', top: 8, right: 8,
          width: 32, height: 32, display: 'inline-flex',
          alignItems: 'center', justifyContent: 'center',
          background: 'transparent', border: 'none', cursor: 'pointer',
          color: 'var(--text-secondary, #666)', borderRadius: 8,
        }}
      >
        <X size={18} strokeWidth={2.4} />
      </button>

      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
        <span style={{
          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
          width: 32, height: 32, borderRadius: 9, flexShrink: 0,
          background: 'var(--accent, #FF5C2B)',
          border: '2px solid var(--text-primary, #0A0A0A)',
          boxShadow: 'var(--shadow-hard-chip, 2px 2px 0 var(--text-primary, #0A0A0A))',
        }}>
          <Heart size={17} strokeWidth={2.4} color="#fff" fill="#fff" />
        </span>
        <span style={{ fontWeight: 800, fontSize: 'var(--fs-lg, 1.1rem)' }}>{COPY.title}</span>
      </div>

      {step === 'intro' ? (
        <>
          <p style={{
            color: 'var(--text-secondary, #555)', fontSize: 'var(--fs-sm, 0.9rem)',
            lineHeight: 1.55, margin: '0 0 10px',
          }}>
            {COPY.body}
          </p>
          <p style={{
            color: 'var(--text-secondary, #666)', fontSize: 'var(--fs-xs, 0.8rem)',
            lineHeight: 1.5, margin: '0 0 14px',
          }}>
            {COPY.contactLead}{' '}
            <a href={COPY.contactUrl} target="_blank" rel="noopener noreferrer"
               style={{ color: 'var(--accent, #FF5C2B)', fontWeight: 700, textDecoration: 'none' }}>
              {COPY.contactHandle}
            </a>
          </p>
          {err && (
            <div style={{ color: 'var(--accent, #FF5C2B)', fontSize: 'var(--fs-sm)', marginBottom: 10 }}>{err}</div>
          )}
          <button
            type="button"
            onClick={() => setStep('consent')}
            className="editorial-press"
            style={{
              width: '100%', minHeight: 48, padding: '12px 20px', borderRadius: 9,
              border: '2px solid var(--text-primary, #0A0A0A)',
              background: 'var(--accent, #FF5C2B)', color: '#fff', fontWeight: 800,
              fontSize: 'var(--fs-base, 1rem)', cursor: 'pointer',
              boxShadow: 'var(--shadow-hard-chip, 2px 2px 0 var(--text-primary, #0A0A0A))',
            }}
          >
            {COPY.cta}
          </button>
        </>
      ) : (
        <>
          <p style={{
            color: 'var(--text-primary, #0A0A0A)', fontSize: 'var(--fs-sm, 0.9rem)',
            lineHeight: 1.55, margin: '0 0 8px', fontWeight: 600,
          }}>
            {COPY.consentLead(offer.days, amountStr, dateStr)}
          </p>
          <p style={{
            color: 'var(--text-secondary, #666)', fontSize: 'var(--fs-xs, 0.8rem)',
            lineHeight: 1.5, margin: '0 0 12px',
          }}>
            {COPY.consentCard}
          </p>
          <label style={{
            display: 'flex', alignItems: 'flex-start', gap: 9, cursor: 'pointer',
            fontSize: 'var(--fs-sm, 0.9rem)', marginBottom: 14, lineHeight: 1.4,
          }}>
            <input
              type="checkbox"
              checked={consent}
              onChange={(e) => setConsent(e.target.checked)}
              style={{ width: 18, height: 18, marginTop: 2, flexShrink: 0, accentColor: 'var(--accent, #FF5C2B)' }}
            />
            <span>{COPY.consentCheckbox}</span>
          </label>
          {err && (
            <div style={{ color: 'var(--accent, #FF5C2B)', fontSize: 'var(--fs-sm)', marginBottom: 10 }}>{err}</div>
          )}
          <div style={{ display: 'flex', gap: 8 }}>
            <button
              type="button"
              onClick={() => { setStep('intro'); setConsent(false); setErr(null); }}
              disabled={submitting}
              style={{
                flexShrink: 0, minHeight: 48, padding: '12px 16px', borderRadius: 9,
                border: '2px solid var(--text-primary, #0A0A0A)', background: 'transparent',
                color: 'var(--text-primary, #0A0A0A)', fontWeight: 700,
                fontSize: 'var(--fs-sm, 0.9rem)', cursor: 'pointer',
              }}
            >
              {COPY.back}
            </button>
            <button
              type="button"
              onClick={activate}
              disabled={!consent || submitting}
              className="editorial-press"
              style={{
                flex: 1, minHeight: 48, padding: '12px 20px', borderRadius: 9,
                border: '2px solid var(--text-primary, #0A0A0A)',
                background: consent ? 'var(--accent, #FF5C2B)' : 'var(--bg-tertiary, #eee)',
                color: consent ? '#fff' : 'var(--text-secondary, #999)',
                fontWeight: 800, fontSize: 'var(--fs-sm, 0.9rem)',
                cursor: consent && !submitting ? 'pointer' : 'not-allowed',
                opacity: submitting ? 0.7 : 1,
              }}
            >
              {submitting ? 'Переход к привязке…' : COPY.confirm}
            </button>
          </div>
        </>
      )}
    </div>
  );
}
