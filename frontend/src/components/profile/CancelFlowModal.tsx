/**
 * Многошаговый путь отмены подписки.
 *
 * Раньше клик «Отменить подписку» сразу показывал оффер со скидкой. Теперь путь
 * длиннее, как у крупных подписочных сервисов:
 *
 *   1. loss    — что станет недоступно после окончания оплаченного периода
 *   2. reason  — почему уходите (опрос, ответ уходит в /cancel и в лог)
 *   3. offer   — персональное предложение под причину: скидка (если ещё не
 *                использовали) и/или переход Pro → Basic. Шаг пропускается,
 *                если предложить нечего.
 *   4. confirm — итог: что будет с автопродлением и доступом
 *   5. done    — результат (отменено / скидка применена)
 *
 * Принципы, чтобы путь был длинным, но честным:
 *   - на каждом шаге есть явная кнопка «Продолжить отмену» и крестик;
 *   - опрос необязательный — можно пропустить;
 *   - никакого стыжения в формулировках («Нет, я не люблю деньги» и т.п.).
 */
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { X as XIcon, Minus, Check, ArrowLeft, Gift, ArrowDownCircle } from 'lucide-react';
import { pricingUrlFrom } from '../../utils/purchaseSource';

export type CancelReason =
  | 'expensive'
  | 'rarely'
  | 'pause'
  | 'missing'
  | 'other_service'
  | 'other';

type Step = 'loss' | 'reason' | 'offer' | 'confirm' | 'done';

interface Props {
  tier: string;
  expiresLabel: string;                 // уже отформатированная дата окончания
  features: string[];                   // что пропадёт (без «Всё из Basic»)
  /** Условия скидки с бэкенда (plans.retention_terms) или null, если скидки нет. */
  retentionOffer: RetentionOffer | null;
  loading: boolean;
  onClose: () => void;
  /** true — скидка применена. */
  onAcceptOffer: () => Promise<boolean>;
  /** true — подписка отменена. */
  onConfirmCancel: (reason: CancelReason | null, comment: string) => Promise<boolean>;
}

export interface RetentionOffer {
  period: 'monthly' | 'yearly';
  pct: number;           // % к каждому льготному списанию
  periods: number;       // сколько продлений подряд со скидкой
  discount_rub: number;  // суммарная экономия
}

const REASONS: { id: CancelReason; label: string }[] = [
  { id: 'expensive', label: 'Дорого' },
  { id: 'rarely', label: 'Редко пользуюсь' },
  { id: 'pause', label: 'Сейчас не торгую' },
  { id: 'missing', label: 'Не хватает функций' },
  { id: 'other_service', label: 'Ухожу в другой сервис' },
  { id: 'other', label: 'Другое' },
];

// Заголовок оффера подстраивается под причину ухода.
const OFFER_COPY: Record<CancelReason | 'none', { title: string; text: string }> = {
  expensive: {
    title: 'Сделаем дешевле',
    text: 'Дадим скидку на продление.',
  },
  rarely: {
    title: 'Платите меньше',
    text: 'Доступ останется, а следующий период обойдётся дешевле.',
  },
  pause: {
    title: 'Не теряйте настройки',
    text: 'Рынок вернётся, а сигналы останутся с вами. Следующий период со скидкой.',
  },
  missing: {
    title: 'Дайте нам ещё период',
    text: 'Мы прочитаем ваш ответ. А следующий период дадим со скидкой.',
  },
  other_service: {
    title: 'Сравните спокойно',
    text: 'Останьтесь ещё на период со скидкой и сравните.',
  },
  other: {
    title: 'Останьтесь со скидкой',
    text: 'Продление обойдётся дешевле.',
  },
  none: {
    title: 'Останьтесь со скидкой',
    text: 'Продление обойдётся дешевле.',
  },
};

const card = {
  backgroundColor: 'var(--bg-secondary)',
  border: '2px solid var(--text-primary)',
  boxShadow: 'var(--shadow-hard-chip, 6px 6px 0 var(--text-primary))',
  color: 'var(--text-primary)',
} as const;

const primaryBtn = {
  backgroundColor: 'var(--accent)',
  color: 'var(--text-inverse)',
  border: '2px solid var(--text-primary)',
  boxShadow: 'var(--shadow-hard-chip, 3px 3px 0 var(--text-primary))',
} as const;

const quietBtn = { backgroundColor: 'transparent', color: 'var(--text-muted)' } as const;

export default function CancelFlowModal({
  tier, expiresLabel, features, retentionOffer, loading,
  onClose, onAcceptOffer, onConfirmCancel,
}: Props) {
  const { t } = useTranslation();
  const [step, setStep] = useState<Step>('loss');
  const [reason, setReason] = useState<CancelReason | null>(null);
  const [comment, setComment] = useState('');
  const [result, setResult] = useState<'cancelled' | 'discount' | null>(null);
  const [error, setError] = useState<string | null>(null);

  const canDowngrade = tier === 'pro';
  const retentionEligible = !!retentionOffer;
  const hasOffer = retentionEligible || canDowngrade;
  // Месячным: «−25%» на N месяцев; годовым: сумма в рублях одним вычетом.
  const isYearlyOffer = retentionOffer?.period === 'yearly';
  const rub = (retentionOffer?.discount_rub ?? 0).toLocaleString('ru-RU');
  const offerBadge = retentionOffer
    ? (isYearlyOffer ? `−${rub} ₽` : `−${retentionOffer.pct}%`)
    : '';

  // Шаги, которые реально увидит человек (offer — только если есть что предложить).
  const flow = useMemo<Step[]>(
    () => (hasOffer ? ['loss', 'reason', 'offer', 'confirm'] : ['loss', 'reason', 'confirm']),
    [hasOffer],
  );
  const stepIdx = flow.indexOf(step);

  const next = () => { setError(null); setStep(flow[Math.min(stepIdx + 1, flow.length - 1)]); };
  const back = () => { setError(null); setStep(flow[Math.max(stepIdx - 1, 0)]); };

  // Esc закрывает модалку (кроме момента запроса).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape' && !loading) onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [loading, onClose]);

  const acceptOffer = async () => {
    setError(null);
    if (await onAcceptOffer()) { setResult('discount'); setStep('done'); }
    else setError(t('Не получилось применить скидку. Попробуйте ещё раз.'));
  };

  const confirmCancel = async () => {
    setError(null);
    if (await onConfirmCancel(reason, comment.trim())) { setResult('cancelled'); setStep('done'); }
    else setError(t('Не получилось отменить подписку. Попробуйте ещё раз.'));
  };

  const offerCopy = OFFER_COPY[reason ?? 'none'];
  const tierLabel = tier === 'pro' ? 'Pro' : tier === 'basic' ? 'Basic' : tier;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" role="dialog" aria-modal="true">
      <div
        className="absolute inset-0"
        style={{ backgroundColor: 'rgba(0,0,0,0.6)' }}
        onClick={() => !loading && onClose()}
      />
      <div className="relative w-full max-w-md rounded-2xl p-6 max-h-[90vh] overflow-y-auto" style={card}>
        {/* Шапка: шаг N из M + закрыть */}
        <div className="flex items-center justify-between mb-4">
          {step !== 'done' ? (
            <div className="flex items-center gap-3">
              {stepIdx > 0 && (
                <button
                  onClick={back}
                  disabled={loading}
                  aria-label={t('Назад')}
                  className="hover:opacity-70 disabled:opacity-40"
                  style={{ color: 'var(--text-muted)' }}
                >
                  <ArrowLeft size={18} />
                </button>
              )}
              <div className="flex gap-1.5" aria-hidden>
                {flow.map((s, i) => (
                  <span
                    key={s}
                    className="h-1.5 rounded-full transition-all"
                    style={{
                      width: i === stepIdx ? 22 : 8,
                      backgroundColor: i <= stepIdx ? 'var(--accent)' : 'var(--border-color)',
                    }}
                  />
                ))}
              </div>
              <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
                {t('Шаг {{n}} из {{total}}', { n: stepIdx + 1, total: flow.length })}
              </span>
            </div>
          ) : <span />}
          <button
            onClick={onClose}
            disabled={loading}
            aria-label={t('Закрыть')}
            className="hover:opacity-70 disabled:opacity-40"
            style={{ color: 'var(--text-muted)' }}
          >
            <XIcon size={18} />
          </button>
        </div>

        {/* ── 1. Что пропадёт ── */}
        {step === 'loss' && (
          <>
            <h3 className="text-lg font-bold mb-2">{t('Что изменится после отмены')}</h3>
            <p className="text-sm mb-4" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
              {t('До')} <b style={{ color: 'var(--text-primary)' }}>{expiresLabel}</b>{' '}
              {t('всё работает. Потом тариф {{tier}} отключится, и пропадёт:', { tier: tierLabel })}
            </p>
            <div className="space-y-2 mb-6">
              {features.map((f, i) => (
                <div key={i} className="flex items-start gap-2.5 text-sm">
                  <Minus size={16} className="shrink-0 mt-0.5" style={{ color: 'var(--danger)' }} />
                  <span style={{ color: 'var(--text-secondary)' }}>{t(f)}</span>
                </div>
              ))}
            </div>
            <div className="flex flex-col gap-2.5">
              <button
                onClick={onClose}
                className="w-full px-4 py-3 rounded-full text-sm font-bold transition-opacity hover:opacity-90"
                style={primaryBtn}
              >
                {t('Оставить подписку')}
              </button>
              <button
                onClick={next}
                className="w-full px-4 py-2.5 rounded-full text-sm font-medium transition-opacity hover:opacity-80"
                style={quietBtn}
              >
                {t('Продолжить отмену')}
              </button>
            </div>
          </>
        )}

        {/* ── 2. Причина ── */}
        {step === 'reason' && (
          <>
            <h3 className="text-lg font-bold mb-1">{t('Почему решили уйти?')}</h3>
            <p className="text-sm mb-4" style={{ color: 'var(--text-secondary)' }}>
              {t('Необязательно, но нам это поможет.')}
            </p>
            <div className="space-y-2 mb-4">
              {REASONS.map(r => {
                const active = reason === r.id;
                return (
                  <button
                    key={r.id}
                    onClick={() => setReason(r.id)}
                    className="w-full flex items-center gap-3 text-left px-3 py-2.5 rounded-xl text-sm transition-colors"
                    style={{
                      border: `1.5px solid ${active ? 'var(--accent)' : 'var(--border-color)'}`,
                      backgroundColor: active ? 'color-mix(in srgb, var(--accent) 10%, transparent)' : 'transparent',
                      color: 'var(--text-primary)',
                    }}
                    aria-pressed={active}
                  >
                    <span
                      className="w-4 h-4 rounded-full shrink-0 flex items-center justify-center"
                      style={{ border: `1.5px solid ${active ? 'var(--accent)' : 'var(--text-muted)'}` }}
                    >
                      {active && <span className="w-2 h-2 rounded-full" style={{ backgroundColor: 'var(--accent)' }} />}
                    </span>
                    {t(r.label)}
                  </button>
                );
              })}
            </div>
            {(reason === 'missing' || reason === 'other' || reason === 'other_service') && (
              <textarea
                value={comment}
                onChange={e => setComment(e.target.value.slice(0, 500))}
                rows={3}
                placeholder={
                  reason === 'missing' ? t('Чего не хватило?')
                  : reason === 'other_service' ? t('Куда переходите?')
                  : t('Расскажите подробнее')
                }
                className="w-full mb-4 px-3 py-2 rounded-xl border outline-none text-sm resize-none"
                style={{
                  backgroundColor: 'var(--bg-primary)',
                  borderColor: 'var(--border-color)',
                  color: 'var(--text-primary)',
                }}
              />
            )}
            <div className="flex flex-col gap-2.5">
              <button
                onClick={onClose}
                className="w-full px-4 py-3 rounded-full text-sm font-bold transition-opacity hover:opacity-90"
                style={primaryBtn}
              >
                {t('Оставить подписку')}
              </button>
              <button
                onClick={next}
                className="w-full px-4 py-2.5 rounded-full text-sm font-medium transition-opacity hover:opacity-80"
                style={quietBtn}
              >
                {reason ? t('Продолжить отмену') : t('Пропустить')}
              </button>
            </div>
          </>
        )}

        {/* ── 3. Персональное предложение ── */}
        {step === 'offer' && (
          <>
            <h3 className="text-lg font-bold mb-1">{t(retentionEligible ? offerCopy.title : 'Может, хватит Basic?')}</h3>
            <p className="text-sm mb-4" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
              {retentionEligible
                ? t(offerCopy.text)
                : t('Basic дешевле Pro, а свежие данные остаются.')}
            </p>

            {retentionEligible && (
              <div
                className="rounded-xl p-4 mb-3 text-center"
                style={{
                  border: '1.5px dashed var(--accent)',
                  backgroundColor: 'color-mix(in srgb, var(--accent) 8%, transparent)',
                }}
              >
                <div className="flex items-center justify-center gap-2 mb-1" style={{ color: 'var(--accent)' }}>
                  <Gift size={18} />
                  <span className="text-3xl font-extrabold">{offerBadge}</span>
                </div>
                <div className="text-sm" style={{ color: 'var(--text-secondary)' }}>
                  {isYearlyOffer
                    ? t('к следующему годовому продлению. Предложение разовое.')
                    : t('на {{n}} следующих месяца. Дальше по обычной цене. Предложение разовое.', { n: retentionOffer?.periods })}
                </div>
              </div>
            )}

            {canDowngrade && (
              <Link
                to={pricingUrlFrom('profile')}
                onClick={onClose}
                className="flex items-center gap-3 rounded-xl p-3 mb-5 text-sm transition-opacity hover:opacity-80"
                style={{ border: '1.5px solid var(--border-color)', color: 'var(--text-primary)' }}
              >
                <ArrowDownCircle size={20} className="shrink-0" style={{ color: 'var(--text-muted)' }} />
                <span>
                  <b>{t('Перейти на Basic')}</b>
                  <span className="block text-xs" style={{ color: 'var(--text-muted)' }}>
                    {t('Свежие данные и скринер останутся')}
                  </span>
                </span>
              </Link>
            )}
            {!canDowngrade && <div className="mb-2" />}

            <div className="flex flex-col gap-2.5">
              {retentionEligible ? (
                <button
                  onClick={acceptOffer}
                  disabled={loading}
                  className="w-full px-4 py-3 rounded-full text-sm font-bold transition-opacity hover:opacity-90 disabled:opacity-50"
                  style={primaryBtn}
                >
                  {loading
                    ? t('Применяем…')
                    : isYearlyOffer
                      ? t('Получить скидку')
                      : t('Получить скидку {{pct}}%', { pct: retentionOffer?.pct })}
                </button>
              ) : (
                <button
                  onClick={onClose}
                  className="w-full px-4 py-3 rounded-full text-sm font-bold transition-opacity hover:opacity-90"
                  style={primaryBtn}
                >
                  {t('Оставить подписку')}
                </button>
              )}
              <button
                onClick={next}
                disabled={loading}
                className="w-full px-4 py-2.5 rounded-full text-sm font-medium transition-opacity hover:opacity-80 disabled:opacity-50"
                style={quietBtn}
              >
                {t('Нет, продолжить отмену')}
              </button>
            </div>
          </>
        )}

        {/* ── 4. Подтверждение ── */}
        {step === 'confirm' && (
          <>
            <h3 className="text-lg font-bold mb-3">{t('Подтвердите отмену')}</h3>
            <div className="space-y-2.5 mb-6 text-sm" style={{ color: 'var(--text-secondary)', lineHeight: 1.45 }}>
              <div className="flex items-start gap-2.5">
                <Check size={16} className="shrink-0 mt-0.5" style={{ color: 'var(--text-muted)' }} />
                <span>{t('Автопродление отключится, списаний больше не будет.')}</span>
              </div>
              <div className="flex items-start gap-2.5">
                <Check size={16} className="shrink-0 mt-0.5" style={{ color: 'var(--text-muted)' }} />
                <span>
                  {t('Тариф {{tier}} работает до', { tier: tierLabel })}{' '}
                  <b style={{ color: 'var(--text-primary)' }}>{expiresLabel}</b>, {t('потом бесплатный план.')}
                </span>
              </div>
              <div className="flex items-start gap-2.5">
                <Check size={16} className="shrink-0 mt-0.5" style={{ color: 'var(--text-muted)' }} />
                <span>{t('До этой даты подписку можно вернуть в профиле.')}</span>
              </div>
            </div>
            <div className="flex flex-col gap-2.5">
              <button
                onClick={onClose}
                disabled={loading}
                className="w-full px-4 py-3 rounded-full text-sm font-bold transition-opacity hover:opacity-90 disabled:opacity-50"
                style={primaryBtn}
              >
                {t('Оставить подписку')}
              </button>
              <button
                onClick={confirmCancel}
                disabled={loading}
                className="w-full px-4 py-2.5 rounded-full text-sm font-medium transition-opacity hover:opacity-80 disabled:opacity-50"
                style={{ backgroundColor: 'transparent', color: 'var(--danger)', border: '1.5px solid var(--danger)' }}
              >
                {loading ? t('Отменяем…') : t('Отменить подписку')}
              </button>
            </div>
          </>
        )}

        {/* ── 5. Результат ── */}
        {step === 'done' && (
          <div className="text-center">
            {result === 'discount' ? (
              <>
                <div className="text-3xl font-extrabold mb-1" style={{ color: 'var(--accent)' }}>{offerBadge}</div>
                <h3 className="text-lg font-bold mb-2">{t('Скидка применена')}</h3>
                <p className="text-sm mb-5" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                  {isYearlyOffer
                    ? t('Следующее продление ({{date}}) будет дешевле на {{rub}} ₽. Спасибо, что остаётесь!', { date: expiresLabel, rub })
                    : t('Следующие {{n}} продления будут на {{pct}}% дешевле. Спасибо, что остаётесь!', { n: retentionOffer?.periods, pct: retentionOffer?.pct })}
                </p>
              </>
            ) : (
              <>
                <h3 className="text-lg font-bold mb-2">{t('Подписка отменена')}</h3>
                <p className="text-sm mb-5" style={{ color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                  {t('Тариф {{tier}} работает до', { tier: tierLabel })} <b style={{ color: 'var(--text-primary)' }}>{expiresLabel}</b>.{' '}
                  {t('Передумаете, нажмите «Возобновить» в профиле.')}
                </p>
              </>
            )}
            <button
              onClick={onClose}
              className="w-full px-4 py-3 rounded-full text-sm font-bold transition-opacity hover:opacity-90"
              style={primaryBtn}
            >
              {t('Готово')}
            </button>
          </div>
        )}

        {error && (
          <p className="mt-3 text-sm text-center" style={{ color: 'var(--danger)' }}>{error}</p>
        )}
      </div>
    </div>
  );
}
