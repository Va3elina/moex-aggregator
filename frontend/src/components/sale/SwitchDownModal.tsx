/**
 * SwitchDownModal — переход с действующего тарифа на тариф ниже одним окном
 * (макет 7-switch-to-basic.html). Отменять подписку в профиле не нужно:
 *
 *   1. старший тариф дослуживает оплаченный срок, его автопродление сервер
 *      выключает в момент ОПЛАТЫ (не при открытии окна — брошенное окно
 *      действующую подписку не трогает);
 *   2. новая подписка начинается со дня окончания старшего тарифа.
 *
 * Все даты и суммы — из предпросмотра сервера (utils/checkoutPreview), та же
 * функция выставляет сумму в платеже.
 */
import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { ConsentRow, MethodOption } from '../pricing/CheckoutBits';
import { giftLabel } from '../../hooks/useSale';
import { fmtDay, fmtRub, type CheckoutPreview } from '../../utils/checkoutPreview';
import '../../styles/sale.css';

export default function SwitchDownModal({
  toTitle,
  preview,
  hasSbp,
  consent,
  onConsentChange,
  onConfirm,
  onClose,
  isLoading,
  yearly,
}: {
  /** Название тарифа, на который переходят («Basic»). */
  toTitle: string;
  /** null — ещё грузится. */
  preview: CheckoutPreview | null;
  hasSbp: boolean;
  consent: boolean;
  onConsentChange: (v: boolean) => void;
  onConfirm: (rail: 'sbp' | 'card') => void;
  onClose: () => void;
  isLoading: boolean;
  yearly: boolean;
}) {
  const { t } = useTranslation();
  const [method, setMethod] = useState<'sbp' | 'card'>(hasSbp ? 'sbp' : 'card');
  const cur = preview?.current;
  const fromTitle = cur?.title?.split(' — ')[0] ?? cur?.tier?.toUpperCase() ?? '';
  const curUntil = fmtDay(cur?.expires_at);
  const gift = preview ? giftLabel(preview.gift_days, t) : '';
  const blocked = preview?.blocked ?? null;
  const ready = !!preview && !blocked;
  const linkStyle = { color: 'inherit', textDecoration: 'underline' } as const;

  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center px-4 py-6"
      style={{ background: 'rgba(0,0,0,0.65)' }}
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-labelledby="switch-down-title"
    >
      <div
        className="relative w-full max-w-md rounded-2xl p-6 overflow-y-auto"
        style={{
          background: 'var(--bg-primary)',
          border: '1.5px solid var(--text-primary)',
          maxHeight: 'calc(100dvh - 48px)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          aria-label={t('Закрыть')}
          className="absolute top-2 right-2 w-11 h-11 rounded-full flex items-center justify-center"
          style={{ color: 'var(--text-secondary)', background: 'transparent' }}
        >
          <X size={18} />
        </button>

        <div className="flex items-center flex-wrap" style={{ gap: 10, paddingRight: 32 }}>
          <h2
            id="switch-down-title"
            style={{ fontSize: 22, fontWeight: 800, letterSpacing: '-0.02em', color: 'var(--text-primary)' }}
          >
            {t('Перейти на {{tier}}', { tier: toTitle })}
          </h2>
          {preview?.sale_title && <span className="bf-tag">{preview.sale_title}</span>}
        </div>
        <p style={{ margin: '6px 0 16px', fontSize: 13, lineHeight: 1.5, color: 'var(--text-secondary)' }}>
          {t('Сейчас у вас {{tier}}. Отменять его вручную не нужно — всё произойдёт после оплаты.', { tier: fromTitle })}
        </p>

        {!preview ? (
          <div style={{ padding: '28px 0', textAlign: 'center', fontSize: 13, color: 'var(--text-secondary)' }}>
            {t('Считаем сроки…')}
          </div>
        ) : blocked ? (
          <div
            className="rounded-xl px-4 py-3 text-sm"
            style={{ border: '1.5px solid var(--text-primary)', color: 'var(--text-primary)' }}
          >
            {blocked}
          </div>
        ) : (
          <>
            <div className="bf-steps">
              <div className="bf-step bf-now">
                <span className="bf-dot">1</span>
                <span>
                  <b>{t('{{tier}} остаётся до {{date}}', { tier: fromTitle, date: curUntil })}</b>
                  <i>
                    {t('Срок уже оплачен, вы ничего не теряете.')}{' '}
                    {cur?.autorenew
                      ? <>{t('Автопродление {{tier}}', { tier: fromTitle })} <em>{t('отключится сразу после оплаты')}</em>.</>
                      : t('Автопродление {{tier}} уже выключено.', { tier: fromTitle })}
                  </i>
                </span>
              </div>
              <div className="bf-step bf-next">
                <span className="bf-dot">2</span>
                <span>
                  <b>
                    {t('С {{from}} — {{tier}} до {{to}}', {
                      from: fmtDay(preview.starts_at), tier: toTitle, to: fmtDay(preview.expires_at),
                    })}
                  </b>
                  <i>
                    {preview.promo_code
                      ? (yearly ? t('Год по цене акции') : t('По цене акции'))
                      : (yearly ? t('Подписка на год') : t('Подписка на месяц'))}
                    {gift && <> {t('и')} <em>{gift}</em></>}
                    . {t('Начнётся сам, когда закончится {{tier}}.', { tier: fromTitle })}
                  </i>
                </span>
              </div>
            </div>

            <div className="bf-lose">
              <b>{t('На {{tier}} не будет', { tier: toTitle })}</b>
              {t('Своего терминала, индикаторов в терминале Т-Инвестиций, экспорта CSV / Excel. Уведомлений в Telegram — 20 вместо безлимита.')}
            </div>

            {hasSbp && (
              <div role="radiogroup" aria-label={t('Способ оплаты')} style={{ marginTop: 16 }}>
                <div
                  className="mb-2"
                  style={{ fontSize: 12, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', color: 'var(--text-secondary)' }}
                >
                  {t('Способ оплаты')}
                </div>
                <MethodOption
                  kind="sbp"
                  selected={method === 'sbp'}
                  disabled={isLoading}
                  onSelect={() => setMethod('sbp')}
                  title={t('Система быстрых платежей')}
                  note={t('Без ввода данных карты')}
                />
                <MethodOption
                  kind="tpay"
                  selected={method === 'card'}
                  disabled={isLoading}
                  onSelect={() => setMethod('card')}
                  title={t('Картой или T-Pay')}
                  note={t('Карта любого банка, Мир, T-Pay')}
                />
              </div>
            )}

            <div
              className="flex items-baseline justify-between gap-3 mb-3 pt-3"
              style={{ marginTop: 8, borderTop: '1px solid var(--border-color)' }}
            >
              <span style={{ fontSize: 13, color: 'var(--text-secondary)' }}>{t('Спишется сегодня')}</span>
              <span style={{ fontSize: 20, fontWeight: 800, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>
                {fmtRub(preview.amount)}&nbsp;₽
              </span>
            </div>

            <ConsentRow
              checked={consent}
              onChange={onConsentChange}
              label={
                <>
                  {t('Согласен на автоматическое списание')}{' '}
                  {yearly
                    ? t('{{sum}} ₽ раз в год', { sum: fmtRub(preview.renew_amount) })
                    : t('{{sum}} ₽ раз в месяц', { sum: fmtRub(preview.renew_amount) })}
                  {' — '}
                  {t('по обычной цене {{tier}}, первое {{date}}', { tier: toTitle, date: fmtDay(preview.renew_at) })}
                  {' — '}{t('по')}{' '}
                  <Link to="/recurring" target="_blank" style={linkStyle}>
                    {t('Договору о рекуррентных платежах')}
                  </Link>
                  .{' '}
                  <span style={{ color: 'var(--text-muted)' }}>
                    {t('Напомним за 3 дня до списания. Отключить автопродление можно в профиле.')}
                  </span>
                </>
              }
            />
          </>
        )}

        <button
          onClick={() => onConfirm(hasSbp && method === 'sbp' ? 'sbp' : 'card')}
          disabled={!ready || !consent || isLoading}
          className="w-full mt-3 py-3 rounded-xl text-sm font-bold disabled:opacity-50 disabled:cursor-not-allowed"
          style={{
            background: 'var(--accent)',
            color: 'var(--text-primary)',
            border: '1.5px solid var(--text-primary)',
            minHeight: 48,
          }}
        >
          {isLoading ? t('Создаём…') : t('Оплатить и перейти на {{tier}}', { tier: toTitle })}
        </button>
        {ready && !consent && !isLoading && (
          <p className="mt-2 text-center" style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
            {t('Отметьте согласие на автосписание, чтобы продолжить')}
          </p>
        )}
        <button type="button" className="bf-back" onClick={onClose}>
          {t('Остаться на {{tier}}', { tier: fromTitle })}
        </button>
        <p className="mt-3 text-center" style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 }}>
          {t('Нажимая «Оплатить», вы принимаете')}{' '}
          <Link to="/agreement" target="_blank" style={linkStyle}>{t('Пользовательское соглашение')}</Link>
          . {t('Как мы обрабатываем данные — в')}{' '}
          <Link to="/privacy" target="_blank" style={linkStyle}>{t('Политике конфиденциальности')}</Link>.
        </p>
      </div>
    </div>
  );
}
