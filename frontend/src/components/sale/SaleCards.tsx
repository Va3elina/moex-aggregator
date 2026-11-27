/**
 * SaleCards — карточки тарифов на время акции (макет 5-pricing.html): скидка,
 * цена года, цена в пересчёте на месяц (обычная зачёркнута), экономия, подарок.
 * Списка возможностей в карточках нет — он в таблице сравнения ниже.
 *
 * Что написано на кнопке и под ней, решает страница (гость, подписчик того же
 * тарифа, переход вверх или вниз) — сюда приходит готовое действие.
 */
import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { giftLabel, type Sale, type SaleTier } from '../../hooks/useSale';
import { fmtRub } from '../../utils/checkoutPreview';
import '../../styles/sale.css';

export interface SaleCardAction {
  label: string;
  /** Строка под кнопкой: что будет с действующей подпиской. */
  hint?: ReactNode;
  onClick: () => void;
  disabled?: boolean;
  title?: string;
}

export default function SaleCards({ sale, action }: {
  sale: Sale;
  action: (tier: SaleTier) => SaleCardAction;
}) {
  const { t } = useTranslation();
  const bestPct = Math.max(...sale.tiers.map((x) => x.pct));
  const hasBest = sale.tiers.length > 1;
  return (
    <section className="bf-grid">
      {sale.tiers.map((p) => {
        const best = hasBest && p.pct === bestPct;
        const perMonth = Math.round(p.saleAmount / 12);
        const gift = giftLabel(p.giftDays, t);
        const a = action(p);
        return (
          <article key={p.tier} className={`bf-plan${best ? ' bf-best' : ''}`}>
            {best && <div className="bf-badge">{t('Лучшее предложение')}</div>}
            <div className="bf-name">{p.title}</div>
            <div className="bf-pct">−{p.pct}<sup>%</sup></div>
            <div className="bf-price">{fmtRub(p.saleAmount)}&nbsp;₽</div>
            <div className="bf-per">{t('за год')}</div>
            <div className="bf-rule">{t('то есть')}</div>
            <div className="bf-month">
              <s>{fmtRub(p.monthlyAmount)}&nbsp;₽</s>
              <b>{fmtRub(perMonth)}&nbsp;₽</b> {t('/ месяц')}
            </div>
            <div className="bf-save">
              {t('Экономия —')} <b>{fmtRub(p.listAmount - p.saleAmount)}&nbsp;₽</b> {t('в год')}
              {gift && <><br />{gift}</>}
            </div>
            <button
              type="button"
              className={`bf-cta${best ? '' : ' bf-ghost'}`}
              onClick={a.onClick}
              disabled={a.disabled}
              title={a.title}
            >
              {a.label}
            </button>
            {a.hint && <div className="bf-mine">{a.hint}</div>}
          </article>
        );
      })}
    </section>
  );
}
