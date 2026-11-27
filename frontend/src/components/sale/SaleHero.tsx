/**
 * SaleHero — первый экран главной на время акции (макет 1-hero.html).
 * Главную видят только гости (см. HomeRoute в App.tsx), поэтому кнопка одна:
 * на страницу тарифов. Остальная часть главной идёт ниже без изменений.
 */
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { saleGiftLine, type Sale } from '../../hooks/useSale';
import { pricingUrlFrom } from '../../utils/purchaseSource';
import { SaleCountdown, SaleTitle } from './SaleParts';

export default function SaleHero({ sale }: { sale: Sale }) {
  const { t } = useTranslation();
  const gift = saleGiftLine(sale, t);
  return (
    <section className="bf-scene bf-hero">
      <div className="bf-hero-in">
        <div>
          <SaleTitle sale={sale} as="h1" stacked />
          <p className="bf-lead">
            {t('Распродажа уже началась. Все индикаторы российского рынка на одном экране — со скидкой.')}
          </p>
        </div>
        <div className="bf-card">
          <div className="bf-eyebrow">{t('Подписка Basic и Pro')}</div>
          <div className="bf-display bf-big">{t('Скидки до {{n}}%', { n: sale.pctMax })}</div>
          {gift && <div className="bf-sub">{gift}</div>}
          <div className="bf-ends">{t('Распродажа закончится через')}</div>
          <SaleCountdown endsAt={sale.endsAt} />
          <Link className="bf-cta" to={pricingUrlFrom('sale_banner')}>
            {t('Выбрать тариф со скидкой')}
          </Link>
        </div>
      </div>
      <div className="bf-scroll bf-eyebrow">{t('Прокрутите вниз')} ↓</div>
    </section>
  );
}
