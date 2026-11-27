/**
 * Кнопка акции в шапке — на месте «Plus» (макет 2-header.html).
 * size: 'full' — десктоп, 'compact' — телефон, 'tiny' — узкий телефон (<400px),
 * где две строки не помещаются рядом с логотипом: остаётся одна, со скидкой.
 */
import { Link } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { isBlackFriday, type Sale } from '../../hooks/useSale';
import { pricingUrlFrom } from '../../utils/purchaseSource';
import '../../styles/sale.css';

export default function SaleHeaderButton({ sale, size = 'full', className = '' }: {
  sale: Sale; size?: 'full' | 'compact' | 'tiny'; className?: string;
}) {
  const { t } = useTranslation();
  const name = isBlackFriday(sale) ? t('Чёрная пятница') : sale.title;
  const cls = `bf-hbtn${size === 'compact' ? ' bf-compact' : size === 'tiny' ? ' bf-tiny' : ''} ${className}`;
  return (
    <Link to={pricingUrlFrom('header')} className={cls} title={t('Перейти к тарифам')} aria-label={name}>
      {size === 'tiny' ? (
        <span className="bf-l1">−{sale.pctMax}%</span>
      ) : (
        <>
          <span className="bf-l1">{name}</span>
          <span className="bf-l2">{t('скидки до −{{n}}%', { n: sale.pctMax })}</span>
        </>
      )}
    </Link>
  );
}
