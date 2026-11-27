/**
 * Общие части акции: обратный отсчёт и шапка «Black Friday / Скидки до N% /
 * подарок / таймер». Макеты — design/black-friday (1, 3, 5).
 */
import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { isBlackFriday, saleGiftLine, type Sale } from '../../hooks/useSale';
import '../../styles/sale.css';

const pad = (n: number) => String(n).padStart(2, '0');

function split(msLeft: number) {
  let s = Math.max(0, Math.floor(msLeft / 1000));
  const d = Math.floor(s / 86400); s -= d * 86400;
  const h = Math.floor(s / 3600); s -= h * 3600;
  const m = Math.floor(s / 60); s -= m * 60;
  return { d, h, m, s };
}

/** Четыре клетки: дни, часы, минуты, секунды до конца акции. */
export function SaleCountdown({ endsAt, onInk = false }: { endsAt: number; onInk?: boolean }) {
  const { t } = useTranslation();
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);
  const { d, h, m, s } = split(endsAt - now);
  const cell = (n: number, label: string) => (
    <div className={`bf-cell${onInk ? ' on-ink' : ''}`}>
      <span className="bf-num">{pad(n)}</span>
      <span className="bf-lbl">{label}</span>
    </div>
  );
  return (
    <div className="bf-count" role="timer" aria-label={t('Распродажа закончится через')}>
      {cell(d, t('дней'))}
      {cell(h, t('часов'))}
      {cell(m, t('минут'))}
      {cell(s, t('секунд'))}
    </div>
  );
}

/** Название акции крупно: «Black Friday» в две краски или её заголовок как есть. */
export function SaleTitle({ sale, as: Tag = 'div', stacked = false }: {
  sale: Sale; as?: 'div' | 'h1'; stacked?: boolean;
}) {
  if (!isBlackFriday(sale)) {
    return <Tag className="bf-display bf-title bf-plain">{sale.title}</Tag>;
  }
  return (
    <Tag className="bf-display bf-title" aria-label="Black Friday">
      <span className="bf-w1">Black</span>{stacked ? null : ' '}
      <span className="bf-w2 bf-heat">Friday</span>
    </Tag>
  );
}

/** Шапка акции: заголовок, «Скидки до N%», подарок, таймер. */
export function SaleHead({ sale, titleAs = 'div' }: { sale: Sale; titleAs?: 'div' | 'h1' }) {
  const { t } = useTranslation();
  const gift = saleGiftLine(sale, t);
  return (
    <div className="bf-head">
      <SaleTitle sale={sale} as={titleAs} />
      <div className="bf-display bf-big">{t('Скидки до {{n}}%', { n: sale.pctMax })}</div>
      {gift && <div className="bf-sub">{gift}</div>}
      <div className="bf-ends">{t('Распродажа закончится через')}</div>
      <SaleCountdown endsAt={sale.endsAt} onInk />
    </div>
  );
}
