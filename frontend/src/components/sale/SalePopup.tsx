/**
 * SalePopup — всплывающее окно акции поверх индикаторов (макет 3-popup.html).
 *
 * Показывается не больше двух раз на устройство за акцию:
 *   1) один раз — при первом открытии любого индикатора;
 *   2) ещё один раз — в последние 24 часа акции.
 * Кто впервые пришёл уже в последние сутки, видит окно один раз. Отметка
 * ставится в момент показа, а не закрытия: перезагрузка страницы с открытым
 * окном не должна показывать его снова.
 *
 * Не мешает другим окнам: ждёт, пока закрыт баннер cookies, и не открывается
 * поверх чужого диалога (тур по странице, оплата, поиск) — попробует при
 * следующем переходе.
 */
import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { useSale } from '../../hooks/useSale';
import { useAnalytics } from '../../contexts/AnalyticsContext';
import { pricingUrlFrom } from '../../utils/purchaseSource';
import { SaleHead } from './SaleParts';

// Страницы индикаторов. На тарифах, в профиле, на оплате и в документах окно
// не нужно: человек либо уже выбирает тариф, либо занят другим.
const INDICATOR_PATHS = [
  '/heatmap', '/oi', '/funds-money', '/fund-trades', '/strength', '/seasonality',
  '/cbr-flows', '/buffett', '/funds', '/terminal', '/hot', '/repo',
];
const DAY_MS = 24 * 3600 * 1000;
const OPEN_DELAY_MS = 1500;

const key = (code: string, stage: 'first' | 'last') => `frame_sale_popup:${code}:${stage}`;
function seen(k: string): boolean {
  try { return localStorage.getItem(k) === '1'; } catch { return true; }  // нет хранилища — не надоедаем
}
function mark(k: string): void {
  try { localStorage.setItem(k, '1'); } catch { /* ignore */ }
}

/** Какой показ сейчас положен: 'first' | 'last' | null. */
function duePopupStage(code: string, endsAt: number, now: number): 'first' | 'last' | null {
  if (now >= endsAt) return null;
  if (now >= endsAt - DAY_MS) return seen(key(code, 'last')) ? null : 'last';
  return seen(key(code, 'first')) ? null : 'first';
}

export default function SalePopup() {
  const { t } = useTranslation();
  const sale = useSale();
  const { consent } = useAnalytics();
  const { pathname } = useLocation();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  // Тик раз в минуту: человек мог сидеть на сайте, когда начались последние сутки.
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!sale) return;
    const id = setInterval(() => setTick((n) => n + 1), 60_000);
    return () => clearInterval(id);
  }, [sale]);

  const onIndicator = INDICATOR_PATHS.some((p) => pathname === p || pathname.startsWith(p + '/'));
  const code = sale?.code;
  const endsAt = sale?.endsAt;

  useEffect(() => {
    if (!code || !endsAt || !onIndicator || open || consent === null) return;
    const stage = duePopupStage(code, endsAt, Date.now());
    if (!stage) return;
    const id = setTimeout(() => {
      // Поверх чужого окна не лезем; отметку не ставим — покажем позже.
      if (document.querySelector('[role="dialog"], [aria-modal="true"], #tour-spotlight-mask')) return;
      // Последние сутки закрывают и первый показ: больше двух окон не будет.
      mark(key(code, stage));
      if (stage === 'last') mark(key(code, 'first'));
      setOpen(true);
    }, OPEN_DELAY_MS);
    return () => clearTimeout(id);
  }, [code, endsAt, onIndicator, open, consent, pathname, tick]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('keydown', onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.removeEventListener('keydown', onKey);
      document.body.style.overflow = prev;
    };
  }, [open]);

  if (!open || !sale) return null;

  return createPortal(
    <div className="bf-pop-backdrop" onClick={() => setOpen(false)}>
      <div
        className="bf-pop bf-scene"
        role="dialog"
        aria-modal="true"
        aria-label={sale.title}
        onClick={(e) => e.stopPropagation()}
      >
        <button className="bf-pop-x" aria-label={t('Закрыть')} onClick={() => setOpen(false)}>
          <X size={16} strokeWidth={2.4} />
        </button>
        <div className="bf-pop-in">
          <SaleHead sale={sale} />
          <button
            className="bf-cta"
            onClick={() => { setOpen(false); navigate(pricingUrlFrom('sale_popup')); }}
          >
            {t('Выбрать тариф со скидкой')}
          </button>
          <button className="bf-later" onClick={() => setOpen(false)}>{t('Не сейчас')}</button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
