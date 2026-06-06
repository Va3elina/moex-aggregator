/**
 * MobileBottomRail — горизонтальный нав-бар внизу экрана.
 *
 * 8 индикаторов в виде кнопок «иконка + название» с горизонтальным
 * скроллом. Активный — рыжий accent + hard shadow.
 *
 * При смене страницы скроллит к активному элементу автоматически.
 */
import { useEffect, useRef } from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
  Grid3X3,
  BarChart3,
  Wallet,
  Scale,
  Activity,
  CalendarDays,
  Banknote,
  ShoppingBag,
} from 'lucide-react';

interface RailItem {
  id: string;
  path: string;
  label: string;
  Icon: typeof BarChart3;
}

const ITEMS: RailItem[] = [
  { id: 'heatmap',       path: '/heatmap',       label: 'Карта',  Icon: Grid3X3 },
  { id: 'oi',            path: '/oi',            label: 'ОИ',     Icon: BarChart3 },
  { id: 'funds-money',   path: '/funds-money',   label: 'Фонды',  Icon: Wallet },
  { id: 'fund-trades',   path: '/fund-trades',   label: 'Покупки', Icon: ShoppingBag },
  { id: 'buffett',       path: '/buffett',       label: 'Баффет', Icon: Scale },
  { id: 'strength',      path: '/strength',      label: 'Сила',   Icon: Activity },
  { id: 'seasonality',   path: '/seasonality',   label: 'Сезон',  Icon: CalendarDays },
  { id: 'cbr-flows',     path: '/cbr-flows',     label: 'Капитал',     Icon: Banknote },
];

export default function MobileBottomRail() {
  const location = useLocation();
  const railRef = useRef<HTMLDivElement>(null);

  // Auto-scroll активного элемента в центр view'а.
  useEffect(() => {
    const rail = railRef.current;
    const active = rail?.querySelector('.fm-rail-item.active') as HTMLElement | null;
    if (!rail || !active) return;
    // Уже полностью видно → не трогаем скролл. Без этого крайние элементы
    // (особенно правый край) дёргались: target уходил за maxScroll, и iOS
    // Safari отыгрывал rubber-band «бьётся неприятно».
    const itemLeft = active.offsetLeft;
    const itemRight = itemLeft + active.clientWidth;
    if (itemLeft >= rail.scrollLeft && itemRight <= rail.scrollLeft + rail.clientWidth) return;
    // Центрируем, но жёстко клампим в [0, maxScroll] — оба края, не только 0.
    const maxScroll = rail.scrollWidth - rail.clientWidth;
    const target = itemLeft - rail.clientWidth / 2 + active.clientWidth / 2;
    rail.scrollTo({ left: Math.max(0, Math.min(target, maxScroll)), behavior: 'smooth' });
  }, [location.pathname]);

  return (
    <nav className="fm-bottomrail" ref={railRef}>
      {ITEMS.map((item) => {
        const isActive = location.pathname.startsWith(item.path);
        const Icon = item.Icon;
        return (
          <Link
            key={item.id}
            to={item.path}
            className={`fm-rail-item ${isActive ? 'active' : ''}`}
            style={{ textDecoration: 'none' }}
          >
            <span className="fm-rail-ico">
              <Icon size={16} strokeWidth={2.2} />
            </span>
            <span>{item.label}</span>
          </Link>
        );
      })}
    </nav>
  );
}
