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
  LayoutGrid,
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
  { id: 'buffett',       path: '/buffett',       label: 'Баффет', Icon: Scale },
  { id: 'strength',      path: '/strength',      label: 'Сила',   Icon: Activity },
  { id: 'seasonality',   path: '/seasonality',   label: 'Сезон',  Icon: CalendarDays },
  { id: 'cbr-flows',     path: '/cbr-flows',     label: 'ЦБ',     Icon: Banknote },
  { id: 'funds-catalog', path: '/funds-catalog', label: 'Состав', Icon: LayoutGrid },
];

export default function MobileBottomRail() {
  const location = useLocation();
  const railRef = useRef<HTMLDivElement>(null);

  // Auto-scroll активного элемента в центр view'а
  useEffect(() => {
    const rail = railRef.current;
    const active = rail?.querySelector('.fm-rail-item.active') as HTMLElement | null;
    if (rail && active) {
      const target = active.offsetLeft - rail.clientWidth / 2 + active.clientWidth / 2;
      rail.scrollTo({ left: Math.max(0, target), behavior: 'smooth' });
    }
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
