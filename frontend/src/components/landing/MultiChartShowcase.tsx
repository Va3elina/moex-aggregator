/**
 * MultiChartShowcase — декоративный scattered-фон из скриншотов индикаторов.
 *
 * 12 карточек разбросаны "веером" по всей секции — каждая с уникальным
 * rotation, scale, blur, opacity, z-index. Перекрываются друг с другом
 * как стопка бумаг или карты на столе. Создаёт ощущение богатства данных
 * за заголовком секции.
 *
 * Размещается через `position: absolute` внутри section'а LandingPage —
 * позади heading'а и SmallFeatures grid'а. Cover whole section.
 */

interface ScatteredTile {
  src: string;
  /** Позиция X в % от ширины контейнера */
  x: number;
  /** Позиция Y в % от высоты контейнера */
  y: number;
  /** Угол поворота, deg */
  rot: number;
  /** Масштаб тайла */
  scale: number;
  /** Размытие, px */
  blur: number;
  /** Прозрачность 0-1 */
  op: number;
  /** Z-order (1-12) для overlap */
  z: number;
  /** Ширина тайла в % от контейнера */
  w: number;
}

// 8 тайлов разбросаны по секции — без дубликатов, более крупно и видимо.
// В editorial-стиле: 1.5px ink outline + hard-shadow вместо мягкой тени.
// Меньше rotation, больше opacity (0.6-0.8) для нормальной видимости.
const SCATTERED_DESKTOP: ScatteredTile[] = [
  { src: '/showcase/heatmap.jpg',           x: -3,  y: -6,  rot: -5, scale: 1.0, blur: 0, op: 0.65, z: 4, w: 30 },
  { src: '/showcase/oi.jpg',                x: 26,  y: -4,  rot:  4, scale: 1.0, blur: 0, op: 0.70, z: 7, w: 28 },
  { src: '/showcase/seasonality-week.jpg',  x: 52,  y: -8,  rot: -2, scale: 0.95, blur: 0, op: 0.60, z: 3, w: 28 },
  { src: '/showcase/buffett.jpg',           x: 75,  y: -4,  rot:  6, scale: 1.0, blur: 0, op: 0.70, z: 6, w: 30 },

  { src: '/showcase/funds-money-flows.jpg', x: -5,  y: 50,  rot:  3, scale: 0.95, blur: 0, op: 0.62, z: 2, w: 27 },
  { src: '/showcase/strength.jpg',          x: 22,  y: 55,  rot: -4, scale: 1.0, blur: 0, op: 0.72, z: 8, w: 29 },
  { src: '/showcase/seasonality-month.jpg', x: 50,  y: 52,  rot:  4, scale: 1.0, blur: 0, op: 0.65, z: 5, w: 28 },
  { src: '/showcase/funds-money-aum.jpg',   x: 75,  y: 56,  rot: -3, scale: 1.0, blur: 0, op: 0.68, z: 6, w: 30 },
];

// Mobile: 4 tiles в компактном 2×2 layout, w=46% (вместо 28-30% desktop'а)
// чтобы они были видимыми на 375vw и не overflow'или editorial-frame.
// Расположены по углам секции с большими y-gaps (12% и 70%) — heading
// сидит между ними. На <375vw тайлы получают w≈173px (около половины
// viewport width), что balance между "заметные" и "не доминируют".
const SCATTERED_MOBILE: ScatteredTile[] = [
  { src: '/showcase/heatmap.jpg', x: -10, y: -3,  rot: -4, scale: 1.0, blur: 0, op: 0.55, z: 4, w: 50 },
  { src: '/showcase/oi.jpg',      x: 60,  y: -5,  rot:  4, scale: 1.0, blur: 0, op: 0.55, z: 6, w: 50 },
  { src: '/showcase/buffett.jpg', x: -8,  y: 70,  rot:  3, scale: 1.0, blur: 0, op: 0.55, z: 5, w: 50 },
  { src: '/showcase/strength.jpg', x: 58, y: 72,  rot: -3, scale: 1.0, blur: 0, op: 0.55, z: 7, w: 50 },
];

import { useEffect, useState } from 'react';

export default function MultiChartShowcase() {
  // Detect mobile via viewport width — useState с listener resize, чтобы при
  // повороте телефона или ресайзе окна layout пересчитывался без reload.
  // Match'ит breakpoint Tailwind md (768px) — тот же что в LandingPage.
  const [isMobile, setIsMobile] = useState(
    typeof window !== 'undefined' && window.innerWidth < 768,
  );
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener('resize', handler);
    return () => window.removeEventListener('resize', handler);
  }, []);

  const tiles = isMobile ? SCATTERED_MOBILE : SCATTERED_DESKTOP;

  return (
    <div
      className="absolute inset-0 overflow-hidden pointer-events-none"
      aria-hidden="true"
      style={{ zIndex: 0 }}
    >
      {tiles.map((t, i) => (
        <div
          key={i}
          className="absolute overflow-hidden"
          style={{
            left: `${t.x}%`,
            top: `${t.y}%`,
            width: `${t.w}%`,
            aspectRatio: '1280 / 950',
            transform: `rotate(${t.rot}deg) scale(${t.scale})`,
            transformOrigin: 'center center',
            filter: t.blur > 0 ? `blur(${t.blur}px)` : 'none',
            opacity: t.op,
            zIndex: t.z,
            backgroundColor: 'var(--bg-primary)',
            // Editorial: 1.5px ink outline + 4px hard-shadow (как .editorial-frame)
            border: '1.5px solid var(--text-primary)',
            boxShadow: '4px 4px 0 var(--text-primary)',
            borderRadius: 8,
          }}
        >
          <img
            src={t.src}
            alt=""
            loading="lazy"
            className="w-full h-full object-cover"
          />
        </div>
      ))}

      {/* Edge fades по 4 сторонам — растворяет scattered-карточки в фоне.
          Мягкая paper-fade чтобы не было резкой границы. */}
      <div
        className="absolute top-0 left-0 right-0 h-24 md:h-40"
        style={{
          background: 'linear-gradient(to bottom, var(--bg-primary) 0%, transparent 100%)',
          zIndex: 20,
        }}
      />
      <div
        className="absolute bottom-0 left-0 right-0 h-24 md:h-40"
        style={{
          background: 'linear-gradient(to top, var(--bg-primary) 0%, transparent 100%)',
          zIndex: 20,
        }}
      />
      <div
        className="absolute top-0 bottom-0 left-0 w-12 md:w-24"
        style={{
          background: 'linear-gradient(to right, var(--bg-primary) 0%, transparent 100%)',
          zIndex: 20,
        }}
      />
      <div
        className="absolute top-0 bottom-0 right-0 w-12 md:w-24"
        style={{
          background: 'linear-gradient(to left, var(--bg-primary) 0%, transparent 100%)',
          zIndex: 20,
        }}
      />
    </div>
  );
}
