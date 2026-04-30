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

// 12 тайлов разбросаны по секции — некоторые дублируются для плотности
const SCATTERED: ScatteredTile[] = [
  { src: '/showcase/heatmap.jpg',           x: -2,  y: -8,  rot: -7, scale: 1.0, blur: 1.5, op: 0.35, z: 4, w: 28 },
  { src: '/showcase/oi.jpg',                x: 24,  y: -5,  rot:  5, scale: 1.0, blur: 1.0, op: 0.40, z: 7, w: 26 },
  { src: '/showcase/seasonality-week.jpg',  x: 50,  y: -10, rot: -3, scale: 0.9, blur: 2.0, op: 0.30, z: 3, w: 26 },
  { src: '/showcase/buffett.jpg',           x: 73,  y: -6,  rot:  8, scale: 1.0, blur: 1.5, op: 0.40, z: 6, w: 28 },

  { src: '/showcase/funds-money-flows.jpg', x: -5,  y: 28,  rot:  4, scale: 0.95, blur: 1.5, op: 0.32, z: 2, w: 25 },
  { src: '/showcase/strength.jpg',          x: 18,  y: 30,  rot: -6, scale: 1.05, blur: 1.0, op: 0.42, z: 8, w: 27 },
  { src: '/showcase/seasonality-month.jpg', x: 45,  y: 32,  rot:  6, scale: 1.0, blur: 1.5, op: 0.36, z: 5, w: 26 },
  { src: '/showcase/funds-money-aum.jpg',   x: 70,  y: 30,  rot: -4, scale: 1.0, blur: 1.0, op: 0.38, z: 6, w: 28 },

  { src: '/showcase/oi.jpg',                x: -3,  y: 62,  rot:  6, scale: 0.95, blur: 2.5, op: 0.28, z: 1, w: 24 },
  { src: '/showcase/buffett.jpg',           x: 22,  y: 65,  rot: -3, scale: 1.0, blur: 1.5, op: 0.35, z: 4, w: 26 },
  { src: '/showcase/heatmap.jpg',           x: 50,  y: 68,  rot:  5, scale: 1.0, blur: 2.0, op: 0.32, z: 3, w: 27 },
  { src: '/showcase/strength.jpg',          x: 75,  y: 60,  rot: -7, scale: 1.05, blur: 1.5, op: 0.38, z: 7, w: 26 },
];

export default function MultiChartShowcase() {
  return (
    <div
      className="absolute inset-0 overflow-hidden pointer-events-none"
      aria-hidden="true"
      style={{ zIndex: 0 }}
    >
      {SCATTERED.map((t, i) => (
        <div
          key={i}
          className="absolute rounded-xl border overflow-hidden"
          style={{
            left: `${t.x}%`,
            top: `${t.y}%`,
            width: `${t.w}%`,
            aspectRatio: '1280 / 950',
            transform: `rotate(${t.rot}deg) scale(${t.scale})`,
            transformOrigin: 'center center',
            filter: `blur(${t.blur}px)`,
            opacity: t.op,
            zIndex: t.z,
            backgroundColor: 'var(--bg-secondary)',
            borderColor: 'var(--border-color)',
            boxShadow: '0 20px 60px rgba(0,0,0,0.4)',
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

      {/* Edge fades по 4 сторонам — растворяет scattered-карточки в фоне */}
      <div
        className="absolute top-0 left-0 right-0 h-24 md:h-32"
        style={{
          background: 'linear-gradient(to bottom, var(--bg-primary) 0%, transparent 100%)',
          zIndex: 20,
        }}
      />
      <div
        className="absolute bottom-0 left-0 right-0 h-24 md:h-32"
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
