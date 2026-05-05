/**
 * ThermalLinesHero — Hero-фон в стиле "Dune thermal lines" из landing handoff.
 *
 * Концепция: 3 горизонтальные волнистые "ленты" по высоте экрана. Линии стоят
 * на месте — а ВНУТРИ них вертикальный градиент течёт сверху вниз в бесконечном
 * цикле. Эффект "капли воды на m&m драже".
 *
 * Реализация:
 *   - 3 SVG-пути (волнистые ленты) на y = 38%, 58%, 78% от высоты viewBox
 *   - Форма волны = sin(x*f) + 0.4*sin(x*2.3f) — сумма двух синусов разной частоты
 *   - Каждая лента clipPath'ом обрезает <g> с двумя стэками <rect> (фон + дубликат
 *     сверху). На <g> накладывается CSS keyframe `transform: translateY(0 → 900px)`
 *     → вертикальный градиент бесконечно "стекает" вниз. GPU-плавно (transform).
 *   - Поверх — 2 div'а с linear-gradient(180deg, ...) для "спектра" сверху-вниз
 *   - Edge vignette в углах
 *   - feTurbulence + feDisplacementMap для лёгкого "мокрого" искажения
 *
 * Цвета через CSS-vars `--accent`, `--accent-core` (warm yellow), `--accent-dark`
 * (brick) — задаются в index.css. При смене акцентного цвета достаточно
 * обновить эти 3 переменных.
 *
 * Children рендерятся поверх анимации (z-index: 5) — это место для текста Hero.
 */
import type { ReactNode } from 'react';

interface ThermalLinesHeroProps {
  children: ReactNode;
  /** Высота секции — по дефолту полный viewport. */
  height?: string;
}

// 3 волнистые ленты — параметры из design handoff (landing-hero.jsx).
// yPct — вертикальный центр ленты в % высоты viewBox 900px;
// amp — амплитуда вертикального колебания;
// freq — частота синуса (меньше → плавнее);
// phase — сдвиг фазы (чтобы ленты не были одинаковыми);
// thick — толщина ленты;
// dur — период анимации стекающего градиента в секундах.
const BANDS = [
  { yPct: 38, amp: 200, freq: 0.014, phase: 0.0, thick: 130, dur: 9 },
  { yPct: 58, amp: 260, freq: 0.011, phase: 1.9, thick: 160, dur: 11 },
  { yPct: 78, amp: 180, freq: 0.016, phase: 3.4, thick: 110, dur: 8 },
];

// Закрытый SVG-path для одной ленты: волна сверху + волна снизу + Z.
// Сэмплируем 240 точек, форма — сумма 2 синусов разной частоты для богатства.
function bandPath(yBase: number, amp: number, freq: number, phase: number, thick: number): string {
  const W = 1440;
  const STEPS = 240;
  let top = '';
  let bot = '';
  for (let i = 0; i <= STEPS; i++) {
    const x = (i / STEPS) * W;
    const y = yBase
      + Math.sin(x * freq + phase) * amp
      + Math.sin(x * freq * 2.3 + phase * 1.7) * (amp * 0.4);
    top += (i === 0 ? 'M' : ' L') + ` ${x.toFixed(1)} ${(y - thick / 2).toFixed(1)}`;
  }
  for (let i = STEPS; i >= 0; i--) {
    const x = (i / STEPS) * W;
    const y = yBase
      + Math.sin(x * freq + phase) * amp
      + Math.sin(x * freq * 2.3 + phase * 1.7) * (amp * 0.4);
    bot += ` L ${x.toFixed(1)} ${(y + thick / 2).toFixed(1)}`;
  }
  return top + bot + ' Z';
}

export default function ThermalLinesHero({ children, height = '100vh' }: ThermalLinesHeroProps) {
  return (
    <section
      className="relative overflow-hidden"
      style={{
        height,
        background: 'var(--accent)',
      }}
    >
      {/* Inline keyframes — каждой ленте свой период, чтобы анимация была богаче. */}
      <style>{`
        ${BANDS.map((b, i) => `
          @keyframes thermal-flow-${i} {
            from { transform: translateY(0); }
            to   { transform: translateY(900px); }
          }
          .thermal-band-${i} {
            animation: thermal-flow-${i} ${b.dur}s linear infinite;
            transform-box: fill-box;
            will-change: transform;
          }
        `).join('')}
        @media (prefers-reduced-motion: reduce) {
          .thermal-band-0, .thermal-band-1, .thermal-band-2 {
            animation: none;
          }
        }
      `}</style>

      {/* Turbulence фильтр — лёгкое "мокрое" искажение лент.
          Применяется к контейнеру с SVG через filter: url(#thermal-drip). */}
      <svg style={{ position: 'absolute', width: 0, height: 0 }} aria-hidden>
        <defs>
          <filter id="thermal-drip" x="-10%" y="-10%" width="120%" height="120%">
            <feTurbulence type="fractalNoise" baseFrequency="0.008 0.02" numOctaves={2} seed={3} />
            <feDisplacementMap in="SourceGraphic" scale={30} xChannelSelector="R" yChannelSelector="G" />
          </filter>
        </defs>
      </svg>

      {/* Главный SVG с лентами — blur(40px) для glow-эффекта.
          Каждая лента — clipPath + <g> с двумя rect'ами (один над другим
          через y=-900 и y=0). CSS keyframe двигает <g> вниз на 900px,
          второй rect появляется снизу — создаётся бесконечная "стекающая"
          лента. */}
      <div style={{ position: 'absolute', inset: 0, filter: 'url(#thermal-drip)' }}>
        <svg
          style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', filter: 'blur(40px)' }}
          preserveAspectRatio="none"
          viewBox="0 0 1440 900"
        >
          <defs>
            {BANDS.map((b, i) => (
              <g key={i}>
                <clipPath id={`thermal-clip-${i}`}>
                  <path d={bandPath((b.yPct / 100) * 900, b.amp, b.freq, b.phase, b.thick)} />
                </clipPath>
                <linearGradient id={`thermal-grad-${i}`} x1="0%" y1="0%" x2="0%" y2="100%">
                  <stop offset="0%"   stopColor="var(--accent-dark)" />
                  <stop offset="20%"  stopColor="var(--accent)" />
                  <stop offset="40%"  stopColor="var(--accent-core)" />
                  <stop offset="60%"  stopColor="var(--accent)" />
                  <stop offset="80%"  stopColor="var(--accent-dark)" />
                  <stop offset="100%" stopColor="var(--accent)" />
                </linearGradient>
              </g>
            ))}
          </defs>
          {BANDS.map((_, i) => (
            <g key={i} clipPath={`url(#thermal-clip-${i})`}>
              <g className={`thermal-band-${i}`}>
                {/* Два rect'а друг над другом → бесшовный flow при translateY. */}
                <rect x={0} y={-900} width={1440} height={900} fill={`url(#thermal-grad-${i})`} />
                <rect x={0} y={0}    width={1440} height={900} fill={`url(#thermal-grad-${i})`} />
              </g>
            </g>
          ))}
        </svg>
      </div>

      {/* Vertical spectrum overlay 1 — soft-light blend для "затемнения сверху,
          высветления снизу". Создаёт ощущение "глубины" — верх жарче, низ светлее. */}
      <div
        style={{
          position: 'absolute',
          inset: 0,
          pointerEvents: 'none',
          background: 'linear-gradient(180deg, color-mix(in srgb, var(--accent-dark) 100%, transparent) 0%, color-mix(in srgb, var(--accent-dark) 60%, transparent) 25%, color-mix(in srgb, var(--accent) 35%, transparent) 50%, color-mix(in srgb, var(--accent) 50%, white 30%) 75%, color-mix(in srgb, white 50%, var(--accent)) 100%)',
          mixBlendMode: 'soft-light',
        }}
      />
      {/* Vertical spectrum overlay 2 — без mix-blend, добавляет затемнение по краям. */}
      <div
        style={{
          position: 'absolute',
          inset: 0,
          pointerEvents: 'none',
          background: 'linear-gradient(180deg, color-mix(in srgb, var(--accent-dark) 50%, transparent) 0%, transparent 45%, transparent 70%, color-mix(in srgb, white 25%, transparent) 100%)',
        }}
      />

      {/* Edge vignette — слегка затемняет края для фокусировки взгляда в центр. */}
      <div
        style={{
          position: 'absolute',
          inset: 0,
          pointerEvents: 'none',
          background: 'radial-gradient(ellipse at 50% 50%, transparent 45%, color-mix(in srgb, var(--accent-dark) 65%, transparent) 100%)',
        }}
      />

      {/* Контент поверх анимации */}
      <div style={{ position: 'relative', zIndex: 5, height: '100%' }}>
        {children}
      </div>
    </section>
  );
}
