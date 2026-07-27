/**
 * PositionComet — «комета позиции» строки скринера сигналов ОИ.
 *
 * Ось перекоса ФИКСИРОВАНА −100…+100 (ноль по центру, слева шорт, справа
 * лонг) — строки сравнимы глазами. Три величины закодированы:
 *   • X головы     = ГДЕ стоит толпа сейчас (у края = «полный лонг/шорт»);
 *   • длина хвоста = НА СКОЛЬКО сдвинулись за день (нормирована на максимум
 *     |Δ п.п.| видимой выборки — maxAbsDelta);
 *   • размер головы = сила ×N, нормированная на фактический размах дня
 *     (ratioLo…ratioHi). Абсолютная шкала «2×…7×» не различала реальный
 *     разброс дня 2,9…4,3× — радиусы выходили почти одинаковыми.
 *
 * Чисел в строке нет совсем (макет Вадима, 2026-07): точный перекос
 * сегодня/вчера, дневная дельта и сила — в title-подсказке.
 *
 * HTML/CSS вместо SVG: дорожка 6px (тона ног), хвост = clip-path-клин с
 * градиентом «прозрачный → плотный к голове», голова = круг с ободком цвета
 * фона (отделяет её от хвоста и дорожки). Цвет = нога (зелёный лонг /
 * красный шорт); не-sharp ряды приглушены.
 */
import type { CSSProperties } from 'react';

interface Props {
  netPct: number | null;         // перекос сегодня, −100…+100
  netPctPrev: number | null;     // перекос вчера (для хвоста)
  ratio: number | null;          // сила сигнала ×N (размер головы)
  status: 'sharp' | 'normal' | 'illiquid' | 'nodata';
  /** Мин/макс ×N среди видимых sharp-строк — калибровка головы «по дню». */
  ratioLo?: number | null;
  ratioHi?: number | null;
  /** Максимум |Δ п.п.| среди видимых строк — калибровка длины хвоста. */
  maxAbsDelta?: number | null;
}

const H = 32;    // высота контейнера, px
const CY = 16;   // вертикальный центр оси

const clamp = (v: number, a: number, b: number) => Math.max(a, Math.min(b, v));

function fmtPct(p: number): string {
  return Math.abs(p).toFixed(1).replace('.', ',') + '%';
}
function fmtSigned(p: number): string {
  return (p >= 0 ? '+' : '−') + Math.abs(p).toFixed(1).replace('.', ',');
}

/** «Теплота» ×N в размахе дня 0…1 — радиус головы 3…9px; вырожденный размах
 *  (<0.5×) → середина шкалы, чтобы не раздувать шумовую разницу. Экспорт —
 *  таблица красит этим же значением число ×N. */
export function ratioHeat(ratio: number, lo: number, hi: number): number {
  const span = hi - lo;
  return span < 0.5 ? 0.5 : clamp((ratio - lo) / span, 0, 1);
}

export default function PositionComet({ netPct, netPctPrev, ratio, status, ratioLo, ratioHi, maxAbsDelta }: Props) {
  if (netPct == null) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  const long = netPct >= 0;
  const leg = long ? 'var(--oi-green)' : 'var(--oi-short)';
  const dim = status !== 'sharp';
  const dotPct = (clamp(netPct, -100, 100) + 100) / 2;

  const headR = ratio != null && ratioLo != null && ratioHi != null
    ? Math.round(3 + ratioHeat(ratio, ratioLo, ratioHi) * 6)
    : 3;

  // Хвост: клин от «вчера» к голове, длина по |Δ п.п.| относительно самого
  // подвижного видимого ряда (10…36px — минимум, чтобы микро-сдвиг читался).
  const delta = netPctPrev != null ? netPct - netPctPrev : null;
  let tail: { px: number; ml: number; clip: string; bg: string } | null = null;
  if (delta != null && delta !== 0) {
    const maxD = maxAbsDelta || Math.abs(delta);
    const px = Math.round(10 + 26 * clamp(Math.abs(delta) / maxD, 0, 1));
    const toRight = delta >= 0;   // двигались вправо → хвост тянется слева
    tail = {
      px,
      ml: toRight ? -px : 0,
      clip: toRight
        ? 'polygon(0 44%, 100% 0, 100% 100%, 0 56%)'
        : 'polygon(0 0, 100% 44%, 100% 56%, 0 100%)',
      bg: `linear-gradient(to ${toRight ? 'right' : 'left'}, color-mix(in srgb, ${leg} 12%, transparent), color-mix(in srgb, ${leg} 70%, transparent))`,
    };
  }

  const title = `Перекос сегодня ${fmtPct(netPct)} ${long ? 'лонг' : 'шорт'}`
    + (netPctPrev != null ? ` · вчера ${fmtPct(netPctPrev)} ${netPctPrev >= 0 ? 'лонг' : 'шорт'}` : '')
    + (delta != null ? ` · за день ${fmtSigned(delta)} п.п.` : '')
    + (ratio != null ? ` · сила ×${ratio.toFixed(1).replace('.', ',')}` : '');

  // border-box (глобальный preflight): ширина включает ободок 2px → +4 к
  // диаметру, чтобы ВИДИМЫЙ цветной радиус остался headR.
  const headBox = headR * 2 + 4;
  const headStyle: CSSProperties = {
    position: 'absolute',
    left: `${dotPct}%`,
    top: CY - headR - 2,
    width: headBox,
    height: headBox,
    marginLeft: -(headR + 2),
    borderRadius: '50%',
    border: '2px solid var(--bg-secondary)',
    background: leg,
  };

  return (
    <div title={title} style={{ position: 'relative', height: H, cursor: 'help', opacity: dim ? 0.6 : 1 }}>
      {/* дорожка: слева тон шорта, справа тон лонга */}
      <div style={{ position: 'absolute', left: 0, right: 0, top: CY - 3, height: 6, borderRadius: 3, overflow: 'hidden', display: 'flex' }}>
        <div style={{ flex: 1, background: 'var(--oi-short)', opacity: 0.1 }} />
        <div style={{ flex: 1, background: 'var(--oi-green)', opacity: 0.12 }} />
      </div>
      {/* ноль */}
      <div style={{ position: 'absolute', left: '50%', top: CY - 11, height: 22, width: 1, background: 'var(--text-secondary)', opacity: 0.55 }} />
      {/* хвост */}
      {tail && (
        <div style={{ position: 'absolute', top: CY - 6, height: 12, left: `${dotPct}%`, width: tail.px, marginLeft: tail.ml, clipPath: tail.clip, background: tail.bg }} />
      )}
      {/* голова */}
      <div style={headStyle} />
    </div>
  );
}
