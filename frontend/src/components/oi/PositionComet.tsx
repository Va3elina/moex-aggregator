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
 * HTML/CSS вместо SVG: дорожка ~14px (тона ног) со сплошной осью по центру и
 * штриховой вертикалью нуля, хвост = clip-path-клин с
 * градиентом «прозрачный → плотный к голове» и высотой по диаметру головы,
 * голова = круг с ободком цвета фона (отделяет её от хвоста и дорожки).
 * Цвет = нога (зелёный лонг / красный шорт). Приглушения по силе сигнала нет:
 * полоса одинаково яркая и у тихих рядов (о слабости движения говорят число
 * силы и текст сигнала, которые остаются тусклыми).
 *
 * Все размеры МАСШТАБИРУЮТСЯ по ширине viewport (cometScale): макет собирался
 * на ноутбуке, и на большом мониторе комета оставалась теми же 3…9px — то есть
 * вдвое мельче относительно строки. ≤768px = базовый размер, ≥1600px = ×1,5.
 */
import type { CSSProperties } from 'react';
import { useViewportWidth } from '../../hooks/useViewportWidth';

interface Props {
  netPct: number | null;         // перекос сегодня, −100…+100
  netPctPrev: number | null;     // перекос на начало окна (для хвоста)
  ratio: number | null;          // сила сигнала ×N (размер головы)
  /** Мин/макс ×N среди видимых sharp-строк — калибровка головы «по дню». */
  ratioLo?: number | null;
  ratioHi?: number | null;
  /** Максимум |Δ п.п.| среди видимых строк — калибровка длины хвоста. */
  maxAbsDelta?: number | null;
  /** Подписи окна в подсказке: у среднесрочной ленты хвост тянется не от
   *  «вчера», а от точки 14 торговых дней назад — врать в тексте нельзя. */
  prevLabel?: string;            // 'вчера' | '2 недели назад'
  deltaLabel?: string;           // 'за день' | 'за 2 недели'
}

const clamp = (v: number, a: number, b: number) => Math.max(a, Math.min(b, v));

/**
 * Масштаб кометы по ширине viewport. Все размеры внутри считаются в JS (радиус
 * головы и длина хвоста — производные ОТ ДАННЫХ), поэтому CSS-clamp тут не
 * работает: множитель применяется ко всем константам разом, пропорции
 * (основание хвоста = диаметр головы, ободок) сохраняются.
 *
 * ≤768px — базовый размер (макет делался на ноутбуке), 1440px → ×1,4,
 * ≥1600px → ×1,5. Верхняя граница выбрана так, чтобы высота контейнера (32→48)
 * не переросла высоту строки, которую и так держит иконка актива 32px + падинги.
 */
function cometScale(vw: number): number {
  return clamp(1 + ((vw - 768) / (1600 - 768)) * 0.5, 1, 1.5);
}

function fmtPct(p: number): string {
  return Math.abs(p).toFixed(1).replace('.', ',') + '%';
}
function fmtSigned(p: number): string {
  return (p >= 0 ? '+' : '−') + Math.abs(p).toFixed(1).replace('.', ',');
}

/** «Теплота» ×N в размахе дня 0…1 — радиус головы 4…10px до масштаба; вырожденный размах
 *  (<0.5×) → середина шкалы, чтобы не раздувать шумовую разницу. Экспорт —
 *  таблица красит этим же значением число ×N. */
export function ratioHeat(ratio: number, lo: number, hi: number): number {
  const span = hi - lo;
  return span < 0.5 ? 0.5 : clamp((ratio - lo) / span, 0, 1);
}

export default function PositionComet({
  netPct, netPctPrev, ratio, ratioLo, ratioHi, maxAbsDelta,
  prevLabel = 'вчера', deltaLabel = 'за день',
}: Props) {
  const s = cometScale(useViewportWidth());
  const H = Math.round(32 * s);   // высота контейнера, px
  const CY = Math.round(H / 2);   // вертикальный центр оси

  if (netPct == null) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  const long = netPct >= 0;
  const leg = long ? 'var(--oi-green)' : 'var(--oi-short)';
  const dotPct = (clamp(netPct, -100, 100) + 100) / 2;

  // База 4…7px: пол диапазона держит тихие ряды читаемыми (голова 3px читалась
  // как пылинка, а ободок 2px съедает контраст тем сильнее, чем меньше кружок),
  // потолок опущен с 10 до 7 — самые крупные головы забивали строку.
  const headR = ratio != null && ratioLo != null && ratioHi != null
    ? Math.round((4 + ratioHeat(ratio, ratioLo, ratioHi) * 3) * s)
    : Math.round(4 * s);
  const ring = s >= 1.3 ? 3 : 2;   // ободок «от фона» — растёт вместе с головой

  // Хвост: клин от «вчера» к голове, длина по |Δ п.п.| относительно самого
  // подвижного видимого ряда (10…36px — минимум, чтобы микро-сдвиг читался).
  //
  // Высота хвоста ПРИВЯЗАНА к диаметру головы (была фиксированные 12px): у
  // слабого сигнала голова 6px, и клин в 12px торчал из неё вдвое толще
  // кружка. Теперь основание клина = диаметр головы, кончик = 35% от него,
  // поэтому комета выглядит одинаково при любом размере.
  const delta = netPctPrev != null ? netPct - netPctPrev : null;
  const tailH = headR * 2;
  const TIP = 0.35;                        // доля высоты, до которой сходит клин
  const tipEdge = (100 - TIP * 100) / 2;   // 32.5% / 67.5%
  let tail: { px: number; ml: number; clip: string; bg: string } | null = null;
  if (delta != null && delta !== 0) {
    const maxD = maxAbsDelta || Math.abs(delta);
    const px = Math.round((10 + 26 * clamp(Math.abs(delta) / maxD, 0, 1)) * s);
    const toRight = delta >= 0;   // двигались вправо → хвост тянется слева
    tail = {
      px,
      ml: toRight ? -px : 0,
      clip: toRight
        ? `polygon(0 ${tipEdge}%, 100% 0, 100% 100%, 0 ${100 - tipEdge}%)`
        : `polygon(0 0, 100% ${tipEdge}%, 100% ${100 - tipEdge}%, 0 100%)`,
      // Плотнее прежних 12→70%: на своей же зелёной/красной дорожке хвост
      // сливался с фоном, особенно у головы.
      bg: `linear-gradient(to ${toRight ? 'right' : 'left'}, color-mix(in srgb, ${leg} 22%, transparent), color-mix(in srgb, ${leg} 88%, transparent))`,
    };
  }

  const title = `Перекос сегодня ${fmtPct(netPct)} ${long ? 'лонг' : 'шорт'}`
    + (netPctPrev != null ? ` · ${prevLabel} ${fmtPct(netPctPrev)} ${netPctPrev >= 0 ? 'лонг' : 'шорт'}` : '')
    + (delta != null ? ` · ${deltaLabel} ${fmtSigned(delta)} п.п.` : '')
    + (ratio != null ? ` · сила ×${ratio.toFixed(1).replace('.', ',')}` : '');

  // border-box (глобальный preflight): ширина включает ободок 2px → +4 к
  // диаметру, чтобы ВИДИМЫЙ цветной радиус остался headR.
  const headBox = headR * 2 + ring * 2;
  const headStyle: CSSProperties = {
    position: 'absolute',
    left: `${dotPct}%`,
    top: CY - headR - ring,
    width: headBox,
    height: headBox,
    marginLeft: -(headR + ring),
    borderRadius: '50%',
    border: `${ring}px solid var(--bg-secondary)`,
    background: leg,
  };

  // Дорожка заметно толще исходных 6px: тонкая полоса терялась под головой
  // кометы, и «поле», по которому она ходит, читалось хуже самой точки.
  const trackH = Math.round(14.4 * s);
  const zeroH = Math.round(22 * s);
  const lineW = s >= 1.3 ? 2 : 1;   // толщина осевых линий

  // Комета НЕ приглушается у тихих рядов (ratio < 2): позиция и дневной сдвиг —
  // факт, одинаково достоверный при любой силе движения, и полосу нужно читать
  // во всех строках. Тише остаются только число силы и текст сигнала — они и
  // говорят «события нет».
  return (
    <div title={title} style={{ position: 'relative', height: H, cursor: 'help' }}>
      {/* дорожка: слева тон шорта, справа тон лонга */}
      <div style={{ position: 'absolute', left: 0, right: 0, top: CY - trackH / 2, height: trackH, borderRadius: trackH / 2, overflow: 'hidden', display: 'flex' }}>
        <div style={{ flex: 1, background: 'var(--oi-short)', opacity: 0.1 }} />
        <div style={{ flex: 1, background: 'var(--oi-green)', opacity: 0.12 }} />
      </div>
      {/* ось: сплошная линия по центру дорожки — по ней «ходит» комета */}
      <div style={{ position: 'absolute', left: 0, right: 0, top: CY - lineW / 2, height: lineW, background: 'var(--text-secondary)', opacity: 0.5 }} />
      {/* ноль: штриховая вертикаль */}
      <div style={{ position: 'absolute', left: '50%', top: CY - zeroH / 2, height: zeroH, width: 0, marginLeft: -lineW / 2, borderLeft: `${lineW}px dashed var(--text-primary)`, opacity: 0.55 }} />
      {/* хвост */}
      {tail && (
        <div style={{ position: 'absolute', top: CY - headR, height: tailH, left: `${dotPct}%`, width: tail.px, marginLeft: tail.ml, clipPath: tail.clip, background: tail.bg }} />
      )}
      {/* голова */}
      <div style={headStyle} />
    </div>
  );
}
