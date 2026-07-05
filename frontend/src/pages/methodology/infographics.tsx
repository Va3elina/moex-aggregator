/**
 * Инфографика для страниц методологии — в стиле мини-легенды из тура по скринеру
 * (просьба Вадима: «хочу такую же инфографику во всех методологиях, понятнее»).
 *
 * Принципы (как и вся методология):
 *   - НЕ раскрываем формулы расчёта — только СМЫСЛ («во сколько раз сильнее»,
 *     «где стоит толпа»), наглядно и на пальцах.
 *   - Тема-независимо: нейтральные элементы = currentColor (цвет задаёт Figure),
 *     лонг/шорт = зелёный/красный (одинаково читаются в светлой и тёмной).
 *   - Каждая инфографика самодостаточна и подписана (figcaption).
 */
import type { ReactNode } from 'react';

// Единая палитра инфографик (лонг/шорт/рост/падение — одинаково в свет/тёмной).
// Нейтральные элементы рисуем currentColor (цвет задаёт Figure).
export const GREEN = '#4a9959';
export const RED = '#c0492a';
export const ACCENT = '#FF5C2B';

/** Обёртка: SVG в карточке + подпись снизу. currentColor берётся из этой карточки. */
export function Figure({
  children,
  caption,
  maxWidth = 380,
}: {
  children: ReactNode;
  caption?: ReactNode;
  maxWidth?: number;
}) {
  return (
    <figure style={{ margin: '8px 0 2px' }}>
      <div
        style={{
          padding: '14px 12px 10px',
          borderRadius: 12,
          background: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
          color: 'var(--text-primary)',
          display: 'flex',
          justifyContent: 'center',
        }}
      >
        <div style={{ width: '100%', maxWidth }}>{children}</div>
      </div>
      {caption && (
        <figcaption
          style={{ marginTop: 8, fontSize: 13, color: 'var(--text-secondary)', textAlign: 'center', lineHeight: 1.5 }}
        >
          {caption}
        </figcaption>
      )}
    </figure>
  );
}

/**
 * Чистая позиция = покупки − продажи. Две встречные полосы + итог.
 */
export function NetPositionFigure() {
  return (
    <Figure caption={<>«Чистая позиция» — это <b>перевес одной стороны</b>: покупки минус продажи. Здесь покупателей больше — перевес в лонг.</>}>
      <svg viewBox="0 0 320 118" style={{ width: '100%', display: 'block' }} role="img" aria-label="Чистая позиция как разница покупок и продаж">
        {/* Покупки */}
        <text x="8" y="26" fontSize="11" fontWeight="700" fill={GREEN}>Покупки</text>
        <rect x="78" y="16" width="210" height="16" rx="4" fill={GREEN} opacity="0.85" />
        {/* Продажи */}
        <text x="8" y="58" fontSize="11" fontWeight="700" fill={RED}>Продажи</text>
        <rect x="78" y="48" width="140" height="16" rx="4" fill={RED} opacity="0.85" />
        {/* Разница = чистая позиция */}
        <line x1="218" y1="40" x2="218" y2="86" stroke="currentColor" strokeWidth="1" strokeDasharray="3 3" opacity="0.5" />
        <line x1="288" y1="24" x2="288" y2="86" stroke="currentColor" strokeWidth="1" strokeDasharray="3 3" opacity="0.5" />
        <rect x="218" y="80" width="70" height="14" rx="4" fill={GREEN} />
        <text x="253" y="108" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={GREEN}>чистая позиция</text>
      </svg>
    </Figure>
  );
}

/**
 * Перекос −100…+100: ось скоса позиции группы. Маркер + зоны лонг/шорт.
 */
export function PerekosAxisFigure() {
  return (
    <Figure caption={<>«Перекос» — <b>насколько скошена</b> чистая позиция группы: 0 = поровну, +100% = все в лонг, −100% = все в шорт. Это про <b>позицию</b>, не про число участников.</>}>
      <svg viewBox="0 0 320 96" style={{ width: '100%', display: 'block' }} role="img" aria-label="Ось перекоса от минус ста до плюс ста процентов">
        <rect x="20" y="40" width="130" height="16" rx="4" fill={RED} opacity="0.14" />
        <rect x="150" y="40" width="150" height="16" rx="4" fill={GREEN} opacity="0.16" />
        <line x1="20" y1="48" x2="300" y2="48" stroke="currentColor" strokeWidth="1.4" opacity="0.5" />
        <line x1="150" y1="32" x2="150" y2="64" stroke="currentColor" strokeWidth="1.4" strokeDasharray="3 3" opacity="0.7" />
        {/* маркер «сейчас» на +60% */}
        <circle cx="234" cy="48" r="7" fill={GREEN} />
        <line x1="234" y1="24" x2="234" y2="41" stroke={GREEN} strokeWidth="1" opacity="0.8" />
        <text x="234" y="18" textAnchor="middle" fontSize="10" fontWeight="700" fill={GREEN}>+60% лонг</text>
        {/* подписи краёв */}
        <text x="22" y="80" fontSize="10.5" fontWeight="700" fill={RED}>−100% · весь шорт</text>
        <text x="150" y="80" textAnchor="middle" fontSize="10" fill="currentColor" opacity="0.75">0 · поровну</text>
        <text x="298" y="80" textAnchor="end" fontSize="10.5" fontWeight="700" fill={GREEN}>весь лонг · +100%</text>
      </svg>
    </Figure>
  );
}

/**
 * Резкое движение ×N: обычный дневной шаг vs сегодняшний.
 */
export function SharpMoveFigure() {
  const base = [26, 18, 30, 22, 27, 20, 25]; // «обычные» дни
  return (
    <Figure caption={<>«Резкое движение» — сегодняшний сдвиг чистой позиции <b>во сколько раз сильнее обычного дня</b> (сравниваем со средним шагом за 2 недели). 2× — заметно, 5× — экстремально.</>}>
      <svg viewBox="0 0 320 110" style={{ width: '100%', display: 'block' }} role="img" aria-label="Сравнение обычного дневного шага и резкого движения">
        <line x1="20" y1="86" x2="300" y2="86" stroke="currentColor" strokeWidth="1" opacity="0.4" />
        {base.map((h, i) => (
          <rect key={i} x={26 + i * 20} y={86 - h} width="12" height={h} rx="2" fill="currentColor" opacity="0.35" />
        ))}
        <text x="86" y="102" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">обычные дни</text>
        {/* сегодня — крупный */}
        <rect x="228" y="18" width="26" height="68" rx="3" fill="#FF5C2B" />
        <line x1="228" y1="52" x2="200" y2="52" stroke="currentColor" strokeWidth="0.8" strokeDasharray="2 2" opacity="0.5" />
        <text x="241" y="12" textAnchor="middle" fontSize="11" fontWeight="800" fill="#FF5C2B">сегодня · ×3</text>
        <text x="278" y="102" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">резкий сдвиг</text>
      </svg>
    </Figure>
  );
}

/**
 * Новый рекорд перекоса: линия перекоса пробивает прошлый пик.
 */
export function RecordFigure() {
  // Ломаная перекоса, последняя точка — новый максимум.
  const pts = '20,70 55,58 90,64 125,46 160,52 195,40 230,48 265,22';
  return (
    <Figure caption={<>«Новый максимум / минимум» — перекос группы дошёл до уровня, которого <b>не было за выбранный период</b> (год, 2, 3, 5 лет или за всё время). Ловит медленный дрейф, который резкое движение пропускает.</>}>
      <svg viewBox="0 0 320 100" style={{ width: '100%', display: 'block' }} role="img" aria-label="Линия перекоса ставит новый максимум">
        {/* прошлый пик */}
        <line x1="20" y1="40" x2="300" y2="40" stroke="currentColor" strokeWidth="1" strokeDasharray="4 3" opacity="0.4" />
        <text x="24" y="35" fontSize="9.5" fill="currentColor" opacity="0.7">прошлый пик</text>
        <polyline points={pts} fill="none" stroke={GREEN} strokeWidth="2" opacity="0.85" />
        {/* новый максимум */}
        <circle cx="265" cy="22" r="6" fill={GREEN} />
        <text x="265" y="14" textAnchor="middle" fontSize="10" fontWeight="800" fill={GREEN}>★ новый максимум</text>
      </svg>
    </Figure>
  );
}
