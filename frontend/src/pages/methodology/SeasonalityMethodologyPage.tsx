/**
 * SeasonalityMethodologyPage — методология «Сезонность».
 *
 * Коротко: смысл индикатора и как его читать. Без описания настроек серий,
 * лимитов и способов усреднения.
 */
import { useTranslation } from 'react-i18next';
import { getLang } from '../../i18n';
import { CalendarDays } from 'lucide-react';
import { MethodologyWrapper, Section, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED, ACCENT } from './infographics';

/* ── Инфографика 1: помесячный средний паттерн ─────────────────────────── */
function MonthlyPatternFigure() {
  const { t } = useTranslation();
  const bars = [
    { m: 'Я', v: 4 }, { m: 'Ф', v: 2 }, { m: 'М', v: 3 }, { m: 'А', v: 5 },
    { m: 'М', v: 1 }, { m: 'И', v: -2 }, { m: 'И', v: -4 }, { m: 'А', v: -1 },
    { m: 'С', v: -3 }, { m: 'О', v: 2 }, { m: 'Н', v: 5 }, { m: 'Д', v: 6 },
  ];
  const monthInitials = ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D'];
  const isEn = getLang() === 'en';
  const zeroY = 60;
  const unit = 7;
  return (
    <Figure caption={<>{t('«Календарь» показывает')} <b>{t('средний ход по периодам года')}</b>. {t('Столбик вверх (зелёный) — период, в который актив в среднем рос; вниз (красный) — в среднем снижался. Здесь виден типичный рисунок: слабое лето и сильный конец года.')}</>}>
      <svg viewBox="0 0 320 126" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Средний ход цены по месяцам года')}>
        <line x1="16" y1={zeroY} x2="306" y2={zeroY} stroke="currentColor" strokeWidth="1" opacity="0.45" />
        <text x="310" y={zeroY + 3} fontSize="8" fill="currentColor" opacity="0.6" textAnchor="end">0</text>
        {bars.map((b, i) => {
          const x = 22 + i * 24;
          const h = Math.abs(b.v) * unit;
          const y = b.v >= 0 ? zeroY - h : zeroY;
          const color = b.v >= 0 ? GREEN : RED;
          return (
            <g key={i}>
              <rect x={x} y={y} width="15" height={h} rx="2" fill={color} opacity="0.85" />
              <text x={x + 7.5} y={112} fontSize="9" fill="currentColor" opacity="0.7" textAnchor="middle">{isEn ? monthInitials[i] : b.m}</text>
            </g>
          );
        })}
      </svg>
    </Figure>
  );
}

/* ── Инфографика 2: годовая траектория + текущий год ───────────────────── */
function YearlyTrackFigure() {
  const { t } = useTranslation();
  const avgPts = '20,72 60,64 100,66 140,52 180,56 220,42 260,34 300,24';
  const curPts = '20,72 55,70 95,74 135,68 175,72 210,66 245,62 275,58';
  return (
    <Figure caption={<>{t('«Годовая» —')} <b>{t('усреднённый путь цены внутри года')}</b> {t('по всей истории (серая линия): типичная форма года. Оранжевая линия —')} <b>{t('текущий год')}</b> {t('поверх: видно, идёт он лучше или хуже обычного.')}</>}>
      <svg viewBox="0 0 320 108" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Средняя траектория года и текущий год поверх')}>
        <line x1="20" y1="16" x2="20" y2="86" stroke="currentColor" strokeWidth="1" opacity="0.3" />
        <text x="20" y="96" fontSize="8.5" fill="currentColor" opacity="0.6" textAnchor="start">{t('январь')}</text>
        <text x="300" y="96" fontSize="8.5" fill="currentColor" opacity="0.6" textAnchor="end">{t('декабрь')}</text>
        <polyline points={avgPts} fill="none" stroke="currentColor" strokeWidth="2" opacity="0.5" />
        <text x="300" y="18" fontSize="9" fill="currentColor" opacity="0.7" textAnchor="end">{t('средний год')}</text>
        <polyline points={curPts} fill="none" stroke={ACCENT} strokeWidth="2.4" />
        <circle cx="275" cy="58" r="4" fill={ACCENT} />
        <text x="275" y="50" fontSize="9" fontWeight="800" fill={ACCENT} textAnchor="middle">{t('сейчас')}</text>
      </svg>
    </Figure>
  );
}

export default function SeasonalityMethodologyPage() {
  const { t } = useTranslation();
  return (
    <MethodologyWrapper icon={CalendarDays} title={t('Сезонность')} backTo="/seasonality">
      <Section title={t('Что это')}>
        <p>
          {t('Среднее изменение цены актива по периодам года: как бумага, индекс или валюта обычно ведут себя в определённые часы, дни недели, числа месяца и месяцы — усреднённо по всей доступной истории. Данные — с Московской биржи.')}
        </p>
        <p className="mt-3">
          {t('Это не прогноз, а описание прошлого: в какие периоды актив в среднем чаще рос, а в какие — снижался. Прошлое поведение ничего не гарантирует на будущее.')}
        </p>
        <MonthlyPatternFigure />
      </Section>

      <Section title={t('Два вида графика')}>
        <p>
          {t('«Календарь» — столбики среднего хода по выбранному срезу: вверх — период в среднем рос, вниз — снижался. «Годовая» — усреднённая траектория цены внутри года, поверх которой можно наложить текущий год и сравнить его с обычным ходом. Можно сравнивать разные отрезки истории между собой — свежие годы и всю историю целиком.')}
        </p>
        <YearlyTrackFigure />
      </Section>

      <Section title={t('Как читать')}>
        <Interpretation
          rows={[
            {
              label: t('Зелёный столбец выше остальных'),
              meaning: t('В этот период средний рост в истории был наибольшим. Иногда такое повторяется из года в год, но это не обязательно.'),
            },
            {
              label: t('Красный столбец'),
              meaning: t('В этот период актив в истории в среднем чаще снижался. Бывает связано с сезонными причинами: налоговый период, отчётность, расходы компаний.'),
            },
            {
              label: t('Короткая история'),
              meaning: t('Несколько лет наблюдений — слабая статистика. Чем длиннее история актива, тем устойчивее его сезонный рисунок; у молодых тикеров паттерн ещё не сформирован.'),
            },
            {
              label: t('Текущий год сильно ниже среднего пути'),
              meaning: t('Год пока идёт хуже типичного. Это описание факта, а не сигнал: расхождение может как сократиться, так и усилиться.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        <ReplayTourButton tourKey="seasonality" indicatorPath="/seasonality" />
      </Section>
    </MethodologyWrapper>
  );
}
