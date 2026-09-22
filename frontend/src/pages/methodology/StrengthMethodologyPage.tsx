/**
 * StrengthMethodologyPage — методология «Сила рынка».
 *
 * Коротко: смысл индикатора и как его читать. Без параметров расчёта
 * (длина средней, состав набора, границы зон) и без описания кнопок.
 */
import { Activity } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { MethodologyWrapper, Section, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED, ACCENT } from './infographics';

/* ── Инфографика 1: суть индикатора — доля акций выше своей средней линии ──── */
function BreadthShareFigure() {
  const { t } = useTranslation();
  const cols = [
    { above: true }, { above: true }, { above: false }, { above: true }, { above: true },
    { above: false }, { above: true }, { above: true }, { above: false }, { above: true },
  ];
  const x0 = 20;
  const step = 28;
  const lineY = 60;
  return (
    <Figure
      caption={
        <>
          {t('Каждая акция сравнивается со')} <b>{t('своей средней линией')}</b>{t('. Здесь')} <b>{t('7 из 10')}</b> {t('акций стоят выше — значит «Сила рынка» =')} <b>70%</b>{t('. Индикатор считает только долю, а не насколько именно выше.')}
        </>
      }
    >
      <svg viewBox="0 0 320 126" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Доля акций выше своей средней линии')}>
        {cols.map((c, i) => {
          const cx = x0 + i * step;
          const dotY = c.above ? lineY - 18 : lineY + 18;
          const color = c.above ? GREEN : RED;
          return (
            <g key={i}>
              <line x1={cx - 8} y1={lineY} x2={cx + 8} y2={lineY} stroke="currentColor" strokeWidth="1.4" opacity="0.4" />
              <line x1={cx} y1={lineY} x2={cx} y2={dotY} stroke={color} strokeWidth="1.4" opacity="0.6" />
              <circle cx={cx} cy={dotY} r="5" fill={color} />
            </g>
          );
        })}
        <text x={x0 - 6} y="20" fontSize="10" fontWeight="700" fill={GREEN}>{t('выше линии')}</text>
        <text x={x0 - 6} y="112" fontSize="10" fontWeight="700" fill={RED}>{t('ниже линии')}</text>
        <text x="300" y="20" textAnchor="end" fontSize="11" fontWeight="700" fill="currentColor" opacity="0.75">{t('7 из 10 = 70%')}</text>
      </svg>
    </Figure>
  );
}

/* ── Инфографика 2: широкий рост vs узкий рост (та же цена индекса) ───────── */
function BroadVsNarrowFigure() {
  const { t } = useTranslation();
  return (
    <Figure
      caption={
        <>
          {t('Индекс может расти')} <b>{t('двумя способами')}</b>{t('. Слева тянут почти все бумаги — рост широкий и здоровый. Справа индекс держат')}{' '}
          <b>{t('пара тяжеловесов')}</b>{t(', остальное падает — «Сила рынка» низкая, движение хрупкое.')}
        </>
      }
    >
      <svg viewBox="0 0 320 136" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Широкий рост против узкого роста')}>
        <text x="70" y="16" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={GREEN}>{t('широкий рост')}</text>
        {[0, 1, 2, 3, 4, 5].map((i) => {
          const up = i !== 2;
          const x = 22 + i * 18;
          const h = up ? 30 + (i % 3) * 8 : 16;
          return <rect key={i} x={x} y={90 - h} width="11" height={h} rx="2" fill={up ? GREEN : RED} opacity={up ? 0.85 : 0.7} />;
        })}
        <text x="70" y="106" textAnchor="middle" fontSize="9" fill="currentColor" opacity="0.7">{t('растёт большинство')}</text>
        <text x="70" y="122" textAnchor="middle" fontSize="10" fontWeight="700" fill={ACCENT}>{t('индекс ↑')}</text>

        <line x1="160" y1="10" x2="160" y2="120" stroke="currentColor" strokeWidth="1" strokeDasharray="3 3" opacity="0.3" />

        <text x="250" y="16" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={RED}>{t('узкий рост')}</text>
        {[0, 1, 2, 3, 4, 5].map((i) => {
          const up = i === 4;
          const x = 202 + i * 18;
          const h = up ? 58 : 14 + (i % 3) * 4;
          return <rect key={i} x={x} y={90 - h} width="11" height={h} rx="2" fill={up ? GREEN : RED} opacity={up ? 0.9 : 0.7} />;
        })}
        <text x="250" y="106" textAnchor="middle" fontSize="9" fill="currentColor" opacity="0.7">{t('тянут единицы')}</text>
        <text x="250" y="122" textAnchor="middle" fontSize="10" fontWeight="700" fill={ACCENT}>{t('индекс ↑')}</text>
      </svg>
    </Figure>
  );
}

/* ── Инфографика 3: дивергенция — индекс вверх, а сила вниз ──────────────── */
function DivergenceFigure() {
  const { t } = useTranslation();
  const idx = '20,74 60,66 100,60 140,50 180,44 220,38 260,32 300,26';
  const breadth = '20,40 60,44 100,46 140,52 180,58 220,64 260,70 300,78';
  return (
    <Figure
      caption={
        <>
          <b style={{ color: ACCENT }}>{t('Индекс растёт')}</b>{t(', а')} <b style={{ color: RED }}>{t('сила падает')}</b> {t('— расхождение. Всё меньше бумаг поддерживает подъём: индекс держится на более узкой опоре. На такое стоит обращать внимание.')}
        </>
      }
    >
      <svg viewBox="0 0 344 100" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Расхождение индекса и силы рынка')}>
        <line x1="20" y1="88" x2="300" y2="88" stroke="currentColor" strokeWidth="1" opacity="0.3" />
        <polyline points={idx} fill="none" stroke={ACCENT} strokeWidth="2.2" />
        <polyline points={breadth} fill="none" stroke={RED} strokeWidth="2.2" strokeDasharray="5 3" opacity="0.9" />
        <text x="304" y="26" fontSize="10" fontWeight="700" fill={ACCENT}>{t('индекс')}</text>
        <text x="304" y="82" fontSize="10" fontWeight="700" fill={RED}>{t('сила')}</text>
      </svg>
    </Figure>
  );
}

export default function StrengthMethodologyPage() {
  const { t } = useTranslation();
  return (
    <MethodologyWrapper icon={Activity} title={t('Сила рынка')} backTo="/strength">
      <Section title={t('Что это')}>
        <p>
          {t('«Сила рынка» — это')} <strong>{t('доля акций, которые сейчас торгуются выше своей средней линии')}</strong>{t('. Каждую бумагу сравнивают с её собственным ориентиром: цена выше линии — акция «в силе», ниже — «в слабости». Индикатор показывает, какой процент бумаг сейчас в силе. Данные — с Московской биржи.')}
        </p>
        <BreadthShareFigure />
      </Section>

      <Section title={t('Широкий рост против узкого')}>
        <p>
          {t('Индекс показывает только итог. Одно и то же движение может быть здоровым — когда растёт большинство бумаг, или хрупким — когда весь подъём держится на паре гигантов. «Сила рынка» показывает, сколько акций реально участвуют в движении.')}
        </p>
        <BroadVsNarrowFigure />
      </Section>

      <Section title={t('Индекс и сила рядом')}>
        <p>
          {t('Верхний график — индекс, нижний — сила в процентах. Их сопоставление и есть главная ценность индикатора: расхождение между ними часто говорит больше, чем каждый график по отдельности.')}
        </p>
        <DivergenceFigure />
      </Section>

      <Section title={t('Как читать')}>
        <Interpretation
          rows={[
            {
              label: t('Сила высокая'),
              meaning: t('Большинство акций выше своей линии. Рост широкий, движение поддержано многими бумагами.'),
            },
            {
              label: t('Сила низкая'),
              meaning: t('Большинство акций ниже линии. Слабость широкая, не только у отдельных бумаг.'),
            },
            {
              label: t('Индекс растёт, сила падает'),
              meaning: t('Расхождение: индекс тащат единицы, широкий рынок слаб. Подъём держится на более узкой опоре.'),
            },
            {
              label: t('Индекс падает, сила растёт'),
              meaning: t('Большинство акций уже подрастают, а индекс отстаёт. Иногда такое совпадает со сменой направления после долгого падения.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        <ReplayTourButton tourKey="strength" indicatorPath="/strength" label={t('Показать вводный тур ещё раз')} />
      </Section>
    </MethodologyWrapper>
  );
}
