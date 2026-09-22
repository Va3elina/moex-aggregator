/**
 * FundsMoneyMethodologyPage — методология «Деньги в фондах».
 *
 * Коротко: смысл индикатора и как читать. Без описания переключателей,
 * меню и колонок таблицы.
 */
import { Wallet } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { MethodologyWrapper, Section, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED, ACCENT } from './infographics';

/** Что такое СЧА: растёт от притока людей И от подорожания активов. */
function NavStackFigure() {
  const { t } = useTranslation();
  return (
    <Figure
      maxWidth={340}
      caption={
        <>
          {t('СЧА — весь «денежный пул» фонда. Растёт по двум причинам:')}{' '}
          <span style={{ color: ACCENT, fontWeight: 700 }}>{t('люди заносят деньги')}</span> {t('и')}{' '}
          <span style={{ color: GREEN, fontWeight: 700 }}>{t('активы фонда дорожают')}</span>.
        </>
      }
    >
      <svg viewBox="0 0 340 162" xmlns="http://www.w3.org/2000/svg" style={{ width: '100%', height: 'auto' }}>
        <line x1="20" y1="130" x2="300" y2="130" stroke="currentColor" strokeWidth="1" opacity="0.35" />
        <rect x="46" y="70" width="70" height="60" rx="4" fill="currentColor" opacity="0.28" />
        <text x="81" y="146" textAnchor="middle" fontSize="10.5" fontWeight="700" fill="currentColor">{t('вчера')}</text>
        <rect x="204" y="70" width="70" height="60" rx="4" fill="currentColor" opacity="0.28" />
        <rect x="204" y="44" width="70" height="26" fill={ACCENT} opacity="0.85" />
        <rect x="204" y="24" width="70" height="20" rx="4" fill={GREEN} opacity="0.85" />
        <text x="239" y="146" textAnchor="middle" fontSize="10.5" fontWeight="700" fill="currentColor">{t('сегодня')}</text>
        <text x="282" y="37" fontSize="9.5" fontWeight="700" fill={GREEN}>{t('+ цена')}</text>
        <text x="282" y="61" fontSize="9.5" fontWeight="700" fill={ACCENT}>{t('+ приток')}</text>
        <path d="M132 100 L188 100" stroke="currentColor" strokeWidth="1.4" opacity="0.5" markerEnd="url(#navArrow)" />
        <defs>
          <marker id="navArrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0 0 L6 3 L0 6 Z" fill="currentColor" opacity="0.5" />
          </marker>
        </defs>
      </svg>
    </Figure>
  );
}

/** Притоки-Оттоки: гистограмма чистого движения денег. */
function FlowsHistogramFigure() {
  const { t } = useTranslation();
  const bars = [3, 5, 2, -2, -4, 6, 4, -1, 7, -3];
  const bw = 20;
  const gap = 8;
  const zeroY = 60;
  const scale = 6;
  return (
    <Figure
      maxWidth={340}
      caption={
        <>
          {t('Каждый столбик — чистое движение денег за шаг.')}{' '}
          <span style={{ color: GREEN, fontWeight: 700 }}>{t('Вверх — приток')}</span> {t('(внесли больше, чем забрали),')}{' '}
          <span style={{ color: RED, fontWeight: 700 }}>{t('вниз — отток')}</span>. {t('Рост котировок сюда не попадает.')}
        </>
      }
    >
      <svg viewBox="0 0 300 130" xmlns="http://www.w3.org/2000/svg" style={{ width: '100%', height: 'auto' }}>
        <line x1="20" y1={zeroY} x2="288" y2={zeroY} stroke="currentColor" strokeWidth="1.2" opacity="0.5" />
        <text x="14" y={zeroY + 3} textAnchor="end" fontSize="9" fill="currentColor" opacity="0.6">0</text>
        {bars.map((v, i) => {
          const x = 24 + i * (bw + gap);
          const h = Math.abs(v) * scale;
          const y = v >= 0 ? zeroY - h : zeroY;
          return <rect key={i} x={x} y={y} width={bw} height={h} rx="2.5" fill={v >= 0 ? GREEN : RED} opacity="0.85" />;
        })}
        <text x="24" y="122" fontSize="9.5" fontWeight="700" fill={GREEN}>{t('приток ↑')}</text>
        <text x="284" y="122" textAnchor="end" fontSize="9.5" fontWeight="700" fill={RED}>{t('↓ отток')}</text>
      </svg>
    </Figure>
  );
}

/** СЧА vs индекс: расхождение = деньги приходят/уходят не только из-за рынка. */
function NavVsIndexFigure() {
  const { t } = useTranslation();
  const navPts = '20,95 70,88 120,74 170,66 220,48 270,34';
  const idxPts = '20,90 70,86 120,82 170,80 220,74 270,70';
  return (
    <Figure
      maxWidth={360}
      caption={
        <>
          <span style={{ color: ACCENT, fontWeight: 700 }}>{t('СЧА фондов')}</span> {t('обгоняет')}{' '}
          <span style={{ color: GREEN, fontWeight: 700 }}>{t('индекс-эталон')}</span> {t('— рост не только от рынка, а и от чистых притоков денег.')}
        </>
      }
    >
      <svg viewBox="0 0 300 120" xmlns="http://www.w3.org/2000/svg" style={{ width: '100%', height: 'auto' }}>
        <line x1="20" y1="105" x2="290" y2="105" stroke="currentColor" strokeWidth="1" opacity="0.35" />
        <line x1="20" y1="15" x2="20" y2="105" stroke="currentColor" strokeWidth="1" opacity="0.35" />
        <polyline points={idxPts} fill="none" stroke={GREEN} strokeWidth="2.2" opacity="0.85" />
        <polyline points={navPts} fill="none" stroke={ACCENT} strokeWidth="2.2" />
        <circle cx="270" cy="34" r="4" fill={ACCENT} />
        <circle cx="270" cy="70" r="4" fill={GREEN} />
        <line x1="270" y1="38" x2="270" y2="66" stroke="currentColor" strokeWidth="1" strokeDasharray="3 3" opacity="0.5" />
        <text x="264" y="55" textAnchor="end" fontSize="9.5" fontWeight="700" fill="currentColor" opacity="0.75">{t('приток')}</text>
      </svg>
    </Figure>
  );
}

export default function FundsMoneyMethodologyPage() {
  const { t } = useTranslation();
  return (
    <MethodologyWrapper icon={Wallet} title={t('Деньги в фондах')} backTo="/funds-money">
      <Section title={t('Что это')}>
        <p>
          {t('Индикатор показывает, где сейчас находятся деньги частных инвесторов в биржевых фондах и куда они перетекают между направлениями: денежный рынок, акции, облигации, золото, юань. Для каждого фонда видно, как меняется стоимость его активов и сколько денег инвесторы заносят или забирают. Данные — с Московской биржи, обновление раз в торговый день.')}
        </p>
        <p className="mt-3">
          {t('Это «барометр настроения»: когда деньги массово уходят в безопасные инструменты — на рынке осторожность; когда возвращаются в акции — аппетит к риску.')}
        </p>
      </Section>

      <Section title={t('Притоки и СЧА')}>
        <p>
          {t('«Притоки-Оттоки» — чистое движение денег: зелёный столбик — в фонды занесли больше, чем забрали, красный — наоборот. Этот режим очищен от движения котировок и показывает решения людей, а не переоценку активов. «СЧА» — стоимость чистых активов, общий «денежный пул» фондов направления. Она растёт и когда люди заносят деньги, и когда активы дорожают, поэтому рядом рисуется рыночный ориентир направления — так видно, откуда рост.')}
        </p>
        <FlowsHistogramFigure />
        <NavStackFigure />
        <NavVsIndexFigure />
      </Section>

      <Section title={t('Как читать')}>
        <p className="mb-4">
          {t('Отдельная цифра сама по себе — не сигнал. Индикатор показывает направление денег и настроение инвесторов. Самое информативное — сопоставить притоки, СЧА и ориентир:')}
        </p>
        <Interpretation
          rows={[
            {
              label: t('СЧА растёт, ориентир стоит или падает'),
              meaning: t('Притоки заметнее движения рынка — инвесторы заносят деньги. Для денежного рынка это обычная картина.'),
            },
            {
              label: t('СЧА падает, ориентир растёт'),
              meaning: t('Отток денег при растущем рынке — возможна фиксация прибыли или перекладка средств.'),
            },
            {
              label: t('Заметный приток в денежный рынок'),
              meaning: t('Инвесторы перекладываются в менее рисковые инструменты. Может отражать осторожность на рынке.'),
            },
            {
              label: t('Заметный приток в акции'),
              meaning: t('Может отражать возвращающийся аппетит к риску — особенно если параллельно идут оттоки из денежного рынка.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        <ReplayTourButton tourKey="funds-money" indicatorPath="/funds-money" label={t('Показать вводный тур ещё раз')} />
      </Section>
    </MethodologyWrapper>
  );
}
