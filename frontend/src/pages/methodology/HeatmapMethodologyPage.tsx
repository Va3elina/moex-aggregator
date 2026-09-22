/**
 * HeatmapMethodologyPage — методология «Карты рынка».
 *
 * Коротко: что это и как читать. Без описания переключателей.
 */
import { useTranslation } from 'react-i18next';
import { Grid3X3 } from 'lucide-react';
import { MethodologyWrapper, Section, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED } from './infographics';

/* ── Инфографика 1: как читать одну плитку ─────────────────────────────── */
function TileAnatomyFigure() {
  const { t } = useTranslation();
  return (
    <Figure caption={<>{t('Одна плитка = одна компания.')} <b>{t('Размер')}</b> — {t('вклад в рынок,')} <b>{t('цвет')}</b> — {t('изменение цены, внутри — тикер и процент.')}</>}>
      <svg viewBox="0 0 320 162" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Устройство одной плитки карты рынка')}>
        <rect x="20" y="20" width="150" height="110" rx="8" fill={GREEN} opacity="0.9" />
        <text x="95" y="72" textAnchor="middle" fontSize="22" fontWeight="800" fill="#fff">SBER</text>
        <text x="95" y="96" textAnchor="middle" fontSize="15" fontWeight="700" fill="#fff">{t('+1,84%')}</text>
        <line x1="20" y1="140" x2="170" y2="140" stroke="currentColor" strokeWidth="1" opacity="0.5" />
        <text x="95" y="150" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">{t('размер = вклад в рынок')}</text>
        <line x1="170" y1="60" x2="210" y2="60" stroke="currentColor" strokeWidth="0.8" strokeDasharray="3 3" opacity="0.6" />
        <text x="214" y="63" fontSize="10.5" fontWeight="700" fill="currentColor">{t('тикер компании')}</text>
        <line x1="170" y1="90" x2="210" y2="90" stroke="currentColor" strokeWidth="0.8" strokeDasharray="3 3" opacity="0.6" />
        <text x="214" y="93" fontSize="10.5" fontWeight="700" fill={GREEN}>{t('изменение цены')}</text>
      </svg>
    </Figure>
  );
}

/* ── Инфографика 2: шкала цвета ─────────────────────────────────────────── */
function ColorScaleFigure() {
  const { t } = useTranslation();
  const cells = ['#7a3528', '#5a3128', '#3a2c28', '#2a2a2a', '#2c3a30', '#2d5138', '#2d6b3f'];
  return (
    <Figure caption={<>{t('Цвет плитки — это изменение цены. Красный = падение, зелёный = рост, тёмно-серый центр = «почти без движения».')} <b>{t('Насыщеннее цвет — сильнее движение.')}</b></>}>
      <svg viewBox="0 0 320 96" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Шкала цвета от красного через серый к зелёному')}>
        {cells.map((c, i) => (
          <rect key={i} x={20 + i * 40} y="24" width="38" height="30" rx="4" fill={c} />
        ))}
        <text x="24" y="72" fontSize="10.5" fontWeight="700" fill={RED}>{t('падение')}</text>
        <text x="160" y="72" textAnchor="middle" fontSize="10" fill="currentColor" opacity="0.75">{t('без изменений')}</text>
        <text x="296" y="72" textAnchor="end" fontSize="10.5" fontWeight="700" fill={GREEN}>{t('рост')}</text>
        <text x="160" y="18" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">{t('← насыщеннее = сильнее движение →')}</text>
      </svg>
    </Figure>
  );
}

/* ── Инфографика 3: группировка по секторам ─────────────────────────────── */
function SectorGroupingFigure() {
  const { t } = useTranslation();
  return (
    <Figure caption={<>{t('Плитки собраны в блоки по отраслям. Внутри каждого блока — компании этого сектора. Так за секунду видно, какая отрасль сегодня зелёная, а какая красная.')}</>}>
      <svg viewBox="0 0 320 150" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Плитки сгруппированы по секторам')}>
        <text x="16" y="18" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Нефть и газ')}</text>
        <rect x="16" y="24" width="82" height="48" rx="5" fill={GREEN} opacity="0.85" />
        <rect x="102" y="24" width="52" height="48" rx="5" fill={GREEN} opacity="0.55" />
        <text x="170" y="18" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Банки')}</text>
        <rect x="170" y="24" width="60" height="48" rx="5" fill={RED} opacity="0.8" />
        <rect x="234" y="24" width="70" height="48" rx="5" fill={GREEN} opacity="0.5" />
        <text x="16" y="92" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Металлы')}</text>
        <rect x="16" y="98" width="66" height="42" rx="5" fill={RED} opacity="0.85" />
        <rect x="86" y="98" width="68" height="42" rx="5" fill={RED} opacity="0.55" />
        <text x="170" y="92" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Потребительский')}</text>
        <rect x="170" y="98" width="60" height="42" rx="5" fill={GREEN} opacity="0.7" />
        <rect x="234" y="98" width="70" height="42" rx="5" fill="#2a2a2a" />
      </svg>
    </Figure>
  );
}

export default function HeatmapMethodologyPage() {
  const { t } = useTranslation();
  return (
    <MethodologyWrapper icon={Grid3X3} title={t('Карта рынка')} backTo="/heatmap">
      <Section title={t('Что это')}>
        <p>
          {t('Все акции Московской биржи в виде плиток на одном экране. Каждая плитка — одна компания: размер показывает её вклад в рынок, цвет — изменение цены за выбранный период. За пару секунд видно общее состояние рынка, какие сектора лидируют и кто двигает индекс.')}
        </p>
        <TileAnatomyFigure />
      </Section>

      <Section title={t('Цвет и группировка')}>
        <p>
          {t('Зелёная плитка — цена выросла, красная — упала, тёмно-серая — почти не изменилась. Чем насыщеннее цвет, тем сильнее движение. Плитки собраны в блоки по отраслям, поэтому отраслевые движения видны сразу. Наведение на плитку открывает карточку с ценой и изменениями за разные периоды.')}
        </p>
        <ColorScaleFigure />
        <SectorGroupingFigure />
      </Section>

      <Section title={t('Как читать')}>
        <Interpretation
          rows={[
            {
              label: t('Большие плитки тёмно-красные'),
              meaning: t('Тяжеловесы снижаются. Их вес в рынке большой, поэтому индекс в такие дни чаще тоже в минусе, даже если средние акции в плюсе.'),
            },
            {
              label: t('Море красного, но яркие зелёные пятна'),
              meaning: t('Общий день слабый, но отдельные сектора или акции идут против рынка — обычно на отчётности, новостях или дивидендах.'),
            },
            {
              label: t('Один сектор полностью зелёный'),
              meaning: t('Отраслевой рост — нередко связан с новостями по отрасли. Внутри сектора движение обычно ведут крупнейшие бумаги.'),
            },
            {
              label: t('Красный за день при зелёном за месяц'),
              meaning: t('Локальный откат внутри более длинного роста. Красный и там и там — более затяжное снижение.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        <ReplayTourButton tourKey="heatmap" indicatorPath="/heatmap" />
      </Section>
    </MethodologyWrapper>
  );
}
