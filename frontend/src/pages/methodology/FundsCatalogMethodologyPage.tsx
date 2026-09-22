/**
 * FundsCatalogMethodologyPage — методология «Сделки фондов» (/fund-trades).
 *
 * Коротко: смысл раздела и как читать. Без описания фильтров, колонок
 * и внутренней математики режимов.
 *
 * i18n: все видимые строки — через t() (словарь src/i18n/en/fundtrades.json).
 */
import { LayoutGrid } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { MethodologyWrapper, Section, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED, ACCENT } from './infographics';

/** Два месячных снимка состава фонда и что между ними меняется. */
function TwoSnapshotsFigure() {
  const { t } = useTranslation();
  return (
    <Figure caption={<>{t('Фонд раскрывает состав')} <b>{t('раз в месяц')}</b>{t('. Мы кладём два соседних снимка рядом и смотрим, что изменилось: где доля выросла (докупил), где упала (продал), где бумага появилась или исчезла.')}</>}>
      <svg viewBox="0 0 320 150" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Сравнение состава фонда за два месяца')}>
        <text x="14" y="18" fontSize="10.5" fontWeight="700" fill="currentColor" opacity="0.75">{t('Прошлый месяц')}</text>
        <rect x="14" y="26" width="120" height="14" rx="3" fill="currentColor" opacity="0.3" />
        <rect x="14" y="44" width="90" height="14" rx="3" fill="currentColor" opacity="0.3" />
        <rect x="14" y="62" width="60" height="14" rx="3" fill="currentColor" opacity="0.3" />
        <text x="186" y="18" fontSize="10.5" fontWeight="700" fill="currentColor" opacity="0.75">{t('Этот месяц')}</text>
        <rect x="186" y="26" width="120" height="14" rx="3" fill="currentColor" opacity="0.3" />
        <rect x="186" y="44" width="120" height="14" rx="3" fill={GREEN} opacity="0.85" />
        <rect x="186" y="62" width="40" height="14" rx="3" fill={RED} opacity="0.85" />
        <rect x="186" y="80" width="70" height="14" rx="3" fill={ACCENT} opacity="0.9" />
        <text x="310" y="37" textAnchor="end" fontSize="8.5" fill="currentColor" opacity="0.6">{t('без изменений')}</text>
        <text x="310" y="55" textAnchor="end" fontSize="8.5" fontWeight="700" fill={GREEN}>{t('докупил')}</text>
        <text x="230" y="73" fontSize="8.5" fontWeight="700" fill={RED}>{t('продал')}</text>
        <text x="262" y="91" fontSize="8.5" fontWeight="700" fill={ACCENT}>{t('новая')}</text>
        <line x1="140" y1="52" x2="180" y2="52" stroke="currentColor" strokeWidth="1.2" opacity="0.4" />
        <path d="M180 52 l-7 -4 v8 z" fill="currentColor" opacity="0.4" />
        <text x="160" y="130" textAnchor="middle" fontSize="9" fill="currentColor" opacity="0.6">{t('разница снимков = движение фонда')}</text>
      </svg>
    </Figure>
  );
}

/** Консенсус фондов: одну бумагу покупают одни и продают другие. */
function ConsensusFigure() {
  const { t } = useTranslation();
  return (
    <Figure caption={<>{t('Вкладка «Сделки фондов» складывает движение по')} <b>{t('всем фондам сразу')}</b>{t('. Слева — сколько фондов бумагу докупили, справа — сколько сократили. Видно, куда движется рынок фондов в целом.')}</>}>
      <svg viewBox="0 0 320 134" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Консенсус фондов по одной бумаге')}>
        <line x1="160" y1="14" x2="160" y2="104" stroke="currentColor" strokeWidth="1.2" strokeDasharray="3 3" opacity="0.5" />
        <text x="160" y="122" textAnchor="middle" fontSize="9" fill="currentColor" opacity="0.6">{t('одна бумага')}</text>
        <text x="14" y="20" fontSize="10" fontWeight="700" fill={GREEN}>{t('докупают')}</text>
        <rect x="70" y="26" width="90" height="14" rx="3" fill={GREEN} opacity="0.85" />
        <rect x="98" y="46" width="62" height="14" rx="3" fill={GREEN} opacity="0.6" />
        <rect x="120" y="66" width="40" height="14" rx="3" fill={GREEN} opacity="0.4" />
        <text x="66" y="98" textAnchor="end" fontSize="9" fontWeight="700" fill={GREEN}>{t('4 фонда')}</text>
        <text x="306" y="20" textAnchor="end" fontSize="10" fontWeight="700" fill={RED}>{t('сокращают')}</text>
        <rect x="160" y="26" width="40" height="14" rx="3" fill={RED} opacity="0.7" />
        <rect x="160" y="46" width="24" height="14" rx="3" fill={RED} opacity="0.5" />
        <text x="254" y="98" textAnchor="start" fontSize="9" fontWeight="700" fill={RED}>{t('2 фонда')}</text>
      </svg>
    </Figure>
  );
}

/** Потоки по компании: помесячная гистограмма чистого притока/оттока. */
function CompanyFlowFigure() {
  const { t } = useTranslation();
  const bars = [
    { x: 26, h: 34, up: true },
    { x: 62, h: 20, up: false },
    { x: 98, h: 46, up: true },
    { x: 134, h: 14, up: true },
    { x: 170, h: 30, up: false },
    { x: 206, h: 52, up: true },
    { x: 242, h: 22, up: true },
    { x: 278, h: 40, up: false },
  ];
  const zero = 66;
  return (
    <Figure caption={<>{t('«Потоки по компании» показывают')} <b>{t('одну бумагу помесячно')}</b>{t(': сколько денег фонды в неё вложили (вверх, зелёным) или вывели (вниз, красным). Тултип раскрывает, какой именно фонд дал главный вклад.')}</>}>
      <svg viewBox="0 0 320 126" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Помесячные потоки денег в одну бумагу')}>
        <line x1="14" y1={zero} x2="306" y2={zero} stroke="currentColor" strokeWidth="1" opacity="0.4" />
        {bars.map((b, i) => (
          <rect key={i} x={b.x} y={b.up ? zero - b.h : zero} width="20" height={b.h} rx="2" fill={b.up ? GREEN : RED} opacity="0.85" />
        ))}
        <text x="20" y="20" fontSize="9" fontWeight="700" fill={GREEN}>{t('приток денег ↑')}</text>
        <text x="306" y="118" textAnchor="end" fontSize="9" fontWeight="700" fill={RED}>{t('отток денег ↓')}</text>
        <text x="14" y="118" fontSize="9" fill="currentColor" opacity="0.6">{t('по месяцам →')}</text>
      </svg>
    </Figure>
  );
}

export default function FundsCatalogMethodologyPage() {
  const { t } = useTranslation();
  return (
    <MethodologyWrapper icon={LayoutGrid} title={t('Сделки фондов')} backTo="/fund-trades">
      <Section title={t('Что это')}>
        <p>
          {t('Раздел показывает, во что вложены крупные биржевые фонды акций и как их портфели меняются от месяца к месяцу — какие бумаги управляющие')}
          <strong> {t('накапливают')}</strong>, {t('а какие')} <strong>{t('распродают')}</strong>. {t('В основе — публичные раскрытия по данным Банка России. Шаг данных — один снимок в месяц, поэтому любое «движение» здесь — сравнение месяца с предыдущим, а не ежедневные сделки.')}
        </p>
        <TwoSnapshotsFigure />
      </Section>

      <Section title={t('Что внутри')}>
        <p>
          {t('Состав каждого фонда на сегодня; свод по всем фондам сразу — что дружно докупали и что сокращали; помесячная история денег фондов в одной бумаге; и разбор одного фонда за месяц: что докупил, что продал, какие позиции открыл и из каких вышел.')}
        </p>
        <ConsensusFigure />
        <CompanyFlowFigure />
        <p className="mt-4">
          {t('Зелёный — покупка, накопление, приток денег. Красный — продажа, сокращение, отток. Оранжевый — новая позиция, которой раньше в фонде не было.')}
        </p>
      </Section>

      <Section title={t('Как читать')}>
        <p className="mb-4">
          {t('Покупки и продажи фондов — не сигнал «покупать или продавать». Это взгляд на то,')}
          <strong> {t('куда двигаются деньги профессиональных управляющих')}</strong>. {t('Смотреть лучше на совпадения между фондами:')}
        </p>
        <Interpretation
          rows={[
            {
              label: t('Много фондов дружно докупают одну бумагу'),
              meaning: t('Управляющие сошлись на одной бумаге — это можно рассматривать как интерес к ней, но не гарантию роста.'),
            },
            {
              label: t('Много фондов сокращают одну бумагу'),
              meaning: t('Несколько фондов уменьшают позицию. Иногда это связано с ребалансировкой под индекс или переоценкой перспектив компании.'),
            },
            {
              label: t('Индексный фонд или авторский'),
              meaning: t('Если состав почти повторяет индекс, движения в основном отражают его ребалансировки. Если сильно отличается — управляющий ставит на свой отбор, и его сделки несут больше информации.'),
            },
            {
              label: t('Приток денег в бумагу растёт месяц за месяцем'),
              meaning: t('Разовый всплеск может быть просто ребалансировкой, а несколько месяцев подряд иногда складываются в более устойчивую картину.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        {/* gitleaks:allow — tourKey это имя ключа онбординг-тура в localStorage, не секрет. */}
        <ReplayTourButton tourKey="fund-trades-v2" indicatorPath="/fund-trades" label={t('Показать вводный тур ещё раз')} /> {/* gitleaks:allow */}
      </Section>
    </MethodologyWrapper>
  );
}
