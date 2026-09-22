/**
 * CbrFlowsMethodologyPage — методология индикатора «Поток капитала».
 *
 * Коротко: смысл индикатора, кто есть кто среди категорий и как читать.
 * Без описания кнопок и подсказок.
 */
import { Banknote } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { MethodologyWrapper, Section, LineBlock, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED, ACCENT } from './infographics';

/** Баланс рынка: покупки одной стороны всегда равны продажам другой. */
function BalanceFigure() {
  const { t } = useTranslation();
  return (
    <Figure
      caption={
        <>
          {t('Рынок всегда')} <b>{t('сбалансирован')}</b>{t(': сколько одни купили, столько другие продали. Сумма всех сегментов за период = 0. Над нулём —')}{' '}
          <b style={{ color: GREEN }}>{t('кто покупал')}</b>{t(', под нулём —')}{' '}
          <b style={{ color: RED }}>{t('кто продавал')}</b>.
        </>
      }
    >
      <svg viewBox="0 0 320 190" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Баланс покупок и продаж за один период')}>
        <line x1="20" y1="95" x2="300" y2="95" stroke="currentColor" strokeWidth="1.5" opacity="0.55" />
        <rect x="130" y="60" width="60" height="34" rx="2" fill={GREEN} opacity="0.9" />
        <rect x="130" y="34" width="60" height="24" rx="2" fill={GREEN} opacity="0.6" />
        <text x="160" y="26" textAnchor="middle" fontSize="10" fontWeight="700" fill={GREEN}>{t('покупатели')}</text>
        <rect x="130" y="96" width="60" height="40" rx="2" fill={RED} opacity="0.9" />
        <rect x="130" y="138" width="60" height="18" rx="2" fill={RED} opacity="0.6" />
        <text x="160" y="172" textAnchor="middle" fontSize="10" fontWeight="700" fill={RED}>{t('продавцы')}</text>
        <text x="70" y="60" textAnchor="middle" fontSize="10" fontWeight="700" fill={GREEN}>+58</text>
        <text x="70" y="136" textAnchor="middle" fontSize="10" fontWeight="700" fill={RED}>−58</text>
        <text x="248" y="90" textAnchor="middle" fontSize="11" fontWeight="800" fill="currentColor" opacity="0.8">{t('итог 0')}</text>
      </svg>
    </Figure>
  );
}

/** Дивергенция: две категории смотрят в разные стороны. */
function DivergenceFigure() {
  const { t } = useTranslation();
  return (
    <Figure
      caption={
        <>
          {t('Самое ценное —')} <b>{t('расхождение категорий')}</b>{t('. Когда физлица массово покупают')}{' '}
          <span style={{ color: GREEN }}>▲</span>{t(', а крупные участники продают')}{' '}
          <span style={{ color: RED }}>▼</span> {t('(или наоборот) — иногда это совпадает со сменой направления рынка.')}
        </>
      }
    >
      <svg viewBox="0 0 320 150" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Расхождение категорий участников')}>
        <line x1="20" y1="75" x2="300" y2="75" stroke="currentColor" strokeWidth="1.5" opacity="0.55" />
        <rect x="70" y="35" width="44" height="40" rx="2" fill={GREEN} opacity="0.9" />
        <text x="92" y="27" textAnchor="middle" fontSize="10" fontWeight="700" fill={GREEN}>{t('физлица ▲')}</text>
        <text x="92" y="93" textAnchor="middle" fontSize="9" fill="currentColor" opacity="0.6">{t('покупают')}</text>
        <rect x="206" y="75" width="44" height="44" rx="2" fill={RED} opacity="0.9" />
        <text x="228" y="133" textAnchor="middle" fontSize="10" fontWeight="700" fill={RED}>{t('крупные ▼')}</text>
        <text x="228" y="68" textAnchor="middle" fontSize="9" fill="currentColor" opacity="0.6">{t('продают')}</text>
        <text x="160" y="72" textAnchor="middle" fontSize="13" fontWeight="800" fill={ACCENT}>⇄</text>
        <text x="160" y="90" textAnchor="middle" fontSize="9" fill={ACCENT} opacity="0.9">{t('смена?')}</text>
      </svg>
    </Figure>
  );
}

export default function CbrFlowsMethodologyPage() {
  const { t } = useTranslation();
  return (
    <MethodologyWrapper icon={Banknote} title={t('Поток капитала')} backTo="/cbr-flows">
      <Section title={t('Что это')}>
        <p>
          {t('«Поток капитала» отвечает на один вопрос:')} <strong>{t('кто покупает и кто продаёт')}</strong>{' '}
          {t('на Московской бирже — в разбивке по типам участников. За каждый период считается чистая операция каждой категории: над нулём — чистые покупки, под нулём — чистые продажи. Отдельно по акциям, гособлигациям и валюте. В основе — публичные данные Банка России.')}
        </p>
        <p className="mt-3">
          {t('Рынок всегда сбалансирован: сколько одни купили, столько другие продали. Индикатор показывает именно эту структуру — кто на какой стороне и насколько крупно.')}
        </p>
        <BalanceFigure />
      </Section>

      <Section title={t('Кто эти категории')}>
        <div className="space-y-3">
          <LineBlock color={ACCENT} name={t('Нерезиденты')} desc={t('Иностранные банки, фонды и инвесторы.')} />
          <LineBlock color={ACCENT} name={t('Физические лица')} desc={t('Частные инвесторы, торгующие со своих брокерских счетов.')} />
          <LineBlock color={ACCENT} name={t('СЗКО')} desc={t('Системно значимые кредитные организации — крупнейшие банки страны.')} />
          <LineBlock color={ACCENT} name={t('Прочие банки')} desc={t('Российские банки вне перечня системно значимых.')} />
          <LineBlock color={ACCENT} name={t('НФО')} desc={t('Брокеры, страховые, пенсионные фонды, управляющие компании — когда торгуют на свои деньги.')} />
          <LineBlock color={ACCENT} name={t('Доверительное управление')} desc={t('Те же организации, но на деньги клиентов: фонды и стратегии управления.')} />
          <LineBlock color={ACCENT} name={t('Нефинансовые организации')} desc={t('Компании реального сектора. На валютном рынке — прежде всего экспортёры и импортёры.')} />
        </div>
        <p className="mt-4">
          {t('Абсолютные суммы важны, но ещё важнее — куда смотрят разные группы относительно друг друга.')}
        </p>
        <DivergenceFigure />
      </Section>

      <Section title={t('Как читать')}>
        <p className="mb-4">
          {t('У каждой категории свой «характер». Сценарии ниже — не сигналы, а типичные трактовки, которые стоит проверять по всей картине рынка. Отдельные разовые операции могут искажать картину, поэтому цифры стоит читать как тенденцию.')}
        </p>
        <Interpretation
          rows={[
            {
              label: t('Физлица массово покупают'),
              meaning: t('Розничные инвесторы иногда заходят ближе к концу тренда. Бывает, что покупки идут возле локальных максимумов, но это лишь одна из возможных трактовок.'),
            },
            {
              label: t('Физлица массово продают'),
              meaning: t('Иногда совпадает с близостью локального дна: фиксация убытков в страхе дальнейшего падения. Это наблюдение, а не правило.'),
            },
            {
              label: t('Крупные банки продают акции и покупают гособлигации'),
              meaning: t('Классический уход в защитные активы: сравните рынки акций и облигаций, чтобы это увидеть.'),
            },
            {
              label: t('Все категории в одну сторону'),
              meaning: t('Редкое единодушие — обычно подтверждает сильный общий фактор: резкое движение нефти, решение по ставке, санкции.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        <ReplayTourButton tourKey="cbr-flows" indicatorPath="/cbr-flows" label={t('Показать вводный тур ещё раз')} />
      </Section>
    </MethodologyWrapper>
  );
}
