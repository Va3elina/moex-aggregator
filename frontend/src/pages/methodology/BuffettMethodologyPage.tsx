/**
 * BuffettMethodologyPage — методология индикатора Баффетта.
 *
 * Коротко: смысл индикатора и как его читать. Без границ зон, без описания
 * кнопок и настроек прогноза.
 */
import { Scale } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { MethodologyWrapper, Section, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED, ACCENT } from './infographics';

/** Зоны оценки: дёшево / справедливо / дорого + маркер «сейчас». */
function ZonesFigure() {
  const { t } = useTranslation();
  return (
    <Figure
      caption={
        <>
          {t('Горизонтальными линиями график делит значение на')} <b>{t('зоны')}</b>{t(': чем выше процент, тем дороже рынок относительно экономики. Маркер — где рынок сейчас.')}
        </>
      }
    >
      <svg viewBox="0 0 320 142" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Зоны оценки: дёшево, справедливо, дорого')}>
        <rect x="20" y="18" width="280" height="30" rx="4" fill={RED} opacity="0.14" />
        <rect x="20" y="52" width="280" height="30" rx="4" fill="currentColor" opacity="0.08" />
        <rect x="20" y="86" width="280" height="30" rx="4" fill={GREEN} opacity="0.16" />
        <line x1="20" y1="52" x2="300" y2="52" stroke="currentColor" strokeWidth="1" strokeDasharray="4 3" opacity="0.5" />
        <line x1="20" y1="86" x2="300" y2="86" stroke="currentColor" strokeWidth="1" strokeDasharray="4 3" opacity="0.5" />
        <text x="28" y="37" fontSize="11" fontWeight="700" fill={RED}>{t('Дорого')}</text>
        <text x="28" y="71" fontSize="11" fontWeight="700" fill="currentColor" opacity="0.75">{t('Справедливо')}</text>
        <text x="28" y="105" fontSize="11" fontWeight="700" fill={GREEN}>{t('Дёшево')}</text>
        <circle cx="250" cy="67" r="7" fill={ACCENT} />
        <text x="250" y="126" textAnchor="middle" fontSize="10" fontWeight="700" fill={ACCENT}>{t('сейчас')}</text>
      </svg>
    </Figure>
  );
}

/** Смысл отношения: капитализация рынка сопоставляется с размером экономики. */
function RatioFigure() {
  const { t } = useTranslation();
  return (
    <Figure
      caption={
        <>
          {t('Индикатор сравнивает')} <b>{t('стоимость всех акций')}</b> {t('(капитализацию) с')}{' '}
          <b>{t('размером экономики')}</b> {t('(ВВП или денежная масса). Результат — во сколько раз рынок больше или меньше знаменателя.')}
        </>
      }
    >
      <svg viewBox="0 0 320 120" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Капитализация делится на размер экономики')}>
        <rect x="30" y="20" width="60" height="70" rx="5" fill={ACCENT} opacity="0.85" />
        <text x="60" y="106" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={ACCENT}>{t('Капитализация')}</text>
        <text x="128" y="62" textAnchor="middle" fontSize="30" fontWeight="700" fill="currentColor" opacity="0.6">÷</text>
        <rect x="160" y="20" width="60" height="70" rx="5" fill="currentColor" opacity="0.35" />
        <text x="190" y="106" textAnchor="middle" fontSize="10.5" fontWeight="700" fill="currentColor" opacity="0.75">{t('ВВП / M2')}</text>
        <text x="252" y="62" textAnchor="middle" fontSize="26" fontWeight="700" fill="currentColor" opacity="0.6">=</text>
        <text x="288" y="58" textAnchor="middle" fontSize="18" fontWeight="800" fill={GREEN}>%</text>
        <text x="288" y="78" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">{t('оценка')}</text>
      </svg>
    </Figure>
  );
}

export default function BuffettMethodologyPage() {
  const { t } = useTranslation();
  return (
    <MethodologyWrapper icon={Scale} title={t('Индикатор Баффетта')} backTo="/buffett">
      <Section title={t('Что это')}>
        <p>
          {t('Макроэкономическая оценка фондового рынка в целом: насколько рынок дорог или дёшев относительно размера экономики страны. Назван в честь Уоррена Баффетта, который считал этот показатель одним из лучших ориентиров общего состояния рынка.')}
        </p>
        <p className="mt-3">
          {t('Стоимость всех торгуемых акций сравнивается с тем, сколько экономика производит (ВВП) или сколько в ней денег (денежная масса). ВВП меняется медленно и даёт долгосрочную картину; денежная масса чувствительнее к ставкам и быстрее реагирует на монетарные условия. В основе — публичные данные Московской биржи и официальная статистика.')}
        </p>
        <RatioFigure />
      </Section>

      <Section title={t('Зоны оценки')}>
        <p>
          {t('Горизонтальные линии делят график на зоны «дёшево», «справедливо» и «дорого». Для российского рынка эти уровни свои — они ниже американских из-за структуры экономики. Серая линия капитализации в рублях — вспомогательный контекст: видно, растёт ли отношение из-за рынка или из-за экономики.')}
        </p>
        <ZonesFigure />
      </Section>

      <Section title={t('Как читать')}>
        <p className="mb-4">
          {t('Индикатор не говорит, что делать прямо сейчас, — это долгосрочный ориентир макро-контекста.')}
        </p>
        <Interpretation
          rows={[
            {
              label: t('Значение в нижней зоне'),
              meaning: t('Рынок выглядит «дешёвым» относительно того, что производит экономика. На такие уровни долгосрочные инвесторы часто смотрят с интересом.'),
            },
            {
              label: t('Значение в верхней зоне'),
              meaning: t('Рынок выглядит «дорогим» относительно экономики. С таких уровней последующая долгосрочная доходность бывала скромнее.'),
            },
            {
              label: t('Отношение к денежной массе быстро растёт'),
              meaning: t('Может отражать переток денег из депозитов и облигаций в акции. Нередко наблюдается в фазы более низких ставок.'),
            },
            {
              label: t('Две версии расходятся'),
              meaning: t('Если относительно ВВП рынок в норме, а относительно денежной массы дорог — свободных денег может быть мало, и рост бывает ограничен ликвидностью. Если наоборот — экономика как будто догоняет рынок.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        <ReplayTourButton tourKey="buffett" indicatorPath="/buffett" label={t('Показать вводный тур ещё раз')} />
      </Section>
    </MethodologyWrapper>
  );
}
