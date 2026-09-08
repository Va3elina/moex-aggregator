/**
 * HeatmapMethodologyPage — подробная методология «Карты рынка».
 *
 * Без формул, без торговых идей, без англицизмов.
 * Источник — Московская биржа (публичная информация).
 * Инфографики нарисованы встроенным SVG в стиле OI-методологии
 * (currentColor для нейтральных элементов, GREEN/RED/ACCENT для смысла).
 */
import { useTranslation } from 'react-i18next';
import { Grid3X3, Camera } from 'lucide-react';
import { MethodologyWrapper, Section, ModeBlock, ActionBlock, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED } from './infographics';

/* ── Инфографика 1: как читать одну плитку ─────────────────────────────── */
function TileAnatomyFigure() {
  const { t } = useTranslation();
  return (
    <Figure caption={<>{t('Одна плитка = одна компания.')} <b>{t('Размер')}</b> — {t('вклад в рынок,')} <b>{t('цвет')}</b> — {t('изменение цены, внутри — тикер и процент.')}</>}>
      <svg viewBox="0 0 320 162" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Устройство одной плитки карты рынка')}>
        {/* большая зелёная плитка */}
        <rect x="20" y="20" width="150" height="110" rx="8" fill={GREEN} opacity="0.9" />
        <text x="95" y="72" textAnchor="middle" fontSize="22" fontWeight="800" fill="#fff">SBER</text>
        <text x="95" y="96" textAnchor="middle" fontSize="15" fontWeight="700" fill="#fff">{t('+1,84%')}</text>
        {/* стрелка «размер» */}
        <line x1="20" y1="140" x2="170" y2="140" stroke="currentColor" strokeWidth="1" opacity="0.5" />
        <text x="95" y="150" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">{t('размер = вклад в рынок')}</text>
        {/* выноски */}
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
        {/* Нефть и газ */}
        <text x="16" y="18" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Нефть и газ')}</text>
        <rect x="16" y="24" width="82" height="48" rx="5" fill={GREEN} opacity="0.85" />
        <rect x="102" y="24" width="52" height="48" rx="5" fill={GREEN} opacity="0.55" />
        {/* Банки */}
        <text x="170" y="18" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Банки')}</text>
        <rect x="170" y="24" width="60" height="48" rx="5" fill={RED} opacity="0.8" />
        <rect x="234" y="24" width="70" height="48" rx="5" fill={GREEN} opacity="0.5" />
        {/* Металлы */}
        <text x="16" y="92" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Металлы')}</text>
        <rect x="16" y="98" width="66" height="42" rx="5" fill={RED} opacity="0.85" />
        <rect x="86" y="98" width="68" height="42" rx="5" fill={RED} opacity="0.55" />
        {/* Потребительский */}
        <text x="170" y="92" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">{t('Потребительский')}</text>
        <rect x="170" y="98" width="60" height="42" rx="5" fill={GREEN} opacity="0.7" />
        <rect x="234" y="98" width="70" height="42" rx="5" fill="#2a2a2a" />
      </svg>
    </Figure>
  );
}

/* ── Инфографика 4: размер = вклад в рынок ──────────────────────────────── */
function SizeWeightFigure() {
  const { t } = useTranslation();
  return (
    <Figure caption={<>{t('Размер плитки пропорционален выбранной величине: «Капитализация» — стоимость компании, «Оборот» — сколько наторговали. Тяжеловесы занимают больше места.')}</>}>
      <svg viewBox="0 0 320 130" style={{ width: '100%', display: 'block' }} role="img" aria-label={t('Крупные компании занимают больше места')}>
        <rect x="16" y="20" width="120" height="94" rx="7" fill="currentColor" opacity="0.28" />
        <text x="76" y="72" textAnchor="middle" fontSize="13" fontWeight="800" fill="currentColor">{t('тяжеловес')}</text>
        <rect x="144" y="20" width="72" height="94" rx="7" fill="currentColor" opacity="0.2" />
        <text x="180" y="72" textAnchor="middle" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.85">{t('средний')}</text>
        <rect x="224" y="20" width="40" height="46" rx="5" fill="currentColor" opacity="0.14" />
        <rect x="224" y="70" width="40" height="44" rx="5" fill="currentColor" opacity="0.14" />
        <text x="270" y="46" fontSize="9.5" fill="currentColor" opacity="0.7">{t('мелкие')}</text>
        <text x="270" y="96" fontSize="9.5" fill="currentColor" opacity="0.7">{t('эшелоны')}</text>
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
          {t('Все акции Московской биржи в виде прямоугольных плиток на одном экране. Каждая плитка — одна компания. Плитки сгруппированы по отраслям. За две-три секунды видно общее состояние рынка: сегодня сила или слабость, какие сектора лидируют и кто двигает индексы.')}
        </p>
        <p className="mt-3">
          {t('Карта устроена так, что несёт')} <strong>{t('две величины одновременно')}</strong>: {t('размер плитки — вклад компании в рынок, цвет плитки — её изменение цены за выбранный период.')}
        </p>
        <TileAnatomyFigure />
      </Section>

      <Section title={t('Источник данных')}>
        <p>
          {t('Данные — с Московской биржи.')}
        </p>
      </Section>

      <Section title={t('Элементы управления')}>
        <p className="mb-4">
          {t('Над картой стоит ряд переключателей. Разберём каждый — что он делает и что означает каждый его вариант.')}
        </p>

        <p className="mb-2" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          {t('Вселенная акций')}
        </p>
        <div className="space-y-3">
          <ModeBlock
            title={t('Индекс IMOEX')}
            desc={t('Только акции из основного индекса Московской биржи — крупнейшие компании. Их около пятидесяти, но они покрывают большую часть рынка по капитализации. Карта компактная и вся читается.')}
          />
          <ModeBlock
            title={t('Все акции')}
            desc={t('Вся вселенная Московской биржи — сотни компаний, включая второй и третий эшелоны. Видно всё, но самые мелкие плитки могут стать нечитаемыми. Доступно на платных тарифах.')}
          />
        </div>

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          {t('Группировка')}
        </p>
        <div className="space-y-3">
          <ModeBlock
            title={t('По секторам')}
            desc={t('Плитки собраны в блоки по отраслям: нефть и газ, банки, металлы, потребительский сектор и так далее. Над каждым блоком — название отрасли. Легче увидеть отраслевые движения.')}
          />
          <ModeBlock
            title={t('Без группировки')}
            desc={t('Отрасли отключены, все плитки лежат вместе и отсортированы только по размеру. Крупнейшие компании собираются в одном углу — удобно, когда важен именно размер, а не сектор.')}
          />
        </div>
        <SectorGroupingFigure />

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          {t('Размер плитки')}
        </p>
        <div className="space-y-3">
          <ModeBlock
            title={t('Капитализация')}
            desc={t('Площадь плитки пропорциональна рыночной стоимости компании. Крупнейшие эмитенты занимают больше места. Величина устойчивая — меняется медленно.')}
          />
          <ModeBlock
            title={t('Оборот')}
            desc={t('Размер зависит от того, сколько бумаги наторговали за выбранный период. Подсвечивает, где сейчас сосредоточена активность. Величина подвижная — может резко вырасти на новостях.')}
          />
        </div>
        <SizeWeightFigure />

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          {t('Период изменения')}
        </p>
        <p className="mb-3">
          {t('Определяет, за какой срок считается изменение цены, которое показывает цвет плитки. Меняется только окраска — размер и группировка остаются прежними.')}
        </p>
        <div className="space-y-3">
          <ModeBlock title={t('1Д')} desc={t('Изменение за сегодняшний торговый день, от закрытия прошлого дня.')} />
          <ModeBlock title={t('1Н')} desc={t('Изменение за последнюю неделю.')} />
          <ModeBlock title={t('1М')} desc={t('Изменение за последний месяц.')} />
          <ModeBlock title={t('1Г')} desc={t('Изменение за последний год.')} />
        </div>

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          {t('Снимок карты')}
        </p>
        <div className="space-y-3">
          <ActionBlock
            icon={Camera}
            title={t('Снимок карты')}
            desc={t('Кнопка-камера сохраняет текущий вид карты в картинку — со всеми плитками, подписями режима и периода и водяным знаком сайта. Удобно для отчётов.')}
          />
        </div>
      </Section>

      <Section title={t('Как читать цвет и цифры')}>
        <p className="mb-3">
          {t('Внутри каждой достаточно крупной плитки написан тикер компании и её процент изменения за выбранный период. Сам цвет плитки повторяет этот же процент:')}
        </p>
        <ul className="mt-1 space-y-2 list-none">
          <li>
            <span style={{ color: GREEN, fontWeight: 600 }}>{t('Зелёная')}</span>
            {' — '}{t('цена выросла. Чем насыщеннее зелёный, тем больше рост.')}
          </li>
          <li>
            <span style={{ color: RED, fontWeight: 600 }}>{t('Красная')}</span>
            {' — '}{t('цена упала. Чем насыщеннее красный, тем сильнее падение.')}
          </li>
          <li>
            <span style={{ color: 'var(--text-muted)', fontWeight: 600 }}>{t('Тёмно-серая')}</span>
            {' — '}{t('цена почти не изменилась.')}
          </li>
        </ul>
        <ColorScaleFigure />
        <p className="mt-4">
          {t('Насыщенность цвета подстраивается под выбранный период, чтобы карта оставалась контрастной и на дне, и на годовом горизонте.')}
        </p>
        <p className="mt-3">
          {t('Если навести курсор на плитку, появится карточка с подробностями: тикер и полное название компании, текущая цена в рублях и изменения сразу за все четыре периода — день, неделю, месяц и год. Это позволяет сравнить, как бумага вела себя на разных горизонтах, не переключая период у всей карты.')}
        </p>
      </Section>

      <Section title={t('Как интерпретировать карту')}>
        <p className="mb-4">
          {t('Карта показывает')} <strong>{t('две вещи одновременно')}</strong>: {t('размер плитки — вклад компании в рынок, цвет — её изменение. Это даёт мгновенную картину «где деньги и что с ними происходит».')}
        </p>
        <Interpretation
          rows={[
            {
              label: t('Большие плитки тёмно-красные'),
              meaning: t('Тяжеловесы (Сбер, Лукойл, Газпром) снижаются. Их вес в рынке большой, поэтому индекс в такие дни чаще тоже в минусе, даже если средние акции в плюсе.'),
            },
            {
              label: t('Море красного, но яркие зелёные пятна'),
              meaning: t('Общий день слабый, но отдельные сектора или акции идут против общего движения. Часто такое бывает на отчётности, новостях по конкретной бумаге или дивидендах.'),
            },
            {
              label: t('Один сектор полностью зелёный'),
              meaning: t('Отраслевой рост — нередко связан с новостями (цены на нефть, изменения регулирования, сделки). Внутри сектора движение обычно ведут крупнейшие бумаги.'),
            },
            {
              label: t('Большие плитки нейтральные (тёмно-серые)'),
              meaning: t('Тяжеловесы почти стоят, а движение сосредоточено в малых эшелонах. Индекс меняется слабо, хотя в малой капитализации активности бывает много.'),
            },
            {
              label: t('Сравнение периодов 1Д, 1Н, 1М'),
              meaning: t('Переключи период и сравни. Красный за день при зелёном за месяц может говорить о локальном откате внутри более длинного роста; красный и там и там — о более затяжном снижении.'),
            },
            {
              label: t('Плитка резко выросла при переключении «Капитализация → Оборот»'),
              meaning: t('Большая плитка оборота у не самой крупной бумаги указывает на повышенную активность — нередко вокруг какой-то новости по компании.'),
            },
          ]}
        />
      </Section>

      <Section title={t('Вводный тур')}>
        <p className="mb-4">
          {t('Если хочешь повторить пошаговый гайд по переключателям и карте, который показывался при первом открытии страницы — нажми кнопку ниже.')}
        </p>
        <ReplayTourButton tourKey="heatmap" indicatorPath="/heatmap" />
      </Section>
    </MethodologyWrapper>
  );
}
