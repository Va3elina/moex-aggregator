/**
 * HeatmapMethodologyPage — подробная методология «Карты рынка».
 *
 * Без формул, без торговых идей, без англицизмов.
 * Источник — Московская биржа (публичная информация).
 * Инфографики нарисованы встроенным SVG в стиле OI-методологии
 * (currentColor для нейтральных элементов, GREEN/RED/ACCENT для смысла).
 */
import { Grid3X3, Camera } from 'lucide-react';
import { MethodologyWrapper, Section, ModeBlock, ActionBlock, Interpretation, ReplayTourButton } from './shared';
import { Figure, GREEN, RED } from './infographics';

/* ── Инфографика 1: как читать одну плитку ─────────────────────────────── */
function TileAnatomyFigure() {
  return (
    <Figure caption={<>Одна плитка = одна компания. <b>Размер</b> — вклад в рынок, <b>цвет</b> — изменение цены, внутри — тикер и процент.</>}>
      <svg viewBox="0 0 320 162" style={{ width: '100%', display: 'block' }} role="img" aria-label="Устройство одной плитки карты рынка">
        {/* большая зелёная плитка */}
        <rect x="20" y="20" width="150" height="110" rx="8" fill={GREEN} opacity="0.9" />
        <text x="95" y="72" textAnchor="middle" fontSize="22" fontWeight="800" fill="#fff">SBER</text>
        <text x="95" y="96" textAnchor="middle" fontSize="15" fontWeight="700" fill="#fff">+1,84%</text>
        {/* стрелка «размер» */}
        <line x1="20" y1="140" x2="170" y2="140" stroke="currentColor" strokeWidth="1" opacity="0.5" />
        <text x="95" y="150" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">размер = вклад в рынок</text>
        {/* выноски */}
        <line x1="170" y1="60" x2="210" y2="60" stroke="currentColor" strokeWidth="0.8" strokeDasharray="3 3" opacity="0.6" />
        <text x="214" y="63" fontSize="10.5" fontWeight="700" fill="currentColor">тикер компании</text>
        <line x1="170" y1="90" x2="210" y2="90" stroke="currentColor" strokeWidth="0.8" strokeDasharray="3 3" opacity="0.6" />
        <text x="214" y="93" fontSize="10.5" fontWeight="700" fill={GREEN}>изменение цены</text>
      </svg>
    </Figure>
  );
}

/* ── Инфографика 2: шкала цвета ─────────────────────────────────────────── */
function ColorScaleFigure() {
  const cells = [
    { c: '#7a3528', t: '−сильно' },
    { c: '#5a3128', t: '' },
    { c: '#3a2c28', t: '' },
    { c: '#2a2a2a', t: '0' },
    { c: '#2c3a30', t: '' },
    { c: '#2d5138', t: '' },
    { c: '#2d6b3f', t: '+сильно' },
  ];
  return (
    <Figure caption={<>Цвет плитки — это изменение цены. Красный = падение, зелёный = рост, тёмно-серый центр = «почти без движения». <b>Насыщеннее цвет — сильнее движение.</b></>}>
      <svg viewBox="0 0 320 96" style={{ width: '100%', display: 'block' }} role="img" aria-label="Шкала цвета от красного через серый к зелёному">
        {cells.map((cell, i) => (
          <rect key={i} x={20 + i * 40} y="24" width="38" height="30" rx="4" fill={cell.c} />
        ))}
        <text x="24" y="72" fontSize="10.5" fontWeight="700" fill={RED}>падение</text>
        <text x="160" y="72" textAnchor="middle" fontSize="10" fill="currentColor" opacity="0.75">без изменений</text>
        <text x="296" y="72" textAnchor="end" fontSize="10.5" fontWeight="700" fill={GREEN}>рост</text>
        <text x="160" y="18" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">← насыщеннее = сильнее движение →</text>
      </svg>
    </Figure>
  );
}

/* ── Инфографика 3: группировка по секторам ─────────────────────────────── */
function SectorGroupingFigure() {
  return (
    <Figure caption={<>Плитки собраны в блоки по отраслям. Внутри каждого блока — компании этого сектора. Так за секунду видно, какая отрасль сегодня зелёная, а какая красная.</>}>
      <svg viewBox="0 0 320 150" style={{ width: '100%', display: 'block' }} role="img" aria-label="Плитки сгруппированы по секторам">
        {/* Нефть и газ */}
        <text x="16" y="18" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">Нефть и газ</text>
        <rect x="16" y="24" width="82" height="48" rx="5" fill={GREEN} opacity="0.85" />
        <rect x="102" y="24" width="52" height="48" rx="5" fill={GREEN} opacity="0.55" />
        {/* Банки */}
        <text x="170" y="18" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">Банки</text>
        <rect x="170" y="24" width="60" height="48" rx="5" fill={RED} opacity="0.8" />
        <rect x="234" y="24" width="70" height="48" rx="5" fill={GREEN} opacity="0.5" />
        {/* Металлы */}
        <text x="16" y="92" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">Металлы</text>
        <rect x="16" y="98" width="66" height="42" rx="5" fill={RED} opacity="0.85" />
        <rect x="86" y="98" width="68" height="42" rx="5" fill={RED} opacity="0.55" />
        {/* Потребительский */}
        <text x="170" y="92" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.75">Потребительский</text>
        <rect x="170" y="98" width="60" height="42" rx="5" fill={GREEN} opacity="0.7" />
        <rect x="234" y="98" width="70" height="42" rx="5" fill="#2a2a2a" />
      </svg>
    </Figure>
  );
}

/* ── Инфографика 4: размер = вклад в рынок ──────────────────────────────── */
function SizeWeightFigure() {
  return (
    <Figure caption={<>Размер плитки пропорционален выбранной величине: «Капитализация» — стоимость компании, «Оборот» — сколько наторговали. Тяжеловесы занимают больше места.</>}>
      <svg viewBox="0 0 320 130" style={{ width: '100%', display: 'block' }} role="img" aria-label="Крупные компании занимают больше места">
        <rect x="16" y="20" width="120" height="94" rx="7" fill="currentColor" opacity="0.28" />
        <text x="76" y="72" textAnchor="middle" fontSize="13" fontWeight="800" fill="currentColor">тяжеловес</text>
        <rect x="144" y="20" width="72" height="94" rx="7" fill="currentColor" opacity="0.2" />
        <text x="180" y="72" textAnchor="middle" fontSize="10" fontWeight="700" fill="currentColor" opacity="0.85">средний</text>
        <rect x="224" y="20" width="40" height="46" rx="5" fill="currentColor" opacity="0.14" />
        <rect x="224" y="70" width="40" height="44" rx="5" fill="currentColor" opacity="0.14" />
        <text x="270" y="46" fontSize="9.5" fill="currentColor" opacity="0.7">мелкие</text>
        <text x="270" y="96" fontSize="9.5" fill="currentColor" opacity="0.7">эшелоны</text>
      </svg>
    </Figure>
  );
}

export default function HeatmapMethodologyPage() {
  return (
    <MethodologyWrapper icon={Grid3X3} title="Карта рынка" backTo="/heatmap">
      <Section title="Что это">
        <p>
          Все акции Московской биржи в виде прямоугольных плиток на одном
          экране. Каждая плитка — одна компания. Плитки сгруппированы по
          отраслям. За две-три секунды видно общее состояние рынка: сегодня
          сила или слабость, какие сектора лидируют и кто двигает индексы.
        </p>
        <p className="mt-3">
          Карта устроена так, что несёт <strong>две величины одновременно</strong>:
          размер плитки — вклад компании в рынок, цвет плитки — её изменение
          цены за выбранный период.
        </p>
        <TileAnatomyFigure />
      </Section>

      <Section title="Источник данных">
        <p>
          Данные — с Московской биржи.
        </p>
      </Section>

      <Section title="Элементы управления">
        <p className="mb-4">
          Над картой стоит ряд переключателей. Разберём каждый — что он делает
          и что означает каждый его вариант.
        </p>

        <p className="mb-2" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          Вселенная акций
        </p>
        <div className="space-y-3">
          <ModeBlock
            title="Индекс IMOEX"
            desc="Только акции из основного индекса Московской биржи — крупнейшие компании. Их около пятидесяти, но они покрывают большую часть рынка по капитализации. Карта компактная и вся читается."
          />
          <ModeBlock
            title="Все акции"
            desc="Вся вселенная Московской биржи — сотни компаний, включая второй и третий эшелоны. Видно всё, но самые мелкие плитки могут стать нечитаемыми. Доступно на платных тарифах."
          />
        </div>

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          Группировка
        </p>
        <div className="space-y-3">
          <ModeBlock
            title="По секторам"
            desc="Плитки собраны в блоки по отраслям: нефть и газ, банки, металлы, потребительский сектор и так далее. Над каждым блоком — название отрасли. Легче увидеть отраслевые движения."
          />
          <ModeBlock
            title="Без группировки"
            desc="Отрасли отключены, все плитки лежат вместе и отсортированы только по размеру. Крупнейшие компании собираются в одном углу — удобно, когда важен именно размер, а не сектор."
          />
        </div>
        <SectorGroupingFigure />

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          Размер плитки
        </p>
        <div className="space-y-3">
          <ModeBlock
            title="Капитализация"
            desc="Площадь плитки пропорциональна рыночной стоимости компании. Крупнейшие эмитенты занимают больше места. Величина устойчивая — меняется медленно."
          />
          <ModeBlock
            title="Оборот"
            desc="Размер зависит от того, сколько бумаги наторговали за выбранный период. Подсвечивает, где сейчас сосредоточена активность. Величина подвижная — может резко вырасти на новостях."
          />
        </div>
        <SizeWeightFigure />

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          Период изменения
        </p>
        <p className="mb-3">
          Определяет, за какой срок считается изменение цены, которое показывает
          цвет плитки. Меняется только окраска — размер и группировка остаются
          прежними.
        </p>
        <div className="space-y-3">
          <ModeBlock title="1Д" desc="Изменение за сегодняшний торговый день, от закрытия прошлого дня." />
          <ModeBlock title="1Н" desc="Изменение за последнюю неделю." />
          <ModeBlock title="1М" desc="Изменение за последний месяц." />
          <ModeBlock title="1Г" desc="Изменение за последний год." />
        </div>

        <p className="mb-2 mt-6" style={{ color: 'var(--text-primary)', fontWeight: 600 }}>
          Снимок карты
        </p>
        <div className="space-y-3">
          <ActionBlock
            icon={Camera}
            title="Снимок карты"
            desc="Кнопка-камера сохраняет текущий вид карты в картинку — со всеми плитками, подписями режима и периода и водяным знаком сайта. Удобно для отчётов."
          />
        </div>
      </Section>

      <Section title="Как читать цвет и цифры">
        <p className="mb-3">
          Внутри каждой достаточно крупной плитки написан тикер компании и её
          процент изменения за выбранный период. Сам цвет плитки повторяет этот
          же процент:
        </p>
        <ul className="mt-1 space-y-2 list-none">
          <li>
            <span style={{ color: GREEN, fontWeight: 600 }}>Зелёная</span>
            {' — '}цена выросла. Чем насыщеннее зелёный, тем больше рост.
          </li>
          <li>
            <span style={{ color: RED, fontWeight: 600 }}>Красная</span>
            {' — '}цена упала. Чем насыщеннее красный, тем сильнее падение.
          </li>
          <li>
            <span style={{ color: 'var(--text-muted)', fontWeight: 600 }}>Тёмно-серая</span>
            {' — '}цена почти не изменилась.
          </li>
        </ul>
        <ColorScaleFigure />
        <p className="mt-4">
          Насыщенность цвета подстраивается под выбранный период, чтобы карта
          оставалась контрастной и на дне, и на годовом горизонте.
        </p>
        <p className="mt-3">
          Если навести курсор на плитку, появится карточка с подробностями:
          тикер и полное название компании, текущая цена в рублях и изменения
          сразу за все четыре периода — день, неделю, месяц и год. Это позволяет
          сравнить, как бумага вела себя на разных горизонтах, не переключая
          период у всей карты.
        </p>
      </Section>

      <Section title="Как интерпретировать карту">
        <p className="mb-4">
          Карта показывает <strong>две вещи одновременно</strong>: размер плитки —
          вклад компании в рынок, цвет — её изменение. Это даёт мгновенную
          картину «где деньги и что с ними происходит».
        </p>
        <Interpretation
          rows={[
            {
              label: 'Большие плитки тёмно-красные',
              meaning:
                'Тяжеловесы (Сбер, Лукойл, Газпром) снижаются. Их вес в рынке большой, поэтому индекс в такие дни чаще тоже в минусе, даже если средние акции в плюсе.',
            },
            {
              label: 'Море красного, но яркие зелёные пятна',
              meaning:
                'Общий день слабый, но отдельные сектора или акции идут против общего движения. Часто такое бывает на отчётности, новостях по конкретной бумаге или дивидендах.',
            },
            {
              label: 'Один сектор полностью зелёный',
              meaning:
                'Отраслевой рост — нередко связан с новостями (цены на нефть, изменения регулирования, сделки). Внутри сектора движение обычно ведут крупнейшие бумаги.',
            },
            {
              label: 'Большие плитки нейтральные (тёмно-серые)',
              meaning:
                'Тяжеловесы почти стоят, а движение сосредоточено в малых эшелонах. Индекс меняется слабо, хотя в малой капитализации активности бывает много.',
            },
            {
              label: 'Сравнение периодов 1Д, 1Н, 1М',
              meaning:
                'Переключи период и сравни. Красный за день при зелёном за месяц может говорить о локальном откате внутри более длинного роста; красный и там и там — о более затяжном снижении.',
            },
            {
              label: 'Плитка резко выросла при переключении «Капитализация → Оборот»',
              meaning:
                'Большая плитка оборота у не самой крупной бумаги указывает на повышенную активность — нередко вокруг какой-то новости по компании.',
            },
          ]}
        />
      </Section>

      <Section title="Вводный тур">
        <p className="mb-4">
          Если хочешь повторить пошаговый гайд по переключателям и карте,
          который показывался при первом открытии страницы — нажми кнопку ниже.
        </p>
        <ReplayTourButton tourKey="heatmap" indicatorPath="/heatmap" label="Показать вводный тур ещё раз" />
      </Section>
    </MethodologyWrapper>
  );
}
