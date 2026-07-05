/**
 * Тур по вкладке «Скринер сигналов» (/oi?tab=screener). Отдельный от тура
 * графика (ключ 'oi-screener') — стартует при первом открытии скринера, не
 * пересекается с чартовым туром. Шаг «Как читать позицию» несёт встроенную
 * инфографику-комету для наглядности (просьба Вадима — «ввести в тур
 * инфографику»).
 */
import type { TourStep } from '../../components/onboarding/OnboardingTour';

/** Мини-легенда кометы прямо в шаге тура — как читать колонку «Позиция». */
function CometLegend() {
  const G = '#4a9959', R = '#c0492a';
  return (
    <svg viewBox="0 0 300 140" style={{ width: '100%', maxWidth: 320, display: 'block', margin: '8px auto 4px' }} role="img" aria-label="Как читать комету позиции">
      {/* зоны */}
      <rect x="20" y="52" width="120" height="16" rx="4" fill={R} opacity="0.14" />
      <rect x="140" y="52" width="140" height="16" rx="4" fill={G} opacity="0.16" />
      {/* ось + ноль */}
      <line x1="20" y1="60" x2="280" y2="60" stroke="currentColor" strokeWidth="1.4" opacity="0.5" />
      <line x1="140" y1="44" x2="140" y2="76" stroke="currentColor" strokeWidth="1.4" strokeDasharray="3 3" opacity="0.7" />
      <text x="140" y="90" textAnchor="middle" fontSize="10" fill="currentColor" opacity="0.75">0 · нейтрально</text>
      {/* подписи ног */}
      <text x="70" y="108" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={R}>← ШОРТ</text>
      <text x="215" y="108" textAnchor="middle" fontSize="10.5" fontWeight="700" fill={G}>ЛОНГ →</text>
      {/* комета: хвост (вчера, слева) → голова (сегодня, справа), крупная = сильный ×N.
          Хвост удлинён, чтобы подписи «вчера»/«сегодня» не наслаивались. */}
      <polygon points="214,52 148,58.6 148,61.4 214,68" fill={G} opacity="0.78" />
      <circle cx="214" cy="60" r="8" fill={G} />
      {/* аннотации — разнесены по горизонтали, чтобы не пересекались */}
      <line x1="148" y1="38" x2="148" y2="55" stroke="currentColor" strokeWidth="0.8" strokeDasharray="2 2" opacity="0.5" />
      <text x="148" y="32" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">вчера</text>
      <line x1="214" y1="35" x2="214" y2="50" stroke={G} strokeWidth="1" opacity="0.7" />
      <text x="214" y="29" textAnchor="middle" fontSize="10" fontWeight="700" fill={G}>сегодня</text>
      <text x="252" y="128" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">размер = сила ×N</text>
      <text x="80" y="128" textAnchor="middle" fontSize="9.5" fill="currentColor" opacity="0.7">длина хвоста = сдвиг за день</text>
    </svg>
  );
}

export const oiScreenerTourSteps: TourStep[] = [
  {
    selector: null,
    title: 'Скринер сигналов',
    body: (
      <>
        <p style={{ marginBottom: 8 }}>
          Это лента <strong>резких движений открытого интереса</strong> — как наши
          Telegram-алерты, только на экране. Показывает, по каким фьючерсам толпа
          (физлица или юрлица) за день <strong>резко</strong> изменила чистую позицию.
        </p>
        <p>Данные дневные (обновляются на следующий день после торгов). Это описание
          факта, а не прогноз цены.</p>
      </>
    ),
  },
  {
    selector: '[data-tour="screener-tabs"]',
    title: 'Две вкладки',
    body: (
      <p>
        Слева — привычный график открытого интереса, справа — этот скринер.
        Переключаются мгновенно; вкладка запоминается в адресе страницы.
      </p>
    ),
    position: 'bottom',
  },
  {
    selector: '[data-tour="screener-toolbar"]',
    title: 'Фильтры',
    body: (
      <>
        <p style={{ marginBottom: 8 }}>Сверху — управление лентой:</p>
        <ul style={{ margin: 0, paddingLeft: 18, lineHeight: 1.5 }}>
          <li><strong>Физлица / Юрлица</strong> — чью позицию смотрим.</li>
          <li><strong>Порог ≥2× / ≥3× / ≥5×</strong> — насколько сильным должно быть
            движение, чтобы попасть в ленту (× — во сколько раз резче обычного).</li>
          <li><strong>Тип актива</strong> и <strong>★ Избранные</strong> — сузить список.</li>
        </ul>
      </>
    ),
    position: 'bottom',
  },
  {
    selector: null,
    title: 'Как читать «Позицию»',
    body: (
      <>
        <p style={{ marginBottom: 4 }}>
          Колонка «Позиция» — это ось перекоса: слева полный шорт, справа полный
          лонг, по центру ноль (поровну). Комета показывает всё сразу:
        </p>
        <CometLegend />
        <ul style={{ margin: '4px 0 0', paddingLeft: 18, lineHeight: 1.5 }}>
          <li><strong>Где голова</strong> — где толпа стоит сейчас (у края = «полный
            лонг/шорт»).</li>
          <li><strong>Хвост</strong> — откуда пришли за день (его длина = размер сдвига).</li>
          <li><strong>Размер головы</strong> — сила сигнала ×N: 2× маленькая, 5× крупная.</li>
        </ul>
      </>
    ),
  },
  {
    selector: '[data-tour="screener-table"]',
    title: 'Чтение сигнала',
    body: (
      <>
        <p style={{ marginBottom: 8 }}>
          В каждой строке — <strong>×N</strong> (во сколько раз движение резче обычного,
          оранжевым — самые сильные) и <strong>трактовка</strong>: «резко нарастили/
          сократили длинную/короткую позицию».
        </p>
        <p>
          <strong>Клик по строке</strong> открывает график этого актива с теми же
          настройками (та же группа, дневной таймфрейм, год истории).
        </p>
      </>
    ),
    position: 'top',
  },
  {
    selector: '[data-tour="screener-favorites"]',
    title: 'Избранное и алерты',
    body: (
      <>
        <p style={{ marginBottom: 8 }}>
          Звёздочка в строке добавляет актив в избранное, а этот тумблер оставляет в
          ленте только их.
        </p>
        <p>
          При добавлении в избранное предложим <strong>поставить алерт</strong> — тогда
          о резком движении сообщим сами, следить вручную не придётся.
        </p>
      </>
    ),
    position: 'bottom',
  },
];
