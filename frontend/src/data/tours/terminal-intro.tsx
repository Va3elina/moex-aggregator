/**
 * terminal-intro — короткий анонс «у нас появился Терминал» (3 шага).
 *
 * Не индикаторный тур, а разовое уведомление о новом функционале: показывается
 * ОДИН раз всем, кто зашёл на сайт, и живёт в Layout (десктопная шапка), а не на
 * конкретной странице. Причина — главная (LandingPage) отдаётся ТОЛЬКО гостям на
 * десктопе: авторизованные с «/» уходят на /heatmap (App.tsx, HomeRoute), то есть
 * анонс на hero не увидели бы ровно те, у кого Pro и есть доступ к терминалу.
 *
 * Второй шаг подсвечивает саму кнопку в шапке (SandboxEntryButton, variant=pill,
 * data-tour="header-terminal") — «функционал вот по этой кнопке». Кнопку видят
 * все, включая не-Pro: по клику вместо перехода открывается UpgradeModal, так что
 * анонс уместен и для гостя.
 *
 * ФОРМАТ: показываем, а не рассказываем — как видео-демо индикаторов на лендинге.
 * Текста минимум (одна строка на шаг), основную работу делает запись экрана.
 * Файлы кладутся по конвенции лендинга (см. MediaArea в IndicatorGroup):
 *   /videos/terminal.webm + /videos/terminal.mp4 + /videos/terminal-poster.jpg
 * Пока записи нет, <video> падает по onError в схематичный мокап — анонс не
 * пустует. Появится файл — код менять НЕ нужно.
 *
 * Закрытие крестиком = пропустить: OnboardingTour при любом закрытии помечает
 * тур просмотренным (см. его docstring), поэтому повторно он не появится.
 */
import { useState } from 'react';
import type { TourStep } from '../../components/onboarding/OnboardingTour';

/** Видео-демо терминала; при отсутствии файла — схематичный мокап. */
function TerminalDemo() {
  const [failed, setFailed] = useState(false);

  if (failed) return <TerminalMock />;

  return (
    <video
      autoPlay
      muted
      loop
      playsInline
      preload="metadata"
      poster="/videos/terminal-poster.jpg"
      onError={() => setFailed(true)}
      style={{
        width: '100%',
        aspectRatio: '16 / 10',
        // Потолок по высоте: без него видео растягивало карточку выше экрана —
        // кнопки «Пропустить/Далее» уезжали за край. 66vh подобрано под ширину
        // карточки (см. width шага) так, чтобы кадр 16:10 заполнял её почти
        // без полей на типовых 1440×900 и 1920×1080.
        maxHeight: '66vh',
        objectFit: 'contain',
        borderRadius: 10,
        border: '1.5px solid var(--border-color)',
        display: 'block',
      }}
    >
      <source src="/videos/terminal.webm" type="video/webm" />
      <source src="/videos/terminal.mp4" type="video/mp4" />
    </video>
  );
}

/**
 * Заглушка на время, пока нет записи: схема рабочего стола — три окна-панели
 * с мини-графиками. Рисуем токенами темы, чтобы жило в обеих темах.
 */
function TerminalMock() {
  const pane = {
    fill: 'var(--bg-secondary)',
    stroke: 'var(--border-color)',
    strokeWidth: 1.5,
    rx: 4,
  };
  return (
    <svg
      viewBox="0 0 320 200"
      role="img"
      aria-label="Схема рабочего стола: три окна с индикаторами"
      style={{
        width: '100%',
        aspectRatio: '16 / 10',
        borderRadius: 10,
        border: '1.5px solid var(--border-color)',
        background: 'var(--bg-primary)',
        display: 'block',
      }}
    >
      {/* Полоса листов сверху — «рабочие пространства» */}
      <rect x={10} y={10} width={40} height={10} rx={3} fill="var(--accent)" />
      <rect x={56} y={10} width={30} height={10} {...pane} />
      <rect x={92} y={10} width={30} height={10} {...pane} />

      {/* Окно 1 — линия */}
      <rect x={10} y={30} width={150} height={75} {...pane} />
      <polyline
        points="20,90 40,72 60,80 80,55 100,64 120,44 150,50"
        fill="none"
        stroke="var(--accent)"
        strokeWidth={2}
      />

      {/* Окно 2 — столбики */}
      <rect x={170} y={30} width={140} height={110} {...pane} />
      <rect x={182} y={95} width={12} height={30} fill="var(--accent)" opacity={0.75} />
      <rect x={200} y={75} width={12} height={50} fill="var(--accent)" opacity={0.75} />
      <rect x={218} y={85} width={12} height={40} fill="var(--accent)" opacity={0.75} />
      <rect x={236} y={60} width={12} height={65} fill="var(--accent)" opacity={0.75} />
      <rect x={254} y={80} width={12} height={45} fill="var(--accent)" opacity={0.75} />

      {/* Окно 3 — таблица/список */}
      <rect x={10} y={115} width={150} height={75} {...pane} />
      {[128, 141, 154, 167].map((y) => (
        <rect key={y} x={20} y={y} width={110} height={5} rx={2.5} fill="var(--text-muted)" opacity={0.5} />
      ))}
    </svg>
  );
}

export const terminalIntroTour: TourStep[] = [
  {
    selector: null,
    title: 'Новое: Терминал',
    // ≈3× стандартных 360: в узкой карточке запись рабочего стола превращалась
    // в нечитаемые пиксели. Значение согласовано с maxHeight видео (66vh),
    // чтобы кадр 16:10 заполнял карточку без полей по бокам. Клампится по
    // вьюпорту в самом OnboardingTour; на мобиле не применяется (анонса там нет).
    width: 1040,
    body: (
      <>
        <TerminalDemo />
        <p style={{ marginTop: 10 }}>
          Индикаторы плавающими окнами на одном экране: рабочий стол, как в
          биржевом терминале.
        </p>
      </>
    ),
  },
  {
    selector: '[data-tour="header-terminal"]',
    title: 'Открывается отсюда',
    body: (
      <p>
        Кнопка в шапке работает с любой страницы. Входит в тариф <strong>Pro</strong>.
      </p>
    ),
    position: 'bottom',
    spotlightPadding: 10,
  },
  {
    // Вторая точка входа — кружок в панели над графиком (SandboxEntryButton
    // variant="icon"). Есть на страницах индикаторов, но НЕ на карте рынка и
    // не на лендинге: там элемента нет, и OnboardingTour сам покажет шаг
    // центрированной карточкой без подсветки — текст написан так, чтобы
    // работать в обоих случаях.
    selector: '[data-tour="chart-terminal"]',
    title: 'И прямо с графика',
    body: (
      <p>
        У индикаторов такая же кнопка стоит в панели над графиком — открыть
        терминал можно, не возвращаясь в шапку.
      </p>
    ),
    position: 'bottom',
    spotlightPadding: 10,
  },
];
