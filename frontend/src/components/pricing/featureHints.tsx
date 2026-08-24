/**
 * featureHints — TradingView-style подсказки к пунктам карточек тарифов.
 *
 * Наведение (десктоп) или тап (мобилка) на строку фичи открывает всплывающую
 * карточку: короткое пояснение в 1-2 предложения, а у фич, которые можно
 * показать, сверху — анимированный мини-мокап интерфейса на чистом CSS
 * (стили .fh-* в index.css). Анимации проигрываются один раз при открытии
 * (popover монтируется заново) и уважают prefers-reduced-motion: без движения
 * мокап остаётся в финальном статичном состоянии.
 *
 * Попап уходит порталом в document.body — карточки тарифов и чипы имеют
 * transform-состояния (editorial-press), absolute-попап внутри был бы заперт
 * в их stacking context (тот же приём, что PeriodSettingsPopover).
 */
import { useCallback, useEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import {
  Grid3X3, BarChart3, Wallet, Activity, Scale,
  CalendarDays, Banknote, LayoutGrid, Briefcase, Bell,
} from 'lucide-react';

// ───────────────────────────────────────────────────────────────
// Визуальные мини-мокапы (HTML+CSS, темо-зависимые через CSS-vars)
// ───────────────────────────────────────────────────────────────

/** Скринер сигналов: строки ленты въезжают одна за другой. */
function VisualScreener() {
  const rows: Array<[string, string, boolean]> = [
    ['SR', '×6 к дневному шагу', true],
    ['GZ', '×4 к дневному шагу', false],
    ['BR', '×3 к дневному шагу', true],
  ];
  return (
    <div className="fh-visual">
      {rows.map(([tk, note, up], i) => (
        <div key={tk} className="fh-scr-row" style={{ animationDelay: `${0.15 + i * 0.22}s` }}>
          <span className={`fh-scr-arrow ${up ? 'fh-up' : 'fh-down'}`}>{up ? '▲' : '▼'}</span>
          <span className="fh-scr-ticker">{tk}</span>
          <span className="fh-scr-note">{note}</span>
        </div>
      ))}
    </div>
  );
}

/** Юрлица и число трейдеров: чип «Физлица» сменяется на «Юрлица», линия перерисовывается. */
function VisualYur() {
  return (
    <div className="fh-visual">
      <div className="fh-yur-chips">
        <span className="fh-yur-chip fh-yur-chip--fiz">Физлица</span>
        <span className="fh-yur-chip fh-yur-chip--yur">Юрлица</span>
      </div>
      <svg viewBox="0 0 240 56" className="fh-yur-chart" aria-hidden="true">
        <polyline className="fh-yur-line fh-yur-line--fiz" points="4,14 40,22 76,18 112,30 148,26 184,40 232,46" />
        <polyline className="fh-yur-line fh-yur-line--yur" points="4,44 40,38 76,42 112,28 148,30 184,16 232,10" />
      </svg>
    </div>
  );
}

/** Фильтры сезонности: тумблер включается, столбик-выброс сжимается к медиане. */
function VisualSeasonality() {
  const bars = [26, 34, 22, 30, 24, 32];
  return (
    <div className="fh-visual">
      <div className="fh-sea-toggle-row">
        <span className="fh-sea-switch"><span className="fh-sea-knob" /></span>
        <span className="fh-sea-label">Без выбросов</span>
      </div>
      <div className="fh-sea-bars">
        {bars.map((h, i) => (
          <span key={i} className="fh-sea-bar" style={{ height: h }} />
        ))}
        <span className="fh-sea-bar fh-sea-bar--outlier" />
        <span className="fh-sea-bar" style={{ height: 28 }} />
      </div>
    </div>
  );
}

/** Свой набор фондов: галочки проставляются по списку, одна строка остаётся пустой. */
function VisualFunds() {
  const rows: Array<[string, boolean]> = [
    ['LQDT · Ликвидность', true],
    ['SBMX · Топ российских акций', true],
    ['TMOS · Крупнейшие компании', false],
    ['GOLD · Золото', true],
  ];
  return (
    <div className="fh-visual">
      {rows.map(([name, on], i) => (
        <div key={name} className="fh-fund-row">
          <span className={`fh-fund-box ${on ? 'fh-fund-box--on' : ''}`} style={on ? { animationDelay: `${0.3 + i * 0.25}s` } : undefined}>
            {on && <svg viewBox="0 0 10 8" className="fh-fund-check" style={{ animationDelay: `${0.3 + i * 0.25}s` }}><path d="M1 4 L4 7 L9 1" /></svg>}
          </span>
          <span className="fh-fund-name">{name}</span>
        </div>
      ))}
    </div>
  );
}

/** Свой период сравнения: подсветка диапазона растягивается по месяцам. */
function VisualRange() {
  const months = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг'];
  return (
    <div className="fh-visual">
      <div className="fh-rng-strip">
        <span className="fh-rng-fill" />
        {months.map((m) => <span key={m} className="fh-rng-cell">{m}</span>)}
      </div>
      <div className="fh-rng-caption">Февраль — Июнь</div>
    </div>
  );
}

/** Алерты: колокольчик качается, тост со срабатыванием выезжает снизу. */
function VisualAlerts() {
  return (
    <div className="fh-visual fh-al-wrap">
      <Bell size={22} className="fh-al-bell" />
      <div className="fh-al-toast">
        <span className="fh-al-dot" />
        <span>SR: чистая позиция физлиц пересекла 0</span>
      </div>
    </div>
  );
}

/** Терминал: панели рабочего стола собираются на экране одна за другой. */
function VisualTerminal() {
  return (
    <div className="fh-visual fh-term-grid">
      {[0, 1, 2, 3].map((i) => (
        <span key={i} className="fh-term-panel" style={{ animationDelay: `${0.12 + i * 0.16}s` }}>
          <span className="fh-term-bar" style={{ width: '55%' }} />
          <span className="fh-term-spark" />
        </span>
      ))}
    </div>
  );
}

/** Все 9 индикаторов: сетка иконок разделов проявляется каскадом. */
function VisualIndicators() {
  const icons = [BarChart3, Wallet, Briefcase, Activity, Scale, CalendarDays, Banknote, Grid3X3, LayoutGrid];
  return (
    <div className="fh-visual fh-ind-grid">
      {icons.map((Icon, i) => (
        <span key={i} className="fh-ind-tile" style={{ animationDelay: `${0.08 + i * 0.07}s` }}>
          <Icon size={15} />
        </span>
      ))}
    </div>
  );
}

const VISUALS: Record<string, () => ReactNode> = {
  indicators: VisualIndicators,
  screener: VisualScreener,
  yur: VisualYur,
  seasonality: VisualSeasonality,
  funds: VisualFunds,
  range: VisualRange,
  alerts: VisualAlerts,
  terminal: VisualTerminal,
};

// ───────────────────────────────────────────────────────────────
// Тексты подсказок
// ───────────────────────────────────────────────────────────────

export interface FeatureHint {
  title: string;
  text: string;
  /** Ключ анимированного мокапа. Нет ключа — подсказка только текстом. */
  visual?: keyof typeof VISUALS & string;
}

export const FEATURE_HINTS: Record<string, FeatureHint> = {
  indicators: {
    title: 'Все инструменты Фрейма',
    text: '9 индикаторов: Открытые позиции, Деньги в фондах, Сделки фондов, Сила рынка, Сезонность, Поток капитала, индикатор Баффетта, Карта рынка и Каталог фондов.',
    visual: 'indicators',
  },
  assets: {
    title: 'Все активы и таймфреймы',
    text: 'Больше 200 фьючерсов в Открытых позициях, акции, фонды и индексы в остальных разделах. Таймфреймы от 5 минут до дня.',
  },
  history: {
    title: 'Вся история данных',
    text: 'Глубина графиков не ограничена тарифом — по многим активам доступно больше 15 лет данных.',
  },
  delay: {
    title: 'Открытые позиции без задержки',
    text: 'Свежие данные биржи появляются сразу после публикации. На бесплатном тарифе раздел отстаёт на 24 часа.',
  },
  yur: {
    title: 'Юрлица и число трейдеров',
    text: 'Позиции юридических лиц и количество трейдеров в каждой группе. Видно, куда встают крупные игроки и насколько широка позиция физлиц.',
    visual: 'yur',
  },
  screener: {
    title: 'Скринер сигналов',
    text: 'Лента резких изменений позиций по всему рынку за день. Готовый список того, что стоит открыть на графике прямо сейчас.',
    visual: 'screener',
  },
  seasonality: {
    title: 'Фильтры сезонности',
    text: 'Медиана вместо среднего убирает влияние кризисных лет, пересчёт с учётом дивидендов — провалы отсечек. Остаётся чистая сезонность.',
    visual: 'seasonality',
  },
  funds: {
    title: 'Свой набор фондов',
    text: 'Своя подборка вместо целой категории: одна УК, только крупные или без индексных. Работает в Деньгах в фондах и Сделках фондов.',
    visual: 'funds',
  },
  range: {
    title: 'Свой период сравнения',
    text: 'Произвольный диапазон месяцев вместо пресетов 1М / 6М / 1Г в Сделках фондов. Любые отрезки истории на выбор.',
    visual: 'range',
  },
  alerts: {
    title: 'Алерты в Telegram',
    text: 'Личные условия на цену, открытый интерес и потоки фондов. Срабатывания приходят в Telegram и на сайт.',
    visual: 'alerts',
  },
  terminal: {
    title: 'Терминал',
    text: 'Рабочий стол с панелями индикаторов на одном экране: своя раскладка, вкладки, несколько графиков рядом.',
    visual: 'terminal',
  },
  download: {
    title: 'Скачивание данных',
    text: 'Выгрузка рядов в CSV и Excel и доступ по API. Функция готовится к запуску и войдёт в Pro.',
  },
};

// ───────────────────────────────────────────────────────────────
// FeatureHintRow — <li> с ховер/тап-попапом
// ───────────────────────────────────────────────────────────────

const POPOVER_WIDTH = 280;

interface FeatureHintRowProps {
  hint?: FeatureHint;
  /** Стили строки (цвет/прозрачность included/soon считает родитель). */
  style?: CSSProperties;
  children: ReactNode;
}

export function FeatureHintRow({ hint, style, children }: FeatureHintRowProps) {
  const [open, setOpen] = useState(false);
  const liRef = useRef<HTMLLIElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const hoverTimer = useRef<number | null>(null);
  // Попап позиционируется fixed над строкой; если сверху места нет — под ней.
  const [pos, setPos] = useState<{ top: number; left: number; below: boolean } | null>(null);

  const place = useCallback(() => {
    const r = liRef.current?.getBoundingClientRect();
    if (!r) return;
    const left = Math.max(8, Math.min(r.left, window.innerWidth - POPOVER_WIDTH - 8));
    // ~240px — верхняя оценка высоты попапа с мокапом.
    const below = r.top < 250;
    setPos({ top: below ? r.bottom + 8 : r.top - 8, left, below });
  }, []);

  const show = useCallback(() => { place(); setOpen(true); }, [place]);
  const hide = useCallback(() => {
    if (hoverTimer.current != null) { window.clearTimeout(hoverTimer.current); hoverTimer.current = null; }
    setOpen(false);
  }, []);

  // Ховер с небольшой задержкой, чтобы попапы не мигали при проводке мыши по списку.
  const onEnter = useCallback(() => {
    if (hoverTimer.current != null) window.clearTimeout(hoverTimer.current);
    hoverTimer.current = window.setTimeout(show, 140);
  }, [show]);

  useEffect(() => () => {
    if (hoverTimer.current != null) window.clearTimeout(hoverTimer.current);
  }, []);

  // Открытый попап: Escape / тап вне / скролл закрывают (тап-режим на мобилке).
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') hide(); };
    const onPointer = (e: MouseEvent | TouchEvent) => {
      const t = e.target as Node;
      if (liRef.current?.contains(t) || popRef.current?.contains(t)) return;
      hide();
    };
    const onScroll = () => hide();
    document.addEventListener('keydown', onKey);
    document.addEventListener('mousedown', onPointer);
    document.addEventListener('touchstart', onPointer);
    window.addEventListener('scroll', onScroll, true);
    return () => {
      document.removeEventListener('keydown', onKey);
      document.removeEventListener('mousedown', onPointer);
      document.removeEventListener('touchstart', onPointer);
      window.removeEventListener('scroll', onScroll, true);
    };
  }, [open, hide]);

  if (!hint) {
    return <li className="flex items-start gap-2" style={style}>{children}</li>;
  }

  const Visual = hint.visual ? VISUALS[hint.visual] : null;

  return (
    <li
      ref={liRef}
      className="flex items-start gap-2 fh-row"
      style={style}
      onMouseEnter={onEnter}
      onMouseLeave={hide}
      onClick={() => (open ? hide() : show())}
    >
      {children}
      {open && pos && createPortal(
        <div
          ref={popRef}
          role="tooltip"
          className="fh-pop"
          style={{
            top: pos.top,
            left: pos.left,
            width: POPOVER_WIDTH,
            transform: pos.below ? undefined : 'translateY(-100%)',
          }}
        >
          {Visual && <Visual />}
          <div className="fh-pop-title">{hint.title}</div>
          <div className="fh-pop-text">{hint.text}</div>
        </div>,
        document.body,
      )}
    </li>
  );
}
