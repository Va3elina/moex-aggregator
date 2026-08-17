/**
 * EmbedToolbar — «терминальный» скелет embed-виджета (замена EmbedShell).
 *
 * Фидбэк Вадима: старая модель (заголовок «Фрейм · X» + все контролы спрятаны за
 * шестерёнкой в drawer'е) выглядела как «мини-сайт вокруг графика». Новая модель —
 * по образцу TradingView: функциональный ТУЛБАР прямо над графиком, а график во
 * всю площадь без карточки-рамки.
 *
 *   [⊕ актив] [инлайн-контролы …………………] [⚙ ещё]
 *   ├───────────────── график (edge-to-edge) ─────────────────┤
 *
 * - Выбор актива — отдельная кнопка-«кружок-плюс» (AssetButton) с поповером.
 * - Первичные контролы — инлайн (PillGroup / Dropdown), видны сразу.
 * - Остальное — за ⚙ в компактном поповере (не полноэкранный drawer).
 *
 * Жесты над графиком:
 *   • Shift + колесо  → масштаб времени (период) — гориз. зум как в TradingView;
 *   • обычное колесо  → вертикальный размер: растит/ужимает высоту панели
 *     (postMessage наверх в widget.js расширения; в pop-out — window.resizeBy).
 *
 * Всё инлайн-стилями с CSS-var, чтобы работать в любой теме внутри iframe.
 */
import { createContext, useContext, useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { PlusCircle, Settings, ChevronDown, Maximize2, Minimize2, X as XIcon, GripVertical, Contrast } from 'lucide-react';

/**
 * Контекст оконных кнопок панели ПЕСОЧНИЦЫ. Когда embed рендерится внутри окна
 * песочницы (SandboxPage оборачивает панель этим провайдером), EmbedFrame дорисовывает
 * кнопки окна (⤢ развернуть / × закрыть) СПРАВА в ту же строку тулбара — единая шапка
 * по §4.1 спеки. В расширении/на сайте контекст пуст → ничего лишнего.
 */
export interface SandboxWindowControls {
  onExpand?: () => void; onClose?: () => void;
  /** true — эта панель СЕЙЧАС развёрнута на весь sb-root (SandboxPage рендерит
   *  ТОЛЬКО её вместо шапки+холста). Переключает иконку/подпись кнопки ⤢ на
   *  обратную (⤡ «Свернуть»), onExpand у обеих — один и тот же toggle. */
  maximized?: boolean;
  /** Индикатор просит панель принять размер w×h (напр. Сезонность — под срез/режим).
   *  SandboxPage сам клампит к границам холста и MINW/MINH; на сайте/расширении
   *  контекста нет → вызов недоступен, embed просто не резайзит. */
  onResize?: (w: number, h: number) => void;
  /** Нажали на грип шапки. Нужен ТОЛЬКО оконному режиму расширения: там панель
   *  живёт в iframe, и оболочка сама поймать pointerdown на грипе не может —
   *  embed сообщает ей экранные координаты, дальше тянет оболочка (см. EmbedPage
   *  и widget.js dragFromEmbed). В песочнице панель тянется своим обработчиком
   *  на контейнере, поэтому поле не задаётся. */
  onDragStart?: (screenX: number, screenY: number) => void;
  /** Переключить тему ЭТОЙ панели. Только расширение: в терминале Т-Инвестиций
   *  окна живут поверх чужого интерфейса и тему удобно менять у каждого окна
   *  отдельно (кнопка ◐ была в шапке оболочки, пока шапка существовала).
   *  В нашей песочнице тема общая и живёт в шапке терминала → поле не задаётся. */
  onTheme?: () => void;
}
export const SandboxWindowCtx = createContext<SandboxWindowControls | null>(null);
import InstrumentIcon from '../../components/InstrumentIcon';
import InstrumentSearchModal from '../../components/InstrumentSearchModal';

const TOOLBAR_H = 40;

/* ───────────────────────────── shell ───────────────────────────── */

// Фон = фон графика: тулбар и график читаются ОДНОЙ поверхностью (без карточки-
// рамки). Зум/масштаб живёт внутри самого графика (SimpleChart axisZoom:
// колесо по оси дат = период, по ценовой оси = масштаб), поэтому здесь wheel нет.
const frameStyle: CSSProperties = {
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  boxSizing: 'border-box',
  position: 'relative',
  background: 'var(--bg-base)',
  color: 'var(--text-primary)',
  overflow: 'hidden',
};

// Тулбар — продолжение поверхности графика: без нижнего разделителя, на том же фоне.
// z-index ВЫШЕ DOM-оверлеев графика (легенда 5 / тултип 6 / экспирации 4): поповеры
// (⚙/дропдауны) рендерятся ВНУТРИ тулбара, и их собственный огромный z-index заперт
// в его стекинг-контексте — при z:3 легенда просвечивала СКВОЗЬ открытую модалку.
const toolbarRow: CSSProperties = {
  flexShrink: 0,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  minHeight: TOOLBAR_H,
  padding: '5px 8px',
  position: 'relative',
  zIndex: 20,
};

// Кнопки окна панели ПЕСОЧНИЦЫ (⤢ ×) — та же геометрия, что у всех иконок-действий
// тулбара (ICON_BTN, 28×28/radius 7/глиф 15): раньше они были 24×24 с глифами 13 и 16,
// и правый верхний угол выглядел разнокалиберным. Отличие только в цвете глифа
// (приглушённый) и ховере: класс .sb-winbtn-close красит закрытие в красный
// (sandbox.css; на сайте/в расширении класс без стилей, а сами кнопки гейтятся
// наличием SandboxWindowCtx).
const sbWinBtn: CSSProperties = { ...iconBtnStyle(false), color: 'var(--muted)' };

/**
 * EmbedFrame — обёртка виджета: тулбар (lead + inline + ⚙more) + область графика
 * edge-to-edge. Никакого «окна вокруг графика»: одна поверхность, кнопки сверху.
 */
export function EmbedFrame({
  lead,
  toolbar,
  actions,
  more,
  moreLabel = 'Ещё',
  toolbarUnified,
  children,
}: {
  lead?: ReactNode;
  toolbar?: ReactNode;
  /** Иконки-действия справа, рядом с ⚙ (напр. 📷 экспорт). Borderless, без рамки. */
  actions?: ReactNode;
  more?: ReactNode;
  moreLabel?: string;
  /** true — lead (ассет-кнопка) и toolbar рендерятся в ОДНОМ флекс-ряду, без
   *  вложенного overflow-x:auto скролл-контейнера (тот давал видимый второй
   *  «блок» со своим скроллбаром при недостатке места). Включать только когда
   *  caller сам гарантирует, что toolbar влезает — напр. свой compact-icon
   *  режим по измерению ширины (см. EmbedOpenInterest) — иначе на совсем узкой
   *  панели контент молча обрежется (frameStyle.overflow:hidden), а не
   *  проскроллится. Дефолт false — остальные embed'ы не затронуты. */
  toolbarUnified?: boolean;
  children: ReactNode;
}) {
  const [moreOpen, setMoreOpen] = useState(false);
  const moreBtnRef = useRef<HTMLButtonElement>(null);
  const win = useContext(SandboxWindowCtx);

  // Тянуть окно можно за ЛЮБОЕ свободное место шапки, не только за грип (фидбек
  // Вадима). Интерактив пропускаем: иначе pointerdown с preventDefault съедает
  // click у кнопок и поповеров. Тот же фильтр, что у панели песочницы.
  const dragHandlers = win?.onDragStart
    ? {
        onPointerDown: (e: React.PointerEvent) => {
          if ((e.target as HTMLElement).closest('button, input, a, select, textarea, [role="dialog"]')) return;
          e.preventDefault();
          win.onDragStart!(e.screenX, e.screenY);
        },
        onDoubleClick: (e: React.MouseEvent) => {
          if ((e.target as HTMLElement).closest('button, input, a, select, textarea, [role="dialog"]')) return;
          win.onExpand?.();
        },
      }
    : {};

  return (
    <div style={frameStyle}>
      <div style={win?.onDragStart ? { ...toolbarRow, cursor: 'grab' } : toolbarRow} {...dragHandlers}>
        {/* Грип-хват окна ПЕСОЧНИЦЫ: тулбар бывает забит кнопками и «взять» окно не за что.
            Не button → проходит фильтр onDragStart (тянем только не-интерактивное). Только в песочнице. */}
        {win && (
          <div
            title="Перетащить окно"
            aria-hidden
            style={{ display: 'flex', alignItems: 'center', color: 'var(--muted, #9A958C)', cursor: 'grab', flexShrink: 0, marginLeft: -2 }}
          >
            <GripVertical size={15} />
          </div>
        )}
        {lead}
        {toolbarUnified ? (
          // Один ряд с lead, БЕЗ вложенного скролл-контейнера — caller (см.
          // toolbarUnified doc выше) сам отвечает за то, что toolbar влезает.
          toolbar
        ) : (
          // Узкая панель (§4.1): контролов больше, чем влезает в строку. Раньше
          // overflow:hidden молча обрезал контейнер, а единственный shrink-able
          // элемент (PillGroup, напр. Физ/Юр) хватал на себя весь дефицит места
          // и визуально «сплющивался» кнопкой справа — выглядело как баг, а не
          // как осознанный скролл. Теперь скроллится ЦЕЛИКОМ вся полоса
          // контролов как одно целое (все контролы — flexShrink:0, полный
          // размер), без выборочного сжатия одного виджета.
          <div className="styled-scrollbar" style={{ display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflowX: 'auto', overflowY: 'hidden' }}>
            {toolbar}
          </div>
        )}
        {(actions || more) && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 2, flexShrink: 0 }}>
            {actions}
            {more && (
              <div style={{ position: 'relative' }}>
                <button
                  ref={moreBtnRef}
                  type="button"
                  onClick={() => setMoreOpen((v) => !v)}
                  title="Ещё настройки"
                  aria-label="Ещё настройки"
                  className="emb-iconbtn"
                  style={iconBtnStyle(moreOpen)}
                >
                  <Settings size={15} />
                </button>
                {moreOpen && (
                  <Popover anchorEl={moreBtnRef.current} align="right" onClose={() => setMoreOpen(false)} title={moreLabel}>
                    {more}
                  </Popover>
                )}
              </div>
            )}
          </div>
        )}
        {/* Кнопки окна песочницы — в ту же строку (единая шапка §4.1, §4.3).
            marginLeft компенсирует gap:8 родителя, когда слева стоит блок
            actions/⚙ (у того внутренний gap 2): между шестерёнкой и ⤢ был
            видимый провал 8px против 2px у остальных иконок ряда. */}
        {win && (
          <div style={{ display: 'flex', gap: 2, flexShrink: 0, ...(actions || more ? { marginLeft: -6 } : {}) }}>
            {win.onTheme && (
              <button
                type="button"
                onClick={win.onTheme}
                title="Тема окна"
                aria-label="Тема окна"
                className="emb-iconbtn"
                style={sbWinBtn}
              >
                <Contrast size={15} />
              </button>
            )}
            {win.onExpand && (
              <button
                type="button"
                onClick={win.onExpand}
                title={win.maximized ? 'Свернуть' : 'Развернуть'}
                aria-label={win.maximized ? 'Свернуть' : 'Развернуть'}
                className="emb-iconbtn"
                style={sbWinBtn}
              >
                {win.maximized ? <Minimize2 size={15} /> : <Maximize2 size={15} />}
              </button>
            )}
            {/* Кнопки «Тема панели» здесь больше нет: тему задаёт ТОЛЬКО шапка
                оболочки, персональной темы у окна не бывает. */}
            {win.onClose && (
              <button type="button" onClick={win.onClose} title="Закрыть" aria-label="Закрыть" className="emb-iconbtn sb-winbtn-close" style={sbWinBtn}>
                <XIcon size={15} />
              </button>
            )}
          </div>
        )}
      </div>
      <div style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {children}
      </div>
    </div>
  );
}

/* ───────────────────────────── popover ───────────────────────────── */

/**
 * Выпадающий поповер, заякоренный к кнопке-триггеру через `position: fixed`.
 *
 * ⚠️ Почему fixed, а не absolute: инлайн-контролы тулбара живут в контейнере с
 * `overflow: hidden` (горизонтальный клип). Absolute-поповер, падающий ВНИЗ,
 * обрезался бы этим overflow — выпадашка открывалась, но была невидима («не
 * работает»). Fixed выходит из-под overflow предков (в iframe fixed = вьюпорт
 * iframe, containing-block-трансформов внутри embed нет). Позицию считаем из
 * getBoundingClientRect кнопки, пересчитываем на scroll/resize.
 */
export function Popover({
  children,
  onClose,
  anchorEl,
  align = 'left',
  title,
  width,
  compact = false,
}: {
  children: ReactNode;
  onClose: () => void;
  anchorEl: HTMLElement | null;
  align?: 'left' | 'right';
  title?: string;
  /** Ширина поповера. Дефолт 300px (⚙-дровер с секциями). Компактные дропдауны
   *  тулбара передают ширину кнопки → список ровно под кнопкой, не растянут вправо. */
  width?: string;
  /** Тесный padding (4px) — для тулбар-дропдаунов, чтобы подписи влезали при ширине кнопки. */
  compact?: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<{ top: number; left?: number; right?: number }>({ top: -9999, left: 6 });

  useLayoutEffect(() => {
    if (!anchorEl) return;
    const place = () => {
      const r = anchorEl.getBoundingClientRect();
      const top = r.bottom + 6;
      setPos(align === 'right'
        ? { top, right: Math.max(6, window.innerWidth - r.right) }
        : { top, left: Math.max(6, r.left) });
    };
    place();
    window.addEventListener('resize', place);
    window.addEventListener('scroll', place, true);
    return () => {
      window.removeEventListener('resize', place);
      window.removeEventListener('scroll', place, true);
    };
  }, [anchorEl, align]);

  useEffect(() => {
    const onDown = (e: Event) => {
      const t = e.target as Node;
      // Клик по самой кнопке-триггеру не закрываем — её onClick сам тогглит.
      if (anchorEl && anchorEl.contains(t)) return;
      // ⚠️ Вложенные порталы (пикер цвета серии) живут в body, а НЕ внутри
      // ref.current — без этой ветки дровер закрывался на pointerdown по
      // свотчу и уносил пикер с собой ДО click'а: палитра выглядела рабочей,
      // но цвет не менялся (Вадим, «кнопки как будто не рабочие»).
      if (t instanceof Element && t.closest('[data-keep-parent-open]')) return;
      if (ref.current && !ref.current.contains(t)) onClose();
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    // defer — чтобы клик, открывший поповер, не закрыл его сразу
    const t = window.setTimeout(() => document.addEventListener('pointerdown', onDown, true), 0);
    document.addEventListener('keydown', onKey);
    return () => {
      window.clearTimeout(t);
      document.removeEventListener('pointerdown', onDown, true);
      document.removeEventListener('keydown', onKey);
    };
  }, [onClose, anchorEl]);

  const style: CSSProperties = {
    position: 'fixed',
    top: pos.top,
    ...(pos.right !== undefined ? { right: pos.right } : { left: pos.left ?? 6 }),
    zIndex: 2147483000,
    width: width ?? 'min(300px, calc(100vw - 20px))',
    maxWidth: 'calc(100vw - 20px)',
    maxHeight: 'calc(100vh - 80px)',
    overflowY: 'auto',
    background: 'var(--bg-base, var(--bg-primary))',
    color: 'var(--text-primary)',
    border: 'var(--emb-pop-bw, 1.5px) solid var(--border-color, rgba(128,128,128,0.4))',
    borderRadius: 10,
    boxShadow: '0 12px 34px rgba(0,0,0,0.4)',
    padding: compact ? 4 : 12,
    display: 'flex',
    flexDirection: 'column',
    gap: compact ? 4 : 14,
  };

  return (
    <div ref={ref} className="styled-scrollbar" style={style} role="dialog" aria-label={title}>
      {children}
    </div>
  );
}

/* ───────────────────────────── asset button ───────────────────────────── */

/**
 * AssetButton — «кружок-плюс» + текущий тикер; клик → ДЕСКТОПНАЯ модалка выбора
 * актива (InstrumentSearchModal — та же, что на сайте: категории, поиск, избранное,
 * сортировка по объёму/изменению). Замена компактного inline-пикера
 * (фидбэк Вадима: «модалка именно с сайта пк версии»).
 *
 * `indicator` НЕ передаём: доступ в embed уже гейтится PRO-токеном на уровне
 * страницы, а user-объект в iframe не догружен → повторный tier-lock ложно
 * заблокировал бы Pro-активы.
 *
 * ⚠️ ИЗБРАННОЕ: модалка держит favoriteInstruments в localStorage, а embed —
 * партиционированный сторонний iframe → это НЕ те же избранные, что на first-party
 * сайте (для полной синхронизации нужен серверный стор — отдельная задача).
 */
export function AssetButton({
  ticker,
  current,
  onSelect,
  filterType,
  excludeType,
  showIntradayBadge,
  hideLowActivity,
}: {
  ticker: string;
  current: string;
  onSelect: (secid: string, name: string) => void;
  filterType?: 'stock' | 'futures';
  excludeType?: string;
  showIntradayBadge?: boolean;
  hideLowActivity?: boolean;
}) {
  const [open, setOpen] = useState(false);
  // В песочнице раскладка по макету §6.1: [лого][тикер][⊕ приглушённый в конце],
  // компактнее и с мягким бордером. На сайте/в расширении — прежний вид.
  const sb = useContext(SandboxWindowCtx) !== null;
  return (
    <div style={{ position: 'relative', flexShrink: 0, display: 'flex', alignItems: 'center' }}>
      <button
        type="button"
        onClick={() => setOpen(true)}
        title="Выбрать актив"
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: sb ? 5 : 6,
          padding: sb ? '2px 6px 2px 4px' : '4px 10px 4px 6px',
          borderRadius: sb ? 6 : 8,
          border: '1.5px solid var(--border-strong, var(--border-color, rgba(128,128,128,0.4)))',
          background: open ? 'color-mix(in srgb, var(--accent) 12%, transparent)' : 'transparent',
          color: 'var(--text-primary)',
          cursor: 'pointer',
        }}
      >
        {!sb && <PlusCircle size={16} style={{ color: 'var(--accent)' }} />}
        <InstrumentIcon sectype={current} size={sb ? 16 : 18} />
        <span style={{ fontWeight: sb ? 700 : 800, fontSize: sb ? 11 : 12.5, letterSpacing: '-0.01em' }}>{ticker}</span>
      </button>
      {open && (
        <InstrumentSearchModal
          onSelect={(s, n) => { onSelect(s, n); setOpen(false); }}
          onClose={() => setOpen(false)}
          filterType={filterType}
          excludeType={excludeType}
          showIntradayBadge={showIntradayBadge}
          hideLowActivity={hideLowActivity}
        />
      )}
    </div>
  );
}

/* ───────────────────────── inline controls ───────────────────────── */

/**
 * Размеры контролов параметризованы CSS-переменными, дефолты = ТЕКУЩИЙ вид.
 * Сайт и расширение переменных не задают → рендерятся как раньше. Песочница
 * переопределяет их в scope `.sb-panel` (sandbox.css) под значения макета —
 * тот же приём, что G-1 для палитры. Blueprint G-4.
 */
/** Экспортируются: меню индикаторов (EmbedIndicators) держит тот же кегль, что
 *  тулбар и его выпадашки — иначе рядом открываются списки разным шрифтом. */
export const CTL_FS = 'var(--emb-ctl-fs, 11.5px)';
export const CTL_FW = 'var(--emb-ctl-fw, 700)' as unknown as CSSProperties['fontWeight'];

/** Компактная группа пилюль для тулбара (мало коротких опций — таймфрейм, режим).
 *  Один слитный pill (рамка+фон на группе, разделитель между сегментами), а не
 *  отдельная кнопка на опцию — тот же паттерн, что у SegmentedControl на сайте. */
export function PillGroup<T extends string | number>({
  value,
  options,
  onChange,
  compact,
  fontSize,
}: {
  value: T;
  options: { id: T; label: string; title?: string; icon?: ReactNode }[];
  onChange: (v: T) => void;
  /** Узкая панель: текст лейблов не помещается — показываем только icon (label уходит в title). */
  compact?: boolean;
  /** Кегль лейблов, если общего тулбарного мало (одиночные символы вроде ₽/$
   *  читаются мельче слов рядом). Высоту кнопки это не меняет: line-height
   *  задан длиной от базового CTL_FS, а не множителем от своего шрифта. */
  fontSize?: string;
}) {
  return (
    // flexShrink:0 — группа держит полный размер и не сплющивается сама по себе;
    // на узкой панели теперь скроллится ЦЕЛИКОМ вся строка тулбара (родитель
    // в EmbedFrame, §4.1), а не отдельные виджеты вперемешку с рядом стоящими
    // жёсткими Dropdown (было видно как «Физ/Юр ужимаются кнопкой справа»).
    <div
      role="group"
      style={{
        display: 'inline-flex',
        flexShrink: 0,
        borderRadius: 6,
        overflow: 'hidden',
        border: '1.5px solid var(--border-color, rgba(128,128,128,0.3))',
      }}
    >
      {options.map((o, i) => {
        const active = o.id === value;
        const iconOnly = compact && o.icon;
        return (
          <button
            key={String(o.id)}
            type="button"
            title={o.title ?? (iconOnly ? o.label : undefined)}
            aria-label={iconOnly ? o.label : undefined}
            aria-pressed={active}
            onClick={() => onChange(o.id)}
            style={{
              fontSize: fontSize ?? CTL_FS,
              fontWeight: CTL_FW,
              padding: iconOnly ? 'var(--emb-pill-pad-icon, 4px 7px)' : 'var(--emb-pill-pad, 4px 9px)',
              border: 'none',
              borderLeft: i > 0 ? '1.5px solid var(--border-color, rgba(128,128,128,0.3))' : 'none',
              background: active ? 'var(--accent)' : 'transparent',
              color: active ? '#fff' : 'var(--emb-pill-off, var(--text-primary))',
              // Длиной, а не множителем: при увеличенном fontSize высота пилюли
              // остаётся такой же, как у соседей по тулбару.
              lineHeight: 'calc(var(--emb-ctl-fs, 11.5px) * 1.2)',
              whiteSpace: 'nowrap',
              cursor: 'pointer',
              flexShrink: 0,
              display: 'inline-flex',
              alignItems: 'center',
              transition: 'background 0.12s, border-color 0.12s',
            }}
          >
            {iconOnly ? o.icon : o.label}
          </button>
        );
      })}
    </div>
  );
}

function ddBtnStyle(open: boolean): CSSProperties {
  return {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 5,
    padding: 'var(--emb-dd-pad, 4px 8px 4px 10px)',
    borderRadius: 'var(--emb-dd-radius, 7px)',
    cursor: 'pointer',
    border: '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
    background: open ? 'color-mix(in srgb, var(--accent) 12%, transparent)' : 'transparent',
    color: 'var(--text-primary)',
    fontSize: CTL_FS,
    fontWeight: CTL_FW,
    whiteSpace: 'nowrap',
    lineHeight: 1.2,
  };
}

/** Компактный дропдаун для тулбара (много/длинных опций — показатель, категория). */
export function Dropdown<T extends string | number>({
  value,
  options,
  onChange,
  title,
  icon,
  compact,
}: {
  value: T;
  options: { id: T; label: string }[];
  onChange: (v: T) => void;
  title?: string;
  /** Иконка контрола — статичная или зависящая от текущего значения. Нужна для
   *  compact-режима (узкая панель прячет текст, остаётся только иконка). */
  icon?: ReactNode | ((value: T) => ReactNode);
  /** Узкая панель: лейбл не помещается — показываем только иконку (label уходит в title). */
  compact?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [btnW, setBtnW] = useState<number>();   // ширина кнопки → ширина списка (Вадим)
  const btnRef = useRef<HTMLButtonElement>(null);
  const cur = options.find((o) => o.id === value);
  // Один вариант (напр. ТФ у EOD-only фьючерса — только «1 день») → выбирать
  // нечего: убираем стрелочку (не обещаем список, которого нет) и не открываем
  // попап по клику.
  const single = options.length <= 1;
  const resolvedIcon = typeof icon === 'function' ? icon(value) : icon;
  const iconOnly = compact && resolvedIcon;
  return (
    // display:inline-flex — без него это блочный контейнер с одним inline-block
    // ребёнком (кнопкой): браузер резервирует ~3px «фантомного» подстрочного
    // пространства (line-height/baseline инлайн-контекста) ПОД кнопкой, и она
    // визуально съезжает вниз относительно соседних PillGroup-пилюль в тулбаре
    // (те уже inline-flex). display:flex убирает инлайн-контекст → кнопка
    // ровно по центру, как остальные контролы.
    <div style={{ position: 'relative', flexShrink: 0, display: 'inline-flex' }}>
      <button
        ref={btnRef}
        type="button"
        title={iconOnly ? [title, cur?.label].filter(Boolean).join(': ') : title}
        aria-label={iconOnly ? [title, cur?.label].filter(Boolean).join(': ') : undefined}
        style={{
          ...ddBtnStyle(open),
          cursor: single ? 'default' : 'pointer',
          ...(iconOnly ? { padding: 'var(--emb-dd-pad-icon, 4px 8px)' } : {}),
        }}
        onClick={() => { if (single) return; if (!open) setBtnW(btnRef.current?.offsetWidth); setOpen((v) => !v); }}
      >
        {resolvedIcon && <span style={{ display: 'inline-flex', flexShrink: 0 }}>{resolvedIcon}</span>}
        {!iconOnly && (
          // Кнопка резервирует ширину под САМЫЙ ДЛИННЫЙ пункт, а не под текущий:
          // список ниже открывается ровно по её ширине, и его правый край больше
          // не «заступает» за кнопку на длинных лейблах («Покупки + Продажи»).
          // Приём — grid-стек: все лейблы в одной ячейке, ширина ячейки = максимум
          // из них; невыбранные с height:0/hidden, поэтому не видны и не растят
          // высоту. Без него пришлось бы мерить текст в JS на каждый рендер.
          <span style={{ display: 'grid', minWidth: 0 }}>
            <span style={{ gridArea: '1 / 1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 150 }}>{cur?.label ?? '—'}</span>
            {options.map((o) => (
              <span
                key={String(o.id)}
                aria-hidden
                style={{ gridArea: '1 / 1', height: 0, overflow: 'hidden', visibility: 'hidden', whiteSpace: 'nowrap', maxWidth: 150 }}
              >
                {o.label}
              </span>
            ))}
          </span>
        )}
        {!single && <ChevronDown size={13} style={{ flexShrink: 0, opacity: 0.7 }} />}
      </button>
      {open && !single && (
        // Ширина списка = ширина кнопки: та уже зарезервирована под самый длинный
        // лейбл (grid-стек выше), поэтому текст влезает, а края совпадают —
        // никакого заступа вправо. В compact-режиме кнопка схлопнута до иконки и
        // ширины не хватит — там список по контенту, как раньше.
        <Popover
          anchorEl={btnRef.current}
          align="left"
          compact
          width={!iconOnly && btnW ? `${btnW}px` : 'max-content'}
          onClose={() => setOpen(false)}
        >
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: iconOnly ? btnW : undefined }}>
            {options.map((o) => {
              const on = o.id === value;
              return (
                <button
                  key={String(o.id)}
                  type="button"
                  onClick={() => { onChange(o.id); setOpen(false); }}
                  style={{
                    textAlign: 'left',
                    padding: '7px 9px',
                    borderRadius: 7,
                    border: 'none',
                    // Выбранный пункт помечен очень слабой акцентной подложкой и
                    // жирностью: оранжевый текст на заливке читался как яркая
                    // кнопка действия, а это всего лишь индикация выбора.
                    background: on ? 'color-mix(in srgb, var(--accent) 6%, transparent)' : 'transparent',
                    color: 'var(--text-primary)',
                    // Размер как у кнопки-триггера (CTL_FS) — Вадим: «как сверху».
                    // Выбор держится на ЖИРНОСТИ, поэтому невыбранные заметно легче
                    // (500 против CTL_FW=700): 700 vs 800 на глаз не различались.
                    // nowrap — чтобы «1 день» не переносился.
                    fontWeight: on ? 800 : 500,
                    fontSize: CTL_FS,
                    whiteSpace: 'nowrap',
                    // Ширина задана кнопкой: если лейбл всё же не влез (кнопка
                    // упёрлась в maxWidth 150), обрезаем многоточием, а не вылезаем.
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    cursor: 'pointer',
                  }}
                >
                  {o.label}
                </button>
              );
            })}
          </div>
        </Popover>
      )}
    </div>
  );
}

/**
 * Кнопка тулбара, открывающая произвольное меню (не выбор значения — для него
 * Dropdown). Схлопывается в иконку на узкой панели тем же правилом, что и
 * остальные контролы: лейбл уходит в title.
 */
/** Кнопка тулбара БЕЗ поповера — оформление как у ToolbarMenuButton, но по
 *  клику вызывает действие (у потоков фондов — модалку выбора фондов, она
 *  рендерится порталом и не может жить внутри поповера). */
export function ToolbarButton({ label, icon, title, compact, active, onClick }: {
  label: string;
  icon: ReactNode;
  title?: string;
  compact?: boolean;
  active?: boolean;
  onClick: () => void;
}) {
  return (
    <div style={{ position: 'relative', flexShrink: 0, display: 'inline-flex' }}>
      <button
        type="button"
        title={title ?? label}
        aria-label={compact ? label : undefined}
        style={{ ...ddBtnStyle(!!active), ...(compact ? { padding: 'var(--emb-dd-pad-icon, 4px 8px)' } : {}) }}
        onClick={onClick}
      >
        <span style={{ display: 'inline-flex', flexShrink: 0 }}>{icon}</span>
        {!compact && <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 150 }}>{label}</span>}
      </button>
    </div>
  );
}

export function ToolbarMenuButton({ label, icon, title, compact, children }: {
  label: string;
  icon: ReactNode;
  title?: string;
  compact?: boolean;
  /** Содержимое поповера. Аргумент — закрыть меню. */
  children: (close: () => void) => ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const btnRef = useRef<HTMLButtonElement>(null);
  return (
    <div style={{ position: 'relative', flexShrink: 0, display: 'inline-flex' }}>
      <button
        ref={btnRef}
        type="button"
        title={title ?? label}
        aria-label={compact ? label : undefined}
        style={{ ...ddBtnStyle(open), ...(compact ? { padding: 'var(--emb-dd-pad-icon, 4px 8px)' } : {}) }}
        onClick={() => setOpen((v) => !v)}
      >
        <span style={{ display: 'inline-flex', flexShrink: 0 }}>{icon}</span>
        {!compact && <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 150 }}>{label}</span>}
      </button>
      {open && (
        <Popover anchorEl={btnRef.current} align="left" compact width="max-content" onClose={() => setOpen(false)}>
          {children(() => setOpen(false))}
        </Popover>
      )}
    </div>
  );
}

/**
 * Единый стиль иконки-действия в тулбаре (⚙ ещё, ✏️ рисование, 📷 экспорт, ⤢ ×
 * окна песочницы). ВСЕ иконки правого угла обязаны идти через него: пока у каждой
 * был свой инлайн-объект, коробки разъезжались (28 против 24) и глифы были разного
 * кегля (13/15/16) — угол выглядел кривым. Ховер — класс .emb-iconbtn.
 */
export function iconBtnStyle(active: boolean): CSSProperties {
  return {
    width: 28,
    height: 28,
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    // Без рамки (Вадим: «убери контейнер вокруг кнопки настройки»); фон-подсветка
    // только в активном состоянии (открыт поповер).
    border: 'none',
    borderRadius: 7,
    background: active ? 'color-mix(in srgb, var(--accent) 14%, transparent)' : 'transparent',
    color: active ? 'var(--accent)' : 'var(--text-secondary)',
    cursor: 'pointer',
    flexShrink: 0,
    padding: 0,
  };
}

/* ───────────────────────── period readout + wheel hint ───────────────────────── */

/** Дим-readout текущего периода в тулбаре (не кнопка — период меняется Shift+колесом). */
export function PeriodReadout({ label, title }: { label: string; title?: string }) {
  return (
    <span
      title={title || 'Период · Shift+колесо над графиком'}
      style={{
        fontSize: 11,
        fontWeight: 800,
        color: 'var(--text-secondary)',
        padding: '0 2px',
        flexShrink: 0,
        fontVariantNumeric: 'tabular-nums',
        letterSpacing: '0.02em',
      }}
    >
      {label}
    </span>
  );
}

/** Подсказка про жесты колеса — в подвале ⚙-поповера. */
export function WheelHint({ children, divider = true }: {
  children: ReactNode;
  /** Отчёркивать сверху. Разделитель уместен, когда подсказка идёт ПОДВАЛОМ под
   *  секциями; если она единственное содержимое дровера — линия висит в воздухе. */
  divider?: boolean;
}) {
  return (
    <div
      style={{
        fontSize: 11,
        color: 'var(--text-secondary)',
        lineHeight: 1.5,
        ...(divider ? {
          borderTop: '1px solid var(--border-color, rgba(128,128,128,0.18))',
          paddingTop: 10,
        } : {}),
      }}
    >
      {children}
    </div>
  );
}
