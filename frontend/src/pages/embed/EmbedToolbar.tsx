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
import { PlusCircle, Settings, ChevronDown, Contrast, Maximize2, X as XIcon } from 'lucide-react';

/**
 * Контекст оконных кнопок панели ПЕСОЧНИЦЫ. Когда embed рендерится внутри окна
 * песочницы (SandboxPage оборачивает панель этим провайдером), EmbedFrame дорисовывает
 * кнопки окна (⤢ развернуть / × закрыть) СПРАВА в ту же строку тулбара — единая шапка
 * по §4.1 спеки. В расширении/на сайте контекст пуст → ничего лишнего.
 */
export interface SandboxWindowControls { onExpand?: () => void; onClose?: () => void; onToggleTheme?: () => void }
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

// Кнопки окна панели ПЕСОЧНИЦЫ (⤢ ◐ ×) — §4.3: 24×24, без бордера, radius 5,
// приглушённый глиф. Ховер — через класс .sb-winbtn (в sandbox.css; на сайте/
// расширении класс без стилей, а сами кнопки гейтятся наличием SandboxWindowCtx).
const sbWinBtn: CSSProperties = {
  width: 24, height: 24, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
  border: 'none', borderRadius: 5, background: 'transparent', color: 'var(--muted)', cursor: 'pointer', flexShrink: 0, padding: 0,
};

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
  children,
}: {
  lead?: ReactNode;
  toolbar?: ReactNode;
  /** Иконки-действия справа, рядом с ⚙ (напр. 📷 экспорт). Borderless, без рамки. */
  actions?: ReactNode;
  more?: ReactNode;
  moreLabel?: string;
  children: ReactNode;
}) {
  const [moreOpen, setMoreOpen] = useState(false);
  const moreBtnRef = useRef<HTMLButtonElement>(null);
  const win = useContext(SandboxWindowCtx);

  return (
    <div style={frameStyle}>
      <div style={toolbarRow}>
        {lead}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {toolbar}
        </div>
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
        {/* Кнопки окна песочницы — в ту же строку (единая шапка §4.1, §4.3). */}
        {win && (
          <div style={{ display: 'flex', gap: 2, flexShrink: 0 }}>
            {win.onExpand && (
              <button type="button" onClick={win.onExpand} title="Развернуть" aria-label="Развернуть" className="sb-winbtn" style={sbWinBtn}>
                <Maximize2 size={13} />
              </button>
            )}
            {win.onToggleTheme && (
              <button type="button" onClick={win.onToggleTheme} title="Тема панели" aria-label="Тема панели" className="sb-winbtn" style={sbWinBtn}>
                <Contrast size={13} />
              </button>
            )}
            {win.onClose && (
              <button type="button" onClick={win.onClose} title="Закрыть" aria-label="Закрыть" className="sb-winbtn sb-winbtn-close" style={sbWinBtn}>
                <XIcon size={16} />
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
        {sb && <PlusCircle size={11} style={{ color: 'var(--muted)', flexShrink: 0 }} />}
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
const CTL_FS = 'var(--emb-ctl-fs, 11.5px)';
const CTL_FW = 'var(--emb-ctl-fw, 700)' as unknown as CSSProperties['fontWeight'];

function pillStyle(active: boolean): CSSProperties {
  return {
    fontSize: CTL_FS,
    fontWeight: CTL_FW,
    padding: 'var(--emb-pill-pad, 4px 9px)',
    borderRadius: 6,
    cursor: 'pointer',
    border: active ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.3))',
    background: active ? 'var(--accent)' : 'transparent',
    color: active ? '#fff' : 'var(--emb-pill-off, var(--text-primary))',
    lineHeight: 1.2,
    whiteSpace: 'nowrap',
    transition: 'background 0.12s, border-color 0.12s',
  };
}

/** Компактная группа пилюль для тулбара (мало коротких опций — таймфрейм, режим). */
export function PillGroup<T extends string | number>({
  value,
  options,
  onChange,
}: {
  value: T;
  options: { id: T; label: string; title?: string }[];
  onChange: (v: T) => void;
}) {
  return (
    <div style={{ display: 'inline-flex', gap: 4, flexShrink: 0 }}>
      {options.map((o) => (
        <button key={String(o.id)} type="button" title={o.title} style={pillStyle(o.id === value)} onClick={() => onChange(o.id)}>
          {o.label}
        </button>
      ))}
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
}: {
  value: T;
  options: { id: T; label: string }[];
  onChange: (v: T) => void;
  title?: string;
}) {
  const [open, setOpen] = useState(false);
  const [btnW, setBtnW] = useState<number>();   // ширина кнопки → ширина списка (Вадим)
  const btnRef = useRef<HTMLButtonElement>(null);
  const cur = options.find((o) => o.id === value);
  return (
    <div style={{ position: 'relative', flexShrink: 0 }}>
      <button
        ref={btnRef}
        type="button"
        title={title}
        style={ddBtnStyle(open)}
        onClick={() => { if (!open) setBtnW(btnRef.current?.offsetWidth); setOpen((v) => !v); }}
      >
        <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 150 }}>{cur?.label ?? '—'}</span>
        <ChevronDown size={13} style={{ flexShrink: 0, opacity: 0.7 }} />
      </button>
      {open && (
        <Popover anchorEl={btnRef.current} align="left" compact width={btnW ? `${btnW}px` : 'max-content'} onClose={() => setOpen(false)}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
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
                    background: on ? 'color-mix(in srgb, var(--accent) 14%, transparent)' : 'transparent',
                    color: on ? 'var(--accent)' : 'var(--text-primary)',
                    // Размер/жирность как у кнопки-триггера (CTL_FS/CTL_FW) — Вадим: «как сверху».
                    // Активный чуть жирнее для индикации выбора. nowrap — чтобы «1 день» не переносился.
                    fontWeight: on ? 800 : CTL_FW,
                    fontSize: CTL_FS,
                    whiteSpace: 'nowrap',
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

function iconBtnStyle(active: boolean): CSSProperties {
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
export function WheelHint({ children }: { children: ReactNode }) {
  return (
    <div
      style={{
        fontSize: 11,
        color: 'var(--text-secondary)',
        lineHeight: 1.5,
        borderTop: '1px solid var(--border-color, rgba(128,128,128,0.18))',
        paddingTop: 10,
      }}
    >
      {children}
    </div>
  );
}
