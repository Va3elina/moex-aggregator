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
import { useEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { PlusCircle, Settings, ChevronDown } from 'lucide-react';
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
const toolbarRow: CSSProperties = {
  flexShrink: 0,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  minHeight: TOOLBAR_H,
  padding: '5px 8px',
  position: 'relative',
  zIndex: 3,
};

/**
 * EmbedFrame — обёртка виджета: тулбар (lead + inline + ⚙more) + область графика
 * edge-to-edge. Никакого «окна вокруг графика»: одна поверхность, кнопки сверху.
 */
export function EmbedFrame({
  lead,
  toolbar,
  more,
  moreLabel = 'Ещё',
  children,
}: {
  lead?: ReactNode;
  toolbar?: ReactNode;
  more?: ReactNode;
  moreLabel?: string;
  children: ReactNode;
}) {
  const [moreOpen, setMoreOpen] = useState(false);

  return (
    <div style={frameStyle}>
      <div style={toolbarRow}>
        {lead}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {toolbar}
        </div>
        {more && (
          <div style={{ position: 'relative', flexShrink: 0 }}>
            <button
              type="button"
              onClick={() => setMoreOpen((v) => !v)}
              title="Ещё настройки"
              aria-label="Ещё настройки"
              style={iconBtnStyle(moreOpen)}
            >
              <Settings size={15} />
            </button>
            {moreOpen && (
              <Popover align="right" onClose={() => setMoreOpen(false)} title={moreLabel}>
                {more}
              </Popover>
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

/** Выпадающий поповер под кнопкой тулбара. Клампится к области панели, скроллится. */
export function Popover({
  children,
  onClose,
  align = 'left',
  title,
}: {
  children: ReactNode;
  onClose: () => void;
  align?: 'left' | 'right';
  title?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const onDown = (e: Event) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
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
  }, [onClose]);

  const style: CSSProperties = {
    position: 'absolute',
    top: 'calc(100% + 6px)',
    zIndex: 60,
    width: 'min(300px, calc(100vw - 20px))',
    maxHeight: 'calc(100vh - 64px)',
    overflowY: 'auto',
    background: 'var(--bg-base, var(--bg-primary))',
    color: 'var(--text-primary)',
    border: '1.5px solid var(--border-color, rgba(128,128,128,0.4))',
    borderRadius: 10,
    boxShadow: '0 12px 34px rgba(0,0,0,0.4)',
    padding: 12,
    display: 'flex',
    flexDirection: 'column',
    gap: 14,
    ...(align === 'right' ? { right: 0 } : { left: 0 }),
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
  return (
    <div style={{ position: 'relative', flexShrink: 0 }}>
      <button
        type="button"
        onClick={() => setOpen(true)}
        title="Выбрать актив"
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 6,
          padding: '4px 10px 4px 6px',
          borderRadius: 8,
          border: '1.5px solid var(--border-color, rgba(128,128,128,0.4))',
          background: open ? 'color-mix(in srgb, var(--accent) 12%, transparent)' : 'transparent',
          color: 'var(--text-primary)',
          cursor: 'pointer',
        }}
      >
        <PlusCircle size={16} style={{ color: 'var(--accent)' }} />
        <InstrumentIcon sectype={current} size={18} />
        <span style={{ fontWeight: 800, fontSize: 12.5, letterSpacing: '-0.01em' }}>{ticker}</span>
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

function pillStyle(active: boolean): CSSProperties {
  return {
    fontSize: 11.5,
    fontWeight: 700,
    padding: '4px 9px',
    borderRadius: 6,
    cursor: 'pointer',
    border: active ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.3))',
    background: active ? 'var(--accent)' : 'transparent',
    color: active ? '#fff' : 'var(--text-primary)',
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
    padding: '4px 8px 4px 10px',
    borderRadius: 7,
    cursor: 'pointer',
    border: '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
    background: open ? 'color-mix(in srgb, var(--accent) 12%, transparent)' : 'transparent',
    color: 'var(--text-primary)',
    fontSize: 11.5,
    fontWeight: 700,
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
  const cur = options.find((o) => o.id === value);
  return (
    <div style={{ position: 'relative', flexShrink: 0 }}>
      <button type="button" title={title} style={ddBtnStyle(open)} onClick={() => setOpen((v) => !v)}>
        <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 150 }}>{cur?.label ?? '—'}</span>
        <ChevronDown size={13} style={{ flexShrink: 0, opacity: 0.7 }} />
      </button>
      {open && (
        <Popover align="left" onClose={() => setOpen(false)}>
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
                    padding: '8px 10px',
                    borderRadius: 7,
                    border: 'none',
                    background: on ? 'color-mix(in srgb, var(--accent) 14%, transparent)' : 'transparent',
                    color: on ? 'var(--accent)' : 'var(--text-primary)',
                    fontWeight: on ? 800 : 600,
                    fontSize: 12.5,
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
    border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
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
