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
import { PlusCircle, Settings, ChevronDown, Search } from 'lucide-react';
import InstrumentIcon from '../../components/InstrumentIcon';
import { formatCompact } from '../../utils/formatNumber';

const TOOLBAR_H = 40;

/* ───────────────────── vertical resize (обычное колесо) ───────────────────── */

/**
 * Высоту панели держит расширение (снаружи iframe). Обычное колесо над графиком
 * шлёт запрос наверх; widget.js растит st.h и панель (а с ней и график) тянется.
 * В pop-out (нет родителя-панели) пробуем ресайзнуть само окно.
 */
function requestVerticalResize(dh: number) {
  try {
    if (window.parent && window.parent !== window) {
      window.parent.postMessage({ source: 'frame-embed', type: 'resize-v', dh }, '*');
      return;
    }
  } catch { /* cross-origin — postMessage всё равно уходит */ }
  try { window.resizeBy(0, dh); } catch { /* окно не скриптовое — no-op */ }
}

/* ───────────────────────────── shell ───────────────────────────── */

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

const toolbarRow: CSSProperties = {
  flexShrink: 0,
  display: 'flex',
  alignItems: 'center',
  gap: 8,
  minHeight: TOOLBAR_H,
  padding: '5px 8px',
  borderBottom: '1px solid var(--border-color, rgba(128,128,128,0.18))',
  position: 'relative',
  zIndex: 3,
};

/**
 * EmbedFrame — обёртка виджета: тулбар (lead + inline + ⚙more) + область графика.
 * onZoomTime(dir): dir=+1 «зум внутрь» (меньше истории), -1 «зум наружу» (больше).
 */
export function EmbedFrame({
  lead,
  toolbar,
  more,
  moreLabel = 'Ещё',
  onZoomTime,
  children,
}: {
  lead?: ReactNode;
  toolbar?: ReactNode;
  more?: ReactNode;
  moreLabel?: string;
  onZoomTime?: (dir: 1 | -1) => void;
  children: ReactNode;
}) {
  const [moreOpen, setMoreOpen] = useState(false);
  const chartRef = useRef<HTMLDivElement>(null);
  const zoomRef = useRef(onZoomTime);
  zoomRef.current = onZoomTime;

  // Нативный не-пассивный wheel: React onWheel пассивен → preventDefault игнорился бы.
  useEffect(() => {
    const el = chartRef.current;
    if (!el) return;
    let accZoom = 0;
    let accH = 0;
    let raf = 0;
    const flush = () => { raf = 0; if (accH) { requestVerticalResize(accH); accH = 0; } };
    const onWheel = (e: WheelEvent) => {
      if (e.shiftKey) {
        const z = zoomRef.current;
        if (!z) return;
        e.preventDefault();
        accZoom += e.deltaY;
        if (Math.abs(accZoom) >= 120) { z(accZoom < 0 ? 1 : -1); accZoom = 0; }
        return;
      }
      e.preventDefault();
      accH += e.deltaY < 0 ? 36 : -36;
      if (!raf) raf = requestAnimationFrame(flush);
    };
    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, []);

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
      <div ref={chartRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
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

interface PickInstrument {
  sec_id: string;
  sectype: string;
  name: string;
  type: string;
  group?: string;
  daily_volume?: number;
  day_change_pct?: number | null;
}

/**
 * AssetButton — «кружок-плюс» + текущий тикер; клик → поповер с поиском актива.
 * Отдельная самостоятельная кнопка (не спрятана в шестерёнке).
 */
export function AssetButton({
  ticker,
  filterType,
  current,
  onSelect,
}: {
  ticker: string;
  filterType: 'stock' | 'futures';
  current: string;
  onSelect: (secid: string, name: string) => void;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ position: 'relative', flexShrink: 0 }}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
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
        <Popover align="left" onClose={() => setOpen(false)}>
          <AssetSearch
            filterType={filterType}
            current={current}
            active={open}
            onSelect={(s, n) => { onSelect(s, n); setOpen(false); }}
          />
        </Popover>
      )}
    </div>
  );
}

/**
 * AssetSearch — поиск + плотный список инструментов (тот же, что в старом
 * AssetPickerInline, но живёт в поповере тулбара). /api/instruments — публичный.
 */
function AssetSearch({
  filterType,
  current,
  active,
  onSelect,
}: {
  filterType: 'stock' | 'futures';
  current: string;
  active: boolean;
  onSelect: (secid: string, name: string) => void;
}) {
  const [q, setQ] = useState('');
  const [items, setItems] = useState<PickInstrument[]>([]);
  const [loading, setLoading] = useState(true);
  const loadedFor = useRef<string | null>(null);

  useEffect(() => {
    if (!active || loadedFor.current === filterType) return;
    let cancelled = false;
    setLoading(true);
    fetch(`/api/instruments?type=${filterType}`)
      .then((r) => r.json())
      .then((d) => {
        if (cancelled) return;
        setItems(d.instruments || []);
        loadedFor.current = filterType;
      })
      .catch(() => { /* список не критичен */ })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [active, filterType]);

  const ql = q.trim().toLowerCase();
  const seen = new Map<string, PickInstrument>();
  for (const it of items) {
    const prev = seen.get(it.sectype);
    if (!prev || (it.daily_volume || 0) > (prev.daily_volume || 0)) seen.set(it.sectype, it);
  }
  const list = Array.from(seen.values())
    .filter((it) => !ql || it.sectype.toLowerCase().includes(ql) || it.name.toLowerCase().includes(ql))
    .sort((a, b) => {
      if (a.sectype === current) return -1;
      if (b.sectype === current) return 1;
      return (b.daily_volume || 0) - (a.daily_volume || 0);
    })
    .slice(0, 60);

  return (
    <div>
      <div style={{ position: 'relative', marginBottom: 8 }}>
        <Search size={14} style={{ position: 'absolute', left: 9, top: '50%', transform: 'translateY(-50%)', color: 'var(--text-secondary)' }} />
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Поиск актива"
          autoFocus
          style={{
            width: '100%',
            boxSizing: 'border-box',
            padding: '7px 10px 7px 30px',
            fontSize: 13,
            borderRadius: 7,
            border: '1.5px solid var(--border-color, rgba(128,128,128,0.4))',
            background: 'var(--bg-secondary, transparent)',
            color: 'var(--text-primary)',
            outline: 'none',
          }}
        />
      </div>
      <div
        className="styled-scrollbar"
        style={{
          maxHeight: 240,
          overflowY: 'auto',
          border: '1px solid var(--border-color, rgba(128,128,128,0.25))',
          borderRadius: 8,
        }}
      >
        {loading ? (
          <div style={{ padding: 16, textAlign: 'center', color: 'var(--text-secondary)', fontSize: 12 }}>Загрузка…</div>
        ) : list.length === 0 ? (
          <div style={{ padding: 16, textAlign: 'center', color: 'var(--text-secondary)', fontSize: 12 }}>Ничего не найдено</div>
        ) : (
          list.map((it) => {
            const isCur = it.sectype === current;
            return (
              <button
                key={it.sectype}
                onClick={() => onSelect(it.sectype, it.name)}
                style={{
                  width: '100%',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 9,
                  padding: '7px 10px',
                  border: 'none',
                  borderBottom: '1px solid var(--border-color, rgba(128,128,128,0.12))',
                  background: isCur ? 'color-mix(in srgb, var(--accent) 12%, transparent)' : 'transparent',
                  color: 'var(--text-primary)',
                  cursor: 'pointer',
                  textAlign: 'left',
                }}
              >
                <span style={{ flexShrink: 0, lineHeight: 0 }}>
                  <InstrumentIcon sectype={it.sectype} size={22} />
                </span>
                <span style={{ fontWeight: 700, fontSize: 12.5, flexShrink: 0 }}>{it.sectype}</span>
                <span style={{ fontSize: 11.5, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1, minWidth: 0 }}>
                  {it.name}
                </span>
                {it.daily_volume ? (
                  <span style={{ fontSize: 11, color: 'var(--text-muted)', flexShrink: 0, fontVariantNumeric: 'tabular-nums', minWidth: 44, textAlign: 'right' }}>
                    {formatCompact(it.daily_volume)}
                  </span>
                ) : null}
              </button>
            );
          })
        )}
      </div>
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
