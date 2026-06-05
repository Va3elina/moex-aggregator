/**
 * EmbedSettings — общий «скелет» виджета: минимальная шапка + выезжающая панель
 * настроек (как у Trading Tools: шестерёнка рядом с темой/закрытием → разделы).
 *
 * Зачем: в плавающей панели мало места. Раньше asset-picker (огромная модалка) +
 * режимы + период толкались в одной строке шапки и не влезали. Теперь шапка —
 * только заголовок, а ВСЕ контролы (актив / таймфрейм / период / режим) живут в
 * drawer'е по разделам. Drawer открывается:
 *   - в панели расширения — кнопкой «⚙» в заголовке панели (widget.js шлёт
 *     postMessage {source:'frame-ext', type:'toggle-settings'} в iframe);
 *   - в отдельном окне (pop-out, расширения нет) — своей шестерёнкой в шапке.
 *
 * Всё инлайн-стилями с CSS-var, чтобы работать в любой теме внутри iframe.
 */
import { useEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { Settings, X, Search } from 'lucide-react';
import InstrumentIcon from '../../components/InstrumentIcon';
import { formatCompact } from '../../utils/formatNumber';

/* ─────────────────────────── hook ─────────────────────────── */

/**
 * Состояние drawer'а + слушатель postMessage от расширения.
 * `standalone` = true, когда embed открыт как самостоятельная страница
 * (pop-out), а не внутри iframe панели — тогда показываем свою шестерёнку.
 */
export function useEmbedSettings() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    function onMsg(e: MessageEvent) {
      const d = e.data;
      if (d && d.source === 'frame-ext' && d.type === 'toggle-settings') {
        setOpen((v) => !v);
      }
    }
    window.addEventListener('message', onMsg);
    return () => window.removeEventListener('message', onMsg);
  }, []);

  const standalone = typeof window !== 'undefined' && window.parent === window;
  return {
    open,
    standalone,
    toggle: () => setOpen((v) => !v),
    close: () => setOpen(false),
  };
}

export type EmbedSettingsApi = ReturnType<typeof useEmbedSettings>;

/* ─────────────────────────── shell ─────────────────────────── */

const column: CSSProperties = {
  width: '100%',
  height: '100%',
  display: 'flex',
  flexDirection: 'column',
  boxSizing: 'border-box',
  padding: 12,
  position: 'relative',
};

const headerRow: CSSProperties = {
  flexShrink: 0,
  display: 'flex',
  alignItems: 'baseline',
  gap: 8,
  minHeight: 26,
  marginBottom: 8,
};

/**
 * EmbedShell — обёртка виджета: шапка (заголовок + подзаголовок + шестерёнка
 * в standalone) + область графика (children) + drawer настроек (drawer).
 */
export function EmbedShell({
  settings,
  title,
  subtitle,
  drawer,
  children,
}: {
  settings: EmbedSettingsApi;
  title: string;
  subtitle?: string;
  drawer: ReactNode;
  children: ReactNode;
}) {
  return (
    <div style={column}>
      <div style={headerRow}>
        <span style={{ fontWeight: 800, fontSize: 14, color: 'var(--text-primary)', letterSpacing: '-0.01em' }}>
          {title}
        </span>
        {subtitle && (
          <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{subtitle}</span>
        )}
        {settings.standalone && (
          <button
            onClick={settings.toggle}
            title="Настройки"
            aria-label="Настройки"
            style={{
              marginLeft: 'auto',
              width: 26,
              height: 26,
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
              borderRadius: 5,
              background: 'transparent',
              color: 'var(--text-secondary)',
              cursor: 'pointer',
              flexShrink: 0,
            }}
          >
            <Settings size={15} />
          </button>
        )}
      </div>

      {children}

      <SettingsDrawer open={settings.open} onClose={settings.close} title={subtitle || title}>
        {drawer}
      </SettingsDrawer>
    </div>
  );
}

/* ─────────────────────────── drawer ─────────────────────────── */

function SettingsDrawer({
  open,
  onClose,
  title,
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  children: ReactNode;
}) {
  return (
    <div
      // position:fixed внутри iframe = область панели. Закрытый — уезжает вправо.
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 50,
        background: 'var(--bg-base, var(--bg-primary))',
        display: 'flex',
        flexDirection: 'column',
        transform: open ? 'translateX(0)' : 'translateX(100%)',
        opacity: open ? 1 : 0,
        pointerEvents: open ? 'auto' : 'none',
        transition: 'transform 0.18s ease, opacity 0.18s ease',
        boxShadow: open ? '-8px 0 24px rgba(0,0,0,0.18)' : 'none',
      }}
      aria-hidden={!open}
    >
      <div
        style={{
          flexShrink: 0,
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          padding: '12px 14px',
          borderBottom: '1.5px solid var(--border-color, rgba(128,128,128,0.3))',
        }}
      >
        <Settings size={16} style={{ color: 'var(--accent)' }} />
        <span style={{ fontWeight: 800, fontSize: 13.5, color: 'var(--text-primary)' }}>Настройки</span>
        <span style={{ fontSize: 12, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          · {title}
        </span>
        <button
          onClick={onClose}
          title="Закрыть"
          aria-label="Закрыть настройки"
          style={{
            marginLeft: 'auto',
            width: 26,
            height: 26,
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
            borderRadius: 5,
            background: 'transparent',
            color: 'var(--text-secondary)',
            cursor: 'pointer',
            flexShrink: 0,
          }}
        >
          <X size={15} />
        </button>
      </div>

      <div
        style={{
          flex: 1,
          minHeight: 0,
          overflowY: 'auto',
          padding: 14,
          display: 'flex',
          flexDirection: 'column',
          gap: 18,
        }}
      >
        {children}
      </div>
    </div>
  );
}

/* ─────────────────────── section + controls ─────────────────────── */

/** Раздел настроек: метка-капс + контрол. */
export function DrawerSection({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <div
        style={{
          fontSize: 10.5,
          fontWeight: 800,
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          color: 'var(--text-secondary)',
          marginBottom: 8,
        }}
      >
        {label}
      </div>
      {children}
    </div>
  );
}

function segStyle(active: boolean): CSSProperties {
  return {
    fontSize: 12,
    fontWeight: 700,
    padding: '6px 12px',
    borderRadius: 6,
    cursor: 'pointer',
    border: active ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
    background: active ? 'var(--accent)' : 'transparent',
    color: active ? '#fff' : 'var(--text-primary)',
    lineHeight: 1.3,
    whiteSpace: 'nowrap',
    transition: 'background 0.12s, border-color 0.12s',
  };
}

/** Группа сегмент-кнопок (режим / период / таймфрейм). Переносится на узких панелях. */
export function SegGroup<T extends string | number>({
  value,
  options,
  onChange,
}: {
  value: T;
  options: { id: T; label: string }[];
  onChange: (v: T) => void;
}) {
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 7 }}>
      {options.map((o) => (
        <button key={String(o.id)} style={segStyle(o.id === value)} onClick={() => onChange(o.id)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

/* ─────────────────────── compact asset picker ─────────────────────── */

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
 * AssetPickerInline — компактный выбор актива ВНУТРИ drawer'а (вместо огромной
 * полноэкранной InstrumentSearchModal). Поиск + плотный список. Гейтинг не нужен:
 * embed целиком под PRO-токеном.
 */
export function AssetPickerInline({
  filterType,
  current,
  active,
  onSelect,
}: {
  filterType: 'stock' | 'futures';
  current: string;
  active: boolean; // грузим инструменты только когда drawer открыт
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

  // Уникальные по sectype (берём контракт с бОльшим объёмом), фильтр по запросу,
  // сорт по объёму. Текущий — наверх.
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
                <span
                  style={{
                    fontSize: 11.5,
                    color: 'var(--text-secondary)',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    flex: 1,
                    minWidth: 0,
                  }}
                >
                  {it.name}
                </span>
                {it.day_change_pct != null && (
                  <span
                    style={{
                      fontSize: 11.5,
                      fontVariantNumeric: 'tabular-nums',
                      flexShrink: 0,
                      color: it.day_change_pct >= 0 ? 'var(--funds-flow-positive)' : 'var(--funds-flow-negative)',
                    }}
                  >
                    {it.day_change_pct >= 0 ? '+' : ''}{it.day_change_pct.toFixed(2)}%
                  </span>
                )}
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
