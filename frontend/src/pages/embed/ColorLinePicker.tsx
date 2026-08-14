/**
 * ColorLinePicker — компактная кнопка «свотч цвета + образец линии» у имени
 * серии в ⚙-настройках и детальный поповер к ней (по мокапу TradingView):
 *   - сетка цветов (серые + 6 оттенков на 10 тонов);
 *   - «свои цвета»: сохранённые + «+» (нативный color input), общие на все панели;
 *   - слайдер прозрачности;
 *   - толщина линии (1..4).
 * Заменил ряд заготовленных свотчей в FormatSection (фидбек Вадима: нужна
 * подробная настройка цвета и линий, как в рисовалке).
 *
 * Поповер идёт порталом в body с fixed-позицией от кнопки: ⚙-дровер узкий и
 * скроллится, absolute внутри него обрезался бы. usePortalTheme — как в
 * ChartSettings: в песочнице тема живёт на .sb-panel, порталу нужен атрибут.
 */
import { useEffect, useRef, useState, type CSSProperties } from 'react';
import { createPortal } from 'react-dom';
import { usePortalTheme } from '../../hooks/usePortalTheme';

export interface ColorLineValue {
  /** null = «Авто» — родной цвет индикатора. */
  color: string | null;
  /** 0..100, дефолт 100. */
  opacity: number;
  /** 1..4; null = авто (глобальная толщина песочницы). */
  width: number | null;
}

interface Props {
  value: ColorLineValue;
  onColor: (c: string | null) => void;
  onOpacity: (o: number) => void;
  onWidth: (w: number) => void;
  /** Чем красить превью при color=null (родной цвет серии). */
  defaultColor?: string;
}

// ── палитра ──────────────────────────────────────────────────────────────────
function hex(h: number, s: number, l: number): string {
  const a = (s / 100) * Math.min(l / 100, 1 - l / 100);
  const f = (n: number) => {
    const k = (n + h / 30) % 12;
    const c = l / 100 - a * Math.max(-1, Math.min(k - 3, 9 - k, 1));
    return Math.round(255 * c).toString(16).padStart(2, '0');
  };
  return `#${f(0)}${f(8)}${f(4)}`.toUpperCase();
}

const GRAYS = ['#FFFFFF', '#E9E9E9', '#D3D3D3', '#BCBCBC', '#A3A3A3', '#8A8A8A', '#6E6E6E', '#525252', '#2D2D2D', '#000000'];
const HUES = [0, 25, 50, 95, 140, 170, 195, 225, 265, 310];
// Строки: яркая, затем от светлой к тёмной (как в мокапе).
const ROWS: [number, number][] = [[90, 58], [70, 82], [68, 72], [66, 62], [64, 48], [62, 36]];
const PALETTE: string[][] = [GRAYS, ...ROWS.map(([s, l]) => HUES.map((h) => hex(h, s, l)))];

// ── свои цвета (общие на все панели/индикаторы) ──────────────────────────────
const CUSTOM_KEY = 'frame:sb:custom-colors';
const CUSTOM_MAX = 10;
function readCustom(): string[] {
  try {
    const j = JSON.parse(localStorage.getItem(CUSTOM_KEY) ?? '[]');
    return Array.isArray(j) ? j.filter((c): c is string => typeof c === 'string' && /^#[0-9A-Fa-f]{6}$/.test(c)).slice(0, CUSTOM_MAX) : [];
  } catch { return []; }
}
function saveCustom(list: string[]) {
  try { localStorage.setItem(CUSTOM_KEY, JSON.stringify(list.slice(0, CUSTOM_MAX))); } catch { /* quota */ }
}

// ── стили ────────────────────────────────────────────────────────────────────
const cell = (active: boolean, bg: string): CSSProperties => ({
  width: 18, height: 18, borderRadius: 4, background: bg, cursor: 'pointer', padding: 0,
  border: active ? '2px solid var(--accent)' : '1px solid rgba(128,128,128,0.3)',
  boxSizing: 'border-box', flexShrink: 0,
});
const capsLabel: CSSProperties = {
  fontSize: 10.5, fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.06em',
  color: 'var(--text-secondary)', margin: '10px 0 6px',
};

export default function ColorLinePicker({ value, onColor, onOpacity, onWidth, defaultColor }: Props) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);
  const [custom, setCustom] = useState<string[]>(readCustom);
  const btnRef = useRef<HTMLButtonElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const colorInputRef = useRef<HTMLInputElement>(null);
  const portalTheme = usePortalTheme();

  const shown = value.color ?? defaultColor ?? 'var(--accent)';
  const lw = value.width ?? 2;

  const POP_W = 246;
  const toggle = () => {
    if (open) { setOpen(false); return; }
    const r = btnRef.current?.getBoundingClientRect();
    if (!r) return;
    // Правый край поповера к правому краю кнопки; вниз, а если не влезает — вверх.
    const left = Math.max(8, Math.min(r.right - POP_W, window.innerWidth - POP_W - 8));
    const below = r.bottom + 6;
    const top = below + 380 > window.innerHeight ? Math.max(8, r.top - 386) : below;
    setPos({ top, left });
    setOpen(true);
  };

  // Клик мимо / Esc / скролл-резайз окна — закрыть.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: PointerEvent) => {
      const t = e.target as Node;
      if (popRef.current?.contains(t) || btnRef.current?.contains(t)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    const onAway = () => setOpen(false);
    document.addEventListener('pointerdown', onDown);
    document.addEventListener('keydown', onKey);
    window.addEventListener('resize', onAway);
    return () => {
      document.removeEventListener('pointerdown', onDown);
      document.removeEventListener('keydown', onKey);
      window.removeEventListener('resize', onAway);
    };
  }, [open]);

  const pick = (c: string) => onColor(c);
  const addCustom = (c: string) => {
    const up = c.toUpperCase();
    setCustom((list) => {
      const next = [up, ...list.filter((x) => x !== up)].slice(0, CUSTOM_MAX);
      saveCustom(next);
      return next;
    });
    onColor(up);
  };

  return (
    <>
      {/* Кнопка-превью: свотч + линия текущей толщины (мокап 2). */}
      <button
        ref={btnRef}
        type="button"
        title="Цвет и линия"
        onClick={toggle}
        style={{
          display: 'inline-flex', alignItems: 'center', gap: 7, cursor: 'pointer',
          padding: '5px 9px', borderRadius: 8, boxSizing: 'border-box',
          background: 'var(--bg-secondary)',
          border: open ? '1.5px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
        }}
      >
        <span style={{ width: 16, height: 14, borderRadius: 4, background: shown, opacity: value.opacity / 100, flexShrink: 0 }} />
        <span style={{ width: 22, height: Math.max(1.5, lw), borderRadius: lw, background: shown, opacity: value.opacity / 100, flexShrink: 0 }} />
      </button>
      {open && pos && createPortal(
        <div
          ref={popRef}
          {...portalTheme}
          // Маркер для outside-click родительских поповеров (⚙-дровер тулбара):
          // мы портал в body, для его ref.contains мы «снаружи» — см. EmbedToolbar.
          data-keep-parent-open=""
          style={{
            // zIndex выше ⚙-дровера (2147483000), иначе поповер уезжает под него.
            position: 'fixed', top: pos.top, left: pos.left, width: POP_W, zIndex: 2147483100,
            background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
            boxShadow: '4px 4px 0 0 var(--text-primary)', borderRadius: 12, padding: 12,
            boxSizing: 'border-box',
          }}
        >
          {/* Авто — сброс на родной цвет индикатора. */}
          <button
            type="button"
            onClick={() => onColor(null)}
            style={{
              width: '100%', marginBottom: 8, padding: '5px 8px', borderRadius: 6, cursor: 'pointer',
              fontSize: 11, fontWeight: 700, textAlign: 'center',
              border: value.color === null ? '2px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
              background: 'var(--bg-secondary)', color: 'var(--text-primary)', boxSizing: 'border-box',
            }}
          >
            Авто (цвет индикатора)
          </button>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {PALETTE.map((row, ri) => (
              <div key={ri} style={{ display: 'flex', gap: 4 }}>
                {row.map((c) => (
                  <button key={c} type="button" title={c} onClick={() => pick(c)} style={cell(value.color === c, c)} />
                ))}
              </div>
            ))}
          </div>
          {/* Свои цвета: сохранённые + «+». */}
          <div style={{ display: 'flex', gap: 4, marginTop: 8, flexWrap: 'wrap', alignItems: 'center' }}>
            {custom.map((c) => (
              <button key={c} type="button" title={c} onClick={() => pick(c)} style={cell(value.color === c, c)} />
            ))}
            <button
              type="button"
              title="Добавить свой цвет"
              onClick={() => colorInputRef.current?.click()}
              style={{ ...cell(false, 'var(--bg-secondary)'), color: 'var(--text-secondary)', fontSize: 13, fontWeight: 700, lineHeight: 1, display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}
            >
              +
            </button>
            <input
              ref={colorInputRef}
              type="color"
              defaultValue={/^#[0-9A-Fa-f]{6}$/.test(value.color ?? '') ? (value.color as string) : '#5DA3E9'}
              onChange={(e) => addCustom(e.target.value)}
              style={{ position: 'absolute', width: 0, height: 0, opacity: 0, pointerEvents: 'none' }}
            />
          </div>
          <div style={capsLabel}>Прозрачность</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <input
              type="range" min={5} max={100} step={5} value={value.opacity}
              onChange={(e) => onOpacity(Number(e.target.value))}
              style={{ flex: 1, accentColor: value.color ?? 'var(--accent)' }}
            />
            <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-primary)', width: 36, textAlign: 'right' }}>{value.opacity}%</span>
          </div>
          <div style={capsLabel}>Толщина</div>
          <div style={{ display: 'flex', gap: 4 }}>
            {[1, 2, 3, 4].map((w) => (
              <button
                key={w}
                type="button"
                title={`${w}px`}
                onClick={() => onWidth(w)}
                style={{
                  flex: 1, height: 28, borderRadius: 6, cursor: 'pointer', padding: 0,
                  display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                  border: lw === w ? '2px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
                  background: 'var(--bg-secondary)', boxSizing: 'border-box',
                }}
              >
                <span style={{ width: 26, height: w, borderRadius: w, background: 'var(--text-primary)' }} />
              </button>
            ))}
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}
