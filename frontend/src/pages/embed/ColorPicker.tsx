/**
 * Выбор цвета элемента индикатора — по образцу TradingView (скриншот Вадима):
 * сетка оттенков, прозрачность, толщина линии и её стиль в одном поповере.
 *
 * Почему сетка, а не шесть свотчей как было: на одной панели теперь живут линия
 * индикатора, её сглаживающая, три границы зон и заливки. Шести цветов на всё
 * это не хватает физически — соседние элементы приходится красить одинаково, и
 * график перестаёт читаться. Сетка строится из оттенков, а не задаётся списком:
 * так добавить колонку — одна цифра, и все ряды остаются согласованными.
 *
 * ⚠️ Цвета здесь ЛИТЕРАЛЬНЫЕ hex, не токены темы. Это осознанный выбор
 * пользователя: он видит конкретный цвет в палитре и ждёт ровно его в обеих
 * темах — та же логика, что у свотчей ⚙-Формата (см. EmbedFormat).
 */
import { useEffect, useRef, useState, type CSSProperties } from 'react';

/** Стиль одного элемента индикатора. */
export interface ElStyle {
  color?: string;
  /** 0..100. Отдельно от цвета: прозрачность крутят чаще, чем меняют тон. */
  opacity?: number;
  width?: 1 | 2 | 3 | 4;
  dash?: 'solid' | 'dashed' | 'dotted';
}

// Базовые тона строятся по кругу HSL. 10 колонок — как в оригинале: больше не
// влезает в узкую панель песочницы, меньше — не хватает на элементы индикатора.
const HUES = [0, 28, 52, 128, 168, 190, 216, 260, 292, 340];
/** Ряды: от светлых к тёмным. Средний ряд (индекс 3) — «чистый» тон. */
const ROWS = [
  { s: 82, l: 88 }, { s: 78, l: 78 }, { s: 74, l: 68 }, { s: 70, l: 58 },
  { s: 68, l: 48 }, { s: 66, l: 38 }, { s: 62, l: 28 },
];
const GREYS = ['#FFFFFF', '#DEDEDE', '#BDBDBD', '#9C9C9C', '#7B7B7B', '#5A5A5A', '#434343', '#2C2C2C', '#161616', '#000000'];

function hsl(h: number, s: number, l: number): string {
  // В hex, а не в hsl(): значение уходит в canvas и в localStorage, а там строка
  // должна быть самодостаточной и одинаково читаться где угодно.
  const a = (s / 100) * Math.min(l / 100, 1 - l / 100);
  const f = (n: number) => {
    const k = (n + h / 30) % 12;
    const v = l / 100 - a * Math.max(-1, Math.min(k - 3, Math.min(9 - k, 1)));
    return Math.round(255 * v).toString(16).padStart(2, '0');
  };
  return `#${f(0)}${f(8)}${f(4)}`.toUpperCase();
}

const GRID: string[][] = [GREYS, ...ROWS.map((r) => HUES.map((h) => hsl(h, r.s, r.l)))];

/** Кнопка-образец: текущий цвет + превью линии. Открывает поповер. */
export function ColorButton({ value, onChange, showLine = true }: {
  value: ElStyle;
  onChange: (p: Partial<ElStyle>) => void;
  /** Для заливок линии не нужны — только цвет и прозрачность. */
  showLine?: boolean;
}) {
  const [open, setOpen] = useState(false);
  // ⚠️ Поповер позиционируется FIXED по прямоугольнику кнопки. Внутри окна
  // настроек тело прокручиваемое (overflow-y), и absolute-поповер оно обрезает —
  // «Стиль линии» просто не видно. Якорь считаем в момент открытия.
  const [anchor, setAnchor] = useState<{ top: number; left: number } | null>(null);
  const ref = useRef<HTMLDivElement | null>(null);
  const btnRef = useRef<HTMLButtonElement | null>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: PointerEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [open]);

  const color = value.color ?? '#9B8BF0';
  const op = value.opacity ?? 100;
  return (
    <div ref={ref} style={{ position: 'relative', display: 'inline-flex' }}>
      <button
        ref={btnRef}
        type="button"
        onClick={() => {
          const r = btnRef.current?.getBoundingClientRect();
          if (r) {
            // Влево от правого края кнопки, вниз — но не за нижнюю кромку экрана.
            const w = 232, h = showLine ? 330 : 190;
            setAnchor({
              top: Math.max(8, Math.min(r.bottom + 5, window.innerHeight - h - 8)),
              left: Math.max(8, Math.min(r.right - w, window.innerWidth - w - 8)),
            });
          }
          setOpen((v) => !v);
        }}
        title="Цвет и линия"
        style={{
          display: 'inline-flex', alignItems: 'center', gap: 5, padding: '2px 5px', cursor: 'pointer',
          borderRadius: 6, border: '1px solid var(--border-color, rgba(128,128,128,0.35))', background: 'transparent',
        }}
      >
        <span style={{ width: 15, height: 15, borderRadius: 3, background: color, opacity: op / 100, flexShrink: 0 }} />
        {showLine && <LinePreview color={color} width={value.width ?? 2} dash={value.dash ?? 'solid'} w={26} />}
      </button>
      {open && anchor && (
        <div style={{ ...POPOVER, top: anchor.top, left: anchor.left }}>
          <div style={{ display: 'grid', gridTemplateColumns: `repeat(${HUES.length}, 1fr)`, gap: 3 }}>
            {GRID.flat().map((c) => (
              <button
                key={c} type="button" title={c} onClick={() => onChange({ color: c })}
                style={{
                  width: 15, height: 15, borderRadius: 3, background: c, cursor: 'pointer', padding: 0,
                  border: c.toUpperCase() === color.toUpperCase() ? '2px solid var(--text-primary)' : '1px solid rgba(128,128,128,0.28)',
                }}
              />
            ))}
          </div>

          <div style={{ marginTop: 10 }}>
            <Label>Прозрачность</Label>
            <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
              <input
                type="range" min={0} max={100} value={op}
                onChange={(e) => onChange({ opacity: Number(e.target.value) })}
                style={{ flex: 1, accentColor: color }}
              />
              <span style={{ fontSize: 11, minWidth: 34, textAlign: 'right', color: 'var(--text-primary)', fontVariantNumeric: 'tabular-nums' }}>{op}%</span>
            </div>
          </div>

          {showLine && (
            <>
              <div style={{ marginTop: 10 }}>
                <Label>Толщина</Label>
                <div style={{ display: 'flex', gap: 4 }}>
                  {([1, 2, 3, 4] as const).map((w) => (
                    <button
                      key={w} type="button" onClick={() => onChange({ width: w })} title={`${w} px`}
                      style={optionBtn((value.width ?? 2) === w)}
                    >
                      <LinePreview color={color} width={w} dash="solid" w={30} />
                    </button>
                  ))}
                </div>
              </div>
              <div style={{ marginTop: 10 }}>
                <Label>Стиль линии</Label>
                <div style={{ display: 'flex', gap: 4 }}>
                  {(['solid', 'dashed', 'dotted'] as const).map((d) => (
                    <button
                      key={d} type="button" onClick={() => onChange({ dash: d })}
                      style={optionBtn((value.dash ?? 'solid') === d)}
                    >
                      <LinePreview color={color} width={2} dash={d} w={40} />
                    </button>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

/** Превью линии — SVG, а не CSS-border: пунктир и точки в border рисуются
 *  браузером по-своему и не совпадают с тем, что даст канвас графика. */
function LinePreview({ color, width, dash, w }: { color: string; width: number; dash: 'solid' | 'dashed' | 'dotted'; w: number }) {
  const da = dash === 'dashed' ? '5 4' : dash === 'dotted' ? '1 3' : undefined;
  return (
    <svg width={w} height={12} style={{ display: 'block', overflow: 'visible' }}>
      <line
        x1={0} y1={6} x2={w} y2={6} stroke={color} strokeWidth={width}
        strokeLinecap={dash === 'dotted' ? 'round' : 'butt'}
        {...(da ? { strokeDasharray: da } : {})}
      />
    </svg>
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 10.5, color: 'var(--text-secondary)', marginBottom: 5 }}>{children}</div>;
}

const optionBtn = (active: boolean): CSSProperties => ({
  flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '5px 4px',
  borderRadius: 6, cursor: 'pointer',
  border: active ? '1px solid var(--accent)' : '1px solid var(--border-color, rgba(128,128,128,0.3))',
  background: active ? 'color-mix(in srgb, var(--accent) 14%, transparent)' : 'transparent',
});

const POPOVER: CSSProperties = {
  position: 'fixed', zIndex: 100001, width: 232, padding: 9,
  borderRadius: 9,
  background: 'var(--bg-secondary, #17161A)',
  border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
  boxShadow: '0 8px 26px rgba(0,0,0,0.45)',
};
