/**
 * EmbedFormat — общий ⚙-блок «Формат» графика (§6 спеки песочницы): тип
 * главной серии (линия / область / столбцы) + свотчи цвета. Применяется
 * СРАЗУ, без закрытия поповера. Настройка персистится per-индикатор.
 *
 * Цвета свотчей — дизайнерская CC-палитра мокапа (литеральные hex, одинаковы
 * в обеих темах): это осознанный выбор пользователя, поэтому он одинаков на
 * сайте, в расширении и в песочнице.
 */
import { useCallback, useEffect, useState, type CSSProperties } from 'react';
import type { LwSeries } from '../../components/LwChart';
import { DrawerSection, SegGroup } from './EmbedSettings';
import { useEmbedPersist } from './embedPersist';

export type ChartKind = 'line' | 'area' | 'histogram';
export interface ChartFormat { kind: ChartKind; color: string | null } // color null = цвет индикатора

const KINDS: { id: ChartKind; label: string }[] = [
  { id: 'line', label: 'Линия' },
  { id: 'area', label: 'Область' },
  { id: 'histogram', label: 'Столбцы' },
];

// CC-палитра дизайнера: price / up / down / sec / amber / cyan.
const SWATCHES = ['#5DA3E9', '#5BD49C', '#EF6F6F', '#9B8BF0', '#E0A34E', '#57C7C7'];

const DEF: ChartFormat = { kind: 'line', color: null };

function parse(raw: string): ChartFormat {
  try {
    const j = JSON.parse(raw) as Partial<ChartFormat>;
    const kind = KINDS.some((k) => k.id === j.kind) ? (j.kind as ChartKind) : 'line';
    const color = typeof j.color === 'string' && SWATCHES.includes(j.color) ? j.color : null;
    return { kind, color };
  } catch { return DEF; }
}

/** Формат с персистом в localStorage по ключу индикатора. `defKind` — родной тип серии. */
export function useChartFormat(lsKey: string, defKind: ChartKind = 'line') {
  const { rd, wr } = useEmbedPersist();
  const [fmt, setFmt] = useState<ChartFormat>(() => {
    const raw = rd(lsKey, '');
    return raw ? parse(raw) : { ...DEF, kind: defKind };
  });
  useEffect(() => { wr(lsKey, JSON.stringify(fmt)); }, [lsKey, fmt, wr]);
  return {
    fmt,
    setKind: (kind: ChartKind) => setFmt((f) => ({ ...f, kind })),
    setColor: (color: string | null) => setFmt((f) => ({ ...f, color })),
  };
}

/**
 * §OI-5 — пер-серийный формат: карта {seriesId → ChartFormat} в одном JSON под
 * ключом индикатора. Каждая линия окна (цена + каждая линия ОИ) кастомизируется
 * отдельно: тип (линия/область/столбцы) + цвет. `get(id)` даёт формат серии (или
 * дефолт), `setKind/setColor(id, …)` меняют её. Персист per-индикатор (pid-namespace).
 */
export function useSeriesFormats(lsKey: string) {
  const { rd, wr } = useEmbedPersist();
  const [map, setMap] = useState<Record<string, ChartFormat>>(() => {
    const raw = rd(lsKey, '');
    if (!raw) return {};
    try {
      const j = JSON.parse(raw) as Record<string, unknown>;
      const out: Record<string, ChartFormat> = {};
      for (const k of Object.keys(j)) out[k] = parse(JSON.stringify(j[k]));
      return out;
    } catch { return {}; }
  });
  useEffect(() => { wr(lsKey, JSON.stringify(map)); }, [lsKey, map, wr]);
  const get = useCallback((id: string, defKind: ChartKind = 'line'): ChartFormat => map[id] ?? { kind: defKind, color: null }, [map]);
  const setKind = useCallback((id: string, kind: ChartKind) => setMap((m) => ({ ...m, [id]: { ...(m[id] ?? DEF), kind } })), []);
  const setColor = useCallback((id: string, color: string | null) => setMap((m) => ({ ...m, [id]: { ...(m[id] ?? DEF), color } })), []);
  return { get, setKind, setColor };
}

/** Применить формат к серии: тип + цвет (+ градиент области / база столбцов). */
export function applyFormat(def: LwSeries, fmt: ChartFormat): LwSeries {
  const color = fmt.color ?? def.color;
  const out: LwSeries = { ...def, type: fmt.kind, color };
  if (fmt.kind === 'area') {
    // Заливка приглушена (13%, было 22%) — тоньше и меньше «давит» на график; низ прозрачный.
    out.areaTop = `color-mix(in srgb, ${color} 13%, transparent)`;
    out.areaBottom = undefined;
  } else {
    delete out.areaTop;
    delete out.areaBottom;
  }
  if (fmt.kind === 'histogram') out.base = def.base ?? 0;
  return out;
}

/** ⚙-секция «Формат»: сегменты типа + свотчи цвета (первый — «Авто», сброс).
 *  `label` — заголовок секции (для пер-серийного формата — имя линии). */
export function FormatSection({ fmt, onKind, onColor, label = 'Формат' }: {
  fmt: ChartFormat;
  onKind: (k: ChartKind) => void;
  onColor: (c: string | null) => void;
  label?: string;
}) {
  return (
    <DrawerSection label={label}>
      <SegGroup<ChartKind> value={fmt.kind} options={KINDS} onChange={onKind} />
      <div style={{ display: 'flex', alignItems: 'center', gap: 7, marginTop: 10 }}>
        <button
          type="button"
          title="Авто (цвет индикатора)"
          onClick={() => onColor(null)}
          style={swatchStyle(fmt.color === null, 'transparent')}
        >
          <span style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-secondary)' }}>А</span>
        </button>
        {SWATCHES.map((c) => (
          <button key={c} type="button" title={c} onClick={() => onColor(c)} style={swatchStyle(fmt.color === c, c)} />
        ))}
      </div>
    </DrawerSection>
  );
}

function swatchStyle(active: boolean, bg: string): CSSProperties {
  return {
    width: 22, height: 22, borderRadius: 6, background: bg, cursor: 'pointer', padding: 0,
    display: 'inline-flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
    border: active ? '2px solid var(--accent)' : '1.5px solid var(--border-color, rgba(128,128,128,0.35))',
    boxSizing: 'border-box',
  };
}
