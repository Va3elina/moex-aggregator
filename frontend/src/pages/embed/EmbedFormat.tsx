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
import type { LwSeries } from '../../components/chart/lwTypes';
import { DrawerSection, SegGroup } from './EmbedSettings';
import { useEmbedPersist } from './embedPersist';

export type ChartKind = 'line' | 'area' | 'histogram' | 'candlestick' | 'bar';
// color null = цвет индикатора; visible — «глаз» строки в списке индикаторов
// (undefined трактуем как true, чтобы старый персист читался без миграции).
export interface ChartFormat { kind: ChartKind; color: string | null; visible?: boolean }

// Реестр «id → подпись». 'histogram' («Столбцы») больше НЕ в выбираемых наборах
// (убран по фидбеку Вадима), но остаётся валидным типом для нативных серий (funds-flow)
// и совместимости со старым персистом.
const KIND_LABELS: Record<ChartKind, string> = {
  line: 'Линия', area: 'Область', candlestick: 'Свечи', bar: 'Бары', histogram: 'Столбцы',
};
// Дефолт выбора — линия/область. Свечи/бары требуют OHLC → включаются только явным
// пропом kinds у серии, где OHLC есть (цена фьючерса) — на прочих индикаторах их нет.
export const DEFAULT_KINDS: ChartKind[] = ['line', 'area'];
// Серия цены фьючерса (несёт OHLC): + свечи/бары (порт режимов графика с сайта).
export const OHLC_KINDS: ChartKind[] = ['line', 'area', 'candlestick', 'bar'];

/** Опции {id,label} для тулбар-контрола «Вид графика» (единый источник подписей). */
export function kindOptions(kinds: ChartKind[] = OHLC_KINDS): { id: ChartKind; label: string }[] {
  return kinds.map((id) => ({ id, label: KIND_LABELS[id] }));
}

// CC-палитра дизайнера: price / up / down / sec / amber / cyan.
const SWATCHES = ['#5DA3E9', '#5BD49C', '#EF6F6F', '#9B8BF0', '#E0A34E', '#57C7C7'];

const DEF: ChartFormat = { kind: 'line', color: null };

// ⚠️ parse ПЕРЕСОБИРАЕТ объект руками, а не спредит распарсенное — это намеренно
// (санитизация чужого JSON из localStorage), но означает, что КАЖДОЕ новое поле
// ChartFormat нужно добавлять и сюда. Иначе оно молча теряется при первом же
// чтении: в UI работает до перезагрузки страницы, а после F5 сбрасывается.
function parse(raw: string): ChartFormat {
  try {
    const j = JSON.parse(raw) as Partial<ChartFormat>;
    const kind = (j.kind && j.kind in KIND_LABELS ? j.kind : 'line') as ChartKind;
    const color = typeof j.color === 'string' && SWATCHES.includes(j.color) ? j.color : null;
    const visible = typeof j.visible === 'boolean' ? j.visible : undefined;
    return { kind, color, ...(visible === undefined ? {} : { visible }) };
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
  // «Глаз» строки в списке индикаторов: нативную серию нельзя удалить, но можно
  // скрыть. undefined трактуется как видимая — старый персист читается как есть.
  const setVisible = useCallback((id: string, visible: boolean) => setMap((m) => ({ ...m, [id]: { ...(m[id] ?? DEF), visible } })), []);
  return { get, setKind, setColor, setVisible };
}

/** Применить формат к серии: тип + цвет (+ градиент области / база столбцов). */
export function applyFormat(def: LwSeries, fmt: ChartFormat): LwSeries {
  const color = fmt.color ?? def.color;
  const out: LwSeries = { ...def, type: fmt.kind, color };
  if (fmt.kind === 'area') {
    // Градиент как на сайте (SimpleChart): верх ~18%, низ — МЯГКИЙ ПОЛ 2% (не полный
    // ноль). Полностью прозрачный низ фейдил заливку в ничто у серий, чья линия сидит
    // высоко в своём диапазоне (ОИ) → «область не включалась». 2%-пол делает её видимой
    // по всей высоте, оставаясь тонкой (Вадим: «не сильно яркая»).
    out.areaTop = `color-mix(in srgb, ${color} 18%, transparent)`;
    out.areaBottom = `color-mix(in srgb, ${color} 2%, transparent)`;
  } else {
    delete out.areaTop;
    delete out.areaBottom;
  }
  if (fmt.kind === 'histogram') out.base = def.base ?? 0;
  return out;
}

/** ⚙-секция «Формат»: сегменты типа + свотчи цвета (первый — «Авто», сброс).
 *  `label` — заголовок секции (для пер-серийного формата — имя линии).
 *  `kinds` — какие режимы доступны (дефолт линия/область; цена фьючерса → OHLC_KINDS). */
export function FormatSection({ fmt, onKind, onColor, label = 'Формат', kinds = DEFAULT_KINDS }: {
  fmt: ChartFormat;
  onKind: (k: ChartKind) => void;
  onColor: (c: string | null) => void;
  label?: string;
  kinds?: ChartKind[];
}) {
  const opts = kinds.map((id) => ({ id, label: KIND_LABELS[id] }));
  return (
    <DrawerSection label={label}>
      <SegGroup<ChartKind> value={fmt.kind} options={opts} onChange={onKind} />
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
