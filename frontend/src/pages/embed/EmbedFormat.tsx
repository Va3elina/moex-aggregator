/**
 * EmbedFormat — общий ⚙-блок «Формат» графика (§6 спеки песочницы): тип
 * главной серии (линия / область / столбцы) + свотчи цвета. Применяется
 * СРАЗУ, без закрытия поповера. Настройка персистится per-индикатор.
 *
 * Цвета свотчей — дизайнерская CC-палитра мокапа (литеральные hex, одинаковы
 * в обеих темах): это осознанный выбор пользователя, поэтому он одинаков на
 * сайте, в расширении и в песочнице.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import type { LwSeries } from '../../components/chart/lwTypes';
import { SegGroup } from './EmbedSettings';
import { useEmbedPersist } from './embedPersist';
import ColorLinePicker from './ColorLinePicker';

export type ChartKind = 'line' | 'area' | 'histogram' | 'candlestick' | 'bar';
// color null = цвет индикатора; visible — «глаз» строки в списке индикаторов
// (undefined трактуем как true, чтобы старый персист читался без миграции).
// opacity 0..100 (дефолт 100), width 1..4 (undefined = глобальная толщина
// песочницы) — детальная настройка линии из ColorLinePicker.
export interface ChartFormat { kind: ChartKind; color: string | null; visible?: boolean; opacity?: number; width?: number }

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

const DEF: ChartFormat = { kind: 'line', color: null };

// ⚠️ parse ПЕРЕСОБИРАЕТ объект руками, а не спредит распарсенное — это намеренно
// (санитизация чужого JSON из localStorage), но означает, что КАЖДОЕ новое поле
// ChartFormat нужно добавлять и сюда. Иначе оно молча теряется при первом же
// чтении: в UI работает до перезагрузки страницы, а после F5 сбрасывается.
function parse(raw: string): ChartFormat {
  try {
    const j = JSON.parse(raw) as Partial<ChartFormat>;
    const kind = (j.kind && j.kind in KIND_LABELS ? j.kind : 'line') as ChartKind;
    // Раньше color валидировался по списку заготовок; теперь любой #RRGGBB
    // (детальный пикер) — старые значения тоже hex, читаются как есть.
    const color = typeof j.color === 'string' && /^#[0-9A-Fa-f]{6}$/.test(j.color) ? j.color : null;
    const visible = typeof j.visible === 'boolean' ? j.visible : undefined;
    const opacity = typeof j.opacity === 'number' ? Math.max(5, Math.min(100, Math.round(j.opacity))) : undefined;
    const width = typeof j.width === 'number' && [1, 2, 3, 4].includes(j.width) ? j.width : undefined;
    return {
      kind, color,
      ...(visible === undefined ? {} : { visible }),
      ...(opacity === undefined ? {} : { opacity }),
      ...(width === undefined ? {} : { width }),
    };
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
    setOpacity: (opacity: number) => setFmt((f) => ({ ...f, opacity })),
    setWidth: (width: number) => setFmt((f) => ({ ...f, width })),
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
  const setOpacity = useCallback((id: string, opacity: number) => setMap((m) => ({ ...m, [id]: { ...(m[id] ?? DEF), opacity } })), []);
  const setWidth = useCallback((id: string, width: number) => setMap((m) => ({ ...m, [id]: { ...(m[id] ?? DEF), width } })), []);
  // ⚠️ useMemo обязателен. Новый объект на каждый рендер попадал в депсы мемо
  // в embed'ах (visibleNative/allSeries → chartPanes), и ЛЮБОЙ ререндер —
  // например, выделение фигуры кликом — пересоздавал ВСЕ серии графика с
  // восстановлением диапазона. Вадим видел это как «нажал на линию и меня
  // телепортировало» плюс микрофриз (пересоздание серий ≈ 100 мс по замеру).
  return useMemo(() => ({ get, setKind, setColor, setVisible, setOpacity, setWidth }), [get, setKind, setColor, setVisible, setOpacity, setWidth]);
}

/** Применить формат к серии: тип + цвет + прозрачность + толщина
 *  (+ градиент области / база столбцов). */
export function applyFormat(def: LwSeries, fmt: ChartFormat): LwSeries {
  const base = fmt.color ?? def.color;
  const op = fmt.opacity ?? 100;
  // color-mix, а не hex-альфа: базой может быть var(--токен) — LwChartPanes
  // резолвит любые CSS-цвета пробой (см. resolveColor).
  const color = op < 100 ? `color-mix(in srgb, ${base} ${op}%, transparent)` : base;
  const out: LwSeries = { ...def, type: fmt.kind, color };
  // Явная толщина юзера побеждает глобальный тумблер песочницы (chartPrefs).
  if (fmt.width) out.userLineWidth = fmt.width;
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

/** ⚙-секция «Формат»: сегменты типа + кнопка «цвет/линия» СПРАВА от подписи —
 *  раскрывается в детальный пикер (палитра, свои цвета, прозрачность, толщина).
 *  Ряд заготовленных свотчей убран (фидбек Вадима, мокап TradingView).
 *  `label` — заголовок секции (для пер-серийного формата — имя линии).
 *  `kinds` — какие режимы доступны (дефолт линия/область; цена фьючерса → OHLC_KINDS).
 *  `defaultColor` — родной цвет серии для превью при «Авто». */
export function FormatSection({ fmt, onKind, onColor, onOpacity, onWidth, label = 'Формат', kinds = DEFAULT_KINDS, defaultColor }: {
  fmt: ChartFormat;
  onKind: (k: ChartKind) => void;
  onColor: (c: string | null) => void;
  onOpacity?: (o: number) => void;
  onWidth?: (w: number) => void;
  label?: string;
  kinds?: ChartKind[];
  defaultColor?: string;
}) {
  const opts = kinds.map((id) => ({ id, label: KIND_LABELS[id] }));
  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <span
          style={{
            fontSize: 10.5, fontWeight: 800, textTransform: 'uppercase',
            letterSpacing: '0.06em', color: 'var(--text-secondary)',
          }}
        >
          {label}
        </span>
        <ColorLinePicker
          value={{ color: fmt.color, opacity: fmt.opacity ?? 100, width: fmt.width ?? null }}
          onColor={onColor}
          onOpacity={onOpacity ?? (() => {})}
          onWidth={onWidth ?? (() => {})}
          defaultColor={defaultColor}
        />
      </div>
      <SegGroup<ChartKind> value={fmt.kind} options={opts} onChange={onKind} />
    </div>
  );
}
