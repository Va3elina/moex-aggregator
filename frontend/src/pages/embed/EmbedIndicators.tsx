/**
 * EmbedIndicators — движок пользовательских индикаторов на графике (модель ТЗ:
 * как в TradingView, где индикатор добавляется списком, настраивается по строке
 * и живёт либо поверх графика, либо отдельной панелью).
 *
 * pane в модели: 0 — наложение поверх ценового графика (MA, EMA, Боллинджер),
 * 1+ — отдельная панель со своей шкалой (RSI, ATR, объёмы). У последних ценовая
 * ось не годится в принципе: RSI это 0..100 против десятков тысяч у цены, на
 * одной шкале он превратился бы в плоскую линию у края — поэтому «вернуть на
 * график» им недоступно (overlayOk).
 *
 * Профиль объёма стоит особняком: он вообще не временной ряд и не может быть
 * серией, поэтому наружу уходит не через indicatorSeriesByPane, а отдельным
 * описанием для примитива (volumeProfileSpec). Панель у него всегда 0 —
 * гистограмма по цене имеет смысл только рядом с ценой (ownPaneOk).
 *
 * Компонент НЕ трогает сам чарт: индикаторы отдаются наружу массивом серий,
 * сгруппированным по панелям, а embed уже собирает из этого panes для
 * LwChartPanes.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { Eye, EyeOff, Settings2, X as XIcon, Plus, PanelBottom, ChartNoAxesColumn } from 'lucide-react';
import type { LwSeries } from '../../components/chart/lwTypes';
import type { VolumeProfileSpec } from '../../components/LwChartPanes';
import { VP_DEFAULTS } from '../../components/chart/volumeProfilePrimitive';
import { sma, ema, bollinger, rsi, atr, volumeBars, VOLUME_UP, VOLUME_DOWN, type IndCandle } from '../../utils/indicators';
import { useEmbedPersist } from './embedPersist';

/** Имя нарочно НЕ IndKind: так уже называется вид панели в SandboxPage. */
export type IndicatorKind = 'ma' | 'ema' | 'bb' | 'rsi' | 'atr' | 'volume' | 'vp';

export interface IndicatorInst {
  id: string;
  kind: IndicatorKind;
  length: number;
  /** Множитель отклонения — только у полос Боллинджера. */
  mult?: number;
  /** null = цвет по палитре, из порядкового номера. */
  color: string | null;
  width: 1 | 2 | 3 | 4;
  visible: boolean;
  /** 0 = поверх графика, 1+ — отдельная панель. */
  pane: number;
  /** К какому краю прижат профиль объёма. Только для kind='vp'. */
  side?: 'left' | 'right';
  /** Границы зон осциллятора (RSI: перекупленность/перепроданность). */
  upper?: number;
  lower?: number;
}

interface KindDef {
  label: string;
  /** Подпись строки: «EMA 20», «Боллинджер 20×2». */
  title: (i: IndicatorInst) => string;
  defLength: number;
  defMult?: number;
  /** Куда кладём по умолчанию: 0 — поверх графика, 1 — отдельной панелью.
   *  У RSI/ATR/объёма своя шкала (0..100 против десятков тысяч у цены), им
   *  на ценовой оси делать нечего. */
  defaultPane: 0 | 1;
  /** Можно ли класть поверх цены вообще (пункт «Вернуть на график»). */
  overlayOk: boolean;
  /** Можно ли вынести в свою панель. false — у профиля объёма: он рисуется
   *  по ценовой шкале и в пустой панели показывать было бы нечего. */
  ownPaneOk?: boolean;
  /** Нужен объём в свече. Такие индикаторы прячем там, где его нет — иначе
   *  пользователь добавляет строку, а на графике не появляется ничего. */
  needsVolume?: boolean;
  /** Подпись поля «Период», если оно значит не период. */
  lengthLabel?: string;
  /** Дефолтные границы зон. Есть — значит у индикатора есть перекупленность и
   *  перепроданность, и он рисует их разметкой. */
  bands?: { upper: number; lower: number; middle: number | null };
}

export const KINDS: Record<IndicatorKind, KindDef> = {
  ma: { label: 'Скользящая средняя (MA)', title: (i) => `MA ${i.length}`, defLength: 20, defaultPane: 0, overlayOk: true },
  ema: { label: 'Экспоненциальная средняя (EMA)', title: (i) => `EMA ${i.length}`, defLength: 20, defaultPane: 0, overlayOk: true },
  bb: { label: 'Полосы Боллинджера', title: (i) => `Боллинджер ${i.length}×${i.mult ?? 2}`, defLength: 20, defMult: 2, defaultPane: 0, overlayOk: true },
  // 30/70 — канон Уайлдера, тот же дефолт в TradingView и любом терминале.
  // Середина 50 отделяет бычью половину диапазона от медвежьей.
  rsi: {
    label: 'RSI', title: (i) => `RSI ${i.length}`, defLength: 14, defaultPane: 1, overlayOk: false,
    bands: { upper: 70, lower: 30, middle: 50 },
  },
  atr: { label: 'ATR', title: (i) => `ATR ${i.length}`, defLength: 14, defaultPane: 1, overlayOk: false },
  volume: { label: 'Объёмы', title: () => 'Объёмы', defLength: 14, defaultPane: 1, overlayOk: false, needsVolume: true },
  vp: {
    label: 'Профиль объёма', title: () => 'Профиль объёма', defLength: VP_DEFAULTS.rows,
    defaultPane: 0, overlayOk: true, ownPaneOk: false, needsVolume: true, lengthLabel: 'Уровней',
  },
};

/** Палитра наложений — та же CC-гамма, что у ⚙-Формата серий. */
const PALETTE = ['#9B8BF0', '#E0A34E', '#57C7C7', '#5BD49C', '#EF6F6F', '#5DA3E9'];

const uid = () => 'ind_' + Date.now().toString(36) + '_' + Math.floor(Math.random() * 1e6).toString(36);

/**
 * Санитизация чужого JSON из localStorage — по образцу parse() в EmbedFormat.
 *
 * ⚠️ Объект собирается ПОЛЯ ЗА ПОЛЕМ, а не спредом распарсенного. Это намеренно
 * (в localStorage может лежать что угодно), но означает, что каждое новое поле
 * IndicatorInst нужно добавлять и сюда — иначе оно работает ровно до F5.
 */
function parseList(raw: string): IndicatorInst[] {
  try {
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) return [];
    return arr.flatMap((x): IndicatorInst[] => {
      if (!x || typeof x !== 'object') return [];
      const kind = x.kind as IndicatorKind;
      if (!(kind in KINDS)) return [];
      const length = Number.isFinite(x.length) ? Math.max(1, Math.min(500, Math.round(x.length))) : KINDS[kind].defLength;
      const width = [1, 2, 3, 4].includes(x.width) ? x.width : 2;
      return [{
        id: typeof x.id === 'string' ? x.id : uid(),
        kind,
        length,
        mult: Number.isFinite(x.mult) ? x.mult : KINDS[kind].defMult,
        color: typeof x.color === 'string' ? x.color : null,
        width,
        visible: x.visible !== false,
        pane: Number.isFinite(x.pane) ? x.pane : 0,
        side: x.side === 'left' || x.side === 'right' ? x.side : undefined,
        upper: Number.isFinite(x.upper) ? x.upper : undefined,
        lower: Number.isFinite(x.lower) ? x.lower : undefined,
      }];
    });
  } catch { return []; }
}

export interface IndicatorsApi {
  list: IndicatorInst[];
  add: (kind: IndicatorKind) => void;
  remove: (id: string) => void;
  patch: (id: string, p: Partial<IndicatorInst>) => void;
  setPane: (id: string, toOwnPane: boolean) => void;
  colorOf: (i: IndicatorInst) => string;
}

/** Стейт + персист + CRUD. Форма как у useSeriesFormats/useDrawTools. */
export function useIndicators(lsKey: string): IndicatorsApi {
  const { rd, wr } = useEmbedPersist();
  const [list, setList] = useState<IndicatorInst[]>(() => parseList(rd(lsKey, '')));
  // Пропускаем первую запись (маунт), иначе затрём сохранённое пустым списком —
  // та же ловушка, что в useDrawTools.
  const ready = useRef(false);
  const keyRef = useRef(lsKey); keyRef.current = lsKey;
  useEffect(() => {
    if (!ready.current) { ready.current = true; return; }
    wr(keyRef.current, JSON.stringify(list));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [list]);

  const add = useCallback((kind: IndicatorKind) => {
    const d = KINDS[kind];
    setList((l) => {
      // Индикаторы со своей шкалой садятся каждый в СВОЮ панель: RSI и ATR на
      // одной оси были бы нечитаемы (0..100 против абсолютных значений цены).
      const pane = d.defaultPane === 0 ? 0 : nextFreePane(l);
      return [...l, { id: uid(), kind, length: d.defLength, mult: d.defMult, color: null, width: 2, visible: true, pane }];
    });
  }, []);
  /** Перенос строки: на график ⇄ в свою панель. */
  const setPane = useCallback((id: string, toOwnPane: boolean) => {
    setList((l) => l.map((x) => (x.id === id ? { ...x, pane: toOwnPane ? nextFreePane(l.filter((y) => y.id !== id)) : 0 } : x)));
  }, []);
  const remove = useCallback((id: string) => setList((l) => l.filter((x) => x.id !== id)), []);
  const patch = useCallback((id: string, p: Partial<IndicatorInst>) => {
    setList((l) => l.map((x) => (x.id === id ? { ...x, ...p } : x)));
  }, []);
  const colorOf = useCallback((i: IndicatorInst) => i.color ?? PALETTE[Math.abs(hash(i.id)) % PALETTE.length], []);

  return { list, add, remove, patch, setPane, colorOf };
}

/** Минимальный свободный номер панели ≥1 (после удаления индикатора номера не
 *  переиспользуются автоматически — иначе оставшиеся строки прыгали бы). */
function nextFreePane(list: IndicatorInst[]): number {
  const used = new Set(list.filter((x) => x.pane > 0).map((x) => x.pane));
  let p = 1;
  while (used.has(p)) p++;
  return p;
}

function hash(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return h;
}

/**
 * Индикаторы → серии, СГРУППИРОВАННЫЕ ПО ПАНЕЛЯМ. Индекс массива = номер панели,
 * 0 — основной график. Пустые панели схлопываются вызывающим.
 *
 * Считаем на клиенте (замер: весь набор на 5000 барах — 2.7 мс против 100 мс на
 * пересоздание серий графика, узкое место не тут).
 *
 * `toSec` передаётся снаружи: у embed'ов время в исходных точках строковое, а
 * правило перевода зависит от таймфрейма.
 */
export function indicatorSeriesByPane(
  list: IndicatorInst[],
  candles: IndCandle<string>[],
  toSec: (t: string) => number,
  colorOf: (i: IndicatorInst) => string,
): LwSeries[][] {
  const out: LwSeries[][] = [[]];
  if (!candles.length) return out;
  const put = (pane: number, sers: LwSeries[]) => {
    while (out.length <= pane) out.push([]);
    out[pane].push(...sers);
  };
  const conv = (pts: { time: string; value: number; color?: string }[]) =>
    pts.map((p) => ({ time: toSec(p.time), value: p.value, ...(p.color ? { color: p.color } : {}) }));

  for (const i of list) {
    if (!i.visible) continue;
    const color = colorOf(i);
    const onMain = i.pane === 0;
    // На основном графике индикатор садится на ЦЕНОВУЮ (левую) ось; в своей
    // панели — на правую, там она единственная.
    const base = {
      scale: (onMain ? 'left' : 'right') as 'left' | 'right',
      color, lineWidth: i.width, type: 'line' as const, lastValueVisible: !onMain,
    };
    if (i.kind === 'ma' || i.kind === 'ema') {
      const pts = (i.kind === 'ma' ? sma : ema)(candles, i.length);
      if (pts.length) put(i.pane, [{ ...base, id: i.id, label: KINDS[i.kind].title(i), data: conv(pts) }]);
    } else if (i.kind === 'bb') {
      const { mid, upper, lower } = bollinger(candles, i.length, i.mult ?? 2);
      if (mid.length) {
        // Полосы тоньше середины и пунктиром — иначе три линии сливаются.
        put(i.pane, [
          { ...base, id: i.id + ':u', label: `${KINDS.bb.title(i)} ↑`, data: conv(upper), dashed: true, lineWidth: 1 },
          { ...base, id: i.id, label: KINDS.bb.title(i), data: conv(mid) },
          { ...base, id: i.id + ':l', label: `${KINDS.bb.title(i)} ↓`, data: conv(lower), dashed: true, lineWidth: 1 },
        ]);
      }
    } else if (i.kind === 'rsi') {
      const pts = rsi(candles, i.length);
      const b = KINDS.rsi.bands!;
      // minMove 0.01 — иначе ось RSI округлит всё до целых и станет ступенчатой.
      if (pts.length) {
        put(i.pane, [{
          ...base, id: i.id, label: KINDS.rsi.title(i), data: conv(pts), minMove: 0.01,
          bands: { upper: i.upper ?? b.upper, lower: i.lower ?? b.lower, middle: b.middle },
        }]);
      }
    } else if (i.kind === 'atr') {
      const pts = atr(candles, i.length);
      if (pts.length) put(i.pane, [{ ...base, id: i.id, label: KINDS.atr.title(i), data: conv(pts), minMove: 0.01 }]);
    } else if (i.kind === 'volume') {
      const pts = volumeBars(candles);
      if (pts.length) {
        put(i.pane, [{
          ...base, type: 'histogram', id: i.id, label: 'Объёмы', data: conv(pts), base: 0,
          axisFmt: (v: number) => (v >= 1e6 ? (v / 1e6).toFixed(1) + 'М' : v >= 1e3 ? Math.round(v / 1e3) + 'т' : String(Math.round(v))),
        }]);
      }
    }
  }
  return out;
}

/**
 * Описание профиля объёма для примитива графика — или null, если профиля в
 * списке нет.
 *
 * Берём ПЕРВЫЙ видимый: два профиля на одной панели легли бы друг на друга и
 * читались как один кривой. Настройки при этом у каждой строки свои, так что
 * пользователь может держать несколько заготовок и переключать их «глазом».
 *
 * Цвета: полосы — те же токены, что у индикатора «Объёмы» (покупки/продажи), а
 * выбранный в палитре цвет уходит на POC. Иначе выбор цвета не значил бы ничего:
 * сам профиль двухцветный по смыслу.
 */
export function volumeProfileSpec(
  list: IndicatorInst[],
  candles: IndCandle<string>[],
  seriesId: string,
  colorOf: (i: IndicatorInst) => string,
): VolumeProfileSpec | null {
  const inst = list.find((i) => i.kind === 'vp' && i.visible);
  if (!inst || !candles.length) return null;
  return {
    seriesId,
    candles,
    rows: Math.max(4, Math.min(400, Math.round(inst.length))),
    side: inst.side ?? VP_DEFAULTS.side,
    widthPct: VP_DEFAULTS.widthPct,
    valueAreaPct: VP_DEFAULTS.valueAreaPct,
    upColor: VOLUME_UP,
    downColor: VOLUME_DOWN,
    pocColor: colorOf(inst),
  };
}

/** Мемо-обёртка: новая ссылка на спеку сама по себе дёшева (серии не трогает),
 *  но лишний вызов applyOptions сбрасывает кэш профиля и заставляет пересчитать
 *  его на ближайшем кадре. */
export function useVolumeProfileSpec(
  list: IndicatorInst[],
  candles: IndCandle<string>[],
  seriesId: string,
  colorOf: (i: IndicatorInst) => string,
): VolumeProfileSpec | null {
  return useMemo(() => volumeProfileSpec(list, candles, seriesId, colorOf), [list, candles, seriesId, colorOf]);
}

// ─────────────────────────────── UI ───────────────────────────────

const SURFACE: CSSProperties = {
  background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 92%, transparent)',
  border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
  backdropFilter: 'blur(3px)',
  boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
};
const ICON_BTN: CSSProperties = {
  width: 20, height: 20, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
  border: 'none', borderRadius: 5, background: 'transparent', color: 'var(--text-secondary)',
  cursor: 'pointer', padding: 0, flexShrink: 0,
};

/** Строка списка для НАТИВНОЙ серии embed'а (цена, ОИ): её нельзя удалить. */
export interface NativeRow {
  id: string;
  label: string;
  color: string;
  visible: boolean;
  onToggle: () => void;
}

/**
 * Список индикаторов — React-оверлей ВНУТРИ области графика.
 *
 * z-index 10: выше слоя рисования (7), хит-слоя (8) и панели слоёв (9), ниже
 * тулбара (20). data-export-ignore обязателен — иначе список попадёт в PNG.
 */
export function IndicatorList({ api, native, visible, hasVolume = false }: {
  api: IndicatorsApi;
  native: NativeRow[];
  visible: boolean;
  /** Есть ли объём в свечах. Без него «Объёмы» и «Профиль объёма» не показываем:
   *  добавились бы строки, за которыми на графике пусто. */
  hasVolume?: boolean;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e: PointerEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setMenuOpen(false);
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [menuOpen]);

  if (!visible) return null;

  return (
    <div ref={rootRef} data-export-ignore="true" style={listBoxStyle}>
      {native.map((r) => (
        <Row key={r.id} color={r.color} label={r.label} visible={r.visible} onToggle={r.onToggle} />
      ))}
      {/* Только наложения. Индикаторы своих панелей рисуют строку САМИ, над
          своим графиком — см. PaneIndicatorList. */}
      {api.list.filter((i) => i.pane === 0).map((i) => (
        <IndicatorRow key={i.id} inst={i} api={api} />
      ))}

      <div style={{ position: 'relative' }}>
        <button
          type="button"
          onClick={() => setMenuOpen((v) => !v)}
          style={{
            display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 7px', marginTop: 2,
            borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: 10.5, fontWeight: 600,
            background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 80%, transparent)',
            color: 'var(--text-secondary)',
          }}
        >
          <Plus size={11} />Индикатор
        </button>
        {menuOpen && (
          <div style={{ position: 'absolute', top: 'calc(100% + 4px)', left: 0, zIndex: 2, minWidth: 232, padding: 5, borderRadius: 9, ...SURFACE }}>
            {(Object.keys(KINDS) as IndicatorKind[]).filter((k) => hasVolume || !KINDS[k].needsVolume).map((k) => {
              const d = KINDS[k];
              return (
                <button
                  key={k}
                  type="button"
                  onClick={() => { api.add(k); setMenuOpen(false); }}
                  style={{
                    display: 'block', width: '100%', textAlign: 'left', padding: '5px 8px', borderRadius: 6,
                    border: 'none', background: 'transparent', fontSize: 11.5,
                    color: 'var(--text-primary)', cursor: 'pointer',
                  }}
                >
                  {d.label}
                  {d.defaultPane > 0 && <span style={{ fontSize: 10, opacity: 0.7 }}> · отдельной панелью</span>}
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * Строки индикаторов КОНКРЕТНОЙ панели. Рендерится внутрь этой панели (проп
 * paneOverlay у LwChartPanes), поэтому садится в её левый верхний угол.
 *
 * Why: индикатор в своей панели и его строка в общем списке наверху — это два
 * разных места для одной сущности. Пользователь ищет подпись там, где линия.
 */
export function PaneIndicatorList({ api, pane }: { api: IndicatorsApi; pane: number }) {
  const rows = api.list.filter((i) => i.pane === pane);
  if (!rows.length) return null;
  return (
    <div data-export-ignore="true" style={listBoxStyle}>
      {rows.map((i) => <IndicatorRow key={i.id} inst={i} api={api} />)}
    </div>
  );
}

/** Общая посадка списков: отступ слева = ширина ценовой оси (её публикует
 *  LwChartPanes), иначе список ложится на цифры шкалы. z 10 — выше слоя
 *  рисования (7) и хит-слоя (8), ниже тулбара (20). */
const listBoxStyle: CSSProperties = {
  position: 'absolute', top: 8, left: 'calc(var(--lw-axis-left, 0px) + 8px)', zIndex: 10,
  display: 'flex', flexDirection: 'column', gap: 1, alignItems: 'flex-start',
};

/** Строка индикатора вместе со своей шестерёнкой: попап живёт рядом со строкой,
 *  а не в корне списка — иначе для панельных строк его пришлось бы отдельно
 *  позиционировать через всю иерархию. */
function IndicatorRow({ inst, api }: { inst: IndicatorInst; api: IndicatorsApi }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: PointerEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [open]);
  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <Row
        color={api.colorOf(inst)}
        label={KINDS[inst.kind].title(inst)}
        visible={inst.visible}
        onToggle={() => api.patch(inst.id, { visible: !inst.visible })}
        onSettings={() => setOpen((v) => !v)}
        onRemove={() => api.remove(inst.id)}
        pane={inst.pane}
        canOverlay={KINDS[inst.kind].overlayOk}
        canOwnPane={KINDS[inst.kind].ownPaneOk}
        onTogglePane={() => api.setPane(inst.id, inst.pane === 0)}
      />
      {open && <SettingsPopover inst={inst} api={api} onClose={() => setOpen(false)} />}
    </div>
  );
}

function Row({ color, label, visible, onToggle, onSettings, onRemove, pane, canOverlay, canOwnPane, onTogglePane }: {
  color: string; label: string; visible: boolean;
  onToggle: () => void; onSettings?: () => void; onRemove?: () => void;
  pane?: number; canOverlay?: boolean; canOwnPane?: boolean; onTogglePane?: () => void;
}) {
  return (
    <div
      style={{
        display: 'flex', alignItems: 'center', gap: 5, padding: '1px 6px 1px 4px', borderRadius: 6,
        background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 72%, transparent)',
        opacity: visible ? 1 : 0.5,
      }}
    >
      {/* Цвет — сырым значением: это CSS-переменная либо литерал, и внутри
          поддерева панели переменная разрешится сама (тема живёт на data-theme). */}
      <span style={{ width: 8, height: 8, borderRadius: 2, background: color, flexShrink: 0 }} />
      <span style={{ fontSize: 10.5, fontWeight: 600, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>{label}</span>
      <button type="button" title={visible ? 'Скрыть' : 'Показать'} onClick={onToggle} style={ICON_BTN}>
        {visible ? <Eye size={11} /> : <EyeOff size={11} />}
      </button>
      {/* У RSI/ATR/объёма своя шкала — «вернуть на график» им недоступно вовсе:
          на ценовой оси они превратились бы в плоскую линию у края. Профиль
          объёма, наоборот, никуда не выносится: он рисуется по ценовой шкале. */}
      {onTogglePane && canOverlay !== false && canOwnPane !== false && (
        <button
          type="button"
          title={pane === 0 ? 'Вынести в отдельную панель' : 'Вернуть на график'}
          onClick={onTogglePane}
          style={ICON_BTN}
        >
          {pane === 0 ? <PanelBottom size={11} /> : <ChartNoAxesColumn size={11} />}
        </button>
      )}
      {onSettings && <button type="button" title="Настройки" onClick={onSettings} style={ICON_BTN}><Settings2 size={11} /></button>}
      {onRemove && <button type="button" title="Удалить" onClick={onRemove} style={ICON_BTN}><XIcon size={11} /></button>}
    </div>
  );
}

const numInput = (w: number): CSSProperties => ({
  width: w, padding: '3px 6px', borderRadius: 6, fontSize: 11.5,
  border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
  background: 'var(--bg-base, transparent)', color: 'var(--text-primary)',
});

/** Границы зон не должны пересекаться, иначе заливка выворачивается наизнанку:
 *  верхняя держится выше середины, нижняя ниже. */
function clampBand(raw: string, min: number, max: number, def: number): number {
  const v = Number(raw);
  if (!Number.isFinite(v)) return def;
  return Math.max(min, Math.min(max, Math.round(v)));
}

function SettingsPopover({ inst, api, onClose }: { inst: IndicatorInst; api: IndicatorsApi; onClose: () => void }) {
  const d = KINDS[inst.kind];
  const label: CSSProperties = { fontSize: 11, color: 'var(--text-secondary)', minWidth: 72 };
  const row: CSSProperties = { display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 };
  return (
    <div style={{ position: 'absolute', top: 0, left: 'calc(100% + 8px)', zIndex: 3, width: 216, padding: 9, borderRadius: 9, ...SURFACE }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <span style={{ fontSize: 11.5, fontWeight: 700, color: 'var(--text-primary)' }}>{d.title(inst)}</span>
        <button type="button" onClick={onClose} style={ICON_BTN}><XIcon size={12} /></button>
      </div>
      <div style={row}>
        <span style={label}>{d.lengthLabel ?? 'Период'}</span>
        <input
          type="number" min={2} max={500} value={inst.length}
          onChange={(e) => api.patch(inst.id, { length: Math.max(2, Math.min(500, Number(e.target.value) || 2)) })}
          style={{ width: 64, padding: '3px 6px', borderRadius: 6, fontSize: 11.5, border: '1px solid var(--border-color, rgba(128,128,128,0.35))', background: 'var(--bg-base, transparent)', color: 'var(--text-primary)' }}
        />
      </div>
      {d.bands && (
        // Границы, а не «включить зоны»: смысл осциллятора в том, где проходит
        // граница, и на разных инструментах её сдвигают (на трендовых 80/20).
        <div style={row}>
          <span style={label}>Зоны</span>
          <input
            type="number" min={51} max={99} value={inst.upper ?? d.bands.upper}
            title="Верхняя граница — выше неё перекупленность"
            onChange={(e) => api.patch(inst.id, { upper: clampBand(e.target.value, 51, 99, d.bands!.upper) })}
            style={numInput(64)}
          />
          <input
            type="number" min={1} max={49} value={inst.lower ?? d.bands.lower}
            title="Нижняя граница — ниже неё перепроданность"
            onChange={(e) => api.patch(inst.id, { lower: clampBand(e.target.value, 1, 49, d.bands!.lower) })}
            style={numInput(64)}
          />
        </div>
      )}
      {inst.kind === 'vp' && (
        // Сторона настраивается, потому что у окна ОИ обе оси заняты: слева цена,
        // справа открытый интерес — какой край свободнее, зависит от показателя.
        <div style={row}>
          <span style={label}>Сторона</span>
          {([['right', 'Справа'], ['left', 'Слева']] as const).map(([v, t]) => (
            <button
              key={v} type="button" onClick={() => api.patch(inst.id, { side: v })}
              style={{
                ...ICON_BTN, width: 'auto', padding: '0 7px', fontSize: 10.5, fontWeight: 700,
                color: (inst.side ?? 'right') === v ? 'var(--accent)' : 'var(--text-secondary)',
              }}
            >
              {t}
            </button>
          ))}
        </div>
      )}
      {inst.kind === 'bb' && (
        <div style={row}>
          <span style={label}>Отклонение</span>
          <input
            type="number" min={0.5} max={5} step={0.5} value={inst.mult ?? 2}
            onChange={(e) => api.patch(inst.id, { mult: Math.max(0.5, Math.min(5, Number(e.target.value) || 2)) })}
            style={{ width: 64, padding: '3px 6px', borderRadius: 6, fontSize: 11.5, border: '1px solid var(--border-color, rgba(128,128,128,0.35))', background: 'var(--bg-base, transparent)', color: 'var(--text-primary)' }}
          />
        </div>
      )}
      <div style={row}>
        {/* У профиля это цвет POC: сами полосы двухцветные по смыслу
            (покупки/продажи), см. volumeProfileSpec. */}
        <span style={label}>{inst.kind === 'vp' ? 'Цвет POC' : 'Цвет'}</span>
        <div style={{ display: 'flex', gap: 3, flexWrap: 'wrap' }}>
          {PALETTE.map((c) => (
            <button
              key={c} type="button" title={c} onClick={() => api.patch(inst.id, { color: c })}
              style={{ width: 14, height: 14, borderRadius: 3, background: c, cursor: 'pointer', padding: 0, border: api.colorOf(inst) === c ? '2px solid var(--text-primary)' : '1px solid rgba(128,128,128,0.35)' }}
            />
          ))}
        </div>
      </div>
      {/* Профиль рисуется прямоугольниками — толщина линии ему не про что. */}
      {inst.kind !== 'vp' && (
        <div style={row}>
          <span style={label}>Толщина</span>
          {([1, 2, 3, 4] as const).map((w) => (
            <button
              key={w} type="button" onClick={() => api.patch(inst.id, { width: w })}
              style={{ ...ICON_BTN, width: 22, fontSize: 10.5, fontWeight: 700, color: inst.width === w ? 'var(--accent)' : 'var(--text-secondary)' }}
            >
              {w}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

/** Мемо-обёртка: пересчёт дешёвый, но новая ссылка на массив пересоздаёт ВСЕ
 *  серии графика (эффект серий зависит от массива), а это уже сотня миллисекунд. */
export function useIndicatorSeries(
  list: IndicatorInst[],
  candles: IndCandle<string>[],
  toSec: (t: string) => number,
  colorOf: (i: IndicatorInst) => string,
): LwSeries[][] {
  return useMemo(() => indicatorSeriesByPane(list, candles, toSec, colorOf), [list, candles, toSec, colorOf]);
}
