/**
 * EmbedIndicators — движок пользовательских индикаторов на графике (модель ТЗ:
 * как в TradingView, где индикатор добавляется списком, настраивается по строке
 * и живёт либо поверх графика, либо отдельной панелью).
 *
 * ЭТОТ ШАГ — только НАЛОЖЕНИЯ поверх графика (pane 0): скользящие средние и
 * полосы Боллинджера. RSI/ATR/объёмы в меню видны, но выключены: им нужна своя
 * шкала (RSI это 0..100 против десятков тысяч у цены — на одной оси получится
 * плоская линия у края), а отдельные панели появятся, когда LwChartPanes
 * доберёт недостающие фичи. Поле `pane` в модели заведено сразу, чтобы при
 * этом переходе не мигрировать персист.
 *
 * Компонент НЕ трогает LwChart/LwChartPanes: индикаторы отдаются наружу обычным
 * массивом LwSeries и конкатенируются к существующим сериям embed'а.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { Eye, EyeOff, Settings2, X as XIcon, Plus } from 'lucide-react';
import type { LwSeries } from '../../components/LwChart';
import { sma, ema, bollinger, type IndCandle } from '../../utils/indicators';
import { useEmbedPersist } from './embedPersist';

/** Имя нарочно НЕ IndKind: так уже называется вид панели в SandboxPage. */
export type IndicatorKind = 'ma' | 'ema' | 'bb' | 'rsi' | 'atr' | 'volume';

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
  /** 0 = поверх графика. 1+ — отдельная панель (пока не поддержано, см. шапку). */
  pane: number;
}

interface KindDef {
  label: string;
  /** Подпись строки: «EMA 20», «Боллинджер 20×2». */
  title: (i: IndicatorInst) => string;
  defLength: number;
  defMult?: number;
  /** Может ли лежать поверх ценового графика: у RSI/ATR/объёма своя шкала. */
  overlay: boolean;
  hint?: string;
}

export const KINDS: Record<IndicatorKind, KindDef> = {
  ma: { label: 'Скользящая средняя (MA)', title: (i) => `MA ${i.length}`, defLength: 20, overlay: true },
  ema: { label: 'Экспоненциальная средняя (EMA)', title: (i) => `EMA ${i.length}`, defLength: 20, overlay: true },
  bb: { label: 'Полосы Боллинджера', title: (i) => `Боллинджер ${i.length}×${i.mult ?? 2}`, defLength: 20, defMult: 2, overlay: true },
  rsi: { label: 'RSI', title: (i) => `RSI ${i.length}`, defLength: 14, overlay: false, hint: 'Нужна отдельная панель — скоро' },
  atr: { label: 'ATR', title: (i) => `ATR ${i.length}`, defLength: 14, overlay: false, hint: 'Нужна отдельная панель — скоро' },
  volume: { label: 'Объёмы', title: () => 'Объёмы', defLength: 0, overlay: false, hint: 'Нужна отдельная панель — скоро' },
};

/** Палитра наложений — та же CC-гамма, что у ⚙-Формата серий. */
const PALETTE = ['#9B8BF0', '#E0A34E', '#57C7C7', '#5BD49C', '#EF6F6F', '#5DA3E9'];

const uid = () => 'ind_' + Date.now().toString(36) + '_' + Math.floor(Math.random() * 1e6).toString(36);

/** Санитизация чужого JSON из localStorage — по образцу parse() в EmbedFormat. */
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
      }];
    });
  } catch { return []; }
}

export interface IndicatorsApi {
  list: IndicatorInst[];
  add: (kind: IndicatorKind) => void;
  remove: (id: string) => void;
  patch: (id: string, p: Partial<IndicatorInst>) => void;
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
    setList((l) => [...l, { id: uid(), kind, length: d.defLength, mult: d.defMult, color: null, width: 2, visible: true, pane: 0 }]);
  }, []);
  const remove = useCallback((id: string) => setList((l) => l.filter((x) => x.id !== id)), []);
  const patch = useCallback((id: string, p: Partial<IndicatorInst>) => {
    setList((l) => l.map((x) => (x.id === id ? { ...x, ...p } : x)));
  }, []);
  const colorOf = useCallback((i: IndicatorInst) => i.color ?? PALETTE[Math.abs(hash(i.id)) % PALETTE.length], []);

  return { list, add, remove, patch, colorOf };
}

function hash(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
  return h;
}

/**
 * Индикаторы → LwSeries. Считаем на клиенте (замер: весь набор на 5000 барах —
 * 2.7 мс против 100 мс на пересоздание серий графика, так что узкое место не тут).
 *
 * `toSec` передаётся снаружи: у embed'ов время в исходных точках строковое, а
 * правило перевода зависит от таймфрейма (дневной — UTC-полночь, интрадей —
 * полный timestamp).
 */
export function indicatorSeries(
  list: IndicatorInst[],
  candles: IndCandle<string>[],
  toSec: (t: string) => number,
  colorOf: (i: IndicatorInst) => string,
): LwSeries[] {
  if (!candles.length) return [];
  const out: LwSeries[] = [];
  const conv = (pts: { time: string; value: number }[]) => pts.map((p) => ({ time: toSec(p.time), value: p.value }));
  for (const i of list) {
    if (!i.visible || i.pane !== 0 || !KINDS[i.kind].overlay) continue;
    const color = colorOf(i);
    const base = { scale: 'left' as const, color, lineWidth: i.width, type: 'line' as const, lastValueVisible: false };
    if (i.kind === 'ma' || i.kind === 'ema') {
      const pts = (i.kind === 'ma' ? sma : ema)(candles, i.length);
      if (!pts.length) continue;
      out.push({ ...base, id: i.id, label: KINDS[i.kind].title(i), data: conv(pts) });
    } else if (i.kind === 'bb') {
      const { mid, upper, lower } = bollinger(candles, i.length, i.mult ?? 2);
      if (!mid.length) continue;
      // Полосы тоньше середины и пунктиром — иначе три одинаковые линии сливаются.
      out.push({ ...base, id: i.id + ':u', label: `${KINDS.bb.title(i)} ↑`, data: conv(upper), dashed: true, lineWidth: 1 });
      out.push({ ...base, id: i.id, label: KINDS.bb.title(i), data: conv(mid) });
      out.push({ ...base, id: i.id + ':l', label: `${KINDS.bb.title(i)} ↓`, data: conv(lower), dashed: true, lineWidth: 1 });
    }
  }
  return out;
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
export function IndicatorList({ api, native, visible }: {
  api: IndicatorsApi;
  native: NativeRow[];
  visible: boolean;
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [settingsFor, setSettingsFor] = useState<string | null>(null);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!menuOpen && !settingsFor) return;
    const onDown = (e: PointerEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) { setMenuOpen(false); setSettingsFor(null); }
    };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [menuOpen, settingsFor]);

  if (!visible) return null;
  const sel = api.list.find((x) => x.id === settingsFor) || null;

  return (
    <div
      ref={rootRef}
      data-export-ignore="true"
      style={{ position: 'absolute', top: 8, left: 8, zIndex: 10, display: 'flex', flexDirection: 'column', gap: 1, alignItems: 'flex-start' }}
    >
      {native.map((r) => (
        <Row key={r.id} color={r.color} label={r.label} visible={r.visible} onToggle={r.onToggle} />
      ))}
      {api.list.map((i) => (
        <Row
          key={i.id}
          color={api.colorOf(i)}
          label={KINDS[i.kind].title(i)}
          visible={i.visible}
          onToggle={() => api.patch(i.id, { visible: !i.visible })}
          onSettings={() => setSettingsFor(settingsFor === i.id ? null : i.id)}
          onRemove={() => api.remove(i.id)}
        />
      ))}

      <div style={{ position: 'relative' }}>
        <button
          type="button"
          onClick={() => { setSettingsFor(null); setMenuOpen((v) => !v); }}
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
            {(Object.keys(KINDS) as IndicatorKind[]).map((k) => {
              const d = KINDS[k];
              return (
                <button
                  key={k}
                  type="button"
                  disabled={!d.overlay}
                  title={d.hint}
                  onClick={() => { if (d.overlay) { api.add(k); setMenuOpen(false); } }}
                  style={{
                    display: 'block', width: '100%', textAlign: 'left', padding: '5px 8px', borderRadius: 6,
                    border: 'none', background: 'transparent', fontSize: 11.5,
                    color: d.overlay ? 'var(--text-primary)' : 'var(--text-muted, #888)',
                    cursor: d.overlay ? 'pointer' : 'default',
                  }}
                >
                  {d.label}
                  {!d.overlay && <span style={{ fontSize: 10, opacity: 0.75 }}> · {d.hint}</span>}
                </button>
              );
            })}
          </div>
        )}
      </div>

      {sel && <SettingsPopover inst={sel} api={api} onClose={() => setSettingsFor(null)} />}
    </div>
  );
}

function Row({ color, label, visible, onToggle, onSettings, onRemove }: {
  color: string; label: string; visible: boolean;
  onToggle: () => void; onSettings?: () => void; onRemove?: () => void;
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
      {onSettings && <button type="button" title="Настройки" onClick={onSettings} style={ICON_BTN}><Settings2 size={11} /></button>}
      {onRemove && <button type="button" title="Удалить" onClick={onRemove} style={ICON_BTN}><XIcon size={11} /></button>}
    </div>
  );
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
        <span style={label}>Период</span>
        <input
          type="number" min={2} max={500} value={inst.length}
          onChange={(e) => api.patch(inst.id, { length: Math.max(2, Math.min(500, Number(e.target.value) || 2)) })}
          style={{ width: 64, padding: '3px 6px', borderRadius: 6, fontSize: 11.5, border: '1px solid var(--border-color, rgba(128,128,128,0.35))', background: 'var(--bg-base, transparent)', color: 'var(--text-primary)' }}
        />
      </div>
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
        <span style={label}>Цвет</span>
        <div style={{ display: 'flex', gap: 3, flexWrap: 'wrap' }}>
          {PALETTE.map((c) => (
            <button
              key={c} type="button" title={c} onClick={() => api.patch(inst.id, { color: c })}
              style={{ width: 14, height: 14, borderRadius: 3, background: c, cursor: 'pointer', padding: 0, border: api.colorOf(inst) === c ? '2px solid var(--text-primary)' : '1px solid rgba(128,128,128,0.35)' }}
            />
          ))}
        </div>
      </div>
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
): LwSeries[] {
  return useMemo(() => indicatorSeries(list, candles, toSec, colorOf), [list, candles, toSec, colorOf]);
}
