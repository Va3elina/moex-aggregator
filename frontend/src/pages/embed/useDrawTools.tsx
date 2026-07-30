/**
 * useDrawTools — общая логика «рисования на графике» (модель TradingView),
 * вынесена из EmbedOpenInterest.tsx (первого индикатора, где появилась) в
 * переиспользуемый хук + JSX-оверлей, чтобы не копипастить ~300 строк в
 * каждый следующий LwChart-based embed (Баффетт/Фонды/…).
 *
 * `persistKey` — ключ персиста фигур (`useEmbedPersist`); ОИ передаёт
 * инструмент-зависимый ключ (`frame:embed:oi:draw:${instrument}`), рыночные
 * индикаторы (без инструмента) — статичный (`frame:embed:buffett:draw`).
 * Смена ключа на лету (переключение инструмента) поддержана — как в ОИ,
 * загрузка триггерится эффектом на [persistKey], сохранение читает АКТУАЛЬНЫЙ
 * ключ из ref (без гонки при быстром переключении).
 */
import {
  useCallback, useEffect, useLayoutEffect, useRef, useState, Suspense, lazy,
  type CSSProperties, type ReactNode,
} from 'react';
import {
  Pencil, Camera, MousePointer2, TrendingUp, Minus, Square, Type, Trash2,
  MoveUpRight, ArrowUpRight, Brush, Circle, AlignJustify, Magnet, Eye, EyeOff, Lock, LockOpen,
  Ruler, Layers, X as XIcon, GripVertical, Repeat, Settings2, MoreHorizontal, Copy,
  ChevronsUp, ChevronsDown, Plus,
} from 'lucide-react';
import type { LwDrawing, LwDrawTool, LwDash, LwMagnet } from '../../components/LwChart';
import type { ExportMetadata } from '../../components/export/types';
import { useEmbedPersist } from './embedPersist';

const ExportModal = lazy(() => import('../../components/export/ExportModal'));

const DRAW_TOOLS: { id: LwDrawTool; title: string; Icon: typeof MousePointer2; rot?: number }[] = [
  { id: 'select', title: 'Выделение / перемещение', Icon: MousePointer2 },
  { id: 'trend', title: 'Трендовая линия', Icon: TrendingUp },
  { id: 'ray', title: 'Луч', Icon: MoveUpRight },
  { id: 'arrow', title: 'Стрелка', Icon: ArrowUpRight },
  { id: 'hline', title: 'Горизонтальная линия', Icon: Minus },
  { id: 'vline', title: 'Вертикальная линия', Icon: Minus, rot: 90 },
  { id: 'rect', title: 'Прямоугольник', Icon: Square },
  { id: 'ellipse', title: 'Эллипс', Icon: Circle },
  { id: 'fib', title: 'Фибоначчи', Icon: AlignJustify },
  { id: 'brush', title: 'Кисть', Icon: Brush },
  { id: 'ruler', title: 'Линейка', Icon: Ruler },
  { id: 'text', title: 'Текст', Icon: Type },
];
const DRAW_TOOL_NAME: Record<string, string> = Object.fromEntries(DRAW_TOOLS.filter((t) => t.id !== 'select').map((t) => [t.id, t.title]));
const DRAW_HOTKEY: Record<string, string> = { trend: 'Alt+T', hline: 'Alt+H', vline: 'Alt+V', fib: 'Alt+F', rect: 'Alt+⇧R' };
/** Палитра поповера цвета: верхний ряд — базовые тона (те же, что были в старом
 *  тулбаре, + нейтрали), нижний — их приглушённые варианты. Держим ЛИТЕРАЛАМИ, а
 *  не токенами темы: цвет фигуры сохраняется в персист и должен остаться тем же
 *  при смене темы/экспорте (токен бы «поплыл»). */
const PALETTE_ROWS: string[][] = [
  ['#F5F1E8', '#C9C4B8', '#9A958C', '#6B6760', '#0A0A0A', '#EF6F6F', '#FF5C2B', '#E0A34E', '#5BD49C', '#57C7C7', '#5DA3E9', '#9B8BF0'],
  ['#FFFFFF', '#E3DED2', '#807B72', '#3A3833', '#1C1B20', '#B14A4A', '#C2451F', '#A97B33', '#3F9E75', '#3A8C8C', '#3E7BB5', '#6E5FC0'],
];
const DASH_NAME: Record<LwDash, string> = { solid: 'Линия', dashed: 'Штриховой пунктир', dotted: 'Точечный пунктир' };

function drawToolBtn(active: boolean): CSSProperties {
  return {
    width: 30, height: 30, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
    border: 'none', borderRadius: 7, cursor: 'pointer', padding: 0,
    background: active ? 'color-mix(in srgb, var(--accent) 20%, transparent)' : 'transparent',
    color: active ? 'var(--accent)' : 'var(--text-secondary)',
  };
}

export interface DrawTools {
  drawMode: boolean;
  setDrawMode: (v: boolean) => void;
  drawTool: LwDrawTool;
  drawColor: string;
  drawings: LwDrawing[];
  setDrawings: (d: LwDrawing[] | ((prev: LwDrawing[]) => LwDrawing[])) => void;
  drawWidth: number;
  drawDash: LwDash;
  drawOpacity: number;
  selectedDrawId: string | null;
  setSelectedDrawId: (id: string | null) => void;
  drawMagnet: LwMagnet;
  drawHidden: boolean;
  setDrawHidden: (v: boolean | ((p: boolean) => boolean)) => void;
  drawLocked: boolean;
  setDrawLocked: (v: boolean | ((p: boolean) => boolean)) => void;
  onToolReset: () => void;
  exportOpen: boolean;
  setExportOpen: (v: boolean) => void;
  /** Поля ниже — только для DrawToolsOverlay/DrawExportActions, не для LwChart-пропов. */
  drawKeep: boolean;
  setDrawKeep: (v: boolean | ((p: boolean) => boolean)) => void;
  setDrawTool: (t: LwDrawTool) => void;
  setDrawMagnet: (v: LwMagnet | ((p: LwMagnet) => LwMagnet)) => void;
  layersOpen: boolean;
  setLayersOpen: (v: boolean | ((p: boolean) => boolean)) => void;
  dragLayerId: string | null;
  setDragLayerId: (id: string | null) => void;
  reorderLayer: (fromId: string, toId: string) => void;
  /** Пиксельный бокс выделенной фигуры (от LwChart/LwChartPanes) — якорь панели свойств. */
  selRect: { x: number; y: number; w: number; h: number } | null;
  setSelRect: (r: { x: number; y: number; w: number; h: number } | null) => void;
  /** Ручное смещение панели свойств (перетаскивание за ⋮⋮). Сбрасывается при смене выделения. */
  panelOffset: { dx: number; dy: number };
  setPanelOffset: (o: { dx: number; dy: number } | ((p: { dx: number; dy: number }) => { dx: number; dy: number })) => void;
  settingsOpen: boolean;
  setSettingsOpen: (v: boolean) => void;
  /** Выделенная фигура целиком (нужна панели/модалке — стиль, текст, координаты). */
  selectedDraw: LwDrawing | null;
  /** Патч ЛЮБОГО поля выделенной фигуры (в отличие от applyStyle не трогает
   *  глобальный стиль «для новых фигур»). */
  patchSelected: (patch: Partial<LwDrawing>) => void;
  deleteSelected: () => void;
  cloneSelected: () => void;
  /** Порядок слоёв: массив drawings рисуется по порядку → конец = передний план. */
  moveSelectedToFront: () => void;
  moveSelectedToBack: () => void;
}

export function useDrawTools(persistKey: string): DrawTools {
  const { rd, wr } = useEmbedPersist();

  const [exportOpen, setExportOpen] = useState(false);
  const [drawMode, setDrawMode] = useState(false);
  const [drawTool, setDrawTool] = useState<LwDrawTool>('select');
  const [drawColor, setDrawColor] = useState('#FF5C2B');
  const [drawings, setDrawings] = useState<LwDrawing[]>([]);
  const [selectedDrawId, setSelectedDrawId] = useState<string | null>(null);
  const [drawMagnet, setDrawMagnet] = useState<LwMagnet>('off');
  const [drawKeep, setDrawKeep] = useState(false);
  const [drawHidden, setDrawHidden] = useState(false);
  const [drawLocked, setDrawLocked] = useState(false);
  const [drawWidth, setDrawWidth] = useState(2);
  const [drawDash, setDrawDash] = useState<LwDash>('solid');
  const [drawOpacity, setDrawOpacity] = useState(1);
  const [layersOpen, setLayersOpen] = useState(false);
  const [dragLayerId, setDragLayerId] = useState<string | null>(null);
  const [selRect, setSelRect] = useState<{ x: number; y: number; w: number; h: number } | null>(null);
  const [panelOffset, setPanelOffset] = useState({ dx: 0, dy: 0 });
  const [settingsOpen, setSettingsOpen] = useState(false);

  // Новое выделение → панель снова «по умолчанию над элементом» (п.3 ТЗ) и без
  // открытой модалки настроек от прошлой фигуры.
  useEffect(() => { setPanelOffset({ dx: 0, dy: 0 }); setSettingsOpen(false); }, [selectedDrawId]);

  const reorderLayer = (fromId: string, toId: string) => {
    if (fromId === toId) return;
    setDrawings((ds) => {
      const arr = ds.slice();
      const fi = arr.findIndex((d) => d.id === fromId), ti = arr.findIndex((d) => d.id === toId);
      if (fi < 0 || ti < 0) return ds;
      const [m] = arr.splice(fi, 1); arr.splice(ti, 0, m); return arr;
    });
  };

  const drawSaveReady = useRef(false);
  const selectedDraw = drawings.find((d) => d.id === selectedDrawId) || null;
  const patchSelected = (patch: Partial<LwDrawing>) => {
    if (!selectedDrawId) return;
    setDrawings((ds) => ds.map((d) => (d.id === selectedDrawId ? { ...d, ...patch } : d)));
    // Стиль последней правки запоминаем как дефолт для НОВЫХ фигур (как в
    // TradingView): раскрасил одну стрелку — следующая рисуется тем же цветом.
    if (patch.color !== undefined) setDrawColor(patch.color);
    if (patch.width !== undefined) setDrawWidth(patch.width);
    if (patch.dash !== undefined) setDrawDash(patch.dash);
    if (patch.opacity !== undefined) setDrawOpacity(patch.opacity);
  };
  const deleteSelected = () => {
    if (!selectedDrawId) return;
    setDrawings((ds) => ds.filter((d) => d.id !== selectedDrawId));
    setSelectedDrawId(null);
  };
  const cloneSelected = () => {
    const src = drawings.find((d) => d.id === selectedDrawId);
    if (!src) return;
    // Смещаем копию на 1 бар вправо и чуть ниже по цене — иначе клон ложится
    // ровно под оригинал и выглядит как «ничего не произошло».
    const dy = src.pts.length > 1 ? Math.abs(src.pts[0].price - src.pts[1].price) * 0.12 : Math.abs(src.pts[0].price) * 0.01;
    const id = 'dr_' + Date.now().toString(36) + '_' + Math.floor(Math.random() * 1e6).toString(36);
    const copy: LwDrawing = { ...src, id, pts: src.pts.map((p) => ({ logical: p.logical + 1, price: p.price - dy })) };
    setDrawings((ds) => [...ds, copy]);
    setSelectedDrawId(id);
  };
  const moveSelectedToFront = () => setDrawings((ds) => {
    const i = ds.findIndex((d) => d.id === selectedDrawId); if (i < 0) return ds;
    const arr = ds.slice(); const [m] = arr.splice(i, 1); arr.push(m); return arr;
  });
  const moveSelectedToBack = () => setDrawings((ds) => {
    const i = ds.findIndex((d) => d.id === selectedDrawId); if (i < 0) return ds;
    const arr = ds.slice(); const [m] = arr.splice(i, 1); arr.unshift(m); return arr;
  });

  // Персист фигур — по persistKey (может меняться, напр. смена инструмента у ОИ).
  const persistKeyRef = useRef(persistKey); persistKeyRef.current = persistKey;
  useEffect(() => {
    const raw = rd(persistKey, '');
    try { setDrawings(raw ? (JSON.parse(raw) as LwDrawing[]) : []); } catch { setDrawings([]); }
    setSelectedDrawId(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [persistKey]);
  useEffect(() => {
    if (!drawSaveReady.current) { drawSaveReady.current = true; return; }
    wr(persistKeyRef.current, JSON.stringify(drawings));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [drawings]);

  // Клавиатура в режиме рисования (как в TradingView) — e.code надёжнее e.key
  // (Alt+буква на Mac даёт диакритику через e.key).
  useEffect(() => {
    if (!drawMode) return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.key === 'Delete' || e.key === 'Backspace') && selectedDrawId) {
        setDrawings((ds) => ds.filter((d) => d.id !== selectedDrawId)); setSelectedDrawId(null); return;
      }
      if (e.key === 'Escape') { setDrawTool('select'); setSelectedDrawId(null); return; }
      if (!e.altKey) return;
      if ((e.ctrlKey || e.metaKey) && e.code === 'KeyH') { e.preventDefault(); setDrawHidden((v) => !v); return; }
      if (e.ctrlKey || e.metaKey) return;
      let tool: LwDrawTool | null = null;
      if (e.code === 'KeyT') tool = 'trend';
      else if (e.code === 'KeyH') tool = 'hline';
      else if (e.code === 'KeyV') tool = 'vline';
      else if (e.code === 'KeyF') tool = 'fib';
      else if (e.shiftKey && e.code === 'KeyR') tool = 'rect';
      if (tool) { e.preventDefault(); setDrawTool(tool); }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [drawMode, selectedDrawId]);

  const onToolReset = useCallback(() => { if (!drawKeep) setDrawTool('select'); }, [drawKeep]);

  return {
    drawMode, setDrawMode, drawTool, drawColor, drawings, setDrawings, drawWidth, drawDash, drawOpacity,
    selectedDrawId, setSelectedDrawId, drawMagnet, drawHidden, setDrawHidden, drawLocked, setDrawLocked,
    onToolReset, exportOpen, setExportOpen,
    drawKeep, setDrawKeep, setDrawTool, setDrawMagnet, layersOpen, setLayersOpen,
    dragLayerId, setDragLayerId, reorderLayer,
    selRect, setSelRect, panelOffset, setPanelOffset, settingsOpen, setSettingsOpen,
    selectedDraw, patchSelected, deleteSelected, cloneSelected, moveSelectedToFront, moveSelectedToBack,
  };
}

/** Кнопки ✎ рисование / 📷 экспорт — для `actions` пропа EmbedFrame. */
export function DrawExportActions({ draw, visible }: { draw: DrawTools; visible: boolean }): ReactNode {
  if (!visible) return undefined;
  return (
    <>
      <button
        type="button"
        onClick={() => { draw.setDrawMode(!draw.drawMode); draw.setSelectedDrawId(null); }}
        title="Рисование на графике"
        aria-label="Рисование на графике"
        style={{
          width: 28, height: 28, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
          border: 'none', borderRadius: 7, cursor: 'pointer', flexShrink: 0, padding: 0,
          background: draw.drawMode ? 'color-mix(in srgb, var(--accent) 16%, transparent)' : 'transparent',
          color: draw.drawMode ? 'var(--accent)' : 'var(--text-secondary)',
        }}
      >
        <Pencil size={15} />
      </button>
      <button
        type="button"
        onClick={() => draw.setExportOpen(true)}
        title="Экспорт графика"
        aria-label="Экспорт графика"
        style={{
          width: 28, height: 28, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
          border: 'none', borderRadius: 7, background: 'transparent', color: 'var(--text-secondary)',
          cursor: 'pointer', flexShrink: 0, padding: 0,
        }}
      >
        <Camera size={15} />
      </button>
    </>
  );
}

// ───────────────── Контекстная панель свойств выделенной фигуры ─────────────────
// Раньше тулбар свойств висел постоянно по центру сверху и правил «стиль для
// новых фигур». Теперь он появляется ТОЛЬКО при выделении и правит КОНКРЕТНУЮ
// фигуру, вставая над ней (модель TradingView) — п.1-4 ТЗ.

const SURFACE: CSSProperties = {
  background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 96%, transparent)',
  border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
  backdropFilter: 'blur(4px)', boxShadow: '0 8px 26px rgba(0,0,0,0.4)',
};
const PBTN: CSSProperties = {
  height: 26, minWidth: 26, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', gap: 3,
  border: 'none', borderRadius: 6, background: 'transparent', color: 'var(--text-primary)',
  cursor: 'pointer', padding: '0 4px',
};
const MENU_ITEM: CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 7, width: '100%', padding: '5px 8px', borderRadius: 6,
  border: 'none', background: 'transparent', color: 'var(--text-primary)', cursor: 'pointer',
  fontSize: 11.5, textAlign: 'left', whiteSpace: 'nowrap',
};

function LinePreview({ dash, width, color, w = 26 }: { dash: LwDash; width: number; color?: string; w?: number }) {
  return (
    <svg width={w} height={12} style={{ display: 'block', flexShrink: 0 }}>
      <line
        x1={1} y1={6} x2={w - 1} y2={6} stroke={color || 'currentColor'} strokeWidth={width}
        strokeDasharray={dash === 'solid' ? undefined : dash === 'dashed' ? '4 3' : '0.5 3'}
        strokeLinecap={dash === 'dotted' ? 'round' : 'butt'}
      />
    </svg>
  );
}

function DrawStylePanel({ draw }: { draw: DrawTools }): ReactNode {
  const { selRect, selectedDraw } = draw;
  const [menu, setMenu] = useState<null | 'color' | 'width' | 'dash' | 'more'>(null);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [pos, setPos] = useState<{ left: number; top: number } | null>(null);

  // Закрытие выпадашки по клику вне панели. capture=true — иначе pointerdown
  // сначала уйдёт на слой рисования графика и снимет выделение.
  useEffect(() => {
    if (!menu) return;
    const onDown = (e: PointerEvent) => { if (rootRef.current && !rootRef.current.contains(e.target as Node)) setMenu(null); };
    document.addEventListener('pointerdown', onDown, true);
    return () => document.removeEventListener('pointerdown', onDown, true);
  }, [menu]);

  // Позиция: по умолчанию НАД фигурой и по её центру; не влезло сверху — под ней;
  // плюс ручное смещение (перетаскивание) и клампинг в границы контейнера.
  useLayoutEffect(() => {
    const el = rootRef.current;
    if (!el || !selRect) return;
    const host = el.offsetParent as HTMLElement | null;
    const HW = host?.clientWidth ?? 0, HH = host?.clientHeight ?? 0;
    const pw = el.offsetWidth, ph = el.offsetHeight;
    let left = selRect.x + selRect.w / 2 - pw / 2 + draw.panelOffset.dx;
    let top = selRect.y - ph - 12 + draw.panelOffset.dy;
    if (top < 4) top = selRect.y + selRect.h + 12 + draw.panelOffset.dy;
    left = Math.max(4, Math.min(Math.max(4, HW - pw - 4), left));
    top = Math.max(4, Math.min(Math.max(4, HH - ph - 4), top));
    setPos((p) => (p && Math.abs(p.left - left) < 0.5 && Math.abs(p.top - top) < 0.5 ? p : { left, top }));
  }, [selRect, draw.panelOffset, menu, selectedDraw?.id]);

  const onGripDown = (e: React.PointerEvent) => {
    e.preventDefault(); e.stopPropagation();
    const sx = e.clientX, sy = e.clientY, d0 = draw.panelOffset;
    const move = (ev: PointerEvent) => draw.setPanelOffset({ dx: d0.dx + (ev.clientX - sx), dy: d0.dy + (ev.clientY - sy) });
    const up = () => { window.removeEventListener('pointermove', move); window.removeEventListener('pointerup', up); };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  if (!selectedDraw || !selRect) return null;
  const d = selectedDraw;
  const op = d.opacity == null ? 1 : d.opacity;
  const dash: LwDash = d.dash ?? 'solid';
  const set = (patch: Partial<LwDrawing>) => draw.patchSelected(patch);
  const pop: CSSProperties = { position: 'absolute', top: 'calc(100% + 6px)', zIndex: 2, padding: 7, borderRadius: 9, ...SURFACE };

  return (
    <div
      ref={rootRef}
      data-export-ignore="true"
      // Панель не должна отдавать клики слою рисования под ней (иначе выделение слетает).
      onPointerDown={(e) => e.stopPropagation()}
      style={{
        position: 'absolute', left: pos?.left ?? -9999, top: pos?.top ?? -9999, zIndex: 11,
        display: 'flex', alignItems: 'center', gap: 2, padding: '3px 4px', borderRadius: 9,
        visibility: pos ? 'visible' : 'hidden', ...SURFACE,
      }}
    >
      <span onPointerDown={onGripDown} title="Переместить панель" style={{ display: 'inline-flex', alignItems: 'center', color: 'var(--text-secondary)', cursor: 'grab', padding: '0 1px', touchAction: 'none' }}>
        <GripVertical size={13} />
      </span>

      {/* ── цвет + прозрачность ── */}
      <div style={{ position: 'relative' }}>
        <button type="button" title="Цвет" onClick={() => setMenu((m) => (m === 'color' ? null : 'color'))} style={PBTN}>
          <span style={{ width: 15, height: 15, borderRadius: 4, background: d.color, opacity: op, border: '1px solid var(--border-color, rgba(128,128,128,0.5))', display: 'inline-block' }} />
        </button>
        {menu === 'color' && (
          <div style={{ ...pop, left: 0, width: 226 }}>
            {PALETTE_ROWS.map((row, ri) => (
              <div key={ri} style={{ display: 'flex', gap: 3, marginBottom: 3 }}>
                {row.map((c) => (
                  <button
                    key={c} type="button" title={c} onClick={() => set({ color: c })}
                    style={{
                      width: 14, height: 14, borderRadius: 3, background: c, cursor: 'pointer', padding: 0,
                      border: d.color.toLowerCase() === c.toLowerCase() ? '2px solid var(--accent)' : '1px solid rgba(128,128,128,0.35)',
                    }}
                  />
                ))}
              </div>
            ))}
            <label style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 7, fontSize: 11, color: 'var(--text-secondary)' }}>
              <span style={{ flexShrink: 0 }}>Прозрачность</span>
              {/* minWidth:0 обязателен: без него flex-элемент не сжимается ниже своей
                  «естественной» ширины и выталкивает процент за край поповера. */}
              <input type="range" min={10} max={100} value={Math.round(op * 100)} onChange={(e) => set({ opacity: Number(e.target.value) / 100 })} style={{ flex: 1, minWidth: 0, accentColor: 'var(--accent)' }} />
              <span style={{ flexShrink: 0, minWidth: 28, textAlign: 'right', color: 'var(--text-primary)' }}>{Math.round(op * 100)}%</span>
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 6, fontSize: 11, color: 'var(--text-secondary)', cursor: 'pointer' }}>
              <Plus size={12} />
              <span>Свой цвет</span>
              <input type="color" value={d.color} onChange={(e) => set({ color: e.target.value })} style={{ width: 26, height: 18, padding: 0, border: 'none', background: 'transparent', cursor: 'pointer' }} />
            </label>
          </div>
        )}
      </div>

      {/* ── толщина ── */}
      <div style={{ position: 'relative' }}>
        <button type="button" title="Толщина линии" onClick={() => setMenu((m) => (m === 'width' ? null : 'width'))} style={PBTN}>
          <LinePreview dash="solid" width={d.width} w={20} />
        </button>
        {menu === 'width' && (
          <div style={{ ...pop, left: 0, minWidth: 92 }}>
            {[1, 2, 3, 4].map((wv) => (
              <button key={wv} type="button" onClick={() => { set({ width: wv }); setMenu(null); }} style={{ ...MENU_ITEM, background: d.width === wv ? 'color-mix(in srgb, var(--accent) 16%, transparent)' : 'transparent' }}>
                <LinePreview dash="solid" width={wv} />
                <span>{wv}px</span>
              </button>
            ))}
          </div>
        )}
      </div>

      {/* ── тип линии ── */}
      <div style={{ position: 'relative' }}>
        <button type="button" title="Тип линии" onClick={() => setMenu((m) => (m === 'dash' ? null : 'dash'))} style={PBTN}>
          <LinePreview dash={dash} width={2} w={20} />
        </button>
        {menu === 'dash' && (
          <div style={{ ...pop, left: 0, minWidth: 168 }}>
            {(['solid', 'dashed', 'dotted'] as LwDash[]).map((id) => (
              <button key={id} type="button" onClick={() => { set({ dash: id }); setMenu(null); }} style={{ ...MENU_ITEM, background: dash === id ? 'color-mix(in srgb, var(--accent) 16%, transparent)' : 'transparent' }}>
                <LinePreview dash={id} width={2} />
                <span>{DASH_NAME[id]}</span>
              </button>
            ))}
          </div>
        )}
      </div>

      <div style={{ width: 1, height: 16, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />

      <button type="button" title="Настройки" onClick={() => { setMenu(null); draw.setSettingsOpen(true); }} style={PBTN}><Settings2 size={14} /></button>
      <button type="button" title={d.locked ? 'Разблокировать' : 'Заблокировать'} onClick={() => set({ locked: !d.locked })} style={{ ...PBTN, color: d.locked ? 'var(--accent)' : 'var(--text-primary)' }}>
        {d.locked ? <Lock size={14} /> : <LockOpen size={14} />}
      </button>
      <button type="button" title="Удалить" onClick={draw.deleteSelected} style={PBTN}><Trash2 size={14} /></button>

      <div style={{ position: 'relative' }}>
        <button type="button" title="Ещё" onClick={() => setMenu((m) => (m === 'more' ? null : 'more'))} style={PBTN}><MoreHorizontal size={14} /></button>
        {menu === 'more' && (
          <div style={{ ...pop, right: 0, minWidth: 176 }}>
            <button type="button" onClick={() => { draw.moveSelectedToFront(); setMenu(null); }} style={MENU_ITEM}><ChevronsUp size={13} />На передний план</button>
            <button type="button" onClick={() => { draw.moveSelectedToBack(); setMenu(null); }} style={MENU_ITEM}><ChevronsDown size={13} />На задний план</button>
            <button type="button" onClick={() => { draw.cloneSelected(); setMenu(null); }} style={MENU_ITEM}><Copy size={13} />Клонировать</button>
            <button type="button" onClick={() => { set({ hidden: true }); draw.setSelectedDrawId(null); setMenu(null); }} style={MENU_ITEM}><EyeOff size={13} />Скрыть</button>
            <button type="button" onClick={() => { draw.setLayersOpen(true); setMenu(null); }} style={MENU_ITEM}><Layers size={13} />Порядок слоёв…</button>
          </div>
        )}
      </div>
    </div>
  );
}

/** Модалка «Настройки фигуры»: Стиль / Текст / Координаты. Правки применяются
 *  сразу; «Отмена» откатывает к снимку, сделанному при открытии. */
function DrawSettingsModal({ draw }: { draw: DrawTools }): ReactNode {
  const [tab, setTab] = useState<'style' | 'text' | 'coords'>('style');
  const d = draw.selectedDraw;
  const dRef = useRef(d); dRef.current = d;
  const snapRef = useRef<LwDrawing | null>(null);
  useEffect(() => { if (draw.settingsOpen) { snapRef.current = dRef.current; setTab('style'); } }, [draw.settingsOpen]);

  if (!draw.settingsOpen || !d) return null;
  const set = (patch: Partial<LwDrawing>) => draw.patchSelected(patch);
  const op = d.opacity == null ? 1 : d.opacity;
  const dash: LwDash = d.dash ?? 'solid';
  const tabs: { id: typeof tab; name: string }[] = [
    { id: 'style', name: 'Стиль' },
    ...(d.tool === 'text' ? [{ id: 'text' as const, name: 'Текст' }] : []),
    { id: 'coords', name: 'Координаты' },
  ];
  const label: CSSProperties = { fontSize: 11, color: 'var(--text-secondary)', minWidth: 96 };
  // flexWrap — иначе строка «Точка №N · Цена · Бар» не влезала в 340px модалки и
  // выдавливала горизонтальный скроллбар вместо переноса.
  const row: CSSProperties = { display: 'flex', alignItems: 'center', gap: 9, marginBottom: 9, flexWrap: 'wrap' };
  const inputSt: CSSProperties = {
    width: 96, padding: '4px 6px', borderRadius: 6, fontSize: 11.5,
    border: '1px solid var(--border-color, rgba(128,128,128,0.35))',
    background: 'var(--bg-base, transparent)', color: 'var(--text-primary)',
  };
  const setPt = (i: number, patch: Partial<{ price: number; logical: number }>) =>
    set({ pts: d.pts.map((p, k) => (k === i ? { ...p, ...patch } : p)) });

  return (
    <div
      data-export-ignore="true"
      onPointerDown={(e) => e.stopPropagation()}
      style={{ position: 'absolute', inset: 0, zIndex: 12, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'rgba(0,0,0,0.34)' }}
    >
      <div style={{ width: 'min(340px, calc(100% - 24px))', maxHeight: 'calc(100% - 24px)', overflowY: 'auto', borderRadius: 11, padding: 12, ...SURFACE }} className="sb-scroll">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 9 }}>
          <span style={{ fontSize: 12.5, fontWeight: 700, color: 'var(--text-primary)' }}>{DRAW_TOOL_NAME[d.tool] || 'Фигура'}</span>
          <button type="button" onClick={() => draw.setSettingsOpen(false)} style={{ ...PBTN, color: 'var(--text-secondary)' }}><XIcon size={14} /></button>
        </div>
        <div style={{ display: 'flex', gap: 3, marginBottom: 11, borderBottom: '1px solid var(--border-color, rgba(128,128,128,0.3))' }}>
          {tabs.map((t) => (
            <button
              key={t.id} type="button" onClick={() => setTab(t.id)}
              style={{
                border: 'none', background: 'transparent', cursor: 'pointer', padding: '5px 9px', fontSize: 11.5,
                fontWeight: tab === t.id ? 700 : 500, color: tab === t.id ? 'var(--text-primary)' : 'var(--text-secondary)',
                borderBottom: tab === t.id ? '2px solid var(--accent)' : '2px solid transparent', marginBottom: -1,
              }}
            >
              {t.name}
            </button>
          ))}
        </div>

        {tab === 'style' && (
          <>
            <div style={row}>
              <span style={label}>Цвет</span>
              <div style={{ display: 'flex', gap: 3, flexWrap: 'wrap' }}>
                {PALETTE_ROWS[0].map((c) => (
                  <button key={c} type="button" title={c} onClick={() => set({ color: c })} style={{ width: 15, height: 15, borderRadius: 3, background: c, cursor: 'pointer', padding: 0, border: d.color.toLowerCase() === c.toLowerCase() ? '2px solid var(--accent)' : '1px solid rgba(128,128,128,0.35)' }} />
                ))}
                <input type="color" value={d.color} onChange={(e) => set({ color: e.target.value })} title="Свой цвет" style={{ width: 22, height: 17, padding: 0, border: 'none', background: 'transparent', cursor: 'pointer' }} />
              </div>
            </div>
            <div style={row}>
              <span style={label}>Толщина</span>
              {[1, 2, 3, 4].map((wv) => (
                <button key={wv} type="button" onClick={() => set({ width: wv })} style={{ ...PBTN, border: '1px solid ' + (d.width === wv ? 'var(--accent)' : 'transparent') }}><LinePreview dash="solid" width={wv} w={22} /></button>
              ))}
            </div>
            <div style={row}>
              <span style={label}>Тип линии</span>
              {(['solid', 'dashed', 'dotted'] as LwDash[]).map((id) => (
                <button key={id} type="button" title={DASH_NAME[id]} onClick={() => set({ dash: id })} style={{ ...PBTN, border: '1px solid ' + (dash === id ? 'var(--accent)' : 'transparent') }}><LinePreview dash={id} width={2} w={22} /></button>
              ))}
            </div>
            <div style={row}>
              <span style={label}>Прозрачность</span>
              <input type="range" min={10} max={100} value={Math.round(op * 100)} onChange={(e) => set({ opacity: Number(e.target.value) / 100 })} style={{ flex: 1, accentColor: 'var(--accent)' }} />
              <span style={{ fontSize: 11, color: 'var(--text-primary)', minWidth: 30, textAlign: 'right' }}>{Math.round(op * 100)}%</span>
            </div>
          </>
        )}

        {tab === 'text' && (
          <>
            <textarea
              value={d.text ?? ''} onChange={(e) => set({ text: e.target.value })} rows={3}
              placeholder="Текст на графике"
              style={{ ...inputSt, width: '100%', resize: 'vertical', marginBottom: 9, fontFamily: 'inherit' }}
            />
            <div style={row}>
              <span style={label}>Размер</span>
              {[1, 2, 3, 4].map((wv) => (
                <button key={wv} type="button" onClick={() => set({ width: wv })} style={{ ...PBTN, fontSize: 11, fontWeight: 700, border: '1px solid ' + (d.width === wv ? 'var(--accent)' : 'transparent') }}>{13 + wv * 2}</button>
              ))}
            </div>
          </>
        )}

        {tab === 'coords' && (
          <>
            {d.pts.map((p, i) => (
              <div key={i} style={row}>
                <span style={{ ...label, minWidth: 66 }}>Точка №{i + 1}</span>
                <label style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: 'var(--text-secondary)' }}>
                  Цена
                  <input type="number" step="any" value={Number(p.price.toFixed(4))} onChange={(e) => setPt(i, { price: Number(e.target.value) })} style={{ ...inputSt, width: 88 }} />
                </label>
                <label style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: 'var(--text-secondary)' }}>
                  Бар
                  <input type="number" step={1} value={Math.round(p.logical)} onChange={(e) => setPt(i, { logical: Number(e.target.value) })} style={{ ...inputSt, width: 58 }} />
                </label>
              </div>
            ))}
            {d.tool === 'brush' && <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>У кисти {d.pts.length} точек — правьте перетаскиванием на графике.</div>}
          </>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 7, marginTop: 13 }}>
          <button
            type="button"
            onClick={() => { if (snapRef.current) draw.patchSelected(snapRef.current); draw.setSettingsOpen(false); }}
            style={{ padding: '5px 13px', borderRadius: 7, fontSize: 11.5, cursor: 'pointer', border: '1px solid var(--border-color, rgba(128,128,128,0.35))', background: 'transparent', color: 'var(--text-secondary)' }}
          >
            Отмена
          </button>
          <button
            type="button" onClick={() => draw.setSettingsOpen(false)}
            style={{ padding: '5px 17px', borderRadius: 7, fontSize: 11.5, fontWeight: 700, cursor: 'pointer', border: 'none', background: 'var(--accent)', color: '#fff' }}
          >
            Ок
          </button>
        </div>
      </div>
    </div>
  );
}

/** Оверлей рисования: контекстная панель свойств + сайдбар инструментов (слева) +
 *  панель слоёв. Рендерить внутри контейнера графика (position:relative). */
export function DrawToolsOverlay({ draw, visible }: { draw: DrawTools; visible: boolean }): ReactNode {
  if (!visible || !draw.drawMode) return null;
  return (
    <>
      {/* п.1-3 ТЗ: панель свойств — НЕ постоянная, а контекстная (только когда
          выбрана фигура), с якорем над этой фигурой и перетаскиванием за ⋮⋮. */}
      <DrawStylePanel draw={draw} />
      <div
        data-export-ignore="true"
        // draw-scroll: в невысоких панелях песочницы рейл инструментов не влезает
        // и включает overflowY → нативный жирный белый скроллбар поперёк иконок.
        // Класс делает его тонким и тема-независимым (см. index.css).
        className="draw-scroll"
        style={{
          position: 'absolute', left: 6, top: '50%', transform: 'translateY(-50%)', zIndex: 8,
          display: 'flex', flexDirection: 'column', gap: 3, padding: 4, borderRadius: 10,
          background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 88%, transparent)',
          border: '1px solid var(--border-color, rgba(128,128,128,0.35))', backdropFilter: 'blur(3px)',
          boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
          maxHeight: 'calc(100% - 16px)', overflowY: 'auto',
        }}
      >
        <button
          type="button"
          title="Выйти из режима рисования"
          aria-label="Выйти из режима рисования"
          onClick={() => { draw.setDrawMode(false); draw.setSelectedDrawId(null); }}
          style={drawToolBtn(false)}
        >
          <XIcon size={16} />
        </button>
        <div style={{ height: 1, background: 'var(--border-color, rgba(128,128,128,0.3))', margin: '2px 3px' }} />
        {DRAW_TOOLS.map((t) => (
          <button
            key={t.id}
            type="button"
            title={t.title + (DRAW_HOTKEY[t.id] ? ` (${DRAW_HOTKEY[t.id]})` : '')}
            aria-label={t.title}
            onClick={() => draw.setDrawTool(t.id)}
            style={drawToolBtn(draw.drawTool === t.id)}
          >
            <t.Icon size={16} style={t.rot ? { transform: `rotate(${t.rot}deg)` } : undefined} />
          </button>
        ))}
        <div style={{ height: 1, background: 'var(--border-color, rgba(128,128,128,0.3))', margin: '2px 3px' }} />
        <button type="button" title={`Магнит: ${draw.drawMagnet === 'off' ? 'выкл' : draw.drawMagnet === 'weak' ? 'слабый (рядом с OHLC)' : 'сильный (всегда к OHLC)'} — клик для смены`} aria-label="Магнит" onClick={() => draw.setDrawMagnet((m) => (m === 'off' ? 'weak' : m === 'weak' ? 'strong' : 'off'))} style={{ ...drawToolBtn(draw.drawMagnet !== 'off'), position: 'relative' }}>
          {draw.drawMagnet === 'strong' && <span style={{ position: 'absolute', top: 3, right: 4, width: 5, height: 5, borderRadius: '50%', background: 'var(--accent)' }} />}
          <Magnet size={16} />
        </button>
        <button type="button" title={draw.drawHidden ? 'Показать рисунки' : 'Скрыть рисунки'} aria-label="Скрыть рисунки" onClick={() => draw.setDrawHidden((v) => !v)} style={drawToolBtn(draw.drawHidden)}>
          {draw.drawHidden ? <EyeOff size={16} /> : <Eye size={16} />}
        </button>
        <button type="button" title={draw.drawLocked ? 'Разблокировать рисунки' : 'Заблокировать (запрет перемещения)'} aria-label="Замок" onClick={() => draw.setDrawLocked((v) => !v)} style={drawToolBtn(draw.drawLocked)}>
          {draw.drawLocked ? <Lock size={16} /> : <LockOpen size={16} />}
        </button>
        <button type="button" title={draw.drawKeep ? 'Один-за-раз (по умолчанию)' : 'Остаться в режиме рисования (рисовать подряд)'} aria-label="Остаться в режиме" onClick={() => draw.setDrawKeep((v) => !v)} style={drawToolBtn(draw.drawKeep)}>
          <Repeat size={16} />
        </button>
        <button type="button" title="Слои (список фигур)" aria-label="Слои" onClick={() => draw.setLayersOpen((v) => !v)} style={drawToolBtn(draw.layersOpen)}>
          <Layers size={16} />
        </button>
        <div style={{ height: 1, background: 'var(--border-color, rgba(128,128,128,0.3))', margin: '2px 3px' }} />
        <button
          type="button"
          title={draw.selectedDrawId ? 'Удалить выделенное' : 'Очистить всё'}
          aria-label="Удалить"
          onClick={() => {
            if (draw.selectedDrawId) { draw.setDrawings((ds) => ds.filter((x) => x.id !== draw.selectedDrawId)); draw.setSelectedDrawId(null); }
            else if (draw.drawings.length && window.confirm('Удалить все рисунки?')) draw.setDrawings([]);
          }}
          style={drawToolBtn(false)}
        >
          <Trash2 size={16} />
        </button>
      </div>
      {draw.layersOpen && (
        <div
          data-export-ignore="true"
          className="draw-scroll"
          style={{
            position: 'absolute', left: 48, top: '50%', transform: 'translateY(-50%)', zIndex: 9,
            width: 194, maxHeight: 'calc(100% - 20px)', overflowY: 'auto', padding: 6, borderRadius: 10,
            background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 94%, transparent)',
            border: '1px solid var(--border-color, rgba(128,128,128,0.35))', backdropFilter: 'blur(3px)',
            boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4, padding: '0 2px' }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-primary)' }}>Слои ({draw.drawings.length})</span>
            <button type="button" onClick={() => draw.setLayersOpen(false)} style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'var(--text-secondary)', padding: 2 }}><XIcon size={13} /></button>
          </div>
          {draw.drawings.length === 0 && <div style={{ fontSize: 10.5, color: 'var(--text-secondary)', padding: '4px 2px' }}>Нет фигур</div>}
          {[...draw.drawings].reverse().map((it) => (
            <div
              key={it.id}
              draggable
              onDragStart={() => draw.setDragLayerId(it.id)}
              onDragOver={(e) => e.preventDefault()}
              onDrop={() => { if (draw.dragLayerId) draw.reorderLayer(draw.dragLayerId, it.id); draw.setDragLayerId(null); }}
              onDragEnd={() => draw.setDragLayerId(null)}
              onClick={() => draw.setSelectedDrawId(it.id)}
              style={{
                display: 'flex', alignItems: 'center', gap: 5, padding: '4px 4px', borderRadius: 6, cursor: 'pointer',
                background: draw.selectedDrawId === it.id ? 'color-mix(in srgb, var(--accent) 14%, transparent)' : 'transparent',
                opacity: draw.dragLayerId === it.id ? 0.5 : 1,
              }}
            >
              <GripVertical size={12} style={{ color: 'var(--text-muted, #888)', cursor: 'grab', flexShrink: 0 }} />
              <span style={{ width: 9, height: 9, borderRadius: 2, background: it.color, flexShrink: 0 }} />
              <span style={{ flex: 1, fontSize: 10.5, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', color: it.hidden ? 'var(--text-muted, #888)' : 'var(--text-primary)' }}>
                {DRAW_TOOL_NAME[it.tool] || it.tool}{it.tool === 'text' && it.text ? `: ${it.text}` : ''}
              </span>
              <button type="button" title={it.hidden ? 'Показать' : 'Скрыть'} onClick={(e) => { e.stopPropagation(); draw.setDrawings((ds) => ds.map((x) => (x.id === it.id ? { ...x, hidden: !x.hidden } : x))); }} style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'var(--text-secondary)', padding: 2, flexShrink: 0 }}>{it.hidden ? <EyeOff size={13} /> : <Eye size={13} />}</button>
              <button type="button" title="Удалить" onClick={(e) => { e.stopPropagation(); draw.setDrawings((ds) => ds.filter((x) => x.id !== it.id)); if (draw.selectedDrawId === it.id) draw.setSelectedDrawId(null); }} style={{ border: 'none', background: 'transparent', cursor: 'pointer', color: 'var(--text-secondary)', padding: 2, flexShrink: 0 }}><Trash2 size={13} /></button>
            </div>
          ))}
        </div>
      )}
      <DrawSettingsModal draw={draw} />
    </>
  );
}

/** Модалка экспорта, привязанная к контейнеру графика + LwChart-хэндлу
 *  (форс-синк размера/фигур перед снятием скриншота — см. LwChart.syncBeforeCapture). */
export function ChartExportModal({
  draw, targetElement, lwChartRef, filename, metadata,
}: {
  draw: DrawTools;
  targetElement: HTMLElement | null;
  /** Любой чарт-хэндл с syncBeforeCapture (LwChartHandle или LwChartPanesHandle —
   *  структурно совместимы, конкретный тип не важен). restoreAfterCapture —
   *  парный откат временного увеличения font-size (см. syncBeforeCapture в
   *  LwChart.tsx) после снятия скриншота. */
  lwChartRef: React.RefObject<{
    syncBeforeCapture: (w: number, h: number) => void;
    restoreAfterCapture?: () => void;
  } | null>;
  filename: string;
  metadata?: ExportMetadata;
}): ReactNode {
  if (!draw.exportOpen || !targetElement) return null;
  return (
    <Suspense fallback={null}>
      <ExportModal
        targetElement={targetElement}
        filename={filename}
        metadata={metadata}
        beforeCapture={() => {
          const r = targetElement.getBoundingClientRect();
          lwChartRef.current?.syncBeforeCapture(r.width, r.height);
        }}
        afterCapture={() => lwChartRef.current?.restoreAfterCapture?.()}
        onClose={() => draw.setExportOpen(false)}
      />
    </Suspense>
  );
}
