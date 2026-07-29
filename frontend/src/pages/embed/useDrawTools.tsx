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
  useCallback, useEffect, useRef, useState, Suspense, lazy,
  type CSSProperties, type ReactNode,
} from 'react';
import {
  Pencil, Camera, MousePointer2, TrendingUp, Minus, Square, Type, Trash2,
  MoveUpRight, ArrowUpRight, Brush, Circle, AlignJustify, Magnet, Eye, EyeOff, Lock, LockOpen,
  Ruler, Layers, X as XIcon, GripVertical, Repeat,
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
const DRAW_COLORS = ['#FF5C2B', '#5DA3E9', '#5BD49C', '#EF6F6F', '#E0A34E', '#F5F1E8'];

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
  curColor: string;
  curWidth: number;
  curDash: LwDash;
  curOpacity: number;
  applyStyle: (patch: Partial<Pick<LwDrawing, 'color' | 'width' | 'dash' | 'opacity'>>) => void;
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
  const curColor = selectedDraw?.color ?? drawColor;
  const curWidth = selectedDraw?.width ?? drawWidth;
  const curDash: LwDash = selectedDraw?.dash ?? drawDash;
  const curOpacity = selectedDraw?.opacity ?? drawOpacity;
  const applyStyle = (patch: Partial<Pick<LwDrawing, 'color' | 'width' | 'dash' | 'opacity'>>) => {
    if (patch.color !== undefined) setDrawColor(patch.color);
    if (patch.width !== undefined) setDrawWidth(patch.width);
    if (patch.dash !== undefined) setDrawDash(patch.dash);
    if (patch.opacity !== undefined) setDrawOpacity(patch.opacity);
    if (selectedDrawId) setDrawings((ds) => ds.map((d) => (d.id === selectedDrawId ? { ...d, ...patch } : d)));
  };

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
    dragLayerId, setDragLayerId, reorderLayer, curColor, curWidth, curDash, curOpacity, applyStyle,
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

/** Оверлей рисования: тулбар свойств (сверху) + сайдбар инструментов (слева) +
 *  панель слоёв. Рендерить внутри контейнера графика (position:relative). */
export function DrawToolsOverlay({ draw, visible }: { draw: DrawTools; visible: boolean }): ReactNode {
  if (!visible || !draw.drawMode) return null;
  return (
    <>
      <div
        data-export-ignore="true"
        style={{
          position: 'absolute', top: 6, left: '50%', transform: 'translateX(-50%)', zIndex: 9,
          display: 'flex', alignItems: 'center', gap: 3, padding: '4px 7px', borderRadius: 9,
          maxWidth: 'calc(100% - 90px)', flexWrap: 'wrap', justifyContent: 'center',
          background: 'color-mix(in srgb, var(--bg-secondary, #17161A) 92%, transparent)',
          border: '1px solid var(--border-color, rgba(128,128,128,0.35))', backdropFilter: 'blur(3px)',
          boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
        }}
      >
        {(['solid', 'dashed', 'dotted'] as LwDash[]).map((id) => (
          <button key={id} type="button" title={id === 'solid' ? 'Сплошная' : id === 'dashed' ? 'Штриховой пунктир' : 'Точечный пунктир'} onClick={() => draw.applyStyle({ dash: id })} style={drawToolBtn(draw.curDash === id)}>
            <svg width={18} height={12} style={{ display: 'block' }}><line x1={1} y1={6} x2={17} y2={6} stroke="currentColor" strokeWidth={2} strokeDasharray={id === 'solid' ? undefined : id === 'dashed' ? '4 3' : '0.5 3'} strokeLinecap={id === 'dotted' ? 'round' : 'butt'} /></svg>
          </button>
        ))}
        <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
        {[1, 2, 3, 4].map((wv) => (
          <button key={wv} type="button" title={`${wv}px`} onClick={() => draw.applyStyle({ width: wv })} style={{ ...drawToolBtn(draw.curWidth === wv), width: 24, fontSize: 11, fontWeight: 700 }}>{wv}</button>
        ))}
        <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
        {DRAW_COLORS.map((c) => (
          <button key={c} type="button" title="Цвет" onClick={() => draw.applyStyle({ color: c })} style={{ width: 20, height: 20, borderRadius: 5, cursor: 'pointer', padding: 0, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', border: draw.curColor === c ? '2px solid var(--text-primary)' : '1px solid transparent' }}>
            <span style={{ width: 12, height: 12, borderRadius: '50%', background: c, display: 'inline-block' }} />
          </button>
        ))}
        <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
        <input type="range" min={10} max={100} value={Math.round(draw.curOpacity * 100)} title="Прозрачность" onChange={(e) => draw.applyStyle({ opacity: Number(e.target.value) / 100 })} style={{ width: 66, accentColor: 'var(--accent)' }} />
        <span style={{ fontSize: 10, color: 'var(--text-secondary)', minWidth: 30, textAlign: 'right' }}>{Math.round(draw.curOpacity * 100)}%</span>
        {draw.selectedDrawId && (
          <>
            <div style={{ width: 1, height: 18, background: 'var(--border-color,rgba(128,128,128,0.3))', margin: '0 2px' }} />
            <button type="button" title="Удалить выделенное" onClick={() => { draw.setDrawings((ds) => ds.filter((x) => x.id !== draw.selectedDrawId)); draw.setSelectedDrawId(null); }} style={drawToolBtn(false)}><Trash2 size={15} /></button>
          </>
        )}
      </div>
      <div
        data-export-ignore="true"
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
   *  структурно совместимы, конкретный тип не важен). */
  lwChartRef: React.RefObject<{ syncBeforeCapture: (w: number, h: number) => void } | null>;
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
        onClose={() => draw.setExportOpen(false)}
      />
    </Suspense>
  );
}
