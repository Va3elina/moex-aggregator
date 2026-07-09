/**
 * SandboxPage — приватная «песочница/конструктор» (роут /sandbox, вне навигации).
 *
 * Про-версия: пользователь вытаскивает индикаторы как плавающие окна-панели,
 * раскладывает по ЛИСТАМ (рабочим пространствам), двигает/масштабирует с магнитом.
 * Аналог рабочего стола терминала (TradingView / Т-Инвестиции), но со своими
 * индикаторами и в фирменном editorial-дизайне.
 *
 * ЭТО ПОРТ оболочки дизайнерского мокапа 1:1 (спека ~/Downloads/Песочница-Фрейм-
 * спецификация.md; исходник декодирован в scratchpad). Модель §2, оболочка §3,
 * окно-панель §4. Токены песочницы — СВОИ (sandbox.css, терминальный язык:
 * тонкий бордер + мягкая тень), НЕ сайтовые editorial (жёсткие кремовые бордеры).
 *
 * КОНТЕНТ панели = наш реальный embed-компонент (реальные данные через сессию
 * юзера, авторизация, тариф, методология — переиспользуем, не мокаем). «Единая
 * шапка» §4.1: у панели нет своего заголовка — шапкой-хватом служит тулбар embed'а,
 * кнопки окна (⤢/×) EmbedFrame дорисовывает через SandboxWindowCtx. Вложенный
 * <Router> НЕЛЬЗЯ (Router-в-Router крашит) — embed рендерится напрямую.
 *
 * Раскладка привязана к листу (bySheet). При смене листа панели неактивного листа
 * ДЕМОНТИРУЮТСЯ React'ом → LwChart внутри embed'а сам зовёт chart.remove() в
 * cleanup (иначе течёт память, §2). Персист в localStorage (v2).
 *
 * TODO (следующие куски порта): ◐ тема панели (нужна прокидка темы в embed),
 * ⚙ общий Формат, per-panel cfg, клик по сигналу → spawn индикатора, общие
 * настройки §9, листы (переименование/дубль/удаление/reorder).
 */
import { useCallback, useEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { Bell, Contrast, LayoutGrid, Plus, PlusCircle, X as XIcon } from 'lucide-react';
import './sandbox.css';
import { SandboxWindowCtx } from '../embed/EmbedToolbar';
import EmbedOpenInterest from '../embed/EmbedOpenInterest';
import EmbedSeasonality from '../embed/EmbedSeasonality';
import EmbedScreener from '../embed/EmbedScreener';
import EmbedBuffett from '../embed/EmbedBuffett';
import EmbedStrength from '../embed/EmbedStrength';
import EmbedFundsMoney from '../embed/EmbedFundsMoney';
import EmbedFundTrades from '../embed/EmbedFundTrades';
import EmbedCbrFlows from '../embed/EmbedCbrFlows';
import EmbedSignals from '../embed/EmbedSignals';

/* ───────────────────────── реестр индикаторов ───────────────────────── */
type IndKind =
  | 'signals' | 'oi' | 'seasonality' | 'screener'
  | 'buffett' | 'strength' | 'funds-money' | 'fund-trades' | 'fund-movers' | 'cbr-flows';
type Group = 'signals' | 'instrument' | 'market';
interface IndicatorDef { type: IndKind; label: string; group: Group; desc: string }

const INDICATORS: IndicatorDef[] = [
  { type: 'signals', label: '🔔 Сигналы', group: 'signals', desc: 'Лента резких движений рынка' },
  { type: 'oi', label: 'Открытые позиции', group: 'instrument', desc: 'Открытый интерес по фьючерсам' },
  { type: 'seasonality', label: 'Сезонность', group: 'instrument', desc: 'Средняя доходность по сезонам' },
  { type: 'screener', label: 'Скринер сигналов', group: 'instrument', desc: 'Резкие сдвиги позиций по всем активам' },
  { type: 'buffett', label: 'Индикатор Баффетта', group: 'market', desc: 'Капитализация рынка к ВВП / M2' },
  { type: 'strength', label: 'Сила рынка', group: 'market', desc: 'Ширина рынка: % акций выше средней' },
  { type: 'funds-money', label: 'Деньги в фондах', group: 'market', desc: 'Притоки и оттоки биржевых фондов' },
  { type: 'fund-trades', label: 'Сделки фондов', group: 'market', desc: 'Что покупают и продают фонды' },
  { type: 'fund-movers', label: 'Покупки фондов', group: 'market', desc: 'Кто набирает и сокращает позиции' },
  { type: 'cbr-flows', label: 'Поток капитала', group: 'market', desc: 'Потоки по участникам биржи (ЦБ)' },
];

// Стартовый размер панели по индикатору (спека: screener 440×470, strength 560×420, прочие 520×360).
const SIZES: Partial<Record<IndKind, { w: number; h: number }>> = {
  signals: { w: 380, h: 560 },
  screener: { w: 440, h: 470 },
  strength: { w: 560, h: 420 },
  oi: { w: 640, h: 440 },
  seasonality: { w: 600, h: 440 },
};
const DEFAULT_SIZE = { w: 520, h: 360 };

// type → embed-компонент (наш реальный индикатор).
function renderIndicator(type: IndKind): ReactNode {
  switch (type) {
    case 'signals': return <EmbedSignals />;
    case 'oi': return <EmbedOpenInterest />;
    case 'seasonality': return <EmbedSeasonality />;
    case 'screener': return <EmbedScreener />;
    case 'buffett': return <EmbedBuffett />;
    case 'strength': return <EmbedStrength />;
    case 'funds-money': return <EmbedFundsMoney />;
    case 'fund-trades': return <EmbedFundTrades />;
    case 'fund-movers': return <EmbedFundTrades lockTab="movers" />;
    case 'cbr-flows': return <EmbedCbrFlows />;
    default: return null;
  }
}

/* ───────────────────────── модель состояния (§2) ───────────────────────── */
type SbTheme = 'dark' | 'light';
interface Sheet { id: string; name: string }
interface Panel { id: string; type: IndKind; x: number; y: number; w: number; h: number; z: number; themeOverride: SbTheme | null }
interface Persisted { sbTheme: SbTheme; sheets: Sheet[]; activeSheet: string; bySheet: Record<string, Panel[]> }
interface Guide { axis: 'v' | 'h'; pos: number }

const LS_KEY = 'frame:sandbox:v2';
const TOPBAR_H = 56;
const DRAG_STRIP = 44;       // «шапка-хват» — верхняя полоса панели (= тулбар embed'а)
const SNAP_DRAG = 18;        // порог магнита при перетаскивании (§4.1)
const SNAP_RESIZE = 16;      // порог магнита при ресайзе (§4.2)
const MINW = 300, MINH = 200;
const GUIDE_Z = 99990;       // направляющие — над панелями, под оверлеями
const OVERLAY_Z = 100000;    // меню/поповеры/дровер (§7: панели при захвате уходят на высокий z)

const uid = (p: string): string => p + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

function defaultState(): Persisted {
  return {
    sbTheme: 'dark',
    sheets: [{ id: 's1', name: 'Лист 1' }],
    activeSheet: 's1',
    bySheet: { s1: [{ id: 'pseed', type: 'oi', x: 28, y: 20, w: 640, h: 440, z: 11, themeOverride: null }] },
  };
}
function loadState(): Persisted {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return defaultState();
    const s = JSON.parse(raw) as Persisted;
    if (!s || !Array.isArray(s.sheets) || !s.sheets.length || !s.bySheet) return defaultState();
    if (!s.bySheet[s.activeSheet]) s.activeSheet = s.sheets[0].id;
    if (s.sbTheme !== 'light') s.sbTheme = 'dark';
    return s;
  } catch { return defaultState(); }
}

/* ───────────────────────── ресайз-хваты (§4.2) ─────────────────────────
   Стороны + нижние углы. Верхний край занят шапкой-хватом (тулбар embed'а) —
   ресайз оттуда конфликтует с кнопками, поэтому n/nw/ne опущены (top-resize
   = следующий кусок с прокидкой z в EmbedFrame). z:2 — выше графика, ниже
   тулбара (z:3 в EmbedFrame): хваты ловят край над графиком, кнопки кликаются. */
const HANDLES: { dir: string; style: CSSProperties }[] = [
  { dir: 'e', style: { top: 14, bottom: 14, right: 0, width: 6, cursor: 'ew-resize' } },
  { dir: 'w', style: { top: 14, bottom: 14, left: 0, width: 6, cursor: 'ew-resize' } },
  { dir: 's', style: { left: 14, right: 14, bottom: 0, height: 6, cursor: 'ns-resize' } },
  { dir: 'se', style: { right: 0, bottom: 0, width: 14, height: 14, cursor: 'nwse-resize' } },
  { dir: 'sw', style: { left: 0, bottom: 0, width: 14, height: 14, cursor: 'nesw-resize' } },
];

export default function SandboxPage() {
  const [st, setSt] = useState<Persisted>(loadState);
  const [menuOpen, setMenuOpen] = useState(false);
  const [signalsOpen, setSignalsOpen] = useState(false);
  const [guides, setGuides] = useState<Guide[]>([]);
  const zTop = useRef(10);
  const restoreRef = useRef<Record<string, { x: number; y: number; w: number; h: number }>>({});
  const canvasRef = useRef<HTMLDivElement>(null);
  const saveTimer = useRef<number | undefined>(undefined);

  const panels = st.bySheet[st.activeSheet] || [];

  // init zTop из загруженной раскладки
  useEffect(() => {
    let m = 10;
    for (const arr of Object.values(st.bySheet)) for (const p of arr) m = Math.max(m, p.z);
    zTop.current = m;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // полноэкранно, без скролла страницы
  useEffect(() => {
    const de = document.documentElement, b = document.body;
    const prev = { de: de.style.overflow, bo: b.style.overflow, bm: b.style.margin };
    de.style.overflow = 'hidden'; b.style.overflow = 'hidden'; b.style.margin = '0';
    return () => { de.style.overflow = prev.de; b.style.overflow = prev.bo; b.style.margin = prev.bm; };
  }, []);

  // персист (дебаунс 400мс — драг/ресайз дёргают state часто)
  useEffect(() => {
    window.clearTimeout(saveTimer.current);
    saveTimer.current = window.setTimeout(() => {
      try { localStorage.setItem(LS_KEY, JSON.stringify(st)); } catch { /* quota */ }
    }, 400);
    return () => window.clearTimeout(saveTimer.current);
  }, [st]);

  // обновить массив панелей АКТИВНОГО листа
  const setActivePanels = useCallback((updater: (prev: Panel[]) => Panel[]) => {
    setSt((s) => ({ ...s, bySheet: { ...s.bySheet, [s.activeSheet]: updater(s.bySheet[s.activeSheet] || []) } }));
  }, []);

  const bringFront = useCallback((id: string) => {
    zTop.current += 1; const z = zTop.current;
    setActivePanels((ps) => ps.map((p) => (p.id === id ? { ...p, z } : p)));
  }, [setActivePanels]);

  const spawn = useCallback((type: IndKind) => {
    const sz = SIZES[type] || DEFAULT_SIZE;
    const rc = canvasRef.current?.getBoundingClientRect();
    const cw = rc?.width ?? window.innerWidth, ch = rc?.height ?? (window.innerHeight - TOPBAR_H);
    zTop.current += 1; const z = zTop.current;
    setActivePanels((ps) => {
      const n = ps.length;
      const x = Math.max(8, Math.min(cw - sz.w - 8, 40 + ((n * 28) % 220)));
      const y = Math.max(8, Math.min(ch - sz.h - 8, 24 + ((n * 28) % 180)));
      return [...ps, { id: uid('p'), type, x, y, w: sz.w, h: sz.h, z, themeOverride: null }];
    });
    setMenuOpen(false);
  }, [setActivePanels]);

  const close = useCallback((id: string) => setActivePanels((ps) => ps.filter((p) => p.id !== id)), [setActivePanels]);

  // ⤢ развернуть / восстановить (§4.3). Прежние габариты — в ref (транзиентно).
  const expand = useCallback((id: string) => {
    const rc = canvasRef.current?.getBoundingClientRect();
    const cw = rc?.width ?? window.innerWidth, ch = rc?.height ?? (window.innerHeight - TOPBAR_H);
    zTop.current += 1; const z = zTop.current;
    setActivePanels((ps) => ps.map((p) => {
      if (p.id !== id) return p;
      const saved = restoreRef.current[id];
      if (saved) { delete restoreRef.current[id]; return { ...p, ...saved, z }; }
      restoreRef.current[id] = { x: p.x, y: p.y, w: p.w, h: p.h };
      const w = Math.min(880, cw - 24), h = Math.min(600, ch - 24);
      return { ...p, x: Math.max(12, Math.round((cw - w) / 2)), y: 12, w, h, z };
    }));
  }, [setActivePanels]);

  // «Выстроить» §4.4: 1–2 → ряд, 3–6 → 2 колонки, 7+ → 3; отступ 28, зазор 20.
  const arrange = useCallback(() => {
    const rc = canvasRef.current?.getBoundingClientRect(); if (!rc) return;
    setActivePanels((ps) => {
      const n = ps.length; if (!n) return ps;
      const cols = n <= 2 ? n : n <= 6 ? 2 : 3;
      const rows = Math.ceil(n / cols);
      const M = 28, G = 20;
      const w = Math.floor((rc.width - M * 2 - G * (cols - 1)) / cols);
      const h = Math.floor((rc.height - M * 2 - G * (rows - 1)) / rows);
      return ps.map((p, i) => ({ ...p, x: M + (i % cols) * (w + G), y: M + Math.floor(i / cols) * (h + G), w, h }));
    });
  }, [setActivePanels]);

  // ── драг + магнит(18) + направляющие (§4.1) ──
  const onDragStart = useCallback((e: React.PointerEvent, id: string) => {
    bringFront(id);
    const el = e.currentTarget as HTMLElement;
    const rect = el.getBoundingClientRect();
    if (e.clientY - rect.top > DRAG_STRIP) return; // тянем только за верхнюю полосу
    if ((e.target as HTMLElement).closest('button, input, a, select, textarea, [role="dialog"]')) return;
    e.preventDefault();
    const list = st.bySheet[st.activeSheet] || [];
    const start = list.find((p) => p.id === id); if (!start) return;
    const others = list.filter((p) => p.id !== id);
    const cr = canvasRef.current!.getBoundingClientRect();
    const sx = e.clientX, sy = e.clientY, ox = start.x, oy = start.y;
    try { el.setPointerCapture(e.pointerId); } catch { /* noop */ }
    const move = (ev: PointerEvent) => {
      let nx = ox + (ev.clientX - sx), ny = oy + (ev.clientY - sy);
      const gs: Guide[] = [];
      const xc = [0, cr.width - start.w]; others.forEach((q) => xc.push(q.x, q.x + q.w - start.w, q.x + q.w, q.x - start.w));
      for (const c of xc) { if (Math.abs(nx - c) < SNAP_DRAG) { nx = c; gs.push({ axis: 'v', pos: nx }, { axis: 'v', pos: nx + start.w }); break; } }
      const yc = [0, cr.height - start.h]; others.forEach((q) => yc.push(q.y, q.y + q.h - start.h, q.y + q.h, q.y - start.h));
      for (const c of yc) { if (Math.abs(ny - c) < SNAP_DRAG) { ny = c; gs.push({ axis: 'h', pos: ny }, { axis: 'h', pos: ny + start.h }); break; } }
      nx = Math.max(0, Math.min(cr.width - start.w, nx));
      ny = Math.max(0, Math.min(cr.height - start.h, ny));
      setGuides(gs);
      setActivePanels((ps) => ps.map((p) => (p.id === id ? { ...p, x: nx, y: ny } : p)));
    };
    const up = () => {
      el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up);
      setGuides([]);
    };
    el.addEventListener('pointermove', move); el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
  }, [st, bringFront, setActivePanels]);

  // ── ресайз (§4.2): стороны + нижние углы, магнит(16) кромки к соседям/краям ──
  const onResizeStart = useCallback((e: React.PointerEvent, id: string, dir: string) => {
    e.stopPropagation(); e.preventDefault();
    bringFront(id);
    const list = st.bySheet[st.activeSheet] || [];
    const start = list.find((p) => p.id === id); if (!start) return;
    const others = list.filter((p) => p.id !== id);
    const cr = canvasRef.current!.getBoundingClientRect();
    const sx = e.clientX, sy = e.clientY; const o = { x: start.x, y: start.y, w: start.w, h: start.h };
    const el = e.currentTarget as HTMLElement;
    try { el.setPointerCapture(e.pointerId); } catch { /* noop */ }
    const edgesX = others.flatMap((q) => [q.x, q.x + q.w]);
    const edgesY = others.flatMap((q) => [q.y, q.y + q.h]);
    const move = (ev: PointerEvent) => {
      const dx = ev.clientX - sx, dy = ev.clientY - sy;
      let x = o.x, y = o.y, w = o.w, h = o.h; const gs: Guide[] = [];
      if (dir.includes('e')) { w = o.w + dx; const R = x + w; for (const c of [cr.width, ...edgesX]) { if (Math.abs(R - c) < SNAP_RESIZE) { w = c - x; gs.push({ axis: 'v', pos: c }); break; } } }
      if (dir.includes('s')) { h = o.h + dy; const B = y + h; for (const c of [cr.height, ...edgesY]) { if (Math.abs(B - c) < SNAP_RESIZE) { h = c - y; gs.push({ axis: 'h', pos: c }); break; } } }
      if (dir.includes('w')) { const R = o.x + o.w; let nx = o.x + dx; for (const c of [0, ...edgesX]) { if (Math.abs(nx - c) < SNAP_RESIZE) { nx = c; gs.push({ axis: 'v', pos: c }); break; } } x = nx; w = R - nx; }
      if (dir.includes('n')) { const B = o.y + o.h; let ny = o.y + dy; for (const c of [0, ...edgesY]) { if (Math.abs(ny - c) < SNAP_RESIZE) { ny = c; gs.push({ axis: 'h', pos: c }); break; } } y = ny; h = B - ny; }
      if (w < MINW) { if (dir.includes('w')) x = o.x + o.w - MINW; w = MINW; }
      if (h < MINH) { if (dir.includes('n')) y = o.y + o.h - MINH; h = MINH; }
      if (x < 0) { w += x; x = 0; } if (y < 0) { h += y; y = 0; }
      if (x + w > cr.width) w = cr.width - x; if (y + h > cr.height) h = cr.height - y;
      w = Math.max(MINW, w); h = Math.max(MINH, h);
      setGuides(gs);
      setActivePanels((ps) => ps.map((p) => (p.id === id ? { ...p, x, y, w, h } : p)));
    };
    const up = () => {
      el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up);
      setGuides([]);
    };
    el.addEventListener('pointermove', move); el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
  }, [st, bringFront, setActivePanels]);

  const toggleTheme = useCallback(() => setSt((s) => ({ ...s, sbTheme: s.sbTheme === 'dark' ? 'light' : 'dark' })), []);
  const addSheet = useCallback(() => setSt((s) => {
    const id = uid('s');
    return { ...s, sheets: [...s.sheets, { id, name: 'Лист ' + (s.sheets.length + 1) }], bySheet: { ...s.bySheet, [id]: [] }, activeSheet: id };
  }), []);
  const pickSheet = useCallback((id: string) => { setGuides([]); setMenuOpen(false); setSt((s) => ({ ...s, activeSheet: id })); }, []);

  return (
    <div className="sb-root" data-sbtheme={st.sbTheme} style={rootStyle}>
      {/* ── Топбар 56px (§3.1) ── */}
      <div style={topbarStyle}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 9, flexShrink: 0 }}>
          <div style={logoSquare}>Ф</div>
          <span style={{ fontWeight: 700, fontSize: 15, letterSpacing: '-0.01em', color: 'var(--text)' }}>Фрейм</span>
        </div>

        <div style={dividerV} />

        {/* Вкладки листов + новый лист */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }}>
          {st.sheets.map((sh) => {
            const on = sh.id === st.activeSheet;
            return (
              <button key={sh.id} type="button" onClick={() => pickSheet(sh.id)} style={sheetTabStyle(on)}>
                {sh.name}
                {on && <span style={sheetUnderline} />}
              </button>
            );
          })}
          <button type="button" onClick={addSheet} title="Новый лист" style={newSheetBtn}><Plus size={15} /></button>
        </div>

        <div style={dividerV} />

        {/* ＋ Индикатор */}
        <div style={{ position: 'relative', flexShrink: 0 }}>
          <button type="button" onClick={() => setMenuOpen((v) => !v)} style={addBtnStyle}>
            <PlusCircle size={15} /> Индикатор
          </button>
        </div>

        <button type="button" onClick={arrange} disabled={!panels.length} title="Разложить по сетке" style={{ ...arrangeBtnStyle, opacity: panels.length ? 1 : 0.4 }}>
          <LayoutGrid size={14} /> Выстроить
        </button>

        {/* Правая группа */}
        <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 10, flexShrink: 0 }}>
          <button type="button" className="sb-hoverable" onClick={() => setSignalsOpen(true)} title="Центр сигналов" style={chromeBtn}>
            <Bell size={17} strokeWidth={1.8} />
          </button>
          <button type="button" className="sb-hoverable" onClick={toggleTheme} title="Тема оболочки" style={chromeBtn}>
            <Contrast size={16} />
          </button>
          <span style={proChip}>PRO</span>
          <div style={avatarStyle}>ВД</div>
        </div>
      </div>

      {/* ── Холст (§3.2) ── */}
      <div ref={canvasRef} style={canvasStyle}>
        <div style={dotGridStyle} />

        {panels.length === 0 && (
          <div style={emptyStyle}>
            <LayoutGrid size={30} style={{ opacity: 0.35 }} />
            <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--text)' }}>Пустой лист</div>
            <div style={{ fontSize: 12.5, color: 'var(--muted)', maxWidth: 320, textAlign: 'center', lineHeight: 1.5 }}>
              Добавьте индикатор — он появится как окно-панель. Панели можно двигать, тянуть за края и прилеплять друг к другу.
            </div>
            <button type="button" onClick={() => setMenuOpen(true)} style={{ ...addBtnStyle, marginTop: 4 }}>
              <PlusCircle size={15} /> Добавить индикатор
            </button>
          </div>
        )}

        {panels.map((p) => (
          <div
            key={p.id}
            className="sb-panel"
            data-sbtheme={p.themeOverride || st.sbTheme}
            style={{ position: 'absolute', left: p.x, top: p.y, width: p.w, height: p.h, zIndex: p.z, ...panelStyle }}
            onPointerDown={(e) => onDragStart(e, p.id)}
          >
            <div style={panelBodyStyle}>
              <SandboxWindowCtx.Provider value={{ onExpand: () => expand(p.id), onClose: () => close(p.id) }}>
                {renderIndicator(p.type)}
              </SandboxWindowCtx.Provider>
            </div>
            {HANDLES.map((hd) => (
              <div key={hd.dir} onPointerDown={(e) => onResizeStart(e, p.id, hd.dir)} style={{ position: 'absolute', zIndex: 2, ...hd.style }} />
            ))}
          </div>
        ))}

        {/* Направляющие магнита (§4.1) */}
        {guides.map((g, i) => (
          <div key={i} style={g.axis === 'v'
            ? { position: 'absolute', left: g.pos, top: 0, bottom: 0, width: 1, background: 'var(--accent)', opacity: 0.7, pointerEvents: 'none', zIndex: GUIDE_Z }
            : { position: 'absolute', top: g.pos, left: 0, right: 0, height: 1, background: 'var(--accent)', opacity: 0.7, pointerEvents: 'none', zIndex: GUIDE_Z }} />
        ))}
      </div>

      {/* ── Меню «＋ Индикатор» (§7.1) ── */}
      {menuOpen && (
        <>
          <div style={{ position: 'fixed', inset: 0, zIndex: OVERLAY_Z }} onClick={() => setMenuOpen(false)} />
          <div className="sb-scroll" style={addMenuStyle}>
            {(['signals', 'instrument', 'market'] as Group[]).map((g) => (
              <div key={g}>
                <div className="sb-uc" style={{ padding: '8px 10px 4px' }}>
                  {g === 'signals' ? 'Центр сигналов' : g === 'instrument' ? 'По инструменту' : 'По рынку'}
                </div>
                {INDICATORS.filter((i) => i.group === g).map((ind) => (
                  <button key={ind.type} type="button" onClick={() => spawn(ind.type)} style={menuItemStyle}>
                    <span style={{ width: 7, height: 7, borderRadius: 4, background: 'var(--accent)', flex: '0 0 auto', marginTop: 5 }} />
                    <span style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
                      <span style={{ fontWeight: 700, fontSize: 13, color: 'var(--text)' }}>{ind.label}</span>
                      <span style={{ fontSize: 11, color: 'var(--muted)', lineHeight: 1.3 }}>{ind.desc}</span>
                    </span>
                  </button>
                ))}
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── Центр сигналов — выезжающий справа дровер (§6.10 / §7.3) ── */}
      {signalsOpen && (
        <>
          <div style={{ position: 'fixed', inset: 0, zIndex: OVERLAY_Z, background: 'rgba(0,0,0,0.28)', animation: 'sb-fade .15s ease' }} onClick={() => setSignalsOpen(false)} />
          <div style={signalsDrawerStyle}>
            <div style={{ display: 'flex', alignItems: 'center', padding: '10px 12px', borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
              <span style={{ fontWeight: 700, fontSize: 14, color: 'var(--text)' }}>Сигналы и алерты</span>
              <button type="button" className="sb-hoverable" onClick={() => setSignalsOpen(false)} title="Закрыть" style={{ ...chromeBtn, width: 28, height: 28, marginLeft: 'auto', border: 'none' }}>
                <XIcon size={16} />
              </button>
            </div>
            <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
              <EmbedSignals />
            </div>
          </div>
        </>
      )}

      <div style={footNote}>песочница · приватный превью</div>
    </div>
  );
}

/* ───────────────────────────── styles ───────────────────────────── */
const rootStyle: CSSProperties = {
  position: 'fixed', inset: 0, background: 'var(--bg)', color: 'var(--text)', overflow: 'hidden',
};
const topbarStyle: CSSProperties = {
  position: 'absolute', top: 0, left: 0, right: 0, height: TOPBAR_H, display: 'flex', alignItems: 'center', gap: 14,
  padding: '0 16px', borderBottom: '1px solid var(--border)', background: 'var(--panel)', zIndex: 50,
};
const logoSquare: CSSProperties = {
  width: 26, height: 26, borderRadius: 6, background: 'var(--accent)', color: '#fff', fontWeight: 800, fontSize: 15,
  display: 'flex', alignItems: 'center', justifyContent: 'center', flex: '0 0 auto',
};
const dividerV: CSSProperties = { width: 1, height: 22, background: 'var(--border)', flex: '0 0 auto' };
function sheetTabStyle(on: boolean): CSSProperties {
  return {
    position: 'relative', padding: '6px 12px', borderRadius: 7, border: 'none', background: 'transparent', cursor: 'pointer',
    fontSize: 12.5, fontWeight: on ? 700 : 500, color: on ? 'var(--text)' : 'var(--muted)', whiteSpace: 'nowrap',
  };
}
const sheetUnderline: CSSProperties = { position: 'absolute', bottom: -6, left: 12, right: 12, height: 2, background: 'var(--accent)', borderRadius: 1 };
const newSheetBtn: CSSProperties = {
  width: 26, height: 26, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', borderRadius: 7,
  border: '1px dashed var(--border-strong)', background: 'transparent', color: 'var(--muted)', cursor: 'pointer', flex: '0 0 auto', padding: 0,
};
const addBtnStyle: CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6, padding: '7px 13px', borderRadius: 8, border: 'none',
  background: 'var(--accent)', color: '#fff', fontSize: 12.5, fontWeight: 600, cursor: 'pointer', whiteSpace: 'nowrap',
};
const arrangeBtnStyle: CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6, padding: '7px 12px', borderRadius: 8, cursor: 'pointer',
  border: '1.5px solid var(--border)', background: 'transparent', color: 'var(--text)', fontSize: 12.5, fontWeight: 600, whiteSpace: 'nowrap', flex: '0 0 auto',
};
const chromeBtn: CSSProperties = {
  width: 34, height: 34, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', borderRadius: 8,
  border: '1.5px solid var(--border)', background: 'transparent', color: 'var(--muted)', cursor: 'pointer', flex: '0 0 auto', padding: 0,
};
const proChip: CSSProperties = {
  fontSize: 10, fontWeight: 700, letterSpacing: '0.06em', color: 'var(--accent)', border: '1px solid var(--accent)',
  borderRadius: 5, padding: '3px 6px', flex: '0 0 auto',
};
const avatarStyle: CSSProperties = {
  width: 30, height: 30, borderRadius: '50%', background: 'var(--bg2)', border: '1px solid var(--border)',
  display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, fontWeight: 700, color: 'var(--text)', flex: '0 0 auto',
};
const canvasStyle: CSSProperties = { position: 'absolute', top: TOPBAR_H, left: 0, right: 0, bottom: 0, overflow: 'hidden', background: 'var(--bg)' };
const dotGridStyle: CSSProperties = {
  position: 'absolute', inset: 0, pointerEvents: 'none',
  backgroundImage: 'radial-gradient(var(--dot) 1.1px, transparent 1.1px)', backgroundSize: '22px 22px',
};
const emptyStyle: CSSProperties = {
  position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 10,
};
const panelStyle: CSSProperties = {
  display: 'flex', flexDirection: 'column', background: 'var(--panel)', border: '1px solid var(--border)',
  borderRadius: 8, boxShadow: 'var(--shadow)', overflow: 'hidden',
};
const panelBodyStyle: CSSProperties = { flex: '1 1 auto', position: 'relative', minHeight: 0, background: 'var(--bg)' };
const addMenuStyle: CSSProperties = {
  position: 'absolute', top: TOPBAR_H + 4, left: 220, zIndex: OVERLAY_Z + 1, width: 300, maxHeight: 'calc(100vh - 80px)', overflowY: 'auto',
  background: 'var(--panel)', border: '1.5px solid var(--border-strong)', borderRadius: 12, boxShadow: 'var(--shadow)', padding: 6,
  animation: 'sb-pop .15s ease',
};
const menuItemStyle: CSSProperties = {
  display: 'flex', alignItems: 'flex-start', gap: 9, width: '100%', padding: '8px 10px', border: 'none', borderRadius: 8,
  background: 'transparent', color: 'var(--text)', textAlign: 'left', cursor: 'pointer',
};
const signalsDrawerStyle: CSSProperties = {
  position: 'fixed', top: 0, right: 0, bottom: 0, width: 360, zIndex: OVERLAY_Z + 1, display: 'flex', flexDirection: 'column',
  background: 'var(--panel)', borderLeft: '1px solid var(--border)', boxShadow: 'var(--shadow)', animation: 'sb-drawer .22s ease',
};
const footNote: CSSProperties = { position: 'fixed', bottom: 6, left: 12, fontSize: 10, color: 'var(--muted)', opacity: 0.55, pointerEvents: 'none', zIndex: 1 };
