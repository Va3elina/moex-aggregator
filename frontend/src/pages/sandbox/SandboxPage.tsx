/**
 * SandboxPage — приватная «песочница/конструктор» (роут /sandbox, НЕ в навигации).
 *
 * Про-версия сайта: рабочее пространство, куда пользователь вытаскивает индикаторы
 * как плавающие панели-окна (drag / resize / магнитный снап), как в терминале.
 * Оболочка — паттерн из расширения (widget.js), но на React. КОНТЕНТ панелей =
 * наши реальные embed-компоненты (реальные данные через сессию юзера) — ничего не
 * мокаем, переиспользуем отточенное.
 *
 * Контент панели = embed напрямую (НЕ через MemoryRouter — Router-в-Router крашит).
 * «Единая шапка» §4.1: у панели НЕТ своего заголовка — шапкой-хватом служит сам
 * тулбар embed'а (тянем за верхние ~44px, кроме контролов), а кнопки окна
 * (⤢ развернуть / × закрыть) EmbedFrame дорисовывает в ту же строку через
 * SandboxWindowCtx. Раскладка (x/y/w/h) в координатах ХОЛСТА (y=0 под топбаром)
 * персистится в localStorage.
 *
 * Приватно: роут вне Layout, без ссылок в меню. Адаптация макета дизайнера по спеке
 * (~/Downloads/Песочница-Фрейм-спецификация.md).
 * TODO: листы, направляющие при снапе, ресайз 8 зон, тема панели ◐, центр сигналов док.
 */
import { useCallback, useEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { PlusCircle, LayoutGrid } from 'lucide-react';
import EmbedOpenInterest from '../embed/EmbedOpenInterest';
import EmbedSeasonality from '../embed/EmbedSeasonality';
import EmbedScreener from '../embed/EmbedScreener';
import EmbedBuffett from '../embed/EmbedBuffett';
import EmbedStrength from '../embed/EmbedStrength';
import EmbedFundsMoney from '../embed/EmbedFundsMoney';
import EmbedFundTrades from '../embed/EmbedFundTrades';
import EmbedCbrFlows from '../embed/EmbedCbrFlows';
import EmbedSignals from '../embed/EmbedSignals';
import { SandboxWindowCtx } from '../embed/EmbedToolbar';

type Group = 'signals' | 'instrument' | 'market';
interface IndicatorDef { id: string; label: string; group: Group }

const INDICATORS: IndicatorDef[] = [
  { id: 'signals', label: '🔔 Сигналы', group: 'signals' },
  { id: 'oi', label: 'Открытые позиции', group: 'instrument' },
  { id: 'seasonality', label: 'Сезонность', group: 'instrument' },
  { id: 'screener', label: 'Скринер сигналов', group: 'instrument' },
  { id: 'buffett', label: 'Индикатор Баффетта', group: 'market' },
  { id: 'strength', label: 'Сила рынка', group: 'market' },
  { id: 'funds-money', label: 'Фонды', group: 'market' },
  { id: 'fund-trades', label: 'Сделки фондов', group: 'market' },
  { id: 'fund-movers', label: 'Покупки фондов', group: 'market' },
  { id: 'cbr-flows', label: 'Потоки ЦБ', group: 'market' },
];

// Стартовый размер панели по индикатору (зеркалит extension SIZES).
const SIZES: Record<string, { w: number; h: number }> = {
  signals: { w: 380, h: 560 },
  oi: { w: 640, h: 460 },
  seasonality: { w: 600, h: 440 },
  screener: { w: 420, h: 560 },
  buffett: { w: 660, h: 440 },
  strength: { w: 600, h: 520 },
  'funds-money': { w: 660, h: 460 },
  'fund-trades': { w: 620, h: 520 },
  'fund-movers': { w: 620, h: 460 },
  'cbr-flows': { w: 660, h: 460 },
};
const DEFAULT_SIZE = { w: 620, h: 460 };

// id → embed-компонент (наш реальный индикатор).
function renderIndicator(id: string): ReactNode {
  switch (id) {
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

interface PanelState { key: string; id: string; x: number; y: number; w: number; h: number; z: number }

const LS_KEY = 'frame:sandbox:panels:v1';
const SNAP = 12; // порог магнита, px
const TOPBAR_H = 48;
const DRAG_STRIP_H = 44; // «шапка-хват» §4.1: верхняя полоса панели (= тулбар embed'а), за неё тянем

function groupLabel(g: Group): string {
  return g === 'signals' ? 'Центр сигналов' : g === 'instrument' ? 'По инструменту' : 'По рынку';
}

function loadPanels(): PanelState[] {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch { return []; }
}

export default function SandboxPage() {
  const [panels, setPanels] = useState<PanelState[]>(loadPanels);
  const [menuOpen, setMenuOpen] = useState(false);
  const zTop = useRef(10);
  const canvasRef = useRef<HTMLDivElement>(null);

  // Полноэкранная песочница без скролла страницы.
  useEffect(() => {
    const de = document.documentElement, b = document.body;
    const prev = { de: de.style.overflow, bo: b.style.overflow, bm: b.style.margin };
    de.style.overflow = 'hidden'; b.style.overflow = 'hidden'; b.style.margin = '0';
    return () => { de.style.overflow = prev.de; b.style.overflow = prev.bo; b.style.margin = prev.bm; };
  }, []);

  // Персист раскладки (дебаунс через микро-таймер не нужен — операции редкие).
  const persist = useCallback((next: PanelState[]) => {
    try { localStorage.setItem(LS_KEY, JSON.stringify(next)); } catch { /* quota */ }
  }, []);

  const update = useCallback((next: PanelState[]) => { setPanels(next); persist(next); }, [persist]);

  const bringToFront = useCallback((key: string) => {
    zTop.current += 1;
    setPanels((ps) => ps.map((p) => (p.key === key ? { ...p, z: zTop.current } : p)));
  }, []);

  const spawn = useCallback((id: string) => {
    const sz = SIZES[id] || DEFAULT_SIZE;
    // Координаты — ОТНОСИТЕЛЬНО холста (canvasStyle уже top:TOPBAR_H): y=0 — под топбаром.
    const vw = window.innerWidth, canvasH = window.innerHeight - TOPBAR_H;
    const n = panels.length;
    const x = Math.max(8, Math.min(vw - sz.w - 8, 40 + ((n * 28) % 220)));
    const y = Math.max(8, Math.min(canvasH - sz.h - 8, 12 + ((n * 28) % 200)));
    zTop.current += 1;
    const key = 'p' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    update([...panels, { key, id, x, y, w: sz.w, h: sz.h, z: zTop.current }]);
    setMenuOpen(false);
  }, [panels, update]);

  const close = useCallback((key: string) => { update(panels.filter((p) => p.key !== key)); }, [panels, update]);

  // ⤢ Развернуть / восстановить панель (§4.3). Прежние габариты держим в ref
  // (транзиентно, не персистим) — второй клик по ⤢ возвращает как было.
  const restoreRef = useRef<Record<string, { x: number; y: number; w: number; h: number }>>({});
  const expand = useCallback((key: string) => {
    const vw = window.innerWidth, canvasH = window.innerHeight - TOPBAR_H;
    zTop.current += 1;
    setPanels((ps) => {
      const next = ps.map((p) => {
        if (p.key !== key) return p;
        const saved = restoreRef.current[key];
        if (saved) { delete restoreRef.current[key]; return { ...p, ...saved, z: zTop.current }; }
        restoreRef.current[key] = { x: p.x, y: p.y, w: p.w, h: p.h };
        const w = Math.min(880, vw - 24), h = Math.min(600, canvasH - 24);
        return { ...p, x: Math.max(12, Math.round((vw - w) / 2)), y: 12, w, h, z: zTop.current };
      });
      persist(next);
      return next;
    });
  }, [persist]);

  // ── Магнитный снап к краям холста и соседним панелям (порт из widget.js) ──
  const snapMove = useCallback((self: PanelState, nx: number, ny: number): { x: number; y: number } => {
    const vw = window.innerWidth, vh = window.innerHeight;
    const xs: number[] = [0, vw - self.w];
    const ys: number[] = [0, (vh - TOPBAR_H) - self.h];
    for (const p of panels) {
      if (p.key === self.key) continue;
      xs.push(p.x, p.x + p.w - self.w, p.x + p.w, p.x - self.w);
      ys.push(p.y, p.y + p.h - self.h, p.y + p.h, p.y - self.h);
    }
    let bx = nx, bdx = SNAP;
    for (const v of xs) { const d = Math.abs(nx - v); if (d < bdx) { bx = v; bdx = d; } }
    let by = ny, bdy = SNAP;
    for (const v of ys) { const d = Math.abs(ny - v); if (d < bdy) { by = v; bdy = d; } }
    return { x: bx, y: by };
  }, [panels]);

  const onDragStart = useCallback((e: React.PointerEvent, key: string) => {
    bringToFront(key); // любой pointerdown по панели поднимает её на верх стопки
    const el = e.currentTarget as HTMLElement;
    // Тянем ТОЛЬКО за верхнюю полосу («шапка-хват» = тулбар embed'а, §4.1) и не по контролам.
    if (e.clientY - el.getBoundingClientRect().top > DRAG_STRIP_H) return;
    if ((e.target as HTMLElement).closest('button, input, a, select, textarea, [role="dialog"]')) return;
    e.preventDefault();
    const start = panels.find((p) => p.key === key);
    if (!start) return;
    const sx = e.clientX, sy = e.clientY, ox = start.x, oy = start.y;
    try { el.setPointerCapture(e.pointerId); } catch { /* noop */ }
    let latest = { x: ox, y: oy };
    const move = (ev: PointerEvent) => {
      const raw = snapMove(start, ox + (ev.clientX - sx), oy + (ev.clientY - sy));
      latest = raw;
      setPanels((ps) => ps.map((p) => (p.key === key ? { ...p, x: raw.x, y: raw.y } : p)));
    };
    const up = () => {
      el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up);
      setPanels((ps) => { const next = ps.map((p) => (p.key === key ? { ...p, x: latest.x, y: latest.y } : p)); persist(next); return next; });
    };
    el.addEventListener('pointermove', move); el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
  }, [panels, bringToFront, snapMove, persist]);

  const onResizeStart = useCallback((e: React.PointerEvent, key: string) => {
    e.preventDefault(); e.stopPropagation();
    bringToFront(key);
    const start = panels.find((p) => p.key === key);
    if (!start) return;
    const sx = e.clientX, sy = e.clientY, ow = start.w, oh = start.h;
    const el = e.currentTarget as HTMLElement;
    try { el.setPointerCapture(e.pointerId); } catch { /* noop */ }
    let latest = { w: ow, h: oh };
    const move = (ev: PointerEvent) => {
      const w = Math.max(300, Math.min(window.innerWidth - start.x - 8, ow + (ev.clientX - sx)));
      const h = Math.max(220, Math.min(window.innerHeight - TOPBAR_H - start.y - 8, oh + (ev.clientY - sy)));
      latest = { w, h };
      setPanels((ps) => ps.map((p) => (p.key === key ? { ...p, w, h } : p)));
    };
    const up = () => {
      el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up);
      setPanels((ps) => { const next = ps.map((p) => (p.key === key ? { ...p, w: latest.w, h: latest.h } : p)); persist(next); return next; });
    };
    el.addEventListener('pointermove', move); el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
  }, [panels, bringToFront, persist]);

  // «Выстроить» — разложить панели в аккуратную сетку.
  const arrange = useCallback(() => {
    const vw = window.innerWidth, vh = window.innerHeight - TOPBAR_H;
    const cols = panels.length <= 1 ? 1 : panels.length <= 4 ? 2 : 3;
    const rows = Math.ceil(panels.length / cols);
    const gap = 10;
    const w = Math.floor((vw - gap * (cols + 1)) / cols);
    const h = Math.floor((vh - gap * (rows + 1)) / rows);
    const next = panels.map((p, i) => ({
      ...p,
      x: gap + (i % cols) * (w + gap),
      y: gap + Math.floor(i / cols) * (h + gap),
      w, h,
    }));
    update(next);
  }, [panels, update]);

  return (
    <div style={rootStyle}>
      {/* ── Топбар ── */}
      <div style={topbarStyle}>
        <span style={{ fontWeight: 800, fontSize: 15, letterSpacing: '-0.02em', color: 'var(--accent, #FF5C2B)' }}>Фрейм</span>
        <span style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: 'var(--text-secondary)', border: '1px solid var(--border-color)', borderRadius: 4, padding: '2px 5px' }}>Песочница</span>

        <div style={{ position: 'relative', marginLeft: 8 }}>
          <button type="button" onClick={() => setMenuOpen((v) => !v)} style={addBtnStyle(menuOpen)}>
            <PlusCircle size={15} /> Индикатор
          </button>
          {menuOpen && (
            <>
              <div style={{ position: 'fixed', inset: 0, zIndex: 90 }} onClick={() => setMenuOpen(false)} />
              <div style={menuStyle}>
                {(['signals', 'instrument', 'market'] as Group[]).map((g) => (
                  <div key={g}>
                    <div style={menuGroupStyle}>{groupLabel(g)}</div>
                    {INDICATORS.filter((i) => i.group === g).map((ind) => (
                      <button key={ind.id} type="button" onClick={() => spawn(ind.id)} style={menuItemStyle}>
                        <span style={{ width: 6, height: 6, borderRadius: 3, background: 'var(--accent, #FF5C2B)', flex: '0 0 auto' }} />
                        {ind.label}
                      </button>
                    ))}
                  </div>
                ))}
              </div>
            </>
          )}
        </div>

        <button type="button" onClick={arrange} disabled={panels.length === 0} title="Разложить по сетке" style={{ ...iconBtnStyle, marginLeft: 4, opacity: panels.length === 0 ? 0.4 : 1 }}>
          <LayoutGrid size={15} /> Выстроить
        </button>

        <span style={{ marginLeft: 'auto', fontSize: 11.5, color: 'var(--text-muted)' }}>
          {panels.length ? `${panels.length} панел${panels.length === 1 ? 'ь' : panels.length < 5 ? 'и' : 'ей'}` : 'Добавьте индикатор →'}
        </span>
      </div>

      {/* ── Холст ── */}
      <div ref={canvasRef} style={canvasStyle}>
        {panels.length === 0 && (
          <div style={emptyStyle}>
            <LayoutGrid size={30} style={{ opacity: 0.4 }} />
            <div style={{ fontSize: 14, fontWeight: 700 }}>Пустая песочница</div>
            <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', maxWidth: 300, textAlign: 'center' }}>
              Нажмите «＋ Индикатор» вверху и соберите своё рабочее пространство. Панели можно двигать, тянуть за угол и прилеплять друг к другу.
            </div>
          </div>
        )}
        {panels.map((p) => (
          <div
            key={p.key}
            style={{ position: 'absolute', left: p.x, top: p.y, width: p.w, height: p.h, zIndex: p.z, ...panelStyle }}
            onPointerDown={(e) => onDragStart(e, p.key)}
          >
            {/* Единая шапка §4.1: отдельного заголовка панели НЕТ — «шапкой-хватом»
                служит верхняя полоса самого тулбара embed'а. Кнопки окна (⤢ развернуть /
                × закрыть) EmbedFrame дорисовывает справа в ту же строку через контекст.
                Реальный embed напрямую (юзер залогинен → apiFetch авторизован; тема — из
                общего ThemeContext). Вложенный <Router> НЕЛЬЗЯ (Router-в-Router крашит). */}
            <div style={panelBodyStyle}>
              <SandboxWindowCtx.Provider value={{ onExpand: () => expand(p.key), onClose: () => close(p.key) }}>
                {renderIndicator(p.id)}
              </SandboxWindowCtx.Provider>
            </div>
            <div onPointerDown={(e) => onResizeStart(e, p.key)} style={resizeHandleStyle} />
          </div>
        ))}
      </div>

      {/* Тонкая подпись: приватная превью-версия */}
      <div style={{ position: 'fixed', bottom: 6, left: 10, fontSize: 10, color: 'var(--text-muted)', opacity: 0.6, pointerEvents: 'none', zIndex: 1 }}>
        песочница · приватный превью
      </div>
    </div>
  );
}

/* ───────────────────────── styles ───────────────────────── */
const rootStyle: CSSProperties = {
  position: 'fixed', inset: 0, background: 'var(--bg-base, #0E0E10)', color: 'var(--text-primary, #F5F1E8)',
  fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Inter, system-ui, sans-serif', overflow: 'hidden',
};
const topbarStyle: CSSProperties = {
  position: 'absolute', top: 0, left: 0, right: 0, height: TOPBAR_H, display: 'flex', alignItems: 'center', gap: 8,
  padding: '0 14px', borderBottom: '1px solid var(--border-color, rgba(245,241,232,0.14))', background: 'var(--bg-secondary, #17161A)', zIndex: 50,
};
const canvasStyle: CSSProperties = { position: 'absolute', top: TOPBAR_H, left: 0, right: 0, bottom: 0, overflow: 'hidden' };
const emptyStyle: CSSProperties = {
  position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 10, color: 'var(--text-primary)',
};
const panelStyle: CSSProperties = {
  display: 'flex', flexDirection: 'column', background: 'var(--bg-base, #0E0E10)',
  border: '1px solid var(--border-color, rgba(245,241,232,0.14))', borderRadius: 8, boxShadow: '0 12px 34px rgba(0,0,0,0.5)', overflow: 'hidden',
};
const panelBodyStyle: CSSProperties = { flex: '1 1 auto', position: 'relative', minHeight: 0, background: 'var(--bg-base, #0E0E10)' };
const resizeHandleStyle: CSSProperties = { position: 'absolute', right: 0, bottom: 0, width: 16, height: 16, cursor: 'nwse-resize', zIndex: 3 };
function addBtnStyle(open: boolean): CSSProperties {
  return {
    display: 'inline-flex', alignItems: 'center', gap: 6, padding: '6px 12px', borderRadius: 7, fontSize: 12.5, fontWeight: 700, cursor: 'pointer',
    border: '1.5px solid var(--accent, #FF5C2B)', background: open ? 'var(--accent, #FF5C2B)' : 'transparent', color: open ? '#fff' : 'var(--accent, #FF5C2B)',
  };
}
const iconBtnStyle: CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6, padding: '6px 10px', borderRadius: 7, fontSize: 12.5, fontWeight: 600, cursor: 'pointer',
  border: '1px solid var(--border-color, rgba(245,241,232,0.14))', background: 'transparent', color: 'var(--text-secondary)',
};
const menuStyle: CSSProperties = {
  position: 'absolute', top: 'calc(100% + 6px)', left: 0, zIndex: 100, minWidth: 220, background: 'var(--bg-secondary, #17161A)', color: 'var(--text-primary)',
  border: '1.5px solid var(--border-color, rgba(245,241,232,0.2))', borderRadius: 10, boxShadow: '0 12px 34px rgba(0,0,0,0.5)', padding: 6, maxHeight: 'calc(100vh - 64px)', overflowY: 'auto',
};
const menuGroupStyle: CSSProperties = { fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--text-muted)', padding: '7px 8px 3px' };
const menuItemStyle: CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 8, width: '100%', padding: '7px 9px', border: 'none', borderRadius: 6, background: 'transparent',
  color: 'var(--text-primary)', fontSize: 13, fontWeight: 600, textAlign: 'left', cursor: 'pointer', whiteSpace: 'nowrap',
};
