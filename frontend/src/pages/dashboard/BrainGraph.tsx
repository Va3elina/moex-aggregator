/**
 * Вид «как в Obsidian»: силовая раскладка подграфа на canvas.
 *
 * ⚠️ РИСУЕМ ПОДГРАФ, А НЕ КАРТУ. Без центра — структурный слой (компании, секторы,
 * держатели, фонды, индексы, ≈550 узлов), его можно показать целиком. С центром —
 * обход на 1–3 шага вокруг узла; новости и кандидаты — только по переключателю,
 * иначе у Сбера две тысячи соседей превращают холст в пыль.
 *
 * ⚠️ БЕЗ БИБЛИОТЕК. Своя симуляция: отталкивание O(n²) (600 узлов — 360 тысяч пар
 * за кадр, хватает), пружины по рёбрам, притяжение к центру, затухание; остывает и
 * останавливается, перетаскивание разогревает. Никакого d3 в бандле.
 *
 * ⚠️ «ВМЕСТЕ В НОВОСТЯХ» ВЫКЛЮЧЕНО ПО УМОЛЧАНИЮ: 800 рёбер уровня D сшивают всё со
 * всем — граф становится клубком. Это подсказка, а не структура.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Loader2 } from 'lucide-react';
import { getBrainGraph } from '../../services/api';
import type { BrainGraph as Граф, BrainGraphNode } from '../../services/api';

type Узел = BrainGraphNode & { x: number; y: number; vx: number; vy: number; r: number; фикс?: boolean };
type Ребро = { a: Узел; b: Узел; связь: string; уровень: string | null };

const ЦВЕТ_ВИДА: Record<string, string> = {
  company: '#F5F1E8', sector: '#5BD49C', holder: '#8B7CF6', fund: '#5DA3E9', index: '#F2A24A',
  news: '#6B6B6B', candidate: '#FF5C2B', post: '#FF5C2B', fact: '#8B7CF6', anomaly: '#F2A24A', signal: '#F2A24A', doc: '#5DA3E9',
};
const ПОДПИСЬ_ВИДА: Record<string, string> = {
  company: 'компании', sector: 'секторы', holder: 'держатели', fund: 'фонды', index: 'индексы',
  news: 'новости', candidate: 'кандидаты', post: 'посты', fact: 'факты', anomaly: 'аномалии', signal: 'сигналы', doc: 'документы',
};
const ЦВЕТ_СВЯЗИ: Record<string, string> = {
  владеет: '#8B7CF6', владеет_долей: '#8B7CF6', держит: '#5DA3E9', включает: '#F2A24A', в_секторе: '#5BD49C',
  вместе_в_новостях: '#6B6B6B', упоминает: '#6B6B6B', о: '#FF5C2B', из_новости: '#FF5C2B', факт_о: '#8B7CF6',
};
const ПОДПИСЬ_СВЯЗИ: Record<string, string> = {
  владеет: 'владение', владеет_долей: 'акционеры', держит: 'фонды-держатели', включает: 'индексы', в_секторе: 'сектор',
  вместе_в_новостях: 'вместе в новостях', упоминает: 'новости', о: 'кандидаты', из_новости: 'из новости', факт_о: 'факты',
};
/** Длина пружины: структурные связи короче, новости — длиннее, чтобы висели снаружи. */
const ДЛИНА: Record<string, number> = { в_секторе: 70, владеет: 60, владеет_долей: 55, держит: 90, включает: 110, вместе_в_новостях: 120 };
const ВСЕГДА_С_ПОДПИСЬЮ = new Set(['company', 'sector', 'index', 'fund']);

function радиус(n: BrainGraphNode) {
  const базовый = n.вид === 'company' ? 4 : n.вид === 'sector' || n.вид === 'index' ? 5 : 3;
  return базовый + Math.min(9, Math.sqrt(Math.max(n.степень, 1)) * 0.55);
}

export default function BrainGraph({ center, onУзел, высота = 560 }: { center?: string; onУзел: (id: string) => void; высота?: number }) {
  const [граф, setГраф] = useState<Граф | null>(null);
  const [ошибка, setОшибка] = useState<string | null>(null);
  const [занято, setЗанято] = useState(false);
  const [глубина, setГлубина] = useState(center ? 2 : 0);
  const [сНовостями, setСНовостями] = useState(false);
  const [вместе, setВместе] = useState(false);
  const [скрытые, setСкрытые] = useState<Set<string>>(new Set());
  const [подписи, setПодписи] = useState(true);
  const [навед, setНавед] = useState<Узел | null>(null);

  const холст = useRef<HTMLCanvasElement>(null);
  const обёртка = useRef<HTMLDivElement>(null);
  const сост = useRef<{
    узлы: Узел[]; рёбра: Ребро[]; поId: Map<string, Узел>; соседи: Map<string, Set<string>>;
    alpha: number; кадр: number; зум: number; сдвиг: { x: number; y: number };
    тянем: Узел | null; панорама: { x: number; y: number; sx: number; sy: number } | null; мышь: { x: number; y: number } | null;
  }>({ узлы: [], рёбра: [], поId: new Map(), соседи: new Map(), alpha: 0, кадр: 0, зум: 1, сдвиг: { x: 0, y: 0 }, тянем: null, панорама: null, мышь: null });

  useEffect(() => {
    let живо = true;
    setЗанято(true); setОшибка(null);
    getBrainGraph(center, center ? глубина : 2, сНовостями)
      .then((g) => { if (живо) setГраф(g); })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); })
      .finally(() => { if (живо) setЗанято(false); });
    return () => { живо = false; };
  }, [center, глубина, сНовостями]);

  /** Сборка узлов и рёбер под текущие фильтры; позиции старых узлов сохраняются, чтобы граф не «прыгал». */
  useEffect(() => {
    if (!граф) return;
    const s = сост.current;
    const старые = s.поId;
    const узлы: Узел[] = [];
    const поId = new Map<string, Узел>();
    const шагов = Math.max(1, ...граф.узлы.map((n) => n.шаг ?? 0));
    граф.узлы.forEach((n, i) => {
      if (скрытые.has(n.вид)) return;
      const был = старые.get(n.id);
      let x: number; let y: number;
      if (был) { x = был.x; y = был.y; } else {
        const угол = (i * 2.399963) % (Math.PI * 2); // золотой угол — равномерная спираль
        const r = center ? (n.id === center ? 0 : 90 + ((n.шаг ?? 1) - 1) * (220 / шагов) + (i % 7) * 12) : 60 + Math.sqrt(i) * 14;
        x = Math.cos(угол) * r; y = Math.sin(угол) * r;
      }
      const у: Узел = { ...n, x, y, vx: 0, vy: 0, r: радиус(n), фикс: n.id === center };
      узлы.push(у); поId.set(у.id, у);
    });
    const рёбра: Ребро[] = [];
    const соседи = new Map<string, Set<string>>();
    for (const e of граф.рёбра) {
      if (e.связь === 'вместе_в_новостях' && !вместе) continue;
      const a = поId.get(e.от); const b = поId.get(e.к);
      if (!a || !b || a === b) continue;
      рёбра.push({ a, b, связь: e.связь, уровень: e.уровень });
      if (!соседи.has(a.id)) соседи.set(a.id, new Set()); if (!соседи.has(b.id)) соседи.set(b.id, new Set());
      соседи.get(a.id)!.add(b.id); соседи.get(b.id)!.add(a.id);
    }
    s.узлы = узлы; s.рёбра = рёбра; s.поId = поId; s.соседи = соседи; s.alpha = 1;
    if (!старые.size) { s.зум = center ? 1 : 0.8; s.сдвиг = { x: 0, y: 0 }; }
    setНавед(null);
  }, [граф, скрытые, вместе, center]);

  const тик = useCallback(() => {
    const s = сост.current;
    const { узлы, рёбра } = s;
    if (s.alpha <= 0.004 || !узлы.length) return false;
    const a = s.alpha;
    const n = узлы.length;
    // отталкивание
    for (let i = 0; i < n; i++) {
      const p = узлы[i];
      for (let j = i + 1; j < n; j++) {
        const q = узлы[j];
        let dx = p.x - q.x; let dy = p.y - q.y;
        let d2 = dx * dx + dy * dy;
        if (d2 > 90000) continue; // дальше 300px не толкаем
        if (d2 < 1) { dx = (Math.random() - 0.5); dy = (Math.random() - 0.5); d2 = 1; }
        const f = (900 * a) / d2;
        const fx = dx * f; const fy = dy * f;
        if (!p.фикс) { p.vx += fx; p.vy += fy; }
        if (!q.фикс) { q.vx -= fx; q.vy -= fy; }
      }
    }
    // пружины
    for (const e of рёбра) {
      const dx = e.b.x - e.a.x; const dy = e.b.y - e.a.y;
      const d = Math.sqrt(dx * dx + dy * dy) || 1;
      const цель = (ДЛИНА[e.связь] ?? 80) + (e.a.r + e.b.r);
      const k = ((d - цель) / d) * 0.08 * a;
      const fx = dx * k; const fy = dy * k;
      if (!e.a.фикс) { e.a.vx += fx; e.a.vy += fy; }
      if (!e.b.фикс) { e.b.vx -= fx; e.b.vy -= fy; }
    }
    // притяжение к центру и шаг
    for (const p of узлы) {
      if (p.фикс) { p.vx = 0; p.vy = 0; continue; }
      p.vx -= p.x * 0.012 * a; p.vy -= p.y * 0.012 * a;
      p.vx *= 0.55; p.vy *= 0.55;
      p.x += p.vx; p.y += p.vy;
    }
    s.alpha *= s.тянем ? 1 : 0.985;
    return true;
  }, []);

  const рисовать = useCallback(() => {
    const c = холст.current; const s = сост.current;
    if (!c) return;
    const dpr = window.devicePixelRatio || 1;
    const w = c.clientWidth; const h = c.clientHeight;
    if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) { c.width = Math.round(w * dpr); c.height = Math.round(h * dpr); }
    const g = c.getContext('2d'); if (!g) return;
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, w, h);
    g.save();
    g.translate(w / 2 + s.сдвиг.x, h / 2 + s.сдвиг.y); g.scale(s.зум, s.зум);
    const выбран = навед;
    const рядом = выбран ? s.соседи.get(выбран.id) ?? new Set<string>() : null;
    for (const e of s.рёбра) {
      const яркое = выбран && (e.a === выбран || e.b === выбран);
      const тусклое = выбран && !яркое;
      g.strokeStyle = ЦВЕТ_СВЯЗИ[e.связь] ?? '#6B6B6B';
      g.globalAlpha = тусклое ? 0.04 : яркое ? 0.9 : e.уровень === 'D' ? 0.12 : 0.28;
      g.lineWidth = (яркое ? 1.6 : e.уровень === 'D' ? 0.5 : 0.9) / s.зум;
      g.beginPath(); g.moveTo(e.a.x, e.a.y); g.lineTo(e.b.x, e.b.y); g.stroke();
    }
    g.globalAlpha = 1;
    for (const p of s.узлы) {
      const тусклый = выбран && p !== выбран && !рядом!.has(p.id);
      g.globalAlpha = тусклый ? 0.18 : 1;
      g.fillStyle = ЦВЕТ_ВИДА[p.вид] ?? '#9A9A9A';
      g.beginPath(); g.arc(p.x, p.y, p.r, 0, Math.PI * 2); g.fill();
      if (p.id === center || p === выбран) { g.strokeStyle = '#FF5C2B'; g.lineWidth = 2 / s.зум; g.stroke(); }
    }
    // подписи: структурные — при достаточном зуме, остальные — только у наведённого и его соседей
    g.textAlign = 'center'; g.textBaseline = 'top';
    for (const p of s.узлы) {
      const близко = p === выбран || (рядом?.has(p.id) ?? false);
      const показать = близко || (подписи && ВСЕГДА_С_ПОДПИСЬЮ.has(p.вид) && (s.зум * p.r > 4.5 || p.вид !== 'company'));
      if (!показать) continue;
      const размер = Math.max(9, Math.min(13, 11 / Math.sqrt(s.зум)));
      g.font = `${близко ? 600 : 500} ${размер}px "Inter", system-ui, sans-serif`;
      g.globalAlpha = выбран && !близко ? 0.25 : близко ? 1 : 0.85;
      const текст = p.заголовок.length > 28 ? p.заголовок.slice(0, 27) + '…' : p.заголовок;
      g.lineWidth = 3 / s.зум; g.strokeStyle = 'rgba(14,14,16,0.85)'; g.strokeText(текст, p.x, p.y + p.r + 2);
      g.fillStyle = близко ? '#F5F1E8' : '#BDB8AE'; g.fillText(текст, p.x, p.y + p.r + 2);
    }
    g.restore();
  }, [навед, подписи, center]);

  // цикл кадров: считаем, пока тепло, рисуем всегда
  useEffect(() => {
    const s = сост.current;
    let живо = true;
    const цикл = () => {
      if (!живо) return;
      const двигалось = тик();
      рисовать();
      s.кадр = requestAnimationFrame(двигалось || s.тянем || s.панорама ? цикл : () => { s.кадр = 0; });
    };
    s.кадр = requestAnimationFrame(цикл);
    const разбудить = () => { if (!s.кадр) s.кадр = requestAnimationFrame(цикл); };
    const w = обёртка.current;
    const наблюдатель = new ResizeObserver(() => { рисовать(); });
    if (w) наблюдатель.observe(w);
    (s as unknown as { разбудить: () => void }).разбудить = разбудить;
    return () => { живо = false; if (s.кадр) cancelAnimationFrame(s.кадр); s.кадр = 0; наблюдатель.disconnect(); };
  }, [тик, рисовать, граф, скрытые, вместе]);

  const разбудить = () => (сост.current as unknown as { разбудить?: () => void }).разбудить?.();

  const вМир = (ev: React.MouseEvent | React.WheelEvent) => {
    const c = холст.current!; const s = сост.current;
    const r = c.getBoundingClientRect();
    const sx = ev.clientX - r.left; const sy = ev.clientY - r.top;
    return { sx, sy, x: (sx - r.width / 2 - s.сдвиг.x) / s.зум, y: (sy - r.height / 2 - s.сдвиг.y) / s.зум };
  };
  const узелПод = (x: number, y: number) => {
    const s = сост.current;
    let лучший: Узел | null = null; let d2min = Infinity;
    for (const p of s.узлы) {
      const dx = p.x - x; const dy = p.y - y; const d2 = dx * dx + dy * dy;
      const порог = (p.r + 6 / s.зум) ** 2;
      if (d2 < порог && d2 < d2min) { лучший = p; d2min = d2; }
    }
    return лучший;
  };

  const onMouseDown = (ev: React.MouseEvent) => {
    const s = сост.current; const m = вМир(ev);
    const p = узелПод(m.x, m.y);
    if (p) { s.тянем = p; p.фикс = true; s.alpha = Math.max(s.alpha, 0.3); }
    else s.панорама = { x: m.sx, y: m.sy, sx: s.сдвиг.x, sy: s.сдвиг.y };
    s.мышь = { x: m.sx, y: m.sy };
    разбудить();
  };
  const onMouseMove = (ev: React.MouseEvent) => {
    const s = сост.current; const m = вМир(ev);
    if (s.тянем) { s.тянем.x = m.x; s.тянем.y = m.y; s.alpha = Math.max(s.alpha, 0.3); разбудить(); return; }
    if (s.панорама) { s.сдвиг = { x: s.панорама.sx + (m.sx - s.панорама.x), y: s.панорама.sy + (m.sy - s.панорама.y) }; разбудить(); return; }
    const p = узелПод(m.x, m.y);
    if (p !== навед) { setНавед(p); }
  };
  const onMouseUp = (ev: React.MouseEvent) => {
    const s = сост.current;
    if (s.тянем) {
      const был = s.мышь; const m = вМир(ev);
      const сдвинули = был && Math.hypot(m.sx - был.x, m.sy - был.y) > 4;
      if (s.тянем.id !== center) s.тянем.фикс = false;
      if (!сдвинули) onУзел(s.тянем.id);
      s.тянем = null; разбудить();
    }
    s.панорама = null;
  };
  const onWheel = (ev: React.WheelEvent) => {
    const s = сост.current; const m = вМир(ev);
    const k = Math.exp(-ev.deltaY * 0.0015);
    const новый = Math.min(6, Math.max(0.15, s.зум * k));
    // зум к курсору: точка под курсором остаётся на месте
    s.сдвиг = { x: m.sx - холст.current!.clientWidth / 2 - m.x * новый, y: m.sy - холст.current!.clientHeight / 2 - m.y * новый };
    s.зум = новый; разбудить();
  };

  const виды = useMemo(() => {
    const c = new Map<string, number>();
    for (const n of граф?.узлы ?? []) c.set(n.вид, (c.get(n.вид) ?? 0) + 1);
    return [...c.entries()].sort((a, b) => b[1] - a[1]);
  }, [граф]);
  const связи = useMemo(() => {
    const c = new Map<string, number>();
    for (const e of граф?.рёбра ?? []) c.set(e.связь, (c.get(e.связь) ?? 0) + 1);
    return [...c.entries()].sort((a, b) => b[1] - a[1]);
  }, [граф]);
  const переключитьВид = (в: string) => setСкрытые((s) => { const n = new Set(s); if (n.has(в)) n.delete(в); else n.add(в); return n; });

  return (
    <div className="flex flex-col" style={{ gap: 8 }}>
      <div className="flex items-center flex-wrap" style={{ gap: 10 }}>
        {center && (
          <div className="flex items-center" style={{ gap: 4 }}>
            <span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>шагов</span>
            {[1, 2, 3].map((d) => <button key={d} className="dash-tab" data-active={глубина === d} onClick={() => setГлубина(d)}>{d}</button>)}
          </div>
        )}
        {center && <button className="dash-tab" data-active={сНовостями} onClick={() => setСНовостями((v) => !v)}>с новостями</button>}
        <button className="dash-tab" data-active={вместе} onClick={() => setВместе((v) => !v)} title="рёбра уровня D — совместные упоминания">вместе в новостях</button>
        <button className="dash-tab" data-active={подписи} onClick={() => setПодписи((v) => !v)}>подписи</button>
        <button className="dash-tab" onClick={() => { const s = сост.current; s.зум = center ? 1 : 0.8; s.сдвиг = { x: 0, y: 0 }; s.alpha = 1; разбудить(); }}>сбросить вид</button>
        <span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)', marginLeft: 'auto' }}>
          {занято ? <Loader2 size={12} className="animate-spin" /> : граф ? `${граф.узлы.length} узлов · ${граф.рёбра.length} связей · ${граф.мс} мс` : ''}
        </span>
      </div>
      <div ref={обёртка} style={{ position: 'relative', height: высота, borderRadius: 10, overflow: 'hidden', background: '#0E0E10', border: '1px solid var(--d-line)' }}>
        <canvas
          ref={холст}
          style={{ width: '100%', height: '100%', display: 'block', cursor: навед ? 'pointer' : 'grab' }}
          onMouseDown={onMouseDown} onMouseMove={onMouseMove} onMouseUp={onMouseUp} onMouseLeave={onMouseUp} onWheel={onWheel}
        />
        {ошибка && <div className="mono" style={{ position: 'absolute', inset: 0, display: 'grid', placeItems: 'center', color: 'var(--d-bad)', fontSize: 12 }}>{ошибка}</div>}
        {навед && (
          <div className="mono" style={{ position: 'absolute', left: 10, bottom: 10, fontSize: 11, color: 'var(--d-ink)', background: 'rgba(23,23,26,0.92)', border: '1px solid var(--d-line-strong)', borderRadius: 8, padding: '6px 10px', maxWidth: 360, pointerEvents: 'none' }}>
            <span style={{ color: ЦВЕТ_ВИДА[навед.вид] ?? 'var(--d-mute)' }}>{ПОДПИСЬ_ВИДА[навед.вид] ?? навед.вид}</span> · {навед.заголовок}
            <span style={{ color: 'var(--d-dim)' }}> · связей {навед.степень.toLocaleString('ru-RU')} · клик — открыть</span>
          </div>
        )}
        <div style={{ position: 'absolute', right: 10, top: 10, display: 'flex', flexDirection: 'column', gap: 3, pointerEvents: 'none' }}>
          {виды.map(([в, n]) => (
            <button key={в} onClick={() => переключитьВид(в)} className="mono flex items-center"
              style={{ pointerEvents: 'auto', gap: 6, fontSize: 10.5, background: 'rgba(23,23,26,0.85)', border: '1px solid var(--d-line)', borderRadius: 6, padding: '2px 8px', color: скрытые.has(в) ? 'var(--d-dim)' : 'var(--d-ink)', cursor: 'pointer', textDecoration: скрытые.has(в) ? 'line-through' : 'none' }}>
              <span style={{ width: 8, height: 8, borderRadius: 999, background: ЦВЕТ_ВИДА[в] ?? '#9A9A9A', opacity: скрытые.has(в) ? 0.3 : 1 }} />
              {ПОДПИСЬ_ВИДА[в] ?? в} <span style={{ color: 'var(--d-dim)' }}>{n}</span>
            </button>
          ))}
        </div>
      </div>
      <div className="flex flex-wrap mono" style={{ gap: 10, fontSize: 10.5, color: 'var(--d-dim)' }}>
        {связи.map(([k, n]) => (
          <span key={k} className="flex items-center" style={{ gap: 5 }}>
            <span style={{ width: 14, height: 2, background: ЦВЕТ_СВЯЗИ[k] ?? '#6B6B6B' }} />{ПОДПИСЬ_СВЯЗИ[k] ?? k} {n}
          </span>
        ))}
        <span style={{ marginLeft: 'auto' }}>колесо — масштаб · фон — панорама · узел — тянуть, клик — открыть</span>
      </div>
    </div>
  );
}
