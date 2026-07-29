/**
 * EmbedHeatmap — «Карта рынка» для панели песочницы/расширения (§6.9 макета).
 * НЕ Lightweight-график: честный squarified treemap (тот же алгоритм squarify,
 * что и на десктопной HeatmapPage/MobileHeatmapPage — utils/squarify.ts), а не
 * flex-wrap список секторов. Два уровня: секторы (площадь ~ суммарный оборот,
 * слабая компрессия pow^0.85 — крупный сектор не съедает всё) → внутри каждого
 * сектора акции (площадь ~ оборот, сильная компрессия pow^0.55 — мелкие тикеры
 * не схлопываются в невидимые полоски). Заливка ровно по контейнеру — без
 * прокрутки, ResizeObserver пересчитывает раскладку при ресайзе панели.
 *
 * Данные — те же эндпоинты, что у полной страницы (HeatmapPage не трогаем):
 * вселенная IMOEX (/api/heatmap/imoex) или все акции (/api/heatmap/data).
 * Метрика меняет ВТОРУЮ строку в плитке (изм.% / оборот); цвет всегда по change_1d.
 */
import { useEffect, useMemo, useRef, useState, lazy, Suspense, type ReactNode } from 'react';
import { Camera, Landmark, Grid3x3, Percent, BarChart3 } from 'lucide-react';
import { getHeatmapData, getHeatmapImoex, type HeatmapResponse, type HeatmapStock } from '../../services/api';
import { squarify, type SquarifyRect } from '../../utils/squarify';
import { EmbedMsg } from './embedUi';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';
import { useToolbarCompact } from './useToolbarCompact';

const ExportModal = lazy(() => import('../../components/export/ExportModal'));

type Universe = 'imoex' | 'all';
type Metric = 'change' | 'vol';
type LoadStatus = 'loading' | 'ok' | 'empty' | 'error';

const UNIVERSES: { id: Universe; label: string }[] = [
  { id: 'imoex', label: 'Индекс IMOEX' },
  { id: 'all', label: 'Все акции' },
];
const UNIVERSE_ICONS: Record<Universe, ReactNode> = {
  imoex: <Landmark size={14} />,
  all: <Grid3x3 size={14} />,
};
const METRICS: { id: Metric; label: string; title: string; icon: ReactNode }[] = [
  { id: 'change', label: 'Изм. %', title: 'Дневное изменение', icon: <Percent size={14} /> },
  { id: 'vol', label: 'Объём', title: 'Оборот за день', icon: <BarChart3 size={14} /> },
];

// Цвет плитки — 1:1 порт getColor с десктопной HeatmapPage (earth-tone: тёмный
// центр #2a2a2a → brick/clay на падении, forest green на росте; НЕ зависит от
// темы страницы — треймап всегда тёмный, как на сайте). maxChange=0.8 — тот же
// порог, что и в change_1d ветке оригинала (embed красит только по change_1d).
function heatColor(ch: number): string {
  const maxChange = 0.8;
  const t = Math.min(Math.abs(ch) / maxChange, 1);
  if (ch > 0) {
    const r = Math.round(42 + t * (45 - 42));
    const g = Math.round(42 + t * (107 - 42));
    const b = Math.round(42 + t * (63 - 42));
    return `rgb(${r},${g},${b})`;
  }
  if (ch < 0) {
    const r = Math.round(42 + t * (122 - 42));
    const g = Math.round(42 + t * (53 - 42));
    const b = Math.round(42 + t * (40 - 42));
    return `rgb(${r},${g},${b})`;
  }
  return '#2a2a2a';
}

// html2canvas при экспорте резолвит fill="var(--text-secondary)" на SVG <text>
// корректно ТОЛЬКО для первого инстанса на странице — у остальных секторов
// подпись просто не рисуется (не обрезка, а полное отсутствие; в живом браузере
// getComputedStyle резолвит var() одинаково для всех — баг именно в парсере
// html2canvas). Та же идиома, что resolveColor в LwChart.tsx/LwChartPanes.tsx:
// заранее резолвим var() в литеральный rgb() через probe-элемент, чтобы
// html2canvas никогда не видел сырую строку "var(...)".
function resolveColor(box: HTMLElement, color: string): string {
  try {
    const probe = document.createElement('span');
    probe.style.color = color;
    probe.style.position = 'absolute';
    probe.style.pointerEvents = 'none';
    box.appendChild(probe);
    const rgb = getComputedStyle(probe).color;
    box.removeChild(probe);
    return rgb || color;
  } catch { return color; }
}

const fmtPct = (v: number): string => (v < 0 ? '−' : '+') + Math.abs(v).toFixed(1).replace('.', ',') + '%';
function fmtVol(v: number): string {
  const a = Math.abs(v);
  if (a >= 1e9) return (a / 1e9).toFixed(1).replace('.', ',') + ' млрд';
  if (a >= 1e6) return Math.round(a / 1e6) + ' млн';
  return Math.round(a / 1e3) + ' тыс';
}

// Компрессия площади (те же показатели степени, что на HeatmapPage): секторы —
// слабая (0.85, крупный сектор не съедает всё), акции внутри сектора — сильная
// (0.55, мелкие тикеры не схлопываются в невидимые полоски).
const sizeVal = (v: number | null | undefined, pow: number): number => Math.pow(Math.max(v ?? 0, 1), pow);

const GAP = 3;
const HEADER_H = 18;
const MIN_TILE_AREA = 14; // если под плитки в секторе остаётся меньше — прячем и header

export default function EmbedHeatmap() {
  const { rd, wr } = useEmbedPersist();

  const [universe, setUniverse] = useState<Universe>(() => rd('frame:embed:heatmap:universe', 'imoex') as Universe);
  const [metric, setMetric] = useState<Metric>(() => rd('frame:embed:heatmap:metric', 'change') as Metric);
  const [data, setData] = useState<HeatmapResponse | null>(null);
  const [status, setStatus] = useState<LoadStatus>('loading');
  const [exportOpen, setExportOpen] = useState(false);

  // Compact-режим тулбара (узкая панель sandbox — см. useToolbarCompact.ts).
  const { wrapRef: toolbarWrapRef, measureRef: toolbarMeasureRef, compact: toolbarCompact } = useToolbarCompact();

  useEffect(() => { wr('frame:embed:heatmap:universe', universe); }, [universe]);
  useEffect(() => { wr('frame:embed:heatmap:metric', metric); }, [metric]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    const load = universe === 'imoex'
      ? getHeatmapImoex('change_1d', 'sector')
      : getHeatmapData('value_1d', 'change_1d', 'sector');
    load
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setStatus((res?.sectors?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/heatmap load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [universe]);

  // Резиновый размер контейнера — squarify заново раскладывает плитки под
  // текущие width/height панели (нет фиксированной высоты и прокрутки).
  const boxRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const r = entries[0]?.contentRect;
      if (r && r.width > 0 && r.height > 0) setSize({ w: r.width, h: r.height });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const sectors = useMemo(() => data?.sectors ?? [], [data]);

  // Двухуровневый treemap: секторы (площадь ~ Σ pow(оборот,0.85)) → внутри
  // каждого сектора акции (площадь ~ pow(оборот,0.55)). sectorItems СОРТИРУЕМ
  // явно перед squarify (не полагаемся на совпадение с исходным порядком) —
  // squarify возвращает rects в порядке своей ВНУТРЕННЕЙ сортировки по value,
  // зип по индексу корректен только если наш массив уже в этом же порядке.
  const layout = useMemo(() => {
    if (!sectors.length || size.w <= 0 || size.h <= 0) return null;
    const sectorItems = sectors
      .map((s) => ({
        id: s.name,
        value: s.stocks.reduce((sum, st) => sum + sizeVal(st.value_1d, 0.85), 0),
        data: s,
      }))
      .sort((a, b) => b.value - a.value);
    const sectorRects = squarify(sectorItems, 0, 0, size.w, size.h);

    const labels: { name: string; x: number; y: number; w: number }[] = [];
    const tiles: SquarifyRect<HeatmapStock>[] = [];
    sectorRects.forEach((sr, i) => {
      const sec = sectorItems[i]?.data;
      if (!sec) return;
      const tileH = sr.height - HEADER_H - GAP;
      if (tileH < MIN_TILE_AREA) return; // сектору некуда положить плитки — скипаем и header
      labels.push({ name: sec.name, x: sr.x, y: sr.y, w: sr.width });
      const stockItems = sec.stocks
        .map((st) => ({ id: st.secId, value: sizeVal(st.value_1d, 0.55), data: st }))
        .sort((a, b) => b.value - a.value);
      tiles.push(...squarify(stockItems, sr.x + GAP, sr.y + HEADER_H, sr.width - GAP * 2, tileH));
    });
    return { labels, tiles };
  }, [sectors, size]);

  return (
    <EmbedFrame
      toolbarUnified
      toolbar={
        <div ref={toolbarWrapRef} style={{ position: 'relative', display: 'flex', alignItems: 'center', gap: 6, flex: 1, minWidth: 0, overflow: 'hidden' }}>
          {/* Невидимый измеритель — см. useToolbarCompact.ts: всегда полные лейблы. */}
          <div ref={toolbarMeasureRef} aria-hidden style={{ position: 'absolute', top: 0, left: 0, visibility: 'hidden', pointerEvents: 'none', display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
            <Dropdown<Universe> value={universe} options={UNIVERSES} onChange={setUniverse} title="Вселенная" icon={UNIVERSE_ICONS[universe]} />
            <PillGroup<Metric> value={metric} options={METRICS} onChange={setMetric} />
          </div>
          <Dropdown<Universe> value={universe} options={UNIVERSES} onChange={setUniverse} title="Вселенная" icon={UNIVERSE_ICONS[universe]} compact={toolbarCompact} />
          <PillGroup<Metric> value={metric} options={METRICS} onChange={setMetric} compact={toolbarCompact} />
        </div>
      }
      actions={
        status === 'ok' && layout ? (
          <button
            type="button"
            onClick={() => setExportOpen(true)}
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
        ) : undefined
      }
    >
      <div ref={boxRef} style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}>
        {status === 'ok' && layout && (() => {
          // Резолвим var(--text-secondary) ОДИН раз на рендер — см. resolveColor
          // выше: html2canvas при экспорте рисует ТОЛЬКО первый <svg> из
          // нескольких СОСЕДНИХ корневых <svg> (по одному на сектор) — остальные
          // молча пропадают (не обрезаются — их просто нет в снимке), даже с уже
          // резолвленным literal-цветом. Один общий <svg> с N дочерних <text> —
          // тот же приём, что и per-item svg, но БЕЗ множественных sibling-корней,
          // на которых html2canvas и спотыкается.
          const labelFill = boxRef.current ? resolveColor(boxRef.current, 'var(--text-secondary)') : '#9A958C';
          return (
          <div style={{ position: 'relative', width: '100%', height: '100%' }}>
            <svg
              style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }}
              width={size.w}
              height={size.h}
            >
              {layout.labels.map((l) => (
                <text
                  key={l.name}
                  x={l.x + 4}
                  y={l.y + 2 + 6.5}
                  dominantBaseline="central"
                  fontSize={10}
                  fontWeight={600}
                  style={{ textTransform: 'uppercase', letterSpacing: '0.06em' }}
                  fill={labelFill}
                >
                  {l.name}
                </text>
              ))}
            </svg>
            {layout.tiles.map((t) => {
              const st = t.data;
              const ch = st.change_1d ?? 0;
              const showTicker = t.width > 24 && t.height > 16;
              const showSub = t.width > 42 && t.height > 30;
              const fs = Math.max(9, Math.min(18, Math.floor(Math.min(t.width / 5, t.height / 3))));
              return (
                <div
                  key={st.secId}
                  title={`${st.name} · ${fmtPct(ch)} · оборот ${fmtVol(st.value_1d ?? 0)} ₽`}
                  style={{
                    position: 'absolute',
                    left: t.x + 1, top: t.y + 1, width: Math.max(0, t.width - 2), height: Math.max(0, t.height - 2),
                    borderRadius: 5, background: heatColor(ch),
                    display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 2,
                    overflow: 'hidden', cursor: 'default',
                  }}
                >
                  {/* Заливка плитки всегда тёмная (earth-tone, независимо от темы страницы,
                      как на сайте) → текст всегда белый, не var(--text-primary) (в светлой
                      теме он тёмный — нечитаем на тёмном тайле). */}
                  {showTicker && (
                    <span style={{ fontSize: fs, fontWeight: 800, color: '#fff', letterSpacing: '-0.01em' }}>
                      {st.secId}
                    </span>
                  )}
                  {showSub && (
                    <span style={{ fontSize: Math.round(fs * 0.72), fontWeight: 700, color: '#fff', opacity: 0.85, fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontVariantNumeric: 'tabular-nums' }}>
                      {metric === 'change' ? fmtPct(ch) : fmtVol(st.value_1d ?? 0)}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
          );
        })()}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
        {exportOpen && boxRef.current && (
          <Suspense fallback={null}>
            <ExportModal
              targetElement={boxRef.current}
              filename={`frame-heatmap-${universe}`}
              metadata={{
                title: 'Карта рынка',
                details: [universe === 'imoex' ? 'Индекс IMOEX' : 'Все акции', METRICS.find((m) => m.id === metric)?.label].filter((x): x is string => !!x),
              }}
              onClose={() => setExportOpen(false)}
            />
          </Suspense>
        )}
      </div>
    </EmbedFrame>
  );
}
