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
import { useEffect, useMemo, useRef, useState } from 'react';
import { useTheme } from '../../contexts/ThemeContext';
import { getHeatmapData, getHeatmapImoex, type HeatmapResponse, type HeatmapStock } from '../../services/api';
import { squarify, type SquarifyRect } from '../../utils/squarify';
import { EmbedMsg } from './embedUi';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';

type Universe = 'imoex' | 'all';
type Metric = 'change' | 'vol';
type LoadStatus = 'loading' | 'ok' | 'empty' | 'error';

const UNIVERSES: { id: Universe; label: string }[] = [
  { id: 'imoex', label: 'Индекс IMOEX' },
  { id: 'all', label: 'Все акции' },
];
const METRICS: { id: Metric; label: string; title: string }[] = [
  { id: 'change', label: 'Изм. %', title: 'Дневное изменение' },
  { id: 'vol', label: 'Объём', title: 'Оборот за день' },
];

// Цвет плитки (порт heatColor мокапа): мешаем зелёный/красный к фону темы,
// интенсивность по |изменению| (насыщение к ±4%). Ноль — нейтральный фон.
function heatColor(ch: number, dark: boolean): string {
  const base: [number, number, number] = dark ? [26, 26, 30] : [240, 236, 226];
  const tint: [number, number, number] = ch >= 0 ? [91, 212, 156] : [239, 111, 111];
  const a = Math.min(1, Math.abs(ch) / 4);
  const k = 0.25 + a * 0.7;
  const mix = (i: number) => Math.round(base[i] + (tint[i] - base[i]) * k);
  return `rgb(${mix(0)},${mix(1)},${mix(2)})`;
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
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const [universe, setUniverse] = useState<Universe>(() => rd('frame:embed:heatmap:universe', 'imoex') as Universe);
  const [metric, setMetric] = useState<Metric>(() => rd('frame:embed:heatmap:metric', 'change') as Metric);
  const [data, setData] = useState<HeatmapResponse | null>(null);
  const [status, setStatus] = useState<LoadStatus>('loading');

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
      toolbar={
        <>
          <Dropdown<Universe> value={universe} options={UNIVERSES} onChange={setUniverse} title="Вселенная" />
          <PillGroup<Metric> value={metric} options={METRICS} onChange={setMetric} />
        </>
      }
    >
      <div ref={boxRef} style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}>
        {status === 'ok' && layout && (
          <div style={{ position: 'relative', width: '100%', height: '100%' }}>
            {layout.labels.map((l) => (
              <div
                key={l.name}
                style={{
                  position: 'absolute', left: l.x + 4, top: l.y + 2, width: Math.max(0, l.w - 8),
                  fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em',
                  color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}
              >
                {l.name}
              </div>
            ))}
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
                    borderRadius: 5, background: heatColor(ch, dark),
                    display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 2,
                    overflow: 'hidden', cursor: 'default',
                  }}
                >
                  {showTicker && (
                    <span style={{ fontSize: fs, fontWeight: 800, color: 'var(--text-primary)', letterSpacing: '-0.01em' }}>
                      {st.secId}
                    </span>
                  )}
                  {showSub && (
                    <span style={{ fontSize: Math.round(fs * 0.72), fontWeight: 700, color: 'var(--text-primary)', opacity: 0.85, fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontVariantNumeric: 'tabular-nums' }}>
                      {metric === 'change' ? fmtPct(ch) : fmtVol(st.value_1d ?? 0)}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedFrame>
  );
}
