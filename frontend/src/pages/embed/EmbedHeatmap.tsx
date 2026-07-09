/**
 * EmbedHeatmap — «Карта рынка» для панели песочницы/расширения (§6.9 макета).
 * НЕ Lightweight-график: flex-тримап из плиток (порт heatmapBody мокапа) —
 * размер ∝ обороту, цвет = дневное изменение (плавный зелёный↔красный, ноль
 * нейтрален), группировка по секторам с uc-подписями. Скролл-список, как скринер.
 *
 * Данные — те же эндпоинты, что у полной страницы (HeatmapPage не трогаем):
 * вселенная IMOEX (/api/heatmap/imoex) или все акции (/api/heatmap/data).
 * Метрика меняет ЗНАЧЕНИЕ в плитке (изм.% / оборот); цвет всегда по change_1d.
 */
import { useEffect, useMemo, useState } from 'react';
import { useTheme } from '../../contexts/ThemeContext';
import { getHeatmapData, getHeatmapImoex, type HeatmapResponse } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { readLS, writeLS } from './embedPersist';

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

export default function EmbedHeatmap() {
  const { theme } = useTheme();
  const dark = theme !== 'editorial-light';

  const [universe, setUniverse] = useState<Universe>(() => readLS('frame:embed:heatmap:universe', 'imoex') as Universe);
  const [metric, setMetric] = useState<Metric>(() => readLS('frame:embed:heatmap:metric', 'change') as Metric);
  const [data, setData] = useState<HeatmapResponse | null>(null);
  const [status, setStatus] = useState<LoadStatus>('loading');

  useEffect(() => { writeLS('frame:embed:heatmap:universe', universe); }, [universe]);
  useEffect(() => { writeLS('frame:embed:heatmap:metric', metric); }, [metric]);

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

  // Секторы по убыванию оборота; внутри — акции по убыванию веса.
  const sectors = useMemo(() => {
    const list = (data?.sectors ?? []).slice().sort((a, b) => b.totalValue - a.totalValue);
    return list.map((s) => ({
      name: s.name,
      total: Math.max(1, s.totalValue),
      stocks: s.stocks.slice().sort((a, b) => (b.value_1d ?? 0) - (a.value_1d ?? 0)),
    }));
  }, [data]);

  return (
    <EmbedFrame
      toolbar={
        <>
          <Dropdown<Universe> value={universe} options={UNIVERSES} onChange={setUniverse} title="Вселенная" />
          <PillGroup<Metric> value={metric} options={METRICS} onChange={setMetric} />
        </>
      }
    >
      <div className="styled-scrollbar" style={{ position: 'absolute', inset: 0, overflowY: 'auto', padding: '4px 8px 8px' }}>
        {status === 'ok' && sectors.map((sec) => (
          <div key={sec.name} style={{ marginBottom: 10 }}>
            <div style={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.07em', color: 'var(--text-secondary)', padding: '4px 2px' }}>
              {sec.name}
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
              {sec.stocks.map((st) => {
                const w = Math.max(0.02, (st.value_1d ?? 0) / sec.total);
                const big = w >= 0.18;
                const ch = st.change_1d ?? 0;
                return (
                  <div
                    key={st.secId}
                    title={`${st.name} · ${fmtPct(ch)} · оборот ${fmtVol(st.value_1d ?? 0)} ₽`}
                    style={{
                      flex: `${Math.round(w * 100)} 1 ${Math.max(56, Math.round(w * 340))}px`,
                      height: big ? 62 : 46,
                      borderRadius: 6,
                      background: heatColor(ch, dark),
                      display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 2,
                      overflow: 'hidden', minWidth: 0, cursor: 'default',
                    }}
                  >
                    <span style={{ fontSize: big ? 12 : 10.5, fontWeight: 800, color: 'var(--text-primary)', letterSpacing: '-0.01em' }}>
                      {st.secId}
                    </span>
                    <span style={{ fontSize: big ? 10.5 : 9.5, fontWeight: 700, color: 'var(--text-primary)', opacity: 0.85, fontFamily: 'var(--font-mono, ui-monospace, monospace)', fontVariantNumeric: 'tabular-nums' }}>
                      {metric === 'change' ? fmtPct(ch) : fmtVol(st.value_1d ?? 0)}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedFrame>
  );
}
