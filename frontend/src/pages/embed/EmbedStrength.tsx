/**
 * EmbedStrength — виджет «Сила рынка». Полный паритет со страницей:
 * опционально сверху линия индекса (IndexChart: IMOEX в ₽ / RTSI в $), снизу —
 * breadth (% акций выше EMA) линией или гистограммой. Все контролы (EMA / период /
 * режим графика / вселенная / валюта / показ индекса) — в drawer'е настроек.
 * Виджет целиком под PRO-токеном, поэтому тир-гейтинга нет.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import IndexChart from '../../components/strength/IndexChart';
import BreadthChart from '../../components/strength/BreadthChart';
import type { ChartPadding } from '../../components/strength/chartUtils';
import { getBreadthHistory, type BreadthUniverse } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { DrawerSection, SegGroup, ToggleRow } from './EmbedSettings';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { readLS, writeLS } from './embedPersist';

type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type Synced = { time: string; breadth: number; imoex: number }[];
type Period = '1y' | '5y' | 'all';
type ChartMode = 'line' | 'histogram';
type Ema = 20 | 50 | 100 | 200;
type UniverseBase = 'imoex' | 'all';
type Currency = 'rub' | 'usd';

const EMAS: { id: Ema; label: string }[] = [
  { id: 20, label: 'EMA 20' },
  { id: 50, label: 'EMA 50' },
  { id: 100, label: 'EMA 100' },
  { id: 200, label: 'EMA 200' },
];
const PERIODS: { id: Period; label: string }[] = [
  { id: '1y', label: '1Г' },
  { id: '5y', label: '5Л' },
  { id: 'all', label: 'Всё' },
];
const CHART_MODES: { id: ChartMode; label: string }[] = [
  { id: 'line', label: 'Линия' },
  { id: 'histogram', label: 'Гистограмма' },
];
// Общий padding для обоих графиков → их X-оси совпадают по пикселям.
// Ссылка стабильна между рендерами — IndexChart/BreadthChart держат padding
// в useMemo-deps, пересоздание объекта прервало бы морфинг-анимацию.
const PAD: ChartPadding = { left: 54, right: 54, top: 6, bottom: 20 };
const LEGEND_H = 18;

// Значения дней зеркалят PERIOD_DAYS на StrengthPage — паритет диапазонов.
function daysForPeriod(period: Period): number {
  switch (period) {
    case '5y': return 1825;
    case 'all': return 7000;
    default: return 365;
  }
}

function MiniLegend({ text }: { text: string }) {
  return (
    <div
      style={{
        flexShrink: 0,
        height: LEGEND_H,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: 11,
        fontWeight: 700,
        color: 'var(--text-secondary)',
      }}
    >
      {text}
    </div>
  );
}

export default function EmbedStrength() {
  const [params] = useSearchParams();

  const [ema, setEma] = useState<Ema>(() => (Number(readLS('frame:embed:strength:ema', '200')) || 200) as Ema);
  const [period, setPeriod] = useState<Period>(() => (params.get('period') || readLS('frame:embed:strength:period', '1y')) as Period);
  const [chartMode, setChartMode] = useState<ChartMode>(() => readLS('frame:embed:strength:chartMode', 'histogram') as ChartMode);
  const [universeBase, setUniverseBase] = useState<UniverseBase>(() => readLS('frame:embed:strength:universeBase', 'imoex') as UniverseBase);
  const [currency, setCurrency] = useState<Currency>(() => readLS('frame:embed:strength:currency', 'rub') as Currency);
  const [showPrice, setShowPrice] = useState<boolean>(() => readLS('frame:embed:strength:showPrice', 'true') !== 'false');

  const [synced, setSynced] = useState<Synced>([]);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { writeLS('frame:embed:strength:ema', String(ema)); }, [ema]);
  useEffect(() => { writeLS('frame:embed:strength:period', period); }, [period]);
  useEffect(() => { writeLS('frame:embed:strength:chartMode', chartMode); }, [chartMode]);
  useEffect(() => { writeLS('frame:embed:strength:universeBase', universeBase); }, [universeBase]);
  useEffect(() => { writeLS('frame:embed:strength:currency', currency); }, [currency]);
  useEffect(() => { writeLS('frame:embed:strength:showPrice', String(showPrice)); }, [showPrice]);

  // Итоговый universe — как на странице: добавляем _usd в долларовом режиме.
  const universe: BreadthUniverse = currency === 'usd'
    ? `${universeBase}_usd` as BreadthUniverse
    : universeBase;

  // Лейбл верхнего графика всегда отражает реально нарисованный индекс
  // (IMOEX в ₽ / RTSI в $) и НЕ зависит от вселенной breadth-метрики.
  const indexLegend = currency === 'usd' ? 'Индекс РТС (RTSI)' : 'Индекс МосБиржи (IMOEX)';

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getBreadthHistory(ema, daysForPeriod(period), universe)
      .then((res) => {
        if (cancelled) return;
        const imoexMap = new Map((res?.imoex ?? []).map((d) => [d.date, d.close]));
        const s: Synced = (res?.data ?? [])
          .filter((d) => imoexMap.has(d.date))
          .map((d) => ({ time: d.date, breadth: d.percent_above, imoex: imoexMap.get(d.date)! }));
        setSynced(s);
        setStatus(s.length ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/strength load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [ema, period, universe]);

  // Измеряем контейнер. Когда индекс показан — делим высоту 55/45 (минус две
  // легенды). Когда скрыт — breadth занимает всю доступную высоту.
  const boxRef = useRef<HTMLDivElement>(null);
  const [boxH, setBoxH] = useState(420);
  useEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setBoxH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const { indexH, breadthH } = useMemo(() => {
    if (showPrice) {
      const avail = Math.max(160, boxH - LEGEND_H * 2);
      const idx = Math.round(avail * 0.55);
      return { indexH: idx, breadthH: avail - idx };
    }
    // Соло-режим: одна легенда сверху, остаток — breadth.
    const avail = Math.max(120, boxH - LEGEND_H);
    return { indexH: 0, breadthH: avail };
  }, [showPrice, boxH]);

  // ── Hover (перекрестие + значения) — порт со StrengthPage, но px4=0: в embed нет
  // px-4 обёрток графиков, только PAD. Двигаем hoverIndex от мыши → оба графика
  // (IndexChart/BreadthChart) сами рисуют своё перекрестие+точку по общему X.
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const rafRef = useRef<number | null>(null);
  const updateHover = useCallback((clientX: number) => {
    if (!boxRef.current || !synced.length) return;
    if (rafRef.current !== null) return; // throttle до одного RAF
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = null;
      if (!boxRef.current) return;
      const rect = boxRef.current.getBoundingClientRect();
      const x = clientX - rect.left - PAD.left;
      const chartW = rect.width - PAD.left - PAD.right;
      if (x < 0 || x > chartW) { setHoverIndex(null); return; } // в жёлобе оси — прячем
      const idx = Math.round((x / chartW) * (synced.length - 1));
      setHoverIndex(idx >= 0 && idx < synced.length ? idx : null);
    });
  }, [synced.length]);
  const handleLeave = useCallback(() => {
    if (rafRef.current !== null) { cancelAnimationFrame(rafRef.current); rafRef.current = null; }
    setHoverIndex(null);
  }, []);
  const hover = hoverIndex !== null ? synced[hoverIndex] : null;
  const indexShort = currency === 'usd' ? 'RTSI' : 'IMOEX';
  const fmtIdx = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 });

  return (
    <EmbedFrame
      toolbar={
        <>
          <PillGroup<ChartMode> value={chartMode} options={CHART_MODES} onChange={setChartMode} />
          <Dropdown<UniverseBase>
            value={universeBase}
            options={[
              { id: 'imoex', label: currency === 'usd' ? 'Индекс RTSI' : 'Индекс IMOEX' },
              { id: 'all', label: '100 акций' },
            ]}
            onChange={setUniverseBase}
            title="Вселенная"
          />
          <PillGroup<Currency> value={currency} options={[{ id: 'rub', label: '₽' }, { id: 'usd', label: '$' }]} onChange={setCurrency} />
          <Dropdown<Period> value={period} options={PERIODS} onChange={setPeriod} title="Период" />
        </>
      }
      more={
        <>
          <DrawerSection label="Скользящая (EMA)">
            <SegGroup<Ema> value={ema} options={EMAS} onChange={setEma} />
          </DrawerSection>
          <DrawerSection label="Индекс сверху">
            <ToggleRow
              label={currency === 'usd' ? 'Показывать RTS' : 'Показывать IMOEX'}
              checked={showPrice}
              onChange={setShowPrice}
            />
          </DrawerSection>
        </>
      }
    >
      <div
        ref={boxRef}
        style={{ position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}
        onMouseMove={(e) => updateHover(e.clientX)}
        onMouseLeave={handleLeave}
        onTouchStart={(e) => { if (e.touches[0]) updateHover(e.touches[0].clientX); }}
        onTouchMove={(e) => { if (e.touches[0]) updateHover(e.touches[0].clientX); }}
        onTouchEnd={handleLeave}
      >
        {status === 'ok' && synced.length > 0 ? (
          <>
            {showPrice && (
              <>
                <MiniLegend text={hover ? `${indexShort} ${fmtIdx(hover.imoex)}` : indexLegend} />
                <IndexChart syncedData={synced} hoverIndex={hoverIndex} height={indexH} padding={PAD} />
              </>
            )}
            <MiniLegend text={hover ? `${hover.breadth.toFixed(1)}% выше EMA${ema}` : `% акций выше EMA${ema}`} />
            <BreadthChart
              syncedData={synced}
              hoverIndex={hoverIndex}
              height={breadthH}
              mode={chartMode}
              padding={PAD}
              showWatermark={!showPrice}
            />
          </>
        ) : (
          <>
            {status === 'loading' && <EmbedMsg text="Загрузка…" />}
            {status === 'empty' && <EmbedMsg text="Нет данных" />}
            {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
          </>
        )}
      </div>
    </EmbedFrame>
  );
}
