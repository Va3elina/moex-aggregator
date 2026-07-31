/**
 * ChartLab — стенд для БАГОВ ВЁРСТКИ графика. Только для разработки (`/chart-lab`,
 * в прод-сборке роут редиректит на главную).
 *
 * Зачем. Почти все дефекты чарта — геометрические: оси разъезжаются, кроссхэйр
 * не совпадает между панелями, оверлей лезет на цифры. Чтобы увидеть такой баг
 * в песочнице, нужны поднятая БД, бэкенд, логин и живые данные — четыре точки
 * отказа, ни одна из которых к самому багу отношения не имеет. Здесь данные
 * синтетические, зависимостей ноль, страница открывается за секунду.
 *
 * И главное — снизу ЗАМЕР геометрии по кнопке. «Кажется, линия съехала» — плохой
 * баг-репорт даже от самого себя; «поле панели 0 начинается на 74px правее, чем
 * у панели 1» — точный. Скриншот оставляем на финальное подтверждение.
 */
import { useCallback, useMemo, useRef, useState, type CSSProperties } from 'react';
import LwChartPanes, { type LwPane } from '../../components/LwChartPanes';
import type { LwSeries } from '../../components/chart/lwTypes';
import { IndicatorList, PaneIndicatorList, indicatorValues, useIndicators, useIndicatorSeries, useVolumeProfileSpec } from '../embed/EmbedIndicators';
import type { NativeRow } from '../embed/EmbedIndicators';

const DAY = 86400;
const BARS = 400;

/** Детерминированный псевдослучайный ряд: стенд должен выглядеть одинаково на
 *  каждой перезагрузке, иначе «баг пропал» невозможно отличить от «данные другие».
 *  Math.random здесь был бы прямым вредительством. */
function lcg(seed: number) {
  let s = seed >>> 0;
  return () => ((s = (s * 1664525 + 1013904223) >>> 0) / 4294967296);
}

interface Bar { time: string; value: number; open: number; high: number; low: number; close: number; volume: number }

function genBars(): Bar[] {
  const rnd = lcg(20260731);
  const out: Bar[] = [];
  let px = 74000;
  const t0 = Math.floor(Date.UTC(2025, 8, 1) / 1000);
  for (let i = 0; i < BARS; i++) {
    const drift = Math.sin(i / 40) * 900;
    const open = px;
    const close = Math.max(1000, open + (rnd() - 0.5) * 1400 + drift * 0.05);
    const high = Math.max(open, close) + rnd() * 600;
    const low = Math.min(open, close) - rnd() * 600;
    px = close;
    out.push({
      time: String(t0 + i * DAY), value: close, open, high, low, close,
      volume: Math.round(2000 + rnd() * 18000),
    });
  }
  return out;
}

export default function ChartLabPage() {
  const bars = useMemo(genBars, []);
  const inds = useIndicators('frame:chartlab:indicators');
  const boxRef = useRef<HTMLDivElement | null>(null);
  const [showPrice, setShowPrice] = useState(true);
  // Тумблер темы — без него светлую тему в стенде воспроизвести нечем, а
  // «на светлой пропали линии» это ровно тот класс багов, ради которого стенд и
  // заведён. data-theme ставим на СУЩЕСТВУЮЩИЙ бокс, а не на новую обёртку:
  // от неё зависит --lw-axis-left (публикуется на родителя корня чарта) и
  // положение списка индикаторов.
  const [dark, setDark] = useState(true);

  const toSec = useMemo(() => (t: string) => Number(t), []);
  const indSeries = useIndicatorSeries(inds.list, bars, toSec, inds.colorOf);
  const vpSpec = useVolumeProfileSpec(inds.list, showPrice ? bars : EMPTY, 'price', inds.colorOf);

  // Ровно та же раскладка, что у окна ОИ: цена на ЛЕВОЙ оси, второй ряд на
  // правой. Именно из-за левой оси только на верхней панели и разъезжается
  // геометрия — стенд обязан это воспроизводить, а не упрощать.
  const native = useMemo<LwSeries[]>(() => {
    const out: LwSeries[] = [];
    if (showPrice) {
      out.push({
        id: 'price', type: 'candlestick', scale: 'left', color: '#5DA3E9', lineWidth: 2,
        label: 'Цена (синтетика)', minMove: 1,
        data: bars.map((b) => ({ time: toSec(b.time), value: b.close, open: b.open, high: b.high, low: b.low, close: b.close })),
      });
    }
    out.push({
      id: 'oi', type: 'line', scale: 'right', color: 'var(--accent)', lineWidth: 2, label: 'Чистая позиция',
      data: bars.map((b, i) => ({ time: toSec(b.time), value: Math.round(Math.sin(i / 25) * 40000 + 60000) })),
    });
    return out;
  }, [bars, showPrice, toSec]);

  const panes = useMemo<LwPane[]>(() => {
    const used = [...new Set(inds.list.filter((i) => i.pane > 0).map((i) => i.pane))].sort((a, b) => a - b);
    const extra = used.map((pane) => ({ pane, series: indSeries[pane] ?? [] }));
    return [
      { series: [...native, ...(indSeries[0] ?? [])], flex: extra.length ? 2.6 : 1 },
      ...extra.map((x) => ({ series: x.series, flex: 1 })),
    ];
  }, [native, indSeries, inds.list]);

  // Соответствие «панель графика → панель индикатора»: пустые схлопываются.
  const paneMap = useMemo(
    () => [...new Set(inds.list.filter((i) => i.pane > 0).map((i) => i.pane))].sort((a, b) => a - b),
    [inds.list],
  );

  // Значения под курсором для строк индикаторов: в терминалах строка показывает
  // то, на чём стоит курсор, а не последний бар.
  const [hoverVals, setHoverVals] = useState<Record<string, number> | null>(null);
  const indValues = useMemo(() => indicatorValues(indSeries, hoverVals), [indSeries, hoverVals]);

  const rows = useMemo<NativeRow[]>(() => [
    { id: 'price', label: 'Цена (синтетика)', color: '#5DA3E9', visible: showPrice, onToggle: () => setShowPrice((v) => !v) },
    { id: 'oi', label: 'Чистая позиция', color: 'var(--accent)', visible: true, onToggle: () => {} },
  ], [showPrice]);

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', background: 'var(--bg-primary, #131316)' }}>
      <div style={{ padding: '8px 12px', fontSize: 12, color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-color, #333)' }}>
        <b style={{ color: 'var(--text-primary)' }}>ChartLab</b> — стенд геометрии, синтетические данные.
        Добавь через «+ Индикатор» RSI и ATR, чтобы получить нижние панели.
        {' · '}
        <button type="button" onClick={() => setDark((v) => !v)} style={{ padding: '1px 8px', borderRadius: 5, cursor: 'pointer', fontSize: 11, border: '1px solid var(--border-color, #444)', background: 'transparent', color: 'var(--text-primary)' }}>
          Тема: {dark ? 'тёмная' : 'светлая'}
        </button>
      </div>
      <div
        ref={boxRef}
        data-theme={dark ? 'editorial-dark' : 'editorial-light'}
        style={{ position: 'relative', flex: 1, minHeight: 0, background: 'var(--bg-primary)' }}
      >
        <LwChartPanes
          panes={panes} hideLegend drawPaneIndex={0} watermark={false} volumeProfile={vpSpec} dark={dark}
          onCrosshairValues={setHoverVals}
          paneOverlay={(i) => (i === 0 ? null : <PaneIndicatorList api={inds} pane={paneMap[i - 1] ?? i} values={indValues} />)}
        />
        <IndicatorList api={inds} native={rows} visible hasVolume values={indValues} />
      </div>
      <Readout />
    </div>
  );
}

const EMPTY: Bar[] = [];

/**
 * Замер геометрии по кнопке. Читает ФАКТИЧЕСКИЕ прямоугольники: где начинается
 * поле каждой панели и где левый край оверлея-списка. Расхождение `поле слева`
 * между панелями — ровно та величина, на которую разъезжается вертикаль
 * кроссхэйра. Мерить ПОСЛЕ того, как график устоялся.
 */
function Readout() {
  const [m, setM] = useState<{ panes: { i: number; plotLeft: number; plotW: number }[]; listLeft: number | null; tick: number }>({ panes: [], listLeft: null, tick: 0 });

  // ⚠️ Замер ПО КНОПКЕ, а не по таймеру. Автообновление я уже писал — оно
  // подвисало на ранних числах (оси ещё нулевой ширины) и показывало их с тем же
  // видом уверенности, что и свежие: дважды отправило чинить уже починенное.
  // Инструмент, которому нельзя верить молча, хуже отсутствующего.
  const measure = useCallback(() => {
    // ⚠️ Панели берём ИЗ ДОКУМЕНТА, а не через React-ссылку на контейнер. Ссылка
    // уже дважды указывала на узлы, которых на экране нет (HMR оставляет
    // отсоединённое дерево), и замер уверенно показывал числа мёртвой копии.
    // На стенде чарт ровно один, так что document-скоуп однозначен.
    const paneEls = Array.from(document.querySelectorAll('[data-lw-pane]')) as HTMLElement[];
    const box = paneEls[0]?.parentElement?.parentElement as HTMLElement | undefined;
    if (!box) return;
    const panes = paneEls.map((el, i) => {
      // НЕ полагаться ни на номер строки таблицы движка, ни на «самый широкий
      // канвас»: строк несколько, а поверх канвасов лежат полноширинные слои
      // рисования. Оба варианта уже показывали числа от другого элемента.
      const tds = Array.from(el.querySelectorAll('table td')) as HTMLElement[];
      const pr = el.getBoundingClientRect();
      let best: DOMRect | null = null;
      for (const td of tds) {
        const r = td.getBoundingClientRect();
        if (!best || r.width > best.width) best = r;
      }
      if (!best) return { i, plotLeft: -1, plotW: -1 };
      return { i, plotLeft: Math.round(best.left - pr.left), plotW: Math.round(best.width) };
    });
    // Только ПРЯМОЙ ребёнок: с тех пор как строки индикаторов переехали внутрь
    // своих панелей, таких оверлеев несколько.
    const list = box.querySelector(':scope > [data-export-ignore="true"]') as HTMLElement | null;
    const listLeft = list ? Math.round(list.getBoundingClientRect().left - box.getBoundingClientRect().left) : null;
    setM({ panes, listLeft, tick: Date.now() % 100000 });
  }, []);

  const lefts = m.panes.map((p) => p.plotLeft);
  const aligned = lefts.length > 1 && lefts.every((x) => x === lefts[0]);
  const cell: CSSProperties = { padding: '2px 10px', fontVariantNumeric: 'tabular-nums' };

  return (
    <div style={{ borderTop: '1px solid var(--border-color, #333)', padding: '6px 12px', fontSize: 11.5, color: 'var(--text-secondary)', background: 'var(--bg-secondary, #17161A)' }}>
      <table style={{ borderCollapse: 'collapse' }}>
        <tbody>
          <tr>
            <td style={cell}>панель</td>
            {m.panes.map((p) => <td key={p.i} style={cell}>#{p.i}</td>)}
          </tr>
          <tr>
            <td style={cell}>поле слева, px</td>
            {m.panes.map((p) => (
              <td key={p.i} style={{ ...cell, color: aligned ? 'var(--text-primary)' : '#EF6F6F', fontWeight: 700 }}>{p.plotLeft}</td>
            ))}
          </tr>
          <tr>
            <td style={cell}>ширина поля, px</td>
            {m.panes.map((p) => <td key={p.i} style={cell}>{p.plotW}</td>)}
          </tr>
        </tbody>
      </table>
      <div style={{ marginTop: 4 }}>
        {m.panes.length > 1 && (
          aligned
            ? <span style={{ color: '#5BD49C' }}>✓ поля панелей выровнены — кроссхэйр совпадёт</span>
            : <span style={{ color: '#EF6F6F' }}>✗ поля разъехались на {Math.max(...lefts) - Math.min(...lefts)}px — вертикаль кроссхэйра сместится ровно на столько</span>
        )}
        {' · '}
        <span style={m.listLeft != null && m.listLeft < (lefts[0] ?? 0) ? { color: '#EF6F6F' } : undefined}>
          список индикаторов слева: {m.listLeft ?? '—'}px (должен быть ≥ поля панели 0: {lefts[0] ?? '—'}px)
        </span>
        {' · '}
        <button type="button" onClick={measure} style={{ padding: '1px 8px', borderRadius: 5, cursor: 'pointer', fontSize: 11, border: '1px solid var(--border-color, #444)', background: 'transparent', color: 'var(--text-primary)' }}>
          Замерить
        </button>
        {m.tick > 0 && <span style={{ opacity: 0.45 }}> · метка {m.tick}</span>}
      </div>
    </div>
  );
}
