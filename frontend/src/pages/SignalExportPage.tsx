/**
 * SignalExportPage — минимальная страница для headless-render'а signal-постов.
 *
 * URL: /signal-export?ticker=SR&clgroup=FIZ&date=2026-05-15&period=1y
 *
 * - Размер ровно 1280×720 (соответствует viewport Playwright).
 * - Нет nav/footer/header сайта — только дamage frame + chart + рамка как в
 *   composeFramedCanvas (header с asset+ticker+details, footer с URL+датой).
 * - SimpleChart переиспользуется как есть (показ цены + OI overlay).
 * - Когда данные загружены и chart прорисовался — устанавливаем
 *   data-render-ready="true" на root → Playwright делает screenshot.
 *
 * НЕ используется человеком в браузере. Тёмная тема всегда. Не зарегистрирован
 * в навигации.
 */
import { useEffect, useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../components/SimpleChart';
import { getChartData } from '../services/api';
import { displayTicker } from '../utils/displayTicker';
import { formatNumber, formatPrice } from '../utils/formatNumber';

// Derive типа из функции — она возвращает локальный (не-exported) тип.
type ChartData = Awaited<ReturnType<typeof getChartData>>;

const CANVAS_W = 1280;
const CANVAS_H = 720;
const PAD = 24;
const HEADER_H = 56;
const FOOTER_H = 28;

export default function SignalExportPage() {
  const [params] = useSearchParams();
  const ticker = params.get('ticker') || '';
  const clgroup = (params.get('clgroup') || 'FIZ') as 'FIZ' | 'YUR';
  const dateStr = params.get('date') || '';
  const period = params.get('period') || '6m';
  const theme = params.get('theme') || 'editorial-light';
  const instrumentName = params.get('name') || '';

  const [data, setData] = useState<ChartData | null>(null);
  const [ready, setReady] = useState(false);

  // Force тему (default editorial-light как на UI-экспорте графика)
  useEffect(() => {
    const prev = document.documentElement.getAttribute('data-theme');
    document.documentElement.setAttribute('data-theme', theme);
    return () => {
      if (prev) document.documentElement.setAttribute('data-theme', prev);
    };
  }, [theme]);

  useEffect(() => {
    if (!ticker) return;
    let cancelled = false;
    getChartData(ticker, ticker, 'futures', 24, clgroup, true, period)
      .then((result) => {
        if (cancelled) return;
        setData(result);
      })
      .catch((err) => {
        console.error('signal-export load failed:', err);
      });
    return () => { cancelled = true; };
  }, [ticker, clgroup, period]);

  // После того как SimpleChart смонтировался с данными — небольшая пауза
  // на анимацию SVG-paths, затем ставим ready=true → Playwright делает screenshot.
  useEffect(() => {
    if (!data) return;
    const t = setTimeout(() => setReady(true), 1500);
    return () => clearTimeout(t);
  }, [data]);

  // Подготовка данных как на OI page
  const chartData = useMemo(() =>
    (data?.candles ?? []).map((c: { time: string; close: number }) => ({
      time: c.time,
      value: c.close,
    })),
  [data]);

  const oiData = useMemo(() =>
    (data?.open_interest ?? [])
      .filter((o: { net_position: number | null }) => o.net_position !== null)
      .map((o: { time: string; net_position: number | null }) => ({
        time: o.time,
        value: o.net_position as number,  // чистая позиция = pos_long + pos_short
      })),
  [data]);

  const displayName = instrumentName || ticker;
  const displayedTicker = displayTicker(ticker);
  const groupLabel = clgroup === 'FIZ' ? 'Физлица' : 'Юрлица';

  const formattedDate = useMemo(() => {
    if (!dateStr) return '';
    try {
      const s = new Date(dateStr).toLocaleDateString('ru-RU', {
        day: 'numeric', month: 'long', year: 'numeric',
      });
      // toLocaleDateString для ru-RU добавляет ' г.' automatically; гарантируем формат
      return s.endsWith(' г.') ? s : `${s} г.`;
    } catch {
      return dateStr;
    }
  }, [dateStr]);

  const periodLabel = useMemo(() => {
    const map: Record<string, string> = {
      '1d': '1Д', '1w': '1Н', '1m': '1М', '3m': '3M',
      '6m': '6M', '1y': '1Y', '2y': '2Y', '5y': '5Y', 'all': 'Всё',
    };
    return map[period] ?? period.toUpperCase();
  }, [period]);

  return (
    <div
      data-signal-export-root="true"
      data-render-ready={ready ? 'true' : 'false'}
      style={{
        width: CANVAS_W,
        height: CANVAS_H,
        background: 'var(--bg-base)',
        color: 'var(--text-primary)',
        padding: PAD,
        display: 'flex',
        flexDirection: 'column',
        fontFamily:
          '-apple-system, BlinkMacSystemFont, "Segoe UI", Inter, system-ui, sans-serif',
        boxSizing: 'border-box',
        overflow: 'hidden',
      }}
    >
      {/* Header — primary line + ticker badge + subtitle */}
      <div style={{ height: HEADER_H, flexShrink: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{ fontSize: 28, fontWeight: 700, letterSpacing: '-0.01em' }}>
            {displayName}
          </div>
          <div
            style={{
              background: 'var(--accent, #FF5C2B)',
              color: '#fff',
              fontWeight: 700,
              fontSize: 18,
              padding: '4px 8px',
              borderRadius: 4,
            }}
          >
            {displayedTicker}
          </div>
        </div>
        <div
          style={{
            color: 'var(--text-secondary)',
            fontSize: 15,
            marginTop: 6,
          }}
        >
          Открытые позиции · 1Д · {periodLabel} · {groupLabel} · Позиции
        </div>
      </div>

      {/* Chart */}
      <div style={{ flex: 1, position: 'relative', marginTop: 8 }}>
        {data && (
          <SimpleChart
            data={chartData}
            secondaryData={oiData}
            showPrimary={true}
            showSecondary={true}
            primaryLabel={displayName}
            secondaryLabel="Чистая позиция"
            primaryColor="var(--chart-line-1)"
            secondaryColor="var(--accent)"
            height={CANVAS_H - HEADER_H - FOOTER_H - PAD * 2 - 24}
            showDownloadButton={false}
            showNavigator={false}
            showValueHeader={false}
            legendPosition="top"
            formatValue={formatPrice}
            formatSecondaryValue={(v) => formatNumber(v, 0)}
          />
        )}
        {/* Marker удалён — на эталоне UI-экспорта его нет, blue/orange value-tags
            на правой оси SimpleChart рисует сам. Если потом понадобится marker,
            добавим обратно (markerPos уже считается). */}
      </div>

      {/* Footer */}
      <div
        style={{
          height: FOOTER_H,
          flexShrink: 0,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          color: 'var(--text-secondary)',
          fontSize: 13,
        }}
      >
        <div>таймфрейм.рф</div>
        <div>{formattedDate}</div>
      </div>
    </div>
  );
}
