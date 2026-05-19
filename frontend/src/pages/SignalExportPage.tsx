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
import { formatNumber } from '../utils/formatNumber';

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
  const period = params.get('period') || '1y';
  const instrumentName = params.get('name') || '';

  const [data, setData] = useState<ChartData | null>(null);
  const [ready, setReady] = useState(false);
  const [markerPos, setMarkerPos] = useState<{ x: number; y: number } | null>(null);

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

  // После того как SimpleChart рендеренmounted с данными — ждём короткий промежуток
  // на анимацию SVG-paths, потом находим last point OI-линии (orange-stroke path)
  // и ставим marker позицию. Затем ready=true → Playwright делает screenshot.
  useEffect(() => {
    if (!data) return;
    const t = setTimeout(() => {
      // Walk SVG, найти secondary (OI) path и взять его endpoint.
      // SimpleChart рисует secondary линию с stroke = secondaryColor (var(--accent)).
      const svg = document.querySelector<SVGSVGElement>('[data-signal-export-root] svg');
      if (svg) {
        // Берём все path-элементы с не-нулевой длиной, выбираем тот что нарисован
        // вторичным цветом (orange). Fallback: последний path в SVG.
        const paths = Array.from(svg.querySelectorAll<SVGPathElement>('path[d]'));
        const accentPath = paths
          .filter((p) => {
            const stroke = p.getAttribute('stroke') || getComputedStyle(p).stroke;
            // Сматчим оранжевый (rgb(255, 92, 43) или var(--accent))
            return /5C2B|255.*92.*43|--accent/i.test(stroke);
          })
          .pop();
        const target = accentPath || paths[paths.length - 1];
        if (target) {
          try {
            const len = target.getTotalLength();
            if (len > 0) {
              const pt = target.getPointAtLength(len);
              // pt в SVG user-space. Конвертируем в screen coords относительно root.
              const ctm = target.getCTM();
              const svgRect = svg.getBoundingClientRect();
              const rootRect =
                document.querySelector('[data-signal-export-root]')?.getBoundingClientRect() ||
                svgRect;
              if (ctm) {
                const screenX = svgRect.left + (pt.x * ctm.a + pt.y * ctm.c + ctm.e);
                const screenY = svgRect.top + (pt.x * ctm.b + pt.y * ctm.d + ctm.f);
                setMarkerPos({
                  x: screenX - rootRect.left,
                  y: screenY - rootRect.top,
                });
              }
            }
          } catch {
            // SVG может ещё не быть готов / path malformed — fallback на approximate
          }
        }
      }
      setReady(true);
    }, 1500);
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
      return new Date(dateStr).toLocaleDateString('ru-RU', {
        day: '2-digit', month: '2-digit', year: 'numeric',
      });
    } catch {
      return dateStr;
    }
  }, [dateStr]);

  return (
    <div
      data-signal-export-root="true"
      data-render-ready={ready ? 'true' : 'false'}
      style={{
        width: CANVAS_W,
        height: CANVAS_H,
        background: 'var(--bg-base, #0B0D12)',
        color: 'var(--text-primary, #f4f4f4)',
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
            color: 'var(--text-secondary, #888)',
            fontSize: 15,
            marginTop: 6,
          }}
        >
          Открытый интерес · {groupLabel} · {period === '1y' ? '1 год' : period}
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
            primaryLabel="Цена"
            secondaryLabel={`Net ${groupLabel.toLowerCase()}`}
            primaryColor="var(--text-primary, #f4f4f4)"
            secondaryColor="var(--accent, #FF5C2B)"
            height={CANVAS_H - HEADER_H - FOOTER_H - PAD * 2 - 24}
            showDownloadButton={false}
            showNavigator={false}
            showValueHeader={false}
            legendPosition="top"
            formatValue={(v) => formatNumber(v, 0)}
            formatSecondaryValue={(v) => formatNumber(v, 0)}
          />
        )}
        {/* Marker — красный кружок точно на endpoint OI-линии (post-render DOM walk). */}
        {ready && markerPos && (
          <div
            style={{
              position: 'absolute',
              left: markerPos.x - 14,
              top: markerPos.y - 14,
              width: 28,
              height: 28,
              borderRadius: '50%',
              border: '3px solid #ef4444',
              background: 'transparent',
              pointerEvents: 'none',
              boxShadow: '0 0 0 2px rgba(239, 68, 68, 0.25)',
            }}
          />
        )}
      </div>

      {/* Footer */}
      <div
        style={{
          height: FOOTER_H,
          flexShrink: 0,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          color: 'var(--text-secondary, #888)',
          fontSize: 13,
        }}
      >
        <div>таймфрейм.рф</div>
        <div>{formattedDate}</div>
      </div>
    </div>
  );
}
