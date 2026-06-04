/**
 * EmbedFundsMoney — виджет «Фонды» (рыночный). Headline — суммарная СЧА (AUM)
 * через SimpleChart + индекс на вторичной оси. Контрол категории внутри.
 * Режим flows (FlowsHistogram) сильно завязан на состояние страницы — в виджет
 * v1 не тащим; для компактного окна AUM достаточно.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import { getFundsChartData } from '../../services/api';
import { EmbedMsg, embedColumn, embedHeader, segBtn } from './embedUi';

type Category = 'money_market' | 'stocks' | 'bonds' | 'gold' | 'yuan';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type FundsResp = Awaited<ReturnType<typeof getFundsChartData>>;

const CATS: { id: Category; label: string }[] = [
  { id: 'money_market', label: 'Денежный' },
  { id: 'stocks', label: 'Акции' },
  { id: 'bonds', label: 'Облигации' },
  { id: 'gold', label: 'Золото' },
  { id: 'yuan', label: 'Юань' },
];

function initCat(p: string | null): Category {
  if (p && CATS.some((c) => c.id === p)) return p as Category;
  try {
    const s = localStorage.getItem('frame:embed:funds:category');
    if (s && CATS.some((c) => c.id === s)) return s as Category;
  } catch { /* ignore */ }
  return 'money_market';
}

export default function EmbedFundsMoney() {
  const [params] = useSearchParams();
  const [category, setCategory] = useState<Category>(() => initCat(params.get('category')));
  const period = params.get('period') || '1y';
  const [data, setData] = useState<FundsResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => {
    try { localStorage.setItem('frame:embed:funds:category', category); } catch { /* quota */ }
  }, [category]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getFundsChartData(category, period as Parameters<typeof getFundsChartData>[1])
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setStatus((res?.total_nav?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/funds load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [category, period]);

  const boxRef = useRef<HTMLDivElement>(null);
  const [chartH, setChartH] = useState(280);
  useEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setChartH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const chartData = useMemo(
    () => (data?.total_nav ?? []).map((p) => ({ time: p.date, value: (p.nav ?? 0) / 1e9 })),
    [data],
  );
  const indexData = useMemo(
    () => (data?.index?.data ? data.index.data.map((d) => ({ time: d.date, value: d.close || 0 })) : undefined),
    [data],
  );

  const fmtNav = (v: number) => (category === 'gold' || category === 'stocks' ? v.toFixed(2) : v.toFixed(0));

  return (
    <div style={embedColumn}>
      <div style={embedHeader}>
        <span style={{ fontWeight: 700, fontSize: 14 }}>Фонды</span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 4, flexWrap: 'wrap' }}>
          {CATS.map((c) => (
            <button key={c.id} style={segBtn(category === c.id)} onClick={() => setCategory(c.id)}>
              {c.label}
            </button>
          ))}
        </div>
      </div>
      <div ref={boxRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {status === 'ok' && chartData.length > 0 && (
          <SimpleChart
            data={chartData}
            secondaryData={indexData}
            height={chartH}
            primaryColor="var(--accent)"
            secondaryColor="var(--funds-flow-positive)"
            showSecondary={!!indexData}
            formatValue={fmtNav}
            formatSecondaryValue={(v) => v.toFixed(2)}
            primaryLabel="Суммарная СЧА, млрд ₽"
            secondaryLabel="Индекс"
            showValueHeader={false}
            legendPosition="top"
            showDownloadButton={false}
            showNavigator={false}
            hideTime
            chartPadding={{ left: 100, right: 60 }}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </div>
  );
}
