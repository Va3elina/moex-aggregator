/**
 * EmbedFundsMoney — виджет «Фонды» (рыночный). Headline — суммарная СЧА (AUM)
 * через SimpleChart + индекс на вторичной оси. Категория и период — в drawer'е.
 * Режим flows сильно завязан на состояние страницы — в виджет v1 не тащим.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import SimpleChart from '../../components/SimpleChart';
import { getFundsChartData, type FundPeriod } from '../../services/api';
import { EmbedMsg } from './embedUi';
import { useEmbedSettings, EmbedShell, DrawerSection, SegGroup } from './EmbedSettings';

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
const PERIODS: { id: FundPeriod; label: string }[] = [
  { id: '3m', label: '3М' },
  { id: '6m', label: '6М' },
  { id: '1y', label: '1Г' },
  { id: '2y', label: '2Г' },
  { id: '3y', label: '3Г' },
  { id: 'all', label: 'Всё' },
];

function initCat(p: string | null): Category {
  if (p && CATS.some((c) => c.id === p)) return p as Category;
  try {
    const s = localStorage.getItem('frame:embed:funds:category');
    if (s && CATS.some((c) => c.id === s)) return s as Category;
  } catch { /* ignore */ }
  return 'money_market';
}

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

export default function EmbedFundsMoney() {
  const [params] = useSearchParams();
  const settings = useEmbedSettings();

  const [category, setCategory] = useState<Category>(() => initCat(params.get('category')));
  const [period, setPeriod] = useState<FundPeriod>(() => (params.get('period') || readLS('frame:embed:funds:period', '1y')) as FundPeriod);
  const [data, setData] = useState<FundsResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { try { localStorage.setItem('frame:embed:funds:category', category); } catch { /* quota */ } }, [category]);
  useEffect(() => { try { localStorage.setItem('frame:embed:funds:period', period); } catch { /* quota */ } }, [period]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getFundsChartData(category, period)
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
  const catLabel = CATS.find((c) => c.id === category)?.label || '';

  return (
    <EmbedShell
      settings={settings}
      title="Фонды"
      subtitle={catLabel}
      drawer={
        <>
          <DrawerSection label="Категория">
            <SegGroup value={category} options={CATS} onChange={(v) => setCategory(v)} />
          </DrawerSection>
          <DrawerSection label="Период">
            <SegGroup value={period} options={PERIODS} onChange={(v) => setPeriod(v)} />
          </DrawerSection>
        </>
      }
    >
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
    </EmbedShell>
  );
}
