/**
 * HotPage — витрина «Главное» (/hot). Пока только для админов: пункт в меню «+»
 * тестовых индикаторов, ручка GET /api/admin/hot (role=admin).
 *
 * Самое необычное за последние две недели по данным, которые смотрит завод
 * постов: позиции физлиц (логика скринера — рекорд перекоса за год и дольше или
 * сдвиг за 2 недели), деньги в фондах (рекорд, разворот, серия), сделки фондов
 * (бумага, которую фонды купили или продали единодушно), сезонность.
 *
 * Карточка — сверху вниз от крупного к мелкому: актив → сигнал словами
 * скринера → график → «что было после» прошлых таких же случаев. Графики —
 * те же компоненты и ручки, что на страницах индикаторов (SimpleChart как на
 * «Открытых позициях», FlowsHistogram как в «Деньгах в фондах», гистограмма
 * «Потоков по компании», годовая сезонность). Контракты не показываем:
 * позиция — перекос, как в скринере (чистая позиция в % от всех позиций).
 */
import { useEffect, useMemo, useState } from 'react';
import type { ComponentProps, CSSProperties, ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Banknote, DollarSign, Flame, JapaneseYen, TrendingUp } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import SegmentedControl from '../components/SegmentedControl';
import SimpleChart from '../components/SimpleChart';
import Skeleton from '../components/Skeleton';
import InstrumentIcon from '../components/InstrumentIcon';
import TickerLogo from '../components/TickerLogo';
import FlowsHistogram from '../components/funds/FlowsHistogram';
import CompanyFlowsHistogram from '../components/fundtrades/CompanyFlowsHistogram';
import type { CompanyFlowsSeries } from '../components/fundtrades/CompanyFlowsHistogram';
import YearlySeasonalityChart from '../components/seasonality/YearlySeasonalityChart';
import { useAuth } from '../contexts/AuthContext';
import { UK_LOGOS, DONUT_COLORS } from '../config/fundConfig';
import { monthShort } from '../i18n';
import {
  getHot, getChartData, getFundsFlows, getCompanyFlows, getSeasonalityYearly,
} from '../services/api';
import type {
  HotCard, HotOiCard, HotFlowsCard, HotTradesCard, HotSeasonCard, HotChip, HotResponse,
  FundsFlowsResponse, CompanyFlowsResponse, YearlySeasonalityResponse,
} from '../services/api';
import type { ChartResponse } from '../types';

type Section = 'all' | 'oi' | 'flows' | 'trades' | 'season';

const CHART_H = 230;

const sgn = (v: number, digits = 0) =>
  `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toLocaleString('ru-RU', { maximumFractionDigits: digits })}`;
const monY = (iso: string) => `${monthShort(Number(iso.slice(5, 7)) - 1)} ${iso.slice(2, 4)}`;
const fmtPct = (v: number) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(Math.round(v))}%`;
const fmtPrice = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: Math.abs(v) < 100 ? 2 : 0 });

const CARD: CSSProperties = {
  background: 'var(--bg-secondary)',
  border: 'var(--card-border-width, 2px) solid var(--card-border-color, var(--border-color))',
  borderRadius: 'var(--card-radius, 12px)',
  boxShadow: 'var(--card-shadow)',
  padding: 14,
  display: 'flex',
  flexDirection: 'column',
  gap: 8,
  minWidth: 0,
};

// Иконка категории фондов — как на странице «Деньги в фондах».
function GoldBars({ size = 18 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="8 13 40 25" fill="currentColor" aria-hidden="true">
      <path d="M21.248 21.555h13.784l-2.01-5.393a1.17 1.17 0 00-.41-.553l-11.364 5.946zm-.038-6.401C21.698 13.842 22.772 13 23.956 13h8.151c1.184 0 2.258.842 2.747 2.154l2.009 5.393c.603 1.618-.371 3.453-1.831 3.453h-14c-1.46 0-2.433-1.835-1.831-3.453l2.01-5.393h-.001zM10.235 35.555h13.757l-2.01-5.393a1.171 1.171 0 00-.41-.553l-11.337 5.946zm-.039-6.401C10.685 27.842 11.76 27 12.943 27h8.124c1.184 0 2.259.842 2.747 2.154l2.009 5.393c.603 1.618-.37 3.453-1.831 3.453H10.017c-1.46 0-2.433-1.835-1.83-3.453l2.01-5.393zm35.89 6.401h-13.85l11.43-5.945c.179.126.323.316.413.553l2.008 5.392zM34.945 27c-1.184 0-2.259.842-2.747 2.154l-2.009 5.393c-.603 1.618.37 3.453 1.831 3.453h14.067c1.46 0 2.433-1.835 1.83-3.453l-2.01-5.393C45.422 27.842 44.348 27 43.164 27h-8.22z" />
    </svg>
  );
}

function CategoryIcon({ category }: { category: HotFlowsCard['category'] }) {
  const icon = {
    money_market: <Banknote size={18} strokeWidth={2.3} />,
    stocks: <TrendingUp size={18} strokeWidth={2.3} />,
    bonds: <DollarSign size={18} strokeWidth={2.3} />,
    gold: <GoldBars />,
    yuan: <JapaneseYen size={18} strokeWidth={2.3} />,
  }[category];
  return (
    <span style={{
      width: 34, height: 34, borderRadius: 8, flex: 'none', display: 'grid', placeItems: 'center',
      background: 'var(--accent)', color: 'var(--text-inverse)', border: '2px solid var(--border-color)',
    }}>{icon}</span>
  );
}

// ── Графики: те же компоненты и ручки, что на страницах индикаторов ──────────

function useLoad<T>(load: () => Promise<T>, deps: unknown[]): { data: T | null; loading: boolean } {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    let off = false;
    setLoading(true);
    load().then(r => { if (!off) setData(r); }).catch(() => { if (!off) setData(null); })
      .finally(() => { if (!off) setLoading(false); });
    return () => { off = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return { data, loading };
}

function OiChart({ card }: { card: HotOiCard }) {
  const { t } = useTranslation();
  const { data, loading } = useLoad<ChartResponse>(
    () => getChartData(card.sectype, card.sectype, 'futures', 24, 'FIZ', true, card.chart_period),
    [card.sectype, card.chart_period],
  );
  const price = useMemo(() => (data?.candles ?? []).map(c => ({ time: c.time, value: c.close })), [data]);
  // Перекос, как в скринере: чистая позиция физлиц в % от всех их позиций.
  const skew = useMemo(() => (data?.open_interest ?? []).flatMap(o => {
    const long = o.pos_long ?? 0, short = Math.abs(o.pos_short ?? 0), gross = long + short;
    if (!gross) return [];
    const net = o.net_position ?? long - short;
    return [{ time: o.time, value: Math.round((net / gross) * 1000) / 10 }];
  }), [data]);
  const recordWord = card.tag.type === 'record' ? card.tag.text.split(' ')[0].toLowerCase() : '';
  const lines = card.level != null
    ? [{ value: card.level, color: 'var(--accent)', label: t('прежний {{w}}', { w: recordWord }), axis: 'secondary' as const }]
    : [];
  const anns = card.episodes.map(e => ({
    time: e.date,
    label: '•',
    description: t('{{when}}: {{what}} через месяц {{chg}}%{{era}}', {
      when: monY(e.date), what: card.what.toLowerCase(), chg: sgn(e.after),
      era: e.post2022 ? '' : t(' (до 2022)'),
    }),
    color: 'var(--accent)',
    textColor: 'var(--text-inverse)',
  }));
  return (
    <SimpleChart
      data={price}
      secondaryData={skew}
      height={CHART_H}
      loading={loading}
      primaryColor="var(--chart-line-1)"
      secondaryColor="var(--oi-cyan)"
      primaryLabel={card.name}
      secondaryLabel={t('Перекос физлиц')}
      formatValue={fmtPrice}
      formatSecondaryValue={fmtPct}
      formatSecondaryAxis={fmtPct}
      niceTicks
      niceTicksSecondary
      gridAxis="secondary"
      alignSecondaryByTime
      horizontalLines={lines}
      annotations={anns}
      showValueHeader={false}
      legendPosition="top"
      showDownloadButton={false}
      showNavigator={false}
      showWatermark={false}
      clampEdgeLabels
      hideTime
      bare
      chartPadding={{ left: 56, right: 56 }}
    />
  );
}

function FlowsChart({ card }: { card: HotFlowsCard }) {
  const { t } = useTranslation();
  const { data, loading } = useLoad<FundsFlowsResponse>(
    () => getFundsFlows(card.category, card.timeframe, card.period),
    [card.category, card.timeframe, card.period],
  );
  return (
    <FlowsHistogram
      bare
      flowsData={data}
      loading={loading}
      showIndex={false}
      height={CHART_H}
      flowTitle={card.timeframe === '1w' ? t('Притоки и оттоки по неделям (млрд ₽)') : t('Притоки и оттоки по месяцам (млрд ₽)')}
      animTrigger={card.id}
    />
  );
}

function TradesChart({ card }: { card: HotTradesCard }) {
  const { t } = useTranslation();
  const { data, loading } = useLoad<CompanyFlowsResponse>(
    () => getCompanyFlows({ isin: card.isin, metric: 'amount' }),
    [card.isin],
  );
  // Два года: видно, бывало ли раньше, чтобы фонды действовали так же единодушно.
  const { months, series } = useMemo(() => {
    if (!data) return { months: [] as string[], series: [] as CompanyFlowsSeries[] };
    const start = Math.max(0, data.months.length - 24);
    return {
      months: data.months.slice(start),
      series: data.funds.map((f, idx) => ({
        label: f.fund_name,
        color: (f.uk_id != null ? UK_LOGOS[String(f.uk_id)]?.bg : undefined) ?? DONUT_COLORS[idx % DONUT_COLORS.length],
        values: f.values.slice(start),
      })),
    };
  }, [data]);
  return (
    <CompanyFlowsHistogram
      months={months}
      series={series}
      height={CHART_H}
      loading={loading}
      title={t('Покупки и продажи фондов (млн ₽)')}
      animTrigger={card.isin}
    />
  );
}

function SeasonChart({ card }: { card: HotSeasonCard }) {
  const { data, loading } = useLoad<YearlySeasonalityResponse>(() => getSeasonalityYearly(card.secid), [card.secid]);
  const [tip, setTip] = useState<ComponentProps<typeof YearlySeasonalityChart>['tooltip']>(null);
  if (loading || !data) return <Skeleton height={CHART_H} />;
  return <YearlySeasonalityChart yearlyData={data} tooltip={tip} setTooltip={setTip} chartHeight={CHART_H} showCurrentYear />;
}

// ── «Что было после» ─────────────────────────────────────────────────────────

function Chips({ label, chips }: { label: string; chips: { text: string; cls?: 'up' | 'dn' | 'old'; sub?: string }[] }) {
  if (!label && !chips.length) return null;
  return (
    <div style={{
      display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 5, borderTop: '1px solid var(--chart-grid)',
      paddingTop: 8, fontSize: 11, color: 'var(--text-muted)',
    }}>
      {label && <span style={{ flexBasis: '100%' }}>{label}</span>}
      {chips.map((c, i) => (
        <span key={i} style={{
          fontFamily: 'var(--font-mono)', fontSize: 11, fontWeight: c.cls === 'old' ? 500 : 700, padding: '5px 6px',
          borderRadius: 6, background: 'var(--bg-tertiary)', whiteSpace: 'nowrap',
          color: c.cls === 'up' ? 'var(--success)' : c.cls === 'dn' ? 'var(--danger)' : 'var(--text-muted)',
        }}>
          {c.text}{c.sub && <span style={{ fontWeight: 500, color: 'var(--text-muted)', marginLeft: 4 }}>{c.sub}</span>}
        </span>
      ))}
    </div>
  );
}

function OiAfter({ card }: { card: HotOiCard }) {
  const { t } = useTranslation();
  if (!card.has_price) return null;
  const eps = card.episodes;
  if (!eps.length) return <Chips label={t('Раньше такого не было')} chips={[]} />;
  const post = eps.filter(e => e.post2022), pre = eps.filter(e => !e.post2022);
  const chips: { text: string; cls?: 'up' | 'dn' | 'old'; sub?: string }[] = post.slice(-4).map(e => ({
    text: `${sgn(e.after)}%`, cls: e.after >= 0 ? 'up' : 'dn', sub: monY(e.date),
  }));
  if (post.length > 4) chips.push({ text: t('рост {{k}} из {{n}}', { k: post.filter(e => e.after > 0).length, n: post.length }), cls: 'old' });
  if (pre.length) chips.push({ text: t('до 2022: рост {{k}} из {{n}}', { k: pre.filter(e => e.after > 0).length, n: pre.length }), cls: 'old' });
  return <Chips label={t('{{what}} через месяц после прошлых таких случаев', { what: card.what })} chips={chips} />;
}

const compareChips = (chips: HotChip[]) =>
  chips.map(c => ({ text: c.text, cls: c.old ? 'old' as const : c.cls, sub: c.sub }));

// ── Карточка ─────────────────────────────────────────────────────────────────

function Tag({ children, tone = 'muted' }: { children: ReactNode; tone?: 'accent' | 'fill' | 'pill' | 'strong' | 'muted' }) {
  const base: CSSProperties = { fontSize: 13, lineHeight: 1, whiteSpace: 'nowrap' };
  const style: Record<string, CSSProperties> = {
    accent: { ...base, fontWeight: 700, color: 'var(--accent)' },
    fill: { ...base, fontWeight: 700, color: 'var(--text-inverse)', background: 'var(--accent)', padding: '5px 9px', borderRadius: 999 },
    pill: { ...base, fontFamily: 'var(--font-mono)', fontWeight: 800, fontSize: 12, color: 'var(--accent)', border: '2px solid var(--accent)', padding: '3px 9px', borderRadius: 999 },
    strong: { ...base, fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--text-primary)' },
    muted: { ...base, fontSize: 11.5, fontWeight: 500, color: 'var(--text-muted)' },
  };
  return <span style={style[tone]}>{children}</span>;
}

function HotCardView({ card }: { card: HotCard }) {
  const { t } = useTranslation();
  const name = card.kind === 'trades' ? card.asset_name : card.name;
  const icon = card.kind === 'flows' ? <CategoryIcon category={card.category} />
    : card.kind === 'trades' ? (card.secid ? <TickerLogo ticker={card.secid} size={34} rounded="md" /> : null)
    : <InstrumentIcon sectype={card.sectype} size={34} rounded="md" />;

  let tags: ReactNode = null;
  let chart: ReactNode = null;
  let after: ReactNode = null;
  if (card.kind === 'oi') {
    tags = card.tag.type === 'record'
      ? <>
          <Tag tone={card.tag.all ? 'fill' : 'accent'}>{card.tag.text}</Tag>
          {card.tag.move && <><Tag tone="pill">{card.tag.move}</Tag><Tag>{t('за 2 недели')}</Tag></>}
        </>
      : <><Tag tone="pill">{card.tag.text}</Tag><Tag>{card.tag.note}</Tag></>;
    chart = <OiChart card={card} />;
    after = <OiAfter card={card} />;
  } else if (card.kind === 'flows') {
    tags = <>
      <Tag tone="strong">{t('{{v}} млрд ₽', { v: sgn(card.amount, Math.abs(card.amount) >= 10 ? 0 : 2) })}</Tag>
      {card.note && <Tag tone="accent">{card.note}</Tag>}
    </>;
    chart = <FlowsChart card={card} />;
    after = card.compare ? <Chips label={card.compare.label} chips={compareChips(card.compare.chips)} /> : null;
  } else if (card.kind === 'trades') {
    tags = <>
      <Tag tone="strong">{card.funds}</Tag>
      <Tag>{t('{{v}} млрд ₽', { v: sgn(card.amount_rub / 1e9, 2) })}</Tag>
    </>;
    chart = <TradesChart card={card} />;
  } else {
    tags = <Tag tone="strong">{card.hits}</Tag>;
    chart = <SeasonChart card={card} />;
    after = <Chips label={card.compare.label} chips={compareChips(card.compare.chips)} />;
  }

  return (
    <article style={CARD}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
        {icon}
        <h3 style={{
          fontFamily: 'var(--font-display)', fontSize: 20, fontWeight: 800, lineHeight: 1.1, margin: 0, flex: 1,
          minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-primary)',
        }}>{name}</h3>
        <span style={{
          fontSize: 10, fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase', color: 'var(--text-muted)',
          textAlign: 'right', maxWidth: 90, lineHeight: 1.2,
        }}>{t(card.section)}</span>
      </div>
      <div style={{ fontSize: 15, fontWeight: 600, lineHeight: 1.25, color: 'var(--text-primary)' }}>{t(card.signal)}</div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', minHeight: 22 }}>
        {tags}
        <Tag>{card.date_label}</Tag>
      </div>
      <div style={{ minWidth: 0 }}>{chart}</div>
      {after}
    </article>
  );
}

// ── Страница ─────────────────────────────────────────────────────────────────

export default function HotPage() {
  const { t } = useTranslation();
  const { user, loading: authLoading } = useAuth();
  const isAdmin = user?.role === 'admin';
  const [resp, setResp] = useState<HotResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [section, setSection] = useState<Section>('all');

  useEffect(() => {
    if (!isAdmin) return;
    let off = false;
    getHot().then(r => { if (!off) setResp(r); })
      .catch(e => { if (!off) setError(e instanceof Error ? e.message : String(e)); });
    return () => { off = true; };
  }, [isAdmin]);

  const cards = resp?.cards ?? [];
  const count = (s: Section) => (s === 'all' ? cards.length : cards.filter(c => c.kind === s).length);
  const shown = section === 'all' ? cards : cards.filter(c => c.kind === section);
  const options = ([
    ['all', t('Все')], ['oi', t('Позиции физлиц')], ['flows', t('Деньги в фондах')],
    ['trades', t('Сделки фондов')], ['season', t('Сезонность')],
  ] as [Section, string][])
    .filter(([k]) => k === 'all' || count(k) > 0)
    .map(([key, label]) => ({ key, label: resp ? `${label} ${count(key)}` : label }));

  if (authLoading) return null;
  if (!isAdmin) return <Navigate to="/" replace />;

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
      <PageHeader
        icon={Flame}
        title={t('Главное')}
        subtitle={t('Самое необычное в позициях физлиц и деньгах фондов за последние две недели')}
      />
      <div className="flex flex-wrap items-center mb-5" style={{ gap: 'var(--sp-3)' }}>
        <SegmentedControl<Section> options={options} value={section} onChange={setSection} />
        {resp && (
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
            {t('Позиции — на {{d}}, до 2022 года рынок был другим', { d: `${Number(resp.as_of.slice(8, 10))} ${monthShort(Number(resp.as_of.slice(5, 7)) - 1)}` })}
          </span>
        )}
      </div>
      {error && <div style={{ color: 'var(--danger)', fontSize: 14 }}>{error}</div>}
      {!resp && !error && (
        <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(min(100%, 380px), 1fr))', gap: 22 }}>
          {Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} height={380} />)}
        </div>
      )}
      {resp && (
        <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(min(100%, 380px), 1fr))', gap: 22 }}>
          {shown.map(c => <HotCardView key={c.id} card={c} />)}
        </div>
      )}
      {resp?.errors?.length ? (
        <div style={{ marginTop: 16, fontSize: 12, color: 'var(--text-muted)' }}>
          {t('Не посчитались разделы: {{s}}', { s: resp.errors.join(', ') })}
        </div>
      ) : null}
      <div style={{ marginTop: 20, fontSize: 12, color: 'var(--text-muted)', maxWidth: '90ch' }}>
        {t('Линия позиции — перекос, как в скринере: чистая позиция физлиц в процентах от всех их позиций. Точки на графике — прошлые такие же случаи. Это наблюдения по данным, а не инвестиционные рекомендации.')}
      </div>
    </div>
  );
}
