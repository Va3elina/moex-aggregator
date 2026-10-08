/**
 * HotPage — витрина «Главное» (/hot). Пока только для админов: пункт в меню «+»
 * тестовых индикаторов, ручка GET /api/admin/hot (role=admin).
 *
 * Самое необычное на последний день данных: позиции физлиц — то же, что
 * скринер сигналов сегодня (сдвиг за день, за 2 недели, рекорд перекоса),
 * деньги в фондах, сделки фондов, сезонность.
 *
 * Карточка — сверху вниз от крупного к мелкому: актив → сигнал словами
 * скринера → плашки → график. Текста минимум: что произошло, видно на
 * графике (components/hot/HotCharts.tsx — компактный движок под карточку в
 * оформлении графиков сайта; общие графики сайта не трогаем). Под графиком —
 * свёрнутый список прошлых похожих случаев этого актива (past с бэкенда).
 */
import { useEffect, useState } from 'react';
import type { CSSProperties, ReactNode } from 'react';
import { Navigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Banknote, ChevronDown, DollarSign, Flame, JapaneseYen, TrendingUp } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import SegmentedControl from '../components/SegmentedControl';
import Skeleton from '../components/Skeleton';
import InstrumentIcon from '../components/InstrumentIcon';
import TickerLogo from '../components/TickerLogo';
import { BarsChartCard, LegsChartCard, SeasonChartCard } from '../components/hot/HotCharts';
import { useAuth } from '../contexts/AuthContext';
import { monthGenitive } from '../i18n';
import { getHot } from '../services/api';
import type { HotCard, HotFlowsCard, HotPast, HotPastCase, HotResponse, HotTag } from '../services/api';

type Section = 'all' | 'oi' | 'flows' | 'trades' | 'season';

const sgn = (v: number, digits = 0) =>
  `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toLocaleString('ru-RU', { maximumFractionDigits: digits })}`;

const CARD: CSSProperties = {
  background: 'var(--bg-secondary)',
  border: 'var(--card-border-width, 2px) solid var(--card-border-color, var(--border-color))',
  borderRadius: 'var(--card-radius, 12px)',
  boxShadow: 'var(--card-shadow)',
  padding: 16,
  display: 'flex',
  flexDirection: 'column',
  gap: 12,
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

function Plaque({ children, tone }: { children: ReactNode; tone: 'fill' | 'accent' | 'pill' | 'strong' | 'muted' }) {
  const base: CSSProperties = { fontSize: 13, lineHeight: 1, whiteSpace: 'nowrap' };
  const style: Record<string, CSSProperties> = {
    accent: { ...base, fontWeight: 700, color: 'var(--accent)' },
    fill: { ...base, fontWeight: 700, color: 'var(--text-inverse)', background: 'var(--accent)', padding: '5px 9px', borderRadius: 999 },
    pill: { ...base, fontFamily: 'var(--font-mono)', fontWeight: 800, fontSize: 12, color: 'var(--accent)', border: '2px solid var(--accent)', padding: '3px 9px', borderRadius: 999 },
    strong: { ...base, fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--text-primary)' },
    muted: { ...base, fontSize: 12, fontWeight: 500, color: 'var(--text-muted)' },
  };
  return <span style={style[tone]}>{children}</span>;
}

// Под заголовком позиций — одна тихая строка: подтверждающие находки через точку, без заливок и оранжевого
// (оранжевый — только зона события на графике).
function OiTags({ tags }: { tags: HotTag[] }) {
  const { t } = useTranslation();
  if (!tags.length) return null;
  return (
    <span style={{ fontSize: 13, lineHeight: 1.35, color: 'var(--text-muted)' }}>
      {tags.map(tg => t(tg.text) + (tg.note ? ` ${t(tg.note)}` : '')).join(' · ')}
    </span>
  );
}

const UP = 'var(--funds-flow-positive)';
const DN = 'var(--funds-flow-negative)';
const pctCell = (v: number | null) => (v == null ? '—' : `${sgn(v, 1)}%`);
const tone = (v: number | null) => (v == null ? 'var(--text-muted)' : v > 0 ? UP : DN);
const shortDate = (d: string) => `${d.slice(8, 10)}.${d.slice(5, 7)}.${d.slice(2, 4)}`;

// «История» под графиком — прошлые похожие случаи: по умолчанию одна строка с их числом,
// по нажатию — список. Средних «+x% в среднем» нет: на истории это монетка, показываем сами случаи.
function PastCases({ past, onPick }: { past: HotPast; onPick?: (c: HotPastCase | null) => void }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [on, setOn] = useState<number | null>(null);
  const pick = (i: number | null) => { setOn(i); onPick?.(i == null ? null : past.cases[i]); };
  const toggle = () => { if (open) pick(null); setOpen(o => !o); };
  const last = past.horizons.length - 1;
  const head: CSSProperties = { flex: 1, minWidth: 0, fontSize: 12, fontWeight: 600, color: 'var(--text-muted)' };
  const grid: CSSProperties = { display: 'grid', gridTemplateColumns: `64px minmax(0, 1fr) ${past.horizons.map(() => '72px').join(' ')}`, gap: 8, alignItems: 'center' };
  const num: CSSProperties = { fontFamily: 'var(--font-mono)', fontWeight: 700, textAlign: 'right' };
  const wrap: CSSProperties = { borderTop: '1px solid var(--chart-grid)', paddingTop: 8 };
  if (!past.cases.length) {
    return (
      <div style={{ ...wrap, display: 'flex', alignItems: 'center', gap: 10, minHeight: 28 }}>
        <span style={head}>{t(past.title)}</span>
        <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{t('раньше такого не было')}</span>
      </div>
    );
  }
  return (
    <div style={wrap}>
      <button type="button" onClick={toggle} aria-expanded={open}
        style={{ all: 'unset', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 10, minHeight: 28, width: '100%', boxSizing: 'border-box' }}>
        <span style={head}>{t(past.title)}</span>
        <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, fontSize: 12, color: 'var(--text-muted)' }}>{past.cases.length}</span>
        <ChevronDown size={14} strokeWidth={2.4} style={{ color: 'var(--text-muted)', transition: 'transform .2s', transform: open ? 'rotate(180deg)' : 'none' }} />
      </button>
      {open && (
        <div style={{ display: 'flex', flexDirection: 'column', marginTop: 6 }}>
          <div style={{ ...grid, fontSize: 10, color: 'var(--text-muted)', paddingBottom: 4 }}>
            <span>{t('дата')}</span><span>{t('что было')}</span>
            {past.horizons.map(hz => <span key={hz} style={{ textAlign: 'right' }}>{t(hz)}</span>)}
          </div>
          {past.cases.map((c, i) => (
            <div key={i} tabIndex={onPick ? 0 : undefined}
              onMouseEnter={() => pick(i)} onMouseLeave={() => pick(null)}
              onFocus={() => pick(i)} onBlur={() => pick(null)} onClick={() => pick(i)}
              style={{
                ...grid, padding: '6px 4px', margin: '0 -4px', borderRadius: 6, borderTop: '1px solid var(--chart-grid)', fontSize: 12,
                cursor: onPick ? 'pointer' : 'default', background: onPick && on === i ? 'var(--bg-primary)' : 'transparent',
              }}>
              <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>{shortDate(c.date)}</span>
              <span style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', color: 'var(--text-primary)' }}>{t(c.label)}</span>
              {c.r.map((v, j) => <span key={j} style={{ ...num, color: tone(v) }}>{pctCell(v)}</span>)}
            </div>
          ))}
          {past.base_up != null && (
            <div style={{ fontSize: 11, color: 'var(--text-muted)', paddingTop: 6 }}>
              {t('В обычный день цена {{h}} росла в {{p}}% случаев', { h: t(past.horizons[last]), p: past.base_up })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function HotCardView({ card }: { card: HotCard }) {
  const { t } = useTranslation();
  const [picked, setPicked] = useState<HotPastCase | null>(null);   // прошлый случай под курсором в списке
  const icon = card.kind === 'flows' ? <CategoryIcon category={card.category} />
    : card.kind === 'trades' ? (card.secid ? <TickerLogo ticker={card.secid} size={34} rounded="md" /> : null)
    : <InstrumentIcon sectype={card.sectype} size={34} rounded="md" />;

  let plaques: ReactNode;
  let chart: ReactNode;
  if (card.kind === 'oi') {
    plaques = <OiTags tags={card.tags} />;
    chart = <LegsChartCard chart={card.chart} priceLabel={card.name} highlight={picked} />;
  } else if (card.kind === 'flows') {
    plaques = <>
      <Plaque tone="strong">{t('{{v}} млрд ₽', { v: sgn(card.amount, Math.abs(card.amount) >= 10 ? 0 : 2) })}</Plaque>
      {card.note && <Plaque tone="muted">{t(card.note)}</Plaque>}
      <Plaque tone="muted">{card.date_label}</Plaque>
    </>;
    chart = <BarsChartCard chart={card.chart} highlight={picked} />;
  } else if (card.kind === 'trades') {
    plaques = <>
      <Plaque tone="strong">{card.funds}</Plaque>
      <Plaque tone="muted">{card.date_label}</Plaque>
    </>;
    chart = <BarsChartCard chart={card.chart} />;
  } else {
    plaques = <>
      <Plaque tone="strong">{card.hits}</Plaque>
      <Plaque tone="muted">{card.date_label}</Plaque>
    </>;
    chart = <SeasonChartCard chart={card.chart} label={t('Этот год')} highlight={picked} />;
  }

  return (
    <article style={CARD}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
        {icon}
        <h3 style={{
          fontFamily: 'var(--font-display)', fontSize: 20, fontWeight: 800, lineHeight: 1.1, margin: 0, minWidth: 0,
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-primary)',
        }}>{card.name}</h3>
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <div style={{ fontSize: 15, fontWeight: 600, lineHeight: 1.25, color: 'var(--text-primary)' }}>{t(card.signal)}</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>{plaques}</div>
      </div>
      <div style={{ minWidth: 0, marginTop: 2 }}>{chart}</div>
      {(card.kind === 'oi' || card.kind === 'flows' || card.kind === 'season') && card.past && <PastCases past={card.past} onPick={setPicked} />}
    </article>
  );
}

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
  const asOf = resp ? `${Number(resp.as_of.slice(8, 10))} ${monthGenitive(Number(resp.as_of.slice(5, 7)) - 1)}` : '';

  if (authLoading) return null;
  if (!isAdmin) return <Navigate to="/" replace />;

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
      <PageHeader icon={Flame} title={t('Главное')} />
      <div className="flex flex-wrap items-center mb-6" style={{ gap: 'var(--sp-3)' }}>
        <SegmentedControl<Section> options={options} value={section} onChange={setSection} />
        {resp && <span style={{ fontSize: 13, color: 'var(--text-muted)' }}>{t('Данные за {{d}}', { d: asOf })}</span>}
      </div>
      {error && <div style={{ color: 'var(--danger)', fontSize: 14 }}>{error}</div>}
      <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(min(100%, 400px), 1fr))', gap: 24 }}>
        {resp
          ? shown.map(c => <HotCardView key={c.id} card={c} />)
          : !error && Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} height={330} />)}
      </div>
      {resp?.errors?.length ? (
        <div style={{ marginTop: 16, fontSize: 12, color: 'var(--text-muted)' }}>
          {t('Не посчитались разделы: {{s}}', { s: resp.errors.join(', ') })}
        </div>
      ) : null}
    </div>
  );
}
