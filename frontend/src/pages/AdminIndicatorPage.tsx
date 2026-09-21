/**
 * AdminIndicatorPage — страница одного раздела: кто смотрит, что внутри,
 * откуда приходят и куда уходят.
 *
 * Route: /admin/indicator/:key  (key = путь раздела без слэша: oi, funds-money…)
 * Source: GET /api/analytics/indicator
 *
 * Глубина у разделов разная: у ОИ — активы, у фондов — категории, вкладки и
 * сами фонды, у терминала — из чего собирают окна. У одностраничных графиков
 * внутри выбирать нечего — и страница так и говорит, а не показывает чужое.
 */
import { useEffect, useState } from 'react';
import { useNavigate, useParams, Link } from 'react-router-dom';
import { ArrowLeft, AlertCircle, ChevronRight } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SimpleChart from '../components/SimpleChart';
import InstrumentIcon from '../components/InstrumentIcon';
import { useAuth } from '../contexts/AuthContext';
import { usePersistedState } from '../hooks/usePersistedState';
import { useDelayedFlag } from '../hooks/useDelayedFlag';
import { resolveFundLogo } from '../config/fundConfig';
import { IndicatorGlyph, indicatorKey, indicatorPath } from '../components/admin/indicatorMeta';
import { PAGE_NAMES } from '../components/admin/ActivityBlocks';
import { getIndicator } from '../services/api';
import type { IndicatorReport } from '../services/api';

const MONO = "'IBM Plex Mono', monospace";
const ru = (n: number) => n.toLocaleString('ru-RU');

const CATEGORY_RU: Record<string, string> = {
  money_market: 'Денежный рынок', bonds: 'Облигации', stocks: 'Акции',
  gold: 'Золото', yuan: 'Юань', mixed: 'Смешанные',
};
const TAB_RU: Record<string, string> = {
  funds: 'Витрина', portfolio: 'Общий портфель', company: 'По бумаге',
  movers: 'Сделки', snapshots: 'Срезы',
};
const PANEL_RU: Record<string, string> = {
  oi: 'Открытые позиции', 'funds-money': 'Деньги в фондах', 'fund-trades': 'Сделки фондов',
  strength: 'Сила рынка', heatmap: 'Карта рынка', seasonality: 'Сезонность',
  buffett: 'Индикатор Баффетта', 'cbr-flows': 'Поток капитала', signals: 'Сигналы',
  screener: 'Скринер сигналов',
};

export default function AdminIndicatorPage() {
  const { user, loading: authLoading } = useAuth();
  const { key } = useParams<{ key: string }>();
  const navigate = useNavigate();
  const path = indicatorPath(key || '');
  const [days, setDays] = usePersistedState<number>('frame:admin:ind:days', 30);
  const [data, setData] = useState<IndicatorReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const dim = useDelayedFlag(loading && !!data);

  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') navigate('/', { replace: true });
  }, [authLoading, user, navigate]);

  useEffect(() => {
    if (!user || user.role !== 'admin' || !key) return;
    let alive = true;
    setLoading(true);
    setError(null);
    getIndicator(path, { days })
      .then(r => { if (alive) setData(r); })
      .catch((e: Error) => { if (alive) setError(e.message || 'Не удалось загрузить'); })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [user, key, path, days]);

  if (authLoading || !user || user.role !== 'admin') return null;

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-10">
      <div className="flex items-center justify-between gap-3 mb-6">
        <Link to="/admin/stats" className="flex items-center gap-1 text-sm hover:opacity-80"
              style={{ color: 'var(--text-secondary)' }}>
          <ArrowLeft size={14} /> Назад к статистике
        </Link>
        <Dropdown<string>
          options={[
            { key: '7', label: '7 дней' }, { key: '30', label: '30 дней' },
            { key: '90', label: '90 дней' }, { key: '180', label: '180 дней' },
          ]}
          value={String(days)} onChange={v => setDays(Number(v))}
        />
      </div>

      {error && (
        <Card padding="md" className="mb-6">
          <div className="flex items-center gap-2" style={{ color: 'var(--danger)' }}>
            <AlertCircle size={16} /><span>{error}</span>
          </div>
        </Card>
      )}

      {!data && loading && (
        <div className="space-y-4">
          <Skeleton height={80} rounded="lg" />
          <Skeleton height={260} rounded="lg" />
          <Skeleton height={300} rounded="lg" />
        </div>
      )}

      {data && (
        <div className="space-y-4 md:space-y-5" style={{ opacity: dim ? 0.55 : 1, transition: 'opacity 120ms' }}>
          <div className="flex items-center gap-3">
            <IndicatorGlyph path={data.path} size={44} />
            <div>
              <h1 className="text-xl md:text-2xl font-semibold" style={{ color: 'var(--text-primary)' }}>
                {data.name}
              </h1>
              <p className="text-sm" style={{ color: 'var(--text-muted)' }}>
                <b style={{ color: 'var(--text-primary)', fontFamily: MONO }}>{ru(data.people)}</b> человек ·{' '}
                {ru(data.registered)} с аккаунтом · {ru(data.views)} просмотров
              </p>
            </div>
          </div>

          {data.trend.length > 1 && (
            <Card padding="md" className="md:p-5">
              <SimpleChart
                data={data.trend.map(t => ({ time: t.date, value: t.people }))}
                primaryColor="var(--accent)"
                primaryLabel="Людей в день"
                formatValue={(v) => Math.round(v).toString()}
                showValueHeader={false}
                legendPosition="top"
                showDownloadButton={false}
                showWatermark={false}
                showNavigator={false}
                hideTime
                defaultHistogram
                height={240}
              />
            </Card>
          )}

          <InsideBlock data={data} />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
            <FlowList title="Откуда приходят сюда" rows={data.prev} />
            <FlowList title="Куда уходят отсюда" rows={data.next} />
          </div>

          <Card padding="md" className="md:p-5">
            <Eyebrow>Кто смотрит чаще всех</Eyebrow>
            {data.top_people.length === 0 ? <Empty text="За период никого" /> : (
              <div className="flex flex-col">
                {data.top_people.map(p => (
                  <button
                    key={p.ident}
                    type="button"
                    onClick={() => navigate(p.user_id ? `/admin/users/${p.user_id}` : `/admin/guests/${p.ident.slice(1)}`)}
                    className="grid items-center text-left hover:bg-white/[0.03] transition-colors"
                    style={{
                      gridTemplateColumns: '1fr auto auto auto', gap: 'var(--sp-3)', padding: '8px 6px',
                      borderTop: '1px solid color-mix(in srgb, var(--border-color) 20%, transparent)',
                    }}
                  >
                    <span className="truncate text-sm" style={{ color: 'var(--text-primary)' }}>
                      {p.name || `гость ${p.ident.slice(1, 9)}`}
                    </span>
                    <span className="text-xs" style={{ color: 'var(--text-muted)', fontFamily: MONO }}>
                      {p.days} дн.
                    </span>
                    <span className="text-sm font-semibold" style={{ fontFamily: MONO }}>{ru(p.views)}</span>
                    <ChevronRight size={15} style={{ color: 'var(--text-muted)' }} />
                  </button>
                ))}
              </div>
            )}
          </Card>
        </div>
      )}
    </div>
  );
}

/* ── что внутри раздела: у каждого своё ───────────────────────────────── */

function InsideBlock({ data }: { data: IndicatorReport }) {
  const ins = data.inside;

  if (ins.kind === 'none') {
    return (
      <Card padding="md" className="md:p-5">
        <Eyebrow>Что внутри</Eyebrow>
        <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
          Это один график без выбора активов — внутри раздела выбирать нечего.
          Смотри ниже, откуда сюда приходят и куда уходят.
        </p>
      </Card>
    );
  }

  if (ins.kind === 'assets') {
    return (
      <Card padding="md" className="md:p-5">
        <Eyebrow>Какие активы смотрят</Eyebrow>
        <Bars rows={ins.assets.map(a => ({
          key: a.secid, label: a.name, note: a.secid, value: a.people, extra: `${ru(a.views)} просм.`,
          icon: <InstrumentIcon sectype={a.secid} size={24} />,
        }))} empty="За период активы не выбирали" />
      </Card>
    );
  }

  if (ins.kind === 'funds_money') {
    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
        <Card padding="md" className="md:p-5">
          <Eyebrow>Какие категории фондов</Eyebrow>
          <Bars rows={ins.categories.map(c => ({
            key: c.key, label: CATEGORY_RU[c.key] || c.key, value: c.people, extra: `${ru(c.views)} раз`,
          }))} empty={SINCE_NOTE} />
        </Card>
        <Card padding="md" className="md:p-5">
          <Eyebrow>Режим и выбранные фонды</Eyebrow>
          <Bars rows={ins.views.map(v => ({
            key: v.key, label: v.key === 'flows' ? 'Притоки и оттоки' : 'Стоимость активов', value: v.people,
          }))} empty={SINCE_NOTE} />
          {ins.funds.length > 0 && (
            <div className="mt-4">
              <Bars rows={ins.funds.map(f => ({
                key: f.ticker, label: f.name || f.ticker, note: f.ticker, value: f.people,
                icon: <FundLogoChip ticker={f.ticker} ukId={f.uk_id} />,
              }))} empty="" />
            </div>
          )}
        </Card>
      </div>
    );
  }

  if (ins.kind === 'fund_trades') {
    return (
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3 md:gap-4">
        <Card padding="md" className="md:p-5">
          <Eyebrow>Какие вкладки</Eyebrow>
          <Bars rows={ins.tabs.map(t => ({
            key: t.key, label: TAB_RU[t.key] || t.key, value: t.people, extra: `${ru(t.views)} раз`,
          }))} empty={SINCE_NOTE} />
        </Card>
        <Card padding="md" className="md:p-5">
          <Eyebrow>Бумаги во вкладке «По бумаге»</Eyebrow>
          <Bars rows={ins.assets.map(a => ({ key: a.name, label: a.name, value: a.people }))} empty={SINCE_NOTE} />
        </Card>
        <Card padding="md" className="md:p-5">
          <Eyebrow>Какие фонды открывают</Eyebrow>
          <Bars rows={ins.funds.map(f => ({
            key: f.ticker, label: f.name || f.ticker, note: f.ticker, value: f.people,
            icon: <FundLogoChip ticker={f.ticker} ukId={f.uk_id} />,
          }))} empty={SINCE_NOTE} />
        </Card>
      </div>
    );
  }

  // terminal
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
      <Card padding="md" className="md:p-5">
        <Eyebrow>Как собирают терминал</Eyebrow>
        <div className="grid grid-cols-3 gap-3">
          <Stat label="с раскладкой" value={ins.with_layout} />
          <Stat label="окон в среднем" value={ins.avg_panels} />
          <Stat label="максимум окон" value={ins.max_panels} />
        </div>
        <p className="text-xs mt-3" style={{ color: 'var(--text-muted)' }}>{SINCE_NOTE}</p>
      </Card>
      <Card padding="md" className="md:p-5">
        <Eyebrow>Из чего собирают окна</Eyebrow>
        <Bars rows={ins.panel_types.map(t => ({
          key: t.key, label: PANEL_RU[t.key] || t.key, value: t.people, extra: `${ru(t.panels)} окон`,
          icon: <IndicatorGlyph path={`/${t.key}`} size={24} />,
        }))} empty={SINCE_NOTE} />
      </Card>
    </div>
  );
}

const SINCE_NOTE = 'Эти данные собираем с 21.09.2026 — пока их мало';

/* ── мелкие кирпичи ───────────────────────────────────────────────────── */

function Eyebrow({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-xs uppercase mb-3" style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}>
      {children}
    </p>
  );
}
function Empty({ text }: { text: string }) {
  return <p className="py-4 text-center text-sm" style={{ color: 'var(--text-muted)' }}>{text}</p>;
}
function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <div className="text-xl font-semibold" style={{ fontFamily: MONO, color: 'var(--text-primary)' }}>{ru(value)}</div>
      <div className="text-xs" style={{ color: 'var(--text-muted)' }}>{label}</div>
    </div>
  );
}

function FundLogoChip({ ticker, ukId }: { ticker: string; ukId?: string | number | null }) {
  const logo = resolveFundLogo(ticker, ukId ?? null);
  if (!logo) return null;
  return (
    <span
      className="flex items-center justify-center rounded-full overflow-hidden text-[10px] font-bold shrink-0"
      style={{ width: 24, height: 24, backgroundColor: logo.bg, color: logo.color }}
      title={logo.name}
    >
      {logo.img ? <img src={logo.img} alt="" className="w-full h-full object-cover" /> : logo.letter}
    </span>
  );
}

function Bars({ rows, empty }: {
  rows: { key: string; label: string; note?: string; value: number; extra?: string; icon?: React.ReactNode }[];
  empty: string;
}) {
  if (!rows.length) return empty ? <Empty text={empty} /> : null;
  const max = Math.max(...rows.map(r => r.value), 1);
  return (
    <div className="flex flex-col">
      {rows.map(r => (
        <div key={r.key} className="relative grid items-center"
             style={{ gridTemplateColumns: r.icon ? 'auto 1fr auto' : '1fr auto', gap: 'var(--sp-2)', padding: '7px 6px' }}>
          <span aria-hidden className="absolute inset-y-px left-0 transition-[width] duration-500"
                style={{ width: `${(r.value / max) * 100}%`, backgroundColor: 'color-mix(in srgb, var(--accent) 11%, transparent)' }} />
          {r.icon && <span className="relative">{r.icon}</span>}
          <span className="relative flex items-baseline gap-2 min-w-0">
            <span className="truncate text-sm" style={{ color: 'var(--text-primary)' }}>{r.label}</span>
            {r.note && <span className="text-xs shrink-0" style={{ color: 'var(--text-muted)' }}>{r.note}</span>}
          </span>
          <span className="relative text-sm font-semibold" style={{ fontFamily: MONO }}>
            {ru(r.value)}
            {r.extra && <em className="not-italic ml-1.5 text-xs font-normal" style={{ color: 'var(--text-muted)' }}>{r.extra}</em>}
          </span>
        </div>
      ))}
    </div>
  );
}

function FlowList({ title, rows }: { title: string; rows: IndicatorReport['prev'] }) {
  const navigate = useNavigate();
  return (
    <Card padding="md" className="md:p-5">
      <Eyebrow>{title}</Eyebrow>
      {rows.length === 0 ? <Empty text="Нет переходов" /> : (
        <div className="flex flex-col">
          {rows.map(r => {
            const isInd = r.path.startsWith('/') && r.name !== r.path;
            const label = r.path.startsWith('(') ? r.path.slice(1, -1) : (r.name !== r.path ? r.name : (PAGE_NAMES[r.path] || r.path));
            const inner = (
              <>
                <span className="flex items-center gap-2 min-w-0">
                  {isInd && <IndicatorGlyph path={r.path} size={22} />}
                  <span className="truncate text-sm" style={{ color: 'var(--text-primary)' }}>{label}</span>
                </span>
                <span className="text-sm font-semibold" style={{ fontFamily: MONO }}>{ru(r.people)}</span>
              </>
            );
            const cls = 'grid items-center text-left';
            const st = { gridTemplateColumns: '1fr auto', gap: 'var(--sp-3)', padding: '7px 6px' } as const;
            return isInd ? (
              <button key={r.path} type="button" className={`${cls} hover:bg-white/[0.03] transition-colors`} style={st}
                      onClick={() => navigate(`/admin/indicator/${indicatorKey(r.path)}`)}>
                {inner}
              </button>
            ) : <div key={r.path} className={cls} style={st}>{inner}</div>;
          })}
        </div>
      )}
    </Card>
  );
}
