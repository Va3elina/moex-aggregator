/**
 * AdminOverviewPage — статистика по правилам docs/admin-design.md.
 *
 * Route: /admin/overview (admin-only). Стоит РЯДОМ со старой /admin/stats:
 * пока она не одобрена, ничего в старой не трогаем.
 *
 * Чем отличается от прошлой попытки, которую откатили:
 *  — контролов на экране не больше четырёх (три в шапке + один в разделе);
 *  — срезы людей это САМИ метрики сверху, а не отдельный ряд кнопок под ними;
 *  — на экране одна таблица: списки гостей и пользователей разведены по
 *    разделам, а не стоят рядом;
 *  — разделы меняют вопрос страницы, а не сужают данные, поэтому они не
 *    считаются фильтрами.
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SimpleChart from '../components/SimpleChart';
import { useAuth } from '../contexts/AuthContext';
import { usePersistedState } from '../hooks/usePersistedState';
import { useDelayedFlag } from '../hooks/useDelayedFlag';
import {
  Metric, MetricRow, Panel, BarList, SplitBar, Empty, Failed,
  SectionTabs, DataTable, Badge,
} from '../components/admin/ui/Primitives';
import { PAGE_NAMES } from '../components/admin/ActivityBlocks';
import {
  getAnalyticsStats, getGrowth, listPeople, getRevenue, listAdminUsers,
} from '../services/api';
import type { PeopleReport, AdminUser } from '../services/api';

type Tab = 'over' | 'people' | 'users' | 'usage' | 'money';
const TABS: { key: Tab; label: string }[] = [
  { key: 'over', label: 'Обзор' },
  { key: 'people', label: 'Люди' },
  { key: 'users', label: 'Пользователи' },
  { key: 'usage', label: 'Что смотрят' },
  { key: 'money', label: 'Деньги' },
];

type Preset = '7d' | '30d' | '90d';
const PRESETS: { key: Preset; label: string }[] = [
  { key: '7d', label: '7 дней' }, { key: '30d', label: '30 дней' }, { key: '90d', label: '90 дней' },
];
const DAYS: Record<Preset, number> = { '7d': 7, '30d': 30, '90d': 90 };

const DEVICE_RU: Record<string, string> = {
  desktop: 'Компьютер', mobile: 'Телефон', tablet: 'Планшет', unknown: 'Не определено',
};
const INDICATOR_RU: Record<string, string> = {
  oi: 'ОИ', open_interest: 'ОИ', seasonality: 'Сезонность', repo: 'Репо',
  funds: 'Фонды', heatmap: 'Карта', strength: 'Сила', buffett: 'Баффет',
};
const MONO = "'IBM Plex Mono', monospace";
const ru = (n: number) => n.toLocaleString('ru-RU');
const rub = (v: number) => `${Math.round(v).toLocaleString('ru-RU')} ₽`;
const fmtSec = (s: number) => (s >= 60 ? `${Math.floor(s / 60)}м ${s % 60}с` : `${s}с`);
const fmtDay = (d: string) => `${d.slice(8, 10)}.${d.slice(5, 7)}`;

export default function AdminOverviewPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [tab, setTab] = usePersistedState<Tab>('frame:admin:ov:tab', 'over');
  const [preset, setPreset] = usePersistedState<Preset>('frame:admin:ov:preset', '30d');
  const [segment, setSegment] = usePersistedState<string>('frame:admin:ov:segment', 'all');
  const [device, setDevice] = usePersistedState<string>('frame:admin:ov:device', 'all');
  /** Срез людей выбирается кликом по метрике, а не отдельным рядом кнопок. */
  const [kind, setKind] = usePersistedState<string>('frame:admin:ov:kind', 'all');

  const range = useMemo(() => ({ days: DAYS[preset] }), [preset]);

  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') navigate('/', { replace: true });
  }, [authLoading, user, navigate]);

  if (authLoading || !user || user.role !== 'admin') return null;

  return (
    <div className="max-w-6xl mx-auto px-4 md:px-6 py-8 md:py-10">
      <div className="flex items-center justify-between gap-3 mb-1">
        <h1 className="text-lg font-semibold" style={{ color: 'var(--text-primary)' }}>Статистика</h1>
        <Link to="/admin/stats" className="text-xs flex items-center gap-1 hover:opacity-70"
              style={{ color: 'var(--text-secondary)' }}>
          <ArrowLeft size={13} /> Старая версия
        </Link>
      </div>
      <p className="text-xs mb-5" style={{ color: 'var(--text-muted)' }}>
        Новая версия по правилам оформления. Старая страница на месте.
      </p>

      {/* Весь бюджет контролов страницы — здесь. В разделах максимум один свой. */}
      <div className="flex flex-wrap gap-2 mb-5">
        <Dropdown<string> options={PRESETS} value={preset} onChange={v => setPreset(v as Preset)} />
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все без админов' },
            { key: 'auth', label: 'Авторизованные' },
            { key: 'guest', label: 'Гости' },
            { key: 'everyone', label: 'Все вместе с админами' },
          ]}
          value={segment} onChange={setSegment}
        />
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все устройства' },
            { key: 'desktop', label: 'Компьютер' },
            { key: 'mobile', label: 'Телефон' },
            { key: 'tablet', label: 'Планшет' },
          ]}
          value={device} onChange={setDevice}
        />
      </div>

      <div className="mb-7">
        <SectionTabs tabs={TABS} value={tab} onChange={setTab} />
      </div>

      {tab === 'over'   && <OverTab range={range} segment={segment} device={device}
                                    onPickKind={k => { setKind(k); setTab('people'); }} />}
      {tab === 'people' && <PeopleTab range={range} segment={segment} device={device}
                                      kind={kind} onKind={setKind} />}
      {tab === 'users'  && <UsersTab range={range} />}
      {tab === 'usage'  && <UsageTab range={range} segment={segment} device={device} />}
      {tab === 'money'  && <MoneyTab />}
    </div>
  );
}

/* ── загрузка ─────────────────────────────────────────────────────────── */

function useAsync<T>(fn: () => Promise<T>, deps: unknown[]) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    let alive = true;
    setLoading(true); setFailed(false);
    fn()
      .then(r => { if (alive) setData(r); })
      .catch(() => { if (alive) setFailed(true); })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  const dim = useDelayedFlag(loading && !!data);
  return { data, loading, failed, dim };
}

function Fading({ dim, children }: { dim: boolean; children: React.ReactNode }) {
  return (
    <div className="space-y-6" style={{ opacity: dim ? 0.5 : 1, transition: 'opacity 120ms cubic-bezier(.2,0,0,1)' }}>
      {children}
    </div>
  );
}

/* ── Обзор ────────────────────────────────────────────────────────────── */

function OverTab({ range, segment, device, onPickKind }: {
  range: { days: number }; segment: string; device: string; onPickKind: (k: string) => void;
}) {
  const { data, loading, failed, dim } = useAsync(
    () => Promise.all([
      listPeople({ ...range, segment, device }),
      getAnalyticsStats({ ...range, segment, device }),
      getGrowth(range),
    ]).then(([p, s, g]) => ({ p, s, g })),
    [range, segment, device],
  );
  if (failed) return <Failed />;
  if (!data && loading) return <LoadingStack />;
  if (!data) return null;
  const { p, s, g } = data;
  const c = p.counts;

  // Рост к такому же предыдущему периоду: «этот месяц против прошлого».
  const growth = p.prev_people
    ? Math.round(((c.people - p.prev_people) / p.prev_people) * 100)
    : null;

  return (
    <Fading dim={dim}>
      <MetricRow>
        <Metric label="Людей" value={c.people} active
                delta={growth === null ? null : { text: `${growth >= 0 ? '+' : '−'}${Math.abs(growth)}%`, good: growth >= 0 }}
                hint={`было ${ru(p.prev_people)}`}
                onClick={() => onPickKind('all')} />
        <Metric label="С аккаунтом" value={c.registered}
                hint={c.people ? `${Math.round(c.registered / c.people * 100)}% всех людей` : undefined}
                onClick={() => onPickKind('reg')} />
        <Metric label="Без аккаунта" value={c.guests}
                hint="ни разу не входили"
                onClick={() => onPickKind('guest')} />
        <Metric label="Платят" value={c.paid} hint="активная платная подписка"
                onClick={() => onPickKind('paid')} />
      </MetricRow>

      {s.trends.length > 1 && (
        <Panel title="Сколько людей по дням" note={`${s.trends.length} дней`}>
          <SimpleChart
            data={s.trends.map(t => ({ time: t.date, value: t.visitors }))}
            primaryColor="var(--accent)"
            primaryLabel="Людей"
            formatValue={(v) => Math.round(v).toString()}
            showValueHeader={false}
            legendPosition="top"
            showDownloadButton={false}
            showNavigator={false}
            hideTime
            height={240}
          />
        </Panel>
      )}

      <Panel title="Что смотрят" note="людей за период">
        <BarList items={p.indicators.slice(0, 7).map(i => ({ label: i.name, value: i.people }))} />
      </Panel>

      <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))' }}>
        <Panel title="Откуда приходят" note="людей">
          <BarList items={p.sources.slice(0, 6).map(x => ({ label: x.source, value: x.people }))} />
        </Panel>
        <Panel title="С какой страницы начинают" note="первая открытая страница">
          <BarList items={p.entry_pages.slice(0, 6).map(e => ({
            label: e.name, note: e.name === e.path ? undefined : e.path, value: e.people,
          }))} />
        </Panel>
      </div>

      {/* Возвращаемость — отдельным блоком, а не в ряду наверху: это ответ на
          другой вопрос, и он нужен не каждый раз. */}
      <Panel title="Как часто возвращаются" note="разных дней на сайте за период">
        <BarList
          items={[
            { label: 'Заходили один раз', value: Math.max(c.people - (g?.guests?.days2 ?? 0) - c.registered, 0) },
            { label: 'Два дня и больше', value: g?.guests?.days2 ?? 0 },
            { label: 'Четыре дня и больше', value: c.loyal, extra: `${c.loyal_guests} без аккаунта` },
            { label: 'Семь дней и больше', value: g?.guests?.days7 ?? 0 },
          ]}
        />
      </Panel>
    </Fading>
  );
}

/* ── Люди ─────────────────────────────────────────────────────────────── */

const KINDS: { key: string; label: string; countKey: keyof PeopleReport['counts']; hint: string }[] = [
  { key: 'all', label: 'Всего', countKey: 'people', hint: 'гости и зарегистрированные' },
  { key: 'guest', label: 'Без аккаунта', countKey: 'guests', hint: 'ни разу не входили' },
  { key: 'reg', label: 'С аккаунтом', countKey: 'registered', hint: 'заходили за период' },
  { key: 'loyal', label: 'Ходят часто', countKey: 'loyal', hint: '4 и более разных дня' },
  { key: 'paid', label: 'Платят', countKey: 'paid', hint: 'активная подписка' },
];

function PeopleTab({ range, segment, device, kind, onKind }: {
  range: { days: number }; segment: string; device: string;
  kind: string; onKind: (k: string) => void;
}) {
  const navigate = useNavigate();
  const [sort, setSort] = usePersistedState<string>('frame:admin:ov:psort', 'days');
  const { data, loading, failed, dim } = useAsync(
    () => listPeople({ ...range, segment, device, kind, sort }),
    [range, segment, device, kind, sort],
  );
  if (failed) return <Failed />;
  if (!data && loading) return <LoadingStack />;
  if (!data) return null;
  const rows = data.people;

  return (
    <Fading dim={dim}>
      {/* Срезы — это метрики, а не ряд кнопок под ними. Выбранная подсвечена. */}
      <MetricRow>
        {KINDS.map(k => (
          <Metric
            key={k.key}
            label={k.label}
            value={data.counts[k.countKey]}
            hint={k.hint}
            active={kind === k.key}
            onClick={() => onKind(k.key)}
          />
        ))}
      </MetricRow>

      <Panel
        title={KINDS.find(k => k.key === kind)?.label || 'Люди'}
        note={data.matched > rows.length ? `показано ${rows.length} из ${ru(data.matched)}` : `${rows.length}`}
        action={
          <Dropdown<string>
            options={[
              { key: 'days', label: 'По числу заходов' },
              { key: 'visits', label: 'По визитам' },
              { key: 'views', label: 'По просмотрам' },
              { key: 'last', label: 'Кто был позже' },
            ]}
            value={sort} onChange={setSort} minWidth={170}
          />
        }
      >
        {rows.length === 0 ? <Empty text="За период таких людей не было" /> : (
          <DataTable
            head={[
              { label: 'Кто' }, { label: 'Дней', align: 'right' }, { label: 'Визитов', align: 'right' },
              { label: 'Просмотров', align: 'right', hide: 'md' },
              { label: 'Что смотрит' }, { label: 'Последний раз', hide: 'lg' },
            ]}
          >
            {rows.map(p => (
              <tr
                key={p.ident}
                className="cursor-pointer transition-colors hover:bg-white/[0.03]"
                style={{ borderTop: '1px solid color-mix(in srgb, var(--border-color) 20%, transparent)' }}
                onClick={() => navigate(p.user_id ? `/admin/users/${p.user_id}` : `/admin/guests/${p.ident.slice(1)}`)}
              >
                <td className="px-2 py-2">
                  <div className="flex items-center gap-2 min-w-0">
                    <Badge
                      text={p.kind === 'paid' ? 'платит' : p.kind === 'invite' ? 'инвайт'
                        : p.kind === 'reg' ? 'аккаунт' : 'гость'}
                      tone={p.kind === 'paid' ? 'good' : p.kind === 'guest' ? 'muted' : 'info'}
                    />
                    <span className="truncate text-[13px]" style={{ color: 'var(--text-primary)' }}>
                      {p.name || p.ident.slice(1, 9)}
                    </span>
                  </div>
                </td>
                <td className="px-2 py-2 text-right text-[13px] font-semibold"
                    style={{ fontFamily: MONO, fontVariantNumeric: 'tabular-nums' }}>{p.days}</td>
                <td className="px-2 py-2 text-right text-[13px]"
                    style={{ fontFamily: MONO, color: 'var(--text-secondary)' }}>{p.visits}</td>
                <td className="px-2 py-2 text-right text-[13px] hidden md:table-cell"
                    style={{ fontFamily: MONO, color: 'var(--text-secondary)' }}>{p.views}</td>
                <td className="px-2 py-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
                  <span className="block truncate" style={{ maxWidth: 240 }}>
                    {p.paths.map(x => PAGE_NAMES[x] || x).join(', ') || '—'}
                  </span>
                </td>
                <td className="px-2 py-2 text-xs hidden lg:table-cell"
                    style={{ color: 'var(--text-muted)', fontFamily: MONO }}>{fmtDay(p.last_day)}</td>
              </tr>
            ))}
          </DataTable>
        )}
      </Panel>
    </Fading>
  );
}

/* ── Пользователи ─────────────────────────────────────────────────────── */

const USER_FILTERS = [
  { key: 'all', label: 'Все' }, { key: 'paid', label: 'Платные' },
  { key: 'invite', label: 'По инвайту' }, { key: 'free', label: 'Бесплатные' },
  { key: 'admin', label: 'Админы' },
];

function UsersTab({ range }: { range: { days: number } }) {
  const navigate = useNavigate();
  const [filter, setFilter] = usePersistedState<string>('frame:admin:ov:ufilter', 'all');
  const { data, loading, failed, dim } = useAsync(
    () => listAdminUsers({ ...range, filter, sort: 'last_active' }),
    [range, filter],
  );
  if (failed) return <Failed />;
  if (!data && loading) return <LoadingStack />;
  if (!data) return null;
  const counts = data.counts || {};

  return (
    <Fading dim={dim}>
      <MetricRow>
        <Metric label="Всего" value={counts.all ?? data.total_count} hint="аккаунтов в базе" />
        <Metric label="Платные" value={data.paid_count} hint="купили за деньги" active />
        <Metric label="По инвайту" value={data.invite_count} hint="подписка подарена" />
      </MetricRow>

      <Panel
        title="Пользователи"
        note={`показано ${data.users.length}`}
        action={
          <Dropdown<string>
            options={USER_FILTERS.map(f => ({
              key: f.key,
              label: counts[f.key] !== undefined ? `${f.label} (${counts[f.key]})` : f.label,
            }))}
            value={filter} onChange={setFilter} minWidth={170}
          />
        }
      >
        {data.users.length === 0 ? <Empty text="Никого не нашли" /> : (
          <DataTable
            minWidth={780}
            head={[
              { label: 'Пользователь' }, { label: 'Подписка' },
              { label: 'Визитов', align: 'right' }, { label: 'Время', align: 'right', hide: 'md' },
              { label: 'Был', align: 'right', hide: 'lg' }, { label: 'Создан', hide: 'lg' },
            ]}
          >
            {data.users.map(u => <UserRow key={u.id} u={u} onOpen={() => navigate(`/admin/users/${u.id}`)} />)}
          </DataTable>
        )}
      </Panel>
    </Fading>
  );
}

function UserRow({ u, onOpen }: { u: AdminUser; onOpen: () => void }) {
  return (
    <tr
      className="cursor-pointer transition-colors hover:bg-white/[0.03]"
      style={{ borderTop: '1px solid color-mix(in srgb, var(--border-color) 20%, transparent)' }}
      onClick={onOpen}
    >
      <td className="px-2 py-2">
        <div className="flex items-center gap-2 min-w-0">
          <div
            className="flex items-center justify-center shrink-0 rounded-full text-[11px] font-bold"
            style={{ width: 26, height: 26, backgroundColor: 'var(--accent)', color: '#fff' }}
          >
            {(u.display_name || u.email || '?')[0].toUpperCase()}
          </div>
          <div className="min-w-0">
            <div className="truncate text-[13px] font-semibold" style={{ color: 'var(--text-primary)' }}>
              {u.display_name || u.username || (u.email || '').split('@')[0]}
            </div>
            <div className="truncate text-[11px]" style={{ color: 'var(--text-muted)' }}>{u.email}</div>
          </div>
        </div>
      </td>
      <td className="px-2 py-2">
        {u.plan ? (
          <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
            <Badge text={u.plan} tone={u.is_invite ? 'muted' : 'good'} />
            {u.plan_expires_at && (
              <span className="text-[11px]" style={{ color: 'var(--text-muted)', fontFamily: MONO }}>
                до {new Date(u.plan_expires_at).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })}
              </span>
            )}
          </span>
        ) : <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>free</span>}
      </td>
      <td className="px-2 py-2 text-right text-[13px] font-semibold"
          style={{ fontFamily: MONO, fontVariantNumeric: 'tabular-nums' }}>{u.sessions_count}</td>
      <td className="px-2 py-2 text-right text-[13px] hidden md:table-cell"
          style={{ fontFamily: MONO, color: 'var(--text-secondary)' }}>
        {u.time_sec ? fmtSec(u.time_sec) : '—'}
      </td>
      <td className="px-2 py-2 text-right text-[11px] hidden lg:table-cell" style={{ color: 'var(--text-muted)' }}>
        {u.last_active_ts ? new Date(u.last_active_ts).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' }) : '—'}
      </td>
      <td className="px-2 py-2 text-[11px] hidden lg:table-cell" style={{ color: 'var(--text-muted)' }}>
        {u.created_at ? new Date(u.created_at).toLocaleDateString('ru-RU') : '—'}
      </td>
    </tr>
  );
}

/* ── Что смотрят ──────────────────────────────────────────────────────── */

function UsageTab({ range, segment, device }: { range: { days: number }; segment: string; device: string }) {
  const [focus, setFocus] = useState<string | null>(null);
  const { data, loading, failed, dim } = useAsync(
    () => Promise.all([
      listPeople({ ...range, segment, device, seen: focus ? [focus] : undefined }),
      getAnalyticsStats({ ...range, segment, device }),
    ]).then(([p, s]) => ({ p, s })),
    [range, segment, device, focus],
  );
  if (failed) return <Failed />;
  if (!data && loading) return <LoadingStack />;
  if (!data) return null;
  const { p, s } = data;

  const oi = p.indicators.find(i => i.path === '/oi')?.people ?? 0;
  const funds = p.indicators
    .filter(i => i.path === '/funds-money' || i.path === '/fund-trades')
    .reduce((a, i) => Math.max(a, i.people), 0);

  return (
    <Fading dim={dim}>
      <MetricRow>
        <Metric label="Людей в срезе" value={p.matched}
                hint={focus ? `смотрят «${p.indicators.find(i => i.path === focus)?.name ?? focus}»` : 'все за период'}
                active={!!focus} onClick={focus ? () => setFocus(null) : undefined} />
        <Metric label="Открытый интерес" value={oi} hint="самый популярный раздел" />
        <Metric label="Фонды" value={funds} hint="деньги и покупки фондов" />
        <Metric label="Просмотров" value={s.summary.pageviews}
                hint={`${(s.summary.pageviews / Math.max(s.summary.visits, 1)).toFixed(1)} за визит`} />
      </MetricRow>

      <Panel title="Разделы" note={focus ? 'клик по выбранному снимает отбор' : 'клик — показать только этих людей'}>
        <BarList
          items={p.indicators.map(i => ({
            key: i.path, label: i.name, value: i.people,
            extra: `${i.registered} с аккаунтом`,
          }))}
          activeKey={focus}
          onPick={k => setFocus(k === focus ? null : k)}
        />
      </Panel>

      <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))' }}>
        <Panel title="Популярные активы" note="и где их смотрят">
          {s.top_assets?.length ? (
            <BarList items={s.top_assets.slice(0, 7).map(a => ({
              label: a.name || a.secid,
              // Индикаторы, на которых актив открывали: без них «популярный»
              // ничего не говорит — непонятно, где именно популярный.
              note: (a.indicators || []).map(i => INDICATOR_RU[i] || i).join(', ') || undefined,
              value: a.visitors,
            }))} />
          ) : <Empty text="За период активы не открывали" />}
        </Panel>
        <Panel title="Устройства" note="людей">
          {s.devices?.length ? (
            <SplitBar parts={s.devices.map((d, i) => ({
              label: DEVICE_RU[d.device] || d.device, value: d.visitors,
              color: ['var(--accent)', 'var(--info)', 'var(--text-muted)'][i] || 'var(--text-muted)',
            }))} />
          ) : <Empty text="Нет данных об устройствах" />}
        </Panel>
      </div>
    </Fading>
  );
}

/* ── Деньги ───────────────────────────────────────────────────────────── */

function MoneyTab() {
  const { data, loading, failed, dim } = useAsync(() => getRevenue(), []);
  if (failed) return <Failed />;
  if (!data && loading) return <LoadingStack />;
  if (!data) return null;
  const conv = Math.round((data.payers / Math.max(data.registered, 1)) * 100);

  return (
    <Fading dim={dim}>
      <MetricRow>
        <Metric label="Выручка" value={rub(data.rub)} hint="без триалов, возвратов и инвайтов" active />
        <Metric label="Платящих" value={data.payers} hint={`${data.payments} платежей`} />
        <Metric label="Платят сейчас" value={data.active_paid} hint={`${rub(data.rub_30)} за 30 дней`} />
        <Metric label="Конверсия" value={`${conv}%`} hint={`${data.payers} из ${data.registered} аккаунтов`} />
      </MetricRow>

      {data.by_month.length > 1 && (
        <Panel title="Выручка по месяцам">
          <SimpleChart
            data={data.by_month.map(m => ({ time: `${m.month}-01`, value: m.rub }))}
            primaryColor="var(--accent)"
            primaryLabel="Выручка"
            formatValue={(v) => `${Math.round(v / 1000)}т`}
            showValueHeader={false}
            legendPosition="top"
            showDownloadButton={false}
            showNavigator={false}
            hideTime
            defaultHistogram
            height={220}
          />
        </Panel>
      )}

      <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))' }}>
        <Panel title="Что покупают" note="платежей">
          <BarList items={data.mix.map(m => ({
            label: `${m.tier === 'pro' ? 'Pro' : 'Basic'} · ${m.period === 'yearly' ? 'год' : 'месяц'}`,
            value: m.n, extra: rub(m.rub),
          }))} />
        </Panel>
        <Panel title="Повторные платежи" note="людей">
          <BarList items={data.repeat.map(r => ({
            label: r.payments === 1 ? 'заплатил один раз' : `платил ${r.payments} раза`,
            value: r.users,
          }))} />
        </Panel>
      </div>
    </Fading>
  );
}

function LoadingStack() {
  return (
    <div className="space-y-6">
      <Skeleton height={72} rounded="md" />
      <Skeleton height={200} rounded="md" />
      <Skeleton height={200} rounded="md" />
    </div>
  );
}
