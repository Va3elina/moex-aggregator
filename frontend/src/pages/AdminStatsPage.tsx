/**
 * AdminStatsPage — admin-only страница со статистикой использования сайта.
 *
 * Source endpoints (все role=admin):
 *   GET /api/analytics/stats   — summary + trends (line chart) + top lists
 *   GET /api/analytics/funnel  — пошаговая конверсия
 *
 * Структура (вертикально):
 *  1. 4 summary cards (DAU / Sessions / AvgTime / Events) + trends chart
 *  2. Top lists (pages/instruments/exports/modes)
 *  3. Users — drill-down таблица в /admin/users/:id
 *  4. Funnel — конверсия по воронке landing → indicator → action
 *
 * Раньше были блоки Realtime/Retention/A/B — удалены 2026-05-13 по решению
 * Вадима. Realtime — для одного admin'а лишний шум; Retention требовал
 * хотя бы пары недель регулярных logins (не было); A/B framework был но не
 * использовался в проде.
 */
import { useEffect, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { BarChart3, TrendingUp, TrendingDown, Activity, Users, Clock, MousePointerClick, Search, ChevronRight } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SimpleChart from '../components/SimpleChart';
import AvatarImg from '../components/AvatarImg';
import { useAuth } from '../contexts/AuthContext';
import {
  getAnalyticsStats,
  getAnalyticsFunnel,
  listAdminUsers,
} from '../services/api';
import type {
  AnalyticsStats,
  FunnelResponse,
  AdminUser,
} from '../services/api';

export default function AdminStatsPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();

  const [days, setDays] = useState<number>(7);
  const [segment, setSegment] = useState<string>('all');
  const [device, setDevice] = useState<string>('all');
  const [data, setData] = useState<AnalyticsStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [funnel, setFunnel] = useState<FunnelResponse | null>(null);

  // Default funnel: pageview('/') → pageview('/seasonality') → instrument_select → chart_export
  // User может выбрать другой preset через dropdown
  const [funnelPreset, setFunnelPreset] = useState<string>('seasonality_export');

  // Guard: только admin
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') {
      navigate('/', { replace: true });
    }
  }, [authLoading, user, navigate]);

  // Funnel presets — типовые воронки. Backend принимает строку 'type:path,type:path,...'
  const FUNNEL_PRESETS: Record<string, { label: string; steps: string }> = {
    seasonality_export: {
      label: 'Сезонность → экспорт',
      steps: 'pageview:/,pageview:/seasonality,instrument_select,chart_export',
    },
    heatmap_to_seasonality: {
      label: 'Карта рынка → Сезонность',
      steps: 'pageview:/,pageview:/heatmap,pageview:/seasonality',
    },
    landing_to_indicator: {
      label: 'Главная → любой индикатор',
      steps: 'pageview:/,pageview:/oi',
    },
    full_engagement: {
      label: 'Полное вовлечение',
      steps: 'pageview:/,pageview:/seasonality,seasonality_mode,instrument_select,chart_export',
    },
  };

  // Fetch summary stats
  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    setLoading(true);
    setError(null);
    getAnalyticsStats({ days, segment, device })
      .then(setData)
      .catch((e: Error) => setError(e.message || 'Не удалось загрузить статистику'))
      .finally(() => setLoading(false));
  }, [user, days, segment, device]);

  // Fetch funnel
  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    const preset = FUNNEL_PRESETS[funnelPreset];
    if (!preset) return;
    getAnalyticsFunnel({ steps: preset.steps, days })
      .then(setFunnel)
      .catch(() => setFunnel(null));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user, funnelPreset, days]);

  if (authLoading || !user || user.role !== 'admin') {
    return null;
  }

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-10">
      {/* Header */}
      <div className="flex items-start gap-3 mb-6 md:mb-8">
        <div
          className="flex items-center justify-center flex-shrink-0"
          style={{
            width: 44, height: 44,
            borderRadius: 'var(--radius-md, 8px)',
            background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
            color: 'var(--accent)',
          }}
        >
          <BarChart3 size={22} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Статистика сайта
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Custom analytics · только для администратора
          </p>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center mb-6 md:mb-8" style={{ gap: 'var(--sp-2)' }}>
        <Dropdown<string>
          options={[
            { key: '1', label: 'Сегодня' },
            { key: '7', label: '7 дней' },
            { key: '30', label: '30 дней' },
            { key: '90', label: '90 дней' },
            { key: '180', label: '180 дней' },
          ]}
          value={String(days)}
          onChange={(v) => setDays(Number(v))}
        />
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все' },
            { key: 'auth', label: 'Авторизованные' },
            { key: 'guest', label: 'Гости' },
            { key: 'admin', label: 'Только admin' },
          ]}
          value={segment}
          onChange={setSegment}
        />
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все устройства' },
            { key: 'desktop', label: 'Desktop' },
            { key: 'mobile', label: 'Mobile' },
            { key: 'tablet', label: 'Tablet' },
          ]}
          value={device}
          onChange={setDevice}
        />
      </div>

      {error && (
        <Card padding="md" className="mb-6">
          <p style={{ color: 'var(--danger)' }}>Ошибка: {error}</p>
        </Card>
      )}

      {/* Summary cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4 mb-6 md:mb-8">
        {data ? (
          <>
            <SummaryCard
              icon={<Users size={16} />}
              label="DAU"
              value={data.summary.dau}
              delta={data.summary.delta_dau}
              deltaSuffix=" vs пред."
            />
            <SummaryCard
              icon={<Activity size={16} />}
              label="Сессий"
              value={data.summary.sessions}
              deltaPct={data.summary.delta_sessions_pct}
            />
            <SummaryCard
              icon={<Clock size={16} />}
              label="Среднее время"
              value={data.summary.avg_session_sec}
              format={(v) => formatDuration(v)}
              delta={data.summary.delta_avg_session_sec}
              deltaSuffix=" сек"
            />
            <SummaryCard
              icon={<MousePointerClick size={16} />}
              label="Events"
              value={data.summary.events}
              deltaPct={data.summary.delta_events_pct}
            />
          </>
        ) : loading ? (
          <>
            <Skeleton height={108} rounded="lg" />
            <Skeleton height={108} rounded="lg" />
            <Skeleton height={108} rounded="lg" />
            <Skeleton height={108} rounded="lg" />
          </>
        ) : null}
      </div>

      {/* Trends — SimpleChart с двумя линиями: DAU (primary, accent) +
          Sessions (secondary, мягкий цвет). Раньше был custom TrendsChart
          (inline SVG), но он не learn axis-labels положение → надписи
          разъезжались. SimpleChart даёт правильные axes / pills / hover. */}
      <Section title="Динамика">
        {data && data.trends.length > 0 ? (
          <Card padding="md" className="md:p-5">
            <SimpleChart
              data={data.trends.map(t => ({ time: t.date, value: t.dau }))}
              secondaryData={data.trends.map(t => ({ time: t.date, value: t.sessions }))}
              showSecondary={true}
              primaryColor="var(--accent)"
              secondaryColor="var(--accent-secondary)"
              primaryLabel="DAU"
              secondaryLabel="Сессии"
              formatValue={(v) => Math.round(v).toString()}
              formatSecondaryAxis={(v) => Math.round(v).toString()}
              showValueHeader={false}
              legendPosition="top"
              showDownloadButton={false}
              showNavigator={false}
              hideTime={true}
              height={320}
              chartPadding={{ right: 100 }}
            />
          </Card>
        ) : loading ? (
          <Skeleton height={320} rounded="lg" />
        ) : (
          <Card padding="md">
            <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
              Недостаточно данных для построения графика
            </p>
          </Card>
        )}
      </Section>

      {/* Top lists row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-6 md:mb-8">
        <TopList
          title="Топ страниц"
          items={data?.top_pages.map((p) => ({ label: p.path, value: p.views })) || null}
          loading={loading}
          emptyText="Нет pageview-событий"
        />
        <TopList
          title="Топ тикеров"
          items={data?.top_instruments.map((p) => ({ label: p.secid, value: p.selects })) || null}
          loading={loading}
          emptyText="Нет выборов тикера"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-6 md:mb-8">
        <TopList
          title="Экспорты PNG"
          items={data?.top_exports.map((p) => ({ label: p.indicator, value: p.count })) || null}
          loading={loading}
          emptyText="Никто не экспортировал"
        />
        <TopList
          title="Сезонность: режимы"
          items={data?.mode_distribution.map((p) => ({ label: p.mode, value: p.count })) || null}
          loading={loading}
          emptyText="Нет переключений режима"
        />
      </div>

      {/* Users — кликабельная таблица для drill-down на /admin/users/:id */}
      <Section title="Пользователи">
        <UsersBlock days={days} />
      </Section>

      {/* Funnel */}
      <Section title="Воронка конверсии">
        <Card padding="md" className="md:p-5">
          <div className="mb-4">
            <Dropdown<string>
              options={Object.entries(FUNNEL_PRESETS).map(([k, v]) => ({ key: k, label: v.label }))}
              value={funnelPreset}
              onChange={setFunnelPreset}
            />
          </div>
          {funnel && funnel.steps.length > 0 ? (
            <FunnelChart data={funnel} />
          ) : (
            <p className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
              Загрузка воронки...
            </p>
          )}
        </Card>
      </Section>

    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SUBCOMPONENTS
// ════════════════════════════════════════════════════════════════════════════

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mb-6 md:mb-8">
      <div className="flex items-center gap-3 mb-4">
        <p
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
        >
          {title}
        </p>
        <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
      </div>
      {children}
    </section>
  );
}

interface SummaryCardProps {
  icon: React.ReactNode;
  label: string;
  value: number;
  delta?: number | null;
  deltaPct?: number | null;
  deltaSuffix?: string;
  format?: (v: number) => string;
}
function SummaryCard({ icon, label, value, delta, deltaPct, deltaSuffix = '', format }: SummaryCardProps) {
  const display = format ? format(value) : value.toLocaleString('ru-RU');
  const deltaValue = deltaPct !== undefined && deltaPct !== null
    ? `${deltaPct >= 0 ? '+' : ''}${deltaPct}%`
    : delta !== undefined && delta !== null
      ? `${delta >= 0 ? '+' : ''}${delta}${deltaSuffix}`
      : null;
  const deltaPositive = (deltaPct ?? delta ?? 0) >= 0;

  return (
    <Card padding="md" className="md:p-5">
      <div className="flex items-center gap-2 mb-2" style={{ color: 'var(--text-muted)' }}>
        {icon}
        <span className="text-xs uppercase" style={{ letterSpacing: '0.1em', fontWeight: 600 }}>
          {label}
        </span>
      </div>
      <div
        className="font-bold mb-1"
        style={{
          color: 'var(--text-primary)',
          fontSize: 'clamp(1.5rem, 3vw, 2.25rem)',
          letterSpacing: '-0.02em',
          fontFamily: "'IBM Plex Mono', monospace",
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        {display}
      </div>
      {deltaValue !== null && (
        <div className="flex items-center gap-1">
          {deltaPositive ? (
            <TrendingUp size={12} style={{ color: 'var(--success)' }} />
          ) : (
            <TrendingDown size={12} style={{ color: 'var(--danger)' }} />
          )}
          <span
            className="text-xs"
            style={{
              color: deltaPositive ? 'var(--success)' : 'var(--danger)',
              fontFamily: "'IBM Plex Mono', monospace",
            }}
          >
            {deltaValue}
          </span>
        </div>
      )}
    </Card>
  );
}

interface TopListProps {
  title: string;
  items: { label: string; value: number }[] | null;
  loading: boolean;
  emptyText: string;
}
function TopList({ title, items, loading, emptyText }: TopListProps) {
  if (loading && !items) return <Skeleton height={240} rounded="lg" />;
  const max = items && items.length > 0 ? items[0].value : 1;
  return (
    <Card padding="md" className="md:p-5">
      <div className="flex items-center gap-2 mb-3">
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          {title}
        </span>
      </div>
      {!items || items.length === 0 ? (
        <p className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
          {emptyText}
        </p>
      ) : (
        <div className="space-y-1.5">
          {items.map((it, i) => (
            <div key={`${it.label}-${i}`} className="relative">
              <div
                className="absolute inset-y-0 left-0 rounded"
                style={{
                  width: `${(it.value / max) * 100}%`,
                  backgroundColor: 'color-mix(in srgb, var(--accent) 14%, transparent)',
                }}
              />
              <div className="relative flex items-center justify-between py-1.5 px-2">
                <span
                  className="text-sm truncate"
                  style={{ color: 'var(--text-primary)' }}
                  title={it.label}
                >
                  {it.label || '—'}
                </span>
                <span
                  className="text-sm font-semibold flex-shrink-0 ml-2"
                  style={{
                    color: 'var(--text-primary)',
                    fontFamily: "'IBM Plex Mono', monospace",
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {it.value.toLocaleString('ru-RU')}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════════════════

function formatDuration(seconds: number): string {
  if (!seconds || seconds < 0) return '0с';
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h > 0) return `${h}ч ${m}м`;
  if (m > 0) return `${m}м ${s}с`;
  return `${s}с`;
}

// ════════════════════════════════════════════════════════════════════════════
// FUNNEL CHART — каждый шаг = bar, ширина пропорциональна conversion %
// ════════════════════════════════════════════════════════════════════════════

function FunnelChart({ data }: { data: FunnelResponse }) {
  return (
    <div className="space-y-3">
      {data.steps.map((s, i) => {
        const widthPct = s.conversion_pct;
        const isFirst = i === 0;
        const dropPrev = isFirst ? 0 : (data.steps[i - 1].sessions - s.sessions);
        return (
          <div key={s.step}>
            <div className="flex items-center justify-between mb-1" style={{ gap: 'var(--sp-2)' }}>
              <span className="text-sm font-semibold truncate" style={{ color: 'var(--text-primary)' }} title={s.label}>
                Шаг {s.step + 1}: {s.label}
              </span>
              <span className="text-sm flex-shrink-0" style={{
                color: 'var(--text-secondary)',
                fontFamily: "'IBM Plex Mono', monospace",
                fontVariantNumeric: 'tabular-nums',
              }}>
                {s.sessions} сессий · {s.conversion_pct}%
              </span>
            </div>
            <div className="relative h-7" style={{
              backgroundColor: 'var(--bg-secondary)',
              borderRadius: 4,
              overflow: 'hidden',
            }}>
              <div
                className="absolute inset-y-0 left-0 transition-all duration-300"
                style={{
                  width: `${widthPct}%`,
                  background: `linear-gradient(90deg, var(--accent), color-mix(in srgb, var(--accent) 70%, transparent))`,
                  borderRadius: '4px 0 0 4px',
                }}
              />
            </div>
            {!isFirst && dropPrev > 0 && (
              <p className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
                ↓ потеряно {dropPrev} сессий ({(100 - (s.sessions / Math.max(data.steps[i - 1].sessions, 1)) * 100).toFixed(1)}%)
              </p>
            )}
          </div>
        );
      })}
    </div>
  );
}



// ════════════════════════════════════════════════════════════════════════════
// USERS BLOCK — drill-down таблица пользователей
// ════════════════════════════════════════════════════════════════════════════

function UsersBlock({ days }: { days: number }) {
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState('last_active');

  // Debounced search — fetch'аем не на каждое нажатие
  useEffect(() => {
    const t = window.setTimeout(() => {
      setLoading(true);
      listAdminUsers({ days, sort, search: search.trim() })
        .then(r => setUsers(r.users))
        .catch(() => setUsers([]))
        .finally(() => setLoading(false));
    }, 250);
    return () => clearTimeout(t);
  }, [days, sort, search]);

  return (
    <Card padding="md" className="md:p-5">
      <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-2)' }}>
        {/* Search */}
        <div
          className="flex items-center flex-1 min-w-[200px]"
          style={{
            backgroundColor: 'var(--bg-secondary)',
            border: '1.5px solid var(--text-primary)',
            borderRadius: 9999,
            padding: 'var(--sp-2) var(--sp-3)',
            gap: 'var(--sp-2)',
          }}
        >
          <Search size={14} style={{ color: 'var(--text-muted)' }} />
          <input
            type="search"
            name="user_search"
            id="admin-user-search"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Поиск по email / имени"
            autoComplete="off"
            className="flex-1 bg-transparent outline-none"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              minWidth: 0,
            }}
          />
        </div>
        <Dropdown<string>
          options={[
            { key: 'last_active', label: 'По активности' },
            { key: 'events', label: 'По событиям' },
            { key: 'sessions', label: 'По сессиям' },
            { key: 'created', label: 'По регистрации' },
          ]}
          value={sort}
          onChange={setSort}
        />
        <span className="text-xs ml-auto" style={{ color: 'var(--text-muted)' }}>
          {users.length} {users.length === 1 ? 'пользователь' : users.length < 5 ? 'пользователя' : 'пользователей'}
        </span>
      </div>

      {/* Table */}
      <div className="overflow-x-auto -mx-2">
        <table className="w-full" style={{ minWidth: 720 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-color)' }}>
              <UCol>Пользователь</UCol>
              <UCol align="left" hide="md">Роль</UCol>
              <UCol align="right">Сессий</UCol>
              <UCol align="right" hide="md">Events</UCol>
              <UCol align="right" hide="lg">Послед. активность</UCol>
              <UCol align="left" hide="lg">Создан</UCol>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {loading && users.length === 0 && (
              <tr>
                <td colSpan={7} className="py-6">
                  <Skeleton height={24} rounded="md" />
                </td>
              </tr>
            )}
            {!loading && users.length === 0 && (
              <tr>
                <td colSpan={7} className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
                  Никого не нашли
                </td>
              </tr>
            )}
            {users.map(u => (
              <tr
                key={u.id}
                className="hover:bg-white/[0.03] transition-colors cursor-pointer"
                style={{ borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)' }}
                onClick={() => { window.location.href = `/admin/users/${u.id}`; }}
              >
                {/* User cell — avatar + email */}
                <td className="px-2 py-2">
                  <div className="flex items-center gap-2 min-w-0">
                    <div
                      className="flex items-center justify-center flex-shrink-0 rounded-full font-bold text-xs"
                      style={{
                        width: 28, height: 28,
                        backgroundColor: 'var(--accent)',
                        color: '#fff',
                        overflow: 'hidden',
                      }}
                    >
                      <AvatarImg url={u.avatar_url} fallback={(u.email[0] || '?').toUpperCase()} />
                    </div>
                    <div className="min-w-0 flex-1">
                      <div
                        className="text-sm font-semibold truncate"
                        style={{ color: 'var(--text-primary)' }}
                        title={u.display_name || u.email}
                      >
                        {u.display_name || u.username || u.email.split('@')[0]}
                      </div>
                      <div
                        className="text-xs truncate"
                        style={{ color: 'var(--text-muted)' }}
                        title={u.email}
                      >
                        {u.email}
                        {u.plan && (
                          <span className="ml-2" style={{ color: 'var(--accent)' }}>
                            · {u.plan}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                </td>

                <td className="px-2 py-2 hidden md:table-cell">
                  <span
                    className="text-xs px-2 py-0.5 rounded-full font-semibold"
                    style={{
                      backgroundColor:
                        u.role === 'admin' ? 'color-mix(in srgb, var(--danger) 18%, transparent)' :
                        u.role === 'pro' || u.role === 'premium' ? 'color-mix(in srgb, var(--warning) 18%, transparent)' :
                        'color-mix(in srgb, var(--accent) 18%, transparent)',
                      color:
                        u.role === 'admin' ? 'var(--danger)' :
                        u.role === 'pro' || u.role === 'premium' ? 'var(--warning)' :
                        'var(--accent)',
                    }}
                  >
                    {u.role}
                  </span>
                </td>
                <td
                  className="text-right px-2 py-2 text-sm font-semibold"
                  style={{
                    color: 'var(--text-primary)',
                    fontFamily: "'IBM Plex Mono', monospace",
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {u.sessions_count}
                </td>
                <td
                  className="text-right px-2 py-2 text-sm hidden md:table-cell"
                  style={{
                    color: 'var(--text-secondary)',
                    fontFamily: "'IBM Plex Mono', monospace",
                    fontVariantNumeric: 'tabular-nums',
                  }}
                >
                  {u.events_count}
                </td>
                <td
                  className="text-right px-2 py-2 text-xs hidden lg:table-cell"
                  style={{ color: 'var(--text-muted)' }}
                >
                  {u.last_active_ts ? fmtRelative(u.last_active_ts) : '—'}
                </td>
                <td
                  className="px-2 py-2 text-xs hidden lg:table-cell"
                  style={{ color: 'var(--text-muted)' }}
                >
                  {u.created_at ? new Date(u.created_at).toLocaleDateString('ru-RU') : '—'}
                </td>
                <td className="px-2 py-2 text-right">
                  <Link
                    to={`/admin/users/${u.id}`}
                    onClick={(e) => e.stopPropagation()}
                    className="inline-flex items-center transition-opacity hover:opacity-70"
                    style={{ color: 'var(--text-muted)' }}
                  >
                    <ChevronRight size={16} />
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

function UCol({ children, align = 'left', hide }: {
  children: React.ReactNode; align?: 'left' | 'right'; hide?: 'md' | 'lg';
}) {
  const cls = `${align === 'right' ? 'text-right' : 'text-left'} px-2 py-2 text-xs uppercase ${
    hide === 'md' ? 'hidden md:table-cell' : hide === 'lg' ? 'hidden lg:table-cell' : ''
  }`;
  return (
    <th className={cls}
        style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}>
      {children}
    </th>
  );
}

function fmtRelative(iso: string): string {
  const now = Date.now();
  const t = new Date(iso).getTime();
  const sec = Math.max(0, Math.floor((now - t) / 1000));
  if (sec < 60) return `${sec}с назад`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} мин`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h}ч`;
  const d = Math.floor(h / 24);
  if (d < 30) return `${d}д`;
  return new Date(iso).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
}
