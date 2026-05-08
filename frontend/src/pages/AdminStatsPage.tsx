/**
 * AdminStatsPage — admin-only страница со статистикой использования сайта.
 *
 * Source endpoints (все role=admin):
 *   GET /api/analytics/stats        — summary + trends + top lists
 *   GET /api/analytics/funnel       — пошаговая конверсия
 *   GET /api/analytics/cohort       — D1/D7/D30 retention для auth users
 *   GET /api/analytics/realtime     — активность за последние 5 минут (auto-refresh)
 *   GET /api/analytics/experiments  — A/B testing list + create/delete
 *
 * Структура (вертикально):
 *  1. Realtime widget (auto-poll 15s) — что прямо сейчас на сайте
 *  2. 4 summary cards + trends line chart
 *  3. Top lists (pages/instruments/exports/modes)
 *  4. Funnel — конверсия по воронке landing → indicator → action
 *  5. Cohort retention — D1/D7/D30 heatmap-style table
 *  6. A/B experiments — list + create form
 */
import { useEffect, useState, useMemo, useCallback } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { BarChart3, TrendingUp, TrendingDown, Activity, Users, Clock, MousePointerClick, Zap, Trash2, Plus, Search, ChevronRight } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import { useAuth } from '../contexts/AuthContext';
import {
  getAnalyticsStats,
  getAnalyticsFunnel,
  getAnalyticsCohort,
  getAnalyticsRealtime,
  listABExperiments,
  createABExperiment,
  deleteABExperiment,
  listAdminUsers,
} from '../services/api';
import type {
  AnalyticsStats,
  FunnelResponse,
  CohortResponse,
  RealtimeResponse,
  ABExperiment,
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

  // Phase 6 sections data
  const [realtime, setRealtime] = useState<RealtimeResponse | null>(null);
  const [funnel, setFunnel] = useState<FunnelResponse | null>(null);
  const [cohort, setCohort] = useState<CohortResponse | null>(null);
  const [experiments, setExperiments] = useState<ABExperiment[]>([]);

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

  // Fetch cohort (фиксировано 8 недель — оптимально читается)
  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    getAnalyticsCohort(8).then(setCohort).catch(() => setCohort(null));
  }, [user]);

  // Realtime polling (15s)
  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    const tick = () => getAnalyticsRealtime(5).then(setRealtime).catch(() => null);
    tick();
    const id = window.setInterval(tick, 15_000);
    return () => clearInterval(id);
  }, [user]);

  // A/B experiments list
  const refreshExperiments = useCallback(async () => {
    try {
      const r = await listABExperiments();
      setExperiments(r.experiments);
    } catch {
      setExperiments([]);
    }
  }, []);
  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    refreshExperiments();
  }, [user, refreshExperiments]);

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

      {/* Realtime widget — auto-refresh каждые 15 секунд */}
      <RealtimeBlock data={realtime} />

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

      {/* Trends line chart */}
      <Section title="Динамика">
        {data && data.trends.length > 0 ? (
          <Card padding="md" className="md:p-5">
            <TrendsChart data={data.trends} />
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

      {/* Cohort retention */}
      <Section title="Retention (8 недель)">
        <Card padding="md" className="md:p-5">
          {cohort && cohort.cohorts.length > 0 ? (
            <CohortTable data={cohort} />
          ) : (
            <p className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
              Недостаточно зарегистрированных пользователей за 8 недель
            </p>
          )}
        </Card>
      </Section>

      {/* A/B experiments */}
      <Section title="A/B эксперименты">
        <ABBlock experiments={experiments} onRefresh={refreshExperiments} />
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

/** Простой dual-line chart: DAU + Sessions per day. SVG, без recharts. */
function TrendsChart({ data }: { data: { date: string; dau: number; sessions: number }[] }) {
  // Адаптивно под viewport (но мы рендерим в Card → почти full-width)
  const W = 1000;
  const H = 240;
  const padL = 40;
  const padR = 12;
  const padT = 16;
  const padB = 32;

  const maxDau = useMemo(() => Math.max(...data.map((d) => d.dau), 1), [data]);
  const maxSes = useMemo(() => Math.max(...data.map((d) => d.sessions), 1), [data]);
  const maxAll = Math.max(maxDau, maxSes);

  const x = (i: number) => padL + (i / Math.max(data.length - 1, 1)) * (W - padL - padR);
  const y = (v: number) => H - padB - (v / maxAll) * (H - padT - padB);

  const dauPath = data.map((d, i) => `${i === 0 ? 'M' : 'L'} ${x(i)} ${y(d.dau)}`).join(' ');
  const sesPath = data.map((d, i) => `${i === 0 ? 'M' : 'L'} ${x(i)} ${y(d.sessions)}`).join(' ');

  // 4 horizontal grid lines
  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => Math.round(maxAll * f));

  // X labels: 6 ticks max
  const labelStep = Math.max(1, Math.floor(data.length / 6));

  return (
    <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" width="100%" height="240">
      {/* Y grid */}
      {yTicks.map((v) => (
        <g key={v}>
          <line
            x1={padL} x2={W - padR}
            y1={y(v)} y2={y(v)}
            stroke="var(--chart-grid, currentColor)"
            strokeOpacity="0.18"
            strokeWidth="1"
          />
          <text
            x={padL - 6} y={y(v)}
            textAnchor="end"
            dominantBaseline="central"
            fill="var(--text-muted)"
            fontSize="11"
          >
            {v}
          </text>
        </g>
      ))}

      {/* Sessions (background line, thinner) */}
      <path
        d={sesPath}
        fill="none"
        stroke="var(--funds-flow-positive)"
        strokeWidth="2"
        strokeOpacity="0.6"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      {/* DAU (foreground, accent) */}
      <path
        d={dauPath}
        fill="none"
        stroke="var(--accent)"
        strokeWidth="2.5"
        strokeLinejoin="round"
        strokeLinecap="round"
      />

      {/* X labels */}
      {data.map((d, i) =>
        i % labelStep === 0 || i === data.length - 1 ? (
          <text
            key={d.date}
            x={x(i)}
            y={H - 8}
            textAnchor="middle"
            fill="var(--text-muted)"
            fontSize="11"
          >
            {formatShortDate(d.date)}
          </text>
        ) : null
      )}

      {/* Legend */}
      <g transform={`translate(${padL}, 8)`}>
        <circle cx="6" cy="8" r="4" fill="var(--accent)" />
        <text x="16" y="11" fill="var(--text-secondary)" fontSize="11">DAU</text>
        <circle cx="60" cy="8" r="4" fill="var(--funds-flow-positive)" />
        <text x="70" y="11" fill="var(--text-secondary)" fontSize="11">Сессии</text>
      </g>
    </svg>
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

function formatShortDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
}

// ════════════════════════════════════════════════════════════════════════════
// REALTIME BLOCK
// ════════════════════════════════════════════════════════════════════════════

function RealtimeBlock({ data }: { data: RealtimeResponse | null }) {
  return (
    <Section title="Сейчас на сайте">
      <Card padding="md" className="md:p-5">
        <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-3)' }}>
          {/* Pulse dot + active sessions count */}
          <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
            <span
              className="inline-block"
              style={{
                width: 8, height: 8, borderRadius: '50%',
                backgroundColor: 'var(--success)',
                animation: 'frame-pulse 2s ease-in-out infinite',
              }}
            />
            <span style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-xs)' }}>
              live · обновляется каждые 15с
            </span>
          </div>
          <span className="ml-auto text-sm font-bold" style={{
            color: 'var(--text-primary)',
            fontFamily: "'IBM Plex Mono', monospace",
          }}>
            {data ? data.active_sessions : '—'} активных сессий
          </span>
        </div>

        {/* Active pages */}
        {data && data.active_pages.length > 0 && (
          <div className="mb-4">
            <p className="text-xs uppercase mb-2" style={{
              color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600,
            }}>
              Активные страницы
            </p>
            <div className="flex flex-wrap" style={{ gap: 'var(--sp-2)' }}>
              {data.active_pages.map(p => (
                <div
                  key={p.path}
                  className="flex items-center"
                  style={{
                    backgroundColor: 'var(--bg-secondary)',
                    border: '1.5px solid var(--text-primary)',
                    borderRadius: 9999,
                    padding: '4px 12px',
                    gap: 'var(--sp-2)',
                    fontSize: 'var(--fs-xs)',
                  }}
                >
                  <span style={{ color: 'var(--text-primary)', fontFamily: "'IBM Plex Mono', monospace" }}>
                    {p.path}
                  </span>
                  <span style={{ color: 'var(--text-muted)' }}>· {p.sessions}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Recent events */}
        {data && data.recent_events.length > 0 && (
          <div>
            <p className="text-xs uppercase mb-2" style={{
              color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600,
            }}>
              Последние события
            </p>
            <div className="space-y-1.5 max-h-48 overflow-y-auto">
              {data.recent_events.map((ev, i) => (
                <div
                  key={`${ev.server_ts}-${i}`}
                  className="flex items-center justify-between py-1 px-2 -mx-2"
                  style={{ fontSize: 'var(--fs-xs)' }}
                >
                  <div className="flex items-center min-w-0 flex-1" style={{ gap: 'var(--sp-2)' }}>
                    <Zap size={10} style={{ color: 'var(--accent)', flexShrink: 0 }} />
                    <span style={{ color: 'var(--text-primary)', fontWeight: 600, flexShrink: 0 }}>
                      {ev.event_type}
                    </span>
                    {ev.event_path && (
                      <span style={{ color: 'var(--text-secondary)', fontFamily: "'IBM Plex Mono', monospace" }} className="truncate">
                        {ev.event_path}
                      </span>
                    )}
                    {ev.payload && (
                      <span style={{ color: 'var(--text-muted)' }} className="truncate">
                        {JSON.stringify(ev.payload).slice(0, 60)}
                      </span>
                    )}
                  </div>
                  <span style={{ color: 'var(--text-muted)', flexShrink: 0, marginLeft: 8 }}>
                    {formatTime(ev.server_ts)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {!data && (
          <p className="text-center py-4 text-sm" style={{ color: 'var(--text-muted)' }}>
            Загрузка realtime...
          </p>
        )}
      </Card>
    </Section>
  );
}

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
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
// COHORT TABLE — heatmap-style
// ════════════════════════════════════════════════════════════════════════════

function CohortTable({ data }: { data: CohortResponse }) {
  /** Цвет ячейки: интенсивность зависит от % retention. Зелёный градиент. */
  const cellColor = (pct: number) => {
    if (pct === 0) return 'var(--bg-secondary)';
    return `color-mix(in srgb, var(--funds-flow-positive) ${Math.min(pct, 80)}%, transparent)`;
  };

  return (
    <div className="overflow-x-auto -mx-2">
      <table className="w-full" style={{ minWidth: 480 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-color)' }}>
            <th className="text-left px-2 py-2 text-xs uppercase" style={{
              color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600,
            }}>
              Когорта
            </th>
            <th className="text-right px-2 py-2 text-xs uppercase" style={{
              color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600,
            }}>
              Размер
            </th>
            <th className="text-center px-2 py-2 text-xs uppercase" style={{
              color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600,
            }}>
              D1
            </th>
            <th className="text-center px-2 py-2 text-xs uppercase" style={{
              color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600,
            }}>
              D7
            </th>
            <th className="text-center px-2 py-2 text-xs uppercase" style={{
              color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600,
            }}>
              D30
            </th>
          </tr>
        </thead>
        <tbody>
          {data.cohorts.map((c) => (
            <tr key={c.cohort_week}
                style={{ borderBottom: '1px solid color-mix(in srgb, var(--border-color) 60%, transparent)' }}>
              <td className="px-2 py-2 text-sm" style={{ color: 'var(--text-primary)' }}>
                {formatShortDate(c.cohort_week)}
              </td>
              <td className="text-right px-2 py-2 text-sm" style={{
                color: 'var(--text-secondary)',
                fontFamily: "'IBM Plex Mono', monospace",
              }}>
                {c.cohort_size}
              </td>
              <CohortCell pct={c.d1_pct} count={c.d1} bg={cellColor(c.d1_pct)} />
              <CohortCell pct={c.d7_pct} count={c.d7} bg={cellColor(c.d7_pct)} />
              <CohortCell pct={c.d30_pct} count={c.d30} bg={cellColor(c.d30_pct)} />
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CohortCell({ pct, count, bg }: { pct: number; count: number; bg: string }) {
  return (
    <td className="text-center px-2 py-2 text-sm" style={{ backgroundColor: bg }}>
      <div style={{
        color: 'var(--text-primary)',
        fontFamily: "'IBM Plex Mono', monospace",
        fontVariantNumeric: 'tabular-nums',
      }}>
        {pct.toFixed(0)}%
      </div>
      <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
        {count}
      </div>
    </td>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// A/B EXPERIMENTS
// ════════════════════════════════════════════════════════════════════════════

interface ABBlockProps {
  experiments: ABExperiment[];
  onRefresh: () => Promise<void>;
}
function ABBlock({ experiments, onRefresh }: ABBlockProps) {
  const [showForm, setShowForm] = useState(false);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [variantA, setVariantA] = useState('control');
  const [variantB, setVariantB] = useState('treatment');
  const [trafficSplit, setTrafficSplit] = useState(50);
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  const submit = async () => {
    if (!name.trim() || !variantA.trim() || !variantB.trim()) {
      setFormError('Заполните name, variant_a и variant_b');
      return;
    }
    setSubmitting(true);
    setFormError(null);
    try {
      await createABExperiment({
        name: name.trim(),
        description: description.trim() || undefined,
        variant_a: variantA.trim(),
        variant_b: variantB.trim(),
        traffic_split: trafficSplit,
        active: true,
      });
      setName(''); setDescription('');
      setShowForm(false);
      await onRefresh();
    } catch (e) {
      setFormError(e instanceof Error ? e.message : 'Ошибка создания');
    } finally {
      setSubmitting(false);
    }
  };

  const remove = async (n: string) => {
    if (!window.confirm(`Удалить эксперимент «${n}» и все его assignments?`)) return;
    await deleteABExperiment(n);
    await onRefresh();
  };

  return (
    <Card padding="md" className="md:p-5">
      <div className="flex items-center mb-4">
        <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
          {experiments.length === 0
            ? 'Эксперименты не созданы. Создай свой первый A/B test.'
            : `Активных: ${experiments.filter(e => e.active).length} из ${experiments.length}`}
        </p>
        <button
          onClick={() => setShowForm(s => !s)}
          className="ml-auto editorial-press font-semibold rounded-full flex items-center"
          style={{
            backgroundColor: showForm ? 'var(--bg-secondary)' : 'var(--accent)',
            color: showForm ? 'var(--text-primary)' : '#fff',
            border: '1.5px solid var(--text-primary)',
            fontSize: 'var(--fs-sm)',
            padding: 'var(--sp-2) var(--sp-3)',
            gap: 'var(--sp-2)',
          }}
        >
          <Plus size={14} />
          {showForm ? 'Отмена' : 'Создать'}
        </button>
      </div>

      {/* Create form */}
      {showForm && (
        <div className="mb-4 p-4 rounded-lg" style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
        }}>
          <div className="grid grid-cols-1 md:grid-cols-2" style={{ gap: 'var(--sp-3)' }}>
            <ABInput label="Имя (slug)" value={name} onChange={setName} placeholder="homepage_v2" />
            <ABInput label="Описание" value={description} onChange={setDescription} placeholder="Тест нового layout главной" />
            <ABInput label="Variant A" value={variantA} onChange={setVariantA} placeholder="control" />
            <ABInput label="Variant B" value={variantB} onChange={setVariantB} placeholder="treatment" />
            <div>
              <label className="block text-xs mb-1" style={{ color: 'var(--text-muted)', fontWeight: 600 }}>
                Traffic split (% в variant A): {trafficSplit}%
              </label>
              <input
                type="range" min={0} max={100} step={5}
                value={trafficSplit}
                onChange={e => setTrafficSplit(Number(e.target.value))}
                className="w-full"
                style={{ accentColor: 'var(--accent)' }}
              />
            </div>
          </div>
          {formError && (
            <p className="mt-3 text-sm" style={{ color: 'var(--danger)' }}>{formError}</p>
          )}
          <button
            onClick={submit}
            disabled={submitting}
            className="mt-3 editorial-press font-semibold rounded-full"
            style={{
              backgroundColor: 'var(--accent)',
              color: '#fff',
              border: '1.5px solid var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-4)',
              opacity: submitting ? 0.6 : 1,
            }}
          >
            {submitting ? 'Создаём...' : 'Создать эксперимент'}
          </button>
        </div>
      )}

      {/* List */}
      {experiments.length > 0 && (
        <div className="space-y-2">
          {experiments.map(e => (
            <div
              key={e.name}
              className="flex items-center p-3 rounded-lg"
              style={{
                gap: 'var(--sp-3)',
                backgroundColor: 'var(--bg-secondary)',
                border: '1px solid var(--border-color)',
              }}
            >
              <div className="flex-1 min-w-0">
                <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
                  <span className="font-bold text-sm" style={{
                    color: 'var(--text-primary)',
                    fontFamily: "'IBM Plex Mono', monospace",
                  }}>
                    {e.name}
                  </span>
                  <span className="text-xs px-2 py-0.5 rounded-full" style={{
                    backgroundColor: e.active
                      ? 'color-mix(in srgb, var(--success) 18%, transparent)'
                      : 'var(--bg-secondary)',
                    color: e.active ? 'var(--success)' : 'var(--text-muted)',
                    border: '1px solid currentColor',
                  }}>
                    {e.active ? 'active' : 'paused'}
                  </span>
                </div>
                {e.description && (
                  <p className="text-xs mt-1 truncate" style={{ color: 'var(--text-secondary)' }}>
                    {e.description}
                  </p>
                )}
                <div className="flex items-center mt-1 text-xs" style={{
                  color: 'var(--text-muted)',
                  gap: 'var(--sp-3)',
                  fontFamily: "'IBM Plex Mono', monospace",
                }}>
                  <span>A:{e.variant_a} ({e.traffic_split}%)</span>
                  <span>B:{e.variant_b} ({100 - e.traffic_split}%)</span>
                  <span>· {e.total_assignments} assignments</span>
                </div>
              </div>
              <button
                onClick={() => remove(e.name)}
                className="grid place-items-center w-8 h-8 rounded-full transition-opacity hover:opacity-60"
                style={{ color: 'var(--danger)' }}
                title="Удалить"
              >
                <Trash2 size={14} />
              </button>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

function ABInput({ label, value, onChange, placeholder }: {
  label: string; value: string; onChange: (v: string) => void; placeholder?: string;
}) {
  return (
    <div>
      <label className="block text-xs mb-1" style={{ color: 'var(--text-muted)', fontWeight: 600 }}>
        {label}
      </label>
      <input
        type="text"
        value={value}
        onChange={e => onChange(e.target.value)}
        placeholder={placeholder}
        style={{
          width: '100%',
          backgroundColor: 'var(--bg-primary)',
          border: '1.5px solid var(--text-primary)',
          borderRadius: 8,
          padding: '6px 10px',
          color: 'var(--text-primary)',
          fontSize: 'var(--fs-sm)',
          outline: 'none',
        }}
      />
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
            type="text"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Поиск по email / имени"
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
                      {u.avatar_url ? (
                        <img src={u.avatar_url} alt="" style={{ width: '100%', height: '100%', objectFit: 'cover' }} />
                      ) : (
                        (u.email[0] || '?').toUpperCase()
                      )}
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
