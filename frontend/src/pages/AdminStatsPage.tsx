/**
 * AdminStatsPage — admin-only страница со статистикой использования сайта.
 *
 * Source: GET /api/analytics/stats?days=N&segment=X&device=Y
 * Только для role=admin (backend require_admin проверяет, frontend redirects).
 *
 * Структура:
 *  - 4 metric cards (DAU / Сессии / Среднее время / Events)
 *  - Filter row (period / segment / device)
 *  - Line chart trends (DAU + sessions × дни)
 *  - Top pages bar
 *  - Top instruments bar
 *  - Top exports bar
 *  - Mode distribution (seasonality)
 */
import { useEffect, useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { BarChart3, TrendingUp, TrendingDown, Activity, Users, Clock, MousePointerClick } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import { useAuth } from '../contexts/AuthContext';
import { getAnalyticsStats } from '../services/api';
import type { AnalyticsStats } from '../services/api';

export default function AdminStatsPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();

  const [days, setDays] = useState<number>(7);
  const [segment, setSegment] = useState<string>('all');
  const [device, setDevice] = useState<string>('all');
  const [data, setData] = useState<AnalyticsStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Guard: только admin
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') {
      navigate('/', { replace: true });
    }
  }, [authLoading, user, navigate]);

  // Fetch
  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    setLoading(true);
    setError(null);
    getAnalyticsStats({ days, segment, device })
      .then(setData)
      .catch((e: Error) => setError(e.message || 'Не удалось загрузить статистику'))
      .finally(() => setLoading(false));
  }, [user, days, segment, device]);

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
