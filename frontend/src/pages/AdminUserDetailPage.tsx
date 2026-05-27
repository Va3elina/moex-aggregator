/**
 * AdminUserDetailPage — admin-only детальная страница одного пользователя.
 *
 * Source: GET /api/analytics/users/{id}?days=N
 * Route: /admin/users/:userId  (only role=admin)
 *
 * Содержит:
 *  - Profile card (avatar, email, role, plan, OAuth provider, last login)
 *  - Summary metrics (sessions / events / avg session / total time / first&last active)
 *  - Subscriptions history
 *  - Activity timeline (последние 100 events с timestamps)
 *  - Top pages / instruments / exports
 *  - Devices + countries breakdown
 */
import { useEffect, useState } from 'react';
import { useNavigate, useParams, Link } from 'react-router-dom';
import {
  ArrowLeft, User, Mail, ShieldCheck, Calendar, Clock, Globe, Monitor,
  Activity, Zap, AlertCircle,
} from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import AvatarImg from '../components/AvatarImg';
import Dropdown from '../components/Dropdown';
import { useAuth } from '../contexts/AuthContext';
import { getAdminUserDetail } from '../services/api';
import type { UserDetailResponse } from '../services/api';

export default function AdminUserDetailPage() {
  const { user, loading: authLoading } = useAuth();
  const { userId } = useParams<{ userId: string }>();
  const navigate = useNavigate();
  const id = userId ? parseInt(userId, 10) : NaN;

  const [days, setDays] = useState<number>(30);
  const [data, setData] = useState<UserDetailResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Guard
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') {
      navigate('/', { replace: true });
    }
  }, [authLoading, user, navigate]);

  useEffect(() => {
    if (!user || user.role !== 'admin' || !Number.isFinite(id)) return;
    setLoading(true);
    setError(null);
    getAdminUserDetail(id, days)
      .then(setData)
      .catch((e: Error) => setError(e.message || 'Не удалось загрузить'))
      .finally(() => setLoading(false));
  }, [user, id, days]);

  if (authLoading || !user || user.role !== 'admin') return null;

  if (!Number.isFinite(id)) {
    return <div className="max-w-3xl mx-auto p-8 text-center" style={{ color: 'var(--danger)' }}>Неверный user_id</div>;
  }

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-10">
      {/* Header back link */}
      <div className="flex items-center justify-between gap-3 mb-6">
        <Link
          to="/admin/stats"
          className="flex items-center gap-1 text-sm transition-opacity hover:opacity-80"
          style={{ color: 'var(--text-secondary)' }}
        >
          <ArrowLeft size={14} />
          Назад к статистике
        </Link>
        <Dropdown<string>
          options={[
            { key: '7', label: '7 дней' },
            { key: '30', label: '30 дней' },
            { key: '90', label: '90 дней' },
            { key: '180', label: '180 дней' },
          ]}
          value={String(days)}
          onChange={(v) => setDays(Number(v))}
        />
      </div>

      {error && (
        <Card padding="md" className="mb-6">
          <div className="flex items-center gap-2" style={{ color: 'var(--danger)' }}>
            <AlertCircle size={16} />
            <span>{error}</span>
          </div>
        </Card>
      )}

      {!data && loading && (
        <div className="space-y-4">
          <Skeleton height={120} rounded="lg" />
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            <Skeleton height={88} rounded="lg" />
            <Skeleton height={88} rounded="lg" />
            <Skeleton height={88} rounded="lg" />
            <Skeleton height={88} rounded="lg" />
          </div>
          <Skeleton height={400} rounded="lg" />
        </div>
      )}

      {data && (
        <>
          {/* Profile card */}
          <Card padding="md" className="md:p-6 mb-4">
            <div className="flex flex-col md:flex-row items-start gap-4">
              {/* Avatar */}
              <div
                className="flex items-center justify-center flex-shrink-0 rounded-full font-bold text-2xl"
                style={{
                  width: 72, height: 72,
                  backgroundColor: 'var(--accent)',
                  color: '#fff',
                  border: '1.5px solid var(--text-primary)',
                  overflow: 'hidden',
                }}
              >
                <AvatarImg url={data.user.avatar_url} fallback={(data.user.email[0] || '?').toUpperCase()} />
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center flex-wrap gap-2 mb-1">
                  <h1 className="text-xl md:text-2xl font-semibold" style={{ color: 'var(--text-primary)' }}>
                    {data.user.display_name || data.user.username || data.user.email.split('@')[0]}
                  </h1>
                  <span
                    className="px-2 py-0.5 rounded-full text-xs font-semibold"
                    style={{
                      backgroundColor:
                        data.user.role === 'admin' ? 'color-mix(in srgb, var(--danger) 18%, transparent)' :
                        data.user.role === 'pro' || data.user.role === 'premium' ? 'color-mix(in srgb, var(--warning) 18%, transparent)' :
                        'color-mix(in srgb, var(--accent) 18%, transparent)',
                      color:
                        data.user.role === 'admin' ? 'var(--danger)' :
                        data.user.role === 'pro' || data.user.role === 'premium' ? 'var(--warning)' :
                        'var(--accent)',
                    }}
                  >
                    {data.user.role}
                  </span>
                  {!data.user.is_active && (
                    <span className="px-2 py-0.5 rounded-full text-xs" style={{
                      backgroundColor: 'var(--bg-secondary)', color: 'var(--text-muted)'
                    }}>inactive</span>
                  )}
                  {data.user.is_verified && <ShieldCheck size={14} style={{ color: 'var(--success)' }} />}
                </div>

                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm" style={{ color: 'var(--text-secondary)' }}>
                  <span className="flex items-center gap-1.5"><Mail size={12} />{data.user.email}</span>
                  {data.user.oauth_provider && (
                    <span style={{ fontFamily: "'IBM Plex Mono', monospace" }}>OAuth: {data.user.oauth_provider}</span>
                  )}
                  {data.user.plan && (
                    <span className="font-semibold" style={{ color: 'var(--accent)' }}>plan: {data.user.plan}</span>
                  )}
                </div>

                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs mt-2" style={{ color: 'var(--text-muted)' }}>
                  {data.user.created_at && (
                    <span className="flex items-center gap-1"><Calendar size={11} />Создан: {fmtDate(data.user.created_at)}</span>
                  )}
                  {data.user.last_login_at && (
                    <span className="flex items-center gap-1"><Clock size={11} />Логин: {fmtDateTime(data.user.last_login_at)}</span>
                  )}
                  {data.user.last_login_ip && (
                    <span style={{ fontFamily: "'IBM Plex Mono', monospace" }}>IP: {data.user.last_login_ip}</span>
                  )}
                  <span className="flex items-center gap-1"><User size={11} />id={data.user.id}</span>
                </div>
              </div>
            </div>
          </Card>

          {/* Summary cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4 mb-6">
            <SmallStat label="Сессий" value={data.summary.sessions} />
            <SmallStat label="Events" value={data.summary.events} />
            <SmallStat label="Среднее время" value={data.summary.avg_session_sec} formatter={fmtDuration} />
            <SmallStat label="Всего времени" value={data.summary.total_time_sec} formatter={fmtDuration} />
          </div>

          {/* Subscriptions */}
          {data.subscriptions.length > 0 && (
            <SectionHeader title="Подписки" />
          )}
          {data.subscriptions.length > 0 && (
            <Card padding="md" className="mb-6">
              <div className="space-y-2">
                {data.subscriptions.map(s => (
                  <div key={s.id}
                       className="flex items-center justify-between p-2 rounded-lg"
                       style={{ backgroundColor: 'var(--bg-secondary)' }}>
                    <div>
                      <div className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>
                        {s.tier} · {s.period} · {s.amount} {s.currency}
                      </div>
                      <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
                        {s.started_at ? fmtDate(s.started_at) : 'не начата'}
                        {s.expires_at && ` → ${fmtDate(s.expires_at)}`}
                      </div>
                    </div>
                    <span className="text-xs px-2 py-1 rounded-full" style={{
                      backgroundColor: s.status === 'active' ? 'color-mix(in srgb, var(--success) 18%, transparent)' : 'var(--bg-primary)',
                      color: s.status === 'active' ? 'var(--success)' : 'var(--text-muted)',
                    }}>{s.status}</span>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {/* Top lists row */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-6">
            <SimpleTopList title="Топ страниц" items={data.top_pages.map(p => ({ label: p.path, value: p.views }))} />
            <SimpleTopList title="Топ тикеров" items={data.top_instruments.map(p => ({ label: p.secid, value: p.selects }))} />
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-6">
            <SimpleTopList title="Экспорты PNG" items={data.top_exports.map(p => ({ label: p.indicator, value: p.count }))} />
            <Card padding="md">
              <p className="text-xs uppercase mb-3" style={{
                color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600,
              }}>
                Устройства / страны
              </p>
              <div className="flex flex-wrap gap-2 mb-3">
                {data.devices.map(d => (
                  <Chip key={d.device} icon={<Monitor size={11} />} label={`${d.device}: ${d.sessions}`} />
                ))}
              </div>
              <div className="flex flex-wrap gap-2">
                {data.countries.map(c => (
                  <Chip key={c.country} icon={<Globe size={11} />} label={`${c.country}: ${c.sessions}`} />
                ))}
                {data.devices.length === 0 && data.countries.length === 0 && (
                  <p className="text-xs" style={{ color: 'var(--text-muted)' }}>Нет данных</p>
                )}
              </div>
            </Card>
          </div>

          {/* Activity timeline */}
          <SectionHeader title="Activity timeline" subtitle={`Последние ${data.timeline.length} событий`} />
          <Card padding="md">
            {data.timeline.length === 0 ? (
              <p className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
                Нет событий за выбранный период
              </p>
            ) : (
              <div className="space-y-1">
                {data.timeline.map((ev, i) => (
                  <div key={i}
                       className="flex items-center justify-between py-1.5 px-2 -mx-2 rounded"
                       style={{
                         fontSize: 'var(--fs-xs)',
                         backgroundColor: i % 2 === 0 ? 'transparent' : 'color-mix(in srgb, var(--bg-secondary) 50%, transparent)',
                       }}>
                    <div className="flex items-center min-w-0 flex-1" style={{ gap: 'var(--sp-2)' }}>
                      <Zap size={10} style={{ color: 'var(--accent)', flexShrink: 0 }} />
                      <span className="font-semibold flex-shrink-0" style={{ color: 'var(--text-primary)' }}>
                        {ev.event_type}
                      </span>
                      {ev.event_path && (
                        <span className="truncate" style={{
                          color: 'var(--text-secondary)',
                          fontFamily: "'IBM Plex Mono', monospace",
                        }}>
                          {ev.event_path}
                        </span>
                      )}
                      {ev.payload && (
                        <span className="truncate" style={{ color: 'var(--text-muted)' }}>
                          {JSON.stringify(ev.payload).slice(0, 80)}
                        </span>
                      )}
                    </div>
                    <div className="flex items-center flex-shrink-0 ml-3" style={{ gap: 'var(--sp-2)', color: 'var(--text-muted)' }}>
                      {ev.device && <span>{ev.device}</span>}
                      <span>{fmtDateTime(ev.server_ts)}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// SUBCOMPONENTS
// ═══════════════════════════════════════════════════════════════════════

function SectionHeader({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div className="flex items-center gap-3 mb-3 mt-6">
      <p className="text-xs uppercase" style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}>
        {title}
      </p>
      <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
      {subtitle && <span className="text-xs" style={{ color: 'var(--text-muted)' }}>{subtitle}</span>}
    </div>
  );
}

function SmallStat({
  label, value, formatter = (v) => v.toLocaleString('ru-RU'),
}: {
  label: string; value: number; formatter?: (v: number) => string;
}) {
  return (
    <Card padding="md">
      <div className="flex items-center gap-2 mb-1" style={{ color: 'var(--text-muted)' }}>
        <Activity size={12} />
        <span className="text-xs uppercase" style={{ letterSpacing: '0.1em', fontWeight: 600 }}>{label}</span>
      </div>
      <div className="font-bold" style={{
        color: 'var(--text-primary)',
        fontSize: 'clamp(1.2rem, 2vw, 1.6rem)',
        fontFamily: "'IBM Plex Mono', monospace",
        fontVariantNumeric: 'tabular-nums',
      }}>
        {formatter(value)}
      </div>
    </Card>
  );
}

function SimpleTopList({ title, items }: { title: string; items: { label: string; value: number }[] }) {
  const max = items.length > 0 ? items[0].value : 1;
  return (
    <Card padding="md">
      <p className="text-xs uppercase mb-3" style={{
        color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600,
      }}>{title}</p>
      {items.length === 0 ? (
        <p className="text-center py-4 text-sm" style={{ color: 'var(--text-muted)' }}>—</p>
      ) : (
        <div className="space-y-1">
          {items.map((it, i) => (
            <div key={`${it.label}-${i}`} className="relative">
              <div className="absolute inset-y-0 left-0 rounded" style={{
                width: `${(it.value / max) * 100}%`,
                backgroundColor: 'color-mix(in srgb, var(--accent) 14%, transparent)',
              }} />
              <div className="relative flex items-center justify-between py-1.5 px-2">
                <span className="text-sm truncate" style={{ color: 'var(--text-primary)' }} title={it.label}>
                  {it.label || '—'}
                </span>
                <span className="text-sm font-semibold flex-shrink-0 ml-2" style={{
                  color: 'var(--text-primary)',
                  fontFamily: "'IBM Plex Mono', monospace",
                }}>
                  {it.value}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

function Chip({ icon, label }: { icon?: React.ReactNode; label: string }) {
  return (
    <span className="inline-flex items-center text-xs px-2 py-1 rounded-full" style={{
      backgroundColor: 'var(--bg-secondary)',
      color: 'var(--text-secondary)',
      border: '1px solid var(--border-color)',
      gap: 4,
    }}>
      {icon}
      {label}
    </span>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// FORMATTERS
// ═══════════════════════════════════════════════════════════════════════

function fmtDate(iso: string): string {
  return new Date(iso).toLocaleDateString('ru-RU');
}
function fmtDateTime(iso: string): string {
  return new Date(iso).toLocaleString('ru-RU', {
    day: '2-digit', month: '2-digit', year: '2-digit',
    hour: '2-digit', minute: '2-digit',
  });
}
function fmtDuration(sec: number): string {
  if (!sec || sec < 0) return '0с';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}ч ${m}м`;
  if (m > 0) return `${m}м ${s}с`;
  return `${s}с`;
}
