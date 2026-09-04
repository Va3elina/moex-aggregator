/**
 * AdminStatsPage — admin-only страница со статистикой использования сайта.
 *
 * Source endpoints (все role=admin):
 *   GET /api/analytics/stats        — summary + trends (line chart) + top lists
 *   GET /api/analytics/alerts-stats — трекинг алертов
 *   GET /api/analytics/users        — таблица пользователей (+фильтр платных)
 *
 * Структура (вертикально):
 *  1. 4 summary cards (Уникальные / Сессии / AvgTime / Events) + trends chart
 *  2. Top lists (pages/instruments/exports/modes)
 *  3. Alerts — жизненный цикл + текущее состояние
 *  4. Users — фильтр платные/бесплатные/админы, drill-down в /admin/users/:id
 *
 * У каждой метрики — HelpTooltip (icon="info") с честным описанием того,
 * как она считается. Тексты — в METRIC_HINTS, менять там же.
 *
 * При переключении периода/сегмента старые данные НЕ сбрасываются в скелетоны:
 * контент приглушается (opacity) до прихода свежих — stale-while-revalidate.
 * Бэкенд дополнительно кэширует /stats в Redis (TTL 3 мин).
 *
 * Раньше были блоки Realtime/Retention/A/B (удалены 2026-05-13) и Воронка
 * конверсии (удалена 2026-07-16 вместе с эндпоинтом — пользы не давала,
 * а стоила до 5 тяжёлых SQL-запросов на каждое переключение периода).
 */
import { useEffect, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { BarChart3, TrendingUp, TrendingDown, Activity, Users, Clock, MousePointerClick, Search, ChevronRight, AlarmClock, AlarmClockOff, Pause, Play, Zap, Loader2, Gift } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SimpleChart from '../components/SimpleChart';
import AvatarImg from '../components/AvatarImg';
import HelpTooltip from '../components/HelpTooltip';
import { useAuth } from '../contexts/AuthContext';
import {
  getAnalyticsStats,
  listAdminUsers,
  getAlertsStats,
} from '../services/api';
import type {
  AnalyticsStats,
  AdminUser,
  AlertsStats,
} from '../services/api';

// ════════════════════════════════════════════════════════════════════════════
// ПОДСКАЗКИ МЕТРИК — короткие честные описания «как это посчитано».
// Показываются по hover (desktop) / tap (mobile) на иконке «i» у метрики.
// ════════════════════════════════════════════════════════════════════════════

const METRIC_HINTS = {
  uniques:
    'Уникальные посетители за период. Авторизованный считается по аккаунту (одинаково на всех устройствах), гость — по вкладке браузера: ID живёт, пока открыта вкладка. Один человек может задвоиться: гостевая сессия до входа + аккаунт после. Дельта — сравнение с предыдущим таким же периодом.',
  sessions:
    'Сессия — открытая вкладка сайта: ID создаётся при заходе и умирает при закрытии вкладки (обновление страницы — та же сессия, новая вкладка — новая). Считаются сессии, у которых за период было хотя бы одно событие.',
  avg_time:
    'Средняя длительность сессии: сумма пауз между её событиями, пауза длиннее 5 минут учитывается как 5 минут (стандарт веб-аналитики). Пока вкладка на экране, раз в 60 сек уходит heartbeat. Сессии с одним событием считаются как 0 сек и тоже входят в среднее.',
  events:
    'Все зафиксированные события за период: просмотры страниц, выборы тикера, экспорты графиков, переключения темы, heartbeat-пульс и события оплаты.',
  trends:
    'Уникальные посетители и сессии по дням. Сумма дневных уникальных больше цифры «за период» — это нормально: один посетитель активен в несколько дней. Дни без событий показаны нулями.',
  top_pages:
    'Число просмотров (pageview) за период — каждый переход по страницам сайта, не уникальные посетители.',
  top_instruments:
    'Сколько раз тикер выбирали в поиске инструмента за период.',
  top_exports:
    'Скачивания графиков в PNG за период, по индикаторам.',
  modes:
    'Переключения режима отображения на странице «Сезонность» за период.',
  alerts_section:
    '«Поставили / Убрали / Пауза / Возобновили» — события за выбранный период. «Активных сейчас» и «Хоть раз сработали» — текущее состояние, от периода не зависят.',
  alerts_active:
    'Число уведомлений со статусом «активен» прямо сейчас — снимок текущего состояния, не за период.',
  alerts_fired:
    'Сколько из существующих уведомлений хоть раз срабатывали за всю историю. Процент — доля от активных сейчас (может быть >100%, если сработавшие уведомления стоят на паузе).',
  users_section:
    'Только зарегистрированные аккаунты. Сессии и события — за выбранный период и только у согласившихся на cookie: у отказавших там нули при реальных визитах. «Последняя активность» — за всё время и видна у всех: события + входы + обновление сессии. «Платные» — активная подписка прямо сейчас.',
} as const;

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

  // Fetch summary stats. Старые данные НЕ сбрасываем (stale-while-revalidate):
  // при переключении периода контент приглушается, а не мигает скелетонами.
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

  const refreshing = loading && data !== null;

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
        {/* Вход в карту состояния проекта — отдельной кнопкой, видной только админу. */}
        <Link
          to="/admin/dashboard"
          className="editorial-press rounded-full flex items-center ml-auto shrink-0"
          style={{ padding: 'var(--sp-1) var(--sp-3)', gap: '6px', fontSize: 'var(--fs-xs)' }}
        >
          <Activity size={14} /> Состояние проекта
        </Link>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center mb-6 md:mb-8" style={{ gap: 'var(--sp-2)' }}>
        <Dropdown<string>
          options={[
            { key: '1', label: '24 часа' },
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
        {refreshing && (
          <span className="inline-flex items-center gap-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>
            <Loader2 size={13} className="animate-spin" />
            обновление…
          </span>
        )}
      </div>

      {error && (
        <Card padding="md" className="mb-6">
          <p style={{ color: 'var(--danger)' }}>Ошибка: {error}</p>
        </Card>
      )}

      {/* Основной контент по /stats — приглушается на время refetch'а */}
      <div style={{ opacity: refreshing ? 0.55 : 1, transition: 'opacity 0.2s ease' }}>
        {/* Summary cards */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4 mb-6 md:mb-8">
          {data ? (
            <>
              <SummaryCard
                icon={<Users size={16} />}
                label="Уникальные за период"
                hint={METRIC_HINTS.uniques}
                value={data.summary.uniques}
                delta={data.summary.delta_uniques}
                deltaSuffix=" vs пред."
              />
              <SummaryCard
                icon={<Activity size={16} />}
                label="Сессий"
                hint={METRIC_HINTS.sessions}
                hintAlign="right"
                value={data.summary.sessions}
                deltaPct={data.summary.delta_sessions_pct}
              />
              <SummaryCard
                icon={<Clock size={16} />}
                label="Среднее время"
                hint={METRIC_HINTS.avg_time}
                value={data.summary.avg_session_sec}
                format={(v) => formatDuration(v)}
                delta={data.summary.delta_avg_session_sec}
                deltaSuffix=" сек"
              />
              <SummaryCard
                icon={<MousePointerClick size={16} />}
                label="Events"
                hint={METRIC_HINTS.events}
                hintAlign="right"
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

        {/* Trends — SimpleChart с двумя линиями: уникальные (accent) + сессии */}
        <Section title="Динамика" hint={METRIC_HINTS.trends}>
          {data && data.trends.length > 0 ? (
            <Card padding="md" className="md:p-5">
              <SimpleChart
                data={data.trends.map(t => ({ time: t.date, value: t.uniques }))}
                secondaryData={data.trends.map(t => ({ time: t.date, value: t.sessions }))}
                showSecondary={true}
                primaryColor="var(--accent)"
                secondaryColor="var(--accent-secondary)"
                primaryLabel="Уникальные/день"
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
            hint={METRIC_HINTS.top_pages}
            items={data?.top_pages.map((p) => ({ label: p.path, value: p.views })) || null}
            loading={loading}
            emptyText="Нет pageview-событий"
          />
          <TopList
            title="Топ тикеров"
            hint={METRIC_HINTS.top_instruments}
            items={data?.top_instruments.map((p) => ({ label: p.secid, value: p.selects })) || null}
            loading={loading}
            emptyText="Нет выборов тикера"
          />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-6 md:mb-8">
          <TopList
            title="Экспорты PNG"
            hint={METRIC_HINTS.top_exports}
            items={data?.top_exports.map((p) => ({ label: p.indicator, value: p.count })) || null}
            loading={loading}
            emptyText="Никто не экспортировал"
          />
          <TopList
            title="Сезонность: режимы"
            hint={METRIC_HINTS.modes}
            items={data?.mode_distribution.map((p) => ({ label: p.mode, value: p.count })) || null}
            loading={loading}
            emptyText="Нет переключений режима"
          />
        </div>
      </div>

      {/* Алерты — трекинг что люди ставят/убирают + конверсия (сколько хоть раз сработало) */}
      <Section title="Уведомления" hint={METRIC_HINTS.alerts_section}>
        <AlertsBlock days={days} />
      </Section>

      {/* Users — фильтр платных + кликабельная таблица для drill-down на /admin/users/:id */}
      <Section title="Пользователи" hint={METRIC_HINTS.users_section}>
        <UsersBlock days={days} />
      </Section>

    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SUBCOMPONENTS
// ════════════════════════════════════════════════════════════════════════════

function Section({ title, hint, children }: { title: string; hint?: string; children: React.ReactNode }) {
  return (
    <section className="mb-6 md:mb-8">
      <div className="flex items-center gap-3 mb-4">
        <p
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
        >
          {title}
        </p>
        {hint && <HelpTooltip icon="info" title={title} content={hint} size={13} />}
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
  /** Текст подсказки «как считается» — рендерит иконку «i» после label. */
  hint?: string;
  /** Куда раскрывать поповер подсказки (right — для карточек у правого края). */
  hintAlign?: 'left' | 'right';
}
function SummaryCard({ icon, label, value, delta, deltaPct, deltaSuffix = '', format, hint, hintAlign = 'left' }: SummaryCardProps) {
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
        <span className="text-xs uppercase min-w-0 truncate" style={{ letterSpacing: '0.1em', fontWeight: 600 }}>
          {label}
        </span>
        {hint && <HelpTooltip icon="info" title={label} content={hint} size={13} align={hintAlign} />}
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
  hint?: string;
}
function TopList({ title, items, loading, emptyText, hint }: TopListProps) {
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
        {hint && <HelpTooltip icon="info" title={title} content={hint} size={13} />}
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
// USERS BLOCK — фильтр платных + drill-down таблица пользователей
// ════════════════════════════════════════════════════════════════════════════

const PLAN_COLORS: Record<string, string> = {
  basic: 'var(--accent)',
  pro: 'var(--warning)',
  premium: 'var(--success)',
};

/**
 * Бейдж подписки: тир цветом + дата окончания. Free — приглушённый текст.
 * Подписка по пригласительной ссылке помечается отдельно — иначе в таблице она
 * неотличима от купленной, и «платных» читается больше, чем есть на самом деле.
 */
function PlanBadge({ plan, expiresAt, isInvite }: { plan: string | null; expiresAt?: string | null; isInvite?: boolean }) {
  if (!plan) {
    return <span className="text-xs" style={{ color: 'var(--text-muted)' }}>free</span>;
  }
  const color = PLAN_COLORS[plan] || 'var(--accent)';
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
      <span
        className="text-xs px-2 py-0.5 rounded-full font-semibold"
        style={{
          backgroundColor: `color-mix(in srgb, ${color} 18%, transparent)`,
          color,
        }}
      >
        {plan}
      </span>
      {isInvite && (
        <span
          className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full font-semibold"
          style={{
            backgroundColor: 'color-mix(in srgb, var(--text-muted) 15%, transparent)',
            color: 'var(--text-secondary)',
          }}
          title="Подписка выдана по пригласительной ссылке, не оплачена"
        >
          <Gift size={11} />
          инвайт
        </span>
      )}
      {expiresAt && (
        <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
          до {new Date(expiresAt).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })}
        </span>
      )}
    </span>
  );
}

function UsersBlock({ days }: { days: number }) {
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [totalCount, setTotalCount] = useState<number | null>(null);
  const [paidCount, setPaidCount] = useState<number | null>(null);
  const [inviteCount, setInviteCount] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState('last_active');
  const [filter, setFilter] = useState('all');

  // Debounced search — fetch'аем не на каждое нажатие
  useEffect(() => {
    const t = window.setTimeout(() => {
      setLoading(true);
      listAdminUsers({ days, sort, search: search.trim(), filter })
        .then(r => {
          setUsers(r.users);
          setTotalCount(r.total_count ?? null);
          setPaidCount(r.paid_count ?? null);
          setInviteCount(r.invite_count ?? null);
        })
        .catch(() => setUsers([]))
        .finally(() => setLoading(false));
    }, 250);
    return () => clearTimeout(t);
  }, [days, sort, search, filter]);

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
            { key: 'all', label: 'Все' },
            { key: 'paid', label: 'Платные' },
            { key: 'invite', label: 'По инвайту' },
            { key: 'free', label: 'Бесплатные' },
            { key: 'admin', label: 'Админы' },
          ]}
          value={filter}
          onChange={setFilter}
        />
        <Dropdown<string>
          options={[
            { key: 'last_active', label: 'По активности' },
            { key: 'plan', label: 'Сначала платные' },
            { key: 'events', label: 'По событиям' },
            { key: 'sessions', label: 'По сессиям' },
            { key: 'created', label: 'По регистрации' },
          ]}
          value={sort}
          onChange={setSort}
        />
        <span className="text-xs ml-auto whitespace-nowrap" style={{ color: 'var(--text-muted)' }}>
          показано {users.length}
          {totalCount !== null && ` из ${totalCount}`}
          {paidCount !== null && (
            <>
              {' · '}
              <span style={{ color: 'var(--success)', fontWeight: 600 }}>платных: {paidCount}</span>
            </>
          )}
          {inviteCount !== null && inviteCount > 0 && (
            <>
              {' · '}
              <span style={{ fontWeight: 600 }}>по инвайту: {inviteCount}</span>
            </>
          )}
        </span>
      </div>

      {/* Table */}
      <div className="overflow-x-auto -mx-2" style={{ opacity: loading && users.length > 0 ? 0.55 : 1, transition: 'opacity 0.2s ease' }}>
        <table className="w-full" style={{ minWidth: 760 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-color)' }}>
              <UCol>Пользователь</UCol>
              <UCol align="left">Подписка</UCol>
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
                <td colSpan={8} className="py-6">
                  <Skeleton height={24} rounded="md" />
                </td>
              </tr>
            )}
            {!loading && users.length === 0 && (
              <tr>
                <td colSpan={8} className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
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
                      </div>
                    </div>
                  </div>
                </td>

                <td className="px-2 py-2">
                  <PlanBadge plan={u.plan} expiresAt={u.plan_expires_at} isInvite={u.is_invite} />
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

// ════════════════════════════════════════════════════════════════════════════
// ALERTS BLOCK — трекинг алертов (поставили/убрали/пауза/возобновили + конверсия)
// ════════════════════════════════════════════════════════════════════════════
//
// days приходит из общего селектора периода страницы. Бэкенд: created/deleted/
// paused/resumed считаются ЗА период; active_now / with_fires / by_source —
// снимок «сейчас» (текущее состояние таблицы alerts/alert_fires).

function AlertsBlock({ days }: { days: number }) {
  const [stats, setStats] = useState<AlertsStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    getAlertsStats(days)
      .then(setStats)
      .catch(() => setStats(null))
      .finally(() => setLoading(false));
  }, [days]);

  if (loading && !stats) {
    return (
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
        <Skeleton height={108} rounded="lg" />
        <Skeleton height={108} rounded="lg" />
        <Skeleton height={108} rounded="lg" />
        <Skeleton height={108} rounded="lg" />
      </div>
    );
  }

  if (!stats) {
    return (
      <Card padding="md">
        <p className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
          Нет данных по уведомлениям
        </p>
      </Card>
    );
  }

  // Конверсия: какая доля активных алертов хоть раз сработала.
  const conversionPct = stats.active_now > 0
    ? Math.round((stats.with_fires / stats.active_now) * 100)
    : null;

  return (
    <div className="space-y-3 md:space-y-4" style={{ opacity: loading ? 0.55 : 1, transition: 'opacity 0.2s ease' }}>
      {/* Lifecycle-события за период */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
        <SummaryCard icon={<AlarmClock size={16} />} label="Поставили" value={stats.created} />
        <SummaryCard icon={<AlarmClockOff size={16} />} label="Убрали" value={stats.deleted} />
        <SummaryCard icon={<Pause size={16} />} label="На паузу" value={stats.paused} />
        <SummaryCard icon={<Play size={16} />} label="Возобновили" value={stats.resumed} />
      </div>

      {/* Текущее состояние + конверсия (снимок «сейчас») */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3 md:gap-4">
        <SummaryCard
          icon={<Activity size={16} />}
          label="Активных сейчас"
          hint={METRIC_HINTS.alerts_active}
          value={stats.active_now}
        />
        <SummaryCard
          icon={<Zap size={16} />}
          label="Хоть раз сработали"
          hint={METRIC_HINTS.alerts_fired}
          value={stats.with_fires}
          delta={conversionPct}
          deltaSuffix="% от активных"
        />
        <Card padding="md" className="md:p-5">
          <div className="flex items-center gap-2 mb-3" style={{ color: 'var(--text-muted)' }}>
            <BarChart3 size={16} />
            <span className="text-xs uppercase" style={{ letterSpacing: '0.1em', fontWeight: 600 }}>
              По источнику
            </span>
          </div>
          {stats.by_source.length === 0 ? (
            <p className="text-sm" style={{ color: 'var(--text-muted)' }}>—</p>
          ) : (
            <div className="space-y-1.5">
              {stats.by_source.map((s) => (
                <div key={s.source} className="flex items-center justify-between">
                  <span className="text-sm" style={{ color: 'var(--text-primary)' }}>
                    {s.source}
                  </span>
                  <span
                    className="text-sm font-semibold"
                    style={{
                      color: 'var(--text-primary)',
                      fontFamily: "'IBM Plex Mono', monospace",
                      fontVariantNumeric: 'tabular-nums',
                    }}
                  >
                    {s.active.toLocaleString('ru-RU')}
                  </span>
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>

      {/* Топ активов по числу активных алертов */}
      {stats.top_assets && stats.top_assets.length > 0 && (
        <TopList
          title="Топ активов (активные уведомления)"
          items={stats.top_assets.map((a) => ({ label: a.asset, value: a.count }))}
          loading={false}
          emptyText="Нет активных уведомлений"
        />
      )}
    </div>
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
