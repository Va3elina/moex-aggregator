/**
 * AdminOverviewPage — пробный «Обзор» по правилам docs/admin-design.md.
 *
 * Route: /admin/overview (admin-only). Стоит РЯДОМ со старой /admin/stats,
 * ничего в ней не меняя: не понравится — удаляется одним файлом.
 *
 * Бюджет страницы: три контрола в шапке и ни одного ниже. Всё, что раньше
 * было рядами кнопок и чипов, здесь — клик по самим данным.
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import { useAuth } from '../contexts/AuthContext';
import { usePersistedState } from '../hooks/usePersistedState';
import { useDelayedFlag } from '../hooks/useDelayedFlag';
import { Metric, MetricRow, Panel, BarList, SplitBar, Empty, Failed } from '../components/admin/ui/Primitives';
import { PAGE_NAMES } from '../components/admin/ActivityBlocks';
import { getAnalyticsStats, getGrowth } from '../services/api';
import type { AnalyticsStats, GrowthReport } from '../services/api';

type Preset = '7d' | '30d' | '90d';
const PRESETS: { key: Preset; label: string }[] = [
  { key: '7d', label: '7 дней' },
  { key: '30d', label: '30 дней' },
  { key: '90d', label: '90 дней' },
];
const DAYS: Record<Preset, number> = { '7d': 7, '30d': 30, '90d': 90 };

export default function AdminOverviewPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [preset, setPreset] = usePersistedState<Preset>('frame:admin:ov:preset', '30d');
  const [segment, setSegment] = usePersistedState<string>('frame:admin:ov:segment', 'all');
  const [device, setDevice] = usePersistedState<string>('frame:admin:ov:device', 'all');

  const [stats, setStats] = useState<AnalyticsStats | null>(null);
  const [growth, setGrowth] = useState<GrowthReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);
  const dim = useDelayedFlag(loading && !!stats);

  const range = useMemo(() => ({ days: DAYS[preset] }), [preset]);

  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') navigate('/', { replace: true });
  }, [authLoading, user, navigate]);

  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    let alive = true;
    setLoading(true);
    setFailed(false);
    Promise.all([
      getAnalyticsStats({ ...range, segment, device }),
      getGrowth(range),
    ])
      .then(([s, g]) => { if (alive) { setStats(s); setGrowth(g); } })
      .catch(() => { if (alive) setFailed(true); })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [user, range, segment, device]);

  if (authLoading || !user || user.role !== 'admin') return null;

  const sum = stats?.summary;
  const g = growth?.guests;

  return (
    <div className="max-w-6xl mx-auto px-4 md:px-6 py-8 md:py-10">
      <div className="flex items-center justify-between gap-3 mb-1">
        <h1 className="text-lg font-semibold" style={{ color: 'var(--text-primary)' }}>Обзор</h1>
        <Link to="/admin/stats" className="text-xs flex items-center gap-1 hover:opacity-70"
              style={{ color: 'var(--text-secondary)' }}>
          <ArrowLeft size={13} /> Полная статистика
        </Link>
      </div>
      <p className="text-xs mb-6" style={{ color: 'var(--text-muted)' }}>
        Пробная страница по новым правилам. Старая статистика на месте.
      </p>

      {/* Бюджет контролов: три, и все здесь. Ниже по странице — ни одного. */}
      <div className="flex flex-wrap gap-2 mb-8">
        <Dropdown<string> options={PRESETS} value={preset} onChange={v => setPreset(v as Preset)} />
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все без админов' },
            { key: 'auth', label: 'Авторизованные' },
            { key: 'guest', label: 'Гости' },
          ]}
          value={segment} onChange={setSegment}
        />
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все устройства' },
            { key: 'desktop', label: 'Компьютер' },
            { key: 'mobile', label: 'Телефон' },
          ]}
          value={device} onChange={setDevice}
        />
      </div>

      {failed && <Failed />}

      {!stats && loading && (
        <div className="space-y-6">
          <Skeleton height={76} rounded="md" />
          <Skeleton height={210} rounded="md" />
          <Skeleton height={210} rounded="md" />
        </div>
      )}

      {stats && sum && (
        <div
          className="space-y-8"
          style={{ opacity: dim ? 0.5 : 1, transition: 'opacity 120ms cubic-bezier(.2,0,0,1)' }}
        >
          {/* Уровень 1 — ответ: ради этих цифр заходят */}
          <MetricRow>
            <Metric label="Людей" value={sum.visitors} hint={`${sum.visits.toLocaleString('ru-RU')} визитов`} active />
            <Metric label="Просмотров" value={sum.pageviews}
                    hint={`${(sum.pageviews / Math.max(sum.visits, 1)).toFixed(1)} за визит`} />
            <Metric label="Среднее время" value={fmtSec(sum.avg_visit_sec)} hint="от первого до последнего действия" />
            <Metric label="Возвращаются" value={sum.returning}
                    hint={sum.returning_pct !== null ? `${Math.round(sum.returning_pct)}% были в разные дни` : undefined} />
            {g && <Metric label="Ходят часто" value={g.days3} hint={`из ${g.total} гостей, 3+ дня`} />}
          </MetricRow>

          {/* Уровень 2 — поддержка */}
          <Panel title="Что смотрят" note="людей за период">
            <BarList
              items={(stats.top_pages || []).slice(0, 8).map(p => ({
                label: PAGE_NAMES[p.path] || p.path,
                note: PAGE_NAMES[p.path] ? p.path : undefined,
                value: p.visitors,
                extra: `${p.views.toLocaleString('ru-RU')} просм.`,
              }))}
            />
          </Panel>

          <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))' }}>
            <Panel title="Откуда приходят" note="визитов">
              <BarList items={(stats.sources || []).slice(0, 6).map(s => ({ label: s.source, value: s.visits }))} />
            </Panel>
            <Panel title="Устройства" note="людей">
              {stats.devices?.length ? (
                <SplitBar
                  parts={stats.devices.map((d, i) => ({
                    label: DEVICE_RU[d.device] || d.device,
                    value: d.visitors,
                    color: ['var(--accent)', 'var(--info)', 'var(--text-muted)'][i] || 'var(--text-muted)',
                  }))}
                />
              ) : <Empty text="За период устройств не видно" />}
            </Panel>
          </div>

          {stats.top_assets?.length ? (
            <Panel title="Популярные активы" note="людей открывали">
              <BarList
                items={stats.top_assets.slice(0, 8).map(a => ({
                  label: a.name || a.secid,
                  note: a.name ? a.secid : undefined,
                  value: a.visitors,
                }))}
              />
            </Panel>
          ) : null}
        </div>
      )}
    </div>
  );
}

const DEVICE_RU: Record<string, string> = {
  desktop: 'Компьютер', mobile: 'Телефон', tablet: 'Планшет', unknown: 'Не определено',
};

function fmtSec(sec: number): string {
  if (!sec) return '0с';
  const m = Math.floor(sec / 60);
  return m > 0 ? `${m}м ${sec % 60}с` : `${sec}с`;
}
