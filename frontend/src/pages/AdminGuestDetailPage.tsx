/**
 * AdminGuestDetailPage — карточка одного гостя: человека, который ходит на
 * сайт и не завёл аккаунт.
 *
 * Source: GET /api/analytics/guests/{visitor_id}?days=N
 * Route:  /admin/guests/:visitorId  (only role=admin)
 *
 * Профиля у гостя нет — есть только поведение, поэтому вся страница состоит
 * из общих блоков активности (ActivityBlocks), тех же, что в карточке
 * пользователя: цифры должны сходиться между двумя карточками.
 */
import { useEffect, useState } from 'react';
import { useNavigate, useParams, Link } from 'react-router-dom';
import { ArrowLeft, Calendar, Clock, AlertCircle, Fingerprint, Globe, Monitor } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import { useAuth } from '../contexts/AuthContext';
import { getAdminGuestDetail } from '../services/api';
import type { GuestDetailResponse } from '../services/api';
import {
  ActivityBlocks, SmallStat, Chip, DETAIL_HINTS, PAGE_NAMES, DEVICE_NAMES,
  fmtDateTime, fmtDuration,
} from '../components/admin/ActivityBlocks';

export default function AdminGuestDetailPage() {
  const { user, loading: authLoading } = useAuth();
  const { visitorId } = useParams<{ visitorId: string }>();
  const navigate = useNavigate();

  const [days, setDays] = useState<number>(30);
  const [data, setData] = useState<GuestDetailResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') navigate('/', { replace: true });
  }, [authLoading, user, navigate]);

  useEffect(() => {
    if (!user || user.role !== 'admin' || !visitorId) return;
    setLoading(true);
    setError(null);
    getAdminGuestDetail(visitorId, days)
      .then(setData)
      .catch((e: Error) => setError(e.message || 'Не удалось загрузить'))
      .finally(() => setLoading(false));
  }, [user, visitorId, days]);

  if (authLoading || !user || user.role !== 'admin') return null;

  const g = data?.guest;

  return (
    <div className="max-w-7xl mx-auto px-4 md:px-6 py-8 md:py-10">
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
          <Skeleton height={110} rounded="lg" />
          <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
            {[0, 1, 2, 3, 4].map(i => <Skeleton key={i} height={88} rounded="lg" />)}
          </div>
          <Skeleton height={400} rounded="lg" />
        </div>
      )}

      {data && g && (
        <>
          <Card padding="md" className="md:p-6 mb-4">
            <div className="flex flex-col md:flex-row items-start gap-4">
              <div
                className="flex items-center justify-center flex-shrink-0 rounded-full"
                style={{
                  width: 72, height: 72,
                  backgroundColor: 'var(--bg-secondary)',
                  color: 'var(--text-muted)',
                  border: '1.5px solid var(--text-primary)',
                }}
              >
                <Fingerprint size={30} />
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center flex-wrap gap-2 mb-1">
                  <h1
                    className="text-xl md:text-2xl font-semibold"
                    style={{ color: 'var(--text-primary)', fontFamily: "'IBM Plex Mono', monospace" }}
                  >
                    {g.visitor_id.slice(0, 8)}
                  </h1>
                  <span className="px-2 py-0.5 rounded-full text-xs font-semibold" style={{
                    backgroundColor: 'color-mix(in srgb, var(--text-muted) 18%, transparent)',
                    color: 'var(--text-secondary)',
                  }}>
                    без аккаунта
                  </span>
                </div>

                {/* Имени и почты у гостя нет — вместо профиля показываем то,
                    чем его вообще можно опознать. */}
                <div className="flex flex-wrap items-center gap-2 mt-2">
                  {g.device && <Chip icon={<Monitor size={11} />} label={DEVICE_NAMES[g.device] || g.device} />}
                  {g.country && <Chip icon={<Globe size={11} />} label={g.country} />}
                  <Chip label={g.source ? `Источник: ${g.source}` : 'Источник: прямой заход'} />
                  {g.utm_source && <Chip label={`utm: ${g.utm_source}${g.utm_campaign ? ` / ${g.utm_campaign}` : ''}`} />}
                </div>

                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs mt-3" style={{ color: 'var(--text-muted)' }}>
                  {g.first_seen_at && (
                    <span className="flex items-center gap-1"><Calendar size={11} />Впервые: {fmtDateTime(g.first_seen_at)}</span>
                  )}
                  {g.last_seen_at && (
                    <span className="flex items-center gap-1"><Clock size={11} />Последний раз: {fmtDateTime(g.last_seen_at)}</span>
                  )}
                  <span style={{ fontFamily: "'IBM Plex Mono', monospace" }} title="ID браузера целиком">
                    {g.visitor_id}
                  </span>
                </div>
              </div>
            </div>
          </Card>

          <div className="grid grid-cols-2 lg:grid-cols-5 gap-3 md:gap-4 mb-6">
            <SmallStat label="Дней на сайте" hint={DETAIL_HINTS.days} value={data.summary.active_days} />
            <SmallStat label="Визитов" hint={DETAIL_HINTS.visits} value={data.summary.sessions} />
            <SmallStat label="Действий" hint={DETAIL_HINTS.actions} value={data.summary.events} />
            <SmallStat label="Среднее время" hint={DETAIL_HINTS.avg} value={data.summary.avg_session_sec} formatter={fmtDuration} />
            <SmallStat label="Всего времени" hint={DETAIL_HINTS.total} value={data.summary.total_time_sec} formatter={fmtDuration} />
          </div>

          {/* ID браузера живёт в localStorage: чистка данных сайта или другое
              устройство дадут нового «гостя». Без этой оговорки карточку легко
              принять за полную историю человека. */}
          <Card padding="md" className="mb-6">
            <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
              Это один браузер, а не обязательно один человек: ID хранится в браузере и живёт год.
              Если человек почистил данные сайта или пришёл с другого устройства, он появится здесь
              как новый гость. А если он когда-нибудь войдёт в аккаунт, вся эта история приклеится
              к его карточке пользователя.
            </p>
          </Card>

          <ActivityBlocks data={data} pageNames={PAGE_NAMES} />
        </>
      )}
    </div>
  );
}
