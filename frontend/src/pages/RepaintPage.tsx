/**
 * RepaintPage — экспериментальный admin-only индикатор «Перекраска» (/admin/repaint).
 *
 * Идея (Вадим, 2026-08-30): сколько % от free float бумаги сменило руки за
 * последний месяц. CDV (кумулятивный дельта-объём) аппроксимируется по свечам
 * (в БД нет биржевого разреза buy/sell), 4Ч-бакеты из часовиков; изменение CDV
 * за месяц делится на количество акций в свободном обращении. Вторая метрика —
 * отклонение CDV от его среднего за месяц (насколько напокупали/напродавали
 * относительно накопленной базы, спекулятивный спрос).
 *
 * Source endpoints (оба role=admin, api/routers/repaint.py):
 *   GET /api/admin/repaint/screener       — метрики по всем акциям (таблица)
 *   GET /api/admin/repaint/series/{secid} — 4Ч-ряд: цена + CDV + метрики
 *
 * Пока индикатор экспериментальный: ссылка на него — только в admin-блоке
 * на главной (OverviewPage), в общий nav не выводится.
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Loader2, Repeat2, Search } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SimpleChart from '../components/SimpleChart';
import HelpTooltip from '../components/HelpTooltip';
import { useAuth } from '../contexts/AuthContext';
import { getRepaintScreener, getRepaintSeries } from '../services/api';
import type { RepaintScreenerRow, RepaintSeries } from '../services/api';

const HINTS = {
  repaint:
    'Изменение CDV за последние 30 дней, делённое на количество акций в свободном обращении (free float). Показывает, какая доля free float нетто сменила руки за месяц: кто-то продал — кто-то новый купил. Знак — в какую сторону: + напокупали, − напродавали. CDV — аппроксимация по свечам (формула из OHLCV, как CDV в TradingView), биржевого разреза покупок/продаж в данных нет.',
  dev:
    'Отклонение текущего CDV от его среднего за 30 дней, в % от free float. Показывает, насколько напокупали или напродавали относительно накопленной базы — оценка величины спекулятивного спроса.',
  cdv:
    'Кумулятивный дельта-объём в штуках акций: сумма дельт всех свечей с начала загруженного периода (базовая точка условна — смысл несут изменения, не уровень). Дельта свечи = sign(close−open) × тело/(тело+тени) × объём.',
  ff:
    'Количество акций в свободном обращении: официальная FF-капитализация МосБиржи (cap_total × ff_factor из корзины MOEXBMI, помесячно) / цена закрытия на дату среза.',
} as const;

/** Штуки акций → компактно: 1.23 млрд / 45.6 млн / 789 тыс. */
function fmtShares(v: number): string {
  const a = Math.abs(v);
  if (a >= 1e9) return `${(v / 1e9).toFixed(2)} млрд`;
  if (a >= 1e6) return `${(v / 1e6).toFixed(1)} млн`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(0)} тыс`;
  return v.toFixed(0);
}

function fmtPct(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
}

function pctColor(v: number | null | undefined): string {
  if (v == null) return 'var(--text-muted)';
  return v >= 0 ? 'var(--success)' : 'var(--danger)';
}

export default function RepaintPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();

  const [rows, setRows] = useState<RepaintScreenerRow[]>([]);
  const [rowsError, setRowsError] = useState<string | null>(null);
  const [secId, setSecId] = useState<string>('SBER');
  const [days, setDays] = useState<number>(365);
  const [series, setSeries] = useState<RepaintSeries | null>(null);
  const [seriesLoading, setSeriesLoading] = useState(true);
  const [seriesError, setSeriesError] = useState<string | null>(null);
  const [query, setQuery] = useState('');

  // Guard: только admin
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') {
      navigate('/', { replace: true });
    }
  }, [authLoading, user, navigate]);

  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    getRepaintScreener()
      .then(r => setRows(r.rows))
      .catch((e: Error) => setRowsError(e.message));
  }, [user]);

  // Ряд по выбранному активу. Старые данные не сбрасываем — приглушаем.
  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    setSeriesLoading(true);
    setSeriesError(null);
    getRepaintSeries(secId, days)
      .then(setSeries)
      .catch((e: Error) => setSeriesError(e.message))
      .finally(() => setSeriesLoading(false));
  }, [user, secId, days]);

  const filteredRows = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(
      r => r.sec_id.toLowerCase().includes(q) || r.name.toLowerCase().includes(q),
    );
  }, [rows, query]);

  // Точки с готовыми метриками (первый месяц — прогрев окна, метрик нет).
  const metricPoints = useMemo(
    () => (series?.points ?? []).filter(p => p.repaint_pct != null),
    [series],
  );

  if (authLoading || !user || user.role !== 'admin') {
    return null;
  }

  const refreshing = seriesLoading && series !== null;

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
          <Repeat2 size={22} strokeWidth={1.8} />
        </div>
        <div>
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Перекраска
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Сколько % free float сменило руки за месяц · эксперимент, только для администратора
          </p>
        </div>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center mb-6 md:mb-8" style={{ gap: 'var(--sp-2)' }}>
        <Dropdown<string>
          options={rows.map(r => ({ key: r.sec_id, label: `${r.name} (${r.sec_id})` }))}
          value={secId}
          onChange={setSecId}
        />
        <Dropdown<string>
          options={[
            { key: '180', label: '180 дней' },
            { key: '365', label: '1 год' },
            { key: '1095', label: '3 года' },
            { key: '1825', label: '5 лет' },
          ]}
          value={String(days)}
          onChange={(v) => setDays(Number(v))}
        />
        {refreshing && (
          <span className="inline-flex items-center gap-1.5 text-xs" style={{ color: 'var(--text-muted)' }}>
            <Loader2 size={13} className="animate-spin" />
            обновление…
          </span>
        )}
      </div>

      {seriesError && (
        <Card padding="md" className="mb-6">
          <p style={{ color: 'var(--danger)' }}>Ошибка: {seriesError}</p>
        </Card>
      )}

      <div style={{ opacity: refreshing ? 0.55 : 1, transition: 'opacity 0.2s ease' }}>
        {/* Summary cards */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4 mb-6 md:mb-8">
          {series ? (
            <>
              <MetricCard
                label="Перекраска за 30 дней"
                hint={HINTS.repaint}
                value={fmtPct(series.summary.repaint_pct)}
                sub="от free float"
                color={pctColor(series.summary.repaint_pct)}
              />
              <MetricCard
                label="Отклонение от среднего"
                hint={HINTS.dev}
                value={fmtPct(series.summary.dev_pct)}
                sub="CDV vs среднее за 30 дней"
                color={pctColor(series.summary.dev_pct)}
              />
              <MetricCard
                label="CDV сейчас"
                hint={HINTS.cdv}
                value={`${series.summary.cdv >= 0 ? '+' : ''}${fmtShares(series.summary.cdv)}`}
                sub="штук акций, с начала периода"
              />
              <MetricCard
                label="Free float"
                hint={HINTS.ff}
                value={`${fmtShares(series.ff_shares)} шт`}
                sub={`срез ${series.ff_month.slice(0, 7)}`}
              />
            </>
          ) : (
            Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} height={96} rounded="lg" />)
          )}
        </div>

        {/* Цена + CDV */}
        <SectionTitle title={`${series?.name ?? secId} — цена и CDV (4Ч)`} hint={HINTS.cdv} />
        <Card padding="md" className="md:p-5 mb-6 md:mb-8">
          {series ? (
            <SimpleChart
              data={series.points.map(p => ({
                time: p.time, value: p.close, open: p.open, high: p.high, low: p.low,
              }))}
              secondaryData={series.points.map(p => ({ time: p.time, value: p.cdv }))}
              showSecondary={true}
              primaryColor="var(--accent)"
              secondaryColor="var(--accent-secondary)"
              primaryLabel="Цена"
              secondaryLabel="CDV"
              formatValue={(v) => `${v.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽`}
              formatSecondaryValue={(v) => `${v >= 0 ? '+' : ''}${fmtShares(v)} шт`}
              formatSecondaryAxis={(v) => fmtShares(v)}
              showValueHeader={false}
              legendPosition="top"
              showDownloadButton={false}
              showNavigator={false}
              height={360}
            />
          ) : (
            <Skeleton height={360} rounded="lg" />
          )}
        </Card>

        {/* Метрики перекраски */}
        <SectionTitle title="Перекраска и отклонение от среднего, % от free float" hint={HINTS.repaint} />
        <Card padding="md" className="md:p-5 mb-6 md:mb-8">
          {series ? (
            metricPoints.length > 0 ? (
              <SimpleChart
                data={metricPoints.map(p => ({ time: p.time, value: p.repaint_pct as number }))}
                secondaryData={metricPoints.map(p => ({ time: p.time, value: p.dev_pct as number }))}
                showSecondary={true}
                primaryColor="var(--accent)"
                secondaryColor="var(--accent-secondary)"
                primaryLabel="Перекраска за 30д"
                secondaryLabel="Отклонение от среднего 30д"
                formatValue={(v) => fmtPct(v)}
                formatPrimaryAxis={(v) => `${v.toFixed(1)}%`}
                formatSecondaryValue={(v) => fmtPct(v)}
                formatSecondaryAxis={(v) => `${v.toFixed(1)}%`}
                showValueHeader={false}
                legendPosition="top"
                showDownloadButton={false}
                showNavigator={false}
                height={300}
              />
            ) : (
              <p className="text-sm py-8 text-center" style={{ color: 'var(--text-muted)' }}>
                Недостаточно истории для месячного окна — метрики появятся, когда часовых свечей
                будет больше 30 дней.
              </p>
            )
          ) : (
            <Skeleton height={300} rounded="lg" />
          )}
        </Card>

        {/* Скринер по всем акциям */}
        <SectionTitle title="Все акции — текущая перекраска" hint={HINTS.repaint} />
        {rowsError && (
          <Card padding="md" className="mb-6">
            <p style={{ color: 'var(--danger)' }}>Ошибка скринера: {rowsError}</p>
          </Card>
        )}
        <Card padding="md" className="md:p-5">
          <div className="relative mb-3" style={{ maxWidth: 320 }}>
            <Search
              size={15}
              className="absolute left-3 top-1/2 -translate-y-1/2"
              style={{ color: 'var(--text-muted)' }}
            />
            <input
              type="text"
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder="Тикер или название…"
              className="w-full pl-9 pr-3 py-2 text-sm outline-none border"
              style={{
                backgroundColor: 'var(--bg-primary)',
                borderColor: 'var(--border-color)',
                borderRadius: 'var(--radius-md, 8px)',
                color: 'var(--text-primary)',
              }}
            />
          </div>
          {rows.length === 0 && !rowsError ? (
            <Skeleton height={300} rounded="lg" />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm" style={{ color: 'var(--text-primary)' }}>
                <thead>
                  <tr
                    className="text-xs uppercase text-left"
                    style={{ color: 'var(--text-muted)', letterSpacing: '0.06em' }}
                  >
                    <th className="py-2 pr-3 font-semibold">Бумага</th>
                    <th className="py-2 px-3 font-semibold text-right">Перекраска 30д</th>
                    <th className="py-2 px-3 font-semibold text-right">Отклонение 30д</th>
                    <th className="py-2 px-3 font-semibold text-right">Free float</th>
                    <th className="py-2 pl-3 font-semibold text-right">Цена</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.map(r => (
                    <tr
                      key={r.sec_id}
                      onClick={() => { setSecId(r.sec_id); window.scrollTo({ top: 0, behavior: 'smooth' }); }}
                      className="cursor-pointer transition-colors"
                      style={{
                        borderTop: '1px solid var(--border-color)',
                        backgroundColor: r.sec_id === secId
                          ? 'color-mix(in srgb, var(--accent) 8%, transparent)'
                          : undefined,
                      }}
                    >
                      <td className="py-2 pr-3">
                        <span className="font-medium">{r.name}</span>
                        <span className="ml-2 text-xs" style={{ color: 'var(--text-muted)' }}>{r.sec_id}</span>
                      </td>
                      <td className="py-2 px-3 text-right font-medium" style={{ color: pctColor(r.repaint_pct) }}>
                        {fmtPct(r.repaint_pct)}
                      </td>
                      <td className="py-2 px-3 text-right" style={{ color: pctColor(r.dev_pct) }}>
                        {fmtPct(r.dev_pct)}
                      </td>
                      <td className="py-2 px-3 text-right" style={{ color: 'var(--text-muted)' }}>
                        {fmtShares(r.ff_shares)} шт
                      </td>
                      <td className="py-2 pl-3 text-right">
                        {r.close.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} ₽
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {filteredRows.length === 0 && (
                <p className="text-sm py-6 text-center" style={{ color: 'var(--text-muted)' }}>
                  Ничего не найдено
                </p>
              )}
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}

function SectionTitle({ title, hint }: { title: string; hint: string }) {
  return (
    <div className="flex items-center gap-2 mb-3">
      <p
        className="text-xs uppercase"
        style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}
      >
        {title}
      </p>
      <HelpTooltip content={hint} icon="info" />
      <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
    </div>
  );
}

function MetricCard({ label, hint, value, sub, color }: {
  label: string;
  hint: string;
  value: string;
  sub: string;
  color?: string;
}) {
  return (
    <Card padding="md">
      <div className="flex items-center gap-1.5 mb-1.5">
        <p className="text-xs" style={{ color: 'var(--text-muted)' }}>{label}</p>
        <HelpTooltip content={hint} icon="info" />
      </div>
      <p
        className="text-xl md:text-2xl font-semibold"
        style={{ color: color ?? 'var(--text-primary)', letterSpacing: '-0.01em' }}
      >
        {value}
      </p>
      <p className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>{sub}</p>
    </Card>
  );
}
