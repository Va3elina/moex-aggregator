/**
 * Общие блоки карточки «действующего лица» — гостя и зарегистрированного.
 *
 * Бэкенд считает их активность одним кодом (`_activity_detail`), поэтому и
 * рисуются они одним компонентом: иначе определения «визита» и «действия»
 * незаметно разъехались бы между двумя страницами.
 */
import { Activity, Globe, Monitor, Zap } from 'lucide-react';
import Card from '../Card';
import HelpTooltip from '../HelpTooltip';
import type { ActivityDetail } from '../../services/api';

const MONO = "'IBM Plex Mono', monospace";

// Человеческие имена разделов для топа страниц. Неизвестный путь показывается как есть.
export const PAGE_NAMES: Record<string, string> = {
  '/': 'Главная',
  '/oi': 'Открытый интерес',
  '/heatmap': 'Карта рынка',
  '/strength': 'Сила рынка',
  '/funds-money': 'Деньги в фондах',
  '/fund-trades': 'Покупки фондов',
  '/seasonality': 'Сезонность',
  '/repo': 'Репо в акциях',
  '/cbr-flows': 'Потоки капитала',
  '/buffett': 'Индикатор Баффета',
  '/pricing': 'Тарифы',
  '/profile': 'Профиль',
  '/login': 'Вход',
};

export const DEVICE_NAMES: Record<string, string> = {
  desktop: 'Компьютер',
  mobile: 'Телефон',
  tablet: 'Планшет',
  unknown: 'Не определено',
};

export const DETAIL_HINTS = {
  visits: 'Визит — серия действий. Пауза дольше 30 минут начинает новый визит, как в Яндекс Метрике.',
  actions: 'Просмотры страниц, показы активов, выборы в поиске, экспорты и другие действия. Служебный сигнал присутствия не считается.',
  avg: 'Среднее время визита: от первого до последнего действия. Пока человек активен на вкладке, раз в минуту уходит сигнал присутствия. Через 5 минут без действий он останавливается.',
  total: 'Сумма времени всех визитов за период.',
  days: 'В скольких разных днях человек был на сайте за период. Это мера привычки: 1 — заглянул, 10+ — ходит почти каждый день.',
  assets: 'Какие активы открывал: считается и выбор в пикере, и переход по ссылке.',
} as const;

export function SectionHeader({ title, subtitle }: { title: string; subtitle?: string }) {
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

export function SmallStat({
  label, value, hint, formatter = (v) => v.toLocaleString('ru-RU'),
}: {
  label: string; value: number; hint?: string; formatter?: (v: number) => string;
}) {
  return (
    <Card padding="md">
      <div className="flex items-center gap-2 mb-1" style={{ color: 'var(--text-muted)' }}>
        <Activity size={12} />
        <span className="text-xs uppercase" style={{ letterSpacing: '0.1em', fontWeight: 600 }}>{label}</span>
        {hint && <HelpTooltip icon="help" title={label} content={hint} size={12} />}
      </div>
      <div className="font-bold" style={{
        color: 'var(--text-primary)',
        fontSize: 'clamp(1.2rem, 2vw, 1.6rem)',
        fontFamily: MONO,
        fontVariantNumeric: 'tabular-nums',
      }}>
        {formatter(value)}
      </div>
    </Card>
  );
}

export function SimpleTopList({ title, items, hint }: {
  title: string;
  items: { label: string; value: number; note?: string }[];
  hint?: string;
}) {
  const max = items.length > 0 ? Math.max(...items.map(i => i.value)) : 1;
  return (
    <Card padding="md">
      <div className="flex items-center gap-2 mb-3">
        <p className="text-xs uppercase" style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}>
          {title}
        </p>
        {hint && <HelpTooltip icon="help" title={title} content={hint} size={12} />}
      </div>
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
                  {it.note && (
                    <span className="ml-2 text-xs" style={{ color: 'var(--text-muted)' }}>{it.note}</span>
                  )}
                </span>
                <span className="text-sm font-semibold flex-shrink-0 ml-2" style={{ color: 'var(--text-primary)', fontFamily: MONO }}>
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

export function Chip({ icon, label }: { icon?: React.ReactNode; label: string }) {
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

/** Топы, устройства и лента — всё, что одинаково у гостя и у юзера. */
export function ActivityBlocks({ data, pageNames }: {
  data: ActivityDetail;
  /** Человеческие названия страниц; для неизвестного пути покажем сам путь. */
  pageNames?: Record<string, string>;
}) {
  const assets = data.top_assets.length > 0
    ? data.top_assets.map(a => ({ label: a.name || a.secid, value: a.views, note: a.name ? a.secid : undefined }))
    : data.top_instruments.map(a => ({ label: a.name || a.secid, value: a.selects, note: a.name ? a.secid : undefined }));

  return (
    <>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4 mb-3 md:mb-4">
        <SimpleTopList
          title="Что смотрит"
          items={data.top_pages.map(p => ({
            label: pageNames?.[p.path] || p.path,
            value: p.views,
            note: p.days > 1 ? `${p.days} дн.` : undefined,
          }))}
        />
        <SimpleTopList
          title={data.assets_all_time ? 'Какие активы · за всё время' : 'Какие активы'}
          hint={DETAIL_HINTS.assets}
          items={assets}
        />
      </div>

      <div className="mb-6">
        <Card padding="md">
          <p className="text-xs uppercase mb-3" style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}>
            Устройства / страны
          </p>
          <div className="flex flex-wrap gap-2 mb-3">
            {data.devices.map(d => <Chip key={d.device} icon={<Monitor size={11} />} label={`${d.device}: ${d.sessions}`} />)}
          </div>
          <div className="flex flex-wrap gap-2 mb-3">
            {data.countries.map(c => <Chip key={c.country} icon={<Globe size={11} />} label={`${c.country}: ${c.sessions}`} />)}
            {data.devices.length === 0 && data.countries.length === 0 && (
              <p className="text-xs" style={{ color: 'var(--text-muted)' }}>Нет данных</p>
            )}
          </div>
          {data.top_exports.length > 0 && (
            <>
              <p className="text-xs uppercase mb-2 mt-4" style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}>
                Экспорты PNG
              </p>
              <div className="flex flex-wrap gap-2">
                {data.top_exports.map(e => <Chip key={e.indicator} label={`${e.indicator}: ${e.count}`} />)}
              </div>
            </>
          )}
        </Card>
      </div>

      <SectionHeader title="Лента действий" subtitle={`Последние ${data.timeline.length} событий`} />
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
                    {/* Бэкенд схлопывает подряд идущие heartbeat'ы в один спан
                        с payload {beats, mins} — рисуем его как человекочитаемое
                        «на сайте», а не простыню событий. */}
                    {ev.event_type === 'session_heartbeat' && typeof ev.payload?.mins === 'number'
                      ? `на сайте ~${String(ev.payload?.mins)} мин`
                      : ev.event_type}
                  </span>
                  {ev.event_path && (
                    <span className="truncate" style={{ color: 'var(--text-secondary)', fontFamily: MONO }}>
                      {ev.event_path}
                    </span>
                  )}
                  {ev.payload && ev.event_type !== 'session_heartbeat' && (
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
  );
}

export function fmtDate(iso: string): string {
  return new Date(iso).toLocaleDateString('ru-RU');
}
export function fmtDateTime(iso: string): string {
  return new Date(iso).toLocaleString('ru-RU', {
    day: '2-digit', month: '2-digit', year: '2-digit',
    hour: '2-digit', minute: '2-digit',
  });
}
export function fmtDuration(sec: number): string {
  if (!sec || sec < 0) return '0с';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}ч ${m}м`;
  if (m > 0) return `${m}м ${s}с`;
  return `${s}с`;
}
