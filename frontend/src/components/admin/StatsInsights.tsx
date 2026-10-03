/**
 * «Коротко» — эксперимент: главное о периоде плитками, а не абзацами.
 * Цифра, цветная стрелка «лучше/хуже прошлого периода», одна строка пояснения.
 *
 * Собирается правилами из уже загруженных ответов (/metrica, /growth, /stats,
 * /features), без ИИ и без отдельного запроса: каждая плитка проверяема по
 * блоку ниже. Правило молчит, если данных нет или цифра слишком мала, чтобы
 * о чём-то говорить.
 *
 * Плитки ждут самый медленный из четырёх ответов, поэтому последний готовый
 * набор хранится в браузере по ключу фильтров: при заходе он виден сразу
 * (приглушённым), а свежий подменяет его, когда придут все ответы. Поэтому
 * иконка — описание (IconSpec), а не готовая разметка: её можно сохранить.
 */
import { useEffect, useMemo } from 'react';
import { TrendingUp, TrendingDown, Users, UserPlus, CreditCard, Flame } from 'lucide-react';
import Card from '../Card';
import Skeleton from '../Skeleton';
import HelpTooltip from '../HelpTooltip';
import { PAGE_NAMES } from './ActivityBlocks';
import { TERMINAL_PANEL } from './FeaturesBlock';
import { IndicatorGlyph } from './indicatorMeta';
import type { AnalyticsStats, FeaturesReport, GrowthReport, MetricaReport } from '../../services/api';

const HINT =
  'Плитки собраны правилами из цифр на этой странице, без ИИ: каждую можно проверить по блокам ниже. '
  + 'Зелёный — лучше прошлого такого же периода, красный — хуже, серый — без заметных изменений. '
  + 'Посетители и источники — по Яндекс Метрике, разделы, фонды и терминал — по нашему трекеру, воронка — без админов.';

// Служебные страницы в «главный раздел» не годятся.
const NOT_SECTIONS = /^\/($|login|pricing|profile|admin|auth|billing|add-email|methodology)/;

// Короткие имена источников Метрики для одной строки пояснения.
const SHORT_SOURCE: [RegExp, string][] = [
  [/поиск/i, 'поиск'], [/прям/i, 'прямые'], [/мессенд/i, 'мессенджеры'], [/соц/i, 'соцсети'],
  [/ссылк/i, 'ссылки'], [/реклам/i, 'реклама'], [/рекоменд/i, 'рекомендации'],
];

type Tone = 'good' | 'bad' | 'flat';

const LUCIDE = { users: Users, userPlus: UserPlus, card: CreditCard, flame: Flame } as const;

/** Иконка плитки: раздел сайта (его глиф) или значок lucide с цветом. */
type IconSpec = { glyph: string } | { lucide: keyof typeof LUCIDE; color: string };

export interface Insight {
  key: string;
  icon: IconSpec;
  label: string;
  value: string;
  /** Изменение к прошлому периоду: текст и цвет. */
  delta?: { text: string; tone: Tone };
  note?: string;
}

const fmt = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: 1 });
const pct = (part: number, whole: number) => Math.round((part / whole) * 100);

function change(cur: number, prev: number | undefined | null): Insight['delta'] {
  if (!prev) return undefined;
  const p = Math.round(((cur - prev) / prev) * 100);
  return { text: `${p >= 0 ? '+' : '−'}${Math.abs(p)}%`, tone: Math.abs(p) < 5 ? 'flat' : p > 0 ? 'good' : 'bad' };
}

function renderIcon(spec: IconSpec) {
  if ('glyph' in spec) return <IndicatorGlyph path={spec.glyph} size={28} />;
  const Icon = LUCIDE[spec.lucide] ?? Users;
  const color = spec.color;
  return (
    <span
      className="flex items-center justify-center rounded-md shrink-0"
      style={{ width: 28, height: 28, color, backgroundColor: `color-mix(in srgb, ${color} 16%, transparent)` }}
    >
      <Icon size={15} />
    </span>
  );
}

export function buildInsights({ metrica, growth, stats, features }: {
  metrica: MetricaReport | null;
  growth: GrowthReport | null;
  stats: AnalyticsStats | null;
  features: FeaturesReport | null;
}): Insight[] {
  const out: Insight[] = [];

  // Посетители и главный источник.
  const cur = metrica?.connected ? metrica.summary : null;
  if (cur && cur.users > 0) {
    const src = metrica?.sources?.[0];
    const total = metrica?.sources?.reduce((a, r) => a + r.value, 0) ?? 0;
    const short = src ? SHORT_SOURCE.find(([re]) => re.test(src.label))?.[1] ?? src.label : null;
    out.push({
      key: 'visitors', icon: { lucide: 'users', color: 'var(--info)' }, label: 'Посетители',
      value: fmt(cur.users), delta: change(cur.users, metrica?.prev_summary?.users),
      note: src && total > 0 ? `${pct(src.value, total)}% визитов — ${short}` : undefined,
    });
  }

  // Регистрации и оплаты.
  const f = growth?.funnel;
  if (f) {
    out.push({
      key: 'registered', icon: { lucide: 'userPlus', color: 'var(--accent)' }, label: 'Регистрации',
      value: fmt(f.registered),
      note: f.visitors ? `${fmt((f.registered / f.visitors) * 100)}% посетителей` : undefined,
    });
    out.push({
      key: 'paid', icon: { lucide: 'card', color: f.paid > 0 ? 'var(--success)' : 'var(--danger)' }, label: 'Оплаты',
      value: fmt(f.paid),
      delta: f.paid > 0 ? undefined : { text: 'нет', tone: 'bad' },
      note: f.registered > 0 ? `из ${f.registered} новых аккаунтов` : undefined,
    });
  }

  // Главный раздел.
  const top = (stats?.top_pages ?? []).find((p) => !NOT_SECTIONS.test(p.path));
  const allVisitors = stats?.summary.visitors ?? 0;
  if (top && allVisitors > 0) {
    out.push({
      key: 'section', icon: { glyph: top.path }, label: 'Главный раздел',
      value: PAGE_NAMES[top.path] ?? top.path, note: `открывали ${pct(top.visitors, allVisitors)}% посетителей`,
    });
  }

  if (features) {
    // Самое заметное изменение среди фондов и терминала.
    const named: [string, string, { people: number; prev_people: number }][] = [
      ['/funds-money', 'Деньги в фондах', features.funds_money],
      ['/fund-trades', 'Сделки фондов', features.fund_trades],
      ['/sandbox', 'Терминал', features.terminal],
    ];
    const moved = named
      .filter(([, , v]) => v.prev_people >= 5 && v.people > 0)
      .map(([path, name, v]) => ({ path, name, v, d: (v.people - v.prev_people) / v.prev_people }))
      .sort((a, b) => Math.abs(b.d) - Math.abs(a.d))[0];
    if (moved && Math.abs(moved.d) >= 0.2) {
      out.push({
        key: 'moved', icon: { glyph: moved.path }, label: moved.name,
        value: `${fmt(moved.v.people)} чел.`, delta: change(moved.v.people, moved.v.prev_people),
        note: 'самый заметный сдвиг',
      });
    }

    const tm = features.terminal;
    const tt = tm.types[0];
    if (tm.with_panels >= 3 && tt && moved?.path !== '/sandbox') {
      out.push({
        key: 'terminal', icon: { glyph: '/sandbox' }, label: 'Терминал',
        value: `${fmt(tm.with_panels)} чел.`,
        note: `~${fmt(tm.avg_panels)} окон, чаще ${(TERMINAL_PANEL[tt.key] ?? tt.key).toLowerCase()}`,
      });
    }
  }

  // Тёплые гости.
  const g = growth?.guests;
  if (g && g.days2 >= 5) {
    out.push({
      key: 'guests', icon: { lucide: 'flame', color: 'var(--warning)' }, label: 'Тёплые гости',
      value: fmt(g.days2), note: '2+ дня без аккаунта',
    });
  }

  return out.slice(0, 6);
}

const TONE_COLOR: Record<Tone, string> = { good: 'var(--success)', bad: 'var(--danger)', flat: 'var(--text-muted)' };

const CACHE_KEY = 'frame:admin:stats:insights';
const CACHE_MAX = 12;

function readCache(): Record<string, Insight[]> {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function writeCache(filterKey: string, items: Insight[]) {
  try {
    const all = readCache();
    delete all[filterKey];
    // Свежий ключ — последним; старые вытесняются, чтобы кэш не рос без конца.
    const entries = [...Object.entries(all), [filterKey, items] as const].slice(-CACHE_MAX);
    localStorage.setItem(CACHE_KEY, JSON.stringify(Object.fromEntries(entries)));
  } catch { /* приватный режим или переполнение — просто без кэша */ }
}

export default function StatsInsights({ items: fresh, loading, cacheKey }: {
  items: Insight[];
  /** Хоть один из четырёх ответов ещё в пути. */
  loading: boolean;
  /** Период + срез: плитки одного набора фильтров не подменяют другой. */
  cacheKey: string;
}) {
  // Читаем при смене фильтров; запись свежего набора ниже его уже не трогает —
  // после загрузки и так показываются свежие плитки.
  const cached = useMemo(() => readCache()[cacheKey] ?? null, [cacheKey]);
  useEffect(() => {
    if (!loading && fresh.length > 0) writeCache(cacheKey, fresh);
  }, [loading, fresh, cacheKey]);

  // Пока ответы в пути, полный прошлый набор лучше половины свежего.
  const stale = loading && !!cached && cached.length > 0;
  const items = stale ? cached! : fresh;
  if (items.length === 0 && !loading) return null;
  return (
    <Card padding="md" className="md:p-5">
      <div className="flex items-center gap-2 mb-3">
        <span className="text-xs uppercase" style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}>
          Коротко
        </span>
        <span
          className="text-xs rounded-full"
          style={{ padding: '1px 8px', border: '1px solid var(--border-color)', color: 'var(--text-muted)' }}
        >
          эксперимент
        </span>
        <HelpTooltip icon="help" title="Как собраны плитки" content={HINT} size={13} />
        {stale && <span className="text-xs ml-auto" style={{ color: 'var(--text-muted)' }}>обновляем…</span>}
      </div>
      {items.length === 0 ? (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-2" aria-label="Загрузка">
          {Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} height={92} rounded="lg" />)}
        </div>
      ) : (
        <div
          className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-2"
          style={{ animation: 'fadeIn 0.2s cubic-bezier(.2,0,0,1)', opacity: stale ? 0.6 : 1, transition: 'opacity 0.12s cubic-bezier(.2,0,0,1)' }}
        >
          {items.map((it) => (
            <div
              key={it.key}
              className="rounded-lg min-w-0"
              style={{
                padding: '10px 12px',
                background: 'var(--bg-tertiary)',
                boxShadow: it.delta ? `inset 3px 0 0 ${TONE_COLOR[it.delta.tone]}` : undefined,
              }}
            >
              <div className="flex items-center gap-2 mb-1.5">
                {renderIcon(it.icon)}
                <span className="text-xs truncate" style={{ color: 'var(--text-muted)' }}>{it.label}</span>
              </div>
              <div className="flex items-baseline gap-1.5 flex-wrap">
                <span
                  className="truncate"
                  style={{
                    color: 'var(--text-primary)', fontWeight: 700, fontSize: 18, lineHeight: 1.25,
                    fontFamily: "'IBM Plex Mono', monospace", fontVariantNumeric: 'tabular-nums',
                  }}
                  title={it.value}
                >
                  {it.value}
                </span>
                {it.delta && (
                  <span
                    className="inline-flex items-center gap-0.5 text-xs"
                    style={{ color: TONE_COLOR[it.delta.tone], fontWeight: 600, fontFamily: "'IBM Plex Mono', monospace" }}
                  >
                    {it.delta.tone === 'good' && <TrendingUp size={12} />}
                    {it.delta.tone === 'bad' && <TrendingDown size={12} />}
                    {it.delta.text}
                  </span>
                )}
              </div>
              {it.note && (
                <div className="text-xs mt-1 truncate" style={{ color: 'var(--text-secondary)' }} title={it.note}>{it.note}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
