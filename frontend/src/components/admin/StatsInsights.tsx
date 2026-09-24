/**
 * «Коротко» — эксперимент: несколько фраз о периоде вместо чтения всех цифр.
 *
 * Собирается правилами из уже загруженных ответов (/metrica, /growth, /stats,
 * /features), без ИИ и без отдельного запроса: каждая фраза проверяема по
 * блоку ниже. Правило молчит, если данных для него нет или цифра слишком
 * мала, чтобы о чём-то говорить — лучше три верные фразы, чем шесть натянутых.
 */
import Card from '../Card';
import HelpTooltip from '../HelpTooltip';
import { PAGE_NAMES } from './ActivityBlocks';
import { TERMINAL_PANEL } from './FeaturesBlock';
import type { AnalyticsStats, FeaturesReport, GrowthReport, MetricaReport } from '../../services/api';

const HINT =
  'Фразы собраны правилами из цифр на этой странице, без ИИ: каждую можно проверить по блокам ниже. '
  + 'Посетители и источники — по Яндекс Метрике, разделы, фонды и терминал — по нашему трекеру, воронка — без админов. '
  + 'Раздел экспериментальный: скажите, какие выводы полезны, а какие нет.';

// Служебные страницы в «главный раздел» не годятся.
const NOT_SECTIONS = /^\/($|login|pricing|profile|admin|auth|billing|add-email|methodology)/;

function people(n: number): string {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return 'человек';
  if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return 'человека';
  return 'человек';
}

function windows(n: number): string {
  const r = Math.round(n);
  const m10 = r % 10, m100 = r % 100;
  if (!Number.isInteger(n)) return 'окна';
  if (m10 === 1 && m100 !== 11) return 'окно';
  if (m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14)) return 'окна';
  return 'окон';
}

const fmt = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: 1 });
const pct = (part: number, whole: number) => Math.round((part / whole) * 100);

function change(cur: number, prev: number): string | null {
  if (!prev) return null;
  const p = Math.round(((cur - prev) / prev) * 100);
  if (Math.abs(p) < 5) return 'примерно столько же, сколько';
  return `на ${Math.abs(p)}% ${p > 0 ? 'больше' : 'меньше'}, чем`;
}

export function buildInsights({ metrica, growth, stats, features, days }: {
  metrica: MetricaReport | null;
  growth: GrowthReport | null;
  stats: AnalyticsStats | null;
  features: FeaturesReport | null;
  days: number;
}): string[] {
  const out: string[] = [];

  // 1. Сколько людей и откуда.
  const cur = metrica?.connected ? metrica.summary : null;
  if (cur && cur.users > 0) {
    const ch = metrica?.prev_summary ? change(cur.users, metrica.prev_summary.users) : null;
    let s = `На сайте было ${fmt(cur.users)} ${people(cur.users)}`;
    if (ch) s += ` — ${ch} за предыдущие ${days} дн.`;
    const src = metrica?.sources?.[0];
    const total = metrica?.sources?.reduce((a, r) => a + r.value, 0) ?? 0;
    if (src && total > 0) s += `${ch ? ' ' : '. '}Больше всего визитов — «${src.label}», ${pct(src.value, total)}%.`;
    else if (!ch) s += '.';
    out.push(s);
  }

  // 2. Воронка.
  const f = growth?.funnel;
  if (f && f.visitors && f.visitors > 0) {
    let s = `Зарегистрировались ${f.registered} из ${fmt(f.visitors)} посетителей`;
    if (f.registered > 0) s += ` (${fmt((f.registered / f.visitors) * 100)}%)`;
    s += f.paid > 0 ? `, оплатили ${f.paid}.` : ', оплат среди них пока нет.';
    out.push(s);
  }

  // 3. Главный раздел.
  const sections = (stats?.top_pages ?? []).filter((p) => !NOT_SECTIONS.test(p.path));
  const top = sections[0];
  const allVisitors = stats?.summary.visitors ?? 0;
  if (top && allVisitors > 0) {
    const name = PAGE_NAMES[top.path] ?? top.path;
    let s = `Главный раздел — «${name}»: его открывали ${pct(top.visitors, allVisitors)}% посетителей`;
    const second = sections[1];
    if (second) s += `, следом «${PAGE_NAMES[second.path] ?? second.path}» — ${pct(second.visitors, allVisitors)}%`;
    out.push(`${s}.`);
  }

  // 4. Фонды и терминал: самое заметное изменение и что внутри.
  if (features) {
    const named: [string, { people: number; prev_people: number }][] = [
      ['Деньги в фондах', features.funds_money],
      ['Сделки фондов', features.fund_trades],
      ['Терминал', features.terminal],
    ];
    const moved = named
      .filter(([, v]) => v.prev_people >= 5 && v.people > 0)
      .map(([n, v]) => ({ n, v, d: (v.people - v.prev_people) / v.prev_people }))
      .sort((a, b) => Math.abs(b.d) - Math.abs(a.d))[0];
    if (moved && Math.abs(moved.d) >= 0.2) {
      out.push(`«${moved.n}» открыли ${moved.v.people} ${people(moved.v.people)} — `
        + `на ${Math.round(Math.abs(moved.d) * 100)}% ${moved.d > 0 ? 'больше' : 'меньше'}, чем за прошлый период.`);
    }

    const tm = features.terminal;
    const tt = tm.types[0];
    if (tm.with_panels >= 3 && tt) {
      out.push(`Терминал собрали ${tm.with_panels} ${people(tm.with_panels)}, в среднем ${fmt(tm.avg_panels)} ${windows(tm.avg_panels)}; `
        + `чаще всего в окнах — «${TERMINAL_PANEL[tt.key] ?? tt.key}» (${pct(tt.people, tm.with_panels)}%).`);
    }

    const asset = features.fund_trades.assets[0];
    const fund = features.fund_trades.opened[0];
    if (asset && asset.people >= 3) {
      let s = `В «Сделках фондов» чаще всего смотрят бумагу «${asset.key}» (${asset.people} ${people(asset.people)})`;
      if (fund && fund.people >= 3) s += `, из фондов открывают ${fund.name || fund.ticker}`;
      out.push(`${s}.`);
    }
  }

  // 5. Тёплые гости.
  const g = growth?.guests;
  if (g && g.days2 >= 5) {
    out.push(`${g.days2} гостей заходили в 2+ разных дня, но так и не зарегистрировались — это самые тёплые кандидаты.`);
  }

  return out.slice(0, 6);
}

export default function StatsInsights({ items, loading }: { items: string[]; loading: boolean }) {
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
        <HelpTooltip icon="help" title="Откуда эти фразы" content={HINT} size={13} />
      </div>
      {items.length === 0 ? (
        <p className="text-sm" style={{ color: 'var(--text-muted)' }}>Собираем цифры…</p>
      ) : (
        <ul className="space-y-2" style={{ animation: 'fadeIn 0.3s ease-out' }}>
          {items.map((t) => (
            <li key={t} className="flex gap-2 text-sm" style={{ color: 'var(--text-primary)', lineHeight: 1.5 }}>
              <span aria-hidden style={{ color: 'var(--accent)', flexShrink: 0 }}>—</span>
              <span>{t}</span>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}
