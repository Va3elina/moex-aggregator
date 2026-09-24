/**
 * «Фонды и терминал» в /admin/stats: что делают внутри «Денег в фондах»,
 * «Сделок фондов» и терминала. Данные — GET /api/analytics/features.
 *
 * Все цифры в людях: настройки пишутся снимками, у одного человека их
 * десятки, и в штуках они мерили бы усидчивость, а не интерес. Снимки
 * пишутся с 21.09.2026 — «Открывали раздел» бывает больше «Настройки
 * записаны»: часть людей пришла раньше или отключила статистику.
 */
import Card from '../Card';
import Skeleton from '../Skeleton';
import HelpTooltip from '../HelpTooltip';
import TopList from './TopList';
import FundLogoChip from './FundLogoChip';
import InstrumentIcon from '../InstrumentIcon';
import { IndicatorGlyph, INDICATOR_ICONS } from './indicatorMeta';
import type { FeaturesReport, FeaturePick, FeatureFund } from '../../services/api';

// Подписи — те же, что на самих страницах (FundsMoneyPage, FundTradesPage,
// CompanyFlowsTab, SandboxPage). Неизвестный ключ показывается как есть.
const FUND_CATEGORY: Record<string, string> = {
  money_market: 'Денежный рынок', stocks: 'Акции', bonds: 'Облигации', gold: 'Золото', yuan: 'Юань',
};
const FUNDS_VIEW: Record<string, string> = { flows: 'Притоки-Оттоки', aum: 'СЧА' };
const FUNDS_PERIOD: Record<string, string> = { '1w': '1Н', '1m': '1М', '1y': '1Г', '3y': '3Г', all: 'Всё' };
const FUNDS_TF: Record<string, string> = { '1d': 'дни', '1w': 'недели', '1m': 'месяцы', '3m': 'кварталы', '1y': 'годы' };
const TRADES_TAB: Record<string, string> = {
  portfolio: 'Общий портфель', company: 'По бумаге', funds: 'Витрина', movers: 'Сделки', snapshots: 'Снимки',
};
const TRADES_MODE: Record<string, string> = { map: 'Сделки', rub: 'Позиция', cap: '% в обращении', overhang: 'Навес' };
const TRADES_PERIOD: Record<string, string> = { '1y': '1 год', '3y': '3 года', all: 'Всё' };
const PORTFOLIO_MODE: Record<string, string> = { rub: 'По капиталу', share: 'По доле' };
export const TERMINAL_PANEL: Record<string, string> = {
  signals: 'Сигналы', oi: 'Открытые позиции', seasonality: 'Сезонность', screener: 'Скринер сигналов',
  buffett: 'Индикатор Баффетта', strength: 'Сила рынка', 'funds-money': 'Деньги в фондах',
  'fund-trades': 'Сделки фондов', 'fund-movers': 'Сделки фондов (старое окно)', 'cbr-flows': 'Поток капитала',
  heatmap: 'Карта рынка',
};
const THEME: Record<string, string> = { dark: 'тёмная', light: 'светлая' };
const SHEETS: Record<string, string> = { '1': '1 лист', '2': '2 листа', '3+': '3 и больше' };
const WINDOW_ORDER = ['0', '1', '2–3', '4–6', '7+'];

const NUM_FONT = "'IBM Plex Mono', monospace";

const HINTS = {
  section:
    'Считаем людей, а не просмотры: сколько разных людей хоть раз выбрали категорию, вкладку или бумагу. '
    + 'Настройки внутри разделов записываются с 21.09.2026, поэтому «настройки записаны» бывает меньше, чем «открывали»: '
    + 'часть людей заходила раньше или отключила статистику в профиле. Сравнение — с таким же периодом сразу перед выбранным.',
  funds_picked:
    'Фонды, которые человек сам оставил на графике, убрав остальные. Кто смотрит всю категорию целиком, сюда не попадает.',
  terminal:
    'Раскладка — по последнему состоянию терминала человека за период: сколько окон и листов у него в итоге осталось. '
    + '«Добавляли» — сами действия: какой индикатор выносили в новое окно.',
  terminal_assets: 'Активы, которые выбирали в окнах терминала.',
};

function named(rows: FeaturePick[], names: Record<string, string>) {
  return rows.map((r) => ({ label: names[r.key] ?? r.key, value: r.people }));
}

function fundItems(rows: FeatureFund[]) {
  return rows.map((f) => ({
    label: f.name || f.ticker, note: f.name ? f.ticker : undefined, value: f.people,
    icon: <FundLogoChip ticker={f.ticker} ukId={f.uk_id} size={20} />,
  }));
}

/** Иконка индикатора окна терминала — та же, что у раздела на сайте. */
function panelIcon(key: string) {
  return INDICATOR_ICONS[`/${key}`] ? <IndicatorGlyph path={`/${key}`} size={20} /> : undefined;
}

function deltaText(cur: number, prev: number): { text: string; good: boolean } | null {
  if (!prev) return null;
  const pct = Math.round(((cur - prev) / prev) * 100);
  return { text: `${pct >= 0 ? '+' : '−'}${Math.abs(pct)}%`, good: pct >= 0 };
}

/** Строка «Режим — Притоки-Оттоки 35 · СЧА 8»: мелкие разрезы без отдельной карточки. */
function PickLine({ label, rows, names }: { label: string; rows: FeaturePick[]; names: Record<string, string> }) {
  if (rows.length === 0) return null;
  return (
    <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1 text-sm" style={{ padding: '6px 0' }}>
      <span className="text-xs uppercase" style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600, minWidth: 96 }}>
        {label}
      </span>
      {rows.map((r) => (
        <span key={r.key} style={{ color: 'var(--text-secondary)' }}>
          {names[r.key] ?? r.key}{' '}
          <span style={{ color: 'var(--text-primary)', fontWeight: 600, fontFamily: NUM_FONT, fontVariantNumeric: 'tabular-nums' }}>
            {r.people}
          </span>
        </span>
      ))}
    </div>
  );
}

/** Шапка подраздела: сколько людей открывали, дельта, у скольких записаны настройки. */
function Head({ path, title, people, prev, tracked, trackedLabel = 'настройки записаны у' }: {
  path: string; title: string; people: number; prev: number; tracked: number; trackedLabel?: string;
}) {
  const d = deltaText(people, prev);
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mb-3">
      <h3 className="text-base font-semibold inline-flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
        <IndicatorGlyph path={path} size={28} />
        {title}
      </h3>
      <span className="text-sm" style={{ color: 'var(--text-secondary)' }}>
        открывали{' '}
        <span style={{ color: 'var(--text-primary)', fontWeight: 600, fontFamily: NUM_FONT }}>{people.toLocaleString('ru-RU')}</span>
        {d && (
          <span className="text-xs ml-1.5" style={{ color: d.good ? 'var(--success)' : 'var(--danger)', fontFamily: NUM_FONT }}>
            {d.text}
          </span>
        )}
      </span>
      <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
        {trackedLabel} {tracked.toLocaleString('ru-RU')}
      </span>
    </div>
  );
}

/** text — значение словом («Открытые позиции»): мельче и без моноширинного, чтобы не переносилось. */
function Stat({ label, value, sub, text }: { label: string; value: string; sub?: string; text?: boolean }) {
  return (
    <div>
      <div className="text-xs uppercase mb-1" style={{ color: 'var(--text-muted)', letterSpacing: '0.08em', fontWeight: 600 }}>{label}</div>
      <div
        style={text
          ? { color: 'var(--text-primary)', fontSize: 17, fontWeight: 600, lineHeight: 1.3, paddingTop: 4 }
          : { color: 'var(--text-primary)', fontSize: 24, fontWeight: 700, fontFamily: NUM_FONT, fontVariantNumeric: 'tabular-nums' }}
      >
        {value}
      </div>
      {sub && <div className="text-xs mt-0.5" style={{ color: 'var(--text-muted)' }}>{sub}</div>}
    </div>
  );
}

export default function FeaturesBlock({ data, loading, error }: {
  data: FeaturesReport | null; loading: boolean; error: string | null;
}) {
  if (loading && !data) {
    return (
      <div className="space-y-4">
        {Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} height={280} rounded="lg" />)}
      </div>
    );
  }
  if (!data) {
    return (
      <Card padding="md">
        <p className="text-sm" style={{ color: 'var(--danger)' }}>{error || 'Не удалось загрузить данные по фондам и терминалу.'}</p>
      </Card>
    );
  }
  const fm = data.funds_money;
  const ft = data.fund_trades;
  const tm = data.terminal;
  const windows = [...tm.windows].sort((a, b) => WINDOW_ORDER.indexOf(a.key) - WINDOW_ORDER.indexOf(b.key));
  const topType = tm.types[0];
  const [theme1, theme2] = tm.themes;
  const themeValue = !theme1 ? '—' : theme2 && theme2.people === theme1.people ? 'поровну' : (THEME[theme1.key] ?? theme1.key);

  return (
    <div className="space-y-8" style={{ animation: 'fadeIn 0.35s ease-out' }}>
      <p className="text-sm inline-flex items-center gap-1.5" style={{ color: 'var(--text-muted)' }}>
        Все цифры — разные люди за период
        <HelpTooltip icon="help" title="Как считаем" content={HINTS.section} size={13} />
      </p>

      {/* ── Терминал ── */}
      <section>
        <Head path="/sandbox" title="Терминал" people={tm.people} prev={tm.prev_people} tracked={tm.with_panels} trackedLabel="собрали окна" />
        <Card padding="md" className="md:p-5 mb-3 md:mb-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Stat label="Собрали терминал" value={tm.with_panels.toLocaleString('ru-RU')}
              sub={tm.tracked > tm.with_panels ? `ещё ${tm.tracked - tm.with_panels} открыли пустым` : undefined} />
            <Stat label="Окон в среднем" value={tm.avg_panels ? tm.avg_panels.toLocaleString('ru-RU') : '—'}
              sub={tm.max_panels ? `максимум ${tm.max_panels}` : undefined} />
            <Stat label="Чаще всего в окнах" text value={topType ? (TERMINAL_PANEL[topType.key] ?? topType.key) : '—'}
              sub={topType ? `у ${topType.people} из ${tm.with_panels}` : undefined} />
            <Stat label="Тема" text value={themeValue}
              sub={tm.themes.length > 1 ? tm.themes.map((t) => `${THEME[t.key] ?? t.key} ${t.people}`).join(' · ') : undefined} />
          </div>
          <div className="mt-4 pt-3" style={{ borderTop: '1px solid color-mix(in srgb, var(--text-muted) 25%, transparent)' }}>
            <PickLine label="Листов" rows={tm.sheets} names={SHEETS} />
            <PickLine label="Добавляли" rows={tm.added.map((a) => ({ key: a.key, people: a.people }))} names={TERMINAL_PANEL} />
          </div>
        </Card>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-3 md:gap-4">
          <TopList title="Сколько окон" hint={HINTS.terminal} columns={['людей']}
            items={windows.map((w) => ({ label: w.key === '0' ? 'пустой терминал' : `${w.key} ${w.key === '1' ? 'окно' : w.key === '2–3' ? 'окна' : 'окон'}`, value: w.people }))}
            loading={false} emptyText="Раскладок за период нет" />
          <TopList title="Индикаторы в окнах" columns={['людей', 'окон']}
            items={tm.types.map((t) => ({ label: TERMINAL_PANEL[t.key] ?? t.key, value: t.people, value2: t.panels, icon: panelIcon(t.key) }))}
            loading={false} emptyText="Окон за период нет" />
          <TopList title="Активы в терминале" hint={HINTS.terminal_assets} hintAlign="right" columns={['людей']}
            items={tm.assets.map((a) => ({ label: a.name, note: a.name !== a.key ? a.key : undefined, value: a.people, icon: <InstrumentIcon sectype={a.key} size={20} /> }))}
            loading={false} emptyText="Активы в терминале не выбирали" />
        </div>
      </section>

      {/* ── Сделки фондов ── */}
      <section>
        <Head path="/fund-trades" title="Сделки фондов" people={ft.people} prev={ft.prev_people} tracked={ft.tracked} />
        <Card padding="md" className="md:p-5 mb-3 md:mb-4">
          <PickLine label="Вкладки" rows={ft.tabs} names={TRADES_TAB} />
          <PickLine label="По бумаге" rows={ft.modes} names={TRADES_MODE} />
          <PickLine label="Период" rows={ft.periods} names={TRADES_PERIOD} />
          <PickLine label="Портфель" rows={ft.portfolio_modes} names={PORTFOLIO_MODE} />
          {ft.tabs.length === 0 && (
            <p className="text-sm" style={{ color: 'var(--text-muted)' }}>Настроек за период не записано.</p>
          )}
        </Card>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
          <TopList title="Какие бумаги смотрят" hint="Бумаги во вкладке «По бумаге»: чьи сделки фондов по ней открывали."
            columns={['людей']} items={ft.assets.map((a) => ({ label: a.key, value: a.people }))}
            loading={false} emptyText="Во вкладке «По бумаге» бумаги не выбирали" />
          <TopList title="Какие фонды открывают" hint="Карточки фондов, открытые из «Сделок фондов»." hintAlign="right"
            columns={['людей']} items={fundItems(ft.opened)} loading={false} emptyText="Карточки фондов не открывали" />
        </div>
      </section>

      {/* ── Деньги в фондах ── */}
      <section>
        <Head path="/funds-money" title="Деньги в фондах" people={fm.people} prev={fm.prev_people} tracked={fm.tracked} />
        <Card padding="md" className="md:p-5 mb-3 md:mb-4">
          <PickLine label="Режим" rows={fm.views} names={FUNDS_VIEW} />
          <PickLine label="Период" rows={fm.periods} names={FUNDS_PERIOD} />
          <PickLine label="Шаг" rows={fm.timeframes} names={FUNDS_TF} />
          {fm.views.length === 0 && (
            <p className="text-sm" style={{ color: 'var(--text-muted)' }}>Настроек за период не записано.</p>
          )}
        </Card>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
          <TopList title="Какие категории" columns={['людей']} items={named(fm.categories, FUND_CATEGORY)}
            loading={false} emptyText="Категории не выбирали" />
          <TopList
            title={fm.narrowed ? `Фонды, которые оставляют · ${fm.narrowed} чел.` : 'Фонды, которые оставляют'}
            hint={HINTS.funds_picked} hintAlign="right" columns={['людей']}
            items={fundItems(fm.funds)} loading={false} emptyText="Все смотрят категорию целиком" />
        </div>
      </section>
    </div>
  );
}
