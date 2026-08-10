/**
 * SandboxPage — приватная «песочница/конструктор» (роут /sandbox, вне навигации).
 *
 * Про-версия: пользователь вытаскивает индикаторы как плавающие окна-панели,
 * раскладывает по ЛИСТАМ (рабочим пространствам), двигает/масштабирует с магнитом.
 * Аналог рабочего стола терминала (TradingView / Т-Инвестиции), но со своими
 * индикаторами и в фирменном editorial-дизайне.
 *
 * ЭТО ПОРТ оболочки дизайнерского мокапа 1:1 (спека ~/Downloads/Песочница-Фрейм-
 * спецификация.md; исходник декодирован в scratchpad). Модель §2, оболочка §3,
 * окно-панель §4. Токены песочницы — СВОИ (sandbox.css, терминальный язык:
 * тонкий бордер + мягкая тень), НЕ сайтовые editorial (жёсткие кремовые бордеры).
 *
 * КОНТЕНТ панели = наш реальный embed-компонент (реальные данные через сессию
 * юзера, авторизация, тариф, методология — переиспользуем, не мокаем). «Единая
 * шапка» §4.1: у панели нет своего заголовка — шапкой-хватом служит тулбар embed'а,
 * кнопки окна (⤢/×) EmbedFrame дорисовывает через SandboxWindowCtx. Вложенный
 * <Router> НЕЛЬЗЯ (Router-в-Router крашит) — embed рендерится напрямую.
 *
 * Раскладка привязана к листу (bySheet). При смене листа панели неактивного листа
 * ДЕМОНТИРУЮТСЯ React'ом → LwChart внутри embed'а сам зовёт chart.remove() в
 * cleanup (иначе течёт память, §2). Персист в localStorage (v2).
 *
 * TODO (следующие куски порта):
 * ⚙ общий Формат, per-panel cfg, клик по сигналу → spawn индикатора, общие
 * настройки §9, листы (переименование/дубль/удаление/reorder).
 */
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import {
  Activity, ArrowLeftRight, Bell, CalendarDays, Grid3x3, Layers, LayoutGrid,
  ListFilter, LogOut, Plus, PlusCircle, Scale, SlidersHorizontal, TrendingUp, Wallet, Waves, X as XIcon,
  type LucideIcon,
} from 'lucide-react';
import './sandbox.css';
import ThemeGlyph from '../../components/ThemeGlyph';
import { SandboxWindowCtx } from '../embed/EmbedToolbar';
import { EmbedPidCtx } from '../embed/embedPersist';
import { ChartPrefsCtx, type ChartPrefs } from '../../components/chart/lwTypes';
import FrameLogo from '../../components/FrameLogo';
import { ThemeContext, useTheme } from '../../contexts/ThemeContext';
import { useAuth } from '../../contexts/AuthContext';
import { getAnomalyFeed, type AnomalyDeepLink } from '../../services/api';
import EmbedOpenInterest from '../embed/EmbedOpenInterest';
import EmbedSeasonality from '../embed/EmbedSeasonality';
import EmbedScreener from '../embed/EmbedScreener';
import EmbedBuffett from '../embed/EmbedBuffett';
import EmbedStrength from '../embed/EmbedStrength';
import EmbedFundsMoney from '../embed/EmbedFundsMoney';
import EmbedFundTrades from '../embed/EmbedFundTrades';
import EmbedCbrFlows from '../embed/EmbedCbrFlows';
import EmbedHeatmap from '../embed/EmbedHeatmap';
import EmbedSignals from '../embed/EmbedSignals';

/* ───────────────────────── реестр индикаторов ───────────────────────── */
type IndKind =
  | 'signals' | 'oi' | 'seasonality' | 'screener'
  | 'buffett' | 'strength' | 'funds-money' | 'fund-trades' | 'fund-movers' | 'cbr-flows' | 'heatmap';
type Group = 'signals' | 'instrument' | 'market';
interface IndicatorDef { type: IndKind; label: string; group: Group; desc: string }

const INDICATORS: IndicatorDef[] = [
  { type: 'signals', label: 'Сигналы', group: 'signals', desc: 'Лента резких движений рынка' },
  { type: 'oi', label: 'Открытые позиции', group: 'instrument', desc: 'Открытый интерес по фьючерсам' },
  { type: 'seasonality', label: 'Сезонность', group: 'instrument', desc: 'Средняя доходность по сезонам' },
  { type: 'screener', label: 'Скринер сигналов', group: 'instrument', desc: 'Резкие сдвиги позиций по всем активам' },
  { type: 'buffett', label: 'Индикатор Баффетта', group: 'market', desc: 'Капитализация рынка к ВВП / M2' },
  { type: 'strength', label: 'Сила рынка', group: 'market', desc: 'Ширина рынка: % акций выше средней' },
  { type: 'funds-money', label: 'Деньги в фондах', group: 'market', desc: 'Притоки и оттоки биржевых фондов' },
  // «Сделки фондов» (fund-movers) убран из выбора — это таб «Сделки» внутри
  // «Сделки фондов» (case ниже оставлен ради уже сохранённых панелей).
  { type: 'fund-trades', label: 'Сделки фондов', group: 'market', desc: 'Что покупают и продают фонды' },
  { type: 'cbr-flows', label: 'Поток капитала', group: 'market', desc: 'Потоки по участникам биржи (ЦБ)' },
  { type: 'heatmap', label: 'Карта рынка', group: 'market', desc: 'Тепловая карта: размер = оборот, цвет = изменение' },
];

// Иконка-плитка пункта меню (§7.1 макета): глиф + подложка в цвет серии.
const ICONS: Record<IndKind, { Icon: LucideIcon; color: string }> = {
  signals: { Icon: Bell, color: 'var(--accent)' },
  oi: { Icon: Layers, color: 'var(--c-cyan)' },
  seasonality: { Icon: CalendarDays, color: 'var(--c-up)' },
  screener: { Icon: ListFilter, color: 'var(--c-amber)' },
  buffett: { Icon: Scale, color: 'var(--c-price)' },
  strength: { Icon: Activity, color: 'var(--c-sec)' },
  'funds-money': { Icon: Wallet, color: 'var(--c-up)' },
  'fund-trades': { Icon: ArrowLeftRight, color: 'var(--c-price)' },
  'fund-movers': { Icon: TrendingUp, color: 'var(--c-amber)' },
  'cbr-flows': { Icon: Waves, color: 'var(--c-sec)' },
  heatmap: { Icon: Grid3x3, color: 'var(--c-down)' },
};

// Стартовый размер панели по индикатору (спека: screener 440×470, strength 560×420, прочие 520×360).
const SIZES: Partial<Record<IndKind, { w: number; h: number }>> = {
  signals: { w: 380, h: 560 },
  // Скринер стал сайтовой таблицей (актив·комета·сила·сигнал·★) — прежним
  // 440 не хватало на все колонки, звезда уезжала за край. 560 даёт всем
  // колонкам расправиться сразу при добавлении, без ручного растягивания.
  screener: { w: 560, h: 520 },
  strength: { w: 560, h: 420 },
  oi: { w: 640, h: 440 },
  seasonality: { w: 600, h: 440 },
  // Треймап читается лучше в landscape (лейблы горизонтальные, секторные
  // колонки нужны вширь) — шире и чуть ниже, чем было (470×420).
  heatmap: { w: 640, h: 380 },
  // StackedBidirectionalHistogram: fixed CSS chart-padding (100/95px, не
  // container-aware) съедает почти всю ширину MINW=300 — X-метки дат
  // налезают друг на друга. Легенда/число меток внутри уже адаптивны под
  // РЕАЛЬНУЮ ширину панели, но паддинг — нет; ширина пошире держит дефолт
  // подальше от этого предела (пользователь всё ещё может ужать вручную).
  'cbr-flows': { w: 620, h: 380 },
};
const DEFAULT_SIZE = { w: 560, h: 400 };   // чуть больше — тулбар не так забит на старте

/** Стартовые настройки панели (из deep_link сигнала). Дальше панель живёт сама. */
interface PanelCfg { instrument?: string; category?: string }

// Маршрут сигнала → тип индикатора (§6.10: клик открывает панель на активе).
const ROUTE_TO_KIND: Record<string, IndKind> = {
  '/oi': 'oi',
  '/seasonality': 'seasonality',
  '/screener': 'screener',
  '/buffett': 'buffett',
  '/strength': 'strength',
  '/funds-money': 'funds-money',
  '/fund-trades': 'fund-trades',
  '/cbr-flows': 'cbr-flows',
  '/heatmap': 'heatmap',
};

// type → embed-компонент (наш реальный индикатор).
function renderIndicator(type: IndKind, cfg: PanelCfg | undefined, onSignal: (dl: AnomalyDeepLink) => void): ReactNode {
  switch (type) {
    case 'signals': return <EmbedSignals onPick={onSignal} />;
    case 'oi': return <EmbedOpenInterest initialInstrument={cfg?.instrument} />;
    case 'seasonality': return <EmbedSeasonality initialInstrument={cfg?.instrument} />;
    case 'screener': return <EmbedScreener onPick={(r) => onSignal({ route: '/oi', secid: r.sectype })} />;
    case 'buffett': return <EmbedBuffett />;
    case 'strength': return <EmbedStrength />;
    case 'funds-money': return <EmbedFundsMoney initialCategory={cfg?.category} />;
    case 'fund-trades': return <EmbedFundTrades />;
    case 'fund-movers': return <EmbedFundTrades lockTab="movers" />;
    case 'cbr-flows': return <EmbedCbrFlows />;
    case 'heatmap': return <EmbedHeatmap />;
    default: return null;
  }
}

/* ───────────────────────── модель состояния (§2) ───────────────────────── */
type SbTheme = 'dark' | 'light';
interface Sheet { id: string; name: string }
interface Panel { id: string; type: IndKind; x: number; y: number; w: number; h: number; z: number; themeOverride: SbTheme | null; cfg?: PanelCfg }
/** Общие настройки песочницы (§9). Персистятся вместе с раскладкой. */
interface SbPrefs {
  snap: boolean; snapTh: number; grid: 'dots' | 'lines' | 'clean'; arrangeCols: 0 | 2 | 3 | 4;
  // Дефолты графиков (прокидываются во ВСЕ панели через ChartPrefsCtx):
  lineW: 1 | 2 | 3; crosshair: boolean; chartGrid: boolean; lastValue: boolean;
}
const DEF_PREFS: SbPrefs = {
  snap: true, snapTh: 18, grid: 'dots', arrangeCols: 0,
  lineW: 2, crosshair: true, chartGrid: true, lastValue: true,
};

interface Persisted { sbTheme: SbTheme; sheets: Sheet[]; activeSheet: string; bySheet: Record<string, Panel[]>; prefs?: SbPrefs }
interface Guide { axis: 'v' | 'h'; pos: number }

const LS_KEY = 'frame:sandbox:v2';
const TOPBAR_H = 56;
const DRAG_STRIP = 44;       // «шапка-хват» — верхняя полоса панели (= тулбар embed'а)
const MINW = 300, MINH = 200;
// Пер-индикаторный пол ширины — для ОИ глобальных 300px не хватает: тулбар
// (грип + ассет + 4 compact-иконки контролов + рисование + экспорт + ⚙ + ⤢ + ×)
// уже не скроллится горизонтально (toolbarUnified в EmbedFrame, PR #805) — при
// недостатке места контент молча обрежется overflow:hidden (см. toolbarWrapRef
// в EmbedOpenInterest), а не уедет в скролл. 340 (прошлый коммит) оказалось
// МАЛО — не учитывал 5 промежутков gap:8 у toolbarRow, реальную ширину
// ассет-кнопки и полный набор иконок (рисование/экспорт не рендерятся без
// данных — недомерил при первом замере). Пересчитано по исходникам всех
// компонентов тулбара с запасом.
// seasonality/funds-money/strength получили тот же toolbarUnified+compact-icon
// паттерн, что и ОИ (см. useToolbarCompact.ts) — те же 300px глобальных не
// хватает, когда все контролы тулбара одновременно на месте. buffett — самый
// лёгкий тулбар (1 Dropdown + 1 короткий PillGroup без иконок) — укладывается
// в 300 с запасом, отдельная запись не нужна.
// Минимальная ширина панели по типу индикатора. ⚠️ Это ПЕРВАЯ линия защиты от
// наложения кнопок тулбара: слева контролы индикатора, справа — хром окна
// (рисование/экспорт/⚙/развернуть/тема/закрыть), и когда места не хватает,
// они наезжают друг на друга. Добавили контрол — проверь и подними число.
// Вторая линия — overflow:hidden на обёртке тулбара (обрежет, а не наедет).
const MINW_BY_TYPE: Partial<Record<IndKind, number>> = {
  // На глобальном MINW=300 компакт-тулбар «Сигналов» терял 2 таба из 4;
  // от 340 весь ряд (4 иконки + ★) влезает (замер на проде 2026-08-07).
  signals: 340,
  oi: 540,
  // +«Периоды» к типу графика и разрезу — прежних 380 не хватает.
  seasonality: 470,
  // +период к таймфрейму и категории — прежних 420 уже не хватало;
  // +«Фонды» (фильтр состава) — ещё одна кнопка в том же ряду.
  'funds-money': 580,
  strength: 440,
  // Компактная сетка скринера (минимумы колонок ~415) + иконки-компакт в
  // тулбаре позволяют ужимать сильнее прежних 520. Ниже 460 в тулбаре
  // обрезается кнопка «★ Избранные» (замер на проде 2026-08-07).
  screener: 460,
  heatmap: 320,
  buffett: 380,
  // Ряд табов «Сделки·Снапшот·Состав·Потоки»: на 460 «Потоки» ещё подрезан,
  // от 490 влезает (замер на проде 2026-08-07). fund-movers без табов — ему
  // хватает глобальных 300.
  'fund-trades': 490,
  // +«Участники» к типу и периоду (фильтр переехал из ⚙ на панель) — 480 уже мало.
  'cbr-flows': 560,
};
const GUIDE_Z = 99990;       // направляющие — над панелями, под оверлеями
const OVERLAY_Z = 100000;    // меню/поповеры/дровер (§7: панели при захвате уходят на высокий z)

const uid = (p: string): string => p + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

/**
 * SandboxThemeScope — вложенный ThemeContext.Provider с ЭФФЕКТИВНОЙ темой панели
 * (themeOverride панели или тема оболочки). Embed внутри читает её через
 * useTheme() → LwChart получает верный `dark`, series-цвета резолвятся probe'ом
 * в поддереве панели (CSS-vars от data-theme на .sb-panel). §4.3 спеки.
 */
function SandboxThemeScope({ eff, children }: { eff: SbTheme; children: ReactNode }) {
  const parent = useTheme();
  const value = useMemo(() => ({
    ...parent,
    theme: (eff === 'light' ? 'editorial-light' : 'editorial-dark') as typeof parent.theme,
  }), [parent, eff]);
  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

/** Тема сайта на момент ПЕРВОГО входа в терминал — чтобы переход не выглядел
 *  сменой продукта. Дальше у терминала своя тема (переключатель в его шапке),
 *  она живёт в LS_KEY и сайтом уже не перетирается.
 *  Ключ 'theme' пишет ThemeProvider; дефолт сайта — editorial-light. */
function siteTheme(): SbTheme {
  try {
    return localStorage.getItem('theme') === 'editorial-dark' ? 'dark' : 'light';
  } catch {
    return 'light';   // localStorage недоступен (private mode) — как на сайте
  }
}

function defaultState(): Persisted {
  return {
    sbTheme: siteTheme(),
    sheets: [{ id: 's1', name: 'Лист 1' }],
    activeSheet: 's1',
    bySheet: { s1: [{ id: 'pseed', type: 'oi', x: 28, y: 20, w: 640, h: 440, z: 11, themeOverride: null }] },
  };
}
function loadState(): Persisted {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return defaultState();
    const s = JSON.parse(raw) as Persisted;
    if (!s || !Array.isArray(s.sheets) || !s.sheets.length || !s.bySheet) return defaultState();
    if (!s.bySheet[s.activeSheet]) s.activeSheet = s.sheets[0].id;
    if (s.sbTheme !== 'light') s.sbTheme = 'dark';
    s.prefs = { ...DEF_PREFS, ...(s.prefs ?? {}) };
    // Одноразовая миграция: кнопку «Тема панели» (◐) из тулбара окна убрали —
    // тему задаёт только шапка оболочки. У тех, кто успел её нажать, панель
    // осталась бы в чужой теме НАВСЕГДА: поставить оверрайд больше нечем, и
    // снять его — тоже (единственный сброс живёт в toggleTheme, до которого
    // ещё надо додуматься). Обнуляем оверрайды на загрузке. Само поле в модели
    // и персисте оставлено: механика per-panel темы цела и ждёт своего UI.
    for (const arr of Object.values(s.bySheet)) {
      for (const p of arr || []) p.themeOverride = null;
    }
    return s;
  } catch { return defaultState(); }
}

/* ───────────────────────── ресайз-хваты (§4.2) ─────────────────────────
   Стороны + нижние углы, z:2 — выше графика, ниже тулбара embed'а (z:20 в
   EmbedFrame): хваты ловят край над графиком, кнопки кликаются.
   Верхний хват (n) — ОСОБЫЙ: полоса тулбара занята кнопками (z:20), поэтому
   n-хват держим z:21 (ВЫШЕ тулбара) — иначе клики на него не долетают
   вообще, весь прямоугольник тулбара их перехватывает первым. Чтобы при
   этом не перекрыть сами кнопки, полоса тонкая (6px) и лежит строго в
   верхнем padding тулбара (padding-top:5px + доп. отступ до глифов кнопок,
   ~12px запаса) — реальные иконки ниже этой полосы, клики по ним не задеты.
   Верхние углы (nw/ne) НЕ добавлены: там 14×14 неизбежно наехал бы на грип-
   хват/ассет-кнопку (nw) или кнопки окна ⤢× (ne) — тот же конфликт, но
   уже не решаемый утончением полосы. */
const HANDLES: { dir: string; style: CSSProperties }[] = [
  { dir: 'e', style: { top: 14, bottom: 14, right: 0, width: 6, cursor: 'ew-resize' } },
  { dir: 'w', style: { top: 14, bottom: 14, left: 0, width: 6, cursor: 'ew-resize' } },
  { dir: 's', style: { left: 14, right: 14, bottom: 0, height: 6, cursor: 'ns-resize' } },
  { dir: 'se', style: { right: 0, bottom: 0, width: 14, height: 14, cursor: 'nwse-resize' } },
  { dir: 'sw', style: { left: 0, bottom: 0, width: 14, height: 14, cursor: 'nesw-resize' } },
];
const N_HANDLE: { dir: string; style: CSSProperties } = {
  dir: 'n', style: { left: 14, right: 14, top: 0, height: 6, cursor: 'ns-resize', zIndex: 21 },
};

export default function SandboxPage() {
  const { user } = useAuth();   // реальный юзер для аватара шапки
  const avatarInitial = (user?.display_name?.trim()?.[0] || user?.email?.[0] || '?').toUpperCase();
  const [st, setSt] = useState<Persisted>(loadState);
  const [menuOpen, setMenuOpen] = useState(false);
  const [signalsOpen, setSignalsOpen] = useState(false);
  const [guides, setGuides] = useState<Guide[]>([]);
  const [signalCount, setSignalCount] = useState(0);
  // Операции с листом: контекст-меню и инлайн-переименование (§3.3).
  const [sheetMenu, setSheetMenu] = useState<{ id: string; x: number; y: number } | null>(null);
  const [renaming, setRenaming] = useState<{ id: string; value: string } | null>(null);
  // ⤢ Развернуть — id панели, раскрытой на весь sb-root (не «окно покрупнее»,
  // а полная замена шапки+холста тулбаром индикатора). null — обычный режим.
  const [maximizedId, setMaximizedId] = useState<string | null>(null);
  const zTop = useRef(10);
  const canvasRef = useRef<HTMLDivElement>(null);
  const saveTimer = useRef<number | undefined>(undefined);

  const panels = st.bySheet[st.activeSheet] || [];
  const maximizedPanel = maximizedId ? panels.find((p) => p.id === maximizedId) ?? null : null;
  const prefs = st.prefs ?? DEF_PREFS;
  const setPrefs = useCallback((patch: Partial<SbPrefs>) => {
    setSt((s) => ({ ...s, prefs: { ...DEF_PREFS, ...(s.prefs ?? {}), ...patch } }));
  }, []);
  const [prefsOpen, setPrefsOpen] = useState(false);
  // Дефолты графиков → во все панели (мемо, чтобы не пересоздавать чарты каждый рендер).
  const chartPrefsValue = useMemo<ChartPrefs>(() => ({
    lineWidth: prefs.lineW, crosshair: prefs.crosshair, grid: prefs.chartGrid, lastValue: prefs.lastValue,
    // Песочница — приватный конструктор-терминал (не сайтовая витрина), водяной
    // знак «Фрейм» на графиках здесь ни к чему (панелей и так тесно).
    watermark: false,
  }), [prefs.lineW, prefs.crosshair, prefs.chartGrid, prefs.lastValue]);

  // init zTop из загруженной раскладки
  useEffect(() => {
    let m = 10;
    for (const arr of Object.values(st.bySheet)) for (const p of arr) m = Math.max(m, p.z);
    zTop.current = m;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Счётчик свежих сигналов для бейджа колокола (§3.1). Дровер держит свою ленту.
  useEffect(() => {
    let alive = true;
    const load = () => {
      getAnomalyFeed({ limit: 60, maxAgeHours: 24 })
        .then((f) => { if (alive) setSignalCount(f.items.length); })
        .catch(() => { /* бейдж не критичен */ });
    };
    load();
    const t = window.setInterval(load, 90_000);
    return () => { alive = false; window.clearInterval(t); };
  }, []);

  // полноэкранно, без скролла страницы
  useEffect(() => {
    const de = document.documentElement, b = document.body;
    const prev = { de: de.style.overflow, bo: b.style.overflow, bm: b.style.margin };
    de.style.overflow = 'hidden'; b.style.overflow = 'hidden'; b.style.margin = '0';
    return () => { de.style.overflow = prev.de; b.style.overflow = prev.bo; b.style.margin = prev.bm; };
  }, []);

  // персист (дебаунс 400мс — драг/ресайз дёргают state часто)
  useEffect(() => {
    window.clearTimeout(saveTimer.current);
    saveTimer.current = window.setTimeout(() => {
      try { localStorage.setItem(LS_KEY, JSON.stringify(st)); } catch { /* quota */ }
    }, 400);
    return () => window.clearTimeout(saveTimer.current);
  }, [st]);

  // обновить массив панелей АКТИВНОГО листа
  const setActivePanels = useCallback((updater: (prev: Panel[]) => Panel[]) => {
    setSt((s) => {
      const prev = s.bySheet[s.activeSheet] || [];
      const next = updater(prev);
      // Апдейтер вернул ту же ссылку → состояние не менялось; отдаём ТО ЖЕ s,
      // и React пропускает рендер. Раньше новый объект собирался безусловно,
      // из-за чего no-op из bringFront всё равно перерисовывал весь лист.
      if (next === prev) return s;
      return { ...s, bySheet: { ...s.bySheet, [s.activeSheet]: next } };
    });
  }, []);

  const bringFront = useCallback((id: string) => {
    setActivePanels((ps) => {
      // Панель уже сверху → НИЧЕГО не делаем. bringFront зовётся с pointerdown
      // ЛЮБОГО клика внутри панели (не только за шапку), и безусловный setState
      // перерисовывал весь лист на каждый клик по графику — кроссхэйр вздрагивал.
      // На развёрнутом окне этот путь не срабатывает — потому там и не дёргалось.
      // Возврат ТОЙ ЖЕ ссылки — React пропускает рендер целиком.
      const cur = ps.find((p) => p.id === id);
      if (!cur || ps.every((p) => p.id === id || p.z < cur.z)) return ps;
      zTop.current += 1; const z = zTop.current;
      return ps.map((p) => (p.id === id ? { ...p, z } : p));
    });
  }, [setActivePanels]);

  const spawn = useCallback((type: IndKind, cfg?: PanelCfg) => {
    const sz = SIZES[type] || DEFAULT_SIZE;
    const rc = canvasRef.current?.getBoundingClientRect();
    const cw = rc?.width ?? window.innerWidth, ch = rc?.height ?? (window.innerHeight - TOPBAR_H);
    zTop.current += 1; const z = zTop.current;
    setActivePanels((ps) => {
      const n = ps.length;
      const x = Math.max(8, Math.min(cw - sz.w - 8, 40 + ((n * 28) % 220)));
      const y = Math.max(8, Math.min(ch - sz.h - 8, 24 + ((n * 28) % 180)));
      return [...ps, { id: uid('p'), type, x, y, w: sz.w, h: sz.h, z, themeOverride: null, ...(cfg ? { cfg } : {}) }];
    });
    setMenuOpen(false);
  }, [setActivePanels]);

  // §6.10: клик по сигналу → спавн панели нужного индикатора на активе сигнала.
  const onSignal = useCallback((dl: AnomalyDeepLink) => {
    const kind = ROUTE_TO_KIND[dl?.route ?? ''];
    if (!kind) return;
    spawn(kind, { instrument: dl.secid, category: dl.category });
    setSignalsOpen(false);
  }, [spawn]);

  const close = useCallback((id: string) => {
    setActivePanels((ps) => ps.filter((p) => p.id !== id));
    setMaximizedId((cur) => (cur === id ? null : cur));
  }, [setActivePanels]);

  // Индикатор просит новый размер под свой контент (напр. Сезонность — под срез/
  // режим, §6.11). Позиция (x,y) не трогаем — только w/h, клампим к MINW/MINH и
  // оставшемуся месту холста от текущего левого верхнего угла панели.
  const resizePanel = useCallback((id: string, reqW: number, reqH: number) => {
    const rc = canvasRef.current?.getBoundingClientRect();
    const cw = rc?.width ?? window.innerWidth, ch = rc?.height ?? (window.innerHeight - TOPBAR_H);
    setActivePanels((ps) => ps.map((p) => {
      if (p.id !== id) return p;
      const minW = MINW_BY_TYPE[p.type] ?? MINW;
      const w = Math.max(minW, Math.min(reqW, cw - p.x - 8));
      const h = Math.max(MINH, Math.min(reqH, ch - p.y - 8));
      return { ...p, w, h };
    }));
  }, [setActivePanels]);

  // ⤢ Развернуть / ⤡ Свернуть (§4.3, переработано) — тоггл полноэкранного
  // оверлея (см. maximizedPanel в JSX), а не ресайз панели до 880×600.
  // Геометрия панели (x/y/w/h) не трогается вообще — просто временно не
  // рендерим обычный холст, рендерим эту одну панель на весь sb-root.
  const toggleMaximize = useCallback((id: string) => {
    setMaximizedId((cur) => (cur === id ? null : id));
  }, []);

  // «Выстроить» §4.4: 1–2 → ряд, 3–6 → 2 колонки, 7+ → 3.
  // Плитки укладываются встык — без полей по краям холста и без зазоров между
  // окнами (Вадим: «свободное пространство между плитками убрать вообще»).
  // Остаток от деления ширины/высоты раздаём последней колонке/строке, иначе
  // накопленное округление вниз оставляло бы полоску холста справа и снизу.
  const arrange = useCallback(() => {
    const rc = canvasRef.current?.getBoundingClientRect(); if (!rc) return;
    setActivePanels((ps) => {
      const n = ps.length; if (!n) return ps;
      const cols = prefs.arrangeCols || (n <= 2 ? n : n <= 6 ? 2 : 3);
      const rows = Math.ceil(n / cols);
      const w = Math.floor(rc.width / cols);
      const h = Math.floor(rc.height / rows);
      return ps.map((p, i) => {
        const c = i % cols, r = Math.floor(i / cols);
        // Последняя плитка в ряду тянется до правого края — так неполный
        // последний ряд (n не кратно cols) тоже не оставляет дыры.
        const lastInRow = c === cols - 1 || i === n - 1;
        return {
          ...p,
          x: c * w,
          y: r * h,
          w: lastInRow ? Math.round(rc.width) - c * w : w,
          h: r === rows - 1 ? Math.round(rc.height) - r * h : h,
        };
      });
    });
  }, [setActivePanels, prefs]);

  // ── драг + магнит(18) + направляющие (§4.1) ──
  const onDragStart = useCallback((e: React.PointerEvent, id: string) => {
    bringFront(id);
    const el = e.currentTarget as HTMLElement;
    const rect = el.getBoundingClientRect();
    if (e.clientY - rect.top > DRAG_STRIP) return; // тянем только за верхнюю полосу
    if ((e.target as HTMLElement).closest('button, input, a, select, textarea, [role="dialog"]')) return;
    e.preventDefault();
    const list = st.bySheet[st.activeSheet] || [];
    const start = list.find((p) => p.id === id); if (!start) return;
    const others = list.filter((p) => p.id !== id);
    const cr = canvasRef.current!.getBoundingClientRect();
    const sx = e.clientX, sy = e.clientY, ox = start.x, oy = start.y;
    const dragTh = prefs.snap ? prefs.snapTh : 0;
    try { el.setPointerCapture(e.pointerId); } catch { /* noop */ }
    const move = (ev: PointerEvent) => {
      let nx = ox + (ev.clientX - sx), ny = oy + (ev.clientY - sy);
      const gs: Guide[] = [];
      const xc = [0, cr.width - start.w]; others.forEach((q) => xc.push(q.x, q.x + q.w - start.w, q.x + q.w, q.x - start.w));
      for (const c of xc) { if (Math.abs(nx - c) < dragTh) { nx = c; gs.push({ axis: 'v', pos: nx }, { axis: 'v', pos: nx + start.w }); break; } }
      const yc = [0, cr.height - start.h]; others.forEach((q) => yc.push(q.y, q.y + q.h - start.h, q.y + q.h, q.y - start.h));
      for (const c of yc) { if (Math.abs(ny - c) < dragTh) { ny = c; gs.push({ axis: 'h', pos: ny }, { axis: 'h', pos: ny + start.h }); break; } }
      nx = Math.max(0, Math.min(cr.width - start.w, nx));
      ny = Math.max(0, Math.min(cr.height - start.h, ny));
      setGuides(gs);
      setActivePanels((ps) => ps.map((p) => (p.id === id ? { ...p, x: nx, y: ny } : p)));
    };
    const up = () => {
      el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up);
      setGuides([]);
    };
    el.addEventListener('pointermove', move); el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
  }, [st, bringFront, setActivePanels]);

  // ── ресайз (§4.2): стороны + нижние углы, магнит(16) кромки к соседям/краям ──
  const onResizeStart = useCallback((e: React.PointerEvent, id: string, dir: string) => {
    e.stopPropagation(); e.preventDefault();
    bringFront(id);
    const list = st.bySheet[st.activeSheet] || [];
    const start = list.find((p) => p.id === id); if (!start) return;
    const others = list.filter((p) => p.id !== id);
    const cr = canvasRef.current!.getBoundingClientRect();
    const sx = e.clientX, sy = e.clientY; const o = { x: start.x, y: start.y, w: start.w, h: start.h };
    const el = e.currentTarget as HTMLElement;
    try { el.setPointerCapture(e.pointerId); } catch { /* noop */ }
    const minW = MINW_BY_TYPE[start.type] ?? MINW;
        const resizeTh = prefs.snap ? Math.max(6, prefs.snapTh - 2) : 0;
const edgesX = others.flatMap((q) => [q.x, q.x + q.w]);
    const edgesY = others.flatMap((q) => [q.y, q.y + q.h]);
    const move = (ev: PointerEvent) => {
      const dx = ev.clientX - sx, dy = ev.clientY - sy;
      let x = o.x, y = o.y, w = o.w, h = o.h; const gs: Guide[] = [];
      if (dir.includes('e')) { w = o.w + dx; const R = x + w; for (const c of [cr.width, ...edgesX]) { if (Math.abs(R - c) < resizeTh) { w = c - x; gs.push({ axis: 'v', pos: c }); break; } } }
      if (dir.includes('s')) { h = o.h + dy; const B = y + h; for (const c of [cr.height, ...edgesY]) { if (Math.abs(B - c) < resizeTh) { h = c - y; gs.push({ axis: 'h', pos: c }); break; } } }
      if (dir.includes('w')) { const R = o.x + o.w; let nx = o.x + dx; for (const c of [0, ...edgesX]) { if (Math.abs(nx - c) < resizeTh) { nx = c; gs.push({ axis: 'v', pos: c }); break; } } x = nx; w = R - nx; }
      if (dir.includes('n')) { const B = o.y + o.h; let ny = o.y + dy; for (const c of [0, ...edgesY]) { if (Math.abs(ny - c) < resizeTh) { ny = c; gs.push({ axis: 'h', pos: c }); break; } } y = ny; h = B - ny; }
      if (w < minW) { if (dir.includes('w')) x = o.x + o.w - minW; w = minW; }
      if (h < MINH) { if (dir.includes('n')) y = o.y + o.h - MINH; h = MINH; }
      if (x < 0) { w += x; x = 0; } if (y < 0) { h += y; y = 0; }
      if (x + w > cr.width) w = cr.width - x; if (y + h > cr.height) h = cr.height - y;
      w = Math.max(minW, w); h = Math.max(MINH, h);
      setGuides(gs);
      setActivePanels((ps) => ps.map((p) => (p.id === id ? { ...p, x, y, w, h } : p)));
    };
    const up = () => {
      el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up);
      setGuides([]);
    };
    el.addEventListener('pointermove', move); el.addEventListener('pointerup', up); el.addEventListener('pointercancel', up);
  }, [st, bringFront, setActivePanels]);

  // Тема ОБОЛОЧКИ ведёт за собой ВСЕ окна — на всех листах. Сброс themeOverride
  // здесь оставлен НАРОЧНО, хотя ставить оверрайд из UI больше нечем (кнопку
  // «Тема панели» убрали): это страховка на случай раскладки, приехавшей из
  // старого персиста в обход миграции в loadState.
  const toggleTheme = useCallback(() => setSt((s) => {
    const bySheet = Object.fromEntries(
      Object.entries(s.bySheet).map(([sheet, ps]) => [
        sheet,
        (ps || []).map((p) => (p.themeOverride ? { ...p, themeOverride: null } : p)),
      ]),
    );
    return { ...s, sbTheme: s.sbTheme === 'dark' ? 'light' : 'dark', bySheet };
  }), []);
  const addSheet = useCallback(() => setSt((s) => {
    const id = uid('s');
    return { ...s, sheets: [...s.sheets, { id, name: 'Лист ' + (s.sheets.length + 1) }], bySheet: { ...s.bySheet, [id]: [] }, activeSheet: id };
  }), []);
  const pickSheet = useCallback((id: string) => { setGuides([]); setMenuOpen(false); setMaximizedId(null); setSt((s) => ({ ...s, activeSheet: id })); }, []);

  // ── Операции с листами (§3.3) ──
  const renameSheet = useCallback((id: string, name: string) => {
    const clean = name.trim();
    if (!clean) return;
    setSt((s) => ({ ...s, sheets: s.sheets.map((sh) => (sh.id === id ? { ...sh, name: clean } : sh)) }));
  }, []);

  const duplicateSheet = useCallback((id: string) => {
    setSt((s) => {
      const src = s.sheets.find((sh) => sh.id === id);
      if (!src) return s;
      const nid = uid('s');
      // Клон панелей с НОВЫМИ id — иначе React смешает состояние двух листов.
      const clones = (s.bySheet[id] || []).map((p) => ({ ...p, id: uid('p') }));
      const at = s.sheets.findIndex((sh) => sh.id === id) + 1;
      const sheets = [...s.sheets];
      sheets.splice(at, 0, { id: nid, name: `${src.name} (копия)` });
      return { ...s, sheets, bySheet: { ...s.bySheet, [nid]: clones }, activeSheet: nid };
    });
  }, []);

  // Drag-reorder вкладок (§3.3): нативный HTML5 DnD — перетащенный лист
  // вставляется на позицию цели, сдвигая остальных.
  const reorderSheets = useCallback((srcId: string, dstId: string) => {
    if (srcId === dstId) return;
    setSt((s) => {
      const from = s.sheets.findIndex((x) => x.id === srcId);
      const to = s.sheets.findIndex((x) => x.id === dstId);
      if (from < 0 || to < 0) return s;
      const sheets = [...s.sheets];
      const [moved] = sheets.splice(from, 1);
      sheets.splice(to, 0, moved);
      return { ...s, sheets };
    });
  }, []);

  const deleteSheet = useCallback((id: string) => {
    setGuides([]);
    setSt((s) => {
      if (s.sheets.length <= 1) return s; // последний лист не удаляем
      const idx = s.sheets.findIndex((sh) => sh.id === id);
      const sheets = s.sheets.filter((sh) => sh.id !== id);
      const bySheet = { ...s.bySheet };
      delete bySheet[id];
      const active = s.activeSheet === id ? sheets[Math.max(0, idx - 1)].id : s.activeSheet;
      return { ...s, sheets, bySheet, activeSheet: active };
    });
  }, []);

  return (
    <ChartPrefsCtx.Provider value={chartPrefsValue}>
    <div className="sb-root" data-sbtheme={st.sbTheme} style={rootStyle}>
      {maximizedPanel ? (
        // ⤢ Развернуть — полный оверлей на весь sb-root (не «окно покрупнее»):
        // шапка песочницы и остальные панели не рендерятся вообще, вместо шапки —
        // собственный тулбар индикатора (ассет/таймфрейм/… + кнопка «Свернуть» на
        // месте «Развернуть», см. SandboxWindowCtx.maximized в EmbedToolbar).
        // data-sbtheme/data-theme ОБЯЗАТЕЛЬНЫ и здесь: без них CSS-переменные
        // наследовались бы от .sb-root, а SandboxThemeScope обновляет только
        // React-контекст (LwChart.dark) — фон/токены разъехались бы с графиком.
        // У обычной панели эти атрибуты есть (см. ниже), у развёрнутой их забыли.
        <div
          className="sb-max"
          data-sbtheme={maximizedPanel.themeOverride || st.sbTheme}
          data-theme={(maximizedPanel.themeOverride || st.sbTheme) === 'light' ? 'editorial-light' : 'editorial-dark'}
          style={{ position: 'absolute', inset: 0, background: 'var(--bg)' }}
        >
          <SandboxWindowCtx.Provider
            value={{
              onExpand: () => toggleMaximize(maximizedPanel.id),
              maximized: true,
              onClose: () => close(maximizedPanel.id),
              onResize: (w, h) => resizePanel(maximizedPanel.id, w, h),
            }}
          >
            <EmbedPidCtx.Provider value={maximizedPanel.id}>
              <SandboxThemeScope eff={maximizedPanel.themeOverride || st.sbTheme}>
                {renderIndicator(maximizedPanel.type, maximizedPanel.cfg, onSignal)}
              </SandboxThemeScope>
            </EmbedPidCtx.Provider>
          </SandboxWindowCtx.Provider>
        </div>
      ) : (
      <>
      {/* ── Топбар 56px (§3.1) ── */}
      <div style={topbarStyle}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
          {/* Бренд-марк как в шапке сайта: глиф + wordmark «FRAME» (латиница, акцент).
              Клик по логотипу — выход на главный сайт. */}
          <a href="/" title="На главный сайт" style={{ display: 'inline-flex', alignItems: 'center', textDecoration: 'none' }}>
            <FrameLogo size={22} color="var(--accent)" showWordmark />
          </a>
        </div>

        <div style={dividerV} />

        {/* Вкладки листов + новый лист */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 2, flexShrink: 0 }}>
          {st.sheets.map((sh) => {
            const on = sh.id === st.activeSheet;
            if (renaming?.id === sh.id) {
              return (
                <input
                  key={sh.id}
                  autoFocus
                  value={renaming.value}
                  onChange={(e) => setRenaming({ id: sh.id, value: e.target.value })}
                  onBlur={() => { renameSheet(sh.id, renaming.value); setRenaming(null); }}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') { renameSheet(sh.id, renaming.value); setRenaming(null); }
                    if (e.key === 'Escape') setRenaming(null);
                  }}
                  style={sheetInputStyle}
                />
              );
            }
            return (
              <div
                key={sh.id}
                role="button"
                tabIndex={0}
                draggable
                onDragStart={(e) => { e.dataTransfer.setData('text/frame-sheet', sh.id); e.dataTransfer.effectAllowed = 'move'; }}
                onDragOver={(e) => { if (e.dataTransfer.types.includes('text/frame-sheet')) e.preventDefault(); }}
                onDrop={(e) => { e.preventDefault(); const src = e.dataTransfer.getData('text/frame-sheet'); if (src) reorderSheets(src, sh.id); }}
                onClick={() => pickSheet(sh.id)}
                onDoubleClick={() => setRenaming({ id: sh.id, value: sh.name })}
                onContextMenu={(e) => { e.preventDefault(); pickSheet(sh.id); setSheetMenu({ id: sh.id, x: e.clientX, y: e.clientY }); }}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); pickSheet(sh.id); } }}
                title="Двойной клик — переименовать · правый клик — меню · тянуть — поменять порядок"
                style={{ ...sheetTabStyle(on), display: 'inline-flex', alignItems: 'center', gap: 4 }}
              >
                {sh.name}
                {on && <span style={sheetUnderline} />}
                {/* Крестик закрытия — только когда листов больше одного (последний
                    лист нельзя удалить, см. deleteSheet). Быстрее правого клика → меню. */}
                {st.sheets.length > 1 && (
                  <span
                    role="button"
                    tabIndex={-1}
                    className="sb-winbtn-close"
                    onClick={(e) => { e.stopPropagation(); deleteSheet(sh.id); }}
                    title="Закрыть лист"
                    style={sheetCloseStyle}
                  >
                    <XIcon size={11} />
                  </span>
                )}
              </div>
            );
          })}
          <button type="button" onClick={addSheet} title="Новый лист" style={newSheetBtn}><Plus size={15} /></button>
        </div>

        <div style={dividerV} />

        {/* ＋ Индикатор */}
        <div style={{ position: 'relative', flexShrink: 0 }}>
          <button type="button" onClick={() => setMenuOpen((v) => !v)} style={addBtnStyle}>
            <Plus size={15} strokeWidth={2.4} /> Индикатор
          </button>
        </div>

        <button type="button" onClick={arrange} disabled={!panels.length} title="Разложить по сетке" style={{ ...arrangeBtnStyle, opacity: panels.length ? 1 : 0.4 }}>
          <LayoutGrid size={14} /> Выстроить
        </button>

        {/* Правая группа. Ряд выровнен ОПТИЧЕСКИ, а не только по gap: у всех
            элементов одна коробка 34×34/radius 8 (включая аватар, см.
            avatarStyle), глифы одного кегля 16 и одного веса обводки 1.8 (у
            ThemeGlyph свои 1.75 — разницы на глаз нет), бейдж колокола не
            вылезает вбок (badgeStyle). Меняешь один элемент — проверь эти
            четыре условия, иначе зазоры снова «поплывут». */}
        <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
          <div style={{ position: 'relative', flexShrink: 0 }}>
            <button type="button" className="sb-hoverable" onClick={() => setSignalsOpen(true)} title="Центр сигналов" style={chromeBtn}>
              <Bell size={16} strokeWidth={1.8} />
            </button>
            {signalCount > 0 && (
              <span className="sb-mono" style={badgeStyle}>{signalCount > 99 ? '99+' : signalCount}</span>
            )}
          </div>
          <button type="button" className="sb-hoverable" onClick={toggleTheme} title="Тема оболочки" style={chromeBtn}>
            <ThemeGlyph dark={st.sbTheme === 'dark'} size={16} />
          </button>
          <button type="button" className="sb-hoverable" onClick={() => setPrefsOpen(true)} title="Настройки песочницы" style={chromeBtn}>
            <SlidersHorizontal size={16} strokeWidth={1.8} />
          </button>
          {/* Реальный аватар юзера: картинка (avatar_url) или инициал. PRO-чип убран (Вадим). */}
          {user?.avatar_url
            ? <img src={user.avatar_url} alt="" title={user?.display_name || user?.email || ''} style={{ ...avatarStyle, objectFit: 'cover' }} />
            : <div style={avatarStyle} title={user?.display_name || user?.email || ''}>{avatarInitial}</div>}
          {/* Выход на главный сайт (входа в песочницу с сайта пока нет — по решению Вадима). */}
          <a href="/" className="sb-hoverable" title="Выйти на сайт" aria-label="Выйти на сайт" style={{ ...chromeBtn, textDecoration: 'none', color: 'var(--muted)' }}>
            <LogOut size={16} strokeWidth={1.8} />
          </a>
        </div>
      </div>

      {/* ── Холст (§3.2) ── */}
      <div ref={canvasRef} style={canvasStyle}>
        <div style={gridBgStyle(prefs.grid)} />

        {panels.length === 0 && (
          <div style={emptyStyle}>
            <div style={{ width: 64, height: 64, borderRadius: 14, border: '1.5px solid var(--border-strong)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--muted)' }}>
              <LayoutGrid size={26} />
            </div>
            <div style={{ fontSize: 19, fontWeight: 700, color: 'var(--text)' }}>Пустой лист</div>
            <div style={{ fontSize: 13.5, color: 'var(--muted)', maxWidth: 340, textAlign: 'center', lineHeight: 1.5 }}>
              Добавьте индикатор — он появится как окно-панель. Панели можно двигать, тянуть за края и прилеплять друг к другу.
            </div>
            <button type="button" onClick={() => setMenuOpen(true)} style={{ ...addBtnStyle, padding: '11px 18px', fontSize: 13.5 }}>
              <PlusCircle size={15} /> Добавить индикатор
            </button>
          </div>
        )}

        {panels.map((p) => {
          const eff = p.themeOverride || st.sbTheme;
          return (
          <div
            key={p.id}
            className="sb-panel"
            data-sbtheme={eff}
            data-theme={eff === 'light' ? 'editorial-light' : 'editorial-dark'}
            style={{ position: 'absolute', left: p.x, top: p.y, width: p.w, height: p.h, zIndex: p.z, ...panelStyle }}
            onPointerDown={(e) => onDragStart(e, p.id)}
          >
            <div style={panelBodyStyle}>
              <SandboxWindowCtx.Provider value={{ onExpand: () => toggleMaximize(p.id), maximized: false, onClose: () => close(p.id), onResize: (w, h) => resizePanel(p.id, w, h) }}>
                {/* EmbedPidCtx: настройки embed'а неймспейсятся по id панели —
                    две панели одного индикатора живут независимо (§2 мокапа). */}
                <EmbedPidCtx.Provider value={p.id}>
                  <SandboxThemeScope eff={eff}>
                    {renderIndicator(p.type, p.cfg, onSignal)}
                  </SandboxThemeScope>
                </EmbedPidCtx.Provider>
              </SandboxWindowCtx.Provider>
            </div>
            {HANDLES.map((hd) => (
              <div key={hd.dir} onPointerDown={(e) => onResizeStart(e, p.id, hd.dir)} style={{ position: 'absolute', zIndex: 2, ...hd.style }} />
            ))}
            <div key={N_HANDLE.dir} onPointerDown={(e) => onResizeStart(e, p.id, N_HANDLE.dir)} style={{ position: 'absolute', ...N_HANDLE.style }} />
          </div>
          );
        })}

        {/* Направляющие магнита (§4.1) */}
        {guides.map((g, i) => (
          <div key={i} style={g.axis === 'v'
            ? { position: 'absolute', left: g.pos, top: 0, bottom: 0, width: 1, background: 'var(--accent)', opacity: 0.7, pointerEvents: 'none', zIndex: GUIDE_Z }
            : { position: 'absolute', top: g.pos, left: 0, right: 0, height: 1, background: 'var(--accent)', opacity: 0.7, pointerEvents: 'none', zIndex: GUIDE_Z }} />
        ))}
      </div>
      </>
      )}

      {/* ── Контекст-меню листа (§3.3) ── */}
      {sheetMenu && (
        <>
          <div style={{ position: 'fixed', inset: 0, zIndex: OVERLAY_Z }} onClick={() => setSheetMenu(null)} onContextMenu={(e) => { e.preventDefault(); setSheetMenu(null); }} />
          <div style={{ ...sheetMenuStyle, left: sheetMenu.x, top: sheetMenu.y }}>
            <button type="button" style={sheetMenuItem} onClick={() => { const sh = st.sheets.find((x) => x.id === sheetMenu.id); if (sh) setRenaming({ id: sh.id, value: sh.name }); setSheetMenu(null); }}>
              Переименовать
            </button>
            <button type="button" style={sheetMenuItem} onClick={() => { duplicateSheet(sheetMenu.id); setSheetMenu(null); }}>
              Дублировать
            </button>
            <button
              type="button"
              disabled={st.sheets.length <= 1}
              style={{ ...sheetMenuItem, color: st.sheets.length <= 1 ? 'var(--muted)' : 'var(--c-down)', cursor: st.sheets.length <= 1 ? 'default' : 'pointer' }}
              onClick={() => { deleteSheet(sheetMenu.id); setSheetMenu(null); }}
            >
              Удалить
            </button>
          </div>
        </>
      )}

      {/* ── Меню «＋ Индикатор» (§7.1) ── */}
      {menuOpen && (
        <>
          <div style={{ position: 'fixed', inset: 0, zIndex: OVERLAY_Z }} onClick={() => setMenuOpen(false)} />
          <div className="sb-scroll" style={addMenuStyle}>
            {(['instrument', 'market'] as Group[]).map((g) => (
              <div key={g}>
                <div className="sb-uc" style={{ padding: '8px 10px 4px' }}>
                  {g === 'instrument' ? 'По инструменту' : 'По рынку'}
                </div>
                {INDICATORS.filter((i) => i.group === g).map((ind) => (
                  <MenuItem key={ind.type} ind={ind} onPick={spawn} />
                ))}
              </div>
            ))}
            {/* «Сигналы» — отдельной строкой внизу за разделителем (§7.1). */}
            <div style={{ borderTop: '1px solid var(--border)', marginTop: 6, paddingTop: 4 }}>
              {INDICATORS.filter((i) => i.group === 'signals').map((ind) => (
                <MenuItem key={ind.type} ind={ind} onPick={spawn} />
              ))}
            </div>
          </div>
        </>
      )}

      {/* ── Настройки песочницы (§9, «Раскладка») — правая шторка ── */}
      {prefsOpen && (
        <>
          <div style={{ position: 'fixed', inset: 0, zIndex: OVERLAY_Z, background: 'rgba(0,0,0,0.28)', animation: 'sb-fade .15s ease' }} onClick={() => setPrefsOpen(false)} />
          <div style={{ ...signalsDrawerStyle, width: 320 }}>
            <div style={{ display: 'flex', alignItems: 'center', padding: '10px 12px', borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
              <span style={{ fontWeight: 700, fontSize: 14, color: 'var(--text)' }}>Настройки песочницы</span>
              <button type="button" className="sb-hoverable" onClick={() => setPrefsOpen(false)} title="Закрыть" style={{ ...chromeBtn, width: 28, height: 28, marginLeft: 'auto', border: 'none' }}>
                <XIcon size={16} />
              </button>
            </div>
            <div className="sb-scroll" style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: 14, display: 'flex', flexDirection: 'column', gap: 18 }}>
              <div>
                <div className="sb-uc" style={{ marginBottom: 8 }}>Магнит панелей</div>
                <PrefPills
                  value={prefs.snap ? String(prefs.snapTh) : 'off'}
                  options={[{ id: 'off', label: 'Выкл' }, { id: '12', label: 'Слабый' }, { id: '18', label: 'Обычный' }, { id: '28', label: 'Сильный' }]}
                  onChange={(v) => (v === 'off' ? setPrefs({ snap: false }) : setPrefs({ snap: true, snapTh: Number(v) }))}
                />
              </div>
              <div>
                <div className="sb-uc" style={{ marginBottom: 8 }}>Подложка холста</div>
                <PrefPills
                  value={prefs.grid}
                  options={[{ id: 'dots', label: 'Точки' }, { id: 'lines', label: 'Линии' }, { id: 'clean', label: 'Чисто' }]}
                  onChange={(v) => setPrefs({ grid: v as SbPrefs['grid'] })}
                />
              </div>
              <div>
                <div className="sb-uc" style={{ marginBottom: 8 }}>«Выстроить» — колонки</div>
                <PrefPills
                  value={String(prefs.arrangeCols)}
                  options={[{ id: '0', label: 'Авто' }, { id: '2', label: '2' }, { id: '3', label: '3' }, { id: '4', label: '4' }]}
                  onChange={(v) => setPrefs({ arrangeCols: Number(v) as SbPrefs['arrangeCols'] })}
                />
              </div>
              <div>
                <div className="sb-uc" style={{ marginBottom: 8 }}>Толщина линий</div>
                <PrefPills
                  value={String(prefs.lineW)}
                  options={[{ id: '1', label: 'Тонкие' }, { id: '2', label: 'Обычные' }, { id: '3', label: 'Толстые' }]}
                  onChange={(v) => setPrefs({ lineW: Number(v) as SbPrefs['lineW'] })}
                />
              </div>
              <div>
                <div className="sb-uc" style={{ marginBottom: 8 }}>Графики</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  <PrefToggle label="Кроссхэйр" checked={prefs.crosshair} onChange={(v) => setPrefs({ crosshair: v })} />
                  <PrefToggle label="Сетка графика" checked={prefs.chartGrid} onChange={(v) => setPrefs({ chartGrid: v })} />
                  <PrefToggle label="Последнее значение на оси" checked={prefs.lastValue} onChange={(v) => setPrefs({ lastValue: v })} />
                </div>
              </div>
              <div style={{ fontSize: 11, color: 'var(--muted)', lineHeight: 1.5, borderTop: '1px solid var(--border)', paddingTop: 10 }}>
                Настройки применяются сразу и запоминаются. Формат и цвет конкретного графика — в ⚙ на самой панели.
              </div>
            </div>
          </div>
        </>
      )}

      {/* ── Центр сигналов — выезжающий справа дровер (§6.10 / §7.3) ── */}
      {signalsOpen && (
        <>
          <div style={{ position: 'fixed', inset: 0, zIndex: OVERLAY_Z, background: 'rgba(0,0,0,0.28)', animation: 'sb-fade .15s ease' }} onClick={() => setSignalsOpen(false)} />
          <div style={signalsDrawerStyle}>
            <div style={{ display: 'flex', alignItems: 'center', padding: '10px 12px', borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
              <span style={{ fontWeight: 700, fontSize: 14, color: 'var(--text)' }}>Сигналы и алерты</span>
              <button type="button" className="sb-hoverable" onClick={() => setSignalsOpen(false)} title="Закрыть" style={{ ...chromeBtn, width: 28, height: 28, marginLeft: 'auto', border: 'none' }}>
                <XIcon size={16} />
              </button>
            </div>
            <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
              <EmbedSignals onPick={onSignal} />
            </div>
          </div>
        </>
      )}

      {/* В развёрнутом окне (⤢) график занимает весь экран, и position:fixed-подпись
          ложилась поверх подписей оси времени («2026» в левом нижнем углу). Своего
          stacking-контекста у .sb-max нет, так что просто прячем подпись. */}
      {!maximizedPanel && <div style={footNote}>песочница · приватный превью</div>}
    </div>
    </ChartPrefsCtx.Provider>
  );
}

/** Пункт меню «＋ Индикатор»: иконка-плитка 30×30 + название + описание (§7.1). */
function MenuItem({ ind, onPick }: { ind: IndicatorDef; onPick: (t: IndKind) => void }) {
  const { Icon, color } = ICONS[ind.type];
  return (
    <button type="button" onClick={() => onPick(ind.type)} style={menuItemStyle}>
      <span
        style={{
          width: 30, height: 30, borderRadius: 7, flex: '0 0 auto', color,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: `color-mix(in srgb, ${color} 14%, transparent)`,
          border: `1px solid color-mix(in srgb, ${color} 34%, transparent)`,
        }}
      >
        <Icon size={15} />
      </span>
      <span style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
        <span style={{ fontWeight: 600, fontSize: 12.5, color: 'var(--text)' }}>{ind.label}</span>
        <span style={{ fontSize: 11, color: 'var(--muted)', lineHeight: 1.3 }}>{ind.desc}</span>
      </span>
    </button>
  );
}

/** Тоггл-переключатель для шторки настроек (§9). */
function PrefToggle({ label, checked, onChange }: { label: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <button
      type="button"
      onClick={() => onChange(!checked)}
      style={{
        display: 'flex', alignItems: 'center', width: '100%', padding: '7px 10px', borderRadius: 8,
        border: '1px solid var(--border)', background: 'transparent', color: 'var(--text)', fontSize: 12.5, cursor: 'pointer',
      }}
    >
      <span style={{ textAlign: 'left' }}>{label}</span>
      <span style={{ marginLeft: 'auto', width: 34, height: 18, borderRadius: 9, background: checked ? 'var(--accent)' : 'var(--border-strong)', position: 'relative', flexShrink: 0, transition: 'background .12s' }}>
        <span style={{ position: 'absolute', top: 2, left: checked ? 18 : 2, width: 14, height: 14, borderRadius: '50%', background: '#fff', transition: 'left .12s' }} />
      </span>
    </button>
  );
}

/** Ряд пилюль-выборов для шторки настроек (§9). */
function PrefPills({ value, options, onChange }: {
  value: string;
  options: { id: string; label: string }[];
  onChange: (v: string) => void;
}) {
  return (
    <div style={{ display: 'flex', gap: 5, flexWrap: 'wrap' }}>
      {options.map((o) => {
        const on = o.id === value;
        return (
          <button
            key={o.id}
            type="button"
            onClick={() => onChange(o.id)}
            style={{
              padding: '5px 11px', borderRadius: 7, fontSize: 12, fontWeight: on ? 700 : 500, cursor: 'pointer',
              border: on ? '1.5px solid var(--accent)' : '1.5px solid var(--border)',
              background: on ? 'var(--accent)' : 'transparent', color: on ? '#fff' : 'var(--text)',
            }}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

/* ───────────────────────────── styles ───────────────────────────── */
const rootStyle: CSSProperties = {
  position: 'fixed', inset: 0, background: 'var(--bg)', color: 'var(--text)', overflow: 'hidden',
};
const topbarStyle: CSSProperties = {
  position: 'absolute', top: 0, left: 0, right: 0, height: TOPBAR_H, display: 'flex', alignItems: 'center', gap: 14,
  padding: '0 16px', borderBottom: '1px solid var(--border)', background: 'var(--panel)', zIndex: 50,
};
const dividerV: CSSProperties = { width: 1, height: 22, background: 'var(--border)', flex: '0 0 auto' };
function sheetTabStyle(on: boolean): CSSProperties {
  return {
    position: 'relative', padding: '6px 12px', borderRadius: 7, border: 'none', background: 'transparent', cursor: 'pointer',
    fontSize: 12.5, fontWeight: on ? 700 : 500, color: on ? 'var(--text)' : 'var(--muted)', whiteSpace: 'nowrap',
  };
}
const sheetUnderline: CSSProperties = { position: 'absolute', bottom: -6, left: 12, right: 12, height: 2, background: 'var(--accent)', borderRadius: 2 };
const sheetCloseStyle: CSSProperties = {
  display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
  width: 16, height: 16, borderRadius: 4, color: 'var(--muted)', cursor: 'pointer', flexShrink: 0,
};
const sheetInputStyle: CSSProperties = {
  padding: '5px 8px', borderRadius: 7, border: '1.5px solid var(--accent)', background: 'var(--bg2)',
  color: 'var(--text)', fontSize: 12.5, fontWeight: 700, width: 130, outline: 'none', flex: '0 0 auto',
};
const sheetMenuStyle: CSSProperties = {
  position: 'fixed', zIndex: OVERLAY_Z + 1, minWidth: 170, padding: 5, display: 'flex', flexDirection: 'column', gap: 1,
  background: 'var(--panel)', border: '1px solid var(--border)', borderRadius: 10, boxShadow: 'var(--shadow)',
  animation: 'sb-pop .14s ease',
};
const sheetMenuItem: CSSProperties = {
  textAlign: 'left', padding: '7px 9px', border: 'none', borderRadius: 6, background: 'transparent',
  color: 'var(--text)', fontSize: 12.5, fontWeight: 600, cursor: 'pointer', whiteSpace: 'nowrap',
};
const newSheetBtn: CSSProperties = {
  width: 26, height: 26, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', borderRadius: 7,
  border: '1px dashed var(--border-strong)', background: 'transparent', color: 'var(--muted)', cursor: 'pointer', flex: '0 0 auto', padding: 0,
};
const addBtnStyle: CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6, padding: '7px 13px', borderRadius: 8, border: 'none',
  background: 'var(--accent)', color: '#fff', fontSize: 12.5, fontWeight: 600, cursor: 'pointer', whiteSpace: 'nowrap',
};
const arrangeBtnStyle: CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6, padding: '7px 12px', borderRadius: 8, cursor: 'pointer',
  border: '1.5px solid var(--border)', background: 'transparent', color: 'var(--text)', fontSize: 12.5, fontWeight: 600, whiteSpace: 'nowrap', flex: '0 0 auto',
};
const chromeBtn: CSSProperties = {
  width: 34, height: 34, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', borderRadius: 8,
  border: '1.5px solid var(--border)', background: 'transparent', color: 'var(--text)', cursor: 'pointer', flex: '0 0 auto', padding: 0,
};
// Бейдж колокола. ⚠️ Вылет ТОЛЬКО ВВЕРХ (right:0): раньше было right:-5, и
// бейдж съедал 5 из 8px зазора до соседней кнопки — при signalCount > 0 ряд
// «поджимался» справа от колокола, а без сигналов был ровным. По вертикали
// вылезать можно свободно: кнопка 34 в полосе 56, сверху ~11px воздуха.
// Габариты ужаты (было 16/4px), чтобы бейдж, потерявший боковой вылет, не
// наползал на сам глиф колокола сильнее прежнего. Кольцо в цвет топбара
// отделяет его от рамки кнопки, на угол которой он теперь заходит целиком.
const badgeStyle: CSSProperties = {
  position: 'absolute', top: -6, right: 0, minWidth: 14, height: 15, padding: '0 3px', borderRadius: 8,
  background: 'var(--accent)', color: '#fff', fontSize: 9.5, fontWeight: 700, lineHeight: '15px',
  textAlign: 'center', pointerEvents: 'none', border: '1.5px solid var(--panel)', boxSizing: 'content-box',
};
// Аватар — ТА ЖЕ коробка, что у chromeBtn (34×34, radius 8, та же рамка), а не
// круг 30×30: при общем gap:8 меньший круг между квадратными кнопками читался
// как «стоит с бо́льшим отступом» — оптический зазор у круга шире физического,
// а недобор 4px по стороне добавлял ещё по 2px с каждой стороны. Единая коробка
// снимает обе поправки разом, ряд становится ровным без ручных отступов.
const avatarStyle: CSSProperties = {
  ...chromeBtn, background: 'var(--bg2)', overflow: 'hidden',
  fontSize: 13, fontWeight: 700, cursor: 'default',
};
const canvasStyle: CSSProperties = { position: 'absolute', top: TOPBAR_H, left: 0, right: 0, bottom: 0, overflow: 'hidden', background: 'var(--bg)' };
// Подложка холста (§3.2, gridStyle дизайнера): точки / линии / чисто.
function gridBgStyle(g: 'dots' | 'lines' | 'clean'): CSSProperties {
  const base: CSSProperties = { position: 'absolute', inset: 0, pointerEvents: 'none' };
  if (g === 'lines') return { ...base, backgroundImage: 'linear-gradient(var(--grid-line) 1px, transparent 1px), linear-gradient(90deg, var(--grid-line) 1px, transparent 1px)', backgroundSize: '32px 32px' };
  if (g === 'clean') return { ...base, display: 'none' };
  return { ...base, backgroundImage: 'radial-gradient(var(--dot) 1.1px, transparent 1.1px)', backgroundSize: '22px 22px' };
}
const emptyStyle: CSSProperties = {
  position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 14,
};
const panelStyle: CSSProperties = {
  display: 'flex', flexDirection: 'column', background: 'var(--panel)', border: '1px solid var(--border)',
  borderRadius: 8, boxShadow: 'var(--shadow)', overflow: 'hidden',
};
const panelBodyStyle: CSSProperties = { flex: '1 1 auto', position: 'relative', minHeight: 0, background: 'var(--bg)' };
const addMenuStyle: CSSProperties = {
  position: 'absolute', top: TOPBAR_H + 4, left: 220, zIndex: OVERLAY_Z + 1, width: 300, maxHeight: 'calc(100vh - 80px)', overflowY: 'auto',
  background: 'var(--panel)', border: '1px solid var(--border)', borderRadius: 12, boxShadow: 'var(--shadow)', padding: 6,
  animation: 'sb-pop .15s ease',
};
const menuItemStyle: CSSProperties = {
  display: 'flex', alignItems: 'center', gap: 10, width: '100%', padding: '9px 10px', border: 'none', borderRadius: 8,
  background: 'transparent', color: 'var(--text)', textAlign: 'left', cursor: 'pointer',
};
const signalsDrawerStyle: CSSProperties = {
  position: 'fixed', top: 0, right: 0, bottom: 0, width: 360, zIndex: OVERLAY_Z + 1, display: 'flex', flexDirection: 'column',
  background: 'var(--panel)', borderLeft: '1px solid var(--border)', boxShadow: 'var(--shadow)', animation: 'sb-drawer .22s ease',
};
const footNote: CSSProperties = { position: 'fixed', bottom: 6, left: 12, fontSize: 10, color: 'var(--muted)', opacity: 0.55, pointerEvents: 'none', zIndex: 1 };
