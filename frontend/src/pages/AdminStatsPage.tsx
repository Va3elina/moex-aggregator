/**
 * AdminStatsPage — admin-only страница со статистикой использования сайта.
 *
 * Source endpoints (все role=admin):
 *   GET /api/analytics/stats        — сводка + динамика + топы
 *   GET /api/analytics/alerts-stats — трекинг уведомлений
 *   GET /api/analytics/users        — таблица пользователей
 *
 * Определения метрик переписаны 2026-09-11 после сверки с Яндекс Метрикой
 * (см. шапку /stats в api/routers/analytics.py): посетитель = аккаунт или
 * постоянный ID браузера, визит = разрыв 30 минут, время = только пока человек
 * активен, дни по Москве, админы по умолчанию исключены.
 *
 * У каждой метрики — HelpTooltip «?» с описанием, как она считается. Тексты
 * в METRIC_HINTS, менять там же.
 *
 * Все фильтры (период, сегмент, устройство, фильтры таблицы пользователей)
 * сохраняются в localStorage: после перехода в карточку пользователя и назад
 * ничего не сбрасывается.
 *
 * При переключении периода старые данные не сбрасываются в скелетоны:
 * контент приглушается до прихода свежих. Бэкенд кэширует /stats на 3 минуты.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { BarChart3, TrendingUp, TrendingDown, Activity, Users, Clock, Eye, Search, ChevronRight, AlarmClock, AlarmClockOff, Pause, Play, Zap, Loader2, Gift, LogOut, Repeat, ExternalLink, Globe } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import Dropdown from '../components/Dropdown';
import SimpleChart from '../components/SimpleChart';
import MetricaSourcesChart from '../components/admin/MetricaSourcesChart';
import AvatarImg from '../components/AvatarImg';
import HelpTooltip from '../components/HelpTooltip';
import { useAuth } from '../contexts/AuthContext';
import { usePersistedState } from '../hooks/usePersistedState';
import { useDelayedFlag } from '../hooks/useDelayedFlag';
import { useTweened } from '../hooks/useTweened';
import {
  getAnalyticsStats,
  listAdminUsers,
  getAlertsStats,
  getMetrica,
} from '../services/api';
import type {
  AnalyticsStats,
  AdminUser,
  AlertsStats,
  AdminRange,
  MetricaReport,
  MetricaRow,
  MetricaMetric,
  MetricaSummary,
  MetricaBySource,
} from '../services/api';

// ════════════════════════════════════════════════════════════════════════════
// ПОДСКАЗКИ МЕТРИК — как именно посчитана каждая цифра.
// ════════════════════════════════════════════════════════════════════════════

const METRIC_HINTS = {
  metrica:
    'Цифры здесь и в Яндекс Метрике считаются по-разному, полного совпадения не будет. '
    + '1) До 11.09.2026 наш трекер писал только тех, кто нажал «Окей» в баннере cookies, а Метрика видит всех. Поэтому за старые периоды у нас меньше людей. '
    + '2) Метрика узнаёт браузер по своей cookie, у нас с 11.09.2026 так же. Раньше гость у нас считался по вкладке. '
    + '3) Визит в обеих системах заканчивается после 30 минут бездействия. '
    + '4) Блокировщики рекламы режут Метрику чаще, чем наш трекер. '
    + '5) Вошедших и админов Метрика узнаёт по номеру аккаунта, который получает с 11.09.2026, и только в браузерах, где после этого входили в аккаунт. Наш трекер знает всех вошедших. '
    + '6) Метрика считает роботов по своей базе, мы отсекаем их по строке браузера.',
  metrica_section:
    'Трафик сайта берём из Яндекс Метрики: она видит всех посетителей, сама отсекает роботов, знает поисковые фразы, города и браузеры. Данные обновляются раз в 5 минут, у самой Метрики задержка несколько минут. Свой трекер оставлен для того, чего у Метрики нет: активы, действия внутри индикаторов, связь с аккаунтами и подписками.',
  metrica_pageviews: 'Сколько раз открывали страницы сайта. Каждый переход на другую страницу и каждое обновление страницы — отдельный просмотр.',
  metrica_visits: 'Заходы на сайт. Визит — серия действий одного посетителя. Если он ничего не делает 30 минут, следующее действие начинает новый визит.',
  metrica_users: 'Разные люди, точнее браузеры: Метрика узнаёт их по своей cookie. Один человек с телефона и с ноутбука — два посетителя. За период каждый считается один раз, поэтому посетителей за месяц меньше, чем сумма по дням.',
  metrica_new: 'Посетители, которые пришли на сайт впервые за всю историю счётчика.',
  metrica_time: 'Средняя длительность визита: от входа на сайт до последнего действия.',
  metrica_depth: 'Сколько страниц в среднем открывают за визит: просмотры, делённые на визиты.',
  metrica_bounce: 'Доля визитов, где открыли одну страницу и пробыли на ней меньше 15 секунд. Рост отказов — плохо, поэтому цвета изменения перевёрнуты.',
  metrica_chart:
    'Выбранный показатель по дням, на периоде от 92 дней — по неделям. «Всего» — по всему сайту, цветные линии — по источникам. '
    + 'Источник считается по последнему значимому переходу, как в Метрике по умолчанию: визит по закладке после прихода из поиска засчитывается поиску. '
    + 'Выходные подсвечены. В легенде линии включаются и выключаются, там же итог за период.',
  metrica_sources: 'Тип источника последнего значимого перехода: поиск, прямые заходы, ссылки на сайтах, соцсети, мессенджеры.',
  metrica_phrases: 'Поисковые запросы, по которым пришли из Яндекса и других поисковиков. Google почти все фразы скрывает.',
  metrica_phrases_unsegmented:
    'Сегмент из шапки к фразам не применяется: Метрика скрывает поисковые фразы, если фильтровать по аккаунту. Фильтр устройства действует.',
  metrica_referrers: 'Сайты, со ссылок на которых пришли посетители.',
  metrica_entry: 'Страница, с которой начался визит.',
  metrica_pages: 'Самые посещаемые страницы. Главная цифра — посетители, серая — просмотры.',
  own_section:
    'То же самое по нашему трекеру. Нужен для сверки с Метрикой и как запасной вариант, если Метрика недоступна. Цифры будут отличаться: Метрику режут блокировщики рекламы, а наш трекер не видит тех, кто отключил статистику в профиле.',
  inside_section:
    'Этого нет в Метрике: какие активы открывают на индикаторах, что ищут, что скачивают. Считается нашим трекером по всем посетителям, кроме отключивших статистику в профиле.',
  period:
    'Дни по московскому времени. Дельты на карточках сравнивают с таким же числом дней сразу перед выбранным периодом. Сырые события хранятся 180 дней, более ранние периоды будут пустыми.',
  segment:
    '«Все без админов» — вариант по умолчанию: вкладки админов открыты часами и раньше давали пятую часть всего времени на сайте. Авторизованные и гости определяются по посетителю: гость, который потом вошёл, считается авторизованным. '
    + 'Фильтры действуют и на блок Метрики. Там аккаунт виден по номеру, который Метрика получает при входе с 11.09.2026, и привязан к браузеру вместе с его прошлыми визитами. '
    + 'Браузер, в котором после этой даты ни разу не входили, Метрика считает гостем, даже если раньше в нём входили.',
  visitors:
    'Сколько разных людей было на сайте. Вошедший в аккаунт считается по аккаунту на всех устройствах. Гость — по постоянному ID браузера, он живёт год, как cookie Метрики. События гостя до входа приклеиваются к его аккаунту, поэтому человек не двоится. До 11.09.2026 ID браузера не было, гость считался по вкладке: старые периоды немного завышены.',
  visits:
    'Визит — серия действий одного посетителя. Пауза дольше 30 минут начинает новый визит, как в Метрике. Несколько вкладок одного человека одновременно дают один визит.',
  pageviews:
    'Переходы между страницами сайта. Смена актива или вкладки внутри страницы сюда не входит.',
  avg_time:
    'Среднее время визита: от первого до последнего действия. Пока вкладка на экране и человек двигает мышью, листает или нажимает клавиши, раз в минуту уходит сигнал присутствия. Через 5 минут без действий сигнал останавливается. Строка ниже — медиана: половина визитов короче неё. Среднее тянут вверх редкие длинные визиты.',
  bounce:
    'Доля визитов, где был один просмотр страницы и меньше 15 секунд. Так же считает Метрика. Здесь рост — плохо, поэтому цвета дельты перевёрнуты.',
  returning:
    'Посетители, которые приходили хотя бы в 2 разных дня выбранного периода. Процент — от всех посетителей. На периоде в 1 день всегда 0. До 11.09.2026 гостей узнавали только по вкладке, поэтому их возвраты за старые периоды почти не видны.',
  trends:
    'Посетители и визиты по дням, по московскому времени. Сумма дневных посетителей больше цифры за период: один человек приходит в разные дни. Дни без данных показаны нулями.',
  top_pages:
    'Главная цифра — сколько разных посетителей открыли страницу. Серая — сколько всего было просмотров.',
  top_assets:
    'Какие активы реально смотрят. Считается любой показ актива на графике ОИ, Сезонности и Репо: из поиска, по ссылке или сохранённый с прошлого раза. Главная цифра — разные посетители, серая — показы. Собирается с 11.09.2026, за более ранние даты список пуст.',
  top_search:
    'Что выбирают в окне поиска инструмента. Это интерес к поиску, а не все просмотры: актив по умолчанию и открытия по ссылке сюда не попадают. Главная цифра — выборы, серая — разные посетители.',
  sources:
    'Откуда начался визит, по сайту-источнику первой страницы. Прямые — источник не передан: закладка, ввод адреса, часть приложений вроде Telegram. Внутренние — визит начался с перехода внутри сайта, например после паузы больше 30 минут в той же вкладке. Метки utm показаны отдельно.',
  devices:
    'Посетители по типу устройства, определяется по браузеру. Один человек с телефона и компьютера попадёт в обе строки.',
  top_exports:
    'Скачивания графиков в PNG по индикаторам. Главная цифра — скачивания, серая — разные посетители.',
  modes:
    'Переключения режима на странице Сезонность. Главная цифра — переключения, серая — разные посетители.',
  alerts_section:
    'Первые четыре карточки — события за выбранный период. «Активных сейчас», «Хоть раз сработали», «По источнику» и топ активов — снимок на текущий момент, от периода не зависят.',
  alerts_created: 'Сколько уведомлений пользователи создали за период.',
  alerts_deleted: 'Сколько уведомлений удалили за период.',
  alerts_paused: 'Сколько раз уведомления ставили на паузу за период.',
  alerts_resumed: 'Сколько раз уведомления снимали с паузы за период.',
  alerts_active:
    'Число уведомлений со статусом «активен» прямо сейчас. Снимок текущего состояния, не за период.',
  alerts_fired:
    'Сколько из существующих уведомлений хоть раз срабатывали за всю историю. Процент — доля от активных сейчас. Может быть больше 100%, если сработавшие уведомления стоят на паузе.',
  alerts_source: 'Активные уведомления прямо сейчас по разделу сайта: ОИ или фонды.',
  alerts_top:
    'Активы, на которые прямо сейчас стоит больше всего активных уведомлений. Цифра — число уведомлений. От периода не зависит.',
  users_section:
    'Только зарегистрированные аккаунты. Подписка и фильтры по ней — текущее состояние, от периода не зависят. Визиты и время — за выбранный период, по действиям под аккаунтом, считаются так же, как в сводке. Последняя активность — за всё время и видна даже у тех, кто отказался от статистики: учитываются входы и обновление сессии.',
  users_filter:
    'Платные — подписка куплена и активна. Инвайт — доступ только по подарочной ссылке. Бывшие платные — сейчас без подписки, но раньше платили. Не дошли до оплаты — начинали оплату, но ни разу не заплатили. Цифра в скобках — сколько людей в фильтре по всей базе.',
} as const;

// ════════════════════════════════════════════════════════════════════════════
// ПЕРИОДЫ — пресеты и произвольный диапазон, всё в московских датах
// ════════════════════════════════════════════════════════════════════════════

type Preset = 'today' | 'yesterday' | '7d' | '30d' | 'this_month' | 'last_month' | '90d' | '180d' | '365d' | 'custom';

const PRESET_OPTIONS: { key: Preset; label: string }[] = [
  { key: 'today', label: 'Сегодня' },
  { key: 'yesterday', label: 'Вчера' },
  { key: '7d', label: '7 дней' },
  { key: '30d', label: '30 дней' },
  { key: 'this_month', label: 'Этот месяц' },
  { key: 'last_month', label: 'Прошлый месяц' },
  { key: '90d', label: '90 дней' },
  { key: '180d', label: '180 дней' },
  { key: '365d', label: 'Год' },
  { key: 'custom', label: 'Свой период' },
];

function mskToday(): string {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Moscow' }).format(new Date());
}

function addDays(iso: string, n: number): string {
  const d = new Date(`${iso}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

function presetRange(preset: Preset, from: string, to: string): Required<Pick<AdminRange, 'dateFrom' | 'dateTo'>> {
  const today = mskToday();
  const monthStart = `${today.slice(0, 8)}01`;
  switch (preset) {
    case 'today': return { dateFrom: today, dateTo: today };
    case 'yesterday': { const y = addDays(today, -1); return { dateFrom: y, dateTo: y }; }
    case '30d': return { dateFrom: addDays(today, -29), dateTo: today };
    case 'this_month': return { dateFrom: monthStart, dateTo: today };
    case 'last_month': {
      const lastDay = addDays(monthStart, -1);
      return { dateFrom: `${lastDay.slice(0, 8)}01`, dateTo: lastDay };
    }
    case '90d': return { dateFrom: addDays(today, -89), dateTo: today };
    case '180d': return { dateFrom: addDays(today, -179), dateTo: today };
    case '365d': return { dateFrom: addDays(today, -364), dateTo: today };
    case 'custom': {
      const a = from || addDays(today, -6);
      const b = to || today;
      return a <= b ? { dateFrom: a, dateTo: b } : { dateFrom: b, dateTo: a };
    }
    case '7d':
    default: return { dateFrom: addDays(today, -6), dateTo: today };
  }
}

function fmtDate(iso: string): string {
  const [y, m, d] = iso.split('-');
  return `${d}.${m}.${y}`;
}

function fmtRange(a: string, b: string): string {
  return a === b ? fmtDate(a) : `${fmtDate(a)} – ${fmtDate(b)}`;
}

// Человеческие имена разделов для топа страниц. Неизвестный путь показывается как есть.
const PAGE_NAMES: Record<string, string> = {
  '/': 'Главная',
  '/oi': 'Открытый интерес',
  '/heatmap': 'Карта рынка',
  '/strength': 'Сила рынка',
  '/funds-money': 'Деньги в фондах',
  '/fund-trades': 'Покупки фондов',
  '/seasonality': 'Сезонность',
  '/repo': 'Репо в акциях',
  '/pricing': 'Тарифы',
  '/profile': 'Профиль',
  '/login': 'Вход',
};

const INDICATOR_NAMES: Record<string, string> = {
  oi: 'ОИ',
  seasonality: 'Сезонность',
  repo: 'Репо',
  funds: 'Фонды',
};

const DEVICE_NAMES: Record<string, string> = {
  desktop: 'Компьютер',
  mobile: 'Телефон',
  tablet: 'Планшет',
  unknown: 'Не определено',
};

export default function AdminStatsPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();

  const [preset, setPreset] = usePersistedState<Preset>('frame:admin:stats:preset', '7d');
  const [customFrom, setCustomFrom] = usePersistedState<string>('frame:admin:stats:from', '');
  const [customTo, setCustomTo] = usePersistedState<string>('frame:admin:stats:to', '');
  const [segment, setSegment] = usePersistedState<string>('frame:admin:stats:segment', 'all');
  const [device, setDevice] = usePersistedState<string>('frame:admin:stats:device', 'all');
  const [data, setData] = useState<AnalyticsStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metrica, setMetrica] = useState<MetricaReport | null>(null);
  const [metricaLoading, setMetricaLoading] = useState(true);
  const [showOwn, setShowOwn] = usePersistedState<boolean>('frame:admin:stats:showOwn', false);
  // Раз в 5 минут, пока вкладка на экране, перезапрашиваем всё: Метрика
  // обновляет отчёты с задержкой в несколько минут, бэкенд кэширует на 5.
  const [tick, setTick] = useState(0);
  useEffect(() => {
    const id = window.setInterval(() => {
      if (document.visibilityState === 'visible') setTick((t) => t + 1);
    }, 5 * 60_000);
    return () => clearInterval(id);
  }, []);

  const range = useMemo(() => presetRange(preset, customFrom, customTo), [preset, customFrom, customTo]);

  // Guard: только admin
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') {
      navigate('/', { replace: true });
    }
  }, [authLoading, user, navigate]);

  // Ответы по набору фильтров, пока открыта страница: переключение туда и
  // обратно мгновенное, свежие данные догружаются тихо, без затемнения.
  const statsCache = useRef(new Map<string, AnalyticsStats>());
  const metricaCache = useRef(new Map<string, MetricaReport>());

  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    const key = `${JSON.stringify(range)}|${segment}|${device}`;
    const hit = statsCache.current.get(key);
    // Ответ на прошлый фильтр, пришедший позже нового, не должен его затереть.
    let alive = true;
    if (hit) setData(hit);
    setLoading(!hit);
    setError(null);
    getAnalyticsStats({ ...range, segment, device })
      .then((d) => {
        statsCache.current.set(key, d);
        if (alive) setData(d);
      })
      .catch((e: Error) => {
        if (alive && !hit) setError(e.message || 'Не удалось загрузить статистику');
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => { alive = false; };
  }, [user, range, segment, device, tick]);

  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    const key = `${JSON.stringify(range)}|${segment}|${device}`;
    const hit = metricaCache.current.get(key);
    let alive = true;
    if (hit) setMetrica(hit);
    setMetricaLoading(!hit);
    getMetrica(range, segment, device)
      .then((r) => {
        metricaCache.current.set(key, r);
        if (alive) setMetrica(r);
      })
      .catch(() => {
        if (alive && !hit) setMetrica(null);
      })
      .finally(() => {
        if (alive) setMetricaLoading(false);
      });
    return () => { alive = false; };
  }, [user, range, segment, device, tick]);

  // Затемняем только блок, который реально ждёт, и только если он ждёт
  // дольше 250 мс: ответ из кэша или быстрый ответ проходит без вспышки.
  const ownDim = useDelayedFlag(loading && data !== null);
  const metricaDim = useDelayedFlag(metricaLoading && metrica !== null);

  if (authLoading || !user || user.role !== 'admin') {
    return null;
  }

  const refreshing = ownDim || metricaDim;
  const metricaOn = !!metrica?.connected;
  // Свой трафик показываем, если Метрика не подключена или админ сам попросил сверку.
  const ownTrafficVisible = !metricaOn || showOwn;
  const s = data?.summary;
  const p = data?.prev_summary;

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
          <p className="text-sm mt-1 inline-flex items-center gap-1.5" style={{ color: 'var(--text-muted)' }}>
            Собственный трекер · почему не совпадает с Метрикой
            <HelpTooltip icon="help" title="Сравнение с Яндекс Метрикой" content={METRIC_HINTS.metrica} size={14} />
          </p>
        </div>
        <Link
          to="/admin/dashboard"
          className="editorial-press rounded-full flex items-center ml-auto shrink-0"
          style={{ padding: 'var(--sp-1) var(--sp-3)', gap: '6px', fontSize: 'var(--fs-xs)' }}
        >
          <Activity size={14} /> Состояние проекта
        </Link>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-end mb-2" style={{ gap: 'var(--sp-2)' }}>
        <Dropdown<Preset>
          options={PRESET_OPTIONS}
          value={preset}
          onChange={(v) => {
            // При переходе на «Свой период» подставляем текущий диапазон,
            // чтобы поля дат не открывались пустыми.
            if (v === 'custom' && preset !== 'custom') {
              setCustomFrom(range.dateFrom);
              setCustomTo(range.dateTo);
            }
            setPreset(v);
          }}
          trailing={<HelpTooltip icon="help" title="Период" content={METRIC_HINTS.period} size={13} />}
        />
        {preset === 'custom' && (
          <>
            <DateField label="С" value={range.dateFrom} max={mskToday()} onChange={setCustomFrom} />
            <DateField label="По" value={range.dateTo} max={mskToday()} onChange={setCustomTo} />
          </>
        )}
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все без админов' },
            { key: 'auth', label: 'Авторизованные' },
            { key: 'guest', label: 'Гости' },
            { key: 'admin', label: 'Только админы' },
            { key: 'everyone', label: 'Все вместе с админами' },
          ]}
          value={segment}
          onChange={setSegment}
          trailing={<HelpTooltip icon="help" title="Кого считаем" content={METRIC_HINTS.segment} size={13} />}
        />
        <Dropdown<string>
          options={[
            { key: 'all', label: 'Все устройства' },
            { key: 'desktop', label: 'Компьютер' },
            { key: 'mobile', label: 'Телефон' },
            { key: 'tablet', label: 'Планшет' },
          ]}
          value={device}
          onChange={setDevice}
        />
        {refreshing && (
          <span className="inline-flex items-center gap-1.5 text-xs self-center" style={{ color: 'var(--text-muted)' }}>
            <Loader2 size={13} className="animate-spin" />
            обновление…
          </span>
        )}
      </div>
      <p className="text-xs mb-6 md:mb-8" style={{ color: 'var(--text-muted)' }}>
        {fmtRange(range.dateFrom, range.dateTo)}
        {data && ` · сравнение с ${fmtRange(data.prev_date_from, data.prev_date_to)}`}
      </p>

      {error && (
        <Card padding="md" className="mb-6">
          <p style={{ color: 'var(--danger)' }}>Ошибка: {error}</p>
        </Card>
      )}

      <div>
        {/* ═══ Трафик из Яндекс Метрики ═══ */}
        <Section title="Трафик · Яндекс Метрика" hint={METRIC_HINTS.metrica_section}>
          <div style={dimStyle(metricaDim)}>
            <MetricaBlock report={metrica} loading={metricaLoading} />
          </div>
        </Section>

        {metricaOn && (
          <button
            type="button"
            onClick={() => setShowOwn(!showOwn)}
            className="editorial-press rounded-full text-xs mb-6 md:mb-8"
            style={{ padding: 'var(--sp-1) var(--sp-3)' }}
          >
            {showOwn ? 'Скрыть наш трекер' : 'Сверить с нашим трекером'}
          </button>
        )}

        {ownTrafficVisible && (
          <div style={dimStyle(ownDim)}>
          <Section
            title={metricaOn ? 'Трафик · наш трекер'
              : metrica?.token_error ? 'Трафик · наш трекер (токен Метрики не действует)'
              : 'Трафик · наш трекер (Метрика не подключена)'}
            hint={METRIC_HINTS.own_section}
          >
        {/* Summary cards */}
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3 md:gap-4 mb-6 md:mb-8">
          {s && p ? (
            <>
              <SummaryCard
                icon={<Users size={16} />}
                label="Посетители"
                hint={METRIC_HINTS.visitors}
                value={s.visitors.toLocaleString('ru-RU')}
                sub={s.auth_visitors > 0 ? `из них с аккаунтом ${s.auth_visitors}` : undefined}
                delta={pctDelta(s.delta_visitors_pct)}
                prev={p.visitors.toLocaleString('ru-RU')}
              />
              <SummaryCard
                icon={<Activity size={16} />}
                label="Визиты"
                hint={METRIC_HINTS.visits}
                value={s.visits.toLocaleString('ru-RU')}
                delta={pctDelta(s.delta_visits_pct)}
                prev={p.visits.toLocaleString('ru-RU')}
              />
              <SummaryCard
                icon={<Eye size={16} />}
                label="Просмотры"
                hint={METRIC_HINTS.pageviews}
                hintAlign="right"
                value={s.pageviews.toLocaleString('ru-RU')}
                sub={s.visits > 0 ? `${(s.pageviews / s.visits).toFixed(1).replace('.', ',')} на визит` : undefined}
                delta={pctDelta(s.delta_pageviews_pct)}
                prev={p.pageviews.toLocaleString('ru-RU')}
              />
              <SummaryCard
                icon={<Clock size={16} />}
                label="Время визита"
                hint={METRIC_HINTS.avg_time}
                value={formatDuration(s.avg_visit_sec)}
                sub={`медиана ${formatDuration(s.median_visit_sec)}`}
                delta={s.delta_avg_visit_sec === null ? null : {
                  text: `${s.delta_avg_visit_sec >= 0 ? '+' : '−'}${formatDuration(Math.abs(s.delta_avg_visit_sec))}`,
                  good: s.delta_avg_visit_sec >= 0,
                }}
                prev={formatDuration(p.avg_visit_sec)}
              />
              <SummaryCard
                icon={<LogOut size={16} />}
                label="Отказы"
                hint={METRIC_HINTS.bounce}
                value={s.bounce_pct === null ? '—' : `${fmtNum(s.bounce_pct)}%`}
                delta={s.delta_bounce_pp === null ? null : {
                  text: `${s.delta_bounce_pp >= 0 ? '+' : '−'}${fmtNum(Math.abs(s.delta_bounce_pp))} п.п.`,
                  good: s.delta_bounce_pp <= 0,
                }}
                prev={p.bounce_pct === null ? '—' : `${fmtNum(p.bounce_pct)}%`}
              />
              <SummaryCard
                icon={<Repeat size={16} />}
                label="Вернулись"
                hint={METRIC_HINTS.returning}
                hintAlign="right"
                value={s.returning.toLocaleString('ru-RU')}
                sub={s.returning_pct === null ? undefined : `${fmtNum(s.returning_pct)}% посетителей`}
                delta={pctDelta(s.delta_returning_pct)}
                prev={p.returning.toLocaleString('ru-RU')}
              />
            </>
          ) : loading ? (
            Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} height={128} rounded="lg" />)
          ) : null}
        </div>

        {/* Trends */}
        <Section title="Динамика по дням" hint={METRIC_HINTS.trends}>
          {data && data.trends.length > 1 ? (
            <Card padding="md" className="md:p-5">
              <SimpleChart
                data={data.trends.map(t => ({ time: t.date, value: t.visitors }))}
                secondaryData={data.trends.map(t => ({ time: t.date, value: t.visits }))}
                showSecondary={true}
                primaryColor="var(--accent)"
                secondaryColor="var(--accent-secondary)"
                primaryLabel="Посетители"
                secondaryLabel="Визиты"
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
          ) : loading && !data ? (
            <Skeleton height={320} rounded="lg" />
          ) : (
            <Card padding="md">
              <p className="text-center py-8 text-sm" style={{ color: 'var(--text-muted)' }}>
                {data && data.trends.length === 1
                  ? 'За один день графика нет, смотрите карточки выше'
                  : 'Недостаточно данных для построения графика'}
              </p>
            </Card>
          )}
        </Section>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-3 md:gap-4 mb-6 md:mb-8">
              <TopList
                title="Топ страниц"
                hint={METRIC_HINTS.top_pages}
                columns={['посетители', 'просмотры']}
                items={data?.top_pages.map((r) => ({
                  label: PAGE_NAMES[r.path] || r.path,
                  note: PAGE_NAMES[r.path] ? r.path : undefined,
                  value: r.visitors,
                  value2: r.views,
                })) || null}
                loading={loading}
                emptyText="Нет просмотров страниц"
              />
              <TopList
                title="Источники визитов"
                hint={METRIC_HINTS.sources}
                columns={['визиты']}
                items={data?.sources.map((r) => ({ label: r.source, value: r.visits })) || null}
                loading={loading}
                emptyText="Нет визитов"
              />
              {device === 'all' && (
                <TopList
                  title="Устройства"
                  hint={METRIC_HINTS.devices}
                  hintAlign="right"
                  columns={['посетители']}
                  items={data?.devices.map((r) => ({ label: DEVICE_NAMES[r.device] || r.device, value: r.visitors })) || null}
                  loading={loading}
                  emptyText="Нет данных"
                />
              )}
            </div>
          </Section>
          </div>
        )}

        {/* ═══ Чего нет в Метрике ═══ */}
        <div style={dimStyle(ownDim)}>
        <Section title="Что смотрят внутри сайта" hint={METRIC_HINTS.inside_section}>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
            <TopList
              title="Топ активов"
              hint={METRIC_HINTS.top_assets}
              columns={['посетители', 'показы']}
              items={data?.top_assets.map((r) => ({
                label: r.name || r.secid,
                note: [r.name ? r.secid : null, r.indicators.map(i => INDICATOR_NAMES[i] || i).join(', ')].filter(Boolean).join(' · '),
                value: r.visitors,
                value2: r.views,
              })) || null}
              loading={loading}
              emptyText="Данные собираются с 11.09.2026"
            />
            <TopList
              title="Выбор в поиске"
              hint={METRIC_HINTS.top_search}
              hintAlign="right"
              columns={['выборы', 'посетители']}
              items={data?.top_search.map((r) => ({
                label: r.name || r.secid,
                note: r.name ? r.secid : undefined,
                value: r.picks,
                value2: r.visitors,
              })) || null}
              loading={loading}
              emptyText="Нет выборов в поиске"
            />
            <TopList
              title="Экспорты PNG"
              hint={METRIC_HINTS.top_exports}
              columns={['скачивания', 'посетители']}
              items={data?.top_exports.map((r) => ({ label: INDICATOR_NAMES[r.indicator] || r.indicator, value: r.count, value2: r.visitors })) || null}
              loading={loading}
              emptyText="Никто не экспортировал"
            />
            <TopList
              title="Сезонность: режимы"
              hint={METRIC_HINTS.modes}
              hintAlign="right"
              columns={['переключения', 'посетители']}
              items={data?.mode_distribution.map((r) => ({ label: r.mode, value: r.count, value2: r.visitors })) || null}
              loading={loading}
              emptyText="Нет переключений режима"
            />
          </div>
        </Section>
        </div>
      </div>

      <Section title="Уведомления" hint={METRIC_HINTS.alerts_section}>
        <AlertsBlock range={range} />
      </Section>

      <Section title="Пользователи" hint={METRIC_HINTS.users_section}>
        <UsersBlock range={range} />
      </Section>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SUBCOMPONENTS
// ════════════════════════════════════════════════════════════════════════════

/** Блок ждёт данных дольше 250 мс — чуть гасим, но оставляем читаемым. */
function dimStyle(on: boolean): React.CSSProperties {
  return { opacity: on ? 0.6 : 1, transition: 'opacity 0.25s ease' };
}

const fmtClock = (iso: string) => new Date(iso).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });

/** Значение показателя: при смене досчитывается до нового, а не прыгает. */
function TweenedValue({ value, format, decimals }: { value: number; format: (v: number) => string; decimals?: boolean }) {
  const v = useTweened(value);
  return <>{format(decimals ? Math.round(v * 10) / 10 : Math.round(v))}</>;
}

/** Приложение Яндекс OAuth, на котором выпущен токен Метрики (то же, что у
 *  scripts/seo). Client ID не секрет: он всегда виден в ссылке авторизации. */
const METRIKA_TOKEN_URL = 'https://oauth.yandex.ru/authorize?response_type=token&client_id=13ef24cf955147729f6ccbe3021b8f3e';

type MetricaDelta = { text: string; good: boolean; up: boolean } | null;

interface MetricaMetricDef {
  key: MetricaMetric;
  label: string;
  hint: string;
  format: (v: number) => string;
  formatAxis?: (v: number) => string;
  /** Отказы сравниваем в процентных пунктах, остальное — в процентах. */
  points?: boolean;
  lowerIsBetter?: boolean;
}

const fmtInt = (v: number) => Math.round(v).toLocaleString('ru-RU');

/** Показатели в порядке сводки Метрики. */
const METRICA_METRICS: MetricaMetricDef[] = [
  { key: 'pageviews', label: 'Просмотры', hint: METRIC_HINTS.metrica_pageviews, format: fmtInt },
  { key: 'visits', label: 'Визиты', hint: METRIC_HINTS.metrica_visits, format: fmtInt },
  { key: 'users', label: 'Посетители', hint: METRIC_HINTS.metrica_users, format: fmtInt },
  { key: 'new_users', label: 'Новые', hint: METRIC_HINTS.metrica_new, format: fmtInt },
  {
    key: 'avg_visit_sec', label: 'Время на сайте', hint: METRIC_HINTS.metrica_time, format: formatDuration,
    formatAxis: (v) => (v > 0 && v % 60 === 0 ? `${v / 60}м` : formatDuration(v)),
  },
  { key: 'page_depth', label: 'Глубина', hint: METRIC_HINTS.metrica_depth, format: fmtNum },
  {
    key: 'bounce_pct', label: 'Отказы', hint: METRIC_HINTS.metrica_bounce, format: (v) => `${fmtNum(v)}%`,
    points: true, lowerIsBetter: true,
  },
];

/** Стрелка — куда сдвинулось значение, цвет — хорошо это или плохо. */
function metricaDelta(def: MetricaMetricDef, cur: number, prev: number | undefined): MetricaDelta {
  if (prev === undefined) return null;
  const diff = cur - prev;
  const good = def.lowerIsBetter ? diff <= 0 : diff >= 0;
  if (def.points) return { text: `${diff >= 0 ? '+' : '−'}${fmtNum(Math.abs(diff))} п.п.`, good, up: diff >= 0 };
  if (!prev) return null;
  const pct = Math.round((diff / prev) * 100);
  return { text: `${pct >= 0 ? '+' : '−'}${Math.abs(pct)}%`, good, up: diff >= 0 };
}

/** Показатели Метрики переключателями, как на её сводке, и график выбранного по источникам. */
function MetricaTraffic({ cur, prev, bySource, error }: {
  cur: MetricaSummary;
  prev: MetricaSummary | null;
  bySource: MetricaBySource | null;
  error?: string;
}) {
  const [picked, setPicked] = usePersistedState<MetricaMetric>('frame:admin-stats:metrica-metric', 'visits');
  const def = METRICA_METRICS.find((m) => m.key === picked) ?? METRICA_METRICS[1];
  return (
    <Card padding="md" className="md:p-5">
      <div role="radiogroup" aria-label="Показатель на графике" className="grid grid-cols-2 sm:grid-cols-4 xl:grid-cols-7 gap-2 mb-4 md:mb-5">
        {METRICA_METRICS.map((m, i) => {
          const on = m.key === def.key;
          const d = metricaDelta(m, cur[m.key], prev?.[m.key]);
          return (
            <div
              key={m.key}
              role="radio"
              aria-checked={on}
              tabIndex={0}
              onClick={() => setPicked(m.key)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setPicked(m.key); }
              }}
              className="cursor-pointer rounded-lg min-w-0"
              style={{
                padding: 'var(--sp-2) var(--sp-3)',
                background: on ? 'var(--bg-tertiary)' : 'transparent',
                boxShadow: on ? 'inset 0 -2px 0 var(--accent)' : 'none',
                transition: 'background-color 0.2s ease, box-shadow 0.2s ease',
              }}
            >
              <div className="flex items-center gap-1.5 mb-1" style={{ color: 'var(--text-muted)' }}>
                <span
                  aria-hidden
                  className="shrink-0 rounded-full"
                  style={{
                    width: 10, height: 10,
                    border: `2px solid ${on ? 'var(--accent)' : 'var(--text-muted)'}`,
                    background: on ? 'var(--accent)' : 'transparent',
                  }}
                />
                <span className="text-xs min-w-0 truncate">{m.label}</span>
                <span className="inline-flex shrink-0" onClick={(e) => e.stopPropagation()} onKeyDown={(e) => e.stopPropagation()}>
                  <HelpTooltip icon="help" title={m.label} content={m.hint} size={12} align={i >= 4 ? 'right' : 'left'} />
                </span>
              </div>
              <div
                className="font-bold"
                style={{
                  color: 'var(--text-primary)',
                  fontSize: 'clamp(1.2rem, 1.7vw, 1.5rem)',
                  letterSpacing: '-0.02em',
                  fontFamily: "'IBM Plex Mono', monospace",
                  fontVariantNumeric: 'tabular-nums',
                  whiteSpace: 'nowrap',
                }}
              >
                <TweenedValue value={cur[m.key]} format={m.format} decimals={m.key === 'page_depth' || m.key === 'bounce_pct'} />
              </div>
              {d && (
                <div
                  className="flex items-center gap-1 text-xs mt-0.5"
                  style={{ color: d.good ? 'var(--success)' : 'var(--danger)', fontFamily: "'IBM Plex Mono', monospace" }}
                >
                  {d.up ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                  {d.text}
                </div>
              )}
            </div>
          );
        })}
      </div>
      {bySource && bySource.dates.length > 0 ? (
        <MetricaSourcesChart
          data={bySource}
          metric={def.key}
          label={def.label}
          hint={METRIC_HINTS.metrica_chart}
          totalValue={cur[def.key]}
          format={def.format}
          formatAxis={def.formatAxis}
        />
      ) : (
        <p className="text-sm" style={{ color: error ? 'var(--danger)' : 'var(--text-muted)' }}>
          {error ? `График не пришёл: ${error}` : 'Нет данных за период'}
        </p>
      )}
    </Card>
  );
}

/** Трафик из Яндекс Метрики. Токена нет или он не действует — инструкция, как выдать новый. */
function MetricaBlock({ report, loading }: { report: MetricaReport | null; loading: boolean }) {
  if (loading && !report) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3 md:gap-4">
        {Array.from({ length: 6 }).map((_, i) => <Skeleton key={i} height={128} rounded="lg" />)}
      </div>
    );
  }
  if (!report) {
    return (
      <Card padding="md">
        <p className="text-sm" style={{ color: 'var(--danger)' }}>Не удалось получить данные Метрики.</p>
      </Card>
    );
  }
  if (!report.connected) {
    return (
      <Card padding="md" className="md:p-5">
        {report.token_error ? (
          <>
            <p className="text-sm font-semibold mb-1" style={{ color: 'var(--danger)' }}>
              Токен Метрики больше не действует.
            </p>
            <p className="text-sm mb-2" style={{ color: 'var(--text-secondary)' }}>
              {report.token_error}. Обычно это истёкший срок: токен живёт около полугода.
            </p>
          </>
        ) : (
          <p className="text-sm font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>
            Метрика не подключена: на сервере нет токена доступа к API.
          </p>
        )}
        <ol className="text-sm list-decimal pl-5 space-y-1" style={{ color: 'var(--text-secondary)' }}>
          <li>
            Под аккаунтом Яндекса с доступом к счётчику {report.counter} открыть{' '}
            <a href={METRIKA_TOKEN_URL} target="_blank" rel="noreferrer" style={{ color: 'var(--accent)' }}>страницу выдачи токена</a>,
            нажать «Разрешить» и скопировать токен.
          </li>
          <li>Записать его в /opt/frame/.env строкой YANDEX_METRIKA_TOKEN=токен и пересоздать контейнер api.</li>
          <li>Тем же токеном обновить scripts/seo/.env: им пользуется SEO-скрипт.</li>
        </ol>
        <p className="text-xs mt-2" style={{ color: 'var(--text-muted)' }}>
          Пока ниже показан трафик по нашему трекеру.
        </p>
      </Card>
    );
  }

  const cur = report.summary;
  const prev = report.prev_summary;
  const err = report.errors || {};
  const rows = (list: MetricaRow[] | null | undefined) =>
    list ? list.map((r) => ({ label: r.label, value: r.value, value2: r.value2 ?? undefined })) : null;
  const empty = (key: string) => (err[key] ? `Отчёт не пришёл: ${err[key]}` : 'Нет данных за период');
  const counter = report.counter;

  return (
    <div className="space-y-3 md:space-y-4" style={{ animation: 'fadeIn 0.35s ease-out' }}>
      <div className="flex flex-wrap gap-2">
        <a
          href={`https://metrika.yandex.ru/dashboard?id=${counter}`}
          target="_blank" rel="noreferrer"
          className="editorial-press rounded-full inline-flex items-center text-xs"
          style={{ padding: 'var(--sp-1) var(--sp-3)', gap: 6 }}
        >
          <ExternalLink size={12} /> Открыть Метрику
        </a>
        <a
          href={`https://metrika.yandex.ru/stat/visor?id=${counter}`}
          target="_blank" rel="noreferrer"
          className="editorial-press rounded-full inline-flex items-center text-xs"
          style={{ padding: 'var(--sp-1) var(--sp-3)', gap: 6 }}
        >
          <ExternalLink size={12} /> Вебвизор
        </a>
      </div>

      {cur ? (
        <MetricaTraffic cur={cur} prev={prev ?? null} bySource={report.by_source ?? null} error={err.by_source} />
      ) : (
        <Card padding="md"><p className="text-sm" style={{ color: 'var(--danger)' }}>{empty('summary')}</p></Card>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 md:gap-4">
        <TopList title="Источники трафика" hint={METRIC_HINTS.metrica_sources} columns={['визиты', 'посетители']}
          items={rows(report.sources)} loading={false} emptyText={empty('sources')} />
        <TopList
          title={report.phrases_unsegmented ? 'Поисковые фразы · все посетители' : 'Поисковые фразы'}
          hint={report.phrases_unsegmented
            ? `${METRIC_HINTS.metrica_phrases} ${METRIC_HINTS.metrica_phrases_unsegmented}`
            : METRIC_HINTS.metrica_phrases}
          hintAlign="right" columns={['визиты', 'посетители']}
          items={rows(report.search_phrases)} loading={false} emptyText={empty('search_phrases')} />
        <TopList title="Поисковые системы" columns={['визиты', 'посетители']}
          hint="Из каких поисковиков приходят." items={rows(report.search_engines)} loading={false} emptyText={empty('search_engines')} />
        <TopList title="Сайты-источники" hint={METRIC_HINTS.metrica_referrers} hintAlign="right" columns={['визиты', 'посетители']}
          items={rows(report.referrers)} loading={false} emptyText={empty('referrers')} />
        <TopList title="Популярные страницы" hint={METRIC_HINTS.metrica_pages} columns={['посетители', 'просмотры']}
          items={rows(report.pages)?.map(r => ({ ...r, label: PAGE_NAMES[r.label] || r.label, note: PAGE_NAMES[r.label] ? r.label : undefined })) ?? null}
          loading={false} emptyText={empty('pages')} />
        <TopList title="Страницы входа" hint={METRIC_HINTS.metrica_entry} hintAlign="right" columns={['визиты', 'посетители']}
          items={rows(report.entry_pages)?.map(r => ({ ...r, label: PAGE_NAMES[r.label] || r.label, note: PAGE_NAMES[r.label] ? r.label : undefined })) ?? null}
          loading={false} emptyText={empty('entry_pages')} />
        <TopList title="Города" hint="География посетителей по IP, определяет Метрика." columns={['посетители', 'визиты']}
          items={rows(report.cities)} loading={false} emptyText={empty('cities')} />
        <TopList title="Соцсети и мессенджеры" hint="Переходы из соцсетей и мессенджеров." hintAlign="right" columns={['визиты', 'посетители']}
          items={rows(report.social)} loading={false} emptyText={empty('social')} />
        <TopList title="Устройства" hint="Тип устройства по Метрике." columns={['посетители', 'визиты']}
          items={rows(report.devices)} loading={false} emptyText={empty('devices')} />
        <TopList title="Браузеры" hint="Браузеры посетителей." hintAlign="right" columns={['посетители', 'визиты']}
          items={rows(report.browsers)} loading={false} emptyText={empty('browsers')} />
      </div>
      <p className="text-xs inline-flex items-center gap-1.5" style={{ color: 'var(--text-muted)' }}>
        <Globe size={12} />
        {`${report.updated_at
          ? `Данные Метрики на ${fmtClock(report.updated_at)}, обновляются раз в 5 минут`
          : 'Обновляется раз в 5 минут'}. Метрика, счётчик ${counter}.`}
      </p>
    </div>
  );
}

function DateField({ label, value, max, onChange }: {
  label: string; value: string; max: string; onChange: (v: string) => void;
}) {
  return (
    <label className="inline-flex flex-col" style={{ gap: 2 }}>
      <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>{label}</span>
      <input
        type="date"
        value={value}
        max={max}
        onChange={(e) => { if (e.target.value) onChange(e.target.value); }}
        style={{
          padding: '6px 10px',
          background: 'var(--bg-secondary)',
          border: '1.5px solid var(--text-primary)',
          borderRadius: 9999,
          color: 'var(--text-primary)',
          fontSize: 'var(--fs-sm)',
          fontWeight: 600,
        }}
      />
    </label>
  );
}

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
        {hint && <HelpTooltip icon="help" title={title} content={hint} size={13} />}
        <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
      </div>
      {children}
    </section>
  );
}

type Delta = { text: string; good: boolean } | null;

function pctDelta(v: number | null | undefined): Delta {
  if (v === null || v === undefined) return null;
  return { text: `${v >= 0 ? '+' : '−'}${Math.abs(v)}%`, good: v >= 0 };
}

function fmtNum(v: number): string {
  return v.toLocaleString('ru-RU', { maximumFractionDigits: 1 });
}

interface SummaryCardProps {
  icon: React.ReactNode;
  label: string;
  value: string | number;
  /** Строка под значением (медиана, доля и т.п.). */
  sub?: string;
  delta?: Delta;
  /** Значение за предыдущий период — показывается рядом с дельтой. */
  prev?: string;
  hint?: string;
  hintAlign?: 'left' | 'right';
}
function SummaryCard({ icon, label, value, sub, delta, prev, hint, hintAlign = 'left' }: SummaryCardProps) {
  const display = typeof value === 'number' ? value.toLocaleString('ru-RU') : value;
  return (
    <Card padding="md" className="md:p-5">
      <div className="flex items-center gap-2 mb-2" style={{ color: 'var(--text-muted)' }}>
        {icon}
        <span className="text-xs uppercase min-w-0 truncate" style={{ letterSpacing: '0.1em', fontWeight: 600 }}>
          {label}
        </span>
        {hint && <HelpTooltip icon="help" title={label} content={hint} size={13} align={hintAlign} />}
      </div>
      <div
        className="font-bold"
        style={{
          color: 'var(--text-primary)',
          fontSize: 'clamp(1.5rem, 2.4vw, 2rem)',
          letterSpacing: '-0.02em',
          fontFamily: "'IBM Plex Mono', monospace",
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        {display}
      </div>
      {sub && (
        <div className="text-xs mt-0.5" style={{ color: 'var(--text-muted)' }}>{sub}</div>
      )}
      {(delta || prev) && (
        <div className="flex items-center flex-wrap gap-x-1.5 gap-y-0.5 mt-1">
          {delta && (
            <>
              {delta.good ? (
                <TrendingUp size={12} style={{ color: 'var(--success)' }} />
              ) : (
                <TrendingDown size={12} style={{ color: 'var(--danger)' }} />
              )}
              <span
                className="text-xs"
                style={{
                  color: delta.good ? 'var(--success)' : 'var(--danger)',
                  fontFamily: "'IBM Plex Mono', monospace",
                }}
              >
                {delta.text}
              </span>
            </>
          )}
          {prev && (
            <span className="text-xs" style={{ color: 'var(--text-muted)' }}>было {prev}</span>
          )}
        </div>
      )}
    </Card>
  );
}

interface TopItem { label: string; note?: string; value: number; value2?: number }

interface TopListProps {
  title: string;
  items: TopItem[] | null;
  loading: boolean;
  emptyText: string;
  hint?: string;
  hintAlign?: 'left' | 'right';
  /** Подписи колонок: главная цифра и (опц.) серая вторая. */
  columns?: [string] | [string, string];
}
function TopList({ title, items, loading, emptyText, hint, hintAlign = 'left', columns }: TopListProps) {
  if (loading && !items) return <Skeleton height={240} rounded="lg" />;
  const max = items && items.length > 0 ? Math.max(...items.map(i => i.value)) || 1 : 1;
  return (
    <Card padding="md" className="md:p-5">
      <div className="flex items-center gap-2 mb-3">
        <span
          className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.1em', fontWeight: 600 }}
        >
          {title}
        </span>
        {hint && <HelpTooltip icon="help" title={title} content={hint} size={13} align={hintAlign} />}
        {columns && items && items.length > 0 && (
          <span className="ml-auto text-xs" style={{ color: 'var(--text-muted)' }}>
            {columns[0]}{columns[1] ? ` · ${columns[1]}` : ''}
          </span>
        )}
      </div>
      {!items || items.length === 0 ? (
        <p className="text-center py-6 text-sm" style={{ color: 'var(--text-muted)' }}>
          {emptyText}
        </p>
      ) : (
        <div className="space-y-1.5" style={{ animation: 'fadeIn 0.3s ease-out' }}>
          {items.map((it, i) => (
            <div key={`${it.label}-${i}`} className="relative">
              <div
                className="absolute inset-y-0 left-0 rounded"
                style={{
                  width: `${(it.value / max) * 100}%`,
                  backgroundColor: 'color-mix(in srgb, var(--accent) 14%, transparent)',
                  transition: 'width 0.45s cubic-bezier(0.22, 1, 0.36, 1)',
                }}
              />
              <div className="relative flex items-center justify-between py-1.5 px-2 gap-2">
                <span className="text-sm truncate min-w-0" style={{ color: 'var(--text-primary)' }} title={it.note ? `${it.label} · ${it.note}` : it.label}>
                  {it.label || '—'}
                  {it.note && (
                    <span className="text-xs ml-1.5" style={{ color: 'var(--text-muted)' }}>{it.note}</span>
                  )}
                </span>
                <span
                  className="text-sm flex-shrink-0 whitespace-nowrap"
                  style={{ fontFamily: "'IBM Plex Mono', monospace", fontVariantNumeric: 'tabular-nums' }}
                >
                  <span className="font-semibold" style={{ color: 'var(--text-primary)' }}>
                    {it.value.toLocaleString('ru-RU')}
                  </span>
                  {it.value2 !== undefined && (
                    <span className="text-xs ml-1.5" style={{ color: 'var(--text-muted)' }}>
                      · {it.value2.toLocaleString('ru-RU')}
                    </span>
                  )}
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
  const sec = seconds % 60;
  if (h > 0) return `${h}ч ${m}м`;
  if (m > 0) return `${m}м ${sec}с`;
  return `${sec}с`;
}

// ════════════════════════════════════════════════════════════════════════════
// USERS BLOCK — фильтры по подписке + drill-down таблица пользователей
// ════════════════════════════════════════════════════════════════════════════

const PLAN_COLORS: Record<string, string> = {
  basic: 'var(--accent)',
  pro: 'var(--warning)',
  premium: 'var(--success)',
};

const STATUS_NAMES: Record<string, string> = {
  expired: 'истекла',
  cancelled: 'отменена',
  pending: 'не оплачена',
  failed: 'оплата не прошла',
};

/**
 * Бейдж подписки: тир цветом + дата окончания. Подписка по пригласительной
 * ссылке помечается подарком, иначе в таблице она неотличима от купленной.
 * У тех, кто сейчас без подписки, показываем последнюю платную попытку:
 * «была basic, истекла 03.09» — так видно бывших платных.
 */
function PlanBadge({ plan, expiresAt, isInvite, inviteNote, lastPaid }: {
  plan: string | null;
  expiresAt?: string | null;
  isInvite?: boolean;
  inviteNote?: string | null;
  lastPaid?: AdminUser['last_paid_sub'];
}) {
  if (!plan) {
    return (
      <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
        free
        {lastPaid && (
          <span title="Последняя подписка за деньги">
            {' · '}{lastPaid.tier} {STATUS_NAMES[lastPaid.status] || lastPaid.status}
            {lastPaid.expires_at && ` ${fmtShortDate(lastPaid.expires_at)}`}
          </span>
        )}
      </span>
    );
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
          title={inviteNote ? `Инвайт: ${inviteNote}` : 'Подписка выдана по пригласительной ссылке, не оплачена'}
        >
          <Gift size={11} />
          {inviteNote}
        </span>
      )}
      {expiresAt && (
        <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
          до {fmtShortDate(expiresAt)}
        </span>
      )}
    </span>
  );
}

function fmtShortDate(iso: string): string {
  return new Date(iso).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
}

const USER_FILTERS: { key: string; label: string }[] = [
  { key: 'all', label: 'Все' },
  { key: 'paid', label: 'Платные' },
  { key: 'paid_pro', label: 'Платные Pro' },
  { key: 'paid_basic', label: 'Платные Basic' },
  { key: 'invite', label: 'По инвайту' },
  { key: 'churned', label: 'Бывшие платные' },
  { key: 'pending', label: 'Не дошли до оплаты' },
  { key: 'free', label: 'Бесплатные' },
  { key: 'admin', label: 'Админы' },
];

const USER_SORTS: { key: string; label: string }[] = [
  { key: 'last_active', label: 'По активности' },
  { key: 'tier', label: 'По тарифу: Pro, Basic, инвайт' },
  { key: 'plan', label: 'Сначала купившие' },
  { key: 'expires', label: 'Скоро закончится подписка' },
  { key: 'visits', label: 'По визитам' },
  { key: 'time', label: 'По времени на сайте' },
  { key: 'created', label: 'По регистрации' },
];

function UsersBlock({ range }: { range: AdminRange }) {
  const navigate = useNavigate();
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [counts, setCounts] = useState<Record<string, number> | null>(null);
  const [loading, setLoading] = useState(true);
  const dim = useDelayedFlag(loading && users.length > 0);
  // Всё сохраняется: после перехода в карточку пользователя и назад
  // фильтр, сортировка и поиск остаются как были.
  const [search, setSearch] = usePersistedState<string>('frame:admin:users:search', '');
  const [sort, setSort] = usePersistedState<string>('frame:admin:users:sort', 'last_active');
  const [filter, setFilter] = usePersistedState<string>('frame:admin:users:filter', 'all');

  const safeFilter = USER_FILTERS.some(f => f.key === filter) ? filter : 'all';
  const safeSort = USER_SORTS.some(o => o.key === sort) ? sort : 'last_active';

  useEffect(() => {
    const t = window.setTimeout(() => {
      setLoading(true);
      listAdminUsers({ ...range, sort: safeSort, search: search.trim(), filter: safeFilter })
        .then(r => {
          setUsers(r.users);
          setCounts(r.counts ?? {
            all: r.total_count, paid: r.paid_count, invite: r.invite_count,
          });
        })
        .catch(() => setUsers([]))
        .finally(() => setLoading(false));
    }, 250);
    return () => clearTimeout(t);
  }, [range, safeSort, search, safeFilter]);

  const filterOptions = USER_FILTERS.map(f => ({
    key: f.key,
    label: counts && counts[f.key] !== undefined ? `${f.label} (${counts[f.key]})` : f.label,
  }));

  return (
    <Card padding="md" className="md:p-5">
      <div className="flex flex-wrap items-center mb-4" style={{ gap: 'var(--sp-2)' }}>
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
          options={filterOptions}
          value={safeFilter}
          onChange={setFilter}
          menuMaxWidth={320}
          trailing={<HelpTooltip icon="help" title="Фильтр по подписке" content={METRIC_HINTS.users_filter} size={13} align="right" />}
        />
        <Dropdown<string>
          options={USER_SORTS}
          value={safeSort}
          onChange={setSort}
          menuMaxWidth={320}
        />
        <span className="text-xs ml-auto whitespace-nowrap" style={{ color: 'var(--text-muted)' }}>
          показано {users.length}
          {counts?.all !== undefined && ` из ${counts.all}`}
          {counts?.paid !== undefined && (
            <>
              {' · '}
              <span style={{ color: 'var(--success)', fontWeight: 600 }}>платных: {counts.paid}</span>
            </>
          )}
          {counts?.invite !== undefined && counts.invite > 0 && (
            <>
              {' · '}
              <span style={{ fontWeight: 600 }}>по инвайту: {counts.invite}</span>
            </>
          )}
        </span>
      </div>

      <div className="overflow-x-auto -mx-2" style={dimStyle(dim)}>
        <table className="w-full" style={{ minWidth: 820 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-color)' }}>
              <UCol>Пользователь</UCol>
              <UCol align="left">Подписка</UCol>
              <UCol align="left" hide="md">Роль</UCol>
              <UCol align="right">Визитов</UCol>
              <UCol align="right" hide="md">Время</UCol>
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
                onClick={() => navigate(`/admin/users/${u.id}`)}
              >
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
                  <PlanBadge
                    plan={u.plan}
                    expiresAt={u.plan_expires_at}
                    isInvite={u.is_invite}
                    inviteNote={u.invite_note}
                    lastPaid={u.last_paid_sub}
                  />
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
                  {u.time_sec ? formatDuration(u.time_sec) : '—'}
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
// ALERTS BLOCK — трекинг уведомлений (поставили/убрали/пауза/возобновили + снимок)
// ════════════════════════════════════════════════════════════════════════════

function AlertsBlock({ range }: { range: AdminRange }) {
  const [stats, setStats] = useState<AlertsStats | null>(null);
  const [loading, setLoading] = useState(true);
  const dim = useDelayedFlag(loading && stats !== null);

  useEffect(() => {
    setLoading(true);
    getAlertsStats(range)
      .then(setStats)
      .catch(() => setStats(null))
      .finally(() => setLoading(false));
  }, [range]);

  if (loading && !stats) {
    return (
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
        {Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} height={108} rounded="lg" />)}
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

  const conversionPct = stats.active_now > 0
    ? Math.round((stats.with_fires / stats.active_now) * 100)
    : null;

  return (
    <div className="space-y-3 md:space-y-4" style={{ ...dimStyle(dim), animation: 'fadeIn 0.35s ease-out' }}>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
        <SummaryCard icon={<AlarmClock size={16} />} label="Поставили" hint={METRIC_HINTS.alerts_created} value={stats.created} />
        <SummaryCard icon={<AlarmClockOff size={16} />} label="Убрали" hint={METRIC_HINTS.alerts_deleted} value={stats.deleted} />
        <SummaryCard icon={<Pause size={16} />} label="На паузу" hint={METRIC_HINTS.alerts_paused} value={stats.paused} />
        <SummaryCard icon={<Play size={16} />} label="Возобновили" hint={METRIC_HINTS.alerts_resumed} hintAlign="right" value={stats.resumed} />
      </div>

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
          sub={conversionPct === null ? undefined : `${conversionPct}% от активных`}
        />
        <Card padding="md" className="md:p-5">
          <div className="flex items-center gap-2 mb-3" style={{ color: 'var(--text-muted)' }}>
            <BarChart3 size={16} />
            <span className="text-xs uppercase" style={{ letterSpacing: '0.1em', fontWeight: 600 }}>
              По источнику
            </span>
            <HelpTooltip icon="help" title="По источнику" content={METRIC_HINTS.alerts_source} size={13} align="right" />
          </div>
          {stats.by_source.length === 0 ? (
            <p className="text-sm" style={{ color: 'var(--text-muted)' }}>—</p>
          ) : (
            <div className="space-y-1.5">
              {stats.by_source.map((src) => (
                <div key={src.source} className="flex items-center justify-between">
                  <span className="text-sm" style={{ color: 'var(--text-primary)' }}>
                    {src.source === 'oi' ? 'ОИ' : src.source === 'funds' ? 'Фонды' : src.source}
                  </span>
                  <span
                    className="text-sm font-semibold"
                    style={{
                      color: 'var(--text-primary)',
                      fontFamily: "'IBM Plex Mono', monospace",
                      fontVariantNumeric: 'tabular-nums',
                    }}
                  >
                    {src.active.toLocaleString('ru-RU')}
                  </span>
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>

      {stats.top_assets && stats.top_assets.length > 0 && (
        <TopList
          title="Топ активов в уведомлениях"
          hint={METRIC_HINTS.alerts_top}
          columns={['уведомлений']}
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
