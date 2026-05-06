import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { ChevronDown, BarChart3 } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import InstrumentIcon from '../components/InstrumentIcon';
import { usePrefetchLogos } from '../hooks/usePrefetchLogos';
import { METHODOLOGY } from '../data/methodology';
import { getChartData, getInstrument } from '../services/api';
import type { ChartResponse } from '../types';
import type { ChartAnnotation } from '../components/SimpleChart';
import SimpleChart from '../components/SimpleChart';
import InstrumentSearchModal from '../components/InstrumentSearchModal';
import Dropdown, { type DropdownOption } from '../components/Dropdown';
import { PERIOD_LABELS as ALL_PERIOD_LABELS, INTERVAL_LABELS } from '../config/chartConfig';
import { useAuth } from '../contexts/AuthContext';
import { useTheme } from '../contexts/ThemeContext';
import { isIntervalAllowed, isPeriodAllowed, getDefaultPeriod } from '../config/accessControl';
import { useRealtimeData } from '../hooks/useRealtimeData';
import { useFitToViewport } from '../hooks/useFitToViewport';

type DisplayMode = 'price' | 'positions' | 'participants';
type OIVariant = 'oi' | 'long' | 'short' | 'both' | 'net';
type Period = '1d' | '1w' | '1m' | '3m' | '6m' | '1y' | '2y' | '5y' | 'all';

const PERIOD_LABELS: Record<Period, string> = {
  '1d':  ALL_PERIOD_LABELS['1d'],
  '1w':  ALL_PERIOD_LABELS['1w'],
  '1m':  ALL_PERIOD_LABELS['1m'],
  '3m':  ALL_PERIOD_LABELS['3m'],
  '6m':  ALL_PERIOD_LABELS['6m'],
  '1y':  ALL_PERIOD_LABELS['1y'],
  '2y':  ALL_PERIOD_LABELS['2y'],
  '5y':  ALL_PERIOD_LABELS['5y'],
  'all': ALL_PERIOD_LABELS['all'],
};

// Локальный INSTRUMENT_ICONS удалён — используется shared
// `<InstrumentIcon sectype={...} />` из components/InstrumentIcon.tsx
// (он сам разруливает фьючерсы → акции → лого, валюты → custom badge).

// Цветовая палитра — все цвета через CSS-переменные чтобы автоматически
// адаптироваться к теме (в editorial-light → muted blue/orange, в OKX dark
// → яркие неоновые). См. --chart-line-1 / --oi-* в index.css.
const COLORS = {
  primary: 'var(--chart-line-1)',     // Цена инструмента (Сбербанк) — blue в editorial, indigo в OKX
  amber:   'var(--oi-amber)',          // ОИ — оранжевый чип
  emerald: 'var(--oi-green)',          // Покупки/Long — зелёный чип
  rose:    'var(--oi-red)',            // Продажи/Short — красный чип
  cyan:    'var(--oi-cyan)',           // Чистая позиция — циан чип
  lime:    'var(--oi-amber)',          // legacy alias
};

export default function OpenInterestPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { isAuthenticated } = useAuth();
  const { theme: _theme } = useTheme();
  const navigate = useNavigate();

  // Фоновая предзагрузка лого один раз — модалка выбора актива потом
  // открывается мгновенно из SW cache, без 100 запросов.
  usePrefetchLogos();

  // Адаптивная высота графика. Anchor-ref на wrapper самого графика —
  // хук вычитает позицию anchor.top от window.innerHeight, плюс buffer на
  // range-slider внутри SimpleChart и нижний padding страницы.
  // Без хардкода: всё что добавляется/убирается выше графика учтётся
  // автоматически (margins, error плашки, multi-row controls и т.д.),
  // потому что хук смотрит на реальную позицию anchor в DOM.
  const headerRef = useRef<HTMLDivElement>(null);
  const controlsRef = useRef<HTMLDivElement>(null);
  const chartAnchorRef = useRef<HTMLDivElement>(null);
  const chartHeight = useFitToViewport(chartAnchorRef, {
    min: 360,
    max: 720,
    bottomBuffer: 96, // range slider в SimpleChart (~48) + page py-bottom (~32) + safety margin
    watchRefs: [headerRef, controlsRef],
  });

  // Инструмент
  // ВАЖНО: sec_id может прийти из URL-параметра (например `?instrument=IMOEXF`),
  // а instrumentName инициализируется дефолтом 'Сбербанк'. Поэтому ниже стоит
  // useEffect который при первом рендере (или при любом расхождении ticker/name)
  // резолвит имя через /api/instruments/{sec_id} — иначе был баг
  // "Сбербанк [IMOEXF]" в UI-кнопке.
  const [selectedInstrument, setSelectedInstrument] = useState(
    searchParams.get('instrument') || 'SR'
  );
  const [instrumentName, setInstrumentName] = useState(
    searchParams.get('instrument') ? '' : 'Сбербанк'  // пустое имя если пришло из URL — будет резолвлено
  );
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Синхронизация имени инструмента с тикером.
  // Срабатывает когда selectedInstrument меняется без вызова handleSelectInstrument:
  //   - при первой загрузке страницы если в URL передан ?instrument=XXX
  //   - при ручном редактировании URL
  // Если имя пусто → резолвим через API. handleSelectInstrument ставит имя сам,
  // так что этот useEffect ничего не ломает при выборе из модалки.
  useEffect(() => {
    if (instrumentName && instrumentName.length > 0) return;
    let cancelled = false;
    getInstrument(selectedInstrument).then((inst) => {
      if (cancelled) return;
      if (inst?.name) {
        setInstrumentName(inst.name);
      } else {
        // API не ответил — fallback на сам тикер, чтобы хоть что-то показать
        setInstrumentName(selectedInstrument);
      }
    });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedInstrument]);

  // Данные графика
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<ChartResponse | null>(null);
  // Интервал, для которого загружены текущие данные — обновляется атомарно с data
  const [dataInterval, setDataInterval] = useState(24);

  // Настройки
  const [interval, setIntervalValue] = useState(24);
  const [clgroup, setClgroup] = useState<'FIZ' | 'YUR'>('YUR');
  const [displayMode, setDisplayMode] = useState<DisplayMode>('positions');
  const [oiVariant, setOiVariant] = useState<OIVariant>('net');
  const [showExpirations, setShowExpirations] = useState(false);
  const [period, setPeriod] = useState<Period>(getDefaultPeriod('6m', isAuthenticated) as Period);

  // Фильтрация нерабочих дней и пре-маркета.
  // Алгопак возвращает forward-fill данные за выходные, праздники и
  // пре-маркет (07:40-08:55) — значения идентичны предыдущему закрытию.
  const filteredData = useMemo(() => {
    if (!data) return null;

    // Праздники MOEX 2024-2026 (YYYY-MM-DD)
    const MOEX_HOLIDAYS = new Set([
      '2024-01-01','2024-01-02','2024-01-03','2024-01-04','2024-01-05','2024-01-08',
      '2024-02-23','2024-03-08','2024-05-01','2024-05-09','2024-06-12','2024-11-04','2024-12-31',
      '2025-01-01','2025-01-02','2025-01-03','2025-01-06','2025-01-07','2025-01-08',
      '2025-02-24','2025-03-10','2025-05-01','2025-05-02','2025-05-09','2025-06-12','2025-06-13',
      '2025-11-04','2025-12-31',
      '2026-01-01','2026-01-02','2026-01-05','2026-01-06','2026-01-07','2026-01-08',
      '2026-02-23','2026-03-09','2026-05-01','2026-05-11','2026-06-12','2026-11-04','2026-12-31',
    ]);

    const isNonTrading = (timeStr: string): boolean => {
      const d = new Date(timeStr);
      const day = d.getDay(); // 0=Sun, 6=Sat
      if (day === 0 || day === 6) return true;
      // Проверяем праздники по YYYY-MM-DD
      const dateKey = timeStr.slice(0, 10);
      if (MOEX_HOLIDAYS.has(dateKey)) return true;
      // Пре-маркет (до 09:00) — только для 5-мин
      if (dataInterval === 5 && d.getHours() < 9) return true;
      return false;
    };

    const filterItems = <T extends { time: string }>(items: T[]): T[] =>
      items.filter((item) => !isNonTrading(item.time));

    const newCandles = filterItems(data.candles);
    const newOi = filterItems(data.open_interest);

    // Если ничего не отфильтровалось — вернуть оригинал (избежать лишних ре-рендеров)
    if (newCandles.length === data.candles.length && newOi.length === data.open_interest.length) {
      return data;
    }

    return {
      ...data,
      candles: newCandles,
      open_interest: newOi,
    };
  }, [data, dataInterval]);

  const availableIntervals = filteredData?.available_intervals || [24];
  const hasInterval = (int: number) => availableIntervals.includes(int);
  const showOi = displayMode !== 'price';

  // Ограничения периодов для интервалов (для производительности)
  // 5мин: макс 1 месяц, 1час: макс 6 месяцев, 1день: все
  const MAX_PERIODS_BY_INTERVAL: Record<number, Period[]> = {
    5: ['1d', '1w', '1m'],
    60: ['1d', '1w', '1m', '3m', '6m'],
    24: ['1d', '1w', '1m', '3m', '6m', '1y', '2y', '5y', 'all']
  };

  const isPeriodAvailable = (p: Period): boolean => {
    const allowed = MAX_PERIODS_BY_INTERVAL[interval] || MAX_PERIODS_BY_INTERVAL[24];
    return allowed.includes(p);
  };

  // Авто-переключение периода при смене интервала
  const handleIntervalChange = (newInterval: number) => {
    const allowed = MAX_PERIODS_BY_INTERVAL[newInterval] || MAX_PERIODS_BY_INTERVAL[24];
    setIntervalValue(newInterval);

    // 1Д ТФ + 1Д период не имеет смысла → переключаем на 1Н
    if (newInterval === 24 && period === '1d') {
      setPeriod('1w');
      return;
    }

    // Если текущий период недоступен — переключаем на максимальный доступный
    if (!allowed.includes(period)) {
      setPeriod(allowed[allowed.length - 1]);
    }
  };

  // Загрузка данных графика
  const loadData = useCallback(async () => {
    if (!selectedInstrument) return;

    try {
      setLoading(true);
      setError(null);
      const result = await getChartData(
        selectedInstrument,
        selectedInstrument,
        'futures',
        interval,
        clgroup,
        showOi,
        period
      );
      setData(result);
      setDataInterval(interval); // обновляем вместе с data (React 18 batches)

      if (result.available_intervals?.length > 0 &&
        !result.available_intervals.includes(interval)) {
        setIntervalValue(Math.max(...result.available_intervals));
      }
    } catch (err) {
      setError('Ошибка загрузки данных');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [selectedInstrument, interval, clgroup, showOi, period]);

  useEffect(() => { loadData(); }, [loadData]);

  // SSE: автоматическое обновление при новых данных
  useRealtimeData(['5min', 'hourly'], loadData);

  // Выбор инструмента из модалки
  const handleSelectInstrument = (sectype: string, name: string) => {
    setSelectedInstrument(sectype);
    setInstrumentName(name);
    setSearchParams({ instrument: sectype });
    setIsModalOpen(false);
  };

  // Данные для графика (мемоизированы — иначе каждый рендер создаёт новый массив,
  // что приводит к ложным перезапускам анимации в SimpleChart)
  const chartData = useMemo(() =>
    filteredData?.candles.map((c) => ({
      time: c.time,
      value: c.close,
    })) || []
    , [filteredData]);

  // Выравнивание OI данных по временным меткам свечей.
  // OI имеет меньше точек в час (нет 08:00, 18:00), что вызывает
  // временной сдвиг при index-based X mapping в SimpleChart.
  // Для каждой свечи берём последнее известное значение OI на этот момент.
  const alignToCandles = (
    oiSeries: { time: string; value: number }[] | undefined
  ): { time: string; value: number }[] | undefined => {
    if (!oiSeries || oiSeries.length === 0 || chartData.length === 0) return oiSeries;

    // Для дневных свечей: ключ по ДАТЕ (свечи T00:00:00, OI T23:50:00).
    // Для интрадей (5мин/1час): ключ по полному timestamp — OI и свечи
    // имеют одинаковые метки, и нужно сохранить внутридневную гранулярность.
    const isIntraday = dataInterval !== 24;

    const oiMap = new Map<string, number>();
    for (const p of oiSeries) {
      const key = isIntraday ? p.time : p.time.slice(0, 10);
      oiMap.set(key, p.value);
    }

    const aligned: { time: string; value: number }[] = [];
    let lastValue: number | null = null;

    for (const candle of chartData) {
      const key = isIntraday ? candle.time : candle.time.slice(0, 10);
      const val = oiMap.get(key);
      if (val !== undefined) {
        lastValue = val;
      }
      if (lastValue !== null) {
        aligned.push({ time: candle.time, value: lastValue });
      }
    }

    return aligned.length > 0 ? aligned : oiSeries;
  };

  const { secondary: oiData, third: oiDataThird } = useMemo(() => {
    if (!filteredData?.open_interest || displayMode === 'price') {
      return { secondary: undefined, third: undefined };
    }
    const isPositions = displayMode === 'positions';
    let secondary: { time: string; value: number }[] | undefined;
    let third: { time: string; value: number }[] | undefined;

    switch (oiVariant) {
      case 'oi':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions
            ? (oi.pos_long || 0) + Math.abs(oi.pos_short || 0)
            : (oi.pos_long_num || 0) + (oi.pos_short_num || 0),
        }));
        break;
      case 'long':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? (oi.pos_long || 0) : (oi.pos_long_num || 0),
        }));
        break;
      case 'short':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0),
        }));
        break;
      case 'both':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? (oi.pos_long || 0) : (oi.pos_long_num || 0),
        }));
        third = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions ? Math.abs(oi.pos_short || 0) : (oi.pos_short_num || 0),
        }));
        break;
      case 'net':
        secondary = filteredData.open_interest.map((oi) => ({
          time: oi.time,
          value: isPositions
            ? (oi.net_position ?? ((oi.pos_long || 0) + (oi.pos_short || 0)))
            : (oi.pos_long_num || 0) - (oi.pos_short_num || 0),
        }));
        break;
    }

    return {
      secondary: alignToCandles(secondary),
      third: alignToCandles(third),
    };
  }, [filteredData, displayMode, oiVariant, chartData]);

  const getColors = () => {
    switch (oiVariant) {
      case 'oi': return { secondary: COLORS.amber, third: '' };
      case 'long': return { secondary: COLORS.emerald, third: '' };
      case 'short': return { secondary: COLORS.rose, third: '' };
      case 'both': return { secondary: COLORS.emerald, third: COLORS.rose };
      case 'net': return { secondary: COLORS.cyan, third: '' };
      default: return { secondary: COLORS.amber, third: '' };
    }
  };

  const colors = getColors();

  const getLabels = () => {
    const isPositions = displayMode === 'positions';
    switch (oiVariant) {
      case 'oi': return { secondary: 'Открытый интерес', third: '' };
      case 'long': return { secondary: isPositions ? 'Покупки' : 'Покупатели', third: '' };
      case 'short': return { secondary: isPositions ? 'Продажи' : 'Продавцы', third: '' };
      case 'both': return {
        secondary: isPositions ? 'Покупки' : 'Покупатели',
        third: isPositions ? 'Продажи' : 'Продавцы'
      };
      case 'net': return { secondary: 'Чистая позиция', third: '' };
      default: return { secondary: '', third: '' };
    }
  };

  const labels = getLabels();

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8">
      <div ref={headerRef}>
        <PageHeader
          icon={BarChart3}
          title="Открытый интерес"
          subtitle="Анализ позиций участников по фьючерсам MOEX"
          help={METHODOLOGY.oi}
          helpLink="/methodology/oi"
        />
      </div>

      {/* Editorial frame — обнимает controls + chart в один контейнер с
          1.5px outline + hard-shadow 5×5×0 (как в design handoff page.jsx).
          В non-editorial темах класс не имеет стилей — структура остаётся
          плоской, как раньше. */}
      <div className="editorial-frame">

      {/* Контролы — все режимы через Dropdown'ы для экономии места.
          Asset + FIZ/YUR + Interval + Period + DisplayMode + OI variant + Экспирации
          в одну строку (на узких экранах wraps). Стиль editorial pill через
          Dropdown компонент. */}
      <div ref={controlsRef} className="mb-4 md:mb-6">
        <div className="flex flex-wrap items-center gap-2 md:gap-3">
          {/* Селектор инструмента — открывает модалку */}
          <button
            onClick={() => setIsModalOpen(true)}
            className="widget-flat font-medium transition-colors flex items-center hover:opacity-90"
            style={{
              color: 'var(--text-primary)',
              fontSize: 'var(--fs-sm)',
              padding: 'var(--sp-2) var(--sp-4)',
              gap: 'var(--sp-3)',
              minWidth: 'clamp(140px, 30vw, 200px)',
            }}
          >
            <InstrumentIcon sectype={selectedInstrument} size={24} rounded="full" eager />
            <div className="flex-1 text-left">
              <div className="font-medium">{instrumentName}</div>
              <div className="text-theme-secondary" style={{ fontSize: 'var(--fs-2xs)' }}>{selectedInstrument}</div>
            </div>
            <ChevronDown size={14} className="text-theme-secondary" />
          </button>

          {/* FIZ/YUR — только если displayMode !== price */}
          {displayMode !== 'price' && (
            <Dropdown<'FIZ' | 'YUR'>
              options={[
                { key: 'FIZ', label: 'Физлица' },
                { key: 'YUR', label: 'Юрлица' },
              ]}
              value={clgroup}
              onChange={setClgroup}
            />
          )}

          {/* Таймфрейм */}
          <Dropdown<string>
            options={[5, 60, 24].map((int): DropdownOption<string> => {
              const available = displayMode === 'price' || hasInterval(int);
              const allowed = isIntervalAllowed(int, isAuthenticated);
              return {
                key: String(int),
                label: INTERVAL_LABELS[int as keyof typeof INTERVAL_LABELS],
                locked: !allowed || !available,
              };
            })}
            value={String(interval)}
            onChange={(k) => {
              const int = Number(k);
              const allowed = isIntervalAllowed(int, isAuthenticated);
              if (!allowed) { navigate('/login'); return; }
              const available = displayMode === 'price' || hasInterval(int);
              if (available) handleIntervalChange(int);
            }}
          />

          {/* Период */}
          <Dropdown<Period>
            options={(Object.keys(PERIOD_LABELS) as Period[]).map((p): DropdownOption<Period> => {
              const available = isPeriodAvailable(p);
              const allowed = isPeriodAllowed(p, isAuthenticated);
              return {
                key: p,
                label: PERIOD_LABELS[p],
                locked: !allowed || !available,
              };
            })}
            value={period}
            onChange={(p) => {
              const allowed = isPeriodAllowed(p, isAuthenticated);
              if (!allowed) { navigate('/login'); return; }
              const available = isPeriodAvailable(p);
              if (!available) return;
              if (p === '1d' && interval === 24) setIntervalValue(60);
              setPeriod(p);
            }}
          />

          {/* Режим отображения */}
          <Dropdown<DisplayMode>
            options={[
              { key: 'price', label: 'Только цена' },
              { key: 'positions', label: 'Позиции' },
              { key: 'participants', label: 'Участники' },
            ]}
            value={displayMode}
            onChange={(m) => {
              setDisplayMode(m);
              if (m !== 'price') setOiVariant('oi');
            }}
          />

          {/* Варианты OI — каждый с цветной полоской слева */}
          {displayMode !== 'price' && (
            <Dropdown<OIVariant>
              options={[
                { key: 'oi',    label: 'Открытый интерес',                                                     color: 'var(--oi-amber)' },
                { key: 'long',  label: displayMode === 'positions' ? 'Покупки' : 'Покупатели',                 color: 'var(--oi-green)' },
                { key: 'short', label: displayMode === 'positions' ? 'Продажи' : 'Продавцы',                   color: 'var(--oi-red)' },
                { key: 'both',  label: displayMode === 'positions' ? 'Покупки + Продажи' : 'Покупатели + Продавцы', color: 'var(--oi-purple)' },
                { key: 'net',   label: 'Чистая позиция',                                                       color: 'var(--oi-cyan)' },
              ]}
              value={oiVariant}
              onChange={setOiVariant}
            />
          )}

          {/* Экспирации — отдельный toggle (boolean state, не входит в OI variants) */}
          {displayMode !== 'price' && (
            <button
              onClick={() => setShowExpirations(!showExpirations)}
              className="editorial-press font-semibold rounded-full"
              style={{
                backgroundColor: showExpirations ? 'var(--accent)' : 'var(--bg-secondary)',
                color: showExpirations ? 'var(--text-inverse)' : 'var(--text-primary)',
                border: '2px solid var(--text-primary)',
                boxShadow: showExpirations ? 'var(--shadow-hard-chip)' : undefined,
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-2) var(--sp-4)',
              }}
            >
              Экспирации
            </button>
          )}
        </div>
      </div>

      {/* Ошибка */}
      {error && (
        <div className="rounded-xl p-4 mb-6" style={{ backgroundColor: 'color-mix(in srgb, var(--danger) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--danger) 30%, transparent)' }}>
          <p className="text-theme-danger">{error}</p>
        </div>
      )}

      {/* График — фон и border заданы внутри SimpleChart (bg-theme-primary,
          border + hard shadow в editorial). Обёртка убрана чтобы не было
          двойной рамки. chartAnchorRef нужен хуку useFitToViewport для
          расчёта высоты графика «остаток до низа viewport». */}
      <div ref={chartAnchorRef}>
      <SimpleChart
        data={chartData}
        secondaryData={oiData}
        thirdData={oiDataThird}
        showSecondary={displayMode !== 'price' && !!oiData}
        showThird={oiVariant === 'both' && !!oiDataThird}
        primaryColor={COLORS.primary}
        secondaryColor={colors.secondary}
        thirdColor={colors.third}
        height={chartHeight}
        loading={loading}
        formatValue={(v) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })}
        primaryLabel={instrumentName || selectedInstrument}
        secondaryLabel={labels.secondary}
        thirdLabel={labels.third}
        showValueHeader={false}
        legendPosition="top"
        showDownloadButton={false}
        showNavigator={true}
        annotations={useMemo(() => {
          if (!showExpirations) return undefined;
          const switches = filteredData?.contract_switches;
          if (!switches || switches.length <= 1) return undefined;
          return switches.slice(1).map((sw): ChartAnnotation => {
            return {
              time: sw.date,
              label: sw.to,
              description: `${sw.from} → ${sw.to}`,
              color: '#3a3f4f',
              textColor: '#9CA3B8',
            };
          });
        }, [filteredData?.contract_switches, showExpirations])}
      />
      </div>{/* /chartAnchorRef */}

      </div>{/* /editorial-frame */}

      {/* Легенда — оформлена как editorial card (frame с hard shadow в editorial,
          обычная widget панель в OKX/dark). Inner bg = secondary чтобы выделяться
          на page-bg, в editorial CSS override применит outline + hard shadow. */}
      <div className="mt-6 bg-theme-secondary border border-theme rounded-2xl widget" style={{ padding: 'var(--sp-5)' }}>
        <div style={{ fontSize: 'var(--fs-sm)', display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
          {/* Линии графика */}
          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.primary }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.primary }}>График цены</span>
              <span className="text-theme-secondary"> — стоимость фьючерса на срочном рынке</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.amber }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.amber }}>Открытый интерес</span>
              <span className="text-theme-secondary"> — сумма позиций на покупку и на продажу</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.emerald }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.emerald }}>Покупки</span>
              <span className="text-theme-secondary"> — объём позиций на рост / количество покупателей</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.rose }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.rose }}>Продажи</span>
              <span className="text-theme-secondary"> — объём позиций на падение / количество продавцов</span>
            </div>
          </div>

          <div className="flex items-start" style={{ gap: 'var(--sp-3)' }}>
            <span className="legend-dot mt-1.5" style={{ backgroundColor: COLORS.cyan }} />
            <div>
              <span className="font-medium" style={{ color: COLORS.cyan }}>Чистая позиция</span>
              <span className="text-theme-secondary"> — разница между покупками и продажами</span>
            </div>
          </div>

          {/* Режимы */}
          <div className="pt-3 border-t border-theme mt-4">
            <div className="text-theme-secondary">
              <span className="font-medium text-theme-primary">Режимы:</span>{' '}
              <span style={{ color: COLORS.primary }}>Позиции</span> — объём в контрактах,{' '}
              <span style={{ color: COLORS.primary }}>Участники</span> — количество участников торгов
            </div>
          </div>
        </div>
      </div>

      {/* Модалка выбора инструмента */}
      {isModalOpen && (
        <InstrumentSearchModal
          onSelect={handleSelectInstrument}
          onClose={() => setIsModalOpen(false)}
          filterType="futures"
        />
      )}
    </div>
  );
}