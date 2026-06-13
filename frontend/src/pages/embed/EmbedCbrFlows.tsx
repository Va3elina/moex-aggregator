/**
 * EmbedCbrFlows — виджет «Потоки ЦБ» (рыночный, тикер не нужен).
 * Полный паритет со страницей /cbr-flows: тип инструмента (акции / ОФЗ / валюты),
 * период (1Г / Всё) и фильтр категорий участников — всё в drawer'е настроек.
 * Переиспользует StackedBidirectionalHistogram.
 *
 * getCbrFlows(type) — единственный fetch-триггер. Период и категории — клиентская
 * нарезка (slice / filter), без обращения к API.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import StackedBidirectionalHistogram from '../../components/cbr/StackedBidirectionalHistogram';
import { getCategoryColor } from '../../components/cbr/cbrPalette';
import { getCategoryInfo } from '../../components/cbr/cbrCategoryInfo';
import { getCbrFlows } from '../../services/api';
import { useTheme } from '../../contexts/ThemeContext';
import { EmbedMsg } from './embedUi';
import { useEmbedSettings, EmbedShell, DrawerSection, SegGroup, Checklist } from './EmbedSettings';

type CbrType = 'stocks' | 'ofz' | 'fx';
type PeriodFilter = '1y' | 'all';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type CbrResp = Awaited<ReturnType<typeof getCbrFlows>>;

const TYPES: { id: CbrType; label: string }[] = [
  { id: 'stocks', label: 'Акции' },
  { id: 'ofz', label: 'ОФЗ' },
  { id: 'fx', label: 'Валюты' },
];

const PERIODS: { id: PeriodFilter; label: string; months: number | null }[] = [
  { id: '1y', label: '1Г', months: 12 },
  { id: 'all', label: 'Всё', months: null },
];

function readLS(key: string, fallback: string): string {
  try { return localStorage.getItem(key) || fallback; } catch { return fallback; }
}

export default function EmbedCbrFlows() {
  const [params] = useSearchParams();
  const settings = useEmbedSettings();
  const { theme } = useTheme();

  const [type, setType] = useState<CbrType>((params.get('type') as CbrType) || 'stocks');
  const [period, setPeriod] = useState<PeriodFilter>(
    () => (params.get('period') || readLS('frame:embed:cbr:period', '1y')) as PeriodFilter,
  );
  // Скрытые категории. При смене типа — сбрасываем (категории различаются для stocks/ofz/fx).
  const [hiddenCategories, setHiddenCategories] = useState<Set<string>>(new Set());
  const [data, setData] = useState<CbrResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');

  useEffect(() => { try { localStorage.setItem('frame:embed:cbr:period', period); } catch { /* quota */ } }, [period]);

  useEffect(() => {
    let cancelled = false;
    setStatus('loading');
    getCbrFlows(type)
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setStatus((res?.periods?.length ?? 0) > 0 ? 'ok' : 'empty');
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('embed/cbr-flows load failed:', err);
        setStatus('error');
      });
    return () => { cancelled = true; };
  }, [type]);

  // Сброс скрытых категорий при смене типа актива.
  useEffect(() => { setHiddenCategories(new Set()); }, [type]);

  // Видимые категории (для передачи в график).
  const visibleCategories = useMemo(
    () => (data ? data.categories.filter((c) => !hiddenCategories.has(c)) : []),
    [data, hiddenCategories],
  );

  // Нарезка periods по выбранному периоду (последние N месяцев / всё).
  const visiblePeriods = useMemo(() => {
    if (!data) return [];
    const months = PERIODS.find((o) => o.id === period)?.months ?? null;
    return months === null ? data.periods : data.periods.slice(-months);
  }, [data, period]);

  const toggleCategory = (cat: string) => {
    setHiddenCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) {
        next.delete(cat);
      } else {
        // Нельзя скрыть последнюю видимую категорию.
        if (visibleCategories.length <= 1) return prev;
        next.add(cat);
      }
      return next;
    });
  };

  const boxRef = useRef<HTMLDivElement>(null);
  const [chartH, setChartH] = useState(300);
  useEffect(() => {
    const el = boxRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect.height;
      if (h && h > 0) setChartH(Math.round(h));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const typeLabel = TYPES.find((t) => t.id === type)?.label || '';

  return (
    <EmbedShell
      settings={settings}
      title="Потоки ЦБ"
      subtitle={typeLabel}
      drawer={
        <>
          <DrawerSection label="Тип инструмента">
            <SegGroup value={type} options={TYPES} onChange={(v) => setType(v)} />
          </DrawerSection>
          <DrawerSection label="Период">
            <SegGroup value={period} options={PERIODS} onChange={(v) => setPeriod(v)} />
          </DrawerSection>
          {data && data.categories.length > 0 && (
            <DrawerSection label="Участники биржи">
              <Checklist
                items={data.categories.map((cat) => {
                  const on = !hiddenCategories.has(cat);
                  // Последнюю видимую категорию нельзя выключить.
                  const lockLast = on && visibleCategories.length <= 1;
                  return {
                    id: cat,
                    label: cat,
                    on,
                    color: getCategoryColor(cat, theme),
                    desc: getCategoryInfo(cat) || undefined,
                    disabled: lockLast,
                    onToggle: () => toggleCategory(cat),
                  };
                })}
              />
            </DrawerSection>
          )}
        </>
      }
    >
      <div ref={boxRef} style={{ flex: 1, position: 'relative', minHeight: 0 }}>
        {status === 'ok' && data && (
          <StackedBidirectionalHistogram
            periods={visiblePeriods}
            categories={visibleCategories}
            allPeriods={data.periods}
            unit={data.unit ?? 'млрд руб.'}
            height={chartH}
            animTrigger={`${type}|${period}`}
          />
        )}
        {status === 'loading' && <EmbedMsg text="Загрузка…" />}
        {status === 'empty' && <EmbedMsg text="Нет данных" />}
        {status === 'error' && <EmbedMsg text="Ошибка загрузки" />}
      </div>
    </EmbedShell>
  );
}
