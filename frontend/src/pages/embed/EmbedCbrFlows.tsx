/**
 * EmbedCbrFlows — виджет «Потоки ЦБ» (рыночный, тикер не нужен).
 * Полный паритет со страницей /cbr-flows: тип инструмента (акции / ОФЗ / валюты),
 * период (1Г / Всё) и фильтр категорий участников — всё в drawer'е настроек.
 * Переиспользует StackedBidirectionalHistogram.
 *
 * getCbrFlows(type) — единственный fetch-триггер. Период и категории — клиентская
 * нарезка (slice / filter), без обращения к API.
 */
import { useEffect, useMemo, useRef, useState, lazy, Suspense } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Camera } from 'lucide-react';
import StackedBidirectionalHistogram, {
  type StackedBidirectionalHistogramHandle,
} from '../../components/cbr/StackedBidirectionalHistogram';
import { getCategoryColor } from '../../components/cbr/cbrPalette';
import { getCategoryInfo } from '../../components/cbr/cbrCategoryInfo';
import { getDefaultHiddenCategories } from '../../components/cbr/cbrDefaultVisibility';
import { getCbrFlows } from '../../services/api';
import { useTheme } from '../../contexts/ThemeContext';
import { EmbedMsg } from './embedUi';
import { DrawerSection, Checklist } from './EmbedSettings';
import { EmbedFrame, PillGroup, Dropdown } from './EmbedToolbar';
import { useEmbedPersist } from './embedPersist';

const ExportModal = lazy(() => import('../../components/export/ExportModal'));

type CbrType = 'stocks' | 'ofz' | 'fx';
type PeriodFilter = '1y' | '3y' | 'all';
type LoadStatus = 'idle' | 'loading' | 'ok' | 'empty' | 'error';
type CbrResp = Awaited<ReturnType<typeof getCbrFlows>>;

const TYPES: { id: CbrType; label: string }[] = [
  { id: 'stocks', label: 'Акции' },
  { id: 'ofz', label: 'ОФЗ' },
  { id: 'fx', label: 'Валюты' },
];

const PERIODS: { id: PeriodFilter; label: string; months: number | null }[] = [
  { id: '1y', label: '1Г', months: 12 },
  { id: '3y', label: '3Г', months: 36 },
  { id: 'all', label: 'Всё', months: null },
];

export default function EmbedCbrFlows() {
  const { rd, wr } = useEmbedPersist();
  const [params] = useSearchParams();
  const { theme } = useTheme();

  const [type, setType] = useState<CbrType>((params.get('type') as CbrType) || 'stocks');
  const [period, setPeriod] = useState<PeriodFilter>(
    () => (params.get('period') || rd('frame:embed:cbr:period', '1y')) as PeriodFilter,
  );
  // Скрытые категории. При смене типа — сбрасываем на дефолт (категории различаются
  // для stocks/ofz/fx); дефолт сужен до базовых категорий — до 7 участников
  // (stocks/ofz) не помещаются на узкой embed-ширине, см. cbrDefaultVisibility.
  const [hiddenCategories, setHiddenCategories] = useState<Set<string>>(() => getDefaultHiddenCategories(type));
  const [data, setData] = useState<CbrResp | null>(null);
  const [status, setStatus] = useState<LoadStatus>('idle');
  const [exportOpen, setExportOpen] = useState(false);

  useEffect(() => { wr('frame:embed:cbr:period', period); }, [period]);

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

  // Сброс скрытых категорий на дефолт при смене типа актива.
  useEffect(() => { setHiddenCategories(getDefaultHiddenCategories(type)); }, [type]);

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
  // Ref на гистограмму — для settleForCapture() перед PNG-экспортом (см.
  // StackedBidirectionalHistogramHandle): без него export сразу после
  // «Развернуть»/смены периода мог захватить кадр с недоигранной
  // reveal-анимацией (пустые бары) и/или устаревшей шириной контейнера
  // (слипшиеся X-подписи дат).
  const histogramRef = useRef<StackedBidirectionalHistogramHandle>(null);
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

  return (
    <EmbedFrame
      toolbar={
        <>
          <PillGroup value={type} options={TYPES} onChange={(v) => setType(v)} />
          <Dropdown value={period} options={PERIODS} onChange={(v) => setPeriod(v)} title="Период" />
        </>
      }
      actions={
        status === 'ok' && data ? (
          <button
            type="button"
            onClick={() => setExportOpen(true)}
            title="Экспорт графика"
            aria-label="Экспорт графика"
            style={{
              width: 28, height: 28, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
              border: 'none', borderRadius: 7, background: 'transparent', color: 'var(--text-secondary)',
              cursor: 'pointer', flexShrink: 0, padding: 0,
            }}
          >
            <Camera size={15} />
          </button>
        ) : undefined
      }
      more={
        <>
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
      <div ref={boxRef} style={{ position: 'absolute', inset: 0 }}>
        {status === 'ok' && data && (
          <StackedBidirectionalHistogram
            ref={histogramRef}
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
        {exportOpen && boxRef.current && (
          <Suspense fallback={null}>
            <ExportModal
              targetElement={boxRef.current}
              filename={`frame-cbr-flows-${type}-${period}`}
              metadata={{
                title: 'Поток капитала',
                details: [TYPES.find((t) => t.id === type)?.label, PERIODS.find((p) => p.id === period)?.label].filter((x): x is string => !!x),
              }}
              beforeCapture={() => histogramRef.current?.settleForCapture()}
              onClose={() => setExportOpen(false)}
            />
          </Suspense>
        )}
      </div>
    </EmbedFrame>
  );
}
