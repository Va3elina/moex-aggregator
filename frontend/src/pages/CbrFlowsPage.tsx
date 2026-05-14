/**
 * /cbr-flows — Потоки участников биржевых торгов по данным ЦБ (ОРФР).
 *
 * Источник: ежемесячный обзор финансовой стабильности Банка России.
 * Бэкенд парсит XLSX с cbr.ru/analytics/finstab/orfr/ и upsert'ит в БД
 * (см. CBR/fetch_orfr_flows.py).
 *
 * Структура:
 *   • Header (title + help link)
 *   • Editorial frame
 *     • Chip-row из 3 типов (Акции / ОФЗ / Валюты) — переключатель
 *     • Stacked bidirectional histogram (positive вверх, negative вниз)
 *     • Подпись года под осью X
 *   • Footer (источник + дата обновления)
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import { LineChart, Landmark, DollarSign, Building2 } from 'lucide-react';
import PageHeader from '../components/PageHeader';
import { METHODOLOGY } from '../data/methodology';
import {
  getCbrFlows,
  type CbrFlowsResponse,
  type CbrInstrumentType,
} from '../services/api';
import { useFitToViewport } from '../hooks/useFitToViewport';
import StackedBidirectionalHistogram from '../components/cbr/StackedBidirectionalHistogram';
import { getCategoryColor } from '../components/cbr/cbrPalette';
import { getCategoryInfo } from '../components/cbr/cbrCategoryInfo';
import ChartCaptureButton from '../components/export/ChartCaptureButton';
import { useTheme } from '../contexts/ThemeContext';

const INSTRUMENT_TABS: Array<{
  key: CbrInstrumentType;
  label: string;
  Icon: typeof LineChart;
}> = [
  { key: 'stocks', label: 'Акции',  Icon: LineChart },
  { key: 'ofz',    label: 'ОФЗ',    Icon: Landmark },
  { key: 'fx',     label: 'Валюты', Icon: DollarSign },
];

export default function CbrFlowsPage() {
  const { theme } = useTheme();
  const [type, setType] = useState<CbrInstrumentType>('stocks');
  const [data, setData] = useState<CbrFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Категории-фильтр: какие категории скрыты из графика.
  // При смене type — сбрасываем (категории различаются для stocks/ofz/fx).
  const [hiddenCategories, setHiddenCategories] = useState<Set<string>>(new Set());

  const chartAnchorRef = useRef<HTMLDivElement>(null);
  const chartHeight = useFitToViewport(chartAnchorRef, {
    min: 360,
    max: 640,
    bottomBuffer: 96,
  });

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    getCbrFlows(type)
      .then((res) => {
        if (cancelled) return;
        setData(res);
        setLoading(false);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(e?.message ?? 'Не удалось загрузить данные');
        setLoading(false);
      });
    return () => { cancelled = true; };
  }, [type]);

  // Reset hidden при смене типа актива
  useEffect(() => {
    setHiddenCategories(new Set());
  }, [type]);

  // Видимые категории (для передачи в график)
  const visibleCategories = useMemo(() => {
    if (!data) return [];
    return data.categories.filter((c) => !hiddenCategories.has(c));
  }, [data, hiddenCategories]);

  const toggleCategory = (cat: string) => {
    setHiddenCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) {
        next.delete(cat);
      } else {
        // Нельзя скрыть последнюю видимую категорию
        if (visibleCategories.length <= 1) return prev;
        next.add(cat);
      }
      return next;
    });
  };

  return (
    <div className="max-w-[1408px] mx-auto px-4 md:px-6 py-6 md:py-8 text-theme-primary min-h-screen">
      <PageHeader
        icon={Building2}
        title="Потоки участников биржи"
        subtitle="Кто покупает и кто продаёт по типам активов — по данным Банка России"
        help={METHODOLOGY.cbrFlows}
        helpLink="/methodology/cbr-flows"
      />

      <div className="editorial-frame">
        {/* === Controls row: chip-переключатель активов + camera button справа ===
            Flex layout с flex:1 для chip'ов даёт равное распределение (как
            grid-cols-3) и одновременно позволяет добавить camera button с
            ml-auto. На mobile при недостатке места flex-wrap → camera уходит
            на отдельную строку — без overflow. */}
        <div
          className="flex flex-wrap items-center mb-4 md:mb-6"
          style={{ gap: 'var(--sp-2)' }}
        >
          {INSTRUMENT_TABS.map((t) => {
            const isActive = type === t.key;
            const Icon = t.Icon;
            return (
              <button
                key={t.key}
                onClick={() => setType(t.key)}
                className="editorial-press flex items-center justify-center font-semibold rounded-full min-w-0"
                style={{
                  flex: '1 1 0',
                  minWidth: '90px',
                  gap: 'var(--sp-2)',
                  padding: 'var(--sp-2) var(--sp-3)',
                  fontSize: 'var(--fs-sm)',
                  backgroundColor: isActive ? 'var(--accent)' : 'var(--bg-secondary)',
                  color: isActive ? 'var(--text-inverse)' : 'var(--text-primary)',
                  border: '2px solid var(--text-primary)',
                  boxShadow: isActive ? 'var(--shadow-hard-chip)' : undefined,
                }}
              >
                <Icon
                  className="shrink-0"
                  style={{ width: 'var(--ico-sm)', height: 'var(--ico-sm)' }}
                />
                <span className="truncate">{t.label}</span>
              </button>
            );
          })}

          {/* Camera button — экспорт графика в PNG (html2canvas). Metadata
              включает текущий тип актива (Акции/ОФЗ/Валюты) и источник ЦБ. */}
          <ChartCaptureButton
            getTargetElement={() => chartAnchorRef.current}
            filename={`frame-cbr-flows-${type}`}
            metadata={{
              title: 'Потоки участников биржи',
              asset: data?.instrument_label ?? INSTRUMENT_TABS.find(t => t.key === type)?.label ?? '',
              details: [
                'Источник: Банк России · ОРФР',
                data?.source ?? '',
              ].filter(Boolean) as string[],
            }}
            className="ml-auto shrink-0"
          />
        </div>

        {/* === Inner paper-card вокруг графика ===
            bg-primary = главный фон сайта (paper light / чёрный dark) —
            графики лежат на «странице», а не на secondary card-фоне
            frame'а. 1.5px outline + rounded-2xl как у Heatmap. */}
        <div
          ref={chartAnchorRef}
          className="rounded-2xl"
          style={{
            background: 'var(--bg-primary)',
            border: '1.5px solid var(--text-primary)',
            // padding-bottom 12 — gap между year labels и rounded paper-card edge.
            // Без него html2canvas при export обрезал year (близко к радиусной зоне).
            // overflow убран — content может вылезти за rounded corners, но
            // axis labels ОТКЛ paper-card edges (через CSS-var paddings), не
            // достают углов.
            padding: '0 0 12px 0',
          }}
        >
          {error ? (
            <div
              className="flex items-center justify-center"
              style={{
                height: `${chartHeight}px`,
                color: 'var(--text-secondary)',
                fontSize: 'var(--fs-sm)',
                padding: 'var(--sp-4)',
                textAlign: 'center',
              }}
            >
              <div>
                <div className="font-bold mb-2">Не удалось загрузить данные</div>
                <div style={{ fontSize: 'var(--fs-xs)', opacity: 0.8 }}>{error}</div>
              </div>
            </div>
          ) : (
            <StackedBidirectionalHistogram
              periods={data?.periods ?? []}
              categories={visibleCategories}
              unit={data?.unit ?? 'млрд руб.'}
              height={chartHeight}
              loading={loading}
            />
          )}
        </div>
      </div>{/* /editorial-frame */}

      {/* ═══ Таблица «Участники» — match с pattern FundsTable.
          Header c title и счётчиком, чекбокс toggle visibility, dot + name + description. */}
      {data && data.categories.length > 0 && (
        <div
          className="mt-6 rounded-2xl overflow-hidden editorial-frame"
          style={{ background: 'var(--bg-secondary)', padding: 0 }}
        >
          <div
            className="border-b border-theme flex items-center justify-between"
            style={{ padding: 'var(--sp-3) var(--sp-4)' }}
          >
            <h3 className="font-semibold" style={{ fontSize: 'var(--fs-base)' }}>
              Участники биржи
            </h3>
            <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
              <span className="text-theme-secondary" style={{ fontSize: 'var(--fs-sm)' }}>
                Видно на графике:
              </span>
              <span className="font-mono font-bold" style={{ color: 'var(--accent)', fontSize: 'var(--fs-sm)' }}>
                {visibleCategories.length} из {data.categories.length}
              </span>
            </div>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full" style={{ fontSize: 'var(--fs-sm)' }}>
              <thead>
                <tr className="text-theme-secondary text-left">
                  <th className="px-4 py-3 font-medium w-10"></th>
                  <th className="px-4 py-3 font-medium">Категория</th>
                  <th className="px-4 py-3 font-medium">Описание</th>
                </tr>
              </thead>
              <tbody>
                {data.categories.map((cat) => {
                  const isHidden = hiddenCategories.has(cat);
                  const isLastVisible = !isHidden && visibleCategories.length === 1;
                  const color = getCategoryColor(cat, theme);
                  const info = getCategoryInfo(cat);
                  return (
                    <tr
                      key={cat}
                      className={`border-t border-theme transition-colors ${
                        isHidden ? 'opacity-50' : 'hover:bg-white/5'
                      }`}
                    >
                      <td className="px-4 py-3">
                        <div
                          className={`flex items-center justify-center w-8 h-8 rounded-lg transition-colors ${
                            isLastVisible ? 'cursor-not-allowed' : 'hover:bg-white/5 cursor-pointer'
                          }`}
                          onClick={() => {
                            if (!isLastVisible) toggleCategory(cat);
                          }}
                          title={isLastVisible ? 'Нельзя скрыть последнюю видимую категорию' : ''}
                        >
                          <input
                            type="checkbox"
                            checked={!isHidden}
                            onChange={() => {}}
                            disabled={isLastVisible}
                            className="w-4 h-4 rounded border-theme cursor-pointer"
                            style={{ accentColor: 'var(--accent)' }}
                          />
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <span
                            className="legend-dot flex-shrink-0"
                            style={{ backgroundColor: color }}
                          />
                          <span className="font-medium">{cat}</span>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-theme-secondary" style={{ lineHeight: 1.4 }}>
                        {info}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
