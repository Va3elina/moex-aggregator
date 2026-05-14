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

import { useEffect, useRef, useState } from 'react';
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
import ChartCaptureButton from '../components/export/ChartCaptureButton';

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
  const [type, setType] = useState<CbrInstrumentType>('stocks');
  const [data, setData] = useState<CbrFlowsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
              categories={data?.categories ?? []}
              unit={data?.unit ?? 'млрд руб.'}
              height={chartHeight}
              loading={loading}
            />
          )}
        </div>
      </div>{/* /editorial-frame */}
    </div>
  );
}
