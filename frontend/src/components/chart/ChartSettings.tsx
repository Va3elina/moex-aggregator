import { useEffect, useState, type CSSProperties, type ReactNode } from 'react';
import { Settings2, X, LineChart, AreaChart, ChartCandlestick, ChartColumnBig } from 'lucide-react';
import { useChartPalette, type ChartPalette } from '../../hooks/useChartPalette';
import { useChartSettings, OHLC_TYPES, type ChartSeriesType } from '../../hooks/useChartSettings';

interface Props {
  /** Показывать секцию «Тип графика». false — для страниц, где основной чарт
   *  не SimpleChart (treemap/donut/кастомные SVG) и тип не к чему применять;
   *  палитра и штрих-режим остаются (они глобальные). */
  showType?: boolean;
  /** На ЭТОМ графике есть OHLC-данные → свечи/бары/Хайкен-Аши применятся прямо
   *  здесь. false (default) — OHLC-типы в пикере приглушены с пометкой (глобально
   *  выбрать можно, на текущем графике применится линия). */
  ohlcHere?: boolean;
  /** Доп. классы для кнопки-шестерёнки (размер/позиция как у соседних кнопок). */
  className?: string;
}

// Editorial-модалка: overlay + карточка с «жёсткой» тенью 5px 5px 0 (как
// CreateFundAlertModal / UpgradeModal). Стили-константы держим тут же.
const overlay: CSSProperties = {
  position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.5)',
  display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 'var(--sp-4)',
};
const card: CSSProperties = {
  background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
  boxShadow: '5px 5px 0 0 var(--text-primary)', borderRadius: 16, padding: 24,
  width: '100%', maxWidth: 420, maxHeight: '88vh', overflowY: 'auto',
};
const sectionLabel: CSSProperties = {
  fontSize: 'var(--fs-xs)', fontWeight: 700, textTransform: 'uppercase',
  letterSpacing: '0.04em', color: 'var(--text-secondary)', marginBottom: 10,
};
const sectionHint: CSSProperties = {
  fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', margin: '0 0 10px', lineHeight: 1.4,
};
// Сегмент-опция — editorial pill. active = accent-заливка + тень.
// dimmed — опция глобально доступна, но на текущем графике не применится.
const optionPill = (active: boolean, dimmed = false): CSSProperties => ({
  display: 'flex', alignItems: 'center', justifyContent: 'flex-start', gap: 8,
  padding: '9px 12px', borderRadius: 10, cursor: 'pointer',
  border: '2px solid var(--text-primary)',
  background: active ? 'var(--accent)' : 'var(--bg-secondary)',
  color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
  boxShadow: active ? '3px 3px 0 0 var(--text-primary)' : 'none',
  transition: 'box-shadow 0.12s, background 0.12s',
  fontSize: 'var(--fs-sm)', fontWeight: 600,
  opacity: dimmed && !active ? 0.5 : 1,
});

type IconProps = { size?: number };

// Кастомные иконки (в lucide нет OHLC-баров и Хайкен-Аши) — тот же stroke-стиль.
const OhlcBarsIcon = ({ size = 18 }: IconProps) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <path d="M7 4v13M4 8h3M7 12h3M17 7v13M14 15h3M17 11h3" />
  </svg>
);
const HeikinIcon = ({ size = 18 }: IconProps) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
    <path d="M8 3v3M8 16v4M16 5v3M16 17v3" />
    <rect x="5.5" y="6" width="5" height="10" rx="2.2" />
    <rect x="13.5" y="8" width="5" height="9" rx="2.2" />
  </svg>
);

const CHART_TYPES: { key: ChartSeriesType; label: string; Icon: (p: IconProps) => ReactNode }[] = [
  { key: 'line', label: 'Линия', Icon: (p) => <LineChart size={p.size} /> },
  { key: 'area', label: 'Область', Icon: (p) => <AreaChart size={p.size} /> },
  { key: 'columns', label: 'Столбики', Icon: (p) => <ChartColumnBig size={p.size} /> },
  { key: 'candles', label: 'Свечи', Icon: (p) => <ChartCandlestick size={p.size} /> },
  { key: 'bars', label: 'Бары', Icon: OhlcBarsIcon },
  { key: 'heikin', label: 'Хайкен-Аши', Icon: HeikinIcon },
];

// Свотчи (покупки/продажи) для превью в пикере. Для 'default' — представительные
// editorial-dark значения (реальные --oi-* перекрыты при активной палитре). tag —
// человеческое пояснение типа, без клиники.
const PALETTES: { key: ChartPalette; label: string; tag: string; buy: string; sell: string }[] = [
  { key: 'default',    label: 'Обычная',          tag: 'по умолчанию',   buy: '#5BD49C', sell: '#B91C5C' },
  { key: 'redgreen',   label: 'Сине-янтарная',    tag: 'красно-зелёный', buy: '#4C8FBF', sell: '#DE9540' },
  { key: 'blueyellow', label: 'Бирюза-роза',      tag: 'сине-жёлтый',    buy: '#2FA090', sell: '#C6577F' },
  { key: 'contrast',   label: 'Высокий контраст', tag: 'макс. яркость',  buy: '#EFE7D4', sell: '#FF8A5B' },
];

// Карточка палитры — свотчи + подпись, левое выравнивание. active = accent-заливка.
const paletteCard = (active: boolean): CSSProperties => ({
  display: 'flex', alignItems: 'center', gap: 9, textAlign: 'left', cursor: 'pointer',
  padding: '9px 11px', borderRadius: 10, border: '2px solid var(--text-primary)',
  background: active ? 'var(--accent)' : 'var(--bg-secondary)',
  color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
  boxShadow: active ? '3px 3px 0 0 var(--text-primary)' : 'none',
  transition: 'box-shadow 0.12s, background 0.12s',
});
const swatch = (bg: string): CSSProperties => ({
  width: 14, height: 14, borderRadius: 4, background: bg, flex: '0 0 auto',
  boxShadow: 'inset 0 0 0 1px rgba(0,0,0,0.18)',
});

/**
 * ChartSettings — единая кнопка-шестерёнка (в editorial-стиле, под стать
 * ChartCaptureButton: круглая 44×44) + стилизованная модалка настроек графика.
 * ВСЕ настройки ГЛОБАЛЬНЫЕ (одно место → весь сайт → localStorage):
 *  - тип графика (6 видов; OHLC-типы применяются где есть данные, иначе линия);
 *  - палитра доступности (4 схемы под разные типы цветовосприятия);
 *  - штрих-режим (различие линий формой — полная монохромность).
 * Состояние — в useChartSettings/useChartPalette, пропсов-коллбэков нет.
 */
export default function ChartSettings({ showType = true, ohlcHere = false, className = '' }: Props) {
  const [open, setOpen] = useState(false);
  const [palette, setPalette] = useChartPalette();
  const { type, dash, setType, setDash } = useChartSettings();

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open]);

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        data-export-ignore="true"
        className={`editorial-press rounded-full inline-flex items-center justify-center ${className}`}
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '2px solid var(--text-primary)',
          color: 'var(--text-primary)',
          width: 44, height: 44,
        }}
        aria-label="Настройки графика"
        title="Настройки графика"
      >
        <Settings2 size={22} />
      </button>

      {open && (
        <div style={overlay} onClick={() => setOpen(false)} role="dialog" aria-modal="true">
          <div style={card} onClick={(e) => e.stopPropagation()}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
              <span style={{ display: 'flex', alignItems: 'center', gap: 8, fontWeight: 700, fontSize: 'var(--fs-lg)', color: 'var(--text-primary)' }}>
                <Settings2 size={20} /> Настройки графика
              </span>
              <button
                onClick={() => setOpen(false)}
                aria-label="Закрыть"
                className="editorial-press"
                style={{ color: 'var(--text-secondary)', width: 36, height: 36, borderRadius: 8, display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}
              >
                <X size={20} />
              </button>
            </div>

            {showType && (
              <>
                <div style={sectionLabel}>Тип графика</div>
                <p style={sectionHint}>
                  Действует на все графики. Свечи, бары и Хайкен-Аши — там, где есть
                  биржевые данные открытия/максимума/минимума (цена на «Открытых
                  позициях»); на остальных применится линия.
                </p>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                  {CHART_TYPES.map(({ key, label, Icon }) => {
                    const dimmed = !ohlcHere && OHLC_TYPES.includes(key);
                    return (
                      <button
                        key={key}
                        type="button"
                        className="editorial-press"
                        onClick={() => setType(key)}
                        style={optionPill(type === key, dimmed)}
                        aria-pressed={type === key}
                        title={dimmed ? 'На этом графике нет OHLC-данных — применится линия' : undefined}
                      >
                        <Icon size={18} /> {label}
                      </button>
                    );
                  })}
                </div>
              </>
            )}

            {/* Палитра — ГЛОБАЛЬНАЯ (accessibility), всегда доступна на любом графике.
                Несколько схем: одна не покрывает все типы дальтонизма. */}
            <div style={{ ...sectionLabel, marginTop: showType ? 22 : 0 }}>Палитра</div>
            <p style={sectionHint}>
              Выбери, где обе линии (покупки / продажи) хорошо различаешь. Действует на все графики.
            </p>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {PALETTES.map((p) => (
                <button
                  key={p.key}
                  type="button"
                  className="editorial-press"
                  onClick={() => setPalette(p.key)}
                  style={paletteCard(palette === p.key)}
                  aria-pressed={palette === p.key}
                >
                  <span style={{ display: 'flex', gap: 5 }}>
                    <i style={swatch(p.buy)} />
                    <i style={swatch(p.sell)} />
                  </span>
                  <span style={{ display: 'flex', flexDirection: 'column', lineHeight: 1.2, minWidth: 0 }}>
                    <span style={{ fontWeight: 700, fontSize: 'var(--fs-sm)', whiteSpace: 'nowrap' }}>{p.label}</span>
                    <span style={{ fontSize: 11, opacity: 0.7 }}>{p.tag}</span>
                  </span>
                </button>
              ))}
            </div>

            {/* Штрих-режим — если и палитры не помогают (полная монохромность):
                линии различаются ФОРМОЙ (штрих/точки), а не цветом. */}
            <div style={{ ...sectionLabel, marginTop: 22 }}>Линии</div>
            <p style={sectionHint}>
              Если цвета не различаются совсем: вторая линия — штрихом, третья — точками.
              Различие формой, а не цветом.
            </p>
            <div style={{ display: 'flex', gap: 10 }}>
              <button
                type="button"
                className="editorial-press"
                onClick={() => setDash(false)}
                style={{ ...optionPill(!dash), flex: 1, justifyContent: 'center' }}
                aria-pressed={!dash}
              >
                Сплошные
              </button>
              <button
                type="button"
                className="editorial-press"
                onClick={() => setDash(true)}
                style={{ ...optionPill(dash), flex: 1, justifyContent: 'center' }}
                aria-pressed={dash}
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round">
                  <path d="M3 12h4M10 12h4M17 12h4" />
                </svg>
                Штрихом
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
