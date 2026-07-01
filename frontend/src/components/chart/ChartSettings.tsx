import { useEffect, useState, type CSSProperties } from 'react';
import { Settings2, X, LineChart, AreaChart } from 'lucide-react';
import { useChartPalette, type ChartPalette } from '../../hooks/useChartPalette';

export type ChartType = 'line' | 'area';

interface Props {
  /** Тип отображения основной (линейной) серии. Передаётся ТОЛЬКО для графиков,
   *  где смена типа применима (линия ↔ область). Для гистограмм/treemap — не задаём. */
  chartType?: ChartType;
  onChartType?: (t: ChartType) => void;
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
// Сегмент-опция (тип графика) — editorial pill. active = accent-заливка + тень.
const optionPill = (active: boolean): CSSProperties => ({
  display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8, flex: 1,
  padding: '10px 14px', borderRadius: 10, cursor: 'pointer',
  border: '2px solid var(--text-primary)',
  background: active ? 'var(--accent)' : 'var(--bg-secondary)',
  color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
  boxShadow: active ? '3px 3px 0 0 var(--text-primary)' : 'none',
  transition: 'box-shadow 0.12s, background 0.12s',
  fontSize: 'var(--fs-sm)', fontWeight: 600,
});

const CHART_TYPES: { key: ChartType; label: string; Icon: typeof LineChart }[] = [
  { key: 'line', label: 'Линия', Icon: LineChart },
  { key: 'area', label: 'Область', Icon: AreaChart },
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
 * Все опции кастомизации графика живут ЗДЕСЬ (не россыпью тумблеров, не дефолтами).
 * Пока: тип графика (Линия/Область). Задел под палитры / свечи-Хайкен-Аши — новые
 * секции ниже. Состояние настроек живёт у родителя (persisted), сюда приходит пропом.
 */
export default function ChartSettings({ chartType, onChartType, className = '' }: Props) {
  const [open, setOpen] = useState(false);
  const [palette, setPalette] = useChartPalette();

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

            {chartType && onChartType && (
              <>
                <div style={sectionLabel}>Тип графика</div>
                <div style={{ display: 'flex', gap: 10 }}>
                  {CHART_TYPES.map(({ key, label, Icon }) => (
                    <button
                      key={key}
                      type="button"
                      className="editorial-press"
                      onClick={() => onChartType(key)}
                      style={optionPill(chartType === key)}
                      aria-pressed={chartType === key}
                    >
                      <Icon size={18} /> {label}
                    </button>
                  ))}
                </div>
              </>
            )}

            {/* Палитра — ГЛОБАЛЬНАЯ (accessibility), всегда доступна на любом графике.
                Несколько схем: одна не покрывает все типы дальтонизма. */}
            <div style={{ ...sectionLabel, marginTop: chartType && onChartType ? 22 : 0 }}>Палитра</div>
            <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', margin: '0 0 10px', lineHeight: 1.4 }}>
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
          </div>
        </div>
      )}
    </>
  );
}
