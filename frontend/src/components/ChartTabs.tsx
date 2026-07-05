/**
 * ChartTabs — вкладки выбора над графиком (editorial folder-tabs).
 *
 * Активная вкладка сливается с панелью (`.editorial-frame.has-tabs`): та же
 * поверхность (--bg-secondary), без видимой нижней границы. Неактивные затемнены
 * (--text-muted на чуть тёмной поверхности) и отделены границей. Акцент
 * (--accent) НЕ используется — он зарезервирован за контролами отображения
 * внутри панели. Theme-aware через токены (light/dark). Стили — в index.css
 * (`.chart-tabs` / `.chart-tab`).
 *
 * Размещать НЕПОСРЕДСТВЕННО перед `<div className="editorial-frame has-tabs">`.
 */
import type { ElementType } from 'react';

export interface ChartTabItem<K extends string = string> {
    key: K;
    label: string;
    /** Вторая строка ячейки — например тикер индекса (RUSFAR3M). */
    sublabel?: string;
    /** Бейдж-пилюля рядом с label («Beta») — тот же стиль, что бейдж
     *  «BETA» у пунктов главной навигации (accent-рамка, uppercase). */
    badge?: string;
    /** Иконка lucide. */
    Icon?: ElementType;
    disabled?: boolean;
    title?: string;
}

interface ChartTabsProps<K extends string> {
    items: ChartTabItem<K>[];
    value: K;
    onChange: (key: K) => void;
    /** data-tour для онбординга (вешается на контейнер вкладок). */
    tourId?: string;
}

export default function ChartTabs<K extends string>({
    items,
    value,
    onChange,
    tourId,
}: ChartTabsProps<K>) {
    return (
        <div className="chart-tabs" role="tablist" data-tour={tourId}>
            {items.map((it) => {
                const active = it.key === value;
                const Icon = it.Icon;
                return (
                    <button
                        key={it.key}
                        type="button"
                        role="tab"
                        aria-selected={active}
                        disabled={it.disabled}
                        title={it.title ?? it.label}
                        onClick={() => { if (!it.disabled) onChange(it.key); }}
                        className={`chart-tab${active ? ' is-active' : ''}`}
                    >
                        {Icon && <Icon className="ct-ico" aria-hidden="true" />}
                        <span className="ct-name">{it.label}</span>
                        {it.badge && (
                            /* Стиль 1:1 с BETA-бейджем в шапке (Layout NAV) */
                            <span
                                className="uppercase font-bold"
                                style={{
                                    fontSize: '0.62rem',
                                    letterSpacing: '0.06em',
                                    color: 'var(--accent)',
                                    border: '1px solid var(--accent)',
                                    borderRadius: '3px',
                                    padding: '1px 5px',
                                    lineHeight: 1.2,
                                    whiteSpace: 'nowrap',
                                    marginLeft: 6,
                                }}
                            >
                                {it.badge}
                            </span>
                        )}
                        {it.sublabel && <span className="ct-sub">{it.sublabel}</span>}
                    </button>
                );
            })}
        </div>
    );
}
