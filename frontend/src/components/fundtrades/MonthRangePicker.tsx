// MonthRangePicker — произвольный диапазон месяцев для блока «Сделки фондов»:
// кнопка-календарь справа от пресетов 1М/6М/1Г/3Г, по клику — сетка месяцев по годам.
//
// Семантика диапазона ровно та же, что у пресетов: сравниваем снапшот месяца «с»
// (база) со снапшотом месяца «по» (цель). Подпись «мар – май 2024» читается как
// «что изменилось между составом на конец марта и составом на конец мая» — поэтому
// пресет 1М и диапазон апр→май дают одинаковый результат.
//
// Первый клик — просто якорь, направление задаёт второй: кликнул позже якоря —
// якорь стал базой, кликнул раньше — якорь стал целью. Месяцы без снапшота серые
// и не кликаются; месяцы под тарифной задержкой (Free/гость) — с замочком, клик
// уходит в onLockedClick.
//
// Порядок в сетке ПЕРЕВЁРНУТ (год свежее — выше, внутри года дек → янв): так время
// монотонно растёт вверх, декабрь встаёт под январём следующего года, и диапазон
// через границу года подсвечивается сплошной полосой, а не двумя кусками в разных
// концах блоков.
//
// variant='inline' — без кнопки и поповера (сетка прямо в мобильном sheet'е).

import { useEffect, useMemo, useRef, useState } from 'react';
import { CalendarRange, Lock } from 'lucide-react';
import { useTierAccess } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';

/** Границы диапазона — ISO-даты снапшотов из available_months (месяц-энды). */
export interface MonthRange { from: string; to: string }

const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'май', 'июн',
    'июл', 'авг', 'сен', 'окт', 'ноя', 'дек'];

// Порядок ячеек в сетке — дек…янв: годы идут свежими сверху, поэтому и месяцы
// внутри года должны убывать. Тогда порядок чтения (слева направо, сверху вниз)
// монотонен по времени и диапазон всегда сплошной, без разрыва на стыке годов.
const MONTH_ORDER = Array.from({ length: 12 }, (_, i) => 11 - i);

// Ячейки года по строкам сетки (4 в ряд), пустые строки выброшены: год, у которого
// данные начинаются с середины (или у выбранных УК их часть отсутствует), не должен
// занимать ряды пустых клеток. Внутри оставшихся строк дырки остаются пустыми
// местами — иначе месяцы разъедутся по колонкам между годами.
function rowsOf(cells: (string | null)[]): number[] {
    const out: number[] = [];
    for (let i = 0; i < MONTH_ORDER.length; i += 4) {
        const row = MONTH_ORDER.slice(i, i + 4);
        if (row.some((m) => cells[m])) out.push(...row);
    }
    return out;
}

// Год/месяц берём из строки, а не через new Date(): ISO-дата парсится как UTC и в
// западных таймзонах month-end съезжает на месяц назад.
const isoYear = (iso: string) => Number(iso.slice(0, 4));
const isoMonth = (iso: string) => Number(iso.slice(5, 7)) - 1;

/** «мар – май 2024», через границу года — «окт 2023 – май 2024». */
export function monthRangeLabel(fromISO: string, toISO: string): string {
    const fy = isoYear(fromISO), ty = isoYear(toISO);
    const fm = MONTHS_SHORT[isoMonth(fromISO)], tm = MONTHS_SHORT[isoMonth(toISO)];
    return fy === ty ? `${fm} – ${tm} ${ty}` : `${fm} ${fy} – ${tm} ${ty}`;
}

interface Props {
    /** Месяцы с данными (ISO-даты снапшотов, порядок любой). */
    availableMonths: string[];
    /** Текущий диапазон; null = активен пресет. */
    value: MonthRange | null;
    onChange: (r: MonthRange) => void;
    /** Сброс к пресету (кнопка «Сбросить» в подвале сетки). */
    onReset: () => void;
    /** Месяц закрыт тарифной задержкой (Free/гость) — замочек вместо выбора. */
    monthLocked?: (m: string) => boolean;
    onLockedClick?: (m: string) => void;
    variant?: 'popover' | 'inline';
}

export default function MonthRangePicker({
    availableMonths, value, onChange, onReset, monthLocked, onLockedClick, variant = 'popover',
}: Props) {
    const inline = variant === 'inline';
    const [open, setOpen] = useState(false);

    // Свой период — с Basic (матрица, fund_trades.custom_range). Гейт живёт
    // ЗДЕСЬ, в самом пикере: он один на все поверхности — «Сделки» Общего
    // портфеля, карточка фонда и мобильный sheet (variant='inline').
    // `isLoading ||` — пока тариф не резолвнут, замок не вешаем.
    const access = useTierAccess('fund_trades');
    const { showUpgrade } = useUpgradePrompt();
    const canUseRange = access.isLoading || access.canUseFlag('custom_range');
    const promptRangeUpgrade = () => showUpgrade({
        tier: access.requiredTierFor({ flag: 'custom_range' }) ?? 'basic',
        featureName: 'свой период',
        indicator: 'fund_trades',
    });
    // pending — первый выбранный месяц (база), ждём второй клик.
    const [pending, setPending] = useState<string | null>(null);
    const [hover, setHover] = useState<string | null>(null);
    const wrapRef = useRef<HTMLDivElement>(null);

    // Закрытие поповера по клику вне и ESC (как у Dropdown).
    useEffect(() => {
        if (!open || inline) return;
        const onClick = (e: MouseEvent) => {
            if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
        };
        const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
        document.addEventListener('mousedown', onClick);
        document.addEventListener('keydown', onKey);
        return () => {
            document.removeEventListener('mousedown', onClick);
            document.removeEventListener('keydown', onKey);
        };
    }, [open, inline]);

    // Незакрытый выбор не должен «висеть» между открытиями поповера.
    useEffect(() => { if (!open) { setPending(null); setHover(null); } }, [open]);

    // Год → 12 ячеек (ISO-дата снапшота или null, если месяца в данных нет).
    const years = useMemo(() => {
        const map = new Map<number, (string | null)[]>();
        for (const iso of availableMonths) {
            const y = isoYear(iso);
            if (!map.has(y)) map.set(y, Array(12).fill(null));
            const cell = map.get(y)!;
            const m = isoMonth(iso);
            if (!cell[m] || iso > cell[m]!) cell[m] = iso;
        }
        return Array.from(map.entries()).sort((a, b) => b[0] - a[0]);
    }, [availableMonths]);

    const pick = (iso: string) => {
        // Заперт тарифом: сетка открыта и читаема, но выбор месяца ведёт в
        // апселл (в inline-варианте кнопки нет — это единственный вход).
        if (!canUseRange) { promptRangeUpgrade(); return; }
        if (monthLocked?.(iso)) { onLockedClick?.(iso); return; }
        if (!pending) { setPending(iso); return; }
        if (iso === pending) { setPending(null); return; }
        // Первый клик — якорь без роли: второй клик решает, вперёд диапазон или назад.
        onChange(iso < pending ? { from: iso, to: pending } : { from: pending, to: iso });
        setPending(null);
        setOpen(false);
    };

    // Подсветка: концы диапазона залиты, промежуточные месяцы — лёгкий тинт.
    // При незакрытом выборе показываем превью до месяца под курсором — в обе
    // стороны от якоря, ведь направление ещё не выбрано.
    const preview = pending && hover && hover !== pending ? hover : null;
    const edgeA = pending
        ? (preview && preview < pending ? preview : pending)
        : (value?.from ?? null);
    const edgeB = pending
        ? (preview ? (preview < pending ? pending : preview) : null)
        : (value?.to ?? null);
    const isEdge = (iso: string) => iso === edgeA || (!!edgeB && iso === edgeB);
    const inRange = (iso: string) => !!edgeA && !!edgeB && iso > edgeA && iso < edgeB;

    const hint = pending
        ? `${MONTHS_SHORT[isoMonth(pending)]} ${isoYear(pending)} → выберите вторую границу`
        : 'Выберите два месяца — с какого по какой';

    const grid = (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div style={{ fontSize: 'var(--fs-2xs)', fontWeight: 600, color: 'var(--text-muted)', lineHeight: 1.4 }}>
                {hint}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10, maxHeight: inline ? 260 : '52vh', overflowY: 'auto' }}>
                {years.map(([year, cells]) => (
                    <div key={year}>
                        <div style={{ fontSize: 'var(--fs-2xs)', fontWeight: 800, letterSpacing: '0.08em', color: 'var(--text-muted)', marginBottom: 4 }}>
                            {year}
                        </div>
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 4 }}>
                            {rowsOf(cells).map((m) => {
                                const iso = cells[m];
                                // Месяца нет у выбранных УК — держим место в сетке
                                // (иначе колонки годов разъедутся), но не рисуем.
                                if (!iso) return <span key={m} aria-hidden="true" />;
                                const locked = !!monthLocked?.(iso);
                                const edge = isEdge(iso);
                                const mid = inRange(iso);
                                return (
                                    <button
                                        key={m}
                                        type="button"
                                        title={locked ? 'Свежий срез — по подписке' : undefined}
                                        onClick={() => pick(iso)}
                                        onMouseEnter={() => setHover(iso)}
                                        onMouseLeave={() => setHover((h) => (h === iso ? null : h))}
                                        style={{
                                            display: 'inline-flex', alignItems: 'center', justifyContent: 'center', gap: 3,
                                            padding: '5px 2px', borderRadius: 999,
                                            fontSize: 'var(--fs-xs)', fontWeight: edge ? 800 : 600,
                                            border: edge ? '2px solid var(--text-primary)' : '2px solid transparent',
                                            backgroundColor: edge
                                                ? 'var(--accent)'
                                                : mid
                                                    ? 'color-mix(in srgb, var(--accent) 22%, transparent)'
                                                    : 'transparent',
                                            // Тарифный locked не затемняем (2026-08-10) —
                                            // замочек есть, клик ведёт в апселл.
                                            color: edge ? 'var(--text-inverse)' : 'var(--text-primary)',
                                            cursor: 'pointer',
                                            transition: 'background-color 0.12s ease, color 0.12s ease',
                                        }}
                                    >
                                        {MONTHS_SHORT[m]}
                                        {locked && <Lock size={9} className="flex-shrink-0" />}
                                    </button>
                                );
                            })}
                        </div>
                    </div>
                ))}
            </div>
            {(value || pending) && (
                <button
                    type="button"
                    onClick={() => { setPending(null); onReset(); setOpen(false); }}
                    style={{
                        alignSelf: 'flex-start', padding: '4px 10px', borderRadius: 999,
                        border: '2px solid var(--text-primary)', backgroundColor: 'var(--bg-secondary)',
                        color: 'var(--text-primary)', fontSize: 'var(--fs-2xs)', fontWeight: 700, cursor: 'pointer',
                    }}
                >
                    Сбросить
                </button>
            )}
        </div>
    );

    if (inline) return grid;

    return (
        <div ref={wrapRef} style={{ position: 'relative', display: 'inline-flex' }}>
            <button
                type="button"
                // Заперт: поповер не открываем — сразу апселл. Кнопка при этом
                // выглядит обычной (без затемнения), платность выдаёт замочек.
                onClick={() => { if (!canUseRange) { promptRangeUpgrade(); return; } setOpen((o) => !o); }}
                aria-label="Свой период"
                aria-pressed={!!value}
                title={canUseRange
                    ? (value ? `Свой период: ${monthRangeLabel(value.from, value.to)}` : 'Свой период — выбрать месяцы')
                    : 'Свой период — на тарифе Basic или Pro'}
                style={{
                    display: 'inline-flex', alignItems: 'center', justifyContent: 'center', gap: 3,
                    padding: '0 10px', borderRadius: 999,
                    border: '2px solid var(--text-primary)',
                    backgroundColor: value ? 'var(--accent)' : 'var(--bg-secondary)',
                    color: value ? 'var(--text-inverse)' : 'var(--text-primary)',
                    cursor: 'pointer',
                    transition: 'background-color 0.12s ease, color 0.12s ease',
                }}
            >
                <CalendarRange size={16} />
                {!canUseRange && <Lock size={11} strokeWidth={2.4} />}
            </button>
            {open && (
                <div
                    className="rounded-xl"
                    style={{
                        position: 'absolute', top: 'calc(100% + 6px)', right: 0, zIndex: 50,
                        width: 288, padding: 12,
                        backgroundColor: 'var(--bg-secondary)',
                        border: '2px solid var(--text-primary)',
                        boxShadow: 'var(--shadow-hard-chip, 4px 4px 0 var(--text-primary))',
                    }}
                >
                    {grid}
                </div>
            )}
        </div>
    );
}
