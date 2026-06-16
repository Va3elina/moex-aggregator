/**
 * AlertFiresList — история сработавших сигналов по одному алерту.
 * Раскрывается по клику на «N сигналов» в строке алерта (TelegramAlertsSection).
 * Лениво подгружает страницу через getAlertFires(id) и показывает дату + значение
 * (+ опц. message_text). Пагинация «показать ещё» по total из X-Total-Count.
 *
 * Кодируется ПРОТИВ контракта Agent A (services/api.ts):
 *   getAlertFires(id, {limit, offset}) → { items: AlertFire[]; total: number }
 */
import { useCallback, useEffect, useState } from 'react';
import { getAlertFires, type AlertFire } from '../../services/api';

const PAGE = 20;

/** Дата сигнала → «16 июн, 14:30». Без секунд — компактно для списка. */
function fmtФired(iso: string | null): string {
    if (!iso) return '—';
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return '—';
    return d.toLocaleString('ru-RU', {
        day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit',
    });
}

/** Значение в момент срабатывания + единица (наследуется от родителя). */
function fmtValue(v: number | null, unit: string): string {
    if (v === null || v === undefined || Number.isNaN(v)) return '';
    const s = Math.abs(v) >= 100 ? v.toFixed(0) : v.toFixed(2);
    return `${s}${unit}`;
}

interface Props {
    alertId: number;
    /** Единица значения (' ₽' / 'σ' / '') — берётся из unitFor родителя. */
    unit: string;
}

export default function AlertFiresList({ alertId, unit }: Props) {
    const [items, setItems] = useState<AlertFire[]>([]);
    const [total, setTotal] = useState(0);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const sub = 'var(--text-secondary)';

    const loadPage = useCallback(async (offset: number) => {
        setLoading(true); setError(null);
        try {
            const page = await getAlertFires(alertId, { limit: PAGE, offset });
            setTotal(page.total);
            setItems((prev) => (offset === 0 ? page.items : [...prev, ...page.items]));
        } catch (e) {
            setError((e as Error).message || 'Не удалось загрузить историю');
        } finally {
            setLoading(false);
        }
    }, [alertId]);

    useEffect(() => { loadPage(0); }, [loadPage]);

    if (error) {
        return (
            <div style={{ color: 'var(--funds-flow-negative, #FF7A5C)', fontSize: 'var(--fs-xs)', padding: '8px 0' }}>
                {error}
            </div>
        );
    }

    if (loading && items.length === 0) {
        return <div style={{ color: sub, fontSize: 'var(--fs-xs)', padding: '8px 0' }}>Загрузка истории…</div>;
    }

    if (items.length === 0) {
        return <div style={{ color: sub, fontSize: 'var(--fs-xs)', padding: '8px 0' }}>Сигналов ещё не было.</div>;
    }

    return (
        <div style={{ marginTop: 'var(--sp-2)', paddingTop: 'var(--sp-2)', borderTop: '1px dashed var(--border-color)' }}>
            <ul style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {items.map((f) => (
                    <li key={f.id} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 'var(--sp-2)', fontSize: 'var(--fs-xs)' }}>
                            <span style={{ color: sub }}>{fmtФired(f.fired_at)}</span>
                            <span style={{ fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{fmtValue(f.value, unit)}</span>
                        </div>
                        {f.message_text && (
                            <div style={{ color: sub, fontSize: 'var(--fs-xs)', lineHeight: 1.3, opacity: 0.85 }}>
                                {f.message_text}
                            </div>
                        )}
                    </li>
                ))}
            </ul>
            {items.length < total && (
                <button
                    onClick={() => loadPage(items.length)}
                    disabled={loading}
                    className="editorial-press"
                    style={{
                        marginTop: 'var(--sp-2)', background: 'transparent', border: 'none', padding: 0,
                        color: 'var(--accent)', fontSize: 'var(--fs-xs)', cursor: 'pointer',
                    }}
                >
                    {loading ? 'Загрузка…' : `Показать ещё (${total - items.length})`}
                </button>
            )}
        </div>
    );
}
