/**
 * CreateFundAlertModal — конструктор фонд-алертов «аномальный поток ×N».
 * Упрощённый брат CreateAlertModal (без пикера активов, без clgroup, ТФ всегда
 * дневной): актив = ТЕКУЩАЯ категория страницы «Деньги в фондах», метрика —
 * «аномальный поток» (ATR-кратность дневного net_flow категории).
 *
 * Контракт fund-алерта: indicator='funds_flow', asset∈{money_market|stocks|
 * bonds|gold}, metric='net_flow', op='gt', threshold=×N (кратность), source
 * 'funds' проставляет бэкенд. Требует привязки Telegram (как OI).
 *
 * Inline-styles + CSS-vars (как CreateAlertModal/UpgradeModal — переживает
 * portal/тему).
 */
import { useEffect, useState, type CSSProperties } from 'react';
import { createPortal } from 'react-dom';
import { Bell, X, Check, ExternalLink } from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, createAlert,
    type AlertCreatePayload,
} from '../../services/api';
import MessengerChoice from './MessengerChoice';

interface Props {
    /** Ключ категории — asset фонд-алерта. */
    category: string;          // 'money_market' | 'stocks' | 'bonds' | 'gold'
    /** Человекочитаемое имя категории (для шапки + asset_name). */
    categoryName: string;
    onClose: () => void;
}

const overlay: CSSProperties = {
    position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.5)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 'var(--sp-4)',
};
const card: CSSProperties = {
    background: 'var(--bg-primary)', border: '2px solid var(--text-primary)',
    boxShadow: '5px 5px 0 0 var(--text-primary)', borderRadius: 16, padding: 24,
    width: '100%', maxWidth: 480, maxHeight: '88vh', overflowY: 'auto',
};
const field: CSSProperties = {
    width: '100%', padding: '10px 12px', borderRadius: 10,
    border: '2px solid var(--text-primary)', background: 'var(--bg-secondary)',
    color: 'var(--text-primary)', fontSize: 'var(--fs-sm)',
};
const primaryBtn: CSSProperties = {
    ...field, background: 'var(--accent)', color: 'var(--text-inverse)',
    cursor: 'pointer', fontWeight: 600, textAlign: 'center',
};

// Сегмент-пилл (уровень сигнала / режим) — editorial-стиль. active = целиком
// оранжевый (accent fill + inverse text), иначе бордер + «тень выбора».
const pill = (active: boolean): CSSProperties => ({
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '8px 12px', borderRadius: 10, cursor: 'pointer',
    border: '2px solid var(--text-primary)',
    background: active ? 'var(--accent)' : 'var(--bg-secondary)',
    color: active ? 'var(--text-inverse)' : 'var(--text-primary)',
    boxShadow: active ? '3px 3px 0 0 var(--text-primary)' : 'none',
    transition: 'box-shadow 0.12s, background 0.12s',
    textAlign: 'left', width: '100%',
});

// Уровни сигнала — ступени множителя ATR (×), как у oi_move. «Сильное» по умолчанию.
type SignalLevel = { key: string; label: string; mult: number; freq: string };
const SIGNAL_LEVELS: SignalLevel[] = [
    { key: 'notable', label: 'Заметное', mult: 2, freq: 'случается чаще' },
    { key: 'strong', label: 'Сильное', mult: 3, freq: 'оптимальный баланс' },
    { key: 'extreme', label: 'Экстремальное', mult: 5, freq: 'только редкие всплески' },
];

export default function CreateFundAlertModal({ category, categoryName, onClose }: Props) {
    const [linked, setLinked] = useState<boolean | null>(null);  // null = загрузка
    const [linkUrl, setLinkUrl] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const [msg, setMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);
    const [created, setCreated] = useState(false);

    // Уровень сигнала (множитель ATR) + ввод «Своё значение».
    const [levelKey, setLevelKey] = useState('strong');   // default «Сильное»
    const [customMult, setCustomMult] = useState('');
    const isCustomLevel = levelKey === 'custom';
    const activeLevel = SIGNAL_LEVELS.find((l) => l.key === levelKey);

    // Режим срабатывания: один раз / каждый раз.
    const [mode, setMode] = useState<'once' | 'repeat'>('once');

    useEffect(() => {
        getTelegramStatus().then((s) => setLinked(s.linked)).catch(() => setLinked(false));
    }, []);

    // poll статуса после генерации ссылки (юзер жмёт Start в боте)
    useEffect(() => {
        if (!linkUrl || linked) return;
        const t = setInterval(async () => {
            try {
                const s = await getTelegramStatus();
                if (s.linked) { setLinked(true); clearInterval(t); }
            } catch { /* ignore */ }
        }, 3000);
        return () => clearInterval(t);
    }, [linkUrl, linked]);

    const handleConnect = async () => {
        setBusy(true); setMsg(null);
        try {
            const { deep_link } = await createTelegramLink();
            setLinkUrl(deep_link);
            window.open(deep_link, '_blank');
        } catch (e) {
            setMsg({ type: 'err', text: (e as Error).message });
        } finally { setBusy(false); }
    };

    // Множитель ступени (числовое значение порога).
    const resolvedMult = (): number | null => {
        if (isCustomLevel) {
            const v = parseFloat(customMult.replace(',', '.'));
            return Number.isNaN(v) ? null : v;
        }
        return activeLevel?.mult ?? null;
    };

    const handleCreate = async () => {
        const mult = resolvedMult();
        if (mult == null || mult <= 0) { setMsg({ type: 'err', text: 'Укажите множитель уровня' }); return; }
        setBusy(true); setMsg(null);
        try {
            // Контракт fund-алерта: indicator='funds_flow', asset=категория,
            // metric='net_flow', op='gt', threshold=×N. timeframe всегда '1d'
            // (потоки фондов дневные); source='funds' проставит бэкенд; clgroup нет.
            const payload: AlertCreatePayload = {
                indicator: 'funds_flow',
                asset: category,
                asset_name: categoryName,
                metric: 'net_flow',
                op: 'gt',
                threshold: mult,
                mode,
                timeframe: '1d',
                ...(mode === 'repeat' ? { cooldown_hours: 24 } : {}),
            };
            await createAlert(payload);
            setCreated(true);
        } catch (e) {
            setMsg({ type: 'err', text: (e as Error).message });
        } finally { setBusy(false); }
    };

    // Портал в document.body (как CreateAlertModal): иначе fixed-overlay (z 9999)
    // проваливается под sticky-шапку из-за stacking-контекста родителя.
    return createPortal(
        <div style={overlay} onClick={onClose}>
            <div style={card} onClick={(e) => e.stopPropagation()}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 8, fontWeight: 700, fontSize: 'var(--fs-lg)' }}>
                        <Bell size={20} style={{ color: 'var(--accent)' }} /> Сигнал по фондам
                    </span>
                    <button onClick={onClose} aria-label="Закрыть" className="editorial-press" style={{ color: 'var(--text-secondary)', width: 36, height: 36, borderRadius: 8, display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}><X size={20} /></button>
                </div>

                {created ? (
                    <div style={{ textAlign: 'center', padding: '12px 0' }}>
                        <Check size={40} style={{ color: 'var(--accent)', margin: '0 auto 8px', display: 'block' }} />
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>Сигнал создан</div>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                            Уведомление придёт в Telegram (@framesignalbot), когда поток в фонды «{categoryName}» станет аномальным.
                        </div>
                        <button onClick={onClose} className="editorial-press" style={{ ...primaryBtn, width: 'auto', padding: '10px 24px', margin: '0 auto', display: 'inline-block' }}>Готово</button>
                    </div>
                ) : linked === null ? (
                    <div style={{ color: 'var(--text-secondary)', textAlign: 'center', padding: '20px 0' }}>Загрузка…</div>
                ) : !linked ? (
                    <div>
                        {linkUrl ? (
                            <>
                                <p style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 12, lineHeight: 1.5 }}>
                                    Откройте бота, нажмите <b>Start</b> — и вернитесь сюда. Статус обновится сам.
                                </p>
                                <a href={linkUrl} target="_blank" rel="noreferrer" className="editorial-press"
                                    style={{ ...primaryBtn, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8, textDecoration: 'none', marginBottom: 8 }}>
                                    <ExternalLink size={16} /> Открыть @framesignalbot
                                </a>
                                <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-xs)', textAlign: 'center' }}>Ждём подключения…</div>
                            </>
                        ) : (
                            <>
                                <p style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)', marginBottom: 16, lineHeight: 1.5 }}>
                                    Чтобы получать сигналы, подключите мессенджер. После этого все ваши алерты будут приходить туда.
                                </p>
                                <MessengerChoice onTelegram={handleConnect} busy={busy} />
                            </>
                        )}
                    </div>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                        {/* ── АКТИВ (текущая категория, не пикер) ── */}
                        <div style={{ color: 'var(--text-secondary)', fontSize: 'var(--fs-sm)' }}>
                            Категория: <b style={{ color: 'var(--text-primary)' }}>{categoryName}</b>
                        </div>

                        {/* ── УРОВЕНЬ СИГНАЛА — ступени множителя ATR ── */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Уровень сигнала</div>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                                {SIGNAL_LEVELS.map((l) => {
                                    const checked = levelKey === l.key;
                                    return (
                                        <button
                                            key={l.key}
                                            type="button"
                                            onClick={() => setLevelKey(l.key)}
                                            className="editorial-press"
                                            style={pill(checked)}
                                        >
                                            <span style={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
                                                <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700 }}>
                                                    {l.label}
                                                    <span style={{ fontWeight: 600, opacity: 0.85 }}> · {l.mult}×</span>
                                                </span>
                                                <span style={{ fontSize: 'var(--fs-xs)', opacity: checked ? 0.85 : 1, color: checked ? 'var(--text-inverse)' : 'var(--text-secondary)' }}>{l.freq}</span>
                                            </span>
                                        </button>
                                    );
                                })}
                                {/* Своё значение — пилл + числовой ввод × раскрывается при выборе */}
                                <button
                                    type="button"
                                    onClick={() => setLevelKey('custom')}
                                    className="editorial-press"
                                    style={pill(isCustomLevel)}
                                >
                                    <span style={{ fontSize: 'var(--fs-sm)', fontWeight: 700 }}>Своё значение</span>
                                    {isCustomLevel && (
                                        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, marginLeft: 'auto' }}>
                                            <input
                                                type="text" inputMode="decimal" value={customMult}
                                                onChange={(e) => { setCustomMult(e.target.value); setLevelKey('custom'); }}
                                                onClick={(e) => { e.stopPropagation(); setLevelKey('custom'); }}
                                                placeholder="4"
                                                style={{ ...field, width: 64, padding: '6px 8px', textAlign: 'right' }}
                                            />
                                            <span style={{ fontSize: 'var(--fs-sm)' }}>×</span>
                                        </span>
                                    )}
                                </button>
                            </div>
                        </div>

                        {/* ── РЕЖИМ — один раз / каждый раз ── */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>Режим</div>
                            <div style={{ display: 'flex', gap: 6 }}>
                                {([
                                    { value: 'once', label: 'Один раз' },
                                    { value: 'repeat', label: 'Каждый раз' },
                                ] as const).map((m) => {
                                    const checked = mode === m.value;
                                    return (
                                        <button
                                            key={m.value}
                                            type="button"
                                            onClick={() => setMode(m.value)}
                                            className="editorial-press"
                                            style={{ ...pill(checked), justifyContent: 'center', fontSize: 'var(--fs-sm)', fontWeight: 700 }}
                                        >
                                            {m.label}
                                        </button>
                                    );
                                })}
                            </div>
                        </div>

                        {/* ── ТЕКСТ ФОРМУЛЫ ── */}
                        <div style={{
                            borderTop: '1px solid var(--border-color)', paddingTop: 12,
                            fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.5,
                        }}>
                            <div style={{ fontWeight: 700, color: 'var(--text-primary)', marginBottom: 4 }}>
                                Как считается аномальный поток
                            </div>
                            Берётся чистый дневной приток-отток денег в фонды категории и делится на
                            средний дневной шаг за последние 14 дней. Получается «во сколько раз
                            сегодняшний поток больше обычного»: 1 — обычный день, 3 — втрое сильнее
                            обычного. Сигнал придёт, когда |поток| превысит выбранную кратность —
                            и в тексте укажет направление (приток или отток).
                        </div>

                        {/* Сводка */}
                        <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)' }}>
                            Сигнал: <b style={{ color: 'var(--text-primary)' }}>{categoryName}</b>
                            {' · поток ≥ '}
                            <b style={{ color: 'var(--text-primary)' }}>
                                {isCustomLevel ? `${customMult || '—'}×` : `${activeLevel?.mult}×`}
                            </b>
                        </div>

                        <button
                            disabled={busy}
                            onClick={handleCreate}
                            className="editorial-press"
                            style={primaryBtn}
                        >
                            {busy ? 'Создаём…' : 'Создать сигнал'}
                        </button>
                    </div>
                )}

                {msg && (
                    <div style={{ marginTop: 12, fontSize: 'var(--fs-sm)', color: msg.type === 'ok' ? 'var(--accent)' : 'var(--funds-flow-negative, #FF7A5C)' }}>
                        {msg.text}
                    </div>
                )}
            </div>
        </div>,
        document.body,
    );
}
