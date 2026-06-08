/**
 * TelegramAlertsSection — центр управления Telegram-алертами в ЛК.
 * Закрывает «синхронизацию аккаунта с Telegram» + edge-кейсы:
 *  - статус (подключён @X / не подключён) + Подключить / Отвязать / Переподключить;
 *  - предупреждение «N алертов не придут, пока не подключён»;
 *  - список алертов (актив · условие · статус) + пауза/возобновить/удалить;
 *  - tier-aware: Free (quota 0) → upgrade на Basic.
 * Рендерится внутри карточки ProfilePage (как ExtensionTokenSection).
 */
import { useCallback, useEffect, useState, type CSSProperties } from 'react';
import { Bell, ExternalLink, Send, Trash2, Pause, Play, AlertTriangle } from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, unlinkTelegram,
    listAlerts, deleteAlert, deleteAllAlerts, setAlertStatus,
    type AlertInfo,
} from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';
import MessengerChoice from '../alerts/MessengerChoice';

/** Inline-глиф колокольчика — повторяет кнопку-колокол с индикаторов (Bell в обведённом
 *  кружке), чтобы текст «кнопкой …» указывал на реальный контрол, а не на эмодзи. */
const BellGlyph = () => (
    <span style={{
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        width: 18, height: 18, borderRadius: '50%', margin: '0 2px',
        border: '1.5px solid var(--text-primary)', background: 'var(--bg-secondary)',
        color: 'var(--text-primary)', verticalAlign: '-4px',
    }} aria-label="колокол алертов">
        <Bell size={11} strokeWidth={2.2} />
    </span>
);

// Editorial-чип для текстовых действий (Переподключить/Отвязать/назад) — даёт
// press-анимацию (translate+hard-shadow) и ≥36px тач-таргет, как везде в проекте.
const chipBtn: CSSProperties = {
    padding: '7px 12px', borderRadius: 8, border: '1.5px solid var(--text-primary)',
    background: 'var(--bg-primary)', fontSize: 'var(--fs-sm)', lineHeight: 1.2,
};
// Квадратная icon-кнопка 36×36 для pause/resume/delete в списке алертов.
const iconBtn: CSSProperties = {
    width: 36, height: 36, borderRadius: 8, border: '1.5px solid var(--text-primary)',
    background: 'var(--bg-primary)', display: 'inline-flex',
    alignItems: 'center', justifyContent: 'center',
};

const OP_LABEL: Record<string, string> = {
    gt: 'выше', lt: 'ниже', cross_up: '↑ пересечёт', cross_down: '↓ пересечёт',
};
const METRIC_LABEL: Record<string, string> = {
    price: 'Цена', oi_zscore: 'OI z-score',
};
const STATUS_LABEL: Record<string, string> = {
    active: 'Активен', paused: 'Пауза', fired: 'Сработал',
};

function unitFor(a: AlertInfo): string {
    return a.indicator === 'price' ? ' ₽' : a.indicator === 'oi_zscore' ? 'σ' : '';
}

export default function TelegramAlertsSection() {
    const quota = useCommonFeatures().telegram_alerts_quota; // 0 / число / null(∞)
    const { showUpgrade } = useUpgradePrompt();
    const [linked, setLinked] = useState<boolean | null>(null);
    const [username, setUsername] = useState<string | null>(null);
    const [alerts, setAlerts] = useState<AlertInfo[]>([]);
    const [linkUrl, setLinkUrl] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const [msg, setMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);

    const refresh = useCallback(async () => {
        try {
            const s = await getTelegramStatus();
            setLinked(s.linked); setUsername(s.username);
        } catch { setLinked(false); }
        try { setAlerts(await listAlerts()); } catch { /* free / no access */ }
    }, []);

    useEffect(() => { refresh(); }, [refresh]);

    // poll статуса после генерации ссылки (юзер жмёт Start в боте)
    useEffect(() => {
        if (!linkUrl || linked) return;
        const t = setInterval(async () => {
            try {
                const s = await getTelegramStatus();
                if (s.linked) { setLinked(true); setUsername(s.username); setLinkUrl(null); clearInterval(t); }
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
        } catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
        finally { setBusy(false); }
    };
    const handleUnlink = async () => {
        if (!window.confirm('Отвязать Telegram? Алерты перестанут приходить.')) return;
        setBusy(true); setMsg(null);
        try { await unlinkTelegram(); setLinked(false); setUsername(null); setLinkUrl(null); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
        finally { setBusy(false); }
    };
    const toggle = async (a: AlertInfo) => {
        try { await setAlertStatus(a.id, a.status === 'active' ? 'paused' : 'active'); refresh(); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
    };
    const remove = async (a: AlertInfo) => {
        if (!window.confirm('Удалить алерт?')) return;
        try { await deleteAlert(a.id); refresh(); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
    };
    // Массовое удаление — на случай, если случайно создал группу из десятков алертов.
    const removeAll = async () => {
        if (!window.confirm(`Удалить ВСЕ ${alerts.length} алертов? Это необратимо.`)) return;
        try { const r = await deleteAllAlerts(); refresh(); setMsg({ type: 'ok', text: `Удалено ${r.deleted}` }); }
        catch (e) { setMsg({ type: 'err', text: (e as Error).message }); }
    };

    const activeCount = alerts.filter((a) => a.status === 'active').length;
    const link = 'var(--accent)';
    const sub = 'var(--text-secondary)';

    return (
        <div>
            <h2 className="text-lg font-bold mb-1" style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)' }}>
                <Bell size={18} style={{ color: link }} /> Алерты в мессенджере
            </h2>
            <p style={{ color: sub, fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                Уведомления при достижении уровней (цена, аномалии OI). Создаются с индикаторов кнопкой<BellGlyph />.
                Сейчас доступен Telegram, мессенджер&nbsp;МАКС — в&nbsp;разработке.
            </p>

            {quota === 0 ? (
                <button onClick={() => showUpgrade({ tier: 'basic', featureName: 'Алерты в мессенджере', indicator: 'alerts' })}
                    className="editorial-press" style={{ padding: '10px 16px', borderRadius: 10, border: '2px solid var(--text-primary)', background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 600 }}>
                    Доступно на Basic и Pro — улучшить тариф
                </button>
            ) : (
                <>
                    {/* ── Статус Telegram ── */}
                    <div className="rounded-xl p-4 mb-4" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}>
                        {linked === null ? (
                            <span style={{ color: sub }}>Загрузка…</span>
                        ) : linked ? (
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
                                <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <Send size={16} style={{ color: link }} />
                                    <span>Подключён: <b>Telegram</b>{username ? ` · @${username}` : ''}</span>
                                </span>
                                <span style={{ display: 'flex', gap: 'var(--sp-2)' }}>
                                    <button onClick={handleConnect} disabled={busy} title="Сменить чат или мессенджер" className="editorial-press" style={{ ...chipBtn, color: link }}>Переподключить</button>
                                    <button onClick={handleUnlink} disabled={busy} className="editorial-press" style={{ ...chipBtn, color: sub }}>Отвязать</button>
                                </span>
                            </div>
                        ) : linkUrl ? (
                            <div>
                                <a href={linkUrl} target="_blank" rel="noreferrer" className="editorial-press"
                                    style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '8px 14px', borderRadius: 10, border: '2px solid var(--text-primary)', background: 'var(--accent)', color: 'var(--text-inverse)', textDecoration: 'none', fontWeight: 600 }}>
                                    <ExternalLink size={15} /> Открыть @framesignalbot
                                </a>
                                <div style={{ color: sub, fontSize: 'var(--fs-xs)', marginTop: 6 }}>Нажмите Start в боте — статус обновится сам.</div>
                                <button onClick={() => setLinkUrl(null)} className="editorial-press" style={{ ...chipBtn, color: sub, fontSize: 'var(--fs-xs)', marginTop: 'var(--sp-2)' }}>← выбрать другой мессенджер</button>
                            </div>
                        ) : (
                            <div>
                                <div style={{ color: sub, marginBottom: 12 }}>Мессенджер не подключён.</div>
                                <MessengerChoice onTelegram={handleConnect} busy={busy} title={null} />
                            </div>
                        )}
                        {linked === false && activeCount > 0 && (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, color: 'var(--funds-flow-negative, #FF7A5C)', fontSize: 'var(--fs-xs)', marginTop: 8 }}>
                                <AlertTriangle size={14} /> {activeCount} активных алертов не придут, пока не подключите мессенджер.
                            </div>
                        )}
                    </div>

                    {/* ── Список алертов ── */}
                    {alerts.length === 0 ? (
                        <div style={{ color: sub, fontSize: 'var(--fs-sm)' }}>
                            Пока нет алертов. Создайте кнопкой<BellGlyph /> на индикаторе (сейчас — «Открытый интерес»).
                        </div>
                    ) : (
                        <>
                        <ul style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            {alerts.map((a) => (
                                <li key={a.id} className="rounded-xl p-3" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 'var(--sp-2)' }}>
                                    <div style={{ minWidth: 0 }}>
                                        <div style={{ fontWeight: 600, fontSize: 'var(--fs-sm)' }}>{a.asset_name || a.asset}</div>
                                        <div style={{ color: sub, fontSize: 'var(--fs-xs)' }}>
                                            {METRIC_LABEL[a.indicator] || a.indicator} {OP_LABEL[a.op] || a.op} {a.threshold}{unitFor(a)}
                                            {' · '}<span style={{ color: a.status === 'active' ? link : sub }}>{STATUS_LABEL[a.status] || a.status}</span>
                                        </div>
                                    </div>
                                    <div style={{ display: 'flex', gap: 'var(--sp-2)', flexShrink: 0 }}>
                                        {a.status !== 'fired' && (
                                            <button onClick={() => toggle(a)} title={a.status === 'active' ? 'Пауза' : 'Возобновить'} aria-label={a.status === 'active' ? 'Пауза' : 'Возобновить'} className="editorial-press" style={{ ...iconBtn, color: sub }}>
                                                {a.status === 'active' ? <Pause size={16} /> : <Play size={16} />}
                                            </button>
                                        )}
                                        <button onClick={() => remove(a)} title="Удалить" aria-label="Удалить алерт" className="editorial-press" style={{ ...iconBtn, color: 'var(--funds-flow-negative, #FF7A5C)' }}><Trash2 size={16} /></button>
                                    </div>
                                </li>
                            ))}
                        </ul>
                        {alerts.length > 1 && (
                            <button onClick={removeAll} className="editorial-press" style={{ marginTop: 'var(--sp-2)', alignSelf: 'flex-start', background: 'transparent', border: 'none', color: 'var(--funds-flow-negative, #FF7A5C)', fontSize: 'var(--fs-xs)', cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 6, padding: 0 }}>
                                <Trash2 size={14} /> Удалить все ({alerts.length})
                            </button>
                        )}
                        </>
                    )}
                </>
            )}

            {msg && <div style={{ marginTop: 12, fontSize: 'var(--fs-sm)', color: msg.type === 'ok' ? link : 'var(--funds-flow-negative, #FF7A5C)' }}>{msg.text}</div>}
        </div>
    );
}
