/**
 * TelegramAlertsSection — центр управления Telegram-алертами в ЛК.
 * Закрывает «синхронизацию аккаунта с Telegram» + edge-кейсы:
 *  - статус (подключён @X / не подключён) + Подключить / Отвязать / Переподключить;
 *  - предупреждение «N алертов не придут, пока не подключён»;
 *  - список алертов (актив · условие · статус) + пауза/возобновить/удалить;
 *  - tier-aware: Free (quota 0) → upgrade на Basic.
 * Рендерится внутри карточки ProfilePage (как ExtensionTokenSection).
 */
import { useCallback, useEffect, useState } from 'react';
import { Bell, ExternalLink, Check, Trash2, Pause, Play, AlertTriangle } from 'lucide-react';
import {
    getTelegramStatus, createTelegramLink, unlinkTelegram,
    listAlerts, deleteAlert, setAlertStatus,
    type AlertInfo,
} from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';

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

    const activeCount = alerts.filter((a) => a.status === 'active').length;
    const link = 'var(--accent)';
    const sub = 'var(--text-secondary)';

    return (
        <div>
            <h2 className="text-lg font-bold mb-1" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Bell size={18} style={{ color: link }} /> Telegram-алерты
            </h2>
            <p style={{ color: sub, fontSize: 'var(--fs-sm)', marginBottom: 16 }}>
                Уведомления при достижении уровней (цена, аномалии OI). Создаются с индикаторов кнопкой 🔔.
            </p>

            {quota === 0 ? (
                <button onClick={() => showUpgrade({ tier: 'basic', featureName: 'Telegram-алерты', indicator: 'alerts' })}
                    className="editorial-press" style={{ padding: '10px 16px', borderRadius: 10, border: '2px solid var(--text-primary)', background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 600 }}>
                    Доступно на Basic и Pro — улучшить тариф
                </button>
            ) : (
                <>
                    {/* ── Статус Telegram ── */}
                    <div className="rounded-xl p-4 mb-4" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)' }}>
                        {linked === null ? (
                            <span style={{ color: sub }}>Загрузка…</span>
                        ) : linked ? (
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
                                <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                    <Check size={18} style={{ color: link }} />
                                    Подключён{username ? ` · @${username}` : ''}
                                </span>
                                <span style={{ display: 'flex', gap: 8 }}>
                                    <button onClick={handleConnect} disabled={busy} style={{ color: link, fontSize: 'var(--fs-sm)' }}>Переподключить</button>
                                    <button onClick={handleUnlink} disabled={busy} style={{ color: sub, fontSize: 'var(--fs-sm)' }}>Отвязать</button>
                                </span>
                            </div>
                        ) : linkUrl ? (
                            <div>
                                <a href={linkUrl} target="_blank" rel="noreferrer" className="editorial-press"
                                    style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '8px 14px', borderRadius: 10, border: '2px solid var(--text-primary)', background: 'var(--accent)', color: 'var(--text-inverse)', textDecoration: 'none', fontWeight: 600 }}>
                                    <ExternalLink size={15} /> Открыть @framesignalbot
                                </a>
                                <div style={{ color: sub, fontSize: 'var(--fs-xs)', marginTop: 6 }}>Нажмите Start в боте — статус обновится сам.</div>
                            </div>
                        ) : (
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
                                <span style={{ color: sub }}>Telegram не подключён.</span>
                                <button onClick={handleConnect} disabled={busy} className="editorial-press"
                                    style={{ padding: '8px 14px', borderRadius: 10, border: '2px solid var(--text-primary)', background: 'var(--accent)', color: 'var(--text-inverse)', fontWeight: 600 }}>
                                    {busy ? '…' : 'Подключить Telegram'}
                                </button>
                            </div>
                        )}
                        {linked === false && activeCount > 0 && (
                            <div style={{ display: 'flex', alignItems: 'center', gap: 6, color: 'var(--funds-flow-negative, #FF7A5C)', fontSize: 'var(--fs-xs)', marginTop: 8 }}>
                                <AlertTriangle size={14} /> {activeCount} активных алертов не придут, пока не подключите Telegram.
                            </div>
                        )}
                    </div>

                    {/* ── Список алертов ── */}
                    {alerts.length === 0 ? (
                        <div style={{ color: sub, fontSize: 'var(--fs-sm)' }}>
                            Пока нет алертов. Создайте кнопкой 🔔 на индикаторе (сейчас — «Открытый интерес»).
                        </div>
                    ) : (
                        <ul style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                            {alerts.map((a) => (
                                <li key={a.id} className="rounded-xl p-3" style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                                    <div style={{ minWidth: 0 }}>
                                        <div style={{ fontWeight: 600, fontSize: 'var(--fs-sm)' }}>{a.asset_name || a.asset}</div>
                                        <div style={{ color: sub, fontSize: 'var(--fs-xs)' }}>
                                            {METRIC_LABEL[a.indicator] || a.indicator} {OP_LABEL[a.op] || a.op} {a.threshold}{unitFor(a)}
                                            {' · '}<span style={{ color: a.status === 'active' ? link : sub }}>{STATUS_LABEL[a.status] || a.status}</span>
                                        </div>
                                    </div>
                                    <div style={{ display: 'flex', gap: 10, flexShrink: 0 }}>
                                        {a.status !== 'fired' && (
                                            <button onClick={() => toggle(a)} title={a.status === 'active' ? 'Пауза' : 'Возобновить'} style={{ color: sub }}>
                                                {a.status === 'active' ? <Pause size={16} /> : <Play size={16} />}
                                            </button>
                                        )}
                                        <button onClick={() => remove(a)} title="Удалить" style={{ color: 'var(--funds-flow-negative, #FF7A5C)' }}><Trash2 size={16} /></button>
                                    </div>
                                </li>
                            ))}
                        </ul>
                    )}
                </>
            )}

            {msg && <div style={{ marginTop: 12, fontSize: 'var(--fs-sm)', color: msg.type === 'ok' ? link : 'var(--funds-flow-negative, #FF7A5C)' }}>{msg.text}</div>}
        </div>
    );
}
