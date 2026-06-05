/**
 * ExtensionTokenSection — Profile: токен для браузерного расширения (плавающее
 * окно индикаторов в терминале Т-Инвестиций).
 *
 * UX:
 *   - Только PRO. Не-PRO при попытке генерации → upgrade-модалка (предупреждение).
 *   - Генерация → токен показывается ОДИН раз с copy + «вставьте в popup расширения».
 *   - Список активных токенов + «Отозвать».
 */
import { useEffect, useState } from 'react';
import { MonitorSmartphone, Lock, Plus, Copy, Trash2, AlertCircle } from 'lucide-react';
import {
    listExtensionTokens,
    createExtensionToken,
    revokeExtensionToken,
    type ExtensionTokenInfo,
    type ExtensionTokenCreated,
} from '../../services/api';
import { useAuth } from '../../contexts/AuthContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';

const UPGRADE = { tier: 'pro' as const, featureName: 'Расширение для терминала', indicator: 'api_access' };

export default function ExtensionTokenSection() {
    const { user } = useAuth();
    const { showUpgrade } = useUpgradePrompt();
    // PRO или ADMIN — как на бэкенде (require_pro: role in [pro, admin]).
    const isPro = user?.role === 'pro' || user?.role === 'admin';

    const [tokens, setTokens] = useState<ExtensionTokenInfo[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [created, setCreated] = useState<ExtensionTokenCreated | null>(null);
    const [inFlight, setInFlight] = useState(false);

    const load = async () => {
        try {
            setLoading(true);
            setError(null);
            setTokens(await listExtensionTokens());
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setLoading(false);
        }
    };
    useEffect(() => {
        void load();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const handleGenerate = async () => {
        if (!isPro) {
            showUpgrade(UPGRADE); // не-PRO → предупреждение/upsell
            return;
        }
        setInFlight(true);
        try {
            const c = await createExtensionToken();
            setCreated(c);
            setTokens((prev) => [
                { id: c.id, name: 'Терминал Т-Инвестиций', token_prefix: c.token_prefix, created_at: new Date().toISOString(), last_used_at: null },
                ...prev,
            ]);
        } catch (e) {
            // eslint-disable-next-line no-alert
            alert((e as Error).message);
        } finally {
            setInFlight(false);
        }
    };

    const handleRevoke = async (t: ExtensionTokenInfo) => {
        // eslint-disable-next-line no-alert
        if (!confirm(`Отозвать токен ${t.token_prefix}…? Расширение перестанет работать с ним.`)) return;
        try {
            await revokeExtensionToken(t.id);
            setTokens((prev) => prev.filter((x) => x.id !== t.id));
        } catch (e) {
            // eslint-disable-next-line no-alert
            alert((e as Error).message);
        }
    };

    const copy = (s: string) => navigator.clipboard?.writeText(s);

    return (
        <section>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <MonitorSmartphone size={18} style={{ color: 'var(--text-secondary)' }} />
                <h2 style={{ fontSize: 'var(--fs-lg)', fontWeight: 700, color: 'var(--text-primary)', margin: 0 }}>
                    Расширение для терминала
                </h2>
            </div>
            <p style={{ marginTop: 6, fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                Плавающее окно с индикаторами Фрейм поверх терминала Т-Инвестиций. Сгенерируйте
                токен и вставьте его в popup расширения — индикаторы разблокируются.
            </p>

            {/* Non-Pro баннер */}
            {!isPro && (
                <div
                    style={{
                        marginTop: 12,
                        padding: 14,
                        background: 'color-mix(in srgb, var(--accent) 6%, var(--bg-secondary))',
                        border: '1.5px solid color-mix(in srgb, var(--accent) 30%, transparent)',
                        borderRadius: 10,
                        display: 'flex',
                        alignItems: 'center',
                        gap: 12,
                        flexWrap: 'wrap',
                    }}
                >
                    <Lock size={18} style={{ color: 'var(--accent)', flexShrink: 0 }} />
                    <div style={{ flex: 1, minWidth: 200 }}>
                        <div style={{ fontSize: 'var(--fs-sm)', fontWeight: 700, color: 'var(--text-primary)', marginBottom: 2 }}>
                            Токен — только на тарифе Pro
                        </div>
                        <div style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.45 }}>
                            Оформите Pro, чтобы генерировать токен и пользоваться расширением в терминале.
                        </div>
                    </div>
                    <button
                        onClick={() => showUpgrade(UPGRADE)}
                        style={{
                            padding: '6px 14px',
                            background: 'var(--accent)',
                            color: 'var(--text-inverse)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-xs)',
                            fontWeight: 700,
                            cursor: 'pointer',
                            flexShrink: 0,
                        }}
                    >
                        Перейти на Pro
                    </button>
                </div>
            )}

            {/* Показ токена один раз */}
            {created && (
                <div
                    style={{
                        marginTop: 12,
                        padding: 16,
                        background: 'color-mix(in srgb, var(--accent) 8%, var(--bg-secondary))',
                        border: '2px solid var(--accent)',
                        borderRadius: 12,
                    }}
                >
                    <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10, marginBottom: 12 }}>
                        <AlertCircle size={20} style={{ color: 'var(--accent)', flexShrink: 0, marginTop: 2 }} />
                        <div style={{ flex: 1 }}>
                            <p style={{ fontWeight: 700, fontSize: 'var(--fs-sm)', marginBottom: 4, color: 'var(--text-primary)' }}>
                                Скопируйте токен — больше его не покажем
                            </p>
                            <p style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', lineHeight: 1.4 }}>
                                Вставьте его в popup расширения «Фрейм» в браузере. Мы храним только хеш —
                                если потеряете, сгенерируйте новый.
                            </p>
                        </div>
                    </div>
                    <div
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 8,
                            padding: '10px 14px',
                            background: 'var(--bg-primary)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 8,
                            fontFamily: 'monospace',
                            fontSize: 'var(--fs-sm)',
                            wordBreak: 'break-all',
                        }}
                    >
                        <code style={{ flex: 1 }}>{created.token}</code>
                        <button
                            onClick={() => copy(created.token)}
                            title="Копировать"
                            style={{
                                background: 'var(--bg-secondary)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 6,
                                padding: 6,
                                cursor: 'pointer',
                                color: 'var(--text-primary)',
                                flexShrink: 0,
                            }}
                        >
                            <Copy size={14} />
                        </button>
                    </div>
                    <button
                        onClick={() => setCreated(null)}
                        style={{
                            marginTop: 10,
                            background: 'transparent',
                            border: 'none',
                            color: 'var(--text-secondary)',
                            cursor: 'pointer',
                            fontSize: 'var(--fs-xs)',
                            textDecoration: 'underline',
                        }}
                    >
                        Я сохранил, скрыть
                    </button>
                </div>
            )}

            {/* Кнопка генерации */}
            <div style={{ marginTop: 12 }}>
                <button
                    onClick={handleGenerate}
                    disabled={inFlight}
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 6,
                        padding: '8px 16px',
                        background: 'var(--bg-secondary)',
                        color: 'var(--text-primary)',
                        border: '1.5px solid var(--text-primary)',
                        borderRadius: 999,
                        fontSize: 'var(--fs-sm)',
                        fontWeight: 700,
                        cursor: inFlight ? 'wait' : 'pointer',
                    }}
                >
                    <Plus size={14} />
                    {inFlight ? 'Генерируем…' : 'Сгенерировать токен'}
                </button>
            </div>

            {/* Список */}
            <div style={{ marginTop: 16 }}>
                {loading && <p style={{ color: 'var(--text-muted)' }}>Загружаем…</p>}
                {error && <p style={{ color: 'var(--funds-flow-negative)' }}>{error}</p>}
                {!loading && tokens.length === 0 && (
                    <p style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>Токенов пока нет.</p>
                )}
                {tokens.map((t) => (
                    <div
                        key={t.id}
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 12,
                            padding: '10px 14px',
                            borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                        }}
                    >
                        <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{ fontWeight: 700, fontSize: 'var(--fs-sm)', color: 'var(--text-primary)' }}>
                                {t.name || '(без имени)'}
                            </div>
                            <div style={{ fontFamily: 'monospace', fontSize: 'var(--fs-xs)', color: 'var(--text-secondary)', marginTop: 2 }}>
                                {t.token_prefix}…
                            </div>
                            <div style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', marginTop: 2 }}>
                                Создан {new Date(t.created_at).toLocaleDateString('ru-RU')}
                                {t.last_used_at && <> · Использован {new Date(t.last_used_at).toLocaleDateString('ru-RU')}</>}
                            </div>
                        </div>
                        <button
                            onClick={() => handleRevoke(t)}
                            title="Отозвать"
                            style={{
                                background: 'transparent',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 8,
                                padding: '6px 10px',
                                cursor: 'pointer',
                                color: 'var(--text-primary)',
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: 4,
                                fontSize: 'var(--fs-xs)',
                            }}
                        >
                            <Trash2 size={12} />
                            Отозвать
                        </button>
                    </div>
                ))}
            </div>
        </section>
    );
}
