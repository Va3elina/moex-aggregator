/**
 * ApiKeysSection — Profile-страница: management API-ключей (Pro tier).
 *
 * UX:
 *   - List active + revoked keys с metadata (prefix, name, last_used).
 *   - "Создать ключ" → modal с name input.
 *   - После create — показывается plain text **один раз** с copy-button +
 *     красным warning "сохраните, больше не покажем".
 *   - Каждый active key — кнопка "Отозвать" с confirm.
 *
 * Tier-gating: показывает CTA на upgrade для не-Pro, скрывает UI.
 */
import { useEffect, useState } from 'react';
import { Key, Plus, Copy, Trash2, AlertCircle, ExternalLink } from 'lucide-react';
import { Link } from 'react-router-dom';
import {
    listApiKeys,
    createApiKey,
    revokeApiKey,
    type ApiKeyInfo,
    type ApiKeyCreated,
} from '../../services/api';
import { useCommonFeatures } from '../../contexts/TierFeaturesContext';
import { useUpgradePrompt } from '../tier/UpgradeModal';

export default function ApiKeysSection() {
    const common = useCommonFeatures();
    const { showUpgrade } = useUpgradePrompt();
    const [keys, setKeys] = useState<ApiKeyInfo[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Create flow
    const [creating, setCreating] = useState(false);
    const [newName, setNewName] = useState('');
    const [createdKey, setCreatedKey] = useState<ApiKeyCreated | null>(null);
    const [creatingInFlight, setCreatingInFlight] = useState(false);

    const load = async () => {
        if (!common.api_access) {
            setLoading(false);
            return;
        }
        try {
            setLoading(true);
            setError(null);
            const list = await listApiKeys();
            setKeys(list);
        } catch (e) {
            setError((e as Error).message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        void load();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [common.api_access]);

    const handleCreate = async () => {
        if (!common.api_access) {
            showUpgrade({ tier: 'pro', featureName: 'API-доступ', indicator: 'api_access' });
            return;
        }
        setCreatingInFlight(true);
        try {
            const created = await createApiKey(newName.trim() || undefined);
            setCreatedKey(created);
            setKeys((prev) => [created, ...prev]);
            setNewName('');
            setCreating(false);
        } catch (e) {
            // eslint-disable-next-line no-alert
            alert((e as Error).message);
        } finally {
            setCreatingInFlight(false);
        }
    };

    const handleRevoke = async (key: ApiKeyInfo) => {
        if (!confirm(`Отозвать ключ ${key.key_prefix}…? Восстановить нельзя.`)) return;
        try {
            await revokeApiKey(key.id);
            setKeys((prev) => prev.map((k) => (k.id === key.id ? { ...k, is_revoked: true } : k)));
        } catch (e) {
            // eslint-disable-next-line no-alert
            alert((e as Error).message);
        }
    };

    const copyToClipboard = (text: string) => {
        navigator.clipboard?.writeText(text);
    };

    // Non-Pro юзер — CTA + краткое описание фичи.
    if (!common.api_access) {
        return (
            <section style={{ marginTop: 32 }}>
                <SectionHeader />
                <div
                    className="editorial-frame"
                    style={{
                        padding: 24,
                        textAlign: 'center',
                        marginTop: 12,
                    }}
                >
                    <Key size={32} style={{ color: 'var(--text-muted)', margin: '0 auto 12px' }} />
                    <p
                        style={{
                            fontSize: 'var(--fs-md)',
                            fontWeight: 700,
                            marginBottom: 8,
                            color: 'var(--text-primary)',
                        }}
                    >
                        API-доступ для автоматизации
                    </p>
                    <p
                        style={{
                            fontSize: 'var(--fs-sm)',
                            color: 'var(--text-secondary)',
                            marginBottom: 16,
                            maxWidth: 480,
                            marginLeft: 'auto',
                            marginRight: 'auto',
                            lineHeight: 1.5,
                        }}
                    >
                        Программный доступ к данным через REST API. Получайте котировки, Силу
                        рынка, ОИ, фонды и сезонность из своих скриптов и торговых ботов.
                    </p>
                    <button
                        onClick={() =>
                            showUpgrade({ tier: 'pro', featureName: 'API-доступ', indicator: 'api_access' })
                        }
                        style={{
                            padding: '8px 18px',
                            background: 'var(--accent)',
                            color: 'var(--text-inverse)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 999,
                            fontSize: 'var(--fs-sm)',
                            fontWeight: 700,
                            cursor: 'pointer',
                            boxShadow: 'var(--shadow-hard-chip)',
                        }}
                    >
                        Перейти на Pro
                    </button>
                </div>
            </section>
        );
    }

    return (
        <section style={{ marginTop: 32 }}>
            <SectionHeader />

            {/* После создания — показать plain key один раз */}
            {createdKey && (
                <div
                    style={{
                        marginTop: 12,
                        padding: 16,
                        background: 'color-mix(in srgb, var(--accent) 8%, var(--bg-secondary))',
                        border: '2px solid var(--accent)',
                        borderRadius: 12,
                    }}
                >
                    <div
                        style={{
                            display: 'flex',
                            alignItems: 'flex-start',
                            gap: 10,
                            marginBottom: 12,
                        }}
                    >
                        <AlertCircle
                            size={20}
                            style={{ color: 'var(--accent)', flexShrink: 0, marginTop: 2 }}
                        />
                        <div style={{ flex: 1 }}>
                            <p
                                style={{
                                    fontWeight: 700,
                                    fontSize: 'var(--fs-sm)',
                                    marginBottom: 4,
                                    color: 'var(--text-primary)',
                                }}
                            >
                                Сохраните ключ — больше его не покажем
                            </p>
                            <p
                                style={{
                                    fontSize: 'var(--fs-xs)',
                                    color: 'var(--text-secondary)',
                                    lineHeight: 1.4,
                                }}
                            >
                                Это ваш единственный шанс скопировать ключ. Мы храним только хеш
                                для проверок. Если потеряете — создайте новый.
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
                        <code style={{ flex: 1 }}>{createdKey.plain_key}</code>
                        <button
                            onClick={() => copyToClipboard(createdKey.plain_key)}
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
                        onClick={() => setCreatedKey(null)}
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

            {/* Create button / inline form */}
            <div style={{ marginTop: 12 }}>
                {!creating ? (
                    <button
                        onClick={() => setCreating(true)}
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
                            cursor: 'pointer',
                        }}
                    >
                        <Plus size={14} />
                        Создать ключ
                    </button>
                ) : (
                    <div
                        style={{
                            display: 'flex',
                            gap: 8,
                            padding: 12,
                            background: 'var(--bg-secondary)',
                            border: '1.5px solid var(--text-primary)',
                            borderRadius: 10,
                        }}
                    >
                        <input
                            value={newName}
                            onChange={(e) => setNewName(e.target.value)}
                            placeholder="Имя ключа (e.g. prod-bot)"
                            maxLength={100}
                            autoFocus
                            style={{
                                flex: 1,
                                padding: '6px 12px',
                                background: 'var(--bg-primary)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 8,
                                color: 'var(--text-primary)',
                                fontSize: 'var(--fs-sm)',
                            }}
                        />
                        <button
                            onClick={handleCreate}
                            disabled={creatingInFlight}
                            style={{
                                padding: '6px 14px',
                                background: 'var(--accent)',
                                color: 'var(--text-inverse)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 999,
                                fontSize: 'var(--fs-sm)',
                                fontWeight: 700,
                                cursor: creatingInFlight ? 'wait' : 'pointer',
                            }}
                        >
                            {creatingInFlight ? 'Создаём…' : 'Создать'}
                        </button>
                        <button
                            onClick={() => {
                                setCreating(false);
                                setNewName('');
                            }}
                            style={{
                                padding: '6px 14px',
                                background: 'transparent',
                                color: 'var(--text-primary)',
                                border: '1.5px solid var(--text-primary)',
                                borderRadius: 999,
                                fontSize: 'var(--fs-sm)',
                                fontWeight: 600,
                                cursor: 'pointer',
                            }}
                        >
                            Отмена
                        </button>
                    </div>
                )}
            </div>

            {/* List */}
            <div style={{ marginTop: 16 }}>
                {loading && <p style={{ color: 'var(--text-muted)' }}>Загружаем…</p>}
                {error && <p style={{ color: 'var(--funds-flow-negative)' }}>{error}</p>}
                {!loading && keys.length === 0 && (
                    <p style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-sm)' }}>
                        Ключей пока нет. Создайте первый чтобы начать использовать API.
                    </p>
                )}
                {keys.map((k) => (
                    <div
                        key={k.id}
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 12,
                            padding: '10px 14px',
                            borderBottom: '1px dashed color-mix(in srgb, var(--text-primary) 12%, transparent)',
                            opacity: k.is_revoked ? 0.5 : 1,
                        }}
                    >
                        <div style={{ flex: 1, minWidth: 0 }}>
                            <div
                                style={{
                                    fontWeight: 700,
                                    fontSize: 'var(--fs-sm)',
                                    color: 'var(--text-primary)',
                                }}
                            >
                                {k.name || '(без имени)'}{' '}
                                {k.is_revoked && (
                                    <span
                                        style={{
                                            fontSize: 'var(--fs-2xs)',
                                            color: 'var(--funds-flow-negative)',
                                            fontWeight: 600,
                                            marginLeft: 6,
                                        }}
                                    >
                                        ОТОЗВАН
                                    </span>
                                )}
                            </div>
                            <div
                                style={{
                                    fontFamily: 'monospace',
                                    fontSize: 'var(--fs-xs)',
                                    color: 'var(--text-secondary)',
                                    marginTop: 2,
                                }}
                            >
                                {k.key_prefix}…
                            </div>
                            <div
                                style={{
                                    fontSize: 'var(--fs-2xs)',
                                    color: 'var(--text-muted)',
                                    marginTop: 2,
                                }}
                            >
                                Создан {new Date(k.created_at).toLocaleDateString('ru-RU')}
                                {k.last_used_at && (
                                    <>
                                        {' · '}
                                        Использован {new Date(k.last_used_at).toLocaleDateString('ru-RU')}
                                    </>
                                )}
                            </div>
                        </div>
                        {!k.is_revoked && (
                            <button
                                onClick={() => handleRevoke(k)}
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
                        )}
                    </div>
                ))}
            </div>

            {/* Docs link */}
            <div style={{ marginTop: 16 }}>
                <Link
                    to="/api-docs"
                    style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 4,
                        fontSize: 'var(--fs-sm)',
                        color: 'var(--accent)',
                        textDecoration: 'underline',
                    }}
                >
                    Документация API <ExternalLink size={12} />
                </Link>
            </div>
        </section>
    );
}

function SectionHeader() {
    return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Key size={18} style={{ color: 'var(--text-secondary)' }} />
            <h2
                style={{
                    fontSize: 'var(--fs-lg)',
                    fontWeight: 700,
                    color: 'var(--text-primary)',
                    margin: 0,
                }}
            >
                API-ключи
            </h2>
        </div>
    );
}
