/**
 * AdminBillingInvites — секция в ProfilePage для админа.
 *
 * Показывается только если user.role === 'admin'.
 * Даёт:
 *   - форму "создать N invite-ссылок" (выбор tier / days / count / note)
 *   - список существующих токенов с кнопками "копировать ссылку" и "отозвать"
 */
import { useEffect, useState } from 'react';
import { Copy, Trash2, Plus, Link2, Check } from 'lucide-react';
import { apiFetch } from '../services/api';

interface Invite {
  token: string;
  tier: string;
  duration_days: number;
  max_uses: number;
  uses_count: number;
  expires_at: string | null;
  revoked_at: string | null;
  note: string | null;
  created_at: string | null;
  is_active: boolean;
}

const TIER_COLORS: Record<string, string> = {
  basic: '#60A5FA',
  pro: '#C8FF2E',
  premium: '#F97316',
};

export default function AdminBillingInvites() {
  const [invites, setInvites] = useState<Invite[]>([]);
  const [loading, setLoading] = useState(true);
  const [createOpen, setCreateOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Форма создания
  const [formTier, setFormTier] = useState('premium');
  const [formDays, setFormDays] = useState(365);
  const [formCount, setFormCount] = useState(1);
  const [formExpires, setFormExpires] = useState(30);
  const [formMaxUses, setFormMaxUses] = useState(1);
  const [formNote, setFormNote] = useState('');
  const [creating, setCreating] = useState(false);

  const [copiedToken, setCopiedToken] = useState<string | null>(null);

  const loadInvites = async () => {
    try {
      const r = await apiFetch('/api/billing/admin/invites');
      if (!r.ok) throw new Error('Не удалось загрузить список');
      const data = await r.json();
      setInvites(data.invites || []);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Ошибка');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadInvites();
  }, []);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreating(true);
    setError(null);
    try {
      const r = await apiFetch('/api/billing/admin/invites', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tier: formTier,
          duration_days: formDays,
          count: formCount,
          expires_in_days: formExpires,
          max_uses_per_token: formMaxUses,
          note: formNote || null,
        }),
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || 'Ошибка создания');
      }
      setCreateOpen(false);
      setFormNote('');
      await loadInvites();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Ошибка');
    } finally {
      setCreating(false);
    }
  };

  const handleRevoke = async (token: string) => {
    if (!confirm('Отозвать эту ссылку? Она перестанет работать навсегда.')) return;
    try {
      const r = await apiFetch(`/api/billing/admin/invites/${token}`, { method: 'DELETE' });
      if (!r.ok) throw new Error('Не удалось отозвать');
      await loadInvites();
    } catch (e) {
      alert(e instanceof Error ? e.message : 'Ошибка');
    }
  };

  const handleCopy = (token: string) => {
    const url = `${window.location.origin}/billing/redeem?token=${token}`;
    navigator.clipboard.writeText(url).then(() => {
      setCopiedToken(token);
      setTimeout(() => setCopiedToken(null), 2000);
    });
  };

  return (
    <div className="rounded-2xl border p-6" style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
          <Link2 size={20} style={{ color: '#F97316' }} />
          Invite-ссылки <span className="text-xs text-theme-muted">(admin)</span>
        </h2>
        <button
          onClick={() => setCreateOpen(!createOpen)}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border"
          style={{
            backgroundColor: createOpen ? 'var(--bg-tertiary)' : 'transparent',
            borderColor: 'var(--border-color)',
            color: 'var(--text-primary)',
          }}
        >
          <Plus size={14} />
          {createOpen ? 'Скрыть форму' : 'Создать'}
        </button>
      </div>

      {/* Форма создания */}
      {createOpen && (
        <form onSubmit={handleCreate} className="rounded-xl border p-4 mb-4 space-y-3"
          style={{ borderColor: 'var(--border-color)', backgroundColor: 'var(--bg-primary)' }}>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-theme-secondary mb-1 block">Тариф</label>
              <select value={formTier} onChange={e => setFormTier(e.target.value)}
                className="w-full px-3 py-2 rounded-lg border text-sm"
                style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', color: 'var(--text-primary)' }}>
                <option value="basic">Basic</option>
                <option value="pro">Pro</option>
              </select>
            </div>
            <div>
              <label className="text-xs text-theme-secondary mb-1 block">Срок подписки (дней)</label>
              <input type="number" name="duration_days" id="invite-duration" min={1} max={3650} value={formDays}
                onChange={e => setFormDays(Number(e.target.value))}
                className="w-full px-3 py-2 rounded-lg border text-sm"
                style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', color: 'var(--text-primary)' }} />
            </div>
            <div>
              <label className="text-xs text-theme-secondary mb-1 block">Сколько ссылок</label>
              <input type="number" name="count" id="invite-count" min={1} max={1000} value={formCount}
                onChange={e => setFormCount(Number(e.target.value))}
                className="w-full px-3 py-2 rounded-lg border text-sm"
                style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', color: 'var(--text-primary)' }} />
            </div>
            <div>
              <label className="text-xs text-theme-secondary mb-1 block">Сам токен истекает через (дней)</label>
              <input type="number" name="expires_days" id="invite-expires" min={1} max={365} value={formExpires}
                onChange={e => setFormExpires(Number(e.target.value))}
                className="w-full px-3 py-2 rounded-lg border text-sm"
                style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', color: 'var(--text-primary)' }} />
            </div>
            <div>
              <label className="text-xs text-theme-secondary mb-1 block">Макс. использований одного токена</label>
              <input type="number" name="max_uses" id="invite-max-uses" min={1} max={10000} value={formMaxUses}
                onChange={e => setFormMaxUses(Number(e.target.value))}
                className="w-full px-3 py-2 rounded-lg border text-sm"
                style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', color: 'var(--text-primary)' }} />
            </div>
            <div>
              <label className="text-xs text-theme-secondary mb-1 block">Заметка (для админа)</label>
              <input type="text" name="note" id="invite-note" value={formNote} onChange={e => setFormNote(e.target.value)}
                placeholder="Для Пети / конкурс 2026"
                className="w-full px-3 py-2 rounded-lg border text-sm"
                style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)', color: 'var(--text-primary)' }} />
            </div>
          </div>
          <button type="submit" disabled={creating}
            className="w-full py-2 rounded-lg font-medium text-sm disabled:opacity-50"
            style={{ backgroundColor: 'var(--accent)', color: 'var(--bg-primary)' }}>
            {creating ? 'Создаём...' : `Создать ${formCount} ссылк${formCount === 1 ? 'у' : 'и'}`}
          </button>
        </form>
      )}

      {error && (
        <div className="mb-4 px-3 py-2 rounded-lg text-sm text-red-300 border border-red-500/40 bg-red-500/10">
          {error}
        </div>
      )}

      {/* Список */}
      {loading ? (
        <div className="text-sm text-theme-muted">Загрузка...</div>
      ) : invites.length === 0 ? (
        <div className="text-sm text-theme-muted text-center py-8">
          Пока нет ни одной ссылки. Создай первую через форму выше.
        </div>
      ) : (
        <div className="space-y-2">
          {invites.map((inv) => {
            const color = TIER_COLORS[inv.tier] || '#9CA3B8';
            return (
              <div key={inv.token}
                className="rounded-xl border p-3 flex items-center gap-3 text-sm"
                style={{
                  borderColor: 'var(--border-color)',
                  opacity: inv.is_active ? 1 : 0.5,
                }}>
                <div
                  className="px-2 py-0.5 rounded text-xs font-bold uppercase shrink-0"
                  style={{ backgroundColor: `${color}26`, color: color }}
                >
                  {inv.tier}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-theme-primary">{inv.duration_days} дней</span>
                    <span className="text-theme-muted">•</span>
                    <span className="text-theme-secondary">
                      {inv.uses_count}/{inv.max_uses} {inv.max_uses === 1 ? 'использ.' : 'исп.'}
                    </span>
                    {inv.note && <>
                      <span className="text-theme-muted">•</span>
                      <span className="text-theme-secondary italic">{inv.note}</span>
                    </>}
                  </div>
                  <div className="text-xs text-theme-muted mt-0.5">
                    {inv.revoked_at ? (
                      <span className="text-red-400">Отозван</span>
                    ) : inv.is_active ? (
                      <span>Истекает {inv.expires_at ? new Date(inv.expires_at).toLocaleDateString('ru-RU') : '?'}</span>
                    ) : (
                      <span>Недействителен</span>
                    )}
                  </div>
                </div>
                {inv.is_active && (
                  <>
                    <button onClick={() => handleCopy(inv.token)}
                      title={copiedToken === inv.token ? 'Скопировано!' : 'Копировать ссылку'}
                      className="p-2 rounded-lg transition-colors hover:bg-theme-tertiary"
                      style={{ color: copiedToken === inv.token ? '#22c55e' : 'var(--text-secondary)' }}>
                      {copiedToken === inv.token ? <Check size={16} /> : <Copy size={16} />}
                    </button>
                    <button onClick={() => handleRevoke(inv.token)}
                      title="Отозвать"
                      className="p-2 rounded-lg transition-colors hover:bg-red-500/10 text-red-400">
                      <Trash2 size={16} />
                    </button>
                  </>
                )}
              </div>
            );
          })}
        </div>
      )}

      <div className="mt-4 text-xs text-theme-muted">
        Ссылки вида{' '}
        <code className="px-1 rounded" style={{ backgroundColor: 'var(--bg-tertiary)' }}>
          /billing/redeem?token=…
        </code>
        {' '}— кликаешь → активируется подписка. Работает и для новых пользователей
        (после регистрации подписка сохраняется).
      </div>
    </div>
  );
}
