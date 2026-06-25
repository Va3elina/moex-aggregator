/**
 * ChannelNews — секция «Новости каналов» на главной (OverviewPage). Карточки
 * последних постов @FrameTool + @Thor_INV (источник channel_posts, ридер
 * signals/channel_scan.py), клик → пост в Telegram. Не рендерится если постов нет.
 */
import { useEffect, useState } from 'react';
import { Send, ExternalLink } from 'lucide-react';
import { getChannelPosts, type ChannelPost } from '../services/api';

function relTime(iso?: string | null): string {
  if (!iso) return '';
  const m = Math.floor((Date.now() - new Date(iso).getTime()) / 60_000);
  if (m < 1) return 'только что';
  if (m < 60) return `${m} мин`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} ч`;
  const d = Math.floor(h / 24);
  return d === 1 ? 'вчера' : `${d} дн`;
}

export default function ChannelNews() {
  const [posts, setPosts] = useState<ChannelPost[]>([]);
  useEffect(() => { getChannelPosts(9).then(setPosts).catch(() => {}); }, []);

  if (posts.length === 0) return null;

  return (
    <section className="mb-10 md:mb-12">
      <div className="flex items-center gap-3 mb-5 md:mb-6">
        <p className="text-xs uppercase"
          style={{ color: 'var(--text-muted)', letterSpacing: '0.12em', fontWeight: 600 }}>
          Новости каналов
        </p>
        <div className="h-px flex-1" style={{ backgroundColor: 'var(--border-color)' }} />
        <a href="https://t.me/FrameTool" target="_blank" rel="noopener noreferrer"
          className="text-xs flex items-center gap-1 transition-opacity hover:opacity-80"
          style={{ color: 'var(--text-secondary)' }}>
          @FrameTool <ExternalLink size={12} />
        </a>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 md:gap-4">
        {posts.map((p) => (
          <a key={p.id} href={p.link} target="_blank" rel="noopener noreferrer"
            className="block rounded-xl p-4 transition-opacity hover:opacity-90"
            style={{ background: 'var(--bg-secondary)', border: '0.5px solid var(--border-color)' }}>
            <div className="flex items-center justify-between mb-2">
              <span className="flex items-center gap-1.5 text-xs"
                style={{ color: 'var(--accent-cyan, #22D3EE)' }}>
                <Send size={12} /> {p.channel_name || p.channel}
              </span>
              <span className="text-xs" style={{ color: 'var(--text-muted)' }}>{relTime(p.posted_at)}</span>
            </div>
            {p.photo_url && (
              <img src={p.photo_url} alt="" loading="lazy"
                onError={(e) => { (e.currentTarget as HTMLImageElement).style.display = 'none'; }}
                className="w-full rounded-lg mb-2" style={{ maxHeight: 150, objectFit: 'cover' }} />
            )}
            {p.text && (
              <p className="text-sm" style={{ color: 'var(--text-primary)', lineHeight: 1.45 }}>
                {p.text.length > 160 ? `${p.text.slice(0, 160)}…` : p.text}
              </p>
            )}
          </a>
        ))}
      </div>
    </section>
  );
}
