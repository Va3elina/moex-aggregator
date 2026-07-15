/**
 * AdminContentNewsPage — admin-only Kanban состояний content-пайплайна («Новости»).
 *
 * Пайплайн (реализация — отдельная задача, эта страница только визуализирует
 * состояния): календарь MOEX + TG-каналы-хайп (MarketTwits/newssmartlab) →
 * оценка ИИ → сверка с данными (OI/потоки фондов) → черновик поста → ревью в
 * Telegram-боте → публикация. 8 колонок = 8 статусов content_candidates
 * (db/migrations/022). Источник — api/routers/content_news.py.
 *
 * Карточки одной колонки грузятся независимо (KanbanColumn сам себе fetch'ит) —
 * без общего barrier-запроса, колонки друг от друга не зависят. Клик по
 * карточке → детальная модалка (полный текст/обоснование/тред).
 */
import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Newspaper, RefreshCw, Repeat2, Zap, Link2 } from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import CenteredModalShell from '../components/CenteredModalShell';
import { useAuth } from '../contexts/AuthContext';
import {
  listContentCandidates,
  getContentCandidateDetail,
  updateContentCandidateStatus,
} from '../services/api';
import type {
  ContentCandidate,
  ContentCandidateDetail,
  ContentCandidateStatus,
} from '../services/api';

const COLUMN_ORDER: ContentCandidateStatus[] = [
  'candidate', 'discarded', 'pending', 'draft_ready',
  'no_data', 'in_review', 'published', 'rejected',
];

const STATUS_META: Record<ContentCandidateStatus, { label: string; tint: string }> = {
  candidate:   { label: 'Кандидат',             tint: 'var(--text-secondary)' },
  discarded:   { label: 'Отброшено',            tint: 'var(--danger)' },
  pending:     { label: 'Ждёт подтверждения',    tint: 'var(--warning)' },
  draft_ready: { label: 'Черновик готов',        tint: 'var(--accent)' },
  no_data:     { label: 'Не хватило данных',     tint: 'var(--text-muted)' },
  in_review:   { label: 'На ревью',              tint: 'var(--warning)' },
  published:   { label: 'Опубликовано',          tint: 'var(--success)' },
  rejected:    { label: 'Отклонено',             tint: 'var(--danger)' },
};

// Тред (thread_key) прошивает несколько колонок одной цветной полосой слева —
// так повторные сигналы по одному тикеру визуально связываются на всей доске.
const THREAD_PALETTE = [
  'var(--accent)', 'var(--funds-flow-positive)', 'var(--funds-flow-negative)',
  'var(--warning)', 'var(--success)', 'var(--text-secondary)',
];
function threadColor(key: string): string {
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
  return THREAD_PALETTE[hash % THREAD_PALETTE.length];
}

function daysLeft(expiresAtIso: string | null): number | null {
  if (!expiresAtIso) return null;
  const ms = new Date(expiresAtIso).getTime() - Date.now();
  return Math.ceil(ms / (24 * 3600 * 1000));
}

function fmtRelative(iso: string): string {
  const sec = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (sec < 60) return `${sec}с назад`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} мин`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h}ч`;
  const d = Math.floor(h / 24);
  if (d < 30) return `${d}д`;
  return new Date(iso).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
}

function statusTone(status: ContentCandidateStatus): 'neutral' | 'accent' | 'warning' | 'danger' | 'success' {
  if (status === 'published') return 'success';
  if (status === 'rejected' || status === 'discarded') return 'danger';
  if (status === 'pending' || status === 'in_review') return 'warning';
  if (status === 'draft_ready') return 'accent';
  return 'neutral';
}

// Маленькая дышащая точка — "мониторинг идёт прямо сейчас в фоне", не просто
// статичная пилюля-статус. Цвет параметризуется через CSS-переменную (см.
// .live-dot в index.css), respects prefers-reduced-motion сам через CSS.
function LiveDot({ color, title }: { color: string; title: string }) {
  return (
    <span
      className="live-dot inline-block rounded-full flex-shrink-0"
      style={{
        width: 6, height: 6, marginRight: 5,
        backgroundColor: color,
        ['--live-dot-color' as string]: color,
      } as React.CSSProperties}
      title={title}
    />
  );
}

// Доска живая — событийный пайплайн реально двигает карточки между колонками
// в фоне (Шаг А/Б/Б′/В стреляют сразу, не ждут открытия страницы). Без
// авто-обновления это было незаметно: доска выглядела статичной, хотя под
// капотом всё движется. 40с — тот же порядок, что и в исходном плане
// («polling 30-60с, без SSE» — нагрузка на горстку карточек в день не требует
// сложности вебсокетов).
const AUTO_REFRESH_MS = 40_000;

export default function AdminContentNewsPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [refreshTick, setRefreshTick] = useState(0);
  const [selectedId, setSelectedId] = useState<number | null>(null);

  // Guard: только admin (тот же паттерн, что AdminStatsPage — silent redirect,
  // реальный гейт всё равно на бэке через require_admin).
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') {
      navigate('/', { replace: true });
    }
  }, [authLoading, user, navigate]);

  useEffect(() => {
    const id = setInterval(() => setRefreshTick((t) => t + 1), AUTO_REFRESH_MS);
    return () => clearInterval(id);
  }, []);

  if (authLoading || !user || user.role !== 'admin') {
    return null;
  }

  return (
    <div className="max-w-[1600px] mx-auto px-4 md:px-6 py-8 md:py-10">
      {/* Header */}
      <div className="flex items-start gap-3 mb-6 md:mb-8">
        <div
          className="flex items-center justify-center flex-shrink-0"
          style={{
            width: 44, height: 44,
            borderRadius: 'var(--radius-md, 8px)',
            background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
            color: 'var(--accent)',
          }}
        >
          <Newspaper size={22} strokeWidth={1.8} />
        </div>
        <div className="flex-1 min-w-0">
          <h1
            className="text-2xl md:text-3xl font-semibold"
            style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
          >
            Новости
          </h1>
          <p className="text-sm mt-1" style={{ color: 'var(--text-muted)' }}>
            Content-пайплайн · Kanban состояний · только для администратора
          </p>
        </div>
        <button
          type="button"
          onClick={() => setRefreshTick((t) => t + 1)}
          className="editorial-press inline-flex items-center flex-shrink-0 rounded-full font-semibold"
          style={{
            border: '1.5px solid var(--text-primary)',
            backgroundColor: 'transparent',
            color: 'var(--text-primary)',
            padding: 'var(--sp-2) var(--sp-3)',
            fontSize: 'var(--fs-sm)',
            gap: 'var(--sp-1)',
          }}
        >
          <RefreshCw size={14} strokeWidth={2} />
          <span className="hidden sm:inline">Обновить</span>
        </button>
      </div>

      {/* Board — горизонтальный скролл колонок (работает и на мобилке свайпом) */}
      <div className="flex items-start overflow-x-auto" style={{ paddingBottom: 'var(--sp-3)' }}>
        {COLUMN_ORDER.map((status) => (
          <KanbanColumn
            key={status}
            status={status}
            refreshTick={refreshTick}
            onOpenCard={setSelectedId}
          />
        ))}
      </div>

      <CandidateDetailModal
        candidateId={selectedId}
        onClose={() => setSelectedId(null)}
        onNavigate={setSelectedId}
        onChanged={() => setRefreshTick((t) => t + 1)}
      />
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// KANBAN COLUMN
// ════════════════════════════════════════════════════════════════════════════

function KanbanColumn({
  status, refreshTick, onOpenCard,
}: {
  status: ContentCandidateStatus;
  refreshTick: number;
  onOpenCard: (id: number) => void;
}) {
  const meta = STATUS_META[status];
  const [items, setItems] = useState<ContentCandidate[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Карточки, появившиеся в этой колонке или изменившиеся с прошлого фетча —
  // подсвечиваем на секунду-две, чтобы движение конвейера было ВИДНО, а не
  // только теоретически подразумевалось. seenRef переживает ре-рендеры, не
  // триггерит их сам.
  const [freshIds, setFreshIds] = useState<Set<number>>(new Set());
  const seenRef = useRef<Map<number, string>>(new Map());
  const isFirstLoad = useRef(true);

  useEffect(() => {
    setLoading(true);
    setError(null);
    listContentCandidates(status, { limit: 50 })
      .then((r) => {
        setItems(r.items);
        setTotal(r.total);
        if (!isFirstLoad.current) {
          const fresh = new Set<number>();
          for (const it of r.items) {
            const prevStamp = seenRef.current.get(it.id);
            const stamp = it.updated_at || it.created_at || '';
            if (prevStamp === undefined || prevStamp !== stamp) fresh.add(it.id);
          }
          if (fresh.size > 0) {
            setFreshIds(fresh);
            setTimeout(() => setFreshIds(new Set()), 2200);
          }
        }
        isFirstLoad.current = false;
        seenRef.current = new Map(r.items.map((it) => [it.id, it.updated_at || it.created_at || '']));
      })
      .catch((e: Error) => setError(e.message || 'Ошибка загрузки'))
      .finally(() => setLoading(false));
  }, [status, refreshTick]);

  return (
    <div className="flex flex-col flex-shrink-0" style={{ width: 300, marginRight: 'var(--sp-3)' }}>
      <div className="flex items-center justify-between" style={{ marginBottom: 'var(--sp-2)', gap: 'var(--sp-2)' }}>
        <span
          className="font-semibold uppercase truncate inline-flex items-center"
          style={{ fontSize: 'var(--fs-xs)', letterSpacing: '0.06em', color: meta.tint, gap: 6 }}
        >
          {status === 'pending' && <LiveDot color={meta.tint} title="Мониторинг активен — ждём подтверждения данными" />}
          {meta.label}
        </span>
        <span
          className="rounded-full font-semibold flex-shrink-0"
          style={{
            fontSize: 'var(--fs-2xs)',
            padding: '1px 7px',
            color: 'var(--text-primary)',
            backgroundColor: 'var(--bg-secondary)',
            border: '1px solid var(--border-color)',
          }}
        >
          {total}
        </span>
      </div>
      <div
        className="flex flex-col overflow-y-auto"
        style={{ gap: 'var(--sp-2)', maxHeight: 'calc(100vh - 320px)', minHeight: 120 }}
      >
        {loading && items.length === 0 && (
          <>
            <Skeleton height={96} rounded="lg" />
            <Skeleton height={96} rounded="lg" />
          </>
        )}
        {error && (
          <Card padding="sm">
            <p style={{ color: 'var(--danger)', fontSize: 'var(--fs-xs)' }}>{error}</p>
          </Card>
        )}
        {!loading && !error && items.length === 0 && (
          <p
            className="text-center"
            style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-xs)', padding: 'var(--sp-4) 0' }}
          >
            Пусто
          </p>
        )}
        {items.map((it) => (
          <ContentCard
            key={it.id}
            item={it}
            fresh={freshIds.has(it.id)}
            onClick={() => onOpenCard(it.id)}
          />
        ))}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// CARD
// ════════════════════════════════════════════════════════════════════════════

function ContentCard({
  item, fresh, onClick,
}: {
  item: ContentCandidate;
  fresh: boolean;
  onClick: () => void;
}) {
  const left = item.thread_key ? threadColor(item.thread_key) : 'transparent';
  const dLeft = item.status === 'pending' ? daysLeft(item.pending_expires_at) : null;
  // Тикаем раз в 20с только чтобы форсировать пересчёт fmtRelative — без
  // этого «3 мин назад» застывает до следующего фетча колонки.
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 20_000);
    return () => clearInterval(id);
  }, []);

  return (
    <Card
      padding="sm"
      hoverable
      onClick={onClick}
      className={`cursor-pointer${fresh ? ' content-card-fresh' : ''}`}
      style={{ borderLeft: `3px solid ${left}` }}
    >
      <p
        className="font-semibold"
        style={{
          fontSize: 'var(--fs-sm)', color: 'var(--text-primary)',
          lineHeight: 1.3, marginBottom: 'var(--sp-2)',
          display: '-webkit-box', WebkitLineClamp: 3, WebkitBoxOrient: 'vertical', overflow: 'hidden',
        }}
      >
        {item.headline}
      </p>

      {(item.source || item.tickers.length > 0) && (
        <div className="flex flex-wrap items-center" style={{ gap: 'var(--sp-1)', marginBottom: 'var(--sp-2)' }}>
          {item.source && <Pill>{item.source}</Pill>}
          {item.tickers.slice(0, 4).map((t) => (
            <Pill key={t} tone="accent">{t}</Pill>
          ))}
        </div>
      )}

      <div className="flex items-center justify-between" style={{ gap: 'var(--sp-2)' }}>
        <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}>
          {item.created_at ? fmtRelative(item.created_at) : '—'}
        </span>
        {item.importance_1_5 != null && (
          <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)', fontWeight: 600 }}>
            Значимость {item.importance_1_5}/5
          </span>
        )}
      </div>

      {item.status === 'discarded' && item.reasoning && (
        <p
          style={{
            fontSize: 'var(--fs-2xs)', color: 'var(--danger)', marginTop: 'var(--sp-2)',
            display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden',
          }}
        >
          {item.reasoning}
        </p>
      )}

      {dLeft !== null && (
        <div style={{ marginTop: 'var(--sp-2)' }}>
          <Pill tone={dLeft <= 1 ? 'danger' : dLeft <= 2 ? 'warning' : 'neutral'}>
            {dLeft < 0 ? 'Просрочено' : dLeft === 0 ? 'Истекает сегодня' : `Осталось ${dLeft} дн.`}
          </Pill>
        </div>
      )}

      {(item.forwards_count != null || item.matched_anomaly_id != null || item.thread_key) && (
        <div
          className="flex flex-wrap items-center"
          style={{ gap: 'var(--sp-3)', marginTop: 'var(--sp-2)' }}
        >
          {item.forwards_count != null && (
            <span
              className="inline-flex items-center"
              style={{ gap: 4, fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)' }}
              title="Репостов в Telegram"
            >
              <Repeat2 size={11} /> {item.forwards_count}
            </span>
          )}
          {item.matched_anomaly_id != null && (
            <span
              className="inline-flex items-center"
              style={{ gap: 4, fontSize: 'var(--fs-2xs)', color: 'var(--accent)' }}
              title="Связанная аномалия в данных"
            >
              <Zap size={11} /> #{item.matched_anomaly_id}
            </span>
          )}
          {item.thread_key && (
            <span
              className="inline-flex items-center"
              style={{ gap: 4, fontSize: 'var(--fs-2xs)', color: left }}
              title={`Тред: ${item.thread_key}`}
            >
              <Link2 size={11} />
            </span>
          )}
        </div>
      )}
    </Card>
  );
}

function Pill({
  children, tone = 'neutral',
}: {
  children: React.ReactNode;
  tone?: 'neutral' | 'accent' | 'warning' | 'danger' | 'success';
}) {
  const colorMap: Record<string, string> = {
    neutral: 'var(--text-secondary)',
    accent: 'var(--accent)',
    warning: 'var(--warning)',
    danger: 'var(--danger)',
    success: 'var(--success)',
  };
  const c = colorMap[tone];
  return (
    <span
      className="rounded-full font-semibold"
      style={{
        fontSize: 'var(--fs-2xs)',
        padding: '1px 8px',
        color: c,
        backgroundColor: `color-mix(in srgb, ${c} 14%, transparent)`,
        whiteSpace: 'nowrap',
      }}
    >
      {children}
    </span>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// DETAIL MODAL
// ════════════════════════════════════════════════════════════════════════════

// Действия, которые реально решает ЧЕЛОВЕК из Kanban — не дублируем сюда
// pending→draft_ready/no_data (это content_match.py, автоматически, по данным).
const MANUAL_ACTIONS: Partial<Record<ContentCandidateStatus, { to: ContentCandidateStatus; label: string; tone: 'danger' | 'accent' | 'success' }[]>> = {
  candidate: [
    { to: 'discarded', label: 'Отбросить', tone: 'danger' },
    { to: 'pending', label: 'Пропустить в очередь', tone: 'accent' },
  ],
  draft_ready: [
    { to: 'in_review', label: 'Отправить на ревью', tone: 'accent' },
  ],
  in_review: [
    { to: 'published', label: 'Одобрить', tone: 'success' },
    { to: 'rejected', label: 'Отклонить', tone: 'danger' },
  ],
};

function CandidateDetailModal({
  candidateId, onClose, onNavigate, onChanged,
}: {
  candidateId: number | null;
  onClose: () => void;
  onNavigate: (id: number) => void;
  onChanged: () => void;
}) {
  const [detail, setDetail] = useState<ContentCandidateDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [acting, setActing] = useState(false);

  useEffect(() => {
    if (candidateId == null) { setDetail(null); return; }
    setLoading(true);
    setError(null);
    getContentCandidateDetail(candidateId)
      .then(setDetail)
      .catch((e: Error) => setError(e.message || 'Не удалось загрузить карточку'))
      .finally(() => setLoading(false));
  }, [candidateId]);

  async function handleAction(to: ContentCandidateStatus) {
    if (candidateId == null || acting) return;
    setActing(true);
    setError(null);
    try {
      const updated = await updateContentCandidateStatus(candidateId, to);
      setDetail((prev) => (prev ? { ...prev, ...updated } : prev));
      onChanged();
    } catch (e) {
      setError((e as Error).message || 'Не удалось изменить статус');
    } finally {
      setActing(false);
    }
  }

  return (
    <CenteredModalShell
      open={candidateId !== null}
      onClose={onClose}
      title={detail?.headline || 'Кандидат'}
      maxWidth={640}
    >
      {loading && <Skeleton height={220} rounded="lg" />}
      {error && <p style={{ color: 'var(--danger)', fontSize: 'var(--fs-sm)' }}>{error}</p>}
      {detail && !loading && (
        <div className="flex flex-col" style={{ gap: 'var(--sp-3)' }}>
          <div className="flex flex-wrap items-center" style={{ gap: 'var(--sp-2)' }}>
            <Pill tone={statusTone(detail.status)}>{STATUS_META[detail.status].label}</Pill>
            {detail.source && <Pill>{detail.source}</Pill>}
            {detail.importance_1_5 != null && (
              <Pill tone="accent">Значимость {detail.importance_1_5}/5</Pill>
            )}
          </div>

          {detail.tickers.length > 0 && (
            <div className="flex flex-wrap" style={{ gap: 'var(--sp-1)' }}>
              {detail.tickers.map((t) => <Pill key={t} tone="accent">{t}</Pill>)}
            </div>
          )}

          <p style={{ color: 'var(--text-muted)', fontSize: 'var(--fs-xs)' }}>
            Создано {detail.created_at ? new Date(detail.created_at).toLocaleString('ru-RU') : '—'}
            {detail.updated_at && detail.updated_at !== detail.created_at && (
              <> · обновлено {new Date(detail.updated_at).toLocaleString('ru-RU')}</>
            )}
          </p>

          {detail.raw_text && <Field label="Исходный текст">{detail.raw_text}</Field>}

          {detail.reasoning && (
            <Field label={detail.status === 'discarded' ? 'Причина отбраковки' : 'Обоснование'}>
              {detail.reasoning}
            </Field>
          )}

          {detail.status === 'pending' && detail.pending_expires_at && (
            <Field label="Окно ожидания данных">
              До {new Date(detail.pending_expires_at).toLocaleString('ru-RU')}
            </Field>
          )}

          {detail.matched_anomaly_id != null && (
            <Field label="Связанная аномалия">Аномалия #{detail.matched_anomaly_id}</Field>
          )}

          {detail.draft_text && (
            <Field label="Черновик поста" accent>{detail.draft_text}</Field>
          )}

          {detail.synth_declined_reason && (
            <Field label="Черновик не написан">{detail.synth_declined_reason}</Field>
          )}

          {detail.forwards_count != null && <Field label="Репостов">{detail.forwards_count}</Field>}

          {(detail.reviewer_action || detail.reviewer_id != null) && (
            <Field label="Решение ревьюера">
              {detail.reviewer_action === 'approved' ? 'Одобрено' : detail.reviewer_action === 'rejected' ? 'Отклонено' : '—'}
              {detail.reviewer_id != null && ` · admin #${detail.reviewer_id}`}
            </Field>
          )}

          {detail.published_at && (
            <Field label="Опубликовано">{new Date(detail.published_at).toLocaleString('ru-RU')}</Field>
          )}

          {detail.thread.length > 0 && (
            <div>
              <p
                className="uppercase font-semibold"
                style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', letterSpacing: '0.08em', marginBottom: 'var(--sp-1)' }}
              >
                Тред ({detail.thread.length + 1})
              </p>
              <div className="flex flex-col" style={{ gap: 'var(--sp-1)' }}>
                {detail.thread.map((t) => (
                  <button
                    type="button"
                    key={t.id}
                    onClick={() => onNavigate(t.id)}
                    className="w-full text-left rounded-lg"
                    style={{ padding: 'var(--sp-2)', border: '1px solid var(--border-color)', backgroundColor: 'var(--bg-secondary)' }}
                  >
                    <div className="flex items-center justify-between" style={{ gap: 'var(--sp-2)' }}>
                      <span className="truncate" style={{ fontSize: 'var(--fs-xs)', color: 'var(--text-primary)' }}>
                        {t.headline}
                      </span>
                      <Pill tone={statusTone(t.status)}>{STATUS_META[t.status].label}</Pill>
                    </div>
                  </button>
                ))}
              </div>
            </div>
          )}

          {error && <p style={{ color: 'var(--danger)', fontSize: 'var(--fs-sm)' }}>{error}</p>}

          {(MANUAL_ACTIONS[detail.status] ?? []).length > 0 && (
            <div
              className="flex flex-wrap"
              style={{ gap: 'var(--sp-2)', paddingTop: 'var(--sp-2)', borderTop: '1px solid var(--border-color)' }}
            >
              {MANUAL_ACTIONS[detail.status]!.map((a) => (
                <button
                  key={a.to}
                  type="button"
                  disabled={acting}
                  onClick={() => handleAction(a.to)}
                  className="rounded-lg font-semibold"
                  style={{
                    padding: 'var(--sp-2) var(--sp-4)',
                    fontSize: 'var(--fs-sm)',
                    color: a.tone === 'success' ? 'var(--bg-primary)' : `var(--${a.tone})`,
                    backgroundColor: a.tone === 'success' ? 'var(--success)' : 'transparent',
                    border: `1.5px solid var(--${a.tone})`,
                    opacity: acting ? 0.6 : 1,
                    cursor: acting ? 'default' : 'pointer',
                  }}
                >
                  {a.label}
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </CenteredModalShell>
  );
}

function Field({ label, children, accent }: { label: string; children: React.ReactNode; accent?: boolean }) {
  return (
    <div>
      <p
        className="uppercase font-semibold"
        style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-muted)', letterSpacing: '0.08em', marginBottom: 'var(--sp-1)' }}
      >
        {label}
      </p>
      <div
        style={{
          fontSize: 'var(--fs-sm)',
          color: 'var(--text-primary)',
          backgroundColor: accent ? 'color-mix(in srgb, var(--accent) 8%, transparent)' : 'var(--bg-secondary)',
          border: accent ? '1px solid color-mix(in srgb, var(--accent) 30%, transparent)' : '1px solid var(--border-color)',
          borderRadius: 8,
          padding: 'var(--sp-2) var(--sp-3)',
          whiteSpace: 'pre-wrap',
        }}
      >
        {children}
      </div>
    </div>
  );
}
