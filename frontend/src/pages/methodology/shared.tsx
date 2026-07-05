/**
 * Shared-компоненты для страниц методологии.
 *
 * Принципы:
 *   - Сухо, без торговых идей и интерпретаций
 *   - Без формул расчёта, без внутренних источников
 *   - Только Московская биржа упоминается как публичный источник
 *   - Без англицизмов в тексте
 */
import { Link, useNavigate } from 'react-router-dom';
import { ArrowLeft, PlayCircle, type LucideIcon } from 'lucide-react';
import { clearTourSeen } from '../../hooks/useFirstVisit';

interface MethodologyWrapperProps {
  icon: LucideIcon;
  /** Название индикатора — идёт под словом «Методология». */
  title: string;
  backTo: string;
  backLabel?: string;
  children: React.ReactNode;
}

export function MethodologyWrapper({
  icon: Icon,
  title,
  backTo,
  backLabel = 'К индикатору',
  children,
}: MethodologyWrapperProps) {
  return (
    <div className="max-w-3xl mx-auto px-4 md:px-6 py-6 md:py-8">
      <Link
        to={backTo}
        className="inline-flex items-center gap-2 text-sm mb-6 transition-colors"
        style={{ color: 'var(--text-secondary)' }}
      >
        <ArrowLeft size={14} />
        {backLabel}
      </Link>

      {/* Собственный header — single H1 «Методология · [Индикатор]».
          Иерархия: одна строка с крупным шрифтом, без маленького eyebrow-
          несоответствия. */}
      <header className="flex items-center gap-4 mb-8">
        <div
          className="flex items-center justify-center flex-shrink-0"
          style={{
            width: 48,
            height: 48,
            borderRadius: 'var(--radius-md, 8px)',
            background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
            color: 'var(--accent)',
          }}
        >
          <Icon size={24} strokeWidth={1.8} />
        </div>
        <h1
          className="text-2xl md:text-3xl font-bold"
          style={{
            color: 'var(--text-primary)',
            letterSpacing: '-0.015em',
            lineHeight: 1.15,
          }}
        >
          Методология{' '}
          <span style={{ color: 'var(--text-secondary)', fontWeight: 600 }}>·</span>{' '}
          <span style={{ color: 'var(--text-primary)' }}>{title}</span>
        </h1>
      </header>

      <div
        className="space-y-8"
        style={{ color: 'var(--text-secondary)', lineHeight: 1.65, fontSize: 15 }}
      >
        {children}

        <div className="pt-6 border-t" style={{ borderColor: 'var(--border-color)' }}>
          <Link
            to={backTo}
            className="inline-flex items-center gap-2 px-4 py-2 text-sm font-semibold transition-colors"
            style={{
              backgroundColor: 'var(--accent)',
              color: 'var(--text-inverse)',
              borderRadius: 'var(--radius-md, 8px)',
            }}
          >
            Перейти к индикатору
          </Link>
        </div>
      </div>
    </div>
  );
}

export function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h2
        className="text-xl font-bold mb-3"
        style={{ color: 'var(--text-primary)', letterSpacing: '-0.01em' }}
      >
        {title}
      </h2>
      {children}
    </section>
  );
}

export function ModeBlock({ title, desc }: { title: string; desc: string }) {
  return (
    <div
      className="p-4 rounded-xl border"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        borderColor: 'var(--border-color)',
      }}
    >
      <div className="font-bold mb-1" style={{ color: 'var(--text-primary)', fontSize: 16 }}>
        {title}
      </div>
      <div style={{ color: 'var(--text-secondary)', fontSize: 14 }}>{desc}</div>
    </div>
  );
}

/**
 * ActionBlock — как ModeBlock, но с иконкой кнопки слева (для описания
 * элементов управления: показываем логотип кнопки, чтобы юзер её узнал).
 */
export function ActionBlock({ icon: Icon, title, desc }: { icon: LucideIcon; title: string; desc: string }) {
  return (
    <div
      className="flex items-start gap-3 p-4 rounded-xl border"
      style={{ backgroundColor: 'var(--bg-secondary)', borderColor: 'var(--border-color)' }}
    >
      <div
        className="flex items-center justify-center flex-shrink-0"
        style={{
          width: 34, height: 34, borderRadius: 8,
          background: 'color-mix(in srgb, var(--accent) 12%, transparent)',
          color: 'var(--accent)',
        }}
      >
        <Icon size={18} strokeWidth={1.9} />
      </div>
      <div>
        <div className="font-bold mb-1" style={{ color: 'var(--text-primary)', fontSize: 15 }}>{title}</div>
        <div style={{ color: 'var(--text-secondary)', fontSize: 14 }}>{desc}</div>
      </div>
    </div>
  );
}

/**
 * ReplayTourButton — кнопка перезапуска onboarding-тура.
 *
 * Очищает `localStorage['frame_tour_seen:<key>']` и редиректит на страницу
 * индикатора. На странице индикатора `useFirstVisit` снова прочитает
 * пустой ключ и auto-откроет тур.
 */
export function ReplayTourButton({
  tourKey,
  indicatorPath,
  label = 'Показать вводный тур ещё раз',
}: {
  /** Ключ тура — должен совпадать с тем что используется в useFirstVisit. */
  tourKey: string;
  /** Путь к странице индикатора (можно с ?query — напр. /oi?tab=screener). */
  indicatorPath: string;
  /** Подпись кнопки (для страниц с несколькими турами). */
  label?: string;
}) {
  const navigate = useNavigate();
  return (
    <button
      onClick={() => {
        clearTourSeen(tourKey);   // сбрасываем актуальный v4-ключ → тур авто-откроется
        navigate(indicatorPath);
      }}
      className="inline-flex items-center gap-2 px-4 py-2 text-sm font-semibold transition-colors hover:opacity-90"
      style={{
        backgroundColor: 'var(--bg-secondary)',
        color: 'var(--text-primary)',
        border: '2px solid var(--text-primary)',
        borderRadius: 'var(--radius-md, 8px)',
      }}
    >
      <PlayCircle size={16} />
      {label}
    </button>
  );
}

/**
 * Interpretation — секция «Как интерпретировать значения».
 *
 * Универсальный блок: одна или несколько пар «Сценарий → Что это значит».
 * Используется во всех методологиях для трактовки графика и тренда.
 */
export function Interpretation({ rows }: { rows: { label: string; meaning: string }[] }) {
  return (
    <div className="space-y-3">
      {rows.map((row, i) => (
        <div
          key={i}
          className="p-4 rounded-xl border"
          style={{
            backgroundColor: 'var(--bg-secondary)',
            borderColor: 'var(--border-color)',
          }}
        >
          <div className="font-bold mb-1" style={{ color: 'var(--text-primary)', fontSize: 15 }}>
            {row.label}
          </div>
          <div style={{ color: 'var(--text-secondary)', fontSize: 14 }}>{row.meaning}</div>
        </div>
      ))}
    </div>
  );
}

export function LineBlock({
  color,
  name,
  desc,
}: {
  color: string;
  name: string;
  desc: string;
}) {
  return (
    <div className="flex items-start gap-3">
      <span
        className="legend-dot mt-1.5"
        style={{ backgroundColor: color }}
      />
      <div>
        <div className="font-medium" style={{ color }}>
          {name}
        </div>
        <div style={{ color: 'var(--text-secondary)', fontSize: 14 }}>{desc}</div>
      </div>
    </div>
  );
}
