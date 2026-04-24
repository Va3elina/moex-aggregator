/**
 * Shared-компоненты для страниц методологии.
 *
 * Принципы:
 *   - Сухо, без торговых идей и интерпретаций
 *   - Без формул расчёта, без внутренних источников
 *   - Только Московская биржа упоминается как публичный источник
 *   - Без англицизмов в тексте
 */
import { Link } from 'react-router-dom';
import { ArrowLeft, type LucideIcon } from 'lucide-react';

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
        className="w-4 h-0.5 mt-2.5 rounded-full flex-shrink-0"
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
