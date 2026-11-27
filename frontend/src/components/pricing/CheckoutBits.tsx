/**
 * Части окна оплаты, общие для обычного окна (PricingPage → ConsentModal) и
 * окна перехода на тариф ниже (sale/SwitchDownModal): строка способа оплаты и
 * строка согласия с галочкой. Вынесены из PricingPage без изменений.
 */
import React from 'react';
import { Check } from 'lucide-react';

/**
 * MethodOption — строка выбора способа оплаты (вариант C): значок, название,
 * пояснение и отметка справа.
 */
export function MethodOption({
  kind,
  selected,
  disabled,
  onSelect,
  title,
  note,
}: {
  kind: 'tpay' | 'sbp';
  selected: boolean;
  disabled?: boolean;
  onSelect: () => void;
  title: string;
  note: string;
}) {
  return (
    <button
      type="button"
      role="radio"
      aria-checked={selected}
      disabled={disabled}
      onClick={onSelect}
      className="w-full flex items-center gap-3 text-left mb-2 p-3 rounded-xl border transition-colors"
      style={{
        borderColor: selected ? 'var(--accent)' : 'var(--border-color)',
        background: selected ? 'rgba(255,92,43,0.06)' : 'transparent',
        minHeight: 56,
      }}
    >
      <PayMark kind={kind} />
      <span className="flex-1 min-w-0">
        <span className="block" style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-primary)' }}>{title}</span>
        <span className="block mt-0.5" style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.35 }}>{note}</span>
      </span>
      <span
        className="flex-shrink-0 flex items-center justify-center"
        style={{
          width: 20,
          height: 20,
          borderRadius: '50%',
          border: `1.5px solid ${selected ? 'var(--accent)' : 'var(--border-color)'}`,
          background: selected ? 'var(--accent)' : 'transparent',
          color: 'var(--bg-primary)',
        }}
      >
        {selected && <Check size={12} strokeWidth={3} />}
      </span>
    </button>
  );
}

/**
 * PayMark — значок способа оплаты в развилке «Как оплатить».
 *
 * СБП — официальный знак из медиакита НСПК (sbp.nspk.ru/file/logo.zip,
 * SBP_logo_RGB, уменьшен до 160px). Лежит в public/pay/sbp.png.
 * Карта/T-Pay — плашка T-Pay в public/pay/tpay.svg (2.4:1). В том же боксе
 * 36×26 она встаёт в 36×15 — по высоте как видимый знак СБП (~15px).
 */
function PayMark({ kind }: { kind: 'tpay' | 'sbp' }) {
  // Знак СБП на 30% крупнее (47×34), T-Pay прежнего размера; оба в боксе
  // одной ширины, чтобы подписи справа стояли ровно.
  const [w, h] = kind === 'sbp' ? [47, 34] : [36, 26];
  return (
    <span className="shrink-0 flex items-center justify-center" style={{ width: 47, height: 34 }}>
      <img
        src={kind === 'sbp' ? '/pay/sbp.png' : '/pay/tpay.svg'}
        alt=""
        aria-hidden="true"
        width={w}
        height={h}
        style={{ width: w, height: h, objectFit: 'contain' }}
      />
    </span>
  );
}

/**
 * ConsentRow — одна строка с галочкой и подписью.
 */
export function ConsentRow({
  checked,
  onChange,
  label,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  label: React.ReactNode;
}) {
  return (
    <label
      className="flex items-start gap-2.5 cursor-pointer select-none mb-3 px-2.5 py-2 rounded-lg border transition-colors"
      style={{
        borderColor: checked ? 'var(--accent)' : 'var(--border-color)',
        background: checked ? 'rgba(255,92,43,0.06)' : 'transparent',
      }}
    >
      <span
        className="relative inline-flex flex-shrink-0 items-center justify-center mt-0.5"
        style={{
          width: 16,
          height: 16,
          borderRadius: 4,
          background: checked ? 'var(--accent)' : 'transparent',
          border: `1.5px solid ${checked ? 'var(--accent)' : 'var(--border-color)'}`,
          color: 'var(--bg-primary)',
          transition: 'background 0.18s ease, border-color 0.18s ease',
        }}
      >
        {checked && <Check size={11} strokeWidth={3} />}
        <input
          type="checkbox"
          checked={checked}
          onChange={(e) => onChange(e.target.checked)}
          style={{
            position: 'absolute',
            opacity: 0,
            inset: 0,
            cursor: 'pointer',
          }}
        />
      </span>
      <span
        style={{
          fontSize: 12,
          color: 'var(--text-primary)',
          lineHeight: 1.4,
        }}
      >
        {label}
      </span>
    </label>
  );
}
