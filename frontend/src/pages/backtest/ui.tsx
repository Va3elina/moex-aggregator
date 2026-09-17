// Стенд: мелкие кирпичи интерфейса — логотип бумаги, выпадающее меню, столбики.
import { useEffect, useRef, useState, type ReactNode } from 'react';
import { LOGO, num } from './lib';

export function Logo({ st, size = 20 }: { st: string; size?: number }) {
  const [step, setStep] = useState(0);
  const t = LOGO[st];
  if (!t || step > 1) return <span className="bt-logo-fb" style={{ width: size, height: size, fontSize: size * 0.42 }}>{st}</span>;
  return <img className="bt-logo-img" alt="" width={size} height={size} loading="lazy" src={step === 0 ? `/logos/sm/${t}.png` : `/logos/${t}.png`} onError={() => setStep(s => s + 1)} />;
}

/** Кнопка с выпадающим меню; закрывается кликом мимо и Esc. */
export function Menu({ label, title, children, align = 'left', className = '' }: { label: ReactNode; title?: string; children: (close: () => void) => ReactNode; align?: 'left' | 'right'; className?: string }) {
  const [open, setOpen] = useState(false); const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const down = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); };
    const key = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    window.addEventListener('mousedown', down); window.addEventListener('keydown', key);
    return () => { window.removeEventListener('mousedown', down); window.removeEventListener('keydown', key); };
  }, [open]);
  return (
    <div className={`bt-menu ${className}`} ref={ref}>
      <button className={`bt-tb ${open ? 'on' : ''}`} title={title} onClick={() => setOpen(o => !o)}>{label}</button>
      {open && <div className={`bt-pop ${align}`}>{children(() => setOpen(false))}</div>}
    </div>
  );
}
export const MenuItem = ({ children, onClick, on, hint }: { children: ReactNode; onClick: () => void; on?: boolean; hint?: ReactNode }) =>
  <button className={`bt-mi ${on ? 'on' : ''}`} onClick={onClick}><span>{children}</span>{hint != null && <small>{hint}</small>}{on && <i>✓</i>}</button>;

/** Столбики ±: положительные вверх зелёные, отрицательные вниз красные. */
export function Bars({ data, height = 170, fmt = (v: number) => num(v) }: { data: { k: string; v: number; n?: number }[]; height?: number; fmt?: (v: number) => string }) {
  if (!data.length) return <div className="bt-nodata">Недостаточно данных</div>;
  const max = Math.max(...data.map(d => Math.abs(d.v)), 1e-12); const hasNeg = data.some(d => d.v < 0), hasPos = data.some(d => d.v > 0);
  const posH = hasNeg && hasPos ? 0.5 : hasPos ? 1 : 0; const every = Math.ceil(data.length / 14);
  return (
    <div className="bt-bars" style={{ height }}>
      <div className="bt-bars-plot">
        {data.map((d, i) => (
          <div key={d.k + i} className="bt-bar" title={`${d.k}: ${fmt(d.v)}${d.n != null ? ` · сделок ${d.n}` : ''}`}>
            <div className="pos" style={{ height: `${posH * 100}%` }}>{d.v > 0 && <span style={{ height: `${100 * d.v / max}%` }} />}</div>
            <div className="neg" style={{ height: `${(1 - posH) * 100}%` }}>{d.v < 0 && <span style={{ height: `${100 * -d.v / max}%` }} />}</div>
          </div>
        ))}
      </div>
      <div className="bt-bars-x">{data.map((d, i) => <span key={d.k + i}>{i % every === 0 ? d.k : ''}</span>)}</div>
    </div>
  );
}

export function Donut({ parts }: { parts: { label: string; n: number; color: string }[] }) {
  const total = parts.reduce((a, p) => a + p.n, 0) || 1; const R = 54, L = 2 * Math.PI * R;
  const offs = parts.map((_, i) => parts.slice(0, i).reduce((a, p) => a + L * p.n / total, 0));
  return (
    <div className="bt-donut">
      <svg viewBox="0 0 140 140" width={150} height={150}>
        {parts.map((p, i) => { const len = L * p.n / total; return <circle key={p.label} cx={70} cy={70} r={R} fill="none" stroke={p.color} strokeWidth={16} strokeDasharray={`${len} ${L - len}`} strokeDashoffset={-offs[i]} transform="rotate(-90 70 70)" />; })}
        <text x={70} y={68} textAnchor="middle" fill="#e6e8eb" fontSize={20} fontWeight={600}>{total}</text>
        <text x={70} y={86} textAnchor="middle" fill="#8a8f98" fontSize={10}>всего сделок</text>
      </svg>
      <div>{parts.map(p => <div key={p.label} className="bt-donut-row"><span className="dot" style={{ background: p.color }} />{p.label}<b>{p.n} · {num(100 * p.n / total, 1)}%</b></div>)}</div>
    </div>
  );
}
