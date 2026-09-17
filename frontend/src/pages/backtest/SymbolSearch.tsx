// Стенд: выбор бумаги — как поиск символа в TradingView: строка поиска, группы, логотипы. Ничего лишнего справа.
import { useEffect, useMemo, useRef, useState } from 'react';
import { GROUP } from './lib';
import { Logo } from './ui';

export interface SymRow { st: string; name: string }

export default function SymbolSearch({ rows, current, recent, onPick, onClose }: { rows: SymRow[]; current: string; recent: string[]; onPick: (st: string) => void; onClose: () => void }) {
  const [q, setQ] = useState(''); const [grp, setGrp] = useState('Все'); const [idx, setIdx] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => { input.current?.focus(); }, []);
  const groups = ['Все', ...new Set(rows.map(r => GROUP[r.st] ?? 'Другое'))];
  const list = useMemo(() => {
    const s = q.trim().toLowerCase();
    const f = rows.filter(r => (grp === 'Все' || GROUP[r.st] === grp) && (!s || r.st.toLowerCase().includes(s) || r.name.toLowerCase().includes(s)));
    return s ? f : [...f].sort((a, b) => (recent.indexOf(a.st) + 1 || 99) - (recent.indexOf(b.st) + 1 || 99));
  }, [rows, q, grp, recent]);
  const key = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') onClose();
    else if (e.key === 'ArrowDown') { e.preventDefault(); setIdx(i => Math.min(list.length - 1, i + 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setIdx(i => Math.max(0, i - 1)); }
    else if (e.key === 'Enter' && list[idx]) onPick(list[idx].st);
  };
  return (
    <div className="bt-overlay" onMouseDown={onClose}>
      <div className="bt-modal bt-sym" onMouseDown={e => e.stopPropagation()}>
        <header><b>Поиск инструмента</b><button className="bt-x" onClick={onClose}>✕</button></header>
        <input ref={input} className="bt-search" placeholder="Название или код: Сбербанк, SR, доллар…" value={q} onChange={e => { setQ(e.target.value); setIdx(0); }} onKeyDown={key} />
        <div className="bt-pills">{groups.map(g => <button key={g} className={g === grp ? 'on' : ''} onClick={() => { setGrp(g); setIdx(0); }}>{g}</button>)}</div>
        <div className="bt-symlist">
          {list.map((r, i) => (
            <button key={r.st} className={`bt-symrow ${i === idx ? 'hover' : ''} ${r.st === current ? 'on' : ''}`} onMouseEnter={() => setIdx(i)} onClick={() => onPick(r.st)}>
              <Logo st={r.st} size={26} /><b>{r.st}</b><span>{r.name}<small>фьючерс · {GROUP[r.st] ?? ''} · MOEX</small></span>
            </button>
          ))}
          {!list.length && <div className="bt-nodata">Ничего не найдено</div>}
        </div>
      </div>
    </div>
  );
}
