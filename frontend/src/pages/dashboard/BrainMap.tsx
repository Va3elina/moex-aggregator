/**
 * Второй мозг как карта нодов: вход через узел, кольца по типам связей, обратные
 * связи списком, путь между двумя узлами.
 *
 * ⚠️ ВХОД ВСЕГДА ЧЕРЕЗ УЗЕЛ, А НЕ ЧЕРЕЗ ВСЮ КАРТУ. 41 тысяча узлов одним силовым
 * графом — пыль на холсте, каждый раз другая. Здесь центр — один узел, вокруг —
 * кольца: каждое кольцо — вид связи, точки на нём — свежие соседи. Клик по точке
 * переносит центр (как graph view в Notion). Адрес хранит узел: ?n=company:SBER.
 *
 * ⚠️ УЗЕЛ ЖИВЁТ В QUERY, А НЕ В ПУТИ. У новостей id вида news:markettwits/130070 —
 * со слэшем; в сегменте пути роутер его расщепляет.
 *
 * ⚠️ ЭКРАН НИЧЕГО НЕ СЧИТАЕТ. Кольца и счётчики приходят с бэкенда одним вызовом
 * (node/{id}?per_ring=24), список кольца — вторым; раскладка точек — арифметика.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { ExternalLink, Loader2, Search } from 'lucide-react';
import {
  getBrainNeighbors, getBrainNode, getBrainPath, getBrainSearch, getBrainSimilar, getBrainStats, getBrainTop,
} from '../../services/api';
import type {
  BrainNeighbor, BrainNode, BrainNodePage, BrainPath, BrainRing, BrainSearchHit, BrainStats, DashboardOverview,
} from '../../services/api';
import TickerJump from './TickerJump';

const ШИРИНА = 900;
const ЦЕНТР = { x: 450, y: 250 };

/** Подписи связей по-человечески и цвет кольца. Порядок — структурные связи ближе к центру. */
const СВЯЗИ: Record<string, { имя: string; цвет: string; порядок: number }> = {
  владеет:       { имя: 'владение',            цвет: '#8B7CF6', порядок: 0 },
  владеет_долей: { имя: 'акционеры',           цвет: '#8B7CF6', порядок: 1 },
  держит:        { имя: 'фонды-держатели',     цвет: '#5BD49C', порядок: 2 },
  включает:      { имя: 'индексы',             цвет: '#5BD49C', порядок: 3 },
  факт_о:        { имя: 'факты',               цвет: '#8B7CF6', порядок: 4 },
  о:             { имя: 'кандидаты и посты',   цвет: '#FF5C2B', порядок: 5 },
  из_новости:    { имя: 'исходные новости',    цвет: '#FF5C2B', порядок: 6 },
  сигнал_о:      { имя: 'сигналы о владении',  цвет: '#F2A24A', порядок: 7 },
  аномалия_по:   { имя: 'аномалии',            цвет: '#F2A24A', порядок: 8 },
  отчитался:     { имя: 'документы',           цвет: '#5DA3E9', порядок: 9 },
  упоминает:     { имя: 'новости',             цвет: '#9A9A9A', порядок: 10 },
};
const ВИД_ПОДПИСЬ: Record<string, string> = {
  company: 'компания', news: 'новость', candidate: 'кандидат', post: 'пост', doc: 'документ', fund: 'фонд',
  index: 'индекс', fact: 'факт', anomaly: 'аномалия', signal: 'сигнал', holder: 'держатель',
};
const ОКНА: Array<[number | undefined, string]> = [[7, '7 дн'], [30, '30 дн'], [90, '90 дн'], [undefined, 'всё']];

const числоРус = (n: number) => n.toLocaleString('ru-RU');
function датаРус(iso: string | null | undefined): string {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' });
}
const связь = (k: string) => СВЯЗИ[k] ?? { имя: k, цвет: '#9A9A9A', порядок: 99 };

/** Куда ведёт узел за пределы карты: пост — в разбор, новость — в t.me, документ — в файл. */
function внешняяСсылка(n: BrainNode): { to?: string; href?: string; подпись: string } | null {
  const д = (n.данные ?? {}) as Record<string, unknown>;
  const id = n.id.split(':').slice(1).join(':');
  switch (n.вид) {
    case 'candidate': case 'post': return { to: `/admin/dashboard/posts/${id}`, подпись: 'разбор' };
    case 'news': return typeof д.url === 'string' ? { href: д.url, подпись: 't.me' } : null;
    case 'doc': return typeof д.url === 'string' ? { href: д.url, подпись: 'файл' } : null;
    case 'fund': return { to: `/admin/dashboard/db/fund_holdings?fund=${id}`, подпись: 'состав' };
    case 'index': return { to: `/admin/dashboard/db/index_composition?index_id=${id}`, подпись: 'состав' };
    case 'anomaly': return { to: `/admin/dashboard/db/anomalies?с=2020-01-01`, подпись: 'аномалии' };
    case 'signal': return { to: `/admin/dashboard/graph`, подпись: 'очередь' };
    case 'company': return { to: `/admin/dashboard/graph/${id}`, подпись: 'связи' };
    default: return null;
  }
}

/** Кольцевая раскладка: радиус растёт с порядком связи, точки — по дуге, самые свежие сверху. */
function раскладка(кольца: BrainRing[]) {
  const упоряд = [...кольца].sort((a, b) => связь(a.связь).порядок - связь(b.связь).порядок);
  const шаг = упоряд.length > 6 ? 26 : 32;
  return упоряд.map((к, i) => {
    const r = 62 + i * шаг;
    const точек = Math.min(к.последние.length, 24);
    const точки = к.последние.slice(0, точек).map((n, j) => {
      const угол = -Math.PI / 2 + (j / Math.max(точек, 1)) * Math.PI * 2 + i * 0.13;
      return { n, x: ЦЕНТР.x + Math.cos(угол) * r, y: ЦЕНТР.y + Math.sin(угол) * r };
    });
    return { к, r, точки, i };
  });
}

function Ярлык({ children, цвет }: { children: React.ReactNode; цвет?: string }) {
  return (
    <span className="mono" style={{
      fontSize: 10.5, padding: '2px 8px', borderRadius: 999, whiteSpace: 'nowrap',
      background: 'rgba(245,241,232,0.08)', color: цвет ?? 'var(--d-mute)',
    }}>{children}</span>
  );
}

function Поиск({ выбрать }: { выбрать: (id: string) => void }) {
  const [q, setQ] = useState('');
  const [режим, setРежим] = useState<'word' | 'meaning'>('word');
  const [итоги, setИтоги] = useState<BrainSearchHit[]>([]);
  const [занято, setЗанято] = useState(false);
  const [ошибка, setОшибка] = useState<string | null>(null);
  const таймер = useRef<number | null>(null);
  useEffect(() => {
    if (таймер.current) window.clearTimeout(таймер.current);
    if (q.trim().length < 2) { setИтоги([]); return; }
    // По смыслу — только по Enter или после паузы подлиннее: каждый вызов считает вектор запроса.
    таймер.current = window.setTimeout(async () => {
      setЗанято(true); setОшибка(null);
      try { setИтоги((await getBrainSearch(q.trim(), undefined, 12, режим)).найдено); }
      catch (e) { setИтоги([]); setОшибка(e instanceof Error ? e.message : 'сбой'); }
      finally { setЗанято(false); }
    }, режим === 'meaning' ? 600 : 250);
    return () => { if (таймер.current) window.clearTimeout(таймер.current); };
  }, [q, режим]);
  return (
    <div style={{ position: 'relative' }}>
      <div className="flex items-center" style={{ gap: 8 }}>
        {занято ? <Loader2 size={14} className="animate-spin" style={{ color: 'var(--d-dim)' }} /> : <Search size={14} style={{ color: 'var(--d-dim)' }} />}
        <input className="dash-input" value={q} onChange={(e) => setQ(e.target.value)}
          placeholder={режим === 'meaning' ? 'о чём новость — своими словами…' : 'тикер, компания, слово из новости…'}
          style={{ width: 320 }}
          onKeyDown={(e) => { if (e.key === 'Enter' && итоги[0]) { выбрать(итоги[0].id); setQ(''); } }} />
        <div className="flex" style={{ gap: 2 }}>
          <button className="dash-tab" data-active={режим === 'word'} onClick={() => setРежим('word')}>по слову</button>
          <button className="dash-tab" data-active={режим === 'meaning'} onClick={() => setРежим('meaning')}>по смыслу</button>
        </div>
        {ошибка && <span className="mono" style={{ fontSize: 11, color: 'var(--d-warn)' }}>{ошибка}</span>}
      </div>
      {итоги.length > 0 && (
        <div className="dash-jump" style={{ minWidth: 360, maxHeight: 340, overflowY: 'auto' }}>
          {итоги.map((h) => (
            <a key={h.id} href="#" onClick={(e) => { e.preventDefault(); выбрать(h.id); setQ(''); setИтоги([]); }}
              className="flex items-baseline" style={{ gap: 8 }}>
              <Ярлык>{ВИД_ПОДПИСЬ[h.вид] ?? h.вид}</Ярлык>
              <span className="truncate" style={{ flex: 1 }}>{h.заголовок}</span>
              <span className="mono" style={{ fontSize: 10, color: 'var(--d-dim)' }}>{h.почему}</span>
            </a>
          ))}
        </div>
      )}
    </div>
  );
}

function Кольца({ страница, выбранноеКольцо, onКольцо, onУзел }: {
  страница: BrainNodePage; выбранноеКольцо: string | null;
  onКольцо: (k: string | null) => void; onУзел: (id: string) => void;
}) {
  const слои = useMemo(() => раскладка(страница.кольца), [страница]);
  const [навед, setНавед] = useState<BrainNode | null>(null);
  const высота = Math.max(500, ЦЕНТР.y + (слои.length ? слои[слои.length - 1].r : 60) + 40);
  const у = страница.узел;
  return (
    <div style={{ position: 'relative' }}>
      <svg viewBox={`0 0 ${ШИРИНА} ${высота}`} style={{ width: '100%', display: 'block' }} role="img" aria-label="Кольца связей узла">
        {слои.map(({ к, r, точки, i }) => {
          const с = связь(к.связь);
          const активно = !выбранноеКольцо || выбранноеКольцо === к.связь;
          return (
            <g key={к.связь + к.направление} opacity={активно ? 1 : 0.22}>
              <circle cx={ЦЕНТР.x} cy={ЦЕНТР.y} r={r} fill="none" stroke={с.цвет} strokeOpacity={0.5} strokeWidth={1.2}
                strokeDasharray={к.связь === 'упоминает' ? '3 5' : undefined} style={{ cursor: 'pointer' }}
                onClick={() => onКольцо(выбранноеКольцо === к.связь ? null : к.связь)} />
              {/* ⚠️ Подписи — по диагоналям вниз, а не на горизонтальной оси: на оси десять
                  подписей десяти колец лежали в одной строке и читались как каша. На
                  диагонали каждая уходит на свой радиус — шаг между ними ≈ 20 px. */}
              <text x={ЦЕНТР.x + (i % 2 === 0 ? 1 : -1) * (r + 4) * 0.7071} y={ЦЕНТР.y + (r + 4) * 0.7071 + 4} fontSize={10.5}
                textAnchor={i % 2 === 0 ? 'start' : 'end'} fill={с.цвет} fontFamily="'JetBrains Mono', monospace"
                style={{ cursor: 'pointer' }} onClick={() => onКольцо(выбранноеКольцо === к.связь ? null : к.связь)}>
                {с.имя} · {числоРус(к.сколько)}
              </text>
              {точки.map(({ n, x, y }) => (
                <circle key={n.id} cx={x} cy={y} r={навед?.id === n.id ? 6 : 4} fill={с.цвет} fillOpacity={0.95}
                  style={{ cursor: 'pointer' }}
                  onMouseEnter={() => setНавед(n)} onMouseLeave={() => setНавед(null)}
                  onClick={() => onУзел(n.id)}>
                  <title>{n.заголовок}</title>
                </circle>
              ))}
            </g>
          );
        })}
        <circle cx={ЦЕНТР.x} cy={ЦЕНТР.y} r={38} fill="var(--d-card)" stroke="var(--d-ink)" strokeWidth={2} />
        <text x={ЦЕНТР.x} y={ЦЕНТР.y + 5} textAnchor="middle" fontSize={у.вид === 'company' ? 15 : 11} fontWeight={800}
          fill="var(--d-ink)" fontFamily="Archivo, Inter, sans-serif">
          {у.вид === 'company' ? у.id.split(':')[1] : (ВИД_ПОДПИСЬ[у.вид] ?? у.вид)}
        </text>
      </svg>
      {навед && (
        <div className="dash-inner mono" style={{
          position: 'absolute', left: 12, bottom: 12, maxWidth: 420, padding: '8px 10px', fontSize: 11.5,
          border: '1px solid var(--d-line-strong)', pointerEvents: 'none',
        }}>
          <span style={{ color: 'var(--d-dim)' }}>{ВИД_ПОДПИСЬ[навед.вид] ?? навед.вид} · {датаРус(навед.время)}</span><br />
          {навед.заголовок}
        </div>
      )}
    </div>
  );
}

function Список({ узел, кольцо, окно, onУзел }: { узел: string; кольцо: string | null; окно?: number; onУзел: (id: string) => void }) {
  const [d, setD] = useState<{ всего: number; соседи: BrainNeighbor[] } | null>(null);
  const [занято, setЗанято] = useState(false);
  const загрузить = useCallback(async (offset: number) => {
    setЗанято(true);
    try {
      const r = await getBrainNeighbors(узел, кольцо ?? undefined, окно, 60, offset);
      setD((prev) => offset && prev ? { всего: r.всего, соседи: [...prev.соседи, ...r.соседи] } : { всего: r.всего, соседи: r.соседи });
    } finally { setЗанято(false); }
  }, [узел, кольцо, окно]);
  useEffect(() => { setD(null); загрузить(0); }, [загрузить]);
  if (!d) return <div className="mono" style={{ fontSize: 11.5, color: 'var(--d-dim)', padding: 8 }}>{занято ? 'читаю связи…' : ''}</div>;
  return (
    <div>
      <div className="flex flex-col" style={{ gap: 4 }}>
        {d.соседи.map((s) => {
          const с = связь(s.связь);
          const вн = внешняяСсылка(s);
          return (
            <div key={s.id + s.связь} className="flex items-baseline" style={{ gap: 10, fontSize: 12.5, padding: '5px 8px', borderRadius: 6, background: 'var(--d-sunk)' }}>
              <span className="mono shrink-0" style={{ fontSize: 10.5, color: 'var(--d-dim)', minWidth: 58 }}>{датаРус(s.время)}</span>
              <span style={{ width: 7, height: 7, borderRadius: '50%', background: с.цвет, flexShrink: 0, alignSelf: 'center' }} />
              <Ярлык>{ВИД_ПОДПИСЬ[s.вид] ?? s.вид}</Ярлык>
              <button onClick={() => onУзел(s.id)} className="truncate"
                style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'var(--d-ink)', textAlign: 'left', flex: 1, minWidth: 0 }}
                title={s.заголовок}>
                {s.заголовок}
              </button>
              {s.вес != null && <span className="mono shrink-0" style={{ fontSize: 10.5, color: 'var(--d-mute)' }}>{Number(s.вес).toLocaleString('ru-RU', { maximumFractionDigits: 2 })}{s.связь === 'держит' || s.связь === 'включает' || s.связь === 'владеет_долей' ? ' %' : ''}</span>}
              {вн && (вн.to
                ? <Link to={вн.to} className="mono shrink-0" style={{ fontSize: 10.5, color: 'var(--d-accent)', textDecoration: 'none' }}>{вн.подпись} →</Link>
                : <a href={вн.href} target="_blank" rel="noreferrer" className="mono shrink-0 flex items-center" style={{ fontSize: 10.5, color: 'var(--d-accent)', gap: 3 }}>{вн.подпись} <ExternalLink size={9} /></a>)}
            </div>
          );
        })}
      </div>
      <div className="flex items-center justify-between mt-2" style={{ gap: 10 }}>
        <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>показано {числоРус(d.соседи.length)} из {числоРус(d.всего)}</span>
        {d.соседи.length < d.всего && (
          <button className="dash-press" disabled={занято} onClick={() => загрузить(d.соседи.length)} style={{ padding: '4px 12px', fontSize: 11.5 }}>
            {занято ? 'гружу…' : 'ещё'}
          </button>
        )}
      </div>
    </div>
  );
}

function Похожие({ id, onУзел }: { id: string; onУзел: (id: string) => void }) {
  const [вид, setВид] = useState<string | undefined>(undefined);
  const [d, setD] = useState<Array<BrainNode & { сходство: number }> | null>(null);
  const [ошибка, setОшибка] = useState<string | null>(null);
  useEffect(() => {
    let живо = true;
    setD(null); setОшибка(null);
    getBrainSimilar(id, вид, 12)
      .then((r) => { if (живо) setD(r.похожие); })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); });
    return () => { живо = false; };
  }, [id, вид]);
  return (
    <div>
      <div className="flex flex-wrap mb-2" style={{ gap: 3 }}>
        {([[undefined, 'всё'], ['company', 'компании'], ['news', 'новости'], ['candidate', 'кандидаты']] as Array<[string | undefined, string]>).map(([k, п]) => (
          <button key={п} className="dash-tab" data-active={вид === k} onClick={() => setВид(k)} style={{ padding: '3px 9px', fontSize: 11 }}>{п}</button>
        ))}
      </div>
      {ошибка && <div className="mono" style={{ fontSize: 11, color: 'var(--d-warn)' }}>{ошибка}</div>}
      {!d && !ошибка && <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>считаю…</div>}
      {d && d.length === 0 && <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>вектора у узла ещё нет</div>}
      {d && d.length > 0 && (
        <div className="flex flex-col" style={{ gap: 3 }}>
          {d.map((x) => (
            <button key={x.id} onClick={() => onУзел(x.id)} className="flex items-baseline"
              style={{ gap: 8, background: 'none', border: 'none', padding: '3px 0', cursor: 'pointer', color: 'var(--d-ink)', textAlign: 'left', fontSize: 12 }}>
              <span className="mono shrink-0" style={{ fontSize: 10.5, color: 'var(--d-dim)', minWidth: 30 }}>{Math.round(x.сходство * 100)}%</span>
              <Ярлык>{ВИД_ПОДПИСЬ[x.вид] ?? x.вид}</Ярлык>
              <span className="truncate" title={x.заголовок}>{x.заголовок}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function Путь({ от }: { от: string }) {
  const [до, setДо] = useState('');
  const [путь, setПуть] = useState<BrainPath | null>(null);
  const [занято, setЗанято] = useState(false);
  const искать = async () => {
    const t = до.trim(); if (!t) return;
    setЗанято(true);
    try {
      const цель = t.includes(':') ? t : `company:${t.toUpperCase()}`;
      setПуть(await getBrainPath(от, цель));
    } catch (e) { setПуть({ от, до: t, путь: null, шагов: null }); } finally { setЗанято(false); }
  };
  return (
    <div>
      <div className="flex items-center" style={{ gap: 6 }}>
        <input className="dash-input" value={до} onChange={(e) => setДо(e.target.value)} placeholder="тикер или id узла"
          style={{ width: 150 }} onKeyDown={(e) => { if (e.key === 'Enter') искать(); }} />
        <button className="dash-press" onClick={искать} disabled={занято} style={{ padding: '5px 12px', fontSize: 11.5 }}>
          {занято ? '…' : 'найти связь'}
        </button>
      </div>
      {путь && (
        <div className="mt-2" style={{ fontSize: 12 }}>
          {путь.путь ? (
            <div className="flex flex-col" style={{ gap: 3 }}>
              {путь.путь.map((ш, i) => (
                <div key={i} className="flex items-baseline flex-wrap" style={{ gap: 6 }}>
                  <span className="mono" style={{ color: связь(ш.связь).цвет, fontSize: 10.5 }}>{связь(ш.связь).имя}</span>
                  <span style={{ color: 'var(--d-dim)' }}>{ш.направление === 'исх' ? '→' : '←'}</span>
                  <span>{ш.узел?.заголовок ?? ш.к}</span>
                </div>
              ))}
            </div>
          ) : <span style={{ color: 'var(--d-dim)' }}>связи в три шага нет</span>}
        </div>
      )}
    </div>
  );
}

export default function BrainMap({ покрытие }: { покрытие?: DashboardOverview['второй_мозг'] }) {
  const [searchParams, setSearchParams] = useSearchParams();
  const n = searchParams.get('n');
  const [страница, setСтраница] = useState<BrainNodePage | null>(null);
  const [ошибка, setОшибка] = useState<string | null>(null);
  const [окно, setОкно] = useState<number | undefined>(undefined);
  const [кольцо, setКольцо] = useState<string | null>(null);
  const [статистика, setСтатистика] = useState<BrainStats | null>(null);
  const [топ, setТоп] = useState<Array<BrainNode & { связей: number }>>([]);

  const открыть = useCallback((id: string) => {
    const p = new URLSearchParams(searchParams); p.set('n', id); setSearchParams(p); setКольцо(null);
  }, [searchParams, setSearchParams]);

  useEffect(() => {
    let живо = true;
    getBrainStats().then((s) => { if (живо) setСтатистика(s); }).catch(() => undefined);
    getBrainTop('company', 24).then((t) => { if (живо) setТоп(t.узлы); }).catch(() => undefined);
    return () => { живо = false; };
  }, []);

  useEffect(() => {
    if (!n) { setСтраница(null); return; }
    let живо = true;
    setОшибка(null);
    getBrainNode(n, окно, 24)
      .then((p) => { if (живо) setСтраница(p); })
      .catch((e) => { if (живо) { setОшибка(e instanceof Error ? e.message : 'сбой'); setСтраница(null); } });
    return () => { живо = false; };
  }, [n, окно]);

  const у = страница?.узел;
  const д = (у?.данные ?? {}) as Record<string, unknown>;
  const вн = у ? внешняяСсылка(у) : null;

  return (
    <div className="flex flex-col" style={{ gap: 12 }}>
      <div className="dash-card" style={{ padding: '14px 18px' }}>
        <div className="flex items-center justify-between flex-wrap" style={{ gap: 12 }}>
          <div className="flex items-center flex-wrap" style={{ gap: 14 }}>
            <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Карта нодов</div>
            <Поиск выбрать={открыть} />
          </div>
          {статистика && (
            <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
              {числоРус(статистика.всего_узлов)} узлов · {числоРус(статистика.всего_рёбер)} связей · синхронизация каждые 15 мин
            </span>
          )}
        </div>
      </div>

      {ошибка && <div className="dash-card" style={{ padding: 14, color: 'var(--d-bad)', fontSize: 13 }}>{ошибка}</div>}

      {!n && (
        <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 2fr) minmax(280px, 1fr)', gap: 12, alignItems: 'start' }}>
          <div className="dash-card" style={{ padding: '16px 18px' }}>
            <div className="disp mb-1" style={{ fontSize: 15, fontWeight: 700 }}>Самые связанные компании</div>
            <p style={{ fontSize: 12.5, color: 'var(--d-mute)', margin: '0 0 12px', lineHeight: 1.5 }}>
              Вход — через узел. Выберите компанию или найдите что угодно в поиске: тикер, название, слово из новости.
            </p>
            <div className="flex flex-wrap" style={{ gap: 6 }}>
              {топ.map((t) => (
                <button key={t.id} onClick={() => открыть(t.id)} className="dash-press" style={{ padding: '5px 11px', fontSize: 12 }}>
                  <span style={{ fontWeight: 600 }}>{t.заголовок}</span>
                  <span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)', marginLeft: 7 }}>{числоРус(t.связей)}</span>
                </button>
              ))}
              {!топ.length && <span className="mono" style={{ fontSize: 11.5, color: 'var(--d-dim)' }}>читаю карту…</span>}
            </div>
          </div>
          <div className="flex flex-col" style={{ gap: 12 }}>
            {статистика && (
              <div className="dash-card" style={{ padding: '14px 16px' }}>
                <div className="mono mb-2" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Из чего состоит</div>
                <div className="flex flex-col" style={{ gap: 5 }}>
                  {Object.entries(статистика.узлов).sort((a, b) => b[1] - a[1]).map(([k, v]) => (
                    <div key={k} className="flex justify-between" style={{ fontSize: 12.5 }}>
                      <span style={{ color: 'var(--d-mute)' }}>{ВИД_ПОДПИСЬ[k] ?? k}</span>
                      <span className="mono" style={{ fontWeight: 600 }}>{числоРус(v)}</span>
                    </div>
                  ))}
                </div>
                <div className="mono mt-3 mb-2" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Связи</div>
                <div className="flex flex-wrap" style={{ gap: 5 }}>
                  {Object.entries(статистика.рёбер).sort((a, b) => b[1] - a[1]).map(([k, v]) => (
                    <Ярлык key={k} цвет={связь(k).цвет}>{связь(k).имя} · {числоРус(v)}</Ярлык>
                  ))}
                </div>
              </div>
            )}
            {покрытие && (
              <div className="dash-card" style={{ padding: '14px 16px' }}>
                <div className="mono mb-2" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Справочник и карточки</div>
                <div className="flex flex-col" style={{ gap: 5 }}>
                  {Object.entries(покрытие).map(([k, v]) => (
                    <div key={k} className="flex justify-between" style={{ fontSize: 12.5 }}>
                      <span style={{ color: 'var(--d-mute)' }}>{k.replace(/_/g, ' ')}</span>
                      <span className="mono" style={{ fontWeight: 600 }}>{числоРус(v)}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {n && страница && у && (
        <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 2fr) minmax(280px, 1fr)', gap: 12, alignItems: 'start' }}>
          <div className="flex flex-col" style={{ gap: 12 }}>
            <div className="dash-card" style={{ padding: '14px 18px' }}>
              <div className="flex items-center justify-between flex-wrap mb-1" style={{ gap: 10 }}>
                <div className="flex items-baseline flex-wrap" style={{ gap: 10 }}>
                  <Ярлык>{ВИД_ПОДПИСЬ[у.вид] ?? у.вид}</Ярлык>
                  <div className="disp" style={{ fontSize: 18, fontWeight: 700 }}>{у.заголовок}</div>
                  {у.вид === 'company' && <TickerJump t={у.id.split(':')[1]} />}
                </div>
                <div className="flex" style={{ gap: 4 }}>
                  {ОКНА.map(([д, п]) => (
                    <button key={п} className="dash-tab" data-active={окно === д} onClick={() => setОкно(д)}>{п}</button>
                  ))}
                </div>
              </div>
              {у.кратко && <p style={{ fontSize: 12.5, color: 'var(--d-mute)', margin: '0 0 6px', lineHeight: 1.5 }}>{у.кратко}</p>}
              {страница.кольца.length === 0
                ? <div className="mono" style={{ fontSize: 12, color: 'var(--d-dim)', padding: '20px 0' }}>связей за это окно нет</div>
                : <Кольца страница={страница} выбранноеКольцо={кольцо} onКольцо={setКольцо} onУзел={открыть} />}
              <div className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
                кольцо — вид связи, точка — сосед (до 24 самых свежих); клик по точке переносит центр, клик по кольцу фильтрует список
              </div>
            </div>
            <div className="dash-card" style={{ padding: '14px 18px' }}>
              <div className="flex items-baseline justify-between mb-2" style={{ gap: 10 }}>
                <div className="disp" style={{ fontSize: 14, fontWeight: 700 }}>
                  {кольцо ? `${связь(кольцо).имя}` : 'Все связи'}
                  {кольцо && <button onClick={() => setКольцо(null)} className="mono" style={{ marginLeft: 10, background: 'none', border: 'none', color: 'var(--d-accent)', cursor: 'pointer', fontSize: 11 }}>снять фильтр</button>}
                </div>
                <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>свежие первыми</span>
              </div>
              <Список узел={у.id} кольцо={кольцо} окно={окно} onУзел={открыть} />
            </div>
          </div>

          <div className="flex flex-col" style={{ gap: 12, position: 'sticky', top: 12 }}>
            <div className="dash-card" style={{ padding: '14px 16px' }}>
              <div className="mono mb-2" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Узел</div>
              <div className="mono" style={{ fontSize: 11, color: 'var(--d-mute)', wordBreak: 'break-all' }}>{у.id}</div>
              {у.время && <div className="mono mt-1" style={{ fontSize: 11, color: 'var(--d-dim)' }}>{датаРус(у.время)}</div>}
              <div className="flex flex-col mt-2" style={{ gap: 4, fontSize: 12 }}>
                {Object.entries(д).filter(([, v]) => v !== null && v !== '' && !(Array.isArray(v) && !v.length) && typeof v !== 'object' || Array.isArray(v)).slice(0, 10).map(([k, v]) => (
                  <div key={k} className="flex justify-between" style={{ gap: 8 }}>
                    <span style={{ color: 'var(--d-dim)' }}>{k.replace(/_/g, ' ')}</span>
                    <span className="mono truncate" style={{ maxWidth: 170 }} title={Array.isArray(v) ? v.join(', ') : String(v)}>
                      {Array.isArray(v) ? v.join(', ') : String(v)}
                    </span>
                  </div>
                ))}
              </div>
              {вн && (
                <div className="mt-3">
                  {вн.to
                    ? <Link to={вн.to} style={{ color: 'var(--d-accent)', fontSize: 12, textDecoration: 'none' }}>{вн.подпись} →</Link>
                    : <a href={вн.href} target="_blank" rel="noreferrer" className="flex items-center" style={{ color: 'var(--d-accent)', fontSize: 12, gap: 4 }}>{вн.подпись} <ExternalLink size={11} /></a>}
                </div>
              )}
            </div>
            <div className="dash-card" style={{ padding: '14px 16px' }}>
              <div className="mono mb-2" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Кольца</div>
              <div className="flex flex-col" style={{ gap: 4 }}>
                {[...страница.кольца].sort((a, b) => связь(a.связь).порядок - связь(b.связь).порядок).map((к) => (
                  <button key={к.связь + к.направление} onClick={() => setКольцо(кольцо === к.связь ? null : к.связь)} className="dash-slice" data-active={кольцо === к.связь}
                    style={{ padding: '4px 8px' }}>
                    <span className="flex items-center justify-between" style={{ gap: 8 }}>
                      <span className="flex items-center" style={{ gap: 7 }}>
                        <span style={{ width: 8, height: 8, borderRadius: '50%', background: связь(к.связь).цвет, display: 'inline-block' }} />
                        {связь(к.связь).имя}
                      </span>
                      <span className="mono" style={{ fontSize: 11 }}>{числоРус(к.сколько)} <span style={{ color: 'var(--d-dim)' }}>· {датаРус(к.свежее)}</span></span>
                    </span>
                  </button>
                ))}
              </div>
            </div>
            <div className="dash-card" style={{ padding: '14px 16px' }}>
              <div className="mono mb-2" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Похожие по смыслу</div>
              <Похожие id={у.id} onУзел={открыть} />
            </div>
            <div className="dash-card" style={{ padding: '14px 16px' }}>
              <div className="mono mb-2" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>При чём тут…</div>
              <Путь от={у.id} />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
