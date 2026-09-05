/**
 * Связи между компаниями: архипелаг рёбер владения + очередь сигналов.
 *
 * ⚠️ РАСКЛАДКА ДЕТЕРМИНИРОВАННАЯ, ПО КЛАСТЕРАМ. Данных мало (28 узлов, 23 ребра,
 * семь компонент, степень не выше четырёх), и один силовой граф на них рисует
 * пыль по холсту, каждый раз по-новому. Бэкенд отдаёт компоненты; здесь каждая
 * компонента — остров: самый связанный узел в центре, остальные по кругу. Острова
 * стоят сеткой по убыванию размера. Ничего не считается по таймеру.
 *
 * ⚠️ ТОЛЩИНЫ ПО ДОЛЕ НЕТ — намеренно. Доля живёт прозой в тексте факта; в панели
 * она показывается с пометкой «из текста», но линию по ней не рисуем: в базе
 * записано «точная доля не зафиксирована», и уверенная картинка была бы враньём.
 * Что кодирует ребро: цвет — возраст снимка (зелёный < года, оранжевый до двух,
 * красный старше или без даты), пунктир — косвенное владение, прозрачность —
 * уверенность, стрелка — направление (нет стрелки = направление не закодировано).
 *
 * ⚠️ КАЗНАЧЕЙСКИЙ ПАКЕТ — КОЛЬЦО ВОКРУГ УЗЛА, не петля: компания владеет собой.
 *
 * ⚠️ ОЧЕРЕДЬ СИГНАЛОВ НИЧЕГО НЕ ПИШЕТ В ГРАФ. «Подтвердить» означает «посмотрел
 * и завёл ребро руками» — граница, ради которой очередь и существует.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import TickerJump from './TickerJump';
import { Loader2 } from 'lucide-react';
import {
  getOwnershipGraph, getOwnershipSignals, reviewOwnershipSignal,
} from '../../services/api';
import type { GraphEdge, GraphNode, OwnershipGraph as Граф, OwnershipSignal } from '../../services/api';

const ШИРИНА = 1240;
const ФИОЛЕТ = '#5B5BD6'; // единственное место в системе с этим цветом — казначейский пакет

function цветВозраста(дней: number | null): string {
  if (дней === null) return 'var(--d-bad)';
  if (дней < 365) return 'var(--d-ok)';
  if (дней < 730) return 'var(--d-warn)';
  return 'var(--d-bad)';
}
function возраст(дней: number | null): string {
  if (дней === null) return 'даты нет';
  if (дней < 45) return `${дней} дн назад`;
  if (дней < 400) return `${Math.round(дней / 30)} мес назад`;
  return `${(дней / 365).toFixed(1)} г назад`;
}

interface Точка { x: number; y: number }

/**
 * Острова: компонента → круг; крупные первыми; сетка по три в ряд.
 *
 * ⚠️ ОДИНОЧКИ — НА ПОЛКУ, А НЕ В ОСТРОВА. 17 из 45 компаний имеют только
 * казначейский пакет и ни одного ребра. Как отдельные «острова» они превращали
 * холст в россыпь кружков, среди которой семь настоящих кластеров терялись.
 * Полка внизу держит их в одну строку, мелко: видно, что они есть, и не мешают.
 */
function разложить(узлы: GraphNode[]): { позиции: Record<string, Точка>; высота: number; полка: string[] } {
  const поКомпонентам = new Map<number, GraphNode[]>();
  for (const у of узлы) {
    if (!поКомпонентам.has(у.компонента)) поКомпонентам.set(у.компонента, []);
    поКомпонентам.get(у.компонента)!.push(у);
  }
  const все = [...поКомпонентам.entries()].sort((a, b) => b[1].length - a[1].length);
  const острова = все.filter(([, g]) => g.length > 1);
  const полка = все.filter(([, g]) => g.length === 1).map(([, g]) => g[0].тикер).sort();
  const позиции: Record<string, Точка> = {};
  const колонок = 3;
  const шагX = ШИРИНА / колонок;
  let y0 = 40;
  let максВысотаРяда = 0;
  острова.forEach(([, группа], i) => {
    const колонка = i % колонок;
    if (i > 0 && колонка === 0) { y0 += максВысотаРяда + 60; максВысотаРяда = 0; }
    const n = группа.length;
    // Радиус растёт с числом узлов, но упирается в ширину колонки: у острова на
    // семь узлов 34·7 = 238 — и левый край вылезал за холст (GMKN резало пополам).
    const радиус = n <= 2 ? 70 : Math.min(Math.max(90, 34 * n), шагX / 2 - 64);
    const cx = шагX * колонка + шагX / 2;
    const cy = y0 + радиус + 30;
    const отсорт = [...группа].sort((a, b) => b.степень - a.степень);
    if (n === 1) {
      позиции[отсорт[0].тикер] = { x: cx, y: cy };
    } else if (n === 2) {
      позиции[отсорт[0].тикер] = { x: cx - 80, y: cy };
      позиции[отсорт[1].тикер] = { x: cx + 80, y: cy };
    } else {
      // Самый связанный — в центре, остальные по кругу. Так «звёзды» вроде
      // АФК Системы читаются сразу, а не как случайный узел на окружности.
      позиции[отсорт[0].тикер] = { x: cx, y: cy };
      отсорт.slice(1).forEach((у, k, arr) => {
        const угол = -Math.PI / 2 + (2 * Math.PI * k) / arr.length;
        позиции[у.тикер] = { x: cx + радиус * Math.cos(угол), y: cy + радиус * Math.sin(угол) };
      });
    }
    максВысотаРяда = Math.max(максВысотаРяда, радиус * 2 + 60);
  });
  let высота = y0 + максВысотаРяда + 20;
  if (полка.length) {
    const yПолки = высота + 36;
    const шаг = Math.min(70, (ШИРИНА - 80) / Math.max(полка.length, 1));
    полка.forEach((t, i) => { позиции[t] = { x: 40 + шаг / 2 + i * шаг, y: yПолки }; });
    высота = yПолки + 50;
  }
  return { позиции, высота, полка };
}

function Плашка({ children, цвет }: { children: React.ReactNode; цвет?: string }) {
  return (
    <span className="mono" style={{
      fontSize: 10.5, padding: '2px 8px', borderRadius: 999, whiteSpace: 'nowrap',
      background: 'rgba(245,241,232,0.08)', color: цвет ?? 'var(--d-mute)',
    }}>{children}</span>
  );
}

function SignalsQueue({ вГрафе }: { вГрафе: Set<string> }) {
  const [очередь, setОчередь] = useState<OwnershipSignal[]>([]);
  const [поСтатусам, setПоСтатусам] = useState<Record<string, number>>({});
  const [строгие, setСтрогие] = useState(true);
  const [занят, setЗанят] = useState<number | null>(null);
  const [заметка, setЗаметка] = useState<Record<number, string>>({});
  const [ошибка, setОшибка] = useState<string | null>(null);

  const загрузить = useCallback(async () => {
    try {
      const r = await getOwnershipSignals({ status: 'новый', only_strong: строгие, limit: 40 });
      setОчередь(r.очередь); setПоСтатусам(r.по_статусам); setОшибка(null);
    } catch (e) { setОшибка(e instanceof Error ? e.message : 'сбой'); }
  }, [строгие]);
  useEffect(() => { загрузить(); }, [загрузить]);

  const отметить = async (id: number, статус: 'подтверждён' | 'отклонён') => {
    setЗанят(id);
    try {
      await reviewOwnershipSignal(id, статус, заметка[id] ?? '');
      setОчередь((q) => q.filter((s) => s.id !== id));
      setПоСтатусам((p) => ({ ...p, новый: (p.новый ?? 1) - 1, [статус]: (p[статус] ?? 0) + 1 }));
    } catch (e) { setОшибка(e instanceof Error ? e.message : 'сбой'); }
    finally { setЗанят(null); }
  };

  return (
    <div className="dash-card" style={{ padding: '16px 18px' }}>
      <div className="flex items-baseline justify-between mb-2" style={{ gap: 12, flexWrap: 'wrap' }}>
        <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Поводы посмотреть на граф</div>
        <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
          новых {поСтатусам.новый ?? 0} · подтверждено {поСтатусам['подтверждён'] ?? 0} · отклонено {поСтатусам['отклонён'] ?? 0}
        </span>
      </div>
      <p style={{ fontSize: 12, color: 'var(--d-dim)', margin: '0 0 10px', lineHeight: 1.5 }}>
        Это не факты, а упоминания в новостях. «Подтвердить» — значит «посмотрел и завёл ребро
        руками»: в граф отсюда ничего не попадает автоматически.
      </p>
      <label className="flex items-center mb-3" style={{ gap: 8, fontSize: 12.5, cursor: 'pointer' }}>
        <input type="checkbox" checked={строгие} onChange={(e) => setСтрогие(e.target.checked)} />
        только строгие формулировки с процентом
      </label>
      {ошибка && <div style={{ color: 'var(--d-bad)', fontSize: 12.5, marginBottom: 8 }}>{ошибка}</div>}
      <div className="flex flex-col" style={{ gap: 7 }}>
        {очередь.length === 0 && (
          <div className="mono" style={{ fontSize: 12, color: 'var(--d-mute)' }}>очередь пуста</div>
        )}
        {очередь.map((s) => (
          <div key={s.id} className="dash-inner" style={{
            padding: '10px 12px',
            borderLeft: `3px solid ${s.edge_state === 'есть_ребро' ? 'var(--d-accent)' : s.strength === 'строгий' ? 'var(--d-ok)' : 'var(--d-line-strong)'}`,
          }}>
            <div className="flex items-center flex-wrap mb-1" style={{ gap: 6 }}>
              {s.tickers.map((t) => вГрафе.has(t)
                ? <Link key={t} to={`/admin/dashboard/graph/${t}`} style={{ textDecoration: 'none' }}><Плашка цвет="var(--d-cold)">{t} →</Плашка></Link>
                : <Плашка key={t}>{t}</Плашка>)}
              <Плашка цвет={s.strength === 'строгий' ? 'var(--d-ok)' : undefined}>{s.strength}</Плашка>
              {s.has_percent && <Плашка цвет="var(--d-warn)">есть %</Плашка>}
              <Плашка цвет={s.edge_state === 'есть_ребро' ? 'var(--d-accent)' : undefined}>
                {s.edge_state === 'есть_ребро' ? 'ребро уже есть' : 'ребра нет'}
              </Плашка>
              <span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)', marginLeft: 'auto' }}>
                {new Date(s.posted_at).toLocaleDateString('ru-RU')}
              </span>
            </div>
            <div style={{ fontSize: 12.5, color: 'var(--d-mute)', lineHeight: 1.45 }}>{s.snippet}</div>
            <div className="flex items-center mt-2" style={{ gap: 6, flexWrap: 'wrap' }}>
              <input
                value={заметка[s.id] ?? ''} placeholder="пометка (необязательно)"
                onChange={(e) => setЗаметка((z) => ({ ...z, [s.id]: e.target.value }))}
                className="mono" style={{
                  flex: 1, minWidth: 160, background: 'var(--d-sunk)', border: '1px solid var(--d-line)',
                  borderRadius: 6, padding: '4px 8px', color: 'var(--d-ink)', fontSize: 11,
                }}
              />
              <button className="dash-press" disabled={занят === s.id} onClick={() => отметить(s.id, 'подтверждён')}
                style={{ padding: '4px 10px', fontSize: 11, color: 'var(--d-ok)' }}>
                {занят === s.id ? <Loader2 size={12} className="animate-spin" /> : 'посмотрел, ребро завёл'}
              </button>
              <button className="dash-press" disabled={занят === s.id} onClick={() => отметить(s.id, 'отклонён')}
                style={{ padding: '4px 10px', fontSize: 11, color: 'var(--d-bad)' }}>отклонить</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function OwnershipGraph({ выбран, onВыбрать }: {
  /** Тикер из адреса (/graph/:тикер). */
  выбран?: string | null;
  onВыбрать?: (тикер: string | null) => void;
}) {
  const [граф, setГраф] = useState<Граф | null>(null);
  const [ошибка, setОшибка] = useState<string | null>(null);
  const [ребро, setРебро] = useState<GraphEdge | null>(null);

  useEffect(() => {
    let живо = true;
    getOwnershipGraph()
      .then((g) => { if (живо) { setГраф(g); setОшибка(null); } })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); });
    return () => { живо = false; };
  }, []);

  const { позиции, высота, полка } = useMemo(
    () => (граф ? разложить(граф.узлы) : { позиции: {}, высота: 200, полка: [] as string[] }), [граф]);
  const наПолке = useMemo(() => new Set(полка), [полка]);
  const поТикеру = useMemo(() => new Map((граф?.узлы ?? []).map((у) => [у.тикер, у])), [граф]);
  const вГрафе = useMemo(() => new Set(поТикеру.keys()), [поТикеру]);

  const выбратьУзел = (t: string | null) => { setРебро(null); onВыбрать?.(t); };
  const узел = выбран ? поТикеру.get(выбран) ?? null : null;
  const рёбраУзла = useMemo(
    () => (граф && выбран ? граф.рёбра.filter((e) => e.от === выбран || e.к === выбран) : []),
    [граф, выбран]);
  const соседи = useMemo(() => new Set(рёбраУзла.flatMap((e) => [e.от, e.к])), [рёбраУзла]);

  if (ошибка) return <div className="dash-card" style={{ padding: 16, color: 'var(--d-bad)', fontSize: 13 }}>{ошибка}</div>;
  if (!граф) return <div className="dash-card mono" style={{ padding: 16, fontSize: 12, color: 'var(--d-dim)' }}>собираю граф…</div>;

  const и = граф.итого;

  return (
    <div className="flex flex-col" style={{ gap: 12 }}>
      <div className="dash-card" style={{ padding: '16px 18px' }}>
        <div className="flex items-baseline justify-between mb-2" style={{ gap: 12, flexWrap: 'wrap' }}>
          <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Связи между компаниями</div>
          <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
            {и.рёбер} рёбер · {и.узлов} компаний · {и.казначейских} казначейских пакетов · {граф.компоненты.length} островов
          </span>
        </div>
        <div className="flex flex-wrap mb-3" style={{ gap: 14, fontSize: 11, color: 'var(--d-dim)' }}>
          {([['var(--d-ok)', 'снимок моложе года'], ['var(--d-warn)', 'до двух лет'],
             ['var(--d-bad)', 'старше или без даты']] as Array<[string, string]>).map(([c, t]) => (
            <span key={t} className="flex items-center" style={{ gap: 6 }}>
              <span style={{ width: 20, height: 2, background: c, display: 'inline-block' }} />{t}
            </span>
          ))}
          <span>пунктир — косвенное владение · стрелка — направление · кольцо — казначейский пакет</span>
        </div>

        <div style={{ overflowX: 'auto' }}>
          <svg viewBox={`0 0 ${ШИРИНА} ${высота}`} style={{ width: '100%', minWidth: 860, display: 'block' }}
            role="img" aria-label="Граф владения между компаниями">
            <defs>
              <marker id="own-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
                <path d="M0,0 L10,5 L0,10 z" fill="var(--d-dim)" />
              </marker>
            </defs>

            {граф.рёбра.map((e) => {
              const a = позиции[e.от], b = позиции[e.к];
              if (!a || !b) return null;
              const r = 30;
              const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1;
              const x1 = a.x + (dx / len) * r, y1 = a.y + (dy / len) * r;
              const x2 = b.x - (dx / len) * (r + 4), y2 = b.y - (dy / len) * (r + 4);
              const активно = !выбран || e.от === выбран || e.к === выбран;
              const выделено = ребро?.id === e.id;
              return (
                <line key={e.id} x1={x1} y1={y1} x2={x2} y2={y2}
                  stroke={цветВозраста(e.снимку_дней)}
                  strokeWidth={выделено ? 3.5 : 2}
                  strokeDasharray={e.косвенно ? '6 5' : undefined}
                  opacity={активно ? Math.max(0.45, e.уверенность ?? 0.7) : 0.12}
                  markerEnd={e.вид === 'без_направления' ? undefined : 'url(#own-arrow)'}
                  style={{ cursor: 'pointer' }}
                  onClick={() => { setРебро(e); }}
                />
              );
            })}

            {полка.length > 0 && (
              <text x={40} y={(позиции[полка[0]]?.y ?? 0) - 30} className="mono" fontSize={10}
                letterSpacing="1.2" fill="var(--d-dim)">
                ТОЛЬКО КАЗНАЧЕЙСКИЙ ПАКЕТ, РЁБЕР НЕТ · {полка.length}
              </text>
            )}
            {граф.узлы.map((у) => {
              const p = позиции[у.тикер]; if (!p) return null;
              const активный = выбран === у.тикер;
              const приглушён = выбран && !активный && !соседи.has(у.тикер);
              const r = наПолке.has(у.тикер) ? 14 : 22 + Math.min(у.степень, 4) * 3;
              return (
                <g key={у.тикер} transform={`translate(${p.x},${p.y})`} opacity={приглушён ? 0.25 : 1}
                  style={{ cursor: 'pointer' }} onClick={() => выбратьУзел(активный ? null : у.тикер)}>
                  {у.казначейский && (
                    <circle r={r + 7} fill="none" stroke={ФИОЛЕТ} strokeWidth={2.5} strokeDasharray="3 5" opacity={0.85} />
                  )}
                  <circle r={r} fill={у.в_справочнике ? 'var(--d-inner)' : 'var(--d-sunk)'}
                    stroke={активный ? 'var(--d-accent)' : 'var(--d-line-strong)'} strokeWidth={активный ? 3 : 1.5} />
                  <text textAnchor="middle" y={4} fontSize={наПолке.has(у.тикер) ? 9 : у.степень >= 3 ? 14 : 12}
                    fontWeight={800} fill="var(--d-ink)" className="disp">{у.тикер}</text>
                  {!наПолке.has(у.тикер) && (
                    <text textAnchor="middle" y={r + 14} fontSize={10} fill="var(--d-mute)">
                      {у.имя !== у.тикер ? у.имя.slice(0, 22) : 'нет в справочнике'}
                    </text>
                  )}
                </g>
              );
            })}
          </svg>
        </div>

        {(узел || ребро) && (
          <div className="dash-inner dash-rise mt-3" style={{ padding: '12px 14px' }}>
            {ребро ? (
              <>
                <div className="flex items-baseline justify-between mb-2" style={{ gap: 10, flexWrap: 'wrap' }}>
                  <div style={{ fontSize: 14, fontWeight: 700 }}>
                    {ребро.от} → {ребро.к}
                    {ребро.вид === 'без_направления' && <Плашка цвет="var(--d-warn)">направление не закодировано</Плашка>}
                  </div>
                  <button className="dash-press mono" style={{ fontSize: 11, padding: '3px 10px' }} onClick={() => setРебро(null)}>закрыть</button>
                </div>
                <div className="flex flex-wrap mb-2" style={{ gap: 6 }}>
                  {ребро.доля_из_текста != null && <Плашка>{ребро.доля_из_текста}% · из текста факта</Плашка>}
                  <Плашка цвет={цветВозраста(ребро.снимку_дней)}>снимок {ребро.снимок ?? '—'} · {возраст(ребро.снимку_дней)}</Плашка>
                  {ребро.уверенность != null && <Плашка>уверенность {ребро.уверенность.toFixed(2)}</Плашка>}
                  {ребро.косвенно && <Плашка цвет="var(--d-warn)">косвенно</Плашка>}
                  {ребро.спорно && <Плашка цвет="var(--d-bad)">спорно</Плашка>}
                  {ребро.источник && <Плашка>{ребро.источник}</Плашка>}
                </div>
                <div style={{ fontSize: 12.5, color: 'var(--d-mute)', lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>{ребро.текст}</div>
                {(ребро.снимку_дней ?? 9999) > 730 && (
                  <div style={{ marginTop: 8, padding: '8px 10px', borderLeft: '3px solid var(--d-warn)', fontSize: 12, color: 'var(--d-warn)' }}>
                    Осторожно в посте: доля указана на снимок старше двух лет.
                  </div>
                )}
              </>
            ) : узел && (
              <>
                <div className="flex items-baseline justify-between mb-2" style={{ gap: 10, flexWrap: 'wrap' }}>
                  <div style={{ fontSize: 14, fontWeight: 700 }}>
                    {узел.имя} <span className="mono" style={{ color: 'var(--d-dim)', fontSize: 11 }}>{узел.тикер}{узел.сектор && ` · ${узел.сектор}`}</span>
                  </div>
                  <button className="dash-press mono" style={{ fontSize: 11, padding: '3px 10px' }} onClick={() => выбратьУзел(null)}>закрыть</button>
                </div>
                {узел.казначейский && (
                  <div style={{ fontSize: 12, color: ФИОЛЕТ, marginBottom: 6 }}>
                    Казначейский пакет{узел.казначейский.доля_из_текста != null && ` ${узел.казначейский.доля_из_текста}%`} · снимок {узел.казначейский.снимок ?? '—'} — не голосует и не торгуется, реальный free float меньше номинального
                  </div>
                )}
                {рёбраУзла.length === 0 ? (
                  <div className="mono" style={{ fontSize: 11.5, color: 'var(--d-mute)' }}>рёбер нет — только казначейский пакет</div>
                ) : (
                  <div className="flex flex-col" style={{ gap: 5 }}>
                    {рёбраУзла.map((e) => (
                      <button key={e.id} onClick={() => setРебро(e)} className="mono" style={{
                        display: 'flex', gap: 10, alignItems: 'baseline', textAlign: 'left',
                        background: 'transparent', border: 'none', cursor: 'pointer', color: 'var(--d-ink)', fontSize: 11.5, padding: 0,
                      }}>
                        <span>{e.от === узел.тикер ? `владеет ${e.к}` : `принадлежит ${e.от}`}</span>
                        {e.доля_из_текста != null && <span style={{ color: 'var(--d-dim)' }}>{e.доля_из_текста}%</span>}
                        <span style={{ color: цветВозраста(e.снимку_дней), marginLeft: 'auto' }}>{возраст(e.снимку_дней)}</span>
                      </button>
                    ))}
                  </div>
                )}
                <div className="mt-2 flex" style={{ gap: 8 }}>
                  <Link to={`/admin/dashboard/posts?ticker=${узел.тикер}`} className="mono" style={{ fontSize: 11, color: 'var(--d-accent)' }}>посты по {узел.тикер} →</Link>
                  <TickerJump t={узел.тикер} кроме="Связи владения" />
                </div>
              </>
            )}
          </div>
        )}
      </div>

      <SignalsQueue вГрафе={вГрафе} />
    </div>
  );
}
