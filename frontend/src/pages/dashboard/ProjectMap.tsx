/**
 * Карта проекта: узлы и рёбра, живое состояние из pipeline_runs.
 *
 * ⚠️ СОСТОЯНИЕ ХРАНИЛИЩ И ВИТРИНЫ — ПРОИЗВОДНОЕ. У таблицы нет своего heartbeat:
 * «Фонды» живы ровно настолько, насколько жив писавший в них фетчер. Поэтому
 * состояние течёт по рёбрам слева направо, и оборванный источник красит всю свою
 * цепочку до витрины. Иначе карта показывала бы зелёный индикатор на сайте, под
 * которым лежат данные недельной давности, — то самое второе состояние, ради
 * которого всё и затевалось.
 *
 * ⚠️ РЁБРА РИСУЮТСЯ ПОД УЗЛАМИ. Порядок в SVG — это z-order; если нарисовать
 * рёбра после узлов, линии лягут поверх текста.
 */
import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { ExternalLink } from 'lucide-react';
import {
  РЁБРА, ПОДПИСИ_КОЛОНОК, УЗЛЫ, ШИРИНА, ШИРИНА_УЗЛА, ВЫСОТА_УЗЛА, ИНДИКАТОРЫ, ВЫСОТА_ВИТРИНЫ,
} from './topology';
import type { УзелКарты } from './topology';
import type { DashboardOverview, DashboardProcess } from '../../services/api';

const САЙТ = 'https://framedata.ru';
const числоРус = (n: number) => n.toLocaleString('ru-RU');
/** Высота узла: витрина индикаторов выше остальных — в ней чипы. */
const высотаУзла = (у: УзелКарты) => (у.id === 'o_site' ? ВЫСОТА_ВИТРИНЫ : ВЫСОТА_УЗЛА);

/** Русское склонение: 1 процесс, 2–4 процесса, 5+ процессов. Тернарник
 *  «один/много» даёт «2 процессов» — мелочь, но она сразу читается как машинный
 *  текст, а панель и так им перегружена. */
function процессов(n: number): string {
  const сотня = n % 100;
  const единица = n % 10;
  if (сотня >= 11 && сотня <= 14) return `${n} процессов`;
  if (единица === 1) return `${n} процесс`;
  if (единица >= 2 && единица <= 4) return `${n} процесса`;
  return `${n} процессов`;
}

/** Чем меньше число, тем хуже. Худшее и побеждает при слиянии. */
const ВЕС: Record<string, number> = {
  сломан: 0, молчит: 1, отстаёт: 2, работает: 3, неизвестно: 4,
};

export type СостояниеУзла = 'сломан' | 'молчит' | 'отстаёт' | 'работает' | 'неизвестно';

const ЦВЕТ: Record<СостояниеУзла, string> = {
  сломан: 'var(--d-bad)',
  молчит: 'var(--d-dim)',
  отстаёт: 'var(--d-warn)',
  работает: 'var(--d-ok)',
  неизвестно: 'var(--d-line-strong)',
};

function изСтатуса(s: string): СостояниеУзла {
  if (s === 'ok') return 'работает';
  if (s === 'молчит') return 'молчит';
  if (s === 'degraded') return 'отстаёт';
  if (s === 'неизвестно') return 'неизвестно';
  return 'сломан';
}

function хуже(a: СостояниеУзла, b: СостояниеУзла): СостояниеУзла {
  return ВЕС[a] <= ВЕС[b] ? a : b;
}

/**
 * Состояние каждого узла. Сначала прямое (у кого есть пайплайны), затем протекает
 * по рёбрам. Проход по колонкам слева направо: топология слоистая, поэтому одного
 * прохода на слой достаточно и цикл невозможен.
 */
function состояния(процессы: DashboardProcess[]): Record<string, СостояниеУзла> {
  const поИмени = new Map(процессы.map((п) => [п.имя, п]));
  const итог: Record<string, СостояниеУзла> = {};

  for (const у of УЗЛЫ) {
    if (у.пайплайны.length === 0) { итог[у.id] = 'неизвестно'; continue; }
    let s: СостояниеУзла = 'неизвестно';
    let нашлись = false;
    for (const имя of у.пайплайны) {
      const п = поИмени.get(имя);
      if (!п) continue;
      нашлись = true;
      s = хуже(s, изСтатуса(п.состояние));
    }
    итог[у.id] = нашлись ? s : 'неизвестно';
  }

  const порядок: Array<УзелКарты['вид']> = ['обработка', 'хранилище', 'витрина'];
  for (const слой of порядок) {
    for (const у of УЗЛЫ.filter((n) => n.вид === слой)) {
      const входящие = РЁБРА.filter((р) => р.к === у.id);
      if (входящие.length === 0) continue;
      // У узла без собственных пайплайнов состояние целиком приходит слева.
      // У узла со своими — своё не улучшаем чужим, но и не ухудшаем: он сам
      // отвечает за себя, а протухший источник виден на его же ребре.
      if (итог[у.id] === 'неизвестно') {
        let s: СостояниеУзла = 'неизвестно';
        for (const р of входящие) s = хуже(s, итог[р.от] ?? 'неизвестно');
        итог[у.id] = s;
      }
    }
  }
  return итог;
}

export default function ProjectMap({ процессы, идут, вспышки, выбран: выбранСнаружи, onВыбрать, источники, журнал }: {
  процессы: DashboardProcess[];
  /** Живые цифры у узлов-источников (ключ — id узла). */
  источники?: DashboardOverview['источники'];
  /** Прогонов и строк за сутки по каждому процессу — из журнала. */
  журнал?: DashboardOverview['журнал_сутки'];
  /** Имена пайплайнов, работающих прямо сейчас. */
  идут?: Set<string>;
  /** Только что закончившие — короткая вспышка исхода. */
  вспышки?: Map<string, 'ok' | 'fail'>;
  /** Выбранный узел, если им управляет адрес страницы (/map/:node). */
  выбран?: string | null;
  onВыбрать?: (id: string | null) => void;
}) {
  // ⚠️ Выбор узла живёт в адресе, когда страница его туда положила: тогда на узел
  // можно дать ссылку, а «назад» возвращает к прошлому узлу. Без onВыбрать
  // компонент работает по-старому, сам по себе.
  const [локально, setЛокально] = useState<string | null>(null);
  const выбран = onВыбрать ? (выбранСнаружи ?? null) : локально;
  const setВыбран = (id: string | null) => (onВыбрать ? onВыбрать(id) : setЛокально(id));
  const сост = useMemo(() => состояния(процессы), [процессы]);
  const поИмени = useMemo(() => new Map(процессы.map((п) => [п.имя, п])), [процессы]);

  // Узел «работает сейчас», если работает хоть один его пайплайн.
  const идущие = идут ?? new Set<string>();
  const вспыхнувшие = вспышки ?? new Map<string, 'ok' | 'fail'>();
  const узелИдёт = (у: УзелКарты) => у.пайплайны.some((имя) => идущие.has(имя));
  const узелВспыхнул = (у: УзелКарты): 'ok' | 'fail' | null => {
    for (const имя of у.пайплайны) {
      const в = вспыхнувшие.get(имя);
      if (в) return в;
    }
    return null;
  };

  const высота = Math.max(...УЗЛЫ.map((у) => у.y + высотаУзла(у))) + 40;
  /** Состояние чипа-индикатора — худшее из его хранилищ. */
  const состояниеИндикатора = (от: string[]): СостояниеУзла =>
    от.reduce<СостояниеУзла>((acc, id) => хуже(acc, сост[id] ?? 'неизвестно'), 'неизвестно');
  const узел = (id: string) => УЗЛЫ.find((у) => у.id === id)!;
  const связан = (id: string) =>
    выбран === null || выбран === id ||
    РЁБРА.some((р) => (р.от === выбран && р.к === id) || (р.к === выбран && р.от === id));

  const выбранныйУзел = выбран ? узел(выбран) : null;

  return (
    <div>
      {/* Легенда */}
      <div className="flex flex-wrap items-center justify-between mb-3" style={{ gap: 12 }}>
        <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Куда течёт всё хозяйство</div>
        <div className="flex flex-wrap" style={{ gap: 14, fontSize: 11, color: 'var(--d-dim)' }}>
          {([['работает', 'свежее'], ['отстаёт', 'отстаёт'], ['сломан', 'оборвано'],
             ['молчит', 'молчит']] as Array<[СостояниеУзла, string]>).map(([к, подпись]) => (
            <span key={к} className="flex items-center" style={{ gap: 6 }}>
              <span style={{ width: 20, height: 2, background: ЦВЕТ[к], display: 'inline-block' }} />
              {подпись}
            </span>
          ))}
        </div>
      </div>

      {/* ⚠️ Карта широкая по замыслу: четыре колонки не сжимаются в телефон без
          потери смысла. Поэтому горизонтальная прокрутка внутри блока, а не
          перенос колонок — страница при этом вбок НЕ едет. */}
      <div style={{ overflowX: 'auto' }}>
        <svg
          viewBox={`0 0 ${ШИРИНА} ${высота}`}
          style={{ width: '100%', minWidth: 900, display: 'block' }}
          role="img"
          aria-label="Карта потоков данных проекта"
        >
          <defs>
            <marker id="dash-arrow" viewBox="0 0 10 10" refX="9" refY="5"
              markerWidth="6" markerHeight="6" orient="auto-start-reverse">
              <path d="M0,0 L10,5 L0,10 z" fill="var(--d-dim)" />
            </marker>
          </defs>

          {ПОДПИСИ_КОЛОНОК.map((к) => (
            <text key={к.текст} x={к.x} y={12} className="mono"
              fontSize={10} letterSpacing="1.2" fill="var(--d-dim)">
              {к.текст.toUpperCase()}
            </text>
          ))}

          {/* Рёбра — под узлами */}
          {РЁБРА.map((р) => {
            const a = узел(р.от); const b = узел(р.к);
            const x1 = a.x + ШИРИНА_УЗЛА; const y1 = a.y + высотаУзла(a) / 2;
            const x2 = b.x;               const y2 = b.y + высотаУзла(b) / 2;
            const dx = (x2 - x1) / 2;
            const s = сост[р.от] ?? 'неизвестно';
            const активно = выбран === null || выбран === р.от || выбран === р.к;
            // ⚠️ Ребро «течёт», только когда процесс идёт ПРЯМО СЕЙЧАС. Первая
            // версия анимировала всё зелёное подряд — экран мерцал целиком и
            // сообщал ровно ноль: если движется всё, не движется ничто.
            const течёт = узелИдёт(узел(р.от)) || узелИдёт(узел(р.к));
            return (
              <path
                key={`${р.от}-${р.к}`}
                d={`M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`}
                fill="none"
                stroke={ЦВЕТ[s]}
                strokeWidth={течёт ? 2.6 : (выбран && активно ? 2.4 : 1.4)}
                strokeDasharray={р.ручное && !течёт ? '5 4' : undefined}
                opacity={активно ? (течёт ? 1 : 0.75) : 0.12}
                className={течёт && активно ? 'dash-edge-live' : undefined}
                markerEnd="url(#dash-arrow)"
              />
            );
          })}

          {/* Узлы */}
          {УЗЛЫ.map((у) => {
            const s = сост[у.id] ?? 'неизвестно';
            const виден = связан(у.id);
            const активный = выбран === у.id;
            const работает = узелИдёт(у);
            const вспышка = узелВспыхнул(у);
            const h = высотаУзла(у);
            const живое = у.вид === 'источник' ? источники?.[у.id]?.заголовок : undefined;
            const живоеТревога = у.вид === 'источник' && !!источники?.[у.id]?.факты.some((ф) => ф.тревога);
            const обводка = активный ? 'var(--d-accent)'
              : работает ? 'var(--d-accent)'
              : вспышка === 'ok' ? 'var(--d-ok)'
              : вспышка === 'fail' ? 'var(--d-bad)'
              : 'var(--d-line)';
            return (
              <g
                key={у.id}
                transform={`translate(${у.x},${у.y})`}
                opacity={виден ? 1 : 0.25}
                style={{ cursor: 'pointer' }}
                onClick={() => setВыбран(активный ? null : у.id)}
              >
                <rect
                  width={ШИРИНА_УЗЛА} height={h} rx={8}
                  fill={у.вид === 'витрина' ? 'var(--d-sunk)' : 'var(--d-inner)'}
                  stroke={обводка}
                  strokeWidth={активный || работает || вспышка ? 2 : 1}
                  className={вспышка ? 'dash-flash' : undefined}
                />
                {работает && (
                  <>
                    {/* Бегущая полоса по низу узла — «идёт прямо сейчас». */}
                    <rect y={h - 3} width={ШИРИНА_УЗЛА} height={3} rx={1.5}
                      fill="var(--d-line)" />
                    <rect y={h - 3} width={ШИРИНА_УЗЛА * 0.32} height={3} rx={1.5}
                      fill="var(--d-accent)" className="dash-runner" />
                    <circle cx={ШИРИНА_УЗЛА - 14} cy={16} r={4}
                      fill="var(--d-accent)" className="dash-pulse" />
                  </>
                )}
                {/* Полоса состояния слева — читается быстрее любого текста. */}
                <rect width={3} height={h} rx={1.5} fill={ЦВЕТ[s]} />
                <text x={14} y={24} fontSize={13} fontWeight={600} fill="var(--d-ink)">
                  {у.имя}
                </text>
                <text x={14} y={41} className="mono" fontSize={10.5} fill="var(--d-mute)">
                  {у.подпись.length > 34 ? `${у.подпись.slice(0, 33)}…` : у.подпись}
                </text>
                {/* ⚠️ У источника третья строка — живая цифра, а не «N процессов»:
                    что именно приехало и сколько. «2 процесса · работает» ничего
                    не говорит о том, что квота FM исчерпана или ряд ЦБ стоит с июня. */}
                {у.пайплайны.length > 0 && (
                  <text x={14} y={55} className="mono" fontSize={10}
                    fill={работает ? 'var(--d-accent)' : живое ? (живоеТревога ? 'var(--d-warn)' : 'var(--d-mute)') : ЦВЕТ[s]}>
                    {работает
                      ? `идёт · ${у.пайплайны.filter((и) => идущие.has(и)).join(', ')}`
                      : живое
                        ? (живое.length > 40 ? `${живое.slice(0, 39)}…` : живое)
                        : `${процессов(у.пайплайны.length)} · ${s}`}
                  </text>
                )}
                {у.id === 'o_site' && ИНДИКАТОРЫ.map((и, k) => {
                  const cx = 10 + (k % 2) * (ШИРИНА_УЗЛА / 2 - 6);
                  const cy = ВЫСОТА_УЗЛА - 4 + Math.floor(k / 2) * 21;
                  const si = состояниеИндикатора(и.от);
                  return (
                    <g key={и.id} transform={`translate(${cx},${cy})`}>
                      <rect width={ШИРИНА_УЗЛА / 2 - 14} height={17} rx={8.5}
                        fill="rgba(245,241,232,0.06)" stroke={ЦВЕТ[si]} strokeWidth={1} />
                      <circle cx={9} cy={8.5} r={2.5} fill={ЦВЕТ[si]} />
                      <text x={16} y={12} fontSize={9.5} fill="var(--d-ink)">
                        {и.имя.length > 17 ? `${и.имя.slice(0, 16)}…` : и.имя}
                      </text>
                    </g>
                  );
                })}
              </g>
            );
          })}
        </svg>
      </div>

      {/* Разбор выбранного узла */}
      {выбранныйУзел && (
        <div className="dash-inner dash-rise mt-3" style={{ padding: '12px 14px' }}>
          <div className="flex items-baseline justify-between mb-2" style={{ gap: 12 }}>
            <div style={{ fontSize: 14, fontWeight: 600 }}>{выбранныйУзел.имя}</div>
            <button className="dash-press mono" style={{ fontSize: 11, padding: '3px 10px' }}
              onClick={() => setВыбран(null)}>
              закрыть
            </button>
          </div>
          {/* Живые цифры источника — выше описания: сначала «что сейчас», потом «как устроено». */}
          {выбранныйУзел.вид === 'источник' && источники?.[выбранныйУзел.id] && (
            <div className="grid mb-3" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 8 }}>
              {источники[выбранныйУзел.id].факты.map((ф) => (
                <div key={ф.ярлык} style={{
                  padding: '9px 12px', borderRadius: 8, background: 'var(--d-sunk)',
                  borderLeft: `3px solid ${ф.тревога ? 'var(--d-warn)' : 'var(--d-ok)'}`,
                }}>
                  <div className="mono" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.06em', textTransform: 'uppercase' }}>{ф.ярлык}</div>
                  <div className="disp" style={{ fontSize: 17, lineHeight: 1.2, marginTop: 2, color: ф.тревога ? 'var(--d-warn)' : 'var(--d-ink)' }}>{ф.значение}</div>
                  {ф.подпись && <div style={{ fontSize: 11.5, color: 'var(--d-mute)', marginTop: 2 }}>{ф.подпись}</div>}
                </div>
              ))}
            </div>
          )}
          {выбранныйУзел.id === 'o_site' && (
            <div className="flex flex-col mb-3" style={{ gap: 5 }}>
              {ИНДИКАТОРЫ.map((и) => {
                const si = состояниеИндикатора(и.от);
                return (
                  <div key={и.id} className="flex items-center flex-wrap" style={{ gap: 10, fontSize: 12.5 }}>
                    <span style={{ width: 8, height: 8, borderRadius: '50%', background: ЦВЕТ[si], display: 'inline-block' }} />
                    <Link to={`/admin/dashboard/indicators/${и.id}`} title="провалиться в индикатор"
                        style={{ fontWeight: 600, minWidth: 150, color: 'var(--d-ink)', textDecoration: 'none', borderBottom: '1px dotted var(--d-line-strong)' }}>{и.имя}</Link>
                    <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
                      из {и.от.map((id) => узел(id).имя.toLowerCase()).join(', ')} · {si}
                    </span>
                    <span className="flex items-center" style={{ gap: 8, marginLeft: 'auto', fontSize: 11 }}>
                      {и.от.map((id) => (
                        <button key={id} onClick={() => setВыбран(id)} className="mono"
                          style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'var(--d-accent)' }}>
                          {узел(id).имя} →
                        </button>
                      ))}
                      {и.методология && (
                        <a href={САЙТ + и.методология} target="_blank" rel="noreferrer" style={{ color: 'var(--d-mute)' }}>методология</a>
                      )}
                      <a href={САЙТ + и.путь} target="_blank" rel="noreferrer" className="flex items-center" style={{ color: 'var(--d-accent)', gap: 3 }}>
                        открыть <ExternalLink size={10} />
                      </a>
                    </span>
                  </div>
                );
              })}
            </div>
          )}
          {выбранныйУзел.детали && (
            <ul style={{
              margin: '0 0 12px', paddingLeft: 18, display: 'flex',
              flexDirection: 'column', gap: 5,
            }}>
              {выбранныйУзел.детали.map((строка) => (
                <li key={строка} style={{ fontSize: 12.5, color: 'var(--d-mute)', lineHeight: 1.5 }}>
                  {строка}
                </li>
              ))}
            </ul>
          )}
          {выбранныйУзел.пайплайны.length === 0 ? (
            <div className="mono" style={{ fontSize: 11.5, color: 'var(--d-mute)' }}>
              Своих процессов нет — состояние приходит слева по рёбрам.
            </div>
          ) : (
            <div className="flex flex-col" style={{ gap: 5 }}>
              {выбранныйУзел.пайплайны.map((имя) => {
                const п = поИмени.get(имя);
                const s = п ? изСтатуса(п.состояние) : 'неизвестно';
                return (
                  <div key={имя} className="flex items-center justify-between mono"
                    style={{ fontSize: 11.5, gap: 12 }}>
                    <Link to={`/admin/dashboard/processes?p=${имя}`}
                      style={{ color: 'var(--d-ink)', textDecoration: 'none', borderBottom: '1px dotted var(--d-line-strong)' }}>
                      {имя}
                    </Link>
                    <span title={п?.заметка || undefined}
                      style={{ color: п?.тревога ? 'var(--d-warn)' : 'var(--d-dim)', flex: 1, textAlign: 'right' }}>
                      {п?.фраза || п?.заметка || (п ? '' : 'в pipeline_runs нет — имя не совпало')}
                    </span>
                    {журнал?.[имя] && (
                      <span style={{ color: 'var(--d-dim)', whiteSpace: 'nowrap' }} title="по журналу прогонов за сутки">
                        {журнал[имя].прогонов}× за сутки{журнал[имя].строк != null ? ` · ${числоРус(журнал[имя].строк!)} стр.` : ''}
                      </span>
                    )}
                    <span style={{ color: ЦВЕТ[s], minWidth: 74, textAlign: 'right' }}>
                      {п?.часов_назад != null ? `${п.часов_назад} ч` : '—'} · {s}
                    </span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
