/**
 * Завод постов: воронка → список кандидатов → разбор пути одного.
 *
 * ⚠️ СПИСОК ОТДАЁТ ВСЕ СТАТУСЫ, ВКЛЮЧАЯ ОТСЕВ. Соблазн показывать только те
 * кандидаты, где след полный, велик: у 1069 отсеянных нет ни черновика, ни судьи.
 * Но вопрос «почему получилось именно так» задаётся как раз к ним, а
 * опубликованных всего пять — на них экран был бы пустым.
 *
 * ⚠️ ЧЕГО В СЛЕДЕ НЕТ — ГОВОРИМ ВСЛУХ. У постов до 31.08 не было судьи, след
 * агента пишется с 04.09, а снимок брифа не сохраняется вовсе. Пустой блок
 * читается как «всё хорошо, просто ничего не происходило» — это ложь умолчанием.
 * Поэтому бэкенд отдаёт список пробелов, и он показывается наравне с данными.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, ExternalLink, Loader2, Search } from 'lucide-react';
import { getPostDetail, getPostList } from '../../services/api';
import type { PostDetail, PostList, PostListItem } from '../../services/api';

const ЦВЕТ_СТАТУСА: Record<string, string> = {
  published: 'var(--d-ok)',
  draft_ready: 'var(--d-accent)',
  in_review: 'var(--d-accent)',
  pending: 'var(--d-cold)',
  candidate: 'var(--d-cold)',
  discarded: 'var(--d-dim)',
  no_data: 'var(--d-warn)',
  rejected: 'var(--d-bad)',
};
const цветСтатуса = (s: string) => ЦВЕТ_СТАТУСА[s] ?? 'var(--d-line-strong)';

const ЦВЕТ_ИСХОДА: Record<string, string> = {
  взято: 'var(--d-ok)', не_взято: 'var(--d-warn)', пусто: 'var(--d-bad)',
};

const ЦВЕТ_ВЕРДИКТА: Record<string, string> = {
  годится: 'var(--d-ok)', спорно: 'var(--d-warn)', брак: 'var(--d-bad)',
};

const числоРус = (n: number) => n.toLocaleString('ru-RU');

function датаРус(iso: string | null | undefined): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('ru-RU',
    { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function Плашка({ children, цвет }: { children: React.ReactNode; цвет?: string }) {
  return (
    <span className="mono" style={{
      fontSize: 10.5, padding: '2px 8px', borderRadius: 999, whiteSpace: 'nowrap',
      background: 'rgba(245,241,232,0.08)', color: цвет ?? 'var(--d-mute)',
    }}>{children}</span>
  );
}

function Секция({ титул, метка, children }: {
  титул: string; метка?: React.ReactNode; children: React.ReactNode;
}) {
  return (
    <div className="dash-inner" style={{ padding: '13px 15px' }}>
      <div className="flex items-baseline justify-between mb-2" style={{ gap: 10 }}>
        <div style={{ fontSize: 13, fontWeight: 700 }}>{титул}</div>
        {метка}
      </div>
      {children}
    </div>
  );
}

/** Столбиковая воронка: этапы идут путём кандидата, а не по убыванию величины. */
function Воронка({ этапы, активный, выбрать }: {
  этапы: PostList['этапы'];
  активный: string;
  выбрать: (код: string) => void;
}) {
  const макс = Math.max(...этапы.map((э) => э.сколько), 1);
  const всего = этапы.reduce((s, э) => s + э.сколько, 0);
  const опубликовано = этапы.find((э) => э.код === 'published')?.сколько ?? 0;
  return (
    <div className="dash-card" style={{ padding: '16px 18px', marginBottom: 12 }}>
      <div className="flex items-baseline justify-between mb-3" style={{ gap: 12, flexWrap: 'wrap' }}>
        <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Где теряются кандидаты</div>
        <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
          {числоРус(опубликовано)} из {числоРус(всего)} дошли до канала ·{' '}
          {всего ? ((опубликовано / всего) * 100).toFixed(2) : '0'}%
        </span>
      </div>
      <div className="flex flex-col" style={{ gap: 8 }}>
        {этапы.map((э) => (
          <button
            key={э.код}
            onClick={() => выбрать(активный === э.код ? '' : э.код)}
            style={{
              background: 'transparent', border: 'none', padding: 0, cursor: 'pointer',
              textAlign: 'left', width: '100%',
              opacity: активный && активный !== э.код ? 0.45 : 1,
            }}
          >
            <div className="flex justify-between mb-1" style={{ gap: 10, fontSize: 12.5 }}>
              <span style={{ color: активный === э.код ? 'var(--d-ink)' : 'var(--d-mute)' }}>
                {э.подпись}
                <span className="mono" style={{ color: 'var(--d-dim)', fontSize: 10.5, marginLeft: 6 }}>
                  {э.код}
                </span>
              </span>
              <span className="mono" style={{ fontWeight: 600 }}>{числоРус(э.сколько)}</span>
            </div>
            <div style={{ height: 6, borderRadius: 3, background: 'var(--d-line)' }}>
              <div style={{
                width: `${Math.max(э.сколько ? 1.5 : 0, (э.сколько / макс) * 100)}%`,
                height: 6, borderRadius: 3, background: цветСтатуса(э.код),
              }} />
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

/**
 * ⚠️ ИМЯ ЛАТИНИЦЕЙ, И ЭТО НЕ ПРИДИРКА. Компонент назывался «Разбор», и правило
 * rules-of-hooks его не признавало компонентом: кириллическая «Р» не проходит
 * проверку на заглавную латинскую букву. Значит порядок хуков внутри НЕ
 * проверялся вовсе — а это ровно тот класс ошибок, который в рантайме проявляется
 * редко и загадочно. Остальные здешние компоненты хуков не зовут, поэтому у них
 * это молчало.
 */
function PostTrace({ id, назад }: { id: number; назад: () => void }) {
  const [d, setD] = useState<PostDetail | null>(null);
  const [ошибка, setОшибка] = useState<string | null>(null);

  useEffect(() => {
    let живо = true;
    getPostDetail(id)
      .then((r) => { if (живо) { setD(r); setОшибка(null); } })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); });
    return () => { живо = false; };
  }, [id]);

  if (ошибка) {
    return (
      <div className="dash-card" style={{ padding: '16px 18px' }}>
        <button className="dash-press mb-3" style={{ padding: '4px 10px', fontSize: 11 }} onClick={назад}>
          ← к списку
        </button>
        <div style={{ color: 'var(--d-bad)', fontSize: 13 }}>{ошибка}</div>
      </div>
    );
  }
  if (!d) {
    return (
      <div className="dash-card mono" style={{ padding: '16px 18px', fontSize: 12, color: 'var(--d-dim)' }}>
        собираю путь кандидата…
      </div>
    );
  }

  const к = d.кандидат;
  const абзацы = Array.isArray(d.шаг_г_судья.абзацы)
    ? (d.шаг_г_судья.абзацы as Array<Record<string, unknown>>) : [];

  return (
    <div className="dash-card" style={{ padding: '16px 18px' }}>
      <div className="flex items-center justify-between mb-3" style={{ gap: 12, flexWrap: 'wrap' }}>
        <button className="dash-press flex items-center" style={{ padding: '4px 10px', gap: 5, fontSize: 11 }} onClick={назад}>
          <ArrowLeft size={13} /> к списку
        </button>
        <div className="flex items-center" style={{ gap: 7, flexWrap: 'wrap' }}>
          <Плашка цвет={цветСтатуса(к.статус)}>{к.статус_подпись}</Плашка>
          {к.тикеры.map((t) => <Плашка key={t} цвет="var(--d-cold)">{t}</Плашка>)}
          {к.версия_брифа != null && <Плашка>бриф v{к.версия_брифа}</Плашка>}
          <Плашка>#{к.id}</Плашка>
        </div>
      </div>

      <h3 className="disp" style={{ fontSize: 19, fontWeight: 700, margin: '0 0 4px', lineHeight: 1.25 }}>
        {к.заголовок}
      </h3>
      <div className="mono mb-3" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
        {датаРус(к.создан)}
        {к.опубликован && <> · опубликован {датаРус(к.опубликован)}</>}
        {к.ссылка && (
          <> · <a href={к.ссылка} target="_blank" rel="noreferrer"
                  style={{ color: 'var(--d-accent)' }}>
            исходный пост <ExternalLink size={10} style={{ display: 'inline', verticalAlign: -1 }} />
          </a></>
        )}
      </div>

      <div className="flex flex-col" style={{ gap: 9 }}>

        {d.новость && (
          <Секция титул="Пришла новость"
            метка={<span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
              {String(d.новость.channel)} · {числоРус(Number(d.новость.views ?? 0))} просмотров
            </span>}>
            <div style={{ fontSize: 12.5, color: 'var(--d-mute)', whiteSpace: 'pre-wrap' }}>
              {String(d.новость.text ?? '').slice(0, 700)}
            </div>
            {d.разгон_репостов.length > 0 && (
              <div className="mt-3">
                <div className="mono mb-1" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
                  разгон репостов по минутам · решение принимается на третьей
                  {d.признан_хайпом != null && (d.признан_хайпом ? ' · признан хайпом' : ' · хайпом не признан')}
                </div>
                <div className="flex items-end" style={{ gap: 5, height: 44 }}>
                  {d.разгон_репостов.map((т) => {
                    const макс = Math.max(...d.разгон_репостов.map((x) => x.репостов), 1);
                    return (
                      <div key={т.минута} className="flex flex-col items-center" style={{ gap: 3 }}>
                        <div style={{
                          width: 22, height: Math.max(3, (т.репостов / макс) * 30),
                          borderRadius: 2,
                          background: т.минута === 3 ? 'var(--d-accent)' : 'var(--d-cold)',
                        }} />
                        <span className="mono" style={{ fontSize: 9, color: 'var(--d-dim)' }}>{т.минута}м</span>
                        <span className="mono" style={{ fontSize: 9, color: 'var(--d-mute)' }}>{т.репостов}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </Секция>
        )}

        {d.шаг_н_хайп.решение != null && (
          <Секция титул="Шаг Н · шутка или новость"
            метка={<Плашка цвет={d.шаг_н_хайп.решение ? 'var(--d-ok)' : 'var(--d-warn)'}>
              {d.шаг_н_хайп.решение ? 'новость' : 'не новость'}
            </Плашка>}>
            <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
              {датаРус(d.шаг_н_хайп.когда)}
            </div>
          </Секция>
        )}

        {d.шаг_а_релевантность.обоснование && (
          <Секция титул="Шаг А · судья релевантности"
            метка={<div className="flex" style={{ gap: 6 }}>
              {к.тип_события && <Плашка>{к.тип_события}</Плашка>}
              {к.важность != null && <Плашка цвет={к.важность >= 3 ? 'var(--d-ok)' : 'var(--d-warn)'}>
                важность {к.важность}/5
              </Плашка>}
            </div>}>
            <div style={{ fontSize: 12.5, color: 'var(--d-mute)', whiteSpace: 'pre-wrap' }}>
              {d.шаг_а_релевантность.обоснование}
            </div>
          </Секция>
        )}

        {d.шаг_б_данные.аномалия && (
          <Секция титул="Шаг Б · нашлись данные"
            метка={<Плашка цвет="var(--d-accent)">
              {String(d.шаг_б_данные.аномалия.asset_id)} · {String(d.шаг_б_данные.аномалия.clgroup)}
            </Плашка>}>
            <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 3 }}>
              {String(d.шаг_б_данные.аномалия.headline)}
            </div>
            <div className="mono" style={{ fontSize: 11.5, color: 'var(--d-mute)' }}>
              {String(d.шаг_б_данные.аномалия.context)} · {String(d.шаг_б_данные.аномалия.signal_date)}
            </div>
          </Секция>
        )}

        {d.след.length > 0 && (
          <Секция титул="Что агент спрашивал у базы"
            метка={<span className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
              {d.след.length} обращений
            </span>}>
            <div className="flex flex-col" style={{ gap: 6 }}>
              {d.след.map((ш, i) => (
                <div key={i} className="flex" style={{ gap: 10, alignItems: 'flex-start' }}>
                  <span style={{
                    width: 3, alignSelf: 'stretch', borderRadius: 2,
                    background: ЦВЕТ_ИСХОДА[ш.исход] ?? 'var(--d-line-strong)',
                  }} />
                  <div style={{ minWidth: 0, flex: 1 }}>
                    <div className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
                      {ш.шаг} · {ш.источник}{ш.нашлось != null && ` · нашлось ${ш.нашлось}`}
                    </div>
                    <div style={{ fontSize: 12.5 }}>{ш.вопрос}</div>
                    {ш.результат && (
                      <div style={{ fontSize: 12, color: 'var(--d-mute)' }}>{ш.результат}</div>
                    )}
                    {ш.почему && (
                      <div style={{ fontSize: 11.5, color: 'var(--d-warn)' }}>{ш.почему}</div>
                    )}
                  </div>
                  <Плашка цвет={ЦВЕТ_ИСХОДА[ш.исход]}>{ш.исход}</Плашка>
                </div>
              ))}
            </div>
          </Секция>
        )}

        {(d.шаг_в_черновик.текст || d.шаг_в_черновик.отказ) && (
          <Секция титул="Шаг В · черновик"
            метка={d.шаг_в_черновик.правил_человек
              ? <Плашка цвет="var(--d-accent)">правил человек</Плашка> : undefined}>
            {d.шаг_в_черновик.отказ && (
              <div style={{ fontSize: 12.5, color: 'var(--d-warn)', marginBottom: 8 }}>
                Отказался писать: {d.шаг_в_черновик.отказ}
              </div>
            )}
            {d.шаг_в_черновик.текст && (
              <div style={{ fontSize: 13, whiteSpace: 'pre-wrap', lineHeight: 1.55 }}>
                {d.шаг_в_черновик.текст}
              </div>
            )}
            {d.шаг_в_черновик.аннотация && (
              <div className="mono" style={{
                fontSize: 11, color: 'var(--d-dim)', marginTop: 10,
                paddingTop: 8, borderTop: '1px solid var(--d-line)',
              }}>{d.шаг_в_черновик.аннотация}</div>
            )}
          </Секция>
        )}

        {(d.шаг_г_судья.вердикт || абзацы.length > 0) && (
          <Секция титул="Шаг Г · судья черновика"
            метка={d.шаг_г_судья.вердикт
              ? <Плашка цвет={ЦВЕТ_ВЕРДИКТА[d.шаг_г_судья.вердикт]}>{d.шаг_г_судья.вердикт}</Плашка>
              : undefined}>
            {d.шаг_г_судья.провалено.length > 0 && (
              <div className="flex flex-wrap mb-2" style={{ gap: 5 }}>
                {d.шаг_г_судья.провалено.map((в) => (
                  <Плашка key={в} цвет="var(--d-bad)">{в}</Плашка>
                ))}
              </div>
            )}
            {абзацы.length > 0 && (
              <div className="flex flex-col" style={{ gap: 6 }}>
                {абзацы.map((а, i) => {
                  const принят = а.supported !== false;
                  return (
                    <div key={i} style={{
                      padding: '8px 10px', borderRadius: 6,
                      background: принят ? 'rgba(91,212,156,0.10)' : 'rgba(255,122,92,0.12)',
                      borderLeft: `3px solid ${принят ? 'var(--d-ok)' : 'var(--d-bad)'}`,
                    }}>
                      <div className="mono" style={{ fontSize: 10, color: 'var(--d-dim)' }}>
                        абзац {i + 1} · {принят ? 'с опорой' : 'без опоры'}
                      </div>
                      {typeof а.claim === 'string' && (
                        <div style={{ fontSize: 12.5 }}>{а.claim}</div>
                      )}
                      {/* ⚠️ Сомнение красное ТОЛЬКО у абзаца без опоры. Судья
                          записывает оговорку и там, где абзац принял, — красный
                          на принятом абзаце читается как провал, которого не было. */}
                      {typeof а.doubt === 'string' && а.doubt && (
                        <div style={{
                          fontSize: 11.5, marginTop: 3,
                          color: принят ? 'var(--d-mute)' : 'var(--d-bad)',
                        }}>{а.doubt}</div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
            {d.шаг_г_судья.что_поправил && (
              <div style={{ fontSize: 12, color: 'var(--d-mute)', marginTop: 8 }}>
                Правка судьи: {d.шаг_г_судья.что_поправил}
              </div>
            )}
          </Секция>
        )}

        {(d.человек.решение || d.человек.причина) && (
          <Секция титул="Решение человека"
            метка={d.человек.решение
              ? <Плашка цвет={d.человек.решение === 'approved' ? 'var(--d-ok)' : 'var(--d-bad)'}>
                  {d.человек.решение}
                </Плашка> : undefined}>
            <div style={{ fontSize: 12.5, color: 'var(--d-mute)' }}>
              {d.человек.причина || 'причина не сохранена'}
              {d.человек.код_причины && <> · {d.человек.код_причины}</>}
            </div>
          </Секция>
        )}

        {d.чего_нет.length > 0 && (
          <div className="dash-inner" style={{
            padding: '13px 15px', borderLeft: '3px solid var(--d-warn)',
          }}>
            <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 6 }}>Чего в следе нет</div>
            <ul style={{ margin: 0, paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 4 }}>
              {d.чего_нет.map((п) => (
                <li key={п} style={{ fontSize: 12, color: 'var(--d-mute)', lineHeight: 1.5 }}>{п}</li>
              ))}
            </ul>
          </div>
        )}

      </div>
    </div>
  );
}

export default function PostFactory() {
  const [список, setСписок] = useState<PostList | null>(null);
  const [статус, setСтатус] = useState('');
  const [поиск, setПоиск] = useState('');
  const [запрос, setЗапрос] = useState('');
  const [выбран, setВыбран] = useState<number | null>(null);
  const [грузится, setГрузится] = useState(false);
  const [ошибка, setОшибка] = useState<string | null>(null);

  const загрузить = useCallback(async () => {
    setГрузится(true);
    try {
      setСписок(await getPostList({ status: статус, q: запрос, limit: 60 }));
      setОшибка(null);
    } catch (e) {
      setОшибка(e instanceof Error ? e.message : 'сбой');
    } finally {
      setГрузится(false);
    }
  }, [статус, запрос]);

  useEffect(() => { загрузить(); }, [загрузить]);

  // Поиск с задержкой: иначе запрос уходит на каждую букву.
  useEffect(() => {
    const t = setTimeout(() => setЗапрос(поиск.trim()), 400);
    return () => clearTimeout(t);
  }, [поиск]);

  const этапы = useMemo(() => список?.этапы ?? [], [список]);

  if (выбран !== null) {
    return <PostTrace id={выбран} назад={() => setВыбран(null)} />;
  }

  return (
    <div>
      {этапы.length > 0 && (
        <Воронка этапы={этапы} активный={статус} выбрать={setСтатус} />
      )}

      <div className="dash-card" style={{ padding: '16px 18px' }}>
        <div className="flex items-center justify-between mb-3" style={{ gap: 12, flexWrap: 'wrap' }}>
          <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>
            Кандидаты{статус && <span className="mono" style={{
              fontSize: 12, fontWeight: 400, color: 'var(--d-accent)', marginLeft: 8,
            }}>{статус}</span>}
          </div>
          <div className="flex items-center" style={{ gap: 8 }}>
            <label className="flex items-center" style={{
              gap: 6, background: 'var(--d-sunk)', borderRadius: 8,
              border: '1px solid var(--d-line-strong)', padding: '5px 10px',
            }}>
              <Search size={13} style={{ color: 'var(--d-dim)' }} />
              <input
                value={поиск}
                onChange={(e) => setПоиск(e.target.value)}
                placeholder="поиск по заголовку"
                className="mono"
                style={{
                  background: 'transparent', border: 'none', outline: 'none',
                  color: 'var(--d-ink)', fontSize: 11.5, width: 160,
                }}
              />
            </label>
            {грузится && <Loader2 size={14} className="animate-spin" style={{ color: 'var(--d-dim)' }} />}
          </div>
        </div>

        {ошибка && (
          <div style={{ color: 'var(--d-bad)', fontSize: 13, marginBottom: 10 }}>{ошибка}</div>
        )}

        {список && (
          <div className="mono mb-2" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
            найдено {числоРус(список.всего_по_фильтру)}
            {список.всего_по_фильтру > список.кандидаты.length &&
              ` · показаны первые ${список.кандидаты.length}`}
          </div>
        )}

        <div className="flex flex-col" style={{ gap: 6 }}>
          {(список?.кандидаты ?? []).map((к: PostListItem) => (
            <button
              key={к.id}
              onClick={() => setВыбран(к.id)}
              className="dash-inner"
              style={{
                padding: '10px 12px', border: 'none', cursor: 'pointer',
                textAlign: 'left', width: '100%', display: 'flex',
                gap: 12, alignItems: 'flex-start',
              }}
            >
              <span style={{
                width: 3, alignSelf: 'stretch', borderRadius: 2,
                background: цветСтатуса(к.статус),
              }} />
              <span style={{ minWidth: 0, flex: 1 }}>
                <span style={{ display: 'block', fontSize: 13, fontWeight: 600, color: 'var(--d-ink)' }}>
                  {к.заголовок}
                </span>
                <span className="mono" style={{ display: 'block', fontSize: 10.5, color: 'var(--d-dim)', marginTop: 2 }}>
                  {датаРус(к.создан)} · {к.источник}
                  {к.тикеры.length > 0 && ` · ${к.тикеры.join(', ')}`}
                  {к.шагов_следа > 0 && ` · след ${к.шагов_следа}`}
                </span>
              </span>
              <span className="flex shrink-0" style={{ gap: 5, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                {к.есть_аномалия && <Плашка цвет="var(--d-cold)">данные</Плашка>}
                {к.есть_черновик && <Плашка цвет="var(--d-accent)">черновик</Плашка>}
                {к.вердикт && <Плашка цвет={ЦВЕТ_ВЕРДИКТА[к.вердикт]}>{к.вердикт}</Плашка>}
                <Плашка цвет={цветСтатуса(к.статус)}>{к.статус_подпись}</Плашка>
              </span>
            </button>
          ))}
          {список && список.кандидаты.length === 0 && (
            <div className="mono" style={{ fontSize: 12, color: 'var(--d-mute)', padding: '14px 0' }}>
              под фильтр ничего не попало
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
