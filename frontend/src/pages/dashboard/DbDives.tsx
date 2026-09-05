/**
 * Ныряние в базу: два десятка курируемых срезов вместо браузера по 72 таблицам.
 *
 * ⚠️ ФРОНТ НИЧЕГО НЕ ЗНАЕТ О ТАБЛИЦАХ. Колонки, фильтры, обязательные условия и
 * окна по датам приходят с бэкенда вместе с данными (dashboard_db.py). Здесь
 * только рисование: тип колонки → как показать, тип фильтра → какое поле. Белый
 * список закрыт на сервере, а не спрятан в отсутствующей кнопке.
 *
 * ⚠️ «НУЖЕН ФИЛЬТР» — СОСТОЯНИЕ, НЕ ОШИБКА. У свечей и показателей тикер обязателен:
 * без него бэкенд отдаёт пустой результат с объяснением и полным описанием среза,
 * чтобы форма нарисовалась с этим самым полем. Красный экран здесь был бы ложью:
 * ничего не сломалось, просто ещё не сказано, что смотреть.
 *
 * ⚠️ ПЕРЕХОДЫ ПО ТИКЕРУ — ГЛАВНЫЙ СПОСОБ ДВИГАТЬСЯ. Из любой ячейки с тикером
 * прыгаем в свечи, показатели, акционеров, новости, посты и связи по нему же.
 * Это и есть «перепрыгивать между всем, потому что всё связано».
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { ExternalLink, Loader2, Search } from 'lucide-react';
import TickerJump from './TickerJump';
import { getDbSlice, getDbSlices } from '../../services/api';
import type { DashboardOverview, DbColumnDef, DbFilterDef, DbSliceResult, DbSlices } from '../../services/api';

const ПРЕДЕЛ = 100;
const ОБРЕЗ = 160;

const числоРус = (n: number) => n.toLocaleString('ru-RU');
const число = (v: number, дробных = 2) =>
  v.toLocaleString('ru-RU', { maximumFractionDigits: дробных });

function датаРус(iso: string): string {
  const d = new Date(iso.length === 10 ? iso + 'T00:00:00' : iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
}
function времяРус(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const текущий = d.getFullYear() === new Date().getFullYear();
  return d.toLocaleString('ru-RU', {
    day: '2-digit', month: '2-digit', ...(текущий ? {} : { year: '2-digit' }),
    hour: '2-digit', minute: '2-digit',
  });
}

function Ячейка({ тип, v, раскрыт, раскрыть }: {
  тип: string; v: unknown; раскрыт: boolean; раскрыть: () => void;
}) {
  if (v === null || v === undefined || v === '') {
    return <span style={{ color: 'var(--d-dim)' }}>—</span>;
  }
  switch (тип) {
    case 'ts': return <span className="mono">{времяРус(String(v))}</span>;
    case 'date': return <span className="mono">{датаРус(String(v))}</span>;
    case 'int': return <span className="mono">{числоРус(Math.round(Number(v)))}</span>;
    case 'num': return <span className="mono">{число(Number(v))}</span>;
    case 'pct': return <span className="mono">{число(Number(v))} %</span>;
    case 'bool': return v ? <span style={{ color: 'var(--d-ok)' }}>да</span> : <span style={{ color: 'var(--d-dim)' }}>нет</span>;
    case 'ticker': return <TickerJump t={String(v)} />;
    case 'arr': {
      const список = Array.isArray(v) ? v : [v];
      if (!список.length) return <span style={{ color: 'var(--d-dim)' }}>—</span>;
      return (
        <span className="flex flex-wrap" style={{ gap: 4 }}>
          {список.map((x, i) => <TickerJump key={`${x}-${i}`} t={String(x)} />)}
        </span>
      );
    }
    case 'candidate':
      return (
        <Link to={`/admin/dashboard/posts/${v}`} className="mono" style={{ color: 'var(--d-accent)', textDecoration: 'none' }}>
          #{String(v)}
        </Link>
      );
    case 'pipeline':
      return (
        <Link to={`/admin/dashboard/processes?p=${v}`} className="mono" style={{ color: 'var(--d-accent)', textDecoration: 'none' }}>
          {String(v)}
        </Link>
      );
    case 'tg':
      return (
        <a href={String(v)} target="_blank" rel="noreferrer" style={{ color: 'var(--d-accent)' }} title={String(v)}>
          <ExternalLink size={12} />
        </a>
      );
    case 'json':
      return <pre className="mono" style={{ margin: 0, fontSize: 11, whiteSpace: 'pre-wrap' }}>{JSON.stringify(v, null, 1)}</pre>;
    case 'long': {
      const s = String(v);
      if (s.length <= ОБРЕЗ || раскрыт) {
        return (
          <span style={{ whiteSpace: 'pre-wrap', cursor: s.length > ОБРЕЗ ? 'pointer' : undefined }} onClick={раскрыть}>
            {s}
          </span>
        );
      }
      return (
        <span onClick={раскрыть} style={{ cursor: 'pointer' }} title="раскрыть">
          {s.slice(0, ОБРЕЗ)}<span style={{ color: 'var(--d-accent)' }}>…</span>
        </span>
      );
    }
    default:
      return <span>{String(v)}</span>;
  }
}

const ЧИСЛОВЫЕ = new Set(['int', 'num', 'pct']);

/** Форма фильтров, собранная из описания среза. */
function Фильтры({ defs, значения, изменить, применить, занято }: {
  defs: DbFilterDef[];
  значения: Record<string, string>;
  изменить: (k: string, v: string) => void;
  применить: () => void;
  занято: boolean;
}) {
  const поле = (ф: DbFilterDef) => {
    const v = значения[ф.ключ] ?? '';
    const общий = { className: 'dash-input', style: { width: ф.тип === 'ticker' ? 120 : 160 } };
    switch (ф.тип) {
      case 'select':
        return (
          <select className="dash-select" value={v} onChange={(e) => изменить(ф.ключ, e.target.value)}
            style={{ maxWidth: 260 }}>
            {!ф.обязателен && <option value="">— все —</option>}
            {ф.обязателен && !v && <option value="">— выберите —</option>}
            {(ф.варианты ?? []).map((о) => <option key={о.значение} value={о.значение}>{о.подпись}</option>)}
          </select>
        );
      case 'bool':
        return (
          <label className="flex items-center" style={{ gap: 6, fontSize: 12.5, cursor: 'pointer' }}>
            <input type="checkbox" checked={v === '1'} onChange={(e) => изменить(ф.ключ, e.target.checked ? '1' : '')} />
            {ф.подпись}
          </label>
        );
      case 'date': case 'date_from': case 'date_to':
        return <input type="date" {...общий} style={{ width: 150 }} value={v} onChange={(e) => изменить(ф.ключ, e.target.value)} />;
      case 'number':
        return <input type="number" {...общий} value={v} onChange={(e) => изменить(ф.ключ, e.target.value)} />;
      default:
        return (
          <input type="text" {...общий} value={v} placeholder={ф.подсказка ?? ''}
            onChange={(e) => изменить(ф.ключ, e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') применить(); }} />
        );
    }
  };
  return (
    <div className="flex flex-wrap items-end" style={{ gap: 10 }}>
      {defs.map((ф) => (
        <div key={ф.ключ} className="flex flex-col" style={{ gap: 3 }}>
          {ф.тип !== 'bool' && (
            <span className="mono" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.06em', textTransform: 'uppercase' }}>
              {ф.подпись}{ф.обязателен && <span style={{ color: 'var(--d-accent)' }}> *</span>}
            </span>
          )}
          {поле(ф)}
        </div>
      ))}
      <button className="dash-press flex items-center" disabled={занято} onClick={применить}
        style={{ padding: '6px 14px', gap: 6, fontSize: 12, alignSelf: 'flex-end' }}>
        {занято ? <Loader2 size={13} className="animate-spin" /> : <Search size={13} />} Показать
      </button>
    </div>
  );
}

function SliceView({ код }: { код: string }) {
  const [searchParams, setSearchParams] = useSearchParams();
  // Значения в адресе — источник истины; форма — черновик до «Показать».
  const изАдреса = useMemo(() => {
    const o: Record<string, string> = {};
    searchParams.forEach((v, k) => { if (k !== 'limit' && k !== 'offset') o[k] = v; });
    return o;
  }, [searchParams]);
  const ключАдреса = JSON.stringify(изАдреса);

  const [форма, setФорма] = useState<Record<string, string>>(изАдреса);
  const [res, setRes] = useState<DbSliceResult | null>(null);
  const [строки, setСтроки] = useState<unknown[][]>([]);
  const [занято, setЗанято] = useState(false);
  const [ошибка, setОшибка] = useState<string | null>(null);
  const [раскрытые, setРаскрытые] = useState<Set<string>>(new Set());

  useEffect(() => { setФорма(изАдреса); }, [ключАдреса]); // eslint-disable-line react-hooks/exhaustive-deps

  const загрузить = useCallback(async (offset: number) => {
    setЗанято(true);
    try {
      const r = await getDbSlice(код, изАдреса, ПРЕДЕЛ, offset);
      setRes(r);
      setСтроки((prev) => (offset ? [...prev, ...r.строки] : r.строки));
      setОшибка(null);
      // ⚠️ Значения по умолчанию (интервал = день) приходят с бэкенда и должны
      // оказаться в форме, иначе селект показывает пустоту при реально
      // применённом фильтре.
      if (!offset) setФорма((f) => ({ ...r.фильтры_применены, ...f }));
    } catch (e) {
      setОшибка(e instanceof Error ? e.message : 'сбой');
    } finally {
      setЗанято(false);
    }
  }, [код, изАдреса]);

  useEffect(() => { setРаскрытые(new Set()); загрузить(0); }, [загрузить]);

  const применить = () => {
    const p = new URLSearchParams();
    for (const [k, v] of Object.entries(форма)) if (v !== '') p.set(k, v);
    setSearchParams(p);
  };

  const срез = res?.срез;
  const колонки: DbColumnDef[] = срез?.колонки ?? [];

  return (
    <div className="dash-card" style={{ padding: '16px 18px', minWidth: 0 }}>
      {срез ? (
        <>
          <div className="flex items-baseline justify-between flex-wrap mb-1" style={{ gap: 10 }}>
            <div className="disp" style={{ fontSize: 18, fontWeight: 700 }}>{срез.имя}</div>
            <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
              {срез.таблица}
              {res && !res.ошибка && (
                <> · {res.всего_с_потолком ? `${числоРус(res.всего)}+` : числоРус(res.всего)} строк по фильтру</>
              )}
            </span>
          </div>
          <p style={{ fontSize: 12.5, color: 'var(--d-mute)', margin: '0 0 12px', lineHeight: 1.5, maxWidth: 780 }}>
            {срез.описание}
          </p>
          <div className="dash-inner mb-3" style={{ padding: '12px 14px' }}>
            <Фильтры defs={срез.фильтры} значения={форма} занято={занято}
              изменить={(k, v) => setФорма((f) => ({ ...f, [k]: v }))} применить={применить} />
            {(res?.пояснения.length || срез.предупреждение) && (
              <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)', marginTop: 8 }}>
                {[...(res?.пояснения ?? []), ...(срез.предупреждение ? [срез.предупреждение] : [])].join(' · ')}
              </div>
            )}
          </div>
        </>
      ) : (
        !ошибка && <div className="mono" style={{ fontSize: 12, color: 'var(--d-dim)' }}>открываю срез…</div>
      )}

      {ошибка && (
        <div style={{ padding: 10, borderRadius: 8, fontSize: 12.5, background: 'rgba(255,122,92,0.12)', color: 'var(--d-bad)' }}>
          {ошибка}
        </div>
      )}
      {res?.ошибка && (
        <div style={{ padding: 10, borderRadius: 8, fontSize: 12.5, background: 'rgba(242,162,74,0.12)', color: 'var(--d-warn)' }}>
          {res.ошибка}
        </div>
      )}

      {res && !res.ошибка && (
        строки.length === 0 ? (
          <div className="mono" style={{ fontSize: 12, color: 'var(--d-dim)', padding: '18px 0' }}>
            по этим условиям — пусто
          </div>
        ) : (
          <>
            <div style={{ overflowX: 'auto', maxHeight: '70vh', overflowY: 'auto', borderRadius: 8, border: '1px solid var(--d-line)' }}>
              <table className="dash-table">
                <thead>
                  <tr>{колонки.map((к) => <th key={к.ключ} className={ЧИСЛОВЫЕ.has(к.тип) ? 'num' : undefined}>{к.подпись}</th>)}</tr>
                </thead>
                <tbody>
                  {строки.map((r, i) => (
                    <tr key={i}>
                      {колонки.map((к, j) => {
                        const id = `${i}-${j}`;
                        return (
                          <td key={к.ключ} className={ЧИСЛОВЫЕ.has(к.тип) ? 'num' : undefined}
                            style={к.тип === 'long' ? { minWidth: 260, maxWidth: 560 } : undefined}>
                            <Ячейка тип={к.тип} v={r[j]} раскрыт={раскрытые.has(id)}
                              раскрыть={() => setРаскрытые((s) => { const n = new Set(s); n.has(id) ? n.delete(id) : n.add(id); return n; })} />
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="flex items-center justify-between flex-wrap mt-3" style={{ gap: 10 }}>
              <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
                показано {числоРус(строки.length)} из {res.всего_с_потолком ? `${числоРус(res.всего)}+` : числоРус(res.всего)}
              </span>
              {строки.length < res.всего && (
                <button className="dash-press" disabled={занято} onClick={() => загрузить(строки.length)}
                  style={{ padding: '5px 14px', fontSize: 12 }}>
                  {занято ? 'гружу…' : `ещё ${числоРус(Math.min(ПРЕДЕЛ, res.всего - строки.length))}`}
                </button>
              )}
            </div>
          </>
        )
      )}
    </div>
  );
}

/** Самые тяжёлые таблицы — на стартовом экране, и каждая, у которой есть срез, кликабельна. */
function Хранилища({ таблицы, поТаблице, открыть }: {
  таблицы: DashboardOverview['хранилища'];
  поТаблице: Record<string, string>;
  открыть: (код: string) => void;
}) {
  // ⚠️ ЛОГАРИФМ, И ЭТО ПОДПИСАНО. Разброс тысячекратный: candles 14 ГБ против
  // 13 МБ у хвоста. На линейной шкале всё, кроме свечей, вырождается в ниточку.
  const мин = Math.min(...таблицы.map((т) => т.байт), 1);
  const макс = Math.max(...таблицы.map((т) => т.байт), 1);
  const низ = Math.log10(Math.max(мин, 1));
  const верх = Math.log10(Math.max(макс, 10));
  const доля = (b: number) => (верх === низ ? 1 : (Math.log10(Math.max(b, 1)) - низ) / (верх - низ));
  return (
    <div className="flex flex-col" style={{ gap: 9 }}>
      {таблицы.map((т) => {
        const код = поТаблице[т.таблица];
        return (
          <div key={т.таблица}>
            <div className="flex justify-between mb-1" style={{ gap: 12, fontSize: 12.5 }}>
              {код ? (
                <button onClick={() => открыть(код)} className="mono truncate"
                  style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'var(--d-accent)', textAlign: 'left' }}>
                  {т.таблица} →
                </button>
              ) : (
                <span className="mono truncate" style={{ color: 'var(--d-mute)' }}>{т.таблица}</span>
              )}
              <span className="mono shrink-0" style={{ color: 'var(--d-dim)', fontSize: 11 }}>{числоРус(т.строк)} стр.</span>
              <span className="mono shrink-0" style={{ fontWeight: 600, minWidth: 66, textAlign: 'right' }}>{т.размер}</span>
            </div>
            <div style={{ height: 5, borderRadius: 2.5, background: 'var(--d-line)' }}>
              <div style={{ width: `${Math.max(4, доля(т.байт) * 100)}%`, height: 5, borderRadius: 2.5, background: 'var(--d-cold)' }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

export default function DbDives({ выбран, onВыбрать, хранилища }: {
  выбран: string | null;
  onВыбрать: (код: string | null) => void;
  хранилища: DashboardOverview['хранилища'];
}) {
  const [список, setСписок] = useState<DbSlices | null>(null);
  const [ошибка, setОшибка] = useState<string | null>(null);

  useEffect(() => {
    let живо = true;
    getDbSlices()
      .then((r) => { if (живо) setСписок(r); })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); });
    return () => { живо = false; };
  }, []);

  const поТаблице = useMemo(() => {
    const m: Record<string, string> = {};
    for (const г of список?.группы ?? []) for (const с of г.срезы) m[с.таблица] = с.код;
    return m;
  }, [список]);

  return (
    <div className="grid" style={{ gridTemplateColumns: 'minmax(220px, 260px) minmax(0, 1fr)', gap: 12, alignItems: 'start' }}>
      <div className="dash-card" style={{ padding: '12px 10px', position: 'sticky', top: 12 }}>
        {ошибка && <div style={{ color: 'var(--d-bad)', fontSize: 12, padding: 8 }}>{ошибка}</div>}
        {!список && !ошибка && <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)', padding: 8 }}>читаю реестр…</div>}
        {список?.группы.map((г) => (
          <div key={г.группа} className="mb-2">
            <div className="mono" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '6px 10px 3px' }}>
              {г.группа}
            </div>
            {г.срезы.map((с) => (
              <button key={с.код} className="dash-slice" data-active={выбран === с.код} onClick={() => onВыбрать(с.код)}
                title={с.описание}>
                <span className="flex justify-between items-baseline" style={{ gap: 8 }}>
                  <span className="truncate">{с.имя}</span>
                  <span className="mono shrink-0" style={{ fontSize: 10, color: 'var(--d-dim)' }}>
                    {с.строк != null ? числоРус(с.строк) : ''}
                  </span>
                </span>
              </button>
            ))}
          </div>
        ))}
      </div>

      {выбран ? (
        <SliceView key={выбран} код={выбран} />
      ) : (
        <div className="dash-card" style={{ padding: '16px 18px', minWidth: 0 }}>
          <div className="disp mb-1" style={{ fontSize: 18, fontWeight: 700 }}>Что внутри</div>
          <p style={{ fontSize: 12.5, color: 'var(--d-mute)', margin: '0 0 14px', lineHeight: 1.5, maxWidth: 720 }}>
            Слева — срезы по смыслу: не таблицы, а вопросы, которые к ним задают. У тяжёлых
            (свечи, открытый интерес, отчётность) обязателен ключ и ограничено окно по датам —
            иначе один запрос кладёт базу. Из любого тикера можно перепрыгнуть в свечи,
            показатели, акционеров, новости, посты и связи.
          </p>
          <div className="flex items-baseline justify-between mb-2" style={{ gap: 12 }}>
            <div style={{ fontSize: 13, fontWeight: 700 }}>Самые тяжёлые таблицы</div>
            <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>вес с индексами · шкала логарифмическая</span>
          </div>
          <Хранилища таблицы={хранилища} поТаблице={поТаблице} открыть={(к) => onВыбрать(к)} />
        </div>
      )}
    </div>
  );
}
