/**
 * Провалиться в индикатор: слева выбор индикатора и объекта, в центре график,
 * справа паспорт и переходы. Один шаблон на все восемь — различия живут в
 * реестре на бэкенде (dashboard_indicators.py), экран о них не знает.
 *
 * ⚠️ ГРАФИК — САЙТОВЫЙ SimpleChart, НЕ СВОЯ КОПИЯ. В проекте уже было две копии
 * слоя рисования, и правки доезжали до трёх индикаторов из четырёх. Панель
 * тёмная всегда, а SimpleChart красится сайтовыми токенами — поэтому он
 * обёрнут в data-theme="dark": токены темы наследуются вниз по дереву, и
 * график остаётся читаемым, даже если сайт у владельца в светлой теме.
 *
 * ⚠️ ПРОПУСКИ — ПО ТОРГОВЫМ ДНЯМ. «Нет точки за субботу» — не пропуск; эталон —
 * дни, когда есть значение IMOEX. Последний торговый день не проверяется:
 * дневной ряд пишется вечером.
 */
import { useEffect, useMemo, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { ExternalLink } from 'lucide-react';
import SimpleChart from '../../components/SimpleChart';
import { getIndicatorDetail, getIndicatorList } from '../../services/api';
import type { IndicatorDetail, IndicatorFreshness, IndicatorPassport } from '../../services/api';
import TickerJump from './TickerJump';

const САЙТ = 'https://framedata.ru';
const ПЕРИОДЫ: Array<[string, string]> = [['3m', '3 мес'], ['1y', 'год'], ['3y', '3 года']];
const числоРус = (n: number) => n.toLocaleString('ru-RU', { maximumFractionDigits: 2 });

function датаРус(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso.length === 10 ? iso + 'T00:00:00' : iso);
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

/** Цвет свежести: дневным таблицам — до двух торговых дней норма. */
function цветСвежести(ф: IndicatorFreshness): string {
  if (ф.дней == null) return 'var(--d-dim)';
  if (ф.дней <= 3) return 'var(--d-ok)';
  if (ф.дней <= 10) return 'var(--d-warn)';
  return 'var(--d-bad)';
}

/** Объект похож на тикер бумаги — тогда у него есть меню переходов. */
const похожНаТикер = (s: string | null) => !!s && /^[A-Z0-9]{2,6}$/.test(s);

export default function IndicatorDive({ выбран, onВыбрать }: {
  выбран: string | null;
  onВыбрать: (id: string | null) => void;
}) {
  const [searchParams, setSearchParams] = useSearchParams();
  const obj = searchParams.get('obj') ?? '';
  const period = searchParams.get('period') ?? '';

  const [список, setСписок] = useState<Array<IndicatorPassport & { свежесть: IndicatorFreshness[] }> | null>(null);
  const [d, setD] = useState<IndicatorDetail | null>(null);
  const [занято, setЗанято] = useState(false);
  const [ошибка, setОшибка] = useState<string | null>(null);
  const [поиск, setПоиск] = useState('');

  useEffect(() => {
    let живо = true;
    getIndicatorList()
      .then((r) => { if (живо) setСписок(r.индикаторы); })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); });
    return () => { живо = false; };
  }, []);

  useEffect(() => {
    if (!выбран) { setD(null); return; }
    let живо = true;
    setЗанято(true);
    getIndicatorDetail(выбран, obj || undefined, period || undefined)
      .then((r) => { if (живо) { setD(r); setОшибка(null); } })
      .catch((e) => { if (живо) setОшибка(e instanceof Error ? e.message : 'сбой'); })
      .finally(() => { if (живо) setЗанято(false); });
    return () => { живо = false; };
  }, [выбран, obj, period]);

  const задать = (k: string, v: string) => {
    const p = new URLSearchParams(searchParams);
    if (v) p.set(k, v); else p.delete(k);
    setSearchParams(p);
  };

  // ⚠️ SimpleChart ждёт {time, value}; ряды приходят парами [время, значение].
  const данные = useMemo(() => (d?.ряды[0]?.точки ?? []).map(([t, v]) => ({ time: t, value: v })), [d]);
  const вторые = useMemo(() => (d?.ряды[1]?.точки ?? []).map(([t, v]) => ({ time: t, value: v })), [d]);
  const засечки = useMemo(() => (d?.засечки ?? []).map((з) => ({
    time: з.время, label: з.метка, description: з.описание,
    color: 'rgba(255,92,43,0.18)', textColor: '#FF5C2B',
  })), [d]);
  const объектыПоПоиску = useMemo(() => {
    const q = поиск.trim().toLowerCase();
    const все = d?.объекты ?? [];
    return q ? все.filter((о) => о.подпись.toLowerCase().includes(q)) : все;
  }, [d, поиск]);

  return (
    <div className="grid" style={{ gridTemplateColumns: 'minmax(220px, 250px) minmax(0, 1fr) minmax(240px, 300px)', gap: 12, alignItems: 'start' }}>
      {/* ── слева: индикатор и объект */}
      <div className="dash-card" style={{ padding: '12px 10px', position: 'sticky', top: 12 }}>
        <div className="mono" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '4px 10px 6px' }}>
          Индикатор
        </div>
        {!список && !ошибка && <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)', padding: 8 }}>читаю реестр…</div>}
        {список?.map((и) => {
          const худшая = Math.max(...и.свежесть.map((ф) => ф.дней ?? -1));
          return (
            <button key={и.id} className="dash-slice" data-active={выбран === и.id} onClick={() => { setПоиск(''); onВыбрать(и.id); }}>
              <span className="flex items-center" style={{ gap: 8 }}>
                <span style={{
                  width: 7, height: 7, borderRadius: '50%', flexShrink: 0,
                  background: худшая < 0 ? 'var(--d-dim)' : худшая <= 3 ? 'var(--d-ok)' : худшая <= 10 ? 'var(--d-warn)' : 'var(--d-bad)',
                }} />
                <span className="truncate">{и.имя}</span>
              </span>
            </button>
          );
        })}

        {d?.индикатор.есть_объекты && (
          <div className="mt-3" style={{ borderTop: '1px solid var(--d-line)', paddingTop: 10 }}>
            <div className="mono" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '0 10px 6px' }}>
              {d.индикатор.объект_подпись} · {d.объекты.length}
            </div>
            <input className="dash-input" value={поиск} onChange={(e) => setПоиск(e.target.value)}
              placeholder="найти…" style={{ width: '100%', marginBottom: 6 }} />
            <div style={{ maxHeight: 320, overflowY: 'auto' }}>
              {объектыПоПоиску.slice(0, 200).map((о) => (
                <button key={о.значение} className="dash-slice" data-active={d.выбран === о.значение}
                  onClick={() => задать('obj', о.значение)} style={{ padding: '4px 10px', fontSize: 12 }}>
                  <span className="truncate mono" style={{ display: 'block' }}>{о.подпись}</span>
                </button>
              ))}
              {объектыПоПоиску.length > 200 && (
                <div className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)', padding: '4px 10px' }}>…и ещё {объектыПоПоиску.length - 200}, уточните поиск</div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* ── центр: график */}
      <div className="dash-card" style={{ padding: '16px 18px', minWidth: 0 }}>
        {!выбран && (
          <>
            <div className="disp mb-1" style={{ fontSize: 18, fontWeight: 700 }}>Провалиться в индикатор</div>
            <p style={{ fontSize: 12.5, color: 'var(--d-mute)', margin: 0, lineHeight: 1.5, maxWidth: 640 }}>
              Слева — восемь разделов сайта; точка рядом — свежесть худшей из таблиц, из которых он
              собран. Внутри: сам ряд, из которого считается индикатор, засечки смены контракта и
              экспираций, пропуски по торговым дням и переходы в хранилище, процессы и на сайт.
            </p>
          </>
        )}
        {ошибка && <div style={{ padding: 10, borderRadius: 8, fontSize: 12.5, background: 'rgba(255,122,92,0.12)', color: 'var(--d-bad)' }}>{ошибка}</div>}
        {выбран && d && (
          <>
            <div className="flex items-baseline justify-between flex-wrap mb-1" style={{ gap: 10 }}>
              <div className="disp flex items-baseline" style={{ fontSize: 18, fontWeight: 700, gap: 10 }}>
                {d.индикатор.имя}
                {d.выбран && (похожНаТикер(d.выбран)
                  ? <TickerJump t={d.выбран} />
                  : <span className="mono" style={{ fontSize: 12, color: 'var(--d-cold)' }}>{d.выбран}</span>)}
              </div>
              <div className="flex" style={{ gap: 4 }}>
                {ПЕРИОДЫ.map(([к, п]) => (
                  <button key={к} className="dash-tab" data-active={d.период === к} onClick={() => задать('period', к)}>{п}</button>
                ))}
              </div>
            </div>
            <p style={{ fontSize: 12.5, color: 'var(--d-mute)', margin: '0 0 10px', lineHeight: 1.5 }}>{d.индикатор.описание}</p>

            {данные.length === 0 ? (
              <div className="mono" style={{ fontSize: 12, color: 'var(--d-dim)', padding: '24px 0' }}>
                {занято ? 'строю ряд…' : 'по этому объекту за период точек нет'}
              </div>
            ) : (
              <div data-theme="dark" style={{ minHeight: 380 + 50, opacity: занято ? 0.5 : 1, transition: 'opacity 0.2s' }}>
                <SimpleChart
                  data={данные}
                  secondaryData={вторые}
                  showSecondary={вторые.length > 0}
                  height={380}
                  primaryLabel={d.ряды[0]?.имя}
                  secondaryLabel={d.ряды[1]?.имя}
                  primaryColor="#FF5C2B"
                  secondaryColor="#5DA3E9"
                  formatValue={(v) => числоРус(v)}
                  formatSecondaryValue={(v) => числоРус(v)}
                  annotations={засечки}
                  legendPosition="top"
                  showDownloadButton={false}
                  showValueHeader={false}
                  showNavigator={данные.length > 300}
                />
              </div>
            )}
            <div className="mono flex flex-wrap" style={{ fontSize: 11, color: 'var(--d-dim)', gap: 14, marginTop: 6 }}>
              <span>{числоРус(данные.length)} точек</span>
              {d.засечки.length > 0 && <span>{d.засечки.length} засечек: смена контракта и экспирации</span>}
              {d.примечание && <span>{d.примечание}</span>}
            </div>
          </>
        )}
      </div>

      {/* ── справа: паспорт и переходы */}
      <div className="dash-card" style={{ padding: '14px 16px', position: 'sticky', top: 12 }}>
        {!d && <div className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>выберите индикатор</div>}
        {d && (
          <div className="flex flex-col" style={{ gap: 12 }}>
            <div>
              <div className="mono mb-1" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Из чего собран</div>
              <div className="flex flex-col" style={{ gap: 5 }}>
                {d.свежесть.map((ф) => (
                  <div key={ф.таблица + ф.подпись} className="flex items-baseline justify-between" style={{ gap: 8, fontSize: 12 }}>
                    <span className="truncate">
                      <span className="mono" style={{ color: 'var(--d-mute)' }}>{ф.таблица}</span>
                      {ф.подпись && <span style={{ color: 'var(--d-dim)' }}> · {ф.подпись}</span>}
                    </span>
                    <span className="mono shrink-0" style={{ color: цветСвежести(ф), fontSize: 11 }}>
                      {ф.последняя ? `${датаРус(ф.последняя)} · ${ф.дней} дн` : 'справочник'}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            <div>
              <div className="mono mb-1" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Пропуски за 30 торговых дней</div>
              {!d.пропуски.считается ? (
                <div style={{ fontSize: 12, color: 'var(--d-dim)' }}>ряд не дневной — не считаются</div>
              ) : d.пропуски.пропущено.length === 0 ? (
                <div style={{ fontSize: 12, color: 'var(--d-ok)' }}>ни одного — {d.пропуски.торговых_дней} дней подряд</div>
              ) : (
                <div style={{ fontSize: 12, color: 'var(--d-warn)' }}>
                  {d.пропуски.пропущено.length} из {d.пропуски.торговых_дней}:{' '}
                  <span className="mono">{d.пропуски.пропущено.slice(0, 6).map((x) => датаРус(x)).join(', ')}{d.пропуски.пропущено.length > 6 ? '…' : ''}</span>
                </div>
              )}
            </div>

            {d.индикатор.пайплайны.length > 0 && (
              <div>
                <div className="mono mb-1" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Кто это пишет</div>
                <div className="flex flex-wrap" style={{ gap: 5 }}>
                  {d.индикатор.пайплайны.map((п) => (
                    <Link key={п} to={`/admin/dashboard/processes?p=${п}`} className="mono"
                      style={{ fontSize: 11, padding: '2px 8px', borderRadius: 999, background: 'rgba(245,241,232,0.08)', color: 'var(--d-ink)', textDecoration: 'none' }}>
                      {п}
                    </Link>
                  ))}
                </div>
              </div>
            )}

            <div>
              <div className="mono mb-1" style={{ fontSize: 10, color: 'var(--d-dim)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Куда ещё провалиться</div>
              <div className="flex flex-col" style={{ gap: 4, fontSize: 12 }}>
                <Link to={`/admin/dashboard/map/${d.индикатор.узел_карты}`} style={{ color: 'var(--d-accent)', textDecoration: 'none' }}>узел на карте →</Link>
                {d.индикатор.срезы.map((с) => (
                  <Link key={с.адрес} to={с.адрес + (d.выбран && похожНаТикер(d.выбран) && с.адрес.includes('candles') ? `?secid=${d.выбран}` : '')}
                    style={{ color: 'var(--d-accent)', textDecoration: 'none' }}>срез базы: {с.подпись} →</Link>
                ))}
                <a href={САЙТ + d.индикатор.путь} target="_blank" rel="noreferrer" className="flex items-center" style={{ color: 'var(--d-accent)', gap: 4 }}>
                  открыть на сайте <ExternalLink size={11} />
                </a>
                {d.индикатор.методология && (
                  <a href={САЙТ + d.индикатор.методология} target="_blank" rel="noreferrer" style={{ color: 'var(--d-mute)' }}>методология</a>
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
