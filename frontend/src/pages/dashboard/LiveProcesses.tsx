/**
 * «Живые процессы» — что идёт прямо сейчас и что закончилось только что.
 *
 * ⚠️ ДВА РАЗНЫХ ВОПРОСА, ДВА РАЗНЫХ БЛОКА. «Что работает в эту секунду» и «что
 * отработало за последний час» — не одно и то же, и смешивать их в один список,
 * отсортированный по времени, значит прятать первое во втором: идущих процессов
 * обычно один-два, а закончившихся два десятка.
 *
 * ⚠️ СЕКУНДЫ ТИКАЮТ БЕЗ ЗАПРОСОВ. Счётчик «идёт N с» считается от started_at
 * локально по тику раз в секунду. Спрашивать сервер ради бегущей цифры — это
 * шестьдесят запросов в минуту ровно ни за чем.
 */
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { узелПоПайплайну } from './topology';
import type { DashboardLiveProcess, Pulse } from '../../services/api';
import { getDashboardPulse } from '../../services/api';

const ЦВЕТ: Record<string, string> = {
  ok: 'var(--d-ok)', degraded: 'var(--d-warn)', молчит: 'var(--d-dim)',
  неизвестно: 'var(--d-line-strong)',
};
const цвет = (s: string) => ЦВЕТ[s] ?? 'var(--d-bad)';

function длительность(сек: number | null | undefined): string {
  if (сек === null || сек === undefined) return '—';
  if (сек < 60) return `${сек < 10 ? сек.toFixed(1) : Math.round(сек)} с`;
  const м = Math.floor(сек / 60);
  return `${м} мин ${Math.round(сек % 60)} с`;
}

/**
 * Доля полосы длительности по ЛОГАРИФМИЧЕСКОЙ шкале.
 *
 * ⚠️ ЛИНЕЙНАЯ ЗДЕСЬ БЕСПОЛЕЗНА. Прогоны отличаются в тысячу раз: 0,1 с у
 * content_match против 98 с у funds_daily и минут у полного обхода карточек. На
 * линейной шкале всё, кроме самого долгого, — ниточка одинаковой длины, и
 * сравнить нельзя ничего. Диапазон закреплён (0,1 с … 10 мин), а не выведен из
 * текущей выборки: иначе одна и та же секунда рисуется по-разному в зависимости
 * от того, кто ещё попал в список.
 */
const СЕК_МИН = 0.1;
const СЕК_МАКС = 600;

function доляДлительности(сек: number | null | undefined): number {
  if (!сек || сек <= 0) return 0;
  const низ = Math.log10(СЕК_МИН);
  const верх = Math.log10(СЕК_МАКС);
  const x = Math.log10(Math.min(Math.max(сек, СЕК_МИН), СЕК_МАКС));
  return (x - низ) / (верх - низ);
}

function давно(сек: number | null): string {
  if (сек === null) return 'никогда';
  if (сек < 60) return `${Math.round(сек)} с назад`;
  if (сек < 3600) return `${Math.round(сек / 60)} мин назад`;
  if (сек < 172800) return `${(сек / 3600).toFixed(1)} ч назад`;
  return `${Math.round(сек / 86400)} дн назад`;
}

const числоРус = (n: number) => n.toLocaleString('ru-RU');

/**
 * Пульс за сутки, ковёр шагов и лента «капает прямо сейчас» — всё из журнала
 * прогонов (миграция 080). Обновляется по тому же тику, что и живое состояние:
 * событие о финише приходит по SSE, а этот блок перечитывает журнал при смене
 * числа завершённых прогонов, а не по таймеру.
 */
function PulseBlock({ ключ }: { ключ: number }) {
  const [пульс, setПульс] = useState<Pulse | null>(null);
  useEffect(() => {
    let живо = true;
    getDashboardPulse(24).then((p) => { if (живо) setПульс(p); }).catch(() => {});
    return () => { живо = false; };
  }, [ключ]);
  if (!пульс) return null;

  const макс = Math.max(...пульс.по_часам.map((h) => h.записей), 1);
  const теперь = new Date();
  // 24 столбца по часам — включая пустые: пустой час это тоже информация.
  const столбцы: Array<{ метка: string; h: Pulse['по_часам'][number] | null; текущий: boolean }> = [];
  for (let i = 23; i >= 0; i--) {
    const t = new Date(теперь); t.setMinutes(0, 0, 0); t.setHours(t.getHours() - i);
    const h = пульс.по_часам.find((x) => new Date(x.час).getTime() === t.getTime()) ?? null;
    столбцы.push({ метка: `${String(t.getHours()).padStart(2, '0')}`, h, текущий: i === 0 });
  }
  const цветСтатуса = (s: string) => s === 'ok' ? 'var(--d-ok)' : s === 'degraded' ? 'var(--d-warn)' : 'var(--d-bad)';

  return (
    <div className="dash-card" style={{ padding: '16px 18px' }}>
      <div className="grid" style={{ gridTemplateColumns: 'minmax(0, 1fr) minmax(280px, 380px)', gap: 16 }}>
        <div>
          <div className="flex items-baseline justify-between mb-2" style={{ gap: 12 }}>
            <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Пульс за сутки</div>
            <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
              столбец — час · высота — сколько записей · {числоРус(пульс.всего_в_журнале)} прогонов в журнале
            </span>
          </div>
          <div className="flex items-end" style={{ gap: 3, height: 96 }}>
            {столбцы.map((c, i) => {
              const v = c.h?.записей ?? 0;
              const есть = c.h !== null;
              return (
                <div key={i} className="flex flex-col items-center" style={{ flex: 1, minWidth: 0, gap: 3 }}
                  title={c.h ? `${c.метка}:00 · прогонов ${c.h.прогонов} · записей ${числоРус(c.h.записей)}${c.h.сбоев ? ` · сбоев ${c.h.сбоев}` : ''}` : `${c.метка}:00 · журнал пуст`}>
                  <div style={{
                    width: '100%', height: Math.max(есть ? 3 : 1, (v / макс) * 72), borderRadius: 2,
                    background: c.h?.сбоев ? 'var(--d-bad)' : c.текущий ? 'var(--d-accent)' : есть ? 'var(--d-cold)' : 'var(--d-line)',
                    opacity: есть ? (c.метка >= '07' && c.метка <= '23' ? 1 : 0.55) : 0.5,
                  }} />
                  <span className="mono" style={{ fontSize: 8.5, color: c.текущий ? 'var(--d-accent)' : 'var(--d-dim)' }}>
                    {i % 3 === 0 ? c.метка : ''}
                  </span>
                </div>
              );
            })}
          </div>
          <div className="mono mt-3 mb-1" style={{ fontSize: 10, letterSpacing: '1.2px', color: 'var(--d-dim)' }}>
            КОВЁР · ПОСЛЕДНИЕ {пульс.ковёр.length} ШАГОВ · КВАДРАТ = ШАГ, ЦВЕТ = ИСХОД
          </div>
          <div className="flex flex-wrap" style={{ gap: 3 }}>
            {пульс.ковёр.map((к, i) => (
              <span key={i} title={к.имя} style={{ width: 12, height: 12, borderRadius: 2, background: цветСтатуса(к.статус) }} />
            ))}
          </div>
        </div>
        <div>
          <div className="disp mb-2" style={{ fontSize: 16, fontWeight: 700 }}>Капает прямо сейчас</div>
          <div className="flex flex-col" style={{ gap: 5, maxHeight: 190, overflowY: 'auto' }}>
            {пульс.лента.slice(0, 12).map((e, i) => (
              <div key={i} className="flex" style={{ gap: 8, fontSize: 11.5, alignItems: 'baseline' }}>
                <span className="mono shrink-0" style={{ color: 'var(--d-dim)', width: 46 }}>
                  {new Date(e.когда).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}
                </span>
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: цветСтатуса(e.статус), flexShrink: 0, marginTop: 3 }} />
                <span className="min-w-0" style={{ color: e.тревога ? 'var(--d-warn)' : 'var(--d-mute)' }}>
                  <span style={{ color: 'var(--d-ink)', fontWeight: 600 }}>{e.имя}</span>
                  {' · '}{e.фраза}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

/** Переход из строки процесса на его узел карты. Обратная связь для «пайплайн → узел». */
function НаКарте({ имя }: { имя: string }) {
  const у = узелПоПайплайну(имя);
  if (!у) return null;
  return (
    <Link to={`/admin/dashboard/map/${у.id}`} className="mono shrink-0" title={у.имя}
      style={{ fontSize: 10.5, color: 'var(--d-dim)', textDecoration: 'none' }}>
      на карте →
    </Link>
  );
}

export default function LiveProcesses({ процессы, идут, вспышки, подключено, тик, подсветить }: {
  процессы: DashboardLiveProcess[];
  идут: Set<string>;
  вспышки: Map<string, 'ok' | 'fail'>;
  подключено: boolean;
  тик: number;
  /** Имя процесса из адреса (?p=…): подсветить и прокрутить к нему. */
  подсветить?: string | null;
}) {
  // Пришли по ссылке с карты — показываем именно эту строку, а не начало списка.
  useEffect(() => {
    if (!подсветить) return;
    const el = document.getElementById(`proc-${подсветить}`);
    el?.scrollIntoView({ block: 'center', behavior: 'smooth' });
  }, [подсветить, процессы]);
  const идущие = useMemo(
    () => процессы.filter((п) => идут.has(п.имя) || п.идёт),
    [процессы, идут],
  );
  // «Только что» — час: за него проходит и пятиминутный цикл, и дневной блок.
  const недавние = useMemo(
    () => процессы
      .filter((п) => !идут.has(п.имя) && п.закончил_сек_назад !== null
                     && п.закончил_сек_назад < 3600)
      .sort((a, b) => (a.закончил_сек_назад ?? 0) - (b.закончил_сек_назад ?? 0)),
    [процессы, идут],
  );
  const остальные = useMemo(
    () => процессы
      .filter((п) => !идут.has(п.имя)
                     && (п.закончил_сек_назад === null || п.закончил_сек_назад >= 3600))
      .sort((a, b) => (a.закончил_сек_назад ?? 1e12) - (b.закончил_сек_назад ?? 1e12)),
    [процессы, идут],
  );

  const завершённых = процессы.filter((п) => !идут.has(п.имя)).length + вспышки.size;

  return (
    <div className="flex flex-col" style={{ gap: 12 }}>
      <PulseBlock ключ={завершённых} />

      {/* ── Идут сейчас ── */}
      <div className="dash-card" style={{ padding: '16px 18px' }}>
        <div className="flex items-baseline justify-between mb-3" style={{ gap: 12 }}>
          <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Идут прямо сейчас</div>
          <span className="mono flex items-center" style={{ fontSize: 11, gap: 6, color: 'var(--d-dim)' }}>
            <span
              className={подключено ? 'dash-dot-live' : undefined}
              style={{
                width: 7, height: 7, borderRadius: '50%',
                background: подключено ? 'var(--d-ok)' : 'var(--d-bad)',
                display: 'inline-block',
              }}
            />
            {подключено ? 'поток событий подключён' : 'поток оборван — показано последнее'}
          </span>
        </div>

        {идущие.length === 0 ? (
          <div className="dash-inner" style={{ padding: '18px 14px', textAlign: 'center' }}>
            <div className="mono" style={{ fontSize: 12, color: 'var(--d-mute)' }}>
              сейчас ничего не выполняется
            </div>
            <div className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)', marginTop: 4 }}>
              пятиминутный цикл — в торговые часы, дневной блок — в 19:10 МСК
            </div>
          </div>
        ) : (
          <div className="flex flex-col" style={{ gap: 7 }}>
            {идущие.map((п) => {
              const сек = (п.идёт_сек ?? 0) + тик;
              return (
                <div key={п.имя} className="dash-inner"
                  style={{ padding: '11px 13px', border: '1px solid var(--d-accent)' }}>
                  <div className="flex items-center justify-between" style={{ gap: 12 }}>
                    <div className="flex items-center min-w-0" style={{ gap: 9 }}>
                      <span className="dash-pulse" style={{
                        width: 8, height: 8, borderRadius: '50%',
                        background: 'var(--d-accent)', flexShrink: 0,
                      }} />
                      <span style={{ fontSize: 13.5, fontWeight: 600 }}>{п.имя}</span>
                    </div>
                    <span className="mono" style={{ fontSize: 12, color: 'var(--d-accent)' }}>
                      {длительность(сек)}
                    </span>
                  </div>
                  {/* Бегущая полоса вместо процентов: сколько осталось, мы не знаем,
                      а рисовать выдуманный прогресс — врать. */}
                  <div style={{
                    height: 3, borderRadius: 2, background: 'var(--d-line)',
                    marginTop: 9, overflow: 'hidden',
                  }}>
                    <div className="dash-runner" style={{
                      width: '32%', height: 3, borderRadius: 2, background: 'var(--d-accent)',
                    }} />
                  </div>
                  {п.длился_сек !== null && (
                    <div className="mono" style={{ fontSize: 10.5, color: 'var(--d-dim)', marginTop: 6 }}>
                      в прошлый раз занял {длительность(п.длился_сек)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ── Закончили за последний час ── */}
      <div className="dash-card" style={{ padding: '16px 18px' }}>
        <div className="flex items-baseline justify-between mb-3" style={{ gap: 12 }}>
          <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Отработали за час</div>
          <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
            {недавние.length} из {процессы.length} · шкала длительности логарифмическая
          </span>
        </div>
        {недавние.length === 0 ? (
          <div className="mono" style={{ fontSize: 12, color: 'var(--d-mute)' }}>
            за последний час не отработал никто
          </div>
        ) : (
          <div className="flex flex-col" style={{ gap: 6 }}>
            {недавние.map((п) => {
              const в = вспышки.get(п.имя);
              return (
                <div key={п.имя} id={`proc-${п.имя}`}
                  className={`dash-inner flex items-center${в ? ' dash-flash' : ''}`}
                  style={{
                    gap: 12, padding: '9px 12px',
                    border: в ? `1px solid ${в === 'ok' ? 'var(--d-ok)' : 'var(--d-bad)'}`
                              : подсветить === п.имя ? '1px solid var(--d-accent)' : '1px solid transparent',
                  }}>
                  <span style={{
                    width: 3, alignSelf: 'stretch', borderRadius: 2,
                    background: цвет(п.состояние),
                  }} />
                  <div className="min-w-0" style={{ flex: 1 }}>
                    <div className="flex items-center" style={{ gap: 6 }}>
                      <span style={{ fontSize: 13, fontWeight: 600 }}>{п.имя}</span>
                      {/* «Пусто, но зелёно» — то, ради чего перевод и делался:
                          статус ok, а по сути ничего не сделано. */}
                      {п.тревога && (
                        <span className="mono" style={{
                          fontSize: 9.5, padding: '1px 6px', borderRadius: 999,
                          background: 'rgba(242,162,74,0.16)', color: 'var(--d-warn)',
                        }}>пусто</span>
                      )}
                    </div>
                    {(п.фраза || п.заметка) && (
                      <div className="truncate" title={п.заметка || undefined}
                        style={{ fontSize: 11.5, color: п.тревога ? 'var(--d-warn)' : 'var(--d-mute)' }}>
                        {п.фраза || п.заметка}
                        {/* Быстрее обычного в разы — по медиане из журнала. Только при
                            ok и только если история есть: без неё сказать нечего. */}
                        {п.состояние === 'ok' && п.типично_сек != null && п.длился_сек != null
                          && п.типично_сек >= 2 && п.длился_сек < п.типично_сек / 5 && (
                          <span className="mono" style={{ color: 'var(--d-warn)', marginLeft: 6, fontSize: 10.5 }}>
                            · в {Math.round(п.типично_сек / Math.max(п.длился_сек, 0.1))} раз быстрее обычного
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                  {/* Полоса длительности: 0,1 с и 10 минут на одной картинке. */}
                  <span className="shrink-0" style={{ width: 84 }} title={длительность(п.длился_сек)}>
                    <span style={{
                      display: 'block', height: 5, borderRadius: 2.5, background: 'var(--d-line)',
                    }}>
                      <span style={{
                        display: 'block',
                        width: `${Math.max(п.длился_сек ? 3 : 0, доляДлительности(п.длился_сек) * 100)}%`,
                        height: 5, borderRadius: 2.5, background: 'var(--d-cold)',
                      }} />
                    </span>
                  </span>
                  <span className="mono shrink-0" style={{ fontSize: 11, color: 'var(--d-mute)', minWidth: 96, textAlign: 'right' }}>
                    {давно(п.закончил_сек_назад)} · {длительность(п.длился_сек)}
                  </span>
                  <span className="mono shrink-0"
                    style={{ fontSize: 11, color: цвет(п.состояние), minWidth: 58, textAlign: 'right' }}>
                    {п.состояние}
                  </span>
                  <НаКарте имя={п.имя} />
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ── Давно не запускались ── */}
      {остальные.length > 0 && (
        <div className="dash-card" style={{ padding: '16px 18px' }}>
          <div className="flex items-baseline justify-between mb-3" style={{ gap: 12 }}>
            <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>Дольше часа без запуска</div>
            <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
              суточные и недельные — это норма
            </span>
          </div>
          <div className="flex flex-col" style={{ gap: 6 }}>
            {остальные.map((п) => (
              <div key={п.имя} id={`proc-${п.имя}`} className="dash-inner flex items-center"
                style={{ gap: 12, padding: '8px 12px',
                         border: подсветить === п.имя ? '1px solid var(--d-accent)' : '1px solid transparent' }}>
                <span style={{
                  width: 3, alignSelf: 'stretch', borderRadius: 2, background: цвет(п.состояние),
                }} />
                <span className="min-w-0 truncate" style={{ flex: 1, fontSize: 12.5 }}>{п.имя}</span>
                <span className="mono shrink-0" style={{ fontSize: 11, color: 'var(--d-mute)' }}>
                  {давно(п.закончил_сек_назад)}
                </span>
                <span className="mono shrink-0"
                  style={{ fontSize: 11, color: цвет(п.состояние), minWidth: 58, textAlign: 'right' }}>
                  {п.состояние}
                </span>
                <НаКарте имя={п.имя} />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
