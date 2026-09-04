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
import { useMemo } from 'react';
import type { DashboardLiveProcess } from '../../services/api';

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

function давно(сек: number | null): string {
  if (сек === null) return 'никогда';
  if (сек < 60) return `${Math.round(сек)} с назад`;
  if (сек < 3600) return `${Math.round(сек / 60)} мин назад`;
  if (сек < 172800) return `${(сек / 3600).toFixed(1)} ч назад`;
  return `${Math.round(сек / 86400)} дн назад`;
}

export default function LiveProcesses({ процессы, идут, вспышки, подключено, тик }: {
  процессы: DashboardLiveProcess[];
  идут: Set<string>;
  вспышки: Map<string, 'ok' | 'fail'>;
  подключено: boolean;
  тик: number;
}) {
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

  return (
    <div className="flex flex-col" style={{ gap: 12 }}>
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
            {недавние.length} из {процессы.length}
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
                <div key={п.имя} className={`dash-inner flex items-center${в ? ' dash-flash' : ''}`}
                  style={{
                    gap: 12, padding: '9px 12px',
                    border: в ? `1px solid ${в === 'ok' ? 'var(--d-ok)' : 'var(--d-bad)'}`
                              : '1px solid transparent',
                  }}>
                  <span style={{
                    width: 3, alignSelf: 'stretch', borderRadius: 2,
                    background: цвет(п.состояние),
                  }} />
                  <div className="min-w-0" style={{ flex: 1 }}>
                    <div style={{ fontSize: 13, fontWeight: 600 }}>{п.имя}</div>
                    {п.заметка && (
                      <div className="mono truncate" style={{ fontSize: 10.5, color: 'var(--d-dim)' }}>
                        {п.заметка}
                      </div>
                    )}
                  </div>
                  <span className="mono shrink-0" style={{ fontSize: 11, color: 'var(--d-mute)' }}>
                    {давно(п.закончил_сек_назад)} · {длительность(п.длился_сек)}
                  </span>
                  <span className="mono shrink-0"
                    style={{ fontSize: 11, color: цвет(п.состояние), minWidth: 58, textAlign: 'right' }}>
                    {п.состояние}
                  </span>
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
              <div key={п.имя} className="dash-inner flex items-center"
                style={{ gap: 12, padding: '8px 12px' }}>
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
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
