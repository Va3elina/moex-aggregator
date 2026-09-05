/**
 * AdminDashboardPage — приборная панель проекта. Admin-only, /admin/dashboard.
 *
 * Источник: GET /api/admin/dashboard/overview (снимок собирается раз в 30 с и
 * лежит в Redis — экран его читает, а не пересчитывает).
 *
 * ⚠️ ТРИ СОСТОЯНИЯ, А НЕ ДВА. Работает / работает, но данные устарели / не
 * работает. Второе — самое частое и самое опасное: пайплайн зелёный, а цифры
 * недельной давности. Поэтому у каждого процесса рядом с состоянием стоит
 * «когда обновлялся», а возраст самих данных вынесен отдельным блоком.
 *
 * ⚠️ ВОЗРАСТ ДАННЫХ НЕ ВХОДИТ В ОБЩИЙ ВЕРДИКТ. У части компаний структура
 * акционеров старше двух лет — это норма жизни рынка, а не авария. Тревога,
 * которая горит всегда, гасит внимание ко всем остальным.
 *
 * ⚠️ ПАНЕЛЬ НА СВОИХ ТОКЕНАХ (dashboard.css, всё внутри .dash). Сайт готов и
 * меняться не должен, а панель тёмная всегда и по своему макету. Тот же приём,
 * что у песочницы: отдельная поверхность — отдельный набор переменных.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { ArrowLeft, Loader2, RefreshCw } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { getDashboardOverview } from '../services/api';
import type { DashboardOverview } from '../services/api';
import ProjectMap from './dashboard/ProjectMap';
import LiveProcesses from './dashboard/LiveProcesses';
import { useLivePipelines } from './dashboard/useLivePipelines';
import './dashboard/dashboard.css';

const ИНТЕРВАЛ_МС = 30_000;

// ⚠️ ВКЛАДКА ПРОЦЕССОВ ОДНА. Их было две — «Живые процессы» и «Процессы», — и они
// показывали одно и то же: вторая просто не знала, что идёт сейчас. Разделение
// заставляло помнить, в какой из них смотреть, и ничего за это не давало.
const ВКЛАДКИ = ['Карта', 'Процессы', 'Завод постов', 'Второй мозг', 'База'] as const;
type Вкладка = typeof ВКЛАДКИ[number];

const ПОДПИСИ_МОЗГА: Record<string, string> = {
  эмитентов: 'Эмитентов', бумаг: 'Бумаг', алиасов: 'Псевдонимов', метрик: 'Показателей',
  бумаг_с_карточкой: 'Бумаг с карточкой', документов: 'Документов',
  рёбер: 'Рёбер владения', казначейских: 'Казначейских пакетов',
  сигналов_в_очереди: 'Сигналов в очереди',
};

const ПОДПИСИ_СТАРЕНИЯ: Record<string, string> = {
  акционеры_старше_2лет: 'Структур акционеров старше 2 лет',
  акционеры_всего: 'Структур акционеров всего',
  рёбра_старше_2лет: 'Рёбер владения старше 2 лет',
};

const числоРус = (n: number) => n.toLocaleString('ru-RU');

function Плитка({ ярлык, число, подпись, цвет, рамка }: {
  ярлык: string; число: number | string; подпись: string; цвет?: string; рамка?: string;
}) {
  return (
    <div className="dash-card dash-rise" style={{
      padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 6,
      borderColor: рамка ?? 'var(--d-ink)',
    }}>
      <div style={{
        fontSize: 11, fontWeight: 600, letterSpacing: '0.08em',
        textTransform: 'uppercase', color: цвет ?? 'var(--d-dim)',
      }}>{ярлык}</div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, flexWrap: 'wrap' }}>
        <div className="disp" style={{ fontSize: 40, lineHeight: 1, color: цвет ?? 'var(--d-ink)' }}>
          {число}
        </div>
        <div style={{ fontSize: 13, color: 'var(--d-mute)' }}>{подпись}</div>
      </div>
    </div>
  );
}

function Блок({ заголовок, справа, children }: {
  заголовок: string; справа?: React.ReactNode; children: React.ReactNode;
}) {
  return (
    <div className="dash-card" style={{ padding: '16px 18px' }}>
      <div className="flex items-baseline justify-between mb-3" style={{ gap: 12 }}>
        <div className="disp" style={{ fontSize: 16, fontWeight: 700 }}>{заголовок}</div>
        {справа}
      </div>
      {children}
    </div>
  );
}


/**
 * Воронка завода постов.
 *
 * ⚠️ ГЛАВНОЕ ЧИСЛО — КОНВЕРСИЯ, А НЕ ВЫСОТА СТОЛБИКОВ. Из тысячи с лишним
 * кандидатов до канала доходит горстка, поэтому на общей шкале «опубликовано»
 * невидимо в принципе: пять против тысячи — это полпикселя. Показываем сначала
 * саму дробь крупно, а столбики нормируем на самый большой этап и держим
 * минимальную ширину, чтобы редкий этап был виден, а не угадывался.
 *
 * Порядок статусов задан руками: это путь кандидата, а не рейтинг по величине.
 */
const ПОРЯДОК_ВОРОНКИ = ['pending', 'no_data', 'discarded', 'rejected',
                         'draft_ready', 'published'];
const ПОДПИСИ_ВОРОНКИ: Record<string, string> = {
  pending: 'ждут разбора', no_data: 'нет данных', discarded: 'отсеяны судьёй',
  rejected: 'отклонены вручную', draft_ready: 'черновик готов', published: 'опубликованы',
};
const ЦВЕТ_ВОРОНКИ: Record<string, string> = {
  published: 'var(--d-ok)', draft_ready: 'var(--d-accent)', pending: 'var(--d-cold)',
};

function ВоронкаПостов({ воронка }: { воронка: Record<string, number> }) {
  const всего = Object.values(воронка).reduce((a, b) => a + b, 0);
  const опубликовано = воронка.published ?? 0;
  const конверсия = всего ? (опубликовано / всего) * 100 : 0;
  const известные = ПОРЯДОК_ВОРОНКИ.filter((к) => к in воронка);
  const прочие = Object.keys(воронка).filter((к) => !ПОРЯДОК_ВОРОНКИ.includes(к));
  const этапы = [...известные, ...прочие];
  const макс = Math.max(...Object.values(воронка), 1);

  if (всего === 0) {
    return (
      <Блок заголовок="Воронка кандидатов">
        <div className="mono" style={{ fontSize: 12, color: 'var(--d-mute)' }}>Кандидатов нет</div>
      </Блок>
    );
  }

  return (
    <Блок
      заголовок="Воронка кандидатов"
      справа={
        <span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
          всего {числоРус(всего)}
        </span>
      }
    >
      <div className="dash-inner mb-3" style={{ padding: '12px 14px' }}>
        <div className="flex items-baseline flex-wrap" style={{ gap: 10 }}>
          <span className="disp" style={{ fontSize: 34, lineHeight: 1, color: 'var(--d-ok)' }}>
            {числоРус(опубликовано)}
          </span>
          <span style={{ fontSize: 13, color: 'var(--d-mute)' }}>
            из {числоРус(всего)} дошли до канала
          </span>
          <span className="mono" style={{ fontSize: 12, color: 'var(--d-dim)', marginLeft: 'auto' }}>
            {конверсия < 1 ? конверсия.toFixed(2) : конверсия.toFixed(1)}%
          </span>
        </div>
      </div>

      <div className="flex flex-col" style={{ gap: 10 }}>
        {этапы.map((статус) => {
          const n = воронка[статус];
          const цвет = ЦВЕТ_ВОРОНКИ[статус] ?? 'var(--d-line-strong)';
          return (
            <div key={статус}>
              <div className="flex justify-between mb-1" style={{ gap: 10, fontSize: 12.5 }}>
                <span style={{ color: 'var(--d-mute)' }}>
                  {ПОДПИСИ_ВОРОНКИ[статус] ?? статус}
                  <span className="mono" style={{ color: 'var(--d-dim)', fontSize: 10.5, marginLeft: 6 }}>
                    {статус}
                  </span>
                </span>
                <span className="mono shrink-0" style={{ fontWeight: 600 }}>{числоРус(n)}</span>
              </div>
              <div style={{ height: 6, borderRadius: 3, background: 'var(--d-line)' }}>
                <div style={{
                  width: `${Math.max(1.5, (n / макс) * 100)}%`, height: 6,
                  borderRadius: 3, background: цвет,
                }} />
              </div>
            </div>
          );
        })}
      </div>
    </Блок>
  );
}

export default function AdminDashboardPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [data, setData] = useState<DashboardOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [обновляется, setОбновляется] = useState(false);
  const [вкладка, setВкладка] = useState<Вкладка>('Карта');
  // ⚠️ ЖИВОЕ СОСТОЯНИЕ ЖИВЁТ ОТДЕЛЬНО ОТ СНИМКА. Снимок тяжёлый и лежит в кэше
  // 30 секунд — на нём «прямо сейчас» не построить. Живое приезжает событиями
  // (SSE 'pipeline') и лёгкой ручкой /live, поэтому кнопка «Обновить» нужна
  // только для пересбора самого снимка, а не чтобы увидеть текущий процесс.
  const живое = useLivePipelines(!!user && user.role === 'admin');

  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') navigate('/', { replace: true });
  }, [authLoading, user, navigate]);

  // ⚠️ Спиннер только для ручного обновления: фоновый опрос раз в 30 с не должен
  // крутить иконку — экран, открытый на втором мониторе, дёргался бы всё время.
  const загрузить = useCallback(async (fresh = false, спиннер = false) => {
    if (спиннер) setОбновляется(true);
    try {
      setData(await getDashboardOverview(fresh));
      setError(null);
    } catch (e) {
      // Ошибку показываем, но данные НЕ стираем: сорванный опрос не повод
      // оставить экран пустым, когда на нём есть снимок минутной давности.
      setError(e instanceof Error ? e.message : 'Не удалось загрузить');
    } finally {
      if (спиннер) setОбновляется(false);
    }
  }, []);

  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    загрузить();
    const t = setInterval(() => загрузить(false), ИНТЕРВАЛ_МС);
    return () => clearInterval(t);
  }, [user, загрузить]);

  const свод = useMemo(() => {
    const п = data?.процессы ?? [];
    return {
      всего: п.length,
      работает: п.filter((x) => x.состояние === 'ok').length,
      отстаёт: п.filter((x) => x.состояние === 'degraded').length,
      молчит: п.filter((x) => x.состояние === 'молчит').length,
      сломан: п.filter((x) => !['ok', 'degraded', 'молчит', 'неизвестно'].includes(x.состояние)).length,
    };
  }, [data]);

  if (authLoading || (!data && !error)) {
    return (
      <div className="dash" style={{ padding: 24 }}>
        <div className="mono" style={{ fontSize: 12, color: 'var(--d-dim)' }}>собираю снимок…</div>
      </div>
    );
  }

  return (
    <div className="dash" style={{ padding: '20px 24px 40px' }}>
      <div style={{ maxWidth: 1320, margin: '0 auto' }}>

        {/* ── Шапка ── */}
        <div className="flex items-end justify-between flex-wrap"
          style={{ gap: 20, borderBottom: '2px solid var(--d-ink)', paddingBottom: 12, marginBottom: 16 }}>
          <div className="flex items-baseline flex-wrap" style={{ gap: 16 }}>
            <Link to="/admin/stats" className="dash-press flex items-center"
              style={{ padding: '4px 10px', gap: 5, fontSize: 11, textDecoration: 'none' }}>
              <ArrowLeft size={13} /> Статистика
            </Link>
            <div className="disp" style={{ fontSize: 26 }}>Карта проекта</div>
            <div className="flex flex-wrap" style={{ gap: 4 }}>
              {ВКЛАДКИ.map((в) => (
                <button key={в} className="dash-tab" data-active={вкладка === в}
                  onClick={() => setВкладка(в)}>{в}</button>
              ))}
            </div>
          </div>
          <div className="flex items-center flex-wrap" style={{ gap: 16 }}>
            <div className="flex items-center" style={{ gap: 7 }}>
              <span
                className={живое.подключено ? 'dash-dot-live' : undefined}
                style={{
                  width: 8, height: 8, borderRadius: '50%', display: 'inline-block',
                  background: !живое.подключено ? 'var(--d-bad)'
                    : живое.идут.size ? 'var(--d-accent)' : 'var(--d-ok)',
                }}
              />
              <span className="mono" style={{ fontSize: 11.5, color: 'var(--d-mute)' }}>
                {data && new Date(data.снято).toLocaleTimeString('ru-RU')}
                {data?.из_кэша ? ' · из кэша' : ' · пересобран'}
              </span>
            </div>
            <button onClick={() => загрузить(true, true)} disabled={обновляется}
              className="dash-press flex items-center" style={{ padding: '4px 12px', gap: 6, fontSize: 11 }}>
              {обновляется ? <Loader2 size={13} className="animate-spin" /> : <RefreshCw size={13} />}
              Обновить
            </button>
          </div>
        </div>

        {error && (
          <div className="mb-4" style={{
            padding: 12, borderRadius: 8, fontSize: 13,
            background: 'rgba(255,122,92,0.12)', color: 'var(--d-bad)',
          }}>
            {error}{data && ' — показан предыдущий снимок'}
          </div>
        )}

        {data && (
          <>
            {/* ── Сводка ── */}
            <div className="grid mb-4" style={{
              gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: 12,
            }}>
              <Плитка ярлык="Работает" число={свод.работает} подпись={`из ${свод.всего} процессов`}
                цвет="var(--d-ok)" />
              <Плитка ярлык="Отстаёт" число={свод.отстаёт} подпись="работает, но с оговоркой"
                цвет="var(--d-warn)" рамка="var(--d-warn)" />
              <Плитка ярлык="Не работает" число={свод.сломан + свод.молчит}
                подпись={свод.молчит ? `${свод.молчит} молчит` : 'ничего не упало'}
                цвет={свод.сломан + свод.молчит ? 'var(--d-bad)' : undefined}
                рамка={свод.сломан + свод.молчит ? 'var(--d-bad)' : 'rgba(245,241,232,0.24)'} />
              {/* ⚠️ РАНЬШЕ ТУТ БЫЛО «отметились за 5 мин» — суррогат, потому что
                  начала прогона в базе не было вовсе. Теперь оркестратор пишет
                  старт, и плитка говорит правду: сколько процессов работает в
                  эту секунду. */}
              <Плитка ярлык="Идут сейчас" число={живое.идут.size}
                подпись={живое.идут.size
                  ? [...живое.идут].slice(0, 2).join(', ') + (живое.идут.size > 2 ? '…' : '')
                  : 'очередь пуста'}
                цвет={живое.идут.size ? 'var(--d-accent)' : undefined}
                рамка={живое.идут.size ? 'var(--d-accent)' : 'rgba(245,241,232,0.24)'} />
            </div>

            {вкладка === 'Карта' && (
              <div className="dash-card" style={{ padding: '16px 18px' }}>
                <ProjectMap процессы={data.процессы} идут={живое.идут} вспышки={живое.вспышки} />
              </div>
            )}

            {вкладка === 'Процессы' && (
              <LiveProcesses
                процессы={живое.процессы}
                идут={живое.идут}
                вспышки={живое.вспышки}
                подключено={живое.подключено}
                тик={живое.тик}
              />
            )}

            {вкладка === 'Завод постов' && <ВоронкаПостов воронка={data.воронка_постов} />}

            {вкладка === 'Второй мозг' && (
              <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: 12 }}>
                <Блок заголовок="Справочник и карточки">
                  {/* ⚠️ ПОКРЫТИЕ КАРТОЧЕК — ВПЕРЁД СПИСКА. В плоском перечне «46» и «130»
                      стоят соседними строчками и выглядят одинаково безобидно, хотя
                      это и есть главная дыра: у двух третей бумаг фундамента нет.
                      Полоса показывает разрыв сразу, до чтения цифр. */}
                  {(() => {
                    const есть = data.второй_мозг.бумаг_с_карточкой ?? 0;
                    const всего = data.второй_мозг.бумаг ?? 0;
                    if (!всего) return null;
                    const доля = (есть / всего) * 100;
                    return (
                      <div className="dash-inner mb-3" style={{ padding: '12px 14px' }}>
                        <div className="flex items-baseline flex-wrap mb-2" style={{ gap: 10 }}>
                          <span className="disp" style={{
                            fontSize: 30, lineHeight: 1,
                            color: доля > 90 ? 'var(--d-ok)' : доля > 50 ? 'var(--d-warn)' : 'var(--d-bad)',
                          }}>{числоРус(есть)}</span>
                          <span style={{ fontSize: 13, color: 'var(--d-mute)' }}>
                            из {числоРус(всего)} бумаг с карточкой
                          </span>
                          <span className="mono" style={{ fontSize: 12, color: 'var(--d-dim)', marginLeft: 'auto' }}>
                            {доля.toFixed(0)}%
                          </span>
                        </div>
                        <div style={{ height: 6, borderRadius: 3, background: 'var(--d-line)' }}>
                          <div style={{
                            width: `${доля}%`, height: 6, borderRadius: 3,
                            background: доля > 90 ? 'var(--d-ok)' : доля > 50 ? 'var(--d-warn)' : 'var(--d-bad)',
                          }} />
                        </div>
                      </div>
                    );
                  })()}
                  <div className="flex flex-col" style={{ gap: 7 }}>
                    {Object.entries(data.второй_мозг).map(([k, v]) => (
                      <div key={k} className="flex justify-between" style={{ fontSize: 12.5 }}>
                        <span style={{ color: 'var(--d-mute)' }}>{ПОДПИСИ_МОЗГА[k] || k}</span>
                        <span className="mono" style={{ fontWeight: 600 }}>{числоРус(v)}</span>
                      </div>
                    ))}
                  </div>
                </Блок>
                <Блок заголовок="Возраст самих данных">
                  <p style={{ fontSize: 11.5, color: 'var(--d-dim)', margin: '0 0 10px', lineHeight: 1.5 }}>
                    Не авария: мы пишем исправно, а у источника данные могут быть старыми.
                    В общий вердикт намеренно не входит.
                  </p>
                  <div className="flex flex-col" style={{ gap: 7 }}>
                    {Object.entries(data.стареющие_данные).map(([k, v]) => (
                      <div key={k} className="flex justify-between" style={{ gap: 12, fontSize: 12.5 }}>
                        <span style={{ color: 'var(--d-mute)' }}>{ПОДПИСИ_СТАРЕНИЯ[k] || k}</span>
                        <span className="mono" style={{ fontWeight: 600 }}>{числоРус(v)}</span>
                      </div>
                    ))}
                  </div>
                </Блок>
              </div>
            )}

            {вкладка === 'База' && (
              <Блок заголовок="Самые тяжёлые таблицы"
                справа={<span className="mono" style={{ fontSize: 11, color: 'var(--d-dim)' }}>
                  вес с индексами · шкала логарифмическая
                </span>}>
                {(() => {
                  // ⚠️ ЛОГАРИФМ, И ЭТО ПОДПИСАНО. Разброс тысячекратный: candles 14 ГБ
                  // против 13 МБ у хвоста. На линейной шкале всё, кроме свечей,
                  // вырождается в ниточку одинаковой длины — сравнить нельзя ничего.
                  // Молча менять шкалу нельзя, поэтому в шапке блока сказано прямо.
                  const мин = Math.min(...data.хранилища.map((т) => т.байт), 1);
                  const макс = Math.max(...data.хранилища.map((т) => т.байт), 1);
                  const низ = Math.log10(Math.max(мин, 1));
                  const верх = Math.log10(Math.max(макс, 10));
                  const доля = (b: number) =>
                    верх === низ ? 1 : (Math.log10(Math.max(b, 1)) - низ) / (верх - низ);
                  return (
                    <div className="flex flex-col" style={{ gap: 9 }}>
                      {data.хранилища.map((т) => (
                        <div key={т.таблица}>
                          <div className="flex justify-between mb-1" style={{ gap: 12, fontSize: 12.5 }}>
                            <span className="mono truncate" style={{ color: 'var(--d-mute)' }}>{т.таблица}</span>
                            <span className="mono shrink-0" style={{ color: 'var(--d-dim)', fontSize: 11 }}>
                              {числоРус(т.строк)} стр.
                            </span>
                            <span className="mono shrink-0" style={{ fontWeight: 600, minWidth: 66, textAlign: 'right' }}>
                              {т.размер}
                            </span>
                          </div>
                          <div style={{ height: 5, borderRadius: 2.5, background: 'var(--d-line)' }}>
                            <div style={{
                              width: `${Math.max(4, доля(т.байт) * 100)}%`, height: 5,
                              borderRadius: 2.5, background: 'var(--d-cold)',
                            }} />
                          </div>
                        </div>
                      ))}
                    </div>
                  );
                })()}
              </Блок>
            )}
          </>
        )}
      </div>
    </div>
  );
}
