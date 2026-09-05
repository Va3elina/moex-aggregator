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
import { useNavigate, Link, useParams, useSearchParams } from 'react-router-dom';
import { УЗЛЫ } from './dashboard/topology';
import { ArrowLeft, Loader2, RefreshCw } from 'lucide-react';
import { useAuth } from '../contexts/AuthContext';
import { getDashboardOverview } from '../services/api';
import type { DashboardOverview } from '../services/api';
import ProjectMap from './dashboard/ProjectMap';
import LiveProcesses from './dashboard/LiveProcesses';
import PostFactory from './dashboard/PostFactory';
import OwnershipGraph from './dashboard/OwnershipGraph';
import DbDives from './dashboard/DbDives';
import IndicatorDive from './dashboard/IndicatorDive';
import BrainMap from './dashboard/BrainMap';
import { useLivePipelines } from './dashboard/useLivePipelines';
import './dashboard/dashboard.css';

const ИНТЕРВАЛ_МС = 30_000;

// ⚠️ ВКЛАДКА ПРОЦЕССОВ ОДНА. Их было две — «Живые процессы» и «Процессы», — и они
// показывали одно и то же: вторая просто не знала, что идёт сейчас. Разделение
// заставляло помнить, в какой из них смотреть, и ничего за это не давало.
const ВКЛАДКИ = ['Карта', 'Процессы', 'Индикаторы', 'Завод постов', 'Связи', 'Второй мозг', 'База'] as const;
type Вкладка = typeof ВКЛАДКИ[number];
// Вкладка живёт в адресе: /admin/dashboard/<слаг>[/<объект>]. Слаги латиницей —
// они попадают в строку браузера и в ссылки из чата.
const СЛАГ: Record<Вкладка, string> = {
  'Карта': 'map', 'Процессы': 'processes', 'Индикаторы': 'indicators', 'Завод постов': 'posts', 'Связи': 'graph',
  'Второй мозг': 'brain', 'База': 'db',
};
const ПО_СЛАГУ: Record<string, Вкладка> = Object.fromEntries(
  (Object.entries(СЛАГ) as Array<[Вкладка, string]>).map(([в, с]) => [с, в]),
);

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

export default function AdminDashboardPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [data, setData] = useState<DashboardOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [обновляется, setОбновляется] = useState(false);
  // ⚠️ ВКЛАДКА И ОБЪЕКТ — ИЗ АДРЕСА, а не из useState. Иначе на экран нельзя дать
  // ссылку, а «назад» в браузере уводит с панели целиком. /map/tg — это узел
  // Telegram, /posts/1727 — кандидат, ?p=funds_daily — подсветить процесс.
  const { tab, id } = useParams();
  const [searchParams] = useSearchParams();
  const вкладка: Вкладка = ПО_СЛАГУ[tab ?? ''] ?? 'Карта';
  const перейти = (в: Вкладка) => navigate(`/admin/dashboard/${СЛАГ[в]}`);
  useEffect(() => {
    if (!tab) navigate('/admin/dashboard/map', { replace: true });
  }, [tab, navigate]);
  const узелВАдресе = вкладка === 'Карта' && id ? УЗЛЫ.find((у) => у.id === id) ?? null : null;
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
    // ⚠️ «ОТСТАЁТ» — ПРО ВОЗРАСТ ДАННЫХ, А НЕ ПРО СТАТУС. Статус degraded у процессов
    // не бывает почти никогда, и плитка вечно показывала ноль — при том, что ряд
    // ОФЗ у ЦБ стоял с июня, а фетчер писал «ok». Теперь считаются ряды, у которых
    // последняя точка старше своего порога (см. _источники в dashboard.py).
    const отставшие = Object.entries(data?.источники ?? {})
      .flatMap(([узел, и]) => (и.факты ?? []).filter((ф) => ф.тревога).map((ф) => ({ узел, ...ф })));
    return {
      всего: п.length,
      работает: п.filter((x) => x.состояние === 'ok').length,
      отстаёт: отставшие.length,
      отставшие,
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
                  onClick={() => перейти(в)}>{в}</button>
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

        {(узелВАдресе || ((вкладка === 'Завод постов' || вкладка === 'Связи' || вкладка === 'База' || вкладка === 'Индикаторы') && id) || searchParams.get('p') || searchParams.get('n')) && (
          <div className="mono mb-3 flex items-center flex-wrap" style={{ gap: 6, fontSize: 11.5, color: 'var(--d-dim)' }}>
            <Link to="/admin/dashboard/map" style={{ color: 'var(--d-dim)', textDecoration: 'none' }}>Панель</Link>
            <span>›</span>
            <Link to={`/admin/dashboard/${СЛАГ[вкладка]}`} style={{ color: 'var(--d-dim)', textDecoration: 'none' }}>{вкладка}</Link>
            <span>›</span>
            <span style={{ color: 'var(--d-accent)' }}>
              {узелВАдресе ? узелВАдресе.имя
                : вкладка === 'Завод постов' && id ? `#${id}`
                : (вкладка === 'Связи' || вкладка === 'База' || вкладка === 'Индикаторы') && id ? id
                : searchParams.get('p') || searchParams.get('n')}
            </span>
          </div>
        )}

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
              <Плитка ярлык="Данные отстали" число={свод.отстаёт}
                подпись={свод.отстаёт
                  ? свод.отставшие.slice(0, 2).map((ф) => ф.ярлык).join(', ') + (свод.отстаёт > 2 ? '…' : '')
                  : 'все ряды свежие'}
                цвет={свод.отстаёт ? 'var(--d-warn)' : undefined}
                рамка={свод.отстаёт ? 'var(--d-warn)' : 'rgba(245,241,232,0.24)'} />
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
                <ProjectMap
                  процессы={data.процессы} идут={живое.идут} вспышки={живое.вспышки}
                  источники={data.источники} журнал={data.журнал_сутки}
                  выбран={id ?? null}
                  onВыбрать={(n) => navigate(n ? `/admin/dashboard/map/${n}` : '/admin/dashboard/map')}
                />
              </div>
            )}

            {вкладка === 'Процессы' && (
              <LiveProcesses
                процессы={живое.процессы}
                идут={живое.идут}
                вспышки={живое.вспышки}
                подключено={живое.подключено}
                тик={живое.тик}
                подсветить={searchParams.get('p')}
              />
            )}

            {вкладка === 'Индикаторы' && (
              <IndicatorDive
                выбран={id ?? null}
                onВыбрать={(и) => navigate(и ? `/admin/dashboard/indicators/${и}` : '/admin/dashboard/indicators')}
              />
            )}

            {вкладка === 'Завод постов' && <PostFactory />}

            {вкладка === 'Связи' && (
              <OwnershipGraph
                выбран={id ?? null}
                onВыбрать={(t) => navigate(t ? `/admin/dashboard/graph/${t}` : '/admin/dashboard/graph')}
              />
            )}

            {вкладка === 'Второй мозг' && <BrainMap покрытие={data.второй_мозг} />}

            {вкладка === 'База' && (
              <DbDives
                выбран={id ?? null}
                хранилища={data.хранилища}
                onВыбрать={(к) => navigate(к ? `/admin/dashboard/db/${к}` : '/admin/dashboard/db')}
              />
            )}
          </>
        )}
      </div>
    </div>
  );
}
