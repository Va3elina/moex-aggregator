/**
 * AdminDashboardPage — карта состояния проекта. Admin-only, /admin/dashboard.
 *
 * Источник: GET /api/admin/dashboard/overview (снимок собирается раз в 30 с и
 * лежит в Redis — экран его читает, а не пересчитывает).
 *
 * ⚠️ ТРИ СОСТОЯНИЯ, А НЕ ДВА. Работает / работает, но данные устарели / не
 * работает. Второе — самое частое и самое опасное: пайплайн зелёный, а цифры
 * недельной давности. Поэтому у каждого процесса рядом с состоянием стоит
 * «когда обновлялся», а стареющие данные вынесены в отдельный блок.
 *
 * ⚠️ СТАРЕЮЩИЕ ДАННЫЕ НЕ ВХОДЯТ В ОБЩИЙ ВЕРДИКТ. У части компаний структура
 * акционеров старше двух лет — это норма жизни рынка, а не авария. Тревога,
 * которая горит всегда, гасит внимание ко всем остальным.
 *
 * ⚠️ ОБНОВЛЕНИЕ БЕЗ МИГАНИЯ. Автообновление раз в 30 с (столько же живёт снимок
 * в кэше — чаще опрашивать нечего). Старые данные при этом НЕ сбрасываются в
 * скелетоны, иначе экран, открытый на втором мониторе, дёргается каждые полминуты.
 */
import { useCallback, useEffect, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import {
  Activity, AlertTriangle, ArrowLeft, CheckCircle2, Clock, Database,
  Factory, Loader2, Network, RefreshCw, VolumeX,
} from 'lucide-react';
import Card from '../components/Card';
import Skeleton from '../components/Skeleton';
import { useAuth } from '../contexts/AuthContext';
import { getDashboardOverview } from '../services/api';
import type { DashboardOverview, DashboardProcess } from '../services/api';

const ИНТЕРВАЛ_МС = 30_000;

/** Человеческие подписи для ключей «второго мозга» — бэкенд отдаёт машинные. */
const ПОДПИСИ_МОЗГА: Record<string, string> = {
  эмитентов: 'Эмитентов',
  бумаг: 'Бумаг',
  алиасов: 'Алиасов',
  метрик: 'Метрик',
  бумаг_с_карточкой: 'Бумаг с карточкой',
  документов: 'Документов',
  рёбер: 'Рёбер владения',
  казначейских: 'Казначейских пакетов',
  сигналов_в_очереди: 'Сигналов в очереди',
};

const ПОДПИСИ_СТАРЕНИЯ: Record<string, string> = {
  акционеры_старше_2лет: 'Структур акционеров старше 2 лет',
  акционеры_всего: 'Структур акционеров всего',
  рёбра_старше_2лет: 'Рёбер владения старше 2 лет',
};

function цветСостояния(состояние: string): { фон: string; текст: string } {
  if (состояние === 'ok') return { фон: 'var(--green-soft, rgba(34,160,90,0.14))', текст: 'var(--green, #22a05a)' };
  if (состояние === 'молчит') return { фон: 'rgba(150,150,150,0.16)', текст: 'var(--text-secondary)' };
  if (состояние === 'degraded') return { фон: 'rgba(255,92,43,0.14)', текст: 'var(--accent)' };
  if (состояние === 'неизвестно') return { фон: 'rgba(150,150,150,0.12)', текст: 'var(--text-secondary)' };
  return { фон: 'var(--red-soft, rgba(214,64,64,0.14))', текст: 'var(--red, #d64040)' };
}

function возраст(часов: number | null): string {
  if (часов === null) return 'никогда';
  if (часов < 1) return `${Math.round(часов * 60)} мин назад`;
  if (часов < 48) return `${часов.toFixed(1)} ч назад`;
  return `${Math.round(часов / 24)} дн назад`;
}

function числоРус(n: number): string {
  return n.toLocaleString('ru-RU');
}

/** Заголовок блока — одинаковый во всех секциях, чтобы экран читался сверху вниз. */
function Заголовок({ icon: Icon, children, справа }: {
  icon: React.ComponentType<{ size?: number; style?: React.CSSProperties }>;
  children: React.ReactNode;
  справа?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between mb-3" style={{ gap: 'var(--sp-2)' }}>
      <div className="flex items-center" style={{ gap: 'var(--sp-2)' }}>
        <Icon size={18} style={{ color: 'var(--text-secondary)' }} />
        <h2 className="font-semibold" style={{ fontSize: 'var(--fs-lg)' }}>{children}</h2>
      </div>
      {справа}
    </div>
  );
}

function СтрокаПроцесса({ п }: { п: DashboardProcess }) {
  const c = цветСостояния(п.состояние);
  return (
    <div
      className="flex items-center justify-between rounded-lg"
      style={{
        gap: 'var(--sp-2)',
        padding: 'var(--sp-2) var(--sp-3)',
        // ⚠️ НЕ --bg-secondary: им же покрашен сам Card, строки слились бы с фоном.
        background: 'var(--bg-primary)',
      }}
    >
      <div className="min-w-0 flex-1">
        <div className="font-medium truncate" style={{ fontSize: 'var(--fs-sm)' }}>{п.имя}</div>
        {п.заметка && (
          <div className="truncate" style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)' }}>
            {п.заметка}
          </div>
        )}
      </div>
      <div className="text-right shrink-0" style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)' }}>
        {возраст(п.часов_назад)}
        {п.длился_сек !== null && <> · {п.длился_сек}с</>}
      </div>
      <span
        className="rounded-full font-semibold shrink-0"
        style={{
          padding: '2px 10px',
          fontSize: 'var(--fs-2xs)',
          background: c.фон,
          color: c.текст,
        }}
      >
        {п.состояние}
      </span>
    </div>
  );
}

export default function AdminDashboardPage() {
  const { user, loading: authLoading } = useAuth();
  const navigate = useNavigate();
  const [data, setData] = useState<DashboardOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [обновляется, setОбновляется] = useState(false);

  // Guard: только admin
  useEffect(() => {
    if (authLoading) return;
    if (!user || user.role !== 'admin') navigate('/', { replace: true });
  }, [authLoading, user, navigate]);

  // ⚠️ СПИННЕР ТОЛЬКО ДЛЯ РУЧНОГО ОБНОВЛЕНИЯ. Фоновый опрос раз в 30 с не должен
  // крутить иконку на кнопке: экран, открытый на втором мониторе, дёргался бы
  // каждые полминуты без всякой причины. Заодно это убирает синхронный setState
  // в теле эффекта — на него справедливо ругается react-hooks.
  const загрузить = useCallback(async (fresh = false, показатьСпиннер = false) => {
    if (показатьСпиннер) setОбновляется(true);
    try {
      const d = await getDashboardOverview(fresh);
      setData(d);
      setError(null);
    } catch (e) {
      // ⚠️ Ошибку показываем, но данные НЕ стираем: сорванный опрос не повод
      // оставить экран пустым, когда на нём есть снимок минутной давности.
      setError(e instanceof Error ? e.message : 'Не удалось загрузить');
    } finally {
      if (показатьСпиннер) setОбновляется(false);
    }
  }, []);

  useEffect(() => {
    if (!user || user.role !== 'admin') return;
    загрузить();
    const t = setInterval(() => загрузить(false), ИНТЕРВАЛ_МС);
    return () => clearInterval(t);
  }, [user, загрузить]);

  if (authLoading || (!data && !error)) {
    return (
      <div className="max-w-6xl mx-auto" style={{ padding: 'var(--sp-4)' }}>
        <Skeleton className="h-8 w-64 mb-6" />
        <Skeleton className="h-32 w-full mb-4" />
        <Skeleton className="h-64 w-full" />
      </div>
    );
  }

  const сломано = data?.вердикт === 'сломано';

  return (
    <div className="max-w-6xl mx-auto" style={{ padding: 'var(--sp-4)' }}>
      <div className="flex items-center justify-between mb-6" style={{ gap: 'var(--sp-2)' }}>
        <div className="flex items-center min-w-0" style={{ gap: 'var(--sp-2)' }}>
          <Link to="/admin/stats" className="editorial-press rounded-full flex items-center shrink-0"
            style={{ padding: 'var(--sp-1) var(--sp-2)', gap: '4px', fontSize: 'var(--fs-xs)' }}>
            <ArrowLeft size={14} /> Статистика
          </Link>
          <h1 className="font-bold truncate" style={{ fontSize: 'var(--fs-2xl)' }}>Состояние проекта</h1>
        </div>
        <button
          onClick={() => загрузить(true, true)}
          disabled={обновляется}
          className="editorial-press rounded-full flex items-center shrink-0"
          style={{ padding: 'var(--sp-1) var(--sp-3)', gap: '6px', fontSize: 'var(--fs-xs)' }}
        >
          {обновляется ? <Loader2 size={14} className="animate-spin" /> : <RefreshCw size={14} />}
          Обновить
        </button>
      </div>

      {error && (
        <div className="rounded-lg mb-4" style={{
          padding: 'var(--sp-3)', fontSize: 'var(--fs-sm)',
          background: 'var(--red-soft, rgba(214,64,64,0.14))', color: 'var(--red, #d64040)',
        }}>
          {error}
          {data && ' — показан предыдущий снимок'}
        </div>
      )}

      {data && (
        <>
          {/* ── Вердикт ── */}
          <Card className="mb-4">
            <div className="flex items-center flex-wrap" style={{ gap: 'var(--sp-3)' }}>
              {сломано
                ? <AlertTriangle size={28} style={{ color: 'var(--red, #d64040)' }} />
                : <CheckCircle2 size={28} style={{ color: 'var(--green, #22a05a)' }} />}
              <div className="min-w-0">
                <div className="font-bold" style={{ fontSize: 'var(--fs-xl)' }}>
                  {сломано ? 'Есть проблемы' : 'Всё работает'}
                </div>
                <div style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)' }}>
                  снимок {new Date(data.снято).toLocaleTimeString('ru-RU')}
                  {data.из_кэша ? ' · из кэша' : ' · пересобран'}
                </div>
              </div>
            </div>

            {(data.упали.length > 0 || data.молчат.length > 0) && (
              <div className="flex flex-wrap mt-3" style={{ gap: 'var(--sp-2)' }}>
                {data.упали.map((имя) => (
                  <span key={имя} className="rounded-full font-semibold flex items-center"
                    style={{
                      padding: '2px 10px', gap: '4px', fontSize: 'var(--fs-2xs)',
                      background: 'var(--red-soft, rgba(214,64,64,0.14))', color: 'var(--red, #d64040)',
                    }}>
                    <AlertTriangle size={11} /> {имя}
                  </span>
                ))}
                {data.молчат.map((имя) => (
                  <span key={имя} className="rounded-full font-semibold flex items-center"
                    style={{
                      padding: '2px 10px', gap: '4px', fontSize: 'var(--fs-2xs)',
                      background: 'rgba(150,150,150,0.16)', color: 'var(--text-secondary)',
                    }}>
                    <VolumeX size={11} /> {имя}
                  </span>
                ))}
              </div>
            )}
          </Card>

          {/* ── Процессы ── */}
          <Card className="mb-4">
            <Заголовок
              icon={Activity}
              справа={
                <span style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)' }}>
                  {data.процессы.length} шт.
                </span>
              }
            >
              Процессы
            </Заголовок>
            <div className="flex flex-col" style={{ gap: '6px' }}>
              {data.процессы.map((п) => <СтрокаПроцесса key={п.имя} п={п} />)}
            </div>
          </Card>

          <div className="grid grid-cols-1 md:grid-cols-2" style={{ gap: 'var(--sp-3)' }}>
            {/* ── Завод постов ── */}
            <Card>
              <Заголовок icon={Factory}>Завод постов</Заголовок>
              {Object.keys(data.воронка_постов).length === 0 ? (
                <div style={{ fontSize: 'var(--fs-sm)', color: 'var(--text-secondary)' }}>Кандидатов нет</div>
              ) : (
                <div className="flex flex-col" style={{ gap: '6px' }}>
                  {Object.entries(data.воронка_постов)
                    .sort((a, b) => b[1] - a[1])
                    .map(([статус, n]) => (
                      <div key={статус} className="flex items-center justify-between"
                        style={{ fontSize: 'var(--fs-sm)' }}>
                        <span style={{ color: 'var(--text-secondary)' }}>{статус}</span>
                        <span className="font-semibold tabular-nums">{числоРус(n)}</span>
                      </div>
                    ))}
                </div>
              )}
            </Card>

            {/* ── Второй мозг ── */}
            <Card>
              <Заголовок icon={Network}>Второй мозг</Заголовок>
              <div className="flex flex-col" style={{ gap: '6px' }}>
                {Object.entries(data.второй_мозг).map(([k, v]) => (
                  <div key={k} className="flex items-center justify-between" style={{ fontSize: 'var(--fs-sm)' }}>
                    <span style={{ color: 'var(--text-secondary)' }}>{ПОДПИСИ_МОЗГА[k] || k}</span>
                    <span className="font-semibold tabular-nums">{числоРус(v)}</span>
                  </div>
                ))}
              </div>
            </Card>

            {/* ── Стареющие данные ── */}
            <Card>
              <Заголовок icon={Clock}>Возраст самих данных</Заголовок>
              <p className="mb-3" style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)' }}>
                Не авария: мы пишем исправно, а у источника данные могут быть старыми.
                В общий вердикт не входит.
              </p>
              <div className="flex flex-col" style={{ gap: '6px' }}>
                {Object.entries(data.стареющие_данные).map(([k, v]) => (
                  <div key={k} className="flex items-center justify-between" style={{ fontSize: 'var(--fs-sm)' }}>
                    <span style={{ color: 'var(--text-secondary)' }}>{ПОДПИСИ_СТАРЕНИЯ[k] || k}</span>
                    <span className="font-semibold tabular-nums">{числоРус(v)}</span>
                  </div>
                ))}
              </div>
            </Card>

            {/* ── Хранилища ── */}
            <Card>
              <Заголовок icon={Database}>Самые тяжёлые таблицы</Заголовок>
              <div className="flex flex-col" style={{ gap: '6px' }}>
                {data.хранилища.map((т) => (
                  <div key={т.таблица} className="flex items-center justify-between"
                    style={{ gap: 'var(--sp-2)', fontSize: 'var(--fs-sm)' }}>
                    <span className="truncate" style={{ color: 'var(--text-secondary)' }}>{т.таблица}</span>
                    <span className="shrink-0 tabular-nums" style={{ fontSize: 'var(--fs-2xs)', color: 'var(--text-secondary)' }}>
                      {числоРус(т.строк)} стр.
                    </span>
                    <span className="font-semibold shrink-0 tabular-nums">{т.размер}</span>
                  </div>
                ))}
              </div>
            </Card>
          </div>
        </>
      )}
    </div>
  );
}
