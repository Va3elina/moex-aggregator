/**
 * Живое состояние процессов: события, а не опрос по таймеру.
 *
 * Оркестратор отмечает старт и финиш каждого шага, отправляет их через
 * pg_notify('data_updated') с source='pipeline', и они доезжают до браузера
 * существующим SSE-потоком. Панель реагирует на событие, а не ждёт следующего
 * тика — процесс загорается в тот момент, когда он реально начался.
 *
 * ⚠️ СОБЫТИЕ ПРИМЕНЯЕТСЯ СРАЗУ, СВЕРКА — ПОТОМ. На событие «start» мы зажигаем
 * процесс не дожидаясь ответа сервера: иначе между событием и завершением
 * запроса проходит те самые полсекунды, ради которых всё и делалось. Следом
 * идёт отложенная сверка с /live — она и есть источник правды, а мгновенная
 * реакция лишь убирает задержку.
 *
 * ⚠️ СВЕРКА С ЗАДЕРЖКОЙ И ОДНА НА ПАЧКУ. Дневной блок запускает два десятка шагов
 * подряд, события идут очередью — запрос на каждое означал бы два десятка
 * запросов за секунду. Копим 700 мс и спрашиваем один раз.
 *
 * ⚠️ ЕСТЬ И ПЕРИОДИЧЕСКАЯ СВЕРКА, РАЗ В 20 СЕКУНД. События могут не дойти:
 * разрыв SSE, перезапуск контейнера, вкладка была в фоне. Без неё панель, на
 * которую не пришло «end», показывала бы вечно идущий процесс — та самая ложь,
 * которая выглядит убедительнее правды.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { getDashboardLive } from '../../services/api';
import type { DashboardLiveProcess } from '../../services/api';
import { subscribeSSEEvents } from '../../hooks/useSSE';

/** Сколько держать вспышку «только что отработал». */
const ВСПЫШКА_МС = 6000;
const СВЕРКА_ЗАДЕРЖКА_МС = 700;
const СВЕРКА_ПЕРИОД_МС = 20_000;

export interface ЖивоеСостояние {
  процессы: DashboardLiveProcess[];
  идут: Set<string>;
  /** Имена, только что закончившие — для короткой вспышки на карте. */
  вспышки: Map<string, 'ok' | 'fail'>;
  подключено: boolean;
  /** Растёт каждую секунду: чтобы «идёт N с» тикало без новых запросов. */
  тик: number;
}

export function useLivePipelines(включено: boolean): ЖивоеСостояние {
  const [процессы, setПроцессы] = useState<DashboardLiveProcess[]>([]);
  const [идут, setИдут] = useState<Set<string>>(new Set());
  const [вспышки, setВспышки] = useState<Map<string, 'ok' | 'fail'>>(new Map());
  const [подключено, setПодключено] = useState(false);
  const [тик, setТик] = useState(0);

  const таймерСверки = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const таймерыВспышек = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map());

  const сверить = useCallback(async () => {
    try {
      const d = await getDashboardLive();
      setПроцессы(d.процессы);
      setИдут(new Set(d.идут));
      setПодключено(true);
    } catch {
      // Сорванная сверка не повод гасить экран: предыдущее состояние честнее пустоты.
      setПодключено(false);
    }
  }, []);

  const сверитьПотом = useCallback(() => {
    clearTimeout(таймерСверки.current);
    таймерСверки.current = setTimeout(сверить, СВЕРКА_ЗАДЕРЖКА_МС);
  }, [сверить]);

  const зажечьВспышку = useCallback((имя: string, исход: 'ok' | 'fail') => {
    setВспышки((пред) => new Map(пред).set(имя, исход));
    clearTimeout(таймерыВспышек.current.get(имя));
    таймерыВспышек.current.set(имя, setTimeout(() => {
      setВспышки((пред) => {
        const м = new Map(пред);
        м.delete(имя);
        return м;
      });
      таймерыВспышек.current.delete(имя);
    }, ВСПЫШКА_МС));
  }, []);

  useEffect(() => {
    if (!включено) return;
    // Копия ссылки на карту таймеров: к моменту размонтирования ref мог смениться,
    // и чистка гасила бы уже не те таймеры.
    const таймеры = таймерыВспышек.current;
    сверить();

    const отписаться = subscribeSSEEvents((e) => {
      if (e.source !== 'pipeline') return;
      const имя = (e as unknown as { pipeline?: string }).pipeline;
      const фаза = (e as unknown as { phase?: string }).phase;
      const статус = (e as unknown as { status?: string }).status;
      if (!имя) return;

      if (фаза === 'start') {
        setИдут((пред) => new Set(пред).add(имя));
      } else if (фаза === 'end') {
        setИдут((пред) => {
          const s = new Set(пред);
          s.delete(имя);
          return s;
        });
        зажечьВспышку(имя, статус === 'ok' ? 'ok' : 'fail');
      }
      сверитьПотом();
    });

    const сверкаПоТаймеру = setInterval(сверить, СВЕРКА_ПЕРИОД_МС);
    const тикер = setInterval(() => setТик((t) => t + 1), 1000);

    return () => {
      отписаться();
      clearInterval(сверкаПоТаймеру);
      clearInterval(тикер);
      clearTimeout(таймерСверки.current);
      таймеры.forEach(clearTimeout);
      таймеры.clear();
    };
  }, [включено, сверить, сверитьПотом, зажечьВспышку]);

  return { процессы, идут, вспышки, подключено, тик };
}
