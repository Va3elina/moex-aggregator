/**
 * Хук для подключения к SSE потоку обновлений данных.
 *
 * Автоматически подключается к /api/events/stream и получает уведомления
 * когда оркестратор обновляет данные в БД.
 *
 * Reconnect с exponential backoff: 1s → 2s → 4s → ... → max 30s
 */

import { useEffect, useRef, useState, useCallback } from 'react';

export interface SSEEvent {
  source: string;
  tables?: string[];
  ts: string;
  type?: string;
}

export function useSSE() {
  const [lastEvent, setLastEvent] = useState<SSEEvent | null>(null);
  const [connected, setConnected] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);
  const retryDelay = useRef(1000);
  const retryTimer = useRef<ReturnType<typeof setTimeout>>(undefined);

  const connect = useCallback(() => {
    // Закрываем предыдущее соединение
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    const es = new EventSource('/api/events/stream');

    es.onopen = () => {
      setConnected(true);
      retryDelay.current = 1000; // Сброс backoff
    };

    es.onmessage = (event) => {
      try {
        const data: SSEEvent = JSON.parse(event.data);
        // Пропускаем служебное событие подключения
        if (data.type === 'connected') return;
        setLastEvent(data);
      } catch {
        // Игнорируем невалидный JSON
      }
    };

    es.onerror = (e) => {
      // Browser сам логирует error в Network tab (ERR_CONNECTION_CLOSED /
      // 502 при API restart / HTTP2_PROTOCOL_ERROR). Здесь silent close +
      // reconnect — без console.error spam'a.
      void e;
      es.close();
      setConnected(false);

      // Exponential backoff: 1s → 2s → 4s → 8s → ... → max 30s
      retryTimer.current = setTimeout(connect, retryDelay.current);
      retryDelay.current = Math.min(retryDelay.current * 2, 30000);
    };

    eventSourceRef.current = es;
  }, []);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(retryTimer.current);
      eventSourceRef.current?.close();
    };
  }, [connect]);

  return { lastEvent, connected };
}
