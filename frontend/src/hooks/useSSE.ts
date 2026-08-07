/**
 * Хук для подключения к SSE потоку обновлений данных.
 *
 * Автоматически подключается к /api/events/stream и получает уведомления
 * когда оркестратор обновляет данные в БД.
 *
 * ОДИН EventSource на вкладку (модульный синглтон с подпиской): раньше каждый
 * инстанс хука открывал своё соединение, и десяток панелей песочницы съедал бы
 * лимит браузера (~6 SSE на домен по HTTP/1.1) — терминал с реалтаймом за этим
 * лимитом просто ослеп бы. Хук-обёртка сохраняет старый API {lastEvent,
 * connected}, все потребители не заметили перемены. Соединение открывается с
 * первым подписчиком и закрывается с последним (страницы без SSE не держат
 * стрим зря).
 *
 * Reconnect с exponential backoff: 1s → 2s → 4s → ... → max 30s
 */

import { useEffect, useState } from 'react';

export interface SSEEvent {
  source: string;
  tables?: string[];
  ts: string;
  type?: string;
}

interface SSEState { lastEvent: SSEEvent | null; connected: boolean }

type Listener = (s: SSEState) => void;

const listeners = new Set<Listener>();
let state: SSEState = { lastEvent: null, connected: false };
let es: EventSource | null = null;
let retryDelay = 1000;
let retryTimer: ReturnType<typeof setTimeout> | undefined;

function emit(patch: Partial<SSEState>) {
  state = { ...state, ...patch };
  listeners.forEach((l) => l(state));
}

function connect() {
  es?.close();
  es = new EventSource('/api/events/stream');

  es.onopen = () => {
    emit({ connected: true });
    retryDelay = 1000; // Сброс backoff
  };

  es.onmessage = (event) => {
    try {
      const data: SSEEvent = JSON.parse(event.data);
      // Пропускаем служебное событие подключения
      if (data.type === 'connected') return;
      emit({ lastEvent: data });
    } catch {
      // Игнорируем невалидный JSON
    }
  };

  es.onerror = (e) => {
    // Browser сам логирует error в Network tab (ERR_CONNECTION_CLOSED /
    // 502 при API restart / HTTP2_PROTOCOL_ERROR). Здесь silent close +
    // reconnect — без console.error spam'a.
    void e;
    es?.close();
    es = null;
    emit({ connected: false });

    // Exponential backoff: 1s → 2s → 4s → 8s → ... → max 30s
    retryTimer = setTimeout(connect, retryDelay);
    retryDelay = Math.min(retryDelay * 2, 30000);
  };
}

function subscribe(l: Listener): () => void {
  listeners.add(l);
  if (listeners.size === 1 && !es) {
    retryDelay = 1000;
    connect();
  }
  return () => {
    listeners.delete(l);
    if (listeners.size === 0) {
      clearTimeout(retryTimer);
      es?.close();
      es = null;
    }
  };
}

export function useSSE() {
  const [snap, setSnap] = useState<SSEState>(state);

  useEffect(() => subscribe(setSnap), []);

  return snap;
}
