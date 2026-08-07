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
const eventListeners = new Set<(e: SSEEvent) => void>();
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
      // Прямая доставка КАЖДОГО события. Через lastEvent-стейт пачка фреймов
      // подряд (три NOTIFY за миллисекунды) схлопывалась React-батчем в одно
      // последнее событие — панели, ждавшие промежуточные источники, теряли
      // рефетч (замер на проде 2026-08-07). Подписчикам событий батчинг не
      // страшен: колбэк зовётся на каждый фрейм.
      eventListeners.forEach((l) => l(data));
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

function ensureConnection() {
  if (!es && listeners.size + eventListeners.size > 0) {
    retryDelay = 1000;
    connect();
  }
}
function maybeClose() {
  if (listeners.size + eventListeners.size === 0) {
    clearTimeout(retryTimer);
    es?.close();
    es = null;
  }
}

function subscribe(l: Listener): () => void {
  listeners.add(l);
  ensureConnection();
  return () => { listeners.delete(l); maybeClose(); };
}

/** Подписка на КАЖДОЕ SSE-событие (не снапшот lastEvent): для рефетч-логики,
 *  которой нельзя терять события при React-батчинге пачки фреймов. */
export function subscribeSSEEvents(l: (e: SSEEvent) => void): () => void {
  eventListeners.add(l);
  ensureConnection();
  return () => { eventListeners.delete(l); maybeClose(); };
}

export function useSSE() {
  const [snap, setSnap] = useState<SSEState>(state);

  useEffect(() => subscribe(setSnap), []);

  return snap;
}
