import { useEffect, useRef } from 'react';
import { useAnalytics } from '../contexts/AnalyticsContext';

/** Пауза после последнего изменения, по истечении которой снимок уходит. */
const SNAPSHOT_DEBOUNCE_MS = 1_500;

/**
 * Шлёт снимок состояния экрана — но только когда человек перестал его менять.
 *
 * Настройки перебирают пачками: щёлкают период, меняют режим, двигают панели.
 * Трекинг «на каждое изменение» дал бы десяток событий на одно осмысленное
 * действие и раздул бы и таблицу, и картину активности. Поэтому событие уходит
 * через паузу после последнего изменения и только если снимок реально
 * отличается от предыдущего отправленного.
 *
 * payload сравнивается по JSON, поэтому его можно собирать инлайном, не
 * заботясь о стабильности ссылки.
 */
export function useSnapshotTracking(
  eventType: 'funds_view' | 'fund_trades_view' | 'terminal_layout',
  payload: Record<string, unknown> | null,
) {
  const { track } = useAnalytics();
  const lastSentRef = useRef<string | null>(null);
  const json = payload ? JSON.stringify(payload) : null;

  useEffect(() => {
    if (!json) return;
    if (json === lastSentRef.current) return;
    const timer = window.setTimeout(() => {
      lastSentRef.current = json;
      track(eventType, JSON.parse(json));
    }, SNAPSHOT_DEBOUNCE_MS);
    return () => window.clearTimeout(timer);
  }, [eventType, json, track]);
}
