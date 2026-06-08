/**
 * Фиче-флаг Telegram-алертов.
 *
 * ВКЛЮЧЕНО (2026-06-08): IPv6 хоста восстановлен (статический адрес `2a03:6f00:a::2:a9f`
 * + дефолт-маршрут прописаны в netplan `52-static-ipv6.yaml`) → Telegram снова достижим,
 * бот poll'ит, пуши уходят. UI алертов виден: колокол на индикаторе + секция в ЛК.
 *
 * Если Telegram снова отвалится (Timeweb отзовёт IPv6 / РКН-эскалация) — поставь false,
 * чтобы спрятать UI; durable-вариант на будущее — релей (Cloudflare Worker / VPS abroad).
 * См. HANDOFF.md и память signal_engine.md (инцидент IPv6 + фикс).
 */
export const ALERTS_ENABLED = true;
