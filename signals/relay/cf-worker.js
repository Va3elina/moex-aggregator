/**
 * Cloudflare Worker — прозрачный релей к api.telegram.org.
 *
 * Зачем: хост Фрейма в РФ (Timeweb), РКН блокирует IPv4 Telegram. Вместо того чтобы
 * зависеть от хрупкого IPv6 хоста, хост ходит СЮДА по обычному незаблокированному HTTPS,
 * а Worker (на сети Cloudflare, вне РФ) форвардит запрос в Telegram. Обход без VPN:
 * хост вообще не коннектится к Telegram напрямую — только к релею (паттерн «как почта»,
 * store-and-forward через посредника). Работает и для бота (getUpdates long-poll),
 * и для пушей (sendMessage), и для канала signal-engine (sendPhoto).
 *
 * Защита от открытого релея: первый сегмент пути = секрет (env RELAY_SECRET).
 *   URL:    https://<worker>.workers.dev/<RELAY_SECRET>/bot<TOKEN>/<method>?<query>
 *   на проде: TELEGRAM_API_ROOT = https://<worker>.workers.dev/<RELAY_SECRET>
 *   (код Фрейма строит f"{TELEGRAM_API_ROOT}/bot{TOKEN}/<method>")
 *
 * Деплой: см. README.md рядом.
 */

const TELEGRAM = "https://api.telegram.org";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    // pathname = "/<secret>/bot<token>/<method>/..."
    const parts = url.pathname.split("/");
    const secret = parts[1] || "";

    // Без правильного секрета — 403 (не даём использовать релей как открытый прокси).
    if (!env.RELAY_SECRET || secret !== env.RELAY_SECRET) {
      return new Response("forbidden", { status: 403 });
    }

    // Остаток после секрета: /bot<token>/<method>
    const rest = "/" + parts.slice(2).join("/");
    const target = TELEGRAM + rest + url.search;

    // Чистые заголовки: пробрасываем только content-type (для POST / sendPhoto);
    // Host выставит сам fetch по target. CF-* и Host из входящего НЕ тащим.
    const headers = new Headers();
    const ct = request.headers.get("content-type");
    if (ct) headers.set("content-type", ct);

    const isBodyless = request.method === "GET" || request.method === "HEAD";
    const init = {
      method: request.method,
      headers,
      // arrayBuffer (а не stream) — надёжнее для POST/sendPhoto, без duplex-нюансов.
      body: isBodyless ? undefined : await request.arrayBuffer(),
    };

    const resp = await fetch(target, init);
    // Возвращаем ответ Telegram как есть (статус + тело).
    return new Response(resp.body, {
      status: resp.status,
      statusText: resp.statusText,
      headers: resp.headers,
    });
  },
};
