/**
 * Cloudflare Worker — прозрачный релей для обхода РКН/гео-блоков (без VPN, без IPv6 хоста).
 *
 * Обслуживает ТРИ апстрима, маршрут по первому сегменту пути после секрета:
 *   /<secret>/bot<TOKEN>/<method>  → api.telegram.org   (Telegram-бот/каналы/пуши)
 *   /<secret>/yahoo/<path>         → query1.finance.yahoo.com (сырьё для сезонности)
 *   /<secret>/anthropic/<path>     → api.anthropic.com  (Claude Routine /fire, content-пайплайн)
 *
 * Зачем: хост/контейнер Фрейма в РФ (Timeweb). РКН блокирует IPv4 Telegram, Yahoo Finance
 * недоступен из дата-центра вообще, а api.anthropic.com отдаёт 403 "Request not allowed"
 * на запросы с российских IP (подтверждено 2026-07-13 — идентичный запрос с не-РФ машины
 * проходит на 200). Вместо хрупкого IPv6 — ходим СЮДА по обычному незаблокированному HTTPS,
 * Worker (сеть Cloudflare, вне РФ) форвардит в апстрим. Store-and-forward через посредника,
 * как почта.
 *
 * Защита от открытого релея: первый сегмент пути = секрет (env RELAY_SECRET).
 *   Telegram:  TELEGRAM_API_ROOT      = https://<worker>.workers.dev/<RELAY_SECRET>
 *   Yahoo:     COMMODITY_API_ROOT     = https://<worker>.workers.dev/<RELAY_SECRET>/yahoo
 *   Anthropic: CLAUDE_ROUTINE_API_ROOT = https://<worker>.workers.dev/<RELAY_SECRET>/anthropic
 *
 * Деплой: см. README.md рядом.
 */

const TELEGRAM = "https://api.telegram.org";
const YAHOO = "https://query1.finance.yahoo.com";
const ANTHROPIC = "https://api.anthropic.com";
// Yahoo отдаёт 429 на запросы без браузерного User-Agent — выставляем его на стороне
// воркера, чтобы апстрим видел «браузер», а не голый fetch.
const YAHOO_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    // pathname = "/<secret>/<upstream-or-bot...>/..."
    const parts = url.pathname.split("/");
    const secret = parts[1] || "";

    // Без правильного секрета — 403 (не даём использовать релей как открытый прокси).
    if (!env.RELAY_SECRET || secret !== env.RELAY_SECRET) {
      return new Response("forbidden", { status: 403 });
    }

    const headers = new Headers();
    let target;

    if (parts[2] === "yahoo") {
      // /<secret>/yahoo/<path> → Yahoo. Остаток после "yahoo".
      const rest = "/" + parts.slice(3).join("/");
      target = YAHOO + rest + url.search;
      headers.set("user-agent", YAHOO_UA);
      headers.set("accept", "application/json");
    } else if (parts[2] === "anthropic") {
      // /<secret>/anthropic/<path> → api.anthropic.com. Нужны Bearer-токен и
      // версия/beta-заголовки — без них Claude Routine /fire не аутентифицируется.
      const rest = "/" + parts.slice(3).join("/");
      target = ANTHROPIC + rest + url.search;
      const auth = request.headers.get("authorization");
      if (auth) headers.set("authorization", auth);
      const av = request.headers.get("anthropic-version");
      if (av) headers.set("anthropic-version", av);
      const ab = request.headers.get("anthropic-beta");
      if (ab) headers.set("anthropic-beta", ab);
    } else {
      // /<secret>/bot<token>/<method> → Telegram (как было; обратная совместимость).
      const rest = "/" + parts.slice(2).join("/");
      target = TELEGRAM + rest + url.search;
    }

    // Чистые заголовки: для POST/sendPhoto/fire пробрасываем content-type; Host выставит
    // сам fetch по target. CF-* и Host из входящего НЕ тащим.
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
    // Возвращаем ответ апстрима как есть (статус + тело).
    return new Response(resp.body, {
      status: resp.status,
      statusText: resp.statusText,
      headers: resp.headers,
    });
  },
};
