"""
CSP violation report endpoint.

Браузеры с Content-Security-Policy-Report-Only header'ом отправляют POST
на report-uri каждый раз когда страница пытается загрузить ресурс,
запрещённый политикой. Это даёт нам live-мониторинг что блокируется,
без блокировки реальных юзеров.

Формат body (W3C CSP Level 2):
  {
    "csp-report": {
      "document-uri": "https://таймфрейм.рф/heatmap",
      "violated-directive": "script-src",
      "effective-directive": "script-src",
      "original-policy": "default-src 'self'; ...",
      "blocked-uri": "https://evil.com/skim.js",
      "line-number": 42,
      "column-number": 1,
      "source-file": "https://таймфрейм.рф/assets/main.js",
      "status-code": 0,
      "referrer": ""
    }
  }

Также появился Reporting API (новые браузеры) — Content-Type:
application/reports+json, body — массив reports разных типов. Мы
поддерживаем оба формата.

После 3-7 дней мониторинга в production'е (анализ логов) можно
переключить CSP в enforce-mode (убрать `-Report-Only` суффикс).
"""
from fastapi import APIRouter, Request, status
from fastapi.responses import Response

from api.logger import get_logger

router = APIRouter(prefix="/api/csp-report", tags=["security"])
logger = get_logger("csp_report")


@router.post("", status_code=status.HTTP_204_NO_CONTENT)
async def receive_csp_report(request: Request) -> Response:
    """
    Принимает CSP violation reports от браузеров.

    Безусловно возвращает 204 No Content — браузеры не интересуются
    статус-кодом (это fire-and-forget). Парсим body, логируем структурно.

    Без auth: браузеры отправляют без credentials. Без rate-limit здесь —
    глобальный RateLimitMiddleware уже даст 100 req/min protection,
    но обычно браузеры не спамят (дедупликация на клиенте).
    """
    try:
        # Браузеры могут слать Content-Type: application/csp-report или
        # application/json (старые) или application/reports+json (новые).
        # request.json() умеет парсить оба последних, для csp-report
        # форсим парсинг как json.
        body = await request.body()
        if not body:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        import json
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "CSP report: failed to parse body",
                extra={"extra_data": {"raw_body_size": len(body)}},
            )
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        # Формат 1: legacy CSP1/CSP2 → {"csp-report": {...}}
        if isinstance(data, dict) and "csp-report" in data:
            report = data["csp-report"]
            _log_violation(report, request)

        # Формат 2: Reporting API → [{"type": "csp-violation", "body": {...}}, ...]
        elif isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and entry.get("type") in ("csp-violation", "csp"):
                    _log_violation(entry.get("body", {}), request)

        else:
            logger.info(
                "CSP report: unknown format",
                extra={"extra_data": {"keys": list(data.keys()) if isinstance(data, dict) else "non-dict"}},
            )

    except Exception as e:
        # Никогда не падаем — CSP-report endpoint не должен влиять на сайт.
        logger.error(f"CSP report handler error: {e}", exc_info=True)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _log_violation(report: dict, request: Request) -> None:
    """Логируем структурно: directive, blocked-uri, document-uri."""
    # Совместимые с обоими форматами поля (legacy uses kebab-case, Reporting
    # API uses camelCase). Поэтому пробуем оба варианта ключей.
    def _g(*keys):
        for k in keys:
            v = report.get(k)
            if v:
                return v
        return None

    directive = _g("violated-directive", "violatedDirective", "effective-directive", "effectiveDirective") or "?"
    blocked = _g("blocked-uri", "blockedURL") or "?"
    document = _g("document-uri", "documentURL") or "?"
    source = _g("source-file", "sourceFile")
    line = _g("line-number", "lineNumber")

    client_ip = request.headers.get("x-real-ip") or request.client.host if request.client else "?"
    user_agent = request.headers.get("user-agent", "")[:200]

    logger.warning(
        f"CSP violation: {directive} blocked {blocked}",
        extra={
            "extra_data": {
                "type": "csp_violation",
                "violated_directive": directive,
                "blocked_uri": blocked,
                "document_uri": document,
                "source_file": source,
                "line_number": line,
                "client_ip": client_ip,
                "user_agent": user_agent,
            }
        },
    )
