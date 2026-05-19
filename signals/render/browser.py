"""Headless-browser renderer для signal-постов.

Открывает /signal-export?ticker=...&clgroup=...&date=... через Playwright,
ждёт data-render-ready=true, делает screenshot. Pixel-perfect совпадение с UI.

Не используется напрямую — экспортирует render_chart(...) compatible с render/chart.py.
"""
from __future__ import annotations
import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from signals import config
from signals.db import get_asset_name


log = logging.getLogger("signals.render.browser")

# Базовый URL — на сервере это localhost (через nginx-proxy внутри docker host network).
# Можно override через env SIGNAL_EXPORT_BASE если запускать с других серверов.
DEFAULT_BASE_URL = os.getenv("SIGNAL_EXPORT_BASE", "https://xn--80aklbnczmv.xn--p1ai")

VIEWPORT_W = 1280
VIEWPORT_H = 720

# Timeouts (мс)
PAGE_LOAD_TIMEOUT = 20_000
READY_TIMEOUT = 15_000


def render_chart(
    sectype: str,
    signal_date: date,
    output_path: Path,
    clgroup: str = "FIZ",
) -> Path:
    """Открыть signal-export route → screenshot. Сохранить в output_path."""
    asset_name = get_asset_name(sectype) or sectype
    params = {
        "ticker": sectype,
        "clgroup": clgroup,
        "date": signal_date.isoformat(),
        "name": asset_name,
        "period": "6m",
        "theme": "editorial-light",
    }
    url = f"{DEFAULT_BASE_URL}/signal-export?{urlencode(params)}"
    log.info("rendering: %s", url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(
                viewport={"width": VIEWPORT_W, "height": VIEWPORT_H},
                device_scale_factor=2,  # retina, лучший visual
                ignore_https_errors=True,
            )
            page = ctx.new_page()

            try:
                page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="networkidle")
            except PlaywrightTimeout:
                # Networkidle может не наступить если есть фоновые long-poll —
                # допускаем и продолжаем (страница уже загружена).
                log.warning("networkidle timeout, continuing anyway")

            # Ждём data-render-ready="true" — SignalExportPage ставит после рендера chart.
            try:
                page.wait_for_selector('[data-render-ready="true"]', timeout=READY_TIMEOUT)
            except PlaywrightTimeout as e:
                log.error("render-ready selector timeout — chart could not load")
                raise RuntimeError(f"render-ready timeout for {sectype}/{clgroup}") from e

            # Screenshot самого root-frame'а (clip к viewport — full_page False).
            page.screenshot(path=str(output_path), full_page=False, omit_background=False)
        finally:
            browser.close()

    log.info("rendered ok: %s", output_path)
    return output_path
