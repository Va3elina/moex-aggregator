#!/usr/bin/env python3
"""
Канарейка миграции T-Bank и VK ID на корень Минцифры (Russian Trusted Root CA).

Отвечает на три вопроса:
  1. Переключил ли вендор БОЕВОЙ домен на Минцифры-цепочку?
  2. Готовы ли мы — то есть проходит ли проверка с нашим бандлом?
  3. Жив ли rosstat.gov.ru (фетчер ВВП). Он на Минцифры давно, но там
     запинен ещё и промежуточный сертификат — Росстат его не присылает.
     Ротация Sub CA сломает загрузку ВВП, а пайплайн от этого не покраснеет,
     так что проверять его надо именно тут.

Смысл в связке «боевой + канареечный» домен: тест-контуры вендоров
(rest-api-test.tinkoff.ru, partners.api.vk.ru) переключились раньше боевых,
поэтому по ним видно, что переключение реально идёт.

Запуск (из корня репо):
    python scripts/check_ru_tls.py                # статус миграции
    python scripts/check_ru_tls.py --fingerprints # сверить корень по каналам

Проверить, что бандл реально доехал в боевой образ (папка scripts/ в runtime-
образ не копируется — там whitelist-COPY, поэтому одной строкой):
    docker exec $(docker ps -q -f label=com.docker.compose.service=api) \
        python3 -c "from api.ru_tls import RU_TLS_VERIFY; print(RU_TLS_VERIFY)"
    # ждём путь к бандлу; True = бандла нет, сборка сломана

Exit code: 0 — всё валидируется нашим бандлом; 1 — есть хост, который мы
проверить не можем (то есть что-то сломается или уже сломалось).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import ssl
import subprocess
import sys
from pathlib import Path

# Скрипт должен работать и из корня репо, и из scripts/, и в контейнере.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# (хост, что это, боевой ли, каким бандлом проверяем)
#   vendor  — ru-trusted-bundle.pem (certifi + корень), миграция ещё идёт
#   rosstat — rosstat-bundle.pem (+ Sub CA), уже на Минцифры и навсегда
HOSTS: list[tuple[str, str, bool, str]] = [
    ("securepay.tinkoff.ru", "T-Bank эквайринг", True, "vendor"),
    ("rest-api-test.tinkoff.ru", "T-Bank тест-контур", False, "vendor"),
    ("id.vk.com", "VK ID (OAuth)", True, "vendor"),
    ("partners.api.vk.ru", "VK Partners API", False, "vendor"),
    ("oauth.yandex.ru", "Яндекс OAuth", True, "vendor"),
    ("login.yandex.ru", "Яндекс профиль", True, "vendor"),
    ("rosstat.gov.ru", "Росстат (ВВП)", True, "rosstat"),
]

MINCIFRY = "Russian Trusted"

# Ожидаемый корень — зафиксирован при внедрении 30.07.2026, см.
# certs/russian_trusted_root_ca.pem. Расхождение = повод разбираться вручную,
# а не молча доверять новому CA.
EXPECTED_ROOT_SHA256 = (
    "d26d2d0231b7c39f92cc738512ba54103519e4405d68b5bd703e9788ca8ecf31"
)
GOSUSLUGI_ROOT_URL = (
    "https://gu-st.ru/content/lending/russian_trusted_root_ca_pem.crt"
)


def _s_client(host: str, extra: list[str] | None = None) -> str:
    """Сырой вывод openssl s_client. Пустая строка — если хост недоступен."""
    try:
        return subprocess.run(
            [
                "openssl", "s_client",
                "-connect", f"{host}:443",
                "-servername", host,
                *(extra or []),
            ],
            capture_output=True, text=True, timeout=25,
            input="", check=False,
        ).stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""


def chain_of(host: str) -> list[str]:
    """CN/O каждого звена цепочки, от листа к корню."""
    out = _s_client(host)
    return [m.strip() for m in re.findall(r"^\s*\d+\s+s:(.+)$", out, re.M)]


def pems_of(host: str) -> list[str]:
    """PEM-блоки, которые хост прислал в хендшейке."""
    out = _s_client(host, ["-showcerts"])
    return re.findall(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", out, re.S
    )


def sha256_of_pem(pem: str) -> str:
    return hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()


def _bundle_config() -> tuple[Path, str | bool]:
    """Путь к бандлу и значение для httpx verify=.

    Предпочитаем настоящий api.ru_tls — тогда логика гарантированно одна.
    Если он недоступен (скрипт скопировали куда-то отдельно), воспроизводим
    тот же дефолт вручную.
    """
    try:
        from api.ru_tls import RU_CA_BUNDLE_PATH, RU_TLS_VERIFY
        return RU_CA_BUNDLE_PATH, RU_TLS_VERIFY
    except ImportError:
        p = Path(os.getenv("FRAME_RU_CA_BUNDLE", "/etc/ssl/frame/ru-trusted-bundle.pem"))
        return p, (str(p) if p.is_file() else True)


def _rosstat_verify():
    """verify= для Росстата (httpx принимает и готовый SSLContext).

    Берём контекст из api.ru_tls, а не путь к бандлу: вне контейнера бандла
    нет, и контекст сам соберёт то же доверие из certs/ репозитория. Иначе
    канарейка ругалась бы на Росстат на каждой локальной машине.
    """
    try:
        from api.ru_tls import rosstat_ssl_context

        return rosstat_ssl_context()
    except (ImportError, FileNotFoundError):
        return True  # проверить нечем — пусть честно покажет FAIL


def cmd_status() -> int:
    try:
        import httpx
    except ImportError:
        print("! httpx не установлен — ставь зависимости или запускай в контейнере.")
        return 1

    RU_CA_BUNDLE_PATH, RU_TLS_VERIFY = _bundle_config()
    rosstat_verify = _rosstat_verify()

    bundle_on = RU_TLS_VERIFY is not True
    print(f"Бандл вендоров: {RU_CA_BUNDLE_PATH if bundle_on else 'НЕ НАЙДЕН → обычный certifi'}")
    print(
        "Бандл Росстата: "
        + ("собран (корень + Sub CA)" if rosstat_verify is not True
           else "НЕ СОБРАН → обычный certifi")
        + "\n"
    )

    print(f"{'хост':<26} {'корень цепочки':<26} {'certifi':<9} {'наш бандл'}")
    print("─" * 74)

    broken: list[str] = []
    flipped_prod: list[str] = []

    for host, label, is_prod, kind in HOSTS:
        chain = chain_of(host)
        if not chain:
            print(f"{host:<26} {'— недоступен —':<26} {'?':<9} ?")
            continue

        root = chain[-1]
        is_mincifry = MINCIFRY in root
        if len(chain) == 1:
            # Хост прислал только лист (Росстат) — «корня цепочки» тут нет,
            # иначе в колонку попал бы сам лист и выглядел бы как self-signed.
            root_short = "— только лист —"
        else:
            root_short = MINCIFRY if is_mincifry else re.sub(
                r".*?(?:O|CN)\s*=\s*([^,]+).*", r"\1", root
            )[:25]

        def probe(verify) -> str:
            try:
                httpx.get(f"https://{host}/", timeout=15, verify=verify)
                return "OK"
            except Exception as ex:
                return "FAIL" if "CERTIFICATE_VERIFY" in str(ex) else "err"

        plain = probe(True)
        if kind == "rosstat":
            ours = probe(rosstat_verify)
        else:
            ours = probe(RU_TLS_VERIFY) if bundle_on else plain

        if ours != "OK":
            broken.append(f"{host} ({label})")
        # Росстат на Минцифры был всегда — в сводку о миграции вендоров не идёт.
        if kind == "vendor" and is_prod and is_mincifry:
            flipped_prod.append(f"{host} ({label})")

        mark = " ←" if is_mincifry else ""
        print(f"{host:<26} {root_short:<26} {plain:<9} {ours}{mark}")

    print()
    if flipped_prod:
        print("ПЕРЕКЛЮЧЕНО НА МИНЦИФРЫ (боевые домены):")
        for h in flipped_prod:
            print(f"   • {h}")
    else:
        print("Боевые домены пока на международных CA — переключения не было.")

    if broken:
        print("\nНЕ ВАЛИДИРУЕТСЯ НАШИМ БАНДЛОМ:")
        for h in broken:
            print(f"   • {h}")
        print("\n→ Это сломает платежи, вход или обновление ВВП. Разбираться сейчас.")
        return 1

    print("Все хосты валидируются нашей конфигурацией.")
    return 0


def cmd_fingerprints() -> int:
    """Сверка корня по независимым каналам — как при внедрении."""
    print(f"Ожидаемый корень (зафиксирован в репо):\n   {EXPECTED_ROOT_SHA256}\n")

    seen: dict[str, str] = {}
    for host in ("partners.api.vk.ru", "rest-api-test.tinkoff.ru"):
        pems = pems_of(host)
        if not pems:
            print(f"{host:<28} — недоступен")
            continue
        fp = sha256_of_pem(pems[-1])
        seen[f"хендшейк {host}"] = fp

    try:
        import httpx
        r = httpx.get(GOSUSLUGI_ROOT_URL, timeout=25)
        r.raise_for_status()
        seen["загрузка с gu-st.ru"] = sha256_of_pem(r.text)
    except Exception as e:
        print(f"gu-st.ru — не скачалось: {type(e).__name__}")

    print(f"{'канал':<40} совпадает")
    print("─" * 52)
    ok = bool(seen)
    for channel, fp in seen.items():
        match = fp == EXPECTED_ROOT_SHA256
        ok &= match
        print(f"{channel:<40} {'да' if match else 'НЕТ  ' + fp[:16]}")

    print()
    if ok:
        print("Корень в репо совпадает со всеми доступными каналами.")
        return 0
    print("РАСХОЖДЕНИЕ. Не обновлять сертификат вслепую — разбираться вручную.")
    return 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--fingerprints", action="store_true",
        help="сверить корень из репо с хендшейками вендоров и Госуслугами",
    )
    args = ap.parse_args()
    sys.exit(cmd_fingerprints() if args.fingerprints else cmd_status())
