#!/usr/bin/env python3
"""
sbp_probe.py — диагностика: включена ли на терминале T-Bank привязка счёта по СБП.

ЗАЧЕМ
    Рекуррентный СБП у нас реализован (api/billing/tbank.py: create_sbp_checkout /
    get_account_qr_state / charge_qr), но кнопка убрана 2026-07-02: GetQr(IMAGE)
    стабильно отдавал «Внутренняя ошибка системы». Типовая причина — функционал
    привязки счёта по СБП просто не включён на терминале. Этот скрипт фиксирует
    точный ErrorCode/Message, чтобы было что предъявить менеджеру Т-Бизнеса.

ЧТО ДЕЛАЕТ
    Вариант №2 (то, что у нас в коде): Init(Recurrent=Y, DATA={"QR":"true"}) → GetQr
    Вариант №1 (то, чего в коде нет):  AddAccountQr

ДЕНЬГИ НЕ ДВИГАЕТ. Init создаёт неоплаченный платёж, который сам протухает.
AddAccountQr создаёт заявку на привязку, которую никто не подтверждает.

ЗАПУСК (из корня репо, креды — из .claude/skills/moex-billing/references/secrets.local.md):
    export TBANK_TERMINAL_KEY=... TBANK_PASSWORD=...
    python3 scripts/sbp_probe.py
"""
import json
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

from api.billing.tbank import TBANK_API_BASE, TBankProvider
from api.ru_tls import RU_TLS_VERIFY

AMOUNT_KOP = 100  # 1 ₽ — минимум, платить всё равно никто не будет


def show(label: str, data: dict) -> bool:
    ok = bool(data.get("Success"))
    mark = "OK  " if ok else "FAIL"
    print(f"  [{mark}] {label}")
    if not ok:
        print(f"         ErrorCode: {data.get('ErrorCode')}")
        print(f"         Message:   {data.get('Message')}")
        print(f"         Details:   {data.get('Details')}")
    return ok


def main() -> int:
    key = os.getenv("TBANK_TERMINAL_KEY", "").strip()
    password = os.getenv("TBANK_PASSWORD", "").strip()
    if not key or not password:
        print("Нет TBANK_TERMINAL_KEY / TBANK_PASSWORD в env — см. docstring.")
        return 2

    p = TBankProvider(key, password)
    print(f"Терминал: {key[:6]}… → {TBANK_API_BASE}\n")
    client = httpx.Client(timeout=20, verify=RU_TLS_VERIFY)

    # ── Вариант №2: Init(Recurrent, QR) + GetQr ──────────────────────
    print("Вариант №2 — Init(Recurrent=Y, QR) + GetQr (текущий код):")
    body = {
        "TerminalKey": key,
        "Amount": AMOUNT_KOP,
        "OrderId": uuid.uuid4().hex,
        "Description": "Проба СБП-привязки (НЕ ОПЛАЧИВАТЬ)",
        "Recurrent": "Y",
        "CustomerKey": "sbp-probe",
    }
    body["Token"] = p._make_token(body)
    body["DATA"] = {"QR": "true"}
    init = client.post(f"{TBANK_API_BASE}/Init", json=body).json()
    payment_id = None
    if show("Init", init):
        payment_id = str(init["PaymentId"])
        print(f"         PaymentId: {payment_id}")
        for data_type in ("PAYLOAD", "IMAGE"):
            qr_body = {"TerminalKey": key, "PaymentId": payment_id, "DataType": data_type}
            qr_body["Token"] = p._make_token(qr_body)
            qr = client.post(f"{TBANK_API_BASE}/GetQr", json=qr_body).json()
            if show(f"GetQr({data_type})", qr):
                print(f"         RequestKey: {qr.get('RequestKey')}")
                print(f"         Data[:80]:  {str(qr.get('Data'))[:80]}")

    # ── Вариант №1: AddAccountQr ─────────────────────────────────────
    print("\nВариант №1 — AddAccountQr (в коде отсутствует):")
    aa_body = {
        "TerminalKey": key,
        "CustomerKey": "sbp-probe",
        "DataType": "PAYLOAD",
        "Description": "Проба привязки счёта (НЕ ПОДТВЕРЖДАТЬ)",
    }
    aa_body["Token"] = p._make_token(aa_body)
    aa = client.post(f"{TBANK_API_BASE}/AddAccountQr", json=aa_body).json()
    if show("AddAccountQr", aa):
        print(f"         RequestKey: {aa.get('RequestKey')}")
        print(f"         Data[:80]:  {str(aa.get('Data'))[:80]}")

    # ── Отменяем пробный платёж, чтобы не висел ──────────────────────
    if payment_id:
        c_body = {"TerminalKey": key, "PaymentId": payment_id}
        c_body["Token"] = p._make_token(c_body)
        cancel = client.post(f"{TBANK_API_BASE}/Cancel", json=c_body).json()
        print(f"\nCancel пробного платежа: Success={cancel.get('Success')} "
              f"{cancel.get('Message') or ''}")

    print("\nСырые ответы (для менеджера):")
    print(json.dumps({"init": init, "add_account_qr": aa}, ensure_ascii=False, indent=2))
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
