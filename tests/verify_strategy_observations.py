#!/usr/bin/env python3
"""
Проверка наблюдений из стратегии 'Трендовушка-Универсалка'
через наш API сезонности.
"""
import json, subprocess

# Получаем токен
def get_token():
    body = json.dumps({"email": "test@test.com", "password": "Testpass123"})
    r = subprocess.run(["curl", "-s", "http://localhost:8000/api/auth/login",
                        "-X", "POST", "-H", "Content-Type: application/json", "-d", body],
                       capture_output=True, text=True)
    return json.loads(r.stdout).get("access_token", "")

TOKEN = get_token()

def api(params):
    r = subprocess.run(["curl", "-s", f"http://localhost:8000/api/seasonality?{params}",
                        "-H", f"Authorization: Bearer {TOKEN}"],
                       capture_output=True, text=True)
    return json.loads(r.stdout)

print("=" * 65)
print("  ПРОВЕРКА: СТРАТЕГИЯ ТРЕНДОВУШКА vs НАШ ИНСТРУМЕНТ")
print("=" * 65)

# ─── 1. INTRADAY SBER ───
print("\n" + "━" * 65)
print("  1. INTRADAY SBER (90 дней)")
print("  Ожидание: рост утром (10-11) и вечером (17-18), падение днём")
print("━" * 65)
try:
    d = api("secid=SBER&mode=intraday&iterations=90")
    for b in d["bars"]:
        h = b["key"]
        v = b["avg_change"]
        sign = "+" if v > 0 else ""
        note = ""
        if h == 10: note = " <-- ОТКРЫТИЕ"
        if h in (12, 13, 14, 15): note = " <-- середина дня"
        if h == 17: note = " <-- ЗАКРЫТИЕ"
        if h == 18: note = " <-- вечерка"
        print(f"  {b['label']:<6} {sign}{v:.4f}%  {'>' if v > 0 else '<'}{note}")
except Exception:
    print("  (требует авторизации, пропущен)")

# ─── 2. WEEKDAY SBER ───
print("\n" + "━" * 65)
print("  2. WEEKDAY SBER (90 недель)")
print("  Ожидание: Пн/Вт рост, Ср/Чт падение, Пт неоднозначно")
print("━" * 65)
d = api("secid=SBER&mode=weekday&iterations=90")
expected = {1: "рост", 2: "рост", 3: "падение", 4: "падение", 5: "?"}
for b in d["bars"]:
    v = b["avg_change"]
    exp = expected.get(b["key"], "?")
    actual = "рост" if v > 0 else "падение"
    match = "ok" if (exp == "?" or exp == actual) else "MISS"
    sign = "+" if v > 0 else ""
    print(f"  {b['label']:<4} {sign}{v:.4f}%  ожид: {exp:<8} факт: {actual:<8} {match}")

# ─── 3. MONTHLY SBER ───
print("\n" + "━" * 65)
print("  3. MONTHLY SBER (30 лет) — Sell in May?")
print("  Ожидание: Ноя-Апр >> Май-Окт")
print("━" * 65)
d = api("secid=SBER&mode=monthly&iterations=30")
winter, summer = [], []
for b in d["bars"]:
    m, v = b["key"], b["avg_change"]
    is_winter = m in (11, 12, 1, 2, 3, 4)
    (winter if is_winter else summer).append(v)
    sign = "+" if v > 0 else ""
    tag = "ЗИМА" if is_winter else "лето"
    print(f"  {b['label']:<4} {sign}{v:.4f}%  {tag}")
w = sum(winter) / len(winter)
s = sum(summer) / len(summer)
print(f"  ---")
print(f"  Ноя-Апр (зима): +{w:.4f}% avg")
print(f"  Май-Окт (лето): {'+' if s > 0 else ''}{s:.4f}% avg")
print(f"  Sell in May: {'CONFIRMED' if w > s else 'NOT CONFIRMED'}")

# ─── 4. MONTHDAY SBER ───
print("\n" + "━" * 65)
print("  4. MONTHDAY SBER (30 мес) — начало месяца лучше?")
print("━" * 65)
d = api("secid=SBER&mode=monthday&iterations=30")
early = [b["avg_change"] for b in d["bars"] if b["key"] <= 5]
mid = [b["avg_change"] for b in d["bars"] if 10 <= b["key"] <= 20]
late = [b["avg_change"] for b in d["bars"] if b["key"] >= 25]
e = sum(early) / len(early) if early else 0
m = sum(mid) / len(mid) if mid else 0
l = sum(late) / len(late) if late else 0
print(f"  Начало (1-5):    {'+' if e > 0 else ''}{e:.4f}% avg")
print(f"  Середина (10-20):{'+' if m > 0 else ''}{m:.4f}% avg")
print(f"  Конец (25-31):   {'+' if l > 0 else ''}{l:.4f}% avg")
print(f"  Начало > середина: {'YES' if e > m else 'NO'}")

# ─── 5. MONTHLY IMOEX ───
print("\n" + "━" * 65)
print("  5. MONTHLY IMOEX (30 лет) — Sell in May?")
print("━" * 65)
d = api("secid=IMOEX&mode=monthly&iterations=30")
winter, summer = [], []
for b in d["bars"]:
    m, v = b["key"], b["avg_change"]
    is_winter = m in (11, 12, 1, 2, 3, 4)
    (winter if is_winter else summer).append(v)
    sign = "+" if v > 0 else ""
    tag = "ЗИМА" if is_winter else "лето"
    print(f"  {b['label']:<4} {sign}{v:.4f}%  {tag}")
w = sum(winter) / len(winter)
s = sum(summer) / len(summer)
print(f"  ---")
print(f"  Ноя-Апр: {'+' if w > 0 else ''}{w:.4f}%  |  Май-Окт: {'+' if s > 0 else ''}{s:.4f}%")
print(f"  Sell in May: {'CONFIRMED' if w > s else 'NOT CONFIRMED'}")

# ─── 6. WEEKDAY IMOEX ───
print("\n" + "━" * 65)
print("  6. WEEKDAY IMOEX (90 недель)")
print("━" * 65)
d = api("secid=IMOEX&mode=weekday&iterations=90")
for b in d["bars"]:
    v = b["avg_change"]
    sign = "+" if v > 0 else ""
    print(f"  {b['label']:<4} {sign}{v:.4f}%")

print("\n" + "=" * 65)
