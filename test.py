import asyncio
import aiohttp
from datetime import datetime
import os

# === НАСТРОЙКИ ===
BASE_TICKER = "NV"  # Новатэк
CHECK_YEARS = [3, 4, 5, 6]  # 2023, 2024, 2025, 2026
API_KEY = os.getenv("ALGOPACK_API_KEY", "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaVHA2Tjg1ekE4YTBFVDZ5SFBTajJ2V0ZldzNOc2xiSVR2bnVaYWlSNS1NIn0.eyJleHAiOjE3NjkwMTUxNTYsImlhdCI6MTc2NjQyMzE1NiwianRpIjoiZjBjODFmNDEtZTE3NC00NmRlLWIwMGUtZjAzZGQxY2I2YjhmIiwiaXNzIjoiaHR0cHM6Ly9zc28yLm1vZXguY29tL2F1dGgvcmVhbG1zL2NyYW1sIiwiYXVkIjpbImFjY291bnQiLCJpc3MiXSwic3ViIjoiZjowYmE2YThmMC1jMzhhLTQ5ZDYtYmEwZS04NTZmMWZlNGJmN2U6ZmI2YzRhMTMtMmEyOS00Nzk5LTljZTYtNDQyMTJkN2I5N2UzIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiaXNzIiwic2lkIjoiYmM0ZDYwMTYtYTg4MS00MDc2LThlNGEtNzY3NzAyMGI4NzkyIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIvKiJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBpc3NfYWxnb3BhY2sgcHJvZmlsZSBvZmZsaW5lX2FjY2VzcyBlbWFpbCBiYWNrd2FyZHNfY29tcGF0aWJsZSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiaXNzX3Blcm1pc3Npb25zIjoiMTM3LCAxMzgsIDEzOSwgMTQwLCAxNjUsIDE2NiwgMTY3LCAxNjgsIDMyOSwgNDIxIiwibmFtZSI6ItCQ0LvQtdC60YHQsNC90LTRgCDQotC-0YDQuNGPIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiZmI2YzRhMTMtMmEyOS00Nzk5LTljZTYtNDQyMTJkN2I5N2UzIiwiZ2l2ZW5fbmFtZSI6ItCQ0LvQtdC60YHQsNC90LTRgCIsInNlc3Npb25fc3RhdGUiOiJiYzRkNjAxNi1hODgxLTQwNzYtOGU0YS03Njc3MDIwYjg3OTIiLCJmYW1pbHlfbmFtZSI6ItCi0L7RgNC40Y8ifQ.ht68EDUCuDP_dweBnZalCQlwrkyEXtzfCxRwkO3V6H0zHtveqHh7S0AqIs2KDo57IepE83P20H2aZqWIHHOHlk66DhMn0EDu2V6CJLKHV8InWaoW_uKoinni1tND1b829VcnP5Bd2AdgHif8EWuUOg78P4u7EiRApf1CTMpVg_s2WKdIRmMdRSEFOlWi52oG5uYjqNdGsAT7J-HTzoSPqfQWiRKArnNp_tfPqB2lFkO2-hQgyx79c0ltQ4fQ2PtLyJxC4w25_R8bArpUrhUwvL8XhG4rlfRdC12RTdzJgvNptI_imm0LDgDe4km9oTYWYUn1av5HVW1Wg3sTvMkZcA")

# Проверяем ВСЕ буквы, чтобы убедиться, что работают только 4
MONTH_LETTERS = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']


async def check_contract(session, secid):
    url = f"https://apim.moex.com/iss/engines/futures/markets/forts/boards/rfud/securities/{secid}/candles.json"

    # !!! ВАЖНОЕ ИЗМЕНЕНИЕ !!!
    # Ставим дату начала поиска далеко в прошлом, чтобы найти экспирированные контракты
    params = {
        'interval': 24,
        'from': '2023-01-01',  # Ищем данные начиная с 2023 года
        'till': '2026-12-31'
    }
    headers = {'Authorization': f'Bearer {API_KEY}'}

    try:
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status != 200:
                return False, f"HTTP {resp.status}"

            data = await resp.json()
            candles = data.get('candles', {}).get('data', [])

            if candles:
                first_date = candles[0][6][:10]
                last_date = candles[-1][6][:10]
                count = len(candles)
                return True, f"Свечей: {count} ({first_date} ... {last_date})"
            else:
                return False, "Нет данных"

    except Exception as e:
        return False, f"Ошибка: {str(e)}"


async def main():
    print(f"🔎 Глубокий поиск истории для: {BASE_TICKER}")
    print("-" * 75)
    print(f"{'ТИКЕР':<10} | {'СТАТУС':<10} | {'ИНФО'}")
    print("-" * 75)

    async with aiohttp.ClientSession() as session:
        for year in CHECK_YEARS:
            for letter in MONTH_LETTERS:
                ticker = f"{BASE_TICKER}{letter}{year}"
                exists, msg = await check_contract(session, ticker)

                # Выводим только если нашли, чтобы не засорять экран
                if exists:
                    print(f"\033[92m{ticker:<10} | ✅ ЕСТЬ     | {msg}\033[0m")
                else:
                    # Если буква квартальная (H, M, U, Z), но данных нет - это странно, выводим красным
                    if letter in ['H', 'M', 'U', 'Z'] and year < 6:
                        print(f"\033[91m{ticker:<10} | ❌ ПУСТО    | {msg}\033[0m")

    print("-" * 75)


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())