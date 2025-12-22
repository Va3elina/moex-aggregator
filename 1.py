import asyncio
import aiohttp
from sqlalchemy import create_engine
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from Candles.fetch_candles_futures_realtime import FuturesManager

API_KEY = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaVHA2Tjg1ekE4YTBFVDZ5SFBTajJ2V0ZldzNOc2xiSVR2bnVaYWlSNS1NIn0.eyJleHAiOjE3NjkwMTUxNTYsImlhdCI6MTc2NjQyMzE1NiwianRpIjoiZjBjODFmNDEtZTE3NC00NmRlLWIwMGUtZjAzZGQxY2I2YjhmIiwiaXNzIjoiaHR0cHM6Ly9zc28yLm1vZXguY29tL2F1dGgvcmVhbG1zL2NyYW1sIiwiYXVkIjpbImFjY291bnQiLCJpc3MiXSwic3ViIjoiZjowYmE2YThmMC1jMzhhLTQ5ZDYtYmEwZS04NTZmMWZlNGJmN2U6ZmI2YzRhMTMtMmEyOS00Nzk5LTljZTYtNDQyMTJkN2I5N2UzIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiaXNzIiwic2lkIjoiYmM0ZDYwMTYtYTg4MS00MDc2LThlNGEtNzY3NzAyMGI4NzkyIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIvKiJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBpc3NfYWxnb3BhY2sgcHJvZmlsZSBvZmZsaW5lX2FjY2VzcyBlbWFpbCBiYWNrd2FyZHNfY29tcGF0aWJsZSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiaXNzX3Blcm1pc3Npb25zIjoiMTM3LCAxMzgsIDEzOSwgMTQwLCAxNjUsIDE2NiwgMTY3LCAxNjgsIDMyOSwgNDIxIiwibmFtZSI6ItCQ0LvQtdC60YHQsNC90LTRgCDQotC-0YDQuNGPIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiZmI2YzRhMTMtMmEyOS00Nzk5LTljZTYtNDQyMTJkN2I5N2UzIiwiZ2l2ZW5fbmFtZSI6ItCQ0LvQtdC60YHQsNC90LTRgCIsInNlc3Npb25fc3RhdGUiOiJiYzRkNjAxNi1hODgxLTQwNzYtOGU0YS03Njc3MDIwYjg3OTIiLCJmYW1pbHlfbmFtZSI6ItCi0L7RgNC40Y8ifQ.ht68EDUCuDP_dweBnZalCQlwrkyEXtzfCxRwkO3V6H0zHtveqHh7S0AqIs2KDo57IepE83P20H2aZqWIHHOHlk66DhMn0EDu2V6CJLKHV8InWaoW_uKoinni1tND1b829VcnP5Bd2AdgHif8EWuUOg78P4u7EiRApf1CTMpVg_s2WKdIRmMdRSEFOlWi52oG5uYjqNdGsAT7J-HTzoSPqfQWiRKArnNp_tfPqB2lFkO2-hQgyx79c0ltQ4fQ2PtLyJxC4w25_R8bArpUrhUwvL8XhG4rlfRdC12RTdzJgvNptI_imm0LDgDe4km9oTYWYUn1av5HVW1Wg3sTvMkZcA"
DB_URL = "postgresql+pg8000://postgres:1803@localhost:5432/moex_db"


async def test_fix():
    engine = create_engine(DB_URL)
    manager = FuturesManager(engine, API_KEY)
    manager.invalidate_cache()

    async with aiohttp.ClientSession() as session:
        contracts = await manager.get_active_contracts(session)

    check = ['Si', 'BR', 'NG', 'GD', 'SR', 'GZ', 'RI']

    print("\n📋 Что теперь выбирает скрипт:")
    print("-" * 40)
    for secid, base, name, sectype in contracts:
        if sectype in check:
            print(f"{sectype}: выбран {secid}")


asyncio.run(test_fix())