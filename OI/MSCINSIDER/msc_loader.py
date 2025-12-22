"""Детальная диагностика API запроса"""
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

with engine.connect() as conn:
    print("="*70)
    print("ДИАГНОСТИКА API ЗАПРОСА")
    print("="*70)

    sectype = 'SR'
    inst_type = 'futures'
    interval = 5
    clgroup = 'FIZ'

    # 1. Получаем sec_ids
    print("\n1. sec_ids ИЗ instruments:")
    result = conn.execute(text("""
        SELECT DISTINCT sec_id 
        FROM instruments 
        WHERE sectype = :sectype AND type = :inst_type
    """), {'sectype': sectype, 'inst_type': inst_type})

    sec_ids = [row[0] for row in result.fetchall()]
    print(f"  {sec_ids}")

    # 2. Период свечей (без IN, через JOIN)
    print("\n2. ПЕРИОД СВЕЧЕЙ:")
    result = conn.execute(text("""
        SELECT 
            MIN(c.begin_time) as start,
            MAX(c.begin_time) as end
        FROM candles c
        JOIN instruments i ON c.sec_id = i.sec_id
        WHERE i.sectype = :sectype 
          AND i.type = :inst_type
          AND c.interval = :intv
          AND c.close > 0
    """), {'sectype': sectype, 'inst_type': inst_type, 'intv': interval})

    row = result.fetchone()
    candles_start = row[0]
    candles_end = row[1]
    print(f"  Start: {candles_start}")
    print(f"  End: {candles_end}")

    # 3. Период OI
    print("\n3. ПЕРИОД OI:")
    result = conn.execute(text("""
        SELECT 
            MIN(tradedate) as start_date,
            MAX(tradedate) as end_date
        FROM open_interest
        WHERE sectype = :sectype
          AND clgroup = :clgroup
          AND interval = :intv
    """), {'sectype': sectype, 'clgroup': clgroup, 'intv': interval})

    row = result.fetchone()
    oi_start = row[0]
    oi_end = row[1]
    print(f"  Start: {oi_start}")
    print(f"  End: {oi_end}")

    # 4. Вычисляем пересечение (как в API)
    print("\n4. ВЫЧИСЛЕНИЕ ПЕРЕСЕЧЕНИЯ:")
    if candles_start and oi_start:
        candles_start_date = candles_start.date()
        candles_end_date = candles_end.date()

        work_start = max(candles_start_date, oi_start)
        work_end = min(candles_end_date, oi_end)

        print(f"  candles_start_date: {candles_start_date}")
        print(f"  candles_end_date:   {candles_end_date}")
        print(f"  oi_start:           {oi_start}")
        print(f"  oi_end:             {oi_end}")
        print(f"  ")
        print(f"  work_start = max(candles, oi) = {work_start}")
        print(f"  work_end   = min(candles, oi) = {work_end}")

        # 5. Проверяем количество свечей в work периоде
        print("\n5. СВЕЧИ В WORK ПЕРИОДЕ:")
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM candles c
            JOIN instruments i ON c.sec_id = i.sec_id
            WHERE i.sectype = :sectype
              AND i.type = :inst_type
              AND c.interval = :intv
              AND c.begin_time >= :work_start
              AND c.begin_time <= :work_end
              AND c.close > 0
        """), {
            'sectype': sectype,
            'inst_type': inst_type,
            'intv': interval,
            'work_start': work_start,
            'work_end': work_end
        })
        print(f"  Количество: {result.scalar():,}")

        # 6. Проверяем количество OI в work периоде
        print("\n6. OI В WORK ПЕРИОДЕ:")
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM open_interest
            WHERE sectype = :sectype
              AND clgroup = :clgroup
              AND interval = :intv
              AND tradedate >= :work_start
              AND tradedate <= :work_end
        """), {
            'sectype': sectype,
            'clgroup': clgroup,
            'intv': interval,
            'work_start': work_start,
            'work_end': work_end
        })
        print(f"  Количество: {result.scalar():,}")

        # 7. Проверка - почему work_end такой маленький?
        print("\n7. ДЕТАЛЬНАЯ ПРОВЕРКА ПЕРИОДОВ:")
        print(f"  Свечи: {candles_start_date} - {candles_end_date} ({(candles_end_date - candles_start_date).days} дней)")
        print(f"  OI:    {oi_start} - {oi_end} ({(oi_end - oi_start).days} дней)")
        print(f"  Work:  {work_start} - {work_end} ({(work_end - work_start).days} дней)")