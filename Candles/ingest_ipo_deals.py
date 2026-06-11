#!/usr/bin/env python3
"""Ручной ингест календаря IPO Московской биржи → ipo_deals.

Источник — https://www.moex.com/ru/ipo/calendar (завершённые сделки). Данные
присылаются вручную (как cbr_flows/SCHA): MOEX/e-disclosure — раскрытие, авто-фетч
не делаем. Таблица нужна под два «событийных» паттерна навеса:
  • IPO-навес: Cash-out (действующие акционеры продают) → давление предложения;
  • lock-up: через ~6 мес владельцы могут продать → отложенный навес к дате истечения.
Доля размещения = стартовый free-float; уровень листинга = эшелон/качество.

Колонки сырья (строки — как в выгрузке MOEX, чистятся в коде):
  дата | эмитент | сектор | тикер | уровень | тип | цена | капа_млн | предложение_млн
  | доля_%_капитала | стабилизация_% | дата_lockup | акций_размещено
Запуск: python Candles/ingest_ipo_deals.py
"""
import os
import re
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL")

# Сырьё из выгрузки MOEX (завершённые сделки). '*' = расчётные данные MOEX.
RAW = [
    ("17.04.2026", 'ПАО "B2B-РТС"',        "Технологии и телеком", "BTBR",  2, "Cash-out", "118",  "21 000",  "2 430",  "11,5", "15",  "14.10.2026", "20 558 562"),
    ("10.12.2025", 'ПАО «ГК «БАЗИС»',       "Технологии и телеком", "BAZA",  2, "Cash-out", "109",  "18 000",  "3 000",  "16,7", "10",  "08.06.2026", "27 520 000"),
    ("20.11.2025", 'ПАО «ДОМ.РФ»',          "Финансовые услуги",    "DOMRF", 1, "Cash-in",  "1 750","314 805", "31 686", "10,1", "6.2", "19.05.2026", "18 106 050"),
    ("31.10.2025", 'ПАО "ГЛОРАКС"',         "Девелопмент",          "GLRX",  2, "Cash-in",  "64",   "18 100",  "2 100",  "11,6", "15",  "29.04.2026", "32 812 500"),
    ("30.10.2024", 'ПАО "Ламбумиз"',        "Производство упаковки","LMBZ",  3, "Cash-in",  "425",  "8 800",   "802",    "9",    "5,3", "30.10.2025", "1 887 059"),
    ("17.10.2024", 'ПАО "Озон Фармацевтика"',"Фарма",               "OZPH",  1, "Cash-in",  "35",   "38 450",  "3 450",  "9",    "15",  "15.04.2025", "98 571 440"),
    ("01.10.2024", 'ПАО "Группа Аренадата"', "Технологии и телеком","DATA",  2, "Cash-out", "95",   "19 000",  "2 660",  "14",   "10",  "30.03.2025", "28 000 000"),
    ("30.07.2024", 'ПАО "АПРИ"',            "Девелопмент",          "APRI",  3, "Cash-in",  "9,7",  "10 820",  "880",    "8",    "10",  "26.01.2025", "90 675 810"),
    ("12.07.2024", 'ПАО "Промомед"',        "Биотехнологии",        "PRMD",  2, "Cash-in",  "400",  "85 750",  "5 750",  "7",    "15",  "08.01.2025", "14 375 000"),
    ("05.07.2024", 'ПАО "ВИ.ру"',           "Потребительский сектор","VSEH", 1, "Cash-out", "200",  "100 000", "11 500", "12",   "15",  "01.01.2025", "57 500 000"),
]


def num(s):
    """'31 686' → 31686.0; '16,7' → 16.7; '9,7' → 9.7; '' → None."""
    if s is None:
        return None
    s = re.sub(r"[*\s]", "", str(s)).replace(",", ".")
    if not s:
        return None
    return float(s)


def d(s):
    return datetime.strptime(s, "%d.%m.%Y").date() if s else None


def main():
    eng = create_engine(DB_URL)
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ipo_deals (
                ticker         VARCHAR(20) PRIMARY KEY,
                issuer         TEXT,
                sector         TEXT,
                placement_date DATE,
                listing_level  SMALLINT,        -- 1/2/3 эшелон листинга
                deal_type      VARCHAR(10),      -- Cash-in / Cash-out
                price          NUMERIC(18,4),    -- цена размещения, ₽/акция
                mcap_mln       NUMERIC(18,2),    -- капитализация по итогам, млн ₽
                offer_mln      NUMERIC(18,2),    -- размер предложения, млн ₽
                float_pct      NUMERIC(6,2),     -- доля размещённых акций, % капитала
                stab_pct       NUMERIC(6,2),     -- механизм стабилизации, % предложения
                lockup_expiry  DATE,             -- дата истечения lock-up
                shares_placed  BIGINT
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ipo_lockup ON ipo_deals (lockup_expiry)"))
        n = 0
        for (dt, iss, sec, tic, lvl, typ, px, mc, of, fl, st, lk, sh) in RAW:
            conn.execute(text("""
                INSERT INTO ipo_deals (ticker, issuer, sector, placement_date, listing_level,
                    deal_type, price, mcap_mln, offer_mln, float_pct, stab_pct, lockup_expiry, shares_placed)
                VALUES (:t,:i,:s,:pd,:lv,:dt,:px,:mc,:of,:fl,:st,:lk,:sh)
                ON CONFLICT (ticker) DO UPDATE SET
                    issuer=EXCLUDED.issuer, sector=EXCLUDED.sector, placement_date=EXCLUDED.placement_date,
                    listing_level=EXCLUDED.listing_level, deal_type=EXCLUDED.deal_type, price=EXCLUDED.price,
                    mcap_mln=EXCLUDED.mcap_mln, offer_mln=EXCLUDED.offer_mln, float_pct=EXCLUDED.float_pct,
                    stab_pct=EXCLUDED.stab_pct, lockup_expiry=EXCLUDED.lockup_expiry, shares_placed=EXCLUDED.shares_placed
            """), {"t": tic, "i": iss, "s": sec, "pd": d(dt), "lv": lvl, "dt": typ,
                   "px": num(px), "mc": num(mc), "of": num(of), "fl": num(fl),
                   "st": num(st), "lk": d(lk), "sh": int(num(sh))})
            n += 1
        co = conn.execute(text("SELECT count(*) FILTER (WHERE deal_type='Cash-out'), count(*) FROM ipo_deals")).fetchone()
    print(f"ipo_deals: upsert {n} сделок (Cash-out {co[0]}/{co[1]})")


if __name__ == "__main__":
    main()
