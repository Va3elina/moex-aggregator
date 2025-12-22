from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.models import Instrument
from config import DATABASE_URL

# Список фьючерсов (имя, тикер)
futures = [
    ("Сбербанк", "SR"),
    ("Газпром", "GZ"),
    ("ВТБ", "VB"),
    ("Сбербанк (вечн.)", "SBERF"),
    ("Лукойл", "LK"),
    ("НОВАТЭК", "NK"),
    ("Газпром (вечн.)", "GAZPF"),
    ("СПБ биржа", "SE"),
    ("Норильский никель", "GK"),
    ("ПИК", "PI"),
    ("Яндекс", "YD"),
    ("Полюс Золото", "PX"),
    ("Роснефть", "RN"),
    ("Тинькофф", "TB"),
    ("Сбербанк (преф)", "SP"),
    ("Татнефть", "TT"),
    ("Alibaba Group", "BB"),
    ("Аэрофлот", "AF"),
    ("Московская биржа", "ME"),
    ("Самолет", "SS"),
    ("Сургутнефтегаз (прив)", "SG"),
    ("X5 Group", "X5"),
    ("ММК", "MG"),
    ("Совкомбанк", "SC"),
    ("Магнит", "MN"),
    ("НЛМК", "NM"),
    ("Северсталь", "CH"),
    ("Сургутнефтегаз", "SN"),
    ("Baidu", "BD"),
    ("Интер РАО", "IR"),
    ("Мечел", "MC"),
    ("АФК Система", "AK"),
    ("Ростелеком", "RT"),
    ("Русал", "RL"),
    ("М.Видео", "MV"),
    ("ВКонтакте", "VK"),
    ("Whoosh", "WU"),
    ("Транснефть", "TN"),
    ("Белуга Групп", "NB"),
    ("ФосАгро", "PH"),
    ("Газпром нефть", "SO"),  # ← латинская O
    ("МТС", "MT"),
    ("Сегежа Групп", "SZ"),
    ("АЛРОСА", "AL"),
    ("Positive Technologies", "PS"),
    ("Астра", "AS"),
    ("ДВМП (FESCO)", "FE"),
    ("ФСК Россети", "FS"),
    ("РуссНефть", "RU"),
    ("РусГидро", "HY"),
    ("Совкомфлот", "FL"),
    ("Банк Санкт-Петербург", "BS"),
    ("Распадская", "RA"),
    ("Татнефть (прив)", "TP"),
    ("Юнипро", "UN"),
    ("Европлан", "LE"),
    ("Ренессанс Страхование", "RD"),
    ("HeadHunter", "HD"),
    ("ЭсЭфАй", "SH"),
    ("МКБ", "CM"),
    ("Xiaomi", "XI"),
    ("Softline", "S0"),  # ← цифра ноль
    ("Башнефть", "BN"),
    ("КАМАЗ", "KM"),
    ("Tencent", "TC"),
    ("Артген", "IS"),
]


# Подключаемся к базе
engine = create_engine(DATABASE_URL)

with Session(engine) as session:
    for name, ticker in futures:
        # Проверяем, нет ли уже такого тикера
        exists = session.query(Instrument).filter_by(ticker=ticker).first()
        if not exists:
            session.add(Instrument(name=name, ticker=ticker))
    session.commit()

print("✅ Все инструменты успешно добавлены в таблицу 'instruments'!")
