"""
Подключение к базе данных PostgreSQL
"""
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from dotenv import load_dotenv
from pathlib import Path
import os

# Загружаем .env из корня проекта
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# URL подключения к базе данных
DATABASE_URL = os.getenv("DB_URL")

# Создаём движок SQLAlchemy с настройками пула.
# Наш self-hosted PostgreSQL: max_connections=100.
# Pool=15 base + 15 overflow → 30 concurrent connections max.
# Этого хватает на multi-user dashboard с 9+ concurrent API-запросами.
# (Раньше было 3+2=5 с времён Render Free — теперь узкое место).
_connect_args = {}
if DATABASE_URL and "pg8000" in DATABASE_URL:
    # pg8000 пытается использовать SSL по умолчанию —
    # для локального PostgreSQL это вызывает SSLError: malloc failure
    _connect_args["ssl_context"] = False

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_size=15,         # Базовый размер пула (было 3)
    max_overflow=15,      # Доп. соединения под нагрузкой (итого макс. 30, было 5)
    pool_pre_ping=True,   # Проверка живости соединения перед использованием
    pool_recycle=300,     # Пересоздавать соединения каждые 5 минут
    pool_timeout=10,      # Быстрее fail при pool exhaustion (10с вместо 30с default)
    connect_args=_connect_args,
)

# Жёсткий потолок на длительность любого запроса (защита от «зависших» SELECT).
# Инцидент 01.07.2026: read-only SELECT крутился 3+ дня, съедая ядро CPU, потому что
# у соединений НЕ был выставлен statement_timeout. 120с = запас: часть легитимных
# графиковых запросов исторически занимала 40–48с до оптимизации, поэтому НЕ ставим ниже.
#
# Драйвер pg8000 не поддерживает connect_args={"options": "-c ..."} (это фича libpq/psycopg2),
# поэтому задаём на уровне сессии через SET при установке соединения.
# autocommit=True на время SET обязателен: иначе SET попадёт в неявную транзакцию и
# будет откачен при возврате соединения в пул (reset_on_return='rollback').
_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "120000"))


@event.listens_for(engine, "connect")
def _set_statement_timeout(dbapi_connection, connection_record):
    prev_autocommit = getattr(dbapi_connection, "autocommit", None)
    if prev_autocommit is not None:
        dbapi_connection.autocommit = True
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(f"SET SESSION statement_timeout = {_STATEMENT_TIMEOUT_MS}")
    finally:
        cursor.close()
        if prev_autocommit is not None:
            dbapi_connection.autocommit = prev_autocommit


# Фабрика сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Базовый класс для моделей
class Base(DeclarativeBase):
    pass


# Функция для получения сессии БД
def get_db():
    """
    Генератор сессии базы данных.
    Используется как зависимость в FastAPI.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_engine():
    """Возвращает engine для прямых SQL запросов"""
    return engine