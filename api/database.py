"""
Подключение к базе данных PostgreSQL
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from dotenv import load_dotenv
from pathlib import Path
import os

# Загружаем .env из корня проекта
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# URL подключения к базе данных
DATABASE_URL = os.getenv("DB_URL")

# Создаём движок SQLAlchemy с настройками пула
# Render Free PostgreSQL: лимит 5 connections
# Определяем параметры подключения в зависимости от драйвера
_connect_args = {}
if DATABASE_URL and "pg8000" in DATABASE_URL:
    # pg8000 пытается использовать SSL по умолчанию —
    # для локального PostgreSQL это вызывает SSLError: malloc failure
    _connect_args["ssl_context"] = False

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_size=3,          # Базовый размер пула
    max_overflow=2,       # Дополнительные соединения при нагрузке (итого макс. 5)
    pool_pre_ping=True,   # Проверка живости соединения перед использованием
    pool_recycle=300,     # Пересоздавать соединения каждые 5 минут
    connect_args=_connect_args,
)

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