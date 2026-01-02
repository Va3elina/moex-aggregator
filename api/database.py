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

# Создаём движок SQLAlchemy
engine = create_engine(DATABASE_URL, echo=False)

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