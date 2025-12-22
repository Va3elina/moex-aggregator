from db.models import Base, engine, create_indexes

def create_all():
    print("⏳ Создаём таблицы...")
    Base.metadata.create_all(engine)
    print("✅ Таблицы успешно созданы!")

    with engine.connect() as conn:
        print("⚙️  Создаём индексы...")
        create_indexes(conn)
        print("✅ Индексы успешно созданы!")

if __name__ == "__main__":
    create_all()
