#!/usr/bin/env python3
"""
Скрипт для загрузки полного каталога брендов в базу данных
"""
import json
import sqlite3
from pathlib import Path


def setup_database():
    """Инициализируем базу данных с правильной схемой"""
    db_path = Path("../../data/databases/auto_reviews.db")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Создаем таблицу брендов
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS brands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Создаем таблицу моделей
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            slug TEXT NOT NULL,
            url TEXT NOT NULL,
            reviews_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (brand_id) REFERENCES brands (id),
            UNIQUE(brand_id, slug)
        )
    """
    )

    # Создаем таблицу отзывов
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id INTEGER NOT NULL,
            title TEXT,
            content TEXT,
            author TEXT,
            rating REAL,
            year INTEGER,
            engine_volume REAL,
            fuel_type TEXT,
            transmission TEXT,
            drive_type TEXT,
            city TEXT,
            pros TEXT,
            cons TEXT,
            breakages TEXT,
            photos TEXT,
            source_url TEXT,
            review_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES models (id)
        )
    """
    )

    # Создаем индексы для быстрого поиска
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_brands_slug ON brands(slug)
    """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_models_brand_slug
        ON models(brand_id, slug)
    """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_reviews_model
        ON reviews(model_id)
    """
    )

    conn.commit()
    return conn


def load_brands_to_database():
    """Загружаем полный каталог брендов в базу данных"""

    # Читаем файл с брендами
    brands_file = "../../data/exports/full_brands_catalog.json"
    if not Path(brands_file).exists():
        print(f"Файл {brands_file} не найден!")
        return False

    with open(brands_file, "r", encoding="utf-8") as f:
        brands = json.load(f)

    # Подключаемся к базе данных
    conn = setup_database()
    cursor = conn.cursor()

    # Очищаем существующие бренды если нужно
    cursor.execute("SELECT COUNT(*) FROM brands")
    existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        response = input(
            f"В базе уже есть {existing_count} брендов. "
            f"Очистить и загрузить заново? (y/N): "
        )
        if response.lower() == "y":
            cursor.execute("DELETE FROM reviews")
            cursor.execute("DELETE FROM models")
            cursor.execute("DELETE FROM brands")
            print("Очистили существующие данные")

    # Загружаем бренды
    inserted_count = 0
    skipped_count = 0

    for brand in brands:
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO brands (name, slug, url)
                VALUES (?, ?, ?)
            """,
                (brand["name"], brand["slug"], brand["url"]),
            )

            if cursor.rowcount > 0:
                inserted_count += 1
            else:
                skipped_count += 1

        except Exception as e:
            print(f"Ошибка при добавлении бренда {brand['name']}: {e}")
            skipped_count += 1

    conn.commit()
    conn.close()

    print("\nЗагрузка завершена:")
    print(f"- Добавлено брендов: {inserted_count}")
    print(f"- Пропущено (уже существуют): {skipped_count}")
    print(f"- Всего обработано: {len(brands)}")

    return True


def verify_database():
    """Проверяем содержимое базы данных"""
    conn = sqlite3.connect("../../data/databases/auto_reviews.db")
    cursor = conn.cursor()

    # Общая статистика
    cursor.execute("SELECT COUNT(*) FROM brands")
    brands_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM models")
    models_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM reviews")
    reviews_count = cursor.fetchone()[0]

    print(f"\nСтатистика базы данных:")
    print(f"- Брендов: {brands_count}")
    print(f"- Моделей: {models_count}")
    print(f"- Отзывов: {reviews_count}")

    # Показываем первые 10 брендов
    cursor.execute(
        """
        SELECT name, slug, url FROM brands 
        ORDER BY name LIMIT 10
    """
    )
    brands = cursor.fetchall()

    print(f"\nПервые 10 брендов в базе:")
    for i, (name, slug, url) in enumerate(brands, 1):
        print(f"{i}. {name} ({slug})")

    # Показываем русские бренды
    cursor.execute(
        """
        SELECT name FROM brands 
        WHERE name GLOB '*[а-яё]*'
        ORDER BY name
    """
    )
    russian_brands = cursor.fetchall()

    print(f"\nРусские бренды ({len(russian_brands)}):")
    for (name,) in russian_brands:
        print(f"- {name}")

    conn.close()


if __name__ == "__main__":
    print("=== Загрузка полного каталога брендов в базу данных ===")

    if load_brands_to_database():
        verify_database()
        print("\n✅ Каталог брендов успешно загружен в базу данных!")
    else:
        print("\n❌ Ошибка при загрузке каталога брендов")
