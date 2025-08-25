#!/usr/bin/env python3
"""
Тестирование сохранения характеристик в базу данных
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
import sqlite3


def test_characteristics_saving():
    print("🔧 Тестируем сохранение характеристик...")

    # Создаем тестовую базу
    test_db = "test_characteristics.db"
    if os.path.exists(test_db):
        os.remove(test_db)

    # Инициализируем репозиторий
    db = Database(test_db)
    repo = ReviewRepository(db)

    # Парсим дополнение напрямую
    parser = DromParser()

    # Используем parse_catalog_model для получения отзывов Toyota Camry
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=5)

    print(f"✅ Спарсили {len(reviews)} отзывов")

    # Сохраняем в базу
    for review in reviews:
        print(f"\n📋 Характеристики перед сохранением:")
        print(f"  Год: {review.year}")
        print(f"  Двигатель: {review.engine_volume}")
        print(f"  Трансмиссия: {review.transmission}")
        print(f"  Тип кузова: {review.body_type}")
        print(f"  Привод: {review.drive_type}")
        print(f"  Поколение: {review.generation}")
        print(f"  Тип топлива: {review.fuel_type}")

        repo.save(review)

    # Проверяем что сохранилось
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT url, year, engine_volume, transmission, body_type, 
               drive_type, generation, fuel_type, type
        FROM reviews 
        WHERE url LIKE '%187642%'
    """
    )

    result = cursor.fetchone()
    if result:
        print(f"\n✅ Данные в базе:")
        print(f"  URL: {result[0]}")
        print(f"  Год: {result[1]}")
        print(f"  Двигатель: {result[2]}")
        print(f"  Трансмиссия: {result[3]}")
        print(f"  Тип кузова: {result[4]}")
        print(f"  Привод: {result[5]}")
        print(f"  Поколение: {result[6]}")
        print(f"  Тип топлива: {result[7]}")
        print(f"  Тип контента: {result[8]}")
    else:
        print("❌ Данные не найдены в базе!")

    conn.close()


if __name__ == "__main__":
    test_characteristics_saving()
