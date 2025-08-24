#!/usr/bin/env python3
"""
Заполняем основную базу характеристиками
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
import sqlite3


def update_main_database():
    print("🔧 Обновляем основную базу данных...")

    # Основная база
    db_path = "auto_reviews_structured.db"
    db = Database(db_path)
    repo = ReviewRepository(db)

    # Парсим несколько отзывов Toyota Camry
    parser = DromParser()
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=3)

    print(f"✅ Спарсили {len(reviews)} отзывов")

    # Сохраняем в базу
    for review in reviews:
        print(f"\n📋 Сохраняем отзыв: {review.url}")
        print(f"  Тип: {review.type}")
        print(f"  Год: {review.year}")
        print(f"  Трансмиссия: {review.transmission}")
        print(f"  Тип кузова: {review.body_type}")

        success = repo.save(review)
        print(f"  Сохранено: {'✅' if success else '❌'}")

    # Проверяем что сохранилось
    print(f"\n🔍 Проверяем сохраненные данные...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT url, year, transmission, body_type, type, author
        FROM reviews 
        WHERE year IS NOT NULL
        LIMIT 5
    """
    )

    results = cursor.fetchall()
    print(f"Найдено записей с характеристиками: {len(results)}")

    for result in results:
        print(f"  URL: {result[0]}")
        print(f"  Год: {result[1]}")
        print(f"  Трансмиссия: {result[2]}")
        print(f"  Тип кузова: {result[3]}")
        print(f"  Тип: {result[4]}")
        print(f"  Автор: {result[5]}")
        print()

    conn.close()


if __name__ == "__main__":
    update_main_database()
