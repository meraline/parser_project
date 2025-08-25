#!/usr/bin/env python3
"""
Тестируем извлечение оценок по категориям
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository


def test_ratings_extraction():
    print("🔧 Тестируем извлечение оценок...")

    # Создаем тестовую базу
    test_db = "test_ratings.db"
    if os.path.exists(test_db):
        os.remove(test_db)

    db = Database(test_db)
    repo = ReviewRepository(db)

    # Парсер в обычном режиме
    parser = DromParser(gentle_mode=False)

    # Парсим один отзыв Toyota Camry
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=1)

    print(f"✅ Спарсили {len(reviews)} отзывов")

    if reviews:
        review = reviews[0]
        print(f"\n📋 Оценки в Review:")
        print(f"  Общая оценка: {review.overall_rating}")
        print(f"  Внешний вид: {review.exterior_rating}")
        print(f"  Салон: {review.interior_rating}")
        print(f"  Двигатель: {review.engine_rating}")
        print(f"  Ходовые качества: {review.driving_rating}")

        # Сохраняем в базу
        success = repo.save(review)
        print(f"\n💾 Сохранено в базу: {'✅' if success else '❌'}")

        if success:
            # Проверяем в базе
            import sqlite3

            conn = sqlite3.connect(test_db)
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT overall_rating, exterior_rating, interior_rating,
                       engine_rating, driving_rating
                FROM reviews 
                WHERE url = ?
            """,
                (review.url,),
            )

            result = cursor.fetchone()
            if result:
                print(f"\n🔍 Оценки в базе:")
                print(f"  Общая оценка: {result[0]}")
                print(f"  Внешний вид: {result[1]}")
                print(f"  Салон: {result[2]}")
                print(f"  Двигатель: {result[3]}")
                print(f"  Ходовые качества: {result[4]}")
            else:
                print("❌ Данные не найдены в базе!")

            conn.close()


if __name__ == "__main__":
    test_ratings_extraction()
