#!/usr/bin/env python3
"""
Диагностика проблем сохранения
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
import sqlite3


def diagnose_save_issues():
    print("🔧 Диагностика проблем сохранения...")

    # Создаем тестовую базу
    test_db = "diagnose.db"
    if os.path.exists(test_db):
        os.remove(test_db)

    db = Database(test_db)
    repo = ReviewRepository(db)

    # Парсим один отзыв
    parser = DromParser()
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=1)

    if not reviews:
        print("❌ Нет отзывов для тестирования")
        return

    review = reviews[0]
    print(f"✅ Получен отзыв: {review.url}")

    # Проверяем атрибуты Review
    print(f"\n📋 Атрибуты Review:")
    for attr in dir(review):
        if not attr.startswith("_"):
            value = getattr(review, attr)
            print(f"  {attr}: {value} ({type(value)})")

    # Пробуем сохранить
    print(f"\n💾 Попытка сохранения...")
    try:
        result = repo.save(review)
        print(f"Результат save(): {result}")

        # Проверяем что в базе
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM reviews")
        count = cursor.fetchone()[0]
        print(f"Записей в базе: {count}")

        if count > 0:
            cursor.execute("SELECT url, year, transmission FROM reviews LIMIT 1")
            row = cursor.fetchone()
            print(f"Данные: {row}")

        conn.close()

    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    diagnose_save_issues()
