#!/usr/bin/env python3
"""
Тестируем извлечение всех характеристик
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
import sqlite3


def test_all_characteristics():
    print("🔧 Тестируем извлечение всех характеристик...")

    # Создаем тестовую базу
    test_db = "test_all_chars.db"
    if os.path.exists(test_db):
        os.remove(test_db)

    db = Database(test_db)
    repo = ReviewRepository(db)

    # Парсим один отзыв с характеристиками
    parser = DromParser()
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=1)

    print(f"✅ Спарсили {len(reviews)} отзывов")

    if reviews:
        review = reviews[0]
        print(f"\n📋 Все характеристики Review:")
        print(f"  Год выпуска: {review.year}")
        print(f"  Тип кузова: {review.body_type}")
        print(f"  Трансмиссия: {review.transmission}")
        print(f"  Привод: {review.drive_type}")
        print(f"  Поколение: {review.generation}")
        print(f"  Объем двигателя: {review.engine_volume}")
        print(f"  Мощность двигателя: {review.engine_power}")
        print(f"  Тип топлива: {review.fuel_type}")
        print(f"  Расход по городу: {review.fuel_consumption_city}")
        print(f"  Расход по трассе: {review.fuel_consumption_highway}")
        print(f"  Год приобретения: {review.year_purchased}")
        print(f"  Пробег: {review.mileage}")
        print(f"  Руль: {review.steering_wheel}")

        # Сохраняем в базу
        success = repo.save(review)
        print(f"\n💾 Сохранено в базу: {'✅' if success else '❌'}")

        # Проверяем в базе
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT year, body_type, transmission, drive_type, 
                   engine_volume, engine_power, fuel_consumption_city,
                   fuel_consumption_highway, year_purchased, mileage,
                   steering_wheel
            FROM reviews 
            WHERE url = ?
        """,
            (review.url,),
        )

        result = cursor.fetchone()
        if result:
            print(f"\n🔍 Данные в базе:")
            fields = [
                "Год",
                "Кузов",
                "Трансмиссия",
                "Привод",
                "Объем двигателя",
                "Мощность",
                "Расход город",
                "Расход трасса",
                "Год покупки",
                "Пробег",
                "Руль",
            ]
            for i, field in enumerate(fields):
                print(f"  {field}: {result[i]}")

        conn.close()


if __name__ == "__main__":
    test_all_characteristics()
