#!/usr/bin/env python3
"""
Проверяем основной отзыв для дополнения
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser


def check_main_review():
    print("🔧 Проверяем основной отзыв...")

    # Основной отзыв (без последней части URL)
    main_url = "https://www.drom.ru/reviews/toyota/camry/1428758/"

    parser = DromParser()

    # Парсим основной отзыв
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=1)

    # Ищем наш отзыв
    for review in reviews:
        if "1428758" in review.url:
            print(f"✅ Найден основной отзыв: {review.url}")
            print(f"  Год: {review.year}")
            print(f"  Двигатель: {review.engine_volume}")
            print(f"  Трансмиссия: {review.transmission}")
            print(f"  Тип кузова: {review.body_type}")
            print(f"  Привод: {review.drive_type}")
            print(f"  Поколение: {review.generation}")
            print(f"  Тип топлива: {review.fuel_type}")
            break
    else:
        print("❌ Основной отзыв не найден в результатах")


if __name__ == "__main__":
    check_main_review()
