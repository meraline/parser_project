#!/usr/bin/env python3
"""
Проверяем все извлекаемые характеристики
"""
import sys
import os

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser


def check_all_characteristics():
    print("🔧 Проверяем все извлекаемые характеристики...")

    parser = DromParser()
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=1)

    if not reviews:
        print("❌ Нет отзывов для анализа")
        return

    review = reviews[0]
    print(f"✅ Анализируем отзыв: {review.url}")

    # Смотрим на car_specs в процессе парсинга
    # Нужно модифицировать парсер для отладки

    print(f"\n📋 Текущие поля Review:")
    print(f"  Год: {review.year}")
    print(f"  Тип кузова: {review.body_type}")
    print(f"  Трансмиссия: {review.transmission}")
    print(f"  Привод: {review.drive_type}")
    print(f"  Поколение: {review.generation}")
    print(f"  Объем двигателя: {review.engine_volume}")
    print(f"  Тип топлива: {review.fuel_type}")
    print(f"  Пробег: {review.mileage}")


if __name__ == "__main__":
    check_all_characteristics()
