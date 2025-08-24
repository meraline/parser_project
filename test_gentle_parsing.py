#!/usr/bin/env python3
"""
Тестируем парсинг 20 отзывов в щадящем режиме
"""
import sys
import os
import time

# Добавляем путь к исходному коду
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository


def test_gentle_parsing():
    print("🔧 Тестируем щадящий парсинг 20 отзывов...")

    # Создаем базу
    db_path = "test_20_reviews.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    db = Database(db_path)
    repo = ReviewRepository(db)

    # Парсер в щадящем режиме
    parser = DromParser(gentle_mode=True)

    print(f"⚙️ Настройки парсера:")
    print(f"  Щадящий режим: {parser.gentle_mode}")
    print(f"  Задержка между отзывами: {parser.request_delay} сек")
    print(f"  Задержка между страницами: {parser.page_delay} сек")

    start_time = time.time()

    # Парсим Toyota Camry
    print(f"\n🚗 Начинаем парсинг Toyota Camry (20 отзывов)...")
    reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=20)

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"\n✅ Парсинг завершен!")
    print(f"  Время выполнения: {elapsed:.1f} секунд")
    print(f"  Собрано отзывов: {len(reviews)}")
    print(f"  Средняя скорость: {elapsed/len(reviews):.1f} сек/отзыв")

    # Сохраняем в базу
    saved_count = 0
    for review in reviews:
        if repo.save(review):
            saved_count += 1

    print(f"  Сохранено в базу: {saved_count}")

    # Анализируем результаты
    reviews_with_chars = sum(1 for r in reviews if r.year is not None)
    additions = sum(1 for r in reviews if r.type == "addition")
    main_reviews = sum(1 for r in reviews if r.type == "review")

    print(f"\n📊 Статистика:")
    print(f"  Основных отзывов: {main_reviews}")
    print(f"  Дополнений: {additions}")
    print(f"  С характеристиками: {reviews_with_chars}")

    # Показываем примеры
    print(f"\n📋 Примеры собранных данных:")
    for i, review in enumerate(reviews[:3]):
        print(f"  {i+1}. {review.url}")
        print(
            f"     Год: {review.year}, Объем: {review.engine_volume}л, "
            f"Мощность: {review.engine_power}л.с."
        )
        print(
            f"     Расход город/трасса: {review.fuel_consumption_city}/"
            f"{review.fuel_consumption_highway} л/100км"
        )


if __name__ == "__main__":
    test_gentle_parsing()
