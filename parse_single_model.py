#!/usr/bin/env python3
"""Парсинг 10 отзывов одной модели Toyota Camry."""

import sys
from pathlib import Path


def parse_single_model():
    """Парсим 10 отзывов Toyota Camry."""
    # Добавляем пути для импорта
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))

    from src.auto_reviews_parser.parsers.drom import DromParser
    from src.auto_reviews_parser.database.base import Database
    from src.auto_reviews_parser.database.repositories.review_repository import (
        ReviewRepository,
    )

    print("Начинаем парсинг 10 отзывов Toyota Camry...")
    print("=" * 50)

    # Инициализация базы данных
    db_path = "auto_reviews.db"
    db = Database(db_path)

    # Инициализация
    parser = DromParser()
    repository = ReviewRepository(db)

    try:
        # Парсим только Toyota Camry
        print("Парсим Toyota Camry...")
        reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=10)

        print(f"Получено отзывов: {len(reviews)}")

        if reviews:
            # Сохраняем в базу
            saved_count = 0
            for review in reviews:
                try:
                    repository.add(review)
                    saved_count += 1
                    print(f"✓ Сохранен отзыв {saved_count}: {review.url}")
                except Exception as e:
                    print(f"✗ Ошибка сохранения отзыва: {e}")

            print(f"\nИтого сохранено в базу: {saved_count} отзывов")

            # Показываем примеры
            print("\nПримеры сохраненных отзывов:")
            print("-" * 50)
            for i, review in enumerate(reviews[:3], 1):
                print(f"{i}. {review.brand} {review.model}")
                print(f"   URL: {review.url}")
                print(f"   Контент: {review.content[:100]}...")
                print()
        else:
            print("Отзывы не найдены")

    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        print("Парсинг остановлен")
        return False

    return True


if __name__ == "__main__":
    if parse_single_model():
        print("Парсинг завершен успешно!")
    else:
        print("Парсинг завершен с ошибками!")
        sys.exit(1)
