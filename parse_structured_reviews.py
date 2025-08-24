#!/usr/bin/env python3
"""Парсинг отзывов Toyota Camry с извлечением структурированных данных."""

import sys
import json
from pathlib import Path


def parse_with_structured_data():
    """Парсим отзывы с извлечением структурированных данных."""
    # Добавляем пути для импорта
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))

    from src.auto_reviews_parser.parsers.drom import DromParser
    from src.auto_reviews_parser.database.base import Database
    from src.auto_reviews_parser.database.repositories.review_repository import (
        ReviewRepository,
    )

    print("Парсинг Toyota Camry с извлечением структурированных данных...")
    print("=" * 60)

    # Инициализация базы данных
    db_path = "auto_reviews_structured.db"
    db = Database(db_path)

    # Инициализация
    parser = DromParser()
    repository = ReviewRepository(db)

    try:
        # Парсим только Toyota Camry
        print("Парсим Toyota Camry...")
        reviews = parser.parse_catalog_model("toyota", "camry", max_reviews=5)

        print(f"Получено отзывов: {len(reviews)}")

        if reviews:
            # Сохраняем в базу и собираем структурированные данные
            structured_data = []
            saved_count = 0

            for review in reviews:
                try:
                    repository.add(review)
                    saved_count += 1
                    print(f"✓ Сохранен отзыв {saved_count}: {review.url}")

                    # Собираем структурированные данные
                    review_data = {
                        "url": review.url,
                        "brand": review.brand,
                        "model": review.model,
                        "author": review.author,
                        "content": review.content,
                        "rating": review.rating,
                        "views_count": review.views_count,
                        "comments_count": review.comments_count,
                        "likes_count": review.likes_count,
                        "content_length": len(review.content) if review.content else 0,
                    }
                    structured_data.append(review_data)

                except Exception as e:
                    print(f"✗ Ошибка сохранения отзыва: {e}")

            print(f"\nИтого сохранено в базу: {saved_count} отзывов")

            # Сохраняем структурированные данные в JSON
            output_file = "toyota_camry_reviews.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(structured_data, f, ensure_ascii=False, indent=2)

            print(f"Структурированные данные сохранены в: {output_file}")

            # Показываем примеры структурированных данных
            print("\nПримеры структурированных данных:")
            print("-" * 50)
            for i, data in enumerate(structured_data[:2], 1):
                print(f"{i}. {data['brand']} {data['model']}")
                print(f"   Автор: {data['author']}")
                print(f"   Рейтинг: {data['rating']}")
                print(f"   Просмотры: {data['views_count']}")
                print(f"   Комментарии: {data['comments_count']}")
                print(f"   Лайки: {data['likes_count']}")
                print(f"   Длина отзыва: {data['content_length']} символов")
                print(f"   URL: {data['url']}")
                print(f"   Текст: {data['content'][:200]}...")
                print()

            # Статистика
            print("Статистика по отзывам:")
            print("-" * 30)
            avg_rating = sum(r["rating"] for r in structured_data if r["rating"]) / len(
                [r for r in structured_data if r["rating"]]
            )
            avg_views = sum(r["views_count"] for r in structured_data) / len(
                structured_data
            )
            avg_length = sum(r["content_length"] for r in structured_data) / len(
                structured_data
            )

            print(f"Средний рейтинг: {avg_rating:.1f}")
            print(f"Средние просмотры: {avg_views:.0f}")
            print(f"Средняя длина отзыва: {avg_length:.0f} символов")

        else:
            print("Отзывы не найдены")

    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        print("Парсинг остановлен")
        return False

    return True


if __name__ == "__main__":
    if parse_with_structured_data():
        print("Парсинг завершен успешно!")
    else:
        print("Парсинг завершен с ошибками!")
        sys.exit(1)
