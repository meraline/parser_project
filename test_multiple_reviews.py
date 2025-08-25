#!/usr/bin/env python3
"""
Тестирование парсера на отзывах с рейтингами и без них
"""

import os
import sys

# Добавляем корневую папку проекта в путь
project_root = "/home/analityk/Документы/projects/parser_project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

from src.auto_reviews_parser.parsers.drom import DromParser


def test_multiple_reviews():
    """Тестируем парсер на разных отзывах"""

    # URLs отзывов для тестирования
    test_urls = [
        "https://www.drom.ru/reviews/toyota/camry/1428758/",  # Текущий отзыв
        # Добавим больше URL если найдем
    ]

    parser = DromParser(gentle_mode=True)

    for i, url in enumerate(test_urls, 1):
        print(f"\n{'='*50}")
        print(f"🔍 Тестируем отзыв #{i}: {url}")
        print(f"{'='*50}")

        try:
            # Парсим отзыв
            reviews = parser.parse_single_review(url)

            if reviews:
                review = reviews[0]
                print(f"✅ Успешно спарсили отзыв")
                print(f"   Автор: {review.author}")
                print(f"   Общий рейтинг: {review.overall_rating}")
                print(f"   Рейтинги по категориям:")
                print(f"     - Внешний вид: {review.exterior_rating}")
                print(f"     - Салон: {review.interior_rating}")
                print(f"     - Двигатель: {review.engine_rating}")
                print(f"     - Ходовые качества: {review.driving_rating}")

                # Проверяем логику
                has_category_ratings = any(
                    [
                        review.exterior_rating is not None,
                        review.interior_rating is not None,
                        review.engine_rating is not None,
                        review.driving_rating is not None,
                    ]
                )

                if has_category_ratings:
                    print(f"   ⭐ В отзыве ЕСТЬ категорийные рейтинги")
                else:
                    print(f"   📝 В отзыве НЕТ категорийных рейтингов (это нормально)")

                print(f"   Технические характеристики:")
                print(f"     - Год: {review.year}")
                print(f"     - Тип кузова: {review.body_type}")
                print(
                    f"     - Двигатель: {review.engine_volume}л, {review.engine_power}л.с."
                )
                print(f"     - Трансмиссия: {review.transmission}")
                print(f"     - Привод: {review.drive_type}")
                print(f"     - Пробег: {review.mileage} км")

            else:
                print(f"❌ Не удалось спарсить отзыв")

        except Exception as e:
            print(f"❌ Ошибка при парсинге: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    test_multiple_reviews()
