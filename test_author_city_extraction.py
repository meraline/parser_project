#!/usr/bin/env python3
"""
Тестируем извлечение автора и города на другом отзыве
"""

import os
import sys

# Добавляем корневую папку проекта в путь
project_root = "/home/analityk/Документы/projects/parser_project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

from src.auto_reviews_parser.parsers.drom import DromParser


def test_author_city():
    """Тестируем извлечение автора и города"""

    # Тестируем первый попавшийся отзыв из результатов парсинга
    # где может быть реальный автор
    test_urls = [
        "https://www.drom.ru/reviews/toyota/camry/1344577/",  # Попробуем другой отзыв
    ]

    parser = DromParser(gentle_mode=True)

    for url in test_urls:
        print(f"\n🔍 Тестируем отзыв: {url}")
        print("=" * 60)

        try:
            reviews = parser.parse_single_review(url)

            if reviews:
                review = reviews[0]
                print(f"✅ Успешно спарсили отзыв")
                print(f"   Автор: '{review.author}'")
                print(f"   Общий рейтинг: {review.overall_rating}")
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
            print(f"❌ Ошибка: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    test_author_city()
