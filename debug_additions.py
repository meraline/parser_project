#!/usr/bin/env python3
"""Отладка парсинга дополнений к отзывам."""

import sys
from pathlib import Path


def debug_review_additions():
    """Отладка парсинга дополнений к отзывам."""
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))

    from src.auto_reviews_parser.parsers.drom import DromParser

    parser = DromParser()

    # URL основного отзыва и дополнения
    main_url = "https://www.drom.ru/reviews/toyota/camry/1428758/"
    addition_url = "https://www.drom.ru/reviews/toyota/camry/1428758/187642/"

    print("=== ОТЛАДКА ПАРСИНГА ДОПОЛНЕНИЙ ===")
    print(f"Основной отзыв: {main_url}")
    print(f"Дополление: {addition_url}")
    print()

    # Проверяем фильтрацию URL
    print("=== ФИЛЬТРАЦИЯ URL ===")
    print(f"Основной отзыв проходит фильтр: {parser._is_review_url(main_url)}")
    print(f"Дополление проходит фильтр: {parser._is_review_url(addition_url)}")
    print()

    # Разбираем структуру URL
    print("=== СТРУКТУРА URL ===")

    def analyze_url(url):
        print(f"URL: {url}")
        if url.startswith("https://www.drom.ru"):
            path = url[19:]
        else:
            path = url
        print(f"Путь: {path}")

        if path.startswith("/reviews/"):
            path = path[9:]
            print(f"После удаления /reviews/: {path}")

            parts = [p for p in path.split("/") if p]
            print(f"Части: {parts}")
            print(f"Количество частей: {len(parts)}")

            if len(parts) >= 3:
                print(f"Бренд: {parts[0]}")
                print(f"Модель: {parts[1]}")
                print(f"ID отзыва: {parts[2]}")
                if len(parts) >= 4:
                    print(f"ID дополнения: {parts[3]}")
        print()

    analyze_url(main_url)
    analyze_url(addition_url)

    # Проверяем извлечение бренда и модели
    print("=== ИЗВЛЕЧЕНИЕ БРЕНДА И МОДЕЛИ ===")
    brand1, model1 = parser._extract_brand_model(main_url)
    print(f"Основной отзыв: {brand1} {model1}")

    brand2, model2 = parser._extract_brand_model(addition_url)
    print(f"Дополнение: {brand2} {model2}")


if __name__ == "__main__":
    debug_review_additions()
