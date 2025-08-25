"""Тестовый запуск парсера для AITO."""

import os
from auto_reviews_parser.parsers.drom import DromParser


def get_db_path() -> str:
    """Получить путь к файлу БД."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "auto_reviews.db")


def main():
    """Запуск парсера."""
    # Параметры парсинга
    brands = ["aito"]  # Тестируем на одной марке
    max_reviews = 5  # Ограничиваем количество отзывов
    max_pages = 2  # Ограничиваем количество страниц на модель

    # Запуск парсера
    with DromParser(db_path=get_db_path()) as parser:
        for brand in brands:
            try:
                parser.parse_catalog(
                    brand, max_reviews=max_reviews, max_pages_per_model=max_pages
                )
            except Exception as e:
                print(f"Ошибка при парсинге {brand}: {e}")


if __name__ == "__main__":
    main()
