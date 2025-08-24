"""Тестовый запуск парсера для AITO."""

from auto_reviews_parser.parsers.drom import DromParser


def main():
    """Запуск парсера."""
    brands = ["aito"]  # Тестируем на одной марке
    max_reviews = 5  # Ограничиваем количество отзывов
    max_pages = 2  # Ограничиваем количество страниц на модель

    with DromParser() as parser:
        for brand in brands:
            parser.parse_catalog(
                brand, max_reviews=max_reviews, max_pages_per_model=max_pages
            )


if __name__ == "__main__":
    main()
