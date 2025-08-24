"""Тестовый запуск парсера для AITO."""

import asyncio
import os
from auto_reviews_parser.database.base import Database
from auto_reviews_parser.database.repositories.review_repository import ReviewRepository
from auto_reviews_parser.parsers.drom import DromParser
from auto_reviews_parser.services.parser_service import ParserService


def get_db_path() -> str:
    """Получить путь к файлу БД."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "auto_reviews.db")


async def main():
    """Запуск парсера."""
    # Инициализация компонентов
    db = Database(get_db_path())
    review_repo = ReviewRepository(db)
    parser = DromParser(db_path=get_db_path())
    parser_service = ParserService(review_repo)

    # Параметры парсинга
    brands = ["aito"]  # Тестируем на одной марке
    max_reviews = 5  # Ограничиваем количество отзывов
    max_pages = 2  # Ограничиваем количество страниц на модель

    # Запуск парсера
    async with parser:
        for brand in brands:
            try:
                await parser.parse_catalog(
                    brand, max_reviews=max_reviews, max_pages_per_model=max_pages
                )
            except Exception as e:
                print(f"Ошибка при парсинге {brand}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
