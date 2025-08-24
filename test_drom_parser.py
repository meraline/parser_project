#!/usr/bin/env python3
"""Тестирование парсера Drom.ru"""

import os
import logging
from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.utils.logger import get_logger

# Настройка логирования
logger = get_logger(__name__)
logging.getLogger().setLevel(logging.INFO)


def test_parser():
    """Тестирование основных функций парсера."""
    try:
        # Инициализация парсера
        parser = DromParser(db_path="test_reviews.db")

        # Тестируем парсинг отзывов для конкретной марки
        brand = "toyota"  # Используем Toyota как пример
        max_reviews = 5  # Ограничиваем количество отзывов для теста
        max_pages = 2  # Ограничиваем количество страниц

        logger.info(f"Начинаем парсинг отзывов {brand.title()}")
        parser.parse_catalog(brand=brand, max_reviews=max_reviews, max_pages=max_pages)

        logger.info("Тестирование завершено успешно")

    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        raise


if __name__ == "__main__":
    test_parser()
