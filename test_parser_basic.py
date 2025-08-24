#!/usr/bin/env python3
"""Простой тест парсера Drom.ru - только основные функции."""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.auto_reviews_parser.parsers.drom import DromParser


def test_parser_basic():
    """Базовый тест парсера."""
    parser = DromParser()

    # Тестируем извлечение марки и модели
    url = "https://www.drom.ru/reviews/toyota/camry/1428758/"
    brand, model = parser._extract_brand_model(url)
    print(f"Brand: {brand}, Model: {model}")
    assert brand == "toyota"
    assert model == "camry"

    # Тестируем фильтрацию URL
    test_urls = [
        "https://www.drom.ru/reviews/toyota/camry/1428758/",
        "https://www.drom.ru/reviews/toyota/camry/",
        "https://www.drom.ru/reviews/add/",
    ]

    for url in test_urls:
        result = parser._is_review_url(url)
        print(f"URL {url} is review: {result}")

    print("Базовые тесты прошли успешно!")
    return True


if __name__ == "__main__":
    if test_parser_basic():
        print("Все базовые тесты прошли успешно!")
    else:
        print("Тесты провалились!")
        sys.exit(1)
