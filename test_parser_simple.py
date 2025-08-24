#!/usr/bin/env python3
"""Простой тест парсера Drom.ru."""

import sys
import os
from pathlib import Path

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.auto_reviews_parser.parsers.drom import DromParser


def test_parser():
    """Простой тест парсера."""
    parser = DromParser()

    # Тестируем извлечение марки и модели
    url = "https://www.drom.ru/reviews/toyota/camry/"
    brand, model = parser._extract_brand_model(url)
    print(f"Brand: {brand}, Model: {model}")
    assert brand == "toyota"
    assert model == "camry"

    print("Тест извлечения марки и модели прошел успешно!")

    # Тестируем простой парсинг (ограниченный)
    try:
        # Парсим отзывы конкретной модели
        reviews = parser.parse_reviews("toyota", "camry")
        print(f"Получено отзывов для Toyota Camry: {len(reviews)}")

        if reviews:
            review = reviews[0]
            print(f"Источник: {review.source}")
            print(f"Марка: {review.brand}")
            print(f"Модель: {review.model}")
            print(f"URL: {review.url}")
            print(f"Контент (первые 100 символов): {review.content[:100]}...")

        print("Тест парсинга прошел успешно!")

    except Exception as e:
        print(f"Ошибка при парсинге: {e}")
        return False

    return True


if __name__ == "__main__":
    if test_parser():
        print("Все тесты прошли успешно!")
    else:
        print("Тесты провалились!")
        sys.exit(1)
