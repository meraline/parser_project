#!/usr/bin/env python3
"""Полная отладка процесса парсинга и сохранения."""

import sys
from pathlib import Path


def debug_full_process():
    """Отлаживаем весь процесс от парсинга до сохранения в БД."""
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))

    from src.auto_reviews_parser.parsers.drom import DromParser
    from src.auto_reviews_parser.database.base import Database
    from src.auto_reviews_parser.database.repositories.review_repository import (
        ReviewRepository,
    )
    from src.auto_reviews_parser.models.review import Review
    from playwright.sync_api import sync_playwright
    import os

    print("=== ПОЛНАЯ ОТЛАДКА ПРОЦЕССА ПАРСИНГА ===")
    url = "https://www.drom.ru/reviews/toyota/camry/1428758/187642/"
    print(f"URL: {url}")
    print()

    parser = DromParser()

    # Шаг 1: Парсинг данных
    print("=== ШАГ 1: ПАРСИНГ ДАННЫХ ===")
    with sync_playwright() as p:
        if os.path.exists(parser.chrome_path):
            browser = p.chromium.launch(
                executable_path=parser.chrome_path, headless=True
            )
        else:
            browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            parser._go_to_page(page, url)
            content = page.content()

            # Извлекаем структурированные данные
            structured_data = parser._extract_review_data(content, url)

            print("Структурированные данные:")
            for key, value in structured_data.items():
                if key == "text":
                    print(
                        f"  {key}: '{value[:100]}...'" if value else f"  {key}: ПУСТО"
                    )
                else:
                    print(f"  {key}: {value}")

            # Шаг 2: Создание объекта Review
            print("\n=== ШАГ 2: СОЗДАНИЕ ОБЪЕКТА REVIEW ===")
            review_brand, review_model = parser._extract_brand_model(url)

            # Определяем тип контента
            content_type = "review"
            if url.count("/") >= 6:  # дополнение к отзыву
                content_type = "addition"

            review = Review(
                source="drom.ru",
                type=content_type,
                url=url,
                brand=review_brand,
                model=review_model,
                content=structured_data.get("text", ""),
                author=structured_data.get("author", ""),
                rating=structured_data.get("owner_rating"),
                views_count=structured_data.get("views", 0),
                comments_count=structured_data.get("comments", 0),
                likes_count=structured_data.get("likes", 0),
            )

            print(f"Объект Review создан:")
            print(f"  type: {review.type}")
            print(f"  content длина: {len(review.content)}")
            print(
                f"  content: '{review.content[:100]}...'"
                if review.content
                else "  content: ПУСТО"
            )
            print(f"  author: {review.author}")

            # Шаг 3: Сохранение в БД
            print("\n=== ШАГ 3: СОХРАНЕНИЕ В БД ===")
            db_path = "debug_reviews.db"
            db = Database(db_path)
            repository = ReviewRepository(db)

            # Проверяем, что сохраняется
            print(
                f"Перед сохранением - content: '{review.content[:100]}...'"
                if review.content
                else "Перед сохранением - content: ПУСТО"
            )

            try:
                repository.add(review)
                print("✓ Сохранено в БД успешно")
            except Exception as e:
                print(f"✗ Ошибка сохранения: {e}")

            # Шаг 4: Проверка в БД
            print("\n=== ШАГ 4: ПРОВЕРКА В БД ===")
            import sqlite3

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT url, type, LENGTH(content), SUBSTR(content, 1, 100) FROM reviews WHERE url = ?",
                (url,),
            )
            result = cursor.fetchone()
            conn.close()

            if result:
                print(f"Найдено в БД:")
                print(f"  URL: {result[0]}")
                print(f"  Type: {result[1]}")
                print(f"  Content length: {result[2]}")
                print(f"  Content preview: {result[3]}")
            else:
                print("НЕ НАЙДЕНО В БД")

        finally:
            browser.close()


if __name__ == "__main__":
    debug_full_process()
