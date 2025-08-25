"""Скрипт для проверки результатов парсинга."""

import os
import sqlite3
from typing import List, Dict


def get_db_path() -> str:
    """Получить путь к файлу БД."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "auto_reviews.db")


def get_reviews() -> List[Dict]:
    """Получить все отзывы из БД."""
    db_path = get_db_path()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT source, brand, model, content, author, rating, url
            FROM reviews
            ORDER BY id DESC
            LIMIT 10
        """
        )
        columns = [description[0] for description in cursor.description]
        reviews = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return reviews


def main():
    """Основная функция."""
    reviews = get_reviews()
    print(f"\nНайдено отзывов: {len(reviews)}\n")

    for i, review in enumerate(reviews, 1):
        print(f"Отзыв {i}:")
        print(f"Бренд: {review['brand']}")
        print(f"Модель: {review['model']}")
        print(f"Автор: {review['author']}")
        print(f"Рейтинг: {review['rating']}")
        print(f"URL: {review['url']}")
        print("Контент:")
        print("-" * 50)
        print(review["content"][:300], "..." if len(review["content"]) > 300 else "")
        print("-" * 50)
        print()


if __name__ == "__main__":
    main()
