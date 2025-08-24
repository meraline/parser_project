#!/usr/bin/env python3
"""Скрипт для парсинга первых 10 отзывов и сохранения в базу данных."""

import sys
import sqlite3
from pathlib import Path

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent))

from src.auto_reviews_parser.parsers.drom import DromParser


def create_database():
    """Создает базу данных для отзывов."""
    conn = sqlite3.connect("reviews.db")
    cursor = conn.cursor()

    # Создаем таблицу отзывов
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            type TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            brand TEXT NOT NULL,
            model TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    conn.commit()
    return conn


def save_review_to_db(conn, review):
    """Сохраняет отзыв в базу данных."""
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO reviews (source, type, url, brand, model, content)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                review.source,
                review.type,
                review.url,
                review.brand,
                review.model,
                review.content[:5000],  # Ограничиваем размер контента
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # Отзыв уже существует
        return False


def parse_and_save_reviews():
    """Парсит отзывы и сохраняет их в базу данных."""
    # Создаем базу данных
    conn = create_database()

    # Инициализируем парсер
    parser = DromParser()

    # Бренды для парсинга (первые 10 по алфавиту)
    brands = [
        "audi",
        "bmw",
        "chevrolet",
        "daewoo",
        "ford",
        "honda",
        "hyundai",
        "kia",
        "mazda",
        "nissan",
    ]

    total_saved = 0
    target_reviews = 10

    print(f"Начинаем парсинг первых {target_reviews} отзывов...")
    print("=" * 50)

    for brand in brands:
        if total_saved >= target_reviews:
            break

        print(f"\nПарсим бренд: {brand}")
        try:
            # Парсим отзывы бренда (ограничиваем количество)
            reviews = parser.parse_catalog(brand, max_reviews=5)

            print(f"Найдено отзывов для {brand}: {len(reviews)}")

            for review in reviews:
                if total_saved >= target_reviews:
                    break

                # Сохраняем отзыв в базу
                if save_review_to_db(conn, review):
                    total_saved += 1
                    print(
                        f"✓ Сохранен отзыв #{total_saved}: "
                        f"{review.brand} {review.model}"
                    )
                    print(f"  URL: {review.url}")
                else:
                    print(f"- Отзыв уже существует: {review.url}")

        except Exception as e:
            print(f"✗ Ошибка при парсинге {brand}: {e}")
            continue

    print("\n" + "=" * 50)
    print(f"Парсинг завершен. Сохранено отзывов: {total_saved}")

    # Показываем статистику
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM reviews")
    total_in_db = cursor.fetchone()[0]

    cursor.execute(
        "SELECT brand, COUNT(*) FROM reviews " "GROUP BY brand ORDER BY COUNT(*) DESC"
    )
    brand_stats = cursor.fetchall()

    print(f"Всего отзывов в базе: {total_in_db}")
    print("\nСтатистика по брендам:")
    for brand, count in brand_stats:
        print(f"  {brand}: {count} отзывов")

    conn.close()
    return total_saved


if __name__ == "__main__":
    try:
        saved_count = parse_and_save_reviews()
        print(f"\nУспешно завершено! Сохранено {saved_count} новых отзывов.")
    except KeyboardInterrupt:
        print("\nПарсинг прерван пользователем.")
    except Exception as e:
        print(f"\nОшибка: {e}")
        sys.exit(1)
