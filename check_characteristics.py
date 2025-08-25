#!/usr/bin/env python3
"""
Проверяем какие характеристики сохраняются в базу
"""

import os
import sys
import sqlite3

# Добавляем корневую папку проекта в путь
project_root = "/home/analityk/Документы/projects/parser_project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.database.repositories.review_repository import (
    ReviewRepository,
)
from src.auto_reviews_parser.database.base import Database


def check_database_content():
    """Проверяем содержимое базы данных"""

    print("🔍 Проверяем содержимое базы данных...")

    # Подключаемся к базе напрямую
    conn = sqlite3.connect("auto_reviews.db")
    cursor = conn.cursor()

    # Получаем схему таблицы
    cursor.execute("PRAGMA table_info(reviews)")
    columns = cursor.fetchall()

    print(f"\n📋 Структура таблицы reviews ({len(columns)} полей):")
    for col in columns:
        col_id, name, col_type, not_null, default_val, pk = col
        print(f"   {col_id:2d}. {name:25s} {col_type:15s}")

    # Получаем один последний отзыв
    cursor.execute("SELECT * FROM reviews ORDER BY дата_парсинга DESC LIMIT 1")
    row = cursor.fetchone()

    if row:
        print(f"\n📄 Последний отзыв в базе:")
        for i, value in enumerate(row):
            col_name = columns[i][1]  # название колонки
            if value is not None and str(value).strip():
                print(f"   {col_name:25s}: {value}")

    # Проверяем сколько отзывов с заполненными характеристиками
    checks = [
        ("поколение", "Поколение"),
        ("год_выпуска", "Год выпуска"),
        ("тип_кузова", "Тип кузова"),
        ("трансмиссия", "Трансмиссия"),
        ("тип_привода", "Привод"),
        ("руль", "Руль"),
        ("пробег", "Пробег"),
        ("объем_двигателя", "Объем двигателя"),
        ("мощность_двигателя", "Мощность двигателя"),
        ("тип_топлива", "Тип топлива"),
        ("расход_в_городе", "Расход в городе"),
        ("расход_на_трассе", "Расход на трассе"),
        ("год_приобретения", "Год приобретения"),
    ]

    print(f"\n📊 Статистика заполненности характеристик:")
    for field, description in checks:
        cursor.execute(
            f"SELECT COUNT(*) FROM reviews WHERE {field} IS NOT NULL AND {field} != ''"
        )
        count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM reviews")
        total = cursor.fetchone()[0]
        print(f"   {description:25s}: {count:2d}/{total} ({count/total*100:.0f}%)")

    conn.close()


def test_single_characteristics():
    """Тестируем извлечение характеристик на одном отзыве"""

    print("\n🔧 Тестируем извлечение характеристик...")

    # URL отзыва с богатыми характеристиками
    test_url = "https://www.drom.ru/reviews/toyota/camry/1440434/"  # Михаил, 1985 год

    parser = DromParser(gentle_mode=True)

    try:
        reviews = parser.parse_single_review(test_url)

        if reviews:
            review = reviews[0]
            print(f"\n📄 Извлеченные характеристики из отзыва:")
            print(f"   Автор: {review.author}")
            print(f"   Поколение: '{review.generation}'")
            print(f"   Год: {review.year}")
            print(f"   Тип кузова: '{review.body_type}'")
            print(f"   Трансмиссия: '{review.transmission}'")
            print(f"   Привод: '{review.drive_type}'")
            print(f"   Руль: '{review.steering_wheel}'")
            print(f"   Пробег: {review.mileage}")
            print(f"   Объем двигателя: {review.engine_volume}")
            print(f"   Мощность: {review.engine_power}")
            print(f"   Тип топлива: '{review.fuel_type}'")
            print(f"   Расход в городе: {review.fuel_consumption_city}")
            print(f"   Расход на трассе: {review.fuel_consumption_highway}")
            print(f"   Год приобретения: {review.year_purchased}")

            # Сохраняем в базу
            db = Database("auto_reviews.db")
            repository = ReviewRepository(db)

            try:
                repository.save(review)
                print(f"\n✅ Отзыв сохранен в базу")
            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    print(f"\n⚠️  Отзыв уже существует в базе")
                else:
                    print(f"\n❌ Ошибка сохранения: {e}")

        else:
            print(f"❌ Не удалось спарсить отзыв")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_single_characteristics()
    check_database_content()
